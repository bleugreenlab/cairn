use super::{
    finalize_run, finalize_run_for_recovery, stop_job, stop_session, stop_session_internal,
    suspend_run_for_durable_wait, transition_to_warm_state, StopReason,
};
use crate::agent_process::process::{GoalPauseOwner, RunHandle};
use crate::db::DbState;
use crate::models::{RunStatus, TurnEndReason};
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::TestServicesBuilder;
use crate::services::EventEmitter;
use crate::storage::{DbError, LocalDb, RowExt, SearchIndex};
use cairn_session::agent_process::completion::TurnFailureDisposition;
use serde_json::Value;
use std::sync::{Arc, Mutex};

type DbChangeHook = Box<dyn FnMut() -> bool + Send>;

#[derive(Clone, Default)]
struct SharedCaptureEmitter {
    events: Arc<Mutex<Vec<(String, Value)>>>,
    on_db_change: Arc<Mutex<Option<DbChangeHook>>>,
}

#[tokio::test]
async fn durable_wait_and_pending_park_share_codex_pause_before_interrupt_boundary() {
    for turn_state in ["running", "pending"] {
        let db = test_db().await;
        seed_top_level_run(&db, "run-park", "job-park", "turn-park", "none").await;
        db.execute_script(
            "INSERT INTO sessions (id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES ('session-park','job-park','codex','native-thread','open',1,1,1);
             UPDATE jobs SET current_session_id='session-park' WHERE id='job-park';
             UPDATE runs SET session_id='session-park' WHERE id='run-park';
             UPDATE turns SET session_id='session-park',run_id='run-park' WHERE id='turn-park';",
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE turns SET state = ?1 WHERE id = 'turn-park'",
            (turn_state,),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        let (stdin, requests, provider_turn) =
            crate::backends::codex::scripted_user_stop_stdin_for_test(
                "native-thread",
                "provider-live",
            );
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(stdin))),
            Some("session-park".into()),
            Some("job-park".into()),
        );
        handle.backend = Some("codex".into());
        handle.begin_turn("turn-park");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-park".into(), handle);
        orch.process_state
            .set_current_turn_id("run-park", Some("turn-park"));

        suspend_run_for_durable_wait(
            &orch,
            "run-park",
            "intentional_wait",
            GoalPauseOwner::DurableWait {
                wait_id: "wait-park".into(),
            },
        )
        .expect("wait/self-suspend must pause before parking");
        assert_eq!(
            self::turn_state(&orch, "turn-park").await,
            turn_state,
            "a direct wait parks the provider without rewriting logical work"
        );
        assert!(orch.process_state.is_warm("run-park"));
        assert_eq!(orch.process_state.get_current_turn_id("run-park"), None);
        *provider_turn.lock().unwrap() = None;

        let sent = requests.lock().unwrap();
        assert_eq!(
            sent.len(),
            3,
            "{turn_state} park must not start an idle successor"
        );
        assert_eq!(sent[0]["method"], "thread/goal/get", "{turn_state}");
        assert_eq!(sent[1]["method"], "thread/goal/set", "{turn_state}");
        assert_eq!(sent[1]["params"]["status"], "paused", "{turn_state}");
        assert_eq!(sent[2]["method"], "turn/interrupt", "{turn_state}");
        assert!(
            sent.iter().all(|request| request["method"] != "turn/start"),
            "{turn_state} park must leave the unfinished goal non-continuing"
        );
    }
}

/// The permission/prompt suspension stakes and parks only the exact turn it
/// yielded. A missing owner, a successor attached before the budget expired, or
/// a replacement registration must receive no goal control and no park.
#[tokio::test]
async fn user_input_suspension_stakes_and_parks_only_its_exact_yielded_turn() {
    for case in ["current", "missing", "successor", "replacement"] {
        let db = test_db().await;
        seed_top_level_run(&db, "run-ask", "job-ask", "turn-ask", "none").await;
        db.execute_script(
            "INSERT INTO sessions (id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES ('session-ask','job-ask','codex','native-thread','open',1,1,1);
             UPDATE jobs SET current_session_id='session-ask' WHERE id='job-ask';
             UPDATE runs SET session_id='session-ask' WHERE id='run-ask';
             UPDATE turns SET session_id='session-ask',run_id='run-ask' WHERE id='turn-ask';",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        let scripted_handle = || {
            let (stdin, requests, _provider_turn) =
                crate::backends::codex::scripted_user_stop_stdin_for_test(
                    "native-thread",
                    "provider-live",
                );
            let mut handle = RunHandle::new(
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(Some(stdin))),
                Some("session-ask".into()),
                Some("job-ask".into()),
            );
            handle.backend = Some("codex".into());
            handle.begin_turn("turn-ask");
            (handle, requests)
        };
        let (handle, mut requests) = scripted_handle();
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-ask".into(), handle);
        orch.process_state.yield_for_host("run-ask", "turn-ask");
        let captured = orch.process_state.capture_turn_owner("run-ask", "turn-ask");
        assert!(captured.is_some(), "{case}: the yielded turn is capturable");
        let turn_owner = match case {
            "missing" => None,
            "successor" => {
                assert!(orch.process_state.begin_turn("run-ask", "turn-successor"));
                captured
            }
            "replacement" => {
                // Even the same run and turn id: generation fences the old token.
                let (replacement, replacement_requests) = scripted_handle();
                orch.process_state
                    .processes
                    .lock()
                    .unwrap()
                    .register("run-ask".into(), replacement);
                requests = replacement_requests;
                captured
            }
            _ => captured,
        };
        let current_before = orch.process_state.get_current_turn_id("run-ask");

        crate::orchestrator::lifecycle::suspend_run_for_durable_wait_after_handoff(
            &orch,
            "run-ask",
            "permission_wait_suspended",
            turn_owner,
            GoalPauseOwner::UserInput {
                request_id: "perm-ask".into(),
            },
        );

        let sent = requests.lock().unwrap().clone();
        if case == "current" {
            assert_eq!(sent.len(), 2, "the exact owner stakes its pause at once");
            assert_eq!(sent[0]["method"], "thread/goal/get");
            assert_eq!(sent[1]["method"], "thread/goal/set");
            assert_eq!(sent[1]["params"]["status"], "paused");
            assert!(
                orch.park_slots.armed("run-ask"),
                "the exact owner's park is armed"
            );
        } else {
            assert!(
                sent.is_empty(),
                "{case}: no goal control may reach a turn this suspension does not own"
            );
            assert!(
                !orch.park_slots.armed("run-ask"),
                "{case}: no park may be armed without the exact owner"
            );
            assert_eq!(
                orch.process_state.get_current_turn_id("run-ask"),
                current_before,
                "{case}: the current registration's turn is untouched"
            );
        }
    }
}

#[tokio::test]
async fn user_stop_pauses_codex_goal_before_worst_order_idle_and_resumes_same_backend() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-stop", "job-stop", "turn-stop", "none").await;
    db.execute_script(
        "INSERT INTO sessions (id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
           VALUES ('session-stop','job-stop','codex','native-thread','open',1,1,1);
         UPDATE jobs SET current_session_id='session-stop' WHERE id='job-stop';
         UPDATE runs SET session_id='session-stop' WHERE id='run-stop';
         UPDATE turns SET session_id='session-stop',run_id='run-stop' WHERE id='turn-stop';",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    let (stdin, requests, provider_turn) =
        crate::backends::codex::scripted_user_stop_stdin_for_test("native-thread", "provider-live");
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(stdin))),
        Some("session-stop".into()),
        Some("job-stop".into()),
    );
    handle.backend = Some("codex".into());
    handle.begin_turn("turn-stop");
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run-stop".into(), handle);
    orch.process_state
        .set_current_turn_id("run-stop", Some("turn-stop"));

    stop_session(&orch, "run-stop").expect("user Stop should pause and interrupt");

    {
        let sent = requests.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[0]["method"], "thread/goal/get");
        assert_eq!(sent[1]["method"], "thread/goal/set");
        assert_eq!(sent[1]["params"]["status"], "paused");
        assert!(sent[1]["params"].get("objective").is_none());
        assert_eq!(sent[2]["method"], "turn/interrupt");
    }

    // Worst ordering: the provider reports the interrupted turn terminal and
    // reaches idle immediately after acknowledging the interrupt. Codex owns
    // on_thread_idle; with the goal already paused it emits no turn/start.
    *provider_turn.lock().unwrap() = None;
    assert_eq!(
        requests.lock().unwrap().len(),
        3,
        "provider completion/idle after Stop must not start a successor"
    );

    let content = crate::agent_process::stdin::MessageContent::text("resume explicitly");
    assert!(matches!(
        crate::backends::stdin::reactivate_goal(
            &orch.process_state,
            "run-stop",
            Some(&crate::execution::jobs::GoalReactivation::Operator {
                kind: "explicit_resume",
                id: None,
            }),
            crate::execution::jobs::JobRunnable::Attested,
            crate::execution::jobs::GoalIntent::On,
        )
        .expect("admission should inspect and reactivate the paused goal"),
        crate::backends::stdin::GoalReactivationResult::Lifted { .. }
    ));
    crate::backends::stdin::send_user_message(
        &orch.process_state,
        "run-stop",
        &content,
        "session-stop",
        None,
    )
    .expect("an explicit admitted message should reactivate the paused goal");

    {
        let sent = requests.lock().unwrap();
        assert_eq!(sent[3]["method"], "thread/goal/get");
        assert_eq!(sent[4]["method"], "thread/goal/set");
        assert_eq!(sent[4]["params"]["status"], "active");
        assert_eq!(sent[5]["method"], "turn/start");
        assert_eq!(sent[5]["params"]["threadId"], "native-thread");
        assert_eq!(sent[5]["params"]["input"][0]["text"], "resume explicitly");
    }
    assert_eq!(
        orch.db
            .local
            .query_opt_text(
                "SELECT backend_id FROM sessions WHERE id = ?1",
                ("session-stop".to_string(),),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("native-thread")
    );
}

#[tokio::test]
async fn user_stop_cancels_a_yielded_durable_wait_and_settles_its_turn() {
    let db = test_db().await;
    seed_top_level_run(
        &db,
        "run-wait-stop",
        "job-wait-stop",
        "turn-wait-stop",
        "none",
    )
    .await;
    db.write(|conn| {
        Box::pin(async move {
            conn.execute("INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-wait-stop','job-wait-stop','open','handle-wait-stop',1,1)", ()).await?;
            conn.execute("UPDATE runs SET session_id='session-wait-stop' WHERE id='run-wait-stop'", ()).await?;
            conn.execute("UPDATE turns SET session_id='session-wait-stop',run_id='run-wait-stop',state='yielded',yield_reason='wait' WHERE id='turn-wait-stop'", ()).await?;
            conn.execute("INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-stop','job-wait-stop','run-wait-stop','session-wait-stop','turn-wait-stop','tool-wait-stop','{\"kind\":\"terminal\"}','pending',1)", ()).await?;
            Ok::<_, DbError>(())
        })
    }).await.unwrap();
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-wait-stop", Some("job-wait-stop"));
    orch.process_state
        .set_current_turn_id("run-wait-stop", Some("turn-wait-stop"));

    stop_job(&orch, "job-wait-stop").expect("Stop should abandon the durable wait");

    assert_eq!(turn_state(&orch, "turn-wait-stop").await, "cancelled");
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT state FROM agent_waits WHERE id = ?1",
            "wait-stop"
        )
        .await
        .as_deref(),
        Some("cancelled"),
        "the wait must not fire after Stop"
    );
}

impl SharedCaptureEmitter {
    fn events_named(&self, name: &str) -> Vec<Value> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(event, _)| event == name)
            .map(|(_, payload)| payload.clone())
            .collect()
    }
}

impl EventEmitter for SharedCaptureEmitter {
    fn emit(&self, event: &str, payload: Value) -> Result<(), String> {
        if event == "db-change" {
            let callback = self.on_db_change.lock().unwrap().take();
            if let Some(mut callback) = callback {
                if !callback() {
                    *self.on_db_change.lock().unwrap() = Some(callback);
                }
            }
        }
        self.events
            .lock()
            .unwrap()
            .push((event.to_string(), payload));
        Ok(())
    }

    fn emit_empty(&self, event: &str) -> Result<(), String> {
        self.emit(event, Value::Null)
    }
}

async fn test_db() -> LocalDb {
    let temp = tempfile::tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(temp.keep().join("warm-completion.db"))
        .await
        .unwrap();
    db
}

pub(super) fn test_orchestrator(db: LocalDb) -> Orchestrator {
    test_orchestrator_with_emitter(db, None).0
}

fn test_orchestrator_with_emitter(
    db: LocalDb,
    emitter: Option<SharedCaptureEmitter>,
) -> (Orchestrator, SharedCaptureEmitter) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
    let emitter = emitter.unwrap_or_default();
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_emitter(emitter.clone())
            .build(),
    );
    (
        OrchestratorBuilder::new(db_state, services, config_dir).build(),
        emitter,
    )
}

/// Register a warm-able process so `transition_to_warm` succeeds. The stdin
/// is an in-memory writer; the child slot is empty (no real process).
pub(super) fn register_warm_process(orch: &Orchestrator, run_id: &str, job_id: Option<&str>) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let child = Arc::new(Mutex::new(None));
    let stdin = Arc::new(Mutex::new(Some(
        cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
    )));
    let handle = RunHandle::new(
        child,
        stdin,
        Some(format!("sess-{run_id}")),
        job_id.map(str::to_string),
    );
    processes.register(run_id.to_string(), handle);
}

fn register_process_without_stdin(orch: &Orchestrator, run_id: &str, job_id: Option<&str>) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(None)),
        Some(format!("sess-{run_id}")),
        job_id.map(str::to_string),
    );
    handle.begin_turn("turn-without-stdin");
    processes.register(run_id.to_string(), handle);
}

async fn turn_state(orch: &Orchestrator, id: &str) -> String {
    let id = id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT state FROM turns WHERE id = ?1", (id.as_str(),))
                    .await?;
                rows.next().await?.unwrap().text(0)
            })
        })
        .await
        .unwrap()
}

async fn seed_top_level_run(
    db: &LocalDb,
    run_id: &str,
    job_id: &str,
    turn_id: &str,
    attention: &str,
) {
    let run_id = run_id.to_string();
    let job_id = job_id.to_string();
    let turn_id = turn_id.to_string();
    let attention = attention.to_string();
    db.write(move |conn| {
            let run_id = run_id.clone();
            let job_id = job_id.clone();
            let turn_id = turn_id.clone();
            let attention = attention.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-attn','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-attn','w-attn','Project','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-attn','p-attn',42,'Toast issue','active',?1,1,1)", (attention.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES (?1,'i-attn','p-attn','running','builder','builder',1,1)", (job_id.as_str(),)).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES (?1,?2,'live',1,1)", (run_id.as_str(), job_id.as_str())).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, run_id, sequence, state, start_reason, created_at, updated_at) VALUES (?1,'sess-attn',?2,?3,1,'running','initial',1,1)", (turn_id.as_str(), job_id.as_str(), run_id.as_str())).await?;
                conn.execute("UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2", (turn_id.as_str(), job_id.as_str())).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
}

/// The operator's entries for the seeded project, derived from rows the way
/// the notification center derives them.
async fn operator_entries(orch: &Orchestrator) -> Vec<crate::notifications::OperatorNotification> {
    crate::notifications::derive_snapshot(
        &orch.db.local,
        &orch.db.local,
        &crate::notifications::HoldRegistry::default(),
        "p-attn",
        // Past the settle window, when an unrecovered crash becomes an entry.
        chrono::Utc::now().timestamp() + crate::notifications::AGENT_FAILURE_SETTLE_SECS + 1,
        &std::collections::HashMap::new(),
    )
    .await
    .unwrap()
    .entries
}

#[tokio::test]
async fn stop_hard_terminates_instead_of_warming_when_interrupt_delivery_fails() {
    let db = test_db().await;
    let orch = test_orchestrator(db);
    register_process_without_stdin(&orch, "run-no-stdin", Some("job-no-stdin"));

    stop_session_internal(&orch, "run-no-stdin", StopReason::OperatorStop)
        .expect("hard-stop fallback should complete");

    assert!(
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .get("run-no-stdin")
            .is_none(),
        "failed interrupt delivery must not leave the process parked warm"
    );
}

#[tokio::test]
async fn durable_wait_preserves_warm_process_when_interrupt_delivery_fails() {
    let db = test_db().await;
    let orch = test_orchestrator(db);
    register_process_without_stdin(&orch, "run-suspend", Some("job-suspend"));

    suspend_run_for_durable_wait(
        &orch,
        "run-suspend",
        "delegated_wait_suspended",
        GoalPauseOwner::DependencyWait {
            parked_turn_id: "turn-suspend".into(),
        },
    )
    .expect("durable wait suspend should tolerate interrupt failure");

    let processes = orch.process_state.processes.lock().unwrap();
    let handle = processes
        .get("run-suspend")
        .expect("durable wait must keep the process registered for resume");
    assert!(
        handle.is_warm(),
        "durable wait must park the process warm instead of hard-killing it"
    );
}

#[tokio::test]
async fn warm_transition_persists_artifact_handoff_reason() {
    let db = test_db().await;
    seed_top_level_run(
        &db,
        "run-handoff",
        "job-handoff",
        "turn-handoff",
        "needs_approval",
    )
    .await;
    let (orch, _) = test_orchestrator_with_emitter(db, None);
    register_warm_process(&orch, "run-handoff", Some("job-handoff"));
    orch.process_state
        .set_current_turn_id("run-handoff", Some("turn-handoff"));

    assert!(transition_to_warm_state(
        &orch,
        "run-handoff",
        Some(TurnEndReason::ArtifactHandoff),
    ));

    let reason = orch
        .db
        .local
        .query_opt_text(
            "SELECT end_reason FROM turns WHERE id = ?1",
            ("turn-handoff".to_string(),),
        )
        .await
        .unwrap();
    assert_eq!(reason.as_deref(), Some("artifact_handoff"));
}

/// The `agent-attention` channel is retired: a turn end raises nothing on it,
/// actionable or not. What the operator sees is derived from rows.
#[tokio::test]
async fn warm_transition_raises_no_legacy_attention_event() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-attn", "job-attn", "turn-attn", "needs_approval").await;
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);
    register_warm_process(&orch, "run-attn", Some("job-attn"));
    orch.process_state
        .set_current_turn_id("run-attn", Some("turn-attn"));

    assert!(transition_to_warm_state(&orch, "run-attn", None));

    assert!(emitter.events_named("agent-attention").is_empty());
}

#[tokio::test]
async fn later_finalize_does_not_duplicate_run_completion() {
    let db = test_db().await;
    seed_top_level_run(
        &db,
        "run-dedupe",
        "job-dedupe",
        "turn-dedupe",
        "needs_approval",
    )
    .await;
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);
    register_warm_process(&orch, "run-dedupe", Some("job-dedupe"));
    orch.process_state
        .set_current_turn_id("run-dedupe", Some("turn-dedupe"));

    assert!(transition_to_warm_state(&orch, "run-dedupe", None));
    finalize_run(&orch, "run-dedupe", RunStatus::Exited);

    assert_eq!(emitter.events_named("run-completed").len(), 1);
}

#[tokio::test]
async fn recovery_finalization_preserves_an_already_complete_turn() {
    let db = test_db().await;
    seed_top_level_run(
        &db,
        "run-terminal-recovery",
        "job-terminal-recovery",
        "turn-terminal-recovery",
        "none",
    )
    .await;
    db.execute(
        "UPDATE turns SET state = 'complete', ended_at = 2 WHERE id = 'turn-terminal-recovery'",
        (),
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    finalize_run_for_recovery(&orch, "run-terminal-recovery", RunStatus::Crashed)
        .expect("run ownership should finalize without rewriting the terminal turn");

    assert_eq!(
        turn_state(&orch, "turn-terminal-recovery").await,
        "complete"
    );
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT status FROM runs WHERE id = ?1",
            "run-terminal-recovery"
        )
        .await
        .as_deref(),
        Some("crashed")
    );
}

#[tokio::test]
async fn crashed_run_and_fresh_context_reload_preserve_todo_statuses() {
    let db = test_db().await;
    seed_top_level_run(
        &db,
        "run-todo-resume",
        "job-todo-resume",
        "turn-todo-resume",
        "none",
    )
    .await;
    db.execute_script(
        "
        INSERT INTO todos (
            id, job_id, todo_id, content, status, position, created_at, updated_at
        ) VALUES
            ('todo-active', 'job-todo-resume', '1', 'Continue implementation', 'in_progress', 0, 1, 1),
            ('todo-next', 'job-todo-resume', '2', 'Verify behavior', 'pending', 1, 1, 1);
        ",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    finalize_run(&orch, "run-todo-resume", RunStatus::Crashed);

    // Cross the canonical fresh-session preparation boundary used by a resumed
    // job after its previous provider run has exited.
    let (session_id, session_start, run_start_mode) =
        crate::execution::jobs::prepare_session_for_test(
            orch.db.local.clone(),
            "job-todo-resume",
            2,
        )
        .await
        .unwrap();
    assert_eq!(run_start_mode, "fresh");
    assert!(matches!(
        session_start,
        crate::backends::SessionStart::New { session_id: ref id } if id == &session_id
    ));
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT current_session_id FROM jobs WHERE id = ?1",
            "job-todo-resume"
        )
        .await
        .as_deref(),
        Some(session_id.as_str())
    );

    // Fresh-session context reconstructs authored work state from the same
    // job-scoped todo store; neither boundary may infer completion.
    let todos = crate::todos::get_todos_for_job(&orch.db.local, "job-todo-resume")
        .await
        .unwrap();
    assert_eq!(
        todos
            .iter()
            .map(|todo| (todo.id.as_str(), todo.status.as_str()))
            .collect::<Vec<_>>(),
        vec![("1", "in_progress"), ("2", "pending")]
    );
}

#[tokio::test]
async fn terminal_job_completion_completes_only_in_progress_todos() {
    let db = test_db().await;
    // Use a reducible execution node: a standalone fixture without an
    // execution cannot derive job completion from its terminal turn.
    seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
    db.execute_script(
        "
        INSERT INTO todos (
            id, job_id, todo_id, content, status, position, created_at, updated_at
        ) VALUES
            ('todo-active-complete', 'j-child', '1', 'Finish implementation', 'in_progress', 0, 1, 1),
            ('todo-pending-complete', 'j-child', '2', 'Optional follow-up', 'pending', 1, 1, 1);
        ",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-child", Some("j-child"));
    orch.process_state
        .set_current_turn_id("run-child", Some("child-turn"));

    finalize_run(&orch, "run-child", RunStatus::Exited);

    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("complete")
    );
    let todos = crate::todos::get_todos_for_job(&orch.db.local, "j-child")
        .await
        .unwrap();
    assert_eq!(
        todos
            .iter()
            .map(|todo| (todo.id.as_str(), todo.status.as_str()))
            .collect::<Vec<_>>(),
        vec![("1", "completed"), ("2", "pending")]
    );
}

/// A crash that terminalizes an in-flight turn leaves exactly one open
/// "agent failed" entry, however often finalization runs, and no legacy event.
#[tokio::test]
async fn crash_finalize_leaves_one_open_agent_failed_entry() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-crash", "job-crash", "turn-crash", "none").await;
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);

    finalize_run(&orch, "run-crash", RunStatus::Crashed);
    finalize_run(&orch, "run-crash", RunStatus::Crashed);

    let failed: Vec<_> = operator_entries(&orch)
        .await
        .into_iter()
        .filter(|entry| entry.kind == crate::notifications::NotificationKind::AgentFailed)
        .collect();
    assert_eq!(failed.len(), 1, "{failed:#?}");
    assert_eq!(failed[0].key, "agent_failed:turn-crash");
    assert!(failed[0].is_open());
    assert!(emitter.events_named("agent-attention").is_empty());
}

/// A pooled call has no child of its own. Tearing its run down interrupts its
/// live turn and unsubscribes its thread on the shared server, which keeps
/// running for its neighbours.
#[tokio::test]
async fn killing_a_pooled_run_releases_its_thread_and_spares_the_server() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-pooled", "job-pooled", "turn-pooled", "none").await;
    let orch = test_orchestrator(db);
    let (server, _stream, requests) =
        crate::backends::codex::pool::testing::recording_server(&orch.codex_pool);
    let _call = server.register_call("thread-pooled", "run-pooled");
    let _neighbour = server.register_call("thread-neighbour", "run-neighbour");
    let mut handle = crate::agent_process::process::RunHandle::test_handle(
        Some("session-pooled"),
        Some("job-pooled"),
    );
    handle.pooled_thread = Some(Arc::new(
        crate::backends::codex::pool::PooledCall::new(server.clone(), "thread-pooled".into())
            .tracking_turn(Arc::new(Mutex::new(Some("turn-live".into())))),
    ));
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run-pooled".into(), handle);

    crate::orchestrator::lifecycle::kill_session_with_reason(
        &orch,
        "run-pooled",
        "user_stopped",
        crate::orchestrator::lifecycle::TerminalPolicy::Finalize,
    )
    .unwrap();
    assert!(server.settle_releases(std::time::Duration::from_secs(5)));

    let sent: Vec<(String, Value)> = requests
        .lock()
        .unwrap()
        .iter()
        .map(|request| {
            (
                request["method"].as_str().unwrap_or_default().to_string(),
                request["params"]["threadId"].clone(),
            )
        })
        .collect();
    assert_eq!(
        sent,
        vec![
            (
                "thread/goal/get".to_string(),
                serde_json::json!("thread-pooled")
            ),
            (
                "turn/interrupt".to_string(),
                serde_json::json!("thread-pooled")
            ),
            (
                "thread/unsubscribe".to_string(),
                serde_json::json!("thread-pooled")
            ),
        ]
    );
    assert!(orch
        .codex_pool
        .binding_for_thread("thread-pooled")
        .is_none());
    assert!(!server.is_closed(), "the shared server is never stopped");
    assert_eq!(
        orch.codex_pool
            .binding_for_thread("thread-neighbour")
            .map(|binding| binding.run_id),
        Some("run-neighbour".to_string())
    );
}

/// `stop_and_remove` (admission replacement, call finalization) releases a
/// pooled thread too, even while its reader still holds the thread: removing
/// the handle alone would leave a hung turn running and subscribed.
#[tokio::test]
async fn stop_and_remove_releases_a_pooled_thread_its_reader_still_holds() {
    let db = test_db().await;
    let orch = test_orchestrator(db);
    let (server, _stream, requests) =
        crate::backends::codex::pool::testing::recording_server(&orch.codex_pool);
    let _call = server.register_call("thread-held", "run-held");
    let thread = Arc::new(
        crate::backends::codex::pool::PooledCall::new(server.clone(), "thread-held".into())
            .tracking_turn(Arc::new(Mutex::new(Some("turn-live".into())))),
    );
    let reader_holds = thread.clone();
    let mut handle = RunHandle::test_handle(Some("session-held"), Some("job-held"));
    handle.pooled_thread = Some(thread);
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run-held".into(), handle);

    assert!(orch.process_state.stop_and_remove("run-held"));
    assert!(server.settle_releases(std::time::Duration::from_secs(5)));

    let methods: Vec<String> = requests
        .lock()
        .unwrap()
        .iter()
        .map(|request| request["method"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        methods,
        ["thread/goal/get", "turn/interrupt", "thread/unsubscribe"]
    );
    assert!(orch.codex_pool.binding_for_thread("thread-held").is_none());
    assert!(!server.is_closed());
    drop(reader_holds);
    assert!(server.settle_releases(std::time::Duration::from_secs(5)));
    assert_eq!(
        requests.lock().unwrap().len(),
        3,
        "the reader's drop sends nothing more"
    );
}

/// A pooled Codex server's bearer outlives every run it serves, so finalizing a
/// run is what stops that bearer speaking for it: its threads stop resolving,
/// the moment it finalizes, while its neighbour on the same server keeps going.
#[tokio::test]
async fn finalize_ends_the_run_s_pooled_thread_bindings() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-pooled", "job-pooled", "turn-pooled", "none").await;
    let orch = test_orchestrator(db);
    let (server, _stream) = crate::backends::codex::pool::testing::live_server(&orch.codex_pool);
    let _ended = server.register_call("thread-ended", "run-pooled");
    let _other = server.register_call("thread-other", "run-other");
    let bind = |thread: &str| {
        let mut request = crate::mcp::types::McpCallbackRequest {
            thread_id: Some(thread.into()),
            ..Default::default()
        };
        crate::mcp::run_bearer::bind_request(
            &orch,
            &mut request,
            &crate::mcp::run_bearer::BearerScope::CodexPool(server.id().to_string()),
        )
        .map(|()| request.run_id)
    };
    assert_eq!(bind("thread-ended").unwrap().as_deref(), Some("run-pooled"));

    finalize_run(&orch, "run-pooled", RunStatus::Exited);

    assert!(orch.codex_pool.binding_for_thread("thread-ended").is_none());
    assert!(
        bind("thread-ended").is_err(),
        "the pool's bearer no longer speaks for a finalized run"
    );
    assert_eq!(bind("thread-other").unwrap().as_deref(), Some("run-other"));
}

#[tokio::test]
async fn warm_transition_broadcasts_run_completion() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-bcast", "job-bcast", "turn-bcast", "none").await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-bcast", Some("job-bcast"));
    orch.process_state
        .set_current_turn_id("run-bcast", Some("turn-bcast"));

    // `spawn_task_packets`' inline 45s wait subscribes here to detect a
    // fast-finishing child. A warmed child must broadcast on it just like
    // `finalize_run` does, or a sub-45s batch never returns inline.
    let mut rx = orch.run_completions.subscribe();
    assert!(transition_to_warm_state(&orch, "run-bcast", None));
    assert_eq!(rx.try_recv().ok(), Some("run-bcast".to_string()));

    // The released process owns no turn, so a repeated completion is refused
    // and must not announce the run a second time.
    assert!(!transition_to_warm_state(&orch, "run-bcast", None));
    assert!(rx.try_recv().is_err());
}

/// Seed a parent suspended on a delegated wait (anchor turn + pending
/// `dependency_unblock` successor it points at) and a completed delegated
/// child whose execution snapshot carries the matching packet. The parent
/// has no session, so once the resume gate claims the successor the
/// subsequent `continue_job_impl` fast-fails cleanly — the claimed successor
/// is the observable proof the resume fired.
async fn seed_suspended_parent_with_completed_child(db: &LocalDb, snapshot: String) {
    db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-warm','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-warm','w-warm','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-warm','p-warm',1,'T','active','none',1,1)", ()).await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-warm','recipe','i-warm','p-warm','running',1,1,?1)",
                    (snapshot.as_str(),),
                ).await?;
                // Anchor turn the parent suspended on, plus its pending
                // dependency_unblock successor (what the resume gate claims).
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('anchor','s-parent','j-parent',1,'yielded','dependency_wait','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('succ','s-parent','j-parent',2,'anchor','pending','dependency_unblock',1,1)", ()).await?;
                // Suspended parent: current_turn_id points at the pending
                // successor; no current_session_id so the post-claim
                // continue_job_impl fast-fails after the successor is claimed.
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, current_turn_id, created_at, updated_at) VALUES ('j-parent','e-warm','i-warm','p-warm','waiting','succ',1,1)", ()).await?;
                // Completed delegated child whose run finishes via the warm path.
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('j-child','e-warm','j-parent','i-warm','p-warm','complete',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','live',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
}

#[tokio::test]
async fn warm_completion_resumes_suspended_delegated_parent() {
    let snapshot = serde_json::json!({
            "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual", "nodes": [], "edges": []},
            "agents": {},
            "skills": {},
            "triggerContext": {"issueId": "i-warm", "projectId": "p-warm", "triggerType": "manual"},
            "delegatedPackets": [{
                "id": "pkt-1",
                "parentJobId": "j-parent",
                "parentTurnId": "anchor",
                "parentToolUseId": "tool-1",
                "origin": "task_tool",
                "title": "Explore",
                "problemStatement": "x",
                "agentConfigId": "Explore",
                "ownership": {"cwd": "/tmp"},
                "outputContract": {"schemaType": "return"},
                "resultArtifactJobId": "j-child",
                "status": "completed",
                "taskIndex": 0,
                "createdAt": 0
            }],
            "createdAt": 0
        })
        .to_string();

    let db = test_db().await;
    seed_suspended_parent_with_completed_child(&db, snapshot).await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-child", Some("j-child"));
    // The child's last turn is an untracked one (a direct-message resume), the
    // occupancy `transition_to_active` admits. A registration that never
    // admitted a turn has nothing to complete.
    assert!(orch.process_state.transition_to_active("run-child"));

    // Pre-completion: the parent's resume successor is still pending.
    assert_eq!(turn_state(&orch, "succ").await, "pending");

    // The child completes through the warm path (CAIRN-1576), not finalize_run.
    assert!(transition_to_warm_state(&orch, "run-child", None));

    // The resume gate fired: the pending successor was claimed (flipped
    // terminal), the linkage finalize_run's try_resume_delegated_parent
    // produces. Before the fix this stayed 'pending' forever and the parent
    // batch hung.
    assert_eq!(turn_state(&orch, "succ").await, "complete");
}

/// Seed a parent suspended on a delegated wait (anchor turn + pending
/// `dependency_unblock` successor) and a delegated child that is still
/// *running* — a live run, a `running` job, and a `running` current turn.
/// This is the durable-suspend shape: the parent finalized its own run and
/// is blocked on the resume gate, waiting on this child. A genuine turn
/// failure of the child must fail it terminally and fire the gate.
async fn seed_suspended_parent_with_running_child(db: &LocalDb, snapshot: String) {
    db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-warm','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-warm','w-warm','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-warm','p-warm',1,'T','active','none',1,1)", ()).await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-warm','recipe','i-warm','p-warm','running',1,1,?1)",
                    (snapshot.as_str(),),
                ).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('anchor','s-parent','j-parent',1,'yielded','dependency_wait','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('succ','s-parent','j-parent',2,'anchor','pending','dependency_unblock',1,1)", ()).await?;
                // Parent job stays `running` while suspended on the delegated
                // wait (it is the issue that is `waiting`, not the job). A valid
                // JobStatus matters: the execution sweep loads every job in the
                // execution and parses its status.
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, current_turn_id, created_at, updated_at) VALUES ('j-parent','e-warm','i-warm','p-warm','running','succ',1,1)", ()).await?;
                // Delegated child still running: live run + running current turn.
                // It carries a recipe_node_id for its synthetic node so the
                // execution sweep recomputes its status (production shape).
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, parent_job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('j-child','e-warm','child-node','j-parent','i-warm','p-warm','running',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','live',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('child-turn','s-child','j-child',1,'running','initial',1,1)", ()).await?;
                conn.execute("UPDATE jobs SET current_turn_id = 'child-turn' WHERE id = 'j-child'", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
}

fn running_child_snapshot() -> String {
    // The delegated child materializes as a synthetic Agent node in the
    // execution snapshot, which is what makes the execution sweep recompute
    // its status from its turn outcome. A bare node with no edges is
    // dag-ready and has no downstream artifact contract.
    let child_node = serde_json::json!({
        "id": "child-node",
        "nodeType": "agent",
        "name": "Child",
        "position": {"x": 0.0, "y": 0.0},
        "parentId": null,
        "triggerConfig": null,
        "agentConfig": null,
        "actionConfig": null,
        "checkpointConfig": null,
        "artifactConfig": null,
        "conditionConfig": null,
        "contextConfig": null
    });
    serde_json::json!({
            "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual", "nodes": [child_node], "edges": []},
            "agents": {},
            "skills": {},
            "triggerContext": {"issueId": "i-warm", "projectId": "p-warm", "triggerType": "manual"},
            "delegatedPackets": [{
                "id": "pkt-1",
                "parentJobId": "j-parent",
                "parentTurnId": "anchor",
                "parentToolUseId": "tool-1",
                "origin": "task_tool",
                "title": "Explore",
                "problemStatement": "x",
                "agentConfigId": "Explore",
                "ownership": {"cwd": "/tmp"},
                "outputContract": {"schemaType": "return"},
                "resultArtifactJobId": "j-child",
                "status": "running",
                "taskIndex": 0,
                "createdAt": 0
            }],
            "createdAt": 0
        })
        .to_string()
}

/// The durable-suspend hang, fixed: a genuine turn failure of a *running*
/// delegated child finalizes it terminally `Failed` (not a resumable
/// interrupt), so its packet resolves `Failed` and the suspended parent's
/// resume gate fires (the pending successor is claimed). Before the fix the
/// child's turn was interrupted, the packet stayed `Materialized`, and the
/// parent hung in `running` forever.
#[tokio::test]
async fn fail_run_on_delegated_task_resumes_suspended_parent() {
    let db = test_db().await;
    seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-child", Some("j-child"));
    orch.process_state
        .set_current_turn_id("run-child", Some("child-turn"));

    assert_eq!(turn_state(&orch, "succ").await, "pending");

    super::fail_run(&orch, "run-child", "turn_failed");

    // The child's turn is terminally Failed (not interrupted).
    assert_eq!(turn_state(&orch, "child-turn").await, "failed");
    // The child job derives Failed from the failed turn.
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("failed"),
    );
    // The resume gate fired: the parent's pending successor was claimed.
    assert_eq!(turn_state(&orch, "succ").await, "complete");
}

/// A genuine turn failure of a *top-level* job (no `parent_job_id`) stays a
/// resumable interruption — `fail_run` falls back to the unchanged
/// `finalize_run(Crashed)` path: the running turn is interrupted, not
/// failed, and the job is not driven terminal. Only a delegated task has a
/// blocked parent that needs a terminal answer.
#[tokio::test]
async fn fail_run_on_top_level_job_stays_resumable() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-top", "job-top", "turn-top", "none").await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-top", Some("job-top"));
    orch.process_state
        .set_current_turn_id("run-top", Some("turn-top"));

    super::fail_run(&orch, "run-top", "turn_failed");

    // The running turn is interrupted (resumable), not failed.
    assert_eq!(turn_state(&orch, "turn-top").await, "interrupted");
    // The job is not terminally failed.
    assert_ne!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "job-top")
            .await
            .as_deref(),
        Some("failed"),
    );
}

/// Seed a parent durably suspended on a delegated wait (anchor turn + pending
/// `dependency_unblock` successor) and a NODE-LESS delegated child — a workflow
/// (or ephemeral call) whose pre-materialized packet gives it no recipe node, so
/// the execution sweep never reduces its status. The child has run its turn and
/// written its output artifact; its `run` is still `live` and its job is still
/// `running`. `finalize_run(Exited)` must complete the turn, terminalize the
/// node-less job, and fire the resume gate. This is the exact CAIRN-2559 shape.
async fn seed_suspended_parent_with_completed_nodeless_child(db: &LocalDb, snapshot: String) {
    db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-wf','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-wf','w-wf','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-wf','p-wf',1,'T','active','none',1,1)", ()).await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-wf','recipe','i-wf','p-wf','running',1,1,?1)",
                    (snapshot.as_str(),),
                ).await?;
                // Suspended parent: yielded anchor + the pending successor it points at.
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('anchor','s-parent','j-parent',1,'yielded','dependency_wait','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('succ','s-parent','j-parent',2,'anchor','pending','dependency_unblock',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, current_turn_id, created_at, updated_at) VALUES ('j-parent','e-wf','i-wf','p-wf','running','succ',1,1)", ()).await?;
                // Node-less workflow child: recipe_node_id NULL, an output
                // contract on the job, a running turn (finalize completes it),
                // and its written `return` artifact.
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, output_contract, agent_config_id, created_at, updated_at) VALUES ('j-child','e-wf','j-parent','i-wf','p-wf','running','{\"schemaType\":{\"type\":\"object\"},\"description\":\"Submit\"}','workflow',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','live',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('child-turn','s-child','run-child','j-child',1,'running','initial',1,1)", ()).await?;
                conn.execute("UPDATE jobs SET current_turn_id='child-turn' WHERE id='j-child'", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-child','j-child','return',1,'{\"question\":\"q\",\"summary\":\"s\"}',1,'return',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
}

fn completed_nodeless_child_snapshot(packet_status: &str) -> String {
    serde_json::json!({
            "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual", "nodes": [], "edges": []},
            "agents": {},
            "skills": {},
            "triggerContext": {"issueId": "i-wf", "projectId": "p-wf", "triggerType": "manual"},
            "delegatedPackets": [{
                "id": "pkt-1",
                "parentJobId": "j-parent",
                "parentTurnId": "anchor",
                "parentToolUseId": "tool-1",
                "origin": "call_tool",
                "title": "deep-research",
                "problemStatement": "x",
                "agentConfigId": "workflow",
                "ownership": {"cwd": "/tmp"},
                "outputContract": {"schemaType": {"type": "object"}, "description": "Submit"},
                "resultArtifactJobId": "j-child",
                "status": packet_status,
                "taskIndex": 0,
                "createdAt": 0
            }],
            "createdAt": 0
        })
        .to_string()
}

/// The CAIRN-2559 clean path: a node-less workflow child that exits 0 with its
/// output artifact written must be terminalized `complete` on `finalize_run`
/// (the execution sweep can't reach a node-less job), so its delegated packet
/// resolves and the durably-suspended caller's resume gate fires. Before the fix
/// the child job stayed `running`, the packet stayed `running`, and the parent
/// hung `blocked` forever.
#[tokio::test]
async fn finalize_exit_terminalizes_nodeless_workflow_child_and_resumes_parent() {
    let db = test_db().await;
    seed_suspended_parent_with_completed_nodeless_child(
        &db,
        completed_nodeless_child_snapshot("running"),
    )
    .await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-child", Some("j-child"));
    orch.process_state
        .set_current_turn_id("run-child", Some("child-turn"));

    assert_eq!(turn_state(&orch, "succ").await, "pending");

    finalize_run(&orch, "run-child", RunStatus::Exited);

    // The child's running turn completed on clean exit.
    assert_eq!(turn_state(&orch, "child-turn").await, "complete");
    // The node-less workflow job is terminalized `complete` (the fix — the
    // sweep leaves it `running`).
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("complete"),
    );
    // The resume gate fired: the parent's pending successor was claimed.
    assert_eq!(turn_state(&orch, "succ").await, "complete");
}

/// The failure branch of the same shape: a node-less workflow child whose turn
/// genuinely fails (exit-without-artifact / non-zero exit routes through
/// `fail_run`) must terminalize `failed`, so its packet resolves `Failed` and
/// the suspended caller resumes with the failure instead of hanging.
#[tokio::test]
async fn fail_run_terminalizes_nodeless_workflow_child_and_resumes_parent() {
    let db = test_db().await;
    // No artifact seeded: this child ran but never satisfied its output contract.
    db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-wf','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-wf','w-wf','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-wf','p-wf',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-wf','recipe','i-wf','p-wf','running',1,1,?1)", (completed_nodeless_child_snapshot("running").as_str(),)).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('anchor','s-parent','j-parent',1,'yielded','dependency_wait','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('succ','s-parent','j-parent',2,'anchor','pending','dependency_unblock',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, current_turn_id, created_at, updated_at) VALUES ('j-parent','e-wf','i-wf','p-wf','running','succ',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, output_contract, agent_config_id, created_at, updated_at) VALUES ('j-child','e-wf','j-parent','i-wf','p-wf','running','{\"schemaType\":{\"type\":\"object\"},\"description\":\"Submit\"}','workflow',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','live',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('child-turn','s-child','run-child','j-child',1,'running','initial',1,1)", ()).await?;
                conn.execute("UPDATE jobs SET current_turn_id='child-turn' WHERE id='j-child'", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-child", Some("j-child"));
    orch.process_state
        .set_current_turn_id("run-child", Some("child-turn"));

    assert_eq!(turn_state(&orch, "succ").await, "pending");

    super::fail_run(&orch, "run-child", "workflow exited without output");

    assert_eq!(turn_state(&orch, "child-turn").await, "failed");
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("failed"),
    );
    assert_eq!(turn_state(&orch, "succ").await, "complete");
}

/// Startup safety net: an *already-stuck* node-less child — run terminal
/// (`exited`), turn already `complete`, artifact written, but job still
/// `running` and caller `blocked` — is the shape a host crash under the old code
/// leaves behind. `heal_stuck_delegated_children` must terminalize it and fire
/// the resume gate, since `redispatch_crashed_workflows` (non-terminal runs
/// only) never revisits it.
#[tokio::test]
async fn startup_heal_recovers_stuck_nodeless_child_and_resumes_parent() {
    let db = test_db().await;
    db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-wf','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-wf','w-wf','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-wf','p-wf',1,'T','waiting','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-wf','recipe','i-wf','p-wf','running',1,1,?1)", (completed_nodeless_child_snapshot("running").as_str(),)).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('anchor','s-parent','j-parent',1,'yielded','dependency_wait','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('succ','s-parent','j-parent',2,'anchor','pending','dependency_unblock',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, current_turn_id, created_at, updated_at) VALUES ('j-parent','e-wf','i-wf','p-wf','running','succ',1,1)", ()).await?;
                // The stuck shape: run exited, turn already complete, artifact
                // written, but job never terminalized.
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, output_contract, agent_config_id, created_at, updated_at) VALUES ('j-child','e-wf','j-parent','i-wf','p-wf','running','{\"schemaType\":{\"type\":\"object\"},\"description\":\"Submit\"}','workflow',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','exited',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('child-turn','s-child','run-child','j-child',1,'complete','initial',1,1)", ()).await?;
                conn.execute("UPDATE jobs SET current_turn_id='child-turn' WHERE id='j-child'", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-child','j-child','return',1,'{\"question\":\"q\",\"summary\":\"s\"}',1,'return',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    let orch = test_orchestrator(db);

    assert_eq!(turn_state(&orch, "succ").await, "pending");

    crate::execution::advancement::heal_stuck_delegated_children(&orch).await;

    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("complete"),
    );
    assert_eq!(turn_state(&orch, "succ").await, "complete");
}

/// A packet-less child — a `create_child_task` sub-agent, with `parent_job_id`
/// set and `recipe_node_id` NULL like a call/workflow but no output contract and
/// no delegated packet — is owed the fixed `result` artifact. A completed turn
/// that never wrote it must not complete the job: it stays `running`.
#[tokio::test]
async fn reduce_leaves_packetless_child_task_untouched() {
    let empty_snapshot = serde_json::json!({
        "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual", "nodes": [], "edges": []},
        "agents": {},
        "skills": {},
        "triggerContext": {"issueId": "i-ct", "projectId": "p-ct", "triggerType": "manual"},
        "delegatedPackets": [],
        "createdAt": 0
    })
    .to_string();
    let db = test_db().await;
    db.write(|conn| {
            let s = empty_snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-ct','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-ct','w-ct','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-ct','p-ct',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-ct','recipe','i-ct','p-ct','running',1,1,?1)", (s.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, created_at, updated_at) VALUES ('j-ct-parent','e-ct','i-ct','p-ct','running',1,1)", ()).await?;
                // A create_child_task child: node-less, NO output_contract, NO
                // packet, a completed turn, and no `result` artifact written.
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, agent_config_id, created_at, updated_at) VALUES ('j-ct-child','e-ct','j-ct-parent','i-ct','p-ct','running','Explore',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('ct-turn','s-ct','j-ct-child',1,'complete','initial',1,1)", ()).await?;
                conn.execute("UPDATE jobs SET current_turn_id='ct-turn' WHERE id='j-ct-child'", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    let orch = test_orchestrator(db);

    let outcome =
        crate::execution::advancement::reduce_delegated_child_job(&orch, "j-ct-child").unwrap();
    assert!(
        outcome.is_none(),
        "a packet-less child task must not be reduced by the node-less reducer"
    );
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-ct-child")
            .await
            .as_deref(),
        Some("running"),
        "a child task that never wrote its result must not be marked complete",
    );
}

/// A sub-agent task that wrote its `result`, ended that turn, and was woken
/// again (a sibling's message, a resolved wait): its head is a later
/// `follow_up` turn still running on a live warm run. `recipe_node_id` chooses
/// the shape: `None` is a packet-less `create_child_task` child (every
/// cross-node task), `Some` the `delegated-*-agent` node a self-route task gets.
async fn seed_child_task_woken_after_its_result(
    db: &LocalDb,
    snapshot: String,
    recipe_node_id: Option<&'static str>,
) {
    db.write(move |conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-warm','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-warm','w-warm','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-warm','p-warm',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-warm','recipe','i-warm','p-warm','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, created_at, updated_at) VALUES ('j-parent','e-warm','i-warm','p-warm','running',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, parent_job_id, issue_id, project_id, status, agent_config_id, model, created_at, updated_at) VALUES ('j-child','e-warm',?1,'j-parent','i-warm','p-warm','running','Build','gpt-6.1-sol',1,1)", (recipe_node_id,)).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','live',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, end_reason, start_reason, created_at, updated_at) VALUES ('handoff-turn','s-child','run-child','j-child',1,'complete','artifact_handoff','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-result','j-child','result',0,'{\"summary\":\"s\"}',1,'result',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, run_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('woken-turn','s-child','run-child','j-child',2,'handoff-turn','running','follow_up',2,2)", ()).await?;
                conn.execute("UPDATE jobs SET current_turn_id='woken-turn' WHERE id='j-child'", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
}

fn packetless_snapshot() -> String {
    serde_json::json!({
        "recipe": {"id": "r", "name": "R", "description": null, "trigger": "manual", "nodes": [], "edges": []},
        "agents": {},
        "skills": {},
        "triggerContext": {"issueId": "i-warm", "projectId": "p-warm", "triggerType": "manual"},
        "delegatedPackets": [],
        "createdAt": 0
    })
    .to_string()
}

/// cairn/6467: a cross-node task (packet-less, node-less) that wrote `result`,
/// was woken again, and ended that turn on the warm path settles `complete` at
/// that turn end, once. Before the fix the node-less reducer required a
/// delegated packet, so the job stayed `running` until its issue resolved.
#[tokio::test]
async fn packetless_task_woken_after_its_result_settles_at_turn_end() {
    let db = test_db().await;
    seed_child_task_woken_after_its_result(&db, packetless_snapshot(), None).await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-child", Some("j-child"));
    orch.process_state
        .set_current_turn_id("run-child", Some("woken-turn"));

    assert!(transition_to_warm_state(&orch, "run-child", None));

    assert_eq!(turn_state(&orch, "woken-turn").await, "complete");
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("complete"),
    );
    assert!(
        crate::execution::advancement::reduce_delegated_child_job(&orch, "j-child")
            .unwrap()
            .is_none(),
        "a settled task is not settled again"
    );
}

/// Control for the test above: the same woken-after-result shape on a
/// self-route task, whose `delegated-*-agent` recipe node the execution sweep
/// derives, settles at the same turn end.
#[tokio::test]
async fn node_backed_task_woken_after_its_result_settles_at_turn_end() {
    let db = test_db().await;
    seed_child_task_woken_after_its_result(&db, running_child_snapshot(), Some("child-node")).await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-child", Some("j-child"));
    orch.process_state
        .set_current_turn_id("run-child", Some("woken-turn"));

    assert!(transition_to_warm_state(&orch, "run-child", None));

    assert_eq!(turn_state(&orch, "woken-turn").await, "complete");
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("complete"),
    );
}

/// Startup settles both a completed packet-less task and a packet-backed call
/// whose run ended without a result, even though their parent is not parked.
#[tokio::test]
async fn startup_heal_settles_packetless_tasks_and_turnless_calls() {
    let db = test_db().await;
    db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-warm','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-warm','w-warm','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-warm','p-warm',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-warm','recipe','i-warm','p-warm','running',1,1,?1)", (completed_nodeless_child_snapshot("running").as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, created_at, updated_at) VALUES ('j-parent','e-warm','i-warm','p-warm','running',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, agent_config_id, created_at, updated_at) VALUES ('j-task','e-warm','j-parent','i-warm','p-warm','running','Investigate',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-task','j-task','exited',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES ('task-turn','s-task','run-task','j-task',1,'complete','initial',1,1)", ()).await?;
                conn.execute("UPDATE jobs SET current_turn_id='task-turn' WHERE id='j-task'", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-task','j-task','result',0,'{\"summary\":\"s\"}',1,'result',1,1)", ()).await?;
                // The snapshot's packet names this child: packet-backed, its
                // run crashed before any turn, and its caller is not parked.
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, agent_config_id, created_at, updated_at) VALUES ('j-child','e-warm','j-parent','i-warm','p-warm','running','Explore',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','crashed',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    let orch = test_orchestrator(db);

    crate::execution::advancement::heal_stuck_delegated_children(&orch).await;

    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-task")
            .await
            .as_deref(),
        Some("complete"),
    );
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("failed"),
    );
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT error_message FROM runs WHERE id = ?1",
            "run-child"
        )
        .await
        .as_deref(),
        Some("never started a turn; its run crashed"),
    );
    let packet = cairn_workflow::delegated_packets::find_by_result_job_db(
        &orch.db.local,
        "e-warm",
        "j-child",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(packet.status, crate::models::DelegatedStatus::Failed);
}

/// Only a packet-backed ephemeral call completes without a turn. A packet-less
/// node-less child that has run no turn has no outcome to read: its terminal
/// run must not fail it for the `result` it was never asked for yet.
#[tokio::test]
async fn reduce_leaves_a_turnless_packetless_child_untouched() {
    let db = test_db().await;
    db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-warm','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-warm','w-warm','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-warm','p-warm',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-warm','recipe','i-warm','p-warm','running',1,1,?1)", (packetless_snapshot().as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, created_at, updated_at) VALUES ('j-parent','e-warm','i-warm','p-warm','running',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, agent_config_id, created_at, updated_at) VALUES ('j-child','e-warm','j-parent','i-warm','p-warm','running','Build',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES ('run-child','j-child','exited',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    let orch = test_orchestrator(db);

    assert!(
        crate::execution::advancement::reduce_delegated_child_job(&orch, "j-child")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
            .await
            .as_deref(),
        Some("running"),
    );
}

async fn scalar_text(orch: &Orchestrator, sql: &'static str, id: &str) -> Option<String> {
    let id = id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn.query(sql, (id.as_str(),)).await?;
                match rows.next().await? {
                    Some(row) => row.opt_text(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .unwrap()
}

/// Seed a runless suspended parent: a `running` job with no live run of its
/// own, a yielded work turn plus a pending `dependency_unblock` successor, an
/// open `ask_user` prompt, and a delegated child with a live run. This is the
/// shape a job rests in after suspending on a foreground question or inline
/// task when its run finalized (the OpenRouter owned loop keeps no warm
/// process). Standalone (no execution) so the recompute takes the simple path.
async fn seed_runless_suspended_job(db: &LocalDb) {
    db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-sj','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-sj','w-sj','P','prj','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-sj','p-sj',7,'T','waiting','needs_input',1,1)", ()).await?;
                // Yielded work turn + the pending successor it points at. Inserted
                // before the job because `jobs.current_turn_id` references it.
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at) VALUES ('sj-anchor','s-sj','j-sj-parent',1,'yielded','awaiting_input','initial',1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state, start_reason, created_at, updated_at) VALUES ('sj-succ','s-sj','j-sj-parent',2,'sj-anchor','pending','dependency_unblock',1,1)", ()).await?;
                // Parent job: running, no live run (its prior run exited).
                conn.execute("INSERT INTO jobs (id, issue_id, project_id, status, node_name, current_turn_id, created_at, updated_at) VALUES ('j-sj-parent','i-sj','p-sj','running','builder','sj-succ',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('r-sj-parent','j-sj-parent','i-sj','p-sj','exited',1,1)", ()).await?;
                // Open ask_user prompt on the work turn.
                conn.execute("INSERT INTO prompts (id, run_id, turn_id, questions, response, created_at) VALUES ('sj-prompt','r-sj-parent','sj-anchor','[]',NULL,1)", ()).await?;
                // Delegated child still running.
                conn.execute("INSERT INTO jobs (id, parent_job_id, issue_id, project_id, status, node_name, created_at, updated_at) VALUES ('j-sj-child','j-sj-parent','i-sj','p-sj','running','task',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, issue_id, project_id, status, created_at, updated_at) VALUES ('r-sj-child','j-sj-child','i-sj','p-sj','live',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
}

#[tokio::test]
async fn stop_job_idles_runless_suspended_job_and_stops_children() {
    let db = test_db().await;
    seed_runless_suspended_job(&db).await;
    let orch = test_orchestrator(db);

    stop_job(&orch, "j-sj-parent").unwrap();

    // The open question is withdrawn with the stop as its cause: pending
    // nowhere, and no fake answer is recorded.
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT response FROM prompts WHERE id = ?1",
            "sj-prompt"
        )
        .await,
        None,
        "a stopped question has no answer"
    );
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT withdrawn_cause FROM prompts WHERE id = ?1",
            "sj-prompt"
        )
        .await
        .as_deref(),
        Some("operator_stop")
    );
    // The predicate every pending-question check shares.
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT id FROM prompts WHERE id = ?1 AND response IS NULL AND withdrawn_at IS NULL",
            "sj-prompt"
        )
        .await,
        None,
        "a withdrawn question no longer counts as needing input"
    );
    // A late answer is refused with the stop named, not recorded.
    let late = crate::mcp::handlers::planning::answer_prompt_id(
        &orch,
        "sj-prompt",
        crate::models::question::PromptAnswers::new(
            1,
            vec![crate::models::question::QuestionAnswer {
                index: 0,
                answer: "too late".to_string(),
            }],
        )
        .unwrap(),
        crate::turns::queries::ResolutionProvenance::new(
            cairn_common::identity::AppearanceTransport::LocalInvoke,
            None,
        ),
    )
    .await
    .expect("a late answer is refused, not an error");
    let notice = late.withdrawn.expect("the refusal names the withdrawal");
    assert!(notice.contains("the operator stopped the work"), "{notice}");
    // The yielded work turn and the pending successor are both terminalized,
    // so the job's latest turn is no longer live (drops the pending successor).
    assert_eq!(turn_state(&orch, "sj-anchor").await, "cancelled");
    assert_eq!(turn_state(&orch, "sj-succ").await, "cancelled");
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT status FROM jobs WHERE id = ?1",
            "j-sj-parent"
        )
        .await
        .as_deref(),
        Some("idle"),
        "runless standalone job should recompute to idle"
    );
    // The delegated child's live run is stopped.
    assert_ne!(
        scalar_text(&orch, "SELECT status FROM runs WHERE id = ?1", "r-sj-child")
            .await
            .as_deref(),
        Some("live"),
        "child run should no longer be live"
    );
}

// Exercise the completion consumer against persisted turns and the real resume
// gate. Checking a policy enum alone would miss a lost finalizer or wrong order.
mod backend_turn {
    use super::*;
    use crate::agent_process::completion::{
        ProviderInterruption, SessionLifetime, SessionTurnCompletion, SessionTurnOutcome,
    };
    use crate::agent_process::stream::TokenCounts;
    use crate::orchestrator::lifecycle::{apply_codex_turn_completion, CodexTurnContext};

    fn complete(
        orch: &Orchestrator,
        run_id: &str,
        outcome: SessionTurnOutcome,
        lifetime: SessionLifetime,
        handled: bool,
    ) {
        apply_codex_turn_completion(
            orch,
            &orch.db.local,
            &orch.services.emitter,
            run_id,
            None,
            SessionTurnCompletion {
                outcome,
                usage: TokenCounts {
                    input: Some(11),
                    output: Some(7),
                    cache_read: Some(3),
                    cache_create: Some(2),
                    thinking: Some(5),
                },
                session_lifetime: lifetime,
            },
            CodexTurnContext {
                fatal_cause: None,
                accounting_only: false,
                process_identity: orch.process_state.capture_process_identity(run_id).unwrap(),
                owner: orch.process_state.capture_turn_owner_for_identity(
                    &orch.process_state.capture_process_identity(run_id).unwrap(),
                ),
                failure_disposition: if handled {
                    TurnFailureDisposition::AlreadyHandled
                } else {
                    TurnFailureDisposition::Unhandled
                },
            },
        );
    }

    #[tokio::test]
    async fn failed_completion_settlement_retains_owner_until_retry_succeeds() {
        let db = test_db().await;
        seed_top_level_run(&db, "r", "j", "t", "none").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "r", Some("j"));
        orch.process_state.set_current_turn_id("r", Some("t"));
        let identity = orch.process_state.capture_process_identity("r").unwrap();
        let owner = orch
            .process_state
            .capture_turn_owner_for_identity(&identity)
            .unwrap();
        orch.db
            .local
            .execute_script(
                "CREATE TRIGGER reject_completion BEFORE UPDATE OF state ON turns
             WHEN OLD.id='t' AND NEW.state='complete'
             BEGIN SELECT RAISE(ABORT, 'injected settlement failure'); END;",
            )
            .await
            .unwrap();
        let release = || {
            super::super::finalize::release_owned_completion(
                &orch,
                &identity,
                Some(&owner),
                Some((crate::models::TurnState::Complete, None)),
            )
        };
        let published = emitter.events_named("db-change").len();
        assert!(!release(), "failed settlement must not advertise Idle");
        assert!(orch.process_state.is_turn_owner_current(&owner));
        assert_eq!(turn_state(&orch, "t").await, "running");
        assert_eq!(emitter.events_named("db-change").len(), published);
        assert!(orch.job_launch_lock("j").try_lock().is_ok());
        orch.db
            .local
            .execute_script("DROP TRIGGER reject_completion;")
            .await
            .unwrap();
        assert!(
            release(),
            "the accepted owner can retry a failed settlement"
        );
        assert_eq!(turn_state(&orch, "t").await, "complete");
        assert_eq!(orch.process_state.get_current_turn_id("r"), None);
        assert_eq!(emitter.events_named("db-change").len(), published + 1);
        assert!(!release(), "a successful release cannot replay");
        assert_eq!(emitter.events_named("db-change").len(), published + 1);
    }

    #[tokio::test]
    async fn duplicate_and_replaced_completion_callbacks_preserve_successors() {
        for mode in ["idle", "successor", "replacement"] {
            let db = test_db().await;
            seed_top_level_run(&db, "r", "j", "t", "none").await;
            let (orch, emitter) = test_orchestrator_with_emitter(db, None);
            register_warm_process(&orch, "r", Some("j"));
            orch.process_state.set_current_turn_id("r", Some("t"));
            let identity = orch.process_state.capture_process_identity("r").unwrap();
            let owner = orch
                .process_state
                .capture_turn_owner_for_identity(&identity);
            let replay = || {
                apply_codex_turn_completion(
                    &orch,
                    &orch.db.local,
                    &orch.services.emitter,
                    "r",
                    None,
                    SessionTurnCompletion {
                        outcome: SessionTurnOutcome::Completed,
                        usage: TokenCounts::default(),
                        session_lifetime: SessionLifetime::Persistent,
                    },
                    CodexTurnContext {
                        fatal_cause: None,
                        accounting_only: false,
                        process_identity: identity.clone(),
                        owner: owner.clone(),
                        failure_disposition: TurnFailureDisposition::Unhandled,
                    },
                );
            };
            replay();
            assert_eq!(turn_state(&orch, "t").await, "complete");
            if mode != "idle" {
                orch.db.local.execute_script(
                    "INSERT INTO turns(id,session_id,job_id,run_id,sequence,state,start_reason,created_at,updated_at)
                       VALUES('new','sess-attn','j','r',2,'running','prompt_response',2,2);
                     UPDATE jobs SET current_turn_id='new' WHERE id='j';",
                ).await.unwrap();
                if mode == "replacement" {
                    register_warm_process(&orch, "r", Some("j"));
                }
                orch.process_state.set_current_turn_id("r", Some("new"));
            }
            replay();
            replay();
            assert_eq!(result_count(&orch).await, 1, "{mode}: no duplicate results");
            assert_eq!(
                emitter.events_named("run-turn-completed").len(),
                1,
                "{mode}: no repeated completion effects"
            );
            assert_eq!(
                orch.process_state.get_current_turn_id("r").as_deref(),
                (mode != "idle").then_some("new"),
                "{mode}: preserve the current owner"
            );
            if mode != "idle" {
                assert_eq!(turn_state(&orch, "new").await, "running");
            }
        }
    }

    #[tokio::test]
    async fn one_shot_completion_replay_and_stale_process_have_no_effects() {
        for (occupancy, stale) in ["tracked", "idle", "busy"]
            .into_iter()
            .flat_map(|occupancy| [false, true].map(|stale| (occupancy, stale)))
        {
            let db = test_db().await;
            seed_top_level_run(&db, "r", "j", "t", "none").await;
            let (orch, emitter) = test_orchestrator_with_emitter(db, None);
            register_warm_process(&orch, "r", Some("j"));
            if occupancy == "tracked" {
                orch.process_state.set_current_turn_id("r", Some("t"));
            } else {
                orch.db.local.execute_script(
                    "UPDATE jobs SET current_turn_id=NULL WHERE id='j'; DELETE FROM turns WHERE id='t';"
                ).await.unwrap();
                if occupancy == "busy" {
                    orch.process_state.transition_to_active("r");
                }
            }
            let identity = orch.process_state.capture_process_identity("r").unwrap();
            let owner = orch
                .process_state
                .capture_turn_owner_for_identity(&identity);
            let replay = || {
                apply_codex_turn_completion(
                    &orch,
                    &orch.db.local,
                    &orch.services.emitter,
                    "r",
                    None,
                    SessionTurnCompletion {
                        outcome: SessionTurnOutcome::Completed,
                        usage: TokenCounts::default(),
                        session_lifetime: SessionLifetime::OneShot,
                    },
                    CodexTurnContext {
                        fatal_cause: None,
                        accounting_only: false,
                        process_identity: identity.clone(),
                        owner: owner.clone(),
                        failure_disposition: TurnFailureDisposition::Unhandled,
                    },
                )
            };
            if stale {
                register_warm_process(&orch, "r", Some("j"));
                orch.process_state.set_current_turn_id("r", Some("t"));
            } else {
                replay();
            }
            let before = emitter.events.lock().unwrap().len();
            replay();
            replay();
            assert_eq!(
                emitter.events.lock().unwrap().len(),
                before,
                "stale={stale}: no repeated publication"
            );
            assert_eq!(result_count(&orch).await, i64::from(!stale));
            if occupancy == "tracked" {
                assert_eq!(
                    turn_state(&orch, "t").await,
                    if stale { "running" } else { "complete" }
                );
            } else {
                assert_eq!(
                    orch.db
                        .local
                        .query_opt_i64("SELECT COUNT(*) FROM turns WHERE job_id='j'", (),)
                        .await
                        .unwrap(),
                    Some(0),
                    "untracked startup cannot rediscover a head"
                );
            }
            assert_eq!(
                scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "r")
                    .await
                    .as_deref(),
                Some(if stale { "live" } else { "exited" })
            );
        }
    }

    #[tokio::test]
    async fn shutdown_terminal_claims_leave_lifecycle_to_restart_recovery() {
        for status in [RunStatus::Exited, RunStatus::Crashed] {
            for tracked in [true, false] {
                let db = test_db().await;
                seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
                // Shutdown settles unfinished work before provider cleanup can
                // report success or failure. Neither may replace that cause.
                db.execute_script(
                    "UPDATE runs SET exit_reason='runner_shutdown' WHERE id='run-child';
                     UPDATE turns SET state='interrupted',end_reason='host_restart'
                       WHERE id='child-turn';",
                )
                .await
                .unwrap();
                let (orch, emitter) = test_orchestrator_with_emitter(db, None);
                register_warm_process(&orch, "run-child", Some("j-child"));
                if tracked {
                    orch.process_state
                        .set_current_turn_id("run-child", Some("child-turn"));
                } else {
                    orch.process_state.transition_to_active("run-child");
                }
                let identity = orch
                    .process_state
                    .capture_process_identity("run-child")
                    .unwrap();
                let owner = orch
                    .process_state
                    .capture_turn_owner_for_identity(&identity);
                let published = std::cell::Cell::new(0);
                for _ in 0..2 {
                    assert!(!crate::orchestrator::lifecycle::finalize_owned_completion(
                        &orch,
                        &identity,
                        owner.as_ref(),
                        status.clone(),
                        Some("turn_failed"),
                        None,
                        || published.set(published.get() + 1),
                    ));
                }
                assert_eq!(published.get(), 0);
                assert!(emitter.events.lock().unwrap().is_empty());
                assert_eq!(turn_state(&orch, "child-turn").await, "interrupted");
                assert_eq!(
                    scalar_text(
                        &orch,
                        "SELECT end_reason FROM turns WHERE id=?1",
                        "child-turn"
                    )
                    .await
                    .as_deref(),
                    Some("host_restart"),
                );
                assert_eq!(
                    turn_state(&orch, "succ").await,
                    "pending",
                    "shutdown must not resume the parent",
                );
                assert_eq!(
                    scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "run-child")
                        .await
                        .as_deref(),
                    Some("live"),
                );
                assert_eq!(
                    scalar_text(
                        &orch,
                        "SELECT exit_reason FROM runs WHERE id=?1",
                        "run-child"
                    )
                    .await
                    .as_deref(),
                    Some("runner_shutdown"),
                );
                assert!(orch.process_state.is_process_identity_current(&identity));
                assert_eq!(
                    orch.process_state
                        .get_current_turn_id("run-child")
                        .as_deref(),
                    tracked.then_some("child-turn"),
                );
                assert!(
                    !orch
                        .process_state
                        .processes
                        .lock()
                        .unwrap()
                        .get("run-child")
                        .unwrap()
                        .is_warm(),
                    "refused terminal claims cannot release tracked or untracked busy occupancy",
                );
            }
        }
    }

    #[tokio::test]
    async fn terminal_failure_publication_cannot_finalize_a_replacement_head() {
        for same_run in [false, true] {
            let db = test_db().await;
            seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
            if same_run {
                db.execute_script(r#"INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at)
                 VALUES('terminal-call','local-runtime','isolated','call:run-child','{"residentProcessUnits":1,"logicalStreamUnits":1}',0,1,'active','test',1,9999999999,1,1);"#).await.unwrap();
            }
            let replacement_run = if same_run {
                "run-child"
            } else {
                "replacement-run"
            };
            let (orch, emitter) = test_orchestrator_with_emitter(db, None);
            register_warm_process(&orch, "run-child", Some("j-child"));
            orch.process_state
                .set_current_turn_id("run-child", Some("child-turn"));
            let callback_orch = orch.clone();
            let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let callback_fired = fired.clone();
            *emitter.on_db_change.lock().unwrap() = Some(Box::new(move || {
                let db = callback_orch.db.local.clone();
                let has_result = crate::storage::run_db_blocking({
                    let db = db.clone();
                    move || async move {
                        db.query_opt_i64(
                            "SELECT COUNT(*) FROM events WHERE event_type='result:error'",
                            (),
                        )
                        .await
                        .map_err(|e| e.to_string())
                    }
                })
                .unwrap()
                    == Some(1);
                if !has_result {
                    return false;
                }
                let lock = callback_orch.job_launch_lock("j-child");
                let _guard = lock
                    .try_lock()
                    .expect("result publication releases launch authority");
                crate::storage::run_db_blocking(move || async move {
                if same_run {
                    assert_eq!(db.query_opt_text(
                        "SELECT state FROM runtime_admission_requests WHERE request_id='terminal-call'", (),
                    ).await.unwrap().as_deref(), Some("released"),
                        "call lease cleanup precedes result publication");
                }
                let run_insert = if same_run { String::new() } else {
                    "INSERT INTO runs(id,job_id,session_id,status,created_at,updated_at) VALUES('replacement-run','j-child','replacement-session','live',3,3);".to_string()
                };
                db.execute_script(format!(
                    "INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at)
                       VALUES('replacement-session','j-child','codex','open',2,3,3);
                     {run_insert}
                     INSERT INTO turns(id,session_id,job_id,run_id,sequence,state,start_reason,created_at,updated_at)
                       VALUES('replacement-turn','replacement-session','j-child','{replacement_run}',2,'running','prompt_response',3,3);
                     UPDATE jobs SET current_turn_id='replacement-turn',current_session_id='replacement-session',status='running' WHERE id='j-child';"
                )).await.map_err(|e| e.to_string())
            }).unwrap();
                register_warm_process(&callback_orch, replacement_run, Some("j-child"));
                callback_orch
                    .process_state
                    .set_current_turn_id(replacement_run, Some("replacement-turn"));
                callback_fired.store(true, std::sync::atomic::Ordering::Release);
                true
            }));
            complete(
                &orch,
                "run-child",
                SessionTurnOutcome::Failed {
                    status: "fatal".into(),
                },
                SessionLifetime::Persistent,
                false,
            );
            assert!(
                fired.load(std::sync::atomic::Ordering::Acquire),
                "result:error boundary exercised"
            );
            assert_eq!(turn_state(&orch, "child-turn").await, "failed");
            assert_eq!(turn_state(&orch, "replacement-turn").await, "running");
            assert_eq!(
                scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "run-child")
                    .await
                    .as_deref(),
                Some("crashed")
            );
            if !same_run {
                assert_eq!(
                    scalar_text(
                        &orch,
                        "SELECT status FROM runs WHERE id=?1",
                        replacement_run
                    )
                    .await
                    .as_deref(),
                    Some("live")
                );
            }
            assert_eq!(
                orch.process_state
                    .get_current_turn_id(replacement_run)
                    .as_deref(),
                Some("replacement-turn")
            );
        }
    }

    async fn result_count(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_opt(
                "SELECT COUNT(*) FROM events WHERE event_type LIKE 'result:%'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap()
            .unwrap()
    }

    async fn assert_usage_attached(orch: &Orchestrator, turn_id: &str, event_type: &str) {
        let row = orch.db.local.query_opt(
            "SELECT turn_id,event_type,input_tokens,output_tokens,cache_read_tokens,cache_create_tokens,thinking_tokens FROM events WHERE event_type LIKE 'result:%'",
            (), |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)?, row.i64(3)?, row.i64(4)?, row.i64(5)?, row.i64(6)?)),
        ).await.unwrap().unwrap();
        assert_eq!(
            row,
            (turn_id.to_string(), event_type.to_string(), 11, 7, 3, 2, 5),
            "usage must retain the completing turn even after the process clears its turn ID"
        );
    }

    #[tokio::test]
    async fn persistent_completion_retains_warm_process_and_records_usage_before_transition() {
        let db = test_db().await;
        seed_top_level_run(&db, "r", "j", "t", "none").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "r", Some("j"));
        orch.process_state.set_current_turn_id("r", Some("t"));
        complete(
            &orch,
            "r",
            SessionTurnOutcome::Completed,
            SessionLifetime::Persistent,
            false,
        );
        assert_eq!(turn_state(&orch, "t").await, "complete");
        assert_eq!(
            scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "r")
                .await
                .as_deref(),
            Some("live")
        );
        assert!(orch
            .process_state
            .processes
            .lock()
            .unwrap()
            .get("r")
            .unwrap()
            .is_warm());
        assert_eq!(orch.process_state.get_current_turn_id("r"), None);
        assert_eq!(emitter.events_named("run-turn-completed").len(), 1);
        assert_usage_attached(&orch, "t", "result:success").await;
    }

    #[tokio::test]
    async fn one_shot_completion_finalizes_even_without_a_delegated_parent() {
        let db = test_db().await;
        seed_top_level_run(&db, "r", "j", "t", "none").await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "r", Some("j"));
        orch.process_state.set_current_turn_id("r", Some("t"));
        complete(
            &orch,
            "r",
            SessionTurnOutcome::Completed,
            SessionLifetime::OneShot,
            false,
        );
        assert_eq!(turn_state(&orch, "t").await, "complete");
        assert_eq!(
            scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "r")
                .await
                .as_deref(),
            Some("exited")
        );
        assert_usage_attached(&orch, "t", "result:success").await;
    }

    #[tokio::test]
    async fn completed_and_failed_delegated_turns_both_release_the_suspended_parent() {
        for failed in [false, true] {
            let db = test_db().await;
            seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
            let orch = test_orchestrator(db);
            register_warm_process(&orch, "run-child", Some("j-child"));
            orch.process_state
                .set_current_turn_id("run-child", Some("child-turn"));
            let outcome = if failed {
                SessionTurnOutcome::Failed {
                    status: "failed".into(),
                }
            } else {
                SessionTurnOutcome::Completed
            };
            complete(
                &orch,
                "run-child",
                outcome,
                SessionLifetime::Persistent,
                false,
            );
            assert_eq!(
                turn_state(&orch, "child-turn").await,
                if failed { "failed" } else { "complete" }
            );
            assert_eq!(
                orch.process_state.get_current_turn_id("run-child"),
                None,
                "terminal failure must release its exact process occupancy"
            );
            assert_eq!(
                turn_state(&orch, "succ").await,
                "complete",
                "the actual resume gate must claim the pending parent successor"
            );
            if !failed {
                assert_eq!(
                    scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "run-child")
                        .await
                        .as_deref(),
                    Some("live"),
                    "a completed task turn leaves its persistent run live"
                );
            }
            assert_usage_attached(
                &orch,
                "child-turn",
                if failed {
                    "result:error"
                } else {
                    "result:success"
                },
            )
            .await;
        }
    }

    #[tokio::test]
    async fn interrupted_delegated_turn_stays_live_and_hands_off_only_after_a_terminal_tool() {
        for terminal_tool in [false, true] {
            let db = test_db().await;
            seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
            let orch = test_orchestrator(db);
            register_warm_process(&orch, "run-child", Some("j-child"));
            orch.process_state
                .set_current_turn_id("run-child", Some("child-turn"));
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .get("run-child")
                .unwrap()
                .terminal_tool_called
                .store(terminal_tool, std::sync::atomic::Ordering::Release);
            let identity = orch
                .process_state
                .capture_process_identity("run-child")
                .unwrap();
            let owner = orch
                .process_state
                .capture_turn_owner_for_identity(&identity);
            complete(
                &orch,
                "run-child",
                SessionTurnOutcome::Interrupted(ProviderInterruption::default()),
                SessionLifetime::Persistent,
                false,
            );
            assert_eq!(
                scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "run-child")
                    .await
                    .as_deref(),
                Some("live"),
                "the run outlives the turn while its process is held"
            );
            assert_eq!(
                scalar_text(
                    &orch,
                    "SELECT end_reason FROM turns WHERE id=?1",
                    "child-turn"
                )
                .await
                .as_deref(),
                // With no interrupt Cairn asked for, the abort was the
                // provider's own: the turn was cut off, not completed.
                Some(if terminal_tool {
                    "artifact_handoff"
                } else {
                    "provider_interrupt"
                })
            );
            let before = result_count(&orch).await;
            apply_codex_turn_completion(
                &orch,
                &orch.db.local,
                &orch.services.emitter,
                "run-child",
                None,
                SessionTurnCompletion {
                    outcome: SessionTurnOutcome::Interrupted(ProviderInterruption::default()),
                    usage: TokenCounts::default(),
                    session_lifetime: SessionLifetime::Persistent,
                },
                CodexTurnContext {
                    fatal_cause: None,
                    accounting_only: false,
                    process_identity: identity,
                    owner,
                    failure_disposition: TurnFailureDisposition::Unhandled,
                },
            );
            assert_eq!(
                result_count(&orch).await,
                before,
                "duplicate interruption must not repeat effects or release the parent again"
            );
            assert_usage_attached(&orch, "child-turn", "result:interrupted").await;
        }
    }

    #[tokio::test]
    async fn actual_handoff_late_accounting_and_replay_wake_parent_only_once() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        db.execute_script(
            // One row per wake: an INSERT body reads no table, where a counter
            // UPDATE would walk parent_wakes and the plan audit would charge
            // that trigger scan to the production `UPDATE turns` firing it.
            "CREATE TABLE parent_wakes(turn_id TEXT NOT NULL);
             CREATE TRIGGER count_parent_wake AFTER UPDATE OF state ON turns
             WHEN NEW.id='succ' AND OLD.state='pending' AND NEW.state!='pending'
             BEGIN INSERT INTO parent_wakes(turn_id) VALUES(NEW.id); END;",
        )
        .await
        .unwrap();
        let (orch, capture) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "run-child", Some("j-child"));
        orch.process_state
            .set_current_turn_id("run-child", Some("child-turn"));
        let identity = orch
            .process_state
            .capture_process_identity("run-child")
            .unwrap();
        let owner = orch
            .process_state
            .capture_turn_owner_for_identity(&identity);
        assert!(
            crate::orchestrator::lifecycle::transition_owned_completion_to_warm(
                &orch,
                &identity,
                owner.as_ref(),
                Some((
                    crate::models::TurnState::Complete,
                    Some(TurnEndReason::ArtifactHandoff)
                )),
            )
        );
        assert_eq!(turn_state(&orch, "succ").await, "complete");
        let before = capture.events.lock().unwrap().len();
        for _ in 0..2 {
            apply_codex_turn_completion(
                &orch,
                &orch.db.local,
                &orch.services.emitter,
                "run-child",
                None,
                SessionTurnCompletion {
                    outcome: SessionTurnOutcome::Interrupted(ProviderInterruption {
                        provider_turn_id: Some("p1".into()),
                        cause: Some(
                            cairn_models::interface::session::InterruptCause::ArtifactHandoff,
                        ),
                        ..Default::default()
                    }),
                    usage: TokenCounts {
                        input: Some(11),
                        output: Some(7),
                        cache_read: Some(3),
                        cache_create: Some(2),
                        thinking: Some(5),
                    },
                    session_lifetime: SessionLifetime::Persistent,
                },
                CodexTurnContext {
                    fatal_cause: None,
                    accounting_only: true,
                    process_identity: identity.clone(),
                    owner: owner.clone(),
                    failure_disposition: TurnFailureDisposition::Unhandled,
                },
            );
        }
        assert_eq!(result_count(&orch).await, 1);
        assert_usage_attached(&orch, "child-turn", "result:interrupted").await;
        assert_eq!(
            orch.db
                .local
                .query_opt_i64("SELECT COUNT(*) FROM parent_wakes", ())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(orch.process_state.get_current_turn_id("run-child"), None);
        assert!(capture.events.lock().unwrap()[before..]
            .iter()
            .all(|(name, _)| name != "run-turn-completed" && name != "run-completed"));
    }

    #[tokio::test]
    async fn already_handled_failure_records_usage_without_repeating_lifecycle_or_diagnostic() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "run-child", Some("j-child"));
        orch.process_state
            .set_current_turn_id("run-child", Some("child-turn"));
        complete(
            &orch,
            "run-child",
            SessionTurnOutcome::Failed {
                status: "failed".into(),
            },
            SessionLifetime::Persistent,
            true,
        );
        assert_eq!(
            turn_state(&orch, "child-turn").await,
            "running",
            "the caller owns the failure transition"
        );
        assert_eq!(turn_state(&orch, "succ").await, "pending");
        assert!(emitter.events_named("run-turn-completed").is_empty());
        assert_usage_attached(&orch, "child-turn", "result:error").await;
        assert_eq!(
            orch.db
                .local
                .query_opt(
                    "SELECT COUNT(*) FROM events WHERE event_type='system:error'",
                    (),
                    |row| row.i64(0)
                )
                .await
                .unwrap(),
            Some(0)
        );
    }
    #[tokio::test]
    async fn recorded_fatal_diagnostic_still_finalizes_child_without_a_second_error() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "run-child", Some("j-child"));
        orch.process_state
            .set_current_turn_id("run-child", Some("child-turn"));
        crate::orchestrator::session::insert_error_event(
            &orch,
            "run-child",
            None,
            "Codex error: fatal provider detail",
        );
        apply_codex_turn_completion(
            &orch,
            &orch.db.local,
            &orch.services.emitter,
            "run-child",
            None,
            SessionTurnCompletion {
                outcome: SessionTurnOutcome::Failed {
                    status: "error".into(),
                },
                usage: TokenCounts::default(),
                session_lifetime: SessionLifetime::Persistent,
            },
            CodexTurnContext {
                fatal_cause: None,
                accounting_only: false,
                process_identity: orch
                    .process_state
                    .capture_process_identity("run-child")
                    .unwrap(),
                owner: orch.process_state.capture_turn_owner_for_identity(
                    &orch
                        .process_state
                        .capture_process_identity("run-child")
                        .unwrap(),
                ),
                failure_disposition: TurnFailureDisposition::DiagnosticRecorded,
            },
        );
        assert_eq!(turn_state(&orch, "child-turn").await, "failed");
        assert_eq!(turn_state(&orch, "succ").await, "complete");
        assert_eq!(
            orch.db
                .local
                .query_opt(
                    "SELECT COUNT(*) FROM events WHERE event_type='system:error'",
                    (),
                    |row| row.i64(0)
                )
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            result_count(&orch).await,
            0,
            "missing usage must not manufacture an extra error result"
        );
    }

    /// A Codex completion of `run_id` that failed with `disposition` and the
    /// reader's `cause`.
    fn fail_with(
        orch: &Orchestrator,
        run_id: &str,
        disposition: TurnFailureDisposition,
        cause: Option<&str>,
    ) {
        let process_identity = orch.process_state.capture_process_identity(run_id).unwrap();
        let owner = orch
            .process_state
            .capture_turn_owner_for_identity(&process_identity);
        apply_codex_turn_completion(
            orch,
            &orch.db.local,
            &orch.services.emitter,
            run_id,
            None,
            SessionTurnCompletion {
                outcome: SessionTurnOutcome::Failed {
                    status: "error".into(),
                },
                usage: TokenCounts::default(),
                session_lifetime: SessionLifetime::Persistent,
            },
            CodexTurnContext {
                accounting_only: false,
                process_identity,
                owner,
                failure_disposition: disposition,
                fatal_cause: cause.map(str::to_string),
            },
        );
    }

    async fn run_cause(orch: &Orchestrator, run_id: &str) -> Option<String> {
        scalar_text(orch, "SELECT error_message FROM runs WHERE id=?1", run_id).await
    }

    /// The reader's fatal cause is the failed child run's recorded cause, in
    /// place by the time the parent is resumed, and the stable exit reason
    /// stays apart from it.
    #[tokio::test]
    async fn a_fatal_cause_is_recorded_on_the_failed_child_run_and_its_parent_resumes() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "run-child", Some("j-child"));
        orch.process_state
            .set_current_turn_id("run-child", Some("child-turn"));
        fail_with(
            &orch,
            "run-child",
            TurnFailureDisposition::DiagnosticRecorded,
            Some("Codex error: refresh_token_reused (401)"),
        );
        assert_eq!(
            run_cause(&orch, "run-child").await.as_deref(),
            Some("Codex error: refresh_token_reused (401)")
        );
        assert_eq!(
            scalar_text(
                &orch,
                "SELECT exit_reason FROM runs WHERE id=?1",
                "run-child"
            )
            .await
            .as_deref(),
            Some("turn_failed")
        );
        assert_eq!(turn_state(&orch, "child-turn").await, "failed");
        assert_eq!(turn_state(&orch, "succ").await, "complete");

        // A repeated terminal callback, with another cause, changes nothing.
        fail_with(
            &orch,
            "run-child",
            TurnFailureDisposition::DiagnosticRecorded,
            Some("Codex error: a later, different failure"),
        );
        assert_eq!(
            run_cause(&orch, "run-child").await.as_deref(),
            Some("Codex error: refresh_token_reused (401)")
        );
    }

    /// A provider status is not a cause, and a failure another path already
    /// owns (a capacity retry) records nothing over that path's state.
    #[tokio::test]
    async fn a_bare_status_or_an_already_handled_failure_records_no_cause() {
        for disposition in [
            TurnFailureDisposition::Unhandled,
            TurnFailureDisposition::AlreadyHandled,
        ] {
            let db = test_db().await;
            seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
            let orch = test_orchestrator(db);
            register_warm_process(&orch, "run-child", Some("j-child"));
            orch.process_state
                .set_current_turn_id("run-child", Some("child-turn"));
            let cause = (disposition == TurnFailureDisposition::AlreadyHandled)
                .then_some("Codex error: owned by the retry");
            fail_with(&orch, "run-child", disposition, cause);
            assert_eq!(run_cause(&orch, "run-child").await, None, "{disposition:?}");
            assert_eq!(
                turn_state(&orch, "child-turn").await,
                if cause.is_some() { "running" } else { "failed" },
                "{disposition:?}"
            );
        }
    }

    /// A transient error followed by a completed turn is not a failure, and
    /// records no run cause: only the terminal decision does.
    #[tokio::test]
    async fn a_transient_error_then_success_records_no_run_cause() {
        let db = test_db().await;
        seed_top_level_run(&db, "r", "j", "t", "none").await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "r", Some("j"));
        orch.process_state.set_current_turn_id("r", Some("t"));
        crate::orchestrator::session::insert_error_event(
            &orch,
            "r",
            None,
            "Codex error: stream disconnected, retrying",
        );
        complete(
            &orch,
            "r",
            SessionTurnOutcome::Completed,
            SessionLifetime::Persistent,
            false,
        );
        assert_eq!(run_cause(&orch, "r").await, None);
        assert_eq!(turn_state(&orch, "t").await, "complete");
    }

    #[tokio::test]
    async fn empty_usage_emits_success_and_interrupt_but_no_handled_error_result() {
        for (outcome, expected) in [
            (SessionTurnOutcome::Completed, 1),
            // The interrupted result is the ledger's record of who stopped
            // the turn, so it is kept even with nothing to bill.
            (
                SessionTurnOutcome::Interrupted(ProviderInterruption::default()),
                1,
            ),
            (
                SessionTurnOutcome::Failed {
                    status: "failed".into(),
                },
                0,
            ),
        ] {
            let db = test_db().await;
            seed_top_level_run(&db, "r", "j", "t", "none").await;
            let orch = test_orchestrator(db);
            register_warm_process(&orch, "r", Some("j"));
            orch.process_state.set_current_turn_id("r", Some("t"));
            apply_codex_turn_completion(
                &orch,
                &orch.db.local,
                &orch.services.emitter,
                "r",
                None,
                SessionTurnCompletion {
                    outcome,
                    usage: TokenCounts::default(),
                    session_lifetime: SessionLifetime::Persistent,
                },
                CodexTurnContext {
                    fatal_cause: None,
                    accounting_only: false,
                    process_identity: orch.process_state.capture_process_identity("r").unwrap(),
                    owner: orch.process_state.capture_turn_owner_for_identity(
                        &orch.process_state.capture_process_identity("r").unwrap(),
                    ),
                    failure_disposition: TurnFailureDisposition::AlreadyHandled,
                },
            );
            assert_eq!(result_count(&orch).await, expected);
        }
    }
}

mod claude_and_owned_completion {
    use super::*;
    use crate::agent_process::completion::{ClaudeTurnOutcome, OwnedTurnOutcome};
    use crate::orchestrator::lifecycle::{
        apply_claude_turn_completion, apply_owned_turn_outcome, ClaudeTurnContext,
    };

    fn claude(
        orch: &Orchestrator,
        run_id: &str,
        outcome: ClaudeTurnOutcome,
        context: ClaudeTurnContext,
    ) {
        apply_claude_turn_completion(
            orch,
            &orch.services.emitter,
            run_id,
            Some("session-test"),
            outcome,
            context,
        );
    }

    fn normal_context() -> ClaudeTurnContext {
        ClaudeTurnContext {
            terminal_tool_completed: false,
            terminal_interrupt_ack_pending: false,
        }
    }

    fn error() -> ClaudeTurnOutcome {
        ClaudeTurnOutcome::Failed {
            message: "fixture provider failure".into(),
        }
    }

    async fn diagnostic_count(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_opt(
                "SELECT COUNT(*) FROM events WHERE json_extract(data, '$.content')='fixture provider failure'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn delayed_claude_interrupt_ack_leaves_successor_running() {
        let db = test_db().await;
        seed_top_level_run(&db, "r", "j", "successor", "none").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "r", Some("j"));
        orch.process_state.begin_turn("r", "successor");
        claude(
            &orch,
            "r",
            error(),
            ClaudeTurnContext {
                terminal_interrupt_ack_pending: true,
                ..normal_context()
            },
        );
        assert_eq!(turn_state(&orch, "successor").await, "running");
        assert_eq!(
            orch.process_state.get_current_turn_id("r").as_deref(),
            Some("successor")
        );
        assert!(emitter.events_named("run-turn-completed").is_empty());
        assert_eq!(diagnostic_count(&orch).await, 0);
        // The next real failure must still settle the successor.
        claude(&orch, "r", error(), normal_context());
        assert_eq!(turn_state(&orch, "successor").await, "interrupted");
        assert_eq!(diagnostic_count(&orch).await, 1);
    }

    #[tokio::test]
    async fn claude_success_and_terminal_tool_cleanup_resume_delegated_parent() {
        for terminal_tool_completed in [false, true] {
            let db = test_db().await;
            seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
            let orch = test_orchestrator(db);
            register_warm_process(&orch, "run-child", Some("j-child"));
            orch.process_state.begin_turn("run-child", "child-turn");
            claude(
                &orch,
                "run-child",
                if terminal_tool_completed {
                    error()
                } else {
                    ClaudeTurnOutcome::Completed
                },
                ClaudeTurnContext {
                    terminal_tool_completed,
                    ..normal_context()
                },
            );
            assert_eq!(turn_state(&orch, "child-turn").await, "complete");
            assert_eq!(
                turn_state(&orch, "succ").await,
                "complete",
                "resume gate must claim the parent successor"
            );
            assert_eq!(diagnostic_count(&orch).await, 0);
        }
    }

    #[tokio::test]
    async fn claude_failure_retains_resumable_delegated_turn() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "run-child", Some("j-child"));
        orch.process_state.begin_turn("run-child", "child-turn");
        claude(&orch, "run-child", error(), normal_context());
        assert_eq!(turn_state(&orch, "child-turn").await, "interrupted");
        assert_eq!(turn_state(&orch, "succ").await, "pending");
        assert_eq!(diagnostic_count(&orch).await, 1);
    }

    /// The Claude Result-failure diagnostic is recorded in the turn it ended, so
    /// `/chat/turn/N` holds the cause rather than only the prompt.
    #[tokio::test]
    async fn claude_failure_diagnostic_is_stamped_with_its_turn() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        let orch = test_orchestrator(db);
        register_warm_process(&orch, "run-child", Some("j-child"));
        orch.process_state.begin_turn("run-child", "child-turn");
        claude(&orch, "run-child", error(), normal_context());
        assert_eq!(
            scalar_text(
                &orch,
                "SELECT turn_id FROM events WHERE event_type='system:error' AND run_id=?1",
                "run-child"
            )
            .await
            .as_deref(),
            Some("child-turn")
        );
    }

    #[tokio::test]
    /// A provider-depleted Result is settled as an interrupted turn instead; see
    /// `provider_depleted_claude_turn_is_interrupted_not_completed`.
    async fn claude_host_interrupt_ack_keeps_live_warm_process() {
        let db = test_db().await;
        seed_top_level_run(&db, "r", "j", "t", "none").await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "r", Some("j"));
        orch.process_state.set_current_turn_id("r", Some("t"));
        // The host has already parked occupancy before its interrupt ack.
        orch.process_state.transition_to_warm("r");
        claude(&orch, "r", error(), normal_context());
        assert_eq!(
            scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "r")
                .await
                .as_deref(),
            Some("live")
        );
        assert!(orch
            .process_state
            .processes
            .lock()
            .unwrap()
            .get("r")
            .unwrap()
            .is_warm());
        assert_eq!(emitter.events_named("run-turn-completed").len(), 1);
        assert_eq!(diagnostic_count(&orch).await, 0);
    }

    #[tokio::test]
    async fn owned_failure_and_panic_release_delegated_parent_and_process() {
        for panicked in [false, true] {
            let db = test_db().await;
            seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
            let orch = test_orchestrator(db);
            register_warm_process(&orch, "run-child", Some("j-child"));
            orch.process_state.begin_turn("run-child", "child-turn");
            let message = "fixture failure".to_string();
            apply_owned_turn_outcome(
                &orch,
                "run-child",
                "session-test",
                "Seam",
                if panicked {
                    OwnedTurnOutcome::Panicked { message }
                } else {
                    OwnedTurnOutcome::Failed { message }
                },
            );
            assert_eq!(turn_state(&orch, "child-turn").await, "failed");
            assert_eq!(turn_state(&orch, "succ").await, "complete");
            assert_eq!(
                scalar_text(
                    &orch,
                    "SELECT exit_reason FROM runs WHERE id=?1",
                    "run-child"
                )
                .await
                .as_deref(),
                Some(if panicked {
                    "turn_panicked"
                } else {
                    "turn_failed"
                })
            );
            assert!(orch
                .process_state
                .processes
                .lock()
                .unwrap()
                .get("run-child")
                .is_none());
        }
    }
}

mod honest_pause_regressions {
    use super::*;
    use crate::agent_process::process::{GoalControl, GoalPause, JobPauseState, PauseIntent};
    use crate::backends::stdin::{
        defer_codex_goal_turn, reactivate_goal, GoalPauseCause, GoalReactivationResult,
    };
    use crate::execution::jobs::{GoalIntent, GoalReactivation, JobRunnable};

    fn goal_reply(status: &str) -> Result<Value, String> {
        Ok(serde_json::json!({"result": {"goal": {
            "threadId": "native-thread", "objective": "finish assigned issue",
            "status": status, "tokenBudget": null, "tokensUsed": 0,
            "timeUsedSeconds": 0, "createdAt": 1, "updatedAt": 2
        }}}))
    }

    fn register_codex(
        orch: &Orchestrator,
        run_id: &str,
        job_id: &str,
        turn_id: &str,
        replies: Vec<Result<Value, String>>,
    ) -> (GoalControl, Arc<Mutex<Vec<Value>>>) {
        let (stdin, requests, _) = crate::backends::codex::scripted_owned_goal_stdin_for_test(
            "native-thread",
            "provider-live",
            replies,
        );
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(stdin))),
            Some("session-pause".into()),
            Some(job_id.into()),
        );
        handle.backend = Some("codex".into());
        handle.begin_turn(turn_id);
        let control = handle.goal_control();
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register(run_id.into(), handle);
        orch.process_state
            .set_current_turn_id(run_id, Some(turn_id));
        (control, requests)
    }

    fn claim(control: &GoalControl) -> Option<GoalPause> {
        control
            .with_control(|_, claim| Ok(claim.inspect()))
            .unwrap()
    }

    fn set_statuses(requests: &Arc<Mutex<Vec<Value>>>) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == "thread/goal/set")
            .map(|request| request["params"]["status"].as_str().unwrap().to_string())
            .collect()
    }

    async fn blocked_child(
        extra_replies: Vec<Result<Value, String>>,
    ) -> (Orchestrator, GoalControl, Arc<Mutex<Vec<Value>>>) {
        let db = test_db().await;
        let mut snapshot: Value = serde_json::from_str(&running_child_snapshot()).unwrap();
        // A Blocked job that owes no delegated `return` (no packet names it),
        // so the missing-return re-prompt never resumes it out of Blocked.
        snapshot["delegatedPackets"] = serde_json::json!([]);
        let mut output = snapshot["recipe"]["nodes"][0].clone();
        output["id"] = "return-node".into();
        output["nodeType"] = "artifact".into();
        output["name"] = "Return".into();
        output["artifactConfig"] = serde_json::to_value(crate::models::ArtifactNodeConfig {
            name: "return".into(),
            schema: Some(serde_json::json!({"type": "object"})),
            confirm_policy: crate::models::ConfirmPolicy::Auto,
            content: None,
        })
        .unwrap();
        snapshot["recipe"]["nodes"]
            .as_array_mut()
            .unwrap()
            .push(output);
        snapshot["recipe"]["edges"] = serde_json::to_value(vec![crate::models::RecipeEdge {
            id: "child-return".into(),
            edge_type: crate::models::RecipeEdgeType::Context,
            source_node_id: "child-node".into(),
            source_handle: "context-out".into(),
            target_node_id: "return-node".into(),
            target_handle: "context-in".into(),
        }])
        .unwrap();
        seed_suspended_parent_with_running_child(&db, snapshot.to_string()).await;
        db.execute_script(
            "INSERT INTO sessions (id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES ('session-pause','j-child','codex','native-thread','open',1,1,1);
             UPDATE jobs SET current_session_id='session-pause' WHERE id='j-child';
             UPDATE runs SET session_id='session-pause' WHERE id='run-child';
             UPDATE turns SET session_id='session-pause',run_id='run-child' WHERE id='child-turn';",
        ).await.unwrap();
        let orch = test_orchestrator(db);
        let mut replies = vec![goal_reply("active"), goal_reply("paused")];
        replies.extend(extra_replies);
        let (control, requests) =
            register_codex(&orch, "run-child", "j-child", "child-turn", replies);
        assert!(transition_to_warm_state(&orch, "run-child", None));
        assert_eq!(turn_state(&orch, "child-turn").await, "complete");
        assert_eq!(
            scalar_text(&orch, "SELECT status FROM jobs WHERE id = ?1", "j-child")
                .await
                .as_deref(),
            Some("blocked")
        );
        assert_eq!(
            claim(&control),
            Some(GoalPause::JobState(JobPauseState::Blocked))
        );
        assert_eq!(set_statuses(&requests), ["paused"]);
        (orch, control, requests)
    }

    #[tokio::test]
    async fn blocked_job_finalization_pauses_as_job_state_not_operator_hold() {
        let (orch, control, requests) = blocked_child(vec![goal_reply("paused")]).await;
        let result = reactivate_goal(
            &orch.process_state,
            "run-child",
            None,
            JobRunnable::Unknown,
            GoalIntent::On,
        )
        .unwrap();
        let GoalReactivationResult::StillPaused { cause } = result else {
            panic!("blocked work must remain paused without admission");
        };
        assert_eq!(
            cause,
            GoalPauseCause::Paused(GoalPause::JobState(JobPauseState::Blocked))
        );
        assert_eq!(
            claim(&control),
            Some(GoalPause::JobState(JobPauseState::Blocked))
        );
        assert_eq!(set_statuses(&requests), ["paused"]);
    }

    #[tokio::test]
    async fn ordinary_admitted_steer_resumes_job_state_paused_work() {
        let (orch, control, requests) =
            blocked_child(vec![goal_reply("paused"), goal_reply("active")]).await;
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-child",
                None,
                JobRunnable::Attested,
                GoalIntent::On,
            )
            .unwrap(),
            GoalReactivationResult::Lifted {
                kind: "job_runnable",
                id: None
            }
        ));
        assert_eq!(claim(&control), None);
        assert_eq!(set_statuses(&requests), ["paused", "active"]);
    }

    /// A Stop pauses as interrupted. Nothing lifts it but the job's admission,
    /// and only while the operator's goal intent is on.
    #[tokio::test]
    async fn stop_pause_lifts_only_on_admission_with_the_goal_on() {
        let db = test_db().await;
        seed_top_level_run(&db, "run-pause", "job-pause", "turn-pause", "none").await;
        let orch = test_orchestrator(db);
        let (control, requests) = register_codex(
            &orch,
            "run-pause",
            "job-pause",
            "turn-pause",
            vec![
                goal_reply("active"),
                goal_reply("paused"),
                Ok(Value::Null),
                goal_reply("paused"),
                goal_reply("paused"),
                goal_reply("paused"),
                goal_reply("active"),
            ],
        );
        stop_session_internal(&orch, "run-pause", StopReason::OperatorStop).unwrap();
        assert_eq!(claim(&control), Some(GoalPause::Interrupted));
        assert!(requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request["method"] == "turn/interrupt"));
        let unrelated = GoalReactivation::OwnedConditionResolved(GoalPauseOwner::DurableWait {
            wait_id: "unrelated-wait".into(),
        });
        for (trigger, runnable, intent, expected) in [
            (
                None,
                JobRunnable::Unknown,
                GoalIntent::On,
                GoalPauseCause::Paused(GoalPause::Interrupted),
            ),
            (
                Some(&unrelated),
                JobRunnable::Attested,
                GoalIntent::Off,
                GoalPauseCause::OperatorOff,
            ),
        ] {
            assert_eq!(
                reactivate_goal(&orch.process_state, "run-pause", trigger, runnable, intent)
                    .unwrap(),
                GoalReactivationResult::StillPaused { cause: expected }
            );
            assert_eq!(claim(&control), Some(GoalPause::Interrupted));
            assert_eq!(set_statuses(&requests), ["paused"]);
        }
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-pause",
                None,
                JobRunnable::Attested,
                GoalIntent::On,
            )
            .unwrap(),
            GoalReactivationResult::Lifted {
                kind: "job_runnable",
                ..
            }
        ));
        assert_eq!(claim(&control), None);
        assert_eq!(set_statuses(&requests), ["paused", "active"]);
    }

    #[tokio::test]
    async fn claimless_provider_pause_is_unattributed_and_recovers_on_admission() {
        let orch = test_orchestrator(test_db().await);
        let (control, requests) = register_codex(
            &orch,
            "run-pause",
            "job-pause",
            "turn-pause",
            vec![
                goal_reply("paused"),
                goal_reply("paused"),
                goal_reply("active"),
            ],
        );
        assert_eq!(claim(&control), None);
        let result = reactivate_goal(
            &orch.process_state,
            "run-pause",
            None,
            JobRunnable::Unknown,
            GoalIntent::On,
        )
        .unwrap();
        let GoalReactivationResult::StillPaused { cause } = result else {
            panic!("unattested pause must remain");
        };
        assert_eq!(cause, GoalPauseCause::Unattributed);
        assert_eq!(claim(&control), None);
        assert!(set_statuses(&requests).is_empty());
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-pause",
                None,
                JobRunnable::Attested,
                GoalIntent::On,
            )
            .unwrap(),
            GoalReactivationResult::Lifted {
                kind: "job_runnable",
                id: None
            }
        ));
        assert_eq!(claim(&control), None);
        assert_eq!(set_statuses(&requests), ["active"]);
    }

    #[tokio::test]
    async fn complete_goal_is_never_reactivated() {
        let orch = test_orchestrator(test_db().await);
        let (complete_control, complete_requests) = register_codex(
            &orch,
            "run-complete",
            "job-complete",
            "turn-complete",
            vec![goal_reply("complete")],
        );
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-complete",
                None,
                JobRunnable::Attested,
                GoalIntent::On,
            )
            .unwrap(),
            GoalReactivationResult::StillPaused {
                cause: GoalPauseCause::Complete
            }
        ));
        assert_eq!(claim(&complete_control), None);
        assert!(set_statuses(&complete_requests).is_empty());
        assert_eq!(complete_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn deferred_goal_turn_on_a_claimless_goal_stays_unattributed() {
        let orch = test_orchestrator(test_db().await);
        let (control, requests) = register_codex(
            &orch,
            "run-pause",
            "job-pause",
            "turn-pause",
            vec![
                goal_reply("active"),
                goal_reply("paused"),
                Ok(Value::Null),
                goal_reply("paused"),
                goal_reply("paused"),
                goal_reply("active"),
            ],
        );
        defer_codex_goal_turn(
            &orch.process_state,
            "run-pause",
            "provider-deferred",
            PauseIntent::Reassert,
        )
        .unwrap();
        assert_eq!(claim(&control), None);
        {
            let sent = requests.lock().unwrap();
            assert_eq!(sent.len(), 3);
            assert_eq!(sent[2]["method"], "turn/interrupt");
            assert_eq!(sent[2]["params"]["turnId"], "provider-deferred");
        }
        assert_eq!(set_statuses(&requests), ["paused"]);
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-pause",
                None,
                JobRunnable::Unknown,
                GoalIntent::On,
            )
            .unwrap(),
            GoalReactivationResult::StillPaused {
                cause: GoalPauseCause::Unattributed
            }
        ));
        assert_eq!(claim(&control), None);
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-pause",
                None,
                JobRunnable::Attested,
                GoalIntent::On,
            )
            .unwrap(),
            GoalReactivationResult::Lifted {
                kind: "job_runnable",
                id: None
            }
        ));
        assert_eq!(claim(&control), None);
        assert_eq!(set_statuses(&requests), ["paused", "active"]);
    }
}

/// Drive the publication boundary, not a fabricated unowned database head.
/// The emitter runs after the predecessor settles but before its occupancy is
/// released. A launch admitted there must not be erased by that release.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_publication_cannot_erase_an_attention_successors_occupancy() {
    let db = test_db().await;
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
         INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',1,'I','active',1,1);
         INSERT INTO jobs(id,project_id,issue_id,status,node_name,uri_segment,current_session_id,model,created_at,updated_at)
           VALUES('j','p','i','running','builder','builder','s','opus',1,1);
         INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
           VALUES('s','j','claude','native-thread','open',1,1,1);
         INSERT INTO runs(id,job_id,project_id,session_id,status,started_at,start_mode,created_at,updated_at)
           VALUES('r','j','p','s','live',2,'fresh',1,3);
         INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at)
           VALUES('t','s','r','j',1,'running','initial',1,2,3);
         UPDATE jobs SET current_turn_id='t' WHERE id='j';
         INSERT INTO messages(id,channel_type,sender_run_id,sender_name,content,created_at)
           VALUES('m','direct','peer','peer','please inspect the successor',2);
         INSERT INTO message_recipients(message_id,position,recipient_job_id,recipient_run_id,created_at)
           VALUES('m',0,'j','r',2);",
    ).await.unwrap();
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);
    let queued = orch.clone();
    tokio::task::spawn_blocking(move || {
        crate::messages::delivery::enqueue_direct_push_with_operator_trigger(
            &queued,
            "j",
            "m",
            crate::messages::queued::Delivery::Queue,
            None,
        )
    })
    .await
    .unwrap()
    .unwrap();
    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let sent = Arc::new(Mutex::new(Vec::new()));
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(
            cairn_models::session::claude::ClaudeControl::boxed(Box::new(Capture(sent.clone()))),
        ))),
        Some("s".into()),
        Some("j".into()),
    );
    handle.model = Some("opus".into());
    handle.begin_turn("t");
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("r".into(), handle);
    orch.process_state.set_current_turn_id("r", Some("t"));

    let launched = Arc::new(Mutex::new(None));
    let callback_launch = launched.clone();
    let callback_orch = orch.clone();
    *emitter.on_db_change.lock().unwrap() = Some(Box::new(move || {
        if super::common::turn_state(&callback_orch, "t").as_deref() != Some("complete") {
            return false;
        }
        let lock = callback_orch.job_launch_lock("j");
        assert!(
            lock.try_lock().is_ok(),
            "publication callbacks must run after releasing launch authority"
        );
        // Synchronously force the previously legal ordering:
        // settlement -> successor attach -> predecessor clear. No timing or
        // scheduler luck is involved, and a lock regression fails, not hangs.
        *callback_launch.lock().unwrap() = Some(crate::execution::jobs::continue_job_impl(
            crate::execution::jobs::WakeCause::Addressed,
            &callback_orch,
            "j",
            None,
            None,
            None,
        ));
        true
    }));
    let finishing = orch.clone();
    tokio::task::spawn_blocking(move || {
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(serde_json::json!({
            "method": "turn/completed",
            "params": {"turn": {"id": "provider-predecessor", "status": "completed"}}
        }))
        .unwrap();
        tx.send(serde_json::json!({
            "method": "turn/started",
            "params": {"turn": {"id": "provider-successor"}}
        }))
        .unwrap();
        drop(tx);
        crate::backends::codex::CodexBackend::reader_thread_app_server(
            &finishing,
            &finishing.services.emitter,
            "r",
            Some("s".into()),
            rx,
            Arc::new(
                cairn_models::codex::app_server::AppServerClient::for_test_scripted(
                    vec![],
                    Arc::new(Mutex::new(Vec::new())),
                ),
            ),
            Arc::new(Mutex::new(Some("provider-predecessor".into()))),
            None,
            "codex".into(),
            finishing.db.local.clone(),
            None,
        );
    })
    .await
    .unwrap();
    let run = launched
        .lock()
        .unwrap()
        .take()
        .expect("settlement published")
        .expect("attention continuation launches");
    assert_eq!(run.id, "r", "reuse the retained process");
    let head = orch
        .db
        .local
        .query_opt_text("SELECT current_turn_id FROM jobs WHERE id='j'", ())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(head, "t");
    let error = orch
        .db
        .local
        .query_opt_text(
            "SELECT data FROM events WHERE event_type='system:error' ORDER BY sequence LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert!(
        error.is_none(),
        "the scripted reader must admit the attention successor without watchdog failure: {error:?}"
    );
    assert_eq!(turn_state(&orch, "t").await, "complete");
    assert_eq!(turn_state(&orch, &head).await, "running");
    let current = orch.process_state.get_current_turn_id("r");
    assert_eq!(
        current.as_deref(),
        Some(head.as_str()),
        "the reader must retain the real attention successor's owner"
    );
    let sent = String::from_utf8(sent.lock().unwrap().clone()).unwrap();
    assert_eq!(
        sent.matches("please inspect the successor").count(),
        1,
        "the attention prompt is delivered exactly once: {sent}"
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_i64("SELECT COUNT(*) FROM runs WHERE job_id='j'", (),)
            .await
            .unwrap(),
        Some(1),
        "no replacement run"
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_i64("SELECT COUNT(*) FROM sessions WHERE job_id='j'", (),)
            .await
            .unwrap(),
        Some(1),
        "no replacement session"
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_text(
                "SELECT turn_id FROM events WHERE id='codex-goal-turn:provider-successor'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some(head.as_str())
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_i64("SELECT COUNT(*) FROM turns WHERE job_id='j'", (),)
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE turn_id=?1 AND event_type='attention:briefing'",
                (head,),
            )
            .await
            .unwrap(),
        Some(1)
    );
}

/// Record an open `read` call `tool_use_id` on `turn`, as run `run`'s process
/// made it, in that turn's own session.
async fn seed_open_read(db: &LocalDb, run: &str, turn: &str, tool_use_id: &str) {
    let data = serde_json::json!({
        "eventType": "assistant",
        "isError": false,
        "toolUses": [{"id": tool_use_id, "name": "mcp__cairn__read", "input": {"paths": ["file:a.rs"]}}]
    })
    .to_string();
    db.execute(
        "INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,created_at)
         SELECT 'e-open-' || ?3, ?1, session_id, id,
                COALESCE((SELECT MAX(sequence) + 1 FROM events WHERE run_id = ?1), 50),
                50, 'assistant', ?4, 50 FROM turns WHERE id = ?2",
        cairn_db::turso::params![run, turn, tool_use_id, data],
    )
    .await
    .unwrap();
}

async fn results_for(orch: &Orchestrator, tool_use_id: &str) -> Vec<Value> {
    let tool_use_id = tool_use_id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let tool_use_id = tool_use_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data FROM events WHERE event_type = 'tool_result'
                         AND json_extract(data, '$.toolUseId') = ?1",
                        cairn_db::turso::params![tool_use_id.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(serde_json::from_str(&row.text(0)?).unwrap());
                }
                Ok(out)
            })
        })
        .await
        .unwrap()
}

/// cairn/6123: provider depletion interrupts the turn and stops its process
/// before finalizing, so the cut-off gate never sees a running turn. The
/// calls that process left open are still answered once, with its cause.
#[tokio::test]
async fn provider_depletion_answers_the_calls_its_stopped_process_left_open() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-dep", "job-dep", "turn-dep", "none").await;
    seed_open_read(&db, "run-dep", "turn-dep", "toolu-dep").await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-dep", Some("job-dep"));
    orch.process_state
        .set_current_turn_id("run-dep", Some("turn-dep"));

    super::interrupt_run_for_provider_depletion(&orch, "run-dep");

    assert_eq!(turn_state(&orch, "turn-dep").await, "interrupted");
    let results = results_for(&orch, "toolu-dep").await;
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0]["raw"]["reason"], "process_lost");
    assert_eq!(results[0]["raw"]["exit_reason"], "provider_depleted");
    let text = results[0]["toolResult"].as_str().unwrap();
    assert!(
        text.contains("(its provider account is depleted)"),
        "{text}"
    );
}

/// cairn/6123: a delegated task's `fail_run` marks its turn `failed` before
/// finalizing, so the cut-off gate never sees it. The calls its process left
/// open are still answered once, with the failure's reason.
#[tokio::test]
async fn a_delegated_fail_run_answers_the_calls_its_process_left_open() {
    let db = test_db().await;
    seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
    seed_open_read(&db, "run-child", "child-turn", "toolu-child").await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-child", Some("j-child"));
    orch.process_state
        .set_current_turn_id("run-child", Some("child-turn"));

    super::fail_run(&orch, "run-child", "turn_failed");

    assert_eq!(turn_state(&orch, "child-turn").await, "failed");
    let results = results_for(&orch, "toolu-child").await;
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0]["raw"]["reason"], "process_lost");
    assert_eq!(results[0]["raw"]["exit_reason"], "turn_failed");
}

#[tokio::test]
async fn owned_failure_answers_all_calls_before_publication() {
    for delegated in [false, true] {
        let db = test_db().await;
        let (run, job, turn) = if delegated {
            seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
            ("run-child", "j-child", "child-turn")
        } else {
            seed_top_level_run(&db, "run-owned", "job-owned", "turn-owned", "none").await;
            ("run-owned", "job-owned", "turn-owned")
        };
        for id in ["owned-read-a", "owned-read-b"] {
            seed_open_read(&db, run, turn, id).await;
        }
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, run, Some(job));
        orch.process_state.set_current_turn_id(run, Some(turn));
        let stale_identity = orch.process_state.capture_process_identity(run).unwrap();
        let stale_owner = orch
            .process_state
            .capture_turn_owner_for_identity(&stale_identity)
            .unwrap();
        register_warm_process(&orch, run, Some(job));
        orch.process_state.set_current_turn_id(run, Some(turn));
        assert!(!super::finalize_owned_completion(
            &orch,
            &stale_identity,
            Some(&stale_owner),
            RunStatus::Crashed,
            Some("turn_failed"),
            Some("stale owner's cause"),
            || panic!("stale owner published"),
        ));
        assert!(results_for(&orch, "owned-read-a").await.is_empty());
        assert_eq!(
            orch.db
                .local
                .query_opt_text("SELECT error_message FROM runs WHERE id=?1", (run,))
                .await
                .unwrap(),
            None,
            "a stale registration cannot record its cause"
        );
        const CAUSE: &str = "provider refused\nAuthorization: Bearer abcdefgh12345678";
        let sanitized = crate::security::sanitize::failure_cause(CAUSE).unwrap();
        assert!(!sanitized.contains("abcdefgh12345678"));
        assert!(!sanitized.chars().any(char::is_control));
        let expected_cause = if delegated {
            orch.db
                .local
                .execute(
                    "UPDATE runs SET error_message='first recorded cause' WHERE id=?1",
                    (run,),
                )
                .await
                .unwrap();
            "first recorded cause".to_string()
        } else {
            sanitized
        };
        let expected_for_callback = expected_cause.clone();
        let identity = orch.process_state.capture_process_identity(run).unwrap();
        let owner = orch
            .process_state
            .capture_turn_owner_for_identity(&identity)
            .unwrap();
        let callback_orch = orch.clone();
        *emitter.on_db_change.lock().unwrap() = Some(Box::new(move || {
            let lock = callback_orch.job_launch_lock(job);
            let _guard = lock
                .try_lock()
                .expect("settlement publication must not hold launch lock");
            let db = callback_orch.db.local.clone();
            let count = crate::storage::run_db_blocking(move || async move {
                db.query_opt_i64(
                    "SELECT COUNT(*) FROM events WHERE event_type='tool_result' AND json_extract(data,'$.raw.reason')='process_lost'",
                    (),
                ).await.map_err(|e| e.to_string())
            }).unwrap();
            assert_eq!(count, Some(2), "first notification sees every answer");
            let db = callback_orch.db.local.clone();
            let cause = crate::storage::run_db_blocking(move || async move {
                db.query_opt_text("SELECT error_message FROM runs WHERE id=?1", (run,))
                    .await
                    .map_err(|error| error.to_string())
            })
            .unwrap();
            assert_eq!(
                cause.as_deref(),
                Some(expected_for_callback.as_str()),
                "the cause precedes publication"
            );
            true
        }));
        let published = std::cell::Cell::new(false);
        assert!(super::finalize_owned_completion(
            &orch,
            &identity,
            Some(&owner),
            RunStatus::Crashed,
            Some("turn_failed"),
            Some(CAUSE),
            || published.set(true),
        ));
        assert!(published.get());
        assert!(!super::finalize_owned_completion(
            &orch,
            &identity,
            Some(&owner),
            RunStatus::Crashed,
            Some("turn_failed"),
            Some("duplicate callback's cause"),
            || panic!("duplicate owner published"),
        ));
        assert_eq!(
            orch.db
                .local
                .query_opt_text("SELECT error_message FROM runs WHERE id=?1", (run,))
                .await
                .unwrap()
                .as_deref(),
            Some(expected_cause.as_str()),
            "a duplicate completion cannot replace the first cause"
        );
        assert_eq!(
            turn_state(&orch, turn).await,
            if delegated { "failed" } else { "interrupted" }
        );
        for id in ["owned-read-a", "owned-read-b"] {
            let results = results_for(&orch, id).await;
            assert_eq!(results.len(), 1);
            assert_eq!(results[0]["raw"]["reason"], "process_lost");
            assert_eq!(results[0]["raw"]["exit_reason"], "turn_failed");
        }
        let note =
            super::unanswered_calls::lost_calls_note(&orch.db.local, run, turn, &["process_lost"])
                .await
                .unwrap()
                .expect("successor prompt carries lost-call answers");
        assert!(note.contains("file:a.rs"));
    }
}

#[tokio::test]
async fn owned_failure_drains_write_without_launch_lock_and_blocks_retry() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-drain", "job-drain", "turn-drain", "none").await;
    seed_open_read(&db, "run-drain", "turn-drain", "owned-write").await;
    db.execute(
        "UPDATE events SET data=json_set(data,'$.toolUses[0].name','mcp__cairn__write','$.toolUses[0].input',json(?1)) WHERE id='e-open-owned-write'",
        (r#"{"changes":[{"target":"cairn:~/todos","mode":"append","payload":{"todos":[]}}]}"#,),
    ).await.unwrap();
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-drain", Some("job-drain"));
    orch.process_state
        .set_current_turn_id("run-drain", Some("turn-drain"));
    let identity = orch
        .process_state
        .capture_process_identity("run-drain")
        .unwrap();
    let owner = orch
        .process_state
        .capture_turn_owner_for_identity(&identity)
        .unwrap();
    let delivery = crate::dispatch::Delivery::new();
    let registration = orch.write_stops.enter("run-drain", "owned-write", delivery);
    let writer_orch = orch.clone();
    let writer = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !writer_orch
            .write_stops
            .is_raised("run-drain", "owned-write")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "process-loss drain never began"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        stop_session_internal(&writer_orch, "run-drain", StopReason::OperatorStop).unwrap();
        super::stop::stop_active_turn_for_run(&writer_orch, "run-drain", true);
        finalize_run(&writer_orch, "run-drain", RunStatus::Exited);
        finalize_run(&writer_orch, "run-drain", RunStatus::Crashed);
        assert!(writer_orch.has_terminal_completion_claim("run-drain"));
        assert!(writer_orch.is_turn_terminal_claimed("turn-drain"));
        let shutdown_orch = writer_orch.clone();
        crate::storage::run_db_blocking(move || async move {
            Ok::<_, String>(
                super::stop::stop_agents_for_host_shutdown(
                    &shutdown_orch,
                    std::time::Duration::from_secs(1),
                )
                .await,
            )
        })
        .unwrap();
        assert_eq!(
            writer_orch.process_state.has_process("run-drain"),
            Some(false)
        );
        finalize_run(&writer_orch, "run-drain", RunStatus::Exited);
        finalize_run(&writer_orch, "run-drain", RunStatus::Crashed);
        stop_session_internal(&writer_orch, "run-drain", StopReason::OperatorStop).unwrap();
        super::stop::stop_active_turn_for_run(&writer_orch, "run-drain", true);
        stop_job(&writer_orch, "job-drain").unwrap();
        assert!(
            writer_orch.has_terminal_completion_claim("run-drain"),
            "transport teardown must not erase completion authority"
        );
        let db = writer_orch.db.local.clone();
        let state = crate::storage::run_db_blocking(move || async move {
            db.query_opt_text("SELECT state FROM turns WHERE id='turn-drain'", ())
                .await
                .map_err(|e| e.to_string())
        })
        .unwrap();
        assert_eq!(
            state.as_deref(),
            Some("running"),
            "Stop cannot adopt a finalizing turn"
        );
        let lock = writer_orch.job_launch_lock("job-drain");
        let _guard = lock
            .try_lock()
            .expect("write boundary needs unlocked launch authority");
        assert!(
            crate::execution::jobs::claim_retry_successor_if_head_matches(
                &writer_orch,
                writer_orch.db.local.clone(),
                "job-drain",
                "sess-attn",
                "turn-drain",
            )
            .unwrap()
            .is_none(),
            "active turn prevents retry before answers exist"
        );
        let settlement_lock = writer_orch.call_settlement_lock("run-drain", "turn-drain");
        assert!(
            settlement_lock.try_lock().is_err(),
            "process-loss owns answer discovery through persistence"
        );
        let competing_orch = writer_orch.clone();
        let competing = std::thread::spawn(move || {
            super::unanswered_calls::settle_unanswered_calls(
                &competing_orch,
                "run-drain",
                "turn-drain",
                super::unanswered_calls::InterruptCause::UserStop,
            )
            .unwrap()
        });
        drop(registration);
        assert_eq!(
            competing.join().unwrap(),
            0,
            "Stop cannot answer a call already claimed by process loss"
        );
    });
    assert!(super::finalize_owned_completion(
        &orch,
        &identity,
        Some(&owner),
        RunStatus::Crashed,
        Some("turn_failed"),
        None,
        || {},
    ));
    writer.join().unwrap();
    assert!(!orch.has_terminal_completion_claim("run-drain"));
    assert!(!orch.is_turn_terminal_claimed("turn-drain"));
    let results = results_for(&orch, "owned-write").await;
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]["raw"]["effect"], "none",
        "write drained rather than timing out unknown"
    );
    assert_eq!(results[0]["raw"]["exit_reason"], "turn_failed");
    assert_eq!(
        orch.db
            .local
            .query_opt_text("SELECT exit_reason FROM runs WHERE id='run-drain'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("turn_failed"),
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_text("SELECT end_reason FROM turns WHERE id='turn-drain'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("provider_failure"),
    );
}

#[tokio::test]
async fn owned_failure_guard_keeps_no_process_operator_stop_cancellation() {
    let db = test_db().await;
    seed_top_level_run(
        &db,
        "run-no-process",
        "job-no-process",
        "turn-no-process",
        "none",
    )
    .await;
    db.execute_script(
        "INSERT INTO sessions(id,job_id,status,backend_id,created_at,updated_at)
         VALUES('session-no-process','job-no-process','open','handle-no-process',1,1);
         UPDATE runs SET session_id='session-no-process',status='crashed' WHERE id='run-no-process';
         UPDATE turns SET session_id='session-no-process',state='yielded',yield_reason='wait' WHERE id='turn-no-process';
         INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at)
         VALUES('wait-no-process','job-no-process','run-no-process','session-no-process','turn-no-process','tool-no-process','{\"kind\":\"terminal\"}','pending',1);",
    ).await.unwrap();
    let orch = test_orchestrator(db);
    assert_eq!(
        orch.process_state.has_process("run-no-process"),
        Some(false)
    );
    super::stop::operator_stop_run(&orch, "run-no-process").unwrap();
    assert_eq!(turn_state(&orch, "turn-no-process").await, "cancelled");
    assert_eq!(
        orch.db
            .local
            .query_opt_text(
                "SELECT state FROM agent_waits WHERE id='wait-no-process'",
                ()
            )
            .await
            .unwrap()
            .as_deref(),
        Some("cancelled"),
    );
}

/// A foreground answer rotates the host turn inside a still-running provider
/// turn. Its terminal must follow that authorized rotation, but not a later
/// provider's ordinary turn attachment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_inline_question_rotation_preserves_completion_and_distinct_provider() {
    use crate::mcp::handlers::planning::{answer_prompt_payload, ask_questions};
    use crate::mcp::types::{AskUserPayload, McpCallbackRequest};
    use std::time::Duration;

    let db = test_db().await;
    seed_top_level_run(&db, "r", "j", "t", "none").await;
    db.execute_script(
        "INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
           VALUES('s','j','codex','native-thread','open',1,1,1);
         UPDATE jobs SET current_session_id='s',model='codex' WHERE id='j';
         UPDATE runs SET session_id='s',project_id='p-attn',issue_id='i-attn' WHERE id='r';
         UPDATE turns SET session_id='s' WHERE id='t';",
    )
    .await
    .unwrap();
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);
    // This fixture has no native goal. Reconciliation reads that absence,
    // rather than consuming the unrelated stop/resume fixture's goal script.
    let client = Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_scripted(
            vec![Ok(serde_json::json!({"result":{"goal":null}})); 8],
            Arc::new(Mutex::new(Vec::new())),
        ),
    );
    let provider_turn = Arc::new(Mutex::new(None));
    let stdin = cairn_models::session::codex::CodexControl::new(
        client.clone(),
        "native-thread".into(),
        "/tmp/prj".into(),
        None,
        None,
        None,
        provider_turn.clone(),
    );
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(Box::new(stdin)))),
        Some("s".into()),
        Some("j".into()),
    );
    handle.backend = Some("codex".into());
    handle.model = Some("codex".into());
    handle.begin_turn("t");
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("r".into(), handle);

    let (tx, rx) = crossbeam_channel::unbounded();
    let reader_orch = orch.clone();
    let reader_turn = provider_turn.clone();
    let reader = tokio::task::spawn_blocking(move || {
        crate::backends::codex::CodexBackend::reader_thread_app_server(
            &reader_orch,
            &reader_orch.services.emitter,
            "r",
            Some("s".into()),
            rx,
            client,
            reader_turn,
            None,
            "codex".into(),
            reader_orch.db.local.clone(),
            None,
        );
    });
    let started = |id: &str| {
        serde_json::json!({
            "method":"turn/started","params":{"turn":{"id":id}}
        })
    };
    let completed = |id: &str| {
        serde_json::json!({
            "method":"turn/completed","params":{"turn":{"id":id,"status":"completed"}}
        })
    };
    let usage = |id: &str, total: u32| {
        serde_json::json!({
            "method":"thread/tokenUsage/updated","params":{"turnId":id,"tokenUsage":{
                "last":{"inputTokens":total,"outputTokens":1,"totalTokens":total+1},
                "total":{"inputTokens":total,"outputTokens":1,"totalTokens":total+1}
            }}
        })
    };
    tx.send(started("P")).unwrap();
    // The reader writes this only after binding the owner and arming its lease.
    tokio::time::timeout(Duration::from_secs(5), async {
        while provider_turn.lock().unwrap().as_deref() != Some("P") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual reader starts P under t");
    tx.send(usage("P", 100)).unwrap();

    let request: McpCallbackRequest = serde_json::from_value(serde_json::json!({
        "cwd":"/tmp/prj","run_id":"r","tool":"write","payload":{},
        "tool_use_id":"inline-question"
    }))
    .unwrap();
    let payload: AskUserPayload = serde_json::from_value(serde_json::json!({
        "questions":[{"question":"Continue within this provider turn?","options":[],"multiSelect":false}]
    })).unwrap();
    let receivers_before = orch.prompt_responses.receiver_count();
    let ask = ask_questions(&orch, &request, payload, false, Some("inline-question"));
    let answer = async {
        // Subscription is established after the prompt commits and the process
        // yields. Waiting for that fact avoids racing the broadcast itself.
        tokio::time::timeout(Duration::from_secs(5), async {
            while orch.prompt_responses.receiver_count() <= receivers_before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("question inline waiter subscribed");
        let prompt_id = orch
            .db
            .local
            .query_opt_text("SELECT id FROM prompts WHERE run_id='r'", ())
            .await
            .unwrap()
            .expect("real question stored");
        answer_prompt_payload(
            &orch,
            &prompt_id,
            &serde_json::json!({"answer":"yes"}),
            crate::turns::queries::ResolutionProvenance::new(
                cairn_common::identity::AppearanceTransport::ResourcePatch,
                Some("cairn://p/prj/42/1/reviewer".into()),
            ),
        )
        .await
        .expect("real question answer commits");
    };
    let (outcome, ()) = tokio::join!(ask, answer);
    assert!(
        outcome.answers.is_some(),
        "answer returned inline: {}",
        outcome.text
    );
    let rotated = orch
        .process_state
        .get_current_turn_id("r")
        .expect("answer successor attached");
    assert_ne!(rotated, "t");
    assert_eq!(turn_state(&orch, "t").await, "yielded");
    assert_eq!(turn_state(&orch, &rotated).await, "running");
    assert_eq!(
        orch.db
            .local
            .query_opt_text(
                "SELECT start_reason FROM turns WHERE id=?1",
                (rotated.clone(),),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("prompt_response")
    );

    assert_eq!(
        orch.db
            .local
            .query_opt_text(
                "SELECT state FROM codex_watchdog_leases WHERE run_id='r' AND provider_turn_id='P'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("active"),
    );
    tx.send(completed("P")).unwrap();
    // Publication is after both settlement and occupancy release.
    let published = tokio::time::timeout(Duration::from_secs(5), async {
        while emitter.events_named("run-turn-completed").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let lease = orch
        .db
        .local
        .query_opt_text(
            "SELECT state FROM codex_watchdog_leases WHERE run_id='r' AND provider_turn_id='P'",
            (),
        )
        .await
        .unwrap();
    let state = turn_state(&orch, &rotated).await;
    let occupant = orch.process_state.get_current_turn_id("r");
    assert!(
        published.is_ok(),
        "P completion follows its inline-question rotation: T2={rotated} state={state} occupant={occupant:?} P_lease={lease:?}"
    );
    assert_eq!(lease.as_deref(), Some("disarmed"));
    assert_eq!(state, "complete");
    assert!(orch.process_state.is_warm("r"));
    assert_eq!(orch.process_state.get_current_turn_id("r"), None);
    assert_eq!(emitter.events_named("run-turn-completed").len(), 1);
    assert_eq!(
        orch.db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE event_type='result:success'",
                (),
            )
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_i64(
                "SELECT input_tokens FROM events WHERE event_type='result:success' AND turn_id=?1",
                (rotated.clone(),),
            )
            .await
            .unwrap(),
        Some(100)
    );

    // Q enters through continuation admission, minting a distinct FollowUp
    // from the terminal head despite this fixture having no native goal.
    // It is not another host-side rotation of P.
    tx.send(started("Q")).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while provider_turn.lock().unwrap().as_deref() != Some("Q") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("distinct provider Q starts");
    let next = orch
        .process_state
        .get_current_turn_id("r")
        .expect("Q owns its own turn");
    assert_ne!(next, rotated);
    tx.send(completed("P")).unwrap();
    // A context snapshot is an ordered reader barrier after the duplicate.
    let snapshots_before = emitter.events_named("context-tokens-updated").len();
    tx.send(usage("Q", 110)).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while emitter.events_named("context-tokens-updated").len() <= snapshots_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reader consumes duplicate P before Q usage");
    assert_eq!(turn_state(&orch, &next).await, "running");
    assert_eq!(
        orch.process_state.get_current_turn_id("r").as_deref(),
        Some(next.as_str())
    );
    assert_eq!(emitter.events_named("run-turn-completed").len(), 1);
    assert_eq!(
        orch.db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE event_type='result:success'",
                (),
            )
            .await
            .unwrap(),
        Some(1)
    );
    tx.send(completed("Q")).unwrap();
    tx.send(completed("Q")).unwrap();
    drop(tx);
    reader.await.unwrap();
    assert_eq!(turn_state(&orch, &next).await, "complete");
    assert_eq!(turn_state(&orch, &rotated).await, "complete");
    assert!(orch.process_state.is_warm("r"));
    assert_eq!(orch.process_state.get_current_turn_id("r"), None);
    assert_eq!(emitter.events_named("run-turn-completed").len(), 2);
    assert_eq!(
        orch.db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE event_type='result:success'",
                (),
            )
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        orch.db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM events WHERE event_type='system:error'",
                (),
            )
            .await
            .unwrap(),
        Some(0),
        "no watchdog/admission failure"
    );
}
