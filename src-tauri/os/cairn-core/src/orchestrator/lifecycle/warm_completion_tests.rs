use super::{
    finalize_run, finalize_run_for_recovery, stop_job, stop_session, stop_session_internal,
    suspend_run_for_durable_wait, transition_to_warm_safety_pause, transition_to_warm_state,
    StopReason,
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

#[derive(Clone, Default)]
struct SharedCaptureEmitter {
    events: Arc<Mutex<Vec<(String, Value)>>>,
}

#[tokio::test]
async fn delegated_repeated_output_parks_as_safety_pause_and_notifies_parent() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-safety", "job-safety", "turn-safety", "none").await;
    db.execute_script(
        "INSERT INTO jobs(id,issue_id,project_id,status,node_name,created_at,updated_at)
           VALUES('job-parent','i-attn','p-attn','running','parent',1,1);
         UPDATE jobs SET parent_job_id='job-parent' WHERE id='job-safety';",
    )
    .await
    .unwrap();
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);
    register_warm_process(&orch, "run-safety", Some("job-safety"));
    orch.process_state
        .set_current_turn_id("run-safety", Some("turn-safety"));

    transition_to_warm_safety_pause(&orch, "run-safety");

    assert_eq!(turn_state(&orch, "turn-safety").await, "yielded");
    assert_eq!(
        scalar_text(
            &orch,
            "SELECT yield_reason FROM turns WHERE id = ?1",
            "turn-safety"
        )
        .await
        .as_deref(),
        Some("safety_pause")
    );
    assert_eq!(
        scalar_text(&orch, "SELECT status FROM runs WHERE id = ?1", "run-safety")
            .await
            .as_deref(),
        Some("live"),
        "the warm session remains open and resumable"
    );
    // Delegated work reports to its parent; desktop terminal toasts are for
    // top-level jobs. The parent push below is the actionable notification.
    assert!(agent_attention_events(&emitter, "paused").is_empty());
    assert!(agent_attention_events(&emitter, "failed").is_empty());
    assert!(agent_attention_events(&emitter, "completed").is_empty());
    let parent_pushes = orch.db.local.query_opt(
        "SELECT COUNT(*) FROM attention_pushes WHERE recipient='job-parent' AND key='child-safety:job-safety'",
        (),
        |row| row.i64(0),
    ).await.unwrap().unwrap_or(0);
    assert_eq!(
        parent_pushes, 1,
        "a delegated parent sees one resumable safety pause"
    );
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
        )
        .expect("explicit attention should inspect and reactivate the paused goal"),
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

fn agent_attention_events(emitter: &SharedCaptureEmitter, attention_type: &str) -> Vec<Value> {
    emitter
        .events_named("agent-attention")
        .into_iter()
        .filter(|payload| payload.get("type").and_then(Value::as_str) == Some(attention_type))
        .collect()
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

#[tokio::test]
async fn warm_transition_emits_completion_attention_for_top_level_run() {
    let db = test_db().await;
    // Issue still needs the driver (plan/PR awaiting confirmation looks like
    // this): the turn-end is a real idle-with-work, so the toast fires.
    seed_top_level_run(&db, "run-attn", "job-attn", "turn-attn", "needs_approval").await;
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);
    register_warm_process(&orch, "run-attn", Some("job-attn"));
    orch.process_state
        .set_current_turn_id("run-attn", Some("turn-attn"));

    assert!(transition_to_warm_state(&orch, "run-attn", None));

    let completed = agent_attention_events(&emitter, "completed");
    assert_eq!(completed.len(), 1);
    assert_eq!(
        completed[0].get("projectKey"),
        Some(&Value::String("prj".into()))
    );
    assert_eq!(
        completed[0].get("homeUri").and_then(Value::as_str),
        Some(cairn_common::uri::build_node_uri("prj", 42, 1, "builder").as_str())
    );
}

#[tokio::test]
async fn warm_transition_without_actionable_work_skips_completed_attention() {
    let db = test_db().await;
    // attention=none, status=active, no PR: the turn ended cleanly with
    // nothing for the driver to act on — a planner that just spawned child
    // tasks and is now waiting on them looks exactly like this. The desktop
    // "completed" toast must stay silent (CAIRN-1625).
    seed_top_level_run(&db, "run-quiet", "job-quiet", "turn-quiet", "none").await;
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);
    register_warm_process(&orch, "run-quiet", Some("job-quiet"));
    orch.process_state
        .set_current_turn_id("run-quiet", Some("turn-quiet"));

    assert!(transition_to_warm_state(&orch, "run-quiet", None));

    assert!(agent_attention_events(&emitter, "completed").is_empty());
}

#[tokio::test]
async fn later_finalize_does_not_duplicate_completed_attention() {
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

    assert_eq!(agent_attention_events(&emitter, "completed").len(), 1);
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
async fn clean_finalize_without_prior_warm_does_not_emit_completed_attention() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-finalize", "job-finalize", "turn-finalize", "none").await;
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);

    finalize_run(&orch, "run-finalize", RunStatus::Exited);

    assert!(agent_attention_events(&emitter, "completed").is_empty());
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

#[tokio::test]
async fn crash_finalize_emits_failed_attention_once() {
    let db = test_db().await;
    seed_top_level_run(&db, "run-crash", "job-crash", "turn-crash", "none").await;
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);

    finalize_run(&orch, "run-crash", RunStatus::Crashed);
    finalize_run(&orch, "run-crash", RunStatus::Crashed);

    assert_eq!(agent_attention_events(&emitter, "failed").len(), 1);
}

#[tokio::test]
async fn warm_transition_broadcasts_run_completion() {
    let db = test_db().await;
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run-bcast", None);

    // `spawn_task_packets`' inline 45s wait subscribes here to detect a
    // fast-finishing child. A warmed child must broadcast on it just like
    // `finalize_run` does, or a sub-45s batch never returns inline.
    let mut rx = orch.run_completions.subscribe();
    assert!(transition_to_warm_state(&orch, "run-bcast", None));

    assert_eq!(rx.try_recv().ok(), Some("run-bcast".to_string()));
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

/// Regression: the node-less reducer must NOT touch a packet-less child — a
/// `create_child_task` sub-agent has `parent_job_id` set and `recipe_node_id`
/// NULL just like a call/workflow, but persists no output contract and has no
/// pre-materialized delegated packet, while still being owed a `result`
/// artifact. Reducing it off its absent contract would derive `Complete` even
/// though it never wrote `result`. It must stay `running`.
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

    // The open prompt is cancelled (no longer counts toward NeedsInput).
    assert!(
        scalar_text(
            &orch,
            "SELECT response FROM prompts WHERE id = ?1",
            "sj-prompt"
        )
        .await
        .is_some(),
        "open prompt should be answered/cancelled"
    );
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
        SessionLifetime, SessionTurnCompletion, SessionTurnOutcome,
    };
    use crate::agent_process::stream::TokenCounts;
    use crate::orchestrator::lifecycle::{apply_codex_turn_completion, CodexTurnContext};

    fn complete(
        orch: &Orchestrator,
        run_id: &str,
        outcome: SessionTurnOutcome,
        lifetime: SessionLifetime,
        safety_pause: bool,
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
                repeat_safety_pause: safety_pause,
                failure_disposition: if handled {
                    TurnFailureDisposition::AlreadyHandled
                } else {
                    TurnFailureDisposition::Unhandled
                },
            },
        );
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
                false,
            );
            assert_eq!(
                turn_state(&orch, "child-turn").await,
                if failed { "failed" } else { "complete" }
            );
            assert_eq!(
                turn_state(&orch, "succ").await,
                "complete",
                "the actual resume gate must claim the pending parent successor"
            );
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
    async fn interrupt_finalizes_a_delegated_child_only_after_artifact_handoff() {
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
            complete(
                &orch,
                "run-child",
                SessionTurnOutcome::Interrupted,
                SessionLifetime::Persistent,
                false,
                false,
            );
            assert_eq!(
                scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "run-child")
                    .await
                    .as_deref(),
                Some(if terminal_tool { "exited" } else { "live" })
            );
            assert_eq!(
                scalar_text(
                    &orch,
                    "SELECT end_reason FROM turns WHERE id=?1",
                    "child-turn"
                )
                .await
                .as_deref(),
                terminal_tool.then_some("artifact_handoff")
            );
            assert_usage_attached(&orch, "child-turn", "result:interrupted").await;
        }
    }

    #[tokio::test]
    async fn safety_pause_overrides_success_and_preserves_the_pending_parent() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        let (orch, emitter) = test_orchestrator_with_emitter(db, None);
        register_warm_process(&orch, "run-child", Some("j-child"));
        orch.process_state
            .set_current_turn_id("run-child", Some("child-turn"));
        complete(
            &orch,
            "run-child",
            SessionTurnOutcome::Completed,
            SessionLifetime::Persistent,
            true,
            false,
        );
        assert_eq!(turn_state(&orch, "child-turn").await, "yielded");
        assert_eq!(
            scalar_text(
                &orch,
                "SELECT yield_reason FROM turns WHERE id=?1",
                "child-turn"
            )
            .await
            .as_deref(),
            Some("safety_pause")
        );
        assert_eq!(turn_state(&orch, "succ").await, "pending");
        assert_eq!(
            result_count(&orch).await,
            0,
            "a safety pause must not manufacture a success result"
        );
        assert_eq!(emitter.events_named("run-turn-completed").len(), 1);
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
            false,
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
                repeat_safety_pause: false,
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

    #[tokio::test]
    async fn empty_usage_emits_success_but_no_interrupt_or_handled_error_result() {
        for (outcome, expected) in [
            (SessionTurnOutcome::Completed, 1),
            (SessionTurnOutcome::Interrupted, 0),
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
                    repeat_safety_pause: false,
                    failure_disposition: TurnFailureDisposition::AlreadyHandled,
                },
            );
            assert_eq!(result_count(&orch).await, expected);
        }
    }

    #[tokio::test]
    async fn persisted_repeat_breaker_overrides_interrupted_artifact_handoff() {
        let db = test_db().await;
        seed_suspended_parent_with_running_child(&db, running_child_snapshot()).await;
        db.execute_script("INSERT INTO events(id,run_id,session_id,event_type,data,timestamp,created_at,sequence) VALUES('breaker','run-child','s-child','system:codex_repeat_circuit_breaker','{}',1,1,1);").await.unwrap();
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
            .store(true, std::sync::atomic::Ordering::Release);
        apply_codex_turn_completion(
            &orch,
            &orch.db.local,
            &orch.services.emitter,
            "run-child",
            Some("s-child"),
            SessionTurnCompletion {
                outcome: SessionTurnOutcome::Interrupted,
                usage: TokenCounts::default(),
                session_lifetime: SessionLifetime::Persistent,
            },
            CodexTurnContext {
                repeat_safety_pause: false,
                failure_disposition: TurnFailureDisposition::Unhandled,
            },
        );
        assert_eq!(turn_state(&orch, "child-turn").await, "yielded");
        assert_eq!(
            scalar_text(
                &orch,
                "SELECT yield_reason FROM turns WHERE id=?1",
                "child-turn"
            )
            .await
            .as_deref(),
            Some("safety_pause")
        );
        assert_eq!(turn_state(&orch, "succ").await, "pending");
        assert_eq!(
            scalar_text(&orch, "SELECT status FROM runs WHERE id=?1", "run-child")
                .await
                .as_deref(),
            Some("live")
        );
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
            &orch.db.local,
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
        defer_codex_goal_turn, reactivate_goal, rearm_goal_safety_brake, GoalPauseCause,
        GoalReactivationResult,
    };
    use crate::execution::jobs::{goal_pause_notice, GoalReactivation, JobRunnable};

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
        let result =
            reactivate_goal(&orch.process_state, "run-child", None, JobRunnable::Unknown).unwrap();
        let GoalReactivationResult::StillPaused { cause } = result else {
            panic!("blocked work must remain paused without admission");
        };
        assert_eq!(
            cause,
            GoalPauseCause::Paused(GoalPause::JobState(JobPauseState::Blocked))
        );
        let notice = goal_pause_notice(&cause);
        assert!(notice.contains("blocked"));
        assert!(!notice.to_lowercase().contains("operator"));
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
                JobRunnable::Attested
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

    #[tokio::test]
    async fn operator_stop_stays_sticky_across_peer_traffic_and_owned_resolutions() {
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
        assert_eq!(claim(&control), Some(GoalPause::OperatorHold));
        assert!(requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request["method"] == "turn/interrupt"));
        let unrelated = GoalReactivation::OwnedConditionResolved(GoalPauseOwner::DurableWait {
            wait_id: "unrelated-wait".into(),
        });
        for trigger in [None, Some(&unrelated)] {
            assert!(matches!(
                reactivate_goal(
                    &orch.process_state,
                    "run-pause",
                    trigger,
                    JobRunnable::Attested
                )
                .unwrap(),
                GoalReactivationResult::StillPaused {
                    cause: GoalPauseCause::Paused(GoalPause::OperatorHold)
                }
            ));
            assert_eq!(claim(&control), Some(GoalPause::OperatorHold));
            assert_eq!(set_statuses(&requests), ["paused"]);
        }
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-pause",
                Some(&GoalReactivation::Operator {
                    kind: "explicit_resume",
                    id: None,
                }),
                JobRunnable::Attested
            )
            .unwrap(),
            GoalReactivationResult::Lifted {
                kind: "explicit_resume",
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
        let result =
            reactivate_goal(&orch.process_state, "run-pause", None, JobRunnable::Unknown).unwrap();
        let GoalReactivationResult::StillPaused { cause } = result else {
            panic!("unattested pause must remain");
        };
        assert_eq!(cause, GoalPauseCause::Unattributed);
        assert_eq!(claim(&control), None);
        let notice = goal_pause_notice(&cause);
        assert!(!notice.to_lowercase().contains("operator"));
        assert!(!notice.contains("Stop"));
        assert!(set_statuses(&requests).is_empty());
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-pause",
                None,
                JobRunnable::Attested
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
    async fn safety_brake_and_complete_semantics_are_unchanged() {
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
        crate::backends::stdin::send_interrupt(
            &orch.process_state,
            "run-pause",
            PauseIntent::Stake(GoalPause::SafetyBrake),
        )
        .unwrap();
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-pause",
                None,
                JobRunnable::Attested
            )
            .unwrap(),
            GoalReactivationResult::StillPaused {
                cause: GoalPauseCause::Paused(GoalPause::SafetyBrake)
            }
        ));
        assert_eq!(claim(&control), Some(GoalPause::SafetyBrake));
        assert_eq!(set_statuses(&requests), ["paused"]);
        rearm_goal_safety_brake(&orch.process_state, "run-pause").unwrap();
        assert_eq!(claim(&control), Some(GoalPause::OperatorHold));
        assert!(matches!(
            reactivate_goal(
                &orch.process_state,
                "run-pause",
                Some(&GoalReactivation::Operator {
                    kind: "explicit_resume",
                    id: None,
                }),
                JobRunnable::Attested
            )
            .unwrap(),
            GoalReactivationResult::Lifted {
                kind: "explicit_resume",
                ..
            }
        ));
        assert_eq!(claim(&control), None);
        assert_eq!(set_statuses(&requests), ["paused", "active"]);

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
                JobRunnable::Attested
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
            reactivate_goal(&orch.process_state, "run-pause", None, JobRunnable::Unknown).unwrap(),
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
                JobRunnable::Attested
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
