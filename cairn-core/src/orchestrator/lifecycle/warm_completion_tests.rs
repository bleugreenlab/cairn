use super::{
    finalize_run, stop_job, stop_session_internal, suspend_run_for_durable_wait,
    transition_to_warm_state, InterruptFailurePolicy,
};
use crate::agent_process::process::{wrap_plain_stdin, RunHandle};
use crate::db::DbState;
use crate::models::RunStatus;
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::TestServicesBuilder;
use crate::services::EventEmitter;
use crate::storage::{DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
use serde_json::Value;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct SharedCaptureEmitter {
    events: Arc<Mutex<Vec<(String, Value)>>>,
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
    let db = LocalDb::open(temp.keep().join("warm-completion.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    db
}

fn test_orchestrator(db: LocalDb) -> Orchestrator {
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
fn register_warm_process(orch: &Orchestrator, run_id: &str, job_id: Option<&str>) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let child = Arc::new(Mutex::new(None));
    let stdin = Arc::new(Mutex::new(Some(wrap_plain_stdin(Box::new(
        Vec::<u8>::new(),
    )))));
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
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-attn','w-attn','Project','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-attn','p-attn',42,'Toast issue','active',?1,1,1)", (attention.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, issue_id, project_id, status, node_name, created_at, updated_at) VALUES (?1,'i-attn','p-attn','running','builder',1,1)", (job_id.as_str(),)).await?;
                conn.execute("INSERT INTO runs (id, job_id, status, created_at, updated_at) VALUES (?1,?2,'live',1,1)", (run_id.as_str(), job_id.as_str())).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES (?1,'sess-attn',?2,1,'running','initial',1,1)", (turn_id.as_str(), job_id.as_str())).await?;
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

    stop_session_internal(
        &orch,
        "run-no-stdin",
        true,
        InterruptFailurePolicy::HardKill,
    )
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

    suspend_run_for_durable_wait(&orch, "run-suspend", "delegated_wait_suspended")
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
async fn warm_transition_emits_completion_attention_for_top_level_run() {
    let db = test_db().await;
    // Issue still needs the driver (plan/PR awaiting confirmation looks like
    // this): the turn-end is a real idle-with-work, so the toast fires.
    seed_top_level_run(&db, "run-attn", "job-attn", "turn-attn", "needs_approval").await;
    let (orch, emitter) = test_orchestrator_with_emitter(db, None);
    register_warm_process(&orch, "run-attn", Some("job-attn"));
    orch.process_state
        .set_current_turn_id("run-attn", Some("turn-attn"));

    assert!(transition_to_warm_state(&orch, "run-attn"));

    let completed = agent_attention_events(&emitter, "completed");
    assert_eq!(completed.len(), 1);
    assert_eq!(
        completed[0].get("projectKey"),
        Some(&Value::String("PRJ".into()))
    );
    assert_eq!(
        completed[0].get("issueNumber").and_then(Value::as_i64),
        Some(42)
    );
    assert_eq!(
        completed[0].get("issueTitle").and_then(Value::as_str),
        Some("Toast issue")
    );
    assert_eq!(
        completed[0].get("nodeName").and_then(Value::as_str),
        Some("builder")
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

    assert!(transition_to_warm_state(&orch, "run-quiet"));

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

    assert!(transition_to_warm_state(&orch, "run-dedupe"));
    finalize_run(&orch, "run-dedupe", RunStatus::Exited);

    assert_eq!(agent_attention_events(&emitter, "completed").len(), 1);
    assert_eq!(emitter.events_named("run-completed").len(), 1);
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
    assert!(transition_to_warm_state(&orch, "run-bcast"));

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
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-warm','w-warm','P','PRJ','/tmp/prj',1,1)", ()).await?;
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
    assert!(transition_to_warm_state(&orch, "run-child"));

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
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-warm','w-warm','P','PRJ','/tmp/prj',1,1)", ()).await?;
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
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-sj','w-sj','P','PRJ','/tmp/prj',1,1)", ()).await?;
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
    // The delegated child's live run is stopped.
    assert_ne!(
        scalar_text(&orch, "SELECT status FROM runs WHERE id = ?1", "r-sj-child")
            .await
            .as_deref(),
        Some("live"),
        "child run should no longer be live"
    );
}
