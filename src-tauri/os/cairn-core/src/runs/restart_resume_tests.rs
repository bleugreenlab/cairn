//! The boot path for jobs a host restart interrupted: the stale-run sweep
//! settles and reprojects them, and the post-readiness step resumes each once.
//!
//! Every fixture is seeded in the production row shape (an execution with a
//! recipe snapshot, a job carrying `execution_id` AND `recipe_node_id`, a
//! confirmed session, a `live` run predating boot and a `running` turn on it).
//! A fix whose fixture had `execution_id` NULL once passed here and missed every
//! real row.

use crate::agent_process::process::RunHandle;
use crate::db::DbState;
use crate::orchestrator::Orchestrator;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{migrated_test_db, LocalDb, SearchIndex};
use cairn_db::turso::params;
use std::sync::{Arc, Mutex};

/// This host's boot. Every seeded run starts long before it, so the sweep reads
/// each one as a predecessor's leftover. Rows a launch writes carry the wall
/// clock, far past it.
const BOOT_AT: i64 = 1_000_000;

/// The opening of the restart notice, as the agent reads it.
const RESTART_NOTE: &str = "Cairn restarted while your previous turn was in progress";
const ORPHAN_NOTE: &str = "Cairn restarted before your queued turn started";

fn test_orchestrator(db: LocalDb, recovery_pending: bool) -> Orchestrator {
    let root = tempfile::tempdir().unwrap().keep();
    let state = Arc::new(DbState::new(
        Arc::new(db),
        Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
    ));
    let builder = Orchestrator::builder(state, Arc::new(TestServicesBuilder::new().build()), root);
    if recovery_pending {
        builder.startup_recovery_pending().build()
    } else {
        builder.build()
    }
}

/// A provider that records the prompt and session start each launch carried and
/// registers a process for it, so a cold continuation completes without
/// spawning anything.
struct RecordingBackend {
    prompts: Arc<Mutex<Vec<String>>>,
    starts: Arc<Mutex<Vec<crate::backends::SessionStart>>>,
}

impl crate::backends::AgentBackend for RecordingBackend {
    fn name(&self) -> &str {
        "Claude"
    }
    fn is_available(&self) -> Result<(), String> {
        Ok(())
    }
    fn discover_models(
        &self,
    ) -> Result<Vec<cairn_models::types::catalog::DiscoveredModel>, String> {
        Ok(Vec::new())
    }
    fn serves(
        &self,
        _: &Orchestrator,
        _: Option<&str>,
    ) -> Vec<cairn_models::types::catalog::ServedVerb> {
        Vec::new()
    }
    fn resolve_tools(&self, _: &[String], _: &[String]) -> crate::backends::ResolvedTools {
        crate::backends::ResolvedTools {
            allowed: Vec::new(),
            disallowed: Vec::new(),
        }
    }
    fn supports_resume(&self) -> bool {
        true
    }
    fn supports_warm_processes(&self) -> bool {
        true
    }
    fn start_session(
        &self,
        config: crate::backends::SessionConfig,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<(), String> {
        self.prompts.lock().unwrap().push(config.prompt.clone());
        self.starts
            .lock()
            .unwrap()
            .push(config.session_start.clone());
        let session_id = config.session_start.session_id().to_string();
        orch.process_state
            .register_process(
                config.run_id.clone(),
                RunHandle::test_handle(Some(&session_id), None),
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

/// An orchestrator over a fresh database holding one project, with the
/// recording provider installed. Keep the returned guard alive for the test.
async fn host(
    name: &str,
) -> (
    Orchestrator,
    Arc<LocalDb>,
    Arc<Mutex<Vec<String>>>,
    crate::orchestrator::session::OrchestratorTestSessionBackend,
) {
    host_with(name, false).await
}

/// [`host`], optionally still inside startup recovery.
async fn host_with(
    name: &str,
    recovery_pending: bool,
) -> (
    Orchestrator,
    Arc<LocalDb>,
    Arc<Mutex<Vec<String>>>,
    crate::orchestrator::session::OrchestratorTestSessionBackend,
) {
    let (orch, db, prompts, _starts, backend) = host_recording(name, recovery_pending).await;
    (orch, db, prompts, backend)
}

/// [`host_with`], also handing back the session start each launch carried.
async fn host_recording(
    name: &str,
    recovery_pending: bool,
) -> (
    Orchestrator,
    Arc<LocalDb>,
    Arc<Mutex<Vec<String>>>,
    Arc<Mutex<Vec<crate::backends::SessionStart>>>,
    crate::orchestrator::session::OrchestratorTestSessionBackend,
) {
    let orch = test_orchestrator(migrated_test_db(name).await, recovery_pending);
    let db = orch.db.local.clone();
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);",
    )
    .await
    .unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let recorded = prompts.clone();
    let recorded_starts = starts.clone();
    let backend =
        crate::orchestrator::session::install_orchestrator_test_session_backend(&orch, move || {
            Box::new(RecordingBackend {
                prompts: recorded.clone(),
                starts: recorded_starts.clone(),
            })
        });
    (orch, db, prompts, starts, backend)
}

/// Builder job `job` on issue number `number` (in `issue_status`), its own
/// execution of a builder recipe, a confirmed Claude session `s-{job}`, and a
/// `live` run `r-{job}` that started before boot. Turns are seeded separately.
async fn seed_node_job(db: &LocalDb, job: &str, number: i64, issue_status: &str) {
    let snapshot = serde_json::json!({
        "recipe": {
            "id": "recipe", "name": "Recipe", "description": null,
            "trigger": "manual",
            "nodes": [
                {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
            ],
            "edges": []
        },
        "agents": {}, "skills": {},
        "triggerContext": {"issueId": format!("i-{job}"), "projectId": "p", "triggerType": "manual"},
        "delegatedPackets": [], "createdAt": 1
    })
    .to_string();
    db.execute(
        "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
         VALUES('i-' || ?1,'p',?2,'I',?3,1,1)",
        params![job, number, issue_status],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
         VALUES ('exec-' || ?1, 'recipe', 'i-' || ?1, 'p', 'running', 1, 1, ?2, 'manual')",
        params![job, snapshot],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs(id,execution_id,recipe_node_id,issue_id,project_id,status,node_name,uri_segment,current_session_id,model,created_at,updated_at)
         VALUES(?1,'exec-' || ?1,'builder','i-' || ?1,'p','running','Builder','builder','s-' || ?1,'opus',1,1)",
        params![job],
    )
    .await
    .unwrap();
    seed_session_and_run(db, job).await;
}

/// The session job `job` of thread `thread` (`active` or `closed`), on the
/// thread's host execution, with the same confirmed session and live run.
async fn seed_thread_session(db: &LocalDb, job: &str, thread: &str, thread_status: &str) {
    let snapshot = serde_json::json!({
        "recipe": {
            "id": "host-thread", "name": "Session Host", "description": null,
            "trigger": "manual", "nodes": [], "edges": []
        },
        "agents": {}, "skills": {},
        "triggerContext": {"issueId": null, "projectId": "p", "triggerType": "manual"},
        "delegatedPackets": [], "createdAt": 1
    })
    .to_string();
    db.execute(
        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
         VALUES (?1, 'p', ?1, ?2, 'none', 1, 1)",
        params![thread, thread_status],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot, triggered_by)
         VALUES ('exec-' || ?1, 'host-thread', 'p', 'running', 1, ?2, 'manual')",
        params![job, snapshot],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs(id,execution_id,thread_id,project_id,status,node_name,uri_segment,current_session_id,model,created_at,updated_at)
         VALUES(?1,'exec-' || ?1,?2,'p','running','Thread','thread','s-' || ?1,'opus',1,1)",
        params![job, thread],
    )
    .await
    .unwrap();
    seed_session_and_run(db, job).await;
}

async fn seed_session_and_run(db: &LocalDb, job: &str) {
    db.execute(
        "INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
         VALUES('s-' || ?1,?1,'claude','handle-' || ?1,'open',1,1,1)",
        params![job],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO runs(id,project_id,job_id,session_id,status,started_at,start_mode,created_at,updated_at)
         VALUES('r-' || ?1,'p',?1,'s-' || ?1,'live',2,'fresh',1,2)",
        params![job],
    )
    .await
    .unwrap();
}

/// A turn of `job` at position `seq` (also its creation second), made the job's
/// current turn. `run` NULL is a turn no run ever claimed.
#[allow(clippy::too_many_arguments)]
async fn seed_turn(
    db: &LocalDb,
    id: &str,
    job: &str,
    run: Option<&str>,
    seq: i64,
    state: &str,
    start_reason: &str,
    end_reason: Option<&str>,
) {
    db.execute(
        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,end_reason,created_at,started_at,ended_at,updated_at)
         VALUES(?1,'s-' || ?2,?3,?2,?4,?5,?6,?7,?4,
                CASE WHEN ?5 = 'pending' THEN NULL ELSE ?4 END,
                CASE WHEN ?5 IN ('pending','running') THEN NULL ELSE ?4 END,
                ?4)",
        params![id, job, run, seq, state, start_reason, end_reason],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
        params![id, job],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,parent_tool_use_id,created_at)
         VALUES('e-' || ?1,COALESCE(?2,'r-' || ?3),'s-' || ?3,?1,?4,?4,'user','{\"content\":\"build the feature\"}',NULL,?4)",
        params![id, run, job, seq],
    )
    .await
    .unwrap();
}

/// Run the boot sweep the way startup recovery does, off the async runtime.
async fn reconcile(orch: &Orchestrator) {
    let boot = orch.clone();
    tokio::task::spawn_blocking(move || {
        super::reconcile_stale_runs(&boot, boot.services.emitter.as_ref(), BOOT_AT)
    })
    .await
    .unwrap();
}

/// Run the post-readiness resume step.
async fn resume(orch: &Orchestrator) -> super::RestartResumeSummary {
    let orch = orch.clone();
    tokio::task::spawn_blocking(move || super::resume_jobs_interrupted_by_restart(&orch, BOOT_AT))
        .await
        .unwrap()
}

async fn text(db: &LocalDb, sql: &str, id: &str) -> Option<String> {
    db.query_opt_text(sql, (id.to_string(),)).await.unwrap()
}

async fn status(db: &LocalDb, job: &str) -> String {
    text(db, "SELECT status FROM jobs WHERE id = ?1", job)
        .await
        .expect("job exists")
}

async fn settlement(db: &LocalDb, turn: &str) -> String {
    text(
        db,
        "SELECT state || '/' || COALESCE(end_reason, '-') FROM turns WHERE id = ?1",
        turn,
    )
    .await
    .expect("turn exists")
}

/// Turns a launch on this host minted for `job` (seeded turns predate boot).
async fn launched_turns(db: &LocalDb, job: &str) -> Vec<(String, String)> {
    let job = job.to_string();
    db.read(move |conn| {
        let job = job.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, start_reason FROM turns WHERE job_id = ?1 AND created_at >= ?2",
                    params![job.as_str(), BOOT_AT],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.get::<String>(0)?, row.get::<String>(1)?));
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

async fn launched_runs(db: &LocalDb, job: &str) -> i64 {
    db.query_opt_i64(
        "SELECT COUNT(*) FROM runs WHERE job_id = ?1 AND created_at >= ?2",
        params![job, BOOT_AT],
    )
    .await
    .unwrap()
    .unwrap_or(0)
}

/// Every prompt a launch of `job` carried: what the provider received, and what
/// durable admission holds for a launch still queued.
async fn launch_prompts(db: &LocalDb, recorded: &Mutex<Vec<String>>, job: &str) -> String {
    let mut prompts = recorded.lock().unwrap().clone();
    let job = job.to_string();
    let prepared = db
        .read(move |conn| {
            let job = job.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT prompt FROM prepared_ordinary_launches WHERE job_id = ?1",
                        params![job.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row.get::<String>(0)?);
                }
                Ok(out)
            })
        })
        .await
        .unwrap();
    prompts.extend(prepared);
    prompts.join("\n---\n")
}

/// cairn/5143: boot settled the run and interrupted the turn, then left the job
/// reading `running` with nothing serving it, forever. Now the sweep reprojects
/// it to Pending and the resume step admits exactly one continuation, which the
/// agent opens on the restart notice. A second pass, or a DAG advance of the
/// same execution, launches nothing more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_interrupted_job_is_reprojected_and_resumed_once() {
    let (orch, db, prompts, _backend) = host("restart-resume-node").await;
    seed_node_job(&db, "j", 1, "active").await;
    seed_turn(
        &db,
        "turn-j",
        "j",
        Some("r-j"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    reconcile(&orch).await;

    assert_eq!(
        status(&db, "j").await,
        "pending",
        "a job whose run boot settled must not read running without a run"
    );
    assert_eq!(settlement(&db, "turn-j").await, "interrupted/host_restart");
    assert_eq!(
        text(&db, "SELECT status FROM runs WHERE id = ?1", "r-j")
            .await
            .as_deref(),
        Some("crashed")
    );
    assert!(
        launched_turns(&db, "j").await.is_empty(),
        "the sweep itself launches nothing"
    );

    let summary = resume(&orch).await;
    assert_eq!((summary.selected, summary.admitted), (1, 1), "{summary:?}");
    let turns = launched_turns(&db, "j").await;
    assert_eq!(turns.len(), 1, "exactly one continuation: {turns:?}");
    assert_eq!(turns[0].1, "follow_up");
    assert_eq!(
        text(
            &db,
            "SELECT predecessor_id FROM turns WHERE id = ?1",
            &turns[0].0
        )
        .await
        .as_deref(),
        Some("turn-j")
    );
    assert_eq!(launched_runs(&db, "j").await, 1);
    assert_eq!(status(&db, "j").await, "running");
    let prompt = launch_prompts(&db, &prompts, "j").await;
    assert!(
        prompt.contains(RESTART_NOTE),
        "the agent is told why: {prompt}"
    );
    assert!(
        prompt.contains("unknown outcome"),
        "and that its in-flight call may or may not have landed: {prompt}"
    );

    let again = resume(&orch).await;
    assert_eq!(
        again,
        super::RestartResumeSummary::default(),
        "resumed once"
    );
    assert_eq!(launched_turns(&db, "j").await.len(), 1);

    let claimed = crate::execution::advancement::advance_execution_with_actions(&orch, "exec-j")
        .await
        .unwrap();
    assert!(
        claimed.is_empty(),
        "the DAG must not claim a job the boot resume already launched: {claimed:?}"
    );
    assert_eq!(launched_runs(&db, "j").await, 1);
}

/// Wait for the provider to have received `count` prompts. Promotion after
/// readiness runs in the background.
async fn await_prompts(prompts: &Mutex<Vec<String>>, count: usize) {
    for _ in 0..500 {
        if prompts.lock().unwrap().len() >= count {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "the provider received {} prompt(s), expected {count}",
        prompts.lock().unwrap().len()
    );
}

/// cairn/4758 (a, b): before startup recovery is ready the boot resume starts
/// nothing and spends nothing, because the agent's callbacks would be refused.
/// Once readiness opens it resumes the job exactly once, and a further pass
/// resumes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_resume_waits_for_readiness_then_resumes_once() {
    let (orch, db, prompts, _backend) = host_with("restart-resume-readiness", true).await;
    seed_node_job(&db, "j", 1, "active").await;
    seed_turn(
        &db,
        "turn-j",
        "j",
        Some("r-j"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    reconcile(&orch).await;

    assert_eq!(resume(&orch).await, super::RestartResumeSummary::default());
    assert!(launched_turns(&db, "j").await.is_empty());
    assert_eq!(launched_runs(&db, "j").await, 0);
    assert_eq!(settlement(&db, "turn-j").await, "interrupted/host_restart");

    orch.mark_startup_recovery_ready();
    let summary = resume(&orch).await;
    assert_eq!((summary.selected, summary.admitted), (1, 1), "{summary:?}");
    await_prompts(&prompts, 1).await;
    assert_eq!(launched_runs(&db, "j").await, 1);

    assert_eq!(resume(&orch).await, super::RestartResumeSummary::default());
    assert_eq!(launched_runs(&db, "j").await, 1);
    assert_eq!(prompts.lock().unwrap().len(), 1);
}

/// cairn/4758 (c), the 10-30 specimen: a check-results nudge reached a
/// restart-interrupted job inside the recovery window. Its launch is held in
/// admission rather than handed to a process the gateway would refuse, starts
/// once readiness opens, and is the job's single automatic resume: the boot
/// resume then stands down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wake_inside_the_recovery_window_is_held_and_becomes_the_one_resume() {
    let (orch, db, prompts, _backend) = host_with("restart-resume-held-wake", true).await;
    seed_node_job(&db, "j", 1, "active").await;
    seed_turn(
        &db,
        "turn-j",
        "j",
        Some("r-j"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    reconcile(&orch).await;

    let woken = orch.clone();
    tokio::task::spawn_blocking(move || {
        crate::execution::jobs::continue_job_impl(
            &woken,
            "j",
            Some("check results landed"),
            None,
            None,
        )
    })
    .await
    .unwrap()
    .expect("the wake is accepted and held");

    // Held: a durable queued launch, its run starting and its turn pending, and
    // nothing handed to the provider.
    assert_eq!(launched_runs(&db, "j").await, 1);
    assert_eq!(
        db.query_opt_text(
            "SELECT r.state FROM runtime_admission_requests r JOIN prepared_ordinary_launches p ON p.request_id = r.request_id WHERE p.job_id = 'j'",
            (),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("queued")
    );
    let turns = launched_turns(&db, "j").await;
    assert_eq!(turns.len(), 1, "{turns:?}");
    assert_eq!(settlement(&db, &turns[0].0).await, "pending/-");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        prompts.lock().unwrap().is_empty(),
        "no turn may reach a process before readiness"
    );

    orch.mark_startup_recovery_ready();
    await_prompts(&prompts, 1).await;
    let summary = resume(&orch).await;
    assert_eq!(summary.admitted, 0, "{summary:?}");
    assert_eq!(launched_runs(&db, "j").await, 1);
    assert_eq!(launched_turns(&db, "j").await.len(), 1);
    assert_eq!(prompts.lock().unwrap().len(), 1);
}

/// A cell held by job `job`'s residency, with no process in it yet.
fn terminal_cell(job: &str) -> cairn_common::executor_protocol::PersistentCellState {
    use cairn_common::executor_protocol::*;
    PersistentCellState {
        observed_head: None,
        size_bytes: 0,
        warm_command_classes: Vec::new(),
        executor_id: String::new(),
        executor_display_name: None,
        project_id: "p".into(),
        cell_id: "cell".into(),
        path: "/cell".into(),
        workspace_name: "cell".into(),
        repository: "/tmp/p".into(),
        checkout_kind: CellCheckoutKind::ExistingCheckout,
        git_common_dir: None,
        authority_path: "/cell/.authority".into(),
        lifecycle: PersistentCellLifecycle::Active,
        cell_epoch: 7,
        last_sealed_commit: Some("base".into()),
        last_distance: None,
        quarantine_reason: None,
        last_used_unix_ms: 1,
        last_affinity_key: None,
        preparation_fingerprint: None,
        residency: Some(CellResidency {
            holder: ResidencyHolder::Job { job_id: job.into() },
            repository: RepositoryLocator::ExistingCheckout {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: "/tmp/p".into(),
            },
            owner_ref: None,
            selector: None,
            incarnation_id: "incarnation".into(),
            current_base_commit: "base".into(),
            phase: ResidencyPhase::Active,
            last_heartbeat_unix_ms: 1,
            reclaim_deadline_unix_ms: 0,
            death_policy: OwnerDeathPolicy {
                heartbeat_timeout_ms: 60_000,
                reclaim_grace_ms: 60_000,
            },
            footprint: ResidencyFootprint {
                memory_bytes: 1,
                disk_growth_bytes: 1,
            },
            state_revision: 1,
            events: Vec::new(),
        }),
        occupancy: CellOccupancy::default(),
        seeded_from: None,
    }
}

/// Attach a colocated executor reporting `cell`, and answer every residency
/// operation that reaches its link the way an executor does: a start puts a
/// terminal process named `slug` in the cell, and a stop reports the cell and
/// then the process's exit. Each operation is logged by name, a start or stop
/// as `StartProcess:<process key>` / `StopProcess:<process key>`.
fn attach_answering_executor(
    orch: &Orchestrator,
    mut cell: cairn_common::executor_protocol::PersistentCellState,
    slug: &str,
) -> Arc<Mutex<Vec<String>>> {
    use cairn_common::executor_protocol::*;
    let fleet = orch.fleet.clone();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let generation = fleet.attach_executor(sender);
    assert!(fleet.set_executor_snapshot(
        crate::fleet::COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            cells: vec![cell.clone()],
            ..Default::default()
        },
        ExecutorSubstrateReport::default(),
    ));
    let operations = Arc::new(Mutex::new(Vec::new()));
    let log = operations.clone();
    let slug = slug.to_string();
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            let ExecutorMessage::ResidencyRequest {
                correlation_id,
                operation,
                ..
            } = message
            else {
                continue;
            };
            let stopped = match &operation {
                ResidencyOperation::StartProcess { process_key, .. } => {
                    log.lock()
                        .unwrap()
                        .push(format!("StartProcess:{process_key}"));
                    cell.occupancy.processes.insert(
                        process_key.clone(),
                        ResidentProcess {
                            generation: 1,
                            kind: ResidentProcessKind::Terminal { slug: slug.clone() },
                            spec: None,
                            status: ResidentProcessStatus::Starting,
                            reservation: None,
                            live_usage: None,
                            charge: None,
                        },
                    );
                    assert!(fleet.set_executor_snapshot(
                        crate::fleet::COLOCATED_EXECUTOR_ID,
                        generation,
                        FleetSnapshot {
                            cells: vec![cell.clone()],
                            ..Default::default()
                        },
                        ExecutorSubstrateReport::default(),
                    ));
                    None
                }
                ResidencyOperation::StopProcess { fence, process_key } => {
                    log.lock()
                        .unwrap()
                        .push(format!("StopProcess:{process_key}"));
                    Some((fence.clone(), process_key.clone()))
                }
                other => {
                    let name = format!("{other:?}");
                    log.lock()
                        .unwrap()
                        .push(name.split([' ', '{']).next().unwrap_or_default().into());
                    None
                }
            };
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::ResidencyResponse {
                    correlation_id,
                    result: ResidencyResult::State { cell: cell.clone() },
                },
            );
            if let Some((fence, process_key)) = stopped {
                fleet.handle_executor_message(
                    crate::fleet::COLOCATED_EXECUTOR_ID,
                    generation,
                    ExecutorMessage::ResidentProcessEvent {
                        event: ResidentProcessEvent {
                            holder: fence.holder,
                            incarnation_id: fence.incarnation_id,
                            cell_epoch: fence.cell_epoch,
                            process_key,
                            process_generation: 1,
                            event: ResidentProcessEventKind::State {
                                status: ResidentProcessStatus::Exited {
                                    finished_at_unix_ms: 2,
                                    exit_code: Some(143),
                                    restartable: true,
                                    executor_lost: false,
                                },
                            },
                        },
                    },
                );
            }
        }
    });
    operations
}

/// cairn/6440: retiring an idle warm process under pipe pressure is a release,
/// not a stop. The job's dev-server terminal, bound to a live process on an
/// executor, receives no stop and keeps running; the next message is delivered
/// through the ordinary continuation and resumes the same persisted session.
/// An operator Stop of the same job is the control: it does send the
/// terminal's process a stop, and the terminal ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pipe_pressure_release_keeps_the_bound_terminal_and_the_next_message_resumes_the_session()
{
    use crate::orchestrator::lifecycle::{
        kill_session_with_reason, retire_unowned_idle, RetireCause, RetireGrounds, TerminalPolicy,
    };
    let (mut orch, db, prompts, starts, _backend) =
        host_recording("pipe-release-composed", false).await;
    orch.warm_gc = Some(Arc::new(crate::agent_process::gc::WarmProcessGC::new(6)));
    seed_node_job(&db, "j", 1, "active").await;
    seed_turn(
        &db,
        "turn-j",
        "j",
        Some("r-j"),
        1,
        "complete",
        "initial",
        None,
    )
    .await;

    // The dev server, started through the terminal spawn path on an executor
    // holding the job's cell: a live session whose lease names that cell's
    // process, and the exit handler that settles its row.
    let operations = attach_answering_executor(&orch, terminal_cell("j"), "dev");
    let terminal = crate::mcp::handlers::terminal::spawn_job_terminal_for_test(
        &orch,
        cairn_uri::CairnResource::NodeTerminal {
            project: "p".into(),
            number: 1,
            exec_seq: 1,
            node_id: "builder".into(),
            slug: "dev".into(),
        },
        "j",
        "bun dev",
        cairn_common::executor_protocol::ResidencyFence {
            holder: cairn_common::executor_protocol::ResidencyHolder::Job { job_id: "j".into() },
            incarnation_id: "incarnation".into(),
            cell_epoch: 7,
        },
    )
    .await
    .expect("the dev server starts");
    assert_eq!(
        *operations.lock().unwrap(),
        [format!("StartProcess:{terminal}")]
    );
    let terminal_status = || {
        text(
            &db,
            "SELECT status FROM job_terminals WHERE session_id = ?1",
            &terminal,
        )
    };
    assert_eq!(terminal_status().await.as_deref(), Some("running"));
    let stops = || {
        operations
            .lock()
            .unwrap()
            .iter()
            .filter(|operation| operation.starts_with("StopProcess"))
            .cloned()
            .collect::<Vec<_>>()
    };

    orch.process_state
        .register_process("r-j".into(), RunHandle::test_handle(Some("s-j"), Some("j")))
        .unwrap();
    orch.process_state.transition_to_warm("r-j");

    let releasing = orch.clone();
    let retired = tokio::task::spawn_blocking(move || {
        retire_unowned_idle(&releasing, "r-j", RetireGrounds::PipePressure)
    })
    .await
    .unwrap()
    .expect("idle, unowned and unprotected");
    assert_eq!(retired.cause, RetireCause::PipePressure);
    assert!(!orch.process_state.has_process("r-j").unwrap_or(false));
    assert_eq!(
        text(&db, "SELECT exit_reason FROM runs WHERE id = ?1", "r-j")
            .await
            .as_deref(),
        Some("pipe_pressure_release")
    );

    let woken = orch.clone();
    tokio::task::spawn_blocking(move || {
        crate::execution::jobs::continue_job_impl(&woken, "j", Some("next message"), None, None)
    })
    .await
    .unwrap()
    .expect("the message is delivered");
    await_prompts(&prompts, 1).await;
    let prompt = launch_prompts(&db, &prompts, "j").await;
    assert!(prompt.contains("next message"), "{prompt}");
    assert_eq!(
        *starts.lock().unwrap(),
        [crate::backends::SessionStart::Resume {
            session_id: "s-j".into(),
            backend_id: "handle-j".into(),
        }],
        "the delivery resumes the persisted session natively"
    );
    assert_eq!(
        text(&db, "SELECT status FROM sessions WHERE id = ?1", "s-j")
            .await
            .as_deref(),
        Some("open")
    );
    assert_eq!(
        text(
            &db,
            "SELECT current_session_id FROM jobs WHERE id = ?1",
            "j"
        )
        .await
        .as_deref(),
        Some("s-j")
    );

    // Nothing the release or the delivery did reached the terminal's process.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(stops().is_empty(), "{:?}", operations.lock().unwrap());
    assert_eq!(
        terminal_status().await.as_deref(),
        Some("running"),
        "the dev server survives the release"
    );
    assert!(orch
        .pty_state
        .sessions
        .lock()
        .unwrap()
        .contains_key(&terminal));

    // The control: an operator Stop of the resumed run ends the job's work.
    let resumed = text(
        &db,
        "SELECT id FROM runs WHERE job_id = ?1 AND id != 'r-j'",
        "j",
    )
    .await
    .expect("the delivery launched a run");
    let stopping = orch.clone();
    tokio::task::spawn_blocking(move || {
        kill_session_with_reason(&stopping, &resumed, "user_stop", TerminalPolicy::Finalize)
    })
    .await
    .unwrap()
    .unwrap();
    // The Stop's own stop goes out first; the exit it produces settles the
    // terminal, whose handler then releases the exited process with another.
    let expected = format!("StopProcess:{terminal}");
    let sent = stops();
    assert!(
        !sent.is_empty() && sent.iter().all(|stop| *stop == expected),
        "{sent:?}"
    );
    for _ in 0..500 {
        if terminal_status().await.as_deref() != Some("running") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        terminal_status().await.as_deref(),
        Some("exited"),
        "the operator's Stop ends the terminal"
    );
}

/// A `run` call held open on its socket past grace (cairn/4887), as a host
/// leaves it when it dies: the turn still `running`, and a pending
/// `hold='inline'` run-batch row bound to the call.
async fn seed_held_run_call(db: &LocalDb, job: &str, turn: &str) {
    let condition = serde_json::json!({
        "kind": "run_batch",
        "request_id": "req-held",
        "commits": true,
        "label": "cargo test",
        "host": false
    })
    .to_string();
    db.execute(
        "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,kind,state,created_at,hold)
         VALUES('wait-' || ?1, ?1, 'r-' || ?1, 's-' || ?1, ?2, 'toolu-held', ?3, NULL, 'run_batch', 'pending', 1, 'inline')",
        params![job, turn, condition],
    )
    .await
    .unwrap();
}

async fn held_tool_results(db: &LocalDb) -> Vec<String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data FROM events WHERE event_type = 'tool_result' AND data LIKE '%toolu-held%'",
                    (),
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(row.get::<String>(0)?);
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

/// The host died while a `run` call was held open on its socket, and the
/// batch's machine may still be running it. Boot answers nothing: the held
/// row becomes the park it would have been, and the stale-run sweep settles
/// the turn that way, so cairn/5143's boot resume never selects it. Exactly
/// one thing resumes the turn: the batch's own result when it reaches the
/// row (cairn/5536). Both orders of the two sweeps hold that.
async fn a_held_call_resumes_once_after_restart(name: &str, stale_sweep_first: bool) {
    let (orch, db, _prompts, _backend) = host(name).await;
    seed_node_job(&db, "jh", 1, "active").await;
    seed_turn(
        &db,
        "turn-jh",
        "jh",
        Some("r-jh"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_held_run_call(&db, "jh", "turn-jh").await;

    if stale_sweep_first {
        reconcile(&orch).await;
        assert_eq!(
            settlement(&db, "turn-jh").await,
            "yielded/-",
            "a turn holding a call open settles as the park it becomes"
        );
        crate::mcp::handlers::durable_suspend::reconcile(&orch).await;
    } else {
        crate::mcp::handlers::durable_suspend::reconcile(&orch).await;
        reconcile(&orch).await;
    }

    assert_eq!(
        text(
            &db,
            "SELECT state FROM agent_waits WHERE id = ?1",
            "wait-jh"
        )
        .await
        .as_deref(),
        Some("pending"),
        "boot answers nothing for a batch its machine may still be running"
    );
    assert_eq!(
        text(
            &db,
            "SELECT COALESCE(hold, 'none') FROM agent_waits WHERE id = ?1",
            "wait-jh"
        )
        .await
        .as_deref(),
        Some("none"),
        "the held call became a park"
    );
    assert_eq!(settlement(&db, "turn-jh").await, "yielded/-");
    assert!(held_tool_results(&db).await.is_empty());

    let summary = resume(&orch).await;
    assert_eq!(
        summary.selected, 0,
        "the boot resume must not resume a turn its wait will resume: {summary:?}"
    );

    crate::mcp::handlers::run::adopted::deliver_batch_result(
        &orch,
        &db,
        "req-held",
        "succeeded",
        "the held batch's real result",
    )
    .await;
    let mut resolved = None;
    for _ in 0..200 {
        resolved = text(
            &db,
            "SELECT state FROM agent_waits WHERE id = ?1",
            "wait-jh",
        )
        .await;
        if resolved.as_deref() == Some("resolved") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(resolved.as_deref(), Some("resolved"));
    let results = held_tool_results(&db).await;
    assert_eq!(
        results.len(),
        1,
        "one answer for the held call: {results:?}"
    );
    assert!(
        results[0].contains("the held batch's real result"),
        "{}",
        results[0]
    );
    let successors: i64 = db
        .query_opt_i64(
            "SELECT COUNT(*) FROM turns WHERE predecessor_id = ?1",
            params!["turn-jh"],
        )
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(successors, 1, "exactly one successor");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_call_resumes_once_after_restart_in_boot_order() {
    a_held_call_resumes_once_after_restart("restart-held-boot-order", true).await;
}

/// An assistant event of `turn` on `job`'s run at `sequence`, making `tools`.
async fn seed_calls(db: &LocalDb, job: &str, turn: &str, sequence: i64, tools: serde_json::Value) {
    db.execute(
        "INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,parent_tool_use_id,created_at)
         VALUES('e-calls-' || ?1 || ?3,'r-' || ?1,'s-' || ?1,?2,?3,?3,'assistant',?4,NULL,?3)",
        params![
            job,
            turn,
            sequence,
            serde_json::json!({
                "eventType": "assistant",
                "sessionId": format!("s-{job}"),
                "isError": false,
                "toolUses": tools,
            })
            .to_string()
        ],
    )
    .await
    .unwrap();
}

async fn seed_ledger_row(db: &LocalDb, job: &str, tool_use_id: &str, report: &str) {
    db.execute(
        "INSERT INTO write_replay_ledger(run_id,tool_use_id,report,created_at)
         VALUES('r-' || ?1,?2,?3,1)",
        params![job, tool_use_id, report],
    )
    .await
    .unwrap();
}

/// Every recorded result for one call, as the parsed transcript events.
async fn results_for(db: &LocalDb, tool_use_id: &str) -> Vec<serde_json::Value> {
    let tool_use_id = tool_use_id.to_string();
    db.read(move |conn| {
        let tool_use_id = tool_use_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data FROM events
                     WHERE event_type = 'tool_result'
                       AND json_extract(data, '$.toolUseId') = ?1",
                    params![tool_use_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(serde_json::from_str(&row.get::<String>(0)?).unwrap());
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

/// The restart cut a `run` call off between its `tool_use` and its result. The
/// sweep answers it on the interrupted turn, and the continuation, a fresh turn
/// on a fresh run, still opens owing nothing, with the answer in its prompt. How
/// the provider's native resume presents its own copy of the call is only
/// observable on a real rebuild.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_answers_the_cut_off_call_and_the_resume_owes_none() {
    let (orch, db, prompts, _backend) = host("restart-resume-tool-call").await;
    seed_node_job(&db, "jc", 1, "active").await;
    seed_turn(
        &db,
        "turn-jc",
        "jc",
        Some("r-jc"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    db.execute(
        "INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,parent_tool_use_id,created_at)
         VALUES('e-tool','r-jc','s-jc','turn-jc',2,2,'assistant',?1,NULL,2)",
        params![serde_json::json!({
            "eventType": "assistant",
            "sessionId": "s-jc",
            "isError": false,
            "toolUses": [{"id": "toolu_cut", "name": "mcp__cairn__run", "input": {"command": "cargo build"}}]
        })
        .to_string()],
    )
    .await
    .unwrap();
    let owed = |run: String, turn: String| {
        let orch = orch.clone();
        async move {
            tokio::task::spawn_blocking(move || {
                crate::orchestrator::lifecycle::pending_tool_result_ids(&orch, &run, &turn)
            })
            .await
            .unwrap()
            .unwrap()
        }
    };
    assert_eq!(
        owed("r-jc".into(), "turn-jc".into()).await,
        vec!["toolu_cut".to_string()],
        "the seeded call is one a turn would owe a result for"
    );

    reconcile(&orch).await;
    let summary = resume(&orch).await;
    assert_eq!(summary.admitted, 1, "{summary:?}");

    let turns = launched_turns(&db, "jc").await;
    assert_eq!(turns.len(), 1, "{turns:?}");
    let run = text(
        &db,
        "SELECT id FROM runs WHERE job_id = ?1 AND id != 'r-jc'",
        "jc",
    )
    .await
    .expect("the continuation has its own run");
    assert!(
        owed(run, turns[0].0.clone()).await.is_empty(),
        "the continuation opens owing no tool result"
    );
    assert!(
        owed("r-jc".into(), "turn-jc".into()).await.is_empty(),
        "the interrupted turn owes nothing once the sweep answered it"
    );
    let results = results_for(&db, "toolu_cut").await;
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0]["raw"]["reason"], "host_restart");
    let lost = crate::mcp::handlers::run::run_batch_lost_to_restart_text(false);
    assert_eq!(results[0]["toolResult"], lost.as_str());
    let prompt = launch_prompts(&db, &prompts, "jc").await;
    assert!(prompt.contains(&lost), "{prompt}");
    assert!(prompt.contains("`run` on `cargo build`"), "{prompt}");
}

/// cairn/5000's specimen: a `cairn:~/rebase` replay was in flight when the host
/// was rebuilt, and nothing ever answered it. It resolves once to a failure that
/// says what to re-read, the ledger agrees, the job is not left running, and the
/// agent hears the same words when its one resume opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebase_replay_write_cut_off_by_a_restart_resolves_once_and_the_session_continues() {
    let (orch, db, prompts, _backend) = host("restart-rebase-replay-write").await;
    seed_node_job(&db, "jw", 1, "active").await;
    seed_turn(
        &db,
        "turn-jw",
        "jw",
        Some("r-jw"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_calls(
        &db,
        "jw",
        "turn-jw",
        2,
        serde_json::json!([{
            "id": "toolu_replay",
            "name": "mcp__cairn__write",
            "input": {"changes": [{
                "target": "cairn:~/rebase",
                "mode": "patch",
                "payload": {"action": "replay", "resolution": "take-committed-tip"}
            }]}
        }]),
    )
    .await;
    seed_ledger_row(
        &db,
        "jw",
        "toolu_replay",
        crate::mcp::handlers::write::replay::RESOURCE_PENDING,
    )
    .await;

    reconcile(&orch).await;

    assert_eq!(status(&db, "jw").await, "pending");
    let results = results_for(&db, "toolu_replay").await;
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0]["isError"], true);
    assert_eq!(results[0]["raw"]["reason"], "host_restart");
    assert_eq!(results[0]["raw"]["effect"], "partial");
    let answer = results[0]["toolResult"].as_str().unwrap();
    assert!(answer.contains("re-read `cairn:~/rebase`"), "{answer}");
    assert!(answer.contains("requesting the replay again"), "{answer}");
    assert!(
        !answer.to_lowercase().contains("nothing was committed"),
        "{answer}"
    );
    let row = text(
        &db,
        "SELECT report FROM write_replay_ledger WHERE tool_use_id = ?1",
        "toolu_replay",
    )
    .await
    .unwrap();
    assert!(
        !crate::mcp::handlers::write::replay::is_pending(&row),
        "the ledger settles with the transcript: {row}"
    );

    let summary = resume(&orch).await;
    assert_eq!(summary.admitted, 1, "{summary:?}");
    let prompt = launch_prompts(&db, &prompts, "jw").await;
    assert!(prompt.contains(RESTART_NOTE), "{prompt}");
    assert!(prompt.contains("`write` on `cairn:~/rebase`"), "{prompt}");
    assert!(prompt.contains(answer), "{prompt}");

    // A later pass finds the call answered and the job already resumed.
    reconcile(&orch).await;
    assert_eq!(resume(&orch).await, super::RestartResumeSummary::default());
    assert_eq!(results_for(&db, "toolu_replay").await.len(), 1);
    assert_eq!(launched_turns(&db, "jw").await.len(), 1);
}

/// What a restart says about each kind of call, from the only evidence it has.
#[test]
fn each_kind_of_call_is_answered_with_what_the_restart_can_know() {
    use crate::mcp::handlers::run::run_batch_lost_to_restart_text;
    use crate::orchestrator::lifecycle::{restart_answer, CallEffect, LedgerRow};
    use serde_json::json;

    let opening = "Cairn restarted while this call was in flight, so its result was lost.";
    let resource_write = json!({"changes": [
        {"target": "cairn:~/todos", "mode": "patch", "payload": {}},
        {"target": "cairn:~/plan", "mode": "patch", "payload": {}},
    ]});
    let file_write = json!({
        "changes": [{"target": "file:src/lib.rs", "mode": "patch", "payload": {}}],
        "commit_msg": "edit",
    });
    let row = |report: &str| LedgerRow {
        report: report.to_string(),
        finalized: true,
    };
    let write = "mcp__cairn__write";
    let none = serde_json::Value::Null;

    let read = restart_answer(
        "mcp__cairn__read",
        &json!({"paths": ["file:a"]}),
        None,
        false,
    );
    assert_eq!(read.effect, CallEffect::None);
    assert!(
        read.text.contains("A read changes nothing"),
        "{}",
        read.text
    );

    let never_started = restart_answer(write, &resource_write, None, false);
    assert_eq!(never_started.effect, CallEffect::None);
    assert!(never_started.text.contains("None of its items ran"));

    // Byte-identical open writes cannot name their own call, so they claim no
    // ledger row: the absence of one proves nothing.
    let twin = restart_answer(write, &resource_write, None, true);
    assert_eq!(twin.effect, CallEffect::Unknown);
    assert!(
        !twin.text.contains("None of its items ran"),
        "{}",
        twin.text
    );

    let checkpoint = format!(
        "{}\n{}",
        crate::mcp::handlers::write::replay::RESOURCE_PENDING,
        json!({"applied": [{"index": 0, "target": "cairn:~/todos", "mode": "patch"}]}),
    );
    let pending = restart_answer(write, &resource_write, Some(&row(&checkpoint)), false);
    assert_eq!(pending.effect, CallEffect::Partial);
    let sealed = pending
        .seal_report
        .as_deref()
        .expect("a pending row is sealed");
    assert!(pending.text.ends_with(sealed), "{}", pending.text);
    let report: serde_json::Value = serde_json::from_str(sealed).unwrap();
    assert_eq!(report["applied"][0]["target"], "cairn:~/todos");
    assert_eq!(report["failures"].as_array().unwrap().len(), 1, "{report}");
    assert_eq!(report["failures"][0]["target"], "cairn:~/plan");
    let phrase = report["failures"][0]["error"].as_str().unwrap();
    assert!(phrase.starts_with("not confirmed"), "{phrase}");
    assert!(phrase.contains("`cairn:~/plan`"), "{phrase}");

    let recorded = r#"{"applied":[{"index":0}],"commit":{"status":"committed"}}"#;
    let landed = restart_answer(write, &file_write, Some(&row(recorded)), false);
    assert_eq!(landed.effect, CallEffect::Landed);
    assert!(landed.text.contains(recorded), "{}", landed.text);
    assert!(landed.text.contains("Do not retry"), "{}", landed.text);
    assert!(landed.seal_report.is_none());

    let stopped = json!({"applied": [], "failures": [
        {"index": 0, "kind": crate::mcp::handlers::write::replay::STOPPED_KIND, "error": "not applied"}
    ]})
    .to_string();
    let sealed_by_stop = restart_answer(write, &resource_write, Some(&row(&stopped)), false);
    assert_eq!(sealed_by_stop.effect, CallEffect::Partial);
    assert!(!sealed_by_stop.text.contains("Do not retry"));

    let post_seal = LedgerRow {
        report: recorded.to_string(),
        finalized: false,
    };
    let unfinished = restart_answer(write, &file_write, Some(&post_seal), false);
    assert_eq!(unfinished.effect, CallEffect::Partial);
    assert!(
        unfinished.text.contains("pull request"),
        "{}",
        unfinished.text
    );

    let maybe_committed = restart_answer(write, &file_write, None, false);
    assert_eq!(maybe_committed.effect, CallEffect::Unknown);
    assert!(maybe_committed.text.contains("cairn:@/vcs/log"));
    assert!(!maybe_committed
        .text
        .to_lowercase()
        .contains("nothing was committed"));

    let commit_run = restart_answer(
        "mcp__cairn__run",
        &json!({"commands": [{"command": "x"}], "commit_msg": "m"}),
        None,
        false,
    );
    assert_eq!(commit_run.effect, CallEffect::Unknown);
    assert_eq!(commit_run.text, run_batch_lost_to_restart_text(true));
    let plain_run = restart_answer("run", &json!({"commands": []}), None, false);
    assert_eq!(plain_run.effect, CallEffect::None);
    assert_eq!(plain_run.text, run_batch_lost_to_restart_text(false));

    // A provider's own shell has no `run` semantics: nothing restores its tree.
    for name in ["Bash", "Task", "mcp__other__thing"] {
        let other = restart_answer(name, &none, None, false);
        assert_eq!(other.effect, CallEffect::Unknown, "{name}");
        assert!(other.text.contains("outcome is unknown"), "{}", other.text);
    }

    for answer in [
        &read,
        &never_started,
        &twin,
        &pending,
        &landed,
        &sealed_by_stop,
        &unfinished,
        &maybe_committed,
    ] {
        assert!(answer.text.starts_with(opening), "{}", answer.text);
        crate::system_prompt::assert_no_substrate_vocabulary("restart answer", &answer.text);
    }
}

/// A write whose report was recorded took effect, so its answer carries that
/// report and does not read as a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_that_landed_before_the_restart_is_answered_with_its_report() {
    let (orch, db, _prompts, _backend) = host("restart-landed-write").await;
    seed_node_job(&db, "jl", 1, "active").await;
    seed_turn(
        &db,
        "turn-jl",
        "jl",
        Some("r-jl"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_calls(
        &db,
        "jl",
        "turn-jl",
        2,
        serde_json::json!([{
            "id": "toolu_landed",
            "name": "mcp__cairn__write",
            "input": {"changes": [{"target": "file:a.rs", "mode": "patch", "payload": {}}], "commit_msg": "m"}
        }]),
    )
    .await;
    let report = r#"{"applied":[{"index":0}],"commit":{"status":"committed","sha":"abc"}}"#;
    seed_ledger_row(&db, "jl", "toolu_landed", report).await;

    reconcile(&orch).await;

    let results = results_for(&db, "toolu_landed").await;
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0]["isError"], false);
    assert_eq!(results[0]["raw"]["effect"], "landed");
    assert!(results[0]["toolResult"].as_str().unwrap().contains(report));
    assert_eq!(
        text(
            &db,
            "SELECT report FROM write_replay_ledger WHERE tool_use_id = ?1",
            "toolu_landed"
        )
        .await
        .as_deref(),
        Some(report),
        "a finished row is final"
    );
}

/// One owner per call: an active wait row keeps its call, and the settlement
/// answers the turn's other open call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_an_active_wait_owns_is_left_to_that_wait() {
    let (orch, db, _prompts, _backend) = host("restart-owned-call").await;
    seed_node_job(&db, "jo", 1, "active").await;
    seed_turn(
        &db,
        "turn-jo",
        "jo",
        Some("r-jo"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_calls(
        &db,
        "jo",
        "turn-jo",
        2,
        serde_json::json!([
            {"id": "toolu_owned", "name": "mcp__cairn__run", "input": {"commands": [{"waitFor": {"duration": "3m"}}]}},
            {"id": "toolu_free", "name": "mcp__cairn__read", "input": {"paths": ["file:a.rs"]}},
        ]),
    )
    .await;
    db.execute(
        "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,state,created_at)
         VALUES('wait-jo','jo','r-jo','s-jo','turn-jo','toolu_owned','{\"kind\":\"duration\"}',NULL,'pending',1)",
        (),
    )
    .await
    .unwrap();

    reconcile(&orch).await;

    assert!(results_for(&db, "toolu_owned").await.is_empty());
    let free = results_for(&db, "toolu_free").await;
    assert_eq!(free.len(), 1, "{free:?}");
    assert_eq!(free[0]["raw"]["effect"], "none");
}

/// A turn holding a `run` call open settles as a park, not as interrupted, but a
/// sibling call it had in flight has no wait to answer it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sibling_of_a_held_call_is_answered_and_the_held_call_stays_its_waits() {
    let (orch, db, _prompts, _backend) = host("restart-held-sibling").await;
    seed_node_job(&db, "js", 1, "active").await;
    seed_turn(
        &db,
        "turn-js",
        "js",
        Some("r-js"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_held_run_call(&db, "js", "turn-js").await;
    seed_calls(
        &db,
        "js",
        "turn-js",
        2,
        serde_json::json!([
            {"id": "toolu-held", "name": "mcp__cairn__run", "input": {"commands": [{"command": "cargo test"}]}},
            {"id": "toolu_sibling", "name": "mcp__cairn__read", "input": {"paths": ["file:a.rs"]}},
        ]),
    )
    .await;

    reconcile(&orch).await;

    assert_eq!(settlement(&db, "turn-js").await, "yielded/-");
    assert!(results_for(&db, "toolu-held").await.is_empty());
    assert_eq!(results_for(&db, "toolu_sibling").await.len(), 1);
}

/// Not every recorded result carries its turn. A real answer must not be buried
/// under a false one because it did.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_answered_by_a_result_with_no_turn_is_not_answered_again() {
    let (orch, db, prompts, _backend) = host("restart-turnless-result").await;
    seed_node_job(&db, "jn", 1, "active").await;
    seed_turn(
        &db,
        "turn-jn",
        "jn",
        Some("r-jn"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_calls(
        &db,
        "jn",
        "turn-jn",
        2,
        serde_json::json!([{"id": "toolu_done", "name": "mcp__cairn__read", "input": {"paths": ["file:a.rs"]}}]),
    )
    .await;
    db.execute(
        "INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,parent_tool_use_id,created_at)
         VALUES('e-done','r-jn','s-jn',NULL,3,3,'tool_result',?1,NULL,3)",
        params![serde_json::json!({
            "eventType": "tool_result",
            "sessionId": "s-jn",
            "isError": false,
            "toolUseId": "toolu_done",
            "toolResult": "the real answer",
        })
        .to_string()],
    )
    .await
    .unwrap();

    reconcile(&orch).await;

    let results = results_for(&db, "toolu_done").await;
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0]["toolResult"], "the real answer");

    // With nothing to name, the note is the plain restart notice.
    assert_eq!(resume(&orch).await.admitted, 1);
    let prompt = launch_prompts(&db, &prompts, "jn").await;
    assert!(prompt.contains(RESTART_NOTE), "{prompt}");
    assert!(
        !prompt.contains("The calls that were in flight"),
        "{prompt}"
    );
}

/// A standing thread's session job derives Idle rather than Pending, and nothing
/// else wakes a thread whose turn the restart killed, so it is resumed too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_interrupted_thread_session_is_resumed_once() {
    let (orch, db, prompts, _backend) = host("restart-resume-thread").await;
    seed_thread_session(&db, "jt", "th", "active").await;
    seed_turn(
        &db,
        "turn-jt",
        "jt",
        Some("r-jt"),
        1,
        "running",
        "initial",
        None,
    )
    .await;

    reconcile(&orch).await;
    assert_eq!(status(&db, "jt").await, "idle");
    assert_eq!(settlement(&db, "turn-jt").await, "interrupted/host_restart");

    let summary = resume(&orch).await;
    assert_eq!((summary.selected, summary.admitted), (1, 1), "{summary:?}");
    assert_eq!(launched_turns(&db, "jt").await.len(), 1);
    assert!(launch_prompts(&db, &prompts, "jt")
        .await
        .contains(RESTART_NOTE));

    assert_eq!(resume(&orch).await, super::RestartResumeSummary::default());
    assert_eq!(launched_turns(&db, "jt").await.len(), 1);
}

/// Tenet 822: a pending turn that runner death orphaned before any run claimed
/// it is resumed once at boot, by the same step, and its notice says nothing
/// ran rather than warning about an in-flight call. The two settlements are
/// disjoint by construction (one takes runless turns, the other turns bound to
/// a stale run), so a boot with both resumes each job exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphaned_pending_turn_is_resumed_once_after_readiness() {
    let (orch, db, prompts, _backend) = host("restart-resume-orphan").await;
    seed_node_job(&db, "ja", 1, "active").await;
    seed_turn(
        &db,
        "turn-ja",
        "ja",
        Some("r-ja"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_node_job(&db, "jo", 2, "active").await;
    seed_turn(
        &db,
        "turn-jo0",
        "jo",
        Some("r-jo"),
        1,
        "complete",
        "initial",
        None,
    )
    .await;
    seed_turn(&db, "turn-jo", "jo", None, 2, "pending", "follow_up", None).await;

    reconcile(&orch).await;
    assert_eq!(settlement(&db, "turn-ja").await, "interrupted/host_restart");
    assert_eq!(
        settlement(&db, "turn-jo").await,
        "cancelled/orphaned_pending"
    );
    assert_eq!(status(&db, "jo").await, "pending");

    let summary = resume(&orch).await;
    assert_eq!((summary.selected, summary.admitted), (2, 2), "{summary:?}");
    assert_eq!(launched_turns(&db, "ja").await.len(), 1);
    assert_eq!(launched_turns(&db, "jo").await.len(), 1);
    let orphan_prompt = launch_prompts(&db, &prompts, "jo").await;
    assert!(orphan_prompt.contains(ORPHAN_NOTE), "{orphan_prompt}");
    let prompts_jo: Vec<String> = text(
        &db,
        "SELECT prompt FROM prepared_ordinary_launches WHERE job_id = ?1",
        "jo",
    )
    .await
    .into_iter()
    .collect();
    assert!(
        prompts_jo
            .iter()
            .all(|prompt| !prompt.contains(RESTART_NOTE)),
        "a turn that never ran had no call in flight"
    );

    assert_eq!(resume(&orch).await, super::RestartResumeSummary::default());
}

/// The notice is keyed off the predecessor's end reason, not the resume step: a
/// continuation after a user's Stop carries no restart notice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_continuation_after_a_user_stop_carries_no_restart_notice() {
    let (orch, db, prompts, _backend) = host("restart-note-user-stop").await;
    seed_node_job(&db, "ju", 1, "active").await;
    db.execute("UPDATE runs SET status = 'exited' WHERE id = 'r-ju'", ())
        .await
        .unwrap();
    seed_turn(
        &db,
        "turn-ju",
        "ju",
        Some("r-ju"),
        1,
        "interrupted",
        "initial",
        Some("user_stop"),
    )
    .await;

    let launcher = orch.clone();
    tokio::task::spawn_blocking(move || {
        crate::execution::jobs::continue_job_impl(&launcher, "ju", Some("carry on"), None, None)
    })
    .await
    .unwrap()
    .expect("the continuation launches");

    let prompt = launch_prompts(&db, &prompts, "ju").await;
    assert!(prompt.contains("carry on"), "{prompt}");
    assert!(!prompt.contains(RESTART_NOTE), "{prompt}");
    assert!(!prompt.contains(ORPHAN_NOTE), "{prompt}");
}

/// The jobs that must not auto-resume: a user's interrupt, an operator's Stop
/// (sticky hold), a job on a merged issue, an interrupted memory review, and a
/// closed thread. The restart reaches each of them and none is relaunched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jobs_that_must_not_auto_resume_get_no_continuation() {
    let (orch, db, _prompts, _backend) = host("restart-resume-negatives").await;

    // A warm Stop: the user interrupted the turn; the process stayed warm on a
    // run the restart then killed.
    seed_node_job(&db, "j-user", 1, "active").await;
    seed_turn(
        &db,
        "t-user",
        "j-user",
        Some("r-j-user"),
        1,
        "interrupted",
        "initial",
        Some("user_stop"),
    )
    .await;

    // `stop_job`: the live turn cancelled with no end reason, the goal held off.
    seed_node_job(&db, "j-hold", 2, "active").await;
    seed_turn(
        &db,
        "t-hold",
        "j-hold",
        Some("r-j-hold"),
        1,
        "cancelled",
        "initial",
        None,
    )
    .await;
    db.execute(
        "UPDATE jobs SET goal_intent = 'off' WHERE id = 'j-hold'",
        (),
    )
    .await
    .unwrap();

    // Mid-turn on an issue that has since merged.
    seed_node_job(&db, "j-merged", 3, "merged").await;
    seed_turn(
        &db,
        "t-merged",
        "j-merged",
        Some("r-j-merged"),
        1,
        "running",
        "initial",
        None,
    )
    .await;

    // Work finished; the restart cut off only the memory review after it.
    seed_node_job(&db, "j-review", 4, "active").await;
    seed_turn(
        &db,
        "t-work",
        "j-review",
        Some("r-j-review"),
        1,
        "complete",
        "initial",
        Some("artifact_handoff"),
    )
    .await;
    seed_turn(
        &db,
        "t-review",
        "j-review",
        Some("r-j-review"),
        2,
        "running",
        "memory_review",
        None,
    )
    .await;

    // A closed thread's session, mid-turn.
    seed_thread_session(&db, "j-closed", "th-closed", "closed").await;
    seed_turn(
        &db,
        "t-closed",
        "j-closed",
        Some("r-j-closed"),
        1,
        "running",
        "initial",
        None,
    )
    .await;

    reconcile(&orch).await;
    assert_eq!(
        settlement(&db, "t-merged").await,
        "interrupted/host_restart"
    );
    assert_eq!(status(&db, "j-merged").await, "cancelled");
    assert_eq!(
        settlement(&db, "t-review").await,
        "interrupted/host_restart"
    );

    let summary = resume(&orch).await;
    assert_eq!(summary.admitted, 0, "{summary:?}");
    for job in ["j-user", "j-hold", "j-merged", "j-review", "j-closed"] {
        assert!(
            launched_turns(&db, job).await.is_empty(),
            "{job} must not be resumed"
        );
        assert_eq!(launched_runs(&db, job).await, 0, "{job}");
        assert_ne!(status(&db, job).await, "running", "{job}");
    }
}

/// A head the provider cut off (`provider_interrupt`) or a watchdog kill whose
/// continuation budget ran out (`watchdog_recovery`) is truncated work, not a
/// restart casualty: boot never selects it for a resume. Each head is seeded in
/// the shape every other filter of the selection admits (job resting, no live
/// run, turn ended after boot), beside a `host_restart` head of the same shape
/// that is selected, so only the end reason keeps them out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncated_heads_are_not_selected_for_a_boot_resume() {
    let (orch, db, _prompts, _backend) = host("restart-resume-truncated").await;
    let heads = [
        ("j-provider", "t-provider", "provider_interrupt"),
        ("j-watchdog", "t-watchdog", "watchdog_recovery"),
        ("j-restart", "t-restart", "host_restart"),
    ];
    for (number, (job, turn, end_reason)) in (1..).zip(heads) {
        seed_node_job(&db, job, number, "active").await;
        seed_turn(
            &db,
            turn,
            job,
            Some(&format!("r-{job}")),
            1,
            "interrupted",
            "initial",
            Some(end_reason),
        )
        .await;
        db.execute(
            "UPDATE runs SET status = 'exited' WHERE job_id = ?1",
            params![job],
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE jobs SET status = 'idle' WHERE id = ?1",
            params![job],
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE turns SET ended_at = ?1 WHERE id = ?2",
            params![BOOT_AT + 5, turn],
        )
        .await
        .unwrap();
    }

    let selector = orch.clone();
    let selected =
        tokio::task::spawn_blocking(move || super::select_restart_resumes(&selector, BOOT_AT))
            .await
            .unwrap()
            .expect("the selection reads");
    assert_eq!(
        selected,
        vec![("j-restart".to_string(), "t-restart".to_string())],
        "only the restart casualty is resumed at boot"
    );
}

/// A resume that fails (here: the job's agent cannot be resolved, so the launch
/// is refused after its turn was minted) settles the job wants-to-run, and the
/// step never tries it again. Covers both restart settlements.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_resume_settles_wants_to_run_and_is_not_retried() {
    let (orch, db, _prompts, _backend) = host("restart-resume-refused").await;
    seed_node_job(&db, "jf", 1, "active").await;
    seed_turn(
        &db,
        "t-jf",
        "jf",
        Some("r-jf"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_node_job(&db, "jfo", 2, "active").await;
    seed_turn(&db, "t-jfo", "jfo", None, 1, "pending", "initial", None).await;
    db.execute(
        "UPDATE jobs SET agent_config_id = 'no-such-agent' WHERE id IN ('jf', 'jfo')",
        (),
    )
    .await
    .unwrap();

    reconcile(&orch).await;
    assert_eq!(settlement(&db, "t-jfo").await, "cancelled/orphaned_pending");
    let summary = resume(&orch).await;
    assert_eq!((summary.selected, summary.refused), (2, 2), "{summary:?}");

    for job in ["jf", "jfo"] {
        assert_eq!(
            status(&db, job).await,
            "pending",
            "{job} rests wants-to-run, not failed"
        );
        let turns = launched_turns(&db, job).await;
        assert_eq!(turns.len(), 1, "{job}: {turns:?}");
        assert_eq!(
            settlement(&db, &turns[0].0).await,
            "cancelled/launch_refused"
        );
    }

    assert_eq!(resume(&orch).await, super::RestartResumeSummary::default());
    for job in ["jf", "jfo"] {
        assert_eq!(
            launched_turns(&db, job).await.len(),
            1,
            "{job} gets no second attempt"
        );
    }
}

/// A coordinator node on its own parent issue, with an open session and no turns,
/// so the boot resume has nothing to say about it.
async fn seed_parent_coordinator(db: &LocalDb, number: i64) {
    db.execute(
        "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
         VALUES('parent','p',?1,'Parent','active',1,1)",
        params![number],
    )
    .await
    .unwrap();
    db.execute_script(
        "INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('parent-exec','recipe','parent','p','running',1,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,
                          current_session_id,created_at,updated_at,started_at)
           VALUES('parent-job','p','parent','parent-exec','running','coordinator',
                  'parent-session',3,3,3);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('parent-session','parent-job','open',1,3,3);",
    )
    .await
    .unwrap();
}

/// `(recipient, key, content_ref, fingerprint)` of every `child-unresumed:` push,
/// delivered or not.
async fn unresumed_pushes(db: &LocalDb) -> Vec<(String, String, String, String)> {
    use crate::storage::RowExt as _;
    db.query_all(
        "SELECT recipient, key, content_ref, fingerprint FROM attention_pushes
          WHERE key LIKE 'child-unresumed:%'",
        (),
        |row| {
            Ok((
                row.text(0)?,
                row.text(1)?,
                row.text(2)?,
                row.opt_text(3)?.unwrap_or_default(),
            ))
        },
    )
    .await
    .unwrap()
}

/// Every row the child node owns that the sweep could have written to, so a test
/// can assert it wrote to none of them.
async fn child_footprint(db: &LocalDb, job: &str) -> String {
    use crate::storage::RowExt as _;
    let mut footprint = status(db, job).await;
    for sql in [
        "SELECT id || '/' || state || '/' || COALESCE(end_reason, '-') || '/' || updated_at
           FROM turns WHERE job_id = ?1 ORDER BY id",
        "SELECT id || '/' || COALESCE(status, '-') || '/' || updated_at
           FROM runs WHERE job_id = ?1 ORDER BY id",
        "SELECT id || '/' || status || '/' || updated_at
           FROM sessions WHERE job_id = ?1 ORDER BY id",
    ] {
        let rows = db
            .query_all(sql, (job.to_string(),), |row| row.text(0))
            .await
            .unwrap();
        footprint.push('\n');
        footprint.push_str(&rows.join(","));
    }
    footprint
}

/// Child node `jc` on issue 2 with a parent coordinator on issue 1, plus the
/// `review` task the standard recipe creates up front: `pending`, no turns, and
/// waiting on the node.
async fn seed_child_with_parent(db: &LocalDb) {
    seed_parent_coordinator(db, 1).await;
    seed_node_job(db, "jc", 2, "active").await;
    db.execute_script(
        "UPDATE issues SET parent_issue_id = 'parent' WHERE id = 'i-jc';
         INSERT INTO jobs(id,execution_id,recipe_node_id,parent_job_id,issue_id,project_id,status,
                          node_name,uri_segment,current_session_id,model,created_at,updated_at)
           VALUES('review-jc','exec-jc','review-node','jc','i-jc','p','pending','Review','review',
                  's-review-jc','opus',1,1);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('s-review-jc','review-jc','open',1,1,1);",
    )
    .await
    .unwrap();
}

/// A turn an earlier boot settled and no resume ever took, on a run long over: the
/// shape a pass that never ran, or a refusal before minting, leaves behind.
async fn seed_settled_episode(db: &LocalDb) {
    seed_turn(
        db,
        "turn-jc",
        "jc",
        None,
        2,
        "cancelled",
        "follow_up",
        Some("orphaned_pending"),
    )
    .await;
    db.execute_script(
        "UPDATE turns SET started_at = NULL WHERE id = 'turn-jc';
         UPDATE runs SET status = 'crashed', exit_reason = 'crash' WHERE id = 'r-jc';
         UPDATE jobs SET status = 'pending' WHERE id = 'jc';",
    )
    .await
    .unwrap();
}

/// Until this process's pass has finished the sweep can say nothing honestly, so
/// it neither tells nor withdraws, and a host that never runs a pass stays here.
/// After it, an episode an earlier boot left behind is told like any other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sweep_does_nothing_until_this_processs_resume_pass_has_finished() {
    use crate::orchestrator::lifecycle::{render_unresumed_push_body, sweep_unresumed_children};
    const CHILD_NODE: &str = "cairn://p/p/2/1/builder";

    let (orch, db, _prompts, _backend) = host("restart-unresumed-pass").await;
    seed_child_with_parent(&db).await;
    seed_settled_episode(&db).await;
    db.execute(
        "INSERT INTO attention_pushes (id, recipient, content_ref, wake, boundary, key, created_at)
         VALUES ('stale', 'parent-job', 'cairn://p/p/9/1/builder', 'wake', 'event',
                 'child-unresumed:cairn://p/p/9/1/builder', 1)",
        (),
    )
    .await
    .unwrap();

    let before = sweep_unresumed_children(&orch).await;
    assert!(before.told.is_empty(), "{before:?}");
    assert_eq!(before.withdrawn, 0, "it withdraws nothing either");
    assert_eq!(
        render_unresumed_push_body(&orch, "parent-job", CHILD_NODE).await,
        None,
        "and delivery falls back to the pointer"
    );

    assert_eq!(
        resume(&orch).await.selected,
        0,
        "the turn predates this boot"
    );

    let after = sweep_unresumed_children(&orch).await;
    assert_eq!(
        after.told,
        vec![(CHILD_NODE.to_string(), vec!["parent-job".to_string()])]
    );
    assert_eq!(after.withdrawn, 1, "the node that is not standing goes");
}

/// A parent that drains the fact after the child moved on, in the tick before the
/// sweep withdraws it, reads that nothing is owed. The sweep then clears the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_moved_on_reads_that_nothing_is_owed() {
    use crate::orchestrator::lifecycle::{render_unresumed_push_body, sweep_unresumed_children};
    const CHILD_NODE: &str = "cairn://p/p/2/1/builder";

    let (orch, db, _prompts, _backend) = host("restart-unresumed-moved-on").await;
    seed_child_with_parent(&db).await;
    seed_settled_episode(&db).await;
    resume(&orch).await;
    assert_eq!(sweep_unresumed_children(&orch).await.told.len(), 1);

    seed_turn(&db, "turn-jc2", "jc", None, 3, "running", "follow_up", None).await;

    let body = render_unresumed_push_body(&orch, "parent-job", CHILD_NODE)
        .await
        .expect("the body renders");
    assert!(body.contains(CHILD_NODE), "{body}");
    assert!(body.contains("no longer in that state"), "{body}");
    assert!(body.contains("Nothing is owed"), "{body}");

    sweep_unresumed_children(&orch).await;
    let undelivered = db
        .query_opt_i64(
            "SELECT COUNT(*) FROM attention_pushes
              WHERE key LIKE 'child-unresumed:%' AND delivered_event_id IS NULL",
            (),
        )
        .await
        .unwrap();
    assert_eq!(undelivered, Some(0));
}

/// cairn/4835. A child with a pending runless turn, an open provider session and
/// a parent coordinator goes through the real boot path: the sweep settles the
/// turn `orphaned_pending`, the one resume is refused, and the parent is told once.
/// Neither another tick nor another boot tells it again, and nothing on this path
/// touches the child, because the parent decides what happens to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_whose_orphaned_turn_was_never_resumed_wakes_its_parent_once() {
    use crate::orchestrator::lifecycle::{render_unresumed_push_body, sweep_unresumed_children};
    const CHILD_NODE: &str = "cairn://p/p/2/1/builder";

    let (orch, db, _prompts, _backend) = host("restart-unresumed-child").await;
    seed_child_with_parent(&db).await;
    seed_turn(
        &db,
        "turn-jc0",
        "jc",
        Some("r-jc"),
        1,
        "complete",
        "initial",
        None,
    )
    .await;
    seed_turn(&db, "turn-jc", "jc", None, 2, "pending", "follow_up", None).await;
    db.execute(
        "UPDATE jobs SET agent_config_id = 'no-such-agent' WHERE id = 'jc'",
        (),
    )
    .await
    .unwrap();

    reconcile(&orch).await;
    assert_eq!(
        status(&db, "review-jc").await,
        "pending",
        "the review task is still waiting on the node, as in production"
    );
    assert_eq!(
        settlement(&db, "turn-jc").await,
        "cancelled/orphaned_pending"
    );

    let before_the_pass = sweep_unresumed_children(&orch).await;
    assert!(
        before_the_pass.told.is_empty() && unresumed_pushes(&db).await.is_empty(),
        "the resume has not had its chance yet, so saying it failed would be false"
    );

    let summary = resume(&orch).await;
    assert_eq!(summary.refused, 1, "{summary:?}");
    let footprint = child_footprint(&db, "jc").await;

    let told = sweep_unresumed_children(&orch).await;
    assert_eq!(
        told.told,
        vec![(CHILD_NODE.to_string(), vec!["parent-job".to_string()])],
        "one wake, to the parent coordinator"
    );
    let expected = vec![(
        "parent-job".to_string(),
        format!("child-unresumed:{CHILD_NODE}"),
        CHILD_NODE.to_string(),
        "turn-jc".to_string(),
    )];
    assert_eq!(unresumed_pushes(&db).await, expected);

    let body = render_unresumed_push_body(&orch, "parent-job", CHILD_NODE)
        .await
        .expect("the body renders");
    assert!(body.contains(CHILD_NODE), "{body}");
    assert!(body.contains("turn-jc"), "{body}");
    assert!(body.contains("orphaned_pending"), "{body}");
    assert!(body.contains("1 refused launch since"), "{body}");
    assert!(body.contains("resumable"), "{body}");

    let again = sweep_unresumed_children(&orch).await;
    assert!(again.told.is_empty(), "{again:?}");
    assert_eq!(unresumed_pushes(&db).await, expected);

    let later_boot = chrono::Utc::now().timestamp() + 10;
    let boot = orch.clone();
    tokio::task::spawn_blocking(move || {
        super::reconcile_stale_runs(&boot, boot.services.emitter.as_ref(), later_boot);
        super::resume_jobs_interrupted_by_restart(&boot, later_boot)
    })
    .await
    .map(|summary| assert_eq!(summary.selected, 0, "{summary:?}"))
    .unwrap();
    let next_boot = sweep_unresumed_children(&orch).await;
    assert!(next_boot.told.is_empty(), "{next_boot:?}");
    assert_eq!(
        unresumed_pushes(&db).await,
        expected,
        "another boot over unchanged state repeats nothing"
    );

    assert_eq!(
        child_footprint(&db, "jc").await,
        footprint,
        "the sweep observes and never writes to the child"
    );
}

/// A pending turn bound to a stale run never began. It used to settle `failed`,
/// which reprojection would read as a failed job; it now settles resumable, and
/// its resume is told nothing ran. Its stamped delivery returns to the queue
/// only when the run never started: a started run may already have handed the
/// prompt to its process, and the native resume must not get that wake twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_turn_on_a_stale_run_settles_resumable_and_keeps_a_delivery_it_may_have_made() {
    let (orch, db, prompts, _backend) = host("restart-pending-turn").await;
    // `jn`: a cold launch whose run never started. `js`: a run that started.
    for (number, job) in [(1, "jn"), (2, "js")] {
        seed_node_job(&db, job, number, "active").await;
        let run = format!("r-{job}");
        seed_turn(
            &db,
            &format!("t-{job}0"),
            job,
            Some(&run),
            1,
            "complete",
            "initial",
            None,
        )
        .await;
        seed_turn(
            &db,
            &format!("t-{job}1"),
            job,
            Some(&run),
            2,
            "pending",
            "follow_up",
            None,
        )
        .await;
        db.execute(
            "INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,parent_tool_use_id,created_at)
             VALUES('brief-' || ?1,'r-' || ?1,'s-' || ?1,'t-' || ?1 || '1',3,3,'attention:briefing','{}',NULL,3)",
            params![job],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO attention_pushes (id, recipient, content_ref, wake, boundary, key, created_at, delivered_event_id)
             VALUES ('push-' || ?1, ?1, 'cairn://p/p/1', 'wake', 'event', 'direct:m-' || ?1, 1, 'brief-' || ?1)",
            params![job],
        )
        .await
        .unwrap();
    }
    db.execute(
        "UPDATE runs SET status = 'starting', started_at = NULL WHERE id = 'r-jn'",
        (),
    )
    .await
    .unwrap();

    reconcile(&orch).await;

    for job in ["jn", "js"] {
        assert_eq!(
            settlement(&db, &format!("t-{job}1")).await,
            "cancelled/host_restart",
            "{job}"
        );
        assert_eq!(status(&db, job).await, "pending", "{job}");
    }
    let delivery = |job: &'static str| {
        let db = db.clone();
        async move {
            text(
                &db,
                "SELECT COALESCE(delivered_event_id, 'undelivered') FROM attention_pushes WHERE id = ?1",
                &format!("push-{job}"),
            )
            .await
        }
    };
    assert_eq!(
        delivery("jn").await.as_deref(),
        Some("undelivered"),
        "a run that never started cannot have carried the prompt"
    );
    assert_eq!(
        text(&db, "SELECT id FROM events WHERE id = ?1", "brief-jn").await,
        None
    );
    assert_eq!(
        delivery("js").await.as_deref(),
        Some("brief-js"),
        "a started run may have delivered it, so the delivery stands"
    );

    let summary = resume(&orch).await;
    assert_eq!((summary.selected, summary.admitted), (2, 2), "{summary:?}");
    for job in ["jn", "js"] {
        let prompt = launch_prompts(&db, &prompts, job).await;
        assert!(prompt.contains(ORPHAN_NOTE), "{job}: {prompt}");
        assert!(!prompt.contains(RESTART_NOTE), "{job}: {prompt}");
    }
}

/// The in-session reaper answers a different question and keeps the ordinary
/// crash settlement: no restart marker, and a pending turn still fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reaper_settlement_is_unchanged() {
    let (_orch, db, _prompts, _backend) = host("restart-reaper-unchanged").await;
    seed_node_job(&db, "jr", 1, "active").await;
    seed_turn(
        &db,
        "t-run",
        "jr",
        Some("r-jr"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_turn(
        &db,
        "t-pend",
        "jr",
        Some("r-jr"),
        2,
        "pending",
        "follow_up",
        None,
    )
    .await;

    let settled = db
        .write(|conn| {
            Box::pin(async move {
                super::settle_stale_run(conn, "r-jr", "stale", super::StaleRunSettlement::Reaped, 5)
                    .await
            })
        })
        .await
        .unwrap();

    assert_eq!(settled.map(|turns| turns.len()), Some(2));
    assert_eq!(settlement(&db, "t-run").await, "interrupted/-");
    assert_eq!(settlement(&db, "t-pend").await, "failed/-");
}

/// The reaper settles a turn that held a `run` call open with the ordinary
/// crash shape, not as a park: the host is alive, so nothing restarts the
/// call. Its held row is cancelled, so the next boot cannot resume the
/// crashed turn from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reaper_cancels_a_held_call_and_keeps_the_crash_shape() {
    let (_orch, db, _prompts, _backend) = host("restart-reaper-held-call").await;
    seed_node_job(&db, "jr", 1, "active").await;
    seed_turn(
        &db,
        "t-run",
        "jr",
        Some("r-jr"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    seed_held_run_call(&db, "jr", "t-run").await;

    db.write(|conn| {
        Box::pin(async move {
            super::settle_stale_run(conn, "r-jr", "stale", super::StaleRunSettlement::Reaped, 5)
                .await
        })
    })
    .await
    .unwrap();

    assert_eq!(settlement(&db, "t-run").await, "interrupted/-");
    assert_eq!(
        text(
            &db,
            "SELECT state FROM agent_waits WHERE id = ?1",
            "wait-jr"
        )
        .await
        .as_deref(),
        Some("cancelled")
    );
}

/// The provider can drain its abort frames only after teardown removed its
/// handle. Exercise the real reader, not pre-inserted transcript rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_abort_frames_belong_to_restart_turn_and_resume_once() {
    shutdown_abort_frames(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_stop_before_shutdown_stays_user_interrupted_and_never_resumes() {
    shutdown_abort_frames(true).await;
}

async fn shutdown_abort_frames(operator_stop: bool) {
    let (orch, db, prompts, _backend) = host("shutdown-abort-frames").await;
    seed_node_job(&db, "j", 1, "active").await;
    seed_turn(
        &db,
        "turn-j",
        "j",
        Some("r-j"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    db.execute(
        "INSERT INTO events(id,run_id,session_id,turn_id,sequence,timestamp,event_type,data,created_at)
         VALUES('call-j','r-j','s-j','turn-j',2,2,'assistant',?1,2)",
        (serde_json::json!({"eventType":"assistant","toolUses":[{"id":"toolu_cut","name":"mcp__cairn__run","input":{"commands":[{"command":"cargo test"}]}}]}).to_string(),),
    ).await.unwrap();
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(
            cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
        ))),
        Some("s-j".into()),
        Some("j".into()),
    );
    handle.begin_turn("turn-j");
    orch.process_state
        .register_process("r-j".into(), handle)
        .unwrap();

    let frames = [
        serde_json::json!({"type":"user","uuid":"abort-frame","session_id":"s-j","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_cut","is_error":true,"content":"The user doesn't want to proceed with this tool use. STOP what you are doing and wait for the user to tell you how to proceed."}]}}),
        serde_json::json!({"type":"user","uuid":"abort-frame","session_id":"s-j","message":{"role":"user","content":[{"type":"text","text":"[Request interrupted by user for tool use]"}]}}),
        serde_json::json!({"type":"result","session_id":"s-j","subtype":"error_during_execution","is_error":true,"errors":["[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use"],"terminal_reason":"aborted_tools","stop_reason":"tool_use","duration_ms":10,"duration_api_ms":10,"num_turns":1,"total_cost_usd":0.0,"usage":{"input_tokens":0,"output_tokens":0}}),
    ].map(|frame| {
        serde_json::from_value::<cairn_models::wire::claude_stream::ClaudeEvent>(frame.clone())
            .expect("the specimen frame must pass the production wire parser");
        frame.to_string()
    }).join("\n") + "\n";
    let (drain, gate) = std::sync::mpsc::channel();
    let (ready, reading) = std::sync::mpsc::channel();
    let reader_orch = orch.clone();
    let reader_db = db.clone();
    let reader = tokio::task::spawn_blocking(move || {
        let runtime = crate::backends::claude::claude_runtime(
            &reader_orch,
            &reader_orch.services.emitter,
            "r-j",
            Some("s-j".into()),
            reader_db.clone(),
        );
        cairn_session::backends::claude::ClaudeRuntime::reader_thread(
            &runtime,
            "r-j",
            Some("s-j".into()),
            Some("opus".into()),
            false,
            Box::new(std::io::BufReader::new(ShutdownDrain {
                frames: std::io::Cursor::new(frames.into_bytes()),
                gate: Some(gate),
                ready: Some(ready),
            })),
            None,
            reader_db,
        );
    });
    reading
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    if operator_stop {
        let stopping = orch.clone();
        tokio::task::spawn_blocking(move || {
            crate::orchestrator::lifecycle::operator_stop_run(&stopping, "r-j").unwrap();
        })
        .await
        .unwrap();
    }
    let stopped = crate::orchestrator::lifecycle::stop_agents_for_host_shutdown(
        &orch,
        std::time::Duration::from_secs(30),
    )
    .await;
    assert_eq!(stopped.stopped, 1);
    assert_eq!(orch.process_state.has_process("r-j"), Some(false));
    // The reader was live before shutdown. Release its buffered frames only
    // after the handle is gone, the specimen's attribution-loss interleaving.
    drain.send(()).unwrap();
    reader.await.unwrap();

    assert_eq!(
        settlement(&db, "turn-j").await,
        if operator_stop {
            "interrupted/user_stop"
        } else {
            "interrupted/host_restart"
        },
    );
    if operator_stop {
        assert_eq!(
            text(&db, "SELECT goal_intent FROM jobs WHERE id=?1", "j")
                .await
                .as_deref(),
            Some("off")
        );
        reconcile(&orch).await;
        assert_eq!(resume(&orch).await.selected, 0);
        assert!(launched_turns(&db, "j").await.is_empty());
        return;
    }
    for kind in [
        "tool_result",
        "user",
        "result:error_during_execution",
        "system:error",
    ] {
        let count = db.query_opt_i64(
            "SELECT COUNT(*) FROM events WHERE run_id='r-j' AND event_type=?1 AND turn_id='turn-j' AND sequence>1",
            (kind,),
        ).await.unwrap();
        assert_eq!(count, Some(1), "{kind} must belong to the interrupted turn");
    }
    // The old runner settled it BEFORE the next boot, not during its sweep.
    let boot = chrono::Utc::now().timestamp() + 10;
    let (orch, _restarted_backend) = reboot(&orch, prompts.clone());
    let boot_orch = orch.clone();
    tokio::task::spawn_blocking(move || {
        super::reconcile_stale_runs(&boot_orch, boot_orch.services.emitter.as_ref(), boot);
    })
    .await
    .unwrap();
    let boot_orch = orch.clone();
    let summary = tokio::task::spawn_blocking(move || {
        super::resume_jobs_interrupted_by_restart(&boot_orch, boot)
    })
    .await
    .unwrap();
    assert_eq!((summary.selected, summary.admitted), (1, 1), "{summary:?}");
    assert_eq!(launched_turns(&db, "j").await.len(), 1);
    assert!(launch_prompts(&db, &prompts, "j")
        .await
        .contains("came from the restart, not from the user"));
    assert_eq!(resume(&orch).await.selected, 0);
}

/// Codex's app-server reports a different terminal frame. Neither an aborted
/// turn nor a success racing teardown can overwrite the host's restart cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_codex_terminal_frames_preserve_restart_and_resume_once() {
    for terminal in [
        serde_json::json!({"method":"turn/aborted","params":{"turnId":"p1","reason":"interrupted"}}),
        serde_json::json!({"method":"turn/completed","params":{"turn":{"id":"p1","status":"completed"}}}),
    ] {
        let (orch, db, prompts, _backend) = host("shutdown-codex-frames").await;
        seed_node_job(&db, "j", 1, "active").await;
        seed_turn(
            &db,
            "turn-j",
            "j",
            Some("r-j"),
            1,
            "running",
            "initial",
            None,
        )
        .await;
        db.execute_script(
            "UPDATE sessions SET backend='codex', backend_id='native-thread' WHERE id='s-j'",
        )
        .await
        .unwrap();
        let mut handle = RunHandle::test_handle(Some("s-j"), Some("j"));
        handle.begin_turn("turn-j");
        orch.process_state
            .register_process("r-j".into(), handle)
            .unwrap();
        let stopped = crate::orchestrator::lifecycle::stop_agents_for_host_shutdown(
            &orch,
            std::time::Duration::from_secs(30),
        )
        .await;
        assert_eq!(stopped.stopped, 1);
        let (tx, rx) = crossbeam_channel::unbounded();
        // Aborts with no usage intentionally omit their result event. Supply
        // usage so both terminal shapes exercise late result attribution.
        let counts = serde_json::json!({"totalTokens":12,"inputTokens":10,"cachedInputTokens":0,"outputTokens":2,"reasoningOutputTokens":0});
        tx.send(serde_json::json!({"method":"thread/tokenUsage/updated","params":{"threadId":"native-thread","tokenUsage":{"total":counts,"last":counts}}})).unwrap();
        tx.send(terminal).unwrap();
        drop(tx);
        let reader_orch = orch.clone();
        let reader_db = db.clone();
        tokio::task::spawn_blocking(move || {
            crate::backends::codex::CodexBackend::reader_thread_app_server(
                &reader_orch,
                &reader_orch.services.emitter,
                "r-j",
                Some("s-j".into()),
                rx,
                Arc::new(
                    cairn_models::codex::app_server::AppServerClient::for_test_scripted(
                        vec![],
                        Arc::new(Mutex::new(Vec::new())),
                    ),
                ),
                Arc::new(Mutex::new(Some("p1".into()))),
                None,
                "codex".into(),
                reader_db,
                None,
            );
        })
        .await
        .unwrap();
        assert_eq!(settlement(&db, "turn-j").await, "interrupted/host_restart");
        assert_eq!(db.query_opt_i64(
            "SELECT COUNT(*) FROM events WHERE run_id='r-j' AND event_type LIKE 'result:%' AND turn_id='turn-j'", (),
        ).await.unwrap(), Some(1));
        let boot = chrono::Utc::now().timestamp() + 10;
        let (orch, _restarted_backend) = reboot(&orch, prompts);
        let restarted = orch.clone();
        let summary = tokio::task::spawn_blocking(move || {
            super::reconcile_stale_runs(&restarted, restarted.services.emitter.as_ref(), boot);
            super::resume_jobs_interrupted_by_restart(&restarted, boot)
        })
        .await
        .unwrap();
        assert_eq!((summary.selected, summary.admitted), (1, 1), "{summary:?}");
        assert_eq!(launched_turns(&db, "j").await.len(), 1);
        assert_eq!(resume(&orch).await.selected, 0);
    }
}

/// A successor host shares only durable storage, not the old process registry,
/// launch claims or resume-pass latch.
fn reboot(
    predecessor: &Orchestrator,
    prompts: Arc<Mutex<Vec<String>>>,
) -> (
    Orchestrator,
    crate::orchestrator::session::OrchestratorTestSessionBackend,
) {
    let orch = Orchestrator::builder(
        predecessor.db.clone(),
        Arc::new(TestServicesBuilder::new().build()),
        tempfile::tempdir().unwrap().keep(),
    )
    .build();
    let backend =
        crate::orchestrator::session::install_orchestrator_test_session_backend(&orch, move || {
            Box::new(RecordingBackend {
                prompts: prompts.clone(),
                starts: Arc::default(),
            })
        });
    (orch, backend)
}

struct ShutdownDrain {
    frames: std::io::Cursor<Vec<u8>>,
    gate: Option<std::sync::mpsc::Receiver<()>>,
    ready: Option<std::sync::mpsc::Sender<()>>,
}

impl std::io::Read for ShutdownDrain {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if let Some(gate) = self.gate.take() {
            self.ready.take().unwrap().send(()).unwrap();
            gate.recv_timeout(std::time::Duration::from_secs(30))
                .unwrap();
        }
        std::io::Read::read(&mut self.frames, bytes)
    }
}

/// A pre-mint refusal consumes an orderly shutdown's one attempt, even on
/// another boot with no process registry or in-memory pass latch left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_resume_refused_before_mint_is_not_retried_on_later_boots() {
    let (orch, db, prompts, _backend) = host("shutdown-pre-mint-refusal").await;
    seed_thread_session(&db, "j", "th", "active").await;
    seed_turn(
        &db,
        "turn-j",
        "j",
        Some("r-j"),
        1,
        "running",
        "initial",
        None,
    )
    .await;
    let mut handle = RunHandle::test_handle(Some("s-j"), Some("j"));
    handle.begin_turn("turn-j");
    orch.process_state
        .register_process("r-j".into(), handle)
        .unwrap();
    let stopped = crate::orchestrator::lifecycle::stop_agents_for_host_shutdown(
        &orch,
        std::time::Duration::from_secs(30),
    )
    .await;
    assert_eq!(stopped.stopped, 1);
    db.execute("UPDATE threads SET status='closed' WHERE id='th'", ())
        .await
        .unwrap();
    let boot = chrono::Utc::now().timestamp() + 10;
    let (orch, _backend) = reboot(&orch, prompts.clone());
    let restarting = orch.clone();
    let summary = tokio::task::spawn_blocking(move || {
        super::resume_jobs_interrupted_by_restart(&restarting, boot)
    })
    .await
    .unwrap();
    assert_eq!(
        (summary.selected, summary.refused, summary.admitted),
        (1, 1, 0),
        "{summary:?}"
    );
    assert!(launched_turns(&db, "j").await.is_empty());
    assert_eq!(settlement(&db, "turn-j").await, "interrupted/host_restart");
    assert_eq!(db.query_opt_i64(
        "SELECT COUNT(*) FROM events WHERE turn_id='turn-j' AND event_type='system:restart_resume_attempt'", (),
    ).await.unwrap(), Some(1));
    let (orch, _backend) = reboot(&orch, prompts);
    let summary = tokio::task::spawn_blocking(move || {
        super::resume_jobs_interrupted_by_restart(&orch, boot + 20)
    })
    .await
    .unwrap();
    assert_eq!(summary.selected, 0, "{summary:?}");
}
