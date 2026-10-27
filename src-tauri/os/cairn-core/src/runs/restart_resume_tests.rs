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

fn test_orchestrator(db: LocalDb) -> Orchestrator {
    let root = tempfile::tempdir().unwrap().keep();
    let state = Arc::new(DbState::new(
        Arc::new(db),
        Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
    ));
    Orchestrator::builder(state, Arc::new(TestServicesBuilder::new().build()), root).build()
}

/// A provider that records the prompt each launch carried and registers a
/// process for it, so a cold continuation completes without spawning anything.
struct RecordingBackend {
    prompts: Arc<Mutex<Vec<String>>>,
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
    let orch = test_orchestrator(migrated_test_db(name).await);
    let db = orch.db.local.clone();
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);",
    )
    .await
    .unwrap();
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let recorded = prompts.clone();
    let backend =
        crate::orchestrator::session::install_orchestrator_test_session_backend(&orch, move || {
            Box::new(RecordingBackend {
                prompts: recorded.clone(),
            })
        });
    (orch, db, prompts, backend)
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

/// The restart cut a `run` call off between its `tool_use` and its result. The
/// continuation must not open owing that result: it is a fresh turn on a fresh
/// run, and the interrupted turn's dangling call stays where it was, neither
/// re-issued nor re-attached (cairn/5000 owns settling it). How the provider's
/// native resume treats the dangling call is only observable on a real rebuild.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_turn_inherits_no_pending_tool_call() {
    let (orch, db, _prompts, _backend) = host("restart-resume-tool-call").await;
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
    assert_eq!(
        owed("r-jc".into(), "turn-jc".into()).await,
        vec!["toolu_cut".to_string()],
        "the interrupted turn's dangling call is left as it was"
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
