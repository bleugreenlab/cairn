//! End-to-end coordinator sleep/wake coverage across subscription lookup, durable
//! push creation, the idle-job nudge, and resumed-prompt delivery.

use crate::common;
use cairn_core::credentials::{ClaudeAuth, CredentialStore};
use cairn_core::identity::{CredentialStoreExt, UserIdentity};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_core::internal::agent_process::process::AgentProcessState;
use cairn_core::internal::db::DbState;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::attention_push::list_pending;
use cairn_core::internal::orchestrator::lifecycle::{evaluate_review_readiness, finalize_run};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::{
    MockGitClient, RecordingProcessSpawner, TestServicesBuilder,
};
use cairn_core::internal::services::GitOutput;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::models::{
    AgentNodeConfig, ArtifactNodeConfig, ConfirmPolicy, ExecutionSnapshot, IssueStatus,
    NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeSnapshot,
    RecipeTrigger, RunStatus, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use tempfile::TempDir;

const CHILD_URI: &str = "cairn://p/coord/2";

/// A fixture orchestrator whose resumes can reach the process boundary, and the
/// recorder that observes whether one did.
///
/// The launch funnel fails closed twice before it ever spawns: it refuses a
/// session that has no Cairn-managed credential, and it resolves the backend CLI
/// off the host's PATH. Neither has anything to do with whether a wake routed
/// correctly, but both turn "the coordinator was resumed" into an assertion about
/// the machine running the suite. Supplying an API-key account and a planted
/// managed Claude release removes both, so a recorded spawn means what these
/// tests read it as: the wake reached a launch.
fn orchestrator(temp: &TempDir, db: Arc<LocalDb>) -> (Orchestrator, RecordingProcessSpawner) {
    orchestrator_with_credential(temp, db, Some(ClaudeAuth::ApiKey("test-key".to_string())))
}

/// [`orchestrator`], with the account made optional so one test can exercise a
/// launch that fails after its attention briefing is already assembled.
fn orchestrator_with_credential(
    temp: &TempDir,
    db: Arc<LocalDb>,
    claude_auth: Option<ClaudeAuth>,
) -> (Orchestrator, RecordingProcessSpawner) {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let recorder = RecordingProcessSpawner::new();
    let mut git = MockGitClient::new();
    git.expect_run().times(0..).returning(|_, _| {
        Ok(GitOutput {
            success: true,
            stdout: String::new(),
            stderr: String::new(),
        })
    });
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_process(recorder.clone())
            .with_git(git)
            .build(),
    );
    let config_dir = temp.path().join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();

    // A planted managed release, so the launch never waits on an install. The
    // recording spawner never executes it.
    let process_state = Arc::new(AgentProcessState::default());
    let managed =
        cairn_models::managed::testing::tools_with_claude(&temp.path().join("managed"), "exit 0");

    let credential_store = claude_auth.map(|claude_auth| {
        CredentialStore::from_user_identity(&UserIdentity {
            user_id: "test-user".to_string(),
            email: "test@example.com".to_string(),
            name: "Test User".to_string(),
            claude_auth: Some(claude_auth),
            codex_auth: None,
            github_token: None,
        })
    });

    (
        Orchestrator::builder(db_state, services, config_dir)
            .process_state(process_state)
            .managed_tools(managed)
            .credential_store(credential_store)
            .build(),
        recorder,
    )
}

/// The coordinator half: the project, the parent issue, and the idle coordinator
/// job that watches its children. Shared by every child shape below.
async fn seed_coordinator(db: &LocalDb, root: &std::path::Path) {
    let root = root.to_string_lossy().to_string();
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "coordinator".to_string(),
            name: "Coordinator".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some("parent".to_string()),
            project_id: "project".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap()
    .replace('\'', "''");
    db.execute_script(format!(
        "
        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('project','default','Coordinator','coord','{root}',1,1);
        INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('parent','project',1,'Parent','active','active','none',1,1);
        INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
          VALUES('parent-exec','coordinator','parent','project','running',1,1,'{snapshot}');
        INSERT INTO jobs(id, execution_id, project_id, issue_id, status,
                         uri_segment, node_name, current_session_id, created_at, updated_at)
          VALUES('coordinator','parent-exec','project','parent','complete',
                 'coordinator','Coordinator','coord-session',1,1);
        INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
          VALUES('coord-session','coordinator','claude','claude-session','open',1,2000000000,2000000000);
        INSERT INTO runs(id, project_id, issue_id, job_id, session_id, status, created_at, updated_at, start_mode)
          VALUES('coord-run','project','parent','coordinator','coord-session','complete',1,1,'resume');
        INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
          VALUES('coord-turn','coord-session','coord-run','coordinator',1,'complete','initial',1,1);
        UPDATE jobs SET current_turn_id='coord-turn' WHERE id='coordinator';

        INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                           parent_issue_id, parent_job_id, created_at, updated_at)
          VALUES('child','project',2,'Child','active','waiting','none',
                 'parent','coordinator',1,1);
        INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state,
                                       created_by, created_at, updated_at, one_shot)
          VALUES('child-sub','coordinator','issue','{CHILD_URI}','active','system',1,1,0);
        "
    ))
    .await
    .unwrap();
}

async fn seed_coordinator_and_child(db: &LocalDb, root: &std::path::Path) {
    seed_coordinator(db, root).await;
    let child_snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "build".to_string(),
            name: "Build".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: vec![RecipeNode {
                id: "builder".to_string(),
                node_type: RecipeNodeType::Agent,
                name: "Builder".to_string(),
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: None,
                agent_config: None,
                action_config: None,
                checkpoint_config: None,
                artifact_config: None,
                condition_config: None,
                decision_config: None,
                context_config: None,
            }],
            edges: Vec::new(),
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some("child".to_string()),
            project_id: "project".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap()
    .replace('\'', "''");
    db.execute_script(format!(
        "
        INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
          VALUES('child-exec','build','child','project','running',1,1,'{child_snapshot}');
        INSERT INTO jobs(id, execution_id, recipe_node_id, project_id, issue_id, status,
                         uri_segment, node_name, branch, created_at, updated_at)
          VALUES('child-builder','child-exec','builder','project','child','complete',
                 'builder','Builder','child-branch',1,1);
        INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at)
          VALUES('child-run','project','child','child-builder','complete',1,1);
        INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
          VALUES('child-turn','child-session','child-run','child-builder',1,'complete','initial',1,1);
        INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                   target_branch, status, head_sha, opened_at, updated_at)
          VALUES('child-pr','child-builder','project','child','Child PR','child-branch',
                 'main','open','child-head',1,1);
        "
    ))
    .await
    .unwrap();
}

fn agent_node(id: &str) -> RecipeNode {
    RecipeNode {
        id: id.to_string(),
        node_type: RecipeNodeType::Agent,
        name: id.to_string(),
        position: NodePosition { x: 0.0, y: 0.0 },
        parent_id: None,
        trigger_config: None,
        agent_config: Some(AgentNodeConfig {
            agent_config_id: Some(id.to_string()),
            models: None,
            constraints: Default::default(),
            output_schema: None,
            git_config: None,
        }),
        action_config: None,
        checkpoint_config: None,
        artifact_config: None,
        condition_config: None,
        decision_config: None,
        context_config: None,
    }
}

fn artifact_node(id: &str, name: &str, policy: ConfirmPolicy) -> RecipeNode {
    RecipeNode {
        id: id.to_string(),
        node_type: RecipeNodeType::Artifact,
        name: id.to_string(),
        position: NodePosition { x: 0.0, y: 0.0 },
        parent_id: None,
        trigger_config: None,
        agent_config: None,
        action_config: None,
        checkpoint_config: None,
        artifact_config: Some(ArtifactNodeConfig {
            name: name.to_string(),
            schema: Some(serde_json::json!({"type": "object"})),
            confirm_policy: policy,
            content: None,
        }),
        condition_config: None,
        decision_config: None,
        context_config: None,
    }
}

fn ctx_edge(id: &str, from: &str, to: &str) -> RecipeEdge {
    RecipeEdge {
        id: id.to_string(),
        edge_type: RecipeEdgeType::Context,
        source_node_id: from.to_string(),
        source_handle: "context-out".to_string(),
        target_node_id: to.to_string(),
        target_handle: "context-in".to_string(),
    }
}

/// A planbuild child parked at its plan gate:
/// `planner --context-out-> plan(user) --context-out-> builder`, with the
/// planner's plan artifact written but unconfirmed, its builder still pending,
/// and no PR anywhere.
///
/// The planner's `context-out` terminates in an artifact consumed by the
/// builder, so its branch ships no PR and the turn-end check cadence skips it;
/// no PR exists, so the PR-open edge cannot fire either. The recompute
/// review-readiness hook is the only edge left, and the planner's turn ends by
/// deriving **Blocked** under the `user` confirm gate rather than any terminal
/// status.
///
/// The builder job is seeded exactly as a real execution leaves it — `pending`,
/// no turn, not DAG-ready behind the blocked planner — so it contributes no
/// status change of its own: a sibling transition into a status the hook
/// already accepted would mask the gate.
async fn seed_plan_gate_child(db: &LocalDb) {
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "planbuild".to_string(),
            name: "Plan Build".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: vec![
                agent_node("planner"),
                artifact_node("plan-node", "plan", ConfirmPolicy::User),
                agent_node("builder"),
            ],
            edges: vec![
                ctx_edge("e1", "planner", "plan-node"),
                ctx_edge("e2", "plan-node", "builder"),
            ],
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some("child".to_string()),
            project_id: "project".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap()
    .replace('\'', "''");
    db.execute_script(format!(
        "
        INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
          VALUES('child-exec','planbuild','child','project','running',1,1,'{snapshot}');
        INSERT INTO jobs(id, execution_id, recipe_node_id, project_id, issue_id, status,
                         uri_segment, node_name, created_at, updated_at)
          VALUES('child-planner','child-exec','planner','project','child','running',
                 'planner','Planner',1,1);
        INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at)
          VALUES('child-plan-run','project','child','child-planner','complete',1,1);
        INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason,
                          created_at, ended_at, updated_at)
          VALUES('child-plan-turn','child-plan-session','child-plan-run','child-planner',1,
                 'complete','initial',1,2,2);
        INSERT INTO artifacts(id, job_id, artifact_type, confirmed, data, version,
                              output_name, created_at, updated_at)
          VALUES('child-plan','child-planner','plan',0,'{{}}',1,'plan',1,1);
        INSERT INTO jobs(id, execution_id, recipe_node_id, project_id, issue_id, status,
                         uri_segment, node_name, created_at, updated_at)
          VALUES('child-builder','child-exec','builder','project','child','pending',
                 'builder','Builder',1,1);
        "
    ))
    .await
    .unwrap();
}

/// How long a wait in this file may run before it is reported as a broken
/// fixture. It is not a latency budget. Every wait polls for an observable the
/// code under test produces on its own detached task or thread, and how long
/// that takes is the host's scheduling latency, not the code's behaviour: these
/// tests take about a second on an idle host and from thirteen to seventy-seven
/// on a saturated Linux executor, where a fixed five-second window lost about one
/// run in four. The guard sits far above any load seen and at half nextest's
/// wedged-test bound, so a genuine hang still ends as a named panic saying which
/// observable never arrived.
const FIXTURE_GUARD: std::time::Duration = std::time::Duration::from_secs(300);

/// Poll `probe` until it yields a value, or `None` once [`FIXTURE_GUARD`] has
/// elapsed.
async fn poll_until<T, Fut>(mut probe: impl FnMut() -> Fut) -> Option<T>
where
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = std::time::Instant::now() + FIXTURE_GUARD;
    loop {
        if let Some(value) = probe().await {
            return Some(value);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Wait for the pushes under `key`. The review-readiness hook is detached from
/// the recompute sweep, so the wake lands after `recompute_job` returns.
async fn await_push_rows(db: &LocalDb, key: &str) -> Vec<(String, String, Option<String>)> {
    poll_until(move || async move {
        let rows = push_rows(db, key).await;
        (!rows.is_empty()).then_some(rows)
    })
    .await
    .unwrap_or_else(|| panic!("no attention push was created for {key} within {FIXTURE_GUARD:?}"))
}

async fn push_rows(db: &LocalDb, key: &str) -> Vec<(String, String, Option<String>)> {
    let key = key.to_string();
    db.read(|conn| {
        let key = key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT recipient, wake, delivered_event_id FROM attention_pushes
                     WHERE key=?1 ORDER BY created_at, id",
                    params![key.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?, row.opt_text(2)?));
            }
            Ok(out)
        })
    })
    .await
    .unwrap()
}

/// Wait for the recorder to see `n` spawns. A cold resume is admitted durably
/// and promoted on its own thread, so the spawn trails the call that queued it.
/// Returns without failing; the caller's assertion reports a miss.
async fn await_spawn_count(recorder: &RecordingProcessSpawner, n: usize) {
    poll_until(move || async move { (recorder.spawn_count() >= n).then_some(()) }).await;
}

async fn attention_event_body(db: &LocalDb) -> String {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT data FROM events WHERE event_type='attention:briefing'
                     ORDER BY created_at DESC, sequence DESC LIMIT 1",
                    (),
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .expect("resumed coordinator should persist the delivered attention briefing")
}

#[tokio::test(flavor = "current_thread")]
async fn settled_child_review_wakes_idle_coordinator_while_checks_are_running() {
    let (temp, db) = common::migrated_db().await;
    seed_coordinator_and_child(&db, temp.path()).await;
    let db = Arc::new(db);
    let (orch, recorder) = orchestrator(&temp, db.clone());

    let slot = orch
        .try_begin_turn_end_checks("child-builder", "attempt-running")
        .expect("the slot is free");
    evaluate_review_readiness(&orch, "child").await;
    await_spawn_count(&recorder, 1).await;

    let rows = push_rows(&db, &format!("review:{CHILD_URI}")).await;
    let resumed_run = cairn_core::messages::delivery::latest_run_for_job(&db, "coordinator")
        .expect("the coordinator should have a resumed run");
    assert_ne!(
        resumed_run, "coord-run",
        "the nudge must create the coordinator's successor run"
    );
    assert_eq!(rows.len(), 1, "one fingerprinted review wake is canonical");
    assert_eq!(rows[0].0, "coordinator");
    assert_eq!(rows[0].1, "wake");
    assert!(
        rows[0].2.is_some(),
        "the idle resume must drain and stamp the push"
    );
    assert_eq!(
        recorder.spawn_count(),
        1,
        "the idle coordinator must be resumed"
    );

    let body = attention_event_body(&db).await;
    assert!(body.contains("Work product ready for review"), "{body}");
    assert!(body.contains(CHILD_URI), "{body}");

    drop(slot);
    evaluate_review_readiness(&orch, "child").await;
    assert_eq!(
        push_rows(&db, &format!("review:{CHILD_URI}")).await.len(),
        1,
        "later check completion must not duplicate an unchanged review"
    );
}

/// A planbuild child parks at its plan gate — the planner's turn ends
/// **Blocked** (plan present, unconfirmed, `user` confirm policy) — and the
/// review-readiness hook must treat that transition as settling, minting a wake
/// for the coordinator that actually resumes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_plan_gate_wakes_idle_coordinator() {
    let (temp, db) = common::migrated_db().await;
    seed_coordinator(&db, temp.path()).await;
    seed_plan_gate_child(&db).await;
    let db = Arc::new(db);
    let (orch, recorder) = orchestrator(&temp, db.clone());

    // The production edge: the planner's turn has ended, so its status is
    // recomputed. Nothing else is invoked — no check cadence, no PR.
    cairn_core::internal::execution::advancement::recompute_job(&orch, "child-planner").unwrap();

    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM jobs WHERE id=?1", "child-planner")
            .await,
        Some("blocked".to_string()),
        "a written-but-unconfirmed plan under a user gate derives Blocked"
    );

    let rows = await_push_rows(&db, &format!("review:{CHILD_URI}")).await;
    assert_eq!(rows.len(), 1, "one review wake per gated plan");
    assert_eq!(rows[0].0, "coordinator");
    assert_eq!(
        rows[0].1, "wake",
        "a gate blocks the child on the coordinator's judgment, so it rouses"
    );

    let content_ref = common::scalar_text_by_id(
        &db,
        "SELECT content_ref FROM attention_pushes WHERE recipient=?1 ORDER BY created_at DESC LIMIT 1",
        "coordinator",
    )
    .await
    .expect("the review push carries a content_ref");
    assert!(
        content_ref.ends_with("/planner/plan"),
        "the coordinator is pointed at the plan awaiting its confirmation, got {content_ref}"
    );

    // The nudge trails push creation inside the same detached evaluation, so give
    // it the same bounded window the push itself got.
    await_spawn_count(&recorder, 1).await;
    assert_eq!(
        recorder.spawn_count(),
        1,
        "the idle coordinator must actually be resumed, not merely have a row written"
    );
}

const ADOPTED_URI: &str = "cairn://p/coord/3";

/// A live run's creation time: after this test's orchestrator boots, as a run
/// that runner spawned is, so its tool calls are served (a run older than the
/// boot belonged to an exited runner and is refused).
fn live_since() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 60
}

/// A project with one thread, `arb`, whose session is live in a turn, and one
/// issue that hangs from nothing: no parent of either kind, no subscription.
async fn seed_thread_and_unparented_issue(db: &LocalDb, root: &std::path::Path) {
    let root = root.to_string_lossy().to_string();
    let live = live_since();
    db.execute_script(format!(
        "
        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('project','default','Coordinator','coord','{root}',1,1);
        INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
          VALUES('arb','project','arb','active','none',1,1);
        INSERT INTO jobs(id, project_id, thread_id, status, uri_segment, node_name,
                         current_session_id, created_at, updated_at)
          VALUES('arb-session','project','arb','running','thread','Thread','arb-sess',1,1);
        INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
          VALUES('arb-sess','arb-session','claude','claude-arb','open',1,2000000000,2000000000);
        INSERT INTO runs(id, project_id, job_id, session_id, status, created_at, updated_at, start_mode)
          VALUES('arb-run','project','arb-session','arb-sess','live',{live},{live},'resume');
        INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
          VALUES('arb-turn','arb-sess','arb-run','arb-session',1,'running','initial',1,1);
        UPDATE jobs SET current_turn_id='arb-turn' WHERE id='arb-session';
        INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('loose','project',3,'Loose','backlog','backlog','none',1,1);
        "
    ))
    .await
    .unwrap();
}

/// The bundled planbuild recipe and the agents it names, so the filing runs
/// the recipe a real thread would start.
fn install_planbuild(temp: &TempDir) {
    let config = temp.path().join("config");
    std::fs::write(
        config.join("recipes/planbuild.yaml"),
        include_str!("../../../../packs/core/recipes/planbuild.yaml"),
    )
    .unwrap();
    for (name, body) in [
        (
            "planner",
            include_str!("../../../../packs/core/agents/planner.md"),
        ),
        (
            "build",
            include_str!("../../../../packs/core/agents/build.md"),
        ),
        (
            "pr-review",
            include_str!("../../../../packs/core/agents/pr-review.md"),
        ),
    ] {
        std::fs::write(config.join(format!("agents/{name}.md")), body).unwrap();
    }
}

async fn write_as(orch: &Orchestrator, run_id: &str, changes: serde_json::Value) -> String {
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some(run_id.to_string()),
        tool: "write".to_string(),
        payload: serde_json::json!({ "changes": changes }),
        tool_use_id: None,
        origin: None,
    };
    let cursors = Mutex::new(HashMap::new());
    dispatch_tool(orch, &request, &cursors)
        .await
        .into_inner()
        .content
}

async fn text(db: &LocalDb, sql: &str) -> Option<String> {
    db.query_opt_text(sql, ()).await.unwrap()
}

/// Report 302 (arb/37): a thread started planbuild on an issue that hung from
/// nothing, so no watcher existed, the plan gate minted no wake, and the issue
/// sat ten hours on a confirmation nobody was told about. The thread's filing
/// now adopts the issue, so the gate reaches the thread that asked for the
/// work, through the same parent-derived routing every thread child has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_filing_on_an_unparented_issue_hears_its_plan_gate() {
    let (temp, db) = common::migrated_db().await;
    seed_thread_and_unparented_issue(&db, temp.path()).await;
    let db = Arc::new(db);
    let (orch, recorder) = orchestrator(&temp, db.clone());
    install_planbuild(&temp);

    // The thread's own live run files the work through the write dispatcher.
    let filed = write_as(
        &orch,
        "arb-run",
        serde_json::json!([{
            "target": format!("{ADOPTED_URI}/executions"),
            "mode": "append",
            "payload": { "recipe": "planbuild" },
        }]),
    )
    .await;
    assert!(
        filed.contains("Adopted coord/3 under thread arb"),
        "the filing adopts the issue and says so: {filed}"
    );
    assert_eq!(
        text(&db, "SELECT parent_thread_id FROM issues WHERE id='loose'").await,
        Some("arb".to_string())
    );
    assert_eq!(
        text(&db, "SELECT parent_issue_id FROM issues WHERE id='loose'").await,
        None,
        "a thread edge introduces no issue ancestry"
    );
    assert_eq!(
        text(&db, "SELECT status FROM executions WHERE issue_id='loose'").await,
        Some("running".to_string()),
        "{filed}"
    );
    let planner = match text(
        &db,
        "SELECT j.id FROM jobs j JOIN executions e ON e.id = j.execution_id
          WHERE e.issue_id='loose' AND j.node_name='Planner'",
    )
    .await
    {
        Some(planner) => planner,
        None => panic!(
            "the planbuild execution has its planner; its jobs are {:?}",
            text(
                &db,
                "SELECT group_concat(j.id || ':' || COALESCE(j.recipe_node_id, '-') || ':' ||
                        COALESCE(j.node_name, '-') || ':' || j.status, ' ')
                   FROM jobs j JOIN executions e ON e.id = j.execution_id
                  WHERE e.issue_id='loose'",
            )
            .await
        ),
    };
    let segment =
        common::scalar_text_by_id(&db, "SELECT uri_segment FROM jobs WHERE id=?1", &planner)
            .await
            .expect("the planner has an address");

    // The thread's turn ends, and it goes idle with nothing subscribed.
    db.execute_script(
        "UPDATE turns SET state='complete' WHERE id='arb-turn';
         UPDATE runs SET status='complete' WHERE id='arb-run';
         UPDATE jobs SET status='complete' WHERE id='arb-session';",
    )
    .await
    .unwrap();
    assert_eq!(
        common::query_i64(&db, "SELECT COUNT(*) FROM wake_subscriptions")
            .await
            .unwrap(),
        0,
        "nothing watches the issue: the parent edge alone must route the gate"
    );

    // The planner's turn: it writes its plan through the artifact write, and
    // the turn ends.
    let live = live_since();
    db.execute_script(format!(
        "UPDATE jobs SET status='running' WHERE id='{planner}';
         INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at)
           VALUES('planner-run','project','loose','{planner}','live',{live},{live});
         INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason,
                           created_at, updated_at)
           VALUES('planner-turn','planner-session','planner-run','{planner}',1,
                  'running','initial',1,1);"
    ))
    .await
    .unwrap();
    let written = write_as(
        &orch,
        "planner-run",
        serde_json::json!([{
            "target": format!("{ADOPTED_URI}/1/{segment}/plan"),
            "mode": "create",
            "payload": { "title": "Plan", "content": "Do the work." },
        }]),
    )
    .await;
    assert!(
        !written.contains("\"failures\":[{"),
        "the plan is written: {written}"
    );
    db.execute_script(
        "UPDATE turns SET state='complete', ended_at=2, updated_at=2 WHERE id='planner-turn';
         UPDATE runs SET status='complete' WHERE id='planner-run';",
    )
    .await
    .unwrap();
    cairn_core::internal::execution::advancement::recompute_job(&orch, &planner).unwrap();

    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM jobs WHERE id=?1", &planner).await,
        Some("blocked".to_string()),
        "the unconfirmed plan under planbuild's user gate holds the planner"
    );
    assert_eq!(
        common::scalar_text_by_id(
            &db,
            "SELECT CAST(confirmed AS TEXT) FROM artifacts WHERE job_id=?1 ORDER BY version DESC LIMIT 1",
            &planner,
        )
        .await,
        Some("0".to_string()),
        "the plan is unconfirmed"
    );
    assert_eq!(
        text(
            &db,
            "SELECT j.status FROM jobs j JOIN executions e ON e.id = j.execution_id
              WHERE e.issue_id='loose' AND j.node_name='Builder'",
        )
        .await,
        Some("pending".to_string()),
        "the builder stays gated behind the plan"
    );

    let key = format!("review:{ADOPTED_URI}");
    let rows = await_push_rows(&db, &key).await;
    assert_eq!(rows.len(), 1, "one review wake per gated plan: {rows:?}");
    assert_eq!(
        rows[0].0, "arb-session",
        "the wake reaches the thread: {rows:?}"
    );
    assert_eq!(rows[0].1, "wake");
    let content_ref = common::scalar_text_by_id(
        &db,
        "SELECT content_ref FROM attention_pushes WHERE key=?1",
        &key,
    )
    .await
    .expect("the review push carries a content_ref");
    assert!(
        content_ref.ends_with(&format!("/{segment}/plan")),
        "the thread is pointed at the plan awaiting confirmation, got {content_ref}"
    );

    await_spawn_count(&recorder, 1).await;
    assert_eq!(
        recorder.spawn_count(),
        1,
        "the idle thread is resumed, once"
    );
    // By set, not recency: the filing run is dated past the boot.
    assert_eq!(
        common::query_i64(
            &db,
            "SELECT COUNT(*) FROM runs WHERE job_id='arb-session' AND id <> 'arb-run'"
        )
        .await
        .unwrap(),
        1,
        "the wake creates exactly one successor run for the thread"
    );
    assert!(
        push_rows(&db, &key).await[0].2.is_some(),
        "the resume drains and stamps the push"
    );
    let body = attention_event_body(&db).await;
    assert!(body.contains(ADOPTED_URI), "{body}");

    evaluate_review_readiness(&orch, "loose").await;
    assert_eq!(
        push_rows(&db, &key).await.len(),
        1,
        "re-evaluating unchanged readiness mints no second wake"
    );
    assert_eq!(recorder.spawn_count(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn failed_child_finalize_wakes_once_and_delivers_failure_to_idle_coordinator() {
    let (temp, db) = common::migrated_db().await;
    seed_coordinator_and_child(&db, temp.path()).await;
    db.execute_script(
        "
        INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
          VALUES('child-session','child-builder','claude','child-backend','open',1,1,1);
        DELETE FROM merge_requests WHERE id='child-pr';
        UPDATE issues
          SET status='failed', progress='failed', attention='none', updated_at=2
          WHERE id='child';
        UPDATE executions SET status='failed' WHERE id='child-exec';
        UPDATE jobs
          SET status='running', current_session_id='child-session',
              current_turn_id='child-turn'
          WHERE id='child-builder';
        UPDATE runs
          SET status='live', session_id='child-session',
              error_message='Child provider exited before producing output'
          WHERE id='child-run';
        UPDATE turns SET state='pending' WHERE id='child-turn';
        ",
    )
    .await
    .unwrap();
    let db = Arc::new(db);
    let (orch, _recorder) = orchestrator(&temp, db.clone());

    // This is the production failed-start sequence: finalize marks the turn/run failed,
    // recompute emits Resolved, then the turn-end edge observes the same terminal
    // issue and emits Resolved again. Durable fingerprint dedupe must collapse the
    // second emit.
    finalize_run(&orch, "child-run", RunStatus::Crashed);

    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM issues WHERE id=?1", "child").await,
        Some(IssueStatus::Failed.to_string())
    );
    let rows = push_rows(&db, &format!("resolved:{CHILD_URI}")).await;
    assert_eq!(
        rows.len(),
        1,
        "recompute plus turn-end must create one resolution row"
    );
    assert_eq!(rows[0].1, "wake");
    assert!(rows[0].2.is_some());
    let resumed_run = cairn_core::messages::delivery::latest_run_for_job(&db, "coordinator")
        .expect("the failed child should create a resumed coordinator run");
    assert_ne!(
        resumed_run, "coord-run",
        "the failed-child wake must create the coordinator's successor run"
    );

    let coordinator_run_count = db
        .query_one(
            "SELECT COUNT(*) FROM runs WHERE job_id='coordinator'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(
        coordinator_run_count, 2,
        "failed resolution must create exactly one successor run"
    );
    assert!(list_pending(&db, "child-builder").await.unwrap().is_empty());

    let body = attention_event_body(&db).await;
    assert!(body.contains("Child") && body.contains("failed"), "{body}");
    assert!(
        body.contains(&format!(
            "{CHILD_URI}/1/builder — Child provider exited before producing output. Retry or delegate a fix."
        )),
        "the briefing must identify the failed producer and its persisted reason: {body}"
    );
    assert!(!body.contains("No failure detail was recorded"), "{body}");
    assert!(!body.contains("Successfully"), "{body}");
    assert!(body.contains(CHILD_URI), "{body}");
}
