//! Portable node scratch, end to end across physically separate machines
//! (cairn/5253), one test per acceptance item of the issue, and a holder's
//! machine as affinity rather than a lock (cairn/5784).
//!
//! Every machine here is a real, non-colocated `ExecutorRuntime` with its own
//! home, reaching the runner only over an HTTP object channel. The object
//! server is a thin HTTP shell around production: the reachable-pack builder
//! for the repository and cairn-core's own `scratch/*` router, so a test cannot
//! pass against a second implementation of what the runner serves. Batches go
//! through the agent-facing `run` tool, terminals through `write`.
//!
//! Like the other executor-cell suites these cannot run nested inside a Cairn
//! worktree fence, and each records a declared skip there (skip-manifest.toml).

use crate::common;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use cairn_common::executor_protocol::{
    CellCommandClass, CellOutcome, CellPriority, CellRequest, ExecutorSelector, MutationPolicy,
    ObjectTransferCoordinate, RepositoryLocator,
};
use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::run::handle_run;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::object_plane::build_local_reachable_pack;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::{RealProcessSpawner, RealPtyFactory};
use cairn_core::internal::storage::{LocalDb, SearchIndex};
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use serde::Deserialize;
use serde_json::{json, Value};
use tempfile::TempDir;

pub(crate) const MACHINE_A: &str = "scratch-a";
pub(crate) const MACHINE_B: &str = "scratch-b";
/// Attached only by the tests that need a machine neither of the others is.
const MACHINE_C: &str = "scratch-c";
const OWNER: &str = "cairn://p/nsc/1/1/builder";
const BRANCH: &str = "agent/NSC-1-builder-0";

#[derive(Clone)]
struct ObjectServer {
    orch: Orchestrator,
    repository: PathBuf,
}

pub(crate) struct TwoMachines {
    pub temp: TempDir,
    pub orch: Orchestrator,
    pub db: Arc<LocalDb>,
    pub project_id: String,
    pub repository: PathBuf,
    pub base: String,
    pub homes: [PathBuf; 2],
    pub object_url: String,
    /// Each machine's executor, in `homes` order.
    pub pools: Vec<cairn_exec::Fleet>,
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn init_repo(repo: &Path, config: Option<&str>) -> String {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-b", "main", "-q"]);
    git(repo, &["config", "user.email", "scratch@example.com"]);
    git(repo, &["config", "user.name", "Scratch"]);
    std::fs::write(repo.join("README.md"), "two machines\n").unwrap();
    if let Some(config) = config {
        std::fs::create_dir_all(repo.join(".cairn")).unwrap();
        std::fs::write(repo.join(".cairn/config.yaml"), config).unwrap();
    }
    git(repo, &["add", "."]);
    git(repo, &["commit", "-qm", "base"]);
    git(repo, &["rev-parse", "HEAD"])
}

/// One orchestrator, one project, and two isolated machines attached to it.
pub(crate) async fn two_machines() -> TwoMachines {
    two_machines_with(None).await
}

async fn two_machines_with(config: Option<&str>) -> TwoMachines {
    let (temp, db) = common::migrated_db().await;
    let repository = temp.path().join("runner-repository");
    let base = init_repo(&repository, config);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "nsc", &repository).await;
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_process(RealProcessSpawner)
            .with_pty_factory(RealPtyFactory)
            .build(),
    );
    let orch = Orchestrator::builder(
        Arc::new(DbState::new(db.clone(), search_index)),
        services,
        temp.path().join("config"),
    )
    .boot_at(0)
    .build();
    let server = ObjectServer {
        orch: orch.clone(),
        repository: repository.clone(),
    };
    let app = Router::new()
        .route("/fetch", post(fetch))
        .with_state(server)
        .merge(common::scratch_routes(&orch));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let object_url = format!("http://{address}");
    let homes = [temp.path().join("home-a"), temp.path().join("home-b")];
    let pools = [MACHINE_A, MACHINE_B]
        .into_iter()
        .zip(&homes)
        .map(|(machine, home)| {
            common::attach_isolated_test_executor_as(
                &orch,
                machine,
                home.clone(),
                object_url.clone(),
                project_id.clone(),
            )
        })
        .collect();
    TwoMachines {
        temp,
        orch,
        db,
        project_id,
        repository,
        base,
        homes,
        object_url,
        pools,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FetchRequest {
    coordinate: ObjectTransferCoordinate,
    want_commit: String,
    #[serde(default)]
    have_commits: Vec<String>,
}

async fn fetch(
    State(server): State<ObjectServer>,
    headers: HeaderMap,
    Json(request): Json<FetchRequest>,
) -> Response {
    let authenticated = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(|token| {
            server
                .orch
                .object_plane
                .authenticate(token, common::unix_time_ms())
        });
    if authenticated.is_none()
        || !server
            .orch
            .object_plane
            .authorizes(&request.coordinate, &request.want_commit)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let built = match build_local_reachable_pack(
        &server.repository,
        &request.want_commit,
        &request.have_commits,
    ) {
        Ok(built) => built,
        Err(_) => return StatusCode::UNPROCESSABLE_ENTITY.into_response(),
    };
    let headers = built.response_headers().unwrap();
    let mut response = match built.pack {
        None => StatusCode::NO_CONTENT.into_response(),
        Some(pack) => Response::new(Body::from(pack.pack)),
    };
    for (name, value) in headers {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_str(&value).unwrap());
    }
    response
}

fn request_on(machines: &TwoMachines, suffix: &str, machine: &str) -> CellRequest {
    let command = "printf placed".to_owned();
    CellRequest {
        request_id: format!("nsc-{suffix}"),
        attempt_id: "attempt-1".into(),
        project_id: machines.project_id.clone(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: machines.project_id.clone(),
            repository_id: "node-scratch-repository".into(),
            absolute_path: machines.repository.display().to_string(),
        },
        base_commit: machines.base.clone(),
        command_class: CellCommandClass::classify(&command),
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        command,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: CellPriority::AgentInteractive,
        wait_horizon_unix_ms: u64::MAX,
        waiting_since_unix_ms: 0,
        timeout_ms: 30_000,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        executor: Some(ExecutorSelector {
            name: Some(machine.to_owned()),
            ..ExecutorSelector::default()
        }),
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
    }
}

fn cell_sidecars(home: &Path) -> usize {
    fn walk(root: &Path, found: &mut usize) {
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path
                .file_name()
                .is_some_and(|name| name == "cairn-build-slot.json")
            {
                *found += 1;
            }
        }
    }
    let mut found = 0;
    walk(home, &mut found);
    found
}

/// The harness itself: both machines are real placement targets, and a
/// selector naming one lands there and nowhere else.
#[tokio::test]
async fn a_named_selector_lands_on_each_of_two_isolated_machines() {
    if common::skip_if_fenced("a_named_selector_lands_on_each_of_two_isolated_machines") {
        return;
    }
    let machines = two_machines().await;
    assert_ne!(machines.homes[0], machines.homes[1]);

    for (index, machine) in [MACHINE_A, MACHINE_B].into_iter().enumerate() {
        let outcome = machines
            .orch
            .fleet
            .submit(&machines.orch, request_on(&machines, machine, machine))
            .await;
        match &outcome {
            CellOutcome::Completed {
                output,
                exit_code: Some(0),
                ..
            } => assert!(output.contains("placed"), "{outcome:?}"),
            other => panic!("a batch selecting {machine} must run there, got {other:?}"),
        }
        assert_eq!(
            cell_sidecars(&machines.homes[index]),
            1,
            "{machine} must hold the cell its selector placed"
        );
        for (other, home) in machines.homes.iter().enumerate() {
            if other > index {
                assert_eq!(
                    cell_sidecars(home),
                    0,
                    "a batch selecting {machine} must not land on another machine"
                );
            }
        }
    }
    drop(machines.temp);
}

// ── A node on two machines ─────────────────────────────────────────────────

fn agent_snapshot() -> AgentSnapshot {
    AgentSnapshot {
        edited_at: None,
        id: "agent-1".to_string(),
        name: "Builder".to_string(),
        description: String::new(),
        prompt: String::new(),
        tools: Vec::new(),
        models: Vec::new(),
        selection: None,
        provider_account: None,
        model: None,
        disallowed_tools: None,
        skills: None,
        // Unconfined, so these tests observe the scratch rather than OS policy.
        fence: None,
        sandbox: None,
        on_escape: None,
        resolved_backend: None,
        extras: None,
        seed_budget_tokens: None,
        seed_eras: None,
    }
}

/// An issue node (`cairn://p/nsc/1/1/builder`) with a live run, on a branch
/// provisioned the way production provisions one.
pub(crate) struct Node {
    pub(crate) machines: TwoMachines,
    cwd: String,
}

pub(crate) async fn node() -> Node {
    node_with(None).await
}

async fn node_with(config: Option<&str>) -> Node {
    let machines = two_machines_with(config).await;
    let mut agents = HashMap::new();
    agents.insert("agent-1".to_string(), agent_snapshot());
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "recipe-nsc".to_string(),
            name: "Node scratch".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        agents,
        HashMap::new(),
        TriggerContext {
            issue_id: Some("issue-1".to_string()),
            project_id: machines.project_id.clone(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap();
    let project = machines.project_id.as_str();
    machines
        .db
        .execute(
            "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
             VALUES ('issue-1', ?1, 1, 'Node scratch', 'active', 'none', 1, 1)",
            params![project],
        )
        .await
        .unwrap();
    machines
        .db
        .execute(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
             VALUES ('exec-1', 'recipe-nsc', 'issue-1', ?1, 'running', 1, 1, ?2, 'manual')",
            params![project, snapshot.as_str()],
        )
        .await
        .unwrap();
    machines
        .db
        .execute(
            "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, current_session_id, uri_segment, branch, base_commit, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'agent-1', 'issue-1', ?1, 'builder', 'running', 'session-1', 'builder', ?2, ?3, 1, 1)",
            params![project, BRANCH, machines.base.as_str()],
        )
        .await
        .unwrap();
    seed_session(&machines.db, "1", "job-1", project).await;
    let residence = machines.temp.path().join("residence");
    common::provision_jj_workspace(
        &machines.temp.path().join("config"),
        &machines.repository,
        &residence,
        BRANCH,
    );
    Node {
        cwd: residence.display().to_string(),
        machines,
    }
}

/// A session, run (`r-<suffix>`) and turn for `job_id`.
async fn seed_session(db: &LocalDb, suffix: &str, job_id: &str, project_id: &str) {
    db.execute_script(format!(
        "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
           VALUES ('session-{suffix}', '{job_id}', 'active', 1, 1);
         INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
           VALUES ('r-{suffix}', '{project_id}', 'issue-1', '{job_id}', 'live', 'session-{suffix}', 1, 1, 'resume');
         INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
           VALUES ('turn-{suffix}', 'session-{suffix}', 'r-{suffix}', '{job_id}', 1, 'running', 1, 1);
         UPDATE jobs SET current_turn_id = 'turn-{suffix}' WHERE id = '{job_id}';"
    ))
    .await
    .unwrap();
}

impl Node {
    fn orch(&self) -> &Orchestrator {
        &self.machines.orch
    }

    /// A sub-agent task of the builder: its own job, run, and session, sharing
    /// the node's branch and therefore its scratch. Returns its run id.
    async fn add_task(&self, name: &str) -> String {
        let project = self.machines.project_id.as_str();
        self.machines
            .db
            .execute_script(format!(
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, parent_job_id, node_name, status, current_session_id, uri_segment, branch, base_commit, created_at, updated_at)
                 SELECT 'job-{name}', execution_id, agent_config_id, issue_id, project_id, 'job-1', '{name}', 'running', 'session-{name}', '{name}', branch, base_commit, 1, 1
                 FROM jobs WHERE id = 'job-1';"
            ))
            .await
            .unwrap();
        seed_session(&self.machines.db, name, &format!("job-{name}"), project).await;
        format!("r-{name}")
    }

    async fn run(&self, run_id: &str, payload: Value) -> String {
        let result = handle_run(
            self.orch(),
            &McpCallbackRequest {
                answer_bound_ms: None,
                thread_id: None,
                cwd: self.cwd.clone(),
                run_id: Some(run_id.to_string()),
                tool: "run".to_string(),
                payload,
                tool_use_id: Some(format!("toolu-{}", uuid::Uuid::new_v4().simple())),
                origin: None,
            },
        )
        .await;
        serde_json::from_str::<Value>(&result).unwrap()["text"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// One shell item, on `machine` when one is named.
    pub(crate) async fn shell(&self, run_id: &str, machine: Option<&str>, command: &str) -> String {
        let mut payload = json!({ "commands": [{ "command": command }] });
        if let Some(machine) = machine {
            payload["executor"] = json!({ "name": machine });
        }
        self.run(run_id, payload).await
    }

    pub(crate) async fn drain(&self, machine: &str, drained: bool) {
        self.orch()
            .fleet
            .set_executor_drain_mode(machine, drained)
            .await;
    }

    /// This node's replica roots on one machine.
    fn replicas_on(&self, machine: usize) -> Vec<PathBuf> {
        use cairn_common::executor_paths::{
            is_scratch_tree_name, ScratchTreeKind, SCRATCH_TREES_DIR,
        };
        std::fs::read_dir(self.machines.homes[machine].join(SCRATCH_TREES_DIR))
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .is_some_and(|name| is_scratch_tree_name(ScratchTreeKind::Node, name))
                    })
                    .map(|entry| entry.path())
                    .filter(|path| path.join("state.json").is_file())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The runner's stores for this project.
    fn stores(&self) -> Vec<PathBuf> {
        std::fs::read_dir(
            self.orch()
                .config_dir
                .join("node-scratch-store")
                .join(&self.machines.project_id),
        )
        .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
        .unwrap_or_default()
    }

    pub(crate) async fn start_terminal(&self, slug: &str, script: &str) -> String {
        self.start_terminal_of("r-1", OWNER, slug, None, script)
            .await
    }

    /// A terminal of the job behind `run_id`, whose node is `node_uri`, on
    /// `machine` when one is named.
    async fn start_terminal_of(
        &self,
        run_id: &str,
        node_uri: &str,
        slug: &str,
        machine: Option<&str>,
        script: &str,
    ) -> String {
        assert!(!script.contains('\''), "terminal scripts are single-quoted");
        let mut create = json!({ "command": format!("sh -c '{script}'"), "wake": "exit" });
        if let Some(machine) = machine {
            create["executor"] = json!({ "name": machine });
        }
        let reply = handle_write(
            self.orch(),
            &McpCallbackRequest {
                answer_bound_ms: None,
                thread_id: None,
                cwd: String::new(),
                run_id: Some(run_id.to_string()),
                tool: "write".to_string(),
                payload: json!({ "changes": [{
                    "target": format!("{node_uri}/terminal/{slug}"),
                    "mode": "create",
                    "payload": create
                }]}),
                tool_use_id: None,
                origin: None,
            },
        )
        .await;
        // A create that did not start fails here, by its own reply, rather than
        // later as a holder that is missing or a file that never appears.
        assert!(
            reply.contains("Started terminal"),
            "the terminal did not start: {reply}"
        );
        reply
    }

    /// Attach the runner's own machine, named `local`, beside the node's two.
    /// It is what a job with no route is guessed to found its home on.
    async fn attach_colocated(&self) {
        let orch = self.orch().clone();
        cairn_exec::blocking::offload(move || common::attach_test_executor(&orch)).await;
    }

    /// The placement record of `job`'s fresh home acquisition: what the handler
    /// actually asked for, read from the same record an operator reads.
    fn home_acquisition_of(&self, job: &str) -> cairn_common::executor_protocol::PlacementDecision {
        let request_id = format!("residency-acquire:job:{job}");
        self.orch()
            .fleet
            .inspect_executors(common::unix_time_ms())
            .into_iter()
            .flat_map(|executor| executor.recent_placements)
            .find(|decision| decision.request_id == request_id)
            .unwrap_or_else(|| panic!("{job} acquired no fresh home"))
    }

    /// Attach a third machine, neither of the node's first two.
    async fn attach_third(&self) -> PathBuf {
        let home = self.machines.temp.path().join("home-c");
        let (orch, url, project, attached) = (
            self.orch().clone(),
            self.machines.object_url.clone(),
            self.machines.project_id.clone(),
            home.clone(),
        );
        // Opening an executor runtime reads and writes its home, which the
        // executor's blocking guard admits only inside its own offload.
        cairn_exec::blocking::offload(move || {
            common::attach_isolated_test_executor_as(&orch, MACHINE_C, attached, url, project);
        })
        .await;
        home
    }

    /// How many times the scratch's head has advanced.
    async fn head_seq(&self) -> i64 {
        self.machines
            .db
            .query_opt_i64("SELECT MAX(head_seq) FROM node_scratches", ())
            .await
            .unwrap()
            .unwrap_or(0)
    }

    /// Wait for the scratch's head to have advanced `seq` times.
    async fn head_reaches(&self, seq: i64) {
        for _ in 0..400 {
            if self.head_seq().await >= seq {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the scratch head never advanced {seq} times");
    }

    fn disconnect(&self, machine: &str) {
        let generation = self
            .orch()
            .fleet
            .managed_generation(machine, &format!("{machine}-device"))
            .expect("the machine is attached");
        assert!(self
            .orch()
            .fleet
            .disconnect_advertised_executor(machine, generation));
    }
}

/// Wait for `done`, which watches effects a background teardown produces.
pub(crate) async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..200 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Acceptance 1: a file a parent batch writes to `$TMPDIR` on one machine is
/// read by a sub-agent task's first batch, placed on another machine with no
/// selector because the first is drained.
#[tokio::test(flavor = "multi_thread")]
async fn a_task_placed_elsewhere_reads_what_its_parent_wrote_to_tmpdir() {
    if common::skip_if_fenced("a_task_placed_elsewhere_reads_what_its_parent_wrote_to_tmpdir") {
        return;
    }
    let node = node().await;
    let wrote = node
        .shell(
            "r-1",
            Some(MACHINE_A),
            "mkdir -p \"$TMPDIR/extract/pilot\" && printf 'id,value\\n1,2\\n' > \"$TMPDIR/extract/pilot/rows.csv\"",
        )
        .await;
    assert!(wrote.contains("wrote back"), "{wrote}");
    node.drain(MACHINE_A, true).await;
    let task = node.add_task("review").await;
    let read = node
        .shell(&task, None, "cat \"$TMPDIR/extract/pilot/rows.csv\"")
        .await;
    assert!(
        read.contains("1,2"),
        "the task did not see its parent's data: {read}"
    );
    assert!(
        read.contains(&format!("brought current on {MACHINE_B}")),
        "the task ran on the other machine and synced there: {read}"
    );
}

/// Acceptance 1, reversed (comment #3 on cairn/5253, cairn/5389): a sub-agent
/// task writes into its `$TMPDIR` on one machine and completes, and the
/// parent's next batch, on the other machine, reads what the task wrote. The
/// task's result names what the flow back cost, and the parent's what bringing
/// it current cost.
#[tokio::test(flavor = "multi_thread")]
async fn a_parent_reads_what_its_task_wrote_on_another_machine() {
    if common::skip_if_fenced("a_parent_reads_what_its_task_wrote_on_another_machine") {
        return;
    }
    let node = node().await;
    // The parent has used the scratch on A, so A holds a replica of its own.
    node.shell(
        "r-1",
        Some(MACHINE_A),
        "echo parent > \"$TMPDIR/parent.txt\"",
    )
    .await;
    let task = node.add_task("extract").await;
    let wrote = node
        .shell(
            &task,
            Some(MACHINE_B),
            "mkdir -p \"$TMPDIR/out\" && printf 'rows=42\\n' > \"$TMPDIR/out/summary.txt\"",
        )
        .await;
    let flow_back = wrote
        .lines()
        .find(|line| line.starts_with("Scratch:"))
        .unwrap_or_else(|| panic!("no scratch line on the task's result: {wrote}"));
    assert!(
        flow_back.contains("wrote back ") && flow_back.contains(" s;"),
        "the flow back's bytes and seconds: {flow_back}"
    );
    // The task completes; nothing of it is left running.
    node.machines
        .db
        .execute_script("UPDATE jobs SET status = 'completed' WHERE id = 'job-extract';")
        .await
        .unwrap();

    let read = node
        .shell(
            "r-1",
            Some(MACHINE_A),
            "cat \"$TMPDIR/out/summary.txt\" \"$TMPDIR/parent.txt\"",
        )
        .await;
    assert!(
        read.contains("rows=42"),
        "the parent did not see what its task wrote: {read}"
    );
    assert!(read.contains("parent"), "{read}");
    assert!(
        read.contains(&format!("brought current on {MACHINE_A} (+")),
        "the parent's machine synced what the task wrote: {read}"
    );
}

/// Acceptance 2: consecutive batches of one node on two machines see the same
/// scratch, and the second's run result reports the sync's bytes and duration.
#[tokio::test(flavor = "multi_thread")]
async fn consecutive_batches_on_two_machines_share_a_scratch_and_report_the_sync() {
    if common::skip_if_fenced(
        "consecutive_batches_on_two_machines_share_a_scratch_and_report_the_sync",
    ) {
        return;
    }
    let node = node().await;
    node.shell(
        "r-1",
        Some(MACHINE_A),
        "head -c 300000 /dev/zero > \"$TMPDIR/blob\" && echo first > \"$TMPDIR/note\"",
    )
    .await;
    let second = node
        .shell(
            "r-1",
            Some(MACHINE_B),
            "cat \"$TMPDIR/note\"; wc -c < \"$TMPDIR/blob\"",
        )
        .await;
    assert!(second.contains("first"), "{second}");
    assert!(second.contains("300000"), "{second}");
    let line = second
        .lines()
        .find(|line| line.starts_with("Scratch: brought current on scratch-b (+"))
        .unwrap_or_else(|| panic!("no sync line: {second}"));
    assert!(
        line.contains(" s from "),
        "bytes, seconds and source: {line}"
    );
    assert!(line.contains("of 512 MiB"), "{line}");
}

/// Acceptance 3 (concurrency): two task batches on two machines change the
/// same path at once; both versions survive and the conflict is named.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_task_batches_on_one_path_keep_both_versions() {
    if common::skip_if_fenced("concurrent_task_batches_on_one_path_keep_both_versions") {
        return;
    }
    let node = node().await;
    let first = node.add_task("one").await;
    let second = node.add_task("two").await;
    let (on_a, on_b) = tokio::join!(
        node.shell(
            &first,
            Some(MACHINE_A),
            "sleep 1; echo from-a > \"$TMPDIR/same.txt\""
        ),
        node.shell(
            &second,
            Some(MACHINE_B),
            "sleep 1; echo from-b > \"$TMPDIR/same.txt\""
        ),
    );
    let both = format!("{on_a}\n{on_b}");
    assert!(both.contains("Scratch conflict: `same.txt`"), "{both}");
    let after = node
        .shell("r-1", Some(MACHINE_A), "cat \"$TMPDIR\"/same.txt*")
        .await;
    assert!(
        after.contains("from-a") && after.contains("from-b"),
        "{after}"
    );
}

/// Acceptance 4: a write-back past the ceiling is refused by name and moves
/// nothing; the files stay on the machine until the scratch shrinks.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_back_past_the_ceiling_is_refused_by_name_and_resumes_once_it_fits() {
    if common::skip_if_fenced(
        "a_write_back_past_the_ceiling_is_refused_by_name_and_resumes_once_it_fits",
    ) {
        return;
    }
    let node = node_with(Some("scratch:\n  ceilingMiB: 1\n")).await;
    let refused = node
        .shell(
            "r-1",
            Some(MACHINE_A),
            "head -c 2097152 /dev/zero > \"$TMPDIR/export.bin\"",
        )
        .await;
    for named in ["Scratch write-back refused", OWNER, "1 MiB", "`export.bin`"] {
        assert!(refused.contains(named), "missing {named}: {refused}");
    }
    let again = node
        .shell("r-1", Some(MACHINE_A), "wc -c < \"$TMPDIR/export.bin\"")
        .await;
    assert!(again.contains("2097152"), "the files stayed: {again}");
    assert!(again.contains("Scratch write-back refused"), "{again}");
    let elsewhere = node
        .shell(
            "r-1",
            Some(MACHINE_B),
            "test -f \"$TMPDIR/export.bin\" && echo PRESENT || echo ABSENT",
        )
        .await;
    assert!(elsewhere.contains("ABSENT"), "{elsewhere}");
    assert!(
        elsewhere.contains("Not included: scratch-a's writes refused by the ceiling"),
        "{elsewhere}"
    );
    // Deleting the refused files alone leaves nothing to write back. The
    // batch's start still met the refusal (the file was there then), but its
    // result describes the scratch as it ends (cairn/5910).
    let deleted = node
        .shell("r-1", Some(MACHINE_A), "rm \"$TMPDIR/export.bin\"")
        .await;
    assert!(
        !deleted.contains("Scratch write-back refused"),
        "a refusal of deleted files is not repeated: {deleted}"
    );
    assert!(
        deleted.contains("Scratch write-back refusal cleared"),
        "{deleted}"
    );
    let shrunk = node
        .shell("r-1", Some(MACHINE_A), "echo summary > \"$TMPDIR/summary\"")
        .await;
    assert!(!shrunk.contains("refus"), "{shrunk}");
    assert!(shrunk.contains("wrote back"), "{shrunk}");
}

/// Acceptance 5 (cairn/5784): a live terminal on a machine draws the node's
/// next batch with no selector there by affinity, and when that machine cannot
/// take it the batch runs elsewhere, never refused, and says why.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_terminal_draws_unselected_batches_by_affinity_and_never_refuses_them() {
    if common::skip_if_fenced(
        "a_live_terminal_draws_unselected_batches_by_affinity_and_never_refuses_them",
    ) {
        return;
    }
    let node = node().await;
    // The builder's home lands on a, where the terminal then lives.
    node.drain(MACHINE_B, true).await;
    node.shell("r-1", None, "true").await;
    node.start_terminal("dev", "echo held > \"$TMPDIR/term\"; sleep 120")
        .await;
    node.drain(MACHINE_B, false).await;
    node.drain(MACHINE_A, false).await;
    // A task's own home could be anywhere; the terminal draws it.
    let task = node.add_task("probe").await;
    let where_ran = node.shell(&task, None, "echo \"tmp=$TMPDIR\"").await;
    let home_a = node.machines.homes[0].display().to_string();
    assert!(
        where_ran.contains(&format!("tmp={home_a}")),
        "the batch ran beside the terminal: {where_ran}"
    );
    assert!(
        where_ran.contains("beside terminal `dev`") && where_ran.contains("affinity, not a pin"),
        "{where_ran}"
    );
    node.disconnect(MACHINE_A);
    let elsewhere = node.shell(&task, None, "echo \"tmp=$TMPDIR\"").await;
    let home_b = node.machines.homes[1].display().to_string();
    assert!(
        elsewhere.contains(&format!("tmp={home_b}")),
        "the batch ran on the machine that could take it: {elsewhere}"
    );
    assert!(elsewhere.contains("terminal `dev` on"), "{elsewhere}");
    assert!(
        elsewhere.contains("placed without that affinity"),
        "{elsewhere}"
    );
}

/// cairn/5784: two terminals of one node on two machines both start, each
/// writing a file of its own and both writing one path. Once both have
/// exited, a batch on a third machine sees both files, and the shared path as
/// one winner plus the other's `~conflict-<writer>` copy.
#[tokio::test(flavor = "multi_thread")]
async fn two_terminals_on_two_machines_both_start_and_their_writes_merge() {
    if common::skip_if_fenced("two_terminals_on_two_machines_both_start_and_their_writes_merge") {
        return;
    }
    let node = node().await;
    node.attach_third().await;
    let task = node.add_task("side").await;
    // Both wait for one go-file, so each has synced before either writes back.
    let go = node.machines.temp.path().join("go");
    let script = |own: &str, from: &str| {
        format!(
            "while [ ! -f {go} ]; do sleep 0.1; done; echo {from} > \"$TMPDIR/{own}\"; echo {from} > \"$TMPDIR/shared.txt\"",
            go = go.display()
        )
    };
    // Both start: a live process never refuses another.
    node.start_terminal_of(
        "r-1",
        OWNER,
        "left",
        Some(MACHINE_A),
        &script("a.txt", "from-a"),
    )
    .await;
    node.start_terminal_of(
        &task,
        &format!("{OWNER}/task/side"),
        "right",
        Some(MACHINE_B),
        &script("b.txt", "from-b"),
    )
    .await;
    let holders = cairn_core::internal::node_scratch::holders::holders(
        node.orch(),
        &node.machines.db,
        "job-1",
    )
    .await;
    assert_eq!(
        holders.len(),
        2,
        "both terminals hold the scratch: {holders:?}"
    );
    assert_ne!(holders[0].executor_id, holders[1].executor_id);

    std::fs::write(&go, "").unwrap();
    node.head_reaches(2).await;
    let read = node
        .shell(
            "r-1",
            Some(MACHINE_C),
            "cd \"$TMPDIR\" && cat a.txt b.txt && for f in shared.txt*; do echo \"$f=$(cat \"$f\")\"; done",
        )
        .await;
    assert!(read.contains("from-a") && read.contains("from-b"), "{read}");
    let shared: Vec<&str> = read
        .lines()
        .filter(|line| line.starts_with("shared.txt"))
        .collect();
    assert_eq!(shared.len(), 2, "one winner and one conflict copy: {read}");
    assert!(
        shared.iter().any(|line| line.starts_with("shared.txt=")),
        "{read}"
    );
    assert!(
        shared
            .iter()
            .any(|line| line.starts_with("shared.txt~conflict-")),
        "{read}"
    );
    assert!(
        shared.iter().any(|line| line.ends_with("=from-a"))
            && shared.iter().any(|line| line.ends_with("=from-b")),
        "nothing dropped: {read}"
    );
}

/// cairn/5875's first specimen, through the `run` tool: a fresh job's batch
/// acquires its home under its own selector, whatever else is attached, and the
/// job's later batches follow that home. Read from the placement record, so it
/// holds whichever machine would have won without the selector.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_founds_its_home_under_the_selector_it_declared() {
    if common::skip_if_fenced("a_batch_founds_its_home_under_the_selector_it_declared") {
        return;
    }
    let node = node().await;
    node.attach_colocated().await;
    let local = ExecutorSelector {
        name: Some("local".to_owned()),
        ..ExecutorSelector::default()
    };

    let named = node
        .shell("r-1", Some("local"), "echo \"tmp=$TMPDIR\"")
        .await;
    assert!(named.starts_with("Ran on **local**"), "{named}");
    assert!(!named.contains("detached"), "it runs in its home: {named}");
    assert_eq!(node.home_acquisition_of("job-1").selector, Some(local));

    let later = node.shell("r-1", None, "echo \"tmp=$TMPDIR\"").await;
    assert!(
        later.starts_with("Ran on **local**"),
        "a batch with no selector follows the home the first one founded: {later}"
    );
}

/// The guess half (tenet 1395): a batch a holder's affinity sends to the
/// runner's machine founds its home under nothing, because a guess is not a
/// declaration, while the batch itself still goes where the guess said.
#[tokio::test(flavor = "multi_thread")]
async fn a_scratch_affinity_guess_places_the_batch_and_never_selects_its_home() {
    if common::skip_if_fenced(
        "a_scratch_affinity_guess_places_the_batch_and_never_selects_its_home",
    ) {
        return;
    }
    let node = node().await;
    node.attach_colocated().await;
    let task = node.add_task("side").await;
    node.start_terminal_of(
        &task,
        &format!("{OWNER}/task/side"),
        "held",
        Some("local"),
        "sleep 120",
    )
    .await;

    let guessed = node.shell("r-1", None, "echo \"tmp=$TMPDIR\"").await;
    assert!(guessed.starts_with("Ran on **local**"), "{guessed}");
    assert!(
        guessed.contains("placed on local beside terminal `held`"),
        "the guess placed the batch: {guessed}"
    );
    assert_eq!(
        node.home_acquisition_of("job-1").selector,
        None,
        "the home was acquired under no selector"
    );
}

/// cairn/5784: with live terminals on two machines, a batch that names no
/// machine goes to the warmer holder when both can take it, even past its own
/// job's environment, and elsewhere when neither can, naming the affinity
/// either way.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_prefers_the_warmer_of_two_holders_and_goes_elsewhere_when_neither_can_take_it() {
    if common::skip_if_fenced(
        "a_batch_prefers_the_warmer_of_two_holders_and_goes_elsewhere_when_neither_can_take_it",
    ) {
        return;
    }
    let node = node().await;
    let task = node.add_task("side").await;
    node.start_terminal_of("r-1", OWNER, "left", Some(MACHINE_A), "sleep 120")
        .await;
    node.start_terminal_of(
        &task,
        &format!("{OWNER}/task/side"),
        "right",
        Some(MACHINE_B),
        "sleep 120",
    )
    .await;
    // A write-back from b moves the head past what a holds.
    node.shell(
        &task,
        Some(MACHINE_B),
        "head -c 200000 /dev/zero > \"$TMPDIR/warm-on-b\"",
    )
    .await;

    // The builder's own environment is on a, but b is warmer.
    let warmer = node
        .shell(
            "r-1",
            None,
            "echo \"tmp=$TMPDIR\"; wc -c < \"$TMPDIR/warm-on-b\"",
        )
        .await;
    let home_b = node.machines.homes[1].display().to_string();
    assert!(
        warmer.contains(&format!("tmp={home_b}")),
        "placed on the warmer holder: {warmer}"
    );
    assert!(warmer.contains("200000"), "{warmer}");
    assert!(
        warmer.contains(&format!("placed on {MACHINE_B} beside terminal `right`")),
        "{warmer}"
    );

    // Neither holder's machine can take work now: a third machine does.
    let home_c = node.attach_third().await;
    node.drain(MACHINE_A, true).await;
    node.drain(MACHINE_B, true).await;
    let probe = node.add_task("probe").await;
    let elsewhere = node.shell(&probe, None, "echo \"tmp=$TMPDIR\"").await;
    assert!(
        elsewhere.contains(&format!("tmp={}", home_c.display())),
        "placed where it could be held: {elsewhere}"
    );
    for named in [
        "terminal `left` on",
        "terminal `right` on",
        "placed without that affinity",
    ] {
        assert!(elsewhere.contains(named), "{named}: {elsewhere}");
    }
}

/// cairn/5784: what a live terminal writes reaches a batch on another machine
/// on its executor's periodic write-back pass, while the terminal still runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_terminals_writes_reach_another_machine_without_stopping_it() {
    if common::skip_if_fenced("a_live_terminals_writes_reach_another_machine_without_stopping_it") {
        return;
    }
    let node = node().await;
    let written = node.machines.temp.path().join("written");
    node.start_terminal_of(
        "r-1",
        OWNER,
        "server",
        Some(MACHINE_A),
        &format!(
            "echo serving > \"$TMPDIR/live.txt\"; touch {}; sleep 120",
            written.display()
        ),
    )
    .await;
    eventually("the terminal's write", || written.is_file()).await;
    // What the executor's timer runs; a pass already under way makes one call
    // a no-op, so call until the head moves.
    for _ in 0..100 {
        node.machines.pools[0]
            .retry_node_scratch_write_backs()
            .await;
        if node.head_seq().await >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        node.head_seq().await >= 1,
        "the pass sent the live terminal's replica"
    );
    let read = node
        .shell("r-1", Some(MACHINE_B), "cat \"$TMPDIR/live.txt\"")
        .await;
    assert!(read.contains("serving"), "{read}");
}

/// Acceptance 6: resolving the issue removes the runner store and every
/// machine's replica, and a machine offline at close is cleaned when it
/// reconnects.
#[tokio::test(flavor = "multi_thread")]
async fn resolving_the_issue_removes_the_scratch_everywhere_even_from_a_machine_offline_at_close() {
    if common::skip_if_fenced(
        "resolving_the_issue_removes_the_scratch_everywhere_even_from_a_machine_offline_at_close",
    ) {
        return;
    }
    let node = node().await;
    node.shell("r-1", Some(MACHINE_A), "echo a > \"$TMPDIR/a\"")
        .await;
    node.shell("r-1", Some(MACHINE_B), "cat \"$TMPDIR/a\"")
        .await;
    assert_eq!(node.replicas_on(0).len(), 1);
    assert_eq!(node.replicas_on(1).len(), 1);
    assert_eq!(node.stores().len(), 1);
    node.disconnect(MACHINE_B);

    // What issue teardown calls for every resolved issue (`cleanup_issue_jobs`).
    cairn_core::internal::node_scratch::lifecycle::close_for_issue(node.orch(), "issue-1").await;
    eventually("the runner store to go", || node.stores().is_empty()).await;
    eventually("a's replica to go", || node.replicas_on(0).is_empty()).await;
    assert_eq!(node.replicas_on(1).len(), 1, "b was offline at close");

    // Opening an executor runtime reads and writes its home, which the
    // executor's blocking guard admits only inside its own offload.
    let (orch, home, url, project) = (
        node.orch().clone(),
        node.machines.homes[1].clone(),
        node.machines.object_url.clone(),
        node.machines.project_id.clone(),
    );
    cairn_exec::blocking::offload(move || {
        common::attach_isolated_test_executor_as(&orch, MACHINE_B, home, url, project);
    })
    .await;
    eventually("b's replica to go once it reconnects", || {
        node.replicas_on(1).is_empty()
    })
    .await;
}

/// Acceptance 6, for threads: closing a thread removes its scratch.
#[tokio::test(flavor = "multi_thread")]
async fn closing_the_thread_removes_its_scratch() {
    if common::skip_if_fenced("closing_the_thread_removes_its_scratch") {
        return;
    }
    let node = node().await;
    // The same job, re-shaped as a thread's session job.
    node.machines
        .db
        .execute_script(format!(
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('thread-1', '{project}', 'document', 'active', 'none', 1, 1);
             UPDATE jobs SET thread_id = 'thread-1', issue_id = NULL, uri_segment = 'thread'
               WHERE id = 'job-1';
             UPDATE runs SET issue_id = NULL WHERE id = 'r-1';
             UPDATE executions SET issue_id = NULL, seq = NULL WHERE id = 'exec-1';
             DELETE FROM issues WHERE id = 'issue-1';",
            project = node.machines.project_id
        ))
        .await
        .unwrap();
    cairn_core::threads::db_store::use_test_path(
        "thread-1",
        node.machines
            .temp
            .path()
            .join("thread-databases/thread-1.db"),
    );
    node.shell("r-1", Some(MACHINE_A), "echo t > \"$TMPDIR/t\"")
        .await;
    assert_eq!(node.replicas_on(0).len(), 1);
    let closed = common::change_resource(
        node.orch(),
        json!([{
            "target": "cairn://p/nsc/document",
            "mode": "patch",
            "payload": { "status": "closed" }
        }]),
    )
    .await;
    assert!(!closed.to_lowercase().contains("error"), "{closed}");
    eventually("the thread's store to go", || node.stores().is_empty()).await;
    eventually("the thread's replica to go", || {
        node.replicas_on(0).is_empty()
    })
    .await;
}
