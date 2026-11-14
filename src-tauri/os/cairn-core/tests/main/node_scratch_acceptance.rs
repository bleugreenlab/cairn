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

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use cairn_common::executor_protocol::ObjectTransferCoordinate;
use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::run::handle_run;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::object_plane::{
    object_fetch_refusal, open_runner_fetch, RunnerFetchRequest,
};
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
pub(crate) const OWNER: &str = "cairn://p/nsc/1/1/builder";
/// The run [`node`] seeds for [`OWNER`]'s job.
const OWNER_RUN: &str = "r-1";
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
    /// Each machine's executor, held so both machines stay attached.
    pub _pools: Vec<cairn_exec::Fleet>,
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
    common::load_fleet_settings(&orch);
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
    for machine in [MACHINE_A, MACHINE_B] {
        wait_for_machine_readings(&orch, machine).await;
    }
    TwoMachines {
        temp,
        orch,
        db,
        project_id,
        repository,
        base,
        _pools: pools,
    }
}

// Attach establishes a link, not the first telemetry sample. Unselected
// placement needs the measured CPU and memory, including on a newly added host.
async fn wait_for_machine_readings(orch: &Orchestrator, machine: &str) {
    eventually(&format!("{machine} placement readings"), || {
        orch.fleet
            .inspect_executors(common::unix_time_ms())
            .iter()
            .any(|executor| {
                executor.health.identity.executor_id == machine
                    && executor.health.machine.cpu.value().is_some()
                    && executor.health.machine.memory.value().is_some()
                    && executor
                        .health
                        .host
                        .cpu_admission
                        .fresh(common::unix_time_ms())
            })
    })
    .await;
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
                .ok()
        });
    if authenticated.is_none()
        || !server
            .orch
            .object_plane
            .authorizes(&request.coordinate, &request.want_commit)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    match open_runner_fetch(RunnerFetchRequest {
        repository: server.repository.clone(),
        scratch: server.repository.with_extension("fetch-scratch"),
        coordinate: request.coordinate,
        want_commit: request.want_commit,
        have_commits: request.have_commits,
        limits: Default::default(),
    })
    .await
    {
        Ok(stream) => stream.into_response(),
        Err(failure) => object_fetch_refusal(&failure),
    }
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
    pub(crate) cwd: String,
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
    pub(crate) fn orch(&self) -> &Orchestrator {
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

    pub(crate) async fn run(&self, run_id: &str, payload: Value) -> String {
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
        cairn_core::internal::fleet::management::mutate_fleet(self.orch(), |config| {
            if drained {
                config.drains.impose_thread(
                    machine,
                    OWNER,
                    &cairn_common::identity::PrincipalRef::Agent {
                        node: OWNER.into(),
                        run_id: Some(OWNER_RUN.into()),
                    },
                    "scratch acceptance fixture",
                    1,
                )
            } else {
                config.drains.clear_thread(machine, OWNER)
            }
        })
        .unwrap();
    }

    /// A terminal of the job behind `run_id`, whose node is `node_uri`, on
    /// `machine` when one is named.
    pub(crate) async fn start_terminal_of(
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
    pub(crate) async fn attach_colocated(&self) {
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

/// The guess half (tenet 1395): a batch a holder's affinity sends to the
/// runner's machine founds its home under nothing, because a guess is not a
/// declaration, while the batch itself still goes where the guess said.
#[tokio::test(flavor = "multi_thread")]
async fn a_scratch_affinity_guess_places_the_batch_and_never_selects_its_home() {
    // The held terminal and the batch both spawn wrapped in a Seatbelt profile,
    // which a process that cannot nest one sees refused at `sandbox_apply`.
    if common::skip_if_fenced(
        "a_scratch_affinity_guess_places_the_batch_and_never_selects_its_home",
    ) || cairn_sandbox::skip_without_nested_sandbox(
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
