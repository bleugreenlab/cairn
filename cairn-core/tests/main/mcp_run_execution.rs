//! Execution-substrate tests for the `run` tool: async non-blocking wait,
//! kill-at-the-bound on timeout, bounded reader reaping, and chained-command
//! capture. See CAIRN-1620.

use crate::common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_core::internal::agent_process::process::RunHandle;
use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::run::handle_run;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::RealProcessSpawner;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, Fence, RecipeSnapshot, RecipeTrigger, TriggerContext,
    TriggerType,
};
use cairn_db::turso::params;
use serde_json::{json, Value};
use tempfile::TempDir;

fn orchestrator(temp: &TempDir, db: Arc<LocalDb>) -> Orchestrator {
    let orch = orchestrator_without_executor(temp, db);
    common::attach_test_executor(&orch);
    orch
}

/// What a batch says when it reached placement and found no execution
/// environment there.
///
/// Deliberately the agent-facing phrase and not the typed reason behind it:
/// substrate vocabulary such as `ExecutorUnavailable` must never appear in text
/// an agent reads, so asserting on the type's name pins a leak rather than the
/// contract, and passes only for as long as the leak lasts. The refusal is built
/// in `mcp::handlers::run`.
const REACHED_PLACEMENT: &str = "environment could not be reached";
const DURABLE_SUSPEND: &str = "Run handed off to durable suspend";

/// The canon invariant, end to end: a search over tracked content is served
/// from the job's head coordinate with no executor in the fleet at all. This
/// orchestrator has nothing to place work onto, so if any part of the batch
/// reached lease acquisition or slot placement it could not have succeeded.
#[tokio::test]
async fn managed_searches_are_served_without_any_executor() {
    let (_temp, _db, orch, cwd) = setup_without_executor("run-search-served").await;

    // Each expectation is what the real tool prints for that exact invocation
    // with its output captured rather than attached to a terminal: ripgrep omits
    // line numbers unless asked, grep's `-n` supplies them.
    for (command, expected) in [
        ("rg initial .", "./README.md:initial"),
        ("grep -rn initial .", "./README.md:1:initial"),
        ("rg -n initial .", "./README.md:1:initial"),
    ] {
        let result = handle_run(
            &orch,
            &request(
                &cwd,
                Some("run-search-served"),
                json!({ "commands": [{ "command": command }] }),
            ),
        )
        .await;
        let text = run_text(&result);
        assert!(
            text.contains(expected),
            "{command} should be served from the head coordinate as {expected:?}: {text}"
        );
        assert!(
            !text.contains(REACHED_PLACEMENT),
            "{command} must never reach placement: {text}"
        );
    }
}

/// The same fixture proves the assertion above has teeth. Anything that cannot
/// be reproduced exactly falls through to real execution, which here means it
/// still needs the executor that does not exist — and it arrives there without
/// any complaint about unsupported flags.
#[tokio::test]
async fn unservable_shapes_fall_through_to_real_execution() {
    let (_temp, _db, orch, cwd) = setup_without_executor("run-search-fallthrough").await;

    let batches = [
        // Not a search at all.
        json!({ "commands": [{ "command": "echo hello" }] }),
        // A search mixed with a build: the batch executes whole, because
        // splitting it would break its ordering guarantees.
        json!({ "commands": [{ "command": "rg initial ." }, { "command": "echo hello" }] }),
        // A pipeline stage no post-filter can represent.
        json!({ "commands": [{ "command": "rg initial . | awk '{print $1}'" }] }),
        // `rg --files` has no grep projection to serve.
        json!({ "commands": [{ "command": "rg --files" }] }),
    ];

    for payload in batches {
        let result = handle_run(
            &orch,
            &request(&cwd, Some("run-search-fallthrough"), payload.clone()),
        )
        .await;
        let text = run_text(&result);
        assert!(
            text.contains(REACHED_PLACEMENT),
            "{payload} must fall through to real execution: {text}"
        );
        assert!(
            !text.to_lowercase().contains("does not yet support"),
            "a fall-through must not report a coverage gap to the agent: {text}"
        );
    }
}

fn orchestrator_without_executor(temp: &TempDir, db: Arc<LocalDb>) -> Orchestrator {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_process(RealProcessSpawner)
            .build(),
    );
    Orchestrator::builder(db_state, services, temp.path().join("config")).build()
}

fn git_output(repo: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap_or_else(|e| panic!("failed to run git {args:?}: {e}"))
}

fn git(repo: &Path, args: &[&str]) {
    let output = git_output(repo, args);
    assert!(output.status.success(), "git {args:?} failed");
}

fn init_git_repo(repo: &Path) {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "user.name", "Test User"]);
    std::fs::write(repo.join("README.md"), "initial\n").unwrap();
    git(repo, &["add", "README.md"]);
    git(repo, &["commit", "-m", "initial"]);
}

fn agent_snapshot() -> AgentSnapshot {
    AgentSnapshot {
        id: "agent-1".to_string(),
        name: "Builder".to_string(),
        description: String::new(),
        prompt: String::new(),
        tools: Vec::new(),
        tier: None,
        backend_preference: None,
        selection: None,
        model: None,
        disallowed_tools: None,
        skills: None,
        // An Allow fence means no OS confinement is applied, keeping these tests
        // platform-independent while still exercising the run_context-driven
        // promotion path.
        fence: Some(Fence::Allow),
        sandbox: None,
        on_escape: None,
        resolved_backend: None,
        extras: None,
    }
}

async fn seed_run(
    db: &LocalDb,
    project_id: &str,
    _worktree: &Path,
    branch: &str,
    base_commit: &str,
    run_id: &str,
) {
    let mut agents = HashMap::new();
    agents.insert("agent-1".to_string(), agent_snapshot());
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: format!("recipe-{run_id}"),
            name: "Run execution test".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        agents,
        HashMap::new(),
        TriggerContext {
            issue_id: Some(format!("issue-{run_id}")),
            project_id: project_id.to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap();

    let project_id = project_id.to_string();
    let issue_id = format!("issue-{run_id}");
    let exec_id = format!("exec-{run_id}");
    let job_id = format!("job-{run_id}");
    let branch = branch.to_string();
    let base_commit = base_commit.to_string();
    let run_id = run_id.to_string();
    db.write(move |conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let exec_id = exec_id.clone();
        let job_id = job_id.clone();
        let branch = branch.clone();
        let base_commit = base_commit.clone();
        let run_id = run_id.clone();
        let snapshot = snapshot.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                 VALUES (?1, ?2, 1, 'Run execution', 'active', 1, 1)",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES (?1, 'recipe-1', ?2, ?3, 'running', 1, 1, ?4, 'manual')",
                params![exec_id.as_str(), issue_id.as_str(), project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, branch, base_commit, created_at, updated_at)
                 VALUES (?1, ?2, 'agent-1', ?3, ?4, 'builder', 'running', 'builder', ?5, ?6, 1, 1)",
                params![
                    job_id.as_str(),
                    exec_id.as_str(),
                    issue_id.as_str(),
                    project_id.as_str(),
                    branch.as_str(),
                    base_commit.as_str()
                ],
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at, start_mode)
                 VALUES (?1, ?2, ?3, ?4, 'live', 1, 1, 'resume')",
                params![run_id.as_str(), project_id.as_str(), issue_id.as_str(), job_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn setup(run_id: &str) -> (TempDir, Arc<LocalDb>, Orchestrator, String) {
    setup_with_executor(run_id, true).await
}

async fn setup_without_executor(run_id: &str) -> (TempDir, Arc<LocalDb>, Orchestrator, String) {
    setup_with_executor(run_id, false).await
}

async fn setup_with_executor(
    run_id: &str,
    attach_executor: bool,
) -> (TempDir, Arc<LocalDb>, Orchestrator, String) {
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "RHG", &project_repo).await;
    let worktree = temp.path().join("worktree");
    let branch = "agent/RHG-1-builder-0";
    let base_commit = common::head_sha(&project_repo);
    seed_run(&db, &project_id, &worktree, branch, &base_commit, run_id).await;
    let orch = if attach_executor {
        orchestrator(&temp, db.clone())
    } else {
        orchestrator_without_executor(&temp, db.clone())
    };
    common::provision_jj_workspace(
        &temp.path().join("config"),
        &project_repo,
        &worktree,
        branch,
    );
    let cwd = worktree.display().to_string();
    (temp, db, orch, cwd)
}

/// A callback exactly as the MCP transport delivers one.
///
/// `tool_use_id: None` is the transport's real shape, not an omission: MCP
/// `tools/call` carries no provider tool-use id, so `cairn-cmd` sends `None` for
/// every tool and every MCP-hosted agent arrives here this way. A fixture that
/// supplies an id instead exercises a path production cannot reach, which is
/// exactly how the run grace contract shipped dead: both suspension tests were
/// green while no batch on the machine ever suspended (CAIRN-3229).
fn request(cwd: &str, run_id: Option<&str>, payload: Value) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: run_id.map(ToOwned::to_owned),
        tool: "run".to_string(),
        payload,
        tool_use_id: None,
    }
}

/// The other transport: the Cairn-native tool loop dispatches tools itself, so
/// it knows the provider id and supplies it on the callback.
fn native_loop_request(cwd: &str, run_id: Option<&str>, payload: Value) -> McpCallbackRequest {
    McpCallbackRequest {
        tool_use_id: Some("toolu-native-loop".to_string()),
        ..request(cwd, run_id, payload)
    }
}

/// SIGKILL every live terminal session so a test's detached `sleep` does not
/// linger after the test process exits.
fn kill_all_sessions(orch: &Orchestrator) {
    if let Ok(sessions) = orch.pty_state.sessions.lock() {
        for s in sessions.values() {
            if let Ok(mut s) = s.lock() {
                let _ = s.child.kill();
            }
        }
    }
}

async fn count(db: &LocalDb, sql: &'static str) -> i64 {
    common::query_i64(db, sql).await.unwrap()
}

fn run_text(result: &str) -> String {
    serde_json::from_str::<Value>(result).unwrap()["text"]
        .as_str()
        .unwrap()
        .to_string()
}

// ---- One call in, one final result out (CAIRN-3099) ----------------------

/// A grace window no real batch beats, so the suspended path is exercised
/// without waiting the production two minutes. Shortening grace process-wide is
/// safe: a concurrent batch with no parked-able run falls back to awaiting
/// inline and returns exactly the envelope it always did.
const IMMEDIATE_GRACE_MS: &str = "1";

/// Serializes the tests that shorten the grace window against each other, and
/// keeps the override's window as narrow as one `handle_run` call. Async-aware
/// because the guard is deliberately held across that call's await.
static GRACE_OVERRIDE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn git_stdout(repo: &Path, args: &[&str]) -> String {
    let output = git_output(repo, args);
    assert!(output.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The sha out of a composed `✓ Committed changes (<sha>) …` barrier line.
fn committed_sha(text: &str) -> String {
    const OPENING: &str = "Committed changes (";
    let start = text
        .find(OPENING)
        .unwrap_or_else(|| panic!("no commit barrier line in: {text}"))
        + OPENING.len();
    let rest = &text[start..];
    rest[..rest.find(')').expect("unterminated commit sha")].to_string()
}

/// A published commit's identity: the exact patch it landed and the tree it
/// produced. Duration invariance means both are the same for the same batch.
fn published_identity(repo: &Path, sha: &str) -> (String, String) {
    (
        git_stdout(repo, &["show", "--no-ext-diff", "--format=", sha]),
        git_stdout(repo, &["rev-parse", &format!("{sha}^{{tree}}")])
            .trim()
            .to_string(),
    )
}

/// The provider tool-use id recorded on a turn's seeded `run` tool call — the id
/// an MCP-hosted callback can only reach by correlating with the transcript.
/// One per turn, as a provider's own ids are: `agent_waits` holds
/// `UNIQUE(run_id, tool_use_id)`, so reusing one across turns would make a
/// second suspension fail on the constraint rather than on its own merits.
fn provider_tool_use_id(turn_id: &str) -> String {
    format!("toolu-provider-{turn_id}")
}

/// Record the assistant event carrying the `run` tool call a batch came from,
/// the way a live turn's transcript does. This is what an MCP-hosted callback
/// correlates against to find the call its suspension must answer.
async fn record_run_tool_call(db: &LocalDb, run_id: &str, turn_id: &str, batch: &Value) {
    let id = provider_tool_use_id(turn_id);
    record_run_tool_calls(db, run_id, turn_id, &[(&id, batch)]).await
}

/// Record one assistant event carrying SEVERAL `run` tool calls, in the order the
/// provider emitted them — the shape 1.8% of real assistant events have, and the
/// only shape in which two calls can be indistinguishable.
async fn record_run_tool_calls(
    db: &LocalDb,
    run_id: &str,
    turn_id: &str,
    calls: &[(&str, &Value)],
) {
    let tool_uses: Vec<Value> = calls
        .iter()
        .map(|(id, batch)| {
            serde_json::json!({
                "toolUseId": id,
                "name": "mcp__cairn__run",
                "input": batch,
            })
        })
        .collect();
    let event = serde_json::json!({ "toolUses": tool_uses }).to_string();
    let (id, run_id, turn_id) = (
        format!("event-{turn_id}"),
        run_id.to_string(),
        turn_id.to_string(),
    );
    db.write(move |conn| {
        let (id, run_id, turn_id, event) =
            (id.clone(), run_id.clone(), turn_id.clone(), event.clone());
        Box::pin(async move {
            conn.execute(
                // `events(run_id, sequence)` is UNIQUE, and a run records more
                // than one turn here, so allocate rather than hardcode.
                "INSERT INTO events(id,run_id,turn_id,sequence,timestamp,event_type,data,created_at)
                 VALUES(?1,?2,?3,
                   (SELECT COALESCE(MAX(sequence), -1) + 1 FROM events WHERE run_id = ?2),
                   1,'assistant',?4,1)",
                params![id.as_str(), run_id.as_str(), turn_id.as_str(), event.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Open a fresh turn on an already-suspendable run and record `batch` as its
/// `run` tool call, the way a resumed agent's next turn arrives.
async fn open_next_turn(
    db: &LocalDb,
    orch: &Orchestrator,
    run_id: &str,
    turn_id: &str,
    sequence: i64,
    batch: &Value,
) {
    let (job_id, session_id) = (format!("job-{run_id}"), format!("session-{run_id}"));
    let (owned_job, owned_session, owned_turn, owned_run) = (
        job_id.clone(),
        session_id.clone(),
        turn_id.to_string(),
        run_id.to_string(),
    );
    db.write(move |conn| {
        let (job_id, session_id, turn_id, run_id) = (
            owned_job.clone(),
            owned_session.clone(),
            owned_turn.clone(),
            owned_run.clone(),
        );
        Box::pin(async move {
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'running', 'resume', 1, 1)",
                params![turn_id.as_str(), session_id.as_str(), run_id.as_str(), job_id.as_str(), sequence],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = ?2 WHERE id = ?1",
                params![job_id.as_str(), turn_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    orch.process_state.begin_turn(run_id, turn_id);
    record_run_tool_call(db, run_id, turn_id, batch).await;
}

/// Seed the session and turn a durable suspension binds to, register a warm
/// process carrying that turn the way a live agent's run does, and record the
/// `run` tool call `batch` came from. Without the session and turn a batch has
/// nothing to park; without the recorded tool call an MCP-hosted callback has no
/// call to bind its suspension to. Either way the batch keeps awaiting inline.
async fn seed_suspendable_run(db: &LocalDb, orch: &Orchestrator, run_id: &str, batch: &Value) {
    seed_suspension_context(db, orch, run_id).await;
    record_run_tool_call(db, run_id, &format!("turn-{run_id}"), batch).await;
}

/// The same, with the turn's `run` tool calls stated explicitly, for the tests
/// about which of several calls a suspension may claim.
async fn seed_suspendable_run_with_calls(
    db: &LocalDb,
    orch: &Orchestrator,
    run_id: &str,
    calls: &[(&str, &Value)],
) {
    seed_suspension_context(db, orch, run_id).await;
    record_run_tool_calls(db, run_id, &format!("turn-{run_id}"), calls).await;
}

async fn seed_suspension_context(db: &LocalDb, orch: &Orchestrator, run_id: &str) {
    let job_id = format!("job-{run_id}");
    let session_id = format!("session-{run_id}");
    let turn_id = format!("turn-{run_id}");
    let (owned_job, owned_session, owned_turn, owned_run) = (
        job_id.clone(),
        session_id.clone(),
        turn_id.clone(),
        run_id.to_string(),
    );
    db.write(move |conn| {
        let (job_id, session_id, turn_id, run_id) = (
            owned_job.clone(),
            owned_session.clone(),
            owned_turn.clone(),
            owned_run.clone(),
        );
        Box::pin(async move {
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, backend_id, created_at, updated_at)
                 VALUES (?1, ?2, 'open', 'handle-1', 1, 1)",
                params![session_id.as_str(), job_id.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE runs SET session_id = ?2 WHERE id = ?1",
                params![run_id.as_str(), session_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 1, 'running', 'initial', 1, 1)",
                params![turn_id.as_str(), session_id.as_str(), run_id.as_str(), job_id.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_session_id = ?2, current_turn_id = ?3 WHERE id = ?1",
                params![job_id.as_str(), session_id.as_str(), turn_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    {
        let mut processes = orch.process_state.processes.lock().unwrap();
        processes.register(
            run_id.to_string(),
            RunHandle::new(
                Arc::new(std::sync::Mutex::new(None)),
                Arc::new(std::sync::Mutex::new(None)),
                Some(session_id),
                None,
            ),
        );
    }
    orch.process_state.begin_turn(run_id, &turn_id);
}

/// Poll until the suspension records the result it will resume the agent with.
async fn awaited_resolution(db: &LocalDb) -> String {
    for _ in 0..600 {
        let stored = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT resolution_json FROM agent_waits WHERE resolution_json IS NOT NULL LIMIT 1",
                            (),
                        )
                        .await?;
                    rows.next()
                        .await?
                        .map(|row| row.opt_text(0))
                        .transpose()
                        .map(Option::flatten)
                })
            })
            .await
            .unwrap();
        if let Some(value) = stored {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the suspended batch never recorded a resolution");
}

// ---- No room to run is a wait, not an error (CAIRN-3258) -----------------

/// A machine that can run `concurrency_units` commands at once, and an
/// executor queue budget of one second.
///
/// The short budget is what makes the test about placement rather than about
/// queueing: a batch with nowhere to run is genuinely turned away, over and
/// over, instead of quietly waiting its turn in the executor's queue.
async fn setup_impatient_machine(
    run_id: &str,
    memory_budget_bytes: u64,
    max_queue_entries: usize,
) -> (TempDir, Arc<LocalDb>, Orchestrator, String, String) {
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "RHG", &project_repo).await;
    let worktree = temp.path().join("worktree");
    let branch = "agent/RHG-1-builder-0";
    let base_commit = common::head_sha(&project_repo);
    seed_run(&db, &project_id, &worktree, branch, &base_commit, run_id).await;
    let orch = orchestrator_without_executor(&temp, db.clone());
    common::attach_capacity_limited_test_executor(&orch, memory_budget_bytes, max_queue_entries);
    let config = temp.path().join("config");
    common::provision_jj_workspace(&config, &project_repo, &worktree, branch);
    std::fs::write(
        config.join("settings.yaml"),
        "buildSlots:\n  acquisitionDeadlineSeconds: 1\n",
    )
    .unwrap();
    let cwd = worktree.display().to_string();
    (temp, db, orch, cwd, project_id)
}

/// Block until the executor's own published snapshot satisfies `predicate`.
///
/// The capacity test has to know that the machine is *actually* full at the
/// moment the batch under test arrives. Sleeping for a plausible interval does
/// not know that: two occupants submitted together race each other into the
/// queue, and whichever loses is refused instead of waiting in it, leaving the
/// queue empty exactly when the test needs it full. Sequencing on observed state
/// is what makes the refusal deterministic rather than lucky.
async fn await_fleet(
    orch: &Orchestrator,
    expected: &str,
    predicate: impl Fn(&cairn_common::executor_protocol::FleetSnapshot) -> bool,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        if predicate(&orch.fleet.snapshot()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the fleet never reached the state this test needs: {expected}");
}

/// One unbound command that occupies the machine while it sleeps.
///
/// Unbound is the point: an agent's own batch is admitted past the concurrency
/// budget on purpose, so only work that is not an agent batch can hold the
/// machine against one.
fn occupying_request(
    project_id: &str,
    cwd: &str,
    index: usize,
) -> cairn_core::internal::fleet::CellRequest {
    use cairn_common::executor_protocol::{CellCommandClass, RepositoryLocator};
    use cairn_core::internal::fleet::{CellPriority, CellRequest, MutationPolicy};

    CellRequest {
        request_id: format!("occupant-{index}"),
        attempt_id: format!("occupant-{index}-attempt"),
        project_id: project_id.to_string(),
        // In place, so occupying the machine costs no checkout provisioning.
        repository: RepositoryLocator::ExistingCheckout {
            project_id: project_id.to_string(),
            repository_id: project_id.to_string(),
            absolute_path: cwd.to_string(),
        },
        base_commit: String::new(),
        command: format!("sleep {}", if index == 0 { 8 } else { 1 }),
        command_class: CellCommandClass::Other,
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: CellPriority::ReviewCheck,
        wait_horizon_unix_ms: u64::MAX,
        waiting_since_unix_ms: 0,
        timeout_ms: 30_000,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        executor: None,
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        command_resource_identity: None,
        resource_reservation: cairn_common::executor_protocol::ResourceReservation {
            memory_bytes: 512 * 1024 * 1024,
            ..Default::default()
        },
        learned_estimate: None,
    }
}

/// Capacity is a wait, not an error.
///
/// The machine has room for one command and a long batch takes it, so a second
/// batch cannot be placed at all: its attempts are turned away for as long as
/// the first one runs. The agent must never see that. The call suspends exactly
/// like a batch that is merely slow, nothing wakes it while the machine is
/// full, and the resume carries the real output once room appears.
///
/// Before CAIRN-3258 the second call answered the moment placement was refused,
/// with "This run could not execute … No commands ran" — a fact more tokens
/// could not act on, which agents answered by retrying into the same congestion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_machine_makes_a_batch_slower_not_broken() {
    if common::skip_if_fenced("a_full_machine_makes_a_batch_slower_not_broken") {
        return;
    }
    // Room for one running command and one waiting behind it, so the batch under
    // test is turned away outright rather than merely queued. Merely queued is
    // not the case under test: a queued request is held while the executor
    // reports itself busy, and it would run without any of this change. Only a
    // refusal proves the batch was re-presented instead of failed.
    let (_temp, db, orch, cwd, project_id) =
        setup_impatient_machine("run-capacity", 512 * 1024 * 1024, 1).await;
    let payload = json!({ "commands": [{ "command": "echo room-freed" }] });
    seed_suspendable_run(&db, &orch, "run-capacity", &payload).await;

    // Fill the machine, then its queue, in that order and one at a time. The
    // order is the test: the first occupant must be *executing* before the
    // second is offered, or the second is turned away at the door instead of
    // waiting in the queue, and a queue with room in it refuses nobody.
    //
    // What the batch under test collides with is the queue count bound, which
    // applies to every arrival. It has no execution home yet, so placing it
    // means acquiring one, and acquiring one is itself a cell that has to get
    // through admission. That is where a full machine reaches an agent.
    let mut occupants = Vec::new();
    for index in 0..2 {
        let fleet = orch.fleet.clone();
        let task_orch = orch.clone();
        let request = occupying_request(&project_id, &cwd, index);
        occupants.push(tokio::spawn(async move {
            fleet.submit(&task_orch, request).await
        }));
        let expected = if index == 0 {
            "the first occupant running"
        } else {
            "the second occupant queued behind it"
        };
        let id = format!("occupant-{index}");
        await_fleet(&orch, expected, move |snapshot| {
            if index == 0 {
                snapshot
                    .executing_requests
                    .iter()
                    .any(|executing| executing.request_id == id)
            } else {
                snapshot
                    .queued_requests
                    .iter()
                    .any(|queued| queued.request_id == id)
            }
        })
        .await;
    }

    let marker = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", IMMEDIATE_GRACE_MS);
        let marker =
            run_text(&handle_run(&orch, &request(&cwd, Some("run-capacity"), payload)).await);
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        marker
    };
    assert!(
        marker.contains(DURABLE_SUSPEND),
        "a batch with nowhere to run must suspend, not refuse: {marker}"
    );
    assert!(
        !marker.contains("could not execute"),
        "capacity was reported to the agent as a failure: {marker}"
    );

    let resolution = awaited_resolution(&db).await;
    assert!(
        resolution.contains("room-freed"),
        "the batch never ran once the machine had room: {resolution}"
    );
    assert!(
        !resolution.contains("could not execute"),
        "the resume carried a placement refusal instead of the result: {resolution}"
    );
    for occupant in occupants {
        let _ = occupant.await;
    }
    kill_all_sessions(&orch);
}

/// A request that waits holds ONE place in line, and the panel shows the wait it
/// actually did.
///
/// This is the user-visible half of CAIRN-3268, and the half a refusal test
/// cannot see. The operator's Running panel renders the executor's queue keyed by
/// request id, with a "Waiting" column read from `queued_at_unix_ms`, so the two
/// ways a wait can be misreported are both visible here: the row blinking out
/// when the entry is evicted and re-presented, and the wait resetting to zero
/// when a re-presented request is treated as a new arrival.
///
/// The second assertion is the one only this change can satisfy. The waiting
/// request states that its wait began five minutes ago — what a re-presented
/// request carries — and the queue row has to agree with it. Before this change
/// `queued_at` was minted at enqueue, so a request re-entering the queue reported
/// a wait of zero however long it had really been waiting, and both the panel and
/// the executor's own fairness ranking believed it.
///
/// It runs against non-interactive work on purpose. An agent's own batch, bound
/// to its job's execution home, is admitted past the concurrency budget by
/// design, so it never queues behind an occupant — a full machine reaches it as a
/// refusal at the door instead, which is what
/// `a_full_machine_makes_a_batch_slower_not_broken` covers. Checks and other
/// scheduled work are what genuinely sit in this queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_request_holds_one_place_and_reports_the_wait_it_did() {
    if common::skip_if_fenced("a_waiting_request_holds_one_place_and_reports_the_wait_it_did") {
        return;
    }
    // Room for one command and waiting room behind it, so the second request is
    // queued rather than turned away at the door.
    let (_temp, _db, orch, cwd, project_id) =
        setup_impatient_machine("run-continuity", 512 * 1024 * 1024, 8).await;

    let occupant = {
        let fleet = orch.fleet.clone();
        let task_orch = orch.clone();
        let request = occupying_request(&project_id, &cwd, 0);
        tokio::spawn(async move { fleet.submit(&task_orch, request).await })
    };
    await_fleet(&orch, "the occupant running", |snapshot| {
        snapshot
            .executing_requests
            .iter()
            .any(|executing| executing.request_id == "occupant-0")
    })
    .await;

    // A wait that began five minutes ago and is willing to continue for another
    // fifteen seconds: the two facts a re-presented request carries.
    let began_waiting_at = common::unix_time_ms() - 5 * 60 * 1_000;
    let waiter = {
        let fleet = orch.fleet.clone();
        let task_orch = orch.clone();
        let mut request = occupying_request(&project_id, &cwd, 1);
        request.request_id = "patient-waiter".into();
        request.attempt_id = "patient-waiter-attempt".into();
        request.command = "echo room-freed".into();
        request.waiting_since_unix_ms = began_waiting_at;
        request.wait_horizon_unix_ms = common::unix_time_ms() + 15 * 1_000;
        tokio::spawn(async move { fleet.submit(&task_orch, request).await })
    };
    await_fleet(&orch, "the second request waiting in line", |snapshot| {
        snapshot
            .queued_requests
            .iter()
            .any(|queued| queued.request_id == "patient-waiter")
    })
    .await;

    // Sample across several times the one-second budget this machine is
    // configured with. Each sample is a separate chance to catch an eviction,
    // which shows up as a missing row or as a wait that started over.
    let mut samples = 0;
    for sample in 0..25 {
        let snapshot = orch.fleet.snapshot();
        if snapshot
            .executing_requests
            .iter()
            .all(|executing| executing.request_id != "occupant-0")
        {
            // The occupant finished, so the wait is legitimately over.
            break;
        }
        let row = snapshot
            .queued_requests
            .iter()
            .find(|queued| queued.request_id == "patient-waiter")
            .unwrap_or_else(|| {
                panic!(
                    "sample {sample} lost the waiting row while the machine was still full: {:?}",
                    snapshot.queued_requests
                )
            });
        assert_eq!(
            row.queued_at_unix_ms, began_waiting_at,
            "the panel must show the wait the requester actually did, not the age of this enqueue"
        );
        samples += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        samples >= 3,
        "the wait was never observed for long enough to mean anything ({samples} samples)"
    );

    let outcome = tokio::time::timeout(Duration::from_secs(30), waiter)
        .await
        .expect("the waiting request should be admitted once the occupant finishes")
        .expect("the waiting request's task should not panic");
    assert!(
        matches!(
            outcome,
            cairn_common::executor_protocol::CellOutcome::Completed { .. }
        ),
        "the request that held its place must run once there is room: {outcome:?}"
    );
    let _ = occupant.await;
    kill_all_sessions(&orch);
}

/// The other half of the classifier, and the reason it has to exist.
///
/// A constraint no executor in the fleet can satisfy is not a queue to wait in:
/// waiting would park the agent on something nobody is ever going to clear. So
/// it still refuses, it names what could not be satisfied, and it never parks
/// the call.
#[tokio::test]
async fn an_unsatisfiable_executor_selector_still_refuses_instead_of_queueing() {
    if common::skip_if_fenced(
        "an_unsatisfiable_executor_selector_still_refuses_instead_of_queueing",
    ) {
        return;
    }
    let (_temp, db, orch, cwd, _project_id) =
        setup_impatient_machine("run-unsatisfiable", u64::MAX, usize::MAX).await;
    let text = run_text(
        &handle_run(
            &orch,
            &request(
                &cwd,
                Some("run-unsatisfiable"),
                json!({
                    "commands": [{ "command": "echo never" }],
                    "executor": { "os": "plan9" },
                }),
            ),
        )
        .await,
    );
    assert!(
        text.contains("This run could not execute"),
        "an unsatisfiable executor selector must refuse: {text}"
    );
    assert!(
        text.contains("plan9"),
        "the refusal must name what could not be satisfied: {text}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_waits").await,
        0,
        "a structural refusal must answer its call rather than park it"
    );
}

/// The tracked-writes half of the thread posture, at the `run` door.
///
/// A thread owns no branch, so its job runs ON the base branch: the seal this
/// batch would take at the commit barrier publishes to the project's default
/// branch with no pull request and no review surface. The same batch runs
/// against both fixtures, so the issue's kind is the only variable — an ordinary
/// issue's `commit_msg` lands a real commit carrying the file, and the thread's
/// is refused before the batch's items resolve, so the command never runs.
///
/// The third assertion is the one that keeps this a single mechanism rather than
/// two: a thread's operational dirt without a `commit_msg` is handled by the
/// restore path that already exists, not by anything added here.
#[tokio::test]
async fn a_thread_cannot_commit_from_a_run_and_an_ordinary_issue_still_can() {
    if common::skip_if_fenced("a_thread_cannot_commit_from_a_run_and_an_ordinary_issue_still_can") {
        return;
    }
    let batch = json!({
        "commands": [{ "command": "printf posture > posture.txt" }],
        "commit_msg": "land the marker",
    });

    let (issue_temp, _issue_db, issue_orch, issue_cwd) = setup("run-posture-issue").await;
    let landed = run_text(
        &handle_run(
            &issue_orch,
            &request(&issue_cwd, Some("run-posture-issue"), batch.clone()),
        )
        .await,
    );
    assert!(
        landed.contains("Committed changes"),
        "an ordinary issue's run must still commit: {landed}"
    );
    let landed_files = git_stdout(
        &issue_temp.path().join("project"),
        &["show", "--stat", "--format=", &committed_sha(&landed)],
    );
    assert!(
        landed_files.contains("posture.txt"),
        "the commit must actually carry the batch's file: {landed_files}"
    );

    let (_thread_temp, thread_db, thread_orch, thread_cwd) = setup("run-posture-thread").await;
    thread_db
        .execute(
            "UPDATE issues SET kind = 'thread' WHERE id = 'issue-run-posture-thread'",
            params![],
        )
        .await
        .unwrap();

    let refusal = run_text(
        &handle_run(
            &thread_orch,
            &request(&thread_cwd, Some("run-posture-thread"), batch),
        )
        .await,
    );
    assert!(
        refusal.contains("it is a thread") && refusal.contains("child issue"),
        "the thread must be told the posture and where the work belongs: {refusal}"
    );
    assert!(
        !refusal.contains("Committed changes"),
        "nothing may have been sealed: {refusal}"
    );
    assert!(
        !Path::new(&thread_cwd).join("posture.txt").exists(),
        "a fail-closed refusal is taken before the batch executes: {refusal}"
    );

    let scratch = run_text(
        &handle_run(
            &thread_orch,
            &request(
                &thread_cwd,
                Some("run-posture-thread"),
                json!({ "commands": [{ "command": "printf scratch > scratch.txt" }] }),
            ),
        )
        .await,
    );
    assert!(
        !Path::new(&thread_cwd).join("scratch.txt").exists(),
        "a thread's uncommitted dirt is returned to HEAD by the restore path that \
         already exists, with nothing added for threads: {scratch}"
    );
    assert!(
        scratch.contains("no commit_msg"),
        "and the thread is told why its dirt did not survive: {scratch}"
    );
}

/// Fail-closed means ahead of every branch that can act, not merely early in
/// the executing path.
///
/// A workflow target is the shape that proves the difference. It is a
/// DELEGATION, not a subprocess: `handle_run` dispatches it before the batch's
/// items are ever resolved, starting a workflow node and durably suspending the
/// caller. A thread refusal placed after that dispatch reads as early and is not
/// fail-closed — the thread's `commit_msg` would be silently ignored while the
/// side effect happened anyway.
///
/// The ordinary-issue arm is what gives this teeth. The identical batch
/// demonstrably reaches workflow dispatch, since only that branch can answer
/// about a workflow at all, so the thread's refusal is the guard firing first
/// rather than a target that was never recognized as a workflow.
#[tokio::test]
async fn a_thread_is_refused_before_a_workflow_target_can_suspend_the_caller() {
    let batch = json!({
        "commands": [{ "target": "cairn://workflows/deep-research" }],
        "commit_msg": "should never land",
    });

    let (_issue_temp, _issue_db, issue_orch, issue_cwd) =
        setup_without_executor("run-workflow-issue").await;
    let reached = run_text(
        &handle_run(
            &issue_orch,
            &request(&issue_cwd, Some("run-workflow-issue"), batch.clone()),
        )
        .await,
    );
    assert!(
        reached.contains("deep-research"),
        "an ordinary issue's batch must reach workflow dispatch, or this test proves nothing: {reached}"
    );

    let (_thread_temp, thread_db, thread_orch, thread_cwd) =
        setup_without_executor("run-workflow-thread").await;
    thread_db
        .execute(
            "UPDATE issues SET kind = 'thread' WHERE id = 'issue-run-workflow-thread'",
            params![],
        )
        .await
        .unwrap();

    let refusal = run_text(
        &handle_run(
            &thread_orch,
            &request(&thread_cwd, Some("run-workflow-thread"), batch),
        )
        .await,
    );
    assert!(
        refusal.contains("it is a thread"),
        "the thread must be refused: {refusal}"
    );
    assert!(
        !refusal.contains("deep-research"),
        "the batch must never have reached workflow dispatch: {refusal}"
    );
    assert_eq!(
        count(&thread_db, "SELECT COUNT(*) FROM agent_waits").await,
        0,
        "a refused batch must not have suspended its caller: {refusal}"
    );
    assert_eq!(
        count(&thread_db, "SELECT COUNT(*) FROM jobs").await,
        1,
        "a refused batch must not have created a workflow node: {refusal}"
    );
}

/// The headline contract. The same batch carrying the same `commit_msg`
/// publishes the same commit whether it settled inside the grace window or
/// suspended past it, the suspended call returns a marker rather than a partial
/// result (no intermediate agent wake), and neither shape mints a terminal.
#[tokio::test]
async fn a_batch_publishes_identically_whether_it_settles_fast_or_suspends() {
    if common::skip_if_fenced("a_batch_publishes_identically_whether_it_settles_fast_or_suspends") {
        return;
    }
    let batch = json!({
        "commands": [{ "command": "printf duration-invariant > marker.txt" }],
        "commit_msg": "add marker",
    });

    // Fast: settles well inside the grace window.
    let (fast_temp, fast_db, fast_orch, fast_cwd) = setup("run-invariant-fast").await;
    let fast = run_text(
        &handle_run(
            &fast_orch,
            &request(&fast_cwd, Some("run-invariant-fast"), batch.clone()),
        )
        .await,
    );
    assert!(
        fast.contains("Committed changes"),
        "the fast batch did not publish: {fast}"
    );
    let fast_identity =
        published_identity(&fast_temp.path().join("project"), &committed_sha(&fast));
    assert_eq!(
        count(&fast_db, "SELECT COUNT(*) FROM job_terminals").await,
        0
    );

    // Suspended: the same batch against a grace window it cannot beat.
    let (slow_temp, slow_db, slow_orch, slow_cwd) = setup("run-invariant-slow").await;
    seed_suspendable_run(&slow_db, &slow_orch, "run-invariant-slow", &batch).await;
    let marker = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", IMMEDIATE_GRACE_MS);
        let marker = run_text(
            &handle_run(
                &slow_orch,
                &request(&slow_cwd, Some("run-invariant-slow"), batch),
            )
            .await,
        );
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        marker
    };
    assert!(
        marker.contains(DURABLE_SUSPEND),
        "the batch did not suspend: {marker}"
    );
    assert!(
        !marker.contains("Committed changes"),
        "suspension must not wake the agent with a partial result: {marker}"
    );

    let condition = common::scalar_text_by_id(
        &slow_db,
        "SELECT condition_json FROM agent_waits WHERE run_id = ?1",
        "run-invariant-slow",
    )
    .await
    .expect("a suspended batch records its own wait row");
    assert!(
        condition.contains("\"kind\":\"run_batch\""),
        "unexpected suspension condition: {condition}"
    );

    let resolution = awaited_resolution(&slow_db).await;
    assert!(
        resolution.contains("Committed changes"),
        "the suspended batch resumed without its completed outcome: {resolution}"
    );
    let slow_identity = published_identity(
        &slow_temp.path().join("project"),
        &committed_sha(&resolution),
    );

    assert_eq!(
        fast_identity, slow_identity,
        "elapsed time changed what the batch published"
    );
    assert_eq!(
        count(&slow_db, "SELECT COUNT(*) FROM job_terminals").await,
        0,
        "a batch that outlives its grace window must not mint a terminal"
    );
}

/// An explicit `timeout` is orthogonal to the call's shape: the item is still
/// killed at its bound and still fails with its partial output when the batch as
/// a whole suspended.
#[tokio::test]
async fn an_explicit_timeout_still_kills_inside_a_suspended_batch() {
    if common::skip_if_fenced("an_explicit_timeout_still_kills_inside_a_suspended_batch") {
        return;
    }
    let (_temp, db, orch, cwd) = setup("run-suspended-timeout").await;
    let payload = json!({
        "commands": [{ "command": "echo started; sleep 30", "timeout": PARTIAL_OUTPUT_TIMEOUT_MS }],
    });
    seed_suspendable_run(&db, &orch, "run-suspended-timeout", &payload).await;

    let marker = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", IMMEDIATE_GRACE_MS);
        let marker = run_text(
            &handle_run(
                &orch,
                &request(&cwd, Some("run-suspended-timeout"), payload),
            )
            .await,
        );
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        marker
    };
    assert!(
        marker.contains(DURABLE_SUSPEND),
        "the batch did not suspend: {marker}"
    );

    let resolution = awaited_resolution(&db).await;
    assert!(
        resolution.contains("timed out"),
        "the explicit timeout did not kill the item: {resolution}"
    );
    assert!(
        resolution.contains("started"),
        "the killed item lost its partial output: {resolution}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM job_terminals").await,
        0,
        "a killed item must not turn into a terminal"
    );
    kill_all_sessions(&orch);
}

/// A grace window long enough to time an answer against, and far shorter than
/// the item it races. The assertion is then the production relationship — the
/// host answers inside its own window while the batch is still running — rather
/// than a wall-clock coincidence.
const SCALED_GRACE_MS: &str = "1000";

/// Every `agent_waits` row, oldest first: (tool_use_id, state, resolution).
async fn wait_rows(db: &LocalDb) -> Vec<(String, String, Option<String>)> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT tool_use_id, state, resolution_json FROM agent_waits ORDER BY created_at, rowid",
                    (),
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

/// The defect this file exists to prevent (CAIRN-3229): for the whole life of
/// the grace contract, no `run` batch ever suspended in production. The
/// suspension required `tool_use_id` from the callback, and the MCP transport
/// every agent uses never sends one, so each long batch fell back to awaiting
/// inline and died at cairn-cmd's socket — the agent losing the batch entirely.
///
/// So this drives the real handler over the real transport shape (no tool-use
/// id) and asserts all three halves of the contract: the call answers INSIDE the
/// grace window while its item is still running, the suspension binds to the
/// provider tool call it can only learn from the transcript, and the wait row
/// then resolves with the item's actual result.
#[tokio::test]
async fn a_batch_outliving_grace_suspends_over_the_transport_agents_actually_use() {
    let (_temp, db, orch, cwd) = setup("run-mcp-suspend").await;
    // Runs far past the shortened window, so an inline await could not answer
    // anywhere near it, and prints a marker only completion can produce.
    let batch = json!({
        "commands": [{ "command": "sleep 6; echo settled-past-grace" }],
    });
    seed_suspendable_run(&db, &orch, "run-mcp-suspend", &batch).await;

    let (marker, elapsed) = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", SCALED_GRACE_MS);
        let start = Instant::now();
        let marker =
            run_text(&handle_run(&orch, &request(&cwd, Some("run-mcp-suspend"), batch)).await);
        let elapsed = start.elapsed();
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        (marker, elapsed)
    };

    assert!(
        marker.contains(DURABLE_SUSPEND),
        "a batch that outlived grace did not suspend over the MCP transport: {marker}"
    );
    assert!(
        elapsed < Duration::from_secs(4),
        "the host must answer inside its grace window, not when the batch finishes; took {elapsed:?}"
    );

    let rows = wait_rows(&db).await;
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one suspension row: {rows:?}"
    );
    assert_eq!(
        rows[0].0,
        provider_tool_use_id("turn-run-mcp-suspend"),
        "the suspension must bind to the provider tool call correlated from the transcript"
    );

    let resolution = awaited_resolution(&db).await;
    assert!(
        resolution.contains("settled-past-grace"),
        "the suspended batch resumed without the item's actual result: {resolution}"
    );
}

/// The native tool loop knows its own provider id and supplies it, so its
/// batches must park on that id rather than going looking for one.
#[tokio::test]
async fn a_supplied_tool_use_id_is_used_as_given() {
    let (_temp, db, orch, cwd) = setup("run-native-loop").await;
    let batch = json!({ "commands": [{ "command": "echo native" }] });
    // Deliberately records a DIFFERENT batch as the turn's tool call: a supplied
    // id must not be second-guessed against the transcript.
    seed_suspendable_run(
        &db,
        &orch,
        "run-native-loop",
        &json!({ "commands": [{ "command": "echo unrelated" }] }),
    )
    .await;

    let marker = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", IMMEDIATE_GRACE_MS);
        let marker = run_text(
            &handle_run(
                &orch,
                &native_loop_request(&cwd, Some("run-native-loop"), batch),
            )
            .await,
        );
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        marker
    };

    assert!(
        marker.contains(DURABLE_SUSPEND),
        "a supplied tool-use id must still park the batch: {marker}"
    );
    let rows = wait_rows(&db).await;
    assert_eq!(
        rows.first().map(|row| row.0.as_str()),
        Some("toolu-native-loop"),
        "the supplied id must be used verbatim: {rows:?}"
    );
}

/// Identity, not recency. A turn's assistant event routinely carries several
/// tool calls, so a suspension must bind to the call whose batch it is running —
/// here deliberately not the newest one in the event. Binding by recency would
/// deliver this batch's result to a sibling call.
#[tokio::test]
async fn a_suspension_binds_to_its_own_call_among_siblings() {
    let (_temp, db, orch, cwd) = setup("run-sibling").await;
    let mine = json!({ "commands": [{ "command": "sleep 6; echo mine" }] });
    let newest_sibling = json!({ "commands": [{ "command": "echo sibling" }] });
    seed_suspendable_run_with_calls(
        &db,
        &orch,
        "run-sibling",
        &[("toolu-mine", &mine), ("toolu-newest", &newest_sibling)],
    )
    .await;

    let marker = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", IMMEDIATE_GRACE_MS);
        let marker = run_text(&handle_run(&orch, &request(&cwd, Some("run-sibling"), mine)).await);
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        marker
    };

    assert!(
        marker.contains(DURABLE_SUSPEND),
        "the batch did not suspend: {marker}"
    );
    let rows = wait_rows(&db).await;
    assert_eq!(
        rows.first().map(|row| row.0.as_str()),
        Some("toolu-mine"),
        "the suspension bound to a sibling call instead of its own: {rows:?}"
    );
    kill_all_sessions(&orch);
}

/// The state in which contents cannot be an identity: one assistant event
/// carrying two byte-identical long `run` calls, both crossing grace. Nothing at
/// this boundary tells them apart — the MCP transport carries no per-invocation
/// id — so neither may claim a suspension. Claiming by recency would answer one
/// call with the other's result and leave the same call answered twice.
///
/// What the system owes here is that no call is mis-answered, which it pays for
/// with the pre-existing inline await for both batches. This is the guard that
/// widening the active-suspension bound to one row per CALL did not weaken the
/// refusal: two calls that cannot be told apart still claim nothing, however
/// many rows a turn is now allowed to own.
#[tokio::test]
async fn indistinguishable_concurrent_batches_claim_nothing() {
    let (_temp, db, orch, cwd) = setup("run-ambiguous").await;
    let batch = json!({ "commands": [{ "command": "sleep 3; echo settled" }] });
    seed_suspendable_run_with_calls(
        &db,
        &orch,
        "run-ambiguous",
        &[("toolu-first", &batch), ("toolu-second", &batch)],
    )
    .await;

    let one = request(&cwd, Some("run-ambiguous"), batch.clone());
    let other = request(&cwd, Some("run-ambiguous"), batch.clone());
    let (first, second) = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", IMMEDIATE_GRACE_MS);
        let (first, second) = tokio::join!(handle_run(&orch, &one), handle_run(&orch, &other));
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        (run_text(&first), run_text(&second))
    };

    for (label, text) in [("first", &first), ("second", &second)] {
        assert!(
            !text.contains(DURABLE_SUSPEND),
            "the {label} batch claimed a call it could not identify: {text}"
        );
        assert!(
            text.contains("settled"),
            "the {label} batch must still return its own result inline: {text}"
        );
    }
    let rows = wait_rows(&db).await;
    assert!(
        rows.is_empty(),
        "no call may be claimed while two are indistinguishable: {rows:?}"
    );
    kill_all_sessions(&orch);
}

/// Every turn whose `start_reason` is a resumed wait — the visible record of a
/// suspension having woken its agent.
async fn wait_resolved_turn_count(db: &LocalDb) -> i64 {
    count(
        db,
        "SELECT COUNT(*) FROM turns WHERE start_reason = 'wait_resolved'",
    )
    .await
}

/// Poll until at least `n` suspensions have recorded a result, so an assertion
/// about what has happened by then is not a race against a still-sleeping batch.
///
/// A recorded result is the point a resolver has claimed its row and answered
/// its call, and it is the furthest this harness can follow one: the terminal
/// `resolved` write lands only once a real agent process has accepted the
/// resume, which a seeded warm handle cannot do.
async fn await_answered_suspensions(db: &LocalDb, n: i64) {
    for _ in 0..600 {
        if count(
            db,
            "SELECT COUNT(*) FROM agent_waits WHERE resolution_json IS NOT NULL",
        )
        .await
            >= n
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "fewer than {n} suspensions ever recorded a result; rows were {:?}",
        wait_rows(db).await
    );
}

/// Poll until some suspension has driven its turn's continuation.
async fn await_driven_continuation(db: &LocalDb) {
    for _ in 0..600 {
        if count(
            db,
            "SELECT COUNT(*) FROM agent_waits WHERE successor_turn_id IS NOT NULL",
        )
        .await
            > 0
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "no suspension ever drove the turn's continuation; rows were {:?}",
        wait_rows(db).await
    );
}

/// The residual CAIRN-3229 left behind (CAIRN-3232). A provider routinely emits
/// several tool calls in one assistant event, and once two of them were `run`
/// batches that both outlived grace, only the first could park: parking was
/// bounded per JOB, so the second fell back to awaiting inline and cairn-cmd's
/// socket discarded it whole — the very symptom the grace contract exists to
/// remove, scoped to concurrent batches.
///
/// Both must park now, each bound to its own provider call, sharing the one turn
/// that issued them.
#[tokio::test]
async fn two_concurrent_batches_both_suspend_over_the_real_transport() {
    let (_temp, db, orch, cwd) = setup("run-concurrent").await;
    let tests = json!({ "commands": [{ "command": "sleep 6; echo tests-settled" }] });
    let checks = json!({ "commands": [{ "command": "sleep 6; echo checks-settled" }] });
    seed_suspendable_run_with_calls(
        &db,
        &orch,
        "run-concurrent",
        &[("toolu-tests", &tests), ("toolu-checks", &checks)],
    )
    .await;

    let tests_call = request(&cwd, Some("run-concurrent"), tests.clone());
    let checks_call = request(&cwd, Some("run-concurrent"), checks.clone());
    let (first, second) = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", SCALED_GRACE_MS);
        let (first, second) = tokio::join!(
            handle_run(&orch, &tests_call),
            handle_run(&orch, &checks_call),
        );
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        (run_text(&first), run_text(&second))
    };

    for (label, text) in [("tests", &first), ("checks", &second)] {
        assert!(
            text.contains(DURABLE_SUSPEND),
            "the concurrent {label} batch did not park: {text}"
        );
    }

    let rows = wait_rows(&db).await;
    assert_eq!(rows.len(), 2, "each concurrent batch owns a row: {rows:?}");
    let mut bound: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    bound.sort_unstable();
    assert_eq!(
        bound,
        vec!["toolu-checks", "toolu-tests"],
        "each suspension must bind to its own provider call: {rows:?}"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(DISTINCT predecessor_turn_id) FROM agent_waits"
        )
        .await,
        1,
        "both rows belong to the single turn that issued them"
    );
    kill_all_sessions(&orch);
}

/// The turn resumes when the LAST of its parked calls settles. Resuming on the
/// first would wake the agent with one of its own calls still unanswered, and
/// the slower result would then have no turn left to land in.
#[tokio::test]
async fn the_turn_resumes_only_after_the_last_batch_settles() {
    let (_temp, db, orch, cwd) = setup("run-last-out").await;
    let quick = json!({ "commands": [{ "command": "sleep 3; echo quick-settled" }] });
    let slow = json!({ "commands": [{ "command": "sleep 12; echo slow-settled" }] });
    seed_suspendable_run_with_calls(
        &db,
        &orch,
        "run-last-out",
        &[("toolu-quick", &quick), ("toolu-slow", &slow)],
    )
    .await;

    let quick_call = request(&cwd, Some("run-last-out"), quick.clone());
    let slow_call = request(&cwd, Some("run-last-out"), slow.clone());
    {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", SCALED_GRACE_MS);
        let (first, second) = tokio::join!(
            handle_run(&orch, &quick_call),
            handle_run(&orch, &slow_call),
        );
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        for (label, text) in [("quick", run_text(&first)), ("slow", run_text(&second))] {
            assert!(
                text.contains(DURABLE_SUSPEND),
                "the {label} batch did not park: {text}"
            );
        }
    }

    // The quick batch has settled and answered its own call. The slow one has
    // seconds left to run, so nothing may have resumed yet.
    await_answered_suspensions(&db, 1).await;
    assert_eq!(
        wait_resolved_turn_count(&db).await,
        0,
        "the turn resumed while one of its own calls was still parked"
    );

    // Once the slow one settles too, the turn resumes — exactly once, driven by
    // exactly one of its two parked calls.
    await_answered_suspensions(&db, 2).await;
    await_driven_continuation(&db).await;
    assert_eq!(wait_resolved_turn_count(&db).await, 1);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM agent_waits WHERE successor_turn_id IS NOT NULL"
        )
        .await,
        1,
        "exactly one of the turn's parked calls owns its continuation"
    );
    kill_all_sessions(&orch);
}

/// CAIRN-3159 in the run-batch shape. A job parks one turn at a time, and a row
/// that outlived its turn must be superseded rather than refusing every later
/// suspension — otherwise the first long batch of a job's life would be the only
/// one that ever suspends, and every later one would fall back to the inline
/// await this whole contract exists to remove.
#[tokio::test]
async fn suspension_engages_again_after_a_row_outlives_its_turn() {
    let (_temp, db, orch, cwd) = setup("run-resuspend").await;
    let first = json!({ "commands": [{ "command": "sleep 8" }] });
    seed_suspendable_run(&db, &orch, "run-resuspend", &first).await;

    let second = json!({ "commands": [{ "command": "echo second-batch" }] });
    let (first_marker, second_marker) = {
        let _serialized = GRACE_OVERRIDE.lock().await;
        std::env::set_var("CAIRN_RUN_GRACE_MS", IMMEDIATE_GRACE_MS);
        let first_marker =
            run_text(&handle_run(&orch, &request(&cwd, Some("run-resuspend"), first)).await);
        // The first row is still pending when the agent's next turn opens, which
        // is exactly the state that used to wedge a job into inline waiting.
        open_next_turn(&db, &orch, "run-resuspend", "turn-resuspend-2", 2, &second).await;
        let second_marker =
            run_text(&handle_run(&orch, &request(&cwd, Some("run-resuspend"), second)).await);
        std::env::remove_var("CAIRN_RUN_GRACE_MS");
        (first_marker, second_marker)
    };

    assert!(
        first_marker.contains("Run handed off to durable suspend"),
        "the first batch did not suspend: {first_marker}"
    );
    assert!(
        first_marker.contains(DURABLE_SUSPEND),
        "the first batch did not suspend: {first_marker}"
    );
    assert!(
        second_marker.contains(DURABLE_SUSPEND),
        "a second suspension in the same job was refused instead of superseding the stale row: {second_marker}"
    );

    let rows = wait_rows(&db).await;
    assert_eq!(rows.len(), 2, "each suspension owns a row: {rows:?}");
    assert!(
        rows[0]
            .2
            .as_deref()
            .is_some_and(|r| r.contains("abandoned")),
        "the stale row should have been superseded, not left to block the job: {rows:?}"
    );
    assert_ne!(
        rows[1].1, "cancelled",
        "the new suspension must be live: {rows:?}"
    );
    kill_all_sessions(&orch);
}

// Each item waits for a marker written by the other. Concurrent execution lets
// both succeed; serialization makes the first item fail before the second starts.
// This proves overlap without relying on wall-clock timing under a loaded suite.
#[tokio::test]
async fn process_batch_uses_one_slot_and_preserves_parallel_execution() {
    let (_temp, _db, orch, cwd) = setup("run-parallel").await;
    let payload = json!({
        "commands": [
            { "command": "touch first.started; for _ in $(seq 1 100); do test -f second.started && exit 0; sleep 0.05; done; exit 9" },
            { "command": "touch second.started; for _ in $(seq 1 100); do test -f first.started && exit 0; sleep 0.05; done; exit 9" }
        ],
        "sequential": false,
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-parallel"), payload)).await;
    assert!(
        !result.contains("Exit code: 9"),
        "items sharing one lease did not overlap: {result}"
    );
}

/// Timeout (ms) for the timeout tests that assert the item's PARTIAL stdout
/// (`started`) is captured before the timeout fires. The capture normally lands
/// in tens of milliseconds, so this is deliberately generous: a tight timeout
/// races the subprocess spawn + output pump against the timer and flakes under
/// heavy CI / turn-end load (the review check cadence now runs its heavy suites
/// concurrently, so the machine can be saturated when these run). It stays far
/// below the commands' 30s sleep, so the timeout still fires first.
const PARTIAL_OUTPUT_TIMEOUT_MS: u32 = 3000;

/// An explicit `timeout` is a kill bound and nothing else: the item dies at the
/// bound, fails with the output it produced, and converts into nothing. A
/// terminal is created only by an explicit write to a terminal URI.
#[tokio::test]
async fn explicit_timeout_kills_the_item_and_creates_no_terminal() {
    let (_temp, db, orch, cwd) = setup("run-timeout").await;
    let payload = json!({
        "commands": [{ "command": "echo started; sleep 30", "timeout": PARTIAL_OUTPUT_TIMEOUT_MS }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-timeout"), payload)).await;

    assert!(
        result.contains("started"),
        "missing partial output: {result}"
    );
    assert!(
        result.contains("timed out"),
        "missing timeout result: {result}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM job_terminals").await,
        0,
        "a timed-out item must not turn into a terminal"
    );
    assert_eq!(orch.pty_state.sessions.lock().unwrap().len(), 0);
    kill_all_sessions(&orch);
}

/// The same contract inside a sub-task job, which used to get its own minted
/// `run-N` terminal from the runner callback.
#[tokio::test]
async fn timed_out_item_in_subtask_job_creates_no_terminal() {
    // Depends on a real long-running subprocess hitting the timeout-kill path;
    // the agent fence disrupts that subprocess lifecycle. Skip in a fence;
    // unfenced CI exercises the real timeout.
    if common::skip_if_fenced("timed_out_item_in_subtask_job_creates_no_terminal") {
        return;
    }
    let (_temp, db, orch, cwd) = setup("run-subtask").await;
    // Add a sub-task job under the seeded top-level job, and a run on it.
    db.write(move |conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, parent_job_id, branch, base_commit, created_at, updated_at)
                 SELECT 'job-sub', execution_id, agent_config_id, issue_id, project_id, 'mapper', 'running', 'map-things', id, branch, base_commit, 1, 1
                 FROM jobs WHERE id = 'job-run-subtask'",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at, start_mode)
                 SELECT 'run-subtask-sub', project_id, issue_id, 'job-sub', 'live', 1, 1, 'resume' FROM runs WHERE id = 'run-subtask'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let payload = json!({
        "commands": [{ "command": "echo started; sleep 30", "timeout": PARTIAL_OUTPUT_TIMEOUT_MS }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-subtask-sub"), payload)).await;

    assert!(result.contains("timed out"), "expected timeout: {result}");
    assert!(
        result.contains("started"),
        "missing partial output: {result}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM job_terminals").await,
        0,
        "a timed-out sub-task item must not turn into a terminal"
    );
    assert_eq!(orch.pty_state.sessions.lock().unwrap().len(), 0);
    kill_all_sessions(&orch);
}

#[tokio::test]
async fn sequential_stop_on_error_halts_after_timed_out_slot_item() {
    let (_temp, _db, orch, cwd) = setup("run-halt").await;
    let payload = json!({
        "commands": [
            { "command": "sleep 30", "timeout": 600 },
            { "command": "echo SHOULD_NOT_RUN" }
        ],
        "sequential": true,
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-halt"), payload)).await;

    assert!(
        result.contains("timed out"),
        "missing timeout result: {result}"
    );
    assert!(
        !result.contains("SHOULD_NOT_RUN"),
        "timed-out item must halt a stop_on_error batch: {result}"
    );

    kill_all_sessions(&orch);
}

#[tokio::test]
async fn tree_bound_item_without_resolvable_context_fails_before_execution() {
    // Depends on the same real subprocess timeout-kill lifecycle as the sub-task
    // timeout case. Under the agent worktree fence, an inherited sandbox denial
    // can win before the timeout path this test is meant to exercise.
    if common::skip_if_fenced("timed_out_item_without_run_context_kills_and_returns") {
        return;
    }
    let (_temp, db, orch, _cwd) = setup("run-nokill").await;
    // A cwd that maps to no job (and run_id None) yields no run context, so the
    // request cannot establish executor repository authority. The seeded repo
    // is a worktree, so use an independent throwaway repo instead.
    let loose = tempfile::tempdir().unwrap();
    init_git_repo(loose.path());
    let cwd = loose.path().display().to_string();
    let payload = json!({
        "commands": [{ "command": "sleep 30", "timeout": 600 }],
    });
    let start = Instant::now();
    let result = handle_run(&orch, &request(&cwd, None, payload)).await;
    let elapsed = start.elapsed();

    assert!(
        result.contains("this batch's working directory could not be resolved")
            && result.contains("No commands ran"),
        "expected fail-closed placement error: {result}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "placement failure should return promptly, took {elapsed:?}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM job_terminals").await,
        0,
        "no terminal row without executor repository authority"
    );
    assert!(
        orch.pty_state.sessions.lock().unwrap().is_empty(),
        "no promoted session without executor repository authority"
    );
}

// A child that calls setsid escapes the SIGKILL'd process group and holds the
// stdout pipe write end open, so an unbounded reader join would hang forever.
// The bounded reaping must return before the 8s escapee exits naturally, with
// partial output. The public call also includes lazy slot acquisition and
// materialization, so its wall-clock bound leaves room for substrate setup.
// (Uses perl for setsid so it works on macOS, which lacks the binary.)
#[tokio::test]
async fn setsid_escapee_does_not_hang_the_call() {
    let (_temp, _db, orch, cwd) = setup("run-setsid").await;
    let escapee = "perl -e 'STDOUT->autoflush(1); if (fork()) { exit 0 } require POSIX; POSIX::setsid(); print \"started\\n\"; sleep 8'";
    let payload = json!({
        "commands": [{ "command": escapee, "timeout": 800 }],
    });
    let start = Instant::now();
    let result = handle_run(&orch, &request(&cwd, Some("run-setsid"), payload)).await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(7),
        "a setsid escapee holding the pipe must return before natural exit; took {elapsed:?}"
    );
    assert!(
        result.contains("started"),
        "partial output before the escape should still be captured: {result}"
    );
}

/// Whether an interpreter binary resolves on the test PATH, so a minimal CI
/// image without bun / python3 self-skips these code-item tests rather than
/// failing (mirroring the `jj_bin` skip pattern).
fn binary_available(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test]
async fn typescript_code_item_executes_via_bun() {
    if !binary_available("bun") {
        eprintln!("skipping typescript_code_item_executes_via_bun: bun not resolvable");
        return;
    }
    let (_temp, _db, orch, cwd) = setup("run-code-ts").await;
    let payload = json!({
        "commands": [{
            "code": "const n: number = 41; console.log(`answer=${n + 1}`)",
            "interpreter": "typescript"
        }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-code-ts"), payload)).await;
    assert!(
        result.contains("answer=42"),
        "a typescript code item must run via `bun -e` and return its stdout: {result}"
    );
}

// Process cwd is non-authoritative. A run keeps using its authenticated branch
// coordinate even when its residence happens to be an unrelated Git checkout.
#[tokio::test]
async fn forged_cwd_does_not_replace_authenticated_branch_identity() {
    let (_temp, _db, orch, _managed_cwd) = setup("run-plain-cwd").await;
    let loose = tempfile::tempdir().unwrap();
    init_git_repo(loose.path());
    let loose_path = loose.path().display().to_string();
    let result = handle_run(
        &orch,
        &request(
            &loose_path,
            Some("run-plain-cwd"),
            json!({ "commands": [{ "command": "printf unmanaged-cwd" }] }),
        ),
    )
    .await;

    assert!(result.contains("unmanaged-cwd"), "{result}");
    assert!(
        !result.contains("managed workspace owner"),
        "plain cwd must bypass managed workspace identity resolution: {result}"
    );
}

// Inline python routes through `uv run -` when uv resolves and falls back to
// `python3 -c` otherwise. This end-to-end check is path-agnostic: both
// uv-managed CPython and system python3 print `py3` for the major version, so it
// passes whichever rung of the ladder the host takes.
#[tokio::test]
async fn python_code_item_executes() {
    if !binary_available("uv") && !binary_available("python3") {
        eprintln!("skipping python_code_item_executes: neither uv nor python3 resolvable");
        return;
    }
    let (_temp, _db, orch, cwd) = setup("run-code-py").await;
    let payload = json!({
        "commands": [{
            "code": "import sys; print(f'py{sys.version_info[0]}')",
            "interpreter": "python"
        }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-code-py"), payload)).await;
    assert!(
        result.contains("py3"),
        "a python code item must run (via `uv run -` or the `python3 -c` fallback) and return its stdout: {result}"
    );
}

/// With `uv` resolvable, an inline python item routes through `uv run -` and its
/// PEP 723 inline `# /// script` dependency block is honored: uv parses the
/// metadata from the stdin-delivered script, installs the dep into an ephemeral
/// env, and the import succeeds — proving both the uv rung of the ladder and that
/// the code actually arrives on stdin (a `-c` delivery would skip the metadata
/// and fail to import). Gated on `uv` (and, on a cold cache, network); a machine
/// without uv self-skips, mirroring the python3/bun `binary_available` idiom.
#[tokio::test]
async fn python_code_item_honors_pep723_inline_deps_via_uv() {
    if !binary_available("uv") {
        eprintln!("skipping python_code_item_honors_pep723_inline_deps_via_uv: uv not resolvable");
        return;
    }
    let (_temp, _db, orch, cwd) = setup("run-code-uv-pep723").await;
    // `packaging` is a tiny, pure-python dependency with no transitive build
    // step, so the ephemeral install is fast and reliable (and commonly warm in
    // uv's cache). The inline metadata block is what `-c` would never parse.
    let code = "# /// script\n# dependencies = [\"packaging\"]\n# ///\nfrom packaging.version import Version\nprint(f\"pep723-ok:{Version('1.2.3') < Version('1.10')}\")\n";
    let payload = json!({
        "commands": [{ "code": code, "interpreter": "python" }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-code-uv-pep723"), payload)).await;
    assert!(
        result.contains("pep723-ok:True"),
        "`uv run -` must parse PEP 723 inline deps from the stdin script and import them: {result}"
    );
}

/// Guards the load-bearing zero-config `@cairn/sdk` story: under `bun -e` a bare
/// package specifier resolves from the run cwd's `node_modules`. We stand up a
/// minimal `@cairn/sdk`-named fixture package in the worktree so the test proves
/// the *resolution mechanism* independent of the real package's current exports.
#[tokio::test]
async fn code_item_resolves_bare_package_import_from_worktree_node_modules() {
    if !binary_available("bun") {
        eprintln!("skipping code_item_resolves_bare_package_import_from_worktree_node_modules: bun not resolvable");
        return;
    }
    let (temp, _db, orch, cwd) = setup("run-code-sdk").await;
    let project_repo = temp.path().join("project");
    let package = project_repo.join("node_modules/@cairn/sdk");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(project_repo.join(".gitignore"), "node_modules/\n").unwrap();
    std::fs::write(
        package.join("package.json"),
        r#"{"name":"@cairn/sdk","type":"module","exports":{".":"./index.ts"}}"#,
    )
    .unwrap();
    std::fs::write(
        package.join("index.ts"),
        "export const marker = \"SDK_IMPORT_OK\";\n",
    )
    .unwrap();
    git(
        &project_repo,
        &[
            "add",
            "-f",
            ".gitignore",
            "node_modules/@cairn/sdk/package.json",
            "node_modules/@cairn/sdk/index.ts",
        ],
    );
    git(
        &project_repo,
        &["commit", "-m", "add SDK resolution fixture"],
    );
    let fixture_commit = common::head_sha(&project_repo);
    let jj_bin = common::jj_bin().expect("jj is required for managed run tests");
    let import = Command::new(&jj_bin)
        .args(["git", "import"])
        .current_dir(&cwd)
        .output()
        .unwrap();
    assert!(
        import.status.success(),
        "jj git import failed: {}",
        String::from_utf8_lossy(&import.stderr)
    );
    let output = Command::new(jj_bin)
        .args([
            "bookmark",
            "set",
            "agent/RHG-1-builder-0",
            "-r",
            fixture_commit.as_str(),
        ])
        .current_dir(&cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "jj bookmark set failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload = json!({
        "commands": [{
            "code": "import { marker } from \"@cairn/sdk\"; console.log(marker)",
            "interpreter": "typescript"
        }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-code-sdk"), payload)).await;
    assert!(
        result.contains("SDK_IMPORT_OK"),
        "`bun -e` must resolve a bare package import from the executor projection's node_modules: {result}"
    );
}

#[tokio::test]
async fn code_item_nonzero_exit_is_reported_as_failure() {
    if !binary_available("python3") {
        eprintln!("skipping code_item_nonzero_exit_is_reported_as_failure: python3 not resolvable");
        return;
    }
    let (_temp, _db, orch, cwd) = setup("run-code-exit").await;
    let payload = json!({
        "commands": [{
            "code": "import sys; print('before exit'); sys.exit(3)",
            "interpreter": "python"
        }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-code-exit"), payload)).await;
    // Partial stdout before the exit is captured, and the non-zero exit surfaces
    // exactly like a failed shell command (`Exit code: N`).
    assert!(
        result.contains("before exit"),
        "partial stdout missing: {result}"
    );
    assert!(
        result.contains("Exit code: 3"),
        "a non-zero interpreter exit must be surfaced like a failed command: {result}"
    );
}

/// A timed-out code item flows through the identical partial-output + kill path
/// as a shell timeout — the timeout machinery is per-spawn and kind-agnostic, so
/// inline code inherits it unchanged.
#[tokio::test]
async fn timed_out_code_item_is_killed_with_partial_output() {
    if !binary_available("python3") {
        eprintln!(
            "skipping timed_out_code_item_is_killed_with_partial_output: python3 not resolvable"
        );
        return;
    }
    let (_temp, db, orch, cwd) = setup("run-code-timeout").await;
    let payload = json!({
        "commands": [{
            "code": "import time,sys; print('started'); sys.stdout.flush(); time.sleep(30)",
            "interpreter": "python",
            "timeout": PARTIAL_OUTPUT_TIMEOUT_MS
        }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-code-timeout"), payload)).await;
    assert!(
        result.contains("started"),
        "missing partial output: {result}"
    );
    assert!(
        result.contains("timed out"),
        "missing timeout result: {result}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM job_terminals").await,
        0,
        "a timed-out code item must not turn into a terminal"
    );
    assert_eq!(orch.pty_state.sessions.lock().unwrap().len(), 0);
    kill_all_sessions(&orch);
}

#[tokio::test]
async fn chained_commands_surface_all_segments() {
    let (_temp, _db, orch, cwd) = setup("run-chain").await;
    // `||` chain: the first grep misses, then the second matches README.
    let or_chain = handle_run(
        &orch,
        &request(
            &cwd,
            Some("run-chain"),
            json!({ "commands": [{ "command": "false || grep initial README.md || echo none" }] }),
        ),
    )
    .await;
    assert!(
        or_chain.contains("initial"),
        "|| chain dropped a segment: {or_chain}"
    );

    // `&&` chain: both segments run and both outputs are captured.
    let and_chain = handle_run(
        &orch,
        &request(
            &cwd,
            Some("run-chain"),
            json!({ "commands": [{ "command": "echo SEG_ONE && echo SEG_TWO" }] }),
        ),
    )
    .await;
    assert!(
        and_chain.contains("SEG_ONE") && and_chain.contains("SEG_TWO"),
        "&& chain dropped a segment: {and_chain}"
    );
}

// A `repl` send to a slug with no live session fails closed with the create
// hint (Behaviors #4a) rather than silently spawning a fresh process. Fully
// deterministic — no interpreter required.
#[tokio::test]
async fn repl_unknown_slug_send_fails_closed() {
    let run_id = "run-repl-unknown";
    let (_temp, _db, orch, cwd) = setup(run_id).await;
    let out = handle_run(
        &orch,
        &request(
            &cwd,
            Some(run_id),
            json!({ "commands": [{ "code": "1 + 1", "interpreter": "python", "repl": "ghost" }] }),
        ),
    )
    .await;
    assert!(out.contains("No REPL named 'ghost'"), "got: {out}");
    assert!(out.contains("cairn:~/repl/ghost"), "got: {out}");
}

// State persists across two separate `handle_run` calls routed into the same
// live REPL session: `x = 41` then `x + 1` returns `42`. Guarded to skip if no
// interpreter is available to spawn the eval-server.
#[tokio::test]
async fn repl_state_persists_across_handle_run_calls() {
    use cairn_core::internal::mcp::handlers::repl::{self, ReplLang};
    use cairn_core::internal::mcp::handlers::RunContext;

    let run_id = "run-repl-state";
    let (_temp, _db, orch, cwd) = setup(run_id).await;
    let ctx = RunContext {
        run_id: run_id.to_string(),
        job_id: format!("job-{run_id}"),
        exec_seq: Some(1),
        issue_id: Some(format!("issue-{run_id}")),
        issue_number: Some(1),
        project_id: String::new(),
        project_key: "RHG".to_string(),
        job_name: Some("builder".to_string()),
    };

    let Ok(session) = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Python,
        "analysis",
        &[],
    )
    .await
    else {
        eprintln!("skipping repl_state_persists: no python/uv available to spawn the eval-server");
        return;
    };
    orch.repl_state
        .insert(ctx.job_id.clone(), "analysis".to_string(), session);

    let first = handle_run(
        &orch,
        &request(
            &cwd,
            Some(run_id),
            json!({ "commands": [{ "code": "x = 41", "interpreter": "python", "repl": "analysis" }] }),
        ),
    )
    .await;
    assert!(
        !first.contains("No REPL named"),
        "first send lost the session: {first}"
    );
    assert!(
        !first.contains("died"),
        "first send reported a dead REPL: {first}"
    );

    let second = handle_run(
        &orch,
        &request(
            &cwd,
            Some(run_id),
            json!({ "commands": [{ "code": "x + 1", "interpreter": "python", "repl": "analysis" }] }),
        ),
    )
    .await;
    assert!(
        second.contains("42"),
        "REPL state must persist across handle_run calls: {second}"
    );

    if let Some(session) = orch.repl_state.remove(&ctx.job_id, "analysis") {
        session.stop_and_release(&orch).await;
    }
}

// A typescript REPL rejects `deps` (a uv-only affordance) with a clear message.
// The deps guard fires before the bun probe, so this is deterministic without
// bun installed.
#[tokio::test]
async fn repl_typescript_rejects_deps() {
    use cairn_core::internal::mcp::handlers::repl::{self, ReplLang};
    use cairn_core::internal::mcp::handlers::RunContext;

    let run_id = "run-repl-ts-deps";
    let (_temp, _db, orch, cwd) = setup(run_id).await;
    let ctx = RunContext {
        run_id: run_id.to_string(),
        job_id: format!("job-{run_id}"),
        exec_seq: Some(1),
        issue_id: Some(format!("issue-{run_id}")),
        issue_number: Some(1),
        project_id: String::new(),
        project_key: "RHG".to_string(),
        job_name: Some("builder".to_string()),
    };
    let result = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Typescript,
        "ts",
        &["react".to_string()],
    )
    .await;
    let err = match result {
        Ok(session) => {
            session.stop_and_release(&orch).await;
            panic!("typescript deps must be rejected");
        }
        Err(err) => err,
    };
    assert!(err.contains("python-only"), "got: {err}");
}

// The typescript/bun eval-server persists state across separate `handle_run`
// calls exactly like python: `x = 41` then `x + 1` returns `42`. Skips if bun is
// unavailable to spawn the eval-server.
#[tokio::test]
async fn repl_typescript_state_persists_across_handle_run_calls() {
    use cairn_core::internal::mcp::handlers::repl::{self, ReplLang};
    use cairn_core::internal::mcp::handlers::RunContext;

    let run_id = "run-repl-ts-state";
    let (_temp, _db, orch, cwd) = setup(run_id).await;
    let ctx = RunContext {
        run_id: run_id.to_string(),
        job_id: format!("job-{run_id}"),
        exec_seq: Some(1),
        issue_id: Some(format!("issue-{run_id}")),
        issue_number: Some(1),
        project_id: String::new(),
        project_key: "RHG".to_string(),
        job_name: Some("builder".to_string()),
    };

    let Ok(session) = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Typescript,
        "ts",
        &[],
    )
    .await
    else {
        eprintln!(
            "skipping repl_typescript_state_persists: no bun available to spawn the eval-server"
        );
        return;
    };
    orch.repl_state
        .insert(ctx.job_id.clone(), "ts".to_string(), session);

    let first = handle_run(
        &orch,
        &request(
            &cwd,
            Some(run_id),
            json!({ "commands": [{ "code": "x = 41", "interpreter": "typescript", "repl": "ts" }] }),
        ),
    )
    .await;
    assert!(
        !first.contains("No REPL named"),
        "first send lost the session: {first}"
    );
    assert!(
        !first.contains("died"),
        "first send reported a dead REPL: {first}"
    );

    let second = handle_run(
        &orch,
        &request(
            &cwd,
            Some(run_id),
            json!({ "commands": [{ "code": "x + 1", "interpreter": "typescript", "repl": "ts" }] }),
        ),
    )
    .await;
    assert!(
        second.contains("42"),
        "typescript REPL state must persist across handle_run calls: {second}"
    );

    // A language-mismatched send (python item into a typescript session) is
    // rejected without touching the live session.
    let mismatch = handle_run(
        &orch,
        &request(
            &cwd,
            Some(run_id),
            json!({ "commands": [{ "code": "x + 1", "interpreter": "python", "repl": "ts" }] }),
        ),
    )
    .await;
    assert!(
        mismatch.contains("typescript") && mismatch.contains("python"),
        "mismatched-language send must be rejected naming both languages: {mismatch}"
    );

    if let Some(session) = orch.repl_state.remove(&ctx.job_id, "ts") {
        session.stop_and_release(&orch).await;
    }
}

// Build a REPL run context matching a job seeded by `seed_run`.
fn repl_ctx(run_id: &str, _cwd: &str) -> cairn_core::internal::mcp::handlers::RunContext {
    cairn_core::internal::mcp::handlers::RunContext {
        run_id: run_id.to_string(),
        job_id: format!("job-{run_id}"),
        exec_seq: Some(1),
        issue_id: Some(format!("issue-{run_id}")),
        issue_number: Some(1),
        project_id: String::new(),
        project_key: "RHG".to_string(),
        job_name: Some("builder".to_string()),
    }
}

// Mirror `setup_without_executor` but inject a retained `CapturingEmitter` so a
// test can assert on the `repl-exchange` / `repl-state` events the funnel emits.
async fn setup_with_emitter(
    run_id: &str,
    emitter: Arc<cairn_core::internal::services::testing::CapturingEmitter>,
) -> (TempDir, Arc<LocalDb>, Orchestrator, String) {
    use cairn_core::internal::services::Services;

    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "RHG", &project_repo).await;
    let worktree = temp.path().join("worktree");
    let branch = "agent/RHG-1-builder-0";
    let base_commit = common::head_sha(&project_repo);
    seed_run(&db, &project_id, &worktree, branch, &base_commit, run_id).await;

    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db.clone(), search_index));
    // build() wraps its emitter in a fresh Arc, so swap in ours via struct update
    // to keep a handle for event assertions.
    let base = TestServicesBuilder::new()
        .with_process(RealProcessSpawner)
        .build();
    let services = Arc::new(Services {
        emitter: emitter.clone(),
        ..base
    });
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config")).build();

    common::provision_jj_workspace(
        &temp.path().join("config"),
        &project_repo,
        &worktree,
        branch,
    );
    let cwd = worktree.display().to_string();
    (temp, db, orch, cwd)
}

// The send funnel records each exchange as ONE durable row (inserted pending,
// updated in place on settle) and announces both writes as `db-change` — the
// app-wide invalidation path, which is what keeps the REPL pane fresh whether or
// not it is mounted. A language-mismatched send fails closed before any exchange
// is recorded. Guarded to skip if no python interpreter is available.
#[tokio::test]
async fn repl_funnel_records_history_and_emits_events() {
    use cairn_core::internal::mcp::handlers::repl::{
        self, ReplExchangeStatus, ReplLang, ReplOrigin,
    };
    use cairn_core::internal::repl_host;
    use cairn_core::internal::services::testing::CapturingEmitter;

    let run_id = "run-repl-funnel";
    let emitter = Arc::new(CapturingEmitter::new());
    let (_temp, _db, orch, cwd) = setup_with_emitter(run_id, emitter.clone()).await;
    let ctx = repl_ctx(run_id, &cwd);

    let Ok(session) = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Python,
        "fn",
        &[],
    )
    .await
    else {
        eprintln!("skipping repl_funnel: no python/uv available to spawn the eval-server");
        return;
    };
    orch.repl_state
        .insert(ctx.job_id.clone(), "fn".to_string(), session);

    let exchange = repl::send_recorded(
        &orch,
        &ctx.job_id,
        "fn",
        "z = 7\nz + 1",
        Duration::from_secs(30),
        ReplOrigin::User,
        Some(ReplLang::Python),
    )
    .await
    .expect("a live session send should record an exchange");
    assert_eq!(exchange.status, ReplExchangeStatus::Success);
    assert_eq!(exchange.value.as_deref(), Some("8"));
    assert_eq!(exchange.origin, ReplOrigin::User);
    assert!(exchange.duration_ms.is_some());

    // Both the pending insert and the settle announce themselves on the one
    // app-wide invalidation channel. This is the regression guard for the
    // original bug: nothing here is scoped to an open REPL pane.
    let changes = emitter.events_named("db-change");
    assert!(
        changes
            .iter()
            .filter(|e| e["table"] == "repl_exchanges")
            .count()
            >= 2,
        "expected a db-change per exchange phase: {changes:?}"
    );

    let history = repl_host::get_repl_history(&orch, ctx.job_id.clone(), "fn".to_string()).await;
    assert_eq!(
        history.len(),
        1,
        "the pending row must be updated in place, not duplicated"
    );
    assert_eq!(history[0].seq, exchange.seq);
    assert_eq!(history[0].status, ReplExchangeStatus::Success);
    assert_eq!(history[0].value.as_deref(), Some("8"));

    // The namespace snapshot rode back on the eval response and landed on the
    // row, so a read can report what is bound without asking the interpreter.
    let record = repl::store::load(&orch.db.local, &ctx.job_id, "fn")
        .await
        .expect("row query")
        .expect("a live REPL has a durable row");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, repl::store::ReplRowStatus::Running);
    let z = record
        .bindings
        .iter()
        .find(|b| b.name == "z")
        .unwrap_or_else(|| panic!("expected z among bindings: {:?}", record.bindings));
    assert_eq!(z.kind, "int");
    assert_eq!(z.info.as_deref(), Some("7"));
    assert!(
        repl::render_status("fn", Some(&record), None).contains("z"),
        "the read banner must list bindings"
    );

    // A language mismatch fails closed without recording a phantom exchange.
    let mismatch = repl::send_recorded(
        &orch,
        &ctx.job_id,
        "fn",
        "1",
        Duration::from_secs(5),
        ReplOrigin::Agent,
        Some(ReplLang::Typescript),
    )
    .await;
    assert!(mismatch.is_err(), "mismatched language must fail closed");
    assert_eq!(
        repl_host::get_repl_history(&orch, ctx.job_id.clone(), "fn".to_string())
            .await
            .len(),
        1,
        "a rejected mismatch must not append to history"
    );

    if let Some(session) = orch.repl_state.remove(&ctx.job_id, "fn") {
        session.stop_and_release(&orch).await;
    }
}

/// A settled exchange row for the durable-lifecycle tests, written the way the
/// send funnel writes one: inserted pending, then updated in place.
async fn seed_exchange(
    db: &cairn_core::internal::storage::LocalDb,
    repl_id: &str,
    generation: i64,
    seq: u64,
) {
    use cairn_core::internal::mcp::handlers::repl::store;
    use cairn_core::internal::mcp::handlers::repl::{ReplExchange, ReplExchangeStatus, ReplOrigin};

    let mut exchange = ReplExchange {
        seq,
        generation,
        origin: ReplOrigin::Agent,
        code: format!("x = {seq}"),
        started_at: 1_000 + seq as i64,
        duration_ms: None,
        status: ReplExchangeStatus::Pending,
        value: None,
        stdout: None,
        stderr: None,
        error: None,
        note: None,
        truncated: false,
    };
    store::insert_pending(db, repl_id, None, &exchange)
        .await
        .expect("pending insert");
    exchange.status = ReplExchangeStatus::Success;
    exchange.duration_ms = Some(3);
    store::settle(db, repl_id, &exchange).await.expect("settle");
}

// A REPL's durable identity outlives its process: a dead session keeps its fate
// and its transcript, reads as EXITED rather than missing, refuses to reopen as a
// different language, and resumes as the next generation continuing the same
// transcript. Deterministic — no interpreter involved.
#[tokio::test]
async fn repl_row_outlives_the_process_and_resumes_as_a_new_generation() {
    use cairn_core::internal::mcp::handlers::repl::store::{self, ReplExitReason, ReplRowStatus};
    use cairn_core::internal::mcp::handlers::repl::{self, ReplLang};
    use cairn_core::internal::repl_host;

    let run_id = "run-repl-durable";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let db = &orch.db.local;

    let record = store::begin(
        db,
        &ctx.job_id,
        None,
        "lex",
        ReplLang::Python,
        &["pandas".to_string()],
    )
    .await
    .expect("first create inserts the row");
    assert_eq!(record.generation, 1);
    seed_exchange(db, &record.id, 1, 0).await;
    seed_exchange(db, &record.id, 1, 1).await;

    store::mark_exited(db, &record.id, record.generation, ReplExitReason::Died)
        .await
        .expect("mark exited");
    let dead = store::load(db, &ctx.job_id, "lex")
        .await
        .expect("load")
        .expect("a dead REPL keeps its row");
    assert_eq!(dead.status, ReplRowStatus::Exited);
    assert_eq!(dead.exit_reason, Some(ReplExitReason::Died));
    assert_eq!(dead.exchange_count, 2, "the transcript survives death");

    // Death is the most significant event in a REPL's life, so the read must
    // render it rather than reporting the slug as if it never existed.
    let rendered = repl::render_status("lex", Some(&dead), None);
    assert!(rendered.contains("exited (died)"), "got: {rendered}");
    assert!(!rendered.contains("not found"), "got: {rendered}");

    // Reopening as a DIFFERENT interpreter is refused before anything spawns.
    let refused = repl_host::open_repl(
        &orch,
        &ctx.job_id,
        "",
        &cwd,
        Some(&ctx),
        "lex",
        Some(ReplLang::Typescript),
        None,
    )
    .await
    .expect_err("a different interpreter must be refused");
    assert!(refused.contains("incoherent"), "got: {refused}");

    // Resuming bumps the generation, inherits the recorded deps, and continues
    // the one transcript.
    let resumed = store::begin(db, &ctx.job_id, None, "lex", dead.interpreter, &dead.deps)
        .await
        .expect("resume");
    assert_eq!(resumed.id, dead.id, "a resume is the same logical REPL");
    assert_eq!(resumed.generation, 2);
    assert_eq!(resumed.status, ReplRowStatus::Running);
    assert_eq!(resumed.exit_reason, None);
    assert_eq!(resumed.deps, vec!["pandas".to_string()]);
    assert_eq!(resumed.exchange_count, 2);
    assert_eq!(
        store::next_seq(db, &resumed.id).await.expect("next seq"),
        2,
        "seq stays monotonic across generations"
    );
}

// No interpreter survives a runner restart, so every row still marked running is
// a lie: the startup reap records it as exited via host_restart and settles the
// sends that were in flight, instead of leaving them pending forever.
#[tokio::test]
async fn repl_startup_reap_marks_running_rows_and_settles_inflight_sends() {
    use cairn_core::internal::mcp::handlers::repl::store::{self, ReplExitReason, ReplRowStatus};
    use cairn_core::internal::mcp::handlers::repl::{
        ReplExchange, ReplExchangeStatus, ReplLang, ReplOrigin,
    };
    use cairn_core::internal::repl_host;

    let run_id = "run-repl-reap";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let db = &orch.db.local;

    let record = store::begin(db, &ctx.job_id, None, "lex", ReplLang::Python, &[])
        .await
        .expect("create");
    let inflight = ReplExchange {
        seq: 0,
        generation: 1,
        origin: ReplOrigin::Agent,
        code: "slow()".into(),
        started_at: 1,
        duration_ms: None,
        status: ReplExchangeStatus::Pending,
        value: None,
        stdout: None,
        stderr: None,
        error: None,
        note: None,
        truncated: false,
    };
    store::insert_pending(db, &record.id, None, &inflight)
        .await
        .expect("pending insert");

    assert_eq!(
        repl_host::reap_orphaned_repls(&orch).await.expect("reap"),
        1
    );

    let row = store::load(db, &ctx.job_id, "lex")
        .await
        .expect("load")
        .expect("row");
    assert_eq!(row.status, ReplRowStatus::Exited);
    assert_eq!(row.exit_reason, Some(ReplExitReason::HostRestart));
    let history = store::history(db, &record.id).await.expect("history");
    assert_eq!(history[0].status, ReplExchangeStatus::Died);
    assert!(
        history[0].error.is_some(),
        "a reaped send must say what happened to it"
    );
}

// Stopping and discarding are different acts now that the transcript outlives
// the process. With no live session, `delete` discards the row and cascades the
// transcript, and doing it again is a no-op.
#[tokio::test]
async fn repl_delete_discards_an_exited_repl_and_its_transcript() {
    use cairn_core::internal::mcp::handlers::repl::store::{self, ReplExitReason};
    use cairn_core::internal::mcp::handlers::repl::ReplLang;
    use cairn_core::internal::repl_host;

    let run_id = "run-repl-discard";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let db = &orch.db.local;

    let record = store::begin(db, &ctx.job_id, None, "lex", ReplLang::Python, &[])
        .await
        .expect("create");
    seed_exchange(db, &record.id, 1, 0).await;
    store::mark_exited(db, &record.id, record.generation, ReplExitReason::Closed)
        .await
        .expect("mark exited");

    let summary = repl_host::close_job_repl(&orch, ctx.job_id.clone(), "lex".to_string())
        .await
        .expect("discard");
    assert!(summary.contains("Removed"), "got: {summary}");
    assert!(store::load(db, &ctx.job_id, "lex")
        .await
        .expect("load")
        .is_none());
    assert!(
        store::history(db, &record.id)
            .await
            .expect("history")
            .is_empty(),
        "the transcript is swept with the row"
    );

    let again = repl_host::close_job_repl(&orch, ctx.job_id.clone(), "lex".to_string())
        .await
        .expect("idempotent");
    assert!(again.contains("No REPL named"), "got: {again}");
}

// End-to-end: stopping a live REPL keeps it readable, resuming it continues the
// same transcript as generation 2, and the namespace genuinely starts empty.
// Guarded to skip if no python interpreter is available.
#[tokio::test]
async fn repl_stop_keeps_the_transcript_and_resume_starts_an_empty_namespace() {
    use cairn_core::internal::mcp::handlers::repl::store::{self, ReplExitReason, ReplRowStatus};
    use cairn_core::internal::mcp::handlers::repl::{
        self, ReplExchangeStatus, ReplLang, ReplOrigin,
    };
    use cairn_core::internal::repl_host::{self, ReplOpenKind};

    let run_id = "run-repl-resume";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let db = &orch.db.local;

    let Ok(opened) = repl_host::open_repl(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        "lex",
        Some(ReplLang::Python),
        None,
    )
    .await
    else {
        eprintln!("skipping repl_stop_keeps_the_transcript: no python/uv available");
        return;
    };
    assert_eq!(opened.kind, ReplOpenKind::Created);
    assert_eq!(opened.info.generation, 1);

    let bound = repl::send_recorded(
        &orch,
        &ctx.job_id,
        "lex",
        "x = 41",
        Duration::from_secs(30),
        ReplOrigin::Agent,
        Some(ReplLang::Python),
    )
    .await
    .expect("send");
    assert_eq!(bound.status, ReplExchangeStatus::Success);

    let stopped = repl_host::close_job_repl(&orch, ctx.job_id.clone(), "lex".to_string())
        .await
        .expect("stop");
    assert!(stopped.contains("Stopped"), "got: {stopped}");
    let row = store::load(db, &ctx.job_id, "lex")
        .await
        .expect("load")
        .expect("a stopped REPL keeps its row");
    assert_eq!(row.status, ReplRowStatus::Exited);
    assert_eq!(row.exit_reason, Some(ReplExitReason::Closed));
    assert_eq!(
        repl_host::get_repl_history(&orch, ctx.job_id.clone(), "lex".to_string())
            .await
            .len(),
        1,
        "stopping must not discard the transcript"
    );

    // Resume with NO payload: interpreter and deps are inherited from the row.
    let resumed = repl_host::open_repl(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        "lex",
        None,
        None,
    )
    .await
    .expect("resume");
    assert_eq!(resumed.kind, ReplOpenKind::Resumed);
    assert_eq!(resumed.info.generation, 2);
    assert_eq!(resumed.info.interpreter, "python");
    assert!(
        resumed.summary().contains("EMPTY"),
        "a resume must say the namespace does not come back: {}",
        resumed.summary()
    );

    // The transcript continues; the namespace does not.
    let gone = repl::send_recorded(
        &orch,
        &ctx.job_id,
        "lex",
        "x",
        Duration::from_secs(30),
        ReplOrigin::Agent,
        Some(ReplLang::Python),
    )
    .await
    .expect("send");
    assert_eq!(
        gone.status,
        ReplExchangeStatus::Error,
        "a resumed REPL starts with an empty namespace"
    );
    let history = repl_host::get_repl_history(&orch, ctx.job_id.clone(), "lex".to_string()).await;
    assert_eq!(history.len(), 2, "the transcript spans both generations");
    assert_eq!(history[0].seq, 0);
    assert_eq!(history[0].generation, 1);
    assert_eq!(history[1].seq, 1);
    assert_eq!(
        history[1].generation, 2,
        "the transcript records where the session restarted"
    );

    if let Some(session) = orch.repl_state.remove(&ctx.job_id, "lex") {
        session.stop_and_release(&orch).await;
    }
}

// A namespace snapshot from a superseded generation must not be painted onto the
// live one. A send can receive its response and then be delayed before
// `set_bindings`; a close-plus-resume in that window installs the next generation
// with bindings cleared, and an unfenced write would make the fresh interpreter
// report variables it does not have — confidently wrong, which is worse than
// reporting nothing. Deterministic: it drives the ordering directly.
#[tokio::test]
async fn a_stale_binding_snapshot_cannot_land_on_a_resumed_generation() {
    use cairn_core::internal::mcp::handlers::repl::store::{self, ReplExitReason};
    use cairn_core::internal::mcp::handlers::repl::{ReplBinding, ReplLang};

    let run_id = "run-repl-stale-bindings";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let db = &orch.db.local;

    let first = store::begin(db, &ctx.job_id, None, "lex", ReplLang::Python, &[])
        .await
        .expect("create");
    let bindings = vec![ReplBinding {
        name: "df".into(),
        kind: "DataFrame".into(),
        info: Some("len 1000".into()),
    }];
    store::set_bindings(db, &first.id, first.generation, &bindings)
        .await
        .expect("gen 1 bindings");
    assert_eq!(
        store::load(db, &ctx.job_id, "lex")
            .await
            .expect("load")
            .expect("row")
            .bindings
            .len(),
        1
    );

    // Generation 1 ends and generation 2 resumes, which clears the namespace.
    store::mark_exited(db, &first.id, first.generation, ReplExitReason::Closed)
        .await
        .expect("stop gen 1");
    let second = store::begin(db, &ctx.job_id, None, "lex", ReplLang::Python, &[])
        .await
        .expect("resume");
    assert_eq!(second.generation, 2);
    assert!(
        second.bindings.is_empty(),
        "a resumed generation starts with an empty namespace"
    );

    // The delayed send from generation 1 finally writes its snapshot.
    let updated = store::set_bindings(db, &first.id, first.generation, &bindings)
        .await
        .expect("stale snapshot");
    assert_eq!(updated, 0, "a stale generation must update no rows");
    assert!(
        store::load(db, &ctx.job_id, "lex")
            .await
            .expect("load")
            .expect("row")
            .bindings
            .is_empty(),
        "the resumed generation must not inherit a dead generation's namespace"
    );

    // The owning generation still records its own namespace.
    let updated = store::set_bindings(db, &second.id, second.generation, &bindings)
        .await
        .expect("gen 2 bindings");
    assert_eq!(updated, 1);
}

// A teardown that resolves after a resume must not declare the LIVE generation
// dead. This is the invariant behind the `generation` guard on `mark_exited`:
// stopping a child yields, and a resume can install the next generation in that
// window, so an update keyed only by `repl_id` would mark the replacement exited.
// Fully deterministic — it drives the two orderings directly rather than racing.
#[tokio::test]
async fn a_stale_teardown_cannot_mark_a_resumed_generation_exited() {
    use cairn_core::internal::mcp::handlers::repl::store::{self, ReplExitReason, ReplRowStatus};
    use cairn_core::internal::mcp::handlers::repl::ReplLang;

    let run_id = "run-repl-stale-exit";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let db = &orch.db.local;

    let first = store::begin(db, &ctx.job_id, None, "lex", ReplLang::Python, &[])
        .await
        .expect("create");
    assert_eq!(first.generation, 1);

    // Generation 1 stops, and generation 2 is resumed while its teardown is still
    // in flight (the `stop_and_release` await window).
    store::mark_exited(db, &first.id, first.generation, ReplExitReason::Closed)
        .await
        .expect("stop gen 1");
    let second = store::begin(db, &ctx.job_id, None, "lex", ReplLang::Python, &[])
        .await
        .expect("resume");
    assert_eq!(second.generation, 2);

    // The stale teardown finally lands, still carrying generation 1.
    let updated = store::mark_exited(db, &first.id, first.generation, ReplExitReason::Closed)
        .await
        .expect("stale teardown");
    assert_eq!(updated, 0, "a stale generation must update no rows");

    let row = store::load(db, &ctx.job_id, "lex")
        .await
        .expect("load")
        .expect("row");
    assert_eq!(
        row.status,
        ReplRowStatus::Running,
        "the resumed generation must still be live"
    );
    assert_eq!(row.generation, 2);
    assert_eq!(row.exit_reason, None);

    // The owning generation still closes it correctly.
    let updated = store::mark_exited(db, &second.id, second.generation, ReplExitReason::Closed)
        .await
        .expect("close gen 2");
    assert_eq!(updated, 1);
}

// Opening a REPL must be serialized per slug. Creating a generation spans a
// durable write (`store::begin`) and a registry claim (`insert_if_absent`); if
// that span is not held as one step, a second open bumps the row's generation
// before the first has claimed the slot, leaving the winning session's generation
// disagreeing with the row that names it -- so every exchange it records is
// stamped with an identity the row denies, and the next resume skips a
// generation.
//
// Deterministic by construction: the test holds the slug's lifecycle lock, so the
// open is blocked at a known point rather than raced. A `tokio::join!` of two
// opens does NOT exercise this -- the first runs to completion before the second
// is meaningfully polled, so it passes even with the serialization removed.
#[tokio::test]
async fn open_repl_serializes_generation_creation_per_slug() {
    use cairn_core::internal::mcp::handlers::repl::store;
    use cairn_core::internal::mcp::handlers::repl::ReplLang;
    use cairn_core::internal::repl_host;

    let run_id = "run-repl-serialized-open";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);

    let held_lock = orch.repl_state.lifecycle_lock(&ctx.job_id, "lex");
    let held = held_lock.lock().await;

    let opening = tokio::spawn({
        let orch = orch.clone();
        let job_id = ctx.job_id.clone();
        let cwd = cwd.clone();
        async move {
            repl_host::open_repl(
                &orch,
                &job_id,
                "",
                &cwd,
                None,
                "lex",
                Some(ReplLang::Python),
                None,
            )
            .await
        }
    });

    // Generous: an unserialized open runs to completion (success or failure) well
    // inside this window, so a pass here means it really is gated on the lock.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !opening.is_finished(),
        "open_repl must block on the slug lifecycle lock"
    );
    assert!(
        store::load(&orch.db.local, &ctx.job_id, "lex")
            .await
            .expect("load")
            .is_none(),
        "a blocked open must not have touched the durable identity"
    );

    drop(held);
    let Ok(opened) = opening.await.expect("join") else {
        eprintln!("skipping the post-release half: no python/uv available");
        return;
    };

    // Released, it proceeds exactly once: one generation, and the row names the
    // session that actually holds the slot.
    assert_eq!(opened.info.generation, 1);
    let row = store::load(&orch.db.local, &ctx.job_id, "lex")
        .await
        .expect("load")
        .expect("row");
    assert_eq!(row.generation, 1);
    let session = orch
        .repl_state
        .get(&ctx.job_id, "lex")
        .expect("a session holds the slot");
    assert_eq!(
        session.generation, row.generation,
        "the live session generation must match its row"
    );

    if let Some(session) = orch.repl_state.remove(&ctx.job_id, "lex") {
        session.stop_and_release(&orch).await;
    }
}

// The registry's identity-guarded ops keep a close+recreate-during-a-send safe:
// an obsolete operation holding the old session must not evict or clobber the
// replacement generation installed under the same slug. Guarded to skip if no
// python interpreter is available.
#[tokio::test]
async fn repl_registry_guards_close_recreate_during_inflight() {
    use cairn_core::internal::mcp::handlers::repl::{self, ReplLang};
    use std::sync::Arc;

    let run_id = "run-repl-recreate";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);

    let Ok(a) = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Python,
        "fn",
        &[],
    )
    .await
    else {
        eprintln!("skipping repl_registry_guards: no python/uv available");
        return;
    };
    orch.repl_state
        .insert(ctx.job_id.clone(), "fn".to_string(), a.clone());

    // User stops the busy REPL and recreates the same slug while A's send is
    // still notionally in flight: close (remove + stop/release A), then install B.
    orch.repl_state.remove(&ctx.job_id, "fn");
    a.stop_and_release(&orch).await;
    let Ok(b) = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Python,
        "fn",
        &[],
    )
    .await
    else {
        return;
    };
    assert!(orch
        .repl_state
        .insert_if_absent(ctx.job_id.clone(), "fn".to_string(), b.clone()));

    // A's obsolete Dead/Timeout cleanup (remove_if with the OLD Arc) must not
    // evict the replacement B.
    assert!(
        !orch.repl_state.remove_if(&ctx.job_id, "fn", &a),
        "an obsolete outcome must not evict the replacement generation"
    );
    let current = orch
        .repl_state
        .get(&ctx.job_id, "fn")
        .expect("B still registered");
    assert!(
        Arc::ptr_eq(&current, &b),
        "the replacement generation must survive"
    );

    // A concurrent create racing B must not clobber it either.
    let Ok(c) = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Python,
        "fn",
        &[],
    )
    .await
    else {
        b.stop_and_release(&orch).await;
        return;
    };
    assert!(
        !orch
            .repl_state
            .insert_if_absent(ctx.job_id.clone(), "fn".to_string(), c.clone()),
        "insert must refuse an occupied slot"
    );
    let current = orch
        .repl_state
        .get(&ctx.job_id, "fn")
        .expect("B still registered");
    assert!(Arc::ptr_eq(&current, &b));
    c.stop_and_release(&orch).await;

    // The live session's own close (remove_if with the current Arc) still works.
    assert!(orch.repl_state.remove_if(&ctx.job_id, "fn", &b));
    b.stop_and_release(&orch).await;
}

// A send into a session whose child already exited settles as `Died`,
// unregisters the session, and emits a session-ending `repl-state` `exited`.
// Guarded to skip if no python interpreter is available.
#[tokio::test]
async fn repl_funnel_dead_session_unregisters_and_emits_exited() {
    use cairn_core::internal::mcp::handlers::repl::{
        self, ReplExchangeStatus, ReplLang, ReplOrigin,
    };
    use cairn_core::internal::services::testing::CapturingEmitter;

    let run_id = "run-repl-dead";
    let emitter = Arc::new(CapturingEmitter::new());
    let (_temp, _db, orch, cwd) = setup_with_emitter(run_id, emitter.clone()).await;
    let ctx = repl_ctx(run_id, &cwd);

    let Ok(session) = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Python,
        "dead",
        &[],
    )
    .await
    else {
        eprintln!("skipping repl_funnel_dead: no python/uv available");
        return;
    };
    // Stop and release the executor lease out from under the registry, then send.
    session.stop_and_release(&orch).await;
    orch.repl_state
        .insert(ctx.job_id.clone(), "dead".to_string(), session);

    let exchange = repl::send_recorded(
        &orch,
        &ctx.job_id,
        "dead",
        "1 + 1",
        Duration::from_secs(10),
        ReplOrigin::User,
        Some(ReplLang::Python),
    )
    .await
    .expect("a dead send still records an exchange");
    assert_eq!(exchange.status, ReplExchangeStatus::Died);
    assert!(
        !orch.repl_state.contains(&ctx.job_id, "dead"),
        "a dead session must be unregistered"
    );
    let states = emitter.events_named("repl-state");
    assert!(
        states.iter().any(|e| e["status"] == "exited"),
        "a dead send must emit repl-state exited: {states:?}"
    );
}
