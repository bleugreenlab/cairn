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

fn one_command(command: &str) -> Value {
    json!({ "commands": [{ "command": command }] })
}

/// The canon invariant (tenet 1246), end to end: a read-shaped item is served
/// as a read from the job's head coordinate with no executor in the fleet at
/// all. This orchestrator has nothing to place work onto, so if any part of a
/// batch reached lease acquisition or slot placement it could not have
/// succeeded.
#[tokio::test]
async fn read_shaped_items_are_served_as_labeled_reads_without_any_executor() {
    let (_temp, _db, orch, cwd) = setup_without_executor("run-read-served").await;
    let run = |payload: Value| {
        let request = request(&cwd, Some("run-read-served"), payload);
        let orch = &orch;
        async move { run_text(&handle_run(orch, &request).await) }
    };

    for (command, label, content) in [
        (
            "rg initial .",
            "↳ read file:?grep=initial&output_mode=content [1 matches in 1 files]",
            "1:initial",
        ),
        (
            "grep -rn initial .",
            "↳ read file:?grep=initial&output_mode=content [1 matches in 1 files]",
            "1:initial",
        ),
        ("cat README.md", "↳ read file:README.md", "initial"),
        (
            "find . -name '*.md'",
            "↳ read file:?glob=**/*.md [1 files]",
            "README.md",
        ),
    ] {
        let text = run(one_command(command)).await;
        assert!(
            text.starts_with(label) && text.contains(content),
            "{command} should be served under {label:?}: {text}"
        );
        assert!(
            !text.contains(REACHED_PLACEMENT) && !text.contains("Exit code"),
            "{command} must be served, successfully, without placement: {text}"
        );
    }

    // Exit status follows the shell: a miss is 1, a pipeline is its last
    // stage's 0, a search over a missing path is 2, a missing file is 1.
    for (command, status) in [
        ("rg nomatch .", Some(1)),
        ("rg nomatch . | head -5", None),
        ("rg initial missing/", Some(2)),
        ("cat missing.txt", Some(1)),
    ] {
        let text = run(one_command(command)).await;
        assert!(text.starts_with("↳ read "), "{command} is served: {text}");
        match status {
            Some(code) => assert!(
                text.ends_with(&format!("Exit code: {code}")),
                "{command} should exit {code}: {text}"
            ),
            None => assert!(!text.contains("Exit code"), "{command} exits 0: {text}"),
        }
    }

    // Several items compose into one result, each under its own header.
    let text = run(json!({ "commands": [
        { "command": "rg initial ." },
        { "command": "cat README.md" },
    ] }))
    .await;
    assert!(
        text.starts_with("=== rg initial . ===\n↳ read file:?grep=initial")
            && text.contains("\n=== cat README.md ===\n↳ read file:README.md"),
        "each item renders under its header: {text}"
    );

    // A sequential stop-on-error batch stops after its first failing read.
    let text = run(json!({
        "sequential": true,
        "stop_on_error": true,
        "commands": [{ "command": "rg nomatch ." }, { "command": "rg initial ." }],
    }))
    .await;
    assert!(
        text.ends_with("Exit code: 1") && !text.contains("1:initial"),
        "the batch stops after the miss: {text}"
    );
}

/// Read's per-turn dedup is for the model holding the earlier result, so a run
/// item served as a read is deduped exactly when the run itself is model-facing
/// (the `_cairn_origin: model` stamp): never for a program's run, which neither
/// receives the stub nor records anything that would stub the model.
#[tokio::test]
async fn a_served_read_is_deduped_only_for_a_model_facing_run() {
    use cairn_common::protocol::{stamp_callback_origin, MODEL_FACING_ORIGIN};

    let run_id = "run-read-dedup";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    orch.process_state.processes.lock().unwrap().register(
        run_id.to_string(),
        RunHandle::test_handle(Some("sess"), None),
    );
    assert!(orch.process_state.begin_turn(run_id, "turn-1"));
    let run = |model_facing: bool| {
        let mut payload = one_command("rg initial .");
        if model_facing {
            stamp_callback_origin(&mut payload, MODEL_FACING_ORIGIN);
        }
        let request = request(&cwd, Some(run_id), payload);
        let orch = &orch;
        async move { run_text(&handle_run(orch, &request).await) }
    };

    // An unstamped (SDK-shaped) run is served in full every time.
    for attempt in 1..=2 {
        let text = run(false).await;
        assert!(
            text.contains("1:initial") && !text.contains("[duplicate"),
            "a program's run #{attempt} gets the whole read: {text}"
        );
    }

    // The model's first run is fresh (the program's runs recorded nothing);
    // its unchanged repeat is read's duplicate stub.
    let first = run(true).await;
    assert!(
        first.contains("1:initial") && !first.contains("[duplicate"),
        "{first}"
    );
    let repeat = run(true).await;
    assert!(
        repeat.contains("[duplicate") && !repeat.contains("1:initial"),
        "the model's unchanged repeat is a duplicate: {repeat}"
    );
}

/// A read-shaped item is served under its caller's read budget: a program's run
/// gets a body past the model-context frame whole, and a model-facing run of
/// the same item gets read's framed page with its continue footer.
#[tokio::test]
async fn a_served_read_past_the_context_frame_reaches_a_program_whole() {
    use cairn_common::protocol::{stamp_callback_origin, MODEL_FACING_ORIGIN};

    let run_id = "run-read-whole";
    let (temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let big = temp.path().join("big.txt");
    let body: String = (0..20_000).map(|n| format!("line {n}\n")).collect();
    assert!(body.len() > 150_000);
    std::fs::write(&big, &body).unwrap();
    let run = |model_facing: bool| {
        let mut payload = one_command(&format!("cat {}", big.display()));
        if model_facing {
            stamp_callback_origin(&mut payload, MODEL_FACING_ORIGIN);
        }
        let request = request(&cwd, Some(run_id), payload);
        let orch = &orch;
        async move { run_text(&handle_run(orch, &request).await) }
    };

    let program = run(false).await;
    assert!(
        program.starts_with("↳ read "),
        "served: {}",
        &program[..200]
    );
    assert!(
        program.contains("line 0\n") && program.contains("line 19999"),
        "a program's served read is whole"
    );
    assert!(program.len() > body.len(), "{}", program.len());
    assert!(!program.contains("continue:"));

    let model = run(true).await;
    assert!(model.starts_with("↳ read "), "served: {}", &model[..200]);
    assert!(model.contains("line 0\n") && !model.contains("line 19999"));
    assert!(model.contains("continue:"), "the model's page is framed");
    assert!(model.len() < 60_000, "{}", model.len());
}

/// The same fixture proves the assertion above has teeth. Anything without a
/// read equivalent, and any batch holding one, still needs the executor that
/// does not exist.
#[tokio::test]
async fn shapes_without_a_read_equivalent_reach_placement() {
    let (_temp, _db, orch, cwd) = setup_without_executor("run-read-machine").await;

    let batches = [
        one_command("echo hello"),
        // A read beside a non-read: one batch, one substrate.
        json!({ "commands": [{ "command": "rg initial ." }, { "command": "echo hello" }] }),
        one_command("rg initial . | awk '{print $1}'"),
        one_command("rg -m1 initial ."),
        one_command("rg initial . | sort"),
    ];
    for payload in batches {
        let result = handle_run(
            &orch,
            &request(&cwd, Some("run-read-machine"), payload.clone()),
        )
        .await;
        let text = run_text(&result);
        assert!(
            text.contains(REACHED_PLACEMENT),
            "{payload} must reach placement: {text}"
        );
    }
}

/// A read that only a fence permission could open is never asked about: the
/// item runs on a machine instead, where its own containment applies, and no
/// permission request is ever recorded.
#[tokio::test]
async fn a_read_that_would_need_a_fence_permission_goes_to_a_machine_unasked() {
    let mut agent = agent_snapshot();
    agent.fence = Some(Fence::Ask);
    let (_temp, db, orch, cwd) = setup_with_agent("run-read-fence", false, agent).await;
    let denied = cairn_sandbox::default_deny_read()
        .into_iter()
        .find(|path| path.ends_with(".ssh"))
        .expect("~/.ssh is denied by default")
        .join("cairn-run-read-probe");
    // The deny list matches a path by where it resolves, so the probe has to
    // exist inside the denied directory rather than resolve to its parent.
    std::fs::create_dir_all(denied.parent().unwrap()).unwrap();
    std::fs::write(&denied, "probe\n").unwrap();

    let result = handle_run(
        &orch,
        &request(
            &cwd,
            Some("run-read-fence"),
            one_command(&format!("cat {}", denied.display())),
        ),
    )
    .await;
    let text = run_text(&result);
    assert!(
        text.contains(REACHED_PLACEMENT) && !text.contains(DURABLE_SUSPEND),
        "a fenced read goes to a machine rather than suspending: {text}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM permission_requests").await,
        0,
        "serving a run item as a read must never raise a permission request"
    );
}

fn orchestrator_without_executor(temp: &TempDir, db: Arc<LocalDb>) -> Orchestrator {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_process(RealProcessSpawner)
            .build(),
    );
    // Owns every seeded run, as `common::orchestrator` does: a child's call that
    // the executor relays arrives through `dispatch_tool`, whose ownership fence
    // refuses runs that predate the host's boot.
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config"))
        .boot_at(0)
        .build();
    common::load_fleet_settings(&orch);
    orch
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
    git(repo, &["init", "--initial-branch=main"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "user.name", "Test User"]);
    std::fs::write(repo.join("README.md"), "initial\n").unwrap();
    git(repo, &["add", "README.md"]);
    git(repo, &["commit", "-m", "initial"]);
}

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
        // An Allow fence means no OS confinement is applied, keeping these tests
        // platform-independent while still exercising the run_context-driven
        // promotion path.
        fence: Some(Fence::Allow),
        sandbox: None,
        on_escape: None,
        resolved_backend: None,
        extras: None,
        seed_budget_tokens: None,
        seed_eras: None,
    }
}

async fn seed_run(
    db: &LocalDb,
    project_id: &str,
    worktree: &Path,
    branch: &str,
    base_commit: &str,
    run_id: &str,
) {
    seed_run_as(
        db,
        project_id,
        worktree,
        branch,
        base_commit,
        run_id,
        agent_snapshot(),
    )
    .await;
}

async fn seed_run_as(
    db: &LocalDb,
    project_id: &str,
    _worktree: &Path,
    branch: &str,
    base_commit: &str,
    run_id: &str,
    agent: AgentSnapshot,
) {
    let entity_id = |kind: &str| match run_id.split_once('~') {
        Some((team, _)) => format!("{team}~{}", uuid::Uuid::new_v4()),
        None => format!("{kind}-{run_id}"),
    };
    let issue_id = entity_id("issue");
    let exec_id = entity_id("exec");
    let job_id = entity_id("job");
    let mut agents = HashMap::new();
    agents.insert("agent-1".to_string(), agent);
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
            issue_id: Some(issue_id.clone()),
            project_id: project_id.to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap();

    let project_id = project_id.to_string();
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
    setup_with_agent(run_id, attach_executor, agent_snapshot()).await
}

async fn setup_with_agent(
    run_id: &str,
    attach_executor: bool,
    agent: AgentSnapshot,
) -> (TempDir, Arc<LocalDb>, Orchestrator, String) {
    // A suite run from inside a pinned Cairn batch inherits that batch's
    // `CAIRN_BATCH_EXECUTOR`, and the test executor's shells would inherit it
    // in turn, so a test asserting what a batch's own pin is would read its
    // caller's. No test here wants the caller's pin.
    std::env::remove_var(cairn_common::executor_protocol::BATCH_EXECUTOR_ENV);
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "rhg", &project_repo).await;
    let worktree = temp.path().join("worktree");
    let branch = "agent/RHG-1-builder-0";
    let base_commit = common::head_sha(&project_repo);
    seed_run_as(
        &db,
        &project_id,
        &worktree,
        branch,
        &base_commit,
        run_id,
        agent,
    )
    .await;
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
        answer_bound_ms: None,
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: run_id.map(ToOwned::to_owned),
        tool: "run".to_string(),
        payload,
        tool_use_id: None,
        origin: None,
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
    let project_id = common::insert_project_with_repo(&db, "rhg", &project_repo).await;
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

/// A busy machine is a queue to wait in, but a constraint no executor in the
/// fleet can satisfy is not: waiting would park the agent on something nobody is ever going to clear. So
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

/// A default the machine cannot serve refuses the batch by name, with where the
/// pin came from and the two ways out, rather than placing it elsewhere.
#[tokio::test]
async fn a_node_placement_default_that_cannot_serve_refuses_with_the_way_out() {
    if common::skip_if_fenced("a_node_placement_default_that_cannot_serve_refuses_with_the_way_out")
    {
        return;
    }
    let run_id = "run-node-default-refused";
    let (_temp, db, orch, cwd) = setup(run_id).await;
    // Stored directly: the write door refuses a platform Cairn does not place
    // work on, and this is the shape of a default whose machine went away.
    db.execute(
        "INSERT INTO placement_defaults (id, job_id, selector, set_by, set_at)
         VALUES ('pd-refused', 'job-run-node-default-refused', '{\"os\":\"plan9\"}', 'operator', 1)",
        (),
    )
    .await
    .unwrap();
    let text = run_text(
        &handle_run(
            &orch,
            &request(
                &cwd,
                Some(run_id),
                json!({"commands": [{ "command": "echo never" }]}),
            ),
        )
        .await,
    );
    assert!(text.contains("This run could not execute"), "{text}");
    assert!(text.contains("plan9"), "{text}");
    assert!(
        text.contains("os plan9 is this node's placement default"),
        "{text}"
    );
    assert!(
        text.contains("`write cairn:~ patch {executor:null}`"),
        "{text}"
    );
}

/// `issue_history` is issue-only BY DESIGN, so a thread must be refused rather
/// than handed a read that quietly drops the annotation it asked for.
///
/// This is the honest-refusal half of the CAIRN-4306 sweep. The silent failure
/// it forecloses is the family's whole signature: a thread session's job carries
/// `issue_id = NULL`, so every history query keyed off that column returns zero
/// rows, and the read renders successfully with the history simply absent. The
/// agent cannot tell an empty history from a surface that never looked.
///
/// The ordinary-issue arm is what gives this teeth: the identical target and
/// query on an issue-owned run must NOT hit the refusal, proving the guard reads
/// job ownership rather than rejecting `issue_history` outright.
#[tokio::test]
async fn a_thread_is_refused_issue_history_and_an_ordinary_issue_still_gets_it() {
    const REFUSAL: &str = "this run is thread-owned and has no issue history";

    let (issue_temp, _issue_db, issue_orch, issue_cwd) =
        setup_without_jj("run-history-issue").await;
    let target = issue_temp.path().join("history.txt");
    std::fs::write(&target, "history-probe\n").unwrap();
    let issue_target = format!("file:{}?issue_history=true", target.display());
    let issue_read = read_text(
        &issue_orch,
        &issue_cwd,
        Some("run-history-issue"),
        &issue_target,
    )
    .await;
    assert!(
        !issue_read.contains(REFUSAL),
        "an issue-owned run must still be served issue_history: {issue_read}"
    );
    // The control is only a control if it actually READ. Asserting merely that
    // the refusal is absent would pass just as well for a read that failed for
    // some unrelated reason, which would leave the ownership comparison vacuous.
    assert!(
        issue_read.contains("history-probe"),
        "the issue-owned arm must reach a real read, or it proves nothing: {issue_read}"
    );

    let (thread_temp, thread_db, thread_orch, thread_cwd) =
        setup_without_jj("run-history-thread").await;
    let thread_target = thread_temp.path().join("history.txt");
    std::fs::write(&thread_target, "history-probe\n").unwrap();
    convert_run_to_thread_session(&thread_db, "run-history-thread", "history-thread").await;

    let thread_read = read_text(
        &thread_orch,
        &thread_cwd,
        Some("run-history-thread"),
        &format!("file:{}?issue_history=true", thread_target.display()),
    )
    .await;
    assert!(
        thread_read.contains(REFUSAL),
        "the thread must be told why it gets no history, not handed a silent omission: {thread_read}"
    );
}

/// `setup` without the jj workspace, because this pair never seals anything.
///
/// The refusal under test is taken before any path or VCS work, and the control
/// arm is a plain read; provisioning a jj workspace would only couple the test to
/// a toolchain it does not exercise and make it skip where it could have run.
async fn setup_without_jj(run_id: &str) -> (TempDir, Arc<LocalDb>, Orchestrator, String) {
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "rhg", &project_repo).await;
    let worktree = temp.path().join("worktree");
    std::fs::create_dir_all(&worktree).unwrap();
    let base_commit = common::head_sha(&project_repo);
    seed_run(
        &db,
        &project_id,
        &worktree,
        "agent/RHG-1-builder-0",
        &base_commit,
        run_id,
    )
    .await;
    let orch = orchestrator_without_executor(&temp, db.clone());
    let cwd = worktree.display().to_string();
    (temp, db, orch, cwd)
}

/// Rewrite a seeded issue-owned run into the PRODUCTION shape of a thread
/// session: `jobs.issue_id` NULL with `jobs.thread_id` set, the run detached from
/// the issue, and no issue row left anywhere in the chain.
///
/// The execution SURVIVES the conversion, detached from the issue. A live thread
/// session job carries one — a passive host holding the thread's agent snapshot,
/// stamped on by `ensure_host_agent_snapshot` the first time the session acts —
/// and `execution_id` is machinery rather than ownership, so the job stays
/// thread-owned with it (`threads::classify_owner`). Deleting it produced a
/// shape no live thread has: the run → job → execution → snapshot walk that
/// resolves a fence and an OS sandbox policy found nothing, and every batch such
/// a fixture ran was refused before it started.
///
/// The branch and base commit go, because a thread runs on the base branch and
/// owns no agent branch of its own.
///
/// `execute_script`, not `execute`: this is several statements and `execute`
/// runs only the first, which would leave an ordinary issue-owned job that every
/// thread guard then correctly declines to refuse. The conversion is asserted
/// rather than assumed for the same reason — an issue-shaped fixture asserting
/// thread behavior is the exact false confidence this sweep exists to remove.
async fn convert_run_to_thread_session(db: &LocalDb, run_id: &str, thread_name: &str) {
    let issue_id = format!("issue-{run_id}");
    db.execute_script(format!(
        "INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) \
         SELECT id,project_id,'{thread_name}','active','none',created_at,updated_at \
         FROM issues WHERE id='{issue_id}'; \
         UPDATE jobs SET thread_id='{issue_id}',issue_id=NULL,\
         branch=NULL,base_commit=NULL,uri_segment='thread' \
         WHERE issue_id='{issue_id}'; \
         UPDATE runs SET issue_id=NULL WHERE issue_id='{issue_id}'; \
         UPDATE executions SET issue_id=NULL,seq=NULL WHERE issue_id='{issue_id}'; \
         DELETE FROM issues WHERE id='{issue_id}'"
    ))
    .await
    .unwrap();
    assert_eq!(
        common::scalar_text_by_id(
            db,
            "SELECT thread_id FROM jobs WHERE id = ?1",
            &format!("job-{run_id}")
        )
        .await
        .as_deref(),
        Some(issue_id.as_str()),
        "the job under test must actually belong to the thread"
    );
    // A real thread mints its own database beside the main one the moment it is
    // created (`db_store::initialize_for_creation`). This fixture fabricates the
    // thread row directly, so it has to place the store the same way; without it
    // the thread's batches reach for the developer's live data directory.
    point_thread_store_at_fixture(db, &issue_id);
}

fn point_thread_store_at_fixture(db: &LocalDb, thread_id: &str) {
    cairn_core::threads::db_store::use_test_path(
        thread_id,
        db.path()
            .parent()
            .expect("the fixture database lives in a directory")
            .join("thread-databases")
            .join(format!("{thread_id}.db")),
    );
}

/// Procedures are retired (tenet 1313), and a `run` target that names one is
/// refused with the service shape that replaced it, at the session and through
/// a canonical URI alike. It must never be diagnosed as a skill script: that
/// is a true sentence about a resource the caller never addressed.
#[tokio::test]
async fn a_retired_procedure_target_is_refused_with_the_service_shape() {
    let (_temp, db, orch, cwd) = setup("run-procedure-thread").await;
    convert_run_to_thread_session(&db, "run-procedure-thread", "news-collector").await;

    for target in [
        "cairn:~/procedures/echo-args",
        "cairn:~/procedures",
        "cairn://p/rhg/news-collector/procedures/echo-args",
        "cairn:~/automations",
    ] {
        let refusal = run_text(
            &handle_run(
                &orch,
                &request(
                    &cwd,
                    Some("run-procedure-thread"),
                    json!({"commands": [{"target": target}]}),
                ),
            )
            .await,
        );
        assert!(refusal.contains("tenet 1313"), "{target}: {refusal}");
        assert!(refusal.contains("serve({methods"), "{target}: {refusal}");
        assert!(
            refusal.contains("cairn://p/rhg/news-collector/services"),
            "{target}: {refusal}"
        );
        assert!(!refusal.contains("skill script"), "{target}: {refusal}");
    }
}

async fn read_text(orch: &Orchestrator, cwd: &str, run_id: Option<&str>, path: &str) -> String {
    cairn_core::internal::mcp::handlers::read::handle_read_file(
        orch,
        &McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: cwd.to_string(),
            run_id: run_id.map(ToOwned::to_owned),
            tool: "read".to_string(),
            payload: json!({ "path": path }),
            tool_use_id: None,
            origin: None,
        },
    )
    .await
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
    convert_run_to_thread_session(&thread_db, "run-workflow-thread", "workflow-thread").await;

    let refusal = run_text(
        &handle_run(
            &thread_orch,
            &request(&thread_cwd, Some("run-workflow-thread"), batch),
        )
        .await,
    );
    assert!(
        refusal.contains("thread-owned job"),
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

// Each item waits for a marker written by the other. Concurrent execution lets
// both succeed; serialization makes the first item fail before the second starts.
// This proves overlap without relying on wall-clock timing under a loaded suite.

#[tokio::test]
async fn tree_bound_item_without_resolvable_context_fails_before_execution() {
    // Depends on the same real subprocess timeout-kill lifecycle as the sub-task
    // timeout case. Under the agent worktree fence, an inherited sandbox denial
    // can win before the timeout path this test is meant to exercise.
    if common::skip_if_fenced("tree_bound_item_without_resolvable_context_fails_before_execution") {
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

#[cfg(windows)]
async fn windows_stdout_handoff() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the holder's socket");
    let port = listener.local_addr().unwrap().port();
    // Only the receiver creates the duplicate, directly into its own process,
    // so transport failure cannot strand an unowned handle in the receiver.
    // The fixed-width packet carries the child's PID and its actual fd 1.
    let code = format!(
        "import msvcrt, os, socket, struct\nwith socket.create_connection(('127.0.0.1', {port}), timeout={timeout}) as s:\n    s.sendall(struct.pack('<IQ', os.getpid(), msvcrt.get_osfhandle(1)))\n    if s.recv(1) != b'K':\n        raise RuntimeError('stdout holder did not acknowledge ownership')\nprint('handed off', flush=True)\n",
        timeout = HELD_STDOUT_BOUND.as_secs(),
    );
    (listener, code)
}

#[cfg(windows)]
async fn receive_windows_stdout(listener: &tokio::net::TcpListener) -> std::fs::File {
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use windows_sys::Win32::{
        Foundation::{DuplicateHandle, DUPLICATE_SAME_ACCESS},
        Storage::FileSystem::{GetFileType, FILE_TYPE_PIPE},
        System::Threading::{GetCurrentProcess, OpenProcess, PROCESS_DUP_HANDLE},
    };

    let (mut connection, _) = listener.accept().await.expect("the command connected");
    let mut packet = [0u8; 12];
    connection
        .read_exact(&mut packet)
        .await
        .expect("read the stdout handoff");
    let pid = u32::from_le_bytes(packet[..4].try_into().unwrap());
    let source = usize::try_from(u64::from_le_bytes(packet[4..].try_into().unwrap()))
        .expect("the stdout handle fits this process's pointer width");
    // SAFETY: opening a process requires no pointer arguments. The child waits
    // for our acknowledgement, so its stdout handle remains live while copied.
    let process = unsafe { OpenProcess(PROCESS_DUP_HANDLE, 0, pid) };
    assert!(
        !process.is_null(),
        "open stdout owner: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: OpenProcess returned a new, owned process handle.
    let process = unsafe { OwnedHandle::from_raw_handle(process) };
    let mut duplicate = std::ptr::null_mut();
    // SAFETY: source is the live child's fd 1 handle from the fixed-width
    // packet; process and the current-process pseudo handle name the source
    // and destination. duplicate points to storage for the newly owned handle.
    let copied = unsafe {
        DuplicateHandle(
            process.as_raw_handle(),
            source as _,
            GetCurrentProcess(),
            &mut duplicate,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    };
    assert_ne!(
        copied,
        0,
        "duplicate stdout: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: successful DuplicateHandle created this handle in our process.
    // Own it before any assertion, await, or other fallible operation.
    let held = std::fs::File::from(unsafe { OwnedHandle::from_raw_handle(duplicate) });
    // SAFETY: held owns the live duplicate for the duration of GetFileType.
    assert_eq!(unsafe { GetFileType(held.as_raw_handle()) }, FILE_TYPE_PIPE);
    drop(process);
    connection
        .write_all(b"K")
        .await
        .expect("acknowledge the held stdout");
    held
}

// The holder is this test, outside the command's process tree. Unix transfers
// fd 1 with SCM_RIGHTS; Windows duplicates the child's fd 1 into this process
// and acknowledges ownership before the child exits. A descendant holder would
// not prove this: command teardown may kill it and close the pipe itself.
//
// A warm-up acquires the execution home outside the bound. The handoff has no
// item timeout, because a timeout also bounds placement, not just execution.
// Unix can receive after the call returns because SCM_RIGHTS keeps the pipe
// alive in the kernel's queued message. Windows receives concurrently; join!
// retains the owned duplicate until the call returns. Cancelling the enclosing
// timeout drops both futures and any duplicate, without a background receiver.

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

// Process cwd is non-authoritative. A run keeps using its authenticated branch
// coordinate even when its residence happens to be an unrelated Git checkout.

// Inline python routes through `uv run -` when uv resolves and falls back to
// `python3 -c` otherwise. This end-to-end check is path-agnostic: both
// uv-managed CPython and system python3 print `py3` for the major version, so it
// passes whichever rung of the ladder the host takes.

/// The other half of the resolution story, and the one the runner-owned SDK
/// depends on: a checkout with NO `node_modules` of its own resolves a bare
/// `@cairn/sdk` from the Cairn-owned tree installed ABOVE it. That upward walk
/// is bun's, not ours -- which is exactly why it is worth a test. A bun release
/// that stopped walking would not fail loudly here; it would silently return
/// this promise to the broken state the field reported it in.
///
/// The same fixture pins the precedence rule, because precedence is the reason
/// placement was chosen over a resolver hook: a project that genuinely depends
/// on the SDK has its own copy NEARER the importer, so it wins by the resolution
/// algorithm rather than by code of ours that could get it wrong.
#[test]
fn a_runner_installed_ancestor_tree_resolves_a_bare_specifier() {
    if !binary_available("bun") {
        eprintln!(
            "skipping a_runner_installed_ancestor_tree_resolves_a_bare_specifier: bun not resolvable"
        );
        return;
    }
    let temp = TempDir::new().unwrap();
    // Mirror a real cell coordinate: `<root>/build-slots/<project>/slot-N`.
    let checkout = temp.path().join("build-slots/demo/slot-1");
    std::fs::create_dir_all(&checkout).unwrap();

    let package = |marker: &str| {
        vec![
            cairn_common::executor_protocol::ResidentRuntimeAsset {
                path: "@cairn/sdk/package.json".into(),
                data: br#"{"name":"@cairn/sdk","type":"module","exports":{".":"./index.ts"}}"#
                    .to_vec(),
            },
            cairn_common::executor_protocol::ResidentRuntimeAsset {
                path: "@cairn/sdk/index.ts".into(),
                data: format!("export const marker = \"{marker}\";\n").into_bytes(),
            },
        ]
    };

    let parent = cairn_common::runtime::shared_runtime_parent(&checkout)
        .expect("a build-slot checkout has a Cairn-owned parent");
    let assets = package("ANCESTOR_SDK");
    let version = cairn_common::runtime::assets_version(&assets);
    let root = cairn_common::runtime::install_shared_runtime(&parent, &assets, &version).unwrap();
    assert_eq!(root, temp.path().join("build-slots/demo/node_modules"));

    let resolve = || {
        let output = Command::new("bun")
            .args([
                "-e",
                "import { marker } from \"@cairn/sdk\"; console.log(marker)",
            ])
            .current_dir(&checkout)
            .output()
            .unwrap();
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    };

    let from_ancestor = resolve();
    assert!(
        from_ancestor.contains("ANCESTOR_SDK"),
        "a checkout with no node_modules of its own must resolve the runner tree above it: {from_ancestor}"
    );

    // Now give the checkout its own copy, as a project that really depends on
    // the SDK would have after installing its dependencies.
    let local = checkout.join("node_modules/@cairn/sdk");
    std::fs::create_dir_all(&local).unwrap();
    for asset in package("PROJECT_SDK") {
        let name = asset.path.rsplit('/').next().unwrap();
        std::fs::write(local.join(name), &asset.data).unwrap();
    }

    let from_project = resolve();
    assert!(
        from_project.contains("PROJECT_SDK"),
        "a project's own node_modules is nearer the importer and must win: {from_project}"
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

/// The environment a REPL hands its executor names Cairn's own keys and nothing
/// of the runner's machine.
///
/// The vector crosses to whichever executor holds the owner's cell, which may
/// be an enrolled remote, and the executor layers it over its own environment,
/// so any machine fact here overrides that machine's value. The REPL once built
/// this vector with the host-spawn helper, which shipped the runner's
/// `CAIRN_HOME`, `UV_CACHE_DIR`, jj/git worktree variables, and build-service
/// env to every cell. Asserting on the composed specification rather than on a
/// child's environment is deliberate: a colocated test executor shares this
/// process's `HOME`, so a child could not tell a shipped value from its own.
#[tokio::test]
async fn a_repl_ships_only_cairn_identity_to_its_executor() {
    use cairn_core::internal::mcp::handlers::repl::{self, ReplLang};

    let run_id = "run-repl-env";
    let (_temp, _db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let repo_path = std::path::Path::new(&cwd)
        .parent()
        .unwrap()
        .join("project")
        .display()
        .to_string();

    let spec = repl::process_spec(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        &repo_path,
        Some("agent/RHG-1-builder-0"),
        Some(&ctx),
        ReplLang::Python,
        &["numpy".to_string()],
    )
    .await
    .expect("a python REPL's process spec composes without an executor");

    let value = |key: &str| {
        spec.env
            .iter()
            .rev()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(value("CAIRN_RUN_ID").as_deref(), Some(run_id));
    assert_eq!(
        value("CAIRN_HOME_URI").as_deref(),
        Some("cairn://p/rhg/1/1/builder")
    );
    assert_eq!(
        value("CAIRN_WORKTREE_BRANCH").as_deref(),
        Some("agent/RHG-1-builder-0")
    );
    // Its callback route and capability come from the resident relay as it
    // starts, so the spec names neither.
    let unexpected: Vec<&str> = spec
        .env
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| {
            !matches!(
                *name,
                "CAIRN_RUN_ID" | "CAIRN_HOME_URI" | "CAIRN_WORKTREE_BRANCH"
            )
        })
        .collect();
    assert!(
        unexpected.is_empty(),
        "a REPL must not carry the runner's environment: {unexpected:?}"
    );
    for machine_fact in [
        "HOME",
        "CAIRN_HOME",
        "UV_CACHE_DIR",
        "PATH",
        "TMPDIR",
        "CAIRN_WORKTREE",
    ] {
        assert!(value(machine_fact).is_none(), "{machine_fact} crossed");
    }
    // The eval-server's stdout is one JSON frame per line.
    assert_eq!(
        spec.stdout_framing,
        cairn_common::executor_protocol::ResidentStreamFraming::Lines
    );
    // The deps still reach the interpreter's own launch, not its environment.
    assert_eq!(spec.program, "uv");
    assert!(spec.args.windows(2).any(|pair| pair == ["--with", "numpy"]));
}

// State persists across two separate `handle_run` calls routed into the same
// live REPL session: `x = 41` then `x + 1` returns `42`. Guarded to skip if no
// interpreter is available to spawn the eval-server.

// A REPL answers every send even when a registered secret begins with a
// newline.
//
// The runner registers its MCP secret as 32 raw random bytes, so one install in
// 256 has a secret whose first byte is `\n`. A byte-stream scrubber withholds
// any trailing run that is a proper prefix of a registered form, which for that
// install is the newline closing every response frame: the eval-server has
// answered, but the line never completes, and the send waits out its bound.
// Registering such a value here makes that install deterministic. The guard is
// held for the test's lifetime, and nextest runs each test in its own process.
#[tokio::test]
async fn repl_answers_when_a_registered_secret_begins_with_a_newline() {
    use cairn_core::internal::mcp::handlers::repl::{self, ReplLang};
    use cairn_core::security::{registry, SecretCategory, SecretId, SecretMaterial};

    let _leading_newline = registry()
        .register(
            SecretId::new("test:leading-newline"),
            SecretCategory::CallbackCredential,
            "unit test",
            SecretMaterial::from_bytes(b"\nQ7m2Zx9-RedTeam-leading-newline"),
        )
        .expect("registerable");

    let run_id = "run-repl-leading-newline";
    let (_temp, _db, orch, cwd) = setup(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let Ok(session) = repl::spawn_session(
        &orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        ReplLang::Python,
        "analysis",
        &[],
        None,
    )
    .await
    else {
        eprintln!("skipping repl_answers_when_a_registered_secret_begins_with_a_newline: no python/uv available");
        return;
    };
    orch.repl_state
        .insert(ctx.job_id.clone(), "analysis".to_string(), session);

    let first = handle_run(
        &orch,
        &request(
            &cwd,
            Some(run_id),
            json!({ "commands": [{ "code": "x = 41\nx", "interpreter": "python", "repl": "analysis", "timeout": 15000 }] }),
        ),
    )
    .await;
    assert!(
        first.contains("41") && !first.contains("timed out"),
        "a response frame was held back by the scrubber: {first}"
    );

    let second = handle_run(
        &orch,
        &request(
            &cwd,
            Some(run_id),
            json!({ "commands": [{ "code": "x + 1", "interpreter": "python", "repl": "analysis", "timeout": 15000 }] }),
        ),
    )
    .await;
    assert!(second.contains("42"), "the session must survive: {second}");

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
        thread_id: None,
        project_id: String::new(),
        project_key: "RHG".to_string(),
        job_name: Some("builder".to_string()),
        agent_config_id: None,
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
        None,
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

// Build a REPL run context matching a job seeded by `seed_run`.
fn repl_ctx(run_id: &str, _cwd: &str) -> cairn_core::internal::mcp::handlers::RunContext {
    cairn_core::internal::mcp::handlers::RunContext {
        run_id: run_id.to_string(),
        job_id: format!("job-{run_id}"),
        exec_seq: Some(1),
        issue_id: Some(format!("issue-{run_id}")),
        issue_number: Some(1),
        thread_id: None,
        project_id: String::new(),
        project_key: "RHG".to_string(),
        job_name: Some("builder".to_string()),
        agent_config_id: None,
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
    let project_id = common::insert_project_with_repo(&db, "rhg", &project_repo).await;
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
    common::load_fleet_settings(&orch);

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
        None,
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

// A thread session is branchless by construction: it is commit-fenced, owns no
// branch and no PR. `spawn_session` used to read the job's branch, fall back to
// its base branch, and refuse a job with neither — a refusal no thread could ever
// satisfy, so REPL in a thread pane's + menu was dead and so was
// `write cairn:~/repl/<slug>` from a thread agent. A branchless owner now lands
// in the project's live checkout, exactly where its terminals already run, while
// an issue node's job keeps resolving its own branch.

/// The project a seeded job belongs to, so a test can address the same project a
/// thread session would hang off.
async fn project_id_for_job(db: &LocalDb, job_id: &str) -> String {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT project_id FROM jobs WHERE id = ?1", params![job_id])
                .await?;
            let row = rows.next().await?.expect("the seeded job exists");
            Ok(row.get_value(0)?.as_text().expect("project id").to_string())
        })
    })
    .await
    .expect("read project id")
}

/// A thread and its session job: `uri_segment = 'thread'` with no parent, and
/// NULL branch/base_branch — the branchless shape a thread session always has.
///
/// `execution` binds the job to an execution and a run of its own, which is what
/// a thread looks like once it has delegated a sub-agent task and the only shape
/// in which its worktree fence resolves at all. A thread that has never delegated
/// carries neither, so nothing confines it — tracked as CAIRN-3871.
async fn seed_thread_session_job(
    db: &LocalDb,
    project_id: &str,
    job_id: &str,
    execution: Option<(&str, &str)>,
) {
    let project_id = project_id.to_string();
    let job_id = job_id.to_string();
    let execution = execution.map(|(exec, run)| (exec.to_string(), run.to_string()));
    db.write(move |conn| {
        let project_id = project_id.clone();
        let job_id = job_id.clone();
        let execution = execution.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR IGNORE INTO threads (id, project_id, name, status, attention, created_at, updated_at)
                 VALUES ('thread-1', ?1, 'general', 'active', 'none', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, thread_id, project_id, node_name, uri_segment, status, created_at, updated_at)
                 VALUES (?1, 'thread-1', ?2, 'thread', 'thread', 'idle', 1, 1)",
                params![job_id.as_str(), project_id.as_str()],
            )
            .await?;
            if let Some((execution_id, run_id)) = execution {
                conn.execute(
                    "UPDATE jobs SET execution_id = ?1, agent_config_id = 'agent-1' WHERE id = ?2",
                    params![execution_id.as_str(), job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at, start_mode)
                     VALUES (?1, ?2, ?3, 'live', 1, 1, 'resume')",
                    params![run_id.as_str(), project_id.as_str(), job_id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .expect("seed thread session job");
}

/// The project's live checkout path, which is the tree a branchless owner's
/// processes run in.
async fn repo_path_for_project(db: &LocalDb, project_id: &str) -> String {
    let project_id = project_id.to_string();
    db.read(move |conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT repo_path FROM projects WHERE id = ?1",
                    params![project_id],
                )
                .await?;
            let row = rows.next().await?.expect("the seeded project exists");
            Ok(row.get_value(0)?.as_text().expect("repo path").to_string())
        })
    })
    .await
    .expect("read repo path")
}

/// Re-stamp the seeded execution's agent with a fence. The fixture's default is
/// `allow` (which deliberately applies no confinement anywhere), so a test about
/// what confinement is applied has to ask for a fence that applies some.
async fn set_seeded_fence(db: &LocalDb, execution_id: &str, fence: &str) {
    let execution_id = execution_id.to_string();
    let fence = fence.to_string();
    let snapshot = db
        .read({
            let execution_id = execution_id.clone();
            move |conn| {
                let execution_id = execution_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT snapshot FROM executions WHERE id = ?1",
                            params![execution_id],
                        )
                        .await?;
                    let row = rows.next().await?.expect("the seeded execution exists");
                    Ok(row.get_value(0)?.as_text().expect("snapshot").to_string())
                })
            }
        })
        .await
        .expect("read snapshot");
    let mut parsed: Value = serde_json::from_str(&snapshot).expect("snapshot is json");
    parsed["agents"]["agent-1"]["fence"] = json!(fence);
    let updated = parsed.to_string();
    db.execute(
        "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
        params![updated.as_str(), execution_id.as_str()],
    )
    .await
    .expect("stamp fence");
}

// The fence a REPL spawns under belongs to its OWNER, not to whoever asked for
// it. A REPL opened from a node or thread pane's `+` menu carries no run context
// at all, and deriving the fence identity from the caller answered "nobody's
// agent operation": no policy was built, the resident spec declared itself
// unconfined, and the interpreter could write whatever checkout it landed in —
// including, for a branchless owner, the operator's live tree. A terminal has
// always resolved the job's latest run from the job row for exactly this reason.
#[tokio::test]
async fn repl_sandbox_follows_the_owner_fence_when_the_caller_has_no_run_context() {
    use cairn_common::executor_protocol::ProcessSandboxMode;
    use cairn_core::internal::mcp::handlers::repl;

    let run_id = "run-repl-fence";
    let (_temp, db, orch, cwd) = setup_without_executor(run_id).await;
    let ctx = repl_ctx(run_id, &cwd);
    let project_id = project_id_for_job(&db, &ctx.job_id).await;
    let repo_path = repo_path_for_project(&db, &project_id).await;
    set_seeded_fence(&db, &format!("exec-{run_id}"), "ask").await;

    // An issue node's REPL, opened the way the UI opens one: confined to the
    // agent's own checkout, which it may write.
    let (mode, policy) = repl::repl_sandbox(
        &orch,
        &ctx.job_id,
        &project_id,
        &cwd,
        &repo_path,
        Some("agent/RHG-1-builder-0"),
        None,
    )
    .await;
    let policy = policy.expect("a fenced owner must produce a policy without a caller run context");
    assert_eq!(mode, ProcessSandboxMode::Confined);
    assert!(
        policy.worktree_writable,
        "a REPL in the agent's own execution home writes that checkout"
    );

    // A branchless owner's REPL, opened the same way: the live checkout is its
    // worktree and is NOT writable, and no session grant may reopen it.
    seed_thread_session_job(
        &db,
        &project_id,
        "job-thread-fence",
        Some((&format!("exec-{run_id}"), "run-thread-fence")),
    )
    .await;
    let (mode, policy) = repl::repl_sandbox(
        &orch,
        "job-thread-fence",
        &project_id,
        &cwd,
        &repo_path,
        None,
        None,
    )
    .await;
    let policy = policy.expect("a fenced branchless owner must produce a policy");
    assert_eq!(mode, ProcessSandboxMode::ReadOnlyCheckout);
    assert_eq!(policy.worktree, Path::new(&repo_path));
    assert!(
        !policy.worktree_writable,
        "a branchless REPL must not be able to write the operator's live checkout"
    );
    assert!(
        !policy
            .writable_paths()
            .iter()
            .any(|path| path.starts_with(&repo_path)),
        "nothing inside the live checkout may be writable"
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
        None,
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
        None,
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
        None,
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

// ---- A workflow run across waves (CAIRN-5367) -----------------------------

/// The script a two-wave workflow runs. Each wave is three concurrent host
/// calls; `write` throws on a refused one, which exits the script non-zero.
/// Between the waves it marks that the first landed and waits to be released.
const TWO_WAVE_SCRIPT: &str = r#"import { parallel, output } from "@cairn/harness";
import { write } from "@cairn/sdk";
import { existsSync, writeFileSync } from "node:fs";

const wave = (n: number) =>
  parallel(
    [0, 1, 2].map((i) => () =>
      write({ target: "cairn:~/progress", mode: "append", payload: { kind: "log", text: `wave-${n}-${i}` } }),
    ),
  );

await wave(1);
writeFileSync(MARKER, "");
while (!existsSync(RELEASE)) await Bun.sleep(100);
await wave(2);
await output("both waves landed");
"#;

/// Whether this host can confine a workflow runtime, which starts under the
/// credential floor's profile. A process that cannot nest a Seatbelt profile
/// is the declared `nested-sandbox-exec` gap, recorded as a skip; any other
/// host that cannot confine fails the test.
fn workflow_confines_here(test: &str) -> bool {
    let here = cairn_sandbox::nested_sandbox_here();
    if here.declines() {
        eprintln!("skipping {test}: {}", here.detail());
        cairn_common::skip_ledger::record_skip(test, "nested-sandbox-exec");
        return false;
    }
    if let Err(why) = cairn_sandbox::IsolatedChild::availability() {
        panic!("{test} needs a host that can confine a workflow runtime: {why}");
    }
    true
}

/// What became of a run, for a failure message.
async fn run_story(db: &LocalDb, run_id: &str) -> String {
    db.query_opt_text(
        "SELECT COALESCE(r.status, '') || ' / ' || COALESCE(r.exit_reason, '') || ' / ' ||
                COALESCE(r.error_message, '') || ' / ' ||
                COALESCE((SELECT group_concat(data, ' | ') FROM events WHERE run_id = r.id), '')
         FROM runs r WHERE r.id = ?1",
        (run_id.to_string(),),
    )
    .await
    .unwrap()
    .unwrap_or_default()
}

async fn progress_entries(db: &LocalDb, job_id: &str, wave: &str) -> i64 {
    db.query_opt_i64(
        "SELECT COUNT(*) FROM workflow_progress WHERE job_id = ?1 AND text LIKE ?2",
        (job_id.to_string(), format!("{wave}-%")),
    )
    .await
    .unwrap()
    .unwrap_or(0)
}

/// A two-wave workflow, end to end on a real Bun runtime. Its host calls are
/// answered in both waves, a delegated child settling between them wakes
/// nothing, the stale-run reaper leaves it alone however old it looks, and its
/// clean exit with its output stored completes the run and answers its caller.
///
/// The run this pins lost on all three counts: the reaper settled it because a
/// workflow runtime was invisible to the liveness check, each settled child
/// queued a wake that tried to resume the script as an agent, and its exit read
/// as having written nothing because only an agent's write was ever marked.
///
/// No provider can run a real child agent under test, so the child an
/// `agent()` call would have spawned is stood in for by a background packet on
/// the workflow job, settled through the completion path a finished child
/// takes. That path is where the wake came from.
#[tokio::test(flavor = "multi_thread")]
async fn a_two_wave_workflow_survives_its_children_settling_and_the_reaper() {
    use cairn_core::internal::runs::reap::{
        reap_stale_runs_for_job, STALE_RUN_EXIT_REASON, STALE_RUN_GRACE_SECS,
    };
    use cairn_workflow::delegated_packets;

    if !workflow_confines_here("a_two_wave_workflow_survives_its_children_settling_and_the_reaper")
    {
        return;
    }
    let (temp, db, orch, cwd) = setup("run-two-wave").await;
    let marker = temp.path().join("wave-1.done");
    let release = temp.path().join("wave-2.release");
    let package = temp
        .path()
        .join("config")
        .join("workflows")
        .join("two-wave");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join("workflow.yaml"),
        "name: Two Wave\ndescription: Two waves of host calls.\nscript: main.ts\n",
    )
    .unwrap();
    std::fs::write(package.join("package.json"), "{}").unwrap();
    let js_path = |path: &Path| json!(path.display().to_string()).to_string();
    std::fs::write(
        package.join("main.ts"),
        TWO_WAVE_SCRIPT
            .replace("MARKER", &js_path(&marker))
            .replace("RELEASE", &js_path(&release)),
    )
    .unwrap();
    let invoke = request(
        &cwd,
        Some("run-two-wave"),
        json!({ "commands": [{ "target": "cairn://workflows/two-wave" }] }),
    );

    let drive = async {
        let deadline = Instant::now() + Duration::from_secs(60);
        let (job_id, run_id) = loop {
            let node = db
                .query_opt_text(
                    "SELECT j.id || ' ' || r.id FROM jobs j JOIN runs r ON r.job_id = j.id
                     WHERE j.agent_config_id = 'workflow'",
                    (),
                )
                .await
                .unwrap();
            if let Some((job, run)) = node.as_deref().and_then(|ids| ids.split_once(' ')) {
                break (job.to_string(), run.to_string());
            }
            assert!(
                Instant::now() < deadline,
                "the workflow node never appeared"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        while !marker.is_file() {
            assert!(
                Instant::now() < deadline,
                "the first wave never landed; the run: {}",
                run_story(&db, &run_id).await
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(progress_entries(&db, &job_id, "wave-1").await, 3);
        // The runtime can be answering calls before its spawn returns, while
        // the spawn's launch claim still covers the run. Once the run reads
        // live the claim is gone and the bound lease alone stands behind it,
        // which is the cover the reaper must see.
        loop {
            let status = db
                .query_opt_text("SELECT status FROM runs WHERE id = ?1", (run_id.clone(),))
                .await
                .unwrap();
            if status.as_deref() == Some("live") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the run never went live; the run: {}",
                run_story(&db, &run_id).await
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            orch.claimed_launch_run(&job_id),
            None,
            "a live workflow run holds no launch claim"
        );

        // A background child of the workflow finishes.
        let execution_id = db
            .query_opt_text(
                "SELECT execution_id FROM jobs WHERE id = ?1",
                (job_id.clone(),),
            )
            .await
            .unwrap()
            .expect("the workflow node has an execution");
        let turn_id = db
            .query_opt_text(
                "SELECT current_turn_id FROM jobs WHERE id = ?1",
                (job_id.clone(),),
            )
            .await
            .unwrap();
        db.execute(
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment,
                               agent_config_id, parent_job_id, status, created_at, updated_at)
             SELECT 'wave-child', execution_id, issue_id, project_id, 'Explore', 'wave-child',
                    'Explore', id, 'complete', 1, 1
             FROM jobs WHERE id = ?1",
            params![job_id.as_str()],
        )
        .await
        .unwrap();
        delegated_packets::insert_db(
            &db,
            &execution_id,
            &serde_json::from_value(json!({
                "id": "wave-child-packet",
                "parentJobId": job_id,
                "parentTurnId": turn_id,
                "origin": "task_tool",
                "title": "Explore",
                "problemStatement": "x",
                "agentConfigId": "Explore",
                "ownership": { "cwd": "/tmp" },
                "outputContract": { "schemaType": "return" },
                "status": "running",
                "resultArtifactJobId": "wave-child",
                "background": true,
                "createdAt": 0
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        cairn_core::internal::resume_parent_after_task_completion(&orch, "wave-child").unwrap();
        let child = delegated_packets::find_by_result_job_db(&db, &execution_id, "wave-child")
            .await
            .unwrap()
            .expect("the child's packet");
        assert_eq!(delegated_packets::status_str(&child.status), "completed");

        // Long past the reaper's grace, with a process behind it all along.
        let long_ago = chrono::Utc::now().timestamp() - STALE_RUN_GRACE_SECS - 60;
        db.execute(
            "UPDATE runs SET created_at = ?1, started_at = ?1 WHERE id = ?2",
            params![long_ago, run_id.as_str()],
        )
        .await
        .unwrap();
        // Swept for the workflow's job alone: the caller is a fixture run with
        // no process behind it, which a full sweep would rightly settle.
        assert_eq!(
            reap_stale_runs_for_job(&orch, &db, &job_id),
            0,
            "a workflow run with its runtime alive is not stale"
        );

        std::fs::write(&release, "").unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            let status = db
                .query_opt_text("SELECT status FROM runs WHERE id = ?1", (run_id.clone(),))
                .await
                .unwrap()
                .unwrap_or_default();
            if !["starting", "live"].contains(&status.as_str()) {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "the workflow never finished; the run: {}",
                run_story(&db, &run_id).await
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let story = run_story(&db, &run_id).await;
        assert_eq!(status, "exited", "the run: {story}");
        let exit_reason = db
            .query_opt_text(
                "SELECT exit_reason FROM runs WHERE id = ?1",
                (run_id.clone(),),
            )
            .await
            .unwrap();
        assert_ne!(
            exit_reason.as_deref(),
            Some(STALE_RUN_EXIT_REASON),
            "{story}"
        );
        assert_eq!(progress_entries(&db, &job_id, "wave-2").await, 3, "{story}");
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM artifacts WHERE job_id = ?1 AND output_name = 'return'",
                (job_id.clone(),),
            )
            .await
            .unwrap(),
            Some(1),
            "the output artifact is stored"
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM runs WHERE job_id = ?1",
                (job_id.clone(),)
            )
            .await
            .unwrap(),
            Some(1),
            "nothing resumed the script as an agent"
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let workflow = delegated_packets::find_by_result_job_db(&db, &execution_id, &job_id)
                .await
                .unwrap()
                .expect("the caller's packet for the workflow");
            if delegated_packets::status_str(&workflow.status) == "completed" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the caller's packet never completed: {:?}",
                workflow.status
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let (answer, ()) = tokio::join!(handle_run(&orch, &invoke), drive);
    // The caller answers inline unless the waves outlasted its inline budget,
    // in which case the completed packet above is the answer it resumes with.
    let answer = run_text(&answer);
    assert!(
        answer.contains("both waves landed") || answer.contains(DURABLE_SUSPEND),
        "{answer}"
    );
}

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
        None,
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
