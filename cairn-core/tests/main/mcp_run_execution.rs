//! Execution-substrate tests for the `run` tool: async non-blocking wait,
//! promote-to-terminal on timeout, kill fallback, bounded reader reaping, and
//! chained-command capture. See CAIRN-1620.

use crate::common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_core::internal::db::DbState;
use cairn_core::internal::jj;
use cairn_core::internal::mcp::handlers::run::handle_run;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::RealProcessSpawner;
use cairn_core::internal::storage::{LocalDb, SearchIndex};
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, Fence, RecipeSnapshot, RecipeTrigger, TriggerContext,
    TriggerType,
};
use cairn_db::turso::params;
use serde_json::{json, Value};
use tempfile::TempDir;

fn orchestrator(temp: &TempDir, db: Arc<LocalDb>) -> Orchestrator {
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
    worktree: &Path,
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
    let worktree = worktree.display().to_string();
    let branch = branch.to_string();
    let base_commit = base_commit.to_string();
    let run_id = run_id.to_string();
    db.write(move |conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let exec_id = exec_id.clone();
        let job_id = job_id.clone();
        let worktree = worktree.clone();
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
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, worktree_path, branch, base_commit, created_at, updated_at)
                 VALUES (?1, ?2, 'agent-1', ?3, ?4, 'builder', 'running', 'builder', ?5, ?6, ?7, 1, 1)",
                params![
                    job_id.as_str(),
                    exec_id.as_str(),
                    issue_id.as_str(),
                    project_id.as_str(),
                    worktree.as_str(),
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
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "RHG", &project_repo).await;
    let worktree = temp.path().join("worktree");
    let branch = "agent/RHG-1-builder-0";
    let base_commit = common::head_sha(&project_repo);
    seed_run(&db, &project_id, &worktree, branch, &base_commit, run_id).await;
    let orch = orchestrator(&temp, db.clone());
    common::provision_jj_workspace(
        &temp.path().join("config"),
        &project_repo,
        &worktree,
        branch,
    );
    let job_id = format!("job-{run_id}");
    let identity = jj::WorkspaceIdentity::new(
        job_id.clone(),
        job_id,
        &project_id,
        project_repo,
        worktree.clone(),
        branch,
        jj::workspace_name_for_branch(branch),
        base_commit,
    );
    jj::write_workspace_identity(&worktree, &identity).unwrap();
    let cwd = worktree.display().to_string();
    (temp, db, orch, cwd)
}

fn request(cwd: &str, run_id: Option<&str>, payload: Value) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: run_id.map(ToOwned::to_owned),
        tool: "run".to_string(),
        payload,
        tool_use_id: Some("toolu-run".to_string()),
    }
}

/// SIGKILL every live promoted/terminal session so a test's detached `sleep`
/// does not linger after the test process exits.
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

// Two parallel items on a single-threaded tokio runtime (the `#[tokio::test]`
// default) overlap only if the wait is non-blocking: the old try_wait +
// thread::sleep loop monopolized the one worker, serializing the batch (~sum),
// while the async sleep yields so both sleeps run at once (~max).
#[tokio::test]
async fn parallel_items_do_not_block_each_other() {
    let (_temp, _db, orch, cwd) = setup("run-parallel").await;
    let payload = json!({
        "commands": [{ "command": "sleep 2" }, { "command": "sleep 2" }],
        "sequential": false,
    });
    let start = Instant::now();
    let _ = handle_run(&orch, &request(&cwd, Some("run-parallel"), payload)).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(3500),
        "two parallel `sleep 2` items took {elapsed:?}; a blocking wait would serialize to ~4s"
    );
}

/// Timeout (ms) for the promote-on-timeout tests that assert the item's PARTIAL
/// stdout (`started`) is captured before the timeout fires. The capture normally
/// lands in tens of milliseconds, so this is deliberately generous: a tight
/// timeout races the subprocess spawn + output pump against the timer and flakes
/// under heavy CI / turn-end load (the review check cadence now runs its heavy
/// suites concurrently, so the machine can be saturated when these run). It stays
/// far below the commands' 30s sleep, so the timeout still fires first and the
/// promote-to-terminal behavior under test is unchanged.
const PARTIAL_OUTPUT_TIMEOUT_MS: u32 = 3000;

#[tokio::test]
async fn timed_out_item_with_run_context_promotes_to_terminal() {
    let (_temp, db, orch, cwd) = setup("run-promote").await;
    let payload = json!({
        "commands": [{ "command": "echo started; sleep 30", "timeout": PARTIAL_OUTPUT_TIMEOUT_MS }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-promote"), payload)).await;

    // Partial output + a readable, canonical terminal URI.
    assert!(
        result.contains("started"),
        "missing partial output: {result}"
    );
    assert!(
        result.contains("terminal/run-1"),
        "missing terminal URI: {result}"
    );
    assert!(
        result.contains("You will be notified when the command exits"),
        "timeout result should report the automatic wake subscription: {result}"
    );
    assert!(
        !result.contains("subscribe via cairn:~/wakes"),
        "timeout result should not ask the agent to manually subscribe: {result}"
    );

    // The job_terminals row exists.
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM job_terminals WHERE slug = 'run-1'"
        )
        .await,
        1
    );

    // The session is in pty_state as a pipe-backed (no PTY master) session with
    // its buffer seeded by the still-running readers.
    let session_arc = {
        let sessions = orch.pty_state.sessions.lock().unwrap();
        assert_eq!(sessions.len(), 1, "expected exactly one promoted session");
        sessions.values().next().unwrap().clone()
    };
    {
        let session = session_arc.lock().unwrap();
        assert!(
            session.master.is_none(),
            "promoted session must have no PTY master"
        );
        assert!(
            session.writer.is_none(),
            "promoted session must have no PTY writer"
        );
        let buf = session.output_buffer.as_ref().unwrap().lock().unwrap();
        let content = String::from_utf8_lossy(&buf.iter().copied().collect::<Vec<_>>()).to_string();
        assert!(
            content.contains("started"),
            "buffer missing live output: {content}"
        );
    }

    // Promotion automatically subscribes the current job to the terminal exit,
    // keyed on the canonical terminal URI, so the agent does not need to poll or
    // append its own cairn:~/wakes mutation after ending the turn.
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM wake_subscriptions
             WHERE job_id = 'job-run-promote'
               AND source_kind = 'process'
               AND source_ref LIKE '%/terminal/run-1'
               AND fact_kinds_json = '[\"terminal_exit\"]'
               AND state = 'active'
               AND one_shot = 1"
        )
        .await,
        1,
        "promoted item must subscribe a one-shot terminal-exit wake"
    );

    // The checkpoint cache must NOT record a detached item.
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM checkpoint_command_cache WHERE job_id = 'job-run-promote'"
        )
        .await,
        0,
        "promoted item must not cache a checkpoint result"
    );

    kill_all_sessions(&orch);
}

/// Sub-task jobs promote on timeout to a canonical task terminal URI. This keeps
/// the process readable and killable while preserving the task address instead
/// of incorrectly attaching the terminal to the parent node.
#[tokio::test]
async fn timed_out_item_in_subtask_job_kills_instead_of_promoting() {
    // Depends on a real long-running subprocess hitting the timeout-kill path;
    // the agent fence disrupts that subprocess lifecycle. Skip in a fence;
    // unfenced CI exercises the real timeout.
    if common::skip_if_fenced("timed_out_item_in_subtask_job_kills_instead_of_promoting") {
        return;
    }
    let (_temp, db, orch, cwd) = setup("run-subtask").await;
    // Add a sub-task job under the seeded top-level job, and a run on it.
    db.write(move |conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, parent_job_id, worktree_path, created_at, updated_at)
                 SELECT 'job-sub', execution_id, agent_config_id, issue_id, project_id, 'mapper', 'running', 'map-things', id, worktree_path, 1, 1
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

    assert!(
        result.contains("Command still running; detached"),
        "expected promoted terminal result: {result}"
    );
    assert!(
        result.contains("started"),
        "missing partial output: {result}"
    );
    assert!(
        result.contains("/task/map-things/terminal/run-1"),
        "sub-task timeout must return the canonical task terminal URI: {result}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM job_terminals").await,
        1,
        "a terminal row should be created for the promoted sub-task timeout"
    );
    assert_eq!(
        orch.pty_state.sessions.lock().unwrap().len(),
        1,
        "a promoted session should exist for the sub-task timeout"
    );

    kill_all_sessions(&orch);
}

#[tokio::test]
async fn sequential_stop_on_error_halts_after_promoted_item() {
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
        result.contains("terminal/run-1"),
        "missing terminal URI: {result}"
    );
    assert!(
        !result.contains("SHOULD_NOT_RUN"),
        "promoted (not-succeeded) item must halt a stop_on_error batch: {result}"
    );

    kill_all_sessions(&orch);
}

#[tokio::test]
async fn timed_out_item_without_run_context_kills_and_returns() {
    // Depends on the same real subprocess timeout-kill lifecycle as the sub-task
    // timeout case. Under the agent worktree fence, an inherited sandbox denial
    // can win before the timeout path this test is meant to exercise.
    if common::skip_if_fenced("timed_out_item_without_run_context_kills_and_returns") {
        return;
    }
    let (_temp, db, orch, _cwd) = setup("run-nokill").await;
    // A cwd that maps to no job (and run_id None) yields no run context, so the
    // timeout falls back to a kill rather than promotion. The seeded repo is a
    // worktree, so use an independent throwaway repo instead.
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
        result.contains("timed out"),
        "expected timed-out result: {result}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "kill path should return promptly, took {elapsed:?}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM job_terminals").await,
        0,
        "no terminal row without a run context"
    );
    assert!(
        orch.pty_state.sessions.lock().unwrap().is_empty(),
        "no promoted session without a run context"
    );
}

// A child that calls setsid escapes the SIGKILL'd process group and holds the
// stdout pipe write end open, so an unbounded reader join would hang forever.
// The bounded reaping must return within timeout + ~2s grace with partial
// output. (Uses perl for setsid so it works on macOS, which lacks the binary.)
#[tokio::test]
async fn setsid_escapee_does_not_hang_the_call() {
    let (_temp, _db, orch, cwd) = setup("run-setsid").await;
    let escapee = "perl -e 'STDOUT->autoflush(1); if (fork()) { exit 0 } require POSIX; POSIX::setsid(); print \"started\\n\"; sleep 8'";
    let payload = json!({
        "commands": [{ "command": escapee, "timeout": 800 }],
    });
    let start = Instant::now();
    let result = handle_run(&orch, &request(&cwd, None, payload)).await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "a setsid escapee holding the pipe must not hang the call; took {elapsed:?}"
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

// Managed identity applies only after the physical cwd proves it is a jj
// workspace. A plain Git checkout remains a valid run cwd without branch or
// base-commit metadata on its job.
#[tokio::test]
async fn plain_cwd_run_does_not_require_managed_workspace_identity() {
    let (_temp, db, orch, _managed_cwd) = setup("run-plain-cwd").await;
    let loose = tempfile::tempdir().unwrap();
    init_git_repo(loose.path());
    let loose_path = loose.path().display().to_string();
    db.execute(
        "UPDATE jobs SET worktree_path = ?1, branch = NULL, base_commit = NULL
         WHERE id = 'job-run-plain-cwd'",
        params![loose_path.as_str()],
    )
    .await
    .unwrap();

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
    let (_temp, _db, orch, cwd) = setup("run-code-sdk").await;
    let pkg = Path::new(&cwd).join("node_modules/@cairn/sdk");
    std::fs::create_dir_all(&pkg).unwrap();
    std::fs::write(
        pkg.join("package.json"),
        r#"{"name":"@cairn/sdk","type":"module","exports":{".":"./index.ts"}}"#,
    )
    .unwrap();
    std::fs::write(
        pkg.join("index.ts"),
        "export const marker = \"SDK_IMPORT_OK\";\n",
    )
    .unwrap();
    let payload = json!({
        "commands": [{
            "code": "import { marker } from \"@cairn/sdk\"; console.log(marker)",
            "interpreter": "typescript"
        }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-code-sdk"), payload)).await;
    assert!(
        result.contains("SDK_IMPORT_OK"),
        "`bun -e` must resolve a bare package import from the worktree node_modules: {result}"
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

/// A timed-out code item flows through the identical partial-output +
/// promote-to-terminal path as a shell timeout — the timeout machinery is
/// per-spawn and kind-agnostic, so inline code inherits it unchanged.
#[tokio::test]
async fn timed_out_code_item_promotes_to_terminal_with_partial_output() {
    if !binary_available("python3") {
        eprintln!("skipping timed_out_code_item_promotes_to_terminal_with_partial_output: python3 not resolvable");
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
        result.contains("terminal/run-1"),
        "missing terminal URI: {result}"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM job_terminals WHERE slug = 'run-1'"
        )
        .await,
        1,
        "a timed-out code item must promote to a terminal like a shell timeout"
    );
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
            json!({ "commands": [{ "command": "grep zzzNoMatch README.md || grep initial README.md || echo none" }] }),
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
        worktree_path: Some(cwd.clone()),
    };

    let Ok(session) =
        repl::spawn_session(&orch, &ctx, &cwd, ReplLang::Python, "analysis", &[]).await
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
        session.kill();
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
        worktree_path: Some(cwd.clone()),
    };
    let result = repl::spawn_session(
        &orch,
        &ctx,
        &cwd,
        ReplLang::Typescript,
        "ts",
        &["react".to_string()],
    )
    .await;
    let err = match result {
        Ok(session) => {
            session.kill();
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
        worktree_path: Some(cwd.clone()),
    };

    let Ok(session) = repl::spawn_session(&orch, &ctx, &cwd, ReplLang::Typescript, "ts", &[]).await
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
        session.kill();
    }
}
