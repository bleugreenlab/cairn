//! Execution-substrate tests for the `run` tool: async non-blocking wait,
//! promote-to-terminal on timeout, kill fallback, bounded reader reaping, and
//! chained-command capture. See CAIRN-1620.

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_core::internal::db::DbState;
use cairn_core::internal::mcp::handlers::bash::handle_run;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::RealProcessSpawner;
use cairn_core::internal::storage::{LocalDb, SearchIndex};
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, Fence, RecipeSnapshot, RecipeTrigger, TriggerContext,
    TriggerType,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use turso::params;

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

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap_or_else(|e| panic!("failed to run git {args:?}: {e}"));
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

async fn seed_run(db: &LocalDb, project_id: &str, worktree: &Path, run_id: &str) {
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
    let run_id = run_id.to_string();
    db.write(move |conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let exec_id = exec_id.clone();
        let job_id = job_id.clone();
        let worktree = worktree.clone();
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
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, worktree_path, created_at, updated_at)
                 VALUES (?1, ?2, 'agent-1', ?3, ?4, 'builder', 'running', 'builder', ?5, 1, 1)",
                params![job_id.as_str(), exec_id.as_str(), issue_id.as_str(), project_id.as_str(), worktree.as_str()],
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
    let repo = temp.path().join("repo");
    init_git_repo(&repo);
    // These run-execution tests model commands launched from an agent worktree.
    // Keep the Git repository for simple `git grep` assertions, but add the jj
    // marker that makes the run handler treat the cwd as a worktree rather than
    // the read-only live checkout.
    std::fs::create_dir_all(repo.join(".jj")).unwrap();
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RHG").await;
    seed_run(&db, &project_id, &repo, run_id).await;
    let orch = orchestrator(&temp, db.clone());
    let cwd = repo.display().to_string();
    (temp, db, orch, cwd)
}

fn request(cwd: &str, run_id: Option<&str>, payload: Value) -> McpCallbackRequest {
    McpCallbackRequest {
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

#[tokio::test]
async fn timed_out_item_with_run_context_promotes_to_terminal() {
    let (_temp, db, orch, cwd) = setup("run-promote").await;
    let payload = json!({
        "commands": [{ "command": "echo started; sleep 30", "timeout": 800 }],
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

/// Sub-task jobs must NOT promote on timeout: terminals attach to top-level
/// nodes only (CAIRN-1629), so a promoted task-job terminal URI would never
/// resolve — the agent would hold a running detached process behind a dead
/// pointer. Timeout falls back to the kill path instead.
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
        "commands": [{ "command": "echo started; sleep 30", "timeout": 800 }],
    });
    let result = handle_run(&orch, &request(&cwd, Some("run-subtask-sub"), payload)).await;

    assert!(
        result.contains("timed out"),
        "expected kill-fallback timeout result: {result}"
    );
    assert!(
        result.contains("started"),
        "missing partial output: {result}"
    );
    assert!(
        !result.contains("terminal/run-"),
        "sub-task timeout must not return a promoted terminal URI: {result}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM job_terminals").await,
        0,
        "no terminal row may be created for a sub-task timeout"
    );
    assert!(
        orch.pty_state.sessions.lock().unwrap().is_empty(),
        "no promoted session may exist for a sub-task timeout"
    );
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

#[tokio::test]
async fn chained_commands_surface_all_segments() {
    let (_temp, _db, orch, cwd) = setup("run-chain").await;
    // `||` chain: first git grep misses, second matches the committed README.
    let or_chain = handle_run(
        &orch,
        &request(
            &cwd,
            Some("run-chain"),
            json!({ "commands": [{ "command": "git grep zzzNoMatch || git grep initial || echo none" }] }),
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
