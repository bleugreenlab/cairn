mod common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use cairn_core::internal::db::DbState;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::jj::{self, JjEnv};
use cairn_core::internal::mcp::handlers::bash::handle_run;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::RealProcessSpawner;
use cairn_core::internal::storage::{LocalDb, SearchIndex};
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, Fence, LegacyOnEscape, LegacySandbox, RecipeSnapshot,
    RecipeTrigger, TriggerContext, TriggerType,
};
use serde_json::json;
use tempfile::TempDir;
use turso::params;

// These tests drive the real commit barrier end-to-end. jj is the only VCS
// substrate now, so the agent worktree is a real `.jj` workspace over a shared
// store (mirroring production provisioning). Tests resolve `jj` and self-skip
// with a note when it is unavailable, matching `mcp::vcs`'s jj tests.

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
    assert!(
        output.status.success(),
        "git {args:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_git_repo(repo: &Path) {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "user.name", "Test User"]);
    std::fs::write(repo.join("README.md"), "initial\n").unwrap();
    git(repo, &["add", "README.md"]);
    git(repo, &["commit", "-m", "initial"]);
}

/// The jj binary for the test, or `None` to self-skip when jj is unavailable.
fn jj_bin() -> Option<String> {
    let bin = std::env::var("CAIRN_JJ_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "jj".to_string());
    Command::new(&bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
        .then_some(bin)
}

fn head_sha(repo: &Path) -> String {
    let out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Provision a `.jj` workspace at `ws` over a shared store backed by
/// `project_repo`, mirroring production worktree provisioning. `config_dir` is
/// the orchestrator's config dir, so the barrier's `JjEnv` resolves the same
/// store and config.
fn provision_jj_workspace(config_dir: &Path, project_repo: &Path, ws: &Path, branch: &str) {
    let jj = JjEnv::resolve("jj", config_dir);
    let store = jj::project_store_dir(config_dir, project_repo);
    jj::ensure_project_store(&jj, &store, project_repo).unwrap();
    let base = head_sha(project_repo);
    jj::add_workspace(&jj, &store, ws, branch, &base, None).unwrap();
}

/// Whether the jj workspace `@` is empty (equals its sealed parent).
fn ws_clean(config_dir: &Path, ws: &Path) -> bool {
    let jj = JjEnv::resolve("jj", config_dir);
    !jj::is_working_copy_dirty(&jj, ws).unwrap()
}

fn agent_snapshot(sandbox: LegacySandbox, on_escape: LegacyOnEscape) -> AgentSnapshot {
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
        fence: Some(Fence::from_legacy(Some(sandbox), Some(on_escape))),
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
    run_id: &str,
    sandbox: LegacySandbox,
    on_escape: LegacyOnEscape,
) {
    let mut agents = HashMap::new();
    agents.insert("agent-1".to_string(), agent_snapshot(sandbox, on_escape));
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: format!("recipe-{run_id}"),
            name: "Run hygiene test".to_string(),
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
                 VALUES (?1, ?2, 1, 'Run hygiene', 'active', 1, 1)",
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
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, backend, created_at, updated_at, start_mode)
                 VALUES (?1, ?2, ?3, ?4, 'live', 'codex', 1, 1, 'resume')",
                params![run_id.as_str(), project_id.as_str(), issue_id.as_str(), job_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Build an orchestrator whose seeded run owns a real jj workspace at the cwd.
async fn setup_with_sandbox(
    run_id: &str,
    sandbox: LegacySandbox,
    on_escape: LegacyOnEscape,
) -> (TempDir, Orchestrator, String) {
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RHG").await;
    let ws = temp.path().join("ws");
    seed_run(&db, &project_id, &ws, run_id, sandbox, on_escape).await;
    let orch = orchestrator(&temp, db);
    provision_jj_workspace(
        &temp.path().join("config"),
        &project_repo,
        &ws,
        "agent/RHG-1-builder-0",
    );
    let cwd = ws.display().to_string();
    (temp, orch, cwd)
}

async fn setup(run_id: &str) -> (TempDir, Orchestrator, String) {
    setup_with_sandbox(run_id, LegacySandbox::Worktree, LegacyOnEscape::Allow).await
}

/// Seed a run whose fence is `deny`, so the commit-hygiene gate is active.
async fn setup_deny(run_id: &str) -> (TempDir, Orchestrator, String) {
    setup_with_sandbox(run_id, LegacySandbox::Worktree, LegacyOnEscape::Deny).await
}

fn run_request(
    cwd: &str,
    run_id: &str,
    command: &str,
    commit_msg: Option<&str>,
) -> McpCallbackRequest {
    let mut payload = json!({ "commands": [{ "command": command }] });
    if let Some(commit_msg) = commit_msg {
        payload["commit_msg"] = json!(commit_msg);
    }
    McpCallbackRequest {
        cwd: cwd.to_string(),
        run_id: Some(run_id.to_string()),
        tool: "run".to_string(),
        payload,
        tool_use_id: Some("toolu-run".to_string()),
    }
}

#[tokio::test]
async fn run_without_commit_msg_reverts_new_dirt() {
    // Spawns a real sandbox-exec-confined command; nested inside the agent fence
    // the in-worktree write is denied, so the hygiene revert never sees dirt.
    // Skip in a fence; unfenced CI exercises the real revert.
    if common::skip_if_fenced("run_without_commit_msg_reverts_new_dirt") {
        return;
    }
    let Some(_bin) = jj_bin() else {
        eprintln!("skipping run_without_commit_msg_reverts_new_dirt: jj not resolvable");
        return;
    };
    let (temp, orch, cwd) = setup_deny("run-revert").await;
    let result = handle_run(
        &orch,
        &run_request(&cwd, "run-revert", "echo generated > generated.txt", None),
    )
    .await;

    assert!(result.contains("Run reverted"), "result: {result}");
    assert!(!Path::new(&cwd).join("generated.txt").exists());
    assert!(ws_clean(&temp.path().join("config"), Path::new(&cwd)));
}

#[tokio::test]
async fn full_sandbox_run_without_commit_msg_is_not_gated_or_reverted() {
    let Some(_bin) = jj_bin() else {
        eprintln!("skipping full_sandbox_run_without_commit_msg_is_not_gated_or_reverted: jj not resolvable");
        return;
    };
    let (temp, orch, cwd) = setup_with_sandbox(
        "run-full-sandbox",
        LegacySandbox::Full,
        LegacyOnEscape::Allow,
    )
    .await;
    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            "run-full-sandbox",
            "echo full > full-sandbox.txt",
            None,
        ),
    )
    .await;

    assert!(!result.contains("Run reverted"), "result: {result}");
    assert!(
        !result.contains("no commit_msg was given"),
        "result: {result}"
    );
    assert!(Path::new(&cwd).join("full-sandbox.txt").exists());
    // A full-sandbox run is not gated, so its new dirt is left un-sealed.
    assert!(!ws_clean(&temp.path().join("config"), Path::new(&cwd)));
}

#[tokio::test]
async fn run_with_commit_msg_seals_and_cleans() {
    let Some(_bin) = jj_bin() else {
        eprintln!("skipping run_with_commit_msg_seals_and_cleans: jj not resolvable");
        return;
    };
    let (temp, orch, cwd) = setup("run-commit").await;
    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            "run-commit",
            "echo committed > committed.txt",
            Some("commit generated file"),
        ),
    )
    .await;

    assert!(result.contains("Committed changes"), "result: {result}");
    assert!(Path::new(&cwd).join("committed.txt").exists());
    assert!(ws_clean(&temp.path().join("config"), Path::new(&cwd)));
}

#[tokio::test]
async fn run_with_commit_msg_in_non_worktree_cwd_is_rejected_before_spawning() {
    // A manager / triage / chat agent runs on the project's live checkout (a
    // plain git checkout, no `.jj`). A run carrying commit_msg there must be
    // rejected BEFORE any command runs, so the user's checkout is never written
    // to or dirtied. Mirrors the write-side "changes only in a worktree" guard.
    // No jj or sandbox needed: the rejection is a pure pre-spawn check.
    let dir = tempfile::tempdir().unwrap();
    init_git_repo(dir.path());
    let (_temp, orch) = common::test_orchestrator().await;
    let cwd = dir.path().display().to_string();

    let request = McpCallbackRequest {
        cwd: cwd.clone(),
        run_id: None,
        tool: "run".to_string(),
        payload: json!({
            "commands": [{ "command": "printf x > generated.txt" }],
            "commit_msg": "work"
        }),
        tool_use_id: Some("toolu-run".to_string()),
    };

    let result = handle_run(&orch, &request).await;

    assert!(
        result.contains("Commits require a worktree"),
        "result: {result}"
    );
    assert!(
        !dir.path().join("generated.txt").exists(),
        "the command must not run: no file may be left in the user's live checkout"
    );
}

#[tokio::test]
async fn dirty_worktree_notice_skips_non_worktree_request() {
    let dir = tempfile::tempdir().unwrap();
    init_git_repo(dir.path());
    std::fs::write(dir.path().join("dirty.txt"), "dirty\n").unwrap();
    let (_temp, orch) = common::test_orchestrator().await;
    let cursors = Mutex::new(HashMap::new());
    let request = McpCallbackRequest {
        cwd: dir.path().display().to_string(),
        run_id: None,
        tool: "read".to_string(),
        payload: json!({ "path": "file:README.md" }),
        tool_use_id: None,
    };

    let result = dispatch_tool(&orch, &request, &cursors).await;
    assert!(
        !result
            .reminders
            .iter()
            .any(|r| r.contains("The worktree has uncommitted changes")),
        "{result:?}"
    );
}
