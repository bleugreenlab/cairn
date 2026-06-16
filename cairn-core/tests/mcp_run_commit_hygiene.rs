mod common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use cairn_core::internal::db::DbState;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::handlers::bash::handle_run;
use cairn_core::internal::mcp::handlers::worktree_status_porcelain;
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
    git(repo, &["init"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "user.name", "Test User"]);
    std::fs::write(repo.join("README.md"), "initial\n").unwrap();
    git(repo, &["add", "README.md"]);
    git(repo, &["commit", "-m", "initial"]);
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

async fn setup(run_id: &str) -> (TempDir, Orchestrator, String) {
    setup_with_sandbox(run_id, LegacySandbox::Worktree, LegacyOnEscape::Allow).await
}

/// Seed a run whose fence is `deny`, so the commit-hygiene gate is active.
async fn setup_deny(run_id: &str) -> (TempDir, Orchestrator, String) {
    setup_with_sandbox(run_id, LegacySandbox::Worktree, LegacyOnEscape::Deny).await
}

async fn setup_with_sandbox(
    run_id: &str,
    sandbox: LegacySandbox,
    on_escape: LegacyOnEscape,
) -> (TempDir, Orchestrator, String) {
    let (temp, db) = common::migrated_db().await;
    let repo = temp.path().join("repo");
    init_git_repo(&repo);
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RHG").await;
    seed_run(&db, &project_id, &repo, run_id, sandbox, on_escape).await;
    let orch = orchestrator(&temp, db);
    let cwd = repo.display().to_string();
    (temp, orch, cwd)
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
async fn worktree_status_porcelain_reports_clean_and_dirty() {
    let dir = tempfile::tempdir().unwrap();
    init_git_repo(dir.path());
    let cwd = dir.path().display().to_string();
    assert_eq!(worktree_status_porcelain(&cwd).unwrap(), "");
    std::fs::write(dir.path().join("dirty.txt"), "dirty\n").unwrap();
    assert!(worktree_status_porcelain(&cwd)
        .unwrap()
        .contains("dirty.txt"));
}

#[tokio::test]
async fn run_without_commit_msg_reverts_clean_entry_dirt() {
    // Spawns a real sandbox-exec-confined command; nested inside the agent
    // fence the in-worktree write is denied, so the hygiene revert never sees
    // dirt. Skip in a fence; unfenced CI exercises the real revert.
    if common::skip_if_fenced("run_without_commit_msg_reverts_clean_entry_dirt") {
        return;
    }
    let (_temp, orch, cwd) = setup_deny("run-revert").await;
    let result = handle_run(
        &orch,
        &run_request(&cwd, "run-revert", "echo generated > generated.txt", None),
    )
    .await;

    assert!(result.contains("Run reverted"), "result: {result}");
    assert!(!Path::new(&cwd).join("generated.txt").exists());
    assert_eq!(worktree_status_porcelain(&cwd).unwrap(), "");
}

#[tokio::test]
async fn full_sandbox_run_without_commit_msg_is_not_gated_or_reverted() {
    let (_temp, orch, cwd) = setup_with_sandbox(
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
    assert!(worktree_status_porcelain(&cwd)
        .unwrap()
        .contains("full-sandbox.txt"));
}

#[tokio::test]
async fn run_with_commit_msg_commits_and_cleans() {
    let (_temp, orch, cwd) = setup("run-commit").await;
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
    assert_eq!(worktree_status_porcelain(&cwd).unwrap(), "");
}

#[tokio::test]
async fn no_commit_outside_merge_or_rebase_restores_to_head() {
    let (_temp, orch, cwd) = setup("run-no-commit").await;
    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            "run-no-commit",
            "echo deliberate > deliberate.txt",
            Some("NO_COMMIT"),
        ),
    )
    .await;

    // worktree==HEAD invariant: NO_COMMIT is an escape for in-progress
    // merge/rebase resolution only. Outside a transition the run's dirt is
    // restored to HEAD and the caller is told why.
    assert!(
        result.contains("NO_COMMIT is only valid while resolving an in-progress merge or rebase"),
        "result: {result}"
    );
    assert!(!Path::new(&cwd).join("deliberate.txt").exists());
    assert_eq!(worktree_status_porcelain(&cwd).unwrap(), "");
}

#[tokio::test]
async fn run_ignored_output_without_commit_msg_passes() {
    let (_temp, orch, cwd) = setup("run-ignored").await;
    std::fs::write(Path::new(&cwd).join(".gitignore"), "target/\n").unwrap();
    git(Path::new(&cwd), &["add", ".gitignore"]);
    git(Path::new(&cwd), &["commit", "-m", "ignore target"]);

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            "run-ignored",
            "mkdir -p target && echo out > target/out",
            None,
        ),
    )
    .await;

    assert!(!result.contains("Run reverted"), "result: {result}");
    assert!(Path::new(&cwd).join("target/out").exists());
    assert_eq!(worktree_status_porcelain(&cwd).unwrap(), "");
}

#[tokio::test]
async fn preexisting_dirt_no_new_dirt_passes_and_preserves_dirt() {
    let (_temp, orch, cwd) = setup("run-existing-only").await;
    std::fs::write(Path::new(&cwd).join("preexisting.txt"), "pre\n").unwrap();
    let before = worktree_status_porcelain(&cwd).unwrap();

    let result = handle_run(&orch, &run_request(&cwd, "run-existing-only", "true", None)).await;

    assert!(!result.contains("Run changed"), "result: {result}");
    assert_eq!(worktree_status_porcelain(&cwd).unwrap(), before);
    assert!(Path::new(&cwd).join("preexisting.txt").exists());
}

#[tokio::test]
async fn preexisting_dirt_plus_new_dirt_restores_to_head() {
    if common::skip_if_fenced("preexisting_dirt_plus_new_dirt_restores_to_head") {
        return;
    }
    let (_temp, orch, cwd) = setup_deny("run-existing-new").await;
    std::fs::write(Path::new(&cwd).join("preexisting.txt"), "pre\n").unwrap();

    let result = handle_run(
        &orch,
        &run_request(&cwd, "run-existing-new", "echo new > new.txt", None),
    )
    .await;

    // worktree==HEAD invariant: a successful batch that adds new dirt without a
    // commit_msg is reverted wholesale (reset --hard + clean -fd), and entry
    // dirt does not survive the restore. Entry dirt alone is preserved — see
    // preexisting_dirt_no_new_dirt_passes_and_preserves_dirt — but once the
    // batch mixes new dirt in, the restore cannot tell them apart.
    assert!(
        result.contains("Run reverted: it changed the worktree but no commit_msg was given"),
        "result: {result}"
    );
    assert!(!Path::new(&cwd).join("new.txt").exists());
    assert!(!Path::new(&cwd).join("preexisting.txt").exists());
    assert_eq!(worktree_status_porcelain(&cwd).unwrap(), "");
}

#[tokio::test]
async fn run_that_commits_itself_passes_without_commit_msg() {
    let (_temp, orch, cwd) = setup("run-self-commit").await;
    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            "run-self-commit",
            "echo own > own.txt && git add own.txt && git commit -m own-change",
            None,
        ),
    )
    .await;

    assert!(!result.contains("Run reverted"), "result: {result}");
    assert_eq!(worktree_status_porcelain(&cwd).unwrap(), "");
}

#[tokio::test]
async fn dirty_worktree_notice_appears_and_self_clears() {
    let (_temp, orch, cwd) = setup_deny("run-notice").await;
    let cursors = Mutex::new(HashMap::new());

    // Entry dirt is created outside any tool call: NO_COMMIT no longer leaves
    // a dirty tree outside merge/rebase, so external edits are the legitimate
    // way a worktree is dirty between calls.
    std::fs::write(Path::new(&cwd).join("notice.txt"), "notice\n").unwrap();

    let read_request = McpCallbackRequest {
        cwd: cwd.clone(),
        run_id: Some("run-notice".to_string()),
        tool: "read".to_string(),
        payload: json!({ "path": "file:README.md" }),
        tool_use_id: Some("toolu-read".to_string()),
    };
    let read_dirty = dispatch_tool(&orch, &read_request, &cursors).await;
    assert!(
        read_dirty
            .reminders
            .iter()
            .any(|r| r.contains("The worktree has uncommitted changes")),
        "{read_dirty:?}"
    );

    let clean = dispatch_tool(
        &orch,
        &run_request(&cwd, "run-notice", "true", Some("commit dirty notice file")),
        &cursors,
    )
    .await;
    assert!(
        clean.content.contains("Committed changes"),
        "{}",
        clean.content
    );
    assert!(
        !clean
            .reminders
            .iter()
            .any(|r| r.contains("The worktree has uncommitted changes")),
        "{clean:?}"
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
