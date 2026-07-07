use crate::common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use cairn_core::internal::db::DbState;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::jj::{self, JjEnv};
use cairn_core::internal::mcp::handlers::run::handle_run;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::services::RealProcessSpawner;
use cairn_core::internal::storage::{LocalDb, SearchIndex};
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, Fence, LegacyOnEscape, LegacySandbox, RecipeSnapshot,
    RecipeTrigger, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use serde_json::json;
use tempfile::TempDir;

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

// `jj_bin`, `head_sha`, and `provision_jj_workspace` live in `tests/common`
// (the shared non-colocated jj fixture); call them as `common::*`. `git_stdout`
// and `init_bare_origin` are local to this file's bare-origin push test.
fn git_stdout(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap_or_else(|e| panic!("failed to run git {args:?}: {e}"));
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn init_bare_origin(origin: &Path) {
    std::fs::create_dir_all(origin).unwrap();
    git(origin, &["init", "--bare", "-b", "main"]);
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
    common::provision_jj_workspace(
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

/// Like [`setup`], but nests the jj workspace UNDER an outer git repo (mimicking
/// the `~/.cairn` HOME repo) so the bare-`git` upward-resolution bug can manifest
/// and the injected `GIT_CEILING_DIRECTORIES` can be exercised end-to-end. The
/// store dir is derived from `config_dir`, independent of the ws path, so the
/// nesting is free. Returns the outer HOME repo path alongside the usual triple.
async fn setup_nested_under_git_repo(
    run_id: &str,
) -> (TempDir, Orchestrator, String, std::path::PathBuf) {
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    // The outer repo the bare-git walk would wrongly resolve to (the `~/.cairn`
    // HOME repo in production). The workspace is provisioned under its
    // `worktrees/` subdir, exactly the production layout.
    let home = temp.path().join("home");
    init_git_repo(&home);
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RHG").await;
    let ws = home.join("worktrees").join("ws");
    // `jj workspace add` creates only the final dir, not intermediates.
    std::fs::create_dir_all(ws.parent().unwrap()).unwrap();
    seed_run(
        &db,
        &project_id,
        &ws,
        run_id,
        LegacySandbox::Worktree,
        LegacyOnEscape::Allow,
    )
    .await;
    let orch = orchestrator(&temp, db);
    common::provision_jj_workspace(
        &temp.path().join("config"),
        &project_repo,
        &ws,
        "agent/RHG-1-builder-0",
    );
    let cwd = ws.display().to_string();
    (temp, orch, cwd, home)
}

/// Like [`setup_nested_under_git_repo`], but the project repo also has an
/// `origin` remote pointing at a bare repo, so a bare `jj git push` from the
/// workspace has somewhere to land. Returns the bare origin path.
async fn setup_nested_with_origin(
    run_id: &str,
) -> (TempDir, Orchestrator, String, std::path::PathBuf) {
    let (temp, db) = common::migrated_db().await;
    let origin = temp.path().join("origin");
    init_bare_origin(&origin);
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    git(
        &project_repo,
        &["remote", "add", "origin", &origin.to_string_lossy()],
    );
    git(&project_repo, &["push", "origin", "main"]);
    let home = temp.path().join("home");
    init_git_repo(&home);
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "RHG").await;
    let ws = home.join("worktrees").join("ws");
    // `jj workspace add` creates only the final dir, not intermediates.
    std::fs::create_dir_all(ws.parent().unwrap()).unwrap();
    seed_run(
        &db,
        &project_id,
        &ws,
        run_id,
        LegacySandbox::Worktree,
        LegacyOnEscape::Allow,
    )
    .await;
    let orch = orchestrator(&temp, db);
    common::provision_jj_workspace(
        &temp.path().join("config"),
        &project_repo,
        &ws,
        "agent/RHG-1-builder-0",
    );
    let cwd = ws.display().to_string();
    (temp, orch, cwd, origin)
}

fn sequential_run_request(cwd: &str, run_id: &str, commands: Vec<&str>) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: Some(run_id.to_string()),
        tool: "run".to_string(),
        payload: json!({
            "sequential": true,
            "commands": commands.iter().map(|c| json!({ "command": c })).collect::<Vec<_>>(),
        }),
        tool_use_id: Some("toolu-run".to_string()),
    }
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
        thread_id: None,
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
    let Some(_bin) = common::jj_bin() else {
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
    let Some(_bin) = common::jj_bin() else {
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
    let Some(_bin) = common::jj_bin() else {
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
        thread_id: None,
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

/// End-to-end (test B): a bare `git` run through the run tool inside a
/// non-colocated jj worktree nested under an outer repo must NOT resolve up to
/// that outer (`~/.cairn`-style) repo. The injected `GIT_CEILING_DIRECTORIES`
/// stops the upward walk, so git fails loudly instead of silently answering
/// about the wrong repository. Read-only command (no `commit_msg`, no dirt).
#[tokio::test]
async fn bare_git_in_jj_worktree_does_not_resolve_up_to_outer_repo() {
    if common::skip_if_fenced("bare_git_in_jj_worktree_does_not_resolve_up_to_outer_repo") {
        return;
    }
    let Some(_bin) = common::jj_bin() else {
        eprintln!(
            "skipping bare_git_in_jj_worktree_does_not_resolve_up_to_outer_repo: jj not resolvable"
        );
        return;
    };
    let (_temp, orch, cwd, home) = setup_nested_under_git_repo("run-bare-git").await;
    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            "run-bare-git",
            "git rev-parse --show-toplevel 2>&1",
            None,
        ),
    )
    .await;

    let home_top = std::fs::canonicalize(&home).unwrap().display().to_string();
    assert!(
        !result.contains(&home_top),
        "bare git must NOT resolve to the outer HOME repo {home_top}: {result}"
    );
    assert!(
        result.contains("not a git repository"),
        "bare git must fail loudly (not a git repository): {result}"
    );
}

/// End-to-end (test B, identity half): a bare `jj` commit through the run tool
/// must carry the managed identity. `JJ_CONFIG` reaching the bare jj process is
/// what gives its commit the `agent@cairn.local` committer instead of an
/// empty/unpushable one.
#[tokio::test]
async fn bare_jj_commit_uses_managed_identity() {
    if common::skip_if_fenced("bare_jj_commit_uses_managed_identity") {
        return;
    }
    let Some(_bin) = common::jj_bin() else {
        eprintln!("skipping bare_jj_commit_uses_managed_identity: jj not resolvable");
        return;
    };
    let (_temp, orch, cwd, _home) = setup_nested_under_git_repo("run-bare-jj").await;
    // Seal a commit with a bare jj, then read its committer email back. The
    // sequence leaves `@` empty (a `jj commit`), so the no-commit_msg barrier
    // sees a clean working copy and never reverts.
    let result = handle_run(
        &orch,
        &sequential_run_request(
            &cwd,
            "run-bare-jj",
            vec![
                "echo probe > probe.txt",
                "jj commit -m probe",
                "jj log -r @- --no-graph -T 'committer.email()'",
            ],
        ),
    )
    .await;

    assert!(
        result.contains("agent@cairn.local"),
        "a bare jj commit must use the managed identity (JJ_CONFIG reached the process): {result}"
    );
}

/// Non-regression (test C): the injected env hits EVERY run-tool shell command,
/// including jj's own git-backend ops. A bare `jj git push` from inside the
/// worktree must still land the bookmark on the bare origin — identical to the
/// non-injected outcome — proving `GIT_CEILING_DIRECTORIES` is inert for jj's
/// store ops (it addresses the store by absolute path, not cwd discovery).
#[tokio::test]
async fn bare_jj_git_push_survives_injected_ceiling_env() {
    if common::skip_if_fenced("bare_jj_git_push_survives_injected_ceiling_env") {
        return;
    }
    let Some(_bin) = common::jj_bin() else {
        eprintln!("skipping bare_jj_git_push_survives_injected_ceiling_env: jj not resolvable");
        return;
    };
    let (_temp, orch, cwd, origin) = setup_nested_with_origin("run-bare-push").await;
    let branch = "agent/RHG-1-builder-0";
    let push_cmd = format!("jj git push --remote origin --bookmark {branch} 2>&1");
    let set_cmd = format!("jj bookmark set {branch} -r @-");
    let result = handle_run(
        &orch,
        &sequential_run_request(
            &cwd,
            "run-bare-push",
            vec![
                "echo pushed > pushed.txt",
                "jj commit -m work",
                &set_cmd,
                &push_cmd,
            ],
        ),
    )
    .await;

    // The load-bearing assertion: the bookmark reached the bare origin, so the
    // ceiling did not break jj's git backend. (If it had, the push would fail
    // and the ref would be absent.)
    let refs = git_stdout(
        &origin,
        &["for-each-ref", "--format=%(refname)", "refs/heads/"],
    );
    assert!(
        refs.contains(branch),
        "bare `jj git push` must land {branch} on origin under the injected env\n\
         push output:\n{result}\norigin refs:\n{refs}"
    );
}

/// Build a run request whose single item is inline `code` (no shell command),
/// so the commit barrier's batch-level, VCS-based governance is exercised over a
/// code item exactly as over a shell command.
fn code_run_request(
    cwd: &str,
    run_id: &str,
    code: &str,
    interpreter: &str,
    commit_msg: Option<&str>,
) -> McpCallbackRequest {
    let mut payload = json!({ "commands": [{ "code": code, "interpreter": interpreter }] });
    if let Some(commit_msg) = commit_msg {
        payload["commit_msg"] = json!(commit_msg);
    }
    McpCallbackRequest {
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: Some(run_id.to_string()),
        tool: "run".to_string(),
        payload,
        tool_use_id: Some("toolu-run".to_string()),
    }
}

fn python3_available() -> bool {
    Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// The commit barrier is batch-level and VCS-based, not shell-keyed: a code item
/// that dirties the worktree is sealed by `commit_msg` exactly like a shell item.
#[tokio::test]
async fn code_item_with_commit_msg_seals_and_cleans() {
    if !python3_available() {
        eprintln!("skipping code_item_with_commit_msg_seals_and_cleans: python3 not resolvable");
        return;
    }
    let Some(_bin) = common::jj_bin() else {
        eprintln!("skipping code_item_with_commit_msg_seals_and_cleans: jj not resolvable");
        return;
    };
    let (temp, orch, cwd) = setup("run-code-commit").await;
    let result = handle_run(
        &orch,
        &code_run_request(
            &cwd,
            "run-code-commit",
            "open('code-gen.txt', 'w').write('x')",
            "python",
            Some("commit code-generated file"),
        ),
    )
    .await;

    assert!(result.contains("Committed changes"), "result: {result}");
    assert!(Path::new(&cwd).join("code-gen.txt").exists());
    assert!(ws_clean(&temp.path().join("config"), Path::new(&cwd)));
}

/// The mirror of `run_without_commit_msg_reverts_new_dirt` for a code item: no
/// `commit_msg` means the code item's new dirt is reverted to HEAD, proving the
/// barrier needs no per-kind special-casing.
#[tokio::test]
async fn code_item_without_commit_msg_reverts_new_dirt() {
    // Spawns a real sandbox-exec-confined process; nested inside the agent fence
    // the in-worktree write is denied, so the hygiene revert never sees dirt.
    // Skip in a fence; unfenced CI exercises the real revert.
    if common::skip_if_fenced("code_item_without_commit_msg_reverts_new_dirt") {
        return;
    }
    if !python3_available() {
        eprintln!("skipping code_item_without_commit_msg_reverts_new_dirt: python3 not resolvable");
        return;
    }
    let Some(_bin) = common::jj_bin() else {
        eprintln!("skipping code_item_without_commit_msg_reverts_new_dirt: jj not resolvable");
        return;
    };
    let (temp, orch, cwd) = setup_deny("run-code-revert").await;
    let result = handle_run(
        &orch,
        &code_run_request(
            &cwd,
            "run-code-revert",
            "open('code-gen.txt', 'w').write('x')",
            "python",
            None,
        ),
    )
    .await;

    assert!(result.contains("Run reverted"), "result: {result}");
    assert!(!Path::new(&cwd).join("code-gen.txt").exists());
    assert!(ws_clean(&temp.path().join("config"), Path::new(&cwd)));
}

#[tokio::test]
async fn dirty_worktree_notice_skips_non_worktree_request() {
    let dir = tempfile::tempdir().unwrap();
    init_git_repo(dir.path());
    std::fs::write(dir.path().join("dirty.txt"), "dirty\n").unwrap();
    let (_temp, orch) = common::test_orchestrator().await;
    let cursors = Mutex::new(HashMap::new());
    let request = McpCallbackRequest {
        thread_id: None,
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
