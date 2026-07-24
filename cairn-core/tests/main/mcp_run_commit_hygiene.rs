use crate::common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use cairn_core::internal::db::DbState;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::jj::{self, JjEnv};
use cairn_core::internal::mcp::handlers::read::handle_read_file;
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
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config")).build();
    common::attach_test_executor(&orch);
    orch
}

#[tokio::test]
async fn attached_executor_and_runner_share_project_store_root() {
    let (temp, db) = common::migrated_db().await;
    let repository = temp.path().join("project");
    let config_dir = temp.path().join("config");
    let orch = orchestrator(&temp, Arc::new(db));

    let runner_store = jj::project_store_dir(&config_dir, &repository);
    let executor_store = jj::project_store_dir(&common::attached_executor_home(&orch), &repository);

    assert_eq!(executor_store, runner_store);
}

#[tokio::test]
async fn run_preflight_completes_rebind_interrupted_after_database_cas() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "partial-after-cas";
    let (temp, db, orch, cwd, _, _fixture) = setup_rebind_fixture(run_id).await;
    let original = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    let (new_branch, _) = simulate_partial_rebind(&temp, &db, &cwd, run_id, false).await;

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            run_id,
            "printf resumed > resumed.txt",
            Some("resume partial rebind"),
        ),
    )
    .await;

    assert!(
        result.to_ascii_lowercase().contains("committed"),
        "{result}"
    );
    let completed = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    assert_eq!(completed.branch, new_branch);
    assert_eq!(completed.workspace_name, original.workspace_name);
    assert_eq!(
        jj::read_branch_marker(Path::new(&cwd)).as_deref(),
        Some(new_branch.as_str())
    );
}

#[tokio::test]
async fn run_preflight_completes_rebind_interrupted_between_marker_writes() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "partial-between-markers";
    let (temp, db, orch, cwd, _, _fixture) = setup_rebind_fixture(run_id).await;
    let original = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    let (new_branch, _) = simulate_partial_rebind(&temp, &db, &cwd, run_id, true).await;
    assert_eq!(
        jj::read_workspace_identity(Path::new(&cwd)).unwrap().branch,
        original.branch,
        "the fixture stops after the branch marker and before the identity marker"
    );

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            run_id,
            "printf resumed > resumed.txt",
            Some("resume marker update"),
        ),
    )
    .await;

    assert!(
        result.to_ascii_lowercase().contains("committed"),
        "{result}"
    );
    let completed = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    assert_eq!(completed.branch, new_branch);
    assert_eq!(completed.workspace_name, original.workspace_name);
}

#[tokio::test]
async fn run_preflight_rejects_arbitrary_database_branch_at_current_head() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "arbitrary-db-branch";
    let (temp, db, orch, cwd, _, _) = setup_rebind_fixture(run_id).await;
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let original = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    let head = jj::head_commit(&jj, Path::new(&cwd)).unwrap();
    let arbitrary = "agent/arbitrary-database-reassignment";
    jj::create_bookmark_at(&jj, Path::new(&cwd), arbitrary, &head).unwrap();
    db.execute(
        "UPDATE jobs SET branch = ?1 WHERE id = ?2",
        params![arbitrary, format!("job-{run_id}").as_str()],
    )
    .await
    .unwrap();

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            run_id,
            "printf must-not-run > arbitrary.txt",
            Some("must not execute"),
        ),
    )
    .await;

    assert!(
        result.contains("without a persisted pending rebind"),
        "an arbitrary reassignment at @- must fail closed: {result}"
    );
    assert!(!Path::new(&cwd).join("arbitrary.txt").exists());
    assert_eq!(
        jj::read_workspace_identity(Path::new(&cwd)),
        Some(original.clone())
    );
    assert_eq!(
        jj::read_branch_marker(Path::new(&cwd)).as_deref(),
        Some(original.branch.as_str())
    );
}

#[tokio::test]
async fn run_preflight_rebinds_unowned_legacy_descendant_bookmark() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "legacy-descendant";
    let (temp, _db, orch, cwd, _, _fixture) = setup_rebind_fixture(run_id).await;
    let branch = "agent/RHG-1-builder-0";
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let project_repo = temp.path().join("project");
    let store = jj::project_store_dir(&temp.path().join("config"), &project_repo);
    let sibling = temp.path().join("legacy-advance");
    let sibling_branch = "agent/legacy-descendant-source";
    jj::add_workspace(&jj, &store, &sibling, sibling_branch, branch, None).unwrap();
    std::fs::write(sibling.join("legacy.txt"), "unrelated legacy history\n").unwrap();
    jj::seal(&jj, &sibling, "legacy advance", None).unwrap();
    let sibling_tip = jj::bookmark_commit(&jj, &store, sibling_branch).unwrap();
    jj::set_bookmark_at(&jj, &store, branch, &sibling_tip).unwrap();
    let legacy_tip = jj::bookmark_commit(&jj, &store, branch).unwrap();
    std::fs::remove_file(Path::new(&cwd).join(".jj").join("cairn-workspace-identity")).unwrap();

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            run_id,
            "printf current > current.txt",
            Some("seal without legacy adoption"),
        ),
    )
    .await;

    assert!(
        result.to_ascii_lowercase().contains("committed"),
        "{result}"
    );
    assert_eq!(
        jj::bookmark_commit(&jj, &store, branch).as_deref(),
        Some(legacy_tip.as_str()),
        "the unowned descendant bookmark must remain untouched"
    );
    let rebound = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    assert_ne!(rebound.branch, branch);
    assert!(rebound.branch.starts_with("agent/RHG-1-builder-0-j"));
}

#[tokio::test]
async fn sealed_same_job_retry_keeps_recorded_base_and_passes_next_preflight() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "sealed-retry";
    let (temp, _db, orch, cwd, _, _fixture) = setup_rebind_fixture(run_id).await;
    let branch = "agent/RHG-1-builder-0";
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let project_repo = temp.path().join("project");
    let store = jj::project_store_dir(&temp.path().join("config"), &project_repo);
    let identity = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    let recorded_base = identity.base_commit.clone();

    std::fs::write(Path::new(&cwd).join("sealed.txt"), "prior sealed work\n").unwrap();
    jj::seal(&jj, Path::new(&cwd), "prior sealed work", None).unwrap();
    let sealed_tip = jj::bookmark_commit(&jj, &store, branch).unwrap();
    assert_ne!(sealed_tip, recorded_base);

    std::fs::remove_dir_all(&cwd).unwrap();
    jj::cleanup_workspace_retry(&jj, &store, Path::new(&cwd), &identity.workspace_name).unwrap();
    jj::add_workspace(&jj, &store, Path::new(&cwd), branch, &sealed_tip, None).unwrap();
    jj::write_base_marker(Path::new(&cwd), "main", &sealed_tip).unwrap();
    jj::write_project_root_marker(Path::new(&cwd), &project_repo).unwrap();
    jj::write_workspace_identity(Path::new(&cwd), &identity).unwrap();

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            run_id,
            "printf next > next.txt",
            Some("seal after retry"),
        ),
    )
    .await;

    assert!(
        result.to_ascii_lowercase().contains("committed"),
        "{result}"
    );
    let installed = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    assert_eq!(installed.base_commit, recorded_base);
    assert_eq!(
        std::fs::read_to_string(Path::new(&cwd).join("sealed.txt")).unwrap(),
        "prior sealed work\n"
    );
}

#[tokio::test]
async fn run_preflight_rebinds_prior_lineage_before_command_execution() {
    let Some(_bin) = common::jj_bin() else {
        eprintln!("skipping run_preflight_rebinds_prior_lineage_before_command_execution: jj not resolvable");
        return;
    };
    let run_id = "run-lineage-rebind";
    let (temp, db, orch, cwd, project_id, _fixture) = setup_rebind_fixture(run_id).await;
    let branch = "agent/RHG-1-builder-0";
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let old_tip = jj::bookmark_commit(&jj, Path::new(&cwd), branch).unwrap();

    db.execute(
        "INSERT INTO jobs(id, project_id, status, branch, created_at, updated_at)
         VALUES ('prior-lineage', ?1, 'finished', ?2, 0, 0)",
        params![project_id.as_str(), branch],
    )
    .await
    .unwrap();

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            run_id,
            "printf repaired > repaired.txt",
            Some("seal after lineage rebind"),
        ),
    )
    .await;

    assert!(
        result.to_ascii_lowercase().contains("committed"),
        "result: {result}"
    );
    assert_eq!(
        jj::bookmark_commit(&jj, Path::new(&cwd), branch).as_deref(),
        Some(old_tip.as_str()),
        "the conflicting bookmark must remain unchanged"
    );
    let marker = jj::read_workspace_identity(Path::new(&cwd)).unwrap();
    let repaired = Command::new("jj")
        .args(["file", "show", "-r", &marker.branch, "repaired.txt"])
        .current_dir(&cwd)
        .output()
        .unwrap();
    assert!(
        repaired.status.success(),
        "jj file show failed: {repaired:?}"
    );
    let repaired = String::from_utf8(repaired.stdout).unwrap();
    assert_eq!(repaired, "repaired");
    let read = handle_read_file(
        &orch,
        &McpCallbackRequest {
            thread_id: None,
            cwd: cwd.clone(),
            run_id: Some(run_id.to_string()),
            tool: "read".to_string(),
            payload: json!({ "path": "file:repaired.txt" }),
            tool_use_id: None,
        },
    )
    .await;
    assert!(read.contains("repaired"), "{read}");
    assert_ne!(marker.branch, branch);
    assert!(marker.branch.starts_with("agent/RHG-1-builder-0-j"));
    assert!(jj::bookmark_commit(&jj, Path::new(&cwd), &marker.branch).is_some());
    let persisted_branch = common::scalar_text_by_id(
        &db,
        "SELECT branch FROM jobs WHERE id = ?1",
        &format!("job-{run_id}"),
    )
    .await;
    assert_eq!(persisted_branch.as_deref(), Some(marker.branch.as_str()));
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
// (the shared non-colocated jj fixture); call them as `common::*`.

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

#[allow(clippy::too_many_arguments)]
async fn seed_run(
    db: &LocalDb,
    project_id: &str,
    worktree: &Path,
    branch: &str,
    base_commit: &str,
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
                "INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, worktree_path, branch, base_commit, created_at, updated_at)
                 VALUES (?1, ?2, 'agent-1', ?3, ?4, 'builder', 'running', 'builder', ?5, ?6, ?7, 1, 1)",
                params![job_id.as_str(), exec_id.as_str(), issue_id.as_str(), project_id.as_str(), worktree.as_str(), branch.as_str(), base_commit.as_str()],
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
fn write_managed_identity(
    project_id: &str,
    project_repo: &Path,
    worktree: &Path,
    branch: &str,
    base_commit: &str,
    run_id: &str,
) {
    let job_id = format!("job-{run_id}");
    let identity = jj::WorkspaceIdentity::new(
        job_id.clone(),
        job_id,
        project_id,
        project_repo.to_path_buf(),
        worktree.to_path_buf(),
        branch,
        jj::workspace_name_for_branch(branch),
        base_commit,
    );
    jj::write_workspace_identity(worktree, &identity).unwrap();
}

async fn setup_with_sandbox(
    run_id: &str,
    sandbox: LegacySandbox,
    on_escape: LegacyOnEscape,
) -> (
    TempDir,
    Orchestrator,
    String,
    common::ProvisionedJjWorkspace,
) {
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "RHG", &project_repo).await;
    let ws = temp.path().join("ws");
    let branch = "agent/RHG-1-builder-0";
    let base = common::head_sha(&project_repo);
    seed_run(
        &db,
        &project_id,
        &ws,
        branch,
        &base,
        run_id,
        sandbox,
        on_escape,
    )
    .await;
    let orch = orchestrator(&temp, db);
    let fixture =
        common::provision_jj_workspace(&temp.path().join("config"), &project_repo, &ws, branch);
    write_managed_identity(&project_id, &project_repo, &ws, branch, &base, run_id);
    let cwd = ws.display().to_string();
    (temp, orch, cwd, fixture)
}

async fn setup(run_id: &str) -> (TempDir, Orchestrator, String) {
    let (temp, orch, cwd, _) =
        setup_with_sandbox(run_id, LegacySandbox::Worktree, LegacyOnEscape::Allow).await;
    (temp, orch, cwd)
}

async fn setup_allow_delta(
    run_id: &str,
) -> (
    TempDir,
    Orchestrator,
    String,
    common::ProvisionedJjWorkspace,
) {
    setup_with_sandbox(run_id, LegacySandbox::Worktree, LegacyOnEscape::Allow).await
}

async fn setup_rebind_fixture(
    run_id: &str,
) -> (
    TempDir,
    Arc<LocalDb>,
    Orchestrator,
    String,
    String,
    common::ProvisionedJjWorkspace,
) {
    let (temp, db) = common::migrated_db().await;
    let project_repo = temp.path().join("project");
    init_git_repo(&project_repo);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "RHG", &project_repo).await;
    let ws = temp.path().join("ws");
    let branch = "agent/RHG-1-builder-0";
    let base = common::head_sha(&project_repo);
    seed_run(
        &db,
        &project_id,
        &ws,
        branch,
        &base,
        run_id,
        LegacySandbox::Worktree,
        LegacyOnEscape::Allow,
    )
    .await;
    let orch = orchestrator(&temp, db.clone());
    let fixture =
        common::provision_jj_workspace(&temp.path().join("config"), &project_repo, &ws, branch);
    write_managed_identity(&project_id, &project_repo, &ws, branch, &base, run_id);
    let cwd = ws.display().to_string();
    (temp, db, orch, cwd, project_id, fixture)
}

async fn simulate_partial_rebind(
    temp: &TempDir,
    db: &LocalDb,
    cwd: &str,
    run_id: &str,
    write_new_branch_marker: bool,
) -> (String, String) {
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let head = jj::head_commit(&jj, Path::new(cwd)).unwrap();
    let lineage = format!("job-{run_id}");
    let short: String = lineage
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect();
    let new_branch = format!("agent/RHG-1-builder-0-j{short}");
    jj::create_bookmark_at(&jj, Path::new(cwd), &new_branch, &head).unwrap();
    let mut identity = jj::read_workspace_identity(Path::new(cwd)).unwrap();
    identity.pending_rebind = Some(jj::WorkspaceRebindTransition {
        old_branch: identity.branch.clone(),
        new_branch: new_branch.clone(),
        sealed_head: head.clone(),
    });
    jj::write_workspace_identity(Path::new(cwd), &identity).unwrap();
    db.execute(
        "UPDATE jobs SET branch = ?1 WHERE id = ?2",
        params![new_branch.as_str(), format!("job-{run_id}").as_str()],
    )
    .await
    .unwrap();
    if write_new_branch_marker {
        jj::write_branch_marker(Path::new(cwd), &new_branch).unwrap();
    }
    (new_branch, head)
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
    let project_id = common::insert_project_with_repo(&db, "RHG", &project_repo).await;
    let ws = home.join("worktrees").join("ws");
    // `jj workspace add` creates only the final dir, not intermediates.
    std::fs::create_dir_all(ws.parent().unwrap()).unwrap();
    let branch = "agent/RHG-1-builder-0";
    let base = common::head_sha(&project_repo);
    seed_run(
        &db,
        &project_id,
        &ws,
        branch,
        &base,
        run_id,
        LegacySandbox::Worktree,
        LegacyOnEscape::Allow,
    )
    .await;
    let orch = orchestrator(&temp, db);
    common::provision_jj_workspace(&temp.path().join("config"), &project_repo, &ws, branch);
    write_managed_identity(&project_id, &project_repo, &ws, branch, &base, run_id);
    let cwd = ws.display().to_string();
    (temp, orch, cwd, home)
}

/// Seed a run whose fence is `deny`, so the commit-hygiene gate is active.
async fn setup_deny(run_id: &str) -> (TempDir, Orchestrator, String) {
    let (temp, orch, cwd, _) =
        setup_with_sandbox(run_id, LegacySandbox::Worktree, LegacyOnEscape::Deny).await;
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

    assert!(!result.contains("isolated cell"), "result: {result}");
    assert!(!Path::new(&cwd).join("generated.txt").exists());
    assert!(ws_clean(&temp.path().join("config"), Path::new(&cwd)));
}

#[tokio::test]
async fn full_sandbox_run_without_commit_msg_is_not_gated_or_reverted() {
    let Some(_bin) = common::jj_bin() else {
        eprintln!("skipping full_sandbox_run_without_commit_msg_is_not_gated_or_reverted: jj not resolvable");
        return;
    };
    let (temp, orch, cwd, _) = setup_with_sandbox(
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
    assert!(
        !Path::new(&cwd).join("full-sandbox.txt").exists(),
        "ignored and untracked slot outputs must not leak into the agent workspace"
    );
    assert!(ws_clean(&temp.path().join("config"), Path::new(&cwd)));
}

#[tokio::test]
async fn run_with_commit_msg_seals_and_cleans() {
    let Some(_bin) = common::jj_bin() else {
        eprintln!("skipping run_with_commit_msg_seals_and_cleans: jj not resolvable");
        return;
    };
    let (temp, orch, cwd, _fixture) = setup_allow_delta("run-commit").await;
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
async fn fresh_managed_workspace_commit_runs_write_check_in_git_slot_and_stores_verdict() {
    if common::skip_if_fenced(
        "fresh_managed_workspace_commit_runs_write_check_in_git_slot_and_stores_verdict",
    ) {
        return;
    }
    let Some(_bin) = common::jj_bin() else {
        eprintln!(
            "skipping fresh_managed_workspace_commit_runs_write_check_in_git_slot_and_stores_verdict: jj not resolvable"
        );
        return;
    };
    let run_id = "fresh-write-check";
    let (temp, orch, cwd, fixture) = setup_allow_delta(run_id).await;
    let config = fixture.project_repository.join(".cairn/config.yaml");
    std::fs::create_dir_all(config.parent().unwrap()).unwrap();
    std::fs::write(
        &config,
        "checks:\n  fresh-slot:\n    command: test \"$(cat checked.txt)\" = fresh && printf fresh-slot-ok\n    impact:\n      - checked.txt\n    when: write\n",
    )
    .unwrap();

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            run_id,
            "printf fresh > checked.txt",
            Some("seal fresh checked tree"),
        ),
    )
    .await;

    assert!(result.contains("Committed changes"), "result: {result}");
    assert!(result.contains("✓ fresh-slot"), "result: {result}");
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let sealed_commit = jj::head_commit(&jj, &fixture.workspace).unwrap();
    let sealed_tree = jj::sealed_tree_hash(&jj, &fixture.workspace).unwrap();
    let project_id = jj::read_workspace_identity(&fixture.workspace)
        .unwrap()
        .project_id;
    let stored = orch
        .db
        .local
        .query_text(
            "SELECT tree_hash || ':' || passed || ':' || COALESCE(job_id, '')\n             FROM check_result_cache\n             WHERE project_id = ?1 AND check_name = 'fresh-slot'\n             ORDER BY ran_at DESC LIMIT 1",
            params![project_id],
        )
        .await
        .unwrap()
        .expect("fresh write-check verdict");
    assert_eq!(stored, format!("{sealed_tree}:1:job-{run_id}"));
    assert_eq!(
        std::fs::read_to_string(fixture.workspace.join("checked.txt")).unwrap(),
        "fresh"
    );
    let visible = Command::new("git")
        .args(["cat-file", "-t", &sealed_commit])
        .current_dir(&fixture.project_repository)
        .output()
        .unwrap();
    assert!(visible.status.success());
    assert_eq!(String::from_utf8_lossy(&visible.stdout).trim(), "commit");
    let temporary_refs = Command::new("git")
        .args(["for-each-ref", "--format=%(refname)", "refs/cairn/checks/"])
        .current_dir(&fixture.project_repository)
        .output()
        .unwrap();
    assert!(temporary_refs.status.success());
    assert!(
        String::from_utf8_lossy(&temporary_refs.stdout)
            .trim()
            .is_empty(),
        "sealed-check reachability ref survived cleanup: {}",
        String::from_utf8_lossy(&temporary_refs.stdout)
    );
}

#[tokio::test]
async fn managed_store_seal_bridge_materialize_barrier_uses_workspace_store() {
    let Some(_bin) = common::jj_bin() else {
        eprintln!(
            "skipping managed_store_seal_bridge_materialize_barrier_uses_workspace_store: jj not resolvable"
        );
        return;
    };
    let run_id = "managed-store-publication";
    let (temp, orch, cwd, fixture) = setup_allow_delta(run_id).await;
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let base = jj::head_commit(&jj, &fixture.workspace).unwrap();

    assert!(!fixture.project_repository.join(".jj").exists());
    assert!(!fixture.workspace.join(".git").exists());
    let store_git_target =
        std::fs::read_to_string(fixture.store_dir.join(".jj/repo/store/git_target")).unwrap();
    assert_eq!(
        fixture.git_common_dir,
        std::fs::canonicalize(store_git_target.trim()).unwrap()
    );

    let result = handle_run(
        &orch,
        &run_request(
            &cwd,
            run_id,
            "printf managed-store > managed-store.txt",
            Some("publish through managed store"),
        ),
    )
    .await;

    assert!(result.contains("Committed changes"), "result: {result}");
    assert_eq!(
        std::fs::read_to_string(fixture.workspace.join("managed-store.txt")).unwrap(),
        "managed-store"
    );
    let materialized = jj::head_commit(&jj, &fixture.workspace).unwrap();
    assert_ne!(materialized, base);
    assert!(jj::revset_resolves(&jj, &fixture.store_dir, &materialized));
    assert!(ws_clean(&temp.path().join("config"), &fixture.workspace));

    let temporary_refs = Command::new("git")
        .args([
            "for-each-ref",
            "--format=%(refname)",
            "refs/heads/cairn-build-delta-",
        ])
        .current_dir(&fixture.project_repository)
        .output()
        .unwrap();
    assert!(temporary_refs.status.success());
    assert!(
        String::from_utf8_lossy(&temporary_refs.stdout)
            .trim()
            .is_empty(),
        "temporary publication ref survived cleanup: {}",
        String::from_utf8_lossy(&temporary_refs.stdout)
    );
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
    assert!(!result.contains("isolated cell"), "result: {result}");
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
    let (temp, orch, cwd, _fixture) = setup_allow_delta("run-code-commit").await;
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

    assert!(!result.contains("isolated cell"), "result: {result}");
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

fn branch_run_request(
    cwd: &str,
    run_id: &str,
    branch: &str,
    command: &str,
    commit_msg: Option<&str>,
) -> McpCallbackRequest {
    let mut payload = json!({
        "commands": [{ "command": command }],
        "branch": branch,
    });
    if let Some(message) = commit_msg {
        payload["commit_msg"] = json!(message);
    }
    McpCallbackRequest {
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: Some(run_id.to_string()),
        tool: "run".to_string(),
        payload,
        tool_use_id: Some(format!("toolu-branch-{run_id}")),
    }
}

#[tokio::test]
async fn branch_run_uses_committed_head_and_ignores_dirty_live_workspace() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "branch-commit-true";
    let (temp, orch, cwd) = setup(run_id).await;
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let sentinel = Path::new(&cwd).join("sentinel.txt");
    std::fs::write(&sentinel, "committed-sentinel\n").unwrap();
    let sealed = jj::seal(&jj, Path::new(&cwd), "branch sentinel", None).unwrap();
    std::fs::write(&sentinel, "dirty-live-workspace\n").unwrap();

    let result = handle_run(
        &orch,
        &branch_run_request(
            &cwd,
            run_id,
            "agent/RHG-1-builder-0",
            "cat sentinel.txt",
            None,
        ),
    )
    .await;

    assert!(result.contains("committed-sentinel"), "{result}");
    assert!(!result.contains("dirty-live-workspace"), "{result}");
    assert_eq!(
        std::fs::read_to_string(&sentinel).unwrap(),
        "dirty-live-workspace\n"
    );
    let bookmark = jj::bookmark_commit(&jj, Path::new(&cwd), "agent/RHG-1-builder-0").unwrap();
    assert!(
        bookmark.starts_with(&sealed.sha),
        "{bookmark} != {}",
        sealed.sha
    );
}

#[tokio::test]
async fn branch_run_accepts_ref_without_a_live_checkout() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "branch-no-checkout";
    let (temp, orch, cwd) = setup(run_id).await;
    let jj = JjEnv::resolve("jj", &temp.path().join("config"));
    let head = jj::head_commit(&jj, Path::new(&cwd)).unwrap();
    jj::create_bookmark_at(&jj, Path::new(&cwd), "detached-verdict-ref", &head).unwrap();

    let result = handle_run(
        &orch,
        &branch_run_request(&cwd, run_id, "detached-verdict-ref", "cat README.md", None),
    )
    .await;

    assert!(result.contains("initial"), "{result}");
}

#[tokio::test]
async fn unresolved_branch_ref_fails_before_command_execution() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "branch-unresolved";
    let (_temp, orch, cwd) = setup(run_id).await;
    let side_effect = Path::new(&cwd).join("must-not-run.txt");

    let result = handle_run(
        &orch,
        &branch_run_request(
            &cwd,
            run_id,
            "definitely-missing-ref",
            "printf ran > must-not-run.txt",
            None,
        ),
    )
    .await;

    assert!(result.contains("definitely-missing-ref"), "{result}");
    assert!(result.contains("Could not resolve branch ref"), "{result}");
    assert!(!side_effect.exists());
}

#[tokio::test]
async fn branch_run_is_verdict_only_and_rejects_commit_messages() {
    let Some(_bin) = common::jj_bin() else {
        return;
    };
    let run_id = "branch-verdict-only";
    let (_temp, orch, cwd) = setup(run_id).await;

    let rejected = handle_run(
        &orch,
        &branch_run_request(
            &cwd,
            run_id,
            "agent/RHG-1-builder-0",
            "printf ran > must-not-run.txt",
            Some("forbidden"),
        ),
    )
    .await;
    assert!(rejected.contains("verdict-only"), "{rejected}");
    assert!(!Path::new(&cwd).join("must-not-run.txt").exists());

    let mutation = handle_run(
        &orch,
        &branch_run_request(
            &cwd,
            run_id,
            "agent/RHG-1-builder-0",
            "printf changed > tracked-output.txt",
            None,
        ),
    )
    .await;
    assert!(
        mutation
            .contains("Verdict-only run modified 1 tracked path(s); the mutation was discarded"),
        "{mutation}"
    );
    assert!(mutation.contains("tracked-output.txt"), "{mutation}");
    assert!(!Path::new(&cwd).join("tracked-output.txt").exists());
}
