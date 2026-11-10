//! Integration tests for the host-agnostic PR lifecycle actions
//! (`cairn_core::pr_data::actions`): merge / close / refresh keyed by job_id,
//! plus the live-PR read section.

use crate::common;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use cairn_core::internal::jj;

use cairn_core::models::{
    ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
};

use cairn_core::internal::db::DbState;
use cairn_core::internal::effects::types::WorkflowEffect;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::{MockGitClient, MockHttpClient, TestServicesBuilder};
use cairn_core::internal::services::HttpResponse;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::pr_data::actions;
use cairn_db::turso::params;
use tempfile::TempDir;
use tokio::sync::mpsc::UnboundedReceiver;

const TEST_KEY: &str = include_str!("../fixtures/test_rsa_key.pem");

/// A temp git repo whose `origin` remote points at github.com/octo/widget, so
/// `get_owner_repo` (which shells out to git) resolves to (octo, widget).
fn temp_git_repo() -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir.path())
            .status()
            .unwrap();
        assert!(status.success(), "git {:?} failed", args);
    };
    run(&["init", "-b", "main", "-q"]);
    run(&[
        "remote",
        "add",
        "origin",
        "https://github.com/octo/widget.git",
    ]);
    dir
}

/// A throwaway git project with a committed `main`, suitable for backing a jj
/// store (no remote: the local fold path never pushes).
fn temp_jj_project() -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir.path())
            .status()
            .unwrap();
        assert!(status.success(), "git {:?} failed", args);
    };
    run(&["init", "-q", "-b", "main"]);
    run(&["config", "user.email", "t@e.com"]);
    run(&["config", "user.name", "T"]);
    std::fs::write(dir.path().join("base.txt"), "base\n").unwrap();
    run(&["add", "-A"]);
    run(&["commit", "-q", "-m", "base"]);
    dir
}

/// Commit `contents` at `path` on a [`temp_jj_project`]'s `main`, before a store
/// is provisioned from it.
fn commit_to_project(repo: &Path, path: &str, contents: &str) {
    let file = repo.join(path);
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(&file, contents).unwrap();
    for args in [&["add", "-A"][..], &["commit", "-q", "-m", path][..]] {
        let status = Command::new("git")
            .args(args)
            .current_dir(repo)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }
}

/// Provision a jj store for `repo` under `config_dir` with `feature` sealed one
/// commit ahead of `main`, so a fold of feature→main is a real forward merge.
/// `config_dir` matches the orchestrator's config dir so `merge_pr_for_job`
/// resolves the same store.
fn provision_jj_merge(config_dir: &Path, repo: &Path) {
    let ws = config_dir.join("feature-ws");
    common::provision_jj_workspace(config_dir, repo, &ws, "feature");
    let jj = jj::environment("jj", config_dir);
    std::fs::write(ws.join("change.txt"), "feature change\n").unwrap();
    jj::seal(&jj, &ws, "feature work", None).unwrap();
}

/// Provision a jj store whose `feature` bookmark carries a RECORDED conflict on
/// `shared.txt`: feature edits the file, `main` then advances with a conflicting
/// edit to the same path, and feature is rebased onto the advanced main — the
/// out-of-band conflicted-history state the pre-flight diagnostic must report.
fn provision_jj_conflict(bin: &str, config_dir: &Path, repo: &Path) {
    let ws = config_dir.join("feature-ws");
    common::provision_jj_workspace(config_dir, repo, &ws, "feature");
    let jj = jj::environment(bin, config_dir);
    std::fs::write(ws.join("shared.txt"), "feature-side\n").unwrap();
    jj::seal(&jj, &ws, "feature edits shared", None).unwrap();

    let store = jj::project_store_dir(config_dir, repo);
    let cfg = config_dir.join("jj").join("config.toml");
    let jj_cmd = |args: &[&str]| {
        let out = Command::new(bin)
            .args(args)
            .current_dir(&store)
            .env("JJ_CONFIG", &cfg)
            .env("EDITOR", "true")
            .env("JJ_EDITOR", "true")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "jj {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    // `main` advances with a conflicting change to the same file.
    jj_cmd(&["new", "main"]);
    std::fs::write(store.join("shared.txt"), "main-advanced\n").unwrap();
    jj_cmd(&["describe", "-m", "main advances shared"]);
    jj_cmd(&["bookmark", "set", "main", "-r", "@"]);
    // Rebasing feature onto the advanced main records a conflict on `feature`.
    // Driven through raw jj rather than `rebase_branch_onto`, which now rolls a
    // conflicting rebase back instead of leaving the conflict on the branch — and
    // what this fixture needs is precisely a branch carrying one.
    jj_cmd(&[
        "rebase",
        "-b",
        "feature",
        "-o",
        "main",
        "--ignore-working-copy",
    ]);
    assert!(
        jj::branch_has_conflict(&jj, &store, "feature").unwrap(),
        "feature must carry a recorded conflict after rebase onto advanced main"
    );
}

/// Point a project repo's `origin` at github so `get_owner_repo` resolves the
/// live-PR rendering path to (octo, widget).
fn add_github_origin(repo: &Path) {
    let status = Command::new("git")
        .args([
            "remote",
            "add",
            "origin",
            "https://github.com/octo/widget.git",
        ])
        .current_dir(repo)
        .status()
        .unwrap();
    assert!(status.success(), "git remote add origin failed");
}

/// Give `repo` an origin a landing can fetch: a local bare repository holding
/// `main`, at a path ending `github.com/octo/widget.git` so the repository
/// still reads as GitHub's `octo/widget` for the mocked API. Every landing
/// judges against origin's actual target, so a remote-PR fixture needs one.
/// Returns the directory holding it (keep it alive) and the bare repository.
fn add_fetchable_github_origin(repo: &Path) -> (TempDir, PathBuf) {
    let host = tempfile::tempdir().unwrap();
    let origin = host.path().join("github.com/octo/widget.git");
    std::fs::create_dir_all(&origin).unwrap();
    let origin_url = origin.to_string_lossy().into_owned();
    for (dir, args) in [
        (
            origin.as_path(),
            &["init", "-q", "--bare", "-b", "main"][..],
        ),
        (repo, &["remote", "add", "origin", origin_url.as_str()][..]),
        (repo, &["push", "-q", "origin", "main"][..]),
    ] {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }
    (host, origin)
}

/// The commit `branch` names in a git repository.
fn git_rev_parse(repo: &Path, branch: &str) -> String {
    let out = Command::new("git")
        .args(["rev-parse", branch])
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(out.status.success(), "git rev-parse {branch} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Run a jj template query against a project's shared store and return trimmed
/// stdout. Wired exactly like the in-crate `JjEnv` (same managed config path and
/// non-interactive env), with `--ignore-working-copy` so the scratch `@` is never
/// snapshotted.
fn jj_template(bin: &str, config_dir: &Path, repo: &Path, revset: &str, template: &str) -> String {
    let store = jj::project_store_dir(config_dir, repo);
    let cfg = config_dir.join("jj").join("config.toml");
    let out = Command::new(bin)
        .args([
            "log",
            "-r",
            revset,
            "--no-graph",
            "--ignore-working-copy",
            "-T",
            template,
        ])
        .current_dir(&store)
        .env("JJ_CONFIG", &cfg)
        .env("EDITOR", "true")
        .env("JJ_EDITOR", "true")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "jj log failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Commit id a bookmark resolves to in a project's shared store.
fn jj_bookmark_commit(bin: &str, config_dir: &Path, repo: &Path, bookmark: &str) -> String {
    jj_template(
        bin,
        config_dir,
        repo,
        &format!("bookmarks(exact:{bookmark:?})"),
        "commit_id",
    )
}

async fn orchestrator_with_http(db: LocalDb, http: MockHttpClient) -> (TempDir, Orchestrator) {
    orchestrator_with_http_and_git(db, http, MockGitClient::new()).await
}

/// Like [`orchestrator_with_http`], but rooted at a caller-owned temp dir.
///
/// Needed whenever the jj store must be provisioned BEFORE the HTTP mock is
/// built — a test whose GitHub response has to carry the branch's real head
/// commit cannot learn that commit until the store exists.
async fn orchestrator_with_http_in(
    db: LocalDb,
    http: MockHttpClient,
    cfg: &TempDir,
) -> Orchestrator {
    let mut git = MockGitClient::new();
    git.expect_status()
        .times(0..)
        .returning(|_| Ok(String::new()));
    let db = Arc::new(db);
    let search = Arc::new(SearchIndex::open_or_create(cfg.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search));
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_http(http)
            .with_git(git)
            .build(),
    );
    Orchestrator::builder(db_state, services, cfg.path().join("config")).build()
}

async fn orchestrator_with_http_and_git(
    db: LocalDb,
    http: MockHttpClient,
    mut git: MockGitClient,
) -> (TempDir, Orchestrator) {
    git.expect_status()
        .times(0..)
        .returning(|_| Ok(String::new()));

    let cfg = tempfile::tempdir().unwrap();
    let db = Arc::new(db);
    let search = Arc::new(SearchIndex::open_or_create(cfg.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search));
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_http(http)
            .with_git(git)
            .build(),
    );
    let orch = Orchestrator::builder(db_state, services, cfg.path().join("config")).build();
    (cfg, orch)
}

/// Build an orchestrator with a channel-backed `effect_tx` so tests can
/// observe `WorkflowEffect`s pushed by code under test.
async fn orchestrator_with_effect_channel(
    db: LocalDb,
    http: MockHttpClient,
) -> (TempDir, Orchestrator, UnboundedReceiver<WorkflowEffect>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let cfg = tempfile::tempdir().unwrap();
    let db = Arc::new(db);
    let search = Arc::new(SearchIndex::open_or_create(cfg.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search));
    let mut git = MockGitClient::new();
    git.expect_status()
        .times(0..)
        .returning(|_| Ok(String::new()));
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_http(http)
            .with_git(git)
            .build(),
    );
    let orch = Orchestrator::builder(db_state, services, cfg.path().join("config"))
        .effect_tx(Some(tx))
        .build();
    (cfg, orch, rx)
}

async fn insert_execution(db: &LocalDb, id: &str, project_id: &str, issue_id: &str) {
    // A real execution always carries a snapshot; resolving a PR recomputes the
    // producing execution, which loads it. A minimal empty-graph snapshot is
    // enough for these PR-resolution tests (the producing job has no node).
    let snapshot_json = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "recipe-1".to_string(),
            name: "PR test".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some(issue_id.to_string()),
            project_id: project_id.to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap();
    let id = id.to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    db.execute(
        "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
         VALUES (?1, 'recipe-1', ?2, ?3, 'running', 1, 1, ?4)",
        params![
            id.as_str(),
            issue_id.as_str(),
            project_id.as_str(),
            snapshot_json.as_str()
        ],
    )
    .await
    .unwrap();
}

async fn create_job_with_execution(
    db: &LocalDb,
    project_id: &str,
    issue_id: &str,
    execution_id: Option<&str>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    let execution_id = execution_id.map(str::to_string);
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO jobs (id, project_id, issue_id, execution_id, status, branch, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 'complete', 'feature', ?5, ?5)",
        params![
            id.as_str(),
            project_id.as_str(),
            issue_id.as_str(),
            execution_id.as_deref(),
            now,
        ],
    )
    .await
    .unwrap();
    id
}

async fn create_action_run(
    db: &LocalDb,
    project_id: &str,
    execution_id: &str,
    parent_job_id: &str,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let execution_id = execution_id.to_string();
    let parent_job_id = parent_job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO action_runs (
            id, execution_id, recipe_node_id, action_config_id, project_id,
            status, created_at, parent_job_id, uri_segment
         ) VALUES (?1, ?2, 'pr-1', 'builtin:pr', ?3, 'complete', ?4, ?5, 'pr')",
        params![
            id.as_str(),
            execution_id.as_str(),
            project_id.as_str(),
            now,
            parent_job_id.as_str()
        ],
    )
    .await
    .unwrap();
    id
}

async fn file_change_count_for_job(db: &LocalDb, job_id: &str) -> i64 {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM file_changes WHERE job_id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.unwrap();
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

async fn issue_visible_file_change_count(db: &LocalDb, issue_id: &str) -> i64 {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*)
                     FROM file_changes fc
                     JOIN jobs j ON j.id = fc.job_id
                     WHERE j.issue_id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.unwrap();
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

async fn outbox_entries_for(db: &LocalDb, kind: &str, dedupe_key: &str) -> Vec<(String, String)> {
    let kind = kind.to_string();
    let dedupe_key = dedupe_key.to_string();
    db.read(|conn| {
        let kind = kind.clone();
        let dedupe_key = dedupe_key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, state FROM effect_outbox
                     WHERE kind = ?1 AND dedupe_key = ?2
                     ORDER BY created_at ASC",
                    params![kind.as_str(), dedupe_key.as_str()],
                )
                .await?;
            let mut entries = Vec::new();
            while let Some(row) = rows.next().await? {
                entries.push((row.text(0)?, row.text(1)?));
            }
            Ok(entries)
        })
    })
    .await
    .unwrap()
}

fn drain_effects(rx: &mut UnboundedReceiver<WorkflowEffect>) -> Vec<WorkflowEffect> {
    let mut effects = Vec::new();
    while let Ok(effect) = rx.try_recv() {
        effects.push(effect);
    }
    effects
}

async fn insert_github_app(db: &LocalDb) {
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO github_app (id, app_id, private_key, installation_id)
                 VALUES ('default', 123, ?1, 999)",
                params![TEST_KEY],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn create_project(db: &LocalDb, key: &str, repo_path: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let key = key.to_string();
    let repo_path = repo_path.to_string();
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
         VALUES (?1, 'default', 'Test Project', ?2, ?3, 'main', ?4, ?4)",
        params![id.as_str(), key.as_str(), repo_path.as_str(), now],
    )
    .await
    .unwrap();
    id
}

async fn create_issue(db: &LocalDb, project_id: &str, number: i64) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'Issue', 'active', 'idle', 'none', ?4, ?4)",
        params![id.as_str(), project_id.as_str(), number, now],
    )
    .await
    .unwrap();
    id
}

async fn create_job(db: &LocalDb, project_id: &str, issue_id: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO jobs (id, project_id, issue_id, status, branch, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'complete', 'feature', ?4, ?4)",
        params![id.as_str(), project_id.as_str(), issue_id.as_str(), now],
    )
    .await
    .unwrap();
    id
}

async fn create_merge_request(
    db: &LocalDb,
    job_id: &str,
    project_id: &str,
    issue_id: &str,
    pr_number: i64,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let job_id = job_id.to_string();
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    let now = chrono::Utc::now().timestamp();
    let url = format!("https://github.com/octo/widget/pull/{}", pr_number);
    db.execute(
        "INSERT INTO merge_requests (
            id, job_id, project_id, issue_id, title, source_branch, target_branch,
            status, merge_method, opened_at, updated_at, github_pr_number, github_pr_url
         ) VALUES (?1, ?2, ?3, ?4, 'PR', 'feature', 'main', 'open', 'squash', ?5, ?5, ?6, ?7)",
        params![
            id.as_str(),
            job_id.as_str(),
            project_id.as_str(),
            issue_id.as_str(),
            now,
            pr_number,
            url.as_str()
        ],
    )
    .await
    .unwrap();
    id
}

/// A local (no-remote) merge request: the jj store fold owns the merge, so there
/// is no `github_pr_number` and `is_local` is set.
async fn create_local_merge_request(
    db: &LocalDb,
    job_id: &str,
    project_id: &str,
    issue_id: &str,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO merge_requests (
            id, job_id, project_id, issue_id, title, source_branch, target_branch,
            status, merge_method, opened_at, updated_at, is_local
         ) VALUES (?1, ?2, ?3, ?4, 'PR', 'feature', 'main', 'open', 'squash', ?5, ?5, 1)",
        params![id.as_str(), job_id, project_id, issue_id, now],
    )
    .await
    .unwrap();
    id
}

async fn mr_status(db: &LocalDb, mr_id: &str) -> String {
    let mr_id = mr_id.to_string();
    db.read(|conn| {
        let mr_id = mr_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM merge_requests WHERE id = ?1",
                    params![mr_id.as_str()],
                )
                .await?;
            rows.next().await?.unwrap().text(0)
        })
    })
    .await
    .unwrap()
}

async fn issue_status(db: &LocalDb, issue_id: &str) -> String {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM issues WHERE id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            rows.next().await?.unwrap().text(0)
        })
    })
    .await
    .unwrap()
}

fn token_response() -> HttpResponse {
    HttpResponse::new(
        201,
        serde_json::to_vec(&serde_json::json!({
            "token": "ghs_test",
            "expires_at": "2099-01-01T00:00:00Z"
        }))
        .unwrap(),
    )
}

fn pr_response(state: &str, merged: bool) -> HttpResponse {
    pr_response_with_head(state, merged, "abc123")
}

/// A PR response whose head SHA is chosen by the caller.
///
/// The head matters wherever the branch store is real: the artifact compares
/// GitHub's head against the branch's current commit, so a test that means to
/// exercise something OTHER than head divergence has to hand GitHub the commit
/// the branch actually holds.
fn pr_response_with_head(state: &str, merged: bool, head_sha: &str) -> HttpResponse {
    HttpResponse::new(
        200,
        serde_json::to_vec(&serde_json::json!({
            "title": "My PR",
            "body": "A description",
            "state": state,
            "draft": false,
            "mergeable": true,
            "mergeable_state": "clean",
            "additions": 12,
            "deletions": 3,
            "merged": merged,
            "head": { "sha": head_sha }
        }))
        .unwrap(),
    )
}

fn empty_array_response() -> HttpResponse {
    HttpResponse::new(200, b"[]".to_vec())
}

fn check_runs_response() -> HttpResponse {
    HttpResponse::new(
        200,
        serde_json::to_vec(&serde_json::json!({
            "check_runs": [
                { "name": "build", "status": "completed", "conclusion": "success",
                  "html_url": "https://example.com/1", "output": { "summary": null } }
            ]
        }))
        .unwrap(),
    )
}

fn files_response() -> HttpResponse {
    HttpResponse::new(
        200,
        serde_json::to_vec(&serde_json::json!([
            { "filename": "src/lib.rs", "status": "modified", "additions": 10,
              "deletions": 2, "changes": 12, "patch": "@@ -1 +1 @@\n-old\n+new" }
        ]))
        .unwrap(),
    )
}

/// A GitHub PUT `/merge` response with the given status. 200 is the success
/// shape; 405 is GitHub's "not mergeable" refusal.
fn merge_response(status: u16) -> HttpResponse {
    HttpResponse::new(
        status,
        serde_json::to_vec(&serde_json::json!({
            "merged": status == 200,
            "message": "Pull Request is not mergeable"
        }))
        .unwrap(),
    )
}

#[tokio::test]
async fn try_resolve_and_render_are_none_for_non_pr_job() {
    let (_temp, db) = common::migrated_db().await;
    let project = create_project(&db, "np", "/tmp/none").await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    // No merge_requests row for this job.
    let (_cfg, orch) = orchestrator_with_http(db, MockHttpClient::new()).await;

    let ctx = actions::try_resolve_mr_context_for_job(&orch.db.local, &job)
        .await
        .unwrap();
    assert!(ctx.is_none(), "job without a PR should resolve to None");

    let section = actions::render_live_pr_section(&orch, &job, "cairn://x", false).await;
    assert!(
        section.is_none(),
        "non-PR artifact should get no live PR section"
    );
}

#[tokio::test]
async fn refresh_pr_for_job_updates_cache() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "rf", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("open", false));
    let (_cfg, orch) = orchestrator_with_http(db, http).await;

    let cache = actions::refresh_pr_for_job(&orch, &job).await.unwrap();
    assert_eq!(cache.pr_number, Some(5));
    assert_eq!(cache.additions, Some(12));
    assert_eq!(cache.deletions, Some(3));

    // Cache row updated with live github_state.
    let state = orch
        .db
        .local
        .read(|conn| {
            let mr = mr.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT github_state FROM merge_requests WHERE id = ?1",
                        params![mr.as_str()],
                    )
                    .await?;
                rows.next().await?.unwrap().opt_text(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(state.as_deref(), Some("OPEN"));
}

#[tokio::test]
async fn refresh_pr_for_job_populates_github_state_fields() {
    // Refreshing after opening a PR must populate the display-only GitHub state
    // fields (github_mergeable / github_review / checks_status) for the live PR
    // section.
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "rfs", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("open", false));
    let (_cfg, orch) = orchestrator_with_http(db, http).await;

    actions::refresh_pr_for_job(&orch, &job).await.unwrap();

    let (mergeable, review, checks) = orch
        .db
        .local
        .read(|conn| {
            let mr = mr.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT github_mergeable, github_review, checks_status
                         FROM merge_requests WHERE id = ?1",
                        params![mr.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                Ok::<_, cairn_core::internal::storage::DbError>((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.opt_text(2)?,
                ))
            })
        })
        .await
        .unwrap();

    // pr_response: mergeable true + clean state -> MERGEABLE; no reviews ->
    // review NULL; one successful check -> SUCCESS.
    assert_eq!(mergeable.as_deref(), Some("MERGEABLE"));
    assert_eq!(review, None);
    assert_eq!(checks.as_deref(), Some("SUCCESS"));
}

#[tokio::test]
async fn render_live_pr_section_includes_actions_and_honors_diff_full() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "rv", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let _mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/pulls/5/files", files_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("open", false));
    let (_cfg, orch) = orchestrator_with_http(db, http).await;

    let uri = "cairn://p/rv/1/1/builder/pr";
    let default = actions::render_live_pr_section(&orch, &job, uri, false)
        .await
        .expect("PR artifact should yield a section");
    assert!(default.contains("## Pull Request"));
    assert!(default.contains("PR #5"));
    assert!(
        default.contains("\nChecked: "),
        "dates the GitHub read its facts come from: {default}"
    );
    assert!(default.contains("Changes: +12 -3"));
    assert!(default.contains("src/lib.rs"));
    // Open PR advertises actions.
    assert!(default.contains("## actions"));
    assert!(default.contains("action:\"merge\""));
    // Default omits patch text.
    assert!(!default.contains("```diff"));

    let full = actions::render_live_pr_section(&orch, &job, uri, true)
        .await
        .unwrap();
    assert!(full.contains("```diff"));
    assert!(full.contains("+new"));
}

/// A merged pull request as GitHub reports it, carrying the merge commit.
fn merged_pr_response(merge_commit: &str) -> HttpResponse {
    HttpResponse::new(
        200,
        serde_json::to_vec(&serde_json::json!({
            "title": "My PR",
            "body": "A description",
            "state": "closed",
            "draft": false,
            "mergeable": null,
            "mergeable_state": "unknown",
            "additions": 12,
            "deletions": 3,
            "merged": true,
            "merge_commit_sha": merge_commit,
            "head": { "sha": "abc123" }
        }))
        .unwrap(),
    )
}

async fn text_column(db: &LocalDb, sql: &'static str, id: &str) -> Option<String> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn.query(sql, params![id.as_str()]).await?;
            match rows.next().await? {
                Some(row) => row.opt_text(0),
                None => Ok(None),
            }
        })
    })
    .await
    .unwrap()
}

/// A merge made in GitHub's web UI resolves the merge request the first time a
/// refresh sees it, exactly as a Cairn merge would: the row with GitHub's merge
/// commit, the issue, and the `pr` node's blocked action run. The first reader
/// here is the `pr` node itself, addressed by its action run, which owns no
/// row and used to fail with "No merge request found". A second refresh changes
/// nothing.
#[tokio::test]
async fn a_merge_made_on_github_resolves_on_refresh_exactly_once() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "gh", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    insert_execution(&db, "exec-gh", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-gh")).await;
    let action_run = create_action_run(&db, &project, "exec-gh", &job).await;
    db.execute(
        "UPDATE action_runs SET status = 'blocked' WHERE id = ?1",
        params![action_run.as_str()],
    )
    .await
    .unwrap();
    let mr = create_merge_request(&db, &job, &project, &issue, 43).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/43/reviews", empty_array_response())
        .respond_to("/pulls/43/files", files_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/43", merged_pr_response("b77e76577b"));
    let (_cfg, orch) = orchestrator_with_http(db, http).await;
    let db = &orch.db.local;

    let section = actions::render_live_pr_section(&orch, &action_run, "cairn://p/gh/1/1/pr", false)
        .await
        .expect("the pr node renders its pull request");
    assert!(!section.contains("failed to refresh"), "{section}");
    assert!(section.contains("State: MERGED"), "{section}");
    assert!(
        section.contains("Merged by someone outside Cairn (observed on GitHub)"),
        "{section}"
    );
    // The provenance is a short summary that links to the execution, never the
    // execution's snapshot pasted in as JSON.
    let provenance = section
        .split("<details><summary>Resolution provenance</summary>")
        .nth(1)
        .and_then(|rest| rest.split("</details>").next())
        .expect("the resolution renders its provenance");
    assert!(provenance.len() < 2_048, "{provenance}");
    assert!(
        provenance.contains("Surface: `github-refresh-observation`"),
        "{provenance}"
    );
    assert!(
        provenance.contains("execution: cairn://p/gh/1/1"),
        "{provenance}"
    );
    assert!(!provenance.contains("Lane snapshot"), "{provenance}");
    assert!(!provenance.contains('{'), "{provenance}");
    let stored = text_column(
        db,
        "SELECT lane_snapshot FROM pr_resolution_attributions WHERE merge_request_id = ?1",
        &mr,
    )
    .await
    .expect("the observation journals a lane snapshot");
    assert!(
        !stored.contains("executionSnapshot") && !stored.contains("PR test"),
        "the execution's snapshot is not copied: {stored}"
    );

    assert_eq!(mr_status(db, &mr).await, "merged");
    assert_eq!(
        text_column(
            db,
            "SELECT merged_commit FROM merge_requests WHERE id = ?1",
            &mr
        )
        .await
        .as_deref(),
        Some("b77e76577b")
    );
    let merged_at = text_column(
        db,
        "SELECT CAST(merged_at AS TEXT) FROM merge_requests WHERE id = ?1",
        &mr,
    )
    .await;
    assert!(merged_at.is_some());
    assert_eq!(issue_status(db, &issue).await, "merged");
    assert_eq!(
        text_column(
            db,
            "SELECT status FROM action_runs WHERE id = ?1",
            &action_run
        )
        .await
        .as_deref(),
        Some("complete")
    );
    let observations = "SELECT CAST(COUNT(*) AS TEXT) FROM pr_resolution_attributions
                         WHERE merge_request_id = ?1 AND actor_kind = 'unjournaled'
                           AND surface = 'github-refresh-observation'";
    assert_eq!(
        text_column(db, observations, &mr).await.as_deref(),
        Some("1")
    );

    let cache = actions::refresh_pr_for_job(&orch, &job).await.unwrap();
    assert_eq!(cache.state.to_string(), "MERGED");
    assert_eq!(
        text_column(db, observations, &mr).await.as_deref(),
        Some("1"),
        "a second refresh journals nothing"
    );
    assert_eq!(
        text_column(
            db,
            "SELECT CAST(merged_at AS TEXT) FROM merge_requests WHERE id = ?1",
            &mr
        )
        .await,
        merged_at,
        "a second refresh moves nothing"
    );
}

#[tokio::test]
async fn close_pr_for_job_marks_closed() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "cl", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    // A recipe-backed producing job: standalone (execution-less) jobs were
    // retired, so resolving the PR recomputes through the job's execution sweep.
    insert_execution(&db, "exec-cl", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-cl")).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("closed", false));
    let (_cfg, orch) = orchestrator_with_http(db, http).await;

    let msg = actions::close_pr_for_job(&orch, &job, None).await.unwrap();
    assert!(msg.contains("closed"));
    assert_eq!(mr_status(&orch.db.local, &mr).await, "closed");
}

#[tokio::test]
async fn reconcile_after_merge_for_action_run_owner_stores_files_under_parent_job() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "arf", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    insert_execution(&db, "exec-arf", &project, &issue).await;
    let parent_job = create_job_with_execution(&db, &project, &issue, Some("exec-arf")).await;
    let action_run = create_action_run(&db, &project, "exec-arf", &parent_job).await;
    let _mr = create_merge_request(&db, &action_run, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/files", files_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("closed", true));
    let mut git = MockGitClient::new();
    git.expect_current_branch()
        .returning(|_| Ok("feature".to_string()));
    git.expect_branch_exists()
        .withf(|_, branch| branch == "feature")
        .returning(|_, _| Ok(true));
    git.expect_delete_branch()
        .withf(|_, branch, force| branch == "feature" && *force)
        .returning(|_, _, _| Ok(()));
    git.expect_worktree_list()
        .returning(|_| Ok("worktree /tmp/repo\nHEAD abc\nbranch refs/heads/main\n".to_string()));
    let (_cfg, orch) = orchestrator_with_http_and_git(db, http, git).await;
    let ctx = actions::resolve_merge_mr_context_for_job(&orch.db.local, &action_run)
        .await
        .unwrap();

    actions::reconcile_after_merge(
        orch.clone(),
        orch.db.local.clone(),
        ctx,
        actions::CheckoutReconciliation::RepairLocalFold,
    )
    .await;

    assert_eq!(
        file_change_count_for_job(&orch.db.local, &parent_job).await,
        1
    );
    assert_eq!(
        file_change_count_for_job(&orch.db.local, &action_run).await,
        0
    );
    assert_eq!(
        issue_visible_file_change_count(&orch.db.local, &issue).await,
        1
    );
}

const PIN_BUMP_CONFIG: &str = "checks:
  pin-tests:
    command: cargo nextest run pin
    verdictPlatforms: [linux, macos]
pinBumps:
  tool:
    pin: tools/tool-pin.json
    frozen: [integration]
    checks: [pin-tests]
    platforms: [linux, macos]
";

fn tool_pin(version: &str) -> String {
    format!("{{\"integration\": 1, \"version\": \"{version}\", \"skip\": []}}\n")
}

/// A project whose `main` declares a pinBumps gate over `tools/tool-pin.json`
/// at 1.0.0, with `feature` sealed one commit ahead bumping only that pin to
/// 1.1.0. Returns the feature head commit.
fn provision_pin_bump(bin: &str, config_dir: &Path) -> (TempDir, String) {
    let repo = temp_jj_project();
    std::fs::create_dir_all(repo.path().join(".cairn")).unwrap();
    std::fs::create_dir_all(repo.path().join("tools")).unwrap();
    std::fs::write(repo.path().join(".cairn/config.yaml"), PIN_BUMP_CONFIG).unwrap();
    std::fs::write(repo.path().join("tools/tool-pin.json"), tool_pin("1.0.0")).unwrap();
    for args in [
        &["add", "-A"][..],
        &["commit", "-q", "-m", "declare the pin gate"],
    ] {
        let status = Command::new("git")
            .args(args)
            .current_dir(repo.path())
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }
    let ws = config_dir.join("feature-ws");
    common::provision_jj_workspace(config_dir, repo.path(), &ws, "feature");
    let jj = jj::environment(bin, config_dir);
    std::fs::write(ws.join("tools/tool-pin.json"), tool_pin("1.1.0")).unwrap();
    jj::seal(&jj, &ws, "bump tool to 1.1.0", None).unwrap();
    let head = jj_bookmark_commit(bin, config_dir, repo.path(), "feature");
    (repo, head)
}

/// `(sequence, state, detail)` for every journal event of one merge request.
async fn merge_journal(db: &LocalDb, mr_id: &str) -> Vec<(i64, String, Option<String>)> {
    let mr_id = mr_id.to_string();
    db.read(|conn| {
        let mr_id = mr_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT sequence, state, detail FROM merge_attempt_events
                      WHERE merge_request_id = ?1 ORDER BY created_at, sequence",
                    params![mr_id.as_str()],
                )
                .await?;
            let mut events = Vec::new();
            while let Some(row) = rows.next().await? {
                events.push((row.i64(0)?, row.text(1)?, row.opt_text(2)?));
            }
            Ok(events)
        })
    })
    .await
    .unwrap()
}

/// Without verdicts at the head, a pin bump is refused naming the missing
/// verdict, journaled as a `violation`, and nothing lands.
#[tokio::test]
async fn pin_bump_merge_refuses_without_head_verdicts_and_journals_the_violation() {
    let bin =
        common::jj_bin().expect("jj must be provisioned in the build slot or available on PATH");
    let (_temp, db) = common::migrated_db().await;
    let cfg = tempfile::tempdir().unwrap();
    let (repo, head) = provision_pin_bump(&bin, &cfg.path().join("config"));
    let project = create_project(&db, "pbr", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    insert_execution(&db, "exec-pbr", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-pbr")).await;
    let mr = create_local_merge_request(&db, &job, &project, &issue).await;
    let orch = orchestrator_with_http_in(db, MockHttpClient::new(), &cfg).await;

    let error = actions::merge_pr_for_job(
        &orch,
        &job,
        None,
        None,
        Some(actions::MergePrecondition::PinBump),
    )
    .await
    .expect_err("a bump without verdicts must be refused");
    assert!(
        error.starts_with("Refusing to merge `feature` as a pin bump: verdict missing"),
        "{error}"
    );
    assert!(
        error.contains("pin-tests") && error.contains(&head),
        "{error}"
    );

    let journal = merge_journal(&orch.db.local, &mr).await;
    assert_eq!(journal.len(), 2, "{journal:?}");
    assert_eq!(
        (journal[0].0, journal[0].1.as_str()),
        (0, "policy_disabled")
    );
    assert_eq!((journal[1].0, journal[1].1.as_str()), (1, "violation"));
    assert!(
        journal[1]
            .2
            .as_deref()
            .is_some_and(|detail| detail.starts_with("pinBump: verdict missing")),
        "{journal:?}"
    );
    assert_eq!(mr_status(&orch.db.local, &mr).await, "open");
    assert_ne!(
        jj_bookmark_commit(&bin, &cfg.path().join("config"), repo.path(), "main"),
        head,
        "nothing landed"
    );
}

/// A real remote PR landing on the default branch lands by the store fold,
/// published to origin by a leased fast-forward of `main`; GitHub's merge API is
/// not a landing route, so it is not mocked here and a call to it would fail
/// the merge. Origin and the store's `main` both name the landing, and Cairn
/// marks the merge request merged and resolves the issue.
#[tokio::test]
async fn merge_pr_for_job_remote_default_branch_lands_by_a_leased_push_to_origin() {
    let Some(bin) = common::jj_bin() else {
        eprintln!(
            "skipping merge_pr_for_job_remote_default_branch_lands_by_a_leased_push_to_origin: \
             jj not resolvable"
        );
        return;
    };
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_jj_project();
    let (_origin_dir, origin) = add_fetchable_github_origin(repo.path());
    insert_github_app(&db).await;
    let project = create_project(&db, "gh", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    // A recipe-backed producing job: standalone (execution-less) jobs were
    // retired, so resolving the PR recomputes through the job's execution sweep.
    insert_execution(&db, "exec-gh", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-gh")).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        // Best-effort source-branch delete + background reconcile endpoints.
        .respond_to("/git/refs/heads/feature", merge_response(200))
        .respond_to("/pulls/5/files", files_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("closed", true));
    let (cfg, orch) = orchestrator_with_http(db, http).await;
    let config_dir = cfg.path().join("config");
    provision_jj_merge(&config_dir, repo.path());
    let base_before = jj_bookmark_commit(&bin, &config_dir, repo.path(), "main");

    actions::merge_pr_for_job(&orch, &job, None, None, None)
        .await
        .unwrap();
    assert_eq!(mr_status(&orch.db.local, &mr).await, "merged");
    assert_eq!(issue_status(&orch.db.local, &issue).await, "merged");
    let landed = jj_bookmark_commit(&bin, &config_dir, repo.path(), "main");
    assert_ne!(
        landed, base_before,
        "the landing advanced the store's `main`"
    );
    assert_eq!(
        git_rev_parse(&origin, "main"),
        landed,
        "origin's `main` took the store's landing"
    );
}

/// FAIL-CLOSED: when origin refuses the landing's push (its receiver protects
/// `main`), Cairn surfaces the refusal and marks NOTHING — the merge request
/// stays open, the issue stays active, and neither origin's nor the store's
/// `main` moves.
#[cfg(unix)]
#[tokio::test]
async fn merge_pr_for_job_remote_refused_publication_leaves_unmarked() {
    use std::os::unix::fs::PermissionsExt;
    let Some(bin) = common::jj_bin() else {
        eprintln!(
            "skipping merge_pr_for_job_remote_refused_publication_leaves_unmarked: jj not \
             resolvable"
        );
        return;
    };
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_jj_project();
    let (_origin_dir, origin) = add_fetchable_github_origin(repo.path());
    insert_github_app(&db).await;
    let project = create_project(&db, "ghf", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new().respond_to("access_tokens", token_response());
    let (cfg, orch) = orchestrator_with_http(db, http).await;
    let config_dir = cfg.path().join("config");
    provision_jj_merge(&config_dir, repo.path());
    let base_before = jj_bookmark_commit(&bin, &config_dir, repo.path(), "main");
    let origin_before = git_rev_parse(&origin, "main");
    let hook = origin.join("hooks").join("pre-receive");
    std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
    std::fs::write(&hook, "#!/bin/sh\necho 'main is protected' >&2\nexit 1\n").unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();

    let err = actions::merge_pr_for_job(&orch, &job, None, None, None)
        .await
        .expect_err("a landing origin refuses must fail closed");
    assert!(
        err.contains("Nothing was published"),
        "says the refused landing published nothing: {err}"
    );
    assert_eq!(mr_status(&orch.db.local, &mr).await, "open");
    assert_eq!(issue_status(&orch.db.local, &issue).await, "active");
    assert_eq!(
        jj_bookmark_commit(&bin, &config_dir, repo.path(), "main"),
        base_before,
        "the store's `main` never names a landing origin refused"
    );
    assert_eq!(git_rev_parse(&origin, "main"), origin_before);
}

/// The landing rules a project declares for the binder tests: one numbered
/// directory, one version heading, and the version-paragraphs merge that
/// keeps two branches' protocol paragraphs.
const LANDING_CONFIG: &str = "landing:
  identifiers:
    migration:
      kind: numbered-file
      directory: migrations
      width: 4
    executor-protocol:
      kind: version-heading
      file: protocol.rs
      anchor: EXECUTOR_PROTOCOL_VERSION
  merge:
  - paths: [protocol.rs]
    kind: version-paragraphs
    anchor: EXECUTOR_PROTOCOL_VERSION
";

const PROTOCOL_AT_7: &str =
    "/// Version 7 adds the base.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 7;\n";

/// A project whose `main` declares [`LANDING_CONFIG`] and holds migration
/// 0001 and protocol version 7.
fn binding_project() -> TempDir {
    let repo = temp_jj_project();
    commit_to_project(repo.path(), ".cairn/config.yaml", LANDING_CONFIG);
    commit_to_project(
        repo.path(),
        "migrations/0001_base.sql",
        "-- lineage: shared\nSELECT 1;\n",
    );
    commit_to_project(repo.path(), "protocol.rs", PROTOCOL_AT_7);
    repo
}

/// A producing job on `branch` with its own issue and execution, and a local
/// merge request from `branch` into `main`.
async fn branch_merge_request(
    db: &LocalDb,
    project_id: &str,
    number: i64,
    branch: &str,
) -> (String, String, String) {
    let issue = create_issue(db, project_id, number).await;
    let execution = format!("exec-{branch}");
    insert_execution(db, &execution, project_id, &issue).await;
    let job = uuid::Uuid::new_v4().to_string();
    let mr = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO jobs (id, project_id, issue_id, execution_id, status, branch, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 'complete', ?5, ?6, ?6)",
        params![job.as_str(), project_id, issue.as_str(), execution.as_str(), branch, now],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO merge_requests (
            id, job_id, project_id, issue_id, title, source_branch, target_branch,
            status, merge_method, opened_at, updated_at, is_local
         ) VALUES (?1, ?2, ?3, ?4, 'PR', ?5, 'main', 'open', 'squash', ?6, ?6, 1)",
        params![
            mr.as_str(),
            job.as_str(),
            project_id,
            issue.as_str(),
            branch,
            now
        ],
    )
    .await
    .unwrap();
    (issue, job, mr)
}

/// Cut `branch` from `main` and seal `files` on it.
fn seal_branch(config_dir: &Path, repo: &Path, branch: &str, files: &[(&str, &str)]) {
    let ws = config_dir.join(format!("{}-ws", branch.replace('/', "-")));
    common::provision_jj_workspace(config_dir, repo, &ws, branch);
    for (path, content) in files {
        let target = ws.join(path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(target, content).unwrap();
    }
    jj::seal(&jj::environment("jj", config_dir), &ws, branch, None).unwrap();
}

/// One change takes one version: a branch that heads two protocol versions is
/// refused when it would land, and nothing moves.
#[tokio::test]
async fn a_branch_heading_two_protocol_versions_is_refused_before_it_lands() {
    let Some(bin) = common::jj_bin() else {
        eprintln!("skipping a_branch_heading_two_protocol_versions_is_refused_before_it_lands: jj not resolvable");
        return;
    };
    let (_temp, db) = common::migrated_db().await;
    let repo = binding_project();
    let project = create_project(&db, "bind2", repo.path().to_str().unwrap()).await;
    let (issue, job, mr) = branch_merge_request(&db, &project, 1, "agent/twice").await;
    let (cfg, orch) = orchestrator_with_http(db, MockHttpClient::new()).await;
    let config_dir = cfg.path().join("config");
    seal_branch(
        &config_dir,
        repo.path(),
        "agent/twice",
        &[(
            "protocol.rs",
            "/// Version 7 adds the base.\n///\n/// Version NEXT a.\n///\n/// Version 8 b.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 9;\n",
        )],
    );
    let main_before = jj_bookmark_commit(&bin, &config_dir, repo.path(), "main");

    let err = actions::merge_pr_for_job(&orch, &job, None, None, None)
        .await
        .expect_err("two versions for one change must not land");
    assert!(err.contains("one change takes one version"), "{err}");
    assert_eq!(mr_status(&orch.db.local, &mr).await, "open");
    assert_eq!(issue_status(&orch.db.local, &issue).await, "active");
    assert_eq!(
        jj_bookmark_commit(&bin, &config_dir, repo.path(), "main"),
        main_before
    );
}

/// cairn/5926: a branch whose test selects its provisional migration by
/// version, `"NEXT_gamma"`, is refused when it would land, naming the site,
/// rather than rewritten to the stem `0002_gamma` while the bound version is
/// `0002`. Neither `main` nor the branch moves.
#[tokio::test]
async fn a_branch_quoting_a_provisional_version_is_refused_before_it_lands() {
    let Some(bin) = common::jj_bin() else {
        eprintln!("skipping a_branch_quoting_a_provisional_version_is_refused_before_it_lands: jj not resolvable");
        return;
    };
    let (_temp, db) = common::migrated_db().await;
    let repo = binding_project();
    let project = create_project(&db, "bind3", repo.path().to_str().unwrap()).await;
    let (issue, job, mr) = branch_merge_request(&db, &project, 1, "agent/gamma").await;
    let (cfg, orch) = orchestrator_with_http(db, MockHttpClient::new()).await;
    let config_dir = cfg.path().join("config");
    seal_branch(
        &config_dir,
        repo.path(),
        "agent/gamma",
        &[
            (
                "migrations/NEXT_gamma.sql",
                "-- lineage: shared\nCREATE TABLE gamma (id INTEGER);\n",
            ),
            (
                "gamma_test.rs",
                "fn head() {\n    MIGRATIONS.iter().take_while(|m| m.version != \"NEXT_gamma\");\n}\n",
            ),
        ],
    );
    let main_before = jj_bookmark_commit(&bin, &config_dir, repo.path(), "main");
    let branch_before = jj_bookmark_commit(&bin, &config_dir, repo.path(), "agent/gamma");

    let err = actions::merge_pr_for_job(&orch, &job, None, None, None)
        .await
        .expect_err("a provisional version literal must not land");
    assert!(
        err.starts_with("Refusing to merge `agent/gamma` into `main`: [migration] "),
        "{err}"
    );
    assert!(err.contains("gamma_test.rs:2"), "{err}");
    assert!(err.contains("(`0002` and `0002_gamma`)"), "{err}");
    assert!(err.ends_with("`main` was not advanced."), "{err}");
    assert_eq!(mr_status(&orch.db.local, &mr).await, "open");
    assert_eq!(issue_status(&orch.db.local, &issue).await, "active");
    assert_eq!(
        jj_bookmark_commit(&bin, &config_dir, repo.path(), "main"),
        main_before
    );
    assert_eq!(
        jj_bookmark_commit(&bin, &config_dir, repo.path(), "agent/gamma"),
        branch_before,
        "the prospective bind is undone"
    );
}

/// Pre-flight: the live PR section surfaces a conflicted history BEFORE a merge.
/// It renders CONFLICTING (over GitHub's false mergeable bit), enumerates the
/// conflicted commit and file, and flags the inflated diff as stale.
///
/// The pull request's head is the branch's REAL current commit here, so this
/// isolates the conflict override: with a divergent head the answer would be
/// UNKNOWN instead, which
/// `render_live_pr_section_publishes_no_verdict_for_a_conflicted_tip_at_a_stale_head`
/// covers.
#[tokio::test]
async fn render_live_pr_section_flags_conflicted_history() {
    let Some(bin) = common::jj_bin() else {
        eprintln!("skipping render_live_pr_section_flags_conflicted_history: jj not resolvable");
        return;
    };
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_jj_project();
    add_github_origin(repo.path());
    insert_github_app(&db).await;
    let project = create_project(&db, "rvc", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let _mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    // Provision first so GitHub can be handed the commit `feature` actually
    // holds; otherwise the head would diverge and no verdict would be published.
    let cfg = tempfile::tempdir().unwrap();
    let config_dir = cfg.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    provision_jj_conflict(&bin, &config_dir, repo.path());
    let feature_head = jj_bookmark_commit(&bin, &config_dir, repo.path(), "feature");

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/pulls/5/files", files_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to(
            "/pulls/5",
            pr_response_with_head("open", false, &feature_head),
        );
    let orch = orchestrator_with_http_in(db, http, &cfg).await;

    let section =
        actions::render_live_pr_section(&orch, &job, "cairn://p/rvc/1/1/builder/pr", false)
            .await
            .expect("PR artifact should yield a section");
    // GitHub reports mergeable=true (+12/-3); jj overrides with the conflict reality.
    assert!(
        section.contains("Mergeable: CONFLICTING"),
        "renders CONFLICTING over GitHub's false bit: {section}"
    );
    assert!(
        section.contains("Conflicted history"),
        "enumerates the conflicted history: {section}"
    );
    assert!(
        section.contains("shared.txt"),
        "names the conflicted file: {section}"
    );
    assert!(
        section.contains("stale"),
        "flags the inflated diff as stale: {section}"
    );
    assert!(
        section.contains("resolve-at-base"),
        "names the recovery path: {section}"
    );
}

/// Review regression: a divergent head AND a conflicted current tip.
///
/// These two probes have different subjects — GitHub's mergeability is about the
/// head the pull request holds, the conflict report is about the commit the
/// branch holds now. When the head is stale, letting the conflict probe win
/// would cache CONFLICTING as a property of a pull request whose tree does not
/// contain that conflict at all. The verdict must stay UNKNOWN, and both facts
/// must still be stated in prose.
///
/// This combination is the COMMON shape of the stale head in practice: the same
/// rebase that conflicts a tip also moves the bookmark away from what was pushed.
#[tokio::test]
async fn render_live_pr_section_publishes_no_verdict_for_a_conflicted_tip_at_a_stale_head() {
    let Some(bin) = common::jj_bin() else {
        eprintln!("skipping render_live_pr_section_publishes_no_verdict_for_a_conflicted_tip_at_a_stale_head: jj not resolvable");
        return;
    };
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_jj_project();
    add_github_origin(repo.path());
    insert_github_app(&db).await;
    let project = create_project(&db, "rvcs", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/pulls/5/files", files_response())
        .respond_to("/check-runs", check_runs_response())
        // head `abc123` is not the commit the branch holds: the PR is stale.
        .respond_to("/pulls/5", pr_response("open", false));
    let (cfg, orch) = orchestrator_with_http(db, http).await;
    let config_dir = cfg.path().join("config");
    provision_jj_conflict(&bin, &config_dir, repo.path());
    let branch_head = jj_bookmark_commit(&bin, &config_dir, repo.path(), "feature");

    let section =
        actions::render_live_pr_section(&orch, &job, "cairn://p/rvcs/1/1/builder/pr", false)
            .await
            .expect("PR artifact should yield a section");

    assert!(
        section.contains("Mergeable: UNKNOWN"),
        "a conflict on the current tip is not a verdict on a stale head: {section}"
    );
    assert!(
        !section.contains("Mergeable: CONFLICTING"),
        "must not attribute the branch's conflict to the pull request: {section}"
    );
    // Both facts still reach the reader in prose.
    assert!(
        section.contains("version of `feature`") && section.contains(&branch_head[..12]),
        "names the divergence and both commits: {section}"
    );
    assert!(
        section.contains("Conflicted history") && section.contains("shared.txt"),
        "still enumerates the conflicted history: {section}"
    );

    // The refresh-on-read persists UNKNOWN, so the desktop's cached path cannot
    // serve CONFLICTING about the pull request either.
    let cached = orch
        .db
        .local
        .read(|conn| {
            let mr = mr.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT github_mergeable FROM merge_requests WHERE id = ?1",
                        params![mr.as_str()],
                    )
                    .await?;
                rows.next().await?.unwrap().opt_text(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(cached.as_deref(), Some("UNKNOWN"));
}

/// CAIRN-3197: a pull request whose head lags its branch read OPEN / MERGEABLE
/// while the entire merge-blocking fix sat in commits the pull request did not
/// contain. Every green signal described a tree nobody had validated.
///
/// GitHub reports `mergeable: true` for head `abc123`; the branch store holds a
/// different commit for `feature`. The artifact must name both commits and must
/// not publish an unqualified verdict — and the downgrade must reach the cached
/// row too, since the desktop reads that row without probing.
#[tokio::test]
async fn render_live_pr_section_flags_a_pull_request_behind_its_branch() {
    let Some(bin) = common::jj_bin() else {
        eprintln!(
            "skipping render_live_pr_section_flags_a_pull_request_behind_its_branch: jj not resolvable"
        );
        return;
    };
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_jj_project();
    add_github_origin(repo.path());
    insert_github_app(&db).await;
    let project = create_project(&db, "rvstale", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/pulls/5/files", files_response())
        .respond_to("/check-runs", check_runs_response())
        // mergeable: true, mergeable_state: clean, head.sha: abc123.
        .respond_to("/pulls/5", pr_response("open", false));
    let (cfg, orch) = orchestrator_with_http(db, http).await;
    let config_dir = cfg.path().join("config");
    // `feature` is sealed one commit ahead of `main`, so the store holds a real
    // commit that is not the `abc123` GitHub reports as the pull request's head.
    provision_jj_merge(&config_dir, repo.path());
    let branch_head = jj_bookmark_commit(&bin, &config_dir, repo.path(), "feature");

    let section =
        actions::render_live_pr_section(&orch, &job, "cairn://p/rvstale/1/1/builder/pr", false)
            .await
            .expect("PR artifact should yield a section");

    assert!(
        section.contains("Mergeable: UNKNOWN"),
        "GitHub's MERGEABLE describes the head it holds, not the branch: {section}"
    );
    assert!(
        section.contains("version of `feature`"),
        "says the pull request shows a different version of the branch: {section}"
    );
    assert!(
        section.contains("abc123"),
        "names the commit the pull request holds: {section}"
    );
    assert!(
        section.contains(&branch_head[..12]),
        "names the commit the branch holds ({branch_head}): {section}"
    );

    // The refresh-on-read persists the downgrade, so the desktop's cached path
    // cannot keep serving the green verdict.
    let cached = orch
        .db
        .local
        .read(|conn| {
            let mr = mr.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT github_mergeable FROM merge_requests WHERE id = ?1",
                        params![mr.as_str()],
                    )
                    .await?;
                rows.next().await?.unwrap().opt_text(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(cached.as_deref(), Some("UNKNOWN"));
}

// ----------------------------------------------------------------------------
// DAG advancement after PR resolution
//
// Every path that terminates a PR — webhook merge/close, in-app merge/close —
// must enqueue an `AdvanceDag` for the execution that produced the PR so
// downstream nodes gated on PR-merged-ness wake. The helper
// `advance_producing_execution_after_pr_resolution` is the single shared point
// these paths converge on.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn helper_advances_when_mr_has_producing_execution() {
    let (_temp, db) = common::migrated_db().await;
    let project = create_project(&db, "ad1", "/tmp/ad1").await;
    let issue = create_issue(&db, &project, 1).await;
    insert_execution(&db, "exec-ad1", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-ad1")).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let (_cfg, orch, mut rx) = orchestrator_with_effect_channel(db, MockHttpClient::new()).await;

    actions::advance_producing_execution_after_pr_resolution(&orch, &mr)
        .await
        .unwrap();

    let effects = drain_effects(&mut rx);
    assert_eq!(effects.len(), 1, "expected exactly one AdvanceDag effect");
    match &effects[0] {
        WorkflowEffect::AdvanceDag {
            execution_id,
            outbox_entry_id,
        } => {
            assert_eq!(execution_id, "exec-ad1");
            assert!(
                outbox_entry_id.is_some(),
                "helper-sent AdvanceDag should carry the outbox entry id"
            );
        }
        other => panic!("unexpected effect: {:?}", other),
    }

    let entries = outbox_entries_for(&orch.db.local, "advance_dag", "exec-ad1").await;
    assert_eq!(
        entries.len(),
        1,
        "expected one pending advance_dag outbox row keyed by execution id"
    );
    assert_eq!(entries[0].1, "pending");
}

#[tokio::test]
async fn helper_is_noop_when_mr_id_unknown() {
    // No merge_requests row exists for this id at all — the JOIN returns
    // nothing, modeling a webhook arriving for a PR Cairn doesn't track. The
    // helper must swallow this silently rather than enqueue spurious work.
    let (_temp, db) = common::migrated_db().await;
    let (_cfg, orch, mut rx) = orchestrator_with_effect_channel(db, MockHttpClient::new()).await;

    actions::advance_producing_execution_after_pr_resolution(&orch, "mr-does-not-exist")
        .await
        .unwrap();

    let effects = drain_effects(&mut rx);
    assert!(
        effects.is_empty(),
        "unknown mr should not produce an AdvanceDag, got: {:?}",
        effects
    );
}

#[tokio::test]
async fn helper_is_noop_when_job_has_no_execution_id() {
    let (_temp, db) = common::migrated_db().await;
    let project = create_project(&db, "ad3", "/tmp/ad3").await;
    let issue = create_issue(&db, &project, 1).await;
    // Job exists but has no execution_id (non-recipe / one-off run).
    let job = create_job_with_execution(&db, &project, &issue, None).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 11).await;

    let (_cfg, orch, mut rx) = orchestrator_with_effect_channel(db, MockHttpClient::new()).await;

    actions::advance_producing_execution_after_pr_resolution(&orch, &mr)
        .await
        .unwrap();

    let effects = drain_effects(&mut rx);
    assert!(
        effects.is_empty(),
        "job without execution_id should not advance any DAG"
    );
}

#[tokio::test]
async fn helper_is_idempotent_across_repeat_calls() {
    let (_temp, db) = common::migrated_db().await;
    let project = create_project(&db, "ad4", "/tmp/ad4").await;
    let issue = create_issue(&db, &project, 1).await;
    insert_execution(&db, "exec-ad4", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-ad4")).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let (_cfg, orch, mut rx) = orchestrator_with_effect_channel(db, MockHttpClient::new()).await;

    actions::advance_producing_execution_after_pr_resolution(&orch, &mr)
        .await
        .unwrap();
    actions::advance_producing_execution_after_pr_resolution(&orch, &mr)
        .await
        .unwrap();

    let effects = drain_effects(&mut rx);
    assert_eq!(
        effects.len(),
        2,
        "helper documents at-least-once delivery; reduce_dag is idempotent"
    );
    for effect in &effects {
        match effect {
            WorkflowEffect::AdvanceDag { execution_id, .. } => {
                assert_eq!(execution_id, "exec-ad4");
            }
            other => panic!("unexpected effect: {:?}", other),
        }
    }

    let entries = outbox_entries_for(&orch.db.local, "advance_dag", "exec-ad4").await;
    assert_eq!(entries.len(), 2, "each call writes its own outbox row");
}

#[tokio::test]
async fn close_pr_for_job_advances_producing_execution() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "cla", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    insert_execution(&db, "exec-cla", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-cla")).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("closed", false));
    let (_cfg, orch, mut rx) = orchestrator_with_effect_channel(db, http).await;

    actions::close_pr_for_job(&orch, &job, None).await.unwrap();
    assert_eq!(mr_status(&orch.db.local, &mr).await, "closed");

    let effects = drain_effects(&mut rx);
    let has_advance = effects.iter().any(|e| {
        matches!(
            e,
            WorkflowEffect::AdvanceDag { execution_id, .. } if execution_id == "exec-cla"
        )
    });
    assert!(
        has_advance,
        "in-app close must enqueue AdvanceDag for the producing execution; got: {:?}",
        effects
    );

    let entries = outbox_entries_for(&orch.db.local, "advance_dag", "exec-cla").await;
    assert!(
        !entries.is_empty(),
        "in-app close must persist an advance_dag outbox row"
    );
}

#[tokio::test]
async fn team_pr_resolution_queues_private_replay_without_a_live_worker() {
    use cairn_core::internal::storage::{MigrationRunner, TEAM_MIGRATIONS};
    let (_private_dir, private) = common::migrated_db().await;
    let team_dir = tempfile::tempdir().unwrap();
    let team = Arc::new(
        LocalDb::open(team_dir.path().join("team.db"))
            .await
            .unwrap(),
    );
    MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
        .run(&team)
        .await
        .unwrap();
    let project = "teampr~00000000-0000-4000-8000-000000000001";
    let issue = "teampr~00000000-0000-4000-8000-000000000002";
    let execution = "teampr~00000000-0000-4000-8000-000000000003";
    let job = "teampr~00000000-0000-4000-8000-000000000004";
    let mr = "teampr~00000000-0000-4000-8000-000000000005";
    team.execute_script(
        "INSERT INTO teams(id,name,created_at,updated_at) VALUES('teampr','Team',1,1);",
    )
    .await
    .unwrap();
    team.execute("INSERT INTO projects(id,team_id,name,key,repo_path,created_at,updated_at) VALUES(?1,'teampr','Project','tp','/tmp/tp',1,1)",params![project]).await.unwrap();
    team.execute("INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES(?1,?2,1,'Issue','active','active','none',1,1)",params![issue,project]).await.unwrap();
    insert_execution(&team, execution, project, issue).await;
    team.execute("INSERT INTO jobs(id,project_id,issue_id,execution_id,status,branch,created_at,updated_at) VALUES(?1,?2,?3,?4,'complete','feature',1,1)",params![job,project,issue,execution]).await.unwrap();
    team.execute("INSERT INTO merge_requests(id,job_id,project_id,issue_id,title,source_branch,target_branch,status,merge_method,opened_at,updated_at) VALUES(?1,?2,?3,?4,'PR','feature','main','merged','squash',1,1)",params![mr,job,project,issue]).await.unwrap();
    let (_cfg, orch, rx) = orchestrator_with_effect_channel(private, MockHttpClient::new()).await;
    orch.db
        .insert_team_db_for_test("teampr", team.clone())
        .await;
    drop(rx);

    actions::advance_producing_execution_after_pr_resolution(&orch, mr)
        .await
        .unwrap();
    let entries = outbox_entries_for(&orch.db.local, "advance_dag", execution).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, "pending");
    assert_eq!(
        team.query_one(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'effect_outbox'",
            (),
            |row| row.i64(0)
        )
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn merge_pr_for_job_advances_producing_execution() {
    let Some(_bin) = common::jj_bin() else {
        eprintln!("skipping merge_pr_for_job_advances_producing_execution: jj not resolvable");
        return;
    };
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_jj_project();
    let project = create_project(&db, "mga", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    insert_execution(&db, "exec-mga", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-mga")).await;
    let mr = create_local_merge_request(&db, &job, &project, &issue).await;

    let (cfg, orch, mut rx) = orchestrator_with_effect_channel(db, MockHttpClient::new()).await;
    provision_jj_merge(&cfg.path().join("config"), repo.path());

    actions::merge_pr_for_job(&orch, &job, None, None, None)
        .await
        .unwrap();
    assert_eq!(mr_status(&orch.db.local, &mr).await, "merged");

    let effects = drain_effects(&mut rx);
    let has_advance = effects.iter().any(|e| {
        matches!(
            e,
            WorkflowEffect::AdvanceDag { execution_id, .. } if execution_id == "exec-mga"
        )
    });
    assert!(
        has_advance,
        "in-app merge must enqueue AdvanceDag for the producing execution; got: {:?}",
        effects
    );

    let entries = outbox_entries_for(&orch.db.local, "advance_dag", "exec-mga").await;
    assert!(
        !entries.is_empty(),
        "in-app merge must persist an advance_dag outbox row"
    );
}
