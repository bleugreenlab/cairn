//! Integration tests for the host-agnostic PR lifecycle actions
//! (`cairn_core::pr_data::actions`): merge / close / refresh keyed by job_id,
//! plus the live-PR read section.

mod common;

use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;

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
use tempfile::TempDir;
use tokio::sync::mpsc::UnboundedReceiver;
use turso::params;

const TEST_KEY: &str = include_str!("fixtures/test_rsa_key.pem");

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
    run(&["init", "-q"]);
    run(&[
        "remote",
        "add",
        "origin",
        "https://github.com/octo/widget.git",
    ]);
    dir
}

async fn orchestrator_with_http(db: LocalDb, http: MockHttpClient) -> (TempDir, Orchestrator) {
    orchestrator_with_http_and_git(db, http, MockGitClient::new()).await
}

async fn orchestrator_with_http_and_git(
    db: LocalDb,
    http: MockHttpClient,
    git: MockGitClient,
) -> (TempDir, Orchestrator) {
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
    let services = Arc::new(TestServicesBuilder::new().with_http(http).build());
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
            Ok(rows.next().await?.unwrap().text(0)?)
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
            Ok(rows.next().await?.unwrap().text(0)?)
        })
    })
    .await
    .unwrap()
}

fn token_response() -> HttpResponse {
    HttpResponse {
        status: 201,
        body: serde_json::to_vec(&serde_json::json!({
            "token": "ghs_test",
            "expires_at": "2099-01-01T00:00:00Z"
        }))
        .unwrap(),
    }
}

fn pr_response(state: &str, merged: bool) -> HttpResponse {
    HttpResponse {
        status: 200,
        body: serde_json::to_vec(&serde_json::json!({
            "title": "My PR",
            "body": "A description",
            "state": state,
            "draft": false,
            "mergeable": true,
            "mergeable_state": "clean",
            "additions": 12,
            "deletions": 3,
            "merged": merged,
            "head": { "sha": "abc123" }
        }))
        .unwrap(),
    }
}

fn empty_array_response() -> HttpResponse {
    HttpResponse {
        status: 200,
        body: b"[]".to_vec(),
    }
}

fn check_runs_response() -> HttpResponse {
    HttpResponse {
        status: 200,
        body: serde_json::to_vec(&serde_json::json!({
            "check_runs": [
                { "name": "build", "status": "completed", "conclusion": "success",
                  "html_url": "https://example.com/1", "output": { "summary": null } }
            ]
        }))
        .unwrap(),
    }
}

fn files_response() -> HttpResponse {
    HttpResponse {
        status: 200,
        body: serde_json::to_vec(&serde_json::json!([
            { "filename": "src/lib.rs", "status": "modified", "additions": 10,
              "deletions": 2, "changes": 12, "patch": "@@ -1 +1 @@\n-old\n+new" }
        ]))
        .unwrap(),
    }
}

#[tokio::test]
async fn try_resolve_and_render_are_none_for_non_pr_job() {
    let (_temp, db) = common::migrated_db().await;
    let project = create_project(&db, "NP", "/tmp/none").await;
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
    let project = create_project(&db, "RF", repo.path().to_str().unwrap()).await;
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
    assert_eq!(cache.pr_number, 5);
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
                Ok(rows.next().await?.unwrap().opt_text(0)?)
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
    let project = create_project(&db, "RFS", repo.path().to_str().unwrap()).await;
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
    let project = create_project(&db, "RV", repo.path().to_str().unwrap()).await;
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

    let uri = "cairn://p/RV/1/1/builder/pr";
    let default = actions::render_live_pr_section(&orch, &job, uri, false)
        .await
        .expect("PR artifact should yield a section");
    assert!(default.contains("## Pull Request"));
    assert!(default.contains("PR #5"));
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

#[tokio::test]
async fn close_pr_for_job_marks_closed() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "CL", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("closed", false));
    let (_cfg, orch) = orchestrator_with_http(db, http).await;

    let msg = actions::close_pr_for_job(&orch, &job).await.unwrap();
    assert!(msg.contains("closed"));
    assert_eq!(mr_status(&orch.db.local, &mr).await, "closed");
}

#[tokio::test]
async fn reconcile_after_merge_for_action_run_owner_stores_files_under_parent_job() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "ARF", repo.path().to_str().unwrap()).await;
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
    git.expect_worktree_list()
        .returning(|_| Ok("worktree /tmp/repo\nHEAD abc\nbranch refs/heads/main\n".to_string()));
    let (_cfg, orch) = orchestrator_with_http_and_git(db, http, git).await;
    let ctx = actions::resolve_merge_mr_context_for_job(&orch.db.local, &action_run)
        .await
        .unwrap();

    actions::reconcile_after_merge(orch.clone(), ctx).await;

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

#[tokio::test]
async fn merge_pr_for_job_marks_merged_and_resolves_issue() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "MG", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    let job = create_job(&db, &project, &issue).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to(
            "/pulls/5/merge",
            HttpResponse {
                status: 200,
                body: b"{}".to_vec(),
            },
        )
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/pulls/5/files", files_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("closed", true));
    let (_cfg, orch) = orchestrator_with_http(db, http).await;

    let msg = actions::merge_pr_for_job(&orch, &job, Some("squash".to_string()))
        .await
        .unwrap();
    assert!(msg.contains("merged"));
    assert_eq!(mr_status(&orch.db.local, &mr).await, "merged");
    assert_eq!(issue_status(&orch.db.local, &issue).await, "merged");
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
    let project = create_project(&db, "AD1", "/tmp/ad1").await;
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
    let project = create_project(&db, "AD3", "/tmp/ad3").await;
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
    let project = create_project(&db, "AD4", "/tmp/ad4").await;
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
async fn merge_pr_for_job_advances_producing_execution() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "MGA", repo.path().to_str().unwrap()).await;
    let issue = create_issue(&db, &project, 1).await;
    insert_execution(&db, "exec-mga", &project, &issue).await;
    let job = create_job_with_execution(&db, &project, &issue, Some("exec-mga")).await;
    let mr = create_merge_request(&db, &job, &project, &issue, 5).await;

    let http = MockHttpClient::new()
        .respond_to("access_tokens", token_response())
        .respond_to(
            "/pulls/5/merge",
            HttpResponse {
                status: 200,
                body: b"{}".to_vec(),
            },
        )
        .respond_to("/pulls/5/reviews", empty_array_response())
        .respond_to("/pulls/5/files", files_response())
        .respond_to("/check-runs", check_runs_response())
        .respond_to("/pulls/5", pr_response("closed", true));
    let (_cfg, orch, mut rx) = orchestrator_with_effect_channel(db, http).await;

    actions::merge_pr_for_job(&orch, &job, Some("squash".to_string()))
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

#[tokio::test]
async fn close_pr_for_job_advances_producing_execution() {
    let (_temp, db) = common::migrated_db().await;
    let repo = temp_git_repo();
    insert_github_app(&db).await;
    let project = create_project(&db, "CLA", repo.path().to_str().unwrap()).await;
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

    actions::close_pr_for_job(&orch, &job).await.unwrap();
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
