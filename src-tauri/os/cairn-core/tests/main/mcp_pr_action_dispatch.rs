//! PR action routing and failed-publication retry regressions. Preview requests
//! cover publication operations; lifecycle fixtures inspect reducer effects
//! against a local branch without executing GitHub publication.

use crate::common;

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use crate::common::orchestrator;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::LocalDb;
use cairn_core::models::{
    ExecutionSnapshot, RecipeNode, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use cairn_workflow::effects::{dag::reduce_dag, types::WorkflowEffect};
use serde_json::json;

fn pr_recipe_nodes() -> Vec<RecipeNode> {
    serde_json::from_value(json!([
        {
            "id": "builder", "nodeType": "agent", "name": "builder",
            "position": {"x": 0, "y": 0}, "agentConfig": {"agentConfigId": "builder"}
        },
        {
            "id": "pr", "nodeType": "pr", "name": "pr",
            "position": {"x": 200, "y": 0},
            "actionConfig": {"inputSchema": {
                "name": "create-pr",
                "schema": {
                    "type": "object", "required": ["title", "body"],
                    "properties": {"title": {"type": "string"}, "body": {"type": "string"}},
                    "additionalProperties": false
                }
            }}
        }
    ]))
    .unwrap()
}

fn pr_snapshot(project_id: &str, issue_id: &str) -> String {
    ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "recipe".into(),
            name: "PR retry".into(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: pr_recipe_nodes(),
            edges: serde_json::from_value(json!([
                {"id": "control", "edgeType": "control", "sourceNodeId": "builder",
                 "sourceHandle": "control-out", "targetNodeId": "pr", "targetHandle": "control-in"},
                {"id": "context", "edgeType": "context", "sourceNodeId": "builder",
                 "sourceHandle": "context-out", "targetNodeId": "pr", "targetHandle": "context-in"}
            ]))
            .unwrap(),
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some(issue_id.into()),
            project_id: project_id.into(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap()
}

async fn insert_node(db: &LocalDb, project_id: &str, issue_id: &str) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    let snapshot = pr_snapshot(&project_id, &issue_id);
    db.write(|conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let snapshot = snapshot.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
                 VALUES (?1, ?2, 1, 'Issue', 'active', 'idle', 'none', 1, 1)",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
                 VALUES ('exec-1', 'recipe', ?1, ?2, 'running', 1, 1, ?3)",
                params![issue_id.as_str(), project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment)
                 VALUES ('job-builder', 'exec-1', 'builder', ?1, ?2, 'builder', 'complete', 1, 1, 'builder')",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_merge_request(db: &LocalDb, project_id: &str, issue_id: &str) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    db.execute(
        "INSERT INTO merge_requests (
            id, job_id, project_id, issue_id, title, source_branch, target_branch,
            status, merge_method, opened_at, updated_at, github_pr_number, github_pr_url
         ) VALUES ('mr-1', 'job-builder', ?1, ?2, 'PR', 'feature', 'main', 'open', 'squash', 1, 1, 7, 'https://github.com/o/r/pull/7')",
        params![project_id.as_str(), issue_id.as_str()],
    )
    .await
    .unwrap();
}

async fn change_target(
    orch: &Orchestrator,
    target: &str,
    payload: serde_json::Value,
    preview: bool,
) -> String {
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: std::env::temp_dir().to_string_lossy().to_string(),
        run_id: None,
        tool: "write".to_string(),
        payload: json!({
            "preview": preview,
            "changes": [{
                "target": target,
                "mode": "patch",
                "payload": payload
            }]
        }),
        tool_use_id: None,
    };
    handle_write(orch, &request).await
}

/// Target the builder node's `pr` artifact (the NodeArtifact patch arm).
async fn change(orch: &Orchestrator, payload: serde_json::Value, preview: bool) -> String {
    change_target(orch, "cairn://p/pra/1/1/builder/pr", payload, preview).await
}

/// Seed a first-class `pr` action node: an action_run with `uri_segment = 'pr'`
/// whose `parent_job_id` is the builder job that owns the `merge_requests` row.
async fn insert_pr_action_run(db: &LocalDb, project_id: &str, issue_id: &str) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    db.execute(
        "INSERT INTO action_runs (
            id, execution_id, recipe_node_id, action_config_id, issue_id, project_id,
            status, created_at, parent_job_id, uri_segment
         ) VALUES ('pr-action-run', 'exec-1', 'pr', 'builtin:pr', ?1, ?2, 'blocked', 2, 'job-builder', 'pr')",
        params![issue_id.as_str(), project_id.as_str()],
    )
    .await
    .unwrap();
}

async fn pr_action_node_fixture() -> (tempfile::TempDir, Arc<LocalDb>, String, String) {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "pra").await;
    let issue_id = uuid::Uuid::new_v4().to_string();
    insert_node(&db, &project_id, &issue_id).await;
    (temp, db, project_id, issue_id)
}

#[tokio::test]
async fn action_on_non_pr_artifact_is_rejected() {
    let (temp, db, _project_id, _issue_id) = pr_action_node_fixture().await;
    // No merge_requests row for this node.
    let orch = orchestrator(&temp, db);

    let result = change(&orch, json!({ "action": "merge" }), false).await;
    assert!(
        result.contains("has no PR yet"),
        "expected no-PR rejection, got: {result}"
    );
}

#[tokio::test]
async fn action_and_confirmed_are_mutually_exclusive() {
    let (temp, db, _project_id, _issue_id) = pr_action_node_fixture().await;
    let orch = orchestrator(&temp, db);

    let result = change(&orch, json!({ "action": "merge", "confirmed": true }), true).await;
    assert!(
        result.contains("mutually exclusive"),
        "expected mutual-exclusion rejection, got: {result}"
    );
}

#[tokio::test]
async fn merge_action_dry_run_routes_to_merge() {
    let (temp, db, project_id, issue_id) = pr_action_node_fixture().await;
    insert_merge_request(&db, &project_id, &issue_id).await;
    let orch = orchestrator(&temp, db);

    let result = change(
        &orch,
        json!({ "action": "merge", "method": "rebase" }),
        true,
    )
    .await;
    assert!(
        result.contains("Would merge PR") && result.contains("method=rebase"),
        "expected merge dry-run summary, got: {result}"
    );
}

/// The reported repro: the bare `pr` node arm (`nodes.rs`) resolves the `pr`
/// node to its action_run, whose `parent_job_id` owns the builder-job-keyed
/// `merge_requests` row. Before the shared resolver walked `parent_job_id`, this
/// returned `has no PR yet`. The builder re-writing its create-pr artifact
/// (title/body only, never `job_id`) is simulated first as a guard that it does
/// not affect resolution.
#[tokio::test]
async fn pr_node_merge_resolves_through_parent_job() {
    let (temp, db, project_id, issue_id) = pr_action_node_fixture().await;
    insert_merge_request(&db, &project_id, &issue_id).await;
    insert_pr_action_run(&db, &project_id, &issue_id).await;

    // Simulate the builder re-writing its create-pr artifact: a title/body edit
    // that never re-keys `job_id`. The pr-node merge must still resolve the row.
    db.execute(
        "UPDATE merge_requests SET title = 'Re-written', body = 'Re-written body' WHERE id = 'mr-1'",
        (),
    )
    .await
    .unwrap();

    let orch = orchestrator(&temp, db);

    let result = change_target(
        &orch,
        "cairn://p/pra/1/1/pr",
        json!({ "action": "merge", "method": "squash" }),
        true,
    )
    .await;
    assert!(
        result.contains("Would merge PR") && !result.contains("has no PR yet"),
        "expected the pr-node merge to resolve the parent-job-owned MR, got: {result}"
    );
}

#[tokio::test]
async fn unknown_action_is_rejected() {
    let (temp, db, project_id, issue_id) = pr_action_node_fixture().await;
    insert_merge_request(&db, &project_id, &issue_id).await;
    let orch = orchestrator(&temp, db);

    let result = change(&orch, json!({ "action": "frobnicate" }), true).await;
    assert!(
        result.contains("unknown PR action"),
        "expected unknown-action rejection, got: {result}"
    );
    assert!(
        result.contains("merge|close|refresh|publish"),
        "the rejection lists every PR action, publish included: {result}"
    );
}

/// `action:"publish"` is a PR action on the artifact surface and on the bare
/// `pr` node surface alike.
#[tokio::test]
async fn publish_action_dry_run_routes_to_publish_on_both_surfaces() {
    let (temp, db, project_id, issue_id) = pr_action_node_fixture().await;
    insert_merge_request(&db, &project_id, &issue_id).await;
    insert_pr_action_run(&db, &project_id, &issue_id).await;
    let orch = orchestrator(&temp, db);

    let artifact = change(&orch, json!({ "action": "publish" }), true).await;
    assert!(
        artifact.contains("Would publish the PR branch"),
        "expected publish dry-run summary on the artifact, got: {artifact}"
    );

    let node = change_target(
        &orch,
        "cairn://p/pra/1/1/pr",
        json!({ "action": "publish" }),
        true,
    )
    .await;
    assert!(
        node.contains("Would publish the PR branch"),
        "expected publish dry-run summary on the pr node, got: {node}"
    );
}

/// The reducer's effects are inspected, never executed: these fixtures prove
/// redispatch without publishing to GitHub or needing a remote repository.
async fn reduce_pr(orch: &Orchestrator) -> Vec<String> {
    let orch = orch.clone();
    tokio::task::spawn_blocking(move || {
        let (jobs, effects) =
            reduce_dag(&cairn_core::internal::workflow_runtime(&orch), "exec-1").unwrap();
        assert!(jobs.is_empty(), "the completed builder must not restart");
        effects
            .into_iter()
            .filter_map(|effect| match effect {
                WorkflowEffect::ExecuteAction {
                    action_run_id,
                    execution_id,
                    node_id,
                    ..
                } => {
                    assert_eq!(execution_id, "exec-1");
                    assert_eq!(node_id, "pr");
                    Some(action_run_id)
                }
                _ => None,
            })
            .collect()
    })
    .await
    .unwrap()
}

async fn write_handoff(orch: &Orchestrator, mode: &str, version: i64) {
    let result = common::change_resource(
        orch,
        json!([{
            "target": "cairn://p/pra/1/1/builder/create-pr",
            "mode": mode,
            "payload": {"title": "Ready to publish", "body": format!("Handoff {version}")}
        }]),
    )
    .await;
    assert!(result.contains(&format!("version {version}")), "{result}");
}

async fn builder_status(db: &LocalDb) -> String {
    db.query_text("SELECT status FROM jobs WHERE id = 'job-builder'", ())
        .await
        .unwrap()
        .unwrap()
}

async fn attempt_count(db: &LocalDb) -> i64 {
    common::query_i64(db, "SELECT COUNT(*) FROM pr_publish_attempts")
        .await
        .unwrap()
}

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

struct RetryFixture {
    temp: tempfile::TempDir,
    db: Arc<LocalDb>,
    orch: Orchestrator,
    project_id: String,
    issue_id: String,
    workspace: std::path::PathBuf,
    action_run_id: String,
}

impl RetryFixture {
    async fn new() -> Self {
        let (temp, db, project_id, issue_id) = pr_action_node_fixture().await;
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        git(&repo, &["config", "user.name", "PR fixture"]);
        git(&repo, &["config", "user.email", "fixture@example.test"]);
        git(&repo, &["commit", "--allow-empty", "-q", "-m", "base"]);
        let workspace = temp.path().join("builder-workspace");
        common::provision_jj_workspace(&common::config_dir(&temp), &repo, &workspace, "feature");
        db.execute(
            "UPDATE projects SET repo_path = ?1 WHERE id = ?2",
            params![repo.to_string_lossy().as_ref(), project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE jobs SET branch = 'feature', base_branch = 'main' WHERE id = 'job-builder'",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
                    VALUES ('builder-session', 'job-builder', 'codex', 'open', 1, 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
                    VALUES ('builder-turn', 'builder-session', 'job-builder', 1, 'complete', 'initial', 1, 2, 2)", ()).await.unwrap();
        let orch = orchestrator(&temp, db.clone());
        write_handoff(&orch, "create", 1).await;
        assert_eq!(builder_status(&db).await, "complete");
        let ids = reduce_pr(&orch).await;
        assert_eq!(ids.len(), 1, "first dispatch creates one PR action");
        let action_run_id = ids[0].clone();
        assert_eq!(
            attempt_count(&db).await,
            1,
            "initial dispatch records its input revision"
        );
        assert!(
            db.query_text(
                "SELECT input_revision FROM pr_publish_attempts WHERE attempt = 1",
                ()
            )
            .await
            .unwrap()
            .is_some(),
            "real branch and artifact must resolve to a revision"
        );
        Self {
            temp,
            db,
            orch,
            project_id,
            issue_id,
            workspace,
            action_run_id,
        }
    }

    async fn fail(&self) {
        let orch = self.orch.clone();
        let id = self.action_run_id.clone();
        tokio::task::spawn_blocking(move || {
            cairn_workflow::advancement::mark_action_run_failed(
                &cairn_core::internal::workflow_runtime(&orch),
                &id,
                "push lease rejected",
            )
            .unwrap();
        })
        .await
        .unwrap();
        assert_eq!(
            self.db
                .query_text(
                    "SELECT outcome || ':' || detail FROM pr_publish_attempts WHERE attempt = 1",
                    ()
                )
                .await
                .unwrap()
                .as_deref(),
            Some("failed:push lease rejected")
        );
    }

    fn repair_head(&self) {
        let jj = cairn_core::internal::jj::environment(
            &self.orch.jj_binary_path,
            &common::config_dir(&self.temp),
        );
        std::fs::write(self.workspace.join("repair.txt"), "repaired branch\n").unwrap();
        cairn_core::internal::jj::seal(&jj, &self.workspace, "repair branch", None).unwrap();
    }

    async fn assert_identity_and_status(&self, status: &str) {
        assert_eq!(
            common::query_i64(&self.db, "SELECT COUNT(*) FROM action_runs")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            common::query_i64(&self.db, "SELECT COUNT(*) FROM executions")
                .await
                .unwrap(),
            1
        );
        let identity = self
            .db
            .query_text(
                "SELECT id || ':' || uri_segment || ':' || status FROM action_runs",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(identity, format!("{}:pr:{status}", self.action_run_id));
    }

    async fn assert_retried_once(&self) {
        // This assertion must fail if reduce_dag's PR rearm step is removed.
        assert_eq!(
            reduce_pr(&self.orch).await,
            vec![self.action_run_id.clone()],
            "a revised failed PR must redispatch its existing action_run"
        );
        self.assert_identity_and_status("pending").await;
        assert_eq!(
            self.db
                .query_text("SELECT status FROM executions WHERE id = 'exec-1'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("running")
        );
        assert_eq!(common::query_i64(&self.db,
            "SELECT COUNT(*) FROM action_runs WHERE error_message IS NULL AND completed_at IS NULL"
        ).await.unwrap(), 1);
        assert_eq!(attempt_count(&self.db).await, 2);
        assert_eq!(
            common::query_i64(
                &self.db,
                "SELECT COUNT(DISTINCT input_revision) FROM pr_publish_attempts"
            )
            .await
            .unwrap(),
            2,
            "the retried attempt journals the new revision"
        );
        assert_eq!(common::query_i64(&self.db,
            "SELECT COUNT(*) FROM pr_publish_attempts WHERE attempt = 2 AND outcome IS NULL AND settled_at IS NULL"
        ).await.unwrap(), 1);
        assert!(
            reduce_pr(&self.orch).await.is_empty(),
            "pending attempt must not dispatch twice"
        );
        assert_eq!(attempt_count(&self.db).await, 2);
    }
}

#[tokio::test]
async fn failed_pr_action_retries_on_a_later_create_pr_handoff() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    fixture.repair_head();
    write_handoff(&fixture.orch, "create", 2).await;
    fixture.assert_retried_once().await;
}

/// The push-lease specimen: the branch already has the right tip; only the
/// fresh artifact version changes after a failed publication.
#[tokio::test]
async fn failed_pr_action_retries_on_artifact_only_revision() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    write_handoff(&fixture.orch, "patch", 2).await;
    fixture.assert_retried_once().await;
}

#[tokio::test]
async fn failed_pr_action_retries_on_head_only_revision() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    fixture.repair_head();
    fixture.assert_retried_once().await;
}

#[tokio::test]
async fn unchanged_pr_failure_stays_terminal_and_does_not_loop() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    for _ in 0..3 {
        assert!(reduce_pr(&fixture.orch).await.is_empty());
        fixture.assert_identity_and_status("failed").await;
        assert_eq!(attempt_count(&fixture.db).await, 1);
    }
}

async fn publish_fixture(fixture: &RetryFixture) {
    insert_merge_request(&fixture.db, &fixture.project_id, &fixture.issue_id).await;
    cairn_core::internal::execution::actions::complete_action_run(
        &fixture.orch,
        &fixture.action_run_id,
        &pr_recipe_nodes()[1],
        Some(json!({"pr_url": "https://github.com/o/r/pull/7"})),
    )
    .await
    .unwrap();
    fixture.assert_identity_and_status("blocked").await;
    assert_eq!(
        fixture
            .db
            .query_text(
                "SELECT outcome || ':' || detail FROM pr_publish_attempts WHERE attempt = 1",
                ()
            )
            .await
            .unwrap()
            .as_deref(),
        Some("published:https://github.com/o/r/pull/7")
    );
}

#[tokio::test]
async fn published_pr_action_is_not_retried_by_a_later_handoff() {
    let fixture = RetryFixture::new().await;
    publish_fixture(&fixture).await;
    fixture.repair_head();
    // Seed the later authoritative version directly: a write against a bound
    // remote PR also edits GitHub metadata, outside this redispatch fixture.
    fixture.db.execute(
        "INSERT INTO artifacts(id,job_id,artifact_type,schema_version,data,version,parent_version_id,output_name,confirmed,created_at,updated_at)
         SELECT 'later-handoff',job_id,artifact_type,schema_version,'{\"title\":\"Revised\",\"body\":\"Later handoff\"}',2,id,output_name,1,3,3
         FROM artifacts WHERE job_id='job-builder' AND artifact_type='create-pr' AND version=1",
        (),
    ).await.unwrap();
    assert!(reduce_pr(&fixture.orch).await.is_empty());
    fixture.assert_identity_and_status("blocked").await;
    assert_eq!(attempt_count(&fixture.db).await, 1);
    assert_eq!(
        common::query_i64(&fixture.db, "SELECT COUNT(*) FROM merge_requests")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn retry_action_is_refused_on_a_published_pr_node() {
    let fixture = RetryFixture::new().await;
    publish_fixture(&fixture).await;
    for preview in [true, false] {
        let result = change_target(
            &fixture.orch,
            "cairn://p/pra/1/1/pr",
            json!({"action": "retry"}),
            preview,
        )
        .await;
        assert!(
            result.contains("refresh"),
            "published retry must name refresh: {result}"
        );
        fixture.assert_identity_and_status("blocked").await;
        assert_eq!(attempt_count(&fixture.db).await, 1);
        assert!(reduce_pr(&fixture.orch).await.is_empty());
    }
}

#[tokio::test]
async fn terminal_artifact_write_enqueues_advance_dag_when_builder_already_complete() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    for (mode, version) in [("create", 2), ("patch", 3)] {
        // Mark earlier notifications consumed, so this write must independently
        // enqueue rather than borrowing an outbox entry from initial completion.
        fixture
            .db
            .execute(
                "UPDATE effect_outbox SET state = 'done' WHERE kind = 'advance_dag'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(builder_status(&fixture.db).await, "complete");
        write_handoff(&fixture.orch, mode, version).await;
        assert_eq!(builder_status(&fixture.db).await, "complete");
        assert_eq!(common::query_i64(&fixture.db,
            "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag' AND dedupe_key = 'exec-1' AND state = 'pending'"
        ).await.unwrap(), 1, "{mode} must enqueue even without a builder status transition");
    }
}

#[tokio::test]
async fn failed_pr_action_renders_attempt_cause_and_revision_eligibility() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    let before = common::read_resource(&fixture.orch, "cairn://p/pra/1/1/pr").await;
    for expected in [
        "Publication attempts",
        "failed",
        "push lease rejected",
        "waiting on a new Create-PR handoff",
        "action:\"retry\"",
    ] {
        assert!(before.contains(expected), "missing {expected:?}: {before}");
    }
    write_handoff(&fixture.orch, "patch", 2).await;
    let after = common::read_resource(&fixture.orch, "cairn://p/pra/1/1/pr").await;
    assert!(after.contains("retry-eligible"), "{after}");
    // Reading the explanation is not a retry and must not mutate the journal.
    fixture.assert_identity_and_status("failed").await;
    assert_eq!(attempt_count(&fixture.db).await, 1);
}

#[tokio::test]
async fn failed_pr_action_explicit_retry_preserves_history_without_revision_change() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    let preview = change_target(
        &fixture.orch,
        "cairn://p/pra/1/1/pr",
        json!({"action": "retry"}),
        true,
    )
    .await;
    assert!(preview.contains("Would retry PR publication"), "{preview}");
    fixture.assert_identity_and_status("failed").await;
    assert_eq!(attempt_count(&fixture.db).await, 1);

    let result = change_target(
        &fixture.orch,
        "cairn://p/pra/1/1/pr",
        json!({"action": "retry"}),
        false,
    )
    .await;
    assert!(result.contains("Queued PR publication retry"), "{result}");
    fixture.assert_identity_and_status("pending").await;
    assert_eq!(attempt_count(&fixture.db).await, 2);
    assert_eq!(
        common::query_i64(
            &fixture.db,
            "SELECT COUNT(DISTINCT input_revision) FROM pr_publish_attempts"
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(common::query_i64(&fixture.db,
        "SELECT COUNT(*) FROM pr_publish_attempts WHERE cycle = 1 AND outcome = 'failed' AND detail = 'push lease rejected'").await.unwrap(), 1);
    assert_eq!(common::query_i64(&fixture.db,
        "SELECT COUNT(*) FROM pr_publish_attempts WHERE cycle = 2 AND attempt = 1 AND outcome IS NULL").await.unwrap(), 1);
    let payload = fixture.db.query_text(
        "SELECT payload_json FROM effect_outbox WHERE kind = 'pr_publish' AND dedupe_key = (SELECT id FROM pr_publish_attempts WHERE cycle = 2)", ()
    ).await.unwrap().expect("manual retry must persist a dispatch intent");
    let effect = cairn_workflow::pr_publish_attempts::replay_dispatch(
        &cairn_core::internal::workflow_runtime(&fixture.orch),
        &payload,
        i64::MAX,
    )
    .await
    .unwrap()
    .expect("a later host boot must replay the queued retry without GitHub");
    assert!(
        matches!(effect, WorkflowEffect::ExecuteAction { action_run_id, .. }
        if action_run_id == fixture.action_run_id)
    );
}

#[tokio::test]
async fn retry_action_is_refused_on_a_non_pr_action_node() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    fixture
        .db
        .execute(
            "UPDATE action_runs SET action_config_id = 'builtin:shell'",
            (),
        )
        .await
        .unwrap();
    for preview in [true, false] {
        let result = change_target(
            &fixture.orch,
            "cairn://p/pra/1/1/pr",
            json!({"action": "retry"}),
            preview,
        )
        .await;
        assert!(result.contains("Only a PR action node"), "{result}");
        fixture.assert_identity_and_status("failed").await;
        assert_eq!(attempt_count(&fixture.db).await, 1);
    }
}

#[tokio::test]
async fn legacy_failed_pr_action_requires_one_explicit_retry_to_establish_baseline() {
    let fixture = RetryFixture::new().await;
    fixture.fail().await;
    // Represent a failed action persisted before the attempt journal existed.
    fixture
        .db
        .execute("DELETE FROM pr_publish_attempts", ())
        .await
        .unwrap();
    write_handoff(&fixture.orch, "patch", 2).await;
    fixture.repair_head();
    for _ in 0..3 {
        assert!(reduce_pr(&fixture.orch).await.is_empty());
        fixture.assert_identity_and_status("failed").await;
        assert_eq!(attempt_count(&fixture.db).await, 0);
    }
    let rendered = common::read_resource(&fixture.orch, "cairn://p/pra/1/1/pr").await;
    assert!(
        rendered.contains("no attempt history from before publication journaling"),
        "{rendered}"
    );
    assert!(
        rendered.contains("Use action:\"retry\" once to establish a baseline."),
        "{rendered}"
    );
    let result = change_target(
        &fixture.orch,
        "cairn://p/pra/1/1/pr",
        json!({"action": "retry"}),
        false,
    )
    .await;
    assert!(result.contains("Queued PR publication retry"), "{result}");
    fixture.assert_identity_and_status("pending").await;
    assert_eq!(attempt_count(&fixture.db).await, 1);
    assert_eq!(common::query_i64(&fixture.db,
        "SELECT COUNT(*) FROM pr_publish_attempts WHERE cycle=1 AND attempt=1 AND input_revision IS NOT NULL").await.unwrap(), 1);
    fixture.fail().await;
    assert!(reduce_pr(&fixture.orch).await.is_empty());
    write_handoff(&fixture.orch, "patch", 3).await;
    fixture.assert_retried_once().await;
}
