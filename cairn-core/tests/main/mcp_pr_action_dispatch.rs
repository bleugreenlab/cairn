//! Dispatch-level coverage for the PR action branch on the NodeArtifact patch
//! arm: `change patch {action:"merge|close|refresh"}`. Verifies routing,
//! the no-PR rejection, and action/confirmed mutual exclusion. Exercised in
//! preview (dry-run) mode so no GitHub/git side effects run.

use crate::common;

use std::sync::Arc;

use crate::common::orchestrator;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;
use serde_json::json;

async fn insert_node(db: &LocalDb, project_id: &str, issue_id: &str) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
                 VALUES (?1, ?2, 1, 'Issue', 'active', 'idle', 'none', 1, 1)",
                params![issue_id.as_str(), project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-1', 'recipe', ?1, ?2, 'running', 1, 1)",
                params![issue_id.as_str(), project_id.as_str()],
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
    change_target(orch, "cairn://p/PRA/1/1/builder/pr", payload, preview).await
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
    let project_id = common::create_project(&db, "PRA").await;
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
        "cairn://p/PRA/1/1/pr",
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
}
