//! Shared `#[cfg(test)]` fixtures for the `actions` submodule tests: a migrated
//! in-memory DB, a test `Orchestrator`, and merge-request seed helpers.

use crate::db::DbState;
use crate::orchestrator::Orchestrator;
use crate::services::testing::{MockGitClient, TestServicesBuilder};
use crate::storage::{LocalDb, SearchIndex};
use cairn_db::turso::params;
use std::sync::Arc;

pub(super) async fn migrated_db() -> LocalDb {
    crate::storage::migrated_test_db("reconcile-test.db").await
}

pub(super) fn test_orchestrator(db: LocalDb, git: MockGitClient) -> Orchestrator {
    let temp = tempfile::tempdir().unwrap();
    let config_dir = temp.keep();
    let index_path = config_dir.join("search-index.db");
    let db_state = Arc::new(DbState::new(
        Arc::new(db),
        Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
    ));
    let services = Arc::new(TestServicesBuilder::new().with_git(git).build());
    Orchestrator::builder(db_state, services, config_dir).build()
}

/// Seed a project + issue + a `merge_requests` row owned by `owner_id` whose
/// PR merges into `target_branch`. No jobs are seeded, so worktree teardown
/// during reconcile is a no-op (the issue has nothing to tear down).
pub(super) async fn seed_merge_request(
    db: &LocalDb,
    owner_id: &str,
    repo_path: &str,
    target_branch: &str,
) {
    let owner_id = owner_id.to_string();
    let repo_path = repo_path.to_string();
    let target_branch = target_branch.to_string();
    db.write(|conn| {
            let owner_id = owner_id.clone();
            let repo_path = repo_path.clone();
            let target_branch = target_branch.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'PROJ', ?1, 'main', 1, 1)",
                    params![repo_path.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, github_pr_number, github_pr_url)
                     VALUES ('mr-1', ?1, 'proj-1', 'issue-1', 'PR', 'agent/PROJ-2-child', ?2, 'merged', 1, 1, 7, 'https://example.com/pr/7')",
                    params![owner_id.as_str(), target_branch.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

pub(super) async fn seed_local_open_merge_request(db: &LocalDb, owner_id: &str) {
    let owner_id = owner_id.to_string();
    db.write(|conn| {
            let owner_id = owner_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-local', 'default', 'Project', 'PROJ', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-local', 'proj-local', 2, 'Issue', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, body, source_branch, target_branch, status, opened_at, updated_at)
                     VALUES ('mr-local', ?1, 'proj-local', 'issue-local', 'Old title', 'Old body', 'agent/PROJ-2-builder', 'main', 'open', 1, 1)",
                    params![owner_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

pub(super) async fn seed_pr_node_merge_request_for_artifact_job(db: &LocalDb) {
    let snapshot = serde_json::json!({
            "recipe": {
                "id": "recipe-1",
                "name": "Recipe",
                "description": null,
                "trigger": "manual",
                "nodes": [
                    {"id": "builder", "nodeType": "agent", "name": "Builder", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "pr", "nodeType": "pr", "name": "PR", "position": {"x": 1.0, "y": 0.0}}
                ],
                "edges": [
                    {"id": "edge-1", "edgeType": "context", "sourceNodeId": "builder", "sourceHandle": "create-pr", "targetNodeId": "pr", "targetHandle": "create-pr"}
                ]
            },
            "agents": {},
            "skills": {},
            "triggerContext": {"issueId": "issue-pr-node", "projectId": "proj-pr-node", "triggerType": "manual"},
            "createdAt": 1
        })
        .to_string();
    db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-pr-node', 'default', 'Project', 'PROJ', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-pr-node', 'proj-pr-node', 3, 'Issue', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, snapshot, started_at, seq)
                     VALUES ('exec-pr-node', 'recipe-1', 'issue-pr-node', 'proj-pr-node', 'running', ?1, 1, 1)",
                    params![snapshot.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, status, issue_id, project_id, created_at, updated_at)
                     VALUES ('builder-job', 'exec-pr-node', 'builder', 'complete', 'issue-pr-node', 'proj-pr-node', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, created_at, parent_job_id)
                     VALUES ('pr-action-run', 'exec-pr-node', 'pr', 'builtin:pr', 'issue-pr-node', 'proj-pr-node', 'blocked', 2, 'builder-job')",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, body, source_branch, target_branch, status, opened_at, updated_at)
                     VALUES ('mr-pr-node', 'builder-job', 'proj-pr-node', 'issue-pr-node', 'Old title', 'Old body', 'agent/PROJ-3-builder', 'main', 'open', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}
