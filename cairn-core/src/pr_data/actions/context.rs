//! MR context resolution: the `MrContext` / `MergeMrContext` types and the
//! queries that resolve a producing job to its `merge_requests` row.

use crate::models::{ExecutionSnapshot, RecipeNodeType};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use turso::params;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrNodeResolution {
    Merge,
    Close,
}

/// PR context resolved from a producing job's `merge_requests` row.
#[derive(Debug, Clone)]
pub struct MrContext {
    pub mr_id: String,
    pub pr_url: String,
    pub github_pr_number: Option<i32>,
    pub repo_path: String,
    pub job_id: String,
    /// Creation-time fact: the PR was opened without a GitHub remote. Stored, not
    /// inferred from a missing `github_pr_number` (which a remote PR also lacks
    /// during the window between opening it and GitHub returning its number).
    pub is_local: bool,
}

/// `MrContext` plus the issue/branch/project fields a merge needs.
#[derive(Debug, Clone)]
pub struct MergeMrContext {
    pub mr: MrContext,
    pub issue_id: Option<String>,
    pub default_branch: String,
    pub project_id: String,
    /// The branch the PR merges *into* (`merge_requests.target_branch`). For a
    /// normal PR this is the project default; for a Coordinator child PR it is
    /// the integration branch a sibling worktree has checked out.
    pub target_branch: String,
    pub source_branch: String,
    pub title: String,
    pub is_workspace: bool,
    /// Whether the issue owns a memory-triage batch (rows in
    /// `memory_triage_issue_memories`). Drives canon resolution: resolve/revert
    /// the triage batch on merge/close. This is the structural truth — it does
    /// not depend on a label being applied.
    pub has_triage_batch: bool,
}

pub(super) fn db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

/// Query the `merge_requests` row linked to a producing job (joined with its
/// project for the repo path). `None` when the job has no PR yet.
pub async fn query_mr_context_for_job(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Option<MrContext>> {
    let mut rows = conn
        .query(
            "SELECT mr.id, mr.github_pr_url, mr.github_pr_number, p.repo_path, mr.job_id, mr.is_local
             FROM merge_requests mr
             JOIN projects p ON mr.project_id = p.id
             WHERE mr.job_id = ?1
             LIMIT 1",
            params![job_id],
        )
        .await?;

    rows.next()
        .await?
        .map(|row| {
            Ok(MrContext {
                mr_id: row.text(0)?,
                pr_url: row.opt_text(1)?.unwrap_or_default(),
                github_pr_number: row.opt_i64(2)?.map(|value| value as i32),
                repo_path: row.text(3)?,
                job_id: row.text(4)?,
                is_local: row.opt_i64(5)?.unwrap_or(0) != 0,
            })
        })
        .transpose()
}

async fn query_pr_node_mr_context_for_artifact_job(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Option<MrContext>> {
    let mut job_rows = conn
        .query(
            "SELECT recipe_node_id, execution_id FROM jobs WHERE id = ?1 LIMIT 1",
            params![job_id],
        )
        .await?;
    let Some(job_row) = job_rows.next().await? else {
        return Ok(None);
    };
    let Some(source_node_id) = job_row.opt_text(0)? else {
        return Ok(None);
    };
    let Some(execution_id) = job_row.opt_text(1)? else {
        return Ok(None);
    };

    let snapshot_json = conn
        .query(
            "SELECT snapshot FROM executions WHERE id = ?1 LIMIT 1",
            params![execution_id.as_str()],
        )
        .await?
        .next()
        .await?
        .and_then(|row| row.opt_text(0).ok().flatten());
    let Some(snapshot_json) = snapshot_json else {
        return Ok(None);
    };
    let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
        .map_err(|error| DbError::Row(format!("invalid execution snapshot JSON: {error}")))?;

    let pr_target_nodes: Vec<String> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|edge| {
            edge.source_node_id == source_node_id && edge.edge_type.to_string() == "context"
        })
        .filter_map(|edge| {
            snapshot
                .recipe
                .nodes
                .iter()
                .find(|node| node.id == edge.target_node_id)
                .and_then(|node| match node.node_type {
                    RecipeNodeType::Pr => Some(node.id.clone()),
                    RecipeNodeType::Action
                        if node
                            .action_config
                            .as_ref()
                            .map(|config| config.action == "create_pr")
                            .unwrap_or(false) =>
                    {
                        Some(node.id.clone())
                    }
                    _ => None,
                })
        })
        .collect();

    for target_node_id in pr_target_nodes {
        let mut rows = conn
            .query(
                "SELECT mr.id, mr.github_pr_url, mr.github_pr_number, p.repo_path, mr.job_id, mr.is_local
                 FROM action_runs ar
                 JOIN merge_requests mr ON mr.job_id = ar.parent_job_id OR mr.job_id = ar.id
                 JOIN projects p ON mr.project_id = p.id
                 WHERE ar.execution_id = ?1
                   AND ar.recipe_node_id = ?2
                 ORDER BY CASE WHEN mr.job_id = ar.parent_job_id THEN 0 ELSE 1 END,
                          ar.created_at DESC
                 LIMIT 1",
                params![execution_id.as_str(), target_node_id.as_str()],
            )
            .await?;
        if let Some(row) = rows.next().await? {
            return Ok(Some(MrContext {
                mr_id: row.text(0)?,
                pr_url: row.opt_text(1)?.unwrap_or_default(),
                github_pr_number: row.opt_i64(2)?.map(|value| value as i32),
                repo_path: row.text(3)?,
                job_id: row.text(4)?,
                is_local: row.opt_i64(5)?.unwrap_or(0) != 0,
            }));
        }
    }

    Ok(None)
}

pub(super) async fn query_mr_context_for_create_pr_artifact_job(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Option<MrContext>> {
    if let Some(context) = query_mr_context_for_job(conn, job_id).await? {
        return Ok(Some(context));
    }
    query_pr_node_mr_context_for_artifact_job(conn, job_id).await
}

/// Resolve the PR context for a job, erroring if the job has no PR.
pub async fn resolve_mr_context_for_job(db: &LocalDb, job_id: &str) -> Result<MrContext, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            query_mr_context_for_job(conn, &job_id)
                .await?
                .ok_or_else(|| {
                    DbError::internal(format!("No merge request found for job: {job_id}"))
                })
        })
    })
    .await
    .map_err(|e| db_error("Failed to resolve merge request", e))
}

/// Resolve the merge context (issue/branch/project) for a job.
pub async fn resolve_merge_mr_context_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<MergeMrContext, String> {
    let mr = resolve_mr_context_for_job(db, job_id).await?;
    let mr_id = mr.mr_id.clone();

    let (issue_id, default_branch, project_id, target_branch, source_branch, title, is_workspace, has_triage_batch) = db
        .read(|conn| {
            let mr_id = mr_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT mr.issue_id, p.default_branch, mr.project_id, mr.target_branch, mr.source_branch, mr.title,
                                p.is_workspace,
                                EXISTS(
                                    SELECT 1
                                    FROM memory_triage_issue_memories tm
                                    WHERE tm.issue_id = mr.issue_id
                                ) AS has_triage_batch
                         FROM merge_requests mr
                         JOIN projects p ON mr.project_id = p.id
                         WHERE mr.id = ?1
                         LIMIT 1",
                        params![mr_id.as_str()],
                    )
                    .await?;

                let row = rows.next().await?.ok_or_else(|| {
                    DbError::internal(format!("Merge request not found: {mr_id}"))
                })?;

                Ok((
                    row.opt_text(0)?,
                    row.opt_text(1)?.unwrap_or_else(|| "main".to_string()),
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.text(5)?,
                    row.i64(6)? != 0,
                    row.i64(7)? != 0,
                ))
            })
        })
        .await
        .map_err(|e| db_error("Failed to load merge request context", e))?;

    Ok(MergeMrContext {
        mr,
        issue_id,
        default_branch,
        project_id,
        target_branch,
        source_branch,
        title,
        is_workspace,
        has_triage_batch,
    })
}

pub(super) async fn load_mr_issue_id(db: &LocalDb, mr_id: &str) -> Result<Option<String>, String> {
    let mr_id = mr_id.to_string();
    db.query_opt_text(
        "SELECT issue_id FROM merge_requests WHERE id = ?1",
        params![mr_id.as_str()],
    )
    .await
    .map_err(|e| db_error("Failed to load merge request issue", e))
}

/// The PR's source and target jj bookmarks, used to read jj conflict state and
/// to scope the conflicted-commit enumeration to `<target>..<source>`.
pub(super) async fn load_mr_branches(
    db: &LocalDb,
    mr_id: &str,
) -> Result<Option<(String, String)>, String> {
    let mr_id = mr_id.to_string();
    db.query_opt(
        "SELECT source_branch, target_branch FROM merge_requests WHERE id = ?1",
        params![mr_id.as_str()],
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map_err(|e| db_error("Failed to load merge request branches", e))
}

/// Try to resolve PR context for a job; `Ok(None)` when the job has no PR.
///
/// Owner-aware: the direct `merge_requests.job_id = ?` lookup runs first, then a
/// fallback that walks the execution snapshot from this job's node to the `pr`
/// node that opened the row (`query_mr_context_for_create_pr_artifact_job`). A
/// build recipe opens the child PR via a first-class `pr` action node, but the
/// durable row is keyed by the producing builder job so analytics and file-change
/// joins have a real `jobs.id`. The fallback preserves resolution from the
/// builder artifact and from legacy action-run-keyed rows that could not be
/// repaired.
pub async fn try_resolve_mr_context_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<MrContext>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { query_mr_context_for_create_pr_artifact_job(conn, &job_id).await })
    })
    .await
    .map_err(|e| db_error("Failed to resolve merge request", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_data::actions::test_support::{
        migrated_db, seed_merge_request, seed_pr_node_merge_request_for_artifact_job,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_merge_context_loads_pr_target_branch() {
        let db = migrated_db().await;
        seed_merge_request(&db, "owner-job", "/repo", "integration").await;

        let ctx = resolve_merge_mr_context_for_job(&db, "owner-job")
            .await
            .unwrap();

        // The PR's real base — not the project default ("main") — is carried.
        assert_eq!(ctx.target_branch, "integration");
        assert_eq!(ctx.default_branch, "main");
        assert_eq!(ctx.mr.job_id, "owner-job");
    }

    /// The owner-aware resolution the PR actions and the live PR section depend on:
    /// a build recipe's child PR is opened by the `pr` action_run but durably owned
    /// by the producing builder job, so direct lookup succeeds and the snapshot
    /// fallback remains available for legacy action-run-keyed rows.
    #[tokio::test(flavor = "current_thread")]
    async fn try_resolve_mr_context_follows_pr_node_owner() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;

        let ctx = try_resolve_mr_context_for_job(&db, "builder-job")
            .await
            .unwrap()
            .expect("the pr-node-owned MR resolves from the builder job id");
        assert_eq!(ctx.mr_id, "mr-pr-node");
        assert_eq!(
            ctx.job_id, "builder-job",
            "the resolved owner is the persisted builder job"
        );
    }
}
