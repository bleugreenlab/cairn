//! Host-agnostic PR lifecycle actions (merge / close / refresh), keyed by
//! `job_id`.
//!
//! The node-level `/pr` artifact URI resolves to a job; a `merge_requests` row
//! linked to that job *is* the PR. These functions are the single
//! implementation of merge/close/refresh, called both from the desktop Tauri
//! commands (via the action_run -> job_id indirection) and directly from the
//! `write` dispatcher when a driver patches the `/pr` artifact with a reserved
//! `action` key.

use crate::execution::teardown::{teardown_worktrees, TeardownScope};
use crate::github::api;
use crate::github::credentials::{get_credentials_for_owner, get_owner_repo};
use crate::models::{
    Check, CheckState, ExecutionSnapshot, MergeableState, PrCache, PrState, RecipeNodeType,
};
use crate::orchestrator::Orchestrator;
use crate::pr_data::helpers::{
    compute_checks_status, compute_local_mergeable, fast_forward_active_worktree,
    fetch_checks_via_api, fetch_pr_via_api, local_merge, local_pr_files,
    update_main_repo_after_merge, ParsedPrDetails,
};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use crate::transitions::Resolution;
use std::path::Path;
use turso::params;
use uuid::Uuid;

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
    /// `memory_triage_issue_memories`). Drives canon merge handling: force the
    /// `merge` method and resolve/revert the triage batch on merge/close. This is
    /// the structural truth — it does not depend on a label being applied.
    pub has_triage_batch: bool,
}

fn db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

fn effective_merge_method(merge_context: &MergeMrContext, merge_method: Option<String>) -> String {
    let force_merge = merge_context.is_workspace || merge_context.has_triage_batch;
    if force_merge {
        "merge".to_string()
    } else {
        merge_method.unwrap_or_else(|| "squash".to_string())
    }
}

/// Query the `merge_requests` row linked to a job (joined with its project for
/// the repo path). `None` when the job has no PR yet.
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
                 JOIN merge_requests mr ON mr.job_id = ar.id
                 JOIN projects p ON mr.project_id = p.id
                 WHERE ar.execution_id = ?1
                   AND ar.recipe_node_id = ?2
                 ORDER BY ar.created_at DESC
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

async fn query_mr_context_for_create_pr_artifact_job(
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

async fn load_mr_issue_id(db: &LocalDb, mr_id: &str) -> Result<Option<String>, String> {
    let mr_id = mr_id.to_string();
    db.query_opt_text(
        "SELECT issue_id FROM merge_requests WHERE id = ?1",
        params![mr_id.as_str()],
    )
    .await
    .map_err(|e| db_error("Failed to load merge request issue", e))
}

async fn update_merge_request_github_cache(
    db: &LocalDb,
    mr_id: &str,
    pr_details: &ParsedPrDetails,
    checks: &[Check],
    checks_status: &Option<crate::models::ChecksStatus>,
    now: i64,
) -> Result<(), String> {
    let mr_id = mr_id.to_string();
    let title = pr_details.title.clone();
    let body = pr_details.body.clone();
    let additions = pr_details.additions;
    let deletions = pr_details.deletions;
    let checks_json = serde_json::to_string(checks).unwrap_or_default();
    let state = pr_details.state.to_string();
    let review_decision = pr_details
        .review_decision
        .as_ref()
        .map(|decision| decision.to_string());
    let mergeable = pr_details.mergeable.to_string();
    let checks_status = checks_status.as_ref().map(|status| status.to_string());

    db.write(|conn| {
        let mr_id = mr_id.clone();
        let title = title.clone();
        let body = body.clone();
        let checks_json = checks_json.clone();
        let state = state.clone();
        let review_decision = review_decision.clone();
        let mergeable = mergeable.clone();
        let checks_status = checks_status.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET title = ?1, body = ?2, additions = ?3, deletions = ?4,
                     checks_status = ?5, checks_json = ?6, github_state = ?7,
                     github_review = ?8, github_mergeable = ?9,
                     github_fetched_at = ?10, updated_at = ?10
                 WHERE id = ?11",
                params![
                    title.as_deref().unwrap_or("Untitled"),
                    body.as_deref(),
                    additions,
                    deletions,
                    checks_status.as_deref(),
                    checks_json.as_str(),
                    state.as_str(),
                    review_decision.as_deref(),
                    mergeable.as_str(),
                    now,
                    mr_id.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request", e))
}

async fn mark_merge_request_closed_and_resolve_issue(
    db: &LocalDb,
    mr_id: &str,
    issue_id: Option<&str>,
    now: i64,
) -> Result<Vec<String>, String> {
    let mr_id = mr_id.to_string();
    db.write(|conn| {
        let mr_id = mr_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET status = 'closed', closed_at = ?1, updated_at = ?1
                 WHERE id = ?2",
                params![now, mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request", e))?;

    let Some(issue_id) = issue_id else {
        return Ok(Vec::new());
    };
    crate::issues::crud::resolve(
        db,
        &crate::services::RealClock,
        issue_id,
        Resolution::Closed,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Resolve a `pr` node by its owner id (`merge_requests.job_id`): the producing
/// `pr` action_run (CAIRN-1220) or a legacy `create_pr` producing job.
pub async fn resolve_pr_node(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
) -> Result<Vec<String>, String> {
    let merge_context = resolve_merge_mr_context_for_job(&orch.db.local, owner_id).await?;
    let mr_id = merge_context.mr.mr_id.clone();
    let now = chrono::Utc::now().timestamp();
    let closed_sessions = match resolution {
        PrNodeResolution::Merge => {
            mark_merge_request_merged_and_resolve_issue(
                &orch.db.local,
                &mr_id,
                merge_context.issue_id.as_deref(),
                None,
                now,
            )
            .await?
        }
        PrNodeResolution::Close => {
            mark_merge_request_closed_and_resolve_issue(
                &orch.db.local,
                &mr_id,
                merge_context.issue_id.as_deref(),
                now,
            )
            .await?
        }
    };

    if merge_context.has_triage_batch {
        if let Some(issue_id) = merge_context.issue_id.as_deref() {
            let result = match resolution {
                PrNodeResolution::Merge => {
                    crate::memories::db::resolve_triage_batch_on_merge(&orch.db.local, issue_id)
                        .await
                }
                PrNodeResolution::Close => {
                    crate::memories::db::revert_triage_batch_on_close(&orch.db.local, issue_id)
                        .await
                }
            };
            match result {
                Ok(ids) if !ids.is_empty() => log::info!(
                    "Resolved {} canon memory triage row(s) for issue {} via {:?}",
                    ids.len(),
                    issue_id,
                    resolution
                ),
                Ok(_) => {}
                Err(error) => log::warn!(
                    "Memory triage {:?} reconciliation failed for issue {}: {}",
                    resolution,
                    issue_id,
                    error
                ),
            }
        }
    }

    if let Some(issue_id) = merge_context.issue_id.as_deref() {
        crate::execution::advancement::release_dependent_executions(orch, issue_id).await?;
    }

    let port = match resolution {
        PrNodeResolution::Merge => "merge",
        PrNodeResolution::Close => "close",
    };
    crate::pr_data::ports::fire_pr_node_port_for_owner(&orch.db.local, owner_id, port).await?;
    // A first-class `pr` action_run was `Blocked` while the PR was open;
    // resolution completes it so the execution can finish. Recompute the whole
    // execution to clear the `NeedsApproval` attention and settle status. Legacy
    // `create_pr` PRs own a job (no action_run to flip); a single-job recompute
    // covers that path. See CAIRN-1220.
    match complete_pr_action_run_if_owner(&orch.db.local, owner_id, now).await? {
        Some(execution_id) => {
            crate::execution::advancement::recompute_execution_jobs(orch, &execution_id)?
        }
        None => crate::execution::advancement::recompute_job(orch, owner_id)?,
    }
    if let Err(e) = advance_producing_execution_after_pr_resolution(orch, &mr_id).await {
        log::warn!(
            "Failed to advance producing execution after PR resolution: {}",
            e
        );
    }

    if matches!(resolution, PrNodeResolution::Merge) {
        if let Err(e) =
            crate::orchestrator::base_advance::notify_downstream_of_base_advance(orch, owner_id)
                .await
        {
            log::warn!("Failed to notify downstream jobs of base advance: {}", e);
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );
    if merge_context.issue_id.is_some() {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "issues", "action": "update"}),
        );
    }

    Ok(closed_sessions)
}

/// If `owner_id` is a `pr` action_run, mark it `complete` and return its
/// execution_id. Returns `None` when the owner is a job (legacy `create_pr`),
/// leaving its resolution to the job projection.
async fn complete_pr_action_run_if_owner(
    db: &LocalDb,
    owner_id: &str,
    now: i64,
) -> Result<Option<String>, String> {
    let owner_id = owner_id.to_string();
    db.write(|conn| {
        let owner_id = owner_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT execution_id FROM action_runs WHERE id = ?1 LIMIT 1",
                    params![owner_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let execution_id = row.text(0)?;
            drop(rows);
            conn.execute(
                "UPDATE action_runs SET status = 'complete', completed_at = ?1 WHERE id = ?2",
                params![now, owner_id.as_str()],
            )
            .await?;
            Ok(Some(execution_id))
        })
    })
    .await
    .map_err(|e| db_error("Failed to complete PR action_run", e))
}

/// After a `merge_requests` row transitions to merged or closed, enqueue an
/// `AdvanceDag` for the execution that produced the PR so any downstream node
/// gated on PR-merged-ness wakes. No-op when the row has no linked job or the
/// job has no `execution_id` (manually-attached or non-recipe PRs).
///
/// Idempotency: callers may invoke this more than once for the same `mr_id`
/// without harm. Each call enqueues a fresh outbox entry and sends a follow-on
/// `AdvanceDag`; `reduce_dag` is idempotent, so a duplicate advancement is
/// cheap and strictly safer than dropping one.
///
/// Errors are swallowed at the call sites — webhook handlers must not return
/// errors that GitHub will retry, and the durable outbox row is the recovery
/// mechanism if the in-memory send is lost.
pub async fn advance_producing_execution_after_pr_resolution(
    orch: &Orchestrator,
    mr_id: &str,
) -> Result<(), String> {
    let mr_id_owned = mr_id.to_string();
    let execution_id = match orch
        .db
        .local
        .query_opt_text(
            // The PR's owner (`merge_requests.job_id`) is either a producing
            // job (legacy `create_pr`) or a producing `pr` action_run
            // (CAIRN-1220); resolve the execution from whichever it is.
            "SELECT COALESCE(j.execution_id, ar.execution_id)
             FROM merge_requests mr
             LEFT JOIN jobs j ON mr.job_id = j.id
             LEFT JOIN action_runs ar ON mr.job_id = ar.id
             WHERE mr.id = ?1
             LIMIT 1",
            params![mr_id_owned.as_str()],
        )
        .await
    {
        Ok(value) => value,
        Err(e) => {
            log::warn!(
                "Failed to look up producing execution for mr {}: {}",
                mr_id,
                e
            );
            return Ok(());
        }
    };

    let Some(execution_id) = execution_id else {
        log::debug!(
            "No producing execution for mr {} — skipping DAG advance",
            mr_id
        );
        return Ok(());
    };

    match crate::effects::outbox::insert_pending_with_payload_async(
        &orch.db.local,
        "advance_dag",
        &execution_id,
        "{}",
    )
    .await
    {
        Ok(entry_id) => {
            if let Some(ref tx) = orch.effect_tx {
                let _ = tx.send(crate::effects::types::WorkflowEffect::AdvanceDag {
                    execution_id: execution_id.clone(),
                    outbox_entry_id: Some(entry_id),
                });
            } else {
                log::debug!(
                    "No effect_tx configured — relying on outbox replay for advance_dag of execution {}",
                    &execution_id[..execution_id.len().min(8)]
                );
            }
        }
        Err(e) => log::warn!(
            "Failed to enqueue advance_dag outbox entry for execution {}: {}",
            execution_id,
            e
        ),
    }

    Ok(())
}

async fn persist_merged_commit(
    db: &LocalDb,
    mr_id: &str,
    merged_commit: &str,
) -> Result<(), String> {
    let mr_id = mr_id.to_string();
    let merged_commit = merged_commit.to_string();
    db.write(|conn| {
        let mr_id = mr_id.clone();
        let merged_commit = merged_commit.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests SET merged_commit = ?1 WHERE id = ?2",
                params![merged_commit.as_str(), mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to persist merged commit", e))
}

async fn mark_merge_request_merged_and_resolve_issue(
    db: &LocalDb,
    mr_id: &str,
    issue_id: Option<&str>,
    merged_commit: Option<&str>,
    now: i64,
) -> Result<Vec<String>, String> {
    let mr_id = mr_id.to_string();
    let _source_branch = db
        .read(|conn| {
            let mr_id = mr_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT source_branch FROM merge_requests WHERE id = ?1 LIMIT 1",
                        params![mr_id.as_str()],
                    )
                    .await?;
                crate::storage::next_text(&mut rows, 0).await
            })
        })
        .await
        .map_err(|e| db_error("Failed to query merge request source branch", e))?;

    db.write(|conn| {
        let mr_id = mr_id.clone();
        let merged_commit = merged_commit.map(ToOwned::to_owned);
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET status = 'merged', merged_at = ?1, updated_at = ?1,
                     merged_commit = COALESCE(?2, merged_commit)
                 WHERE id = ?3",
                params![now, merged_commit.as_deref(), mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request", e))?;

    let Some(issue_id) = issue_id else {
        return Ok(Vec::new());
    };
    crate::issues::crud::resolve(
        db,
        &crate::services::RealClock,
        issue_id,
        Resolution::Merged,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Resolve a PR owner id to the `jobs.id` used by `file_changes.job_id`.
async fn resolve_file_change_job_id(
    db: &LocalDb,
    owner_id: &str,
) -> Result<Option<String>, String> {
    let owner_id = owner_id.to_string();
    db.query_opt_text(
        "SELECT id
         FROM (
             SELECT j.id AS id, 0 AS priority
             FROM jobs j
             WHERE j.id = ?1
             UNION ALL
             SELECT ar.parent_job_id AS id, 1 AS priority
             FROM action_runs ar
             JOIN jobs parent ON parent.id = ar.parent_job_id
             WHERE ar.id = ?1 AND ar.parent_job_id IS NOT NULL
         )
         ORDER BY priority
         LIMIT 1",
        params![owner_id.as_str()],
    )
    .await
    .map_err(|e| db_error("Failed to resolve file-change job owner", e))
}

/// Store file changes from a merged PR for issue history tracking.
async fn store_file_changes(
    orch: &Orchestrator,
    job_id: &str,
    files: &[api::PrFile],
) -> Result<(), String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    let files = files.to_vec();
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let job_id = job_id.clone();
        let files = files.clone();
        Box::pin(async move {
            for file in files {
                let mut rows = conn
                    .query(
                        "SELECT id
                         FROM file_changes
                         WHERE job_id = ?1 AND file_path = ?2
                         LIMIT 1",
                        params![job_id.as_str(), file.filename.as_str()],
                    )
                    .await?;
                let existing_id = crate::storage::next_text(&mut rows, 0).await?;
                drop(rows);

                if let Some(existing_id) = existing_id {
                    conn.execute(
                        "UPDATE file_changes
                         SET status = ?1, additions = ?2, deletions = ?3, previous_path = ?4
                         WHERE id = ?5",
                        params![
                            file.status.as_str(),
                            file.additions,
                            file.deletions,
                            file.previous_filename.as_deref(),
                            existing_id.as_str()
                        ],
                    )
                    .await?;
                } else {
                    let id = Uuid::new_v4().to_string();
                    conn.execute(
                        "INSERT INTO file_changes (
                            id, job_id, file_path, status, additions, deletions,
                            previous_path, created_at
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            id.as_str(),
                            job_id.as_str(),
                            file.filename.as_str(),
                            file.status.as_str(),
                            file.additions,
                            file.deletions,
                            file.previous_filename.as_deref(),
                            now
                        ],
                    )
                    .await?;
                }
            }

            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to store file changes", e))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "file_changes", "action": "insert"}),
    );

    Ok(())
}

/// Resolve `pull_on_merge` from workspace settings via the core config loader.
fn pull_on_merge_setting() -> bool {
    crate::config::get_config_dir()
        .map(|dir| crate::config::settings::load_settings(&dir).pull_on_merge)
        .unwrap_or(true)
}

/// Update the local merge-request prose cache after a create-pr artifact edit
/// has become the PR's source of truth.
async fn update_merge_request_title_body(
    db: &LocalDb,
    mr_id: &str,
    title: &str,
    body: Option<&str>,
    now: i64,
) -> Result<(), String> {
    let mr_id = mr_id.to_string();
    let title = title.to_string();
    let body = body.map(ToOwned::to_owned);
    db.write(|conn| {
        let mr_id = mr_id.clone();
        let title = title.clone();
        let body = body.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET title = ?1, body = ?2, updated_at = ?3
                 WHERE id = ?4",
                params![title.as_str(), body.as_deref(), now, mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request title/body", e))
}

fn gh_pr_edit_args<'a>(pr_number: &'a str, title: &'a str, body: &'a str) -> [&'a str; 7] {
    ["pr", "edit", pr_number, "--title", title, "--body", body]
}

async fn edit_github_pr_title_body(
    repo_path: &str,
    pr_number: i32,
    title: &str,
    body: Option<&str>,
) -> Result<(), String> {
    let pr_number = pr_number.to_string();
    let body = body.unwrap_or("");
    let output = crate::env::gh()
        .args(gh_pr_edit_args(&pr_number, title, body))
        .current_dir(repo_path)
        .output()
        .map_err(|e| format!("Failed to run gh pr edit: {e}"))?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "Failed to update GitHub PR #{pr_number}: {}",
        String::from_utf8_lossy(&output.stderr)
    ))
}

/// Sync edited `create-pr` artifact prose into the already-open PR it produced.
///
/// A builder can rewrite its `create-pr` artifact after a PR node/action has
/// opened the live PR. In that case the artifact is the source of truth the
/// reviewer should see, so update both GitHub and the local `merge_requests`
/// cache instead of leaving PR resources stale.
pub(crate) async fn sync_create_pr_artifact_for_job(
    orch: &Orchestrator,
    job_id: &str,
    title: &str,
    body: Option<&str>,
) -> Result<bool, String> {
    let job_id = job_id.to_string();
    let Some(mr_context) = orch
        .db
        .local
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(
                async move { query_mr_context_for_create_pr_artifact_job(conn, &job_id).await },
            )
        })
        .await
        .map_err(|e| db_error("Failed to resolve merge request", e))?
    else {
        return Ok(false);
    };

    if let Some(pr_number) = mr_context.github_pr_number {
        edit_github_pr_title_body(&mr_context.repo_path, pr_number, title, body).await?;
    }

    let now = chrono::Utc::now().timestamp();
    update_merge_request_title_body(&orch.db.local, &mr_context.mr_id, title, body, now).await?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );
    Ok(true)
}

pub async fn refresh_pr_for_job(orch: &Orchestrator, job_id: &str) -> Result<PrCache, String> {
    let mr_context = resolve_mr_context_for_job(&orch.db.local, job_id).await?;
    let mr_id = mr_context.mr_id.clone();
    let pr_url = mr_context.pr_url.clone();
    let repo_path = mr_context.repo_path.clone();

    if mr_context.github_pr_number.is_none() {
        return refresh_local_pr_for_job(orch, job_id, &mr_context).await;
    }
    let pr_number = mr_context.github_pr_number.expect("checked above");

    let (owner, repo) = get_owner_repo(&repo_path)?;
    let creds = get_credentials_for_owner(&orch.db.local, &owner).await?;

    let http = &*orch.services.http;
    let pr_details = fetch_pr_via_api(http, &creds, &owner, &repo, pr_number).await?;
    let checks = fetch_checks_via_api(http, &creds, &owner, &repo, &pr_details.head_sha)
        .await
        .unwrap_or_default();
    let checks_status = compute_checks_status(&checks);

    let now = chrono::Utc::now().timestamp();

    let pr_cache_result = PrCache {
        id: mr_id.clone(),
        job_id: None,
        pr_number,
        pr_url: pr_url.clone(),
        title: pr_details.title.clone(),
        body: pr_details.body.clone(),
        state: pr_details.state.clone(),
        is_draft: pr_details.is_draft,
        review_decision: pr_details.review_decision.clone(),
        mergeable: pr_details.mergeable.clone(),
        additions: pr_details.additions,
        deletions: pr_details.deletions,
        checks_status: checks_status.clone(),
        checks: checks.clone(),
        fetched_at: now,
        updated_at: now,
        is_local: mr_context.is_local,
        source_branch: None,
        target_branch: None,
    };

    update_merge_request_github_cache(
        &orch.db.local,
        &mr_id,
        &pr_details,
        &checks,
        &checks_status,
        now,
    )
    .await?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );

    Ok(pr_cache_result)
}

async fn refresh_local_pr_for_job(
    orch: &Orchestrator,
    job_id: &str,
    mr_context: &MrContext,
) -> Result<PrCache, String> {
    let mr_id = mr_context.mr_id.clone();
    let repo_path = mr_context.repo_path.clone();
    let (title, body, status, source_branch, target_branch, additions, deletions, updated_at) = orch
        .db
        .local
        .read(|conn| {
            let mr_id = mr_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT title, body, status, source_branch, target_branch, additions, deletions, updated_at
                         FROM merge_requests WHERE id = ?1 LIMIT 1",
                        params![mr_id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| DbError::internal("merge request not found"))?;
                Ok((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.opt_i64(5)?.map(|v| v as i32),
                    row.opt_i64(6)?.map(|v| v as i32),
                    row.i64(7)?,
                ))
            })
        })
        .await
        .map_err(|e| db_error("Failed to load local PR", e))?;

    let git = &*orch.services.git;
    let local_files = if status == "open" {
        local_pr_files(git, Path::new(&repo_path), &target_branch, &source_branch)
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let additions = additions.or_else(|| Some(local_files.iter().map(|file| file.additions).sum()));
    let deletions = deletions.or_else(|| Some(local_files.iter().map(|file| file.deletions).sum()));
    let mergeable = if status == "open" {
        compute_local_mergeable(git, Path::new(&repo_path), &target_branch, &source_branch)
    } else {
        MergeableState::Unknown
    };
    let now = chrono::Utc::now().timestamp();
    let mergeable_str = mergeable.to_string();
    orch.db
        .local
        .write(|conn| {
            let mr_id = mr_id.clone();
            let mergeable_str = mergeable_str.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE merge_requests
                     SET github_mergeable = ?1, github_fetched_at = ?2, updated_at = ?2,
                         additions = COALESCE(additions, ?3), deletions = COALESCE(deletions, ?4)
                     WHERE id = ?5",
                    params![
                        mergeable_str.as_str(),
                        now,
                        additions,
                        deletions,
                        mr_id.as_str()
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| db_error("Failed to update local PR cache", e))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );

    Ok(PrCache {
        id: mr_id,
        job_id: Some(job_id.to_string()),
        pr_number: 0,
        pr_url: String::new(),
        title,
        body,
        state: match status.as_str() {
            "merged" => PrState::Merged,
            "closed" => PrState::Closed,
            _ => PrState::Open,
        },
        is_draft: false,
        review_decision: None,
        mergeable,
        additions,
        deletions,
        checks_status: None,
        checks: Vec::new(),
        fetched_at: now,
        updated_at,
        is_local: mr_context.is_local,
        source_branch: Some(source_branch),
        target_branch: Some(target_branch),
    })
}

/// Close a PR without merging, mark the `merge_requests` row closed, and tear
/// down the issue's worktrees.
pub async fn close_pr_for_job(orch: &Orchestrator, job_id: &str) -> Result<String, String> {
    let mr_context = resolve_mr_context_for_job(&orch.db.local, job_id).await?;
    let mr_id = mr_context.mr_id.clone();
    let repo_path = mr_context.repo_path.clone();

    if let Some(pr_number) = mr_context.github_pr_number {
        let (owner, repo) = get_owner_repo(&repo_path)?;
        let creds = get_credentials_for_owner(&orch.db.local, &owner).await?;

        let http = &*orch.services.http;
        api::close_pr(http, &creds, &owner, &repo, pr_number).await?;
    }

    let closed_sessions = resolve_pr_node(orch, job_id, PrNodeResolution::Close).await?;
    for session_id in &closed_sessions {
        orch.process_state.remove_by_session(session_id);
    }

    // Tear down worktrees and branches for the issue (issue-wide, unconditional).
    if let Some(issue_id) = load_mr_issue_id(&orch.db.local, &mr_id).await? {
        let orch_inner = orch.clone();
        tokio::spawn(async move {
            if let Err(e) = teardown_worktrees(&orch_inner, TeardownScope::Issue(issue_id)).await {
                log::warn!("Worktree teardown after PR close failed: {}", e);
            }
        });
    }

    // Refresh PR details to get closed state.
    let _ = refresh_pr_for_job(orch, job_id).await;

    Ok("PR closed successfully".to_string())
}

/// Try to resolve PR context for a job; `Ok(None)` when the job has no PR.
pub async fn try_resolve_mr_context_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<MrContext>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { query_mr_context_for_job(conn, &job_id).await })
    })
    .await
    .map_err(|e| db_error("Failed to resolve merge request", e))
}

fn check_icon(state: &CheckState) -> &'static str {
    match state {
        CheckState::Success => "✓",
        CheckState::Failure => "✗",
        CheckState::Pending => "◐",
        CheckState::Skipped => "⊘",
        CheckState::Cancelled => "⊗",
    }
}

/// Render the live-PR markdown section for a node `/pr` artifact whose job owns
/// a `merge_requests` row. Returns `None` when the job has no PR, so non-PR
/// artifacts (e.g. `plan`) are unaffected.
///
/// Fetching live data refreshes the cached row as a side effect
/// (refresh-on-read). When the PR is open, an `## actions` block advertising
/// merge/close/refresh is appended. `artifact_uri` is the `/pr` URI used in the
/// action examples; `diff_full` inlines the full patch text per file.
pub async fn render_live_pr_section(
    orch: &Orchestrator,
    job_id: &str,
    artifact_uri: &str,
    diff_full: bool,
) -> Option<String> {
    let mr_context = match try_resolve_mr_context_for_job(&orch.db.local, job_id).await {
        Ok(Some(ctx)) => ctx,
        Ok(None) => return None,
        Err(e) => return Some(format!("## Pull Request\n\n(failed to resolve PR: {e})\n")),
    };
    if mr_context.github_pr_number.is_none() {
        let cache = match refresh_local_pr_for_job(orch, job_id, &mr_context).await {
            Ok(cache) => cache,
            Err(e) => {
                return Some(format!(
                    "## Local PR\n\n(failed to refresh local PR: {e})\n"
                ))
            }
        };
        let mut out = format!(
            "## Local PR\n\n{}\nState: {}\nMergeable: {}\n",
            cache.title.as_deref().unwrap_or("Untitled"),
            cache.state,
            cache.mergeable
        );
        if let (Some(additions), Some(deletions)) = (cache.additions, cache.deletions) {
            out.push_str(&format!("Changes: +{} -{}\n", additions, deletions));
        }
        if let Some(body) = cache.body.as_deref().filter(|b| !b.is_empty()) {
            out.push_str("\n### Description\n\n");
            out.push_str(body);
            out.push('\n');
        }
        if matches!(cache.state, PrState::Open) {
            out.push_str(&format!(
                "\n## actions\n- [merge]({uri}): patch with action:\"merge\" (optional method, default squash).\n- [close]({uri}): patch with action:\"close\".\n- [refresh]({uri}): patch with action:\"refresh\".",
                uri = artifact_uri
            ));
        }
        return Some(out);
    }

    let pr_number = mr_context.github_pr_number.expect("checked above");
    let header = format!(
        "## Pull Request\n\nPR #{}: {}\n",
        pr_number, mr_context.pr_url
    );

    let (owner, repo) = match get_owner_repo(&mr_context.repo_path) {
        Ok(v) => v,
        Err(e) => return Some(format!("{header}\n(failed to resolve repo: {e})\n")),
    };
    let creds = match get_credentials_for_owner(&orch.db.local, &owner).await {
        Ok(c) => c,
        Err(e) => return Some(format!("{header}\n(failed to resolve credentials: {e})\n")),
    };

    let http = &*orch.services.http;
    let pr_details = match fetch_pr_via_api(http, &creds, &owner, &repo, pr_number).await {
        Ok(d) => d,
        Err(e) => return Some(format!("{header}\n(failed to fetch live PR: {e})\n")),
    };
    let checks = fetch_checks_via_api(http, &creds, &owner, &repo, &pr_details.head_sha)
        .await
        .unwrap_or_default();
    let checks_status = compute_checks_status(&checks);
    let files = api::fetch_pr_files(http, &creds, &owner, &repo, pr_number)
        .await
        .unwrap_or_default();

    // Refresh-on-read: persist freshly fetched details to the cache.
    let now = chrono::Utc::now().timestamp();
    if let Err(e) = update_merge_request_github_cache(
        &orch.db.local,
        &mr_context.mr_id,
        &pr_details,
        &checks,
        &checks_status,
        now,
    )
    .await
    {
        log::warn!("Failed to refresh PR cache on read: {}", e);
    } else {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "merge_requests", "action": "update"}),
        );
    }

    let mut out = header;
    out.push_str(&format!(
        "State: {}{}\n",
        pr_details.state,
        if pr_details.is_draft { " (draft)" } else { "" }
    ));
    if let Some(review) = &pr_details.review_decision {
        out.push_str(&format!("Review: {}\n", review));
    }
    out.push_str(&format!("Mergeable: {}\n", pr_details.mergeable));
    if let Some(status) = &checks_status {
        out.push_str(&format!("Checks: {}\n", status));
    }
    out.push_str(&format!(
        "Changes: +{} -{}\n",
        pr_details.additions.unwrap_or(0),
        pr_details.deletions.unwrap_or(0)
    ));

    if let Some(body) = pr_details.body.as_deref().filter(|b| !b.is_empty()) {
        out.push_str("\n### Description\n\n");
        out.push_str(body);
        out.push('\n');
    }

    if !checks.is_empty() {
        out.push_str("\n### Checks\n\n");
        for c in &checks {
            out.push_str(&format!("- [{}] {}\n", check_icon(&c.state), c.name));
        }
    }

    if !files.is_empty() {
        out.push_str("\n### Files\n\n");
        for f in &files {
            out.push_str(&format!(
                "- {} (+{} -{}) {}\n",
                f.filename, f.additions, f.deletions, f.status
            ));
        }
    }

    if diff_full {
        out.push_str("\n### Diff\n\n");
        for f in &files {
            if let Some(patch) = f.patch.as_deref() {
                out.push_str(&format!(
                    "#### {}\n\n```diff\n{}\n```\n\n",
                    f.filename, patch
                ));
            }
        }
    } else if !files.is_empty() {
        out.push_str("\nFull patch: append `?diff=full` to this URI.\n");
    }

    // Actions are valid only while the PR is open.
    if matches!(pr_details.state, PrState::Open) {
        out.push_str(&format!(
            "\n## actions\n- [merge]({uri}): patch with action:\"merge\" (optional method, default squash). e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"merge\",method:\"squash\"}}}}]}})\n- [close]({uri}): patch with action:\"close\". e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"close\"}}}}]}})\n- [refresh]({uri}): patch with action:\"refresh\" to re-fetch live PR state. e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"refresh\"}}}}]}})",
            uri = artifact_uri
        ));
    }

    Some(out)
}

/// Background post-merge reconciliation shared by the in-app merge
/// (`merge_pr_for_job`) and the GitHub webhook merge path.
///
/// Runs the side effects that follow a PR landing, in order:
/// 1. capture the merged file list for issue history,
/// 2. pull the main repo when it is on the default branch (gated on the
///    `pull_on_merge` setting),
/// 3. fast-forward an active worktree on the PR's *target* branch — e.g. a
///    Coordinator integration branch a sibling has checked out — always,
///    regardless of `pull_on_merge`, since agent-owned worktrees must track a
///    base that just advanced (the main-repo default-branch checkout is skipped
///    here, owned by step 2),
/// 4. tear down the merged issue's worktrees,
/// 5. refresh the cached PR row.
///
/// Every step is non-fatal: each logs and continues on failure so one broken
/// step never strands the rest. The PR-merged state transition itself
/// (`resolve_pr_node`, which also fires base-advance notifications, plus the
/// caller's attention emit) has already happened by the time this runs. Takes
/// owned values so callers can spawn it detached via `tokio::spawn`.
pub async fn reconcile_after_merge(orch: Orchestrator, ctx: MergeMrContext) {
    let owner_id = ctx.mr.job_id.clone();
    let repo_path = ctx.mr.repo_path.clone();
    let pr_number = ctx.mr.github_pr_number;
    let default_branch = ctx.default_branch.clone();
    let target_branch = ctx.target_branch.clone();
    let issue_id = ctx.issue_id.clone();

    log::info!(
        "Starting post-merge reconciliation for owner {} (target branch '{}')",
        owner_id,
        target_branch
    );

    let file_change_job_id = match resolve_file_change_job_id(&orch.db.local, &owner_id).await {
        Ok(job_id) => job_id,
        Err(e) => {
            log::warn!(
                "Skipping post-merge file capture for owner {}: {}",
                owner_id,
                e
            );
            None
        }
    };
    if file_change_job_id.is_none() {
        log::warn!(
            "Skipping post-merge file capture for owner {}: no jobs.id owner found",
            owner_id
        );
    }

    // 1. Capture file changes for issue history.
    if let Some(pr_number) = pr_number {
        match get_owner_repo(&repo_path) {
            Ok((owner, repo)) => match get_credentials_for_owner(&orch.db.local, &owner).await {
                Ok(creds) => {
                    let http = &*orch.services.http;
                    match api::fetch_pr_files(http, &creds, &owner, &repo, pr_number).await {
                        Ok(files) => {
                            if let Some(file_change_job_id) = file_change_job_id.as_deref() {
                                let count = files.len();
                                if let Err(e) =
                                    store_file_changes(&orch, file_change_job_id, &files).await
                                {
                                    log::warn!("Failed to store file changes: {}", e);
                                } else {
                                    log::info!(
                                        "Stored {} file changes for owner {} under job {}",
                                        count,
                                        owner_id,
                                        file_change_job_id
                                    );
                                }
                            }
                        }
                        Err(e) => log::warn!("Failed to fetch PR files for history: {}", e),
                    }
                }
                Err(e) => log::warn!("Skipping post-merge file capture (credentials): {}", e),
            },
            Err(e) => log::warn!("Skipping post-merge file capture (owner/repo): {}", e),
        }
    } else {
        match local_pr_files(
            &*orch.services.git,
            Path::new(&repo_path),
            &ctx.target_branch,
            &ctx.source_branch,
        ) {
            Ok(files) => {
                if let Some(file_change_job_id) = file_change_job_id.as_deref() {
                    if let Err(e) = store_file_changes(&orch, file_change_job_id, &files).await {
                        log::warn!("Failed to store local file changes: {}", e);
                    } else {
                        log::info!(
                            "Stored {} local file changes for owner {} under job {}",
                            files.len(),
                            owner_id,
                            file_change_job_id
                        );
                    }
                }
            }
            Err(e) => log::warn!("Failed to capture local file changes: {}", e),
        }
    }

    // 2. Pull the main repository to reflect merged changes (only when it is on
    //    the default branch; gated on the `pull_on_merge` setting).
    if pr_number.is_some() && pull_on_merge_setting() {
        let git = &*orch.services.git;
        if let Err(e) = update_main_repo_after_merge(git, &repo_path, &default_branch) {
            log::warn!("Failed to update main repo after merge: {}", e);
        }
    }

    // 3. Fast-forward an active worktree on the PR's target branch (e.g. a
    //    Coordinator integration branch). Always runs — a stale agent worktree
    //    is the bug this fixes — and skips the main-repo/default-branch checkout
    //    handled by step 2.
    {
        let git = &*orch.services.git;
        if pr_number.is_some() {
            if let Err(e) = fast_forward_active_worktree(git, Path::new(&repo_path), &target_branch)
            {
                log::warn!("Worktree fast-forward after merge failed: {}", e);
            }
        }
    }

    // 4. Tear down worktrees and branches for the merged issue (issue-wide).
    //    Scoped to the merged PR's issue, so a Coordinator worktree on a
    //    different issue survives and the step-3 fast-forward sticks.
    if let Some(issue_id) = issue_id.as_deref() {
        if let Err(e) = teardown_worktrees(&orch, TeardownScope::Issue(issue_id.to_string())).await
        {
            log::warn!("Worktree teardown after merge failed: {}", e);
        }
    }

    // 5. Refresh PR details.
    let _ = refresh_pr_for_job(&orch, &owner_id).await;

    log::info!("Post-merge reconciliation completed for owner {}", owner_id);
}

/// Merge a PR, mark the issue merged, and run post-merge reconciliation (file
/// capture, optional main-repo pull, active-worktree fast-forward, teardown, PR
/// refresh) in the background via `reconcile_after_merge`.
pub async fn merge_pr_for_job(
    orch: &Orchestrator,
    job_id: &str,
    merge_method: Option<String>,
) -> Result<String, String> {
    let merge_context = resolve_merge_mr_context_for_job(&orch.db.local, job_id).await?;
    let repo_path = merge_context.mr.repo_path.clone();
    let issue_id = merge_context.issue_id.clone();

    let method = effective_merge_method(&merge_context, merge_method);
    if let Some(pr_number) = merge_context.mr.github_pr_number {
        let (owner, repo) = get_owner_repo(&repo_path)?;
        let creds = get_credentials_for_owner(&orch.db.local, &owner).await?;

        let http = &*orch.services.http;
        api::merge_pr(http, &creds, &owner, &repo, pr_number, &method).await?;
    } else {
        log::info!(
            "Starting local merge request merge for job/action {}: {} -> {} using {} in {}",
            job_id,
            merge_context.source_branch,
            merge_context.target_branch,
            method,
            repo_path
        );
        let local_files = match local_pr_files(
            &*orch.services.git,
            Path::new(&repo_path),
            &merge_context.target_branch,
            &merge_context.source_branch,
        ) {
            Ok(files) => files,
            Err(error) => {
                log::warn!(
                    "Could not capture local merge request file list before merge for job/action {} ({} -> {}): {}",
                    job_id,
                    merge_context.source_branch,
                    merge_context.target_branch,
                    error
                );
                Vec::new()
            }
        };
        let merged_commit = local_merge(
            &*orch.services.git,
            Path::new(&repo_path),
            &merge_context.target_branch,
            &merge_context.source_branch,
            &method,
            &merge_context.title,
        )
        .map_err(|error| {
            let message = format!(
                "Local merge failed for {} -> {} in {}: {}",
                merge_context.source_branch, merge_context.target_branch, repo_path, error
            );
            log::error!("{}", message);
            message
        })?;
        log::info!(
            "Local merge request git merge succeeded for job/action {} at commit {}",
            job_id,
            merged_commit
        );
        if !local_files.is_empty() {
            match resolve_file_change_job_id(&orch.db.local, job_id).await {
                Ok(Some(file_change_job_id)) => {
                    if let Err(error) = store_file_changes(orch, &file_change_job_id, &local_files).await
                    {
                        log::warn!(
                            "Local merge request merged but file-change history capture failed for owner {} (resolved job {}): {}",
                            job_id,
                            file_change_job_id,
                            error
                        );
                    }
                }
                Ok(None) => log::warn!(
                    "Local merge request merged but file-change history capture skipped for owner {}: no jobs.id owner found",
                    job_id
                ),
                Err(error) => log::warn!(
                    "Local merge request merged but file-change history capture skipped for owner {}: {}",
                    job_id,
                    error
                ),
            }
        }
        persist_merged_commit(&orch.db.local, &merge_context.mr.mr_id, &merged_commit).await?;
    }

    let closed_sessions = resolve_pr_node(orch, job_id, PrNodeResolution::Merge)
        .await
        .map_err(|error| {
            log::error!(
                "Merged git branch but failed to mark merge request merged for job/action {}: {}",
                job_id,
                error
            );
            error
        })?;
    for session_id in &closed_sessions {
        orch.process_state.remove_by_session(session_id);
    }

    if let Some(issue_id) = issue_id.as_deref() {
        // Terminal transition — wake any in-flight `cairn watch` on this issue,
        // matching the PR-webhook merge path. Typed emit re-reads the live
        // projection and sends `Resolved` since the issue just became terminal.
        orch.wake_for_issue(issue_id).await;
    }

    // Background reconciliation, shared with the GitHub webhook merge path.
    tokio::spawn(reconcile_after_merge(orch.clone(), merge_context));

    Ok("PR merged successfully".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge_context(is_workspace: bool, has_triage_batch: bool) -> MergeMrContext {
        MergeMrContext {
            mr: MrContext {
                mr_id: "mr".to_string(),
                pr_url: String::new(),
                github_pr_number: None,
                repo_path: "/tmp/repo".to_string(),
                job_id: "job".to_string(),
                is_local: false,
            },
            issue_id: Some("issue".to_string()),
            default_branch: "main".to_string(),
            project_id: "project".to_string(),
            target_branch: "main".to_string(),
            source_branch: "feature".to_string(),
            title: "PR".to_string(),
            is_workspace,
            has_triage_batch,
        }
    }

    #[test]
    fn canon_prs_always_use_merge_method() {
        assert_eq!(
            effective_merge_method(&merge_context(true, false), Some("squash".to_string())),
            "merge"
        );
        assert_eq!(
            effective_merge_method(&merge_context(false, true), Some("rebase".to_string())),
            "merge"
        );
        assert_eq!(
            effective_merge_method(&merge_context(false, false), None),
            "squash"
        );
    }
    use crate::db::DbState;
    use crate::services::testing::{MockGitClient, TestServicesBuilder};
    use crate::services::GitOutput;
    use crate::storage::{LocalDb, SearchIndex};
    use std::path::Path;
    use std::sync::Arc;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("reconcile-test.db").await
    }

    fn test_orchestrator(db: LocalDb, git: MockGitClient) -> Orchestrator {
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
    async fn seed_merge_request(
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

    async fn seed_local_open_merge_request(db: &LocalDb, owner_id: &str) {
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

    #[test]
    fn gh_pr_edit_args_include_title_and_body() {
        assert_eq!(
            gh_pr_edit_args("1538", "New title", "New body"),
            [
                "pr",
                "edit",
                "1538",
                "--title",
                "New title",
                "--body",
                "New body"
            ]
        );
    }

    async fn seed_pr_node_merge_request_for_artifact_job(db: &LocalDb) {
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
                    "INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, created_at)
                     VALUES ('pr-action-run', 'exec-pr-node', 'pr', 'builtin:pr', 'issue-pr-node', 'proj-pr-node', 'blocked', 2)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, body, source_branch, target_branch, status, opened_at, updated_at)
                     VALUES ('mr-pr-node', 'pr-action-run', 'proj-pr-node', 'issue-pr-node', 'Old title', 'Old body', 'agent/PROJ-3-builder', 'main', 'open', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_updates_local_merge_request_cache() {
        let db = migrated_db().await;
        seed_local_open_merge_request(&db, "owner-job").await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(
            &orch,
            "owner-job",
            "New archive semantics",
            Some("Updated PR body"),
        )
        .await
        .unwrap();

        assert!(synced);
        let row: (String, String) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT title, body FROM merge_requests WHERE id = 'mr-local'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.text(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(row.0, "New archive semantics");
        assert_eq!(row.1, "Updated PR body");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_follows_pr_node_owner() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(
            &orch,
            "builder-job",
            "New PR-node title",
            Some("New PR-node body"),
        )
        .await
        .unwrap();

        assert!(synced);
        let row: (String, String) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT title, body FROM merge_requests WHERE id = 'mr-pr-node'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.text(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(row.0, "New PR-node title");
        assert_eq!(row.1, "New PR-node body");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_noops_without_merge_request() {
        let db = migrated_db().await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(&orch, "missing-job", "Title", Some("Body"))
            .await
            .unwrap();

        assert!(!synced);
    }

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

    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_fast_forwards_worktree_on_pr_target_branch() {
        // A real but non-git tempdir as repo_path makes get_owner_repo fail
        // cleanly, so reconcile's credential/HTTP steps (file capture, PR
        // refresh) are graceful no-ops and the test turns purely on git.
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_path = repo_dir.path().to_string_lossy().to_string();

        let db = migrated_db().await;
        seed_merge_request(&db, "owner-job", &repo_path, "integration").await;

        let porcelain = format!(
            "worktree {repo_path}\nHEAD aaa\nbranch refs/heads/main\n\nworktree /wt/coord\nHEAD bbb\nbranch refs/heads/integration\n"
        );
        let repo_path_for_branch = repo_path.clone();

        let mut git = MockGitClient::new();
        git.expect_worktree_list()
            .returning(move |_| Ok(porcelain.clone()));
        git.expect_current_branch().returning(move |repo: &Path| {
            if repo.to_string_lossy() == repo_path_for_branch {
                // != default "main" → the main-repo pull (step 2) skips, isolating
                // the assertion to the worktree fast-forward.
                Ok("feature".to_string())
            } else {
                // The integration worktree at /wt/coord.
                Ok("integration".to_string())
            }
        });
        git.expect_status().returning(|_| Ok(String::new()));
        // The fast-forward must fetch then `merge --ff-only origin/integration`.
        // If target_branch were wrongly the project default, the worktree lookup
        // would resolve to the main repo path and be skipped — these calls would
        // never fire and the `.times(1)` expectations would fail.
        git.expect_run()
            .withf(|_, args| args.first().map(String::as_str) == Some("fetch"))
            .times(1)
            .returning(|_, _| {
                Ok(GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });
        git.expect_run()
            .withf(|_, args| {
                args.first().map(String::as_str) == Some("merge")
                    && args.iter().any(|a| a == "origin/integration")
            })
            .times(1)
            .returning(|_, _| {
                Ok(GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });

        let orch = test_orchestrator(db, git);
        let ctx = resolve_merge_mr_context_for_job(&orch.db.local, "owner-job")
            .await
            .unwrap();

        // No jobs seeded for issue-1 → teardown is a no-op (no further git calls).
        // MockGitClient verifies the `.times(1)` fetch + ff-only merge on drop.
        reconcile_after_merge(orch, ctx).await;
    }
}
