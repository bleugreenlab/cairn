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
    assert_main_checkout_clean_for_default_merge, compute_checks_status, compute_local_mergeable,
    fetch_checks_via_api, fetch_pr_via_api, local_pr_files, reconcile_main_checkout_after_merge,
    ParsedPrDetails,
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

/// The PR's source and target jj bookmarks, used to read jj conflict state and
/// to scope the conflicted-commit enumeration to `<target>..<source>`.
async fn load_mr_branches(db: &LocalDb, mr_id: &str) -> Result<Option<(String, String)>, String> {
    let mr_id = mr_id.to_string();
    db.query_opt(
        "SELECT source_branch, target_branch FROM merge_requests WHERE id = ?1",
        params![mr_id.as_str()],
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map_err(|e| db_error("Failed to load merge request branches", e))
}

/// A conflicted-history report for a PR source branch: the conflicted commits
/// (with files) that block the merge. Built only when the source actually
/// carries a conflict — `source_conflict_report` returns `None` for a clean
/// source, so `commits` is never empty.
pub(crate) struct SourceConflictReport {
    pub commits: Vec<crate::jj::ConflictedCommit>,
}

/// `None` when the source branch is clean; `Some(report)` when its history
/// carries one or more recorded conflicts. Scopes enumeration to
/// `<target>..bookmarks(exact:source)` when `target_branch` is known (excluding
/// commits already on the target), else the source bookmark alone — which still
/// catches the conflict because jj propagates it to the tip.
///
/// jj records merge conflicts *inside* the commit, which GitHub still reports as
/// mergeable (and renders as garbage), so every PR read and merge boundary
/// trusts this over the GitHub mergeable bit. Read-side advisory: returns `None`
/// on any jj error (fail open), so it never weakens the load-bearing boolean
/// gate ([`jj_source_branch_conflicted`]) underneath.
pub(crate) fn source_conflict_report(
    jj_binary_path: &str,
    config_dir: &Path,
    repo_path: &str,
    source_branch: &str,
    target_branch: Option<&str>,
) -> Option<SourceConflictReport> {
    let jj = crate::jj::JjEnv::resolve(jj_binary_path, config_dir);
    let store = crate::jj::project_store_dir(config_dir, Path::new(repo_path));
    let source_revset = format!("bookmarks(exact:{source_branch:?})");
    let range = match target_branch {
        Some(target) => format!("bookmarks(exact:{target:?})..{source_revset}"),
        None => source_revset,
    };
    let commits = crate::jj::conflicted_commits(&jj, &store, &range);
    (!commits.is_empty()).then_some(SourceConflictReport { commits })
}

/// Render a conflicted-commit list as compact markdown bullets, one per commit,
/// naming the short commit id, description, and conflicted files. Shared by every
/// surface so the wording is identical wherever a conflicted history is reported.
pub(crate) fn format_conflicted_commits(commits: &[crate::jj::ConflictedCommit]) -> String {
    commits
        .iter()
        .map(|c| {
            let desc = if c.description.is_empty() {
                String::new()
            } else {
                format!(" ({:?})", c.description)
            };
            let files = if c.files.is_empty() {
                String::new()
            } else {
                format!(": {}", c.files.join(", "))
            };
            format!("- {}{}{}", c.commit_id, desc, files)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The concrete recovery sentence shared across all conflicted-history surfaces:
/// resolve the markers in the workspace and re-seal, or rebuild the branch
/// conflict-free onto the current target tip (resolve-at-base).
pub(crate) fn conflict_recovery_hint(source_branch: &str, target_branch: Option<&str>) -> String {
    let target = target_branch.unwrap_or("target");
    format!(
        "Recovery: resolve the conflict markers in `{source_branch}`'s workspace and let it re-seal, or rebuild `{source_branch}` conflict-free onto the current `{target}` tip (resolve-at-base)."
    )
}

/// Build the conflicted-history detail block (commit list + recovery hint) from
/// an already-resolved jj env + store. Used by the in-fold merge refusals where
/// `jj` and `store` are in hand; enumerates `range_revset & conflicts()`.
fn conflicted_history_detail(
    jj: &crate::jj::JjEnv,
    store: &Path,
    range_revset: &str,
    source_branch: &str,
    target_branch: Option<&str>,
) -> String {
    let commits = crate::jj::conflicted_commits(jj, store, range_revset);
    let listing = if commits.is_empty() {
        String::new()
    } else {
        format!("\n{}", format_conflicted_commits(&commits))
    };
    format!(
        "{listing}\n{}",
        conflict_recovery_hint(source_branch, target_branch)
    )
}

/// Mergeability override for a jj PR read path: `Conflicting` when the source
/// bookmark has a recorded conflict, else `None` (keep the GitHub value).
async fn jj_conflict_mergeable_override(
    orch: &Orchestrator,
    repo_path: &str,
    mr_id: &str,
) -> Option<MergeableState> {
    let (source_branch, target_branch) = load_mr_branches(&orch.db.local, mr_id).await.ok()??;
    source_conflict_report(
        &orch.jj_binary_path,
        &orch.config_dir,
        repo_path,
        &source_branch,
        Some(&target_branch),
    )
    .map(|_| MergeableState::Conflicting)
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
        // Downstream sibling/coordinator reconciliation advances *other* in-flight
        // branches (siblings auto-rebase onto the advanced tip; a Coordinator on
        // the integration branch has its `@` re-parented). It has no bearing on
        // whether THIS PR is merged, is best-effort end to end, and is re-fired
        // idempotently by the GitHub `push` webhook (guarded by the before/after
        // commit-id snapshot in `reconcile_base_advance`). So spawn it off the
        // synchronous merge path: the resolution — and the "merging" button —
        // returns immediately, and the reconcile lands in the background.
        let orch = orch.clone();
        let owner_id = owner_id.to_string();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            if let Err(e) = crate::orchestrator::base_advance::notify_downstream_of_base_advance(
                &orch, &owner_id,
            )
            .await
            {
                log::warn!("Failed to notify downstream jobs of base advance: {}", e);
            }
            log::info!(
                "Deferred downstream base-advance reconcile for owner {} took {:?}",
                owner_id,
                started.elapsed()
            );
        });
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
    let mut pr_details = fetch_pr_via_api(http, &creds, &owner, &repo, pr_number).await?;
    if let Some(mergeable) = jj_conflict_mergeable_override(orch, &repo_path, &mr_id).await {
        pr_details.mergeable = mergeable;
    }
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
    // An open local PR's diff stat must track its live source branch: recompute
    // it from the fresh `git diff --numstat` on every refresh so a rebased or
    // flattened source branch self-corrects. Freezing the first value (via
    // `.or_else`/`COALESCE`) would pin a stale — possibly pre-conflict-resolution
    // — diff forever. For merged/closed PRs `local_files` is empty, so keep the
    // stored stat rather than zeroing it.
    let (additions, deletions) = if status == "open" {
        (
            Some(local_files.iter().map(|file| file.additions).sum()),
            Some(local_files.iter().map(|file| file.deletions).sum()),
        )
    } else {
        (additions, deletions)
    };
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
                         additions = ?3, deletions = ?4
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
    let mut pr_details = match fetch_pr_via_api(http, &creds, &owner, &repo, pr_number).await {
        Ok(d) => d,
        Err(e) => return Some(format!("{header}\n(failed to fetch live PR: {e})\n")),
    };
    // Same jj conflict gate as the cache refresh: a jj-conflicted source bookmark
    // is rendered (and re-cached) as Conflicting, not GitHub's false mergeable.
    // Keep the full report to enumerate the offending commits/files below so the
    // live artifact and the node summary tell one consistent story.
    let source_branches = load_mr_branches(&orch.db.local, &mr_context.mr_id)
        .await
        .ok()
        .flatten();
    let source_conflict = source_branches.as_ref().and_then(|(src, tgt)| {
        source_conflict_report(
            &orch.jj_binary_path,
            &orch.config_dir,
            &mr_context.repo_path,
            src,
            Some(tgt),
        )
    });
    if source_conflict.is_some() {
        pr_details.mergeable = MergeableState::Conflicting;
    }
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
    if source_conflict.is_some() {
        // A conflicted history inflates the diff GitHub reports; flag it so the
        // number can't read as a clean, mergeable change.
        out.push_str(&format!(
            "Changes: +{} -{} (stale — branch carries conflicts; resolve before trusting)\n",
            pr_details.additions.unwrap_or(0),
            pr_details.deletions.unwrap_or(0)
        ));
    } else {
        out.push_str(&format!(
            "Changes: +{} -{}\n",
            pr_details.additions.unwrap_or(0),
            pr_details.deletions.unwrap_or(0)
        ));
    }
    if let (Some(report), Some((src, tgt))) = (&source_conflict, &source_branches) {
        out.push_str("\n⛔ Conflicted history — cannot merge:\n");
        out.push_str(&format_conflicted_commits(&report.commits));
        out.push('\n');
        out.push_str(&conflict_recovery_hint(src.as_str(), Some(tgt.as_str())));
        out.push('\n');
    }

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

    // Turn-end (when:idle/when:review) project checks: live log tail while a suite
    // is in flight, else the cached per-check verdicts for this node's sealed tree.
    if let Some(section) =
        crate::execution::checks_turn_end::render_turn_end_checks_section(orch, job_id).await
    {
        out.push_str(&section);
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
/// (`merge_pr_for_job`) and the GitHub merge path.
///
/// Runs the side effects that follow a PR landing, in order:
/// 1. capture the merged file list for issue history,
/// 2. reconcile the user's main checkout only when the merge advanced the project
///    default branch,
/// 3. tear down the merged issue's worktrees,
/// 4. refresh the cached PR row.
///
/// Every step is non-fatal: each logs and continues on failure so one broken
/// step never strands the rest. The PR-merged state transition itself
/// (`resolve_pr_node`, which also fires base-advance notifications, plus the
/// caller's attention emit) has already happened by the time this runs. Takes
/// owned values so callers can spawn it detached via `tokio::spawn`.
/// `force_checkout_pull` forces the user's main-checkout fast-forward pull
/// regardless of the `pull_on_merge` setting. The GitHub-API merge path sets it
/// for default-branch PRs: nothing locally rewrote the checkout (no local fold),
/// so a `git pull` of the PR base branch is the only way the checkout catches up
/// to the just-merged tip. The local-fold path passes `false` — the fold already
/// advanced the local default ref, and `pull_on_merge` governs only the extra
/// pull.
fn should_reconcile_main_checkout_after_merge(
    target_branch: &str,
    resolved_default_branch: &str,
) -> bool {
    target_branch == resolved_default_branch
}

pub async fn reconcile_after_merge(
    orch: Orchestrator,
    ctx: MergeMrContext,
    force_checkout_pull: bool,
) {
    let owner_id = ctx.mr.job_id.clone();
    let repo_path = ctx.mr.repo_path.clone();
    let pr_number = ctx.mr.github_pr_number;
    let default_branch = ctx.default_branch.clone();
    let resolved_default_branch = resolve_project_default_branch(&repo_path, &default_branch);
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

    // 2. Reconcile the user's project checkout only for a real default-branch
    //    advance. Child→integration merges never detach or move the main checkout;
    //    resetting it there only creates races on unrelated work.
    if should_reconcile_main_checkout_after_merge(&target_branch, &resolved_default_branch) {
        let git = &*orch.services.git;
        let pull = force_checkout_pull || (pr_number.is_some() && pull_on_merge_setting());
        if let Err(e) =
            reconcile_main_checkout_after_merge(git, &repo_path, &resolved_default_branch, pull)
        {
            log::warn!("Failed to reconcile main checkout after merge: {}", e);
        }
    } else {
        log::debug!(
            "Skipping main checkout reconcile after merge into non-default target '{}' (resolved default '{}')",
            target_branch,
            resolved_default_branch
        );
    }

    // 3. Downstream-workspace reconciliation is spawned (not awaited) inside
    //    `resolve_pr_node`, so it runs in the background off the user-facing merge
    //    path, in `base_advance::reconcile_jj_downstream`. It does two asymmetric
    //    things over the shared store: in-flight SIBLINGS (branched FROM the
    //    integration branch) auto-rebase onto the advanced tip, and the workspace
    //    whose branch IS the integration branch (a Coordinator) has its `@`
    //    re-parented onto the folded tip via `crate::jj::advance_workspace_onto` —
    //    the jj-native restoration of the old git "post-merge fast-forward of
    //    active worktrees". So there is no git worktree fast-forward step here.

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

/// Perform a jj source→target merge entirely in the shared store: fold the
/// source's commit into the target bookmark, then (for a project with a
/// remote) push the target to origin. The push advances the target branch and —
/// because the source's head commit is now an ancestor of the target —
/// GitHub's out-of-band "merged outside GitHub" detection marks the source PR
/// Merged (the way `git merge feature; git push` does). Returns the new
/// target-tip commit id to persist as `merge_requests.merged_commit`. For a
/// no-remote project the push is skipped and the fold is purely local.
///
/// Two fold shapes, discriminated by whether the target IS the project default
/// branch:
///
/// - target ≠ default (a child PR into a Coordinator integration branch): the
///   integration tip advances within Cairn's local fold chain as earlier
///   siblings merge in, and downstream sibling reconciliation now runs deferred
///   off the synchronous merge path — so the source may lag the live tip. Rebase
///   the source onto the current integration tip before the forward-only fold
///   (materializing any conflict and failing closed, as the default path does),
///   then — because the rebase rewrites the source's commit id — push the rebased
///   source's PR head before advancing the target on origin so GitHub still marks
///   the child PR Merged.
/// - target == default: the default branch advances OUTSIDE Cairn's fold chain
///   (another PR merged into it, or an external push), so the source's fork
///   point may now lag the live tip and a bare FF would be refused. Fetch the
///   live tip and rebase the source onto it, then FF. For the default `squash`
///   method the rebased chain is first collapsed to a single commit on the live
///   tip (`squash_branch_onto`) so the default branch gains exactly one commit
///   per PR; the `merge` method (workspace / memory-triage PRs) keeps the real
///   per-commit fold via `rebase_then_fold_into`. Either way the rebase/squash
///   rewrites the source's commit id, so origin's PR head SHA is no longer
///   reachable from the new target; push the rewritten source first so its PR
///   head matches the commit that lands on the default branch, then advance the
///   target to mark the PR Merged out of band.
async fn store_merge_child(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
) -> Result<String, String> {
    let repo_path = merge_context.mr.repo_path.as_str();
    let target_branch = merge_context.target_branch.as_str();
    let source_branch = merge_context.source_branch.as_str();
    let default_branch = merge_context.default_branch.as_str();
    let project_id = merge_context.project_id.as_str();
    let has_remote = !merge_context.mr.is_local;
    let squash_title = merge_context.title.as_str();
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));

    if target_branch == default_branch {
        // The default branch advances out of band. Bring its live tip into the
        // store (it may have moved via another Cairn merge OR externally — a
        // fetch covers both) and rebase the source onto it before the FF fold,
        // so the fold can never go backwards.
        let dest = if has_remote {
            // Track + fetch so `<target>@origin` resolves to the live tip
            // (mirrors how `base_advance.rs` learns an external default advance).
            // Best-effort: warn and fall back to whatever the store last saw.
            if let Err(e) = crate::jj::track_bookmark(&jj, &store, target_branch) {
                log::debug!("jj store merge: track {target_branch} (continuing): {e}");
            }
            if let Err(e) = crate::jj::fetch_remote(&jj, &store, "origin") {
                log::warn!(
                    "jj store merge: fetch origin before rebase-then-fold (continuing): {e}"
                );
            }
            format!("{target_branch}@origin")
        } else {
            target_branch.to_string()
        };

        if method == "squash" {
            // Squash landing: rebase the source onto the live default tip, then
            // collapse the rebased chain to a single commit on that tip before
            // the FF — so the default branch gains exactly one commit per PR
            // instead of every per-change commit the agent sealed.
            crate::jj::rebase_branch_onto(&jj, &store, source_branch, &dest)?;
            if crate::jj::branch_has_conflict(&jj, &store, source_branch)? {
                // The rebase recorded a conflict and the default bookmark was NOT
                // moved. The source's live workspace `@` was rebased out from
                // under it and is now stale: materialize the markers (as
                // `reconcile_siblings` does) so resolve-and-retry is actionable,
                // and let the merge gate keep blocking until they are resolved.
                materialize_source_conflict_in_workspaces(orch, project_id, &jj, source_branch)
                    .await;
                return Err(format!(
                    "Refusing to merge: rebasing `{source_branch}` onto the advanced default branch `{target_branch}` recorded a conflict.{detail}",
                    detail = conflicted_history_detail(
                        &jj,
                        &store,
                        &format!("bookmarks(exact:{source_branch:?})"),
                        source_branch,
                        Some(target_branch),
                    )
                ));
            }
            // Idempotence guard (mirrors the old real-fold path's no-op on a
            // retry): if the source already resolves to the LOCAL default
            // bookmark, a prior attempt's fold already landed this PR's content
            // and only local resolution / DB marking failed. Squashing again
            // would mint a fresh empty commit on the default tip and FF onto it,
            // adding one empty commit per retry. Skip the squash+fold and fall
            // through to the return. Compared against the LOCAL target (not
            // `dest`): when an interrupted retry still needs to advance origin,
            // the source already equals the local tip while `dest@origin` may
            // lag, and re-squashing against the lagging origin tip would mint a
            // sideways commit the FF then refuses. The remote push block below
            // still runs, idempotently finishing any unpushed origin advance.
            let already_landed = matches!(
                (
                    crate::jj::bookmark_commit(&jj, &store, source_branch),
                    crate::jj::bookmark_commit(&jj, &store, target_branch),
                ),
                (Some(source_tip), Some(target_tip)) if source_tip == target_tip
            );
            if !already_landed {
                // Collapse to one commit whose parent is the live default tip and
                // whose tree equals the rebased source, then FF the default to it.
                crate::jj::squash_branch_onto(&jj, &store, source_branch, &dest, squash_title)?;
                crate::jj::merge_into_bookmark(&jj, &store, target_branch, source_branch)?;
            }
        } else {
            // Non-squash (workspace / memory-triage): keep the real fold so the
            // default branch carries every sealed commit. Rebase the source onto
            // the live default tip, then FF the default to it.
            if let Err(conflict_err) =
                crate::jj::rebase_then_fold_into(&jj, &store, target_branch, source_branch, &dest)
            {
                // The rebase recorded a conflict on the source bookmark and
                // aborted the fold — the default bookmark was NOT moved. The
                // source's live workspace `@` was rebased out from under it and is
                // now stale: its on-disk files do not yet carry the conflict
                // markers. Materialize them (as `reconcile_siblings` does after a
                // store-driven rebase) so the resolve-and-retry guidance is
                // actionable, and the merge gate keeps blocking until the agent
                // resolves them.
                materialize_source_conflict_in_workspaces(orch, project_id, &jj, source_branch)
                    .await;
                return Err(conflict_err);
            }
        }

        if has_remote {
            // Advance the rebased source's PR head on origin BEFORE advancing the
            // target. The rebase rewrote the source's commit id, so origin's PR
            // head must move to the rebased commit or GitHub never marks the PR
            // Merged (its old head SHA is not reachable from the advanced
            // default). Load-bearing, so fail closed: do NOT advance the target
            // on origin while the PR head still points at the abandoned commit —
            // that would land the content but leave the PR unmerged. A retry is
            // idempotent (the source already sits on the fetched tip).
            if let Err(e) = crate::jj::track_bookmark(&jj, &store, source_branch) {
                log::debug!("jj store merge: track {source_branch} (continuing): {e}");
            }
            crate::jj::push_store_bookmark(&jj, &store, source_branch).map_err(|e| {
                let recovery = if e.contains("conflict") {
                    format!(
                        "\n{}",
                        conflict_recovery_hint(source_branch, Some(target_branch))
                    )
                } else {
                    String::new()
                };
                format!(
                    "Refusing to complete the merge: could not advance the rebased source `{source_branch}` on origin ({e}). The default branch was not advanced on origin; retry the merge.{recovery}"
                )
            })?;
            reflect_child_merge_on_github(&jj, &store, target_branch)?;
        }
    } else {
        // Child→integration: rebase the source onto the live integration tip
        // before the forward-only fold, so this merge is self-contained. The
        // integration tip advances within Cairn's local fold chain as earlier
        // siblings merge into it; downstream sibling reconciliation — which used
        // to rebase the not-yet-merged siblings onto each advance — now runs
        // deferred off the synchronous merge path, so this fold can no longer
        // assume the source already sits on the current tip. Without this rebase a
        // second child merged into the same integration branch before the
        // background reconcile lands would still be based on the pre-advance tip,
        // and `merge_into_bookmark`'s forward-only `bookmark set` would refuse it
        // ("source is not a descendant of the target"). Rebasing here mirrors the
        // default-branch path and keeps sequential Coordinator child merges
        // correct regardless of reconcile timing.
        crate::jj::rebase_branch_onto(&jj, &store, source_branch, target_branch)?;
        if crate::jj::branch_has_conflict(&jj, &store, source_branch)? {
            // The rebase recorded a conflict and the integration bookmark was NOT
            // moved. The source's live workspace `@` was rebased out from under it
            // and is now stale: materialize the markers (as `reconcile_siblings`
            // does) so resolve-and-retry is actionable, and let the merge gate keep
            // blocking until they are resolved.
            materialize_source_conflict_in_workspaces(orch, project_id, &jj, source_branch).await;
            return Err(format!(
                "Refusing to merge: rebasing `{source_branch}` onto the advanced integration branch `{target_branch}` recorded a conflict.{detail}",
                detail = conflicted_history_detail(
                    &jj,
                    &store,
                    &format!("bookmarks(exact:{source_branch:?})"),
                    source_branch,
                    Some(target_branch),
                )
            ));
        }

        // Fold the source's (now-descendant) real commit into the integration
        // bookmark (forward-only).
        crate::jj::merge_into_bookmark(&jj, &store, target_branch, source_branch)?;

        if has_remote {
            // The rebase may have rewritten the source's commit id, so origin's PR
            // head must move to the rebased commit BEFORE the integration ref
            // advances — otherwise the child PR's old head SHA is unreachable from
            // the advanced integration branch and GitHub never marks it Merged.
            // Push the source first and fail closed (do NOT advance the target on
            // origin while the PR head is stale), then advance the target.
            // Defensively track each bookmark, since its `@origin` ref may have been
            // created outside this store's jj (best-effort).
            if let Err(e) = crate::jj::track_bookmark(&jj, &store, source_branch) {
                log::debug!("jj store merge: track {source_branch} (continuing): {e}");
            }
            crate::jj::push_store_bookmark(&jj, &store, source_branch).map_err(|e| {
                let recovery = if e.contains("conflict") {
                    format!(
                        "\n{}",
                        conflict_recovery_hint(source_branch, Some(target_branch))
                    )
                } else {
                    String::new()
                };
                format!(
                    "Refusing to complete the merge: could not advance the rebased source `{source_branch}` on origin ({e}). The integration branch was not advanced on origin; retry the merge.{recovery}"
                )
            })?;
            if let Err(e) = crate::jj::track_bookmark(&jj, &store, target_branch) {
                log::debug!("jj store merge: track {target_branch} (continuing): {e}");
            }
            reflect_child_merge_on_github(&jj, &store, target_branch)?;
        }
    }

    crate::jj::bookmark_commit(&jj, &store, target_branch)
        .ok_or_else(|| format!("target bookmark `{target_branch}` did not resolve after the fold"))
}

/// After `rebase_then_fold_into` records a conflict on the source bookmark and
/// aborts the default-branch fold, the source branch's live workspace `@` was
/// rebased out from under it and is now stale — its on-disk files do not yet
/// carry the conflict markers. Refresh each workspace on the source branch (as
/// `reconcile_siblings` does after a store-driven rebase) so the resolve-and-retry
/// error is actionable: the agent actually sees markers to resolve. Best-effort
/// — a refresh failure only means the agent must `jj workspace update-stale`
/// itself; it never blocks the (already-failed) merge.
async fn materialize_source_conflict_in_workspaces(
    orch: &Orchestrator,
    project_id: &str,
    jj: &crate::jj::JjEnv,
    source_branch: &str,
) {
    let worktrees = match load_worktrees_on_branch(&orch.db.local, project_id, source_branch).await
    {
        Ok(worktrees) => worktrees,
        Err(e) => {
            log::warn!(
                "jj store merge: could not load workspaces on {source_branch} to materialize conflict: {e}"
            );
            return;
        }
    };
    let mut seen = std::collections::HashSet::new();
    for worktree in worktrees {
        // Several jobs can share one physical worktree; refresh each once.
        if !seen.insert(worktree.clone()) {
            continue;
        }
        if let Err(e) = crate::jj::update_stale(jj, Path::new(&worktree)) {
            log::warn!("jj store merge: update-stale {worktree} after conflict failed: {e}");
        }
    }
}

/// Worktree paths of in-flight jobs whose branch IS `branch` (the source branch
/// of a merge). Mirrors `base_advance::load_on_branch_workspaces`' status guard
/// so a just-finished Coordinator (status `complete`) whose PR is not yet marked
/// merged is still found.
async fn load_worktrees_on_branch(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<String>, String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.worktree_path
                     FROM jobs j
                     WHERE j.project_id = ?1
                       AND j.branch = ?2
                       AND j.worktree_path IS NOT NULL
                       AND ( j.status NOT IN ('complete', 'failed', 'cancelled')
                             OR EXISTS (
                               SELECT 1 FROM merge_requests mr
                               WHERE mr.source_branch = j.branch
                                 AND mr.project_id = j.project_id
                                 AND mr.status NOT IN ('merged', 'closed')
                             ) )",
                    params![project_id.as_str(), branch.as_str()],
                )
                .await?;
            let mut worktrees = Vec::new();
            while let Some(row) = rows.next().await? {
                worktrees.push(row.text(0)?);
            }
            Ok(worktrees)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Reflect a folded child merge as Merged on GitHub by pushing the advanced
/// integration bookmark to origin. This is the single swappable seam for the
/// GitHub-state hypothesis: if live testing ever shows GitHub marks the PR Closed
/// (or is unreliable), a state-only merge-API call belongs here and nowhere else
/// — the store already owns the content by this point.
fn reflect_child_merge_on_github(
    jj: &crate::jj::JjEnv,
    store: &Path,
    integration_branch: &str,
) -> Result<(), String> {
    crate::jj::push_store_bookmark(jj, store, integration_branch)
}

/// Resolve the effective merge method for a PR. Squash is the default shape for
/// a normal PR landing on the default branch (one commit per PR), but workspace
/// PRs and memory-triage-batch PRs deliberately preserve their individual
/// commits, so they always force `"merge"` regardless of the requested method.
fn effective_merge_method(merge_context: &MergeMrContext, merge_method: Option<String>) -> String {
    let force_merge = merge_context.is_workspace || merge_context.has_triage_batch;
    if force_merge {
        "merge".to_string()
    } else {
        merge_method.unwrap_or_else(|| "squash".to_string())
    }
}

/// Resolve a project's effective (config-aware) default branch from its stored
/// value. Mirrors how worktree creation resolves the default, so the merge
/// routing and the post-merge checkout reconcile can never disagree with where
/// worktrees were based — stopping short of trusting the raw DB column the merge
/// used to read directly.
fn resolve_project_default_branch(repo_path: &str, stored_default: &str) -> String {
    let config = crate::config::project_settings::load_project_settings(Path::new(repo_path));
    crate::config::project_settings::resolve_default_branch(&config, Some(stored_default))
}

/// Whether this merge should go through GitHub's merge API rather than the local
/// jj fold. Pure so the routing is unit-testable without an `Orchestrator`:
/// `resolved_default` is the config-aware default resolved by the caller.
///
/// True only for a real remote PR (`github_pr_number` present, not a local-only
/// project) that lands on the default branch and is neither a workspace nor a
/// memory-triage PR. A Coordinator child→integration PR (`target_branch` is the
/// integration branch, not the default) and a local-only project both stay on the
/// local fold; workspace / memory-triage PRs that force the keep-every-commit
/// `merge` method are out of scope and also stay local.
fn should_route_to_github(ctx: &MergeMrContext, resolved_default: &str) -> bool {
    ctx.mr.github_pr_number.is_some()
        && !ctx.mr.is_local
        && !ctx.is_workspace
        && !ctx.has_triage_batch
        && ctx.target_branch == resolved_default
}

/// `should_route_to_github` against the project's config-aware default branch.
fn should_merge_via_github(ctx: &MergeMrContext) -> bool {
    let resolved_default = resolve_project_default_branch(&ctx.mr.repo_path, &ctx.default_branch);
    should_route_to_github(ctx, &resolved_default)
}

/// Map a `github::api::merge_pr` failure to user-facing guidance. GitHub returns
/// 405 ("not mergeable") or 409 (head changed / merge conflict) when it refuses
/// the merge; in that case the source was clean *locally* (the conflict gate
/// passed), so there are no local markers to point at — the guidance points at
/// the PR instead, distinct from the local-fold conflict message.
fn map_github_merge_error(source_branch: &str, target_branch: &str, error: String) -> String {
    if error.contains("PR: 405") || error.contains("PR: 409") {
        format!(
            "GitHub refused the merge of `{source_branch}` into `{target_branch}` — the PR has conflicts or failing required checks on GitHub's side. Resolve them on the PR, then retry the merge. (GitHub: {error})"
        )
    } else {
        format!("Failed to merge `{source_branch}` into `{target_branch}` via GitHub: {error}")
    }
}

/// Merge a remote PR through GitHub's merge API, then reconcile locally.
///
/// GitHub performs the squash-merge, closes the PR, and advances the base branch
/// on origin. The ordering is fail-closed: the GitHub call is the load-bearing
/// step, and only once it succeeds do we mark the merge request merged / resolve
/// the issue locally (`resolve_pr_node`). Local reconciliation is then proactive
/// and best-effort — origin is already authoritative, the GitHub `push` webhook
/// and the local sweep also perform this work, and the before/after commit-id
/// guards in `base_advance` make the double-fire a no-op.
async fn merge_remote_pr_via_github(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: MergeMrContext,
    merge_method: Option<String>,
    merge_started: std::time::Instant,
) -> Result<String, String> {
    let repo_path = merge_context.mr.repo_path.clone();
    let issue_id = merge_context.issue_id.clone();
    let source_branch = merge_context.source_branch.clone();
    let target_branch = merge_context.target_branch.clone();
    let pr_number = merge_context
        .mr
        .github_pr_number
        .ok_or_else(|| "GitHub merge requires a PR number".to_string())?;

    // Squash is the default landing shape (one commit per PR on the default
    // branch). The workspace / memory-triage forcing in `effective_merge_method`
    // does not apply here — those PRs never take this branch.
    let method = merge_method.unwrap_or_else(|| "squash".to_string());

    let (owner, repo) = get_owner_repo(&repo_path)?;
    let creds = get_credentials_for_owner(&orch.db.local, &owner).await?;
    let http = &*orch.services.http;

    // FAIL-CLOSED: on any failure, surface it and mark/advance NOTHING locally.
    let github_started = std::time::Instant::now();
    api::merge_pr(http, &creds, &owner, &repo, pr_number, &method)
        .await
        .map_err(|e| map_github_merge_error(&source_branch, &target_branch, e))?;
    log::info!(
        "merge_pr_for_job[{job_id}]: GitHub merge_pr took {:?}",
        github_started.elapsed()
    );

    // GitHub merged: mark the merge request merged, resolve the issue, close
    // sessions. Runs only after the GitHub call succeeded.
    let resolve_started = std::time::Instant::now();
    let closed_sessions = resolve_pr_node(orch, job_id, PrNodeResolution::Merge)
        .await
        .map_err(|error| {
            log::error!(
                "GitHub merged PR but failed to mark merge request merged for job {job_id}: {error}"
            );
            error
        })?;
    log::info!(
        "merge_pr_for_job[{job_id}]: resolve_pr_node took {:?}",
        resolve_started.elapsed()
    );
    for session_id in &closed_sessions {
        orch.process_state.remove_by_session(session_id);
    }

    if let Some(issue_id) = issue_id.as_deref() {
        // Terminal transition — wake any in-flight `cairn watch` on this issue,
        // matching the PR-webhook merge path.
        orch.wake_for_issue(issue_id).await;
    }

    // BEST-EFFORT: delete the merged source branch on GitHub. Nothing downstream
    // depends on it.
    if let Err(e) = api::delete_branch(http, &creds, &owner, &repo, &source_branch).await {
        log::warn!("Best-effort delete of merged source branch {source_branch} failed: {e}");
    }

    // BEST-EFFORT, proactive local reconcile. This path only handles remote PRs
    // whose real base is the project default branch, so the shared reconcile may
    // safely update the user's main checkout.
    let reconcile_ctx = merge_context;
    let orch_clone = orch.clone();
    tokio::spawn(async move {
        // In-flight siblings: fetch origin into the shared store and rebase each
        // onto the advanced `<base>@origin` tip. This is the external-advance
        // reconcile the `push` webhook would also drive; the commit-id guard makes
        // the double-fire idempotent.
        if let Err(e) = crate::orchestrator::base_advance::reconcile_external_default_advance(
            &orch_clone,
            &reconcile_ctx.project_id,
            &target_branch,
        )
        .await
        {
            log::warn!("Post-GitHub-merge sibling reconcile failed: {e}");
        }
        // File capture, user-checkout fast-forward pull (forced — no local fold
        // moved it), worktree teardown, PR refresh.
        reconcile_after_merge(orch_clone, reconcile_ctx, true).await;
    });

    log::info!(
        "merge_pr_for_job[{job_id}]: GitHub merge path took {:?}",
        merge_started.elapsed()
    );
    Ok("PR merged via GitHub".to_string())
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

    // Per-phase timing for the synchronous merge path. The downstream sibling
    // reconcile no longer runs here (it is spawned inside `resolve_pr_node`), so
    // these spans bound only the work the "merging" button actually waits on:
    // the conflict gate, the store fold + origin round-trips, and the DB/
    // execution-recompute resolution. Logged at `info` so a live merge reports
    // real numbers from the user's own instance.
    let merge_started = std::time::Instant::now();

    // Merge boundary: refuse a jj-conflicted source bookmark before the store
    // fold. Fails closed regardless of cached/rendered mergeable state — a
    // conflicted child must never be folded into integration. Under
    // store-owns-merge the old staleness window largely dissolves: a
    // cleanly-rebased sibling is pushed immediately, so origin's PR head tracks
    // the local rebased tip rather than lagging a stale pre-rebase commit.
    let gate_started = std::time::Instant::now();
    if let Some(report) = source_conflict_report(
        &orch.jj_binary_path,
        &orch.config_dir,
        &repo_path,
        &merge_context.source_branch,
        Some(&merge_context.target_branch),
    ) {
        return Err(format!(
            "Refusing to merge: the jj source bookmark `{source}` carries a recorded conflict — a conflicted history cannot fold into `{target}`.\n{commits}\n{recovery}",
            source = merge_context.source_branch,
            target = merge_context.target_branch,
            commits = format_conflicted_commits(&report.commits),
            recovery = conflict_recovery_hint(
                &merge_context.source_branch,
                Some(&merge_context.target_branch)
            ),
        ));
    }
    let resolved_default =
        resolve_project_default_branch(&repo_path, &merge_context.default_branch);
    if merge_context.target_branch == resolved_default {
        assert_main_checkout_clean_for_default_merge(&*orch.services.git, &repo_path)?;
    }
    log::info!(
        "merge_pr_for_job[{job_id}]: merge gates took {:?}",
        gate_started.elapsed()
    );

    // Route a real remote PR landing on the project default branch through
    // GitHub's merge API (GitHub squash-merges, closes the PR, and advances the
    // base on origin; Cairn then reconciles locally). The local jj fold below is
    // kept only where there is no GitHub PR to merge through: local-only projects
    // and Coordinator child→integration PRs, plus workspace / memory-triage PRs
    // that deliberately preserve every commit. The conflict gate above has
    // already run for both paths.
    if should_merge_via_github(&merge_context) {
        return merge_remote_pr_via_github(
            orch,
            job_id,
            merge_context,
            merge_method,
            merge_started,
        )
        .await;
    }

    // The shared jj store owns the merge: fold the child's commit into the
    // integration bookmark and (for a remote project) push it, which advances
    // origin and marks the child PR Merged out-of-band. A no-remote jj project
    // folds locally and skips the push. The method selects the *shape* that lands
    // on the default branch: `squash` (the default) collapses the source to one
    // commit before the fold; `merge` (forced for workspace / memory-triage PRs)
    // keeps every sealed commit.
    let method = effective_merge_method(&merge_context, merge_method);
    let fold_started = std::time::Instant::now();
    // Serialize the fold behind the per-store mutex so a merge-fold and a
    // base-advance reconcile on the same shared store never run jj ops
    // concurrently (which would mint divergent conflicted copies). The detached
    // downstream reconcile spawned later inside `resolve_pr_node` acquires the
    // same lock, after this guard drops — no nested acquisition.
    let merge_store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    let merge_store_lock = orch.jj_store_lock(&merge_store);
    let merged_commit = {
        let _store_guard = merge_store_lock.lock().await;
        store_merge_child(orch, &merge_context, &method).await?
    };
    persist_merged_commit(&orch.db.local, &merge_context.mr.mr_id, &merged_commit).await?;
    log::info!(
        "merge_pr_for_job[{job_id}]: store_merge_child (fold + origin) took {:?}",
        fold_started.elapsed()
    );

    let resolve_started = std::time::Instant::now();
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
    log::info!(
        "merge_pr_for_job[{job_id}]: resolve_pr_node took {:?}",
        resolve_started.elapsed()
    );
    for session_id in &closed_sessions {
        orch.process_state.remove_by_session(session_id);
    }

    if let Some(issue_id) = issue_id.as_deref() {
        // Terminal transition — wake any in-flight `cairn watch` on this issue,
        // matching the PR-webhook merge path. Typed emit re-reads the live
        // projection and sends `Resolved` since the issue just became terminal.
        orch.wake_for_issue(issue_id).await;
    }

    // Background reconciliation, shared with the GitHub webhook merge path. The
    // local fold already re-attached and fast-forwarded the checkout, so the pull
    // is governed by `pull_on_merge` (not forced).
    tokio::spawn(reconcile_after_merge(orch.clone(), merge_context, false));

    log::info!(
        "merge_pr_for_job[{job_id}]: synchronous merge path took {:?} (downstream reconcile deferred)",
        merge_started.elapsed()
    );
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

    /// Squash is the default landing shape; workspace and memory-triage-batch PRs
    /// always force `merge` so their individual commits survive, even when the
    /// caller asks for another method.
    #[test]
    fn effective_merge_method_defaults_squash_and_forces_merge_for_canon() {
        // Workspace PR forces merge regardless of the requested method.
        assert_eq!(
            effective_merge_method(&merge_context(true, false), Some("squash".to_string())),
            "merge"
        );
        // Memory-triage-batch PR forces merge regardless of the requested method.
        assert_eq!(
            effective_merge_method(&merge_context(false, true), Some("rebase".to_string())),
            "merge"
        );
        // A normal PR with no requested method defaults to squash.
        assert_eq!(
            effective_merge_method(&merge_context(false, false), None),
            "squash"
        );
        // An explicit method on a normal PR is honored.
        assert_eq!(
            effective_merge_method(&merge_context(false, false), Some("merge".to_string())),
            "merge"
        );
    }

    /// A remote PR landing on the default branch routes to GitHub; everything
    /// else (local-only, child→integration, workspace, memory-triage) stays on
    /// the local jj fold.
    #[test]
    fn should_route_to_github_only_for_remote_default_branch_pr() {
        // Base case: a real remote PR on the default branch.
        let mut ctx = merge_context(false, false);
        ctx.mr.github_pr_number = Some(7);
        assert!(should_route_to_github(&ctx, "main"));

        // Local-only project (no GitHub PR) stays on the local fold.
        let mut local = ctx.clone();
        local.mr.github_pr_number = None;
        assert!(!should_route_to_github(&local, "main"));
        let mut is_local = ctx.clone();
        is_local.mr.is_local = true;
        assert!(!should_route_to_github(&is_local, "main"));

        // Coordinator child→integration PR (target ≠ default) stays local.
        let mut child = ctx.clone();
        child.target_branch = "agent/CAIRN-1-coordinator-0".to_string();
        assert!(!should_route_to_github(&child, "main"));

        // Workspace and memory-triage PRs stay on the local fold (keep-every-commit).
        let mut workspace = ctx.clone();
        workspace.is_workspace = true;
        assert!(!should_route_to_github(&workspace, "main"));
        let mut triage = ctx.clone();
        triage.has_triage_batch = true;
        assert!(!should_route_to_github(&triage, "main"));

        // A stale stored default that disagrees with the real base routes off the
        // PR's actual base: target "staging" against resolved "staging" routes,
        // against a stale "main" does not.
        let mut staging = ctx.clone();
        staging.target_branch = "staging".to_string();
        assert!(should_route_to_github(&staging, "staging"));
        assert!(!should_route_to_github(&staging, "main"));
    }

    #[test]
    fn dirty_tracked_paths_parses_only_tracked_porcelain_entries() {
        let paths = crate::pr_data::helpers::dirty_tracked_paths_from_porcelain(
            " M src/lib.rs\n?? scratch.txt\n!! ignored.log\nR  old.rs -> new.rs\nA  src-tauri/Cargo.lock\n",
        );
        assert_eq!(
            paths,
            vec![
                "src/lib.rs".to_string(),
                "new.rs".to_string(),
                "src-tauri/Cargo.lock".to_string(),
            ]
        );
    }

    #[test]
    fn main_checkout_dirty_gate_allows_only_regenerable_lockfile_churn() {
        use crate::services::testing::MockGitClient;

        let mut clean = MockGitClient::new();
        clean.expect_status().returning(|_| Ok(String::new()));
        assert!(assert_main_checkout_clean_for_default_merge(&clean, "/repo").is_ok());

        let mut lockfile_only = MockGitClient::new();
        lockfile_only
            .expect_status()
            .returning(|_| Ok(" M src-tauri/Cargo.lock".to_string()));
        assert!(assert_main_checkout_clean_for_default_merge(&lockfile_only, "/repo").is_ok());

        let mut real_edit = MockGitClient::new();
        real_edit
            .expect_status()
            .returning(|_| Ok(" M src-tauri/Cargo.lock\n M src/lib.rs".to_string()));
        let error = assert_main_checkout_clean_for_default_merge(&real_edit, "/repo").unwrap_err();
        assert!(error.contains("Refusing to merge"), "{error}");
        assert!(error.contains("/repo"), "{error}");
        assert!(error.contains("src/lib.rs"), "{error}");
        assert!(!error.contains("src-tauri/Cargo.lock"), "{error}");
    }

    #[test]
    fn main_checkout_reconcile_runs_only_for_default_branch_advances() {
        assert!(should_reconcile_main_checkout_after_merge("main", "main"));
        assert!(!should_reconcile_main_checkout_after_merge(
            "agent/CAIRN-1-coordinator-0",
            "main"
        ));
    }

    /// A GitHub 405/409 refusal points at the PR (no local markers exist); any
    /// other failure is reported as a plain GitHub merge error.
    #[test]
    fn map_github_merge_error_distinguishes_refusal_from_other_failures() {
        let refusal = map_github_merge_error(
            "feature",
            "main",
            "Failed to merge PR: 405 - not mergeable".to_string(),
        );
        assert!(refusal.contains("GitHub refused"), "{refusal}");
        assert!(
            refusal.contains("feature") && refusal.contains("main"),
            "{refusal}"
        );

        let conflict = map_github_merge_error(
            "feature",
            "main",
            "Failed to merge PR: 409 - head changed".to_string(),
        );
        assert!(conflict.contains("GitHub refused"), "{conflict}");

        let other = map_github_merge_error(
            "feature",
            "main",
            "Failed to merge PR: 500 - server error".to_string(),
        );
        assert!(!other.contains("GitHub refused"), "{other}");
        assert!(other.contains("via GitHub"), "{other}");
    }

    /// The merge gate trusts jj's recorded conflict state: a sibling whose
    /// auto-rebase recorded a conflict is reported (with its commits and files),
    /// a cleanly-rebased sibling is not, and a non-jj project never gates.
    /// Self-skips when jj is unresolvable.
    ///
    /// Serialized on the shared `jj` key: like every test that drives the real
    /// jj binary, it must not run concurrently with another, or the spawned jj
    /// subprocesses contend and a `jj config set --repo` intermittently panics.
    #[test]
    #[serial_test::serial(jj)]
    fn source_conflict_report_gates_and_enumerates_recorded_conflicts() {
        let bin = match std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                crate::env::command("jj")
                    .arg("--version")
                    .output()
                    .ok()
                    .filter(|o| o.status.success())
                    .map(|_| "jj".to_string())
            }) {
            Some(bin) => bin,
            None => {
                eprintln!("skipping jj_source_branch_conflicted_gates_only_recorded_conflicts: jj not resolvable");
                return;
            }
        };
        let run_git = |dir: &std::path::Path, args: &[&str]| {
            assert!(
                crate::env::git()
                    .args(args)
                    .current_dir(dir)
                    .status()
                    .unwrap()
                    .success(),
                "git {args:?}"
            );
        };
        let home = tempfile::tempdir().unwrap();
        let proj = tempfile::tempdir().unwrap();
        let wts = tempfile::tempdir().unwrap();

        run_git(proj.path(), &["init", "-q", "-b", "main"]);
        run_git(proj.path(), &["config", "user.email", "t@e.com"]);
        run_git(proj.path(), &["config", "user.name", "T"]);
        std::fs::write(proj.path().join("shared.rs"), "base\n").unwrap();
        run_git(proj.path(), &["add", "-A"]);
        run_git(proj.path(), &["commit", "-q", "-m", "base"]);

        // Build the shared store at the path the gate computes from config_dir.
        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = crate::jj::project_store_dir(home.path(), proj.path());
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
        let cfg = home.path().join("jj").join("config.toml");
        let jj_cfg = |cwd: &std::path::Path, args: &[&str]| {
            let out = crate::env::command(&bin)
                .args(args)
                .current_dir(cwd)
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
        let int = "agent/CAIRN-1940-coordinator-0";
        jj_cfg(&store, &["bookmark", "create", "-r", "main", int]);

        let overlap = "agent/CAIRN-1-builder-0";
        let clean = "agent/CAIRN-2-builder-0";
        let ws_o = wts.path().join("o");
        let ws_c = wts.path().join("c");
        crate::jj::add_workspace(&jj, &store, &ws_o, overlap, int, None).unwrap();
        crate::jj::add_workspace(&jj, &store, &ws_c, clean, int, None).unwrap();
        std::fs::write(ws_o.join("shared.rs"), "overlap-change\n").unwrap();
        crate::jj::seal(&jj, &ws_o, "overlap", None).unwrap();
        std::fs::write(ws_c.join("other.rs"), "clean-change\n").unwrap();
        crate::jj::seal(&jj, &ws_c, "clean", None).unwrap();

        // Advance the integration tip conflictingly, then reconcile (rebase).
        jj_cfg(&store, &["new", int]);
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj_cfg(&store, &["describe", "-m", "advance"]);
        jj_cfg(&store, &["bookmark", "set", int, "-r", "@"]);
        crate::jj::reconcile_siblings(
            &jj,
            &store,
            int,
            &[
                (overlap.to_string(), ws_o.clone()),
                (clean.to_string(), ws_c.clone()),
            ],
        )
        .unwrap();

        let proj_path = proj.path().to_string_lossy().to_string();
        let report = source_conflict_report(&bin, home.path(), &proj_path, overlap, Some(int))
            .expect("a recorded-conflict sibling must gate the merge");
        // The report names the offending commit(s) and conflicted file(s).
        assert!(
            !report.commits.is_empty(),
            "the conflicted source enumerates its commits"
        );
        assert!(
            report
                .commits
                .iter()
                .any(|c| c.files.iter().any(|f| f == "shared.rs")),
            "the conflicted file is named in the report"
        );
        // The shared formatters turn the report into the diagnostic surfaces all
        // share: a commit bullet and a concrete recovery path.
        let listing = format_conflicted_commits(&report.commits);
        assert!(
            listing.starts_with("- ") && listing.contains("shared.rs"),
            "the formatted listing bullets the conflicted commit and file: {listing}"
        );
        let recovery = conflict_recovery_hint(overlap, Some(int));
        assert!(
            recovery.contains("resolve-at-base") && recovery.contains(int),
            "the recovery hint names the resolve-at-base path: {recovery}"
        );

        assert!(
            source_conflict_report(&bin, home.path(), &proj_path, clean, Some(int)).is_none(),
            "a cleanly-rebased sibling must not gate"
        );

        // A non-jj project (no config marker) never gates.
        let git_proj = tempfile::tempdir().unwrap();
        assert!(source_conflict_report(
            &bin,
            home.path(),
            &git_proj.path().to_string_lossy(),
            overlap,
            Some(int),
        )
        .is_none());
    }

    /// The shared formatters render a conflicted-commit list as bullets (commit,
    /// description, files) and a concrete resolve-at-base recovery sentence — the
    /// identical wording every surface (summary, artifact, merge error) emits.
    /// Pure: no jj, always runs.
    #[test]
    fn conflict_formatters_render_bullets_and_recovery() {
        let commits = vec![
            crate::jj::ConflictedCommit {
                commit_id: "27e4383e".into(),
                change_id: "ppvnsuwu".into(),
                description: "merged".into(),
                files: vec!["f.txt".into(), "g.rs".into()],
            },
            crate::jj::ConflictedCommit {
                commit_id: "deadbeef".into(),
                change_id: "zzzzzzzz".into(),
                description: String::new(),
                files: vec![],
            },
        ];
        assert_eq!(
            format_conflicted_commits(&commits),
            "- 27e4383e (\"merged\"): f.txt, g.rs\n- deadbeef"
        );

        let recovery = conflict_recovery_hint("agent/CAIRN-1-builder-0", Some("main"));
        assert!(recovery.contains("agent/CAIRN-1-builder-0"));
        assert!(recovery.contains("`main`"));
        assert!(recovery.contains("resolve-at-base"));
        // An unknown target falls back to a generic placeholder, never panics.
        assert!(conflict_recovery_hint("src", None).contains("`target`"));
    }

    use crate::db::DbState;
    use crate::services::testing::{MockGitClient, TestServicesBuilder};
    use crate::storage::{LocalDb, SearchIndex};
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
}
