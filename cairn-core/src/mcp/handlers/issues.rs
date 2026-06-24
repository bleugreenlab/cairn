//! Issue-related MCP handlers.
//!
//! Handles: update_issue

use crate::orchestrator::Orchestrator;
use turso::params;

use super::{parse_issue_identifier, ProjectContext};
use crate::issues::relations;
use crate::labels::attach;
use crate::mcp::types::{McpCallbackRequest, UpdateIssuePayload};
use crate::models::{Issue, IssueAttention, IssueProgress, IssueStatus};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

// ============================================================================
// Handlers
// ============================================================================

/// Optional create+start request parsed from the issue-create payload's
/// `execution` key. Both fields default like an executions-collection append:
/// `recipe` to the project (then workspace) default manual recipe, `backend` to
/// the recipe/agent default.
#[derive(Debug, Default, Clone)]
pub struct CreateExecutionSpec {
    pub recipe: Option<String>,
    pub backend: Option<String>,
}

/// Persist a new issue and emit the standard side effects (embed, sync,
/// db-change), returning the created row so callers can chain follow-on work
/// (e.g. starting an execution) without re-querying.
async fn insert_issue_with_context(
    orch: &Orchestrator,
    ctx: &ProjectContext,
    title: String,
    description: Option<String>,
    parent_uri: Option<String>,
    labels: Option<Vec<String>>,
    caller_run_id: Option<String>,
) -> Result<Issue, String> {
    let services = &orch.services;
    let embed_text = description.clone().unwrap_or_default();
    let issue = create_issue_row(
        &orch.db.local,
        &ctx.project_id,
        title,
        description,
        parent_uri,
        labels,
        caller_run_id,
    )
    .await
    .map_err(|e| format!("Failed to create issue: {}", e))?;

    let issue_uri = cairn_common::uri::build_issue_uri(&ctx.project_key, issue.number);
    if let Err(error) = crate::orchestrator::wakes::seed_default_child_subscription_for_issue(
        &orch.db.local,
        &issue.id,
        &issue_uri,
    )
    .await
    {
        log::warn!(
            "failed to seed wake subscription for created child issue {}: {}",
            issue_uri,
            error
        );
    }
    orch.enqueue_resource_embed(&issue_uri, embed_text);

    orch.sync(crate::sync::SyncMessage::Issue((&issue).into()));
    if let Err(e) = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    ) {
        log::error!("Failed to emit db-change event: {}", e);
    }
    if !issue.labels.is_empty() {
        if let Err(e) = services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "issue_labels", "action": "insert"}),
        ) {
            log::error!("Failed to emit db-change event: {}", e);
        }
    }

    Ok(issue)
}

fn created_issue_summary(ctx: &ProjectContext, issue: &Issue) -> String {
    format!(
        "Created issue {}-{}: \"{}\"",
        ctx.project_key, issue.number, issue.title
    )
}

async fn create_issue_with_context(
    orch: &Orchestrator,
    ctx: ProjectContext,
    title: String,
    description: Option<String>,
) -> Result<String, String> {
    let issue = insert_issue_with_context(orch, &ctx, title, description, None, None, None).await?;
    Ok(created_issue_summary(&ctx, &issue))
}

pub async fn create_issue_from_request(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: Option<&str>,
    title: String,
    description: Option<String>,
) -> Result<String, String> {
    let ctx = if let Some(key) = project_key {
        lookup_project_by_key(&orch.db.local, key).await?
    } else {
        lookup_project_context(&orch.db.local, request).await?
    };
    create_issue_with_context(orch, ctx, title, description).await
}

/// Outcome of creating an issue through the resource-mutation path: the
/// human-readable summary plus the structured identifiers UI renderers need to
/// resolve the created issue (drag target, live-node lookup) without parsing
/// the summary string.
pub struct CreatedIssueOutcome {
    pub summary: String,
    pub issue_id: String,
    pub project_key: String,
    pub number: i32,
    pub uri: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn create_issue_in_project(
    orch: &Orchestrator,
    project_key: &str,
    title: String,
    description: Option<String>,
    labels: Option<Vec<String>>,
    execution: Option<CreateExecutionSpec>,
    parent_uri: Option<String>,
    caller_run_id: Option<String>,
) -> Result<CreatedIssueOutcome, String> {
    let ctx = lookup_project_by_key(&orch.db.local, project_key).await?;
    let issue = insert_issue_with_context(
        orch,
        &ctx,
        title,
        description,
        parent_uri,
        labels,
        caller_run_id,
    )
    .await?;
    let summary = created_issue_summary(&ctx, &issue);
    let issue_id = issue.id.clone();
    let uri = cairn_common::uri::build_issue_uri(&ctx.project_key, issue.number);
    let project_key = ctx.project_key.clone();
    let number = issue.number;

    let summary = match execution {
        None => summary,
        // Create+start is "create then start", not one transaction: the issue is
        // already durable here. Reuse the executions-collection start path so the
        // recipe/backend resolution, `initiated_via="external"` stamp, and watcher
        // wake are identical. If the start fails (bad recipe id, no default recipe),
        // surface an error that names the created issue so the caller can retry the
        // start instead of silently dropping it.
        Some(spec) => match super::executions::start_execution_from_collection(
            orch,
            &ctx.project_key,
            issue.number,
            spec.recipe.as_deref(),
            spec.backend.as_deref(),
        )
        .await
        {
            Ok(start_msg) => format!("{summary}\n{start_msg}"),
            Err(error) => {
                return Err(format!(
                    "{summary}\nIssue created, but starting the execution failed: {error} \
                     Append {{recipe?, backend?}} to {} to start it.",
                    cairn_common::uri::build_issue_executions_uri(&ctx.project_key, issue.number)
                ))
            }
        },
    };

    Ok(CreatedIssueOutcome {
        summary,
        issue_id,
        project_key,
        number,
        uri,
    })
}

pub async fn create_issue_external_in_project(
    orch: &Orchestrator,
    project_key: &str,
    title: String,
    description: Option<String>,
) -> Result<String, String> {
    let ctx = lookup_project_by_key(&orch.db.local, project_key).await?;
    create_issue_with_context(orch, ctx, title, description).await
}

async fn lookup_project_by_key(db: &LocalDb, key: &str) -> Result<ProjectContext, String> {
    let key = key.to_uppercase();
    let lookup_key = key.clone();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, key FROM projects WHERE key = ?1 LIMIT 1",
                    (lookup_key.as_str(),),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok::<_, DbError>(ProjectContext {
                        project_id: row.text(0)?,
                        project_key: row.text(1)?,
                    })
                })
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("No project found with key '{}'", lookup_key)))
        })
    })
    .await
    .map_err(|_| format!("No project found with key '{}'", key))
}

async fn lookup_project_context(
    db: &LocalDb,
    request: &McpCallbackRequest,
) -> Result<ProjectContext, String> {
    if let Some(run_id) = request.run_id.as_deref() {
        let run_id = run_id.to_string();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT p.id, p.key
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        JOIN projects p ON j.project_id = p.id
                        WHERE r.id = ?1
                        LIMIT 1
                        ",
                        (run_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok::<_, DbError>(ProjectContext {
                            project_id: row.text(0)?,
                            project_key: row.text(1)?,
                        })
                    })
                    .transpose()?
                    .ok_or_else(|| DbError::Row(format!("No run found with id '{}'", run_id)))
            })
        })
        .await
        .map_err(|e| e.to_string())
    } else {
        let cwd = request.cwd.clone();
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT p.id, p.key
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        JOIN projects p ON j.project_id = p.id
                        WHERE r.status IN ('starting', 'live')
                          AND (j.worktree_path = ?1 OR (p.repo_path = ?1 AND j.issue_id IS NULL))
                        ORDER BY
                            CASE WHEN j.worktree_path = ?1 THEN 0 ELSE 1 END,
                            r.created_at DESC
                        LIMIT 1
                        ",
                        (cwd.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok::<_, DbError>(ProjectContext {
                            project_id: row.text(0)?,
                            project_key: row.text(1)?,
                        })
                    })
                    .transpose()?
                    .ok_or_else(|| DbError::Row(format!("No active run found for path '{}'", cwd)))
            })
        })
        .await
        .map_err(|e| e.to_string())
    }
}

/// Resolve the root job of a run's job tree: the run's job walked up
/// `parent_job_id` until a recipe-root (`parent_job_id IS NULL`). Recording
/// this root as a child issue's spawner means a delegated sub-task that spawns
/// the child still points the wake at the owning coordinator, not its own
/// (possibly completed) job. Returns `None` when the run has no job.
async fn root_job_for_run(conn: &turso::Connection, run_id: &str) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
            params![run_id],
        )
        .await?;
    let Some(mut job_id) = (match rows.next().await? {
        Some(row) => row.opt_text(0)?,
        None => None,
    }) else {
        return Ok(None);
    };

    // Walk up to the root. Bounded by tree depth; the guard defends against a
    // cycle in malformed data rather than ever being hit in practice.
    for _ in 0..64 {
        let mut prows = conn
            .query(
                "SELECT parent_job_id FROM jobs WHERE id = ?1 LIMIT 1",
                params![job_id.as_str()],
            )
            .await?;
        match prows.next().await? {
            Some(row) => match row.opt_text(0)? {
                Some(parent_id) => job_id = parent_id,
                None => break,
            },
            None => break,
        }
    }
    Ok(Some(job_id))
}

async fn create_issue_row(
    db: &LocalDb,
    project_id: &str,
    title: String,
    description: Option<String>,
    parent_uri: Option<String>,
    labels: Option<Vec<String>>,
    caller_run_id: Option<String>,
) -> DbResult<Issue> {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let title = title.clone();
        let description = description.clone();
        let parent_uri = parent_uri.clone();
        let labels = labels.clone();
        let caller_run_id = caller_run_id.clone();
        Box::pin(async move {
            let parent_issue_id = if let Some(parent_uri) = parent_uri.as_deref() {
                let resolved = relations::resolve_issue_uri(conn, parent_uri)
                    .await?
                    .ok_or_else(|| DbError::Row(format!("parent issue not found: {parent_uri}")))?;
                let mut project_rows = conn
                    .query(
                        "SELECT project_id FROM issues WHERE id = ?1 LIMIT 1",
                        params![resolved.issue_id.as_str()],
                    )
                    .await?;
                let parent_project_id = project_rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row(format!("parent issue not found: {parent_uri}")))?
                    .text(0)?;
                if parent_project_id != project_id {
                    return Err(DbError::Row(format!(
                        "parent issue must be in the same project: {parent_uri}"
                    )));
                }
                Some(resolved.issue_id)
            } else {
                None
            };

            // Record the spawning coordinator job for a child issue so
            // child-attention wakes resume it directly. We record the ROOT of
            // the caller's job tree (the coordinator / recipe-root), not the
            // literal caller job: if a delegated sub-task spawns the child
            // issue, the wake target must still be the coordinator, never the
            // sub-task's own job (which may be a completed job that retains its
            // session). Only set when an agent (a run bound to a job) created
            // the issue under a parent.
            let parent_job_id = match (parent_issue_id.as_deref(), caller_run_id.as_deref()) {
                (Some(_), Some(run_id)) => root_job_for_run(conn, run_id).await?,
                _ => None,
            };

            let mut rows = conn
                .query(
                    "SELECT next_issue_number FROM projects WHERE id = ?1",
                    (project_id.as_str(),),
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row(format!("project not found: {}", project_id)))?;
            let number = row.opt_i64(0)?.unwrap_or(1) as i32;
            let now = chrono::Utc::now().timestamp() as i32;
            let id = uuid::Uuid::new_v4().to_string();

            conn.execute(
                "UPDATE projects SET next_issue_number = ?1, updated_at = ?2 WHERE id = ?3",
                params![number + 1, now, project_id.as_str()],
            )
            .await?;

            conn.execute(
                "
                INSERT INTO issues (
                    id, project_id, number, title, description, status, progress,
                    attention, priority, created_at, updated_at, model, parent_issue_id,
                    parent_job_id
                )
                VALUES (?1, ?2, ?3, ?4, ?5, 'backlog', 'backlog', 'none', 0, ?6, ?6, NULL, ?7, ?8)
                ",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    number,
                    title.as_str(),
                    description.as_deref(),
                    now,
                    parent_issue_id.as_deref(),
                    parent_job_id.as_deref()
                ],
            )
            .await?;

            if let Some(labels) = labels {
                attach::replace_issue_labels(conn, &id, &labels, now as i64)
                    .await
                    .map_err(DbError::Row)?;
            }

            load_issue_by_id(conn, &id).await.or_else(|_| {
                Ok(Issue {
                    id,
                    project_id,
                    number,
                    title,
                    description: description.unwrap_or_default(),
                    status: IssueStatus::Backlog,
                    progress: IssueProgress::Backlog,
                    attention: IssueAttention::None,
                    priority: 0,
                    completed_at: None,
                    dismissed_at: None,
                    created_at: now as i64,
                    updated_at: now as i64,
                    backend_override: None,
                    merged_at: None,
                    closed_at: None,
                    parent_issue_id,
                    unmet_dependency_count: 0,
                    depends_on: Vec::new(),
                    unmet_depends_on: Vec::new(),
                    labels: Vec::new(),
                })
            })
        })
    })
    .await
}

pub struct IssuePatchFields {
    pub title: Option<String>,
    pub description: Option<String>,
    pub depends_on: Option<Vec<String>>,
    pub labels: Option<Vec<String>>,
    /// Resolution to apply via [`crate::issues::status::update_status`]. Only
    /// `merged`/`closed` are accepted at the URI layer; callers validate before
    /// setting this. `None` leaves the resolution untouched.
    pub status: Option<String>,
    /// Re-parenting. `None` leaves parent untouched; `Some(None)` orphans the
    /// issue (clears parent); `Some(Some(uri))` adopts under the given canonical
    /// issue URI. The URI is resolved to an issue id, validated same-project, and
    /// checked for parent-chain cycles inside the update transaction.
    pub parent: Option<Option<String>>,
}

async fn update_issue_row(
    db: &LocalDb,
    project_id: &str,
    issue_num: i32,
    patch: IssuePatchFields,
    caller_run_id: Option<String>,
) -> DbResult<Option<Issue>> {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let title = patch.title.clone();
        let description = patch.description.clone();
        let depends_on = patch.depends_on.clone();
        let labels = patch.labels.clone();
        let parent = patch.parent.clone();
        let caller_run_id = caller_run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM issues WHERE project_id = ?1 AND number = ?2",
                    params![project_id.as_str(), issue_num],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let issue_id = row.text(0)?;
            let now = chrono::Utc::now().timestamp() as i32;

            conn.execute(
                "
                UPDATE issues
                SET title = COALESCE(?1, title),
                    description = CASE WHEN ?2 IS NULL THEN description ELSE ?2 END,
                    updated_at = ?3
                WHERE id = ?4
                ",
                params![
                    title.as_deref(),
                    description.as_deref(),
                    now,
                    issue_id.as_str()
                ],
            )
            .await?;

            if let Some(dependencies) = depends_on {
                relations::replace_dependencies(conn, &issue_id, &dependencies, now as i64)
                    .await
                    .map_err(DbError::Row)?;
            }
            if let Some(labels) = labels {
                attach::replace_issue_labels(conn, &issue_id, &labels, now as i64)
                    .await
                    .map_err(DbError::Row)?;
            }

            match parent {
                // Parent left untouched.
                None => {}
                // Orphan: clear parent and its stale spawner. A parent_job_id
                // without a parent_issue_id is meaningless to wake routing.
                Some(None) => {
                    conn.execute(
                        "UPDATE issues SET parent_issue_id = NULL, parent_job_id = NULL, updated_at = ?1 WHERE id = ?2",
                        params![now, issue_id.as_str()],
                    )
                    .await?;
                }
                Some(Some(parent_uri)) => {
                    let resolved = relations::resolve_issue_uri(conn, &parent_uri)
                        .await?
                        .ok_or_else(|| {
                            DbError::Row(format!("parent issue not found: {parent_uri}"))
                        })?;
                    if resolved.issue_id == issue_id {
                        return Err(DbError::Row("an issue cannot be its own parent".into()));
                    }
                    // Same-project: a cross-project parent would branch from
                    // another repo. Mirrors the create path's guard.
                    let mut project_rows = conn
                        .query(
                            "SELECT project_id FROM issues WHERE id = ?1 LIMIT 1",
                            params![resolved.issue_id.as_str()],
                        )
                        .await?;
                    let parent_project_id = project_rows
                        .next()
                        .await?
                        .ok_or_else(|| {
                            DbError::Row(format!("parent issue not found: {parent_uri}"))
                        })?
                        .text(0)?;
                    if parent_project_id != project_id {
                        return Err(DbError::Row(format!(
                            "parent issue must be in the same project: {parent_uri}"
                        )));
                    }
                    relations::validate_no_parent_cycle(conn, &issue_id, &resolved.issue_id)
                        .await
                        .map_err(DbError::Row)?;
                    // Mirror create: record the adopting coordinator's root job
                    // for wake routing. When the caller is not a job-bound run,
                    // leave it NULL and let parent-wake fall back to the parent
                    // issue's root coordinator job.
                    let parent_job_id = match caller_run_id.as_deref() {
                        Some(run_id) => root_job_for_run(conn, run_id).await?,
                        None => None,
                    };
                    conn.execute(
                        "UPDATE issues SET parent_issue_id = ?1, parent_job_id = ?2, updated_at = ?3 WHERE id = ?4",
                        params![
                            resolved.issue_id.as_str(),
                            parent_job_id.as_deref(),
                            now,
                            issue_id.as_str()
                        ],
                    )
                    .await?;
                }
            }

            load_issue_by_id(conn, &issue_id).await.map(Some)
        })
    })
    .await
}

pub async fn update_issue_by_project_number(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    issue_num: i32,
    patch: IssuePatchFields,
) -> Result<String, String> {
    if patch.title.is_none()
        && patch.description.is_none()
        && patch.depends_on.is_none()
        && patch.labels.is_none()
        && patch.status.is_none()
        && patch.parent.is_none()
    {
        return Err("No fields to update".to_string());
    }

    let ctx = if project_key.is_empty() {
        lookup_project_context(&orch.db.local, request).await?
    } else {
        lookup_project_by_key(&orch.db.local, project_key).await?
    };

    let embed_description = patch.description.clone();
    let labels_changed = patch.labels.is_some();
    let parent_changed = patch.parent.is_some();
    let status = patch.status.clone();
    let issue = update_issue_row(
        &orch.db.local,
        &ctx.project_id,
        issue_num,
        patch,
        request.run_id.clone(),
    )
    .await
    .map_err(|e| format!("Failed to update issue: {e}"))?
    .ok_or_else(|| format!("Issue {}-{} not found", ctx.project_key, issue_num))?;

    // Re-embed only when the description was part of this update; a title-only
    // or dependency-only patch leaves the stored description untouched.
    let issue_uri = cairn_common::uri::build_issue_uri(&ctx.project_key, issue.number);
    if let Some(description) = embed_description {
        orch.enqueue_resource_embed(&issue_uri, description);
    }

    if parent_changed {
        if let Err(error) =
            crate::orchestrator::wakes::reconcile_default_child_subscription_for_issue(
                &orch.db.local,
                &issue.id,
                &issue_uri,
            )
            .await
        {
            log::warn!(
                "failed to reconcile wake subscription for patched child issue {}: {}",
                issue_uri,
                error
            );
        }
    }

    orch.sync(crate::sync::SyncMessage::Issue((&issue).into()));
    if let Err(e) = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    ) {
        log::error!("Failed to emit db-change event: {}", e);
    }
    if labels_changed {
        if let Err(e) = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "issue_labels", "action": "update"}),
        ) {
            log::error!("Failed to emit db-change event: {}", e);
        }
    }

    // Apply a resolution change last so update_status's own sync/emit carries the
    // final issue state; the field sync above would otherwise overwrite it with a
    // stale (pre-resolution) snapshot.
    if let Some(status) = status.as_deref() {
        crate::issues::status::update_status(orch, &issue.id, status).await?;
    }

    Ok(format!(
        "Patched issue {}-{}{}",
        ctx.project_key,
        issue.number,
        status.map(|s| format!(" (status={s})")).unwrap_or_default()
    ))
}

/// Handle update_issue tool call
pub async fn handle_update_issue(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: UpdateIssuePayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    let (project_key_opt, issue_num) = match parse_issue_identifier(&payload.issue_number) {
        Some(parsed) => parsed,
        None => {
            return format!(
                "Invalid issue number: '{}'. Use formats like: 37, #37, or CAIRN-37",
                payload.issue_number
            )
        }
    };

    log::info!("update_issue: #{}", issue_num);

    match update_issue_by_project_number(
        orch,
        request,
        project_key_opt.as_deref().unwrap_or(""),
        issue_num,
        IssuePatchFields {
            title: payload.title,
            description: payload.description,
            depends_on: payload.depends_on,
            labels: payload.labels,
            // The legacy update_issue tool does not change resolution.
            status: None,
            // The legacy update_issue tool does not re-parent.
            parent: None,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => error,
    }
}

async fn load_issue_by_id(conn: &turso::Connection, issue_id: &str) -> DbResult<Issue> {
    let mut rows = conn
        .query(
            "
            SELECT id, project_id, number, title, description, status, progress,
                   attention, priority, completed_at, dismissed_at, created_at,
                   updated_at, model, merged_at, closed_at, parent_issue_id
            FROM issues
            WHERE id = ?1
            ",
            (issue_id,),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row(format!("issue not found: {}", issue_id)))?;

    let depends_on = relations::list_dependency_uris(conn, issue_id).await?;
    let unmet_depends_on = relations::filter_unmet_dependencies(conn, &depends_on).await?;

    Ok(Issue {
        id: row.text(0)?,
        project_id: row.text(1)?,
        number: row.i64(2)? as i32,
        title: row.text(3)?,
        description: row.opt_text(4)?.unwrap_or_default(),
        status: row.text(5)?.parse().unwrap_or(IssueStatus::Backlog),
        progress: row.text(6)?.parse().unwrap_or(IssueProgress::Backlog),
        attention: row.text(7)?.parse().unwrap_or(IssueAttention::None),
        priority: row.opt_i64(8)?.unwrap_or(0) as i32,
        completed_at: row.opt_i64(9)?,
        dismissed_at: row.opt_i64(10)?,
        created_at: row.i64(11)?,
        updated_at: row.i64(12)?,
        backend_override: row.opt_text(13)?,
        merged_at: row.opt_i64(14)?,
        closed_at: row.opt_i64(15)?,
        parent_issue_id: row.opt_text(16)?,
        unmet_dependency_count: unmet_depends_on.len() as i64,
        depends_on,
        unmet_depends_on,
        labels: attach::list_labels_for_issue(conn, issue_id).await?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("issues-handler.db").await
    }

    /// Seed a project, a coordinator (recipe-root) job, a delegated sub-task
    /// job under it, and a run on each.
    async fn seed_jobs(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO jobs(id, project_id, status, created_at, updated_at)
             VALUES('coord', 'p', 'running', 1, 1);
            INSERT INTO jobs(id, project_id, status, parent_job_id, created_at, updated_at)
             VALUES('subtask', 'p', 'complete', 'coord', 2, 2);
            INSERT INTO runs(id, job_id, status, created_at, updated_at)
             VALUES('run-coord', 'coord', 'running', 1, 1);
            INSERT INTO runs(id, job_id, status, created_at, updated_at)
             VALUES('run-subtask', 'subtask', 'running', 2, 2);
            ",
        )
        .await
        .unwrap();
    }

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    fn request(run_id: Option<&str>) -> McpCallbackRequest {
        McpCallbackRequest {
            cwd: "/tmp/repo".to_string(),
            run_id: run_id.map(ToString::to_string),
            tool: "write".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
        }
    }

    async fn root_job(db: &LocalDb, run_id: &str) -> Option<String> {
        let run_id = run_id.to_string();
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move { root_job_for_run(conn, &run_id).await })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn parent_patch_reconciles_default_child_subscription() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w2', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p2', 'w2', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('parent', 'p2', 1, 'Parent', 'active', 'active', 'none', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('child', 'p2', 2, 'Child', 'active', 'active', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
             VALUES('coord', 'p2', 'parent', 'running', 's-coord', 1, 1);
            INSERT INTO runs(id, job_id, status, created_at, updated_at)
             VALUES('run-coord', 'coord', 'running', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
             VALUES('manual', 'p2', 'parent', 'running', 's-manual', 1, 1);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
             VALUES('manual-sub', 'manual', 'issue', 'cairn://p/PROJ/2', 'active', 'agent', 1, 1, 0);
            ",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        update_issue_by_project_number(
            &orch,
            &request(Some("run-coord")),
            "PROJ",
            2,
            IssuePatchFields {
                title: None,
                description: None,
                depends_on: None,
                labels: None,
                status: None,
                parent: Some(Some("cairn://p/PROJ/1".to_string())),
            },
        )
        .await
        .unwrap();

        let coord_subs =
            crate::orchestrator::wakes::list_subscriptions_for_job(&orch.db.local, "coord")
                .await
                .unwrap();
        assert_eq!(coord_subs.len(), 1);
        assert_eq!(
            coord_subs[0].source_ref.as_deref(),
            Some("cairn://p/PROJ/2")
        );

        update_issue_by_project_number(
            &orch,
            &request(Some("run-coord")),
            "PROJ",
            2,
            IssuePatchFields {
                title: None,
                description: None,
                depends_on: None,
                labels: None,
                status: None,
                parent: Some(None),
            },
        )
        .await
        .unwrap();

        assert!(
            crate::orchestrator::wakes::list_subscriptions_for_job(&orch.db.local, "coord")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            crate::orchestrator::wakes::list_subscriptions_for_job(&orch.db.local, "manual")
                .await
                .unwrap()
                .len(),
            1,
            "orphaning must not remove manual watchers"
        );
    }

    #[tokio::test]
    async fn root_job_for_run_returns_self_for_coordinator() {
        let db = migrated_db().await;
        seed_jobs(&db).await;
        assert_eq!(root_job(&db, "run-coord").await.as_deref(), Some("coord"));
    }

    /// A child issue spawned by a delegated sub-task must record the owning
    /// coordinator, not the sub-task's own (completed-with-session) job.
    #[tokio::test]
    async fn root_job_for_run_walks_subtask_up_to_coordinator() {
        let db = migrated_db().await;
        seed_jobs(&db).await;
        assert_eq!(root_job(&db, "run-subtask").await.as_deref(), Some("coord"));
    }

    #[tokio::test]
    async fn root_job_for_run_none_when_run_has_no_job() {
        let db = migrated_db().await;
        seed_jobs(&db).await;
        db.execute(
            "INSERT INTO runs(id, job_id, status, created_at, updated_at)
             VALUES('run-orphan', NULL, 'running', 3, 3)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(root_job(&db, "run-orphan").await, None);
    }
}
