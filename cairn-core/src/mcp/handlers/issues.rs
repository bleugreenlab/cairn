//! Issue-related MCP handlers.
//!
use crate::orchestrator::Orchestrator;
use cairn_common::ids;
use cairn_db::turso::params;

use super::ProjectContext;
use crate::issues::relations;
use crate::labels::attach;
use crate::mcp::types::McpCallbackRequest;
use crate::models::{Issue, IssueAttention, IssueKind, IssueProgress, IssueStatus, Label};
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
    pub(crate) recipe: Option<String>,
    pub(crate) backend: Option<String>,
}

/// Persist a new issue and emit the standard side effects (embed, sync,
/// db-change), returning the created row so callers can chain follow-on work
/// (e.g. starting an execution) without re-querying.
#[allow(clippy::too_many_arguments)]
async fn insert_issue_with_context(
    orch: &Orchestrator,
    owning_db: &LocalDb,
    ctx: &ProjectContext,
    title: String,
    description: Option<String>,
    parent_uri: Option<String>,
    labels: Option<Vec<String>>,
    kind: IssueKind,
) -> Result<Issue, String> {
    let services = &orch.services;
    let embed_text = description.clone().unwrap_or_default();
    // The issue row is written to the database that owns the project (the team
    // replica for a team project, the private DB for a local one), resolved by
    // the caller via `for_project` so the row lands in the same DB it is read
    // back from (CAIRN-2181).
    let (issue, created_labels) = create_issue_row(
        owning_db,
        &ctx.project_id,
        title,
        description,
        parent_uri,
        labels,
        kind,
    )
    .await
    .map_err(|e| format!("Failed to create issue: {}", e))?;

    // Nothing is minted here for child attention. A parented child's attention
    // is routed to whichever node is driving the parent issue at the moment the
    // fact occurs, derived live from `parent_issue_id`
    // (`wakes::coordinating_job_for_child_issue`).
    let issue_uri = cairn_common::uri::build_issue_uri(&ctx.project_key, issue.number);
    orch.enqueue_resource_embed(&issue_uri, embed_text);

    if let Err(e) = services.emitter.emit(
        "db-change",
        crate::notify::issue_db_change(&issue, "update"),
    ) {
        log::error!("Failed to emit db-change event: {}", e);
    }
    emit_labels_created(orch, &created_labels);
    if !issue.labels.is_empty() {
        if let Err(e) = services.emitter.emit(
            "db-change",
            serde_json::json!({
                "table": "issue_labels",
                "action": "insert",
                "issueId": issue.id,
                "projectId": issue.project_id,
            }),
        ) {
            log::error!("Failed to emit db-change event: {}", e);
        }
    }

    Ok(issue)
}

/// Announce labels an issue write minted on the fly so the workspace
/// vocabulary in the UI picks them up, not just the issue's own chips.
fn emit_labels_created(orch: &Orchestrator, created: &[Label]) {
    if created.is_empty() {
        return;
    }
    if let Err(e) = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "labels", "action": "insert"}),
    ) {
        log::error!("Failed to emit db-change event: {}", e);
    }
}

fn created_issue_summary(ctx: &ProjectContext, issue: &Issue) -> String {
    format!(
        "Created {} {}-{}: \"{}\"",
        issue.kind, ctx.project_key, issue.number, issue.title
    )
}

/// Outcome of creating an issue through the resource-mutation path: the
/// human-readable summary plus the structured identifiers UI renderers need to
/// resolve the created issue (drag target, live-node lookup) without parsing
/// the summary string.
pub struct CreatedIssueOutcome {
    pub(crate) summary: String,
    pub issue_id: String,
    pub(crate) project_key: String,
    pub number: i32,
    pub(crate) uri: String,
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
    kind: IssueKind,
) -> Result<CreatedIssueOutcome, String> {
    let owning_db = orch.db.for_project(project_key).await;
    let ctx = lookup_project_by_key(&owning_db, project_key).await?;
    let issue = insert_issue_with_context(
        orch,
        &owning_db,
        &ctx,
        title,
        description,
        parent_uri,
        labels,
        kind,
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
            None,
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
        Err("Missing authenticated run identity for project-scoped issue operation".to_string())
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_issue_row(
    db: &LocalDb,
    project_id: &str,
    title: String,
    description: Option<String>,
    parent_uri: Option<String>,
    labels: Option<Vec<String>>,
    kind: IssueKind,
) -> DbResult<(Issue, Vec<Label>)> {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let title = title.clone();
        let description = description.clone();
        let parent_uri = parent_uri.clone();
        let labels = labels.clone();
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
            let id = ids::mint_child(project_id.as_str());

            conn.execute(
                "UPDATE projects SET next_issue_number = ?1, updated_at = ?2 WHERE id = ?3",
                params![number + 1, now, project_id.as_str()],
            )
            .await?;

            conn.execute(
                "
                INSERT INTO issues (
                    id, project_id, number, title, description, status, progress,
                    attention, priority, created_at, updated_at, model, parent_issue_id, kind
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
                    kind.to_string()
                ],
            )
            .await?;

            let created_labels = match labels {
                Some(labels) => attach::replace_issue_labels(conn, &id, &labels, now as i64)
                    .await
                    .map_err(DbError::Row)?,
                None => Vec::new(),
            };

            let issue = crate::issues::crud::load_conn(conn, &id)
                .await
                .unwrap_or_else(|_| Issue {
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
                    kind,
                });
            Ok((issue, created_labels))
        })
    })
    .await
}

pub(crate) struct IssuePatchFields {
    pub(crate) title: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) depends_on: Option<Vec<String>>,
    pub(crate) labels: Option<Vec<String>>,
    /// Resolution to apply via [`crate::issues::status::update_status`]. Only
    /// `merged`/`closed` are accepted at the URI layer; callers validate before
    /// setting this. `None` leaves the resolution untouched.
    pub(crate) status: Option<String>,
    /// Whether the caller confirmed stopping the work still live on the issue.
    /// Only meaningful with `status`; the first unconfirmed resolution of an
    /// issue with live work is refused and names this key (CAIRN-3212).
    pub(crate) confirm: bool,
    /// Re-parenting. `None` leaves parent untouched; `Some(None)` orphans the
    /// issue (clears parent); `Some(Some(uri))` adopts under the given canonical
    /// issue URI. The URI is resolved to an issue id, validated same-project, and
    /// checked for parent-chain cycles inside the update transaction.
    pub(crate) parent: Option<Option<String>>,
}

async fn update_issue_row(
    db: &LocalDb,
    project_id: &str,
    issue_num: i32,
    patch: IssuePatchFields,
) -> DbResult<Option<(Issue, Vec<Label>)>> {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let title = patch.title.clone();
        let description = patch.description.clone();
        let depends_on = patch.depends_on.clone();
        let labels = patch.labels.clone();
        let parent = patch.parent.clone();
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
            let created_labels = match labels {
                Some(labels) => attach::replace_issue_labels(conn, &issue_id, &labels, now as i64)
                    .await
                    .map_err(DbError::Row)?,
                None => Vec::new(),
            };

            match parent {
                // Parent left untouched.
                None => {}
                // Orphan: clear the parent edge. With no parent issue there is
                // no coordinator to derive, so the child's attention stops
                // routing anywhere but its own watchers.
                Some(None) => {
                    conn.execute(
                        "UPDATE issues SET parent_issue_id = NULL, updated_at = ?1 WHERE id = ?2",
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
                    // Adopting an issue re-points its attention by itself: the
                    // recipient is whoever drives this parent when a child fact
                    // next occurs, so there is nothing to record here.
                    conn.execute(
                        "UPDATE issues SET parent_issue_id = ?1, updated_at = ?2 WHERE id = ?3",
                        params![resolved.issue_id.as_str(), now, issue_id.as_str()],
                    )
                    .await?;
                }
            }

            crate::issues::crud::load_conn(conn, &issue_id)
                .await
                .map(|issue| Some((issue, created_labels)))
        })
    })
    .await
}

pub(crate) async fn update_issue_by_project_number(
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

    // Resolve the owning DB by key (CAIRN-2181): a team project's issue row lives
    // in its team replica, so the field/parent/dependency/label writes must
    // target the same DB the row is read from. Without a key, authenticated run
    // identity selects the owning replica; ambient callers must supply project context.
    let (owning_db, ctx) = if project_key.is_empty() {
        // A team run's run/job rows live in its replica (CAIRN-2182): route the
        // run-id context lookup to the owning DB so it doesn't error `No run
        // found` against the private DB. An ambient request stays on the private DB.
        let lookup_db = match request.run_id.as_deref() {
            Some(run_id) => crate::execution::routing::routing_db_for_id(&orch.db, run_id)
                .await
                .map_err(|e| e.to_string())?,
            None => orch.db.local.clone(),
        };
        let ctx = lookup_project_context(&lookup_db, request).await?;
        let db = orch.db.for_project(&ctx.project_key).await;
        (db, ctx)
    } else {
        let db = orch.db.for_project(project_key).await;
        let ctx = lookup_project_by_key(&db, project_key).await?;
        (db, ctx)
    };

    let embed_description = patch.description.clone();
    let labels_changed = patch.labels.is_some();
    let status = patch.status.clone();
    let confirm = patch.confirm;

    // A resolution is checked before any field of this write is applied. The
    // resolution itself is applied last (see below), so checking it there too
    // would let a refused `{title, status}` patch rename the issue and then
    // report that it changed nothing — and a caller retrying "the same write"
    // would repeat the rename.
    if let Some(status) = status.as_deref() {
        let issue_id = crate::issues::relations::issue_id_for_project_number(
            &owning_db,
            &ctx.project_key,
            issue_num,
        )
        .await
        .map_err(|e| format!("Failed to resolve issue: {e}"))?
        .ok_or_else(|| format!("Issue {}-{} not found", ctx.project_key, issue_num))?;
        crate::issues::status::check_resolution(
            orch,
            &issue_id,
            status,
            crate::issues::status::ResolutionActor::Agent,
            crate::issues::status::Confirmation::from_flag(confirm),
        )
        .await
        .map_err(|refusal| refusal.to_string())?;
    }
    let (issue, created_labels) = update_issue_row(&owning_db, &ctx.project_id, issue_num, patch)
        .await
        .map_err(|e| format!("Failed to update issue: {e}"))?
        .ok_or_else(|| format!("Issue {}-{} not found", ctx.project_key, issue_num))?;

    // Re-embed only when the description was part of this update; a title-only
    // or dependency-only patch leaves the stored description untouched.
    let issue_uri = cairn_common::uri::build_issue_uri(&ctx.project_key, issue.number);
    if let Some(description) = embed_description {
        orch.enqueue_resource_embed(&issue_uri, description);
    }

    if let Err(e) = orch.services.emitter.emit(
        "db-change",
        crate::notify::issue_db_change(&issue, "update"),
    ) {
        log::error!("Failed to emit db-change event: {}", e);
    }
    emit_labels_created(orch, &created_labels);
    if labels_changed {
        if let Err(e) = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({
                "table": "issue_labels",
                "action": "update",
                "issueId": issue.id,
                "projectId": issue.project_id,
            }),
        ) {
            log::error!("Failed to emit db-change event: {}", e);
        }
    }

    // Apply a resolution change last so update_status's own sync/emit carries the
    // final issue state; the field sync above would otherwise overwrite it with a
    // stale (pre-resolution) snapshot.
    if let Some(status) = status.as_deref() {
        crate::issues::status::update_status(
            orch,
            &issue.id,
            status,
            crate::issues::status::ResolutionActor::Agent,
            crate::issues::status::Confirmation::from_flag(confirm),
        )
        .await
        .map_err(|refusal| refusal.to_string())?;
    }

    Ok(format!(
        "Patched issue {}-{}{}",
        ctx.project_key,
        issue.number,
        status.map(|s| format!(" (status={s})")).unwrap_or_default()
    ))
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
            thread_id: None,
            cwd: "/tmp/repo".to_string(),
            run_id: run_id.map(ToString::to_string),
            tool: "write".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
        }
    }

    /// Adopting and orphaning an issue moves its attention with the parent edge
    /// alone — no subscription row is minted, reconciled, or left behind.
    #[tokio::test]
    async fn parent_patch_repoints_child_attention_without_minting_rows() {
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
                confirm: false,
                parent: Some(Some("cairn://p/PROJ/1".to_string())),
            },
        )
        .await
        .unwrap();

        // The adopting coordinator watches the child immediately, with nothing
        // persisted on its behalf.
        assert_eq!(
            crate::orchestrator::wakes::watcher_jobs_for_issue(&orch.db.local, "cairn://p/PROJ/2")
                .await
                .unwrap(),
            vec!["manual".to_string(), "coord".to_string()]
        );
        assert!(
            crate::orchestrator::wakes::list_subscriptions_for_job(&orch.db.local, "coord")
                .await
                .unwrap()
                .is_empty(),
            "the derived watch must not materialize a row"
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
                confirm: false,
                parent: Some(None),
            },
        )
        .await
        .unwrap();

        // Orphaned: the derived watch is gone with the parent edge, and the
        // manual watcher is untouched.
        assert_eq!(
            crate::orchestrator::wakes::watcher_jobs_for_issue(&orch.db.local, "cairn://p/PROJ/2")
                .await
                .unwrap(),
            vec!["manual".to_string()],
            "orphaning drops the derived watch and keeps manual watchers"
        );
    }

    /// A local project (no team route) still creates its issue in the PRIVATE
    /// database: with no route registered, `for_project` returns `local`, so the
    /// owning-DB routing (CAIRN-2181) is a strict no-op for local-only installs.
    #[tokio::test]
    async fn create_issue_lands_in_private_db_for_local_project() {
        let db = migrated_db().await;
        seed_jobs(&db).await;
        let orch = test_orchestrator(db);

        let outcome = create_issue_in_project(
            &orch,
            "PROJ",
            "Local issue".to_string(),
            Some("body".to_string()),
            None,
            None,
            None,
            IssueKind::Issue,
        )
        .await
        .unwrap();

        assert_eq!(outcome.number, 1);
        let found = orch
            .db
            .local
            .query_text(
                "SELECT title FROM issues WHERE id = ?1",
                (outcome.issue_id.clone(),),
            )
            .await
            .unwrap();
        assert_eq!(
            found.as_deref(),
            Some("Local issue"),
            "a local project's issue lands in the private database"
        );
    }
}
