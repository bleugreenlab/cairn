//! Branch/ref resolution shared by branch-aware MCP handlers.
//!
//! Raw branch values intentionally stay raw for blob reads: jj can resolve
//! bookmarks, commit ids, and change ids directly. Node/task URIs are sugar that
//! resolves through the job row to its recorded branch and checkout.

use std::path::{Path, PathBuf};

use cairn_common::uri::{parse_uri, CairnResource};
use cairn_db::turso::params;

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

#[derive(Debug, Clone)]
pub(crate) struct BranchResolution {
    pub rev: String,
    pub checkout: Option<PathBuf>,
    pub project_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct ProjectBranchContext {
    project_id: String,
    project_path: Option<PathBuf>,
    default_branch: String,
}

#[derive(Debug, Clone)]
struct JobBranchContext {
    branch: Option<String>,
    worktree_path: Option<PathBuf>,
    project_path: Option<PathBuf>,
}

pub(crate) async fn resolve_for_read(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    branch: &str,
) -> Result<BranchResolution, String> {
    if let Some(job) = resolve_node_uri(orch, branch).await? {
        let rev = job
            .branch
            .ok_or_else(|| format!("Node URI '{branch}' has no recorded branch"))?;
        return Ok(BranchResolution {
            rev,
            checkout: job.worktree_path,
            project_path: job.project_path,
        });
    }

    Ok(BranchResolution {
        rev: branch.to_string(),
        checkout: None,
        project_path: project_context_for_request(orch, request)
            .await
            .ok()
            .and_then(|ctx| ctx.project_path),
    })
}

pub(crate) async fn resolve_for_run(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    branch: &str,
) -> Result<BranchResolution, String> {
    if let Some(job) = resolve_node_uri(orch, branch).await? {
        let rev = job
            .branch
            .ok_or_else(|| format!("Node URI '{branch}' has no recorded branch"))?;
        let checkout = live_checkout(job.worktree_path, branch)?;
        return Ok(BranchResolution {
            rev,
            checkout: Some(checkout),
            project_path: job.project_path,
        });
    }

    let project = project_context_for_request(orch, request).await?;
    let requested = branch.trim();
    if requested == project.default_branch || requested == "main" {
        let checkout = live_checkout(project.project_path.clone(), branch)?;
        return Ok(BranchResolution {
            rev: project.default_branch,
            checkout: Some(checkout),
            project_path: project.project_path,
        });
    }

    if let Some(job) = job_by_branch(orch, &project.project_id, requested).await? {
        let checkout = live_checkout(job.worktree_path, branch)?;
        return Ok(BranchResolution {
            rev: job.branch.unwrap_or_else(|| requested.to_string()),
            checkout: Some(checkout),
            project_path: job.project_path,
        });
    }

    Err(format!(
        "No live checkout found for branch '{branch}'. Branch-scoped run is a cd stand-in and cannot materialize a checkout."
    ))
}

pub(crate) fn jj_command_dir(resolution: &BranchResolution, fallback_cwd: &str) -> PathBuf {
    let fallback = PathBuf::from(fallback_cwd);
    if crate::jj::is_jj_dir(&fallback) {
        return fallback;
    }
    resolution
        .checkout
        .clone()
        .or_else(|| resolution.project_path.clone())
        .unwrap_or(fallback)
}

fn live_checkout(path: Option<PathBuf>, branch: &str) -> Result<PathBuf, String> {
    let Some(path) = path else {
        return Err(format!(
            "No live checkout found for branch '{branch}'. Branch-scoped run is a cd stand-in and cannot materialize a checkout."
        ));
    };
    if !path.is_dir() {
        return Err(format!(
            "Live checkout for branch '{branch}' does not exist on disk: {}",
            path.display()
        ));
    }
    Ok(path)
}

async fn project_context_for_request(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<ProjectBranchContext, String> {
    let run = super::run_context::lookup_run(&orch.db.local, request).await?;
    project_context_by_id(orch, &run.project_id).await
}

async fn project_context_by_id(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<ProjectBranchContext, String> {
    let project_id = project_id.to_string();
    orch.db
        .local
        .read(move |conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT repo_path, default_branch FROM projects WHERE id = ?1 LIMIT 1",
                        params![project_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Err(crate::storage::DbError::Row(format!(
                        "No project found with id '{project_id}'"
                    )));
                };
                let repo_path = row.opt_text(0)?;
                let stored_default = row.opt_text(1)?;
                let default_branch = if let Some(path) = repo_path.as_deref() {
                    let settings =
                        crate::config::project_settings::load_project_settings(Path::new(path));
                    crate::config::project_settings::resolve_default_branch(
                        &settings,
                        stored_default.as_deref(),
                    )
                } else {
                    crate::config::project_settings::resolve_default_branch(
                        &crate::config::project_settings::ProjectSettingsFile::default(),
                        stored_default.as_deref(),
                    )
                };
                Ok(ProjectBranchContext {
                    project_id,
                    project_path: repo_path.map(PathBuf::from),
                    default_branch,
                })
            })
        })
        .await
        .map_err(|e| e.to_string())
}

async fn resolve_node_uri(
    orch: &Orchestrator,
    value: &str,
) -> Result<Option<JobBranchContext>, String> {
    let Some(resource) = parse_uri(value) else {
        return Ok(None);
    };
    match resource {
        CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        } => job_by_node_uri(orch, &project, number, exec_seq, &node_id, None)
            .await
            .map(Some),
        CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => job_by_node_uri(orch, &project, number, exec_seq, &node_id, Some(&task_name))
            .await
            .map(Some),
        _ => Ok(None),
    }
}

async fn job_by_branch(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
) -> Result<Option<JobBranchContext>, String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    orch.db
        .local
        .read(move |conn| {
            let project_id = project_id.clone();
            let branch = branch.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.branch, j.worktree_path, p.repo_path
                         FROM jobs j
                         JOIN projects p ON j.project_id = p.id
                         WHERE j.project_id = ?1 AND j.branch = ?2
                         ORDER BY j.created_at DESC
                         LIMIT 1",
                        params![project_id.as_str(), branch.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| job_context_from_row(&row))
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
}

async fn job_by_node_uri(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> Result<JobBranchContext, String> {
    let project = project.to_uppercase();
    let node_id = node_id.to_string();
    let task_name = task_name.map(ToOwned::to_owned);
    orch.db
        .local
        .read(move |conn| {
            let project = project.clone();
            let node_id = node_id.clone();
            let task_name = task_name.clone();
            Box::pin(async move {
                let mut rows = if let Some(task_name) = task_name {
                    conn.query(
                        "SELECT child.branch, child.worktree_path, p.repo_path
                         FROM jobs parent
                         JOIN jobs child ON child.parent_job_id = parent.id
                         JOIN issues i ON parent.issue_id = i.id
                         JOIN projects p ON i.project_id = p.id
                         JOIN executions e ON parent.execution_id = e.id
                         WHERE p.key = ?1
                           AND i.number = ?2
                           AND e.seq = ?3
                           AND parent.parent_job_id IS NULL
                           AND parent.uri_segment = ?4
                           AND child.uri_segment = ?5
                         ORDER BY child.created_at DESC
                         LIMIT 1",
                        params![
                            project.as_str(),
                            number,
                            exec_seq,
                            node_id.as_str(),
                            task_name.as_str()
                        ],
                    )
                    .await?
                } else {
                    conn.query(
                        "SELECT j.branch, j.worktree_path, p.repo_path
                         FROM jobs j
                         JOIN issues i ON j.issue_id = i.id
                         JOIN projects p ON i.project_id = p.id
                         JOIN executions e ON j.execution_id = e.id
                         WHERE p.key = ?1
                           AND i.number = ?2
                           AND e.seq = ?3
                           AND j.parent_job_id IS NULL
                           AND j.uri_segment = ?4
                         ORDER BY j.created_at DESC
                         LIMIT 1",
                        params![project.as_str(), number, exec_seq, node_id.as_str()],
                    )
                    .await?
                };
                rows.next()
                    .await?
                    .map(|row| job_context_from_row(&row))
                    .transpose()?
                    .ok_or_else(|| {
                        crate::storage::DbError::Row(format!(
                            "No job found for branch node URI '{project}-{number}/{exec_seq}/{node_id}'"
                        ))
                    })
            })
        })
        .await
        .map_err(|e| e.to_string())
}

fn job_context_from_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<JobBranchContext> {
    Ok(JobBranchContext {
        branch: row.opt_text(0)?,
        worktree_path: row.opt_text(1)?.map(PathBuf::from),
        project_path: row.opt_text(2)?.map(PathBuf::from),
    })
}
