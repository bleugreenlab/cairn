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
    pub project_id: String,
    /// Runner-owned jj operation repository used for coordinate mutation.
    pub repository_path: PathBuf,
    /// Git object database used by immutable read overlays.
    pub object_repository_path: PathBuf,
    pub rev: String,
    pub commit_id: String,
    pub default_commit_id: String,
}

/// Resolve the durable logical coordinate owned by the current run. This is the
/// ordinary project-content authority; `cwd` is used only by `lookup_run` as a
/// legacy identity fallback and is never inspected for its jj `@` coordinate.
pub(crate) async fn resolve_current_for_read(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<BranchResolution, String> {
    let run = super::run_context::lookup_run(&orch.db.local, request).await?;
    let project = project_context_by_id(orch, &run.project_id).await?;
    let job_id = run.job_id.clone();
    let rev = orch
        .db
        .local
        .read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT branch FROM jobs WHERE id = ?1 LIMIT 1",
                        params![job_id],
                    )
                    .await?;
                rows.next().await?.map(|row| row.opt_text(0)).transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())?
        .flatten()
        .unwrap_or_else(|| project.default_branch.clone());
    let project_path = project
        .project_path
        .ok_or_else(|| format!("project '{}' has no local repository", project.project_id))?;
    let managed_store = crate::jj::project_store_dir(&orch.config_dir, &project_path);
    let repository_path = if crate::jj::is_jj_dir(&managed_store) {
        managed_store
    } else {
        project_path.clone()
    };
    let commit_id = cairn_vcs::resolve_coordinate(&repository_path, &rev)
        .await
        .map_err(|error| map_coordinate_error(error, &rev).to_string())?;
    let default_commit_id =
        cairn_vcs::resolve_coordinate(&repository_path, &project.default_branch)
            .await
            .map_err(|error| map_coordinate_error(error, &project.default_branch).to_string())?;
    Ok(BranchResolution {
        project_id: project.project_id,
        repository_path,
        object_repository_path: project_path,
        rev,
        commit_id,
        default_commit_id,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct RunBranchResolution {
    pub project_id: String,
    pub repository_path: PathBuf,
    pub rev: String,
    pub commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BranchRefResolutionError {
    Invalid {
        requested: String,
    },
    Unresolvable {
        requested: String,
        diagnostic: String,
    },
    Ambiguous {
        requested: String,
    },
}

impl std::fmt::Display for BranchRefResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid { requested } => write!(f, "Invalid branch ref '{requested}'"),
            Self::Unresolvable {
                requested,
                diagnostic,
            } => write!(
                f,
                "Could not resolve branch ref '{requested}' to a commit: {diagnostic}"
            ),
            Self::Ambiguous { requested } => write!(
                f,
                "Branch ref '{requested}' is ambiguous; exactly one commit is required"
            ),
        }
    }
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
    project_id: String,
}

fn map_coordinate_error(
    error: cairn_vcs::CoordinateResolutionError,
    requested: &str,
) -> BranchRefResolutionError {
    match error {
        cairn_vcs::CoordinateResolutionError::Invalid(_) => BranchRefResolutionError::Invalid {
            requested: requested.to_string(),
        },
        cairn_vcs::CoordinateResolutionError::Ambiguous(_) => BranchRefResolutionError::Ambiguous {
            requested: requested.to_string(),
        },
        other => BranchRefResolutionError::Unresolvable {
            requested: requested.to_string(),
            diagnostic: other.to_string(),
        },
    }
}

pub(crate) async fn resolve_for_read(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    branch: &str,
) -> Result<BranchResolution, String> {
    let (project, rev) = match resolve_node_uri(orch, branch).await? {
        Some(job) => {
            let rev = job
                .branch
                .ok_or_else(|| format!("Node URI '{branch}' has no recorded branch"))?;
            (project_context_by_id(orch, &job.project_id).await?, rev)
        }
        None => {
            let project = project_context_for_request(orch, request).await?;
            let rev = if branch.trim() == "main" {
                project.default_branch.clone()
            } else {
                branch.trim().to_string()
            };
            (project, rev)
        }
    };
    let project_path = project
        .project_path
        .ok_or_else(|| format!("project '{}' has no local repository", project.project_id))?;
    let managed_store = crate::jj::project_store_dir(&orch.config_dir, &project_path);
    let repository_path = if crate::jj::is_jj_dir(&managed_store) {
        managed_store
    } else {
        project_path.clone()
    };
    let requested = branch.to_string();
    let commit_id = cairn_vcs::resolve_coordinate(&repository_path, &rev)
        .await
        .map_err(|error| map_coordinate_error(error, &requested).to_string())?;
    let default_commit_id =
        cairn_vcs::resolve_coordinate(&repository_path, &project.default_branch)
            .await
            .map_err(|error| map_coordinate_error(error, &project.default_branch).to_string())?;
    Ok(BranchResolution {
        project_id: project.project_id,
        repository_path,
        object_repository_path: project_path,
        rev,
        commit_id,
        default_commit_id,
    })
}

pub(crate) async fn resolve_for_run(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    branch: &str,
) -> Result<RunBranchResolution, BranchRefResolutionError> {
    let requested = branch.to_string();
    if branch.trim().is_empty() {
        return Err(BranchRefResolutionError::Invalid { requested });
    }

    let (project, rev) = match resolve_node_uri(orch, branch).await.map_err(|diagnostic| {
        BranchRefResolutionError::Unresolvable {
            requested: requested.clone(),
            diagnostic,
        }
    })? {
        Some(job) => {
            let rev = job
                .branch
                .ok_or_else(|| BranchRefResolutionError::Unresolvable {
                    requested: requested.clone(),
                    diagnostic: "the node has no recorded branch".into(),
                })?;
            let project =
                project_context_by_id(orch, &job.project_id)
                    .await
                    .map_err(|diagnostic| BranchRefResolutionError::Unresolvable {
                        requested: requested.clone(),
                        diagnostic,
                    })?;
            (project, rev)
        }
        None => {
            let project =
                project_context_for_request(orch, request)
                    .await
                    .map_err(|diagnostic| BranchRefResolutionError::Unresolvable {
                        requested: requested.clone(),
                        diagnostic,
                    })?;
            let rev = if branch.trim() == "main" {
                project.default_branch.clone()
            } else {
                branch.trim().to_string()
            };
            (project, rev)
        }
    };

    let repository_path =
        project
            .project_path
            .ok_or_else(|| BranchRefResolutionError::Unresolvable {
                requested: requested.clone(),
                diagnostic: format!("project '{}' has no local repository", project.project_id),
            })?;
    let managed_store = crate::jj::project_store_dir(&orch.config_dir, &repository_path);
    let revision_store = if crate::jj::is_jj_dir(&managed_store) {
        managed_store.as_path()
    } else {
        repository_path.as_path()
    };
    let commit_id = cairn_vcs::resolve_coordinate(revision_store, &rev)
        .await
        .map_err(|error| map_coordinate_error(error, &requested))?;

    Ok(RunBranchResolution {
        project_id: project.project_id,
        repository_path,
        rev,
        commit_id,
    })
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
                        "SELECT child.branch, p.id
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
                        "SELECT j.branch, p.id
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
        project_id: row.text(1)?,
    })
}
