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
    worktree_path: Option<PathBuf>,
    project_path: Option<PathBuf>,
    project_id: String,
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
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let managed_store = crate::jj::project_store_dir(&orch.config_dir, &repository_path);
    let revision_store = if crate::jj::is_jj_dir(&managed_store) {
        managed_store.as_path()
    } else {
        repository_path.as_path()
    };
    let output = jj
        .run(
            revision_store,
            &[
                "log",
                "-r",
                &rev,
                "--no-graph",
                "--ignore-working-copy",
                "-T",
                "commit_id ++ \"\\n\"",
            ],
            "resolve branch ref to immutable commit",
        )
        .map_err(|diagnostic| BranchRefResolutionError::Unresolvable {
            requested: requested.clone(),
            diagnostic,
        })?;
    let commits: Vec<_> = output
        .lines()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect();
    let commit_id = match commits.as_slice() {
        [commit] => (*commit).to_string(),
        [] => {
            return Err(BranchRefResolutionError::Unresolvable {
                requested,
                diagnostic: "the revision selected no commits".into(),
            })
        }
        _ => return Err(BranchRefResolutionError::Ambiguous { requested }),
    };

    Ok(RunBranchResolution {
        project_id: project.project_id,
        repository_path,
        rev,
        commit_id,
    })
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
                        "SELECT child.branch, child.worktree_path, p.repo_path, p.id
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
                        "SELECT j.branch, j.worktree_path, p.repo_path, p.id
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
        project_id: row.text(3)?,
    })
}
