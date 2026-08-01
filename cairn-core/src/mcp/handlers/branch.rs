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
/// ordinary project-content authority. Process cwd has no role in identity or
/// coordinate resolution.
pub(crate) async fn resolve_current_for_read(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<BranchResolution, String> {
    let (run, db) = super::run_context::lookup_run_routed(&orch.db, request).await?;
    let project = project_context_by_id_in_db(&db, &run.project_id).await?;
    let job_id = run.job_id.clone();
    let coordinates = db
        .read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT branch, base_branch FROM jobs WHERE id = ?1 LIMIT 1",
                        params![job_id],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok::<_, crate::storage::DbError>((row.opt_text(0)?, row.opt_text(1)?))
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())?
        .unwrap_or((None, None));
    let rev = resolve_job_rev(coordinates.0, coordinates.1, &project.default_branch);
    let project_path = project
        .project_path
        .ok_or_else(|| format!("project '{}' has no local repository", project.project_id))?;
    let managed_store = crate::jj::project_store_dir(&orch.config_dir, &project_path);
    let repository_path = if crate::jj::is_jj_dir(&managed_store) {
        managed_store
    } else {
        project_path.clone()
    };
    let commit_id = resolve_coordinate_repairing_conflicts(
        orch,
        &repository_path,
        &rev,
        &project.default_branch,
    )
    .await
    .map_err(|error| map_coordinate_error(error, &rev).to_string())?;
    let default_commit_id =
        resolve_default_for_read(orch, &repository_path, &project.default_branch, &commit_id).await;
    Ok(BranchResolution {
        project_id: project.project_id,
        repository_path,
        object_repository_path: project_path,
        rev,
        commit_id,
        default_commit_id,
    })
}

/// The coordinate a job's own reads and writes address: the branch it minted,
/// else the base it was launched from, else the project default.
///
/// A job with no branch of its own is not automatically on the default branch:
/// under the base branch target it works directly on `base_branch`, which for a
/// child issue is its parent's integration branch. Falling straight through to
/// the project default would silently write to the wrong branch.
fn resolve_job_rev(
    branch: Option<String>,
    base_branch: Option<String>,
    default_branch: &str,
) -> String {
    branch
        .or(base_branch)
        .unwrap_or_else(|| default_branch.to_string())
}

/// The default branch's commit for a read, degrading to the commit already
/// resolved rather than failing the read.
///
/// A read needs the default branch only as the base its content overlay is
/// cached against; the bytes it serves come from `commit_id`. Propagating a
/// failure here is how one conflicted default branch took out runs, file reads,
/// AND stored patch views at the same moment — three surfaces that share nothing
/// except this resolution, all failing at once and destroying the ability to
/// diagnose any of them. A read that can be served from an unambiguous commit is
/// served.
async fn resolve_default_for_read(
    orch: &Orchestrator,
    repository_path: &Path,
    default_branch: &str,
    commit_id: &str,
) -> String {
    match resolve_coordinate_repairing_conflicts(
        orch,
        repository_path,
        default_branch,
        default_branch,
    )
    .await
    {
        Ok(default_commit_id) => default_commit_id,
        Err(error) => {
            log::warn!(
                "default branch `{default_branch}` did not resolve ({error}); serving this read \
                 from {commit_id} alone"
            );
            commit_id.to_string()
        }
    }
}

/// Resolve a coordinate, repairing a conflicted branch NAME once rather than
/// failing the verb that asked for it.
///
/// A conflicted name used to be terminal from inside a job. Every surface that
/// resolves a branch — running a command, writing a file, reading one — shares
/// this function, so one conflicted name took all three down at once and left
/// the agent unable to diagnose or repair anything from where it stood. The
/// state was always repairable; it was only unreachable. Now the first verb to
/// meet it performs the repair and proceeds.
///
/// Only a conflicted name is retried. Absence and ambiguity are answers about
/// the coordinate, not damage to the store, and repairing them is not a thing
/// that exists.
async fn resolve_coordinate_repairing_conflicts(
    orch: &Orchestrator,
    repository_path: &Path,
    coordinate: &str,
    default_branch: &str,
) -> Result<String, cairn_vcs::CoordinateResolutionError> {
    let error = match cairn_vcs::resolve_coordinate(repository_path, coordinate).await {
        Ok(commit) => return Ok(commit),
        Err(error) => error,
    };
    let cairn_vcs::CoordinateResolutionError::Conflicted { targets, .. } = &error else {
        return Err(error);
    };
    if !crate::jj::is_jj_dir(repository_path) {
        return Err(error);
    }
    log::warn!(
        "branch `{coordinate}` has competing targets {targets:?}; repairing before serving the \
         request that resolved it"
    );

    let authority = if coordinate == default_branch {
        crate::jj::BranchAuthority::Origin
    } else {
        crate::jj::BranchAuthority::Store
    };
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let guard = orch
        .acquire_jj_store_lock(
            repository_path,
            format!("repair conflicted branch name `{coordinate}`"),
        )
        .await;
    let _phase = guard.phase("conflicted branch name repair");
    if let Err(repair_error) =
        crate::jj::repair_conflicted_branch_name(&jj, repository_path, coordinate, authority)
    {
        log::error!("branch `{coordinate}` could not be repaired: {repair_error}");
        return Err(cairn_vcs::CoordinateResolutionError::Conflicted {
            coordinate: coordinate.to_string(),
            targets: targets.clone(),
        });
    }
    // Re-resolve rather than trusting the repair's own answer: what has to hold
    // is that this path now works, not that the repair reported success.
    cairn_vcs::resolve_coordinate(repository_path, coordinate).await
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
    /// Several versions of one branch's history, which automatic repair could
    /// not settle. The only refusal in this family that survives the repair.
    Conflicted {
        requested: String,
        versions: Vec<String>,
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
            // Deliberately says what is still possible before it says what is
            // wrong. This text reaches an agent that has just lost its branch,
            // so it has to leave a way forward that does not depend on
            // understanding how the branch got into this state.
            Self::Conflicted {
                requested,
                versions,
            } => write!(
                f,
                "Branch '{requested}' has {} different versions of its history recorded ({}), and \
                 they could not be combined automatically. You can still read and search files at \
                 any one of them by using that version's id in place of the branch name. The \
                 branch itself needs a maintainer before work on it can continue.",
                versions.len(),
                versions.join(" and ")
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
        cairn_vcs::CoordinateResolutionError::Conflicted { targets, .. } => {
            BranchRefResolutionError::Conflicted {
                requested: requested.to_string(),
                versions: targets,
            }
        }
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
    let commit_id = resolve_coordinate_repairing_conflicts(
        orch,
        &repository_path,
        &rev,
        &project.default_branch,
    )
    .await
    .map_err(|error| map_coordinate_error(error, &requested).to_string())?;
    let default_commit_id =
        resolve_default_for_read(orch, &repository_path, &project.default_branch, &commit_id).await;
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
    let commit_id =
        resolve_coordinate_repairing_conflicts(orch, revision_store, &rev, &project.default_branch)
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
    project_context_by_id_in_db(&orch.db.local, project_id).await
}

async fn project_context_by_id_in_db(
    db: &crate::storage::LocalDb,
    project_id: &str,
) -> Result<ProjectBranchContext, String> {
    let project_id = project_id.to_string();
    db.read(move |conn| {
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

#[cfg(test)]
mod tests {
    use super::{resolve_job_rev, BranchRefResolutionError};

    #[test]
    fn job_rev_prefers_its_own_branch() {
        assert_eq!(
            resolve_job_rev(
                Some("agent/CAIRN-1-builder-1".into()),
                Some("agent/CAIRN-1-coordinator-0".into()),
                "main"
            ),
            "agent/CAIRN-1-builder-1"
        );
    }

    #[test]
    fn branchless_job_falls_back_to_its_base_branch() {
        // The base branch target: the job mints no branch and works directly on
        // the coordinate it was launched from, which for a child issue is its
        // parent's integration branch — not the project default.
        assert_eq!(
            resolve_job_rev(None, Some("agent/CAIRN-1-coordinator-0".into()), "main"),
            "agent/CAIRN-1-coordinator-0"
        );
    }

    #[test]
    fn branchless_job_with_no_base_falls_back_to_the_project_default() {
        assert_eq!(resolve_job_rev(None, None, "trunk"), "trunk");
    }

    /// The one refusal in this family that survives automatic repair, so it is
    /// the one whose text an agent actually has to be able to act on. It has to
    /// leave a way forward and name it in ordinary words: no talk of bookmarks,
    /// exports, coordinates, or anything else naming a mechanism the agent has
    /// no access to.
    #[test]
    fn the_conflicted_branch_refusal_offers_a_way_forward_in_plain_language() {
        let text = BranchRefResolutionError::Conflicted {
            requested: "agent/CAIRN-1-builder-1".into(),
            versions: vec!["aaaa1111".into(), "bbbb2222".into()],
        }
        .to_string();

        assert!(text.contains("agent/CAIRN-1-builder-1"), "{text}");
        assert!(
            text.contains("aaaa1111") && text.contains("bbbb2222"),
            "{text}"
        );
        // What the agent can still do, stated as an action rather than implied.
        assert!(text.contains("read and search files"), "{text}");
        crate::system_prompt::assert_no_substrate_vocabulary("conflicted branch refusal", &text);
        for jargon in [
            "bookmark",
            "export",
            "revset",
            "jj",
            "import",
            "@git",
            "reconcile",
        ] {
            assert!(
                !text.to_lowercase().contains(jargon),
                "the refusal names the machinery ({jargon:?}) instead of the agent's options: {text}"
            );
        }
    }
}
