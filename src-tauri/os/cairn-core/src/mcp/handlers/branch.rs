//! Branch/ref resolution shared by branch-aware MCP handlers.
//!
//! Raw branch values intentionally stay raw for blob reads: jj can resolve
//! bookmarks, commit ids, and change ids directly. Node/task URIs are sugar that
//! resolves through the job row to its recorded branch and checkout.

use std::path::{Path, PathBuf};

use cairn_db::turso::params;
use cairn_uri::{parse_uri, CairnResource};

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
    /// The project's configured default branch, which `rev` names when the
    /// caller asked for `main`.
    pub default_branch: String,
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
    let own_branch = coordinates.0.clone();
    let rev = resolve_job_rev(coordinates.0, coordinates.1, &project.default_branch);
    let project_path = project
        .project_path
        .ok_or_else(|| format!("project '{}' has no local repository", project.project_id))?;
    let repository_path = {
        let jj_binary_path = orch.jj_binary_path.clone();
        let config_dir = orch.config_dir.clone();
        let project_repo = project_path.clone();
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::environment(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .map_err(|error| format!("coordinate repository task failed: {error}"))??
    };
    let commit_id = resolve_coordinate_repairing_conflicts(
        orch,
        &repository_path,
        &rev,
        &project.default_branch,
    )
    .await
    .map_err(|error| {
        let error = map_own_coordinate_error(error, &rev, own_branch.as_deref());
        if matches!(error, BranchRefResolutionError::OwnBranchLost { .. }) {
            log::error!(
                "run {} (job {}) lost its own branch `{rev}`: it no longer resolves in {}",
                run.run_id,
                run.job_id,
                repository_path.display()
            );
        }
        error.to_string()
    })?;
    let default_commit_id =
        resolve_default_for_read(orch, &repository_path, &project.default_branch, &commit_id).await;
    Ok(BranchResolution {
        project_id: project.project_id,
        repository_path,
        object_repository_path: project_path,
        rev,
        commit_id,
        default_commit_id,
        default_branch: project.default_branch,
    })
}

/// The calling run's project and the branch its job's work merges into
/// (`jobs.base_branch`: the parent issue's branch for a stacked child), or
/// `None` when the job records none and so merges into the project default.
/// The same coordinate node `/diff` measures a branch against.
pub(crate) async fn current_base_branch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<(String, Option<String>), String> {
    let (run, db) = super::run_context::lookup_run_routed(&orch.db, request).await?;
    let job_id = run.job_id.clone();
    let base = db
        .read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT base_branch FROM jobs WHERE id = ?1 LIMIT 1",
                        params![job_id],
                    )
                    .await?;
                Ok::<_, crate::storage::DbError>(match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                })
            })
        })
        .await
        .map_err(|error| error.to_string())?
        .filter(|base| !base.trim().is_empty());
    Ok((run.project_id, base))
}

/// Map a failure to resolve the run's OWN coordinate.
///
/// When that coordinate is the branch this job minted and it is simply absent,
/// nothing the session asks for can be served: every file read, file write, and
/// command resolves the same missing name. The generic "could not resolve" text
/// reads like a typo the agent should fix, so it retries. This case gets its own
/// refusal saying the branch is gone, that the agent did not cause it, and what
/// to do instead.
fn map_own_coordinate_error(
    error: cairn_vcs::CoordinateResolutionError,
    requested: &str,
    own_branch: Option<&str>,
) -> BranchRefResolutionError {
    if matches!(error, cairn_vcs::CoordinateResolutionError::Absent { .. })
        && own_branch == Some(requested)
    {
        return BranchRefResolutionError::OwnBranchLost {
            branch: requested.to_string(),
        };
    }
    map_coordinate_error(error, requested)
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
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let guard = orch
        .acquire_jj_store_lock(
            repository_path,
            crate::orchestrator::StoreLockClass::Foreground,
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
            refusal: repair_error.refusal().cloned().map(Box::new),
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
    /// The repository `commit_id` was resolved in: the managed jj store when
    /// one exists, otherwise the project checkout.
    pub revision_store: PathBuf,
    pub rev: String,
    pub commit_id: String,
    pub default_branch: String,
}

/// The stable code a retired-branch refusal leads with, so a tool report, a
/// batch result and a held-commit note can all be recognised as the same
/// refusal.
pub(crate) const BRANCH_RETIRED: &str = "branch_retired";

/// Why a job may no longer publish to the branch it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetirementCause {
    /// The job's issue was merged.
    IssueMerged,
    /// The job's issue was closed without merging.
    IssueClosed,
    /// The job itself was archived: stopped for good rather than parked, which
    /// is what a terminal resolution does to the work it settles, the work of
    /// dependent issues included.
    JobCancelled,
    /// The session the publishing run belongs to was closed by a resolution.
    /// A reopen continues the job in a successor session and leaves this one
    /// closed, so work this run prepared before the resolution stays refused
    /// after the reopen.
    SessionClosed,
}

impl RetirementCause {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::IssueMerged => "issue_merged",
            Self::IssueClosed => "issue_closed",
            Self::JobCancelled => "job_cancelled",
            Self::SessionClosed => "session_closed",
        }
    }
}

/// The refusal a stale writer gets instead of a sealed commit its branch will
/// never carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchRetired {
    pub branch: String,
    pub job_id: String,
    /// `project/number` of the job's issue, when it has one.
    pub issue: Option<String>,
    pub cause: RetirementCause,
}

impl std::fmt::Display for BranchRetired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let issue = self.issue.as_deref().unwrap_or("its issue");
        let why = match self.cause {
            RetirementCause::IssueMerged => format!("issue {issue} was merged"),
            RetirementCause::IssueClosed => format!("issue {issue} was closed"),
            RetirementCause::JobCancelled => {
                "this session's work was stopped for good when its issue (or the issue it \
                 builds on) resolved"
                    .to_string()
            }
            RetirementCause::SessionClosed => format!(
                "the session this work was prepared in was closed when {issue} resolved; if the \
                 issue was reopened since, its work continues in a new session"
            ),
        };
        // Written for a model that would otherwise retry, like `OwnBranchLost`:
        // the terminal fact, that nothing moved, and what still works.
        write!(
            f,
            "{BRANCH_RETIRED} ({cause}): branch `{branch}` no longer accepts commits from this \
             session because {why}. Nothing was committed: no commit was sealed and the branch \
             did not move. Retrying will not change this. Reading files and reading or writing \
             cairn:// resources still work; put anything worth keeping in a comment on the \
             issue, then end your turn.",
            cause = self.cause.as_str(),
            branch = self.branch,
        )
    }
}

/// Whether a job may still publish to its managed branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PublicationAuthority {
    Authorized,
    Retired(BranchRetired),
    /// The job's lifecycle could not be read. Publication must not proceed on
    /// an unknown answer, but nothing is retired either: the caller fails the
    /// way it fails any retryable publication.
    Unknown(String),
}

/// The one publication authority for a managed branch: may `job_id`, acting
/// through `run_id`, still seal a commit onto, or advance the bookmark of,
/// `branch`?
///
/// Every seal and bookmark advance asks this **under the store lock, right
/// before it acts**. Terminal resolution records itself under that same lock
/// (`issues::status::resolve_terminal`), so the two are ordered: a publication
/// that holds the lock first completes before the issue resolves, and one that
/// takes it after sees the resolution and is refused before anything moves.
/// Asking anywhere earlier only narrows the window; it was a task's commit
/// arriving minutes after its issue merged that stranded a bookmark the git
/// export could no longer follow (cairn/6147). The commit-posture gate also
/// asks it before a committing batch resolves its coordinate, so a branch whose
/// coordinate cleanup already removed still answers with this refusal.
///
/// Job status alone is not the authority across a reopen: a reopen lifts the
/// jobs a resolution cancelled and continues them in successor sessions on the
/// same job ids. What stays retired is the session the resolution closed, so
/// the originating run's session is checked as well. A run with no session,
/// and a held delta recorded before held rows named their run, are judged by
/// the job and issue alone.
///
/// A job with no row has no lifecycle to enforce here; every agent path reaches
/// this with a job its authenticated run already resolved. `branch` names the
/// branch in the refusal; `None` uses the job's own.
pub(crate) async fn publication_authority(
    dbs: &crate::db::DbState,
    job_id: &str,
    run_id: Option<&str>,
    branch: Option<&str>,
) -> PublicationAuthority {
    match crate::execution::routing::routing_db_for_id(dbs, job_id).await {
        Ok(db) => publication_authority_in(&db, job_id, run_id, branch).await,
        Err(error) => PublicationAuthority::Unknown(error.to_string()),
    }
}

pub(crate) async fn publication_authority_in(
    db: &crate::storage::LocalDb,
    job_id: &str,
    run_id: Option<&str>,
    branch: Option<&str>,
) -> PublicationAuthority {
    let found = db
        .query_opt(
            "SELECT j.status, i.status, p.key, i.number, COALESCE(j.branch, j.base_branch)
               FROM jobs j
               JOIN projects p ON p.id = j.project_id
               LEFT JOIN issues i ON i.id = j.issue_id
              WHERE j.id = ?1",
            params![job_id.to_string()],
            |row| {
                Ok((
                    row.text(0)?,
                    row.opt_text(1)?,
                    row.text(2)?,
                    row.opt_i64(3)?,
                    row.opt_text(4)?,
                ))
            },
        )
        .await;
    let (job_status, issue_status, project_key, issue_number, job_branch) = match found {
        Ok(Some(found)) => found,
        Ok(None) => return PublicationAuthority::Authorized,
        Err(error) => return PublicationAuthority::Unknown(error.to_string()),
    };
    let mut cause = match issue_status.as_deref() {
        Some("merged") => Some(RetirementCause::IssueMerged),
        Some("closed") => Some(RetirementCause::IssueClosed),
        _ if job_status == "cancelled" => Some(RetirementCause::JobCancelled),
        _ => None,
    };
    if let (None, Some(run_id)) = (cause, run_id) {
        let session = db
            .query_opt(
                "SELECT s.status, s.terminal_reason
                   FROM runs r JOIN sessions s ON s.id = r.session_id
                  WHERE r.id = ?1",
                params![run_id.to_string()],
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await;
        match session {
            Ok(Some((status, reason)))
                if status == "closed"
                    && matches!(reason.as_deref(), Some("issue_merged" | "issue_closed")) =>
            {
                cause = Some(RetirementCause::SessionClosed)
            }
            Ok(_) => {}
            Err(error) => return PublicationAuthority::Unknown(error.to_string()),
        }
    }
    match cause {
        None => PublicationAuthority::Authorized,
        Some(cause) => PublicationAuthority::Retired(BranchRetired {
            branch: branch
                .map(str::to_string)
                .or(job_branch)
                .unwrap_or_else(|| "this session's branch".to_string()),
            job_id: job_id.to_string(),
            issue: issue_number.map(|number| format!("{project_key}/{number}")),
            cause,
        }),
    }
}

/// The authority for a held delta that does not name the run that sealed it
/// (rows held before `held_publications.run_id` existed).
///
/// Its originating session cannot be proven. The row's `created_at` is when
/// publication failed, not when the work began, so a batch that started before
/// a resolution and outlived it is held after the session closed and no
/// timestamp separates it from the successor's work. Every such row predates
/// the column (new holds always name their run), so the fallback is
/// conservative: once any resolution has closed a session of the job, the
/// row's origin is ambiguous and it is refused. It keeps its recovery ref, so
/// the work stays reachable by hand. Asked in addition to
/// [`publication_authority`], which still judges job and issue.
pub(crate) async fn unattributed_held_authority(
    dbs: &crate::db::DbState,
    job_id: &str,
    branch: &str,
) -> PublicationAuthority {
    let db = match crate::execution::routing::routing_db_for_id(dbs, job_id).await {
        Ok(db) => db,
        Err(error) => return PublicationAuthority::Unknown(error.to_string()),
    };
    let closed = db
        .query_opt(
            "SELECT p.key, i.number
               FROM sessions s
               JOIN jobs j ON j.id = s.job_id
               JOIN projects p ON p.id = j.project_id
               LEFT JOIN issues i ON i.id = j.issue_id
              WHERE s.job_id = ?1 AND s.status = 'closed'
                AND s.terminal_reason IN ('issue_merged', 'issue_closed')
              LIMIT 1",
            params![job_id.to_string()],
            |row| Ok((row.text(0)?, row.opt_i64(1)?)),
        )
        .await;
    match closed {
        Ok(None) => PublicationAuthority::Authorized,
        Ok(Some((project_key, number))) => PublicationAuthority::Retired(BranchRetired {
            branch: branch.to_string(),
            job_id: job_id.to_string(),
            issue: number.map(|number| format!("{project_key}/{number}")),
            cause: RetirementCause::SessionClosed,
        }),
        Err(error) => PublicationAuthority::Unknown(error.to_string()),
    }
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
        /// Why the store refused to combine them, when a repair ran and said.
        reason: Option<String>,
        /// The exact commands a maintainer can run to combine the versions.
        commands: Vec<String>,
    },
    /// The branch the calling job minted for itself no longer exists, so no
    /// file read, file write, or command on it can be served.
    OwnBranchLost {
        branch: String,
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
                reason,
                commands,
            } => {
                write!(
                    f,
                    "Branch '{requested}' has {} different versions of its history recorded ({}), \
                     and they could not be combined automatically",
                    versions.len(),
                    versions.join(" and ")
                )?;
                if let Some(reason) = reason {
                    write!(f, " because {reason}")?;
                }
                write!(
                    f,
                    ". You can still read and search files at any one of them by using that \
                     version's id in place of the branch name. Nothing has been discarded. The \
                     branch's rebase page (`cairn:~/rebase` on the node that owns it) lists every \
                     version and what combining them involves."
                )?;
                if !commands.is_empty() {
                    write!(
                        f,
                        " A maintainer can combine them with these commands, choosing one \
                         ordering:\n\n```\n{}\n```",
                        commands.join("\n")
                    )?;
                }
                Ok(())
            }
            // Written for a model that would otherwise retry: it names the
            // terminal fact, says the session did not cause it, rules out
            // retrying, and names what still works and what to do next.
            Self::OwnBranchLost { branch } => write!(
                f,
                "This session's own branch '{branch}' no longer exists, so no file read, file \
                 write, or command on it can be served. Nothing this session did removed it, and \
                 retrying will not bring it back. Reading files by absolute path and reading or \
                 writing cairn:// resources still work. Stop here: put anything worth keeping \
                 where the next attempt will find it, such as a comment on the issue, then end \
                 your turn. A new execution of the issue starts on a fresh branch."
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
        cairn_vcs::CoordinateResolutionError::Conflicted {
            targets, refusal, ..
        } => BranchRefResolutionError::Conflicted {
            requested: requested.to_string(),
            versions: targets,
            reason: refusal.as_ref().map(|refusal| refusal.reason.clone()),
            commands: refusal.map(|refusal| refusal.commands).unwrap_or_default(),
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
        Some(job) => node_read_target(orch, branch, job).await?,
        None => {
            let project = project_context_for_request(orch, request).await?;
            let rev = raw_read_rev(branch, &project);
            (project, rev)
        }
    };
    resolve_read_in_project(orch, project, rev, branch).await
}

/// [`resolve_for_read`] for a surface addressed by project URI: a raw ref
/// resolves in `project_id` whoever asks (a read with no run, such as the
/// desktop's own, included), and a node URI must name a node of that project.
pub(crate) async fn resolve_for_project_read(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
) -> Result<BranchResolution, String> {
    let (project, rev) = match resolve_node_uri(orch, branch).await? {
        Some(job) => node_read_target(orch, branch, job).await?,
        None => {
            let project = project_context_by_id(orch, project_id).await?;
            let rev = raw_read_rev(branch, &project);
            (project, rev)
        }
    };
    if project.project_id != project_id {
        return Err(format!(
            "'{branch}' names a node in a different project; refusing to substitute its branch"
        ));
    }
    resolve_read_in_project(orch, project, rev, branch).await
}

async fn node_read_target(
    orch: &Orchestrator,
    branch: &str,
    job: JobBranchContext,
) -> Result<(ProjectBranchContext, String), String> {
    let rev = job
        .branch
        .ok_or_else(|| format!("Node URI '{branch}' has no recorded branch"))?;
    Ok((project_context_by_id(orch, &job.project_id).await?, rev))
}

/// A raw ref as the store resolves it: `main` is the project's default branch
/// whatever that is named; anything else is passed through.
fn raw_read_rev(branch: &str, project: &ProjectBranchContext) -> String {
    if branch.trim() == "main" {
        project.default_branch.clone()
    } else {
        branch.trim().to_string()
    }
}

async fn resolve_read_in_project(
    orch: &Orchestrator,
    project: ProjectBranchContext,
    rev: String,
    branch: &str,
) -> Result<BranchResolution, String> {
    let project_path = project
        .project_path
        .ok_or_else(|| format!("project '{}' has no local repository", project.project_id))?;
    let repository_path = {
        let jj_binary_path = orch.jj_binary_path.clone();
        let config_dir = orch.config_dir.clone();
        let project_repo = project_path.clone();
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::environment(&jj_binary_path, &config_dir);
            crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
        })
        .await
        .map_err(|error| format!("coordinate repository task failed: {error}"))??
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
        default_branch: project.default_branch,
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
    let revision_store = revision_store.to_path_buf();

    Ok(RunBranchResolution {
        project_id: project.project_id,
        repository_path,
        revision_store,
        rev,
        commit_id,
        default_branch: project.default_branch,
    })
}

/// Resolve a revision named for a surface already bound to one project, taking
/// every form `read ?branch=` and `run branch:` take: a branch name (`main`
/// standing for the project's default branch), a commit or change id, or a node
/// URI standing for the branch that node recorded. Resolution runs through the
/// same conflict-repairing path, in `repository`.
///
/// A node URI must name a node of `project_id`. The answer is about this
/// project's repository, and quietly substituting another project's branch would
/// answer a different question.
pub(crate) async fn resolve_in_project(
    orch: &Orchestrator,
    project_id: &str,
    repository: &Path,
    default_branch: &str,
    requested: &str,
) -> Result<String, BranchRefResolutionError> {
    let unresolvable = |diagnostic: String| BranchRefResolutionError::Unresolvable {
        requested: requested.to_string(),
        diagnostic,
    };
    if requested.trim().is_empty() {
        return Err(BranchRefResolutionError::Invalid {
            requested: requested.to_string(),
        });
    }
    let rev = match resolve_node_uri(orch, requested)
        .await
        .map_err(unresolvable)?
    {
        Some(job) if job.project_id != project_id => {
            return Err(unresolvable(format!(
                "it names a node of another project, and this request belongs to project \
                 '{project_id}'"
            )));
        }
        Some(job) => job
            .branch
            .ok_or_else(|| unresolvable("the node has no recorded branch".into()))?,
        None if requested.trim() == "main" => default_branch.to_string(),
        None => requested.trim().to_string(),
    };
    resolve_coordinate_repairing_conflicts(orch, repository, &rev, default_branch)
        .await
        .map_err(|error| map_coordinate_error(error, requested))
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
                let settings = crate::config::project_settings::load_project_settings_read_only(
                    Path::new(path),
                );
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
    let project = cairn_uri::canonical_project(project);
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
                            "No job found for branch node URI '{project}/{number}/{exec_seq}/{node_id}'"
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
    use super::{
        map_own_coordinate_error, publication_authority_in, resolve_job_rev,
        BranchRefResolutionError, PublicationAuthority, RetirementCause, BRANCH_RETIRED,
    };

    async fn authority_db(issue_status: &str, job_status: &str) -> crate::storage::LocalDb {
        let db = crate::storage::migrated_test_db("publication-authority.db").await;
        db.execute_script(&format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p-1', 'w', 'Proj', 'proj', '/tmp/proj', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES('issue-1', 'p-1', 7, 'Issue', '{issue_status}', 'idle', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, node_name, status, branch, created_at, updated_at)
             VALUES('job-1', 'p-1', 'issue-1', 'builder', '{job_status}', 'agent/proj-7-builder-0', 1, 1);
            "
        ))
        .await
        .unwrap();
        db
    }

    /// A live job on an open issue publishes; nothing about the branch changed.
    #[tokio::test]
    async fn a_live_job_on_an_open_issue_may_publish() {
        let db = authority_db("active", "running").await;
        assert_eq!(
            publication_authority_in(&db, "job-1", None, Some("agent/proj-7-builder-0")).await,
            PublicationAuthority::Authorized
        );
    }

    /// A merged or closed issue retires its branch for every job on it, the
    /// ones the resolution only parked as much as the ones it cancelled.
    #[tokio::test]
    async fn a_resolved_issue_retires_the_branch_with_its_resolution_as_cause() {
        for (status, job_status, cause) in [
            ("merged", "complete", RetirementCause::IssueMerged),
            ("merged", "cancelled", RetirementCause::IssueMerged),
            ("closed", "running", RetirementCause::IssueClosed),
        ] {
            let db = authority_db(status, job_status).await;
            let PublicationAuthority::Retired(retired) =
                publication_authority_in(&db, "job-1", None, Some("agent/proj-7-builder-0")).await
            else {
                panic!("{status}/{job_status}: expected the branch to be retired");
            };
            assert_eq!(retired.cause, cause, "{status}/{job_status}");
            assert_eq!(retired.issue.as_deref(), Some("proj/7"));
            let text = retired.to_string();
            assert!(text.starts_with(BRANCH_RETIRED), "{text}");
            assert!(text.contains("Nothing was committed"), "{text}");
            assert!(text.contains("agent/proj-7-builder-0"), "{text}");
        }
    }

    /// An archived job on an open issue is refused: archival is how a
    /// resolution settles a dependent issue's work, whose own issue stays open.
    /// (A reopen lifts resolution cancellations; what then stays refused is the
    /// closed session, pinned in `issues::status` tests.)
    #[tokio::test]
    async fn a_cancelled_job_on_an_open_issue_is_refused() {
        let db = authority_db("active", "cancelled").await;
        let PublicationAuthority::Retired(retired) =
            publication_authority_in(&db, "job-1", None, Some("agent/proj-7-builder-0")).await
        else {
            panic!("a cancelled job must not publish");
        };
        assert_eq!(retired.cause, RetirementCause::JobCancelled);
    }

    /// A job with no row has no lifecycle to enforce.
    #[tokio::test]
    async fn an_unknown_job_is_not_refused() {
        let db = authority_db("active", "running").await;
        assert_eq!(
            publication_authority_in(&db, "job-missing", None, Some("main")).await,
            PublicationAuthority::Authorized
        );
    }

    fn absent(coordinate: &str) -> cairn_vcs::CoordinateResolutionError {
        cairn_vcs::CoordinateResolutionError::Absent {
            coordinate: coordinate.to_string(),
            diagnostic: format!("Revision `{coordinate}` doesn't exist"),
        }
    }

    /// The job's own minted branch going missing is a different fact from a
    /// mistyped ref: the first means nothing in this session can proceed.
    #[test]
    fn an_absent_own_branch_is_a_lost_branch_not_an_unresolvable_ref() {
        let branch = "agent/cairn-4897-builder-0";
        assert_eq!(
            map_own_coordinate_error(absent(branch), branch, Some(branch)),
            BranchRefResolutionError::OwnBranchLost {
                branch: branch.to_string()
            }
        );
    }

    /// A branchless job resolving its base, or a base that is absent, keeps the
    /// generic refusal: the job never owned that name.
    #[test]
    fn an_absent_base_stays_an_unresolvable_ref() {
        let base = "agent/cairn-1-coordinator-0";
        assert!(matches!(
            map_own_coordinate_error(absent(base), base, None),
            BranchRefResolutionError::Unresolvable { .. }
        ));
        assert!(matches!(
            map_own_coordinate_error(absent(base), base, Some("agent/cairn-1-builder-0")),
            BranchRefResolutionError::Unresolvable { .. }
        ));
    }

    #[test]
    fn the_lost_branch_refusal_says_stop_and_names_what_still_works() {
        let text = BranchRefResolutionError::OwnBranchLost {
            branch: "agent/cairn-4897-builder-0".into(),
        }
        .to_string();
        assert!(text.contains("agent/cairn-4897-builder-0"), "{text}");
        assert!(text.contains("no longer exists"), "{text}");
        assert!(text.contains("retrying will not"), "{text}");
        assert!(text.contains("Stop here"), "{text}");
        assert!(text.contains("absolute path"), "{text}");
        assert!(!text.contains("Could not resolve"), "{text}");
        crate::system_prompt::assert_no_substrate_vocabulary("lost branch refusal", &text);
        for jargon in ["bookmark", "revset", "jj", "store", "rollback"] {
            assert!(
                !text.to_lowercase().contains(jargon),
                "the refusal names the machinery ({jargon:?}) instead of the agent's options: {text}"
            );
        }
    }

    #[test]
    fn job_rev_prefers_its_own_branch() {
        assert_eq!(
            resolve_job_rev(
                Some("agent/cairn-1-builder-1".into()),
                Some("agent/cairn-1-coordinator-0".into()),
                "main"
            ),
            "agent/cairn-1-builder-1"
        );
    }

    #[test]
    fn branchless_job_falls_back_to_its_base_branch() {
        // The base branch target: the job mints no branch and works directly on
        // the coordinate it was launched from, which for a child issue is its
        // parent's integration branch — not the project default.
        assert_eq!(
            resolve_job_rev(None, Some("agent/cairn-1-coordinator-0".into()), "main"),
            "agent/cairn-1-coordinator-0"
        );
    }

    #[test]
    fn branchless_job_with_no_base_falls_back_to_the_project_default() {
        assert_eq!(resolve_job_rev(None, None, "trunk"), "trunk");
    }

    /// The one refusal in this family that survives automatic repair, so it is
    /// the one whose text an agent actually has to be able to act on. Its prose
    /// has to leave a way forward and name it in ordinary words: no talk of
    /// bookmarks, exports, coordinates, or anything else naming a mechanism the
    /// agent has no access to. The maintainer's commands follow in their own
    /// block, verbatim, because a maintainer needs them exact.
    #[test]
    fn the_conflicted_branch_refusal_offers_a_way_forward_in_plain_language() {
        let commands = vec![
            "# Keep aaaa1111 as the base and replay bbbb2222's own commits onto it".to_string(),
            "jj -R '/store' --ignore-working-copy rebase -r 'fork_point(aaaa1111|bbbb2222)..bbbb2222' -d aaaa1111".to_string(),
            "jj -R '/store' --ignore-working-copy bookmark set 'agent/cairn-1-builder-1' --allow-backwards -r 'aaaa1111:: & change_id(zzzz)'".to_string(),
        ];
        let full = BranchRefResolutionError::Conflicted {
            requested: "agent/cairn-1-builder-1".into(),
            versions: vec!["aaaa1111".into(), "bbbb2222".into()],
            reason: Some("neither version is a rewrite of the commit both moved away from".into()),
            commands: commands.clone(),
        }
        .to_string();

        // The maintainer's way out is the exact commands, not "needs a maintainer".
        assert!(!full.contains("needs a maintainer"), "{full}");
        for command in &commands {
            assert!(full.contains(command.as_str()), "{full}");
        }
        assert!(full.contains("cairn:~/rebase"), "{full}");
        let (text, _) = full
            .split_once("```")
            .expect("the commands sit in their own block");

        assert!(text.contains("agent/cairn-1-builder-1"), "{text}");
        assert!(
            text.contains("aaaa1111") && text.contains("bbbb2222"),
            "{text}"
        );
        assert!(text.contains("neither version is a rewrite"), "{text}");
        // What the agent can still do, stated as an action rather than implied.
        assert!(text.contains("read and search files"), "{text}");
        crate::system_prompt::assert_no_substrate_vocabulary("conflicted branch refusal", text);
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
