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
    /// The project's configured default branch, which `rev` names when the
    /// caller asked for `main`.
    pub default_branch: String,
}

/// Compensate a branch allocation that never sealed a commit. The bookmark is
/// removed first; only after that succeeds do we transactionally remove the
/// entity/job and return the counter to the number this mint consumed.
pub(crate) async fn rollback_thread_branch_mint(
    orch: &Orchestrator,
    minted: &MintedThreadBranch,
) -> Result<(), String> {
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let repository_path = minted.resolution.repository_path.clone();
    let git_ref = minted.git_ref.clone();
    tokio::task::spawn_blocking(move || {
        let jj = crate::jj::environment(&jj_binary_path, &config_dir);
        crate::jj::delete_bookmark(&jj, &repository_path, &git_ref)
    })
    .await
    .map_err(|error| format!("bookmark rollback task failed: {error}"))??;

    rollback_thread_branch_allocation(orch, &minted.job_id, minted.number).await
}

/// Remove the database half of a provisional mint. This is separate from
/// bookmark cleanup because a failed bookmark creation must not delete a
/// same-named bookmark that predated the mint attempt.
async fn rollback_thread_branch_allocation(
    orch: &Orchestrator,
    job_id: &str,
    number: i64,
) -> Result<(), String> {
    let job_id = job_id.to_string();
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &job_id).await?;
    db.write(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT thread_id FROM thread_branches WHERE job_id = ?1 AND number = ?2 LIMIT 1",
                    params![job_id.as_str(), number],
                )
                .await?;
            let thread_id = rows.next().await?.ok_or_else(|| {
                crate::storage::DbError::Row(format!(
                    "provisional thread branch {number} for job {job_id} was not found"
                ))
            })?.text(0)?;
            conn.execute(
                "DELETE FROM thread_branches WHERE job_id = ?1 AND number = ?2",
                params![job_id.as_str(), number],
            ).await?;
            conn.execute("DELETE FROM jobs WHERE id = ?1", params![job_id.as_str()]).await?;
            conn.execute(
                "UPDATE threads SET next_branch_number = ?1, updated_at = unixepoch() WHERE id = ?2 AND next_branch_number = ?3",
                params![number, thread_id.as_str(), number + 1],
            ).await?;
            Ok(())
        })
    }).await.map_err(|error| error.to_string())
}

#[derive(Debug, Clone)]
pub(crate) struct MintedThreadBranch {
    pub resolution: BranchResolution,
    pub job_id: String,
    pub number: i64,
    pub git_ref: String,
    pub uri: String,
}

/// Resolve the provisional identity a thread-branch preview would mint without
/// advancing the counter, creating a job/entity row, or materializing a ref.
pub(crate) async fn preview_thread_branch_for_write(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<(i64, String, String), String> {
    let (run, db) = super::run_context::lookup_run_routed(&orch.db, request).await?;
    let job_id = run.job_id;
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT t.name, t.next_branch_number, p.key FROM jobs j JOIN threads t ON t.id = j.thread_id JOIN projects p ON p.id = j.project_id WHERE j.id = ?1 AND j.parent_job_id IS NULL AND j.uri_segment = 'thread' LIMIT 1",
                params![job_id.as_str()],
            ).await?;
            let row = rows.next().await?.ok_or_else(|| crate::storage::DbError::Row(
                "Numbered branches can only be previewed by a thread session".to_string()
            ))?;
            let thread_name = row.text(0)?;
            let number = row.i64(1)?;
            let project_key = row.text(2)?;
            Ok((
                number,
                format!("agent/{thread_name}/{number}"),
                format!("cairn://p/{project_key}/{thread_name}/branches/{number}"),
            ))
        })
    }).await.map_err(|error| error.to_string())
}

/// Allocate a numbered branch owned by the calling thread session. The database
/// allocation is one transaction; the bookmark is then materialized at the
/// already-resolved base commit without creating a workspace.
///
/// The caller must hold the per-store lock for the whole call.
pub(crate) async fn mint_thread_branch_for_write(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    title: Option<String>,
) -> Result<MintedThreadBranch, String> {
    let current = resolve_current_for_read(orch, request).await?;
    let (run, db) = super::run_context::lookup_run_routed(&orch.db, request).await?;
    let session_job_id = run.job_id.clone();
    let project_id = run.project_id.clone();
    let now = chrono::Utc::now().timestamp();
    let base_commit = current.commit_id.clone();
    let (number, git_ref, branch_job_id, project_key, thread_name) = db
        .write(move |conn| {
            let session_job_id = session_job_id.clone();
            let project_id = project_id.clone();
            let base_commit = base_commit.clone();
            let title = title.clone();
            Box::pin(async move {
                let mut rows = conn.query(
                    "SELECT t.id, t.name, t.next_branch_number, p.key FROM jobs j JOIN threads t ON t.id = j.thread_id JOIN projects p ON p.id = j.project_id WHERE j.id = ?1 AND j.parent_job_id IS NULL AND j.uri_segment = 'thread' LIMIT 1",
                    params![session_job_id.as_str()],
                ).await?;
                let row = rows.next().await?.ok_or_else(|| crate::storage::DbError::Row(
                    "Numbered branches can only be minted by a thread session".to_string()
                ))?;
                let thread_id = row.text(0)?;
                let thread_name = row.text(1)?;
                let number = row.i64(2)?;
                let project_key = row.text(3)?;
                let git_ref = format!("agent/{thread_name}/{number}");
                let branch_job_id = uuid::Uuid::new_v4().to_string();
                let branch_id = uuid::Uuid::new_v4().to_string();
                let segment = crate::node_segments::allocate_child_task_segment(
                    conn, session_job_id.as_str(), &format!("branch-{number}"),
                ).await?;
                conn.execute("UPDATE threads SET next_branch_number = ?1, updated_at = ?2 WHERE id = ?3",
                    params![number + 1, now, thread_id.as_str()]).await?;
                conn.execute(
                    "INSERT INTO jobs (id, thread_id, parent_job_id, project_id, branch, base_commit, status, uri_segment, node_name, created_at, updated_at, completed_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'complete', ?7, ?8, ?9, ?9, ?9)",
                    params![branch_job_id.as_str(), thread_id.as_str(), session_job_id.as_str(), project_id.as_str(), git_ref.as_str(), base_commit.as_str(), segment.as_str(), format!("Branch {number}"), now],
                ).await?;
                conn.execute(
                    "INSERT INTO thread_branches (id, thread_id, number, title, stage, git_ref, job_id, created_at, moved_at) VALUES (?1, ?2, ?3, ?4, 'accumulating', ?5, ?6, ?7, ?7)",
                    params![branch_id, thread_id, number, title, git_ref.as_str(), branch_job_id.as_str(), now],
                ).await?;
                Ok((number, git_ref, branch_job_id, project_key, thread_name))
            })
        }).await.map_err(|error| error.to_string())?;
    let jj_binary_path = orch.jj_binary_path.clone();
    let config_dir = orch.config_dir.clone();
    let repository_path = current.repository_path.clone();
    let create_ref = git_ref.clone();
    let base = current.commit_id.clone();
    // The caller holds the per-store lock: the only call site is a file-bearing
    // write batch inside its "logical write transaction" guard. That is what
    // keeps another writer's rollback from erasing this create (see
    // `prepare_store_coordinate`). The lock is not reentrant, so this must not
    // take it again.
    let bookmark_result = match tokio::task::spawn_blocking(move || {
        let jj = crate::jj::environment(&jj_binary_path, &config_dir);
        crate::jj::create_bookmark_at(&jj, &repository_path, &create_ref, &base)
    })
    .await
    {
        Ok(result) => result,
        Err(error) => {
            let error = format!("bookmark task failed: {error}");
            if let Err(rollback_error) =
                rollback_thread_branch_allocation(orch, &branch_job_id, number).await
            {
                return Err(format!(
                    "{error}; additionally failed to roll back provisional branch allocation: {rollback_error}"
                ));
            }
            return Err(error);
        }
    };
    if let Err(error) = bookmark_result {
        if let Err(rollback_error) =
            rollback_thread_branch_allocation(orch, &branch_job_id, number).await
        {
            return Err(format!(
                "{error}; additionally failed to roll back provisional branch allocation: {rollback_error}"
            ));
        }
        return Err(error);
    }
    Ok(MintedThreadBranch {
        resolution: BranchResolution {
            rev: git_ref.clone(),
            ..current
        },
        job_id: branch_job_id,
        number,
        uri: format!("cairn://p/{project_key}/{thread_name}/branches/{number}"),
        git_ref,
    })
}

/// Resolve an explicit write destination without changing the identity of the
/// calling run. Thread-branch URIs are translated through the entity table;
/// raw refs retain the general coordinate grammar used by reads.
pub(crate) async fn resolve_explicit_for_write(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    destination: &str,
) -> Result<(BranchResolution, Option<String>), String> {
    let current = resolve_current_for_read(orch, request).await?;
    let (run, db) = super::run_context::lookup_run_routed(&orch.db, request).await?;
    let entity_number = destination
        .strip_prefix("cairn:~/branches/")
        .and_then(|number| number.parse::<i64>().ok());
    // A raw git ref is not a cairn:// URI and a thread URI stops at
    // `Thread { path }` in the common parser, so the resolver decides whether
    // this destination names a numbered branch — the same reader read and write
    // dispatch use for every other thread sub-resource.
    let canonical = parse_uri(destination)
        .and_then(|parsed| crate::resources::delegate_thread_descendant(parsed).ok());
    let canonical_identity = match canonical.as_ref() {
        Some(CairnResource::ThreadBranch {
            project,
            thread,
            number,
        }) => Some((project.as_str(), thread.as_str(), i64::from(*number))),
        _ => None,
    };
    let number = entity_number.or(canonical_identity.map(|identity| identity.2));
    let (rev, branch_job_id) = if let Some(number) = number {
        let job_id = run.job_id.clone();
        let coordinates = db.read(move |conn| Box::pin(async move {
            let mut rows = conn.query(
                "SELECT b.git_ref, b.job_id, b.stage, p.key, t.name FROM thread_branches b JOIN threads t ON t.id = b.thread_id JOIN projects p ON p.id = t.project_id JOIN jobs session ON session.thread_id = b.thread_id WHERE session.id = ?1 AND b.number = ?2 LIMIT 1",
                params![job_id, number],
            ).await?;
            rows.next().await?.map(|row| Ok::<_, crate::storage::DbError>((row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?, row.text(4)?))).transpose()
        })).await.map_err(|e| e.to_string())?.ok_or_else(|| format!("Thread branch {number} was not found for this thread"))?;
        if let Some((project, thread, _)) = canonical_identity {
            if coordinates.3 != project || coordinates.4 != thread {
                return Err(format!("Refusing branch destination {destination}: it does not belong to the current thread ({}/{}).", coordinates.3, coordinates.4));
            }
        }
        if coordinates.2 == "merged" || coordinates.2 == "abandoned" {
            return Err(format!("Refusing write branch destination {number}: stage '{}' is terminal; mint a new branch instead", coordinates.2));
        }
        (coordinates.0, Some(coordinates.1))
    } else {
        (destination.to_string(), None)
    };
    let commit_id =
        resolve_coordinate_repairing_conflicts(orch, &current.repository_path, &rev, &current.rev)
            .await
            .map_err(|error| map_coordinate_error(error, &rev).to_string())?;
    Ok((
        BranchResolution {
            rev,
            commit_id,
            ..current
        },
        branch_job_id,
    ))
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
    let project = cairn_common::uri::canonical_project(project);
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
    use super::{map_own_coordinate_error, resolve_job_rev, BranchRefResolutionError};

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
