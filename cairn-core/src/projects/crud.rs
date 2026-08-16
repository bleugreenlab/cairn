use crate::db_records::DbProject;
use crate::error::CairnError;
use crate::models::CreateProject;
use crate::services::Clock;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_common::ids;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

/// Runtime paths required to establish the Cairn-managed jj store while a
/// project is created.
#[derive(Clone, Copy)]
pub struct StoreInit<'a> {
    pub jj_binary_path: &'a str,
    pub config_dir: &'a Path,
}

/// Full project creation: DB insert + filesystem setup.
///
/// Project keys cross their canonicalization boundary here and are stored in
/// lowercase. Callers and downstream persistence therefore receive one identity.
///
/// - If `repo_path` is empty and `projects_dir` is provided, creates a directory
///   and initializes a git repo there.
/// - Creates `.cairn/config.yaml`.
/// - Skips filesystem setup for remote project bookmarks.
///
pub async fn create(
    db: &LocalDb,
    clock: &dyn Clock,
    mut input: CreateProject,
    projects_dir: Option<&Path>,
    store_init: Option<StoreInit<'_>>,
) -> Result<DbProject, CairnError> {
    input.key = cairn_common::uri::canonical_project(&input.key);
    if db
        .query_opt_text(
            "SELECT id FROM projects WHERE key = ?1 LIMIT 1",
            (input.key.clone(),),
        )
        .await?
        .is_some()
    {
        return Err(CairnError::Internal(format!(
            "a project with key '{}' already exists",
            input.key
        )));
    }

    if input.repo_path.is_empty() {
        if let Some(base) = projects_dir {
            let project_dir = base.join(input.key.to_lowercase());
            std::fs::create_dir_all(&project_dir)?;

            input.repo_path = project_dir.to_string_lossy().to_string();
        }
    }

    let detected_branch = if input.repo_path.is_empty() {
        None
    } else {
        let repo_path = Path::new(&input.repo_path);
        if repo_path.exists() {
            if repo_path.join(".git").exists() {
                detect_default_branch(repo_path)
            } else {
                Some("main".to_string())
            }
        } else {
            None
        }
    };

    // Establish that the row can be inserted before mutating a user-selected
    // folder. The detected branch is part of that same insert, so creation
    // cannot fail later with a durable row left behind.
    let db_project = create_db_with_default_branch(
        db,
        clock,
        &input,
        detected_branch.as_deref().unwrap_or("main"),
    )
    .await?;

    if !input.repo_path.is_empty() {
        let repo_path = Path::new(&input.repo_path);
        if !repo_path.exists() {
            let error = CairnError::Internal(format!(
                "project repository path does not exist: {}",
                repo_path.display()
            ));
            if let Err(rollback_error) = delete_db(db, &db_project.id).await {
                return Err(CairnError::Internal(format!(
                    "{error}; additionally failed to roll back project row: {rollback_error}"
                )));
            }
            return Err(error);
        }

        if let Err(error) = ensure_project_repository(repo_path) {
            if let Err(rollback_error) = delete_db(db, &db_project.id).await {
                return Err(CairnError::Internal(format!(
                    "{error}; additionally failed to roll back project row: {rollback_error}"
                )));
            }
            return Err(error);
        }

        if let Some(store_init) = store_init {
            let jj_binary_path = store_init.jj_binary_path.to_string();
            let config_dir = store_init.config_dir.to_path_buf();
            let project_repo = repo_path.to_path_buf();
            let store_result = tokio::task::spawn_blocking(move || {
                let jj = crate::jj::JjEnv::with_binary(jj_binary_path, &config_dir);
                crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
            })
            .await
            .map_err(|error| format!("jj store initialization task failed: {error}"))
            .and_then(|result| result);

            if let Err(error) = store_result {
                if let Err(rollback_error) = delete_db(db, &db_project.id).await {
                    return Err(CairnError::Internal(format!(
                        "failed to initialize the Cairn-managed jj store for {}: {error}; additionally failed to roll back project row: {rollback_error}",
                        repo_path.display()
                    )));
                }
                return Err(CairnError::Internal(format!(
                    "failed to initialize the Cairn-managed jj store for {}: {error}. The project row was rolled back; repository files were left in place, and retrying with the same folder is safe",
                    repo_path.display()
                )));
            }
        }
    }

    Ok(db_project)
}

/// Repair project rows created before repository bootstrapping was enforced.
pub(crate) async fn reconcile_project_repositories(db: &LocalDb) {
    let projects = match list_db(db).await {
        Ok(projects) => projects,
        Err(e) => {
            log::warn!("repository repair: failed to load projects: {e}");
            return;
        }
    };

    for project in projects {
        if project.repo_path.is_empty() || project.is_workspace != 0 {
            continue;
        }
        let repo_path = Path::new(&project.repo_path);
        if !repo_path.exists() || repo_path.join(".git").exists() {
            continue;
        }
        match ensure_project_repository(repo_path) {
            Ok(()) => log::info!(
                "repository repair: initialized project {} at {}",
                project.id,
                repo_path.display()
            ),
            Err(e) => log::warn!(
                "repository repair: failed to initialize project {} at {}: {e}",
                project.id,
                repo_path.display()
            ),
        }
    }
}

/// The single repository answer for project-scoped Git and GitHub operations.
///
/// Portable project metadata comes from the owning database, while the checkout
/// path is resolved through this machine's private route overlay for team projects.
pub struct ProjectRepository {
    pub db: Arc<LocalDb>,
    pub project_id: String,
    pub project_key: String,
    pub local_repo_path: PathBuf,
    pub remote_url: Option<String>,
    pub default_branch: String,
    pub is_workspace: bool,
}

pub async fn resolve_project_repository(
    dbs: &crate::db::DbState,
    project_id: &str,
    config_dir: &Path,
) -> Result<ProjectRepository, CairnError> {
    let db = owning_db(dbs, project_id).await?;
    let project = get_db(&db, project_id)
        .await?
        .ok_or_else(|| CairnError::NotFound {
            entity: "project",
            id: project_id.to_string(),
        })?;
    let local_path = resolve_local_repo_path(dbs, &project.id, &project.key, &project.repo_path)
        .await?
        .filter(|path| !path.trim().is_empty())
        .ok_or_else(|| {
            CairnError::Internal(format!(
                "project {} has no checkout registered on this machine",
                project.key
            ))
        })?;
    let local_repo_path = PathBuf::from(local_path);
    validate_project_checkout(&local_repo_path, config_dir)?;
    let remote_url = remote_url(&db, project_id)
        .await?
        .map(|url| crate::projects::remote::normalize_remote_url(&url))
        .transpose()?;

    Ok(ProjectRepository {
        db,
        project_id: project.id,
        project_key: project.key,
        local_repo_path,
        remote_url,
        default_branch: project.default_branch.unwrap_or_else(|| "main".to_string()),
        is_workspace: project.is_workspace != 0,
    })
}

fn validate_project_checkout(path: &Path, config_dir: &Path) -> Result<(), CairnError> {
    if !path.is_absolute() || !path.is_dir() {
        return Err(CairnError::Internal(format!(
            "registered project checkout is unavailable: {}",
            path.display()
        )));
    }
    let canonical = path.canonicalize()?;
    let config = config_dir
        .canonicalize()
        .unwrap_or_else(|_| config_dir.to_path_buf());
    if canonical.starts_with(config.join("scratch"))
        || canonical.starts_with(config.join("build-slots"))
        || canonical.starts_with(config.join("executions"))
        || canonical
            .components()
            .any(|component| matches!(component, Component::Normal(name) if name == "build-slots"))
    {
        return Err(CairnError::Internal(format!(
            "registered project checkout points into disposable execution storage: {}",
            canonical.display()
        )));
    }
    Ok(())
}

/// Make the selected folder a self-contained project repository with a usable HEAD.
fn ensure_project_repository(repo_path: &Path) -> Result<(), CairnError> {
    if !repo_path.join(".git").exists() {
        // Inspect the selected root rather than rev-parse: a folder nested in a
        // parent repository is still its own Cairn project boundary.
        run_git(&["init", "-b", "main"], repo_path)?;
    }

    crate::config::project_settings::create_default_project_config(repo_path)
        .map_err(CairnError::Internal)?;
    ensure_initial_commit(repo_path)
}

/// Ensure a local project repository has at least one commit so git worktrees can branch from it.
fn ensure_initial_commit(repo_path: &Path) -> Result<(), CairnError> {
    let has_head = std::process::Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .current_dir(repo_path)
        .output()?;
    if has_head.status.success() {
        return Ok(());
    }

    run_git(&["add", "-A"], repo_path)?;
    run_git(
        &[
            "-c",
            "user.name=Cairn",
            "-c",
            "user.email=cairn@local.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "Initial commit",
        ],
        repo_path,
    )
}

/// Detect a repository's default branch.
///
/// Prefers the remote's *real* default — `git ls-remote --symref origin HEAD`
/// reports `ref: refs/heads/<branch>\tHEAD`, which is authoritative even when the
/// local `refs/remotes/origin/HEAD` symref was never set. That unset-symref case
/// is exactly what left a repo whose trunk is e.g. "staging" stuck on the stored
/// "main". Falls back to the local symbolic-ref, then the currently checked-out
/// branch. Returns `None` when none resolve, in which case callers keep the
/// stored default.
fn detect_default_branch(repo_path: &Path) -> Option<String> {
    detect_default_branch_with_source(repo_path).map(|(branch, _)| branch)
}

/// Where a detected default branch came from. An *authoritative* detection (the
/// remote's real HEAD, or the local record of it) names the actual trunk; the
/// *checked-out-branch* fallback is a last resort that names whatever branch the
/// working tree happens to be on. The checked-out branch is acceptable as a
/// default only at create time / for a local-only repo with no remote signal —
/// the startup backfill must NOT persist it, because the user may be on any
/// feature or integration branch when the app starts, and persisting that would
/// corrupt the stored default exactly as the stale `main` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DefaultBranchSource {
    /// `git ls-remote --symref origin HEAD`, or local `refs/remotes/origin/HEAD`.
    Authoritative,
    /// `git rev-parse --abbrev-ref HEAD` — the transient checked-out branch.
    CheckedOut,
}

/// Detect a repository's default branch, tagging the result with its source so
/// callers can decide whether it is trustworthy enough to persist.
///
/// Prefers the remote's *real* default — `git ls-remote --symref origin HEAD`
/// reports `ref: refs/heads/<branch>\tHEAD`, authoritative even when the local
/// `refs/remotes/origin/HEAD` symref was never set. Falls back to that local
/// symbolic-ref (also authoritative — a deliberate record of the remote default),
/// then to the currently checked-out branch (NOT authoritative). Returns `None`
/// when none resolve, in which case callers keep the stored default.
fn detect_default_branch_with_source(repo_path: &Path) -> Option<(String, DefaultBranchSource)> {
    let git_line = |args: &[&str]| -> Option<String> {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(repo_path)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let line = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if line.is_empty() {
            None
        } else {
            Some(line)
        }
    };

    // Authoritative: ask the remote for its HEAD symref. Resolves even when the
    // local origin/HEAD was never set (the common cause of the stale "main").
    if let Some(stdout) = git_output(repo_path, &["ls-remote", "--symref", "origin", "HEAD"]) {
        if let Some(branch) = parse_ls_remote_symref(&stdout) {
            return Some((branch, DefaultBranchSource::Authoritative));
        }
    }

    // Authoritative: the local record of the remote default (origin/HEAD set
    // locally by clone or `git remote set-head`).
    if let Some(head) = git_line(&["symbolic-ref", "--short", "refs/remotes/origin/HEAD"]) {
        if let Some(branch) = head.strip_prefix("origin/") {
            if !branch.is_empty() {
                return Some((branch.to_string(), DefaultBranchSource::Authoritative));
            }
        }
    }

    // Last resort: the currently checked-out branch. NOT authoritative — a
    // transient signal the backfill must never persist.
    git_line(&["rev-parse", "--abbrev-ref", "HEAD"])
        .filter(|branch| branch != "HEAD")
        .map(|branch| (branch, DefaultBranchSource::CheckedOut))
}

/// Run a git command and return its stdout (untrimmed) when it succeeds. Unlike
/// the single-line helper inside `detect_default_branch`, this preserves the full
/// multi-line output that `ls-remote --symref` produces.
fn git_output(repo_path: &Path, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(repo_path)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).to_string())
}

/// Parse the branch name out of `git ls-remote --symref origin HEAD` output. The
/// relevant line is `ref: refs/heads/<branch>\tHEAD`; the SHA line that follows is
/// ignored. Returns `None` when no symref line is present (e.g. a detached remote
/// HEAD).
fn parse_ls_remote_symref(stdout: &str) -> Option<String> {
    stdout.lines().find_map(|line| {
        let rest = line.strip_prefix("ref: refs/heads/")?;
        let branch = rest.split_whitespace().next()?.trim();
        (!branch.is_empty()).then(|| branch.to_string())
    })
}

/// Run a git command in a directory, returning an error with stderr if it fails.
fn run_git(args: &[&str], dir: &Path) -> Result<(), CairnError> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CairnError::Internal(format!(
            "`git {}` failed: {}",
            args.join(" "),
            stderr.trim()
        )));
    }

    Ok(())
}

pub async fn create_db(
    db: &LocalDb,
    clock: &dyn Clock,
    input: &CreateProject,
) -> Result<DbProject, CairnError> {
    create_db_with_default_branch(db, clock, input, "main").await
}

async fn create_db_with_default_branch(
    db: &LocalDb,
    clock: &dyn Clock,
    input: &CreateProject,
    default_branch: &str,
) -> Result<DbProject, CairnError> {
    let now = clock.now() as i32;
    let id = input.id.clone().unwrap_or_else(|| {
        let scope = match &input.team_id {
            Some(t) => ids::RouteScope::Team(t.clone()),
            None => ids::RouteScope::Local,
        };
        ids::mint(scope).into_string()
    });
    let name = input.name.clone();
    let key = input.key.clone();
    let repo_path = input.repo_path.clone();
    // Repository identity is its own durable column even though Cairn's current
    // one-repository-per-project creation flow initially assigns the project UUID.
    let repository_id = id.clone();
    let team_id = input.team_id.clone();
    let default_branch = default_branch.to_string();

    db.write(|conn| {
        let id = id.clone();
        let name = name.clone();
        let key = key.clone();
        let repo_path = repo_path.clone();
        let team_id = team_id.clone();
        let repository_id = repository_id.clone();
        let default_branch = default_branch.clone();
        Box::pin(async move {
            match team_id {
                // Team replica: `projects` re-roots at `team_id` (NOT NULL FK to
                // `teams`) with no `workspace_id` column (CAIRN-2129 re-rooting).
                Some(team_id) => {
                    conn.execute(
                        "INSERT INTO projects(
                            id, team_id, name, key, repo_path, repository_id, context, docs_enabled,
                            default_branch, next_issue_number, created_at, updated_at,
                            is_workspace
                         )
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, '', 1, ?7, 1, ?8, ?9, 0)",
                        (
                            id.as_str(),
                            team_id.as_str(),
                            name.as_str(),
                            key.as_str(),
                            repo_path.as_str(),
                            repository_id.as_str(),
                            default_branch.as_str(),
                            now,
                            now,
                        ),
                    )
                    .await?;
                }
                // Private database: the original local-project insert, unchanged.
                None => {
                    conn.execute(
                        "INSERT INTO projects(
                            id, workspace_id, name, key, repo_path, repository_id, context, docs_enabled,
                            default_branch, next_issue_number, created_at, updated_at,
                            is_workspace
                         )
                         VALUES (?1, 'default', ?2, ?3, ?4, ?5, '', 1, ?6, 1, ?7, ?8, 0)",
                        (
                            id.as_str(),
                            name.as_str(),
                            key.as_str(),
                            repo_path.as_str(),
                            repository_id.as_str(),
                            default_branch.as_str(),
                            now,
                            now,
                        ),
                    )
                    .await?;
                }
            }
            Ok(())
        })
    })
    .await?;

    get_db(db, &id).await?.ok_or_else(|| CairnError::NotFound {
        entity: "project",
        id,
    })
}

/// Create a project, routing its row to the database its team selects.
///
/// This is the one place a project's home database is decided (CAIRN-2132). It
/// resolves the target database from `input.team_id` — the private database for
/// a local project (`None`), or the already-open team replica for a shared one —
/// writes the full `projects` row there, then records a `project_routes` stub in
/// the PRIVATE database and refreshes the in-memory route cache so subsequent
/// `for_project` lookups resolve correctly. The write commits to the local
/// replica only; sync propagation is a separate background concern.
///
/// For a local project this is a pure addition over [`create`]: the row still
/// lands in the private DB and the route stub carries a NULL team, which
/// `for_project` already treats as "private". Returns the created row alongside
/// the database it landed in, so callers read it back from the right place.
pub async fn create_routed(
    dbs: &crate::db::DbState,
    clock: &dyn Clock,
    input: CreateProject,
    projects_dir: Option<&Path>,
    store_init: Option<StoreInit<'_>>,
) -> Result<(DbProject, Arc<LocalDb>), CairnError> {
    let key = cairn_common::uri::canonical_project(&input.key);
    let team_id = input.team_id.clone();
    if dbs
        .local
        .query_opt_text(
            "SELECT project_key FROM project_routes WHERE project_key = ?1 LIMIT 1",
            (key.clone(),),
        )
        .await?
        .is_some()
    {
        return Err(CairnError::Internal(format!(
            "a project with key '{key}' already exists"
        )));
    }
    let target_db = match &team_id {
        None => dbs.local.clone(),
        Some(team) => dbs
            .team_db(team)
            .await
            .ok_or_else(|| CairnError::NotFound {
                entity: "team",
                id: team.clone(),
            })?,
    };
    let project = create(&target_db, clock, input, projects_dir, store_init).await?;
    if let Err(error) = insert_project_route(&dbs.local, clock, &key, team_id.as_deref()).await {
        if let Err(rollback_error) = delete_db(&target_db, &project.id).await {
            return Err(CairnError::Internal(format!(
                "a project with key '{key}' already exists; additionally failed to roll back project row: {rollback_error}"
            )));
        }
        log::warn!("route reservation for project key {key} failed: {error}");
        return Err(CairnError::Internal(format!(
            "a project with key '{key}' already exists"
        )));
    }
    if team_id.is_some() && !project.repo_path.is_empty() {
        set_local_repo_path(&dbs.local, &key, Path::new(&project.repo_path)).await?;
    }
    dbs.set_route(&key, team_id).await;
    Ok((project, target_db))
}

/// Resolve the database that owns the project with this `id`. An O(1) prefix
/// parse, fail-closed.
///
/// Collapses onto [`crate::execution::routing::routing_db_for_id`] — the same
/// router [`crate::execution::routing::owning_db_for_project`] uses — so the
/// id-keyed lifecycle seams (the Tauri `get_project`/`update_project`/… commands
/// and their headless equivalents) all share one routing answer. A bare (local)
/// id routes to the private database exactly as the prior `&db.local` path did;
/// a `{team}~…` id routes to that team's open replica. Fail-closed — a
/// team-prefixed id whose replica is not open returns an error rather than
/// silently falling back to the private database (the CAIRN-2170 split-brain
/// class).
pub async fn owning_db(dbs: &crate::db::DbState, id: &str) -> Result<Arc<LocalDb>, CairnError> {
    crate::execution::routing::routing_db_for_id(dbs, id).await
}

/// Effective local repository path for a project on this machine.
///
/// Local projects keep using `projects.repo_path`. Team projects resolve through
/// the private `project_routes.local_repo_path`, because the synced
/// `projects.repo_path` belongs to the creator's machine and must not be
/// overwritten by teammates.
pub async fn resolve_local_repo_path(
    dbs: &crate::db::DbState,
    project_id: &str,
    project_key: &str,
    synced_repo_path: &str,
) -> Result<Option<String>, CairnError> {
    match ids::parse_route_scope(project_id) {
        Ok(ids::RouteScope::Local) => Ok(Some(synced_repo_path.to_string())),
        Ok(ids::RouteScope::Team(_)) => local_repo_path(&dbs.local, project_key).await,
        Err(error) => Err(CairnError::Internal(format!("invalid project id: {error}"))),
    }
}

/// Resolve a project's effective local repository path and key by id.
pub async fn resolve_local_repo_path_and_key(
    dbs: &crate::db::DbState,
    project_id: &str,
) -> Result<(Option<String>, String), CairnError> {
    let db = owning_db(dbs, project_id).await?;
    let project = get_db(&db, project_id)
        .await?
        .ok_or_else(|| CairnError::NotFound {
            entity: "project",
            id: project_id.to_string(),
        })?;
    let local_path =
        resolve_local_repo_path(dbs, &project.id, &project.key, &project.repo_path).await?;
    Ok((local_path, project.key))
}

async fn local_repo_path(
    private_db: &LocalDb,
    project_key: &str,
) -> Result<Option<String>, CairnError> {
    let key = cairn_common::uri::canonical_project(project_key);
    private_db
        .query_opt_text(
            "SELECT local_repo_path FROM project_routes WHERE project_key = ?1",
            (key,),
        )
        .await
        .map_err(CairnError::from)
}

pub async fn set_local_repo_path(
    private_db: &LocalDb,
    project_key: &str,
    path: &Path,
) -> Result<(), CairnError> {
    let key = cairn_common::uri::canonical_project(project_key);
    let path = path.to_string_lossy().to_string();
    private_db
        .execute(
            "UPDATE project_routes SET local_repo_path = ?1 WHERE project_key = ?2",
            (path, key),
        )
        .await?;
    Ok(())
}

pub async fn remote_url(db: &LocalDb, id: &str) -> Result<Option<String>, CairnError> {
    let id = id.to_string();
    db.query_opt_text("SELECT remote_url FROM projects WHERE id = ?1", (id,))
        .await
        .map_err(CairnError::from)
}

pub async fn set_remote_url_db(db: &LocalDb, id: &str, remote_url: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    let remote_url = remote_url.to_string();
    db.execute(
        "UPDATE projects SET remote_url = ?1, updated_at = strftime('%s','now') WHERE id = ?2",
        (remote_url, id),
    )
    .await?;
    Ok(())
}

/// Record a project's routing target in the PRIVATE database's `project_routes`
/// catalog (CAIRN-2132). `team_id` is `None` for a local project — the row
/// stores NULL and `DbState::for_project` resolves it to the private database.
/// The key is normalized lowercase to match every other route lookup. Existing
/// routes are never replaced: the catalog is the global project-key uniqueness
/// boundary across private and team databases.
pub(crate) async fn insert_project_route(
    db: &LocalDb,
    clock: &dyn Clock,
    project_key: &str,
    team_id: Option<&str>,
) -> Result<(), CairnError> {
    let key = cairn_common::uri::canonical_project(project_key);
    let team_id = team_id.map(str::to_string);
    let now = clock.now();
    db.write(|conn| {
        let key = key.clone();
        let team_id = team_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO project_routes(project_key, team_id, created_at)
                 VALUES (?1, ?2, ?3)",
                (key.as_str(), team_id.as_deref(), now),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(CairnError::from)
}

/// Build the canonical `DbProject` SELECT for whichever schema this DB carries.
/// The private database roots projects at `workspace_id`; a team replica
/// re-roots them at `team_id` (CAIRN-2129). Aliasing `team_id AS workspace_id`
/// lets the single `db_project_from_row` decoder serve both, so a team project
/// surfaces with its team id in the `workspace_id` slot.
fn projects_select(db: &LocalDb, tail: &str) -> String {
    let root = if db.is_synced() {
        "team_id AS workspace_id"
    } else {
        "workspace_id"
    };
    format!(
        "SELECT id, {root}, name, key, repo_path, context, docs_enabled,
                default_branch, next_issue_number, created_at, updated_at,
                ci_commands, setup_commands, terminal_commands, config,
                hidden, is_workspace
         FROM projects {tail}"
    )
}

pub async fn get_db(db: &LocalDb, id: &str) -> Result<Option<DbProject>, CairnError> {
    let id = id.to_string();
    let sql = projects_select(db, "WHERE id = ?1");
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(&sql, (id,)).await?;
            rows.next()
                .await?
                .map(|row| db_project_from_row(&row))
                .transpose()
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn list_db(db: &LocalDb) -> Result<Vec<DbProject>, CairnError> {
    let sql = projects_select(db, "ORDER BY name ASC");
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(&sql, ()).await?;
            let mut projects = Vec::new();
            while let Some(row) = rows.next().await? {
                projects.push(db_project_from_row(&row)?);
            }
            Ok(projects)
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn seed_workspace_project_db(
    db: &LocalDb,
    clock: &dyn Clock,
    repo_path: &Path,
) -> Result<(), CairnError> {
    let now = clock.now() as i32;
    let repo_path = repo_path.to_string_lossy().to_string();
    db.write(|conn| {
        let repo_path = repo_path.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR IGNORE INTO projects(
                    id, workspace_id, name, key, repo_path, repository_id, context, docs_enabled,
                    default_branch, next_issue_number, created_at, updated_at,
                    hidden, is_workspace
                 )
                 VALUES ('workspace', 'default', 'Workspace', 'WS', ?1, 'workspace', '', 1,
                         'main', 1, ?2, ?3, 0, 1)",
                (repo_path.as_str(), now, now),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    crate::memories::db::backfill_workspace_project_id(db).await?;
    Ok(())
}

/// Reserved `projects.key` for every team's workspace project. Unique within a
/// team replica (the column is UNIQUE), so `INSERT OR IGNORE` on it is the
/// first-writer-wins guard even when concurrent members mint different ids.
///
/// It is **team-scoped**, not a bare constant, because the machine-local router
/// and clone-path catalog (`project_routes`) are keyed by `project_key` ALONE
/// across ALL teams on a machine. Two teams sharing a constant `WORKSPACE` key
/// would collide on that single primary-key row, so the last team to reconcile
/// would own the one route and `resolve_local_repo_path` could hand team B team
/// A's clone path. Embedding the team id keeps every team's workspace route and
/// clone path distinct while staying constant within one team replica (so the
/// first-writer-wins UNIQUE-key guard still holds there).
pub(crate) fn team_workspace_key(team_id: &str) -> String {
    format!("WORKSPACE-{team_id}")
}

/// Seed the team's workspace `projects` row into its replica, first-writer-wins.
///
/// The team-scoped twin of [`seed_workspace_project_db`]: a single
/// `is_workspace = 1` row per team, carrying a `{team}~{uuid}` id and the
/// team-scoped [`team_workspace_key`]. `INSERT OR IGNORE` on both the id and the
/// UNIQUE key makes it idempotent across repeated opens and concurrent members —
/// the first writer's row wins and every later attempt (a fresh minted id, same
/// reserved key) is ignored. Returns whether THIS call inserted the row (the
/// first-writer signal).
///
/// The row is seeded path-less: the machine-local clone path is per-machine and
/// recorded separately in the private `project_routes` catalog during
/// services-aware provisioning (like any team project, CAIRN-2223), so a member
/// who has not yet materialized the repo simply contributes no config layer.
pub(crate) async fn seed_team_workspace_project_db(
    team_db: &LocalDb,
    now: i64,
    team_id: &str,
    id: &str,
    key: &str,
    repo_path: &str,
) -> Result<bool, CairnError> {
    let affected = team_db
        .execute(
            "INSERT OR IGNORE INTO projects(
                id, team_id, name, key, repo_path, repository_id, context, docs_enabled,
                default_branch, next_issue_number, created_at, updated_at,
                hidden, is_workspace
             )
             VALUES (?1, ?2, 'Team Workspace', ?3, ?4, ?1, '', 1,
                     'main', 1, ?5, ?5, 0, 1)",
            (
                id.to_string(),
                team_id.to_string(),
                key.to_string(),
                repo_path.to_string(),
                now,
            ),
        )
        .await?;
    Ok(affected > 0)
}

/// The team's workspace `projects` row (its single `is_workspace = 1` row), if
/// seeded. Queried against a team replica; the config resolver uses it to find
/// the machine-local clone of the team's config home. Returns `None` before the
/// row is seeded, so callers degrade gracefully rather than erroring.
pub async fn team_workspace_project(team_db: &LocalDb) -> Result<Option<DbProject>, CairnError> {
    let sql = projects_select(team_db, "WHERE is_workspace = 1 LIMIT 1");
    team_db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(&sql, ()).await?;
                rows.next()
                    .await?
                    .map(|row| db_project_from_row(&row))
                    .transpose()
            })
        })
        .await
        .map_err(CairnError::from)
}

pub async fn unhide_workspace_project_db(db: &LocalDb) -> Result<(), CairnError> {
    db.write(|conn| {
        Box::pin(async move {
            conn.execute("UPDATE projects SET hidden = 0 WHERE is_workspace = 1", ())
                .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn update_timestamp_db(
    db: &LocalDb,
    clock: &dyn Clock,
    id: &str,
) -> Result<(), CairnError> {
    let now = clock.now() as i32;
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE projects SET updated_at = ?1 WHERE id = ?2",
                (now, id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn set_default_branch_db(db: &LocalDb, id: &str, branch: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    let branch = branch.to_string();
    db.write(|conn| {
        let id = id.clone();
        let branch = branch.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE projects SET default_branch = ?1 WHERE id = ?2",
                (branch.as_str(), id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

/// Re-detect and persist the default branch for every project with a reachable
/// local checkout, correcting rows left on the unverified `'main'` schema default
/// (or any other stale value — e.g. a repo whose real trunk is `staging`).
///
/// Detection only runs at project-create time, and no SQL migration can shell out
/// to git, so a project created before detection became authoritative — or one
/// whose origin/HEAD was unset when it was created — keeps a wrong stored value
/// forever. That wrong value is the shared root cause of merges dragging the
/// checkout onto `main` and skipping the squash, so this one-time startup
/// reconciliation is what repairs an already-broken project.
///
/// Best-effort and idempotent: a no-op when the stored value already matches the
/// detected one, and a per-project detection or persist failure is logged and
/// never aborts the sweep (the same discipline as the local default-advance
/// sweep).
pub(crate) async fn reconcile_default_branches(db: &LocalDb) {
    let projects = match list_db(db).await {
        Ok(projects) => projects,
        Err(e) => {
            log::warn!("default-branch backfill: failed to load projects: {e}");
            return;
        }
    };
    for project in projects {
        if project.repo_path.is_empty() {
            continue;
        }
        let repo_path = Path::new(&project.repo_path);
        if !repo_path.exists() {
            continue;
        }
        let stored = project.default_branch.unwrap_or_default();
        let Some((detected, source)) = detect_default_branch_with_source(repo_path) else {
            continue;
        };
        // Persist ONLY an authoritative detection. The checked-out-branch fallback
        // names whatever branch the working tree happens to be on at startup (a
        // feature or integration branch, or a stale checkout when the remote is
        // unreachable) — overwriting the stored default with that would corrupt it
        // the same way the unverified 'main' did, just in the other direction.
        if source != DefaultBranchSource::Authoritative {
            continue;
        }
        if detected == stored {
            continue;
        }
        match set_default_branch_db(db, &project.id, &detected).await {
            Ok(()) => log::info!(
                "default-branch backfill: corrected project {} default '{}' -> '{}'",
                project.id,
                stored,
                detected
            ),
            Err(e) => log::warn!(
                "default-branch backfill: failed to persist default for project {}: {e}",
                project.id
            ),
        }
    }
}

pub async fn set_name_db(db: &LocalDb, id: &str, name: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    let name = name.to_string();
    db.write(|conn| {
        let id = id.clone();
        let name = name.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE projects SET name = ?1 WHERE id = ?2",
                (name.as_str(), id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn set_hidden_db(db: &LocalDb, id: &str, hidden: bool) -> Result<(), CairnError> {
    let id = id.to_string();
    let hidden = if hidden { 1 } else { 0 };
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE projects SET hidden = ?1 WHERE id = ?2",
                (hidden, id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn delete_db(db: &LocalDb, id: &str) -> Result<(), CairnError> {
    if get_db(db, id).await?.is_none() {
        return Err(CairnError::NotFound {
            entity: "project",
            id: id.to_string(),
        });
    }

    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute("DELETE FROM projects WHERE id = ?1", (id,))
                .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn repo_path(db: &LocalDb, id: &str) -> Result<Option<String>, CairnError> {
    let id = id.to_string();
    db.query_text("SELECT repo_path FROM projects WHERE id = ?1", (id,))
        .await
        .map_err(CairnError::from)
}

fn db_project_from_row(row: &cairn_db::turso::Row) -> Result<DbProject, DbError> {
    Ok(DbProject {
        id: row.text(0)?,
        workspace_id: row.text(1)?,
        name: row.text(2)?,
        key: row.text(3)?,
        repo_path: row.text(4)?,
        context: row.opt_text(5)?,
        docs_enabled: row.opt_i64(6)?.map(|value| value as i32),
        default_branch: row.opt_text(7)?,
        next_issue_number: row.opt_i64(8)?.map(|value| value as i32),
        created_at: row.i64(9)? as i32,
        updated_at: row.i64(10)? as i32,
        ci_commands: row.opt_text(11)?,
        setup_commands: row.opt_text(12)?,
        terminal_commands: row.opt_text(13)?,
        config: row.opt_text(14)?,
        hidden: row.i64(15)? as i32,
        is_workspace: row.i64(16)? as i32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::Clock;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    struct FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> i64 {
            1234
        }
        fn now_u64(&self) -> u64 {
            1234
        }
    }

    async fn migrated_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join("cairn.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn create_initializes_the_coordinate_store_before_returning() {
        let Some(jj_binary_path) = crate::jj::tests::jj_bin() else {
            eprintln!("skipping create_initializes_the_coordinate_store_before_returning: jj not resolvable");
            return;
        };
        let db = migrated_db().await;
        let repo = tempdir().unwrap();
        let config_dir = tempdir().unwrap();
        let store_dir = crate::jj::project_store_dir(config_dir.path(), repo.path());
        assert!(!crate::jj::is_jj_dir(&store_dir));

        create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("store-ready".to_string()),
                name: "Store Ready".to_string(),
                key: "READY".to_string(),
                repo_path: repo.path().to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            Some(StoreInit {
                jj_binary_path: &jj_binary_path,
                config_dir: config_dir.path(),
            }),
        )
        .await
        .unwrap();

        assert!(crate::jj::is_jj_dir(&store_dir));
    }

    #[tokio::test]
    async fn create_stores_and_resolves_the_canonical_project_key() {
        let db = migrated_db().await;
        let project = create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("mixed-case".to_string()),
                name: "Mixed Case".to_string(),
                key: "LCLTW".to_string(),
                repo_path: String::new(),
                team_id: None,
            },
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(project.key, "lcltw");
        assert_eq!(
            crate::mcp::handlers::run_context::project_id_by_key(&db, "LCLTW")
                .await
                .unwrap(),
            project.id
        );
        assert_eq!(
            crate::mcp::handlers::run_context::project_id_by_key(&db, "lcltw")
                .await
                .unwrap(),
            project.id
        );
    }

    #[tokio::test]
    async fn create_explicitly_refuses_a_duplicate_canonical_project_key() {
        let db = migrated_db().await;
        for (id, key) in [("first", "abc"), ("second", "ABC")] {
            let result = create(
                &db,
                &FixedClock,
                CreateProject {
                    id: Some(id.to_string()),
                    name: id.to_string(),
                    key: key.to_string(),
                    repo_path: String::new(),
                    team_id: None,
                },
                None,
                None,
            )
            .await;
            if id == "first" {
                result.unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "a project with key 'abc' already exists"
                );
            }
        }
    }

    #[tokio::test]
    async fn create_existing_empty_git_repo_creates_initial_commit() {
        let db = migrated_db().await;
        let repo = tempdir().unwrap();
        run_git(&["init"], repo.path()).unwrap();

        create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("empty-repo".to_string()),
                name: "Empty Repo".to_string(),
                key: "ER".to_string(),
                repo_path: repo.path().to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let output = std::process::Command::new("git")
            .args(["rev-parse", "--verify", "HEAD"])
            .current_dir(repo.path())
            .output()
            .unwrap();
        assert!(output.status.success());

        let status = std::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(repo.path())
            .output()
            .unwrap();
        assert!(String::from_utf8_lossy(&status.stdout).trim().is_empty());
    }

    fn git_stdout(repo: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    #[tokio::test]
    async fn create_non_git_folder_commits_contents_and_config_on_main() {
        let db = migrated_db().await;
        let repo = tempdir().unwrap();
        std::fs::write(repo.path().join("notes.txt"), "ship me\n").unwrap();

        let project = create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("plain-folder".to_string()),
                name: "Plain Folder".to_string(),
                key: "PLAIN".to_string(),
                repo_path: repo.path().to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            None,
        )
        .await
        .unwrap();

        assert!(repo.path().join(".git").exists());
        assert_eq!(
            git_stdout(repo.path(), &["branch", "--show-current"]),
            "main"
        );
        let committed = git_stdout(repo.path(), &["ls-tree", "-r", "--name-only", "HEAD"]);
        assert!(committed.lines().any(|path| path == "notes.txt"));
        assert!(committed.lines().any(|path| path == ".cairn/config.yaml"));
        assert_eq!(project.default_branch.as_deref(), Some("main"));
        assert_eq!(
            get_db(&db, "plain-folder")
                .await
                .unwrap()
                .unwrap()
                .default_branch
                .as_deref(),
            Some("main")
        );
    }

    #[tokio::test]
    async fn create_healthy_repository_preserves_head_and_branch() {
        let db = migrated_db().await;
        let repo = tempdir().unwrap();
        run_git(&["init", "-q", "-b", "trunk"], repo.path()).unwrap();
        crate::config::project_settings::create_default_project_config(repo.path()).unwrap();
        std::fs::write(repo.path().join("file.txt"), "existing\n").unwrap();
        run_git(&["add", "-A"], repo.path()).unwrap();
        run_git(
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@local.invalid",
                "commit",
                "-q",
                "-m",
                "existing",
            ],
            repo.path(),
        )
        .unwrap();
        let head_before = git_stdout(repo.path(), &["rev-parse", "HEAD"]);

        let project = create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("healthy".to_string()),
                name: "Healthy".to_string(),
                key: "HEALTHY".to_string(),
                repo_path: repo.path().to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(git_stdout(repo.path(), &["rev-parse", "HEAD"]), head_before);
        assert_eq!(project.default_branch.as_deref(), Some("trunk"));
    }

    #[tokio::test]
    async fn create_nested_folder_initializes_repository_at_selected_root() {
        let db = migrated_db().await;
        let parent = tempdir().unwrap();
        run_git(&["init", "-q", "-b", "parent"], parent.path()).unwrap();
        let nested = parent.path().join("selected");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("nested.txt"), "nested\n").unwrap();

        create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("nested".to_string()),
                name: "Nested".to_string(),
                key: "NESTED".to_string(),
                repo_path: nested.to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            None,
        )
        .await
        .unwrap();

        assert!(nested.join(".git").exists());
        assert_eq!(git_stdout(&nested, &["branch", "--show-current"]), "main");
        assert!(
            git_stdout(&nested, &["ls-tree", "-r", "--name-only", "HEAD"])
                .lines()
                .any(|path| path == "nested.txt")
        );
    }

    #[tokio::test]
    async fn create_rejects_a_nonexistent_explicit_repository_path() {
        let db = migrated_db().await;
        let root = tempdir().unwrap();
        let missing = root.path().join("missing");

        let error = create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("missing-repo".to_string()),
                name: "Missing Repo".to_string(),
                key: "MISSING".to_string(),
                repo_path: missing.to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            None,
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("project repository path does not exist"));
        assert!(get_db(&db, "missing-repo").await.unwrap().is_none());
        assert!(!missing.exists());
    }

    #[tokio::test]
    async fn create_repository_failure_does_not_insert_project_row() {
        let db = migrated_db().await;
        let root = tempdir().unwrap();
        let not_a_directory = root.path().join("file");
        std::fs::write(&not_a_directory, "not a directory").unwrap();

        let result = create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("broken".to_string()),
                name: "Broken".to_string(),
                key: "BROKEN".to_string(),
                repo_path: not_a_directory.to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(get_db(&db, "broken").await.unwrap().is_none());
    }

    async fn insert_duplicate_project(db: &LocalDb, id: &str) {
        create_db(
            db,
            &FixedClock,
            &CreateProject {
                id: Some(id.to_string()),
                name: "Existing".to_string(),
                key: format!("{id}-existing"),
                repo_path: String::new(),
                team_id: None,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rejected_create_does_not_mutate_plain_folder() {
        let db = migrated_db().await;
        insert_duplicate_project(&db, "duplicate-plain").await;
        let folder = tempdir().unwrap();
        std::fs::write(folder.path().join("notes.txt"), "unchanged\n").unwrap();

        let result = create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("duplicate-plain".to_string()),
                name: "Duplicate".to_string(),
                key: "DUPLICATE".to_string(),
                repo_path: folder.path().to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(!folder.path().join(".git").exists());
        assert!(!folder.path().join(".cairn").exists());
        assert_eq!(
            std::fs::read_to_string(folder.path().join("notes.txt")).unwrap(),
            "unchanged\n"
        );
    }

    #[tokio::test]
    async fn rejected_create_does_not_mutate_existing_repository() {
        let db = migrated_db().await;
        insert_duplicate_project(&db, "duplicate-repo").await;
        let repo = tempdir().unwrap();
        run_git(&["init", "-q", "-b", "trunk"], repo.path()).unwrap();
        std::fs::write(repo.path().join("tracked.txt"), "unchanged\n").unwrap();
        run_git(&["add", "-A"], repo.path()).unwrap();
        run_git(
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@local.invalid",
                "commit",
                "-q",
                "-m",
                "existing",
            ],
            repo.path(),
        )
        .unwrap();
        let head_before = git_stdout(repo.path(), &["rev-parse", "HEAD"]);

        let result = create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("duplicate-repo".to_string()),
                name: "Duplicate".to_string(),
                key: "DUPREPO".to_string(),
                repo_path: repo.path().to_string_lossy().to_string(),
                team_id: None,
            },
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(git_stdout(repo.path(), &["rev-parse", "HEAD"]), head_before);
        assert!(git_stdout(repo.path(), &["status", "--porcelain"]).is_empty());
        assert!(!repo.path().join(".cairn").exists());
    }

    #[tokio::test]
    async fn reconcile_project_repositories_repairs_existing_plain_folder() {
        let db = migrated_db().await;
        let repo = tempdir().unwrap();
        std::fs::write(repo.path().join("legacy.txt"), "legacy\n").unwrap();
        create_db(
            &db,
            &FixedClock,
            &CreateProject {
                id: Some("legacy".to_string()),
                name: "Legacy".to_string(),
                key: "LEGACY".to_string(),
                repo_path: repo.path().to_string_lossy().to_string(),
                team_id: None,
            },
        )
        .await
        .unwrap();

        reconcile_project_repositories(&db).await;
        let head = git_stdout(repo.path(), &["rev-parse", "HEAD"]);
        reconcile_project_repositories(&db).await;

        assert!(repo.path().join(".git").exists());
        assert_eq!(git_stdout(repo.path(), &["rev-parse", "HEAD"]), head);
        assert!(
            git_stdout(repo.path(), &["ls-tree", "-r", "--name-only", "HEAD"])
                .lines()
                .any(|path| path == "legacy.txt")
        );
    }

    #[tokio::test]
    async fn reconcile_project_repositories_skips_workspace_project() {
        let db = migrated_db().await;
        let workspace = tempdir().unwrap();
        seed_workspace_project_db(&db, &FixedClock, workspace.path())
            .await
            .unwrap();

        reconcile_project_repositories(&db).await;

        assert!(!workspace.path().join(".git").exists());
    }

    #[test]
    fn project_checkout_validation_rejects_disposable_and_missing_paths() {
        let home = std::env::var_os("HOME").expect("tests require a home directory");
        let root = tempfile::tempdir_in(home).unwrap();
        let config = root.path().join(".cairn");
        let scratch = config.join("scratch/job/repo");
        let build_slot = root.path().join("build-slots/slot/repo");
        std::fs::create_dir_all(&scratch).unwrap();
        std::fs::create_dir_all(&build_slot).unwrap();

        assert!(validate_project_checkout(&scratch, &config).is_err());
        assert!(validate_project_checkout(&build_slot, &config).is_err());
        assert!(validate_project_checkout(&root.path().join("missing"), &config).is_err());
        assert!(validate_project_checkout(root.path(), &config).is_ok());
    }

    #[tokio::test]
    async fn resolve_local_repo_path_uses_synced_path_for_local_projects() {
        let local = Arc::new(migrated_db().await);
        let index = Arc::new(
            crate::storage::SearchIndex::open_or_create(tempdir().unwrap().keep()).unwrap(),
        );
        let dbs = crate::db::DbState::new(local, index);

        let path = resolve_local_repo_path(&dbs, "local-project", "prj", "/repo/local")
            .await
            .unwrap();

        assert_eq!(path.as_deref(), Some("/repo/local"));
    }

    #[tokio::test]
    async fn resolve_local_repo_path_uses_private_route_for_team_projects() {
        let local = Arc::new(migrated_db().await);
        let index = Arc::new(
            crate::storage::SearchIndex::open_or_create(tempdir().unwrap().keep()).unwrap(),
        );
        let dbs = crate::db::DbState::new(local.clone(), index);
        local
            .execute(
                "INSERT INTO teams(id, name, sync_url, replica_path, created_at) VALUES ('teamABC123', 'Team', 'http://sync', '/tmp/team.db', 1)",
                (),
            )
            .await
            .unwrap();
        insert_project_route(&local, &FixedClock, "prj", Some("teamABC123"))
            .await
            .unwrap();

        let missing = resolve_local_repo_path(
            &dbs,
            "teamABC123~00000000-0000-4000-8000-000000000001",
            "prj",
            "/creator/repo",
        )
        .await
        .unwrap();
        assert_eq!(missing, None);

        set_local_repo_path(&local, "prj", Path::new("/member/repo"))
            .await
            .unwrap();
        let resolved = resolve_local_repo_path(
            &dbs,
            "teamABC123~00000000-0000-4000-8000-000000000001",
            "prj",
            "/creator/repo",
        )
        .await
        .unwrap();

        assert_eq!(resolved.as_deref(), Some("/member/repo"));
    }

    #[tokio::test]
    async fn seed_workspace_project_is_visible_and_idempotent() {
        let db = migrated_db().await;
        seed_workspace_project_db(&db, &FixedClock, Path::new("/tmp/cairn-home"))
            .await
            .unwrap();
        seed_workspace_project_db(&db, &FixedClock, Path::new("/tmp/other"))
            .await
            .unwrap();

        let (count, repo_path, hidden, is_workspace, default_branch) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*), repo_path, hidden, is_workspace, default_branch FROM projects WHERE id = 'workspace'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.expect("workspace row");
                    Ok((row.i64(0)?, row.text(1)?, row.i64(2)?, row.i64(3)?, row.text(4)?))
                })
            })
            .await
            .unwrap();

        assert_eq!(count, 1);
        assert_eq!(repo_path, "/tmp/cairn-home");
        assert_eq!(hidden, 0);
        assert_eq!(is_workspace, 1);
        assert_eq!(default_branch, "main");
    }

    #[tokio::test]
    async fn seed_team_workspace_project_is_first_writer_wins() {
        use crate::storage::TEAM_MIGRATIONS;
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("team.db")).await.unwrap();
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        // The team-root FK parent that projects.team_id references.
        db.execute(
            "INSERT INTO teams(id, name, created_at, updated_at) VALUES ('team1', 'Team One', 1, 1)",
            (),
        )
        .await
        .unwrap();

        let key = team_workspace_key("team1");
        let first = seed_team_workspace_project_db(
            &db,
            1,
            "team1",
            "team1~00000000-0000-4000-8000-0000000000ff",
            &key,
            "",
        )
        .await
        .unwrap();
        assert!(first, "the first writer seeds the workspace row");

        // A concurrent member mints a DIFFERENT id; the reserved key collides, so
        // INSERT OR IGNORE drops it (first-writer-wins).
        let second = seed_team_workspace_project_db(
            &db,
            2,
            "team1",
            "team1~00000000-0000-4000-8000-0000000000aa",
            &key,
            "",
        )
        .await
        .unwrap();
        assert!(!second, "a later writer is a no-op, not a clobber");
        assert_eq!(key, "WORKSPACE-team1");

        let (count, id, is_ws) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*), MAX(id), MAX(is_workspace) FROM projects WHERE is_workspace = 1",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.expect("workspace row");
                    Ok((row.i64(0)?, row.text(1)?, row.i64(2)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(count, 1, "exactly one workspace row survives");
        assert_eq!(id, "team1~00000000-0000-4000-8000-0000000000ff");
        assert_eq!(is_ws, 1);
    }

    #[tokio::test]
    async fn unhide_workspace_project_backfills_existing_hidden_rows() {
        let db = migrated_db().await;
        seed_workspace_project_db(&db, &FixedClock, Path::new("/tmp/cairn-home"))
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("UPDATE projects SET hidden = 1 WHERE id = 'workspace'", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        unhide_workspace_project_db(&db).await.unwrap();
        unhide_workspace_project_db(&db).await.unwrap();

        let hidden = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT hidden FROM projects WHERE id = 'workspace'", ())
                        .await?;
                    let row = rows.next().await?.expect("workspace row");
                    row.i64(0)
                })
            })
            .await
            .unwrap();

        assert_eq!(hidden, 0);
    }

    #[test]
    fn parse_ls_remote_symref_extracts_branch_and_ignores_sha_line() {
        let stdout = "ref: refs/heads/staging\tHEAD\nabc123def456\tHEAD\n";
        assert_eq!(parse_ls_remote_symref(stdout).as_deref(), Some("staging"));

        // A detached remote HEAD (no symref line) yields nothing.
        let detached = "abc123def456\tHEAD\n";
        assert_eq!(parse_ls_remote_symref(detached), None);

        // A branch name with slashes survives.
        let slashed = "ref: refs/heads/release/2.0\tHEAD\n";
        assert_eq!(
            parse_ls_remote_symref(slashed).as_deref(),
            Some("release/2.0")
        );
    }

    /// Build a "remote" repo whose trunk is `branch`, plus a local repo with an
    /// `origin` pointing at it but `refs/remotes/origin/HEAD` deliberately unset.
    fn remote_and_local_repos(branch: &str) -> (tempfile::TempDir, tempfile::TempDir) {
        let remote = tempdir().unwrap();
        run_git(&["init", "-q", "-b", branch], remote.path()).unwrap();
        run_git(&["config", "user.email", "t@e.com"], remote.path()).unwrap();
        run_git(&["config", "user.name", "T"], remote.path()).unwrap();
        std::fs::write(remote.path().join("f.txt"), "x\n").unwrap();
        run_git(&["add", "-A"], remote.path()).unwrap();
        run_git(&["commit", "-q", "-m", "init"], remote.path()).unwrap();

        let local = tempdir().unwrap();
        run_git(&["init", "-q", "-b", "main"], local.path()).unwrap();
        let remote_url = remote.path().to_string_lossy().to_string();
        run_git(
            &["remote", "add", "origin", remote_url.as_str()],
            local.path(),
        )
        .unwrap();
        // Deliberately do NOT set refs/remotes/origin/HEAD: ls-remote --symref
        // must still resolve the remote's real default.
        (remote, local)
    }

    #[test]
    fn detect_default_branch_prefers_remote_head_when_origin_head_unset() {
        let (_remote, local) = remote_and_local_repos("staging");
        assert_eq!(
            detect_default_branch(local.path()).as_deref(),
            Some("staging")
        );
    }

    #[tokio::test]
    async fn reconcile_default_branches_corrects_stale_main_and_is_idempotent() {
        let db = migrated_db().await;
        let (_remote, local) = remote_and_local_repos("staging");
        let local_path = local.path().to_string_lossy().to_string();

        // `create_db` hardcodes the schema default 'main' (no detection), so this
        // reproduces a project left on the wrong stored default.
        let project = create_db(
            &db,
            &FixedClock,
            &CreateProject {
                id: Some("p".to_string()),
                name: "p".to_string(),
                key: "p".to_string(),
                repo_path: local_path,
                team_id: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(project.default_branch.as_deref(), Some("main"));

        reconcile_default_branches(&db).await;
        let corrected = get_db(&db, "p").await.unwrap().unwrap();
        assert_eq!(corrected.default_branch.as_deref(), Some("staging"));

        // Idempotent: a second run leaves the corrected value in place.
        reconcile_default_branches(&db).await;
        let again = get_db(&db, "p").await.unwrap().unwrap();
        assert_eq!(again.default_branch.as_deref(), Some("staging"));
    }

    /// The backfill must NOT persist the checked-out-branch fallback: when the
    /// remote default is unreachable/unset and the user's checkout is on a feature
    /// branch, the stored default stays put rather than being overwritten with the
    /// transient branch. (The detection's last-resort fallback is authoritative
    /// only at create time, not for startup backfill.)
    #[tokio::test]
    async fn reconcile_default_branches_ignores_checked_out_branch_fallback() {
        let db = migrated_db().await;
        // A local repo on `feature` with NO origin remote: ls-remote and
        // origin/HEAD both fail, so detection falls back to the checked-out
        // branch.
        let repo = tempdir().unwrap();
        run_git(&["init", "-q", "-b", "feature"], repo.path()).unwrap();
        run_git(&["config", "user.email", "t@e.com"], repo.path()).unwrap();
        run_git(&["config", "user.name", "T"], repo.path()).unwrap();
        std::fs::write(repo.path().join("f.txt"), "x\n").unwrap();
        run_git(&["add", "-A"], repo.path()).unwrap();
        run_git(&["commit", "-q", "-m", "init"], repo.path()).unwrap();
        let repo_path = repo.path().to_string_lossy().to_string();

        // Sanity: detection returns `feature`, tagged as the non-authoritative
        // checked-out fallback.
        assert_eq!(
            detect_default_branch_with_source(repo.path()),
            Some(("feature".to_string(), DefaultBranchSource::CheckedOut))
        );

        create_db(
            &db,
            &FixedClock,
            &CreateProject {
                id: Some("p2".to_string()),
                name: "P2".to_string(),
                key: "P2".to_string(),
                repo_path,
                team_id: None,
            },
        )
        .await
        .unwrap();

        reconcile_default_branches(&db).await;
        let after = get_db(&db, "p2").await.unwrap().unwrap();
        assert_eq!(
            after.default_branch.as_deref(),
            Some("main"),
            "the transient checkout branch must not be persisted as the default"
        );
    }
}
