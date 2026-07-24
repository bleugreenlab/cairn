use super::*;

/// Database-authoritative context for one callback operating in a managed jj
/// workspace. `identity` is stable for the physical workspace; `current_job_id`
/// changes as parent and child jobs intentionally share that workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedWorkspaceContext {
    pub current_job_id: String,
    pub identity: crate::jj::WorkspaceIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedWorkspaceGitTarget {
    pub repository: PathBuf,
    pub git_common_dir: PathBuf,
    pub store_dir: PathBuf,
    pub workspace: PathBuf,
}

fn canonical_path(path: &Path, context: &str) -> Result<PathBuf, String> {
    std::fs::canonicalize(path).map_err(|error| format!("{context} {}: {error}", path.display()))
}

fn resolve_git_common_dir(repository: &Path, purpose: &str) -> Result<PathBuf, String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--git-common-dir"])
        .current_dir(repository)
        .output()
        .map_err(|error| format!("resolve {purpose} repository Git common directory: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "resolve {purpose} repository Git common directory: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let common = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    canonical_path(
        &if common.is_absolute() {
            common
        } else {
            repository.join(common)
        },
        &format!("canonicalize {purpose} repository Git common directory"),
    )
}

/// Prove that a managed workspace, its configured shared store, and the Git
/// repository used for materialization all name one production topology.
/// Callers provide the store path they are actually acting under (the held lock
/// for publication, the canonical project store for check dispatch), so neither
/// path can accidentally address a sibling jj operation store or Git checkout.
pub(crate) fn resolve_managed_workspace_git_target(
    config_dir: &Path,
    context: &ManagedWorkspaceContext,
    store_path: &Path,
    purpose: &str,
) -> Result<ManagedWorkspaceGitTarget, String> {
    let repository = canonical_path(
        &context.identity.project_root,
        &format!("canonicalize {purpose} managed project repository"),
    )?;
    let workspace = canonical_path(
        &context.identity.worktree_path,
        &format!("canonicalize {purpose} managed workspace"),
    )?;
    let store_dir = canonical_path(
        store_path,
        &format!("canonicalize {purpose} managed shared store"),
    )?;
    let expected_store = canonical_path(
        &crate::jj::project_store_dir(config_dir, &context.identity.project_root),
        &format!("canonicalize {purpose} expected managed shared store"),
    )?;
    if store_dir != expected_store {
        return Err(format!(
            "{purpose} store mismatch: selected shared store resolves to {}, managed project store resolves to {}",
            store_dir.display(),
            expected_store.display()
        ));
    }

    let workspace_repo_pointer = workspace.join(".jj").join("repo");
    let workspace_repo = std::fs::read_to_string(&workspace_repo_pointer).map_err(|error| {
        format!(
            "read {purpose} managed workspace repository pointer {}: {error}",
            workspace_repo_pointer.display()
        )
    })?;
    let workspace_repo = PathBuf::from(workspace_repo.trim());
    let workspace_repo = canonical_path(
        &if workspace_repo.is_absolute() {
            workspace_repo
        } else {
            workspace_repo_pointer
                .parent()
                .unwrap_or(&workspace)
                .join(workspace_repo)
        },
        &format!("resolve {purpose} managed workspace .jj/repo"),
    )?;
    let store_repo = canonical_path(
        &store_dir.join(".jj").join("repo"),
        &format!("resolve {purpose} managed shared-store .jj/repo"),
    )?;
    if workspace_repo != store_repo {
        return Err(format!(
            "{purpose} store mismatch: workspace .jj/repo resolves to {}, selected shared store resolves to {}",
            workspace_repo.display(),
            store_repo.display()
        ));
    }

    let git_common_dir = resolve_git_common_dir(&repository, purpose)?;
    let git_target_file = store_repo.join("store").join("git_target");
    let git_target = std::fs::read_to_string(&git_target_file).map_err(|error| {
        format!(
            "read {purpose} shared-store Git target {}: {error}",
            git_target_file.display()
        )
    })?;
    let git_target = PathBuf::from(git_target.trim());
    let git_target = canonical_path(
        &if git_target.is_absolute() {
            git_target
        } else {
            git_target_file
                .parent()
                .unwrap_or(&store_repo)
                .join(git_target)
        },
        &format!("canonicalize {purpose} shared-store Git target"),
    )?;
    if git_target != git_common_dir {
        return Err(format!(
            "{purpose} Git backend mismatch: shared store targets {}, managed project repository common directory is {}",
            git_target.display(),
            git_common_dir.display()
        ));
    }

    Ok(ManagedWorkspaceGitTarget {
        repository,
        git_common_dir,
        store_dir,
        workspace,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchOwnershipEvidence {
    pub prior_same_lineage: bool,
    pub conflicting_owner: Option<String>,
}

/// Inspect persisted branch associations without treating the active lineage
/// root's own assignment as proof that a pre-existing physical bookmark belongs
/// to it. A distinct job in the same inherited workspace lineage is positive
/// evidence; any other association is a conflict.
pub(crate) async fn branch_ownership_evidence(
    db: Arc<LocalDb>,
    project_id: String,
    branch: String,
    lineage_root_job_id: String,
) -> Result<BranchOwnershipEvidence, String> {
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        let lineage_root_job_id = lineage_root_job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM jobs
                     WHERE project_id = ?1 AND branch = ?2
                     ORDER BY created_at ASC, id ASC",
                    (project_id.as_str(), branch.as_str()),
                )
                .await?;
            let mut prior_same_lineage = false;
            while let Some(row) = rows.next().await? {
                let job_id = row.text(0)?;
                if job_id == lineage_root_job_id {
                    continue;
                }
                match resolve_managed_workspace_context_conn(conn, &job_id).await? {
                    Some(context)
                        if context.identity.lineage_root_job_id == lineage_root_job_id =>
                    {
                        prior_same_lineage = true;
                    }
                    _ => {
                        return Ok(BranchOwnershipEvidence {
                            prior_same_lineage,
                            conflicting_owner: Some(job_id),
                        });
                    }
                }
            }
            Ok(BranchOwnershipEvidence {
                prior_same_lineage,
                conflicting_owner: None,
            })
        })
    })
    .await
    .map_err(|e| db_error("Failed to inspect bookmark lineage evidence", e))
}

async fn project_root_conn(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
) -> DbResult<PathBuf> {
    let mut rows = conn
        .query(
            "SELECT repo_path FROM projects WHERE id = ?1",
            (project_id,),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| db_internal(format!("project not found: {project_id}")))?;
    Ok(PathBuf::from(row.text(0)?))
}

/// Resolve the stable owner by walking parent links only while the parent shares
/// the exact physical worktree. This is the durable lineage relation used by
/// inherited tasks/calls/workflows; commit ancestry is deliberately irrelevant.
async fn resolve_managed_workspace_context_conn(
    conn: &cairn_db::turso::Connection,
    current_job_id: &str,
) -> DbResult<Option<ManagedWorkspaceContext>> {
    let Some(current) = load_job_conn(conn, current_job_id).await? else {
        return Ok(None);
    };
    let Some(worktree_path) = current.worktree_path.clone() else {
        return Ok(None);
    };

    let mut owner = current.clone();
    let mut cursor = current;
    while let Some(parent_id) = cursor.parent_job_id.as_deref() {
        let Some(parent) = load_job_conn(conn, parent_id).await? else {
            break;
        };
        if parent.worktree_path.as_deref() != Some(worktree_path.as_str()) {
            break;
        }
        owner = parent.clone();
        cursor = parent;
    }

    let branch = owner.branch.clone().ok_or_else(|| {
        db_internal(format!(
            "managed workspace owner {} has no branch assignment",
            owner.id
        ))
    })?;
    let base_commit = owner.base_commit.clone().ok_or_else(|| {
        db_internal(format!(
            "managed workspace owner {} has no recorded base commit",
            owner.id
        ))
    })?;
    let project_root = project_root_conn(conn, &owner.project_id).await?;
    let workspace_name = crate::jj::read_workspace_identity(Path::new(&worktree_path))
        .filter(|marker| marker.lineage_root_job_id == owner.id)
        .map(|marker| marker.workspace_name)
        .unwrap_or_else(|| crate::jj::workspace_name_for_branch(&branch));

    Ok(Some(ManagedWorkspaceContext {
        current_job_id: current_job_id.to_string(),
        identity: crate::jj::WorkspaceIdentity::new(
            owner.id.clone(),
            owner.id,
            owner.project_id,
            project_root,
            PathBuf::from(worktree_path),
            branch,
            workspace_name,
            base_commit,
        ),
    }))
}

pub(crate) async fn resolve_managed_workspace_context(
    db: Arc<LocalDb>,
    current_job_id: String,
) -> Result<Option<ManagedWorkspaceContext>, String> {
    db.read(|conn| {
        let current_job_id = current_job_id.clone();
        Box::pin(async move { resolve_managed_workspace_context_conn(conn, &current_job_id).await })
    })
    .await
    .map_err(|e| db_error("Failed to resolve managed workspace identity", e))
}

pub(crate) async fn coordinate_owner(
    db: Arc<LocalDb>,
    project_id: String,
    branch: String,
    worktree_path: String,
) -> Result<Option<String>, String> {
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        let worktree_path = worktree_path.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM jobs
                     WHERE project_id = ?1
                       AND (branch = ?2 OR worktree_path = ?3)
                     ORDER BY created_at ASC, id ASC
                     LIMIT 1",
                    (project_id.as_str(), branch.as_str(), worktree_path.as_str()),
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .map_err(|e| db_error("Failed to inspect workspace coordinate ownership", e))
}

/// Persist an intended assignment before provisioning. This is a compare-and-set:
/// retries may repeat the exact assignment, but another actor cannot silently
/// replace coordinates after ownership inspection.
pub(crate) async fn assign_workspace_if_unset_or_same(
    db: Arc<LocalDb>,
    job_id: String,
    expected_path: Option<String>,
    expected_branch: Option<String>,
    path: String,
    branch: String,
    now: i32,
) -> Result<(), String> {
    crate::managed_worktrees::validate_path(std::path::Path::new(&path))?;
    db.write(|conn| {
        let job_id = job_id.clone();
        let expected_path = expected_path.clone();
        let expected_branch = expected_branch.clone();
        let path = path.clone();
        let branch = branch.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE jobs
                     SET worktree_path = ?1, branch = ?2, updated_at = ?3
                     WHERE id = ?4
                       AND worktree_path IS ?5
                       AND branch IS ?6",
                    cairn_db::turso::params![
                        path.as_str(),
                        branch.as_str(),
                        now,
                        job_id.as_str(),
                        expected_path.as_deref(),
                        expected_branch.as_deref()
                    ],
                )
                .await?;
            if changed != 1 {
                return Err(db_internal(format!(
                    "workspace assignment changed concurrently for job {job_id}"
                )));
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to assign managed workspace", e))
}

/// Restore the coordinates that preceded a failed provisioning attempt. The
/// compare-and-swap prevents cleanup from erasing a concurrent reassignment.
pub(crate) async fn restore_workspace_assignment(
    db: Arc<LocalDb>,
    job_id: String,
    current_path: String,
    current_branch: String,
    restore_path: Option<String>,
    restore_branch: Option<String>,
    now: i32,
) -> Result<(), String> {
    db.write(|conn| {
        let job_id = job_id.clone();
        let current_path = current_path.clone();
        let current_branch = current_branch.clone();
        let restore_path = restore_path.clone();
        let restore_branch = restore_branch.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE jobs
                     SET worktree_path = ?1, branch = ?2, updated_at = ?3
                     WHERE id = ?4 AND worktree_path = ?5 AND branch = ?6",
                    cairn_db::turso::params![
                        restore_path.as_deref(),
                        restore_branch.as_deref(),
                        now,
                        job_id.as_str(),
                        current_path.as_str(),
                        current_branch.as_str()
                    ],
                )
                .await?;
            if changed != 1 {
                return Err(db_internal(format!(
                    "workspace assignment changed concurrently while restoring job {job_id}"
                )));
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to restore managed workspace assignment", e))
}

/// Compare-and-swap a rewritten base commit on the stable owner, then update
/// every non-terminal job intentionally sharing the exact physical workspace and
/// old base coordinate. The pack anchor remains historical; only the live lineage
/// proof coordinate follows jj's stable change through its rewritten commit id.
pub(crate) async fn compare_and_swap_owner_base(
    db: &LocalDb,
    owner_job_id: String,
    worktree_path: String,
    old_base: String,
    new_base: String,
    now: i32,
) -> Result<(), String> {
    db.write(|conn| {
        let owner_job_id = owner_job_id.clone();
        let worktree_path = worktree_path.clone();
        let old_base = old_base.clone();
        let new_base = new_base.clone();
        Box::pin(async move {
            let owner = load_job_conn(conn, &owner_job_id)
                .await?
                .ok_or_else(|| db_internal(format!("job not found: {owner_job_id}")))?;
            let changed = conn
                .execute(
                    "UPDATE jobs SET base_commit = ?1, updated_at = ?2
                     WHERE id = ?3 AND worktree_path = ?4 AND base_commit = ?5",
                    (
                        new_base.as_str(),
                        now,
                        owner_job_id.as_str(),
                        worktree_path.as_str(),
                        old_base.as_str(),
                    ),
                )
                .await?;
            if changed == 0 {
                let current = load_job_conn(conn, &owner_job_id)
                    .await?
                    .and_then(|job| job.base_commit);
                if current.as_deref() != Some(new_base.as_str()) {
                    return Err(db_internal(format!(
                        "workspace base assignment changed concurrently for owner {owner_job_id}; expected {old_base} or completed {new_base}, found {}",
                        current.as_deref().unwrap_or("missing")
                    )));
                }
            }
            conn.execute(
                "UPDATE jobs SET base_commit = ?1, updated_at = ?2
                 WHERE project_id = ?3 AND worktree_path = ?4 AND base_commit = ?5
                   AND status IN ('pending', 'running', 'blocked', 'idle')",
                (
                    new_base.as_str(),
                    now,
                    owner.project_id.as_str(),
                    worktree_path.as_str(),
                    old_base.as_str(),
                ),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to refresh managed workspace base", e))
}

/// Execute one forward durable-base transition using the physical marker as the
/// write-ahead record. Retrying after any marker/database boundary is idempotent.
pub(crate) async fn apply_base_transition(
    db: &LocalDb,
    worktree: &Path,
    marker: &mut crate::jj::WorkspaceIdentity,
    old_base: &str,
    new_base: &str,
) -> Result<(), String> {
    if old_base == new_base {
        marker.base_commit = new_base.to_string();
        marker.pending_base_transition = None;
        return crate::jj::write_workspace_identity(worktree, marker);
    }

    match marker.pending_base_transition.as_ref() {
        Some(pending) if pending.old_base == old_base && pending.new_base == new_base => {}
        Some(pending) => {
            return Err(format!(
                "workspace already has a different pending base transition {} -> {}; refused {old_base} -> {new_base}",
                pending.old_base, pending.new_base
            ));
        }
        None => {
            marker.pending_base_transition = Some(crate::jj::WorkspaceBaseTransition {
                old_base: old_base.to_string(),
                new_base: new_base.to_string(),
            });
            crate::jj::write_workspace_identity(worktree, marker).map_err(|error| {
                format!(
                    "could not record pending base transition {old_base} -> {new_base} at {} before database update: {error}",
                    worktree.display()
                )
            })?;
        }
    }

    compare_and_swap_owner_base(
        db,
        marker.owner_job_id.clone(),
        worktree.to_string_lossy().to_string(),
        old_base.to_string(),
        new_base.to_string(),
        chrono::Utc::now().timestamp() as i32,
    )
    .await
    .map_err(|error| {
        format!(
            "pending base transition {old_base} -> {new_base} remains recorded for retry after database update failed: {error}"
        )
    })?;

    marker.base_commit = new_base.to_string();
    marker.pending_base_transition = None;
    crate::jj::write_workspace_identity(worktree, marker).map_err(|error| {
        format!(
            "database base reached {new_base}; pending marker transition {old_base} -> {new_base} remains recoverable at {} because finalization failed: {error}",
            worktree.display()
        )
    })
}

/// Compare-and-swap the stable owner, then update every non-terminal job that
/// intentionally shares the exact project/path/branch coordinates. Terminal
/// historical rows remain immutable evidence of prior ownership.
pub(crate) async fn compare_and_swap_owner_branch(
    db: Arc<LocalDb>,
    owner_job_id: String,
    worktree_path: String,
    old_branch: String,
    new_branch: String,
    now: i32,
) -> Result<(), String> {
    db.write(|conn| {
        let owner_job_id = owner_job_id.clone();
        let worktree_path = worktree_path.clone();
        let old_branch = old_branch.clone();
        let new_branch = new_branch.clone();
        Box::pin(async move {
            let owner = load_job_conn(conn, &owner_job_id)
                .await?
                .ok_or_else(|| db_internal(format!("job not found: {owner_job_id}")))?;
            let changed = conn
                .execute(
                    "UPDATE jobs SET branch = ?1, updated_at = ?2
                     WHERE id = ?3 AND worktree_path = ?4 AND branch = ?5",
                    (
                        new_branch.as_str(),
                        now,
                        owner_job_id.as_str(),
                        worktree_path.as_str(),
                        old_branch.as_str(),
                    ),
                )
                .await?;
            if changed != 1 {
                return Err(db_internal(format!(
                    "workspace branch assignment changed concurrently for owner {owner_job_id}"
                )));
            }
            conn.execute(
                "UPDATE jobs SET branch = ?1, updated_at = ?2
                 WHERE project_id = ?3 AND worktree_path = ?4 AND branch = ?5
                   AND status IN ('pending', 'running', 'blocked', 'idle')",
                (
                    new_branch.as_str(),
                    now,
                    owner.project_id.as_str(),
                    worktree_path.as_str(),
                    old_branch.as_str(),
                ),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to rebind managed workspace branch", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    #[tokio::test]
    async fn inherited_job_resolves_stable_workspace_owner() {
        let db = Arc::new(migrated_test_db("workspace-lineage.db").await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'default', 'Project', 'PRJ', '/repo', 1, 1);
             INSERT INTO jobs (id, project_id, status, worktree_path, branch, base_commit, created_at, updated_at)
             VALUES ('owner', 'project', 'running', '/worktree', 'agent/owner', 'base', 1, 1);
             INSERT INTO jobs (id, parent_job_id, project_id, status, worktree_path, branch, base_commit, created_at, updated_at)
             VALUES ('child', 'owner', 'project', 'running', '/worktree', NULL, 'later', 2, 2);",
        )
        .await
        .unwrap();

        let context = resolve_managed_workspace_context(db, "child".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.current_job_id, "child");
        assert_eq!(context.identity.lineage_root_job_id, "owner");
        assert_eq!(context.identity.owner_job_id, "owner");
        assert_eq!(context.identity.branch, "agent/owner");
        assert_eq!(context.identity.base_commit, "base");
    }

    #[tokio::test]
    async fn partial_workspace_assignment_without_branch_fails_closed() {
        let db = Arc::new(migrated_test_db("workspace-missing-branch.db").await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'default', 'Project', 'PRJ', '/repo', 1, 1);
             INSERT INTO jobs (id, project_id, status, worktree_path, base_commit, created_at, updated_at)
             VALUES ('owner', 'project', 'running', '/worktree', 'base', 1, 1);",
        )
        .await
        .unwrap();

        let error = resolve_managed_workspace_context(db, "owner".to_string())
            .await
            .unwrap_err();
        assert!(
            error.contains("managed workspace owner owner has no branch assignment"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn owner_base_refresh_is_compare_and_swap() {
        let db = Arc::new(migrated_test_db("workspace-base-cas.db").await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'default', 'Project', 'PRJ', '/repo', 1, 1);
             INSERT INTO jobs (id, project_id, status, worktree_path, branch, base_commit, pack_anchor, created_at, updated_at)
             VALUES ('owner', 'project', 'running', '/worktree', 'agent/branch', 'old-base', 'archive-anchor', 1, 1);
             INSERT INTO jobs (id, parent_job_id, project_id, status, worktree_path, branch, base_commit, pack_anchor, created_at, updated_at)
             VALUES ('child', 'owner', 'project', 'blocked', '/worktree', 'agent/branch', 'old-base', 'child-anchor', 2, 2);",
        )
        .await
        .unwrap();
        compare_and_swap_owner_base(
            db.as_ref(),
            "owner".into(),
            "/worktree".into(),
            "old-base".into(),
            "new-base".into(),
            3,
        )
        .await
        .unwrap();
        assert_eq!(
            db.query_text("SELECT base_commit FROM jobs WHERE id = 'owner'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("new-base")
        );
        assert_eq!(
            db.query_text("SELECT base_commit FROM jobs WHERE id = 'child'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("new-base")
        );
        assert_eq!(
            db.query_text("SELECT pack_anchor FROM jobs WHERE id = 'owner'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("archive-anchor"),
            "the archival anchor remains the historical coordinate"
        );
        assert!(compare_and_swap_owner_base(
            db.as_ref(),
            "owner".into(),
            "/worktree".into(),
            "old-base".into(),
            "other-base".into(),
            4,
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn failed_provisioning_restores_only_its_own_reservation() {
        let db = Arc::new(migrated_test_db("workspace-restore-cas.db").await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'default', 'Project', 'PRJ', '/repo', 1, 1);
             INSERT INTO jobs (id, project_id, status, worktree_path, branch, created_at, updated_at)
             VALUES ('job', 'project', 'running', '/reserved', 'agent/reserved', 1, 1);",
        )
        .await
        .unwrap();

        restore_workspace_assignment(
            db.clone(),
            "job".into(),
            "/reserved".into(),
            "agent/reserved".into(),
            None,
            None,
            2,
        )
        .await
        .unwrap();
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM jobs WHERE id = 'job' AND worktree_path IS NULL AND branch IS NULL",
                (),
            )
            .await
            .unwrap(),
            Some(1)
        );
        assert!(restore_workspace_assignment(
            db,
            "job".into(),
            "/reserved".into(),
            "agent/reserved".into(),
            None,
            None,
            3,
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn owner_branch_rebind_is_compare_and_swap() {
        let db = Arc::new(migrated_test_db("workspace-rebind-cas.db").await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project', 'default', 'Project', 'PRJ', '/repo', 1, 1);
             INSERT INTO jobs (id, project_id, status, worktree_path, branch, base_commit, created_at, updated_at)
             VALUES ('owner', 'project', 'running', '/worktree', 'agent/old', 'base', 1, 1);
             INSERT INTO jobs (id, parent_job_id, project_id, status, worktree_path, branch, base_commit, created_at, updated_at)
             VALUES ('child', 'owner', 'project', 'blocked', '/worktree', 'agent/old', 'base', 2, 2);",
        )
        .await
        .unwrap();
        compare_and_swap_owner_branch(
            db.clone(),
            "owner".into(),
            "/worktree".into(),
            "agent/old".into(),
            "agent/new".into(),
            3,
        )
        .await
        .unwrap();
        let branch = db
            .query_text("SELECT branch FROM jobs WHERE id = 'owner'", ())
            .await
            .unwrap();
        assert_eq!(branch.as_deref(), Some("agent/new"));
        let child_branch = db
            .query_text("SELECT branch FROM jobs WHERE id = 'child'", ())
            .await
            .unwrap();
        assert_eq!(child_branch.as_deref(), Some("agent/new"));
        assert!(compare_and_swap_owner_branch(
            db,
            "owner".into(),
            "/worktree".into(),
            "agent/old".into(),
            "agent/other".into(),
            4,
        )
        .await
        .is_err());
    }
}
