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
pub(crate) async fn resolve_managed_workspace_context_conn(
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

/// Compare-and-swap a rewritten base commit on the stable owner, then update
/// every non-terminal job intentionally sharing the exact physical workspace and
/// old base coordinate. The pack anchor remains historical; only the live lineage
/// proof coordinate follows jj's stable change through its rewritten commit id.
pub(crate) async fn compare_and_swap_owner_base(
    db: Arc<LocalDb>,
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
            if changed != 1 {
                return Err(db_internal(format!(
                    "workspace base assignment changed concurrently for owner {owner_job_id}"
                )));
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
            db.clone(),
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
            db,
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
