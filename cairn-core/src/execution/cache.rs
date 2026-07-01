//! Checkpoint cache query operations.
//!
//! Caching system for command results at checkpoint nodes to avoid
//! re-executing expensive operations.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use std::sync::Arc;
use turso::params;

/// Result of querying the checkpoint command cache for a job.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointCacheResult {
    pub command: String,
    pub exit_code: i32,
    pub commit_sha: String,
    pub is_valid: bool,
    pub ran_at: i32,
}

/// Get a cached project-declared check result by project, sealed tree identity,
/// and check name.
pub fn get_check_result(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
    check_name: &str,
) -> Result<Option<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    let tree_hash = tree_hash.to_string();
    let check_name = check_name.to_string();

    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            let tree_hash = tree_hash.clone();
            let check_name = check_name.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT project_id, tree_hash, check_name, exit_code, passed,
                               output_tail, duration_ms, ran_at, target_results_json
                        FROM check_result_cache
                        WHERE project_id = ?1 AND tree_hash = ?2 AND check_name = ?3
                        ",
                        params![project_id.as_str(), tree_hash.as_str(), check_name.as_str()],
                    )
                    .await?;

                rows.next()
                    .await?
                    .map(|row| {
                        Ok::<_, crate::storage::DbError>(CheckResultCacheEntry {
                            project_id: row.text(0)?,
                            tree_hash: row.text(1)?,
                            check_name: row.text(2)?,
                            exit_code: row.i64(3)? as i32,
                            passed: row.i64(4)? != 0,
                            output_tail: row.text(5)?,
                            duration_ms: row.i64(6)?,
                            ran_at: row.i64(7)?,
                            target_results_json: row.opt_text(8)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|e| format!("Failed to load check result cache row: {e}"))
    })
}

/// Store or replace a cached project-declared check result.
pub fn store_check_result(db: Arc<LocalDb>, result: CheckResultCacheWrite) -> Result<(), String> {
    run_checkpoint_cache_db(async move {
        db.write(|conn| {
            let result = result.clone();
            Box::pin(async move {
                let ran_at = chrono::Utc::now().timestamp();
                conn.execute(
                    "
                    INSERT INTO check_result_cache (
                        project_id, tree_hash, check_name, exit_code, passed, output_tail,
                        duration_ms, ran_at, target_results_json
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                    ON CONFLICT(project_id, tree_hash, check_name) DO UPDATE SET
                        exit_code = excluded.exit_code,
                        passed = excluded.passed,
                        output_tail = excluded.output_tail,
                        duration_ms = excluded.duration_ms,
                        ran_at = excluded.ran_at,
                        target_results_json = excluded.target_results_json
                    ",
                    params![
                        result.project_id.as_str(),
                        result.tree_hash.as_str(),
                        result.check_name.as_str(),
                        result.exit_code as i64,
                        if result.passed { 1_i64 } else { 0_i64 },
                        result.output_tail.as_str(),
                        result.duration_ms,
                        ran_at,
                        result.target_results_json.as_deref(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to store check result cache row: {e}"))
    })
}

/// List every cached check result for a project at one sealed tree identity,
/// ordered by check name. Powers the `/checks` projection and the PR-node
/// `### Systematic checks` section, which render all of a tree's verdicts at once.
pub fn list_check_results(
    db: Arc<LocalDb>,
    project_id: &str,
    tree_hash: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    let tree_hash = tree_hash.to_string();
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            let tree_hash = tree_hash.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT project_id, tree_hash, check_name, exit_code, passed,
                               output_tail, duration_ms, ran_at, target_results_json
                        FROM check_result_cache
                        WHERE project_id = ?1 AND tree_hash = ?2
                        ORDER BY check_name ASC
                        ",
                        params![project_id.as_str(), tree_hash.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(CheckResultCacheEntry {
                        project_id: row.text(0)?,
                        tree_hash: row.text(1)?,
                        check_name: row.text(2)?,
                        exit_code: row.i64(3)? as i32,
                        passed: row.i64(4)? != 0,
                        output_tail: row.text(5)?,
                        duration_ms: row.i64(6)?,
                        ran_at: row.i64(7)?,
                        target_results_json: row.opt_text(8)?,
                    });
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list check result cache rows: {e}"))
    })
}

/// Cached result for one project-declared check at one sealed tree identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckResultCacheEntry {
    pub project_id: String,
    pub tree_hash: String,
    pub check_name: String,
    pub exit_code: i32,
    pub passed: bool,
    pub output_tail: String,
    pub duration_ms: i64,
    pub ran_at: i64,
    pub target_results_json: Option<String>,
}

/// Write payload for a check-result cache row.
#[derive(Debug, Clone)]
pub struct CheckResultCacheWrite {
    pub project_id: String,
    pub tree_hash: String,
    pub check_name: String,
    pub exit_code: i32,
    pub passed: bool,
    pub output_tail: String,
    pub duration_ms: i64,
    pub target_results_json: Option<String>,
}

/// Normalize a shell command string for stable cache key comparison.
pub(crate) fn normalize_command(cmd: &str) -> String {
    cmd.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Head commit of an agent worktree — the jj analogue of `git rev-parse HEAD`.
/// An agent worktree is a `.jj` workspace with no `.git`, so this reads jj's
/// `@-` (the base of the empty working-copy commit): the last sealed commit, or
/// the worktree's base when nothing has been sealed yet.
pub(crate) fn get_current_head_sha(
    orch: &Orchestrator,
    worktree_path: &str,
) -> Result<String, String> {
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    crate::jj::head_commit(&jj, std::path::Path::new(worktree_path))
}

/// Whether an agent worktree has un-sealed changes. The in-progress change lives
/// in jj's `@`, so this is the jj-aware dirty check (not `git status`, which
/// fails in a `.jj`-only workspace and would force the checkpoint cache to treat
/// every worktree as perpetually dirty).
pub(crate) fn is_worktree_dirty(orch: &Orchestrator, worktree_path: &str) -> Result<bool, String> {
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    crate::jj::is_working_copy_dirty(&jj, std::path::Path::new(worktree_path))
}

/// Get the checkpoint cache result for a job.
/// Returns the cached CI/checkpoint command result if one exists.
pub fn get_checkpoint_cache_result_impl(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<CheckpointCacheResult>, String> {
    let db = run_checkpoint_cache_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job_id = job_id.to_string();
    // Read the cache row first, then validate it against live worktree state
    // outside the DB closure — the jj-aware head/dirty checks need `orch`, which
    // can't be captured into the 'static DB future.
    let row = run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT c.command, c.exit_code, c.commit_sha, c.is_dirty,
                               c.ran_at, j.worktree_path
                        FROM checkpoint_command_cache c
                        JOIN jobs j ON j.id = c.job_id
                        WHERE c.job_id = ?1
                        ORDER BY c.ran_at DESC
                        LIMIT 1
                        ",
                        (job_id.as_str(),),
                    )
                    .await?;

                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };

                Ok(Some((
                    row.text(0)?,
                    row.i64(1)? as i32,
                    row.text(2)?,
                    row.i64(3)?,
                    row.i64(4)? as i32,
                    row.opt_text(5)?,
                )))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;

    let Some((command, exit_code, commit_sha, is_dirty, ran_at, worktree_path)) = row else {
        return Ok(None);
    };

    let is_valid = if let Some(worktree) = &worktree_path {
        let current_sha = get_current_head_sha(orch, worktree).unwrap_or_default();
        let currently_dirty = is_worktree_dirty(orch, worktree).unwrap_or(true);
        commit_sha == current_sha && is_dirty == 0 && !currently_dirty
    } else {
        false
    };

    Ok(Some(CheckpointCacheResult {
        command,
        exit_code,
        commit_sha: commit_sha[..7.min(commit_sha.len())].to_string(),
        is_valid,
        ran_at,
    }))
}

fn run_checkpoint_cache_db<T>(
    future: impl std::future::Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    fn run<T>(future: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(future)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run(future))
            .join()
            .map_err(|_| "Checkpoint cache DB runtime thread panicked".to_string())?
    } else {
        run(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_result(project_id: &str, tree_hash: &str, check_name: &str) -> CheckResultCacheWrite {
        CheckResultCacheWrite {
            project_id: project_id.to_string(),
            tree_hash: tree_hash.to_string(),
            check_name: check_name.to_string(),
            exit_code: 0,
            passed: true,
            output_tail: "ok".to_string(),
            duration_ms: 123,
            target_results_json: None,
        }
    }

    async fn cache_db() -> Arc<LocalDb> {
        let db = crate::storage::migrated_test_db("check-result-cache-test.db").await;
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-a', 'default', 'Project A', 'PA', '/tmp/project-a', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-b', 'default', 'Project B', 'PB', '/tmp/project-b', 1, 1);
            ",
        )
        .await
        .unwrap();
        Arc::new(db)
    }

    #[tokio::test]
    async fn check_result_cache_hit_and_miss() {
        let db = cache_db().await;
        assert!(get_check_result(db.clone(), "project-a", "tree-a", "rust")
            .unwrap()
            .is_none());

        store_check_result(db.clone(), test_result("project-a", "tree-a", "rust")).unwrap();

        let row = get_check_result(db, "project-a", "tree-a", "rust")
            .unwrap()
            .expect("stored result should be cached");
        assert_eq!(row.project_id, "project-a");
        assert_eq!(row.tree_hash, "tree-a");
        assert_eq!(row.check_name, "rust");
        assert_eq!(row.exit_code, 0);
        assert!(row.passed);
        assert_eq!(row.output_tail, "ok");
        assert_eq!(row.duration_ms, 123);
    }

    #[tokio::test]
    async fn check_result_cache_carries_forward_across_equivalent_tree_commits() {
        // `sealed_tree_hash` is content-addressed (the sealed commit's git tree),
        // so a squash/rebase that rewrites the commit id while preserving file
        // content resolves to the SAME tree hash. The cache seam keys on that
        // hash, so the pre-squash verdict is returned for the post-squash commit
        // without re-running the check — the carry-forward this whole change buys.
        let db = cache_db().await;
        let equivalent_tree = "shared-tree-sha";
        store_check_result(
            db.clone(),
            test_result("project-a", equivalent_tree, "rust"),
        )
        .unwrap();

        // A distinct commit that hashes to the same tree hits the stored verdict.
        let row = get_check_result(db, "project-a", equivalent_tree, "rust")
            .unwrap()
            .expect("equivalent-tree commit reuses the cached verdict");
        assert!(row.passed);
        assert_eq!(row.tree_hash, equivalent_tree);
    }

    #[tokio::test]
    async fn check_result_cache_isolates_trees() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "tree-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-a", "tree-b", "rust")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn check_result_cache_isolates_check_names() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "tree-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-a", "tree-a", "frontend")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn check_result_cache_isolates_projects() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "tree-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-b", "tree-a", "rust")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn store_check_result_replaces_same_key() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "tree-a", "rust")).unwrap();

        let mut replacement = test_result("project-a", "tree-a", "rust");
        replacement.exit_code = 1;
        replacement.passed = false;
        replacement.output_tail = "failed".to_string();
        replacement.duration_ms = 456;
        replacement.target_results_json = Some("{\"targets\":[]}".to_string());
        store_check_result(db.clone(), replacement).unwrap();

        let row = get_check_result(db, "project-a", "tree-a", "rust")
            .unwrap()
            .expect("replacement should keep the cache row");
        assert_eq!(row.exit_code, 1);
        assert!(!row.passed);
        assert_eq!(row.output_tail, "failed");
        assert_eq!(row.duration_ms, 456);
        assert_eq!(row.target_results_json.as_deref(), Some("{\"targets\":[]}"));
    }
}
