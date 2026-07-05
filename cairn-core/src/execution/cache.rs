//! Checkpoint cache query operations.
//!
//! Caching system for command results at checkpoint nodes to avoid
//! re-executing expensive operations.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::sync::Arc;

fn row_to_check_result(
    row: &cairn_db::turso::Row,
) -> Result<CheckResultCacheEntry, crate::storage::DbError> {
    Ok(CheckResultCacheEntry {
        project_id: row.text(0)?,
        tree_hash: row.text(1)?,
        input_hash: row.text(2)?,
        check_name: row.text(3)?,
        exit_code: row.i64(4)? as i32,
        passed: row.i64(5)? != 0,
        output_tail: row.text(6)?,
        duration_ms: row.i64(7)?,
        ran_at: row.i64(8)?,
        target_results_json: row.opt_text(9)?,
        job_id: row.opt_text(10)?,
        cached: row.opt_i64(11)?.map(|v| v != 0),
    })
}

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

/// Get a cached project-declared check result by project, check name, and the
/// per-check INPUT hash (the content identity of just that check's impact-matched
/// files). Keying on the input hash — rather than the whole sealed tree — is what
/// lets a commit that touched none of a check's inputs reuse the stored verdict.
pub fn get_check_result(
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
    input_hash: &str,
) -> Result<Option<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    let check_name = check_name.to_string();
    let input_hash = input_hash.to_string();

    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            let check_name = check_name.clone();
            let input_hash = input_hash.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT project_id, tree_hash, input_hash, check_name, exit_code,
                               passed, output_tail, duration_ms, ran_at, target_results_json,
                               job_id, cached
                        FROM check_result_cache
                        WHERE project_id = ?1 AND check_name = ?2 AND input_hash = ?3
                        ",
                        params![
                            project_id.as_str(),
                            check_name.as_str(),
                            input_hash.as_str()
                        ],
                    )
                    .await?;

                rows.next()
                    .await?
                    .map(|row| row_to_check_result(&row))
                    .transpose()
            })
        })
        .await
        .map_err(|e| format!("Failed to load check result cache row: {e}"))
    })
}

/// Store or replace a cached project-declared check result. Keyed by
/// `(project_id, check_name, input_hash)`; a conflicting write re-stamps
/// `tree_hash` (the whole-tree pointer the `/checks` listing reads) onto the
/// current tree along with the refreshed verdict.
pub fn store_check_result(db: Arc<LocalDb>, result: CheckResultCacheWrite) -> Result<(), String> {
    run_checkpoint_cache_db(async move {
        db.write(|conn| {
            let result = result.clone();
            Box::pin(async move {
                let ran_at = chrono::Utc::now().timestamp();
                conn.execute(
                    "
                    INSERT INTO check_result_cache (
                        project_id, tree_hash, input_hash, check_name, exit_code, passed,
                        output_tail, duration_ms, ran_at, target_results_json, job_id, cached
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                    ON CONFLICT(project_id, check_name, input_hash) DO UPDATE SET
                        tree_hash = excluded.tree_hash,
                        exit_code = excluded.exit_code,
                        passed = excluded.passed,
                        output_tail = excluded.output_tail,
                        duration_ms = excluded.duration_ms,
                        ran_at = excluded.ran_at,
                        target_results_json = excluded.target_results_json,
                        job_id = excluded.job_id,
                        cached = excluded.cached
                    ",
                    params![
                        result.project_id.as_str(),
                        result.tree_hash.as_str(),
                        result.input_hash.as_str(),
                        result.check_name.as_str(),
                        result.exit_code as i64,
                        if result.passed { 1_i64 } else { 0_i64 },
                        result.output_tail.as_str(),
                        result.duration_ms,
                        ran_at,
                        result.target_results_json.as_deref(),
                        result.job_id.as_deref(),
                        result
                            .cached
                            .map(|cached| if cached { 1_i64 } else { 0_i64 }),
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
                        SELECT project_id, tree_hash, input_hash, check_name, exit_code,
                               passed, output_tail, duration_ms, ran_at, target_results_json,
                               job_id, cached
                        FROM check_result_cache
                        WHERE project_id = ?1 AND tree_hash = ?2
                        ORDER BY check_name ASC
                        ",
                        params![project_id.as_str(), tree_hash.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row_to_check_result(&row)?);
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list check result cache rows: {e}"))
    })
}

/// List the MOST RECENT cached result per check name for a project, across every
/// sealed tree the project has ever run against. Where [`list_check_results`] is
/// keyed to one tree (the node/PR views, which show a single tree's verdicts),
/// this powers the project-settings Checks editor, which has no worktree in scope
/// and wants "how did each configured check last do".
///
/// One row per `check_name` is selected by an anti-join: keep the row for which no
/// newer row (by `ran_at`, tie-broken by `tree_hash`) exists for the same check.
/// The tie-break makes the pick deterministic when two trees share a `ran_at`
/// second. Ordered by check name for a stable render. Backed by
/// `idx_check_result_cache_project_ran_at`.
pub fn list_latest_check_results_for_project(
    db: Arc<LocalDb>,
    project_id: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let project_id = project_id.to_string();
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT c.project_id, c.tree_hash, c.input_hash, c.check_name, c.exit_code,
                               c.passed, c.output_tail, c.duration_ms, c.ran_at,
                               c.target_results_json, c.job_id, c.cached
                        FROM check_result_cache c
                        WHERE c.project_id = ?1
                          AND NOT EXISTS (
                              SELECT 1 FROM check_result_cache newer
                              WHERE newer.project_id = c.project_id
                                AND newer.check_name = c.check_name
                                AND (newer.ran_at > c.ran_at
                                     OR (newer.ran_at = c.ran_at
                                         AND newer.tree_hash > c.tree_hash))
                          )
                        ORDER BY c.check_name ASC
                        ",
                        params![project_id.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row_to_check_result(&row)?);
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list latest check result cache rows: {e}"))
    })
}

/// List the most recent cached result per check name for one job, independent of
/// the current worktree/tree pointer. This is the durable fallback for node-level
/// surfaces after worktree teardown or movement.
pub fn list_check_results_for_job(
    db: Arc<LocalDb>,
    job_id: &str,
) -> Result<Vec<CheckResultCacheEntry>, String> {
    let job_id = job_id.to_string();
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT c.project_id, c.tree_hash, c.input_hash, c.check_name, c.exit_code,
                               c.passed, c.output_tail, c.duration_ms, c.ran_at,
                               c.target_results_json, c.job_id, c.cached
                        FROM check_result_cache c
                        WHERE c.job_id = ?1
                          AND NOT EXISTS (
                              SELECT 1 FROM check_result_cache newer
                              WHERE newer.job_id = c.job_id
                                AND newer.check_name = c.check_name
                                AND (newer.ran_at > c.ran_at
                                     OR (newer.ran_at = c.ran_at
                                         AND newer.tree_hash > c.tree_hash)
                                     OR (newer.ran_at = c.ran_at
                                         AND newer.tree_hash = c.tree_hash
                                         AND newer.input_hash > c.input_hash))
                          )
                        ORDER BY c.check_name ASC
                        ",
                        params![job_id.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(row_to_check_result(&row)?);
                }
                Ok::<_, crate::storage::DbError>(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to list job check result cache rows: {e}"))
    })
}

/// Cached result for one project-declared check at one sealed tree identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckResultCacheEntry {
    pub project_id: String,
    pub tree_hash: String,
    /// Per-check input hash: the content identity of just this check's impact-
    /// matched files. The cache's real key (with project + check name).
    pub input_hash: String,
    pub check_name: String,
    pub exit_code: i32,
    pub passed: bool,
    pub output_tail: String,
    pub duration_ms: i64,
    pub ran_at: i64,
    pub target_results_json: Option<String>,
    pub job_id: Option<String>,
    pub cached: Option<bool>,
}

/// Write payload for a check-result cache row.
#[derive(Debug, Clone)]
pub struct CheckResultCacheWrite {
    pub project_id: String,
    pub tree_hash: String,
    /// Per-check input hash — see [`CheckResultCacheEntry::input_hash`].
    pub input_hash: String,
    pub check_name: String,
    pub exit_code: i32,
    pub passed: bool,
    pub output_tail: String,
    pub duration_ms: i64,
    pub target_results_json: Option<String>,
    pub job_id: Option<String>,
    pub cached: Option<bool>,
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

    /// A passing write. The middle argument seeds both `tree_hash` and
    /// `input_hash` to the same value so tests that don't care about the
    /// distinction stay terse; tests that exercise the two independently set
    /// `input_hash` explicitly on the returned struct.
    fn test_result(project_id: &str, hash: &str, check_name: &str) -> CheckResultCacheWrite {
        CheckResultCacheWrite {
            project_id: project_id.to_string(),
            tree_hash: hash.to_string(),
            input_hash: hash.to_string(),
            check_name: check_name.to_string(),
            exit_code: 0,
            passed: true,
            output_tail: "ok".to_string(),
            duration_ms: 123,
            target_results_json: None,
            job_id: None,
            cached: None,
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
        assert!(get_check_result(db.clone(), "project-a", "rust", "input-a")
            .unwrap()
            .is_none());

        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        let row = get_check_result(db, "project-a", "rust", "input-a")
            .unwrap()
            .expect("stored result should be cached");
        assert_eq!(row.project_id, "project-a");
        assert_eq!(row.input_hash, "input-a");
        assert_eq!(row.check_name, "rust");
        assert_eq!(row.exit_code, 0);
        assert!(row.passed);
        assert_eq!(row.output_tail, "ok");
        assert_eq!(row.duration_ms, 123);
    }

    #[tokio::test]
    async fn check_result_cache_round_trips_job_id_and_cached_stamp() {
        let db = cache_db().await;
        let mut write = test_result("project-a", "input-a", "rust");
        write.job_id = Some("job-1".to_string());
        write.cached = Some(false);
        store_check_result(db.clone(), write).unwrap();

        let row = get_check_result(db.clone(), "project-a", "rust", "input-a")
            .unwrap()
            .expect("stored result should be cached");
        assert_eq!(row.job_id.as_deref(), Some("job-1"));
        assert_eq!(row.cached, Some(false));

        let mut restamp = test_result("project-a", "tree-2", "rust");
        restamp.input_hash = "input-a".to_string();
        restamp.job_id = Some("job-1".to_string());
        restamp.cached = Some(true);
        store_check_result(db.clone(), restamp).unwrap();

        let row = get_check_result(db, "project-a", "rust", "input-a")
            .unwrap()
            .expect("restamped result should remain cached");
        assert_eq!(row.tree_hash, "tree-2");
        assert_eq!(row.cached, Some(true));
    }

    #[tokio::test]
    async fn check_result_cache_carries_forward_across_equivalent_tree_commits() {
        // `sealed_tree_hash` is content-addressed (the sealed commit's git tree),
        // so a squash/rebase that rewrites the commit id while preserving file
        // content resolves to the SAME tree hash. The cache seam keys on that
        // hash, so the pre-squash verdict is returned for the post-squash commit
        // without re-running the check — the carry-forward this whole change buys.
        let db = cache_db().await;
        let equivalent_input = "shared-input-sha";
        store_check_result(
            db.clone(),
            test_result("project-a", equivalent_input, "rust"),
        )
        .unwrap();

        // A distinct commit whose matching files hash the same hits the verdict.
        let row = get_check_result(db, "project-a", "rust", equivalent_input)
            .unwrap()
            .expect("equivalent-input commit reuses the cached verdict");
        assert!(row.passed);
        assert_eq!(row.input_hash, equivalent_input);
    }

    #[tokio::test]
    async fn check_result_cache_isolates_input_hashes() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-a", "rust", "input-b")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn check_result_cache_isolates_check_names() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-a", "frontend", "input-a")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn check_result_cache_isolates_projects() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        assert!(get_check_result(db, "project-b", "rust", "input-a")
            .unwrap()
            .is_none());
    }

    /// The hit/miss key is the input hash; `tree_hash` is only the listing
    /// pointer. Two rows with the same tree but different inputs are distinct.
    #[tokio::test]
    async fn get_keys_by_input_hash_not_tree_hash() {
        let db = cache_db().await;
        let mut row = test_result("project-a", "tree-1", "rust");
        row.input_hash = "input-a".to_string();
        store_check_result(db.clone(), row).unwrap();

        assert!(get_check_result(db.clone(), "project-a", "rust", "input-a")
            .unwrap()
            .is_some());
        assert!(get_check_result(db, "project-a", "rust", "other-input")
            .unwrap()
            .is_none());
    }

    /// A later commit with the SAME input hash but a new whole-tree hash re-stamps
    /// the single input-keyed row (upsert updates `tree_hash`) rather than adding
    /// a second row — so the tree-keyed listing follows the current tree.
    #[tokio::test]
    async fn restamp_moves_tree_pointer_for_listing() {
        let db = cache_db().await;
        let mut r1 = test_result("project-a", "tree-1", "rust");
        r1.input_hash = "IH".to_string();
        store_check_result(db.clone(), r1).unwrap();
        assert_eq!(
            list_check_results(db.clone(), "project-a", "tree-1")
                .unwrap()
                .len(),
            1
        );

        let mut r2 = test_result("project-a", "tree-2", "rust");
        r2.input_hash = "IH".to_string();
        store_check_result(db.clone(), r2).unwrap();

        assert!(list_check_results(db.clone(), "project-a", "tree-1")
            .unwrap()
            .is_empty());
        let rows = list_check_results(db, "project-a", "tree-2").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].check_name, "rust");
    }

    #[tokio::test]
    async fn store_check_result_replaces_same_key() {
        let db = cache_db().await;
        store_check_result(db.clone(), test_result("project-a", "input-a", "rust")).unwrap();

        let mut replacement = test_result("project-a", "input-a", "rust");
        replacement.exit_code = 1;
        replacement.passed = false;
        replacement.output_tail = "failed".to_string();
        replacement.duration_ms = 456;
        replacement.target_results_json = Some("{\"targets\":[]}".to_string());
        store_check_result(db.clone(), replacement).unwrap();

        let row = get_check_result(db, "project-a", "rust", "input-a")
            .unwrap()
            .expect("replacement should keep the cache row");
        assert_eq!(row.exit_code, 1);
        assert!(!row.passed);
        assert_eq!(row.output_tail, "failed");
        assert_eq!(row.duration_ms, 456);
        assert_eq!(row.target_results_json.as_deref(), Some("{\"targets\":[]}"));
    }

    /// Insert a row with an explicit `ran_at`/`tree_hash` so recency and the
    /// tie-break are deterministic (the public `store_check_result` stamps
    /// `ran_at` with the wall clock, which can't order two same-second writes).
    async fn insert_row(
        db: &LocalDb,
        project_id: &str,
        tree_hash: &str,
        check_name: &str,
        passed: bool,
        output_tail: &str,
        ran_at: i64,
    ) {
        // `input_hash` is the cache key; here it mirrors `tree_hash` so each
        // distinct-tree insert stays a distinct row under the
        // `(project_id, check_name, input_hash)` primary key.
        db.execute_script(&format!(
            "INSERT INTO check_result_cache
               (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                output_tail, duration_ms, ran_at)
             VALUES ('{project_id}', '{tree_hash}', '{tree_hash}', '{check_name}', {exit}, {passed},
                '{output_tail}', 10, {ran_at});",
            exit = if passed { 0 } else { 1 },
            passed = if passed { 1 } else { 0 },
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn latest_per_check_picks_newest_tree_and_isolates_projects() {
        let db = cache_db().await;
        // `rust` ran against an old failing tree, then a newer passing tree.
        insert_row(&db, "project-a", "tree-old", "rust", false, "old fail", 100).await;
        insert_row(&db, "project-a", "tree-new", "rust", true, "new pass", 200).await;
        // A second check, and a same-named check in another project that must not leak.
        insert_row(&db, "project-a", "tree-new", "frontend", true, "fe", 150).await;
        insert_row(&db, "project-b", "tree-new", "rust", true, "other", 999).await;

        let rows = list_latest_check_results_for_project(db, "project-a").unwrap();
        // One row per check name, ordered by name: frontend, then rust.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].check_name, "frontend");
        assert_eq!(rows[1].check_name, "rust");
        // The `rust` verdict is the NEWER tree's pass, not the older fail.
        assert!(rows[1].passed);
        assert_eq!(rows[1].tree_hash, "tree-new");
        assert_eq!(rows[1].output_tail, "new pass");
    }

    #[tokio::test]
    async fn latest_per_check_breaks_ran_at_ties_deterministically() {
        let db = cache_db().await;
        // Same check at two trees with an IDENTICAL ran_at: the tie-break on
        // tree_hash keeps exactly one row (the lexicographically greater hash).
        insert_row(&db, "project-a", "tree-aaa", "rust", false, "a", 500).await;
        insert_row(&db, "project-a", "tree-bbb", "rust", true, "b", 500).await;

        let rows = list_latest_check_results_for_project(db, "project-a").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tree_hash, "tree-bbb");
    }

    #[tokio::test]
    async fn job_listing_picks_latest_per_check_and_isolates_jobs() {
        let db = cache_db().await;
        db.execute_script(
            "
            INSERT INTO check_result_cache
               (project_id, tree_hash, input_hash, check_name, exit_code, passed,
                output_tail, duration_ms, ran_at, job_id, cached)
             VALUES
               ('project-a', 'tree-old', 'input-old', 'rust', 0, 1, 'old', 10, 100, 'job-1', 0),
               ('project-a', 'tree-new', 'input-new', 'rust', 0, 1, 'new', 10, 200, 'job-1', 1),
               ('project-a', 'tree-new', 'frontend-input', 'frontend', 0, 1, 'fe', 10, 150, 'job-1', 0),
               ('project-a', 'tree-other', 'other-input', 'rust', 0, 1, 'other', 10, 999, 'job-2', 0);
            ",
        )
        .await
        .unwrap();

        let rows = list_check_results_for_job(db, "job-1").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].check_name, "frontend");
        assert_eq!(rows[1].check_name, "rust");
        assert_eq!(rows[1].tree_hash, "tree-new");
        assert_eq!(rows[1].cached, Some(true));
    }

    #[tokio::test]
    async fn latest_per_check_empty_when_no_results() {
        let db = cache_db().await;
        assert!(list_latest_check_results_for_project(db, "project-a")
            .unwrap()
            .is_empty());
    }
}
