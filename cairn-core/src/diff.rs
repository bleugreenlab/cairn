//! Node-tab `diff` facet resolution, computed entirely from local state.
//!
//! Two store-native signals drive the facet. Presence summaries aggregate
//! `file_changes` across jobs sharing a durable branch coordinate. Patch bodies
//! render the branch's own work — its fork point from its integration target up
//! to its head, both resolved live from the store — with an archived range pack
//! fallback from `execution_history`. Executor projections and process
//! residences are never consulted.
//!
//! [`live_branch_range`] is the one place a rendered diff learns its base, and
//! every diff surface in the app routes through it.

use std::collections::HashMap;
use std::path::Path;

use serde::Serialize;

use crate::storage::{count_commits_ahead, render_range_file_diffs, NodeDiffFile, ObjectStore};
use crate::storage::{DbResult, LocalDb, RowExt};

/// Aggregate change counts for a node's branch-coordinate group. `files_changed`
/// (distinct changed paths) is the facet's presence signal; the optional `+`/`-`
/// totals are `None` when every contributing row recorded a NULL count (e.g. a
/// binary change), matching the `file_changes` schema's nullable columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeSummary {
    files_changed: i32,
    additions: Option<i32>,
    deletions: Option<i32>,
}

/// A node's resolved `base..tip` diff: per-file hunks plus rolled-up stats.
#[derive(Debug, Clone)]
pub struct NodeDiff {
    pub files: Vec<NodeDiffFile>,
    pub commits_ahead: i32,
    pub total_additions: i32,
    pub total_deletions: i32,
}

/// One aggregation row drawn from `file_changes`: (path, additions, deletions).
/// Status and previous_path are not needed for the count-only summary.
type ChangeRow = (String, Option<i32>, Option<i32>);

fn merge_counts(existing: Option<i32>, next: Option<i32>) -> Option<i32> {
    match (existing, next) {
        (Some(a), Some(b)) => Some(a + b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Collapse raw rows into a summary: dedupe by path (summing `+`/`-` across a
/// path's rows, tolerating NULL), then count distinct paths.
fn summarize(rows: &[ChangeRow]) -> ChangeSummary {
    let mut per_path: Vec<(String, Option<i32>, Option<i32>)> = Vec::new();
    for (path, additions, deletions) in rows {
        if let Some(entry) = per_path.iter_mut().find(|(p, _, _)| p == path) {
            entry.1 = merge_counts(entry.1, *additions);
            entry.2 = merge_counts(entry.2, *deletions);
        } else {
            per_path.push((path.clone(), *additions, *deletions));
        }
    }

    let mut additions = None;
    let mut deletions = None;
    for (_, add, del) in &per_path {
        additions = merge_counts(additions, *add);
        deletions = merge_counts(deletions, *del);
    }
    ChangeSummary {
        files_changed: per_path.len() as i32,
        additions,
        deletions,
    }
}

/// Summarize the branch-coordinate group for a single node.
pub async fn node_change_summary(db: &LocalDb, job_id: &str) -> DbResult<ChangeSummary> {
    let job_id = job_id.to_string();
    let rows = db
        .read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT fc.file_path, fc.additions, fc.deletions
                         FROM file_changes fc
                         JOIN jobs j ON fc.job_id = j.id
                         WHERE j.execution_id = (SELECT execution_id FROM jobs WHERE id = ?1)
                           AND j.branch = (SELECT branch FROM jobs WHERE id = ?1)
                         ORDER BY fc.file_path ASC",
                        (job_id.as_str(),),
                    )
                    .await?;
                let mut out: Vec<ChangeRow> = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((
                        row.text(0)?,
                        row.opt_i64(1)?.map(|v| v as i32),
                        row.opt_i64(2)?.map(|v| v as i32),
                    ));
                }
                Ok(out)
            })
        })
        .await?;
    Ok(summarize(&rows))
}

/// Summarize every top-level branch-owning node in an execution, aggregated over
/// jobs sharing its branch coordinate. Drives the node-tab strip's diff icons.
pub async fn execution_change_summaries(
    db: &LocalDb,
    execution_id: &str,
) -> DbResult<HashMap<String, ChangeSummary>> {
    let execution_id = execution_id.to_string();
    db.read(move |conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut owners: Vec<(String, String)> = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT id, branch FROM jobs
                     WHERE execution_id = ?1
                       AND parent_job_id IS NULL
                       AND branch IS NOT NULL",
                    (execution_id.as_str(),),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                owners.push((row.text(0)?, row.text(1)?));
            }

            let mut by_branch: HashMap<String, Vec<ChangeRow>> = HashMap::new();
            let mut rows = conn
                .query(
                    "SELECT j.branch, fc.file_path, fc.additions, fc.deletions
                     FROM file_changes fc
                     JOIN jobs j ON fc.job_id = j.id
                     WHERE j.execution_id = ?1 AND j.branch IS NOT NULL
                     ORDER BY fc.file_path ASC",
                    (execution_id.as_str(),),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                let branch = row.text(0)?;
                by_branch.entry(branch).or_default().push((
                    row.text(1)?,
                    row.opt_i64(2)?.map(|v| v as i32),
                    row.opt_i64(3)?.map(|v| v as i32),
                ));
            }

            let mut out: HashMap<String, ChangeSummary> = HashMap::new();
            for (job_id, branch) in owners {
                let summary = by_branch
                    .get(&branch)
                    .map(|rows| summarize(rows))
                    .unwrap_or(ChangeSummary {
                        files_changed: 0,
                        additions: None,
                        deletions: None,
                    });
                out.insert(job_id, summary);
            }
            Ok(out)
        })
    })
    .await
}

/// Job + project coordinates needed to resolve a node's diff range.
struct DiffCoords {
    branch: Option<String>,
    execution_id: Option<String>,
    repo_path: String,
    integration_target: String,
}

async fn load_diff_coords(db: &LocalDb, job_id: &str) -> DbResult<Option<DiffCoords>> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            // `base_branch` is the branch this node's work merges into: the parent
            // issue's branch for a stacked child, the project default otherwise.
            let mut rows = conn
                .query(
                    "SELECT j.branch, j.execution_id, p.repo_path,
                            COALESCE(j.base_branch, p.default_branch, 'main')
                     FROM jobs j JOIN projects p ON j.project_id = p.id
                     WHERE j.id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(DiffCoords {
                branch: row.opt_text(0)?,
                execution_id: row.opt_text(1)?,
                repo_path: row.text(2)?,
                integration_target: row.text(3)?,
            }))
        })
    })
    .await
}

/// The live range a branch's own work occupies: its fork point from the
/// integration target, and its current head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRange {
    pub base: String,
    pub tip: String,
}

/// Resolve what a branch itself changed, live from the store at read time.
///
/// Both endpoints are computed now: the branch bookmark's current commit, and
/// its merge base with the integration target's current commit. That merge base
/// is the branch's true fork point no matter how the target has moved.
///
/// The recorded alternatives — `jobs.base_commit` and `jobs.pack_anchor` — are
/// the coordinate a branch was cut at, and they do not track a base advance.
/// Diffing from a stale one absorbs every commit the target merged in the
/// meantime, which is how a +350/-62 branch renders as +6k/-3k (CAIRN-3150). No
/// rendered diff reads those rows; this function is the only base a diff gets.
///
/// `integration_target` is what `jobs.base_branch` records. An unresolvable
/// endpoint is an error, never a silent fall back to a recorded coordinate.
pub async fn live_branch_range(
    jj_binary_path: &str,
    config_dir: &Path,
    repo_path: &Path,
    branch: &str,
    integration_target: &str,
) -> Result<BranchRange, String> {
    let jj_binary_path = jj_binary_path.to_string();
    let config_dir = config_dir.to_path_buf();
    let project_repo = repo_path.to_path_buf();
    let repository = tokio::task::spawn_blocking(move || {
        let jj = crate::jj::JjEnv::resolve(&jj_binary_path, &config_dir);
        crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
    })
    .await
    .map_err(|error| format!("coordinate repository task failed: {error}"))??;
    let resolved = cairn_vcs::merge_base(&repository, branch, integration_target)
        .await
        .map_err(|error| format!("resolving '{branch}' against '{integration_target}': {error}"))?;
    Ok(BranchRange {
        base: resolved.base,
        tip: resolved.left,
    })
}

/// The live range a job's own work occupies, resolved from its durable row's
/// *names* rather than its recorded commit.
///
/// This is the job-scoped form of [`live_branch_range`], and it is what every
/// surface asking "what did this node change" resolves its base through: the
/// rendered diff, the review-cadence impact gate, and the write-check impact
/// gate. `jobs.branch` and `jobs.base_branch` are names, so they cannot go
/// stale the way `jobs.base_commit` does; both endpoints are computed from the
/// store now.
///
/// `Ok(None)` means the job has no branch of its own and therefore has no
/// range. An unresolvable endpoint is an error, never a silent fall back to a
/// recorded coordinate.
pub async fn live_job_branch_range(
    db: &LocalDb,
    job_id: &str,
    jj_binary_path: &str,
    config_dir: &Path,
) -> Result<Option<BranchRange>, String> {
    let Some(coords) = load_diff_coords(db, job_id)
        .await
        .map_err(|error| format!("loading node diff coordinates: {error}"))?
    else {
        return Ok(None);
    };
    let Some(branch) = coords.branch.as_deref() else {
        return Ok(None);
    };
    live_branch_range(
        jj_binary_path,
        config_dir,
        Path::new(&coords.repo_path),
        branch,
        &coords.integration_target,
    )
    .await
    .map(Some)
}

async fn load_execution_history(
    db: &LocalDb,
    execution_id: &str,
) -> DbResult<Option<(String, String, Option<(Vec<u8>, Vec<u8>)>)>> {
    let execution_id = execution_id.to_string();
    db.read(move |conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT base_sha, tip_sha, pack, pack_idx
                     FROM execution_history WHERE execution_id = ?1 LIMIT 1",
                    (execution_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let base_sha = row.text(0)?;
            let tip_sha = row.text(1)?;
            let pack = match (row.opt_blob(2)?, row.opt_blob(3)?) {
                (Some(pack), Some(idx)) => Some((pack, idx)),
                _ => None,
            };
            Ok(Some((base_sha, tip_sha, pack)))
        })
    })
    .await
}

/// Resolve and render a node's own work as a diff. Returns `Ok(None)` when
/// neither the live store nor archived execution history supplies a coherent
/// base/tip pair.
pub async fn node_base_tip_diff(
    db: &LocalDb,
    job_id: &str,
    jj_binary_path: &str,
    config_dir: &Path,
) -> Result<Option<NodeDiff>, String> {
    let Some(coords) = load_diff_coords(db, job_id)
        .await
        .map_err(|e| format!("loading node diff coordinates: {e}"))?
    else {
        return Ok(None);
    };
    let repo = Path::new(&coords.repo_path);
    let live = match coords.branch.as_deref() {
        Some(branch) => {
            match live_branch_range(
                jj_binary_path,
                config_dir,
                repo,
                branch,
                &coords.integration_target,
            )
            .await
            {
                Ok(range) => Some(range),
                Err(error) => {
                    log::debug!("node {job_id} has no live diff range: {error}");
                    None
                }
            }
        }
        None => None,
    };

    let (store, base_hex, tip_hex) = if let Some(range) = live {
        let store = ObjectStore::new(repo, None)
            .map_err(|e| format!("building logical object store: {e}"))?;
        (store, range.base, range.tip)
    } else {
        let Some(execution_id) = coords.execution_id.clone() else {
            return Ok(None);
        };
        let Some((base, tip, pack)) = load_execution_history(db, &execution_id)
            .await
            .map_err(|e| format!("loading execution history: {e}"))?
        else {
            return Ok(None);
        };
        let store = ObjectStore::new(repo, pack)
            .map_err(|e| format!("building archived object store: {e}"))?;
        (store, base, tip)
    };

    let files = render_range_file_diffs(&store, &base_hex, &tip_hex)?;
    let total_additions = files.iter().map(|f| f.additions as i32).sum();
    let total_deletions = files.iter().map(|f| f.deletions as i32).sum();
    let commits_ahead = count_commits_ahead(&store, &base_hex, &tip_hex);

    Ok(Some(NodeDiff {
        files,
        commits_ahead,
        total_additions,
        total_deletions,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarize_dedupes_paths_and_sums_counts() {
        let rows = vec![
            ("a.rs".to_string(), Some(3), Some(1)),
            ("a.rs".to_string(), Some(2), None),
            ("b.rs".to_string(), None, None),
        ];
        let summary = summarize(&rows);
        assert_eq!(summary.files_changed, 2);
        assert_eq!(summary.additions, Some(5));
        assert_eq!(summary.deletions, Some(1));
    }

    #[test]
    fn summarize_reports_none_when_all_counts_null() {
        let rows = vec![
            ("bin.dat".to_string(), None, None),
            ("bin2.dat".to_string(), None, None),
        ];
        let summary = summarize(&rows);
        assert_eq!(summary.files_changed, 2);
        assert_eq!(summary.additions, None);
        assert_eq!(summary.deletions, None);
    }

    #[test]
    fn summarize_empty_is_zero() {
        let summary = summarize(&[]);
        assert_eq!(summary.files_changed, 0);
        assert_eq!(summary.additions, None);
        assert_eq!(summary.deletions, None);
    }

    mod db {
        use super::super::*;
        use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

        async fn migrated_db() -> LocalDb {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.keep().join("cairn-node-diff.db");
            let db = LocalDb::open(path).await.unwrap();
            MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
                .run(&db)
                .await
                .unwrap();
            db
        }

        /// Seed a project/execution plus an owner job on `branch` and an
        /// inheriting child job. The owner records `main` as its integration
        /// target; the diff range is computed live against that branch.
        async fn seed_worktree_group(db: &LocalDb, repo_path: &str, branch: &str) {
            let repo_path = repo_path.to_string();
            let branch = branch.to_string();
            db.write(move |conn| {
                let repo_path = repo_path.clone();
                let branch = branch.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                         VALUES ('proj','ws','P','p',?1,'main',1,1)",
                        (repo_path.as_str(),),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                        (),
                    )
                    .await?;
                    // Owner job: earliest created, carries the integration target.
                    conn.execute(
                        "INSERT INTO jobs(id, execution_id, project_id, parent_job_id, branch, base_branch, status, created_at, updated_at)
                         VALUES ('owner','exec','proj',NULL,?1,'main','complete',1,1)",
                        (branch.as_str(),),
                    )
                    .await?;
                    // Inheriting child records its parent coordinate, created later.
                    conn.execute(
                        "INSERT INTO jobs(id, execution_id, project_id, parent_job_id, branch, status, created_at, updated_at)
                         VALUES ('child','exec','proj','owner',?1,'complete',2,2)",
                        (branch.as_str(),),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        async fn insert_file_change(
            db: &LocalDb,
            id: &str,
            job_id: &str,
            file_path: &str,
            additions: Option<i64>,
            deletions: Option<i64>,
        ) {
            let id = id.to_string();
            let job_id = job_id.to_string();
            let file_path = file_path.to_string();
            db.write(move |conn| {
                let id = id.clone();
                let job_id = job_id.clone();
                let file_path = file_path.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO file_changes(id, job_id, file_path, status, additions, deletions, created_at)
                         VALUES (?1,?2,?3,'modified',?4,?5,1)",
                        (id.as_str(), job_id.as_str(), file_path.as_str(), additions, deletions),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        #[tokio::test]
        #[serial_test::serial(jj)]
        async fn thread_style_job_creates_coordinate_store_on_demand() {
            let Some(bin) = crate::jj::tests::jj_bin() else {
                eprintln!(
                    "skipping thread_style_job_creates_coordinate_store_on_demand: jj not resolvable"
                );
                return;
            };
            let home = tempfile::tempdir().unwrap();
            let project = tempfile::tempdir().unwrap();
            crate::jj::tests::init_project(project.path());

            let db = migrated_db().await;
            seed_worktree_group(&db, project.path().to_str().unwrap(), "main").await;
            db.execute(
                "UPDATE jobs SET execution_id = NULL, recipe_node_id = NULL WHERE id = 'owner'",
                (),
            )
            .await
            .unwrap();

            let store = crate::jj::project_store_dir(home.path(), project.path());
            assert!(
                !crate::jj::is_jj_dir(&store),
                "the fixture must start without a managed store"
            );

            let range = live_job_branch_range(&db, "owner", &bin, home.path())
                .await
                .unwrap()
                .expect("the thread-style job has a branch coordinate");

            assert_eq!(range.base, range.tip);
            assert!(
                crate::jj::is_jj_dir(&store),
                "coordinate resolution must create the managed store on first use"
            );
        }

        #[tokio::test]
        async fn change_summary_aggregates_across_branch_children() {
            let db = migrated_db().await;
            seed_worktree_group(&db, "/repo", "agent/test").await;
            // The owner delegated all writes: every row is under the child job.
            insert_file_change(&db, "fc1", "child", "a.rs", Some(5), Some(1)).await;
            insert_file_change(&db, "fc2", "child", "a.rs", Some(2), None).await;
            insert_file_change(&db, "fc3", "child", "bin.dat", None, None).await;

            let summary = node_change_summary(&db, "owner").await.unwrap();
            assert_eq!(summary.files_changed, 2, "a.rs deduped + bin.dat");
            assert_eq!(summary.additions, Some(7));
            assert_eq!(summary.deletions, Some(1));
        }

        #[tokio::test]
        async fn execution_summaries_map_owner_when_only_child_wrote() {
            let db = migrated_db().await;
            seed_worktree_group(&db, "/repo", "agent/test").await;
            insert_file_change(&db, "fc1", "child", "a.rs", Some(3), Some(0)).await;

            let map = execution_change_summaries(&db, "exec").await.unwrap();
            assert_eq!(map.len(), 1, "one owner node");
            let owner = map.get("owner").expect("owner present");
            assert_eq!(owner.files_changed, 1);
            assert_eq!(owner.additions, Some(3));
            assert!(!map.contains_key("child"), "children are not owner nodes");
        }

        #[tokio::test]
        async fn node_base_tip_diff_none_without_a_resolvable_store() {
            let db = migrated_db().await;
            // The repository path points nowhere on disk and there is no
            // execution history, so neither endpoint resolves and the surface
            // reports the diff as unavailable rather than inventing a base.
            seed_worktree_group(&db, "/nonexistent/repo", "agent/test").await;
            let diff = node_base_tip_diff(&db, "owner", "", std::path::Path::new("/tmp"))
                .await
                .unwrap();
            assert!(diff.is_none());
        }

        /// A real store in the shape that inflates a recorded-anchor diff: the
        /// branch is cut from main@A and adds one line of its own, then main
        /// advances to B with a few hundred lines of unrelated landed work.
        /// Returns A — the coordinate a job row records at branch-cut time.
        fn advance_base_under_branch(
            jj: &crate::jj::JjEnv,
            store: &std::path::Path,
            project: &std::path::Path,
            workspaces: &std::path::Path,
            branch: &str,
        ) -> String {
            crate::jj::ensure_project_store(jj, store, project).unwrap();
            let cut_coordinate = crate::jj::bookmark_commit(jj, store, "main").unwrap();

            let node = workspaces.join("node");
            crate::jj::add_workspace(jj, store, &node, branch, "main", None).unwrap();
            std::fs::write(node.join("branch.rs"), "branch work\n").unwrap();
            crate::jj::seal(jj, &node, "branch work", None).unwrap();

            let advancing = workspaces.join("advancing");
            crate::jj::add_workspace(jj, store, &advancing, "agent/advance", "main", None).unwrap();
            let bulk: String = (0..400).map(|line| format!("landed {line}\n")).collect();
            std::fs::write(advancing.join("unrelated.rs"), bulk).unwrap();
            crate::jj::seal(jj, &advancing, "unrelated landed work", None).unwrap();
            let advanced = crate::jj::head_commit(jj, &advancing).unwrap();
            jj.run(
                store,
                &[
                    "bookmark",
                    "set",
                    "main",
                    "-r",
                    &advanced,
                    "--allow-backwards",
                ],
                "advance main under a live branch",
            )
            .unwrap();
            cut_coordinate
        }

        /// The regression this module exists for (CAIRN-3150).
        ///
        /// A branch-cut coordinate and the true fork point agree right up until
        /// the branch is rebased onto the advanced base. From then on the row
        /// points below 400 lines of landed work the rebase carried into the
        /// branch, and a diff rendered from it presents that work as the
        /// branch's own. The live merge base moves with the rebase, so both
        /// halves below render exactly the one line the branch actually wrote.
        #[tokio::test]
        #[serial_test::serial(jj)]
        async fn node_diff_excludes_base_traffic_and_survives_a_rebase() {
            let Some(bin) = crate::jj::tests::jj_bin() else {
                eprintln!("skipping node_diff_excludes_base_traffic: jj not resolvable");
                return;
            };
            let home = tempfile::tempdir().unwrap();
            let project = tempfile::tempdir().unwrap();
            let workspaces = tempfile::tempdir().unwrap();
            crate::jj::tests::init_project(project.path());
            let jj = crate::jj::JjEnv::resolve(&bin, home.path());
            let store = crate::jj::project_store_dir(home.path(), project.path());
            let branch = "agent/cairn-3150-builder";
            let cut_coordinate =
                advance_base_under_branch(&jj, &store, project.path(), workspaces.path(), branch);

            let db = migrated_db().await;
            seed_worktree_group(&db, project.path().to_str().unwrap(), branch).await;

            let diff = node_base_tip_diff(&db, "owner", &bin, home.path())
                .await
                .unwrap()
                .expect("the live range resolves from the store");
            let paths: Vec<&str> = diff.files.iter().map(|file| file.path.as_str()).collect();
            assert_eq!(
                paths,
                ["branch.rs"],
                "the diff is the branch's own work, not what main merged after the fork"
            );
            assert_eq!((diff.total_additions, diff.total_deletions), (1, 0));

            // Rebasing onto the advanced base moves the fork point with it, so
            // the branch's rendered work is unchanged.
            jj.run(
                &store,
                &[
                    "rebase",
                    "-b",
                    branch,
                    "-d",
                    "main",
                    "--ignore-working-copy",
                ],
                "rebase the branch onto the advanced base",
            )
            .unwrap();

            let rebased = node_base_tip_diff(&db, "owner", &bin, home.path())
                .await
                .unwrap()
                .expect("the live range resolves after a rebase");
            let rebased_paths: Vec<&str> = rebased
                .files
                .iter()
                .map(|file| file.path.as_str())
                .collect();
            assert_eq!(rebased_paths, paths);
            assert_eq!(
                (rebased.total_additions, rebased.total_deletions),
                (diff.total_additions, diff.total_deletions)
            );

            // Fixture integrity: the branch-cut coordinate a job row records is
            // now BELOW the work the rebase carried in, so rendering from it
            // inflates the diff. That inflation is the defect; if this ever goes
            // clean the fixture has stopped reproducing it and the assertions
            // above prove nothing.
            let rebased_tip = crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
            let objects = ObjectStore::new(project.path(), None).unwrap();
            let from_recorded_row =
                render_range_file_diffs(&objects, &cut_coordinate, &rebased_tip).unwrap();
            assert!(
                from_recorded_row
                    .iter()
                    .any(|file| file.path == "unrelated.rs"),
                "the branch-cut coordinate must render the base's landed work as the branch's own"
            );
            assert!(
                from_recorded_row
                    .iter()
                    .map(|file| file.additions)
                    .sum::<u32>()
                    > 100,
                "and must inflate the counts well past the branch's own single line"
            );
        }

        /// The impact gates' half of the same defect (CAIRN-3224).
        ///
        /// All three check gates — the review cadence, the write-check planner,
        /// and the `/checks` status projection — now learn their base from
        /// [`live_job_branch_range`]. This pins what that base is on a branch
        /// whose recorded row has gone stale: the fork point the branch actually
        /// sits on, and therefore a changed-file set containing only the branch's
        /// own work. Selecting from the row instead is the phantom full-check
        /// wave — 400 lines of landed traffic, three of them in files that would
        /// pull in the whole Rust suite.
        ///
        /// The row is deliberately seeded stale rather than left NULL: the
        /// assertion is that the gate does not read it, not that it has nothing
        /// to read.
        #[tokio::test]
        #[serial_test::serial(jj)]
        async fn the_impact_gate_base_is_the_live_fork_point_not_the_recorded_row() {
            let Some(bin) = crate::jj::tests::jj_bin() else {
                eprintln!(
                    "skipping the_impact_gate_base_is_the_live_fork_point: jj not resolvable"
                );
                return;
            };
            let home = tempfile::tempdir().unwrap();
            let project = tempfile::tempdir().unwrap();
            let workspaces = tempfile::tempdir().unwrap();
            crate::jj::tests::init_project(project.path());
            let jj = crate::jj::JjEnv::resolve(&bin, home.path());
            let store = crate::jj::project_store_dir(home.path(), project.path());
            let branch = "agent/cairn-3224-builder";
            let cut_coordinate =
                advance_base_under_branch(&jj, &store, project.path(), workspaces.path(), branch);

            let db = migrated_db().await;
            seed_worktree_group(&db, project.path().to_str().unwrap(), branch).await;
            let recorded = cut_coordinate.clone();
            db.execute(
                "UPDATE jobs SET base_commit = ?1 WHERE id = 'owner'",
                (recorded.as_str(),),
            )
            .await
            .unwrap();

            let before = live_job_branch_range(&db, "owner", &bin, home.path())
                .await
                .unwrap()
                .expect("the branch has a live range");
            assert_eq!(
                before.base, cut_coordinate,
                "before a rebase the fork point and the branch-cut coordinate agree"
            );

            jj.run(
                &store,
                &[
                    "rebase",
                    "-b",
                    branch,
                    "-d",
                    "main",
                    "--ignore-working-copy",
                ],
                "rebase the branch onto the advanced base",
            )
            .unwrap();

            let after = live_job_branch_range(&db, "owner", &bin, home.path())
                .await
                .unwrap()
                .expect("the branch has a live range after the rebase");
            assert_eq!(
                after.base,
                crate::jj::bookmark_commit(&jj, &store, "main").unwrap(),
                "the fork point followed the rebase onto the advanced base"
            );
            assert_eq!(
                after.tip,
                crate::jj::bookmark_commit(&jj, &store, branch).unwrap(),
                "and the head is the branch's current bookmark commit"
            );
            assert_ne!(
                after.base, recorded,
                "the recorded row still names the pre-advance coordinate; the gate must not use it"
            );

            // What the gates actually plan from, computed exactly as they do.
            let changed = crate::jj::logical_changed_files(&jj, &store, &after.base, &after.tip)
                .expect("the live range resolves a changed-file set");
            let paths: Vec<&str> = changed.iter().map(|file| file.path.as_str()).collect();
            assert_eq!(
                paths,
                ["branch.rs"],
                "the impact gate sees the branch's own work and nothing the base merged"
            );

            // Fixture integrity: planning from the stale row is still inflated,
            // so the assertion above is testing the fix and not the fixture.
            let from_row = crate::jj::logical_changed_files(&jj, &store, &recorded, &after.tip)
                .expect("the recorded coordinate still resolves");
            assert!(
                from_row.iter().any(|file| file.path == "unrelated.rs"),
                "the recorded coordinate must still pull the base's landed work into the gate"
            );
        }
    }
}
