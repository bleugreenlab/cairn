//! Node-tab `diff` facet resolution, computed entirely from local state.
//!
//! Two signals drive the facet, both sourced without git polling:
//!
//! * **Presence / summary** ([`node_change_summary`], [`execution_change_summaries`])
//!   aggregate the `file_changes` table across a worktree's change-group. The
//!   distinct changed-path count is the icon's presence signal; it is available
//!   the instant the first write lands (the row insert fires `worktree-changed`)
//!   and survives worktree teardown because the rows persist.
//!
//! * **Patch body** ([`node_base_tip_diff`]) renders the cumulative `base..tip`
//!   diff from the in-memory [`ObjectStore`]. For a live worktree the base is the
//!   recorded `pack_anchor` (fork point) and the tip is the worktree's latest
//!   sealed commit, resolved jj-natively (`@-`) because agent worktrees are
//!   non-colocated jj workspaces (`.jj`, no `.git`) — a git read of HEAD inside
//!   one walks up the tree and resolves an unrelated repo. Both commits live in
//!   the shared repo object database. For a torn-down worktree the
//!   base/tip and a layered range pack come from `execution_history`. One
//!   renderer serves both, so the diff matches before, during, and after a PR,
//!   and for local-only PRs.
//!
//! ## Change-group
//!
//! A worktree-owning node and its worktree-inheriting recipe children
//! (QuickBuild/Documenter/Proctor) record their own `file_changes` rows under
//! their own `job_id` but share one `worktree_path`. The change-group is thus
//! every job in the execution sharing that path; an owner that delegated all of
//! its writes has zero rows under its own `job_id`, so the aggregation must span
//! the group. The cumulative patch body covers the children for free — they
//! commit to the same branch, so `base..tip` already includes their commits.

use std::collections::HashMap;
use std::path::Path;

use gix_hash::{oid, ObjectId};
use serde::Serialize;

use crate::archival::{render_range_file_diffs, NodeDiffFile, ObjectStore};
use crate::storage::{DbResult, LocalDb, RowExt};

/// Aggregate change counts for a node's worktree change-group. `files_changed`
/// (distinct changed paths) is the facet's presence signal; the optional `+`/`-`
/// totals are `None` when every contributing row recorded a NULL count (e.g. a
/// binary change), matching the `file_changes` schema's nullable columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeSummary {
    pub files_changed: i32,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
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

/// Summarize the change-group for a single node (top-level worktree-owning job).
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
                           AND j.worktree_path = (SELECT worktree_path FROM jobs WHERE id = ?1)
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

/// Summarize every top-level worktree-owning node in an execution: one entry per
/// job with `parent_job_id IS NULL AND worktree_path IS NOT NULL`, aggregated
/// over its change-group. Drives the node-tab strip's diff icons.
pub async fn execution_change_summaries(
    db: &LocalDb,
    execution_id: &str,
) -> DbResult<HashMap<String, ChangeSummary>> {
    let execution_id = execution_id.to_string();
    db.read(move |conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            // Owner jobs and their worktree paths.
            let mut owners: Vec<(String, String)> = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT id, worktree_path FROM jobs
                     WHERE execution_id = ?1
                       AND parent_job_id IS NULL
                       AND worktree_path IS NOT NULL",
                    (execution_id.as_str(),),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                owners.push((row.text(0)?, row.text(1)?));
            }

            // All file changes in the execution, tagged by their job's worktree.
            let mut by_worktree: HashMap<String, Vec<ChangeRow>> = HashMap::new();
            let mut rows = conn
                .query(
                    "SELECT j.worktree_path, fc.file_path, fc.additions, fc.deletions
                     FROM file_changes fc
                     JOIN jobs j ON fc.job_id = j.id
                     WHERE j.execution_id = ?1 AND j.worktree_path IS NOT NULL
                     ORDER BY fc.file_path ASC",
                    (execution_id.as_str(),),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                let worktree = row.text(0)?;
                by_worktree.entry(worktree).or_default().push((
                    row.text(1)?,
                    row.opt_i64(2)?.map(|v| v as i32),
                    row.opt_i64(3)?.map(|v| v as i32),
                ));
            }

            let mut out: HashMap<String, ChangeSummary> = HashMap::new();
            for (job_id, worktree) in owners {
                let summary = by_worktree
                    .get(&worktree)
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

/// Job + project coordinates needed to resolve a node's base..tip diff.
struct DiffCoords {
    worktree_path: Option<String>,
    execution_id: Option<String>,
    repo_path: String,
    default_branch: String,
    base_anchor: Option<String>,
}

async fn load_diff_coords(db: &LocalDb, job_id: &str) -> DbResult<Option<DiffCoords>> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.worktree_path, j.execution_id, p.repo_path, p.default_branch
                     FROM jobs j JOIN projects p ON j.project_id = p.id
                     WHERE j.id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let worktree_path = row.opt_text(0)?;
            let execution_id = row.opt_text(1)?;
            let repo_path = row.text(2)?;
            let default_branch = row.opt_text(3)?.unwrap_or_else(|| "main".to_string());

            // Base anchor: the fork point recorded on the earliest job sharing
            // this worktree (the inheriting children come later), mirroring how
            // archival picks the anchor at teardown.
            let base_anchor = match (&worktree_path, &execution_id) {
                (Some(worktree), Some(execution)) => {
                    let mut anchor_rows = conn
                        .query(
                            "SELECT base_commit, pack_anchor FROM jobs
                             WHERE execution_id = ?1 AND worktree_path = ?2
                             ORDER BY created_at ASC LIMIT 1",
                            (execution.as_str(), worktree.as_str()),
                        )
                        .await?;
                    match anchor_rows.next().await? {
                        Some(r) => r.opt_text(1)?.or(r.opt_text(0)?),
                        None => None,
                    }
                }
                _ => None,
            };

            Ok(Some(DiffCoords {
                worktree_path,
                execution_id,
                repo_path,
                default_branch,
                base_anchor,
            }))
        })
    })
    .await
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

/// Resolve and render a node's cumulative `base..tip` diff. Returns `Ok(None)`
/// when the node owns no worktree, or when neither a live worktree nor an
/// archived `execution_history` row can supply a base/tip pair — the facet's
/// presence signal (file_changes) still works in that case.
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
    let Some(worktree_path) = coords.worktree_path.clone() else {
        return Ok(None);
    };

    let worktree_exists = Path::new(&worktree_path).exists();
    let repo = Path::new(&coords.repo_path);

    let (store, base_hex, tip_hex) = if worktree_exists {
        let wt = Path::new(&worktree_path);
        // Agent worktrees are non-colocated jj workspaces (`.jj`, no `.git`); a
        // git command run inside one resolves repo state against an unrelated
        // repo up the directory tree. Resolve both base and tip jj-natively for
        // those, and keep the git path only for genuine plain-git worktrees.
        let is_jj = crate::jj::is_jj_dir(wt);
        let base = match coords.base_anchor.clone() {
            Some(base) => base,
            None if is_jj => {
                // A jj workspace captures its base anchor jj-natively at job
                // creation, so a missing anchor here is a degenerate state — and
                // the git merge-base fallback can't run in a `.jj`-only worktree.
                log::warn!("node diff: jj workspace {worktree_path} has no recorded base anchor");
                return Ok(None);
            }
            None => match merge_base_fallback(&worktree_path, &coords.default_branch) {
                Some(base) => base,
                None => return Ok(None),
            },
        };
        let tip = if is_jj {
            // `@-` is the latest sealed commit — the jj analogue of git HEAD.
            let jj = crate::jj::JjEnv::resolve(jj_binary_path, config_dir);
            match crate::jj::head_commit(&jj, wt) {
                Ok(sha) if !sha.trim().is_empty() => sha.trim().to_string(),
                Ok(_) => return Ok(None),
                Err(e) => {
                    log::warn!("node diff: jj head_commit failed for {worktree_path}: {e}");
                    return Ok(None);
                }
            }
        } else {
            match git_head(&worktree_path) {
                Some(sha) => sha,
                None => return Ok(None),
            }
        };
        let store =
            ObjectStore::new(repo, None).map_err(|e| format!("building live object store: {e}"))?;
        (store, base, tip)
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

    let base_oid =
        ObjectId::from_hex(base_hex.as_bytes()).map_err(|e| format!("invalid base sha: {e}"))?;
    let tip_oid =
        ObjectId::from_hex(tip_hex.as_bytes()).map_err(|e| format!("invalid tip sha: {e}"))?;

    let files = render_range_file_diffs(&store, &base_oid, &tip_oid)?;
    let total_additions = files.iter().map(|f| f.additions as i32).sum();
    let total_deletions = files.iter().map(|f| f.deletions as i32).sum();
    let commits_ahead = count_commits_ahead(&store, &base_oid, &tip_oid);

    Ok(Some(NodeDiff {
        files,
        commits_ahead,
        total_additions,
        total_deletions,
    }))
}

/// Count commits on the first-parent chain from `tip` back to (but not
/// including) `base`. Bounded so a missing base or a cycle can't spin.
fn count_commits_ahead(store: &ObjectStore, base: &oid, tip: &oid) -> i32 {
    use gix_object::{CommitRefIter, Kind as ObjectKind};
    const HASH_KIND: gix_hash::Kind = gix_hash::Kind::Sha1;
    const CAP: i32 = 100_000;

    let mut count = 0;
    let mut current = tip.to_owned();
    while current.as_ref() != base && count < CAP {
        let Some((kind, bytes)) = store.resolve_object(&current) else {
            break;
        };
        if kind != ObjectKind::Commit {
            break;
        }
        let Some(parent) = CommitRefIter::from_bytes(&bytes, HASH_KIND)
            .parent_ids()
            .next()
        else {
            break;
        };
        count += 1;
        current = parent;
    }
    count
}

fn git_head(worktree_path: &str) -> Option<String> {
    let output = crate::env::git()
        .args(["rev-parse", "HEAD"])
        .current_dir(worktree_path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Resolve a fork point against the default branch when no anchor was recorded.
fn merge_base_fallback(worktree_path: &str, default_branch: &str) -> Option<String> {
    for base_ref in [
        format!("origin/{default_branch}"),
        default_branch.to_string(),
    ] {
        let output = crate::env::git()
            .args(["merge-base", &base_ref, "HEAD"])
            .current_dir(worktree_path)
            .output()
            .ok()?;
        if output.status.success() {
            let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !sha.is_empty() {
                return Some(sha);
            }
        }
    }
    log::warn!("node diff: no base anchor or merge-base for worktree {worktree_path}");
    None
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
        use crate::archival::testutil::{commit_all, init_repo, write_file};
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

        /// Seed a project/execution plus an owner job and an inheriting child job
        /// sharing one worktree. `repo_path`/`worktree_path` and the base anchor
        /// are caller-supplied so a test can point them at a real git repo.
        #[allow(clippy::too_many_arguments)]
        async fn seed_worktree_group(
            db: &LocalDb,
            repo_path: &str,
            worktree_path: &str,
            base_anchor: Option<&str>,
        ) {
            let repo_path = repo_path.to_string();
            let worktree_path = worktree_path.to_string();
            let base_anchor = base_anchor.map(str::to_string);
            db.write(move |conn| {
                let repo_path = repo_path.clone();
                let worktree_path = worktree_path.clone();
                let base_anchor = base_anchor.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                         VALUES ('proj','ws','p','P',?1,'main',1,1)",
                        (repo_path.as_str(),),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                        (),
                    )
                    .await?;
                    // Owner job: earliest created, holds the base anchor.
                    conn.execute(
                        "INSERT INTO jobs(id, execution_id, project_id, parent_job_id, worktree_path, base_commit, pack_anchor, status, created_at, updated_at)
                         VALUES ('owner','exec','proj',NULL,?1,?2,?2,'complete',1,1)",
                        (worktree_path.as_str(), base_anchor.clone()),
                    )
                    .await?;
                    // Inheriting child: shares the worktree, created later.
                    conn.execute(
                        "INSERT INTO jobs(id, execution_id, project_id, parent_job_id, worktree_path, status, created_at, updated_at)
                         VALUES ('child','exec','proj','owner',?1,'complete',2,2)",
                        (worktree_path.as_str(),),
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
        async fn change_summary_aggregates_across_worktree_children() {
            let db = migrated_db().await;
            seed_worktree_group(&db, "/repo", "/wt", Some("base")).await;
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
            seed_worktree_group(&db, "/repo", "/wt", Some("base")).await;
            insert_file_change(&db, "fc1", "child", "a.rs", Some(3), Some(0)).await;

            let map = execution_change_summaries(&db, "exec").await.unwrap();
            assert_eq!(map.len(), 1, "one owner node");
            let owner = map.get("owner").expect("owner present");
            assert_eq!(owner.files_changed, 1);
            assert_eq!(owner.additions, Some(3));
            assert!(!map.contains_key("child"), "children are not owner nodes");
        }

        /// A live worktree resolves base = recorded anchor, tip = worktree HEAD,
        /// and renders the cumulative diff straight from the repo object store.
        #[tokio::test]
        async fn node_base_tip_diff_renders_live_worktree() {
            let temp = tempfile::tempdir().unwrap();
            let dir = temp.path();
            init_repo(dir);
            write_file(dir, "keep.txt", b"a\nb\n");
            let base = commit_all(dir, "base");
            write_file(dir, "keep.txt", b"a\nB\nc\n");
            write_file(dir, "added.txt", b"new\n");
            commit_all(dir, "work");

            let repo = dir.to_str().unwrap();
            let db = migrated_db().await;
            // repo and worktree are the same dir for the test; the worktree exists
            // so the live path is taken.
            seed_worktree_group(&db, repo, repo, Some(&base)).await;

            let diff = node_base_tip_diff(&db, "owner", "jj", dir)
                .await
                .unwrap()
                .expect("live diff present");
            let paths: Vec<&str> = diff.files.iter().map(|f| f.path.as_str()).collect();
            assert_eq!(paths, vec!["added.txt", "keep.txt"]);
            assert_eq!(diff.commits_ahead, 1);
            assert!(diff.total_additions >= 2);
        }

        #[tokio::test]
        async fn node_base_tip_diff_none_without_worktree() {
            let db = migrated_db().await;
            // Worktree path points nowhere on disk and there is no execution
            // history, so the patch body can't resolve.
            seed_worktree_group(&db, "/repo", "/nonexistent/wt", Some("base")).await;
            let diff = node_base_tip_diff(&db, "owner", "jj", std::path::Path::new("/tmp"))
                .await
                .unwrap();
            assert!(diff.is_none());
        }

        /// jj availability gate, mirroring the jj module tests: run only when a
        /// jj binary is resolvable via `CAIRN_JJ_BIN` or PATH `jj`.
        fn jj_bin() -> Option<String> {
            let bin = std::env::var("CAIRN_JJ_BIN")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "jj".to_string());
            crate::env::command(&bin)
                .arg("--version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
                .then_some(bin)
        }

        /// Regression for the git-in-jj-worktree hazard. A live NON-colocated jj
        /// workspace (`.jj`, no `.git`) must resolve its tip jj-natively (`@-`),
        /// not with `git rev-parse HEAD` run inside the worktree (which walks up
        /// to an unrelated repo). The previous code took the git path: in a
        /// `.jj`-only worktree that yields a foreign or unresolvable sha, so the
        /// renderer can't find the tip and the commit walk spins to its cap. The
        /// jj-native resolver renders the real `base..tip`.
        #[tokio::test]
        #[serial_test::serial(jj)]
        async fn node_base_tip_diff_renders_live_jj_workspace() {
            let Some(bin) = jj_bin() else {
                eprintln!("skipping node_base_tip_diff_renders_live_jj_workspace: jj not resolvable via CAIRN_JJ_BIN/PATH");
                return;
            };
            let home = tempfile::tempdir().unwrap();
            let proj = tempfile::tempdir().unwrap();
            let wts = tempfile::tempdir().unwrap();

            // Project git repo with a base commit — the ObjectStore backing.
            init_repo(proj.path());
            write_file(proj.path(), "shared.rs", b"base\n");
            let base = commit_all(proj.path(), "base");

            // Shared jj store over the project git, then a non-colocated workspace.
            let jj = crate::jj::JjEnv::resolve(&bin, home.path());
            let store = home.path().join("jj-stores").join("proj");
            crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
            let ws = wts.path().join("job");
            crate::jj::add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None)
                .unwrap();

            // The invariant that broke the git path: `.jj` present, no `.git`.
            assert!(ws.join(".jj").is_dir(), "workspace carries .jj");
            assert!(
                !ws.join(".git").exists(),
                "workspace is non-colocated (no .git)"
            );

            // A fresh workspace's @- is the base; seal a change to advance the tip.
            assert_eq!(crate::jj::head_commit(&jj, &ws).unwrap(), base);
            std::fs::write(ws.join("added.rs"), "new\n").unwrap();
            crate::jj::seal(&jj, &ws, "work", None).unwrap();

            let db = migrated_db().await;
            // repo_path is the project git (the store backing); the worktree is
            // the live `.jj`-only workspace; the base anchor is the fork point.
            seed_worktree_group(
                &db,
                proj.path().to_str().unwrap(),
                ws.to_str().unwrap(),
                Some(&base),
            )
            .await;

            let diff = node_base_tip_diff(&db, "owner", &bin, home.path())
                .await
                .unwrap()
                .expect("live jj diff present");
            let paths: Vec<&str> = diff.files.iter().map(|f| f.path.as_str()).collect();
            assert_eq!(
                paths,
                vec!["added.rs"],
                "the sealed addition is in the diff"
            );
            assert_eq!(diff.commits_ahead, 1);
            assert!(diff.total_additions >= 1);
        }
    }
}
