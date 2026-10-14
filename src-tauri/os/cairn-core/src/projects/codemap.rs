//! The project code map: one base-branch tree's source inventory, the import
//! graph over it, and the churn each file carried.
//!
//! Two different kinds of fact meet here, and keeping them apart is the whole
//! design.
//!
//! **The tree projection is a function of one commit.** The file inventory and
//! the import graph depend on nothing but the tree, so they are computed once
//! per base commit and cached under that commit's SHA. A base that advances and
//! then reverts finds its map already computed, and nothing but the base moving
//! can make a stored row wrong. Because the key is a commit, the walk must see
//! that commit: see [`CommitCheckout`], which materializes it rather than
//! trusting the project's live checkout, which may sit on another branch, carry
//! uncommitted edits, or hold untracked files.
//!
//! **Churn is a function of the clock as well as the tree.** Per-file merged
//! activity over a trailing window yields different numbers tomorrow for the
//! same commit, so it is deliberately NOT cached: baking it into a commit-keyed
//! row would leave a project whose base never moves serving month-old activity,
//! with no event able to invalidate it. It is joined from `file_changes` on
//! every read instead -- one indexed aggregate, cheap enough that caching it
//! would buy nothing but staleness.
//!
//! Three inputs, each already owned by something else:
//!
//! * the source inventory and the import edges come from `cairn-symbols`, whose
//!   gitignore-respecting walk and bundled tree-sitter grammars parse the tree
//!   in one pass (see [`cairn_symbols::symbols::dependency::import_graph`]);
//! * the base commit comes from the project's own checkout, resolved through the
//!   git service;
//! * churn comes from `file_changes`, which already records per-file additions
//!   and deletions for merged work. Churn is therefore a database sum, never a
//!   `git log` — the history walk this would otherwise need is the one cost that
//!   would make a map too expensive to keep current.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use cairn_symbols::symbols::dependency::import_graph;

use crate::orchestrator::Orchestrator;
use crate::services::GitClient;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

/// Trailing window over which merged work counts toward a file's churn. Churn is
/// meant to say "what is moving now", so it is deliberately a window and not the
/// whole history: a file rewritten two years ago and untouched since is settled,
/// not hot.
pub const CHURN_WINDOW_DAYS: i64 = 60;

/// How many times one refresh pass recomputes before it stops chasing a base
/// that keeps advancing underneath it. The next read arms a fresh pass, so the
/// bound costs at most one stale-flagged read, never a lost map.
const MAX_REFRESH_ROUNDS: usize = 5;

/// One base tree's code map, as stored and as served.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeMap {
    /// The default-branch commit this map projects.
    pub base_commit_sha: String,
    /// Unix seconds at which the walk ran.
    pub computed_at: i64,
    pub files: Vec<CodeMapFile>,
    /// `[from, to]` worktree-relative import edges, deduplicated and sorted, so
    /// the payload is byte-stable for the same tree.
    pub imports: Vec<(String, String)>,
}

/// One tracked source file: what it is, how big it is, and how much merged work
/// has touched it inside the churn window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeMapFile {
    /// Worktree-relative path with forward slashes.
    pub path: String,
    /// Lowercased grammar name — `rust`, `typescript`, `tsx`, `python`.
    pub language: String,
    pub line_count: u32,
    pub size_bytes: u64,
    pub churn_additions: i64,
    pub churn_deletions: i64,
}

/// What the cache stores: everything that is a function of the base commit
/// alone, and nothing that is a function of the clock.
///
/// This is deliberately not [`CodeMap`]. The served payload carries churn; a
/// stored row must not, or the row would go quietly wrong as time passed with
/// nothing able to invalidate it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TreeProjection {
    base_commit_sha: String,
    computed_at: i64,
    files: Vec<TreeFile>,
    imports: Vec<(String, String)>,
}

/// One tracked source file as the tree alone describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TreeFile {
    path: String,
    language: String,
    line_count: u32,
    size_bytes: u64,
}

/// Fill in each file's churn, producing the payload a reader gets.
fn join_churn(tree: TreeProjection, churn: &HashMap<String, (i64, i64)>) -> CodeMap {
    CodeMap {
        base_commit_sha: tree.base_commit_sha,
        computed_at: tree.computed_at,
        files: tree
            .files
            .into_iter()
            .map(|file| {
                let (churn_additions, churn_deletions) =
                    churn.get(&file.path).copied().unwrap_or((0, 0));
                CodeMapFile {
                    path: file.path,
                    language: file.language,
                    line_count: file.line_count,
                    size_bytes: file.size_bytes,
                    churn_additions,
                    churn_deletions,
                }
            })
            .collect(),
        imports: tree.imports,
    }
}

/// One `codemap_cache` row as the read path sees it.
pub(crate) struct CachedCodeMap {
    pub base_commit_sha: String,
    pub computed_at: i64,
    /// Set when the project's base moved past this row. A stale row still
    /// serves — a map of the previous base answers better than nothing while the
    /// recompute runs — but it serves flagged.
    pub stale: bool,
    pub file_count: i64,
    pub edge_count: i64,
    pub codemap_json: String,
}

const CACHE_COLUMNS: &str =
    "base_commit_sha, computed_at, invalidated_at, file_count, edge_count, codemap_json";

fn row_to_cached(row: &cairn_db::turso::Row) -> cairn_db::storage::DbResult<CachedCodeMap> {
    Ok(CachedCodeMap {
        base_commit_sha: row.text(0)?,
        computed_at: row.i64(1)?,
        stale: row.opt_i64(2)?.is_some(),
        file_count: row.i64(3)?,
        edge_count: row.i64(4)?,
        codemap_json: row.text(5)?,
    })
}

/// The map stored for exactly this base commit, stale or not.
pub(crate) async fn load_at_commit(
    db: &LocalDb,
    project_id: &str,
    base_commit_sha: &str,
) -> Result<Option<CachedCodeMap>, String> {
    db.query_opt(
        format!("SELECT {CACHE_COLUMNS} FROM codemap_cache WHERE project_id = ?1 AND base_commit_sha = ?2"),
        params![project_id, base_commit_sha],
        row_to_cached,
    )
    .await
    .map_err(|error| format!("Failed to load code map: {error}"))
}

/// The newest map this project has at any base, which is what a read falls back
/// to while the map for the current base is still being computed.
pub(crate) async fn load_newest(
    db: &LocalDb,
    project_id: &str,
) -> Result<Option<CachedCodeMap>, String> {
    db.query_opt(
        format!(
            "SELECT {CACHE_COLUMNS} FROM codemap_cache WHERE project_id = ?1 \
             ORDER BY computed_at DESC LIMIT 1"
        ),
        params![project_id],
        row_to_cached,
    )
    .await
    .map_err(|error| format!("Failed to load code map: {error}"))
}

/// Store a freshly computed map, replacing any row for the same base commit and
/// clearing its stale marker — a base that advances and then comes back finds
/// its map already computed.
async fn store(db: &LocalDb, project_id: &str, map: &TreeProjection) -> Result<(), String> {
    let json = serde_json::to_string(map)
        .map_err(|error| format!("Failed to serialize code map: {error}"))?;
    db.execute(
        "INSERT INTO codemap_cache (
             project_id, base_commit_sha, computed_at, invalidated_at,
             file_count, edge_count, codemap_json
         ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6)
         ON CONFLICT(project_id, base_commit_sha) DO UPDATE SET
             computed_at = excluded.computed_at,
             invalidated_at = NULL,
             file_count = excluded.file_count,
             edge_count = excluded.edge_count,
             codemap_json = excluded.codemap_json",
        params![
            project_id,
            map.base_commit_sha.as_str(),
            map.computed_at,
            map.files.len() as i64,
            map.imports.len() as i64,
            json.as_str()
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("Failed to store code map: {error}"))
}

/// Mark every map this project holds as one the base has moved past.
///
/// Invalidation does not delete: a stale map is still the best available answer
/// until its replacement exists, and deleting would turn every base advance into
/// a blank surface for the length of a walk.
pub(crate) async fn invalidate(db: &LocalDb, project_id: &str) -> Result<u64, String> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "UPDATE codemap_cache SET invalidated_at = ?2
         WHERE project_id = ?1 AND invalidated_at IS NULL",
        params![project_id, now],
    )
    .await
    .map_err(|error| format!("Failed to invalidate code map: {error}"))
}

/// Per-file merged churn inside the trailing window, keyed by worktree-relative
/// path.
///
/// `file_changes` rows are written when a pull request merges, so their presence
/// already means "this landed" and their `created_at` is the landing time. A file
/// that moved carries its churn under the new path only; the rows recording the
/// rename keep the old path, and attributing that history across the move would
/// need a history walk this projection deliberately avoids.
async fn load_churn(
    db: &LocalDb,
    project_id: &str,
    since_unix_seconds: i64,
) -> Result<HashMap<String, (i64, i64)>, String> {
    let rows = db
        .query_all(
            "SELECT fc.file_path,
                    COALESCE(SUM(fc.additions), 0),
                    COALESCE(SUM(fc.deletions), 0)
               FROM file_changes fc
               JOIN jobs j ON j.id = fc.job_id
              WHERE j.project_id = ?1 AND fc.created_at >= ?2
              GROUP BY fc.file_path",
            params![project_id, since_unix_seconds],
            |row| Ok((row.text(0)?, row.i64(1)?, row.i64(2)?)),
        )
        .await
        .map_err(|error| format!("Failed to load code map churn: {error}"))?;
    Ok(rows
        .into_iter()
        .map(|(path, additions, deletions)| (path, (additions, deletions)))
        .collect())
}

/// A throwaway checkout of exactly one commit, removed when it drops.
///
/// The cache key is a commit, so the walk has to see that commit and nothing
/// else. The project's own checkout cannot answer for it: it may sit on another
/// branch, carry uncommitted edits, or hold untracked source files, and any of
/// those would be stored permanently under a SHA that does not describe them,
/// then served again whenever the base returned to that SHA. A detached
/// worktree is the cheap way to get the real tree -- tracked files only, at
/// exactly this commit, with nothing of the working copy leaking in.
struct CommitCheckout {
    git: Arc<dyn GitClient>,
    repo: PathBuf,
    path: PathBuf,
}

impl CommitCheckout {
    fn create(git: Arc<dyn GitClient>, repo: &Path, commit: &str) -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!("cairn-codemap-{}", uuid::Uuid::new_v4()));
        let output = git.run(
            repo,
            vec![
                "worktree".to_string(),
                "add".to_string(),
                "--detach".to_string(),
                path.display().to_string(),
                commit.to_string(),
            ],
        )?;
        if !output.success {
            return Err(format!(
                "could not materialize {commit} for the code map: {}",
                output.stderr.trim()
            ));
        }
        Ok(Self {
            git,
            repo: repo.to_path_buf(),
            path,
        })
    }
}

impl Drop for CommitCheckout {
    fn drop(&mut self) {
        let path = self.path.display().to_string();
        let _ = self.git.run(
            &self.repo,
            vec![
                "worktree".to_string(),
                "remove".to_string(),
                "--force".to_string(),
                path,
            ],
        );
        // The registry entry is what actually matters to leave clean; the
        // directory is removed defensively in case the worktree command failed
        // partway and left it behind.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Materialize `base_commit_sha` and walk it into the projection cached under
/// that SHA.
///
/// Checkout and walk are both blocking and touch the filesystem for every
/// source file, so the whole thing runs on a blocking thread rather than
/// stalling the async runtime the read path shares. The checkout is reclaimed
/// when the guard drops, including on the error paths.
async fn compute_tree(
    orch: &Orchestrator,
    repo_root: &Path,
    base_commit_sha: &str,
) -> Result<TreeProjection, String> {
    let git = orch.services.git.clone();
    let repo = repo_root.to_path_buf();
    let commit = base_commit_sha.to_string();
    tokio::task::spawn_blocking(move || {
        let checkout = CommitCheckout::create(git, &repo, &commit)?;
        let graph = import_graph(&checkout.path);
        Ok(TreeProjection {
            base_commit_sha: commit,
            computed_at: chrono::Utc::now().timestamp(),
            files: graph
                .files
                .into_iter()
                .map(|file| TreeFile {
                    path: file.path,
                    language: file.language.to_string().to_lowercase(),
                    line_count: file.line_count,
                    size_bytes: file.size_bytes,
                })
                .collect(),
            imports: graph.edges,
        })
    })
    .await
    .map_err(|error| format!("code map walk task failed: {error}"))?
}

/// What a reader gets for one project right now: the best map available, and
/// whether the base has moved past it.
///
/// This is the one "serve the cache, arm a refresh" path. Both readers go
/// through it -- the `cairn://p/{project}/codemap` resource and the frontend's
/// `get_project_codemap` command -- so neither can drift into a different
/// answer about the same project.
pub struct CodeMapView {
    /// `None` while the first map for this project is still being computed.
    pub map: Option<CodeMap>,
    /// The map does not describe the current base and a recompute is running.
    pub stale: bool,
    /// The base commit the project is on now, when it could be resolved.
    pub head: Option<String>,
    /// Empty when the project has a repository checkout and a default branch.
    /// A project with neither has nothing to map, which is not an error.
    pub unmappable: bool,
}

/// Serve this project's code map from the cache, arming a background refresh
/// when what is stored does not describe the current base.
///
/// Never walks a tree and never blocks on one: the walk it arms runs detached,
/// and a stale map is returned flagged rather than withheld.
pub async fn current(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
) -> Result<CodeMapView, String> {
    let (repo_path, default_branch) = db
        .query_one(
            "SELECT COALESCE(repo_path, ''), COALESCE(default_branch, '') FROM projects WHERE id = ?1",
            params![project_id],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("Failed to load project repository: {error}"))?;
    if repo_path.is_empty() || default_branch.is_empty() {
        return Ok(CodeMapView {
            map: None,
            stale: false,
            head: None,
            unmappable: true,
        });
    }

    // A base commit that will not resolve is not fatal: whatever map the project
    // already has still describes it better than an error does.
    let head = head_commit(orch, Path::new(&repo_path), &default_branch)
        .await
        .ok();

    let current = match &head {
        Some(sha) => load_at_commit(db, project_id, sha)
            .await?
            .filter(|cached| !cached.stale),
        None => None,
    };
    let cached = match current {
        Some(cached) => Some(cached),
        // Nothing current: arm the recompute and fall back to whatever this
        // project last computed, at whatever base that was.
        None => {
            spawn_refresh(orch, project_id.to_string(), repo_path, default_branch);
            load_newest(db, project_id).await?
        }
    };

    let Some(cached) = cached else {
        return Ok(CodeMapView {
            map: None,
            stale: false,
            head,
            unmappable: false,
        });
    };
    // A row whose base is not the current head is stale whether or not an
    // invalidation marked it: the two say the same thing, and the head
    // comparison also catches an advance nothing observed.
    let stale = cached.stale
        || head
            .as_deref()
            .is_some_and(|sha| sha != cached.base_commit_sha);
    let tree: TreeProjection = serde_json::from_str(&cached.codemap_json)
        .map_err(|error| format!("Stored code map could not be decoded: {error}"))?;
    // Churn is joined here rather than stored, so the same commit reports the
    // activity that is true NOW rather than the activity that was true when its
    // tree happened to be walked.
    let churn = load_churn(
        db,
        project_id,
        chrono::Utc::now().timestamp() - CHURN_WINDOW_DAYS * 24 * 60 * 60,
    )
    .await?;
    let map = join_churn(tree, &churn);
    Ok(CodeMapView {
        map: Some(map),
        stale,
        head,
        unmappable: false,
    })
}

/// The commit `branch` points at in the project's own checkout.
pub(crate) async fn head_commit(
    orch: &Orchestrator,
    repo_root: &Path,
    branch: &str,
) -> Result<String, String> {
    let git = orch.services.git.clone();
    let repo = repo_root.to_path_buf();
    let branch = branch.to_string();
    let sha = tokio::task::spawn_blocking(move || git.rev_parse(&repo, vec![branch]))
        .await
        .map_err(|error| format!("code map head resolve task failed: {error}"))??;
    let sha = sha.trim().to_string();
    if sha.is_empty() {
        return Err("git rev-parse returned no commit".to_string());
    }
    Ok(sha)
}

/// Whether this refresh is the one that runs. At most one refresh per project is
/// in flight; a request that arrives while one is running is dropped rather than
/// queued, because the running pass re-reads the head after each round and so
/// already covers whatever moved.
fn claim(orch: &Orchestrator, project_id: &str) -> bool {
    orch.codemap_refresh_in_flight
        .lock()
        .unwrap()
        .insert(project_id.to_string())
}

fn release(orch: &Orchestrator, project_id: &str) {
    orch.codemap_refresh_in_flight
        .lock()
        .unwrap()
        .remove(project_id);
}

/// Bring this project's code map up to its current base, in the background.
///
/// Non-blocking and non-fatal by construction: every caller is either a read
/// serving a stale map or a base advance that has other work to do, and neither
/// should wait on a tree walk or fail because one failed.
pub(crate) fn spawn_refresh(
    orch: &Orchestrator,
    project_id: String,
    repo_path: String,
    default_branch: String,
) {
    if repo_path.is_empty() || default_branch.is_empty() {
        return;
    }
    if !claim(orch, &project_id) {
        return;
    }
    let orch = orch.clone();
    tokio::spawn(async move {
        let result = refresh(&orch, &project_id, &repo_path, &default_branch).await;
        release(&orch, &project_id);
        if let Err(error) = result {
            log::warn!("code map refresh for project {project_id} failed: {error}");
        }
    });
}

async fn refresh(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &str,
    default_branch: &str,
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let repo_root = PathBuf::from(repo_path);
    for _ in 0..MAX_REFRESH_ROUNDS {
        let sha = head_commit(orch, &repo_root, default_branch).await?;
        // The loop exits here on the round after a successful store, and
        // immediately when a concurrent pass already computed this base.
        if load_at_commit(&db, project_id, &sha)
            .await?
            .is_some_and(|cached| !cached.stale)
        {
            return Ok(());
        }
        // Walking the commit rather than the checkout is also what makes this
        // loop safe: a base that advances mid-walk cannot mix a newer tree into
        // a row keyed by the older SHA, so the worst case is one extra round.
        let tree = compute_tree(orch, &repo_root, &sha).await?;
        store(&db, project_id, &tree).await?;
    }
    log::warn!(
        "code map refresh for project {project_id} gave up after {MAX_REFRESH_ROUNDS} rounds; \
         the base advanced faster than the walk"
    );
    Ok(())
}

/// The project's base advanced: every stored map now describes a tree that is no
/// longer the base, and the replacement is worth computing before anyone asks.
pub(crate) async fn note_base_advance(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    repo_path: &str,
    default_branch: &str,
) {
    if let Err(error) = invalidate(db, project_id).await {
        log::warn!("code map invalidation for project {project_id} failed: {error}");
    }
    spawn_refresh(
        orch,
        project_id.to_string(),
        repo_path.to_string(),
        default_branch.to_string(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    fn tree() -> TreeProjection {
        TreeProjection {
            base_commit_sha: "abc123".to_string(),
            computed_at: 1_700_000_000,
            files: vec![
                TreeFile {
                    path: "src/lib.rs".to_string(),
                    language: "rust".to_string(),
                    size_bytes: 120,
                    line_count: 7,
                },
                TreeFile {
                    path: "web/app.tsx".to_string(),
                    language: "tsx".to_string(),
                    size_bytes: 40,
                    line_count: 3,
                },
            ],
            imports: vec![("web/app.tsx".to_string(), "src/lib.rs".to_string())],
        }
    }

    #[test]
    fn churn_joins_onto_the_walked_inventory_by_path() {
        let churn = HashMap::from([("src/lib.rs".to_string(), (30, 4))]);
        let map = join_churn(tree(), &churn);

        assert_eq!(map.base_commit_sha, "abc123");
        assert_eq!(map.computed_at, 1_700_000_000);
        assert_eq!(map.files[0].churn_additions, 30);
        assert_eq!(map.files[0].churn_deletions, 4);
        // A file no merged work touched inside the window reads as zero churn,
        // not as absent: it is still part of the tree.
        assert_eq!(map.files[1].churn_additions, 0);
        assert_eq!(map.files[1].churn_deletions, 0);
        assert_eq!(
            map.imports,
            vec![("web/app.tsx".to_string(), "src/lib.rs".to_string())]
        );
    }

    #[test]
    fn the_payload_serializes_edges_as_pairs() {
        let map = join_churn(tree(), &HashMap::new());
        let json = serde_json::to_string(&map).unwrap();
        assert_eq!(serde_json::from_str::<CodeMap>(&json).unwrap(), map);
        // Edges serialize as pairs, which is the shape the map surface lays out.
        assert!(json.contains(r#""imports":[["web/app.tsx","src/lib.rs"]]"#));
    }

    #[test]
    fn what_is_cached_carries_no_churn() {
        // The guard on the whole design: a row keyed by a commit must contain
        // nothing that the clock can change, or it goes wrong with no event able
        // to invalidate it.
        let stored = serde_json::to_string(&tree()).unwrap();
        assert!(!stored.contains("churn"), "{stored}");
        assert_eq!(
            serde_json::from_str::<TreeProjection>(&stored).unwrap(),
            tree()
        );
    }

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("codemap.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-map', 'default', 'Map', 'MAP', '/tmp/map', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db
    }

    fn map_at(sha: &str) -> TreeProjection {
        TreeProjection {
            base_commit_sha: sha.to_string(),
            ..tree()
        }
    }

    #[tokio::test]
    async fn a_stored_map_is_addressed_by_the_commit_it_projects() {
        let db = test_db().await;
        store(&db, "p-map", &map_at("aaa")).await.unwrap();

        let cached = load_at_commit(&db, "p-map", "aaa").await.unwrap().unwrap();
        assert!(!cached.stale);
        assert_eq!(cached.file_count, 2);
        assert_eq!(cached.edge_count, 1);
        assert_eq!(
            serde_json::from_str::<TreeProjection>(&cached.codemap_json).unwrap(),
            map_at("aaa")
        );
        // A base this project has never mapped has no row, which is what makes
        // the read path arm a compute rather than serve someone else's tree.
        assert!(load_at_commit(&db, "p-map", "bbb").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn invalidation_flags_the_stored_map_without_discarding_it() {
        let db = test_db().await;
        store(&db, "p-map", &map_at("aaa")).await.unwrap();

        assert_eq!(invalidate(&db, "p-map").await.unwrap(), 1);
        let cached = load_at_commit(&db, "p-map", "aaa").await.unwrap().unwrap();
        assert!(cached.stale);
        // Still serving: a map of the previous base beats a blank surface while
        // the recompute runs.
        assert_eq!(cached.file_count, 2);
        // Nothing left to invalidate on a second pass, so a storm of advances
        // does not keep rewriting rows.
        assert_eq!(invalidate(&db, "p-map").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn recomputing_the_same_base_clears_its_stale_marker() {
        let db = test_db().await;
        store(&db, "p-map", &map_at("aaa")).await.unwrap();
        invalidate(&db, "p-map").await.unwrap();

        store(&db, "p-map", &map_at("aaa")).await.unwrap();
        let cached = load_at_commit(&db, "p-map", "aaa").await.unwrap().unwrap();
        assert!(!cached.stale);
    }

    #[tokio::test]
    async fn the_newest_map_is_the_fallback_while_the_current_base_is_computing() {
        let db = test_db().await;
        let mut older = map_at("aaa");
        older.computed_at = 100;
        let mut newer = map_at("bbb");
        newer.computed_at = 200;
        store(&db, "p-map", &older).await.unwrap();
        store(&db, "p-map", &newer).await.unwrap();

        let cached = load_newest(&db, "p-map").await.unwrap().unwrap();
        assert_eq!(cached.base_commit_sha, "bbb");
        // Both bases keep their own row, so a base that advances and comes back
        // finds its map already computed.
        assert!(load_at_commit(&db, "p-map", "aaa").await.unwrap().is_some());
    }
}
