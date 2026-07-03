//! Background worktree garbage collection.
//!
//! `~/.cairn/worktrees` accumulates disk that teardown never reclaims: worktrees
//! whose issue reached a terminal state while the app was closed (so no teardown
//! fired), teardowns that failed mid-delete, and legacy debris whose job/issue
//! rows are long gone. This GC is the backstop. It runs at startup and on a ~24h
//! timer (see [`crate::orchestrator::Orchestrator::spawn_worktree_gc`]) in two
//! passes:
//!
//! 1. **DB pass** — reclaim worktrees of jobs whose issue is terminal (any status
//!    other than `active`/`waiting`, which catches `merged`/`closed`/`backlog`
//!    and legacy values) or whose issue row is gone, idle since before the
//!    `orphan_cleanup_days` cutoff. Runs over EVERY open database (the private DB
//!    and each team replica) so terminal team worktrees are reclaimed too, and
//!    executes each target against its owning DB. Executed through the shared
//!    per-target helper, so events are archived first — exactly as a real
//!    teardown would.
//! 2. **Filesystem pass** — remove directories under the worktrees base that NO
//!    job row references (legacy debris) plus `*.trash-*` tombstones the robust
//!    remover left behind, idle since before the cutoff. Each swept dir's parent
//!    repo (read from its `.git` file) gets a `git worktree prune` afterwards, so
//!    the fs-only delete does not strand a `.git/worktrees/<name>` registration.
//!    **Gated to the canonical instance** (`config_dir` == `~/.cairn`, not
//!    `~/.cairn-dev`): the base dir is shared across instances, and a dev DB does
//!    not know about production jobs, so an ungated dev sweep would delete live
//!    production worktrees. The pass also reclaims stale incremental-compile
//!    caches inside the worktrees that survive: fenced agent builds keep
//!    `CARGO_INCREMENTAL` on for iteration speed (see
//!    [`crate::config::build_services::default_sccache_service`]), and deleting
//!    `src-tauri/target/*/incremental` dirs idle past
//!    `INCREMENTAL_MAX_IDLE_SECS` bounds the resulting disk growth.
//!
//! The GC never touches branches — branch-deletion policy lives solely in
//! [`crate::execution::teardown::teardown_worktrees`], which is landed-aware.

use crate::execution::teardown::{
    self, execute_target_cleanup, group_into_targets, worktrees_base_dir, TeardownTarget,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use turso::params;

/// Run one full GC pass: the DB pass, then the (canonical-instance-gated)
/// filesystem pass. Best-effort throughout; a failed pass logs and is retried on
/// the next tick.
pub(crate) async fn run_worktree_gc(orch: &Orchestrator) {
    let days = crate::config::settings::load_settings(&orch.config_dir)
        .orphan_cleanup_days
        .max(0) as i64;
    let cutoff = chrono::Utc::now().timestamp() - days * 24 * 60 * 60;

    // DB pass over EVERY open database — the private DB and each open team
    // replica. A team project's jobs, issues, and events live wholly in its
    // owning replica (see `execution::routing`), so a local-only pass would never
    // reclaim terminal team worktrees — and the filesystem pass cannot compensate
    // because it treats a still-present job row as "referenced". Each target is
    // executed against the very DB that produced it so archival writes to the
    // owning store.
    let mut reclaimed = 0usize;
    for db in orch.db.all_dbs().await {
        match plan_gc_targets(&db, cutoff).await {
            Ok(targets) => {
                for target in &targets {
                    execute_target_cleanup(orch, &db, target).await;
                }
                reclaimed += targets.len();
            }
            Err(e) => log::warn!("Worktree GC: DB pass planning failed: {e}"),
        }
    }
    if reclaimed > 0 {
        log::info!("Worktree GC: DB pass reclaimed {reclaimed} worktree target(s)");
        emit_jobs_change(orch);
    }

    fs_pass(orch, cutoff).await;
}

/// Plan DB-pass targets: distinct worktree paths of jobs whose issue is terminal
/// or gone, idle since before `cutoff`. Mirrors `plan_teardown`'s grouping so the
/// shared per-target executor applies unchanged.
///
/// A `LEFT JOIN` on issues keeps jobs whose issue row was deleted (`i.status IS
/// NULL`) and jobs with a `NULL issue_id`, so orphaned rows are swept too.
pub(crate) async fn plan_gc_targets(
    db: &LocalDb,
    cutoff: i64,
) -> Result<Vec<TeardownTarget>, String> {
    let rows: Vec<(String, String, Option<String>, String)> = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.id, j.worktree_path, j.branch, p.repo_path
                         FROM jobs j
                         JOIN projects p ON j.project_id = p.id
                         LEFT JOIN issues i ON j.issue_id = i.id
                         WHERE j.worktree_path IS NOT NULL
                           AND j.updated_at < ?1
                           AND (i.status IS NULL OR i.status NOT IN ('active', 'waiting'))",
                        params![cutoff],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.text(1)?, row.opt_text(2)?, row.text(3)?));
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to load GC candidates: {e}"))?;
    Ok(group_into_targets(rows))
}

/// Filesystem pass — remove unreferenced dirs and `*.trash-*` tombstones under the
/// worktrees base, idle since before `cutoff`. Gated to the canonical instance.
async fn fs_pass(orch: &Orchestrator, cutoff: i64) {
    if !is_canonical_instance(&orch.config_dir) {
        log::debug!(
            "Worktree GC: skipping filesystem pass on non-canonical instance {}",
            orch.config_dir.display()
        );
        return;
    }
    let base = match worktrees_base_dir() {
        Some(b) => b,
        None => return,
    };
    let referenced = load_referenced_paths(orch).await;
    let outcome = sweep_base_dir(&base, &referenced, cutoff);
    // The sweep deletes dirs with plain fs ops (it has no job row, so no
    // git-aware removal path), which leaves each dir's `.git/worktrees/<name>`
    // registration behind in its parent repo. Prune every repo a swept dir
    // pointed at so registrations don't accumulate and collide with future
    // `git worktree add` calls.
    for repo in &outcome.prune_repos {
        match orch.services.git.worktree_prune(repo) {
            Ok(()) => log::info!(
                "Worktree GC: pruned stale worktree registrations in {}",
                repo.display()
            ),
            Err(e) => log::warn!(
                "Worktree GC: git worktree prune failed for {}: {e}",
                repo.display()
            ),
        }
    }
    if outcome.removed > 0 {
        log::info!(
            "Worktree GC: filesystem pass removed {} director(ies)",
            outcome.removed
        );
        emit_jobs_change(orch);
    }

    // Incremental-cache reclaim. Fenced agent builds run with incremental
    // compilation ON (see `config::build_services::default_sccache_service` for
    // why), so an actively-iterated worktree accretes a large
    // `target/<profile>/incremental` tree. Job-referenced worktrees survive the
    // sweep above by design; this bounds their disk instead. Its cutoff is the
    // fixed `INCREMENTAL_MAX_IDLE_SECS`, not the settings-driven orphan cutoff —
    // the incremental cache is regenerable scratch, and the only cost of a
    // too-eager delete is one slower rebuild.
    let inc_cutoff = chrono::Utc::now().timestamp() - INCREMENTAL_MAX_IDLE_SECS;
    let reclaimed = reclaim_incremental_caches(&base, inc_cutoff);
    if reclaimed > 0 {
        log::info!("Worktree GC: reclaimed {reclaimed} stale incremental cache dir(s)");
    }
}

/// How long a worktree's `src-tauri/target/<profile>/incremental` dir may sit
/// idle before the filesystem pass reclaims it. 48 h: an actively-iterating
/// agent touches its incremental cache on every build, so a live edit-test loop
/// always stays fresh and survives, while a parked worktree pays at worst one
/// cold rebuild of its workspace crates if it ever resumes. Deliberately much
/// shorter than `orphan_cleanup_days` — the cache is regenerable scratch, not
/// the agent's work.
const INCREMENTAL_MAX_IDLE_SECS: i64 = 48 * 60 * 60;

/// Testable core of the incremental-cache reclaim: for every worktree dir under
/// `base`, delete each `src-tauri/target/<profile>/incremental` directory idle
/// since before `cutoff` (unix secs). Plain `remove_dir_all` — unlike swept
/// worktrees, there is no `.git` registration or job row to respect, and the
/// contents are regenerable. Returns the number of dirs removed.
fn reclaim_incremental_caches(base: &Path, cutoff: i64) -> u32 {
    let worktrees = match std::fs::read_dir(base) {
        Ok(e) => e,
        Err(_) => return 0, // absent base already logged by the sweep
    };
    let mut removed = 0u32;
    for wt in worktrees.flatten() {
        let target = wt.path().join("src-tauri").join("target");
        let profiles = match std::fs::read_dir(&target) {
            Ok(e) => e,
            Err(_) => continue, // no Rust target tree in this worktree
        };
        for profile in profiles.flatten() {
            let inc = profile.path().join("incremental");
            if !inc.is_dir() || !incremental_idle_before(&inc, cutoff) {
                continue;
            }
            match std::fs::remove_dir_all(&inc) {
                Ok(()) => {
                    log::info!("Worktree GC: reclaimed incremental cache {}", inc.display());
                    removed += 1;
                }
                Err(e) => log::warn!(
                    "Worktree GC: failed to reclaim incremental cache {}: {e}",
                    inc.display()
                ),
            }
        }
    }
    removed
}

/// Whether an incremental dir has been idle since before `cutoff`: the newest of
/// its own mtime and its immediate children's. rustc writes new session dirs
/// *inside* the per-crate subdirs on every rebuild, so the top dir's own mtime
/// can look stale mid-iteration; one level down is where liveness shows.
/// Conservative like [`dir_idle_before`]: no resolvable mtime keeps the dir.
fn incremental_idle_before(inc: &Path, cutoff: i64) -> bool {
    let mut newest = mtime_secs(inc);
    if let Ok(entries) = std::fs::read_dir(inc) {
        for child in entries.flatten() {
            newest = newest.max(mtime_secs(&child.path()));
        }
    }
    matches!(newest, Some(m) if m < cutoff)
}

/// Result of one filesystem sweep: how many dirs were removed, and the parent
/// repos whose `.git/worktrees` registrations now need pruning.
#[derive(Debug, Default)]
struct SweepOutcome {
    removed: u32,
    /// Parent repos of removed dirs that were linked git worktrees (derived from
    /// each dir's `.git` file before deletion). `BTreeSet` for deterministic
    /// order in logs and tests.
    prune_repos: BTreeSet<PathBuf>,
}

/// Testable core of the filesystem pass: scan `base`, remove each directory that
/// no job references (or any `*.trash-*` tombstone) once it is idle past
/// `cutoff`. Removal goes through the same guarded rename-then-delete the live
/// remover uses. Collects (but does not run) the parent repos to prune — the
/// caller owns the git side.
fn sweep_base_dir(base: &Path, referenced: &HashSet<PathBuf>, cutoff: i64) -> SweepOutcome {
    let entries = match std::fs::read_dir(base) {
        Ok(e) => e,
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                log::warn!(
                    "Worktree GC: cannot read worktrees base {}: {e}",
                    base.display()
                );
            }
            return SweepOutcome::default();
        }
    };

    let mut outcome = SweepOutcome::default();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let is_tombstone = name.to_string_lossy().contains(".trash-");

        // A directory a job still references is teardown/DB-pass territory; leave
        // it. Tombstones carry a renamed name and are never referenced, so they
        // bypass this check and are always sweepable.
        if !is_tombstone && referenced.contains(&path_key(&path)) {
            continue;
        }
        if !dir_idle_before(&path, cutoff) {
            continue;
        }
        // Read the parent repo BEFORE deletion — the `.git` file goes with the dir.
        let parent_repo = parent_repo_of_git_worktree(&path);
        match teardown::remove_dir_tombstoned(&path, base) {
            Ok(()) => {
                log::info!(
                    "Worktree GC: removed {} ({})",
                    path.display(),
                    if is_tombstone {
                        "tombstone"
                    } else {
                        "unreferenced"
                    }
                );
                outcome.removed += 1;
                if let Some(repo) = parent_repo {
                    outcome.prune_repos.insert(repo);
                }
            }
            Err(e) => log::warn!("Worktree GC: failed to remove {}: {e}", path.display()),
        }
    }
    outcome
}

/// The parent repository a linked git worktree belongs to, derived from its
/// `.git` file (`gitdir: <repo>/.git/worktrees/<name>`). `None` for jj
/// workspaces (which have no `.git` file), half-deleted dirs, and anything else
/// without that shape — including a worktree of a bare repo, whose admin dir
/// does not sit under a `.git` directory we can prune from a checkout path.
fn parent_repo_of_git_worktree(dir: &Path) -> Option<PathBuf> {
    let contents = std::fs::read_to_string(dir.join(".git")).ok()?;
    let admin = Path::new(contents.strip_prefix("gitdir:")?.trim());
    // admin = <repo>/.git/worktrees/<name>
    let worktrees = admin.parent()?;
    if worktrees.file_name()? != "worktrees" {
        return None;
    }
    let git_dir = worktrees.parent()?;
    if git_dir.file_name()? != ".git" {
        return None;
    }
    git_dir.parent().map(Path::to_path_buf)
}

/// The canonical (production) instance keys its config dir at `~/.cairn`; a dev
/// instance uses `~/.cairn-dev`. Only the canonical instance owns the shared
/// `~/.cairn/worktrees` filesystem sweep — a dev DB does not know about production
/// jobs, so without this gate it would delete live production worktrees as
/// "unreferenced".
fn is_canonical_instance(config_dir: &Path) -> bool {
    config_dir.file_name().and_then(|n| n.to_str()) == Some(".cairn")
}

/// Every worktree path any job row references, canonicalized for symlink-stable
/// comparison. A directory in this set is owned by a job and off-limits to the
/// filesystem pass. Unions across the private DB **and every open team replica**
/// (the worktrees base is shared, and a team project's job rows live in its
/// synced replica) so an active team worktree is never mistaken for debris.
async fn load_referenced_paths(orch: &Orchestrator) -> HashSet<PathBuf> {
    let mut referenced = HashSet::new();
    for db in orch.db.all_dbs().await {
        let rows: Vec<String> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT DISTINCT worktree_path FROM jobs WHERE worktree_path IS NOT NULL",
                            (),
                        )
                        .await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push(row.text(0)?);
                    }
                    Ok(out)
                })
            })
            .await
            .unwrap_or_default();
        referenced.extend(rows.iter().map(|p| path_key(Path::new(p))));
    }
    referenced
}

/// Canonicalize a path for comparison, falling back to the raw path when it does
/// not exist (a stored path whose dir is already gone).
fn path_key(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

/// Whether a directory has been idle since before `cutoff` (unix secs). Uses the
/// most-recent of the dir's own mtime and `.jj/working_copy`'s mtime — jj rewrites
/// the latter on every snapshot, so it is a good liveness signal for an active
/// workspace even when the root mtime is stale. Conservative: if no mtime
/// resolves at all, returns false (keep the dir).
fn dir_idle_before(path: &Path, cutoff: i64) -> bool {
    let newest = [path.to_path_buf(), path.join(".jj").join("working_copy")]
        .iter()
        .filter_map(|p| mtime_secs(p))
        .max();
    matches!(newest, Some(m) if m < cutoff)
}

fn mtime_secs(path: &Path) -> Option<i64> {
    std::fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
}

fn emit_jobs_change(orch: &Orchestrator) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso::params;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("worktree-gc-test.db").await
    }

    async fn seed_project(db: &LocalDb) {
        db.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1', 'W', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-1', 'w-1', 'P', 'CAIRN', '/tmp/CAIRN', 1, 1)",
            (),
        )
        .await
        .unwrap();
    }

    async fn seed_issue(db: &LocalDb, id: &str, number: i32, status: &str) {
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status,
                 progress, attention, priority, created_at, updated_at)
             VALUES (?1, 'p-1', ?2, 'T', '', ?3, 'active', 'none', 0, 1, 1)",
            params![id, number, status],
        )
        .await
        .unwrap();
    }

    async fn seed_job(
        db: &LocalDb,
        id: &str,
        issue_id: Option<&str>,
        worktree_path: &str,
        updated_at: i64,
    ) {
        db.execute(
            "INSERT INTO jobs (id, status, project_id, issue_id, worktree_path, branch,
                 created_at, updated_at)
             VALUES (?1, 'complete', 'p-1', ?2, ?3, 'agent/x', 1, ?4)",
            params![id, issue_id, worktree_path, updated_at],
        )
        .await
        .unwrap();
    }

    fn planned_paths(targets: &[TeardownTarget]) -> Vec<String> {
        let mut p: Vec<String> = targets.iter().map(|t| t.worktree_path.clone()).collect();
        p.sort();
        p
    }

    /// The DB pass plans only inactive-or-orphaned worktrees idle past the cutoff.
    #[tokio::test]
    async fn plan_gc_targets_selects_inactive_and_orphaned_past_cutoff() {
        let db = migrated_db().await;
        seed_project(&db).await;
        // cutoff = 1000: a job is eligible only with updated_at < 1000.
        let cutoff = 1000;
        let old = 500;
        let recent = 2000;

        seed_issue(&db, "i-active", 1, "active").await;
        seed_job(&db, "j-active", Some("i-active"), "/wt/active", old).await;

        seed_issue(&db, "i-waiting", 2, "waiting").await;
        seed_job(&db, "j-waiting", Some("i-waiting"), "/wt/waiting", old).await;

        seed_issue(&db, "i-merged", 3, "merged").await;
        seed_job(&db, "j-merged", Some("i-merged"), "/wt/merged", old).await;

        seed_issue(&db, "i-closed", 4, "closed").await;
        seed_job(&db, "j-closed", Some("i-closed"), "/wt/closed", old).await;

        seed_issue(&db, "i-failed", 5, "failed").await; // legacy status
        seed_job(&db, "j-failed", Some("i-failed"), "/wt/failed", old).await;

        // Orphaned: no issue row referenced.
        seed_job(&db, "j-orphan", None, "/wt/orphan", old).await;

        // Merged but touched recently — not yet idle past the cutoff.
        seed_issue(&db, "i-recent", 6, "merged").await;
        seed_job(&db, "j-recent", Some("i-recent"), "/wt/recent", recent).await;

        let targets = plan_gc_targets(&db, cutoff).await.unwrap();
        assert_eq!(
            planned_paths(&targets),
            vec!["/wt/closed", "/wt/failed", "/wt/merged", "/wt/orphan"],
        );
    }

    #[test]
    fn canonical_instance_gate() {
        assert!(is_canonical_instance(Path::new("/Users/x/.cairn")));
        assert!(!is_canonical_instance(Path::new("/Users/x/.cairn-dev")));
    }

    fn future_cutoff() -> i64 {
        chrono::Utc::now().timestamp() + 24 * 60 * 60
    }

    #[test]
    fn sweep_removes_unreferenced_dir() {
        let base = tempfile::tempdir().unwrap();
        let unref = base.path().join("CAIRN-9-builder-0");
        std::fs::create_dir_all(&unref).unwrap();

        let outcome = sweep_base_dir(base.path(), &HashSet::new(), future_cutoff());
        assert_eq!(outcome.removed, 1);
        assert!(!unref.exists());
        // A dir with no `.git` file (a jj workspace, plain debris) has no parent
        // repo to prune.
        assert!(outcome.prune_repos.is_empty());
    }

    #[test]
    fn sweep_keeps_referenced_dir() {
        let base = tempfile::tempdir().unwrap();
        let referenced_dir = base.path().join("CAIRN-1-builder-0");
        std::fs::create_dir_all(&referenced_dir).unwrap();
        let referenced: HashSet<PathBuf> = [path_key(&referenced_dir)].into_iter().collect();

        let outcome = sweep_base_dir(base.path(), &referenced, future_cutoff());
        assert_eq!(outcome.removed, 0);
        assert!(referenced_dir.exists(), "a job-referenced dir is untouched");
    }

    #[test]
    fn sweep_keeps_young_dir() {
        let base = tempfile::tempdir().unwrap();
        let young = base.path().join("CAIRN-2-builder-0");
        std::fs::create_dir_all(&young).unwrap();

        // cutoff at the epoch: nothing is idle before it, so a fresh dir stays.
        let outcome = sweep_base_dir(base.path(), &HashSet::new(), 0);
        assert_eq!(outcome.removed, 0);
        assert!(young.exists());
    }

    #[test]
    fn sweep_removes_leftover_tombstone() {
        let base = tempfile::tempdir().unwrap();
        let tombstone = base.path().join("CAIRN-3-builder-0.trash-42");
        std::fs::create_dir_all(&tombstone).unwrap();

        let outcome = sweep_base_dir(base.path(), &HashSet::new(), future_cutoff());
        assert_eq!(outcome.removed, 1);
        assert!(!tombstone.exists());
    }

    /// Sweeping a linked git worktree (its `.git` file names the parent repo's
    /// admin dir) collects that repo for a post-sweep `git worktree prune`, so
    /// the fs-only delete does not strand the registration.
    #[test]
    fn sweep_collects_parent_repo_of_git_worktree_for_prune() {
        let base = tempfile::tempdir().unwrap();
        let wt = base.path().join("CAIRN-4-builder-0");
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(
            wt.join(".git"),
            "gitdir: /repos/cairn/.git/worktrees/CAIRN-4-builder-0\n",
        )
        .unwrap();

        let outcome = sweep_base_dir(base.path(), &HashSet::new(), future_cutoff());
        assert_eq!(outcome.removed, 1);
        assert!(!wt.exists());
        assert_eq!(
            outcome.prune_repos.iter().collect::<Vec<_>>(),
            vec![&PathBuf::from("/repos/cairn")]
        );
    }

    #[test]
    fn reclaim_removes_idle_incremental_dirs_only() {
        let base = tempfile::tempdir().unwrap();
        let debug = base.path().join("CAIRN-5-builder-0/src-tauri/target/debug");
        let release = base
            .path()
            .join("CAIRN-5-builder-0/src-tauri/target/release");
        std::fs::create_dir_all(debug.join("incremental")).unwrap();
        std::fs::create_dir_all(debug.join("deps")).unwrap();
        std::fs::create_dir_all(release.join("incremental")).unwrap();

        let removed = reclaim_incremental_caches(base.path(), future_cutoff());
        assert_eq!(removed, 2, "one per profile dir");
        assert!(!debug.join("incremental").exists());
        assert!(!release.join("incremental").exists());
        // Only the incremental caches go; the rest of target/ and the worktree stay.
        assert!(debug.join("deps").exists());
    }

    #[test]
    fn reclaim_keeps_fresh_incremental_dir() {
        let base = tempfile::tempdir().unwrap();
        let inc = base
            .path()
            .join("CAIRN-6-builder-0/src-tauri/target/debug/incremental");
        std::fs::create_dir_all(&inc).unwrap();

        // cutoff at the epoch: nothing is idle before it, so a fresh cache stays.
        assert_eq!(reclaim_incremental_caches(base.path(), 0), 0);
        assert!(inc.exists());
    }

    /// A rebuild touches the per-crate subdirs inside `incremental/`, not the top
    /// dir itself — a stale top-dir mtime with a fresh child is an actively
    /// iterating agent, and its cache must survive.
    #[test]
    fn reclaim_keeps_incremental_dir_with_fresh_child() {
        let base = tempfile::tempdir().unwrap();
        let inc = base
            .path()
            .join("CAIRN-7-builder-0/src-tauri/target/debug/incremental");
        let crate_dir = inc.join("cairn_core-abc123");
        std::fs::create_dir_all(&crate_dir).unwrap();
        // Age the top dir well past any cutoff; the crate subdir stays fresh.
        filetime::set_file_mtime(&inc, filetime::FileTime::from_unix_time(1, 0)).unwrap();

        let now = chrono::Utc::now().timestamp();
        assert!(!incremental_idle_before(&inc, now - 60));
        assert_eq!(reclaim_incremental_caches(base.path(), now - 60), 0);
        assert!(inc.exists());
    }

    #[test]
    fn parent_repo_parses_gitdir_file_and_rejects_other_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let wt = dir.path().join("wt");
        std::fs::create_dir_all(&wt).unwrap();

        // No `.git` file at all (a jj workspace).
        assert_eq!(parent_repo_of_git_worktree(&wt), None);

        // The canonical linked-worktree shape resolves to the checkout root.
        std::fs::write(wt.join(".git"), "gitdir: /r/x/.git/worktrees/wt\n").unwrap();
        assert_eq!(
            parent_repo_of_git_worktree(&wt),
            Some(PathBuf::from("/r/x"))
        );

        // A gitdir that is not a `.git/worktrees/<name>` admin path is rejected.
        std::fs::write(wt.join(".git"), "gitdir: /somewhere/else\n").unwrap();
        assert_eq!(parent_repo_of_git_worktree(&wt), None);

        // A submodule-style gitdir (`.git/modules/<name>`) is rejected too.
        std::fs::write(wt.join(".git"), "gitdir: /r/x/.git/modules/wt\n").unwrap();
        assert_eq!(parent_repo_of_git_worktree(&wt), None);
    }
}
