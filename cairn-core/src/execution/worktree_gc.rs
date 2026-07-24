//! Background worktree garbage collection.
//!
//! `~/.cairn/worktrees` accumulates disk that teardown never reclaims: worktrees
//! whose issue reached a terminal state while the app was closed (so no teardown
//! fired), teardowns that failed mid-delete, and legacy debris whose job/issue
//! rows are long gone. This GC is the backstop. It runs at startup and on a ~24h
//! timer (see [`crate::orchestrator::Orchestrator::spawn_worktree_gc`]) in three
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
//!    `target/*/incremental` dirs idle past `INCREMENTAL_MAX_IDLE_SECS` bounds
//!    the resulting disk growth. The same pass sweeps stale non-incremental
//!    Cargo `target/` artifacts in live worktrees after
//!    `TARGET_SWEEP_MAX_IDLE_SECS`; these are regenerable build products, so the
//!    worst case is one slower rebuild.
//!    Executor-owned build slots are outside this GC: their continuously hot
//!    check variants are instead bounded by the executor's per-slot target budget.
//! 3. **Repository target sweep** — opt-in cleanup of each project's main
//!    checkout `target/` dirs, controlled by `repo_target_sweep_days`. This uses
//!    the same marker-based discovery and freshness policy as the worktree target
//!    sweep, but defaults off because main checkouts are user-owned.
//!
//! The GC never touches branches — branch-deletion policy lives solely in
//! [`crate::execution::teardown::teardown_worktrees`], which is landed-aware.

use crate::execution::teardown::{
    self, execute_target_cleanup, group_into_targets, TeardownTarget,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::path::{Path, PathBuf};

const CACHEDIR_TAG: &str = "CACHEDIR.TAG";

/// Run one full GC pass: the DB pass, then the (canonical-instance-gated)
/// filesystem pass. Best-effort throughout; a failed pass logs and is retried on
/// the next tick.
pub(crate) async fn run_worktree_gc(orch: &Orchestrator) {
    let settings = crate::config::settings::load_settings(&orch.config_dir);
    let days = settings.orphan_cleanup_days.max(0) as i64;
    let cutoff = chrono::Utc::now().timestamp() - days * 24 * 60 * 60;

    // DB pass over EVERY open database — the private DB and each open team
    // replica. A team project's jobs, issues, and events live wholly in its
    // owning replica (see `execution::routing`), so a local-only pass would never
    // reclaim terminal team worktrees — and the filesystem pass cannot compensate
    // because it treats a still-present job row as "referenced". Each target is
    // executed against the very DB that produced it so archival writes to the
    // owning store.
    let mut reclaimed = 0usize;
    let mut gone_repos: BTreeSet<PathBuf> = BTreeSet::new();
    for db in orch.db.all_dbs().await {
        match plan_gc_targets(&db, &orch.db.local, cutoff).await {
            Ok(targets) => {
                let (live, gone) = partition_targets_by_disk_presence(targets);
                for target in &live {
                    execute_target_cleanup(orch, &db, target).await;
                }
                reclaimed += live.len();
                gone_repos.extend(gone);
            }
            Err(e) => log::warn!("Worktree GC: DB pass planning failed: {e}"),
        }
    }
    // A GC target whose worktree directory is already gone is a no-op for the
    // per-target cleanup — `archive_target` fails at once resolving its missing
    // HEAD and there is nothing on disk to remove — yet running the full
    // `execute_target_cleanup` still spawns a `jj forget` + `git worktree prune`
    // subprocess pair per target. With hundreds of terminal jobs pointing at
    // directories reclaimed long ago, every pass became a subprocess storm that
    // starved the runner's HTTP server. The gone targets are dropped from the
    // executed set above; here we run ONE `git worktree prune` per distinct repo
    // so their stale `.git/worktrees/<name>` registrations are still reclaimed, at
    // per-repo rather than per-target cost.
    for repo in &gone_repos {
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
    if reclaimed > 0 {
        log::info!("Worktree GC: DB pass reclaimed {reclaimed} worktree target(s)");
        emit_jobs_change(orch);
    }

    fs_pass(orch, cutoff).await;
    repo_target_sweep(orch, settings.repo_target_sweep_days).await;
}

/// Plan DB-pass targets: distinct worktree paths of jobs whose issue is terminal
/// or gone, idle since before `cutoff`. Mirrors `plan_teardown`'s grouping so the
/// shared per-target executor applies unchanged.
///
/// A `LEFT JOIN` on issues keeps jobs whose issue row was deleted (`i.status IS
/// NULL`) and jobs with a `NULL issue_id`, so orphaned rows are swept too. It
/// ALSO reclaims a terminal ephemeral-owner task job
/// (`owns_ephemeral_worktree = 1`) regardless of its issue status: an ambient
/// Manager issue is long-lived and never terminalizes, so without this backstop
/// an ephemeral task worktree would leak whenever the app died between task
/// completion and the detached finalize reclaim. The idle cutoff still guards it.
///
/// Restartable-workflow exception (mirrors the finalize reclaim guard): a
/// terminal ephemeral-owner **workflow** job keeps its `workflow_run` record
/// across every restartable state (stop / fail / crash), and `restart_workflow`
/// respawns into the worktree's persisted `working_dir` — so its worktree must
/// survive while that record lives, or GC would strand a still-restartable
/// workflow (the delayed form of the earlier finalize bug). Records live ONLY in
/// the private DB (`local_db`) even when the job row is in a team replica, so the
/// protected set is loaded from `local_db` and candidates are filtered against it
/// here. Once the record is dropped (clean completion or the redispatch sweep),
/// GC reclaims the job as the intended stray backstop.
async fn plan_gc_targets(
    db: &LocalDb,
    local_db: &LocalDb,
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
                           AND (
                             (i.status IS NULL OR i.status NOT IN ('active', 'waiting'))
                             OR (j.owns_ephemeral_worktree = 1
                                 AND j.status IN ('complete', 'failed', 'cancelled'))
                           )",
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
    // A live workflow_run record marks a restartable workflow whose worktree
    // Restart still needs; hold it back until the record is dropped.
    let protected = load_restartable_workflow_job_ids(local_db).await?;
    let rows: Vec<_> = rows
        .into_iter()
        .filter(|(job_id, ..)| !protected.contains(job_id))
        .collect();
    Ok(group_into_targets(rows))
}

/// Partition planned GC targets by whether their worktree directory still exists
/// on disk. A gone directory is a no-op for per-target cleanup (nothing to
/// archive, nothing to remove), so running the full [`execute_target_cleanup`] on
/// it only burns a `jj forget` + `git worktree prune` subprocess pair. Returns the
/// live targets (kept for cleanup) and the distinct repos of the gone targets
/// (pruned ONCE each by the caller so stale registrations are still reclaimed).
///
/// `Path::exists` is the load-bearing check: a present directory keeps its target
/// because removal is real work, and a gone directory's only outstanding concern
/// — its `.git/worktrees/<name>` registration — is exactly what the per-repo
/// prune clears. Deliberately leaves the `jobs` rows untouched (no schema or
/// team-sync churn), so a gone target is re-planned next pass but now costs only a
/// `stat()` plus its repo's shared prune.
fn partition_targets_by_disk_presence(
    targets: Vec<TeardownTarget>,
) -> (Vec<TeardownTarget>, BTreeSet<PathBuf>) {
    let mut live = Vec::new();
    let mut gone_repos = BTreeSet::new();
    for target in targets {
        if Path::new(&target.worktree_path).exists() {
            live.push(target);
        } else {
            gone_repos.insert(PathBuf::from(&target.repo_path));
        }
    }
    (live, gone_repos)
}

/// The set of job ids with a live `workflow_run` re-dispatch record (private DB).
/// These are restartable workflows whose ephemeral worktree must outlive bare
/// terminalization — the GC exclusion set. Loaded in one query since the table is
/// small (runner-transient) and the GC runs infrequently.
async fn load_restartable_workflow_job_ids(db: &LocalDb) -> Result<HashSet<String>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query("SELECT job_id FROM workflow_run", ()).await?;
            let mut out = HashSet::new();
            while let Some(row) = rows.next().await? {
                out.insert(row.text(0)?);
            }
            Ok(out)
        })
    })
    .await
    .map_err(|e| format!("Failed to load restartable workflow job ids: {e}"))
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
    let base = match crate::managed_worktrees::base_dir() {
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
    let inc_base = base.clone();
    let reclaimed =
        tokio::task::spawn_blocking(move || reclaim_incremental_caches(&inc_base, inc_cutoff))
            .await
            .unwrap_or_else(|e| {
                log::warn!("Worktree GC: incremental-cache reclaim task failed: {e}");
                0
            });
    if reclaimed > 0 {
        log::info!("Worktree GC: reclaimed {reclaimed} stale incremental cache dir(s)");
    }

    let target_cutoff = chrono::Utc::now().timestamp() - TARGET_SWEEP_MAX_IDLE_SECS;
    let sweep_base = base.clone();
    let (files, bytes) =
        tokio::task::spawn_blocking(move || sweep_worktree_targets(&sweep_base, target_cutoff))
            .await
            .unwrap_or_else(|e| {
                log::warn!("Worktree GC: target sweep task failed: {e}");
                (0, 0)
            });
    if files > 0 {
        log::info!(
            "Worktree GC: swept stale worktree target artifacts: {files} file(s), {bytes} byte(s) reclaimed"
        );
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

/// How long regenerable Cargo artifacts in surviving agent worktrees may sit
/// unused before the target sweep reclaims them.
const TARGET_SWEEP_MAX_IDLE_SECS: i64 = 14 * 24 * 60 * 60;

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
        let wt_path = wt.path();
        if !wt_path.is_dir() {
            continue;
        }
        for target in find_target_dirs(&wt_path) {
            let profiles = match std::fs::read_dir(&target) {
                Ok(e) => e,
                Err(_) => continue,
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
    }
    removed
}

/// Cargo target dirs under `root`, identified by the CACHEDIR.TAG marker cargo
/// writes at target creation. Bounded breadth-first scan (depth <= 3), skipping
/// hidden dirs and node_modules; a found target dir is not descended into.
fn find_target_dirs(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut queue = VecDeque::from([(root.to_path_buf(), 0usize)]);

    while let Some((dir, depth)) = queue.pop_front() {
        if dir.join(CACHEDIR_TAG).is_file() {
            out.push(dir);
            continue;
        }
        if depth >= 3 {
            continue;
        }
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == "node_modules" || name.starts_with('.') {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                queue.push_back((path, depth + 1));
            }
        }
    }

    out
}

fn sweep_worktree_targets(base: &Path, cutoff: i64) -> (u64, u64) {
    let worktrees = match std::fs::read_dir(base) {
        Ok(e) => e,
        Err(_) => return (0, 0),
    };
    let mut total_files = 0;
    let mut total_bytes = 0;
    for wt in worktrees.flatten() {
        let wt_path = wt.path();
        if !wt_path.is_dir() {
            continue;
        }
        for target in find_target_dirs(&wt_path) {
            let (files, bytes) = sweep_target_dir(&target, cutoff);
            if files > 0 {
                log::info!(
                    "Worktree GC: swept {}: {files} file(s), {bytes} byte(s) reclaimed",
                    target.display()
                );
                total_files += files;
                total_bytes += bytes;
            }
        }
    }
    (total_files, total_bytes)
}

/// Delete every file under `target` whose freshness (max of mtime and atime) is
/// older than `cutoff`, then prune emptied directories bottom-up (keeping the
/// target root itself). Never removes CACHEDIR.TAG. Does not follow symlinks; a
/// stale symlink is removed as a file. Returns (files_removed, bytes_reclaimed).
fn sweep_target_dir(target: &Path, cutoff: i64) -> (u64, u64) {
    fn walk(dir: &Path, cutoff: i64, is_root: bool, totals: &mut (u64, u64)) {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            if metadata.file_type().is_dir() {
                walk(&path, cutoff, false, totals);
                continue;
            }
            if path.file_name().and_then(|name| name.to_str()) == Some(CACHEDIR_TAG) {
                continue;
            }
            let Some(freshness) = freshness_secs(&metadata) else {
                continue;
            };
            if freshness >= cutoff {
                continue;
            }
            let bytes = metadata.len();
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    totals.0 += 1;
                    totals.1 += bytes;
                }
                Err(e) => log::warn!(
                    "Worktree GC: failed to remove stale target artifact {}: {e}",
                    path.display()
                ),
            }
        }

        if !is_root {
            if let Ok(mut entries) = std::fs::read_dir(dir) {
                if entries.next().is_none() {
                    let _ = std::fs::remove_dir(dir);
                }
            }
        }
    }

    let mut totals = (0, 0);
    walk(target, cutoff, true, &mut totals);
    totals
}

fn freshness_secs(metadata: &std::fs::Metadata) -> Option<i64> {
    let modified = metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    let accessed = metadata
        .accessed()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64);
    Some(accessed.map_or(modified, |a| modified.max(a)))
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

async fn repo_target_sweep(orch: &Orchestrator, days: i32) {
    let Some(cutoff) = repo_target_sweep_cutoff(days, chrono::Utc::now().timestamp()) else {
        return;
    };
    if !is_canonical_instance(&orch.config_dir) {
        log::debug!(
            "Worktree GC: skipping repository target sweep on non-canonical instance {}",
            orch.config_dir.display()
        );
        return;
    }

    let repo_roots = load_project_repo_paths(orch).await;
    let (files, bytes) =
        tokio::task::spawn_blocking(move || sweep_repo_targets(repo_roots, cutoff))
            .await
            .unwrap_or_else(|e| {
                log::warn!("Worktree GC: repository target sweep task failed: {e}");
                (0, 0)
            });
    if files > 0 {
        log::info!(
            "Worktree GC: swept stale repository target artifacts: {files} file(s), {bytes} byte(s) reclaimed"
        );
    }
}

fn repo_target_sweep_cutoff(days: i32, now: i64) -> Option<i64> {
    (days > 0).then(|| now - days as i64 * 24 * 60 * 60)
}

fn sweep_repo_targets(repo_roots: Vec<PathBuf>, cutoff: i64) -> (u64, u64) {
    let mut total_files = 0;
    let mut total_bytes = 0;
    for root in repo_roots {
        for target in find_target_dirs(&root) {
            let (files, bytes) = sweep_target_dir(&target, cutoff);
            if files > 0 {
                log::info!(
                    "Worktree GC: swept repo target {}: {files} file(s), {bytes} byte(s) reclaimed",
                    target.display()
                );
                total_files += files;
                total_bytes += bytes;
            }
        }
    }
    (total_files, total_bytes)
}

async fn load_project_repo_paths(orch: &Orchestrator) -> Vec<PathBuf> {
    let mut roots = BTreeSet::new();
    for db in orch.db.all_dbs().await {
        let rows: Vec<String> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT DISTINCT repo_path FROM projects WHERE repo_path IS NOT NULL",
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
        roots.extend(rows.iter().map(|p| path_key(Path::new(p))));
    }
    roots.into_iter().collect()
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
    use cairn_db::turso::params;
    use filetime::FileTime;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

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

        let targets = plan_gc_targets(&db, &db, cutoff).await.unwrap();
        assert_eq!(
            planned_paths(&targets),
            vec!["/wt/closed", "/wt/failed", "/wt/merged", "/wt/orphan"],
        );
    }

    /// A terminal ephemeral-owner WORKFLOW job with a live `workflow_run` record
    /// is a restartable workflow: its worktree must survive the GC backstop so
    /// `restart_workflow` can respawn into the persisted `working_dir`. Once the
    /// record is gone, GC reclaims it as the intended stray backstop. Mirrors the
    /// finalize reclaim guard.
    #[tokio::test]
    async fn plan_gc_targets_spares_restartable_workflow_worktree_until_record_dropped() {
        let db = migrated_db().await;
        seed_project(&db).await;
        let cutoff = 1000;
        let old = 500;

        // An ambient Manager issue stays `active` (long-lived), so ONLY the
        // owns_ephemeral backstop clause can select this workflow job.
        seed_issue(&db, "i-mgr", 1, "active").await;
        db.execute(
            "INSERT INTO jobs (id, status, project_id, issue_id, worktree_path, branch,
                 owns_ephemeral_worktree, agent_config_id, created_at, updated_at)
             VALUES ('j-wf', 'failed', 'p-1', 'i-mgr', '/wt/wf', 'agent/task-j-wf',
                 1, 'workflow', 1, ?1)",
            params![old],
        )
        .await
        .unwrap();
        // Its live re-dispatch record (kept because the run is restartable).
        db.execute(
            "INSERT INTO workflow_run (run_id, job_id, session_id, workflow_id, script_path,
                 args_json, working_dir, output_name, node_path, created_at)
             VALUES ('wf-run', 'j-wf', 'sess', 'repo-reader', '/wf/main.ts', '{}',
                 '/wt/wf', 'return', NULL, 1)",
            (),
        )
        .await
        .unwrap();

        // With the record present, the worktree is spared.
        let targets = plan_gc_targets(&db, &db, cutoff).await.unwrap();
        assert!(
            planned_paths(&targets).is_empty(),
            "a restartable workflow worktree must not be GC-planned while its record lives"
        );

        // Drop the record: now GC reclaims it as the intended stray backstop.
        db.execute("DELETE FROM workflow_run WHERE job_id = 'j-wf'", ())
            .await
            .unwrap();
        let targets = plan_gc_targets(&db, &db, cutoff).await.unwrap();
        assert_eq!(
            planned_paths(&targets),
            vec!["/wt/wf"],
            "once the record is dropped, the stray ephemeral worktree is reclaimed"
        );
    }

    /// A GC target whose worktree directory is gone is dropped from the executed
    /// set (nothing to archive or remove) but its repo is still collected for a
    /// per-repo `git worktree prune`; a target whose directory exists is kept.
    #[test]
    fn partition_drops_gone_dir_targets_and_collects_their_repos_for_prune() {
        let present = tempfile::tempdir().unwrap();
        let present_target = TeardownTarget {
            worktree_path: present.path().display().to_string(),
            branch: Some("agent/live".to_string()),
            repo_path: "/repos/live".to_string(),
            job_ids: vec!["j-live".to_string()],
        };
        // A path under the tempdir that was never created — the reclaimed-worktree
        // case: its `jobs` row still points at a directory long gone from disk.
        let gone_target = TeardownTarget {
            worktree_path: present.path().join("gone-child").display().to_string(),
            branch: Some("agent/gone".to_string()),
            repo_path: "/repos/gone".to_string(),
            job_ids: vec!["j-gone".to_string()],
        };

        let (live, gone_repos) =
            partition_targets_by_disk_presence(vec![present_target.clone(), gone_target]);

        assert_eq!(
            live,
            vec![present_target],
            "the existing directory stays in the executed set"
        );
        assert_eq!(
            gone_repos.into_iter().collect::<Vec<_>>(),
            vec![PathBuf::from("/repos/gone")],
            "the gone directory contributes only its repo for a per-repo prune"
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

    fn target_dir(path: &Path) -> PathBuf {
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(
            path.join(CACHEDIR_TAG),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        path.to_path_buf()
    }

    fn set_file_times(path: &Path, atime: i64, mtime: i64) {
        filetime::set_file_times(
            path,
            FileTime::from_unix_time(atime, 0),
            FileTime::from_unix_time(mtime, 0),
        )
        .unwrap();
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
    fn sweep_target_dir_removes_stale_file_and_keeps_fresh_file() {
        let temp = tempfile::tempdir().unwrap();
        let target = target_dir(&temp.path().join("target"));
        let stale = target.join("debug/deps/libold.rlib");
        let fresh = target.join("debug/deps/libfresh.rlib");
        std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
        std::fs::write(&stale, b"old bytes").unwrap();
        std::fs::write(&fresh, b"fresh").unwrap();
        set_file_times(&stale, 10, 10);

        let (files, bytes) = sweep_target_dir(&target, 100);

        assert_eq!(files, 1);
        assert_eq!(bytes, 9);
        assert!(!stale.exists());
        assert!(fresh.exists());
    }

    #[test]
    fn sweep_target_dir_keeps_cachedir_tag_even_when_stale() {
        let temp = tempfile::tempdir().unwrap();
        let target = target_dir(&temp.path().join("target"));
        let marker = target.join(CACHEDIR_TAG);
        set_file_times(&marker, 10, 10);

        let (files, bytes) = sweep_target_dir(&target, 100);

        assert_eq!((files, bytes), (0, 0));
        assert!(marker.exists());
    }

    #[test]
    fn sweep_target_dir_keeps_stale_mtime_when_atime_is_fresh() {
        let temp = tempfile::tempdir().unwrap();
        let target = target_dir(&temp.path().join("target"));
        let artifact = target.join("debug/deps/libhot.rlib");
        std::fs::create_dir_all(artifact.parent().unwrap()).unwrap();
        std::fs::write(&artifact, b"hot").unwrap();
        set_file_times(&artifact, 1_000, 10);

        let (files, bytes) = sweep_target_dir(&target, 100);

        assert_eq!((files, bytes), (0, 0));
        assert!(artifact.exists());
    }

    #[test]
    fn sweep_target_dir_prunes_emptied_dirs_but_keeps_target_root() {
        let temp = tempfile::tempdir().unwrap();
        let target = target_dir(&temp.path().join("target"));
        let stale = target.join("debug/.fingerprint/old/bin");
        std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
        std::fs::write(&stale, b"x").unwrap();
        set_file_times(&stale, 10, 10);

        let (files, _) = sweep_target_dir(&target, future_cutoff());

        assert_eq!(files, 1);
        assert!(target.exists());
        assert!(!target.join("debug").exists());
    }

    #[cfg(unix)]
    #[test]
    fn sweep_target_dir_removes_stale_symlink_without_following_it() {
        let temp = tempfile::tempdir().unwrap();
        let target = target_dir(&temp.path().join("target"));
        let external = temp.path().join("external");
        std::fs::create_dir_all(&external).unwrap();
        let external_file = external.join("keep.txt");
        std::fs::write(&external_file, b"keep").unwrap();
        let link = target.join("debug/deps/external-link");
        std::fs::create_dir_all(link.parent().unwrap()).unwrap();
        symlink(&external, &link).unwrap();

        let (files, _) = sweep_target_dir(&target, future_cutoff());

        assert_eq!(files, 1);
        assert!(!link.exists());
        assert!(external_file.exists());
    }

    #[test]
    fn find_target_dirs_uses_marker_skip_rules_and_depth_bound() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let root_target = target_dir(&root.join("target"));
        let tauri_target = target_dir(&root.join("src-tauri/target"));
        std::fs::create_dir_all(root.join("crate/target")).unwrap();
        target_dir(&root.join("node_modules/pkg/target"));
        target_dir(&root.join(".hidden/target"));
        target_dir(&root.join("a/b/c/target"));

        let mut found = find_target_dirs(root);
        found.sort();
        let mut expected = vec![root_target, tauri_target];
        expected.sort();

        assert_eq!(found, expected);
    }

    #[test]
    fn repo_target_sweep_cutoff_is_none_when_disabled() {
        assert_eq!(repo_target_sweep_cutoff(0, 10_000), None);
        assert_eq!(repo_target_sweep_cutoff(-1, 10_000), None);
        assert_eq!(
            repo_target_sweep_cutoff(2, 10_000),
            Some(10_000 - 2 * 24 * 60 * 60)
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
        std::fs::write(
            base.path()
                .join("CAIRN-5-builder-0/src-tauri/target/CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();

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
        std::fs::write(
            base.path()
                .join("CAIRN-6-builder-0/src-tauri/target/CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();

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
        std::fs::write(
            base.path()
                .join("CAIRN-7-builder-0/src-tauri/target/CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
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
