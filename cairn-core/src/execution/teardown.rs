//! Single owner for worktree teardown across the lifecycle.
//!
//! ## The shared path/branch key
//!
//! A `worktree_path` is **not** 1:1 with a job. When an `inherits_worktree`
//! child activates (see `execution/jobs/lifecycle.rs`), it copies its parent's
//! `worktree_path` + `branch`, so N jobs end up referencing ONE path. Creation /
//! inheritance (`lifecycle.rs`) and teardown (here) therefore key on the same
//! unit — the `worktree_path`/`branch` pair — never on a single job's completion.
//!
//! ## Lifetime rule: bound to the issue/PR, not a mid-flight refcount
//!
//! A worktree lives until its issue/PR reaches a terminal state — merged,
//! closed, or deleted. It is **never** torn down while the execution is in
//! flight. The previous "refcount-to-zero" model (delete as soon as no job
//! currently holds the path) had an unavoidable race at every node boundary: an
//! inheriting child does not copy the parent's `worktree_path` until it prepares
//! to run, so a freshly-created pending child was invisible to the refcount and
//! its worktree got deleted out from under it.
//!
//! Teardown therefore fires only at issue/PR-terminal transitions (PR
//! merge/close, issue close/merge, issue delete). At that point the work is
//! done, so removing every distinct worktree path in the issue is correct and
//! unconditional. A still-running job on a terminal issue does not block
//! teardown — its terminals are killed first.
//!
//! Teardown is idempotent: `remove_worktree_with_services` and
//! `delete_branch_with_services` no-op when the worktree/branch is already gone.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The managed worktrees base dir, `~/.cairn/worktrees`. Mirrors the hardcoded
/// creation path in `execution/jobs/lifecycle.rs` and is deliberately NOT
/// instance-scoped (`.cairn` vs `.cairn-dev`): the worktrees dir is shared across
/// instances. `None` only when the home dir cannot be resolved.
pub(crate) fn worktrees_base_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".cairn").join("worktrees"))
}

/// Robustly remove a job's working directory. Shared by teardown, the worktree
/// GC, and project deletion. Idempotent and best-effort.
///
/// Order:
/// 1. Best-effort `jj workspace forget` whenever a branch is known and the
///    project jj store exists — idempotent, and correct even when the workspace
///    dir was already half-deleted (`.jj` gone). The old `is_jj_dir(wt_path)`
///    dispatch got this wrong: a partial delete that removed `.jj` first fell
///    through to `git worktree remove` and failed forever with "not a working
///    tree" (CAIRN-2283).
/// 2. If the path is a registered git worktree of the repo, remove it with
///    `git worktree remove` (unregisters + deletes).
/// 3. Otherwise — every jj workspace, and every half-deleted dir — fall back to a
///    rename-then-delete under the managed worktrees base dir.
///
/// Every path except the git-aware remove (which unregisters itself) finishes
/// with a best-effort `git worktree prune` of the repo: a dir deleted without
/// `git worktree remove` — the tombstone fallback, a manual `rm -rf`, a prior
/// fs-only sweep — leaves its `.git/worktrees/<name>` registration behind, and
/// those accumulate until they slow down and collide with future
/// `git worktree add` calls.
pub(crate) fn remove_worktree_robust(
    orch: &Orchestrator,
    repo_path: &str,
    wt_path: &Path,
    branch: Option<&str>,
) -> Result<(), String> {
    // Reap any process still rooted (by cwd) in this worktree BEFORE removal. A
    // dev instance launched from a worktree detaches its runner/tauri/vite into
    // their own process groups (see `scripts/dev-instance.ts`), so neither the
    // PTY terminal kill nor `git worktree remove` reaches them — only a cwd scan
    // does (CAIRN-2390). Guarded to the managed worktrees base so a production
    // process (cwd = main checkout, never under `~/.cairn/worktrees`) is never
    // touched; this mirrors `remove_dir_tombstoned`'s guard and is the
    // load-bearing safety boundary. Reaping before the delete also frees the
    // directory, reducing the ENOTEMPTY/EBUSY races the tombstone retry fights.
    if let Some(base) = worktrees_base_dir() {
        if wt_path.starts_with(&base) {
            let reaped = orch.services.reaper.reap_under(wt_path);
            if !reaped.is_empty() {
                log::info!(
                    "Teardown: reaped {} process(es) rooted in {}",
                    reaped.len(),
                    wt_path.display()
                );
            }
        }
    }

    if let Some(b) = branch {
        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
        if crate::jj::is_jj_dir(&store) {
            let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
            let workspace_name = crate::jj::read_workspace_identity(wt_path)
                .map(|identity| identity.workspace_name)
                .unwrap_or_else(|| crate::jj::workspace_name_for_branch(b));
            let _ = crate::jj::forget_workspace_name(&jj, &store, &workspace_name);
        }
    }

    if !wt_path.exists() {
        // The dir is already gone, but a registration may not be: git keeps a
        // missing worktree registered until pruned.
        prune_worktree_registrations(orch, Path::new(repo_path));
        return Ok(());
    }

    if is_registered_git_worktree(orch, repo_path, wt_path) {
        return crate::git::worktree::remove_worktree_with_services(
            &*orch.services.git,
            &*orch.services.fs,
            Path::new(repo_path),
            wt_path,
            true,
        );
    }

    let base = worktrees_base_dir()
        .ok_or_else(|| "could not resolve worktrees base dir (no home dir)".to_string())?;
    let result = remove_dir_tombstoned(wt_path, &base);
    // Even a partially failed removal renames the live path away (or leaves it
    // intact, where pruning is a no-op), so prune unconditionally. This also
    // deregisters a genuine git worktree that fell through here because
    // `worktree_list` itself errored.
    prune_worktree_registrations(orch, Path::new(repo_path));
    result
}

/// Best-effort `git worktree prune` on `repo_path`, clearing `.git/worktrees`
/// registrations whose worktree dirs no longer exist. Harmless when the repo is
/// a jj project or not a git repo at all (the command just fails; debug-logged).
fn prune_worktree_registrations(orch: &Orchestrator, repo_path: &Path) {
    if let Err(e) = orch.services.git.worktree_prune(repo_path) {
        log::debug!(
            "Teardown: git worktree prune failed for {}: {e}",
            repo_path.display()
        );
    }
}

/// Whether `wt_path` is a registered git worktree of `repo_path`. A jj workspace
/// is never registered in the repo's `git worktree list`, so this returns false
/// for jj (routing it to the rename-then-delete fallback) and true only for a
/// genuine git worktree.
fn is_registered_git_worktree(orch: &Orchestrator, repo_path: &str, wt_path: &Path) -> bool {
    match orch.services.git.worktree_list(Path::new(repo_path)) {
        Ok(list) => worktree_list_contains(&list, wt_path),
        Err(_) => false,
    }
}

/// True if `wt_path` appears as a `worktree <path>` entry in `git worktree list
/// --porcelain` output. Compares canonicalized paths so a symlinked base (macOS
/// `/var` -> `/private/var`) still matches.
fn worktree_list_contains(porcelain: &str, wt_path: &Path) -> bool {
    let target = std::fs::canonicalize(wt_path).unwrap_or_else(|_| wt_path.to_path_buf());
    porcelain
        .lines()
        .filter_map(|l| l.strip_prefix("worktree "))
        .any(|p| {
            let listed = Path::new(p);
            std::fs::canonicalize(listed).unwrap_or_else(|_| listed.to_path_buf()) == target
        })
}

/// Rename `wt_path` to a unique `*.trash-<millis>` sibling, then delete the
/// tombstone with a short retry loop (ENOTEMPTY/EBUSY). The rename is atomic and
/// frees the live name immediately, so a concurrent writer (Finder's
/// `.DS_Store`, a straggler build writing into `target/`) can no longer land
/// files at the live path mid-delete — the ENOTEMPTY failure that stranded
/// hundreds of GB of merged-issue worktrees. If the tombstone delete still fails
/// after retries, the tombstone is LEFT in place (logged with its surviving
/// entries so the writer can be diagnosed) for the worktree GC's `*.trash-*`
/// sweep, and an error returns.
///
/// Guarded to the managed worktrees base dir so a misconfigured `worktree_path`
/// can never delete an arbitrary directory.
pub(crate) fn remove_dir_tombstoned(wt_path: &Path, base: &Path) -> Result<(), String> {
    if !wt_path.starts_with(base) {
        return Err(format!(
            "refusing to remove worktree path outside managed base {}: {}",
            base.display(),
            wt_path.display()
        ));
    }
    if !wt_path.exists() {
        return Ok(());
    }
    let parent = wt_path.parent().unwrap_or(base);
    let name = wt_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("worktree");
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let tombstone = parent.join(format!("{name}.trash-{millis}"));

    match std::fs::rename(wt_path, &tombstone) {
        Ok(()) => remove_dir_with_retries(&tombstone).map_err(|e| {
            log_surviving_entries(&tombstone);
            format!(
                "renamed worktree to tombstone {} but delete failed ({e}); left for GC sweep",
                tombstone.display()
            )
        }),
        Err(rename_err) => {
            // Rename failed (cross-device, or a racing remover already moved it).
            // If the path is already gone we are done; otherwise fall back to an
            // in-place retrying delete.
            if !wt_path.exists() {
                return Ok(());
            }
            log::warn!(
                "Teardown: rename-to-tombstone failed for {} ({rename_err}); deleting in place",
                wt_path.display()
            );
            remove_dir_with_retries(wt_path).map_err(|e| {
                log_surviving_entries(wt_path);
                format!("failed to remove worktree dir {}: {e}", wt_path.display())
            })
        }
    }
}

/// `remove_dir_all` with a short bounded retry/backoff for transient ENOTEMPTY /
/// EBUSY (a writer still touching the tree). A `NotFound` is success.
fn remove_dir_with_retries(path: &Path) -> std::io::Result<()> {
    const ATTEMPTS: usize = 3;
    let mut last = Ok(());
    for attempt in 0..ATTEMPTS {
        match std::fs::remove_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                last = Err(e);
                if attempt + 1 < ATTEMPTS {
                    std::thread::sleep(std::time::Duration::from_millis(
                        100 * (attempt as u64 + 1),
                    ));
                }
            }
        }
    }
    last
}

/// Log up to 20 entries that survived a failed delete — names who is writing into
/// the tombstone (a build process, Finder metadata) so the leak can be diagnosed.
fn log_surviving_entries(path: &Path) {
    if let Ok(entries) = std::fs::read_dir(path) {
        let names: Vec<String> = entries
            .flatten()
            .take(20)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        log::warn!(
            "Teardown: tombstone {} still has entries after retries: {:?}",
            path.display(),
            names
        );
    }
}

/// Archive at-risk events, drop the warm search index, remove the worktree dir
/// robustly, and reclaim scratch dirs for one target. Shared by
/// [`teardown_worktrees`] and the worktree GC. Does NOT touch branches —
/// branch-deletion policy lives solely in teardown.
pub(crate) async fn execute_target_cleanup(
    orch: &Orchestrator,
    db: &LocalDb,
    target: &TeardownTarget,
) {
    // Clear tracked terminal/browser rows and best-effort-kill their PTY
    // children for every job referencing this worktree. This is the per-target
    // home of the job-scoped kill, so the worktree GC path — which reaches
    // cleanup only through here — now clears stale `job_terminals` /
    // `job_browsers` rows and sends the browser Close command, which it never
    // did before. It overlaps harmlessly with the path-scoped reaper in
    // `remove_worktree_robust` (both idempotent, best-effort) while each reaches
    // something the other cannot: detached grandchildren vs. DB rows and the
    // browser channel.
    kill_terminals_for_jobs(orch, db, &target.job_ids).await;
    kill_repls_for_jobs(orch, &target.job_ids);

    // Archive the execution's at-risk events to git coordinates (with a zstd
    // backstop) before the worktree and branch disappear, the immutability
    // boundary. Strictly best-effort: any failure logs a warning and rolls back
    // (rows stay `full`) — archival must never block cleanup. Construct the jj
    // driver unconditionally; archival's forward-mapping helpers self-gate on
    // `is_jj_dir`, so a plain-git worktree is a clean no-op while a jj workspace
    // gets its commits forward-mapped past any auto-rebase.
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    match crate::archival::archive_target(
        db,
        &target.worktree_path,
        &target.repo_path,
        &target.job_ids,
        Some(&jj),
    )
    .await
    {
        Ok(summary) => {
            log::info!(
                "Teardown: archived worktree {} ({} gitcoord-read, {} hybrid-read, {} gitcoord-write, {} system-prompt, {} system-init, {} zstd, {} mismatch-fallback, {} -> {} bytes)",
                target.worktree_path,
                summary.gitcoord_read,
                summary.hybrid_read,
                summary.gitcoord_write,
                summary.system_prompt,
                summary.system_init,
                summary.zstd,
                summary.mismatch_fallback,
                summary.bytes_before,
                summary.bytes_after,
            );
            // Drift tripwire: reads resolved their bytes but every one failed the
            // render-and-compare. The expected signature of a live/archival
            // read-composition divergence (the CAIRN-1676 regression) — surfaced
            // here so it never hides behind a green run.
            if summary.mismatch_fallback > 0
                && summary.gitcoord_read == 0
                && summary.hybrid_read == 0
            {
                log::warn!(
                    "Teardown: worktree {} archived {} reads but none to a git coordinate \
                     (all fell to mismatch-fallback) — the live and archival read composition \
                     may have diverged",
                    target.worktree_path,
                    summary.mismatch_fallback,
                );
            }
        }
        Err(e) => log::warn!(
            "Teardown: archival failed for worktree {}: {}",
            target.worktree_path,
            e
        ),
    }

    let wt_path = PathBuf::from(&target.worktree_path);

    // Drop the warm search index BEFORE removal (CAIRN-2303) so the picker's
    // background scan/watcher threads release the directory first — else they can
    // re-touch files mid-delete. Idempotent when the worktree was never indexed.
    orch.worktree_search.drop_worktree(&wt_path);

    match remove_worktree_robust(orch, &target.repo_path, &wt_path, target.branch.as_deref()) {
        Ok(()) => log::info!("Teardown: removed worktree {}", target.worktree_path),
        Err(e) => log::warn!(
            "Teardown: failed to remove worktree {}: {}",
            target.worktree_path,
            e
        ),
    }

    // Reclaim each referencing job's scratch dir alongside the worktree.
    // Worktrees aren't 1:1 with jobs (inheritance fan-out), so a target may carry
    // several job ids; remove every one's scratch dir. Idempotent and
    // best-effort (a missing dir is fine). Also remove any `when:write` check-clone
    // root for the job, so a crash mid-check never outlives the job (the isolated
    // check runner also removes it per batch via its scope guard).
    for job_id in &target.job_ids {
        crate::scratch::remove_job_scratch_dir(job_id);
        let clone_root =
            crate::execution::check_isolation::clone_root_for_job(&orch.config_dir, job_id);
        let _ = std::fs::remove_dir_all(&clone_root);
    }
}

/// Which jobs a teardown pass considers.
pub enum TeardownScope {
    /// All jobs across an issue (PR merge/close, issue close/delete).
    Issue(String),
    /// Exactly one job's worktree. Used to reclaim an ambient parent's ephemeral
    /// task worktree the moment that task job terminalizes — job-scoped rather
    /// than issue-scoped because the owning issue (an ambient Manager) is
    /// long-lived and never terminalizes, so the issue-scoped sweep would let the
    /// task worktree accumulate.
    Job(String),
}

/// The branch-deletion policy for an issue teardown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownReason {
    /// The issue was recorded as merged WITHOUT a guaranteed fold — the status
    /// resolution path, which flips the row and tears down but never merges. A
    /// branch is deleted only once its tip has landed in its merge target;
    /// an unlanded branch is PRESERVED (local + remote delete skipped) so its
    /// commits are never stranded — the KMCP data-loss invariant (CAIRN-2287).
    Merged,
    /// A verified post-merge reconcile, an explicit PR close, or an issue delete.
    /// The content is already incorporated (a verified fold, or an out-of-band
    /// squash-merge that legitimately rewrites the source off the target's
    /// history) or the discard is deliberate, so branches are deleted
    /// unconditionally — the pre-existing behavior.
    Discarded,
}

/// One worktree+branch unit eligible for teardown, carrying every in-scope job
/// that references it (captures inheritance fan-out).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeardownTarget {
    pub worktree_path: String,
    pub branch: Option<String>,
    pub repo_path: String,
    pub job_ids: Vec<String>,
}

/// Compute the set of worktree paths to tear down in `scope`.
///
/// Groups in-scope jobs by `worktree_path` (deduping inheritance fan-out into a
/// single target per path) and returns every distinct path unconditionally. The
/// caller only invokes this at issue/PR-terminal time, where removing all of the
/// issue's worktrees is the correct, complete action.
///
/// Takes `&LocalDb` rather than `&Orchestrator`: the decision is pure DB work and
/// is the testable heart of the owner.
pub async fn plan_teardown(
    db: &LocalDb,
    scope: &TeardownScope,
) -> Result<Vec<TeardownTarget>, String> {
    let (scope_col, scope_id) = match scope {
        TeardownScope::Issue(id) => ("issue_id", id.clone()),
        TeardownScope::Job(id) => ("id", id.clone()),
    };

    // `scope_col` is a fixed literal from the match above, never user input.
    let sql = format!(
        "SELECT j.id, j.worktree_path, j.branch, p.repo_path
         FROM jobs j
         JOIN projects p ON j.project_id = p.id
         WHERE j.{scope_col} = ?1 AND j.worktree_path IS NOT NULL"
    );

    let rows: Vec<(String, String, Option<String>, String)> = db
        .read(|conn| {
            let sql = sql.clone();
            let scope_id = scope_id.clone();
            Box::pin(async move {
                let mut rows = conn.query(sql.as_str(), (scope_id.as_str(),)).await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.text(1)?, row.opt_text(2)?, row.text(3)?));
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to load teardown candidates: {e}"))?;

    Ok(group_into_targets(rows))
}

/// Group `(job_id, worktree_path, branch, repo_path)` rows into one
/// [`TeardownTarget`] per distinct worktree path, folding the inheritance fan-out
/// (N jobs -> one path) into a single target. A `BTreeMap` keys the grouping so
/// output is sorted by path — deterministic for tests. The first non-`None`
/// branch seen for a path wins. Shared by [`plan_teardown`], the worktree GC, and
/// project-deletion cleanup.
pub(crate) fn group_into_targets(
    rows: Vec<(String, String, Option<String>, String)>,
) -> Vec<TeardownTarget> {
    // worktree_path -> (branch, repo_path, job_ids).
    type Grouped = BTreeMap<String, (Option<String>, String, Vec<String>)>;
    let mut grouped: Grouped = BTreeMap::new();
    for (job_id, worktree_path, branch, repo_path) in rows {
        let entry = grouped
            .entry(worktree_path)
            .or_insert_with(|| (branch.clone(), repo_path, Vec::new()));
        if entry.0.is_none() {
            entry.0 = branch;
        }
        entry.2.push(job_id);
    }
    grouped
        .into_iter()
        .map(
            |(worktree_path, (branch, repo_path, job_ids))| TeardownTarget {
                worktree_path,
                branch,
                repo_path,
                job_ids,
            },
        )
        .collect()
}

/// The single teardown owner: plan the issue's worktree paths, then kill their
/// terminals, remove the worktrees, and delete local + remote branches.
///
/// Reached only at issue/PR-terminal transitions (PR merge/close, issue
/// close/merge, issue delete). Issue-wide and unconditional — the work is done
/// by the time any of these fire.
pub async fn teardown_worktrees(
    orch: &Orchestrator,
    scope: TeardownScope,
    reason: TeardownReason,
) -> Result<(), String> {
    // Route to the scope's owning database: a team issue's jobs (and their
    // worktree_path rows, terminals, and browsers) live wholly in its synced
    // replica, so planning and cleanup must read/write there or the team's
    // worktrees leak and its terminal/browser rows are never cleared. A
    // Job-scoped reclaim routes by the job instead.
    let (db, issue_id): (std::sync::Arc<LocalDb>, Option<String>) = match &scope {
        TeardownScope::Issue(issue_id) => (
            crate::issues::crud::owning_db_for_issue(&orch.db, issue_id)
                .await
                .map_err(|e| e.to_string())?,
            Some(issue_id.clone()),
        ),
        TeardownScope::Job(job_id) => (
            crate::execution::routing::owning_db_for_job(&orch.db, job_id)
                .await
                .map_err(|e| e.to_string())?,
            None,
        ),
    };
    // The Merged reason (landed-aware branch preservation) only ever pairs with an
    // Issue scope; a Job-scoped reclaim is always Discarded. `issue_id` is None
    // only in that Discarded case, so this empty fallback is never consulted for a
    // real merge-target resolution.
    let issue_id_str = issue_id.as_deref().unwrap_or("");
    let targets = plan_teardown(&db, &scope).await?;
    if targets.is_empty() {
        return Ok(());
    }

    // Remove worktrees (archive at-risk events, drop the warm search index,
    // delete the dir robustly, reclaim scratch dirs) then delete local branches;
    // collect branches per repo. The per-target cleanup is shared with the
    // worktree GC; branch deletion below stays exclusive to teardown.
    let mut branches_by_repo: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for target in &targets {
        execute_target_cleanup(orch, &db, target).await;

        if let Some(branch) = &target.branch {
            // On a merged resolution recorded WITHOUT a verified fold (the status
            // path), never delete a branch whose commits have not landed in the
            // merge target: deleting it strands those commits and auto-closes the
            // PR unmerged, the KMCP data-loss failure (CAIRN-2287). Preserve it
            // (skip local + remote delete) and warn loudly. A landed branch — and
            // any Discarded (close / delete / verified-merge) teardown — deletes
            // as before. Worktree, scratch, and terminal cleanup already ran, so
            // only the recoverable branch ref is kept.
            let delete_branch = match reason {
                TeardownReason::Discarded => true,
                TeardownReason::Merged => {
                    match resolve_merge_target_for_source(
                        &db,
                        issue_id_str,
                        branch,
                        &target.job_ids,
                    )
                    .await
                    {
                        // A branch never lands "into itself" (a Coordinator whose
                        // own branch IS the target): nothing folds it elsewhere,
                        // so treat it as landed for teardown.
                        Some(merge_target) if merge_target == *branch => true,
                        Some(merge_target) => {
                            match branch_landed(orch, &target.repo_path, branch, &merge_target) {
                                Ok(true) => true,
                                Ok(false) => {
                                    log::warn!(
                                        "Teardown: PRESERVING branch `{branch}` — its tip has NOT landed in merge target `{merge_target}` for merged issue {issue_id_str}; deleting it would strand commits. Keeping the local branch and skipping the remote delete.",
                                    );
                                    false
                                }
                                Err(e) => {
                                    log::warn!(
                                        "Teardown: could not verify whether branch `{branch}` landed in `{merge_target}` for merged issue {issue_id_str} ({e}); PRESERVING it (fail-closed).",
                                    );
                                    false
                                }
                            }
                        }
                        None => {
                            log::warn!(
                                "Teardown: no merge target resolved for branch `{branch}` on merged issue {issue_id_str}; PRESERVING it (fail-closed).",
                            );
                            false
                        }
                    }
                }
            };
            if delete_branch {
                if let Err(e) = crate::git::worktree::delete_branch_with_services(
                    &*orch.services.git,
                    Path::new(&target.repo_path),
                    branch,
                ) {
                    log::warn!("Teardown: failed to delete local branch {}: {}", branch, e);
                }
                branches_by_repo
                    .entry(target.repo_path.clone())
                    .or_default()
                    .push(branch.clone());
            }
        }
    }

    // Delete remote branches per repo (best-effort, only when creds resolve).
    for (repo_path, mut branches) in branches_by_repo {
        branches.sort();
        branches.dedup();
        delete_remote_branches_for_repo(orch, &repo_path, &branches).await;
    }

    // Intentionally bare: this is an issue-scoped sweep over many torn-down jobs,
    // so it carries no ids and the frontend broad-invalidates ["jobs"]. (See the
    // load-bearing invariant on `crate::notify::job_db_change_ids`.)
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );

    Ok(())
}

/// Robustly remove every worktree registered to a project's jobs (jj-aware,
/// rename-then-delete). Used by project deletion, which otherwise leaks the
/// project's worktrees entirely. Branches are intentionally preserved — the git
/// repo is the user's; Cairn only reclaims its working checkouts. Best-effort,
/// and must run BEFORE the project rows are deleted (it reads job worktree paths
/// and branches). No archival: the project's events are about to be deleted with
/// it, so archiving them into the same about-to-vanish database is pointless.
pub async fn remove_project_worktrees(orch: &Orchestrator, project_id: &str) -> Result<(), String> {
    let db = orch.db.local.clone();
    let targets = plan_project_teardown(&db, project_id).await?;
    if targets.is_empty() {
        return Ok(());
    }
    for target in &targets {
        let wt_path = PathBuf::from(&target.worktree_path);
        orch.worktree_search.drop_worktree(&wt_path);
        if let Err(e) =
            remove_worktree_robust(orch, &target.repo_path, &wt_path, target.branch.as_deref())
        {
            log::warn!(
                "Project deletion: failed to remove worktree {}: {}",
                target.worktree_path,
                e
            );
        }
        for job_id in &target.job_ids {
            crate::scratch::remove_job_scratch_dir(job_id);
        }
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );
    Ok(())
}

/// Plan every distinct worktree target for a project's jobs, grouping the
/// inheritance fan-out like [`plan_teardown`].
async fn plan_project_teardown(
    db: &LocalDb,
    project_id: &str,
) -> Result<Vec<TeardownTarget>, String> {
    let project_id = project_id.to_string();
    let rows: Vec<(String, String, Option<String>, String)> = db
        .read(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.id, j.worktree_path, j.branch, p.repo_path
                         FROM jobs j
                         JOIN projects p ON j.project_id = p.id
                         WHERE j.project_id = ?1 AND j.worktree_path IS NOT NULL",
                        (project_id.as_str(),),
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
        .map_err(|e| format!("Failed to load project worktrees: {e}"))?;
    Ok(group_into_targets(rows))
}

/// Whether `source_branch`'s tip has landed in `target_branch`, dispatching on
/// project kind. A jj project checks the shared store (the source is folded into
/// the target as a descendant, so an ancestor test is exact); a plain-git project
/// runs `git merge-base --is-ancestor`. An `Err` means the VCS query itself
/// failed — the caller treats that as "unknown" and preserves the branch.
///
/// jj-ness is decided from the shared store's presence, NOT the (already-removed)
/// worktree: teardown deletes the worktree before this runs, so `is_jj_dir` on
/// the workspace path would always be false here.
fn branch_landed(
    orch: &Orchestrator,
    repo_path: &str,
    source_branch: &str,
    target_branch: &str,
) -> Result<bool, String> {
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    if crate::jj::is_jj_dir(&store) {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        Ok(crate::jj::bookmark_landed_in(
            &jj,
            &store,
            source_branch,
            target_branch,
        ))
    } else {
        orch.services
            .git
            .is_ancestor(Path::new(repo_path), source_branch, target_branch)
    }
}

/// Resolve the merge target branch a torn-down `source_branch` should have landed
/// in: the issue's MR `target_branch` first (the real destination), then a
/// producing job's `base_branch` (its fork point), then the project default
/// branch. `None` only when the issue's project cannot be resolved at all.
async fn resolve_merge_target_for_source(
    db: &LocalDb,
    issue_id: &str,
    source_branch: &str,
    job_ids: &[String],
) -> Option<String> {
    let issue_id = issue_id.to_string();
    let source_branch = source_branch.to_string();
    let job_ids = job_ids.to_vec();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        let source_branch = source_branch.clone();
        let job_ids = job_ids.clone();
        Box::pin(async move {
            // 1. The MR's recorded target branch (the true merge destination).
            let mut rows = conn
                .query(
                    "SELECT target_branch FROM merge_requests
                     WHERE issue_id = ?1 AND source_branch = ?2
                     ORDER BY opened_at DESC LIMIT 1",
                    params![issue_id.as_str(), source_branch.as_str()],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                if let Some(target) = row.opt_text(0)?.filter(|t| !t.is_empty()) {
                    return Ok(Some(target));
                }
            }
            drop(rows);
            // 2. A producing job's base_branch (what it forked from).
            for job_id in &job_ids {
                let mut rows = conn
                    .query(
                        "SELECT base_branch FROM jobs WHERE id = ?1 LIMIT 1",
                        params![job_id.as_str()],
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    if let Some(base) = row.opt_text(0)?.filter(|b| !b.is_empty()) {
                        return Ok(Some(base));
                    }
                }
            }
            // 3. The project default branch.
            let mut rows = conn
                .query(
                    "SELECT p.default_branch FROM projects p
                     JOIN issues i ON i.project_id = p.id
                     WHERE i.id = ?1 LIMIT 1",
                    params![issue_id.as_str()],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                if let Some(default) = row.opt_text(0)?.filter(|d| !d.is_empty()) {
                    return Ok(Some(default));
                }
            }
            Ok(None)
        })
    })
    .await
    .ok()
    .flatten()
}

/// Tear down worktrees for jobs whose recipe nodes were removed from a live
/// execution snapshot.
///
/// Unlike [`teardown_worktrees`], this is **not** issue-terminal and not
/// issue-wide: it removes only the specific worktree paths exclusively owned by
/// the now-cancelled jobs. The caller (snapshot-edit reconciliation) has already
/// validated that each removed job was discardable (pending/failed/blocked),
/// archived the job rows as `cancelled`, and precomputed `targets` so a path
/// still referenced by a surviving job is never included here.
/// `cancelled_job_ids` is carried only to kill any terminals those jobs left
/// behind.
///
/// Branches are deliberately **preserved**: worktrees are disposable working
/// checkouts, but a removed (e.g. failed) node's commits live on its branch and
/// are history we keep. Removing the worktree reclaims disk while leaving the
/// branch ref intact, so the commits stay recoverable. Entirely best-effort.
pub async fn teardown_removed_node_worktrees(
    orch: &Orchestrator,
    cancelled_job_ids: &[String],
    targets: &[TeardownTarget],
) {
    if !cancelled_job_ids.is_empty() {
        // Best-effort cleanup: resolve the cancelled jobs' owning replica, falling
        // back to private only if the rows are already gone.
        let db = crate::execution::routing::owning_db_for_job(&orch.db, &cancelled_job_ids[0])
            .await
            .unwrap_or_else(|_| orch.db.local.clone());
        kill_terminals_for_jobs(orch, &db, cancelled_job_ids).await;
        kill_repls_for_jobs(orch, cancelled_job_ids);
    }
    if targets.is_empty() {
        return;
    }

    for target in targets {
        let wt_path = PathBuf::from(&target.worktree_path);

        // Drop the warm search index before removal (CAIRN-2303) so the picker's
        // background scan/watcher threads release the directory first; idempotent
        // when never indexed.
        orch.worktree_search.drop_worktree(&wt_path);

        match remove_worktree_robust(orch, &target.repo_path, &wt_path, target.branch.as_deref()) {
            Ok(()) => log::info!("Snapshot edit: removed worktree {}", target.worktree_path),
            Err(e) => log::warn!(
                "Snapshot edit: failed to remove worktree {}: {}",
                target.worktree_path,
                e
            ),
        }

        for job_id in &target.job_ids {
            crate::scratch::remove_job_scratch_dir(job_id);
        }
        // Branch is intentionally not deleted (history preservation).
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );
}

/// Kill and drop the in-memory REPL sessions belonging to the given jobs. The
/// DB-free registry is the single source of truth, so this is the whole story:
/// drain the matching entries and SIGKILL each eval-server. The orphan-prevention
/// guarantee for stateful REPLs.
fn kill_repls_for_jobs(orch: &Orchestrator, job_ids: &[String]) {
    for session in orch.repl_state.remove_for_jobs(job_ids) {
        session.kill();
    }
}

/// Kill running PTY sessions for the given jobs and delete their terminal rows.
///
/// Uses `orch.pty_state`, which both hosts share. On cairn-server (no live PTY
/// sessions) this naturally no-ops the kill while still clearing terminal rows.
async fn kill_terminals_for_jobs(orch: &Orchestrator, db: &LocalDb, job_ids: &[String]) {
    if job_ids.is_empty() {
        return;
    }

    // Kill live PTY sessions for running terminals first.
    match load_running_terminals_for_jobs(db, job_ids).await {
        Ok(running) => {
            for (terminal_id, session_id) in &running {
                let removed = match orch.pty_state.sessions.lock() {
                    Ok(mut sessions) => sessions.remove(session_id),
                    Err(e) => {
                        log::warn!("Teardown: failed to lock PTY sessions: {}", e);
                        continue;
                    }
                };
                if let Some(session_arc) = removed {
                    if let Ok(mut session) = session_arc.lock() {
                        let _ = session.child.kill();
                        let _ = session.child.wait(); // avoid zombies
                        log::info!(
                            "Teardown: killed terminal {} (session {})",
                            terminal_id,
                            session_id
                        );
                    }
                }
            }
        }
        Err(e) => {
            log::warn!("Teardown: failed to load running terminals: {}", e);
        }
    }

    // Delete every terminal row for these jobs — the running ones we just killed
    // and any lingering exited rows retained for post-exit reads / exit wakes.
    if let Err(e) = delete_all_terminal_rows_for_jobs(db, job_ids).await {
        log::warn!("Teardown: failed to delete terminal rows: {}", e);
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "delete"}),
    );

    // Node-scoped browsers are torn down with their jobs (project browsers
    // persist). Core cannot touch the live webview handle, so it sends a Close
    // over the channel for the app-side drain task to destroy it, then deletes
    // the rows.
    close_browsers_for_jobs(orch, db, job_ids).await;
}

/// Close live native webviews for the given jobs and delete their browser rows.
///
/// The live `Webview` handles live app-side; core reaches them only by sending
/// [`BrowserCommand::Close`](crate::browsers::BrowserCommand) over the channel.
/// On hosts without a webview layer the channel is `None` and only the rows are
/// cleared.
async fn close_browsers_for_jobs(orch: &Orchestrator, db: &LocalDb, job_ids: &[String]) {
    if job_ids.is_empty() {
        return;
    }
    match crate::browsers::list_running_browsers_for_jobs(db, job_ids).await {
        Ok(running) => {
            for browser in &running {
                if let Some(tx) = &orch.browser_command_tx {
                    let _ = tx.send(crate::browsers::BrowserCommand::Close {
                        id: browser.id.clone(),
                        label: browser.webview_label.clone(),
                    });
                }
            }
        }
        Err(e) => log::warn!("Teardown: failed to load running browsers: {e}"),
    }
    if let Err(e) = crate::browsers::delete_all_browser_rows_for_jobs(db, job_ids).await {
        log::warn!("Teardown: failed to delete browser rows: {e}");
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_browsers", "action": "delete"}),
    );
}

async fn load_running_terminals_for_jobs(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<Vec<(String, String)>, String> {
    let job_ids = job_ids.to_vec();
    db.read(|conn| {
        let job_ids = job_ids.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            for job_id in &job_ids {
                let mut rows = conn
                    .query(
                        "SELECT id, session_id FROM job_terminals
                         WHERE job_id = ?1 AND status = 'running'",
                        (job_id.as_str(),),
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.text(1)?));
                }
            }
            Ok(out)
        })
    })
    .await
    .map_err(|e| format!("Failed to load running terminals for teardown: {e}"))
}

async fn delete_all_terminal_rows_for_jobs(db: &LocalDb, job_ids: &[String]) -> Result<(), String> {
    let job_ids = job_ids.to_vec();
    db.write(|conn| {
        let job_ids = job_ids.clone();
        Box::pin(async move {
            for job_id in &job_ids {
                conn.execute(
                    "DELETE FROM job_terminals WHERE job_id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to delete job terminals during teardown: {e}"))
}

/// Delete remote branches for a repo (best-effort). No-ops when the repo is not
/// a GitHub remote or no GitHub App credentials are available.
async fn delete_remote_branches_for_repo(
    orch: &Orchestrator,
    repo_path: &str,
    branches: &[String],
) {
    if branches.is_empty() {
        return;
    }
    let (owner, repo) = match crate::github::credentials::get_owner_repo(repo_path) {
        Ok(owner_repo) => owner_repo,
        Err(e) => {
            log::debug!(
                "Teardown: skipping remote branch cleanup (not a GitHub repo): {}",
                e
            );
            return;
        }
    };
    let creds =
        match crate::github::credentials::get_credentials_for_owner(&orch.db.local, &owner).await {
            Ok(creds) => creds,
            Err(e) => {
                log::warn!(
                    "Teardown: skipping remote branch cleanup (no GitHub credentials): {}",
                    e
                );
                return;
            }
        };
    crate::github::api::delete_remote_branches(
        &*orch.services.http,
        &creds,
        &owner,
        &repo,
        branches,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_db::turso::params;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("teardown-test.db").await
    }

    async fn seed_project(db: &LocalDb, project_id: &str, key: &str) {
        let project_id = project_id.to_string();
        let key = key.to_string();
        db.write(|conn| {
            let project_id = project_id.clone();
            let key = key.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces (id, name, created_at, updated_at)
                     VALUES (?1, ?2, 1, 1)",
                    params![format!("w-{project_id}"), format!("Workspace {key}")],
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 1, 1)",
                    params![
                        project_id.as_str(),
                        format!("w-{project_id}"),
                        format!("Project {key}"),
                        key.as_str(),
                        format!("/tmp/{key}")
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn seed_issue(db: &LocalDb, project_id: &str, issue_id: &str, number: i32) {
        let project_id = project_id.to_string();
        let issue_id = issue_id.to_string();
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status,
                 progress, attention, priority, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'T', '', 'active', 'active', 'none', 0, 1, 1)",
            params![issue_id.as_str(), project_id.as_str(), number],
        )
        .await
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_job(
        db: &LocalDb,
        id: &str,
        project_id: &str,
        issue_id: Option<&str>,
        execution_id: Option<&str>,
        worktree_path: Option<&str>,
        branch: Option<&str>,
        status: &str,
    ) {
        let id = id.to_string();
        let project_id = project_id.to_string();
        let issue_id = issue_id.map(str::to_string);
        let execution_id = execution_id.map(str::to_string);
        let worktree_path = worktree_path.map(str::to_string);
        let branch = branch.map(str::to_string);
        let status = status.to_string();
        db.execute(
            "INSERT INTO jobs (id, status, project_id, issue_id, execution_id,
                 worktree_path, branch, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, 1)",
            params![
                id.as_str(),
                status.as_str(),
                project_id.as_str(),
                issue_id.as_deref(),
                execution_id.as_deref(),
                worktree_path.as_deref(),
                branch.as_deref()
            ],
        )
        .await
        .unwrap();
    }

    fn sorted_job_ids(target: &TeardownTarget) -> Vec<String> {
        let mut ids = target.job_ids.clone();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn returns_every_distinct_path_deduping_inheritance_fan_out() {
        let db = migrated_db().await;
        seed_project(&db, "p-1", "CAIRN").await;
        seed_issue(&db, "p-1", "i-1", 1).await;
        // builder + create-pr + review all inherit one worktree path.
        seed_job(
            &db,
            "builder",
            "p-1",
            Some("i-1"),
            None,
            Some("/wt/a"),
            Some("agent/x"),
            "complete",
        )
        .await;
        seed_job(
            &db,
            "create-pr",
            "p-1",
            Some("i-1"),
            None,
            Some("/wt/a"),
            Some("agent/x"),
            "complete",
        )
        .await;
        seed_job(
            &db,
            "review",
            "p-1",
            Some("i-1"),
            None,
            Some("/wt/a"),
            Some("agent/x"),
            "complete",
        )
        .await;
        // A second, independent worktree on the same issue (e.g. a re-run).
        seed_job(
            &db,
            "rerun",
            "p-1",
            Some("i-1"),
            None,
            Some("/wt/b"),
            Some("agent/x-2"),
            "complete",
        )
        .await;

        let targets = plan_teardown(&db, &TeardownScope::Issue("i-1".into()))
            .await
            .unwrap();
        assert_eq!(targets.len(), 2, "two distinct paths, fan-out deduped");

        let shared = targets
            .iter()
            .find(|t| t.worktree_path == "/wt/a")
            .expect("shared path present");
        assert_eq!(shared.branch.as_deref(), Some("agent/x"));
        assert_eq!(shared.repo_path, "/tmp/CAIRN");
        assert_eq!(
            sorted_job_ids(shared),
            vec!["builder", "create-pr", "review"],
            "all inheriting jobs carried on the one target"
        );

        let rerun = targets
            .iter()
            .find(|t| t.worktree_path == "/wt/b")
            .expect("rerun path present");
        assert_eq!(sorted_job_ids(rerun), vec!["rerun"]);
    }

    #[tokio::test]
    async fn running_job_does_not_prevent_teardown() {
        // Closing/merging an issue tears down regardless of in-flight jobs:
        // issue-scope teardown is unconditional and kills terminals first.
        let db = migrated_db().await;
        seed_project(&db, "p-1", "CAIRN").await;
        seed_issue(&db, "p-1", "i-1", 1).await;
        seed_job(
            &db,
            "j1",
            "p-1",
            Some("i-1"),
            None,
            Some("/wt/a"),
            Some("agent/x"),
            "running",
        )
        .await;

        let targets = plan_teardown(&db, &TeardownScope::Issue("i-1".into()))
            .await
            .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].worktree_path, "/wt/a");
        assert_eq!(sorted_job_ids(&targets[0]), vec!["j1"]);
    }

    #[tokio::test]
    async fn ignores_jobs_without_a_worktree_path() {
        let db = migrated_db().await;
        seed_project(&db, "p-1", "CAIRN").await;
        seed_issue(&db, "p-1", "i-1", 1).await;
        // A pending child that has not yet copied its worktree path (NULL) is
        // simply not a teardown target — nothing to remove for it.
        seed_job(
            &db,
            "pending",
            "p-1",
            Some("i-1"),
            None,
            None,
            None,
            "pending",
        )
        .await;
        seed_job(
            &db,
            "builder",
            "p-1",
            Some("i-1"),
            None,
            Some("/wt/a"),
            Some("agent/x"),
            "complete",
        )
        .await;

        let targets = plan_teardown(&db, &TeardownScope::Issue("i-1".into()))
            .await
            .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].worktree_path, "/wt/a");
        assert_eq!(sorted_job_ids(&targets[0]), vec!["builder"]);
    }

    #[tokio::test]
    async fn empty_when_issue_has_no_worktrees() {
        let db = migrated_db().await;
        seed_project(&db, "p-1", "CAIRN").await;
        seed_issue(&db, "p-1", "i-1", 1).await;

        let targets = plan_teardown(&db, &TeardownScope::Issue("i-1".into()))
            .await
            .unwrap();
        assert!(targets.is_empty());
    }

    #[tokio::test]
    async fn job_scope_targets_only_that_job() {
        // A Job-scoped teardown (ephemeral task worktree reclaim) selects exactly
        // the named job's worktree, not the whole issue's — the ambient Manager
        // issue never terminalizes, so an issue-scoped sweep would never fire.
        let db = migrated_db().await;
        seed_project(&db, "p-1", "CAIRN").await;
        seed_issue(&db, "p-1", "i-1", 1).await;
        seed_job(
            &db,
            "task",
            "p-1",
            Some("i-1"),
            None,
            Some("/wt/task"),
            Some("agent/task"),
            "complete",
        )
        .await;
        seed_job(
            &db,
            "other",
            "p-1",
            Some("i-1"),
            None,
            Some("/wt/other"),
            Some("agent/other"),
            "running",
        )
        .await;

        let targets = plan_teardown(&db, &TeardownScope::Job("task".into()))
            .await
            .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].worktree_path, "/wt/task");
        assert_eq!(sorted_job_ids(&targets[0]), vec!["task"]);
    }

    /// The merged-teardown guard resolves a source branch's merge target through
    /// three tiers: the MR's recorded `target_branch`, then a producing job's
    /// `base_branch`, then the project default branch.
    #[tokio::test]
    async fn resolve_merge_target_prefers_mr_then_base_then_default() {
        let db = migrated_db().await;
        seed_project(&db, "p-1", "CAIRN").await;
        seed_issue(&db, "p-1", "i-1", 1).await;

        // Tier 1: the MR's recorded target branch wins.
        db.execute(
            "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-1', 'j-mr', 'p-1', 'i-1', 'PR', 'agent/x', 'integration', 'open', 1, 1)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            resolve_merge_target_for_source(&db, "i-1", "agent/x", &["j-mr".to_string()]).await,
            Some("integration".to_string())
        );

        // Tier 2: no MR for this source — fall back to the job's base_branch.
        db.execute(
            "INSERT INTO jobs (id, status, project_id, issue_id, branch, base_branch, created_at, updated_at)
             VALUES ('j-base', 'complete', 'p-1', 'i-1', 'agent/y', 'some-base', 1, 1)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            resolve_merge_target_for_source(&db, "i-1", "agent/y", &["j-base".to_string()]).await,
            Some("some-base".to_string())
        );

        // Tier 3: no MR and no base_branch — fall back to the project default.
        assert_eq!(
            resolve_merge_target_for_source(&db, "i-1", "agent/z", &[]).await,
            Some("main".to_string())
        );
    }

    /// A half-deleted worktree (its `.jj` already gone, so not a jj dir, and not a
    /// registered git worktree) is removed by the rename-then-delete fallback,
    /// leaving no tombstone behind on success.
    #[test]
    fn remove_dir_tombstoned_removes_and_leaves_no_tombstone() {
        let base = tempfile::tempdir().unwrap();
        let wt = base.path().join("CAIRN-1-builder-0");
        std::fs::create_dir_all(wt.join("target")).unwrap();
        std::fs::write(wt.join("target").join("a.o"), b"x").unwrap();

        remove_dir_tombstoned(&wt, base.path()).unwrap();

        assert!(!wt.exists(), "worktree dir removed");
        let leftover: Vec<_> = std::fs::read_dir(base.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".trash-"))
            .collect();
        assert!(leftover.is_empty(), "no tombstone left on success");
    }

    /// The base-dir guard refuses to delete a path outside the managed worktrees
    /// base, so a misconfigured `worktree_path` can never wipe an arbitrary dir.
    #[test]
    fn remove_dir_tombstoned_rejects_path_outside_base() {
        let base = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let victim = outside.path().join("precious");
        std::fs::create_dir_all(&victim).unwrap();

        let err = remove_dir_tombstoned(&victim, base.path()).unwrap_err();
        assert!(err.contains("outside managed base"), "got: {err}");
        assert!(victim.exists(), "path outside base is left untouched");
    }

    /// A tombstone left by a failed delete is itself removable by a later pass
    /// (the GC re-invokes the same guarded remover on `*.trash-*` leftovers).
    #[test]
    fn remove_dir_tombstoned_removes_a_prior_tombstone() {
        let base = tempfile::tempdir().unwrap();
        let tombstone = base.path().join("CAIRN-1-builder-0.trash-123");
        std::fs::create_dir_all(&tombstone).unwrap();

        remove_dir_tombstoned(&tombstone, base.path()).unwrap();
        assert!(!tombstone.exists());
    }

    #[test]
    fn worktree_list_contains_matches_registered_path() {
        let dir = tempfile::tempdir().unwrap();
        let wt = dir.path().join("feature-x");
        std::fs::create_dir_all(&wt).unwrap();
        let porcelain = format!(
            "worktree {}\nHEAD abc123\nbranch refs/heads/agent/x\n",
            wt.display()
        );
        assert!(worktree_list_contains(&porcelain, &wt));
        assert!(!worktree_list_contains(
            &porcelain,
            &dir.path().join("other")
        ));
    }

    /// The shared per-target cleanup (used by both teardown and the worktree GC)
    /// reaps processes rooted in the worktree AND clears tracked terminal rows.
    /// The GC path reaches cleanup only through `execute_target_cleanup`, so this
    /// guards both behaviours it lacked before CAIRN-2390.
    #[tokio::test]
    async fn execute_target_cleanup_reaps_and_clears_terminals() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::{MockGitClient, RecordingReaper, TestServicesBuilder};
        use crate::storage::SearchIndex;
        use std::sync::Arc;

        // A worktree path under the managed base so the reaper guard admits it.
        // The dir is never created, so removal is a no-op and the RecordingReaper
        // never touches a real process — nothing here has filesystem side effects.
        let Some(base) = worktrees_base_dir() else {
            return; // no resolvable home dir in this environment; skip.
        };
        let wt = base.join("CAIRN-2390-cleanup-test-0");
        let wt_path = wt.to_string_lossy().into_owned();

        let db = migrated_db().await;
        seed_project(&db, "p-1", "CAIRN").await;
        seed_issue(&db, "p-1", "i-1", 1).await;
        seed_job(
            &db,
            "j1",
            "p-1",
            Some("i-1"),
            None,
            Some(&wt_path),
            Some("agent/x"),
            "complete",
        )
        .await;
        // A running terminal row the GC path must delete.
        db.execute(
            "INSERT INTO job_terminals (id, job_id, session_id, command, status, created_at, slug)
             VALUES ('t1', 'j1', 's1', 'bun dev', 'running', 1, 'dev')",
            (),
        )
        .await
        .unwrap();

        let reaper = RecordingReaper::returning(vec![4242]);
        // The only external service call in this path is `git worktree prune`.
        let mut git = MockGitClient::new();
        git.expect_worktree_prune().returning(|_| Ok(()));

        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index =
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_reaper(reaper.clone())
                .with_git(git)
                .build(),
        );
        let orch = OrchestratorBuilder::new(db_state.clone(), services, config_dir).build();

        let target = TeardownTarget {
            worktree_path: wt_path.clone(),
            branch: Some("agent/x".into()),
            repo_path: "/tmp/CAIRN".into(),
            job_ids: vec!["j1".into()],
        };
        execute_target_cleanup(&orch, &db_state.local, &target).await;

        // The reaper was invoked with the worktree path (the base guard admitted
        // it), proving the GC path now reaps rooted processes.
        assert_eq!(reaper.calls(), vec![wt]);

        // The running terminal row was cleared, proving the GC path now runs the
        // job-scoped terminal cleanup it previously skipped.
        let running = load_running_terminals_for_jobs(&db_state.local, &["j1".to_string()])
            .await
            .unwrap();
        assert!(
            running.is_empty(),
            "GC path must clear tracked terminal rows, found {running:?}"
        );
    }
}
