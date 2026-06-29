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
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Remove a job's working directory, dispatching on whether it is a jj workspace
/// (forget it from the shared store, then delete the dir — it is not a git
/// worktree of the repo) or a plain git worktree. Idempotent and best-effort.
fn remove_job_worktree(
    orch: &Orchestrator,
    repo_path: &str,
    wt_path: &Path,
    branch: Option<&str>,
) -> Result<(), String> {
    if crate::jj::is_jj_dir(wt_path) {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
        if let Some(b) = branch {
            let _ = crate::jj::forget_workspace(&jj, &store, b);
        }
        if wt_path.exists() {
            std::fs::remove_dir_all(wt_path)
                .map_err(|e| format!("failed to remove jj workspace dir: {e}"))?;
        }
        Ok(())
    } else {
        crate::git::worktree::remove_worktree_with_services(
            &*orch.services.git,
            &*orch.services.fs,
            Path::new(repo_path),
            wt_path,
            true,
        )
    }
}

/// Which jobs a teardown pass considers.
pub enum TeardownScope {
    /// All jobs across an issue (PR merge/close, issue close/delete).
    Issue(String),
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
    };

    // `scope_col` is a fixed literal from the match above, never user input.
    let sql = format!(
        "SELECT j.id, j.worktree_path, j.branch, p.repo_path
         FROM jobs j
         JOIN projects p ON j.project_id = p.id
         WHERE j.{scope_col} = ?1 AND j.worktree_path IS NOT NULL"
    );

    // worktree_path -> (branch, repo_path, job_ids). BTreeMap keeps output
    // deterministic (sorted by path) for stable tests.
    type Grouped = BTreeMap<String, (Option<String>, String, Vec<String>)>;
    let grouped: Grouped = db
        .read(|conn| {
            let sql = sql.clone();
            let scope_id = scope_id.clone();
            Box::pin(async move {
                let mut rows = conn.query(sql.as_str(), (scope_id.as_str(),)).await?;
                let mut grouped: Grouped = BTreeMap::new();
                while let Some(row) = rows.next().await? {
                    let job_id = row.text(0)?;
                    let worktree_path = row.text(1)?;
                    let branch = row.opt_text(2)?;
                    let repo_path = row.text(3)?;
                    let entry = grouped
                        .entry(worktree_path)
                        .or_insert_with(|| (branch.clone(), repo_path, Vec::new()));
                    if entry.0.is_none() {
                        entry.0 = branch;
                    }
                    entry.2.push(job_id);
                }
                Ok(grouped)
            })
        })
        .await
        .map_err(|e| format!("Failed to load teardown candidates: {e}"))?;

    let targets = grouped
        .into_iter()
        .map(
            |(worktree_path, (branch, repo_path, job_ids))| TeardownTarget {
                worktree_path,
                branch,
                repo_path,
                job_ids,
            },
        )
        .collect();
    Ok(targets)
}

/// The single teardown owner: plan the issue's worktree paths, then kill their
/// terminals, remove the worktrees, and delete local + remote branches.
///
/// Reached only at issue/PR-terminal transitions (PR merge/close, issue
/// close/merge, issue delete). Issue-wide and unconditional — the work is done
/// by the time any of these fire.
pub async fn teardown_worktrees(orch: &Orchestrator, scope: TeardownScope) -> Result<(), String> {
    // Route to the issue's owning database: a team issue's jobs (and their
    // worktree_path rows, terminals, and browsers) live wholly in its synced
    // replica, so planning and cleanup must read/write there or the team's
    // worktrees leak and its terminal/browser rows are never cleared.
    let TeardownScope::Issue(issue_id) = &scope;
    let db = crate::issues::crud::owning_db_for_issue(&orch.db, issue_id)
        .await
        .map_err(|e| e.to_string())?;
    let targets = plan_teardown(&db, &scope).await?;
    if targets.is_empty() {
        return Ok(());
    }

    // Kill terminals owned by any job referencing a torn-down worktree.
    let job_ids: Vec<String> = targets
        .iter()
        .flat_map(|target| target.job_ids.iter().cloned())
        .collect();
    kill_terminals_for_jobs(orch, &db, &job_ids).await;

    // Remove worktrees + delete local branches; collect branches per repo.
    let mut branches_by_repo: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for target in &targets {
        // Archive the execution's at-risk events to git coordinates (with a zstd
        // backstop) before the worktree and branch disappear at teardown, the
        // immutability boundary. Strictly best-effort: any failure logs a warning
        // and rolls back (rows stay `full`) — archival must never block teardown.
        // Construct the jj driver unconditionally; archival's forward-mapping
        // helpers self-gate on `is_jj_dir`, so a plain-git worktree is a clean
        // no-op (identity coordinates, no change-id) while a jj workspace gets
        // its commits forward-mapped past any auto-rebase.
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        match crate::archival::archive_target(
            &db,
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
                // Drift tripwire: reads resolved their bytes but every one failed
                // the render-and-compare. The expected signature of a live/archival
                // read-composition divergence (the CAIRN-1676 regression) on the
                // day it lands — surfaced here so it never hides behind a green run.
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
        match remove_job_worktree(orch, &target.repo_path, &wt_path, target.branch.as_deref()) {
            Ok(()) => log::info!("Teardown: removed worktree {}", target.worktree_path),
            Err(e) => log::warn!(
                "Teardown: failed to remove worktree {}: {}",
                target.worktree_path,
                e
            ),
        }

        // Reclaim each referencing job's scratch dir alongside the worktree.
        // Worktrees aren't 1:1 with jobs (inheritance fan-out), so a target may
        // carry several job ids; remove every one's scratch dir. Idempotent and
        // best-effort (a missing dir is fine).
        for job_id in &target.job_ids {
            crate::scratch::remove_job_scratch_dir(job_id);
        }

        if let Some(branch) = &target.branch {
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
    }
    if targets.is_empty() {
        return;
    }

    for target in targets {
        let wt_path = PathBuf::from(&target.worktree_path);
        match remove_job_worktree(orch, &target.repo_path, &wt_path, target.branch.as_deref()) {
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
    use turso::params;

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
}
