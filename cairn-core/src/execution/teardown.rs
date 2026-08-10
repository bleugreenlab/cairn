//! Coordinate and owner-resource cleanup at issue and project lifecycle boundaries.
//!
//! Agent jobs have no filesystem checkout to follow or remove. Durable branches
//! remain the recoverable history, scratch is disposable process residence, and
//! terminals/REPLs are executor lifetime resources released by their owner fence.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) enum TeardownScope {
    Issue(String),
}

/// Release the issue's dev-instance process trees while terminal resolution can
/// still refuse to commit. The later full cleanup repeats this idempotently.
pub(crate) async fn release_issue_dev_instances(
    orch: &Orchestrator,
    issue_id: &str,
) -> Result<(), String> {
    let db = crate::issues::crud::owning_db_for_issue(&orch.db, issue_id)
        .await
        .map_err(|error| error.to_string())?;
    let (job_ids, targets) = issue_targets(&db, issue_id).await?;
    let mut dev_targets: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for target in targets {
        dev_targets
            .entry(target.project_id)
            .or_default()
            .push(target.branch);
    }
    for (project_id, branches) in dev_targets {
        crate::dev_instances::release_issue_instances(orch, &project_id, &job_ids, &branches)
            .await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownReason {
    Merged,
    Discarded,
}

#[derive(Debug, Clone)]
struct BranchTarget {
    project_id: String,
    branch: String,
    repo_path: String,
    job_ids: Vec<String>,
}

async fn issue_targets(
    db: &LocalDb,
    issue_id: &str,
) -> Result<(Vec<String>, Vec<BranchTarget>), String> {
    let issue_id = issue_id.to_string();
    let rows: Vec<(String, String, Option<String>, String)> = db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT j.id, j.project_id, j.branch, p.repo_path FROM jobs j JOIN projects p ON p.id = j.project_id WHERE j.issue_id = ?1",
                (issue_id.as_str(),),
            ).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?, row.opt_text(2)?, row.text(3)?));
            }
            Ok(out)
        })
    }).await.map_err(|e| format!("Failed to load issue job coordinates: {e}"))?;

    let job_ids = rows.iter().map(|row| row.0.clone()).collect();
    let mut grouped: BTreeMap<(String, String, String), Vec<String>> = BTreeMap::new();
    for (job_id, project_id, branch, repo_path) in rows {
        if let Some(branch) = branch.filter(|branch| !branch.is_empty()) {
            grouped
                .entry((project_id, repo_path, branch))
                .or_default()
                .push(job_id);
        }
    }
    let targets = grouped
        .into_iter()
        .map(|((project_id, repo_path, branch), job_ids)| BranchTarget {
            project_id,
            branch,
            repo_path,
            job_ids,
        })
        .collect();
    Ok((job_ids, targets))
}

pub(crate) async fn cleanup_issue_jobs(
    orch: &Orchestrator,
    scope: TeardownScope,
    reason: TeardownReason,
) -> Result<(), String> {
    let TeardownScope::Issue(issue_id) = scope;
    let db = crate::issues::crud::owning_db_for_issue(&orch.db, &issue_id)
        .await
        .map_err(|e| e.to_string())?;
    let (job_ids, targets) = issue_targets(&db, &issue_id).await?;

    kill_terminals_for_jobs(orch, &db, &job_ids).await;
    kill_repls_for_jobs(orch, &job_ids).await;
    let mut dev_targets: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for target in &targets {
        dev_targets
            .entry(target.project_id.clone())
            .or_default()
            .push(target.branch.clone());
    }
    for (project_id, branches) in dev_targets {
        crate::dev_instances::release_issue_instances(orch, &project_id, &job_ids, &branches)
            .await?;
    }
    for job_id in &job_ids {
        crate::scratch::remove_job_scratch_dir(job_id);
    }

    let mut branches_by_repo: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for target in targets {
        let delete_branch = match reason {
            TeardownReason::Discarded => true,
            TeardownReason::Merged => {
                match resolve_merge_target_for_source(
                    &db,
                    &issue_id,
                    &target.branch,
                    &target.job_ids,
                )
                .await
                {
                    Some(merge_target) if merge_target == target.branch => true,
                    Some(merge_target) => {
                        match branch_landed(orch, &target.repo_path, &target.branch, &merge_target)
                        {
                            Ok(true) => true,
                            Ok(false) => {
                                log::warn!(
                                    "Teardown: preserving unlanded branch `{}` for merged issue {}",
                                    target.branch,
                                    issue_id
                                );
                                false
                            }
                            Err(error) => {
                                log::warn!("Teardown: preserving branch `{}` because landed-state resolution failed: {}", target.branch, error);
                                false
                            }
                        }
                    }
                    None => {
                        log::warn!("Teardown: preserving branch `{}` because its merge target is unresolved", target.branch);
                        false
                    }
                }
            }
        };
        if delete_branch {
            if let Err(error) = crate::git::branch::delete_with_services(
                &*orch.services.git,
                Path::new(&target.repo_path),
                &target.branch,
            ) {
                log::warn!(
                    "Teardown: failed to delete local branch {}: {}",
                    target.branch,
                    error
                );
            }
            branches_by_repo
                .entry(target.repo_path)
                .or_default()
                .push(target.branch);
        }
    }
    for (repo_path, mut branches) in branches_by_repo {
        branches.sort();
        branches.dedup();
        delete_remote_branches_for_repo(orch, &repo_path, &branches).await;
    }
    Ok(())
}

/// Stop resources owned by a project's jobs before their rows are deleted.
pub async fn cleanup_project_jobs(orch: &Orchestrator, project_id: &str) -> Result<(), String> {
    let project_id = project_id.to_string();
    let job_ids: Vec<String> = orch
        .db
        .local
        .read(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs WHERE project_id = ?1",
                        (project_id.as_str(),),
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                Ok(ids)
            })
        })
        .await
        .map_err(|e| format!("Failed to load project jobs for cleanup: {e}"))?;
    kill_terminals_for_jobs(orch, &orch.db.local, &job_ids).await;
    kill_repls_for_jobs(orch, &job_ids).await;
    for job_id in &job_ids {
        crate::scratch::remove_job_scratch_dir(job_id);
    }
    Ok(())
}

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

/// Kill every live interpreter belonging to these jobs. The durable rows stay:
/// teardown ends the process, not the record, so the transcript remains readable
/// until the job's rows cascade away with it.
async fn kill_repls_for_jobs(orch: &Orchestrator, job_ids: &[String]) {
    let mut killed = false;
    for (_job_id, slug, session) in orch.repl_state.remove_for_jobs(job_ids) {
        let repl_id = session.repl_id.clone();
        let generation = session.generation;
        session.stop_and_release(orch).await;
        if let Err(error) = crate::mcp::handlers::repl::store::mark_exited(
            &orch.db.local,
            &repl_id,
            generation,
            crate::mcp::handlers::repl::store::ReplExitReason::Closed,
        )
        .await
        {
            tracing::warn!(%error, %slug, "failed to mark torn-down REPL exited");
        }
        killed = true;
    }
    if killed {
        crate::mcp::handlers::repl::emit_repl_change(orch, "update");
    }
}

/// Kill running PTY sessions for the given jobs and delete their terminal rows.
///
/// Uses the runner's terminal bindings to issue fenced executor stop/release
/// operations before clearing durable rows.
async fn kill_terminals_for_jobs(orch: &Orchestrator, db: &LocalDb, job_ids: &[String]) {
    if job_ids.is_empty() {
        return;
    }

    // Stop live executor-hosted PTYs before releasing their lease authority.
    match load_running_terminals_for_jobs(db, job_ids).await {
        Ok(running) => {
            for (terminal_id, session_id) in &running {
                if let Err(error) =
                    crate::mcp::handlers::terminal::stop_terminal_by_session(orch, session_id).await
                {
                    log::warn!("Teardown: failed to stop terminal {terminal_id}: {error}");
                } else {
                    log::info!("Teardown: stopped terminal {terminal_id} (session {session_id})");
                }
            }
        }
        Err(e) => {
            log::warn!("Teardown: failed to load running terminals: {}", e);
        }
    }

    // A job's terminals are processes inside the job's own execution
    // environment, so there is no separate terminal-owned thing to release: the
    // shells were stopped above, and releasing the job's residency gives back
    // the cell, its checkout, and its footprint in one move. A job holds that
    // residency for as long as it runs, whether or not it ever opened a
    // terminal, so this releases by job identity rather than by what the
    // terminal table happens to record.
    for job_id in job_ids {
        let holder = cairn_common::executor_protocol::ResidencyHolder::Job {
            job_id: job_id.clone(),
        };
        let Some(fence) = orch.fleet.residency_fence(&holder) else {
            continue;
        };
        let result = orch
            .fleet
            .operate_residency(
                orch,
                cairn_common::executor_protocol::ResidencyOperation::Release { fence },
            )
            .await;
        if let cairn_common::executor_protocol::ResidencyResult::Failed { diagnostic, .. } = result
        {
            log::warn!(
                "Teardown: failed to release the execution environment for {job_id}: {diagnostic}"
            );
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
                orch.browser_network.clear(&browser.id);
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
    let auth = match crate::security::broker::github::installation_authority(&orch.db.local, &owner)
        .await
    {
        Ok(auth) => auth,
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
        &auth,
        &owner,
        &repo,
        branches,
    )
    .await;
}
