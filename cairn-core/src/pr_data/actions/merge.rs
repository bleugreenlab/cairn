//! Merge orchestration: the conflict gate, GitHub-vs-local routing, merge-method
//! resolution, and post-merge reconciliation.

use crate::execution::teardown::{teardown_worktrees, TeardownReason, TeardownScope};
use crate::github::api;
use crate::github::credentials::{get_credentials_for_owner, get_owner_repo};
use crate::orchestrator::Orchestrator;
use crate::pr_data::helpers::{
    assert_main_checkout_clean_for_default_merge, local_pr_files,
    reconcile_main_checkout_after_merge,
};
use crate::storage::LocalDb;
use cairn_db::turso::params;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

use super::conflict::{conflict_recovery_hint, format_conflicted_commits, source_conflict_report};
use super::context::{
    db_error, resolve_merge_mr_context_for_job, MergeMrContext, PrNodeResolution,
};
use super::refresh::refresh_pr_for_job;
use super::resolution::{persist_merged_commit, resolve_pr_node};
use super::store_merge::{
    land_verified_store_merge_child, prospective_store_merge_child, store_merge_child,
    VerifiedLanding,
};

/// Resolve a PR owner id to the `jobs.id` used by `file_changes.job_id`.
async fn resolve_file_change_job_id(
    db: &LocalDb,
    owner_id: &str,
) -> Result<Option<String>, String> {
    let owner_id = owner_id.to_string();
    db.query_opt_text(
        "SELECT id
         FROM (
             SELECT j.id AS id, 0 AS priority
             FROM jobs j
             WHERE j.id = ?1
             UNION ALL
             SELECT ar.parent_job_id AS id, 1 AS priority
             FROM action_runs ar
             JOIN jobs parent ON parent.id = ar.parent_job_id
             WHERE ar.id = ?1 AND ar.parent_job_id IS NOT NULL
         )
         ORDER BY priority
         LIMIT 1",
        params![owner_id.as_str()],
    )
    .await
    .map_err(|e| db_error("Failed to resolve file-change job owner", e))
}

/// Store file changes from a merged PR for issue history tracking.
async fn store_file_changes(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    files: &[api::PrFile],
) -> Result<(), String> {
    let job_id = job_id.to_string();
    let files = files.to_vec();
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let job_id = job_id.clone();
        let files = files.clone();
        Box::pin(async move {
            for file in files {
                let mut rows = conn
                    .query(
                        "SELECT id
                         FROM file_changes
                         WHERE job_id = ?1 AND file_path = ?2
                         LIMIT 1",
                        params![job_id.as_str(), file.filename.as_str()],
                    )
                    .await?;
                let existing_id = crate::storage::next_text(&mut rows, 0).await?;
                drop(rows);

                if let Some(existing_id) = existing_id {
                    conn.execute(
                        "UPDATE file_changes
                         SET status = ?1, additions = ?2, deletions = ?3, previous_path = ?4
                         WHERE id = ?5",
                        params![
                            file.status.as_str(),
                            file.additions,
                            file.deletions,
                            file.previous_filename.as_deref(),
                            existing_id.as_str()
                        ],
                    )
                    .await?;
                } else {
                    let id = Uuid::new_v4().to_string();
                    conn.execute(
                        "INSERT INTO file_changes (
                            id, job_id, file_path, status, additions, deletions,
                            previous_path, created_at
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            id.as_str(),
                            job_id.as_str(),
                            file.filename.as_str(),
                            file.status.as_str(),
                            file.additions,
                            file.deletions,
                            file.previous_filename.as_deref(),
                            now
                        ],
                    )
                    .await?;
                }
            }

            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to store file changes", e))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "file_changes", "action": "insert"}),
    );

    Ok(())
}

/// Resolve `pull_on_merge` from workspace settings via the core config loader.
fn pull_on_merge_setting() -> bool {
    crate::config::get_config_dir()
        .map(|dir| crate::config::settings::load_settings(&dir).pull_on_merge)
        .unwrap_or(true)
}

/// Background post-merge reconciliation shared by the in-app merge
/// (`merge_pr_for_job`) and the GitHub merge path.
///
/// Runs the side effects that follow a PR landing, in order:
/// 1. capture the merged file list for issue history,
/// 2. reconcile the user's main checkout only when the merge advanced the project
///    default branch,
/// 3. tear down the merged issue's worktrees,
/// 4. refresh the cached PR row.
///
/// Every step is non-fatal: each logs and continues on failure so one broken
/// step never strands the rest. The PR-merged state transition itself
/// (`resolve_pr_node`, which also fires base-advance notifications, plus the
/// caller's attention emit) has already happened by the time this runs. Takes
/// owned values so callers can spawn it detached via `tokio::spawn`.
/// `force_checkout_pull` forces the user's main-checkout fast-forward pull
/// regardless of the `pull_on_merge` setting. The GitHub-API merge path sets it
/// for default-branch PRs: nothing locally rewrote the checkout (no local fold),
/// so a `git pull` of the PR base branch is the only way the checkout catches up
/// to the just-merged tip. The local-fold path passes `false` — the fold already
/// advanced the local default ref, and `pull_on_merge` governs only the extra
/// pull.
fn should_reconcile_main_checkout_after_merge(
    target_branch: &str,
    resolved_default_branch: &str,
) -> bool {
    target_branch == resolved_default_branch
}

pub async fn reconcile_after_merge(
    orch: Orchestrator,
    db: Arc<LocalDb>,
    ctx: MergeMrContext,
    force_checkout_pull: bool,
) {
    let owner_id = ctx.mr.job_id.clone();
    let repo_path = ctx.mr.repo_path.clone();
    let pr_number = ctx.mr.github_pr_number;
    let default_branch = ctx.default_branch.clone();
    let resolved_default_branch = resolve_project_default_branch(&repo_path, &default_branch);
    let target_branch = ctx.target_branch.clone();
    let issue_id = ctx.issue_id.clone();

    log::info!(
        "Starting post-merge reconciliation for owner {} (target branch '{}')",
        owner_id,
        target_branch
    );

    let file_change_job_id = match resolve_file_change_job_id(&db, &owner_id).await {
        Ok(job_id) => job_id,
        Err(e) => {
            log::warn!(
                "Skipping post-merge file capture for owner {}: {}",
                owner_id,
                e
            );
            None
        }
    };
    if file_change_job_id.is_none() {
        log::warn!(
            "Skipping post-merge file capture for owner {}: no jobs.id owner found",
            owner_id
        );
    }

    // 1. Capture file changes for issue history.
    if let Some(pr_number) = pr_number {
        match get_owner_repo(&repo_path) {
            Ok((owner, repo)) => match get_credentials_for_owner(&orch.db.local, &owner).await {
                Ok(creds) => {
                    let http = &*orch.services.http;
                    match api::fetch_pr_files(http, &creds, &owner, &repo, pr_number).await {
                        Ok(files) => {
                            if let Some(file_change_job_id) = file_change_job_id.as_deref() {
                                let count = files.len();
                                if let Err(e) =
                                    store_file_changes(&orch, &db, file_change_job_id, &files).await
                                {
                                    log::warn!("Failed to store file changes: {}", e);
                                } else {
                                    log::info!(
                                        "Stored {} file changes for owner {} under job {}",
                                        count,
                                        owner_id,
                                        file_change_job_id
                                    );
                                }
                            }
                        }
                        Err(e) => log::warn!("Failed to fetch PR files for history: {}", e),
                    }
                }
                Err(e) => log::warn!("Skipping post-merge file capture (credentials): {}", e),
            },
            Err(e) => log::warn!("Skipping post-merge file capture (owner/repo): {}", e),
        }
    } else {
        match local_pr_files(
            &*orch.services.git,
            Path::new(&repo_path),
            &ctx.target_branch,
            &ctx.source_branch,
        ) {
            Ok(files) => {
                if let Some(file_change_job_id) = file_change_job_id.as_deref() {
                    if let Err(e) = store_file_changes(&orch, &db, file_change_job_id, &files).await
                    {
                        log::warn!("Failed to store local file changes: {}", e);
                    } else {
                        log::info!(
                            "Stored {} local file changes for owner {} under job {}",
                            files.len(),
                            owner_id,
                            file_change_job_id
                        );
                    }
                }
            }
            Err(e) => log::warn!("Failed to capture local file changes: {}", e),
        }
    }

    // 2. Reconcile the user's project checkout only for a real default-branch
    //    advance. Child→integration merges never detach or move the main checkout;
    //    resetting it there only creates races on unrelated work.
    if should_reconcile_main_checkout_after_merge(&target_branch, &resolved_default_branch) {
        let git = &*orch.services.git;
        let pull = force_checkout_pull || (pr_number.is_some() && pull_on_merge_setting());
        if let Err(e) =
            reconcile_main_checkout_after_merge(git, &repo_path, &resolved_default_branch, pull)
        {
            log::warn!("Failed to reconcile main checkout after merge: {}", e);
        }
    } else {
        log::debug!(
            "Skipping main checkout reconcile after merge into non-default target '{}' (resolved default '{}')",
            target_branch,
            resolved_default_branch
        );
    }

    // 3. Downstream-workspace reconciliation is spawned (not awaited) inside
    //    `resolve_pr_node`, so it runs in the background off the user-facing merge
    //    path, in `base_advance::reconcile_jj_downstream`. It does two asymmetric
    //    things over the shared store: in-flight SIBLINGS (branched FROM the
    //    integration branch) auto-rebase onto the advanced tip, and the workspace
    //    whose branch IS the integration branch (a Coordinator) has its `@`
    //    re-parented onto the folded tip via `crate::jj::advance_workspace_onto` —
    //    the jj-native restoration of the old git "post-merge fast-forward of
    //    active worktrees". So there is no git worktree fast-forward step here.

    // 4. Tear down worktrees and branches for the merged issue (issue-wide).
    //    Scoped to the merged PR's issue, so a Coordinator worktree on a
    //    different issue survives and the step-3 fast-forward sticks.
    if let Some(issue_id) = issue_id.as_deref() {
        // A merge that reaches here already ran a verified fold (the merge
        // postcondition) or an out-of-band GitHub squash — the content is
        // incorporated, so branches are cleaned up unconditionally. The
        // preserve-unlanded guard belongs on the record-only status path, not
        // here, where an ancestor test would misfire on a legitimate squash.
        if let Err(e) = teardown_worktrees(
            &orch,
            TeardownScope::Issue(issue_id.to_string()),
            TeardownReason::Discarded,
        )
        .await
        {
            log::warn!("Worktree teardown after merge failed: {}", e);
        }
    }

    // 5. Refresh PR details.
    let _ = refresh_pr_for_job(&orch, &owner_id).await;

    log::info!("Post-merge reconciliation completed for owner {}", owner_id);
}

/// Resolve the effective merge method for a PR. Squash is the default shape for
/// a normal PR landing on the default branch (one commit per PR), but workspace
/// PRs deliberately preserve their individual commits, so they always force
/// `"merge"` regardless of the requested method.
fn effective_merge_method(merge_context: &MergeMrContext, merge_method: Option<String>) -> String {
    let force_merge = merge_context.is_workspace;
    if force_merge {
        "merge".to_string()
    } else {
        merge_method.unwrap_or_else(|| "squash".to_string())
    }
}

/// The merge method a DEFAULT-BRANCH landing will actually use, resolved per
/// route: the GitHub route (`merge_remote_pr_via_github`) passes the caller's
/// method straight through (defaulting to squash), while the local fold applies
/// the workspace forcing (`effective_merge_method`). Only
/// meaningful when the target IS the default branch. The merge gate uses this to
/// refuse a clean-tip / conflicted-intermediate source under a non-`squash`
/// (preserve-every-commit) landing on BOTH routes uniformly — a squash landing
/// flattens the intermediate away, a preserve landing cannot.
fn default_landing_method(
    ctx: &MergeMrContext,
    resolved_default: &str,
    merge_method: Option<String>,
) -> String {
    if should_route_to_github(ctx, resolved_default) {
        merge_method.unwrap_or_else(|| "squash".to_string())
    } else {
        effective_merge_method(ctx, merge_method)
    }
}

/// Resolve a project's effective (config-aware) default branch from its stored
/// value. Mirrors how worktree creation resolves the default, so the merge
/// routing and the post-merge checkout reconcile can never disagree with where
/// worktrees were based — stopping short of trusting the raw DB column the merge
/// used to read directly.
fn resolve_project_default_branch(repo_path: &str, stored_default: &str) -> String {
    let config = crate::config::project_settings::load_project_settings(Path::new(repo_path));
    crate::config::project_settings::resolve_default_branch(&config, Some(stored_default))
}

/// Whether this merge should go through GitHub's merge API rather than the local
/// jj fold. Pure so the routing is unit-testable without an `Orchestrator`:
/// `resolved_default` is the config-aware default resolved by the caller.
///
/// True only for a real remote PR (`github_pr_number` present, not a local-only
/// project) that lands on the default branch and is not a workspace PR. A
/// Coordinator child→integration PR (`target_branch` is the integration branch,
/// not the default) and a local-only project both stay on the local fold;
/// workspace PRs that force the keep-every-commit `merge` method are out of
/// scope and also stay local.
fn should_route_to_github(ctx: &MergeMrContext, resolved_default: &str) -> bool {
    ctx.mr.github_pr_number.is_some()
        && !ctx.mr.is_local
        && !ctx.is_workspace
        && ctx.target_branch == resolved_default
}

/// `should_route_to_github` against the project's config-aware default branch.
fn should_merge_via_github(ctx: &MergeMrContext) -> bool {
    let resolved_default = resolve_project_default_branch(&ctx.mr.repo_path, &ctx.default_branch);
    should_route_to_github(ctx, &resolved_default)
}

/// Map a `github::api::merge_pr` failure to user-facing guidance. GitHub returns
/// 405 ("not mergeable") or 409 (head changed / merge conflict) when it refuses
/// the merge; in that case the source was clean *locally* (the conflict gate
/// passed), so there are no local markers to point at — the guidance points at
/// the PR instead, distinct from the local-fold conflict message.
fn map_github_merge_error(source_branch: &str, target_branch: &str, error: String) -> String {
    if error.contains("PR: 405") || error.contains("PR: 409") {
        format!(
            "GitHub refused the merge of `{source_branch}` into `{target_branch}` — the PR has conflicts or failing required checks on GitHub's side. Resolve them on the PR, then retry the merge. (GitHub: {error})"
        )
    } else {
        format!("Failed to merge `{source_branch}` into `{target_branch}` via GitHub: {error}")
    }
}

/// Merge a remote PR through GitHub's merge API, then reconcile locally.
///
/// GitHub performs the squash-merge, closes the PR, and advances the base branch
/// on origin. The ordering is fail-closed: the GitHub call is the load-bearing
/// step, and only once it succeeds do we mark the merge request merged / resolve
/// the issue locally (`resolve_pr_node`). Local reconciliation is then proactive
/// and best-effort — origin is already authoritative, the GitHub `push` webhook
/// and the local sweep also perform this work, and the before/after commit-id
/// guards in `base_advance` make the double-fire a no-op.
async fn merge_remote_pr_via_github(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    job_id: &str,
    merge_context: MergeMrContext,
    merge_method: Option<String>,
    merge_started: std::time::Instant,
) -> Result<String, String> {
    let repo_path = merge_context.mr.repo_path.clone();
    let issue_id = merge_context.issue_id.clone();
    let source_branch = merge_context.source_branch.clone();
    let target_branch = merge_context.target_branch.clone();
    let pr_number = merge_context
        .mr
        .github_pr_number
        .ok_or_else(|| "GitHub merge requires a PR number".to_string())?;

    // Squash is the default landing shape (one commit per PR on the default
    // branch). The workspace forcing in `effective_merge_method` does not apply
    // here — workspace PRs never take this branch.
    let method = merge_method.unwrap_or_else(|| "squash".to_string());

    let (owner, repo) = get_owner_repo(&repo_path)?;
    let creds = get_credentials_for_owner(&orch.db.local, &owner).await?;
    let http = &*orch.services.http;

    // FAIL-CLOSED: on any failure, surface it and mark/advance NOTHING locally.
    let github_started = std::time::Instant::now();
    api::merge_pr(http, &creds, &owner, &repo, pr_number, &method)
        .await
        .map_err(|e| map_github_merge_error(&source_branch, &target_branch, e))?;
    log::info!(
        "merge_pr_for_job[{job_id}]: GitHub merge_pr took {:?}",
        github_started.elapsed()
    );

    // GitHub merged: mark the merge request merged, resolve the issue, close
    // sessions. Runs only after the GitHub call succeeded.
    let resolve_started = std::time::Instant::now();
    let closed_sessions = resolve_pr_node(orch, job_id, PrNodeResolution::Merge)
        .await
        .map_err(|error| {
            log::error!(
                "GitHub merged PR but failed to mark merge request merged for job {job_id}: {error}"
            );
            error
        })?;
    log::info!(
        "merge_pr_for_job[{job_id}]: resolve_pr_node took {:?}",
        resolve_started.elapsed()
    );
    for session_id in &closed_sessions {
        orch.process_state.remove_by_session(session_id);
    }

    if let Some(issue_id) = issue_id.as_deref() {
        // Terminal transition — wake any in-flight `cairn watch` on this issue,
        // matching the PR-webhook merge path.
        orch.wake_for_issue(issue_id).await;
    }

    // BEST-EFFORT: delete the merged source branch on GitHub. Nothing downstream
    // depends on it.
    if let Err(e) = api::delete_branch(http, &creds, &owner, &repo, &source_branch).await {
        log::warn!("Best-effort delete of merged source branch {source_branch} failed: {e}");
    }

    // BEST-EFFORT, proactive local reconcile. This path only handles remote PRs
    // whose real base is the project default branch, so the shared reconcile may
    // safely update the user's main checkout.
    let reconcile_ctx = merge_context;
    let orch_clone = orch.clone();
    tokio::spawn(async move {
        // In-flight siblings: fetch origin into the shared store and rebase each
        // onto the advanced `<base>@origin` tip. This is the external-advance
        // reconcile the `push` webhook would also drive; the commit-id guard makes
        // the double-fire idempotent.
        if let Err(e) = crate::orchestrator::base_advance::reconcile_external_default_advance(
            &orch_clone,
            &reconcile_ctx.project_id,
            &target_branch,
        )
        .await
        {
            log::warn!("Post-GitHub-merge sibling reconcile failed: {e}");
        }
        // File capture, user-checkout fast-forward pull (forced — no local fold
        // moved it), worktree teardown, PR refresh.
        reconcile_after_merge(orch_clone, db, reconcile_ctx, true).await;
    });

    log::info!(
        "merge_pr_for_job[{job_id}]: GitHub merge path took {:?}",
        merge_started.elapsed()
    );
    Ok("PR merged via GitHub".to_string())
}

/// Merge a PR, mark the issue merged, and run post-merge reconciliation (file
/// capture, optional main-repo pull, active-worktree fast-forward, teardown, PR
/// refresh) in the background via `reconcile_after_merge`.
pub async fn merge_pr_for_job(
    orch: &Orchestrator,
    job_id: &str,
    merge_method: Option<String>,
) -> Result<String, String> {
    // Route to the owning database (team replica or private DB). The merge
    // request, its issue, and the producing job for a team execution all live in
    // the team replica; GitHub credentials stay on the private DB.
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;
    let merge_context = resolve_merge_mr_context_for_job(&db, job_id).await?;
    let repo_path = merge_context.mr.repo_path.clone();
    let issue_id = merge_context.issue_id.clone();

    // Per-phase timing for the synchronous merge path. The downstream sibling
    // reconcile no longer runs here (it is spawned inside `resolve_pr_node`), so
    // these spans bound only the work the "merging" button actually waits on:
    // the conflict gate, the store fold + origin round-trips, and the DB/
    // execution-recompute resolution. Logged at `info` so a live merge reports
    // real numbers from the user's own instance.
    let merge_started = std::time::Instant::now();

    // Merge boundary: refuse a jj-conflicted source bookmark before the store
    // fold. Fails closed regardless of cached/rendered mergeable state — a
    // conflicted child must never be folded into integration. Under
    // store-owns-merge the old staleness window largely dissolves: a
    // cleanly-rebased sibling is pushed immediately, so origin's PR head tracks
    // the local rebased tip rather than lagging a stale pre-rebase commit.
    let gate_started = std::time::Instant::now();
    let resolved_default =
        resolve_project_default_branch(&repo_path, &merge_context.default_branch);
    // Gate on the TIP, not the whole range. A genuinely conflicted tip needs
    // manual marker resolution and can never fold, so it is refused here. A clean
    // tip with conflicted INTERMEDIATE commits is auto-recoverable: it is allowed
    // through to `store_merge_child`, which decides per-path whether to flatten
    // (squash-to-default, child→integration) or refuse (non-squash preserve). The
    // range is still enumerated, solely to build the refusal message.
    if let Some(report) = source_conflict_report(
        &orch.jj_binary_path,
        &orch.config_dir,
        &repo_path,
        &merge_context.source_branch,
        Some(&merge_context.target_branch),
    ) {
        if report.tip_conflicted {
            return Err(format!(
                "Refusing to merge: the jj source bookmark `{source}` carries a recorded conflict on its tip — a conflicted history cannot fold into `{target}`.\n{commits}\n{recovery}",
                source = merge_context.source_branch,
                target = merge_context.target_branch,
                commits = format_conflicted_commits(&report.commits),
                recovery = conflict_recovery_hint(
                    &merge_context.source_branch,
                    Some(&merge_context.target_branch)
                ),
            ));
        }
        // Clean tip, conflicted intermediate(s): auto-recoverable only for a SQUASH
        // landing (which flattens). A preserve-every-commit landing on the default
        // branch cannot flatten, so it must refuse — and the refusal must cover BOTH
        // routes: the local non-squash fold (`store_merge_child`) AND the remote
        // GitHub `merge` route (`merge_remote_pr_via_github`), which passes the
        // method straight to GitHub and would otherwise carry the conflicted
        // intermediate onto the default branch as a merge commit, bypassing the
        // local refusal. The two routes resolve the method differently, so mirror
        // each here. (Child→integration landings always flatten, so they are exempt.)
        if merge_context.target_branch == resolved_default {
            let route_method =
                default_landing_method(&merge_context, &resolved_default, merge_method.clone());
            if route_method != "squash" {
                return Err(format!(
                    "Refusing to merge: `{source}` has a clean tip but conflicted intermediate commit(s), and the `{route_method}` method preserves every commit — a conflicted intermediate cannot land on the default branch `{target}`. Resolve the conflict markers and re-seal, or merge with the squash method (which flattens the intermediate history).\n{commits}",
                    source = merge_context.source_branch,
                    target = merge_context.target_branch,
                    commits = format_conflicted_commits(&report.commits),
                ));
            }
        }
        log::info!(
            "merge_pr_for_job[{job_id}]: source `{source}` has a clean tip with {n} conflicted intermediate(s); allowing through to the guarded flatten",
            source = merge_context.source_branch,
            n = report.commits.len(),
        );
    }
    if merge_context.target_branch == resolved_default {
        assert_main_checkout_clean_for_default_merge(&*orch.services.git, &repo_path)?;
    }
    log::info!(
        "merge_pr_for_job[{job_id}]: merge gates took {:?}",
        gate_started.elapsed()
    );

    // Route a real remote PR landing on the project default branch through
    // GitHub's merge API (GitHub squash-merges, closes the PR, and advances the
    // base on origin; Cairn then reconciles locally). The local jj fold below is
    // kept only where there is no GitHub PR to merge through: local-only projects
    // and Coordinator child→integration PRs, plus workspace PRs that deliberately
    // preserve every commit. The conflict gate above has
    // already run for both paths.
    if should_merge_via_github(&merge_context) {
        return merge_remote_pr_via_github(
            orch,
            db,
            job_id,
            merge_context,
            merge_method,
            merge_started,
        )
        .await;
    }

    // The shared jj store owns the merge: fold the child's commit into the
    // integration bookmark and (for a remote project) push it, which advances
    // origin and marks the child PR Merged out-of-band. A no-remote jj project
    // folds locally and skips the push. The method selects the *shape* that lands
    // on the default branch: `squash` (the default) collapses the source to one
    // commit before the fold; `merge` (forced for workspace PRs) keeps every
    // sealed commit.
    let method = effective_merge_method(&merge_context, merge_method);
    let fold_started = std::time::Instant::now();
    // Serialize the fold behind the per-store mutex so a merge fold and a
    // base-advance reconcile on the same shared store never run jj ops
    // concurrently (which would mint divergent conflicted copies). The detached
    // downstream reconcile spawned later inside `resolve_pr_node` acquires the
    // same lock, after this guard drops — no nested acquisition.
    let merge_store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    let merged_commit = if merge_context.target_branch == resolved_default {
        let _store_guard = orch
            .acquire_jj_store_lock(
                &merge_store,
                format!("merge fold for {}", merge_context.source_branch),
            )
            .await;
        store_merge_child(orch, &merge_context, &method).await?
    } else {
        const MAX_VERIFY_ATTEMPTS: usize = 8;
        let mut landed = None;
        for _attempt in 0..MAX_VERIFY_ATTEMPTS {
            let prospective = {
                let _store_guard = orch
                    .acquire_jj_store_lock(
                        &merge_store,
                        format!("prospective merge fold for {}", merge_context.source_branch),
                    )
                    .await;
                prospective_store_merge_child(orch, &merge_context, &method).await?
            };
            match crate::execution::checks::verify_review_tree(
                orch,
                &merge_context.project_id,
                &repo_path,
                Path::new(&repo_path),
                &prospective.commit_id,
                &prospective.tree_hash,
                &prospective.tree_entries,
                &prospective.changed_files,
                job_id,
                crate::build_slots::BuildSlotPriority::ReviewCheck,
            )
            .await
            {
                crate::execution::checks::ReviewTreeGateResult::Green => {}
                crate::execution::checks::ReviewTreeGateResult::CheckFailed { name, detail } => {
                    return Err(format!("Combined-tree check '{name}' failed: {detail}"));
                }
                crate::execution::checks::ReviewTreeGateResult::InfrastructureFailure(error) => {
                    return Err(format!(
                        "Combined-tree verification infrastructure failure: {error}. Retry the merge."
                    ));
                }
            }
            let landing = {
                let _store_guard = orch
                    .acquire_jj_store_lock(
                        &merge_store,
                        format!("verified merge landing for {}", merge_context.source_branch),
                    )
                    .await;
                land_verified_store_merge_child(orch, &merge_context, &method, &prospective).await?
            };
            match landing {
                VerifiedLanding::Landed(commit) => {
                    landed = Some(commit);
                    break;
                }
                VerifiedLanding::Stale => continue,
            }
        }
        landed.ok_or_else(|| {
            "Combined-tree verification could not stabilize because the source or integration branch kept moving. Retry the merge."
                .to_string()
        })?
    };
    persist_merged_commit(&db, &merge_context.mr.mr_id, &merged_commit).await?;
    log::info!(
        "merge_pr_for_job[{job_id}]: store_merge_child (fold + origin) took {:?}",
        fold_started.elapsed()
    );

    // Merge postcondition: the fold must have advanced the target bookmark to
    // contain the source tip. Verify it BEFORE `resolve_pr_node` marks the PR
    // merged, resolves the issue, or tears anything down — so a fold that
    // silently no-ops (a future regression) converts to a loud, recoverable
    // error instead of the silent data loss CAIRN-2287 traces. No commits are
    // lost on failure: nothing downstream has run, and the source bookmark is
    // untouched. `store_merge_child` FFs the target onto the (possibly squashed)
    // source, so on every success path the source is an ancestor of the target.
    {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        if !crate::jj::bookmark_landed_in(
            &jj,
            &merge_store,
            &merge_context.source_branch,
            &merge_context.target_branch,
        ) {
            return Err(format!(
                "Merge postcondition failed: after folding `{source}` into `{target}`, the target bookmark does not contain the source tip. Refusing to mark the PR merged — no commits were lost and the source branch is intact; retry the merge.",
                source = merge_context.source_branch,
                target = merge_context.target_branch,
            ));
        }
    }

    let resolve_started = std::time::Instant::now();
    let closed_sessions = resolve_pr_node(orch, job_id, PrNodeResolution::Merge)
        .await
        .map_err(|error| {
            log::error!(
                "Merged git branch but failed to mark merge request merged for job/action {}: {}",
                job_id,
                error
            );
            error
        })?;
    log::info!(
        "merge_pr_for_job[{job_id}]: resolve_pr_node took {:?}",
        resolve_started.elapsed()
    );
    for session_id in &closed_sessions {
        orch.process_state.remove_by_session(session_id);
    }

    if let Some(issue_id) = issue_id.as_deref() {
        // Terminal transition — wake any in-flight `cairn watch` on this issue,
        // matching the PR-webhook merge path. Typed emit re-reads the live
        // projection and sends `Resolved` since the issue just became terminal.
        orch.wake_for_issue(issue_id).await;
    }

    // Background reconciliation, shared with the GitHub webhook merge path. The
    // local fold already re-attached and fast-forwarded the checkout, so the pull
    // is governed by `pull_on_merge` (not forced).
    tokio::spawn(reconcile_after_merge(
        orch.clone(),
        db,
        merge_context,
        false,
    ));

    log::info!(
        "merge_pr_for_job[{job_id}]: synchronous merge path took {:?} (downstream reconcile deferred)",
        merge_started.elapsed()
    );
    Ok("PR merged successfully".to_string())
}

#[cfg(test)]
mod tests {
    use super::super::context::MrContext;
    use super::*;

    fn merge_context(is_workspace: bool, has_triage_batch: bool) -> MergeMrContext {
        MergeMrContext {
            mr: MrContext {
                mr_id: "mr".to_string(),
                pr_url: String::new(),
                github_pr_number: None,
                repo_path: "/tmp/repo".to_string(),
                job_id: "job".to_string(),
                is_local: false,
            },
            issue_id: Some("issue".to_string()),
            default_branch: "main".to_string(),
            project_id: "project".to_string(),
            target_branch: "main".to_string(),
            source_branch: "feature".to_string(),
            title: "PR".to_string(),
            is_workspace,
            has_triage_batch,
        }
    }

    /// Squash is the default landing shape; only workspace PRs force `merge` so
    /// their individual commits survive, even when the caller asks for another
    /// method.
    #[test]
    fn effective_merge_method_defaults_squash_and_only_forces_merge_for_workspace() {
        // Workspace PR forces merge regardless of the requested method.
        assert_eq!(
            effective_merge_method(&merge_context(true, false), Some("squash".to_string())),
            "merge"
        );
        // Memory-triage-batch PRs now honor the requested method.
        assert_eq!(
            effective_merge_method(&merge_context(false, true), Some("rebase".to_string())),
            "rebase"
        );
        // Memory-triage-batch PRs default to squash like normal PRs.
        assert_eq!(
            effective_merge_method(&merge_context(false, true), None),
            "squash"
        );
        // A normal PR with no requested method defaults to squash.
        assert_eq!(
            effective_merge_method(&merge_context(false, false), None),
            "squash"
        );
        // An explicit method on a normal PR is honored.
        assert_eq!(
            effective_merge_method(&merge_context(false, false), Some("merge".to_string())),
            "merge"
        );
    }

    /// `default_landing_method` mirrors each route's method resolution so the merge
    /// gate refuses a conflicted-intermediate source under a preserve landing on
    /// BOTH routes. The load-bearing case is the GitHub route: a remote default PR
    /// that requests `merge` resolves to `merge` (passed straight to GitHub), so
    /// the gate catches it before it carries a conflicted intermediate onto the
    /// default branch — the bypass the local `store_merge_child` refusal missed.
    #[test]
    fn default_landing_method_resolves_per_route_for_the_preserve_gate() {
        // Remote default PR (routes to GitHub): default is squash (safe — flattens).
        let mut remote = merge_context(false, false);
        remote.mr.github_pr_number = Some(7);
        assert_eq!(default_landing_method(&remote, "main", None), "squash");
        // Remote default PR requesting `merge`: resolves to merge (the bypass case).
        assert_eq!(
            default_landing_method(&remote, "main", Some("merge".to_string())),
            "merge",
            "a GitHub `merge` request must be seen by the gate as a preserve landing"
        );

        // Local-only default PR (stays on the local fold): a normal PR defaults to
        // squash, a workspace PR is forced to merge (preserve) — both surfaced here.
        let mut local = merge_context(false, false);
        local.mr.is_local = true;
        assert_eq!(default_landing_method(&local, "main", None), "squash");
        let mut local_ws = merge_context(true, false);
        local_ws.mr.is_local = true;
        assert_eq!(
            default_landing_method(&local_ws, "main", Some("squash".to_string())),
            "merge",
            "a workspace PR forces a preserve landing even when squash is requested"
        );

        let mut local_triage = merge_context(false, true);
        local_triage.mr.is_local = true;
        assert_eq!(
            default_landing_method(&local_triage, "main", None),
            "squash",
            "a local memory-triage PR defaults to squash like a normal PR"
        );
    }

    /// A remote PR landing on the default branch routes to GitHub; everything
    /// else (local-only, child→integration, workspace) stays on the local jj fold.
    #[test]
    fn should_route_to_github_only_for_remote_default_branch_pr() {
        // Base case: a real remote PR on the default branch.
        let mut ctx = merge_context(false, false);
        ctx.mr.github_pr_number = Some(7);
        assert!(should_route_to_github(&ctx, "main"));

        // Local-only project (no GitHub PR) stays on the local fold.
        let mut local = ctx.clone();
        local.mr.github_pr_number = None;
        assert!(!should_route_to_github(&local, "main"));
        let mut is_local = ctx.clone();
        is_local.mr.is_local = true;
        assert!(!should_route_to_github(&is_local, "main"));

        // Coordinator child→integration PR (target ≠ default) stays local.
        let mut child = ctx.clone();
        child.target_branch = "agent/CAIRN-1-coordinator-0".to_string();
        assert!(!should_route_to_github(&child, "main"));

        // Workspace PRs stay on the local fold (keep-every-commit); memory-triage
        // PRs route like normal PRs.
        let mut workspace = ctx.clone();
        workspace.is_workspace = true;
        assert!(!should_route_to_github(&workspace, "main"));
        let mut triage = ctx.clone();
        triage.has_triage_batch = true;
        assert!(should_route_to_github(&triage, "main"));

        // A stale stored default that disagrees with the real base routes off the
        // PR's actual base: target "staging" against resolved "staging" routes,
        // against a stale "main" does not.
        let mut staging = ctx.clone();
        staging.target_branch = "staging".to_string();
        assert!(should_route_to_github(&staging, "staging"));
        assert!(!should_route_to_github(&staging, "main"));
    }

    #[test]
    fn dirty_tracked_paths_parses_only_tracked_porcelain_entries() {
        let paths = crate::pr_data::helpers::dirty_tracked_paths_from_porcelain(
            " M src/lib.rs\n?? scratch.txt\n!! ignored.log\nR  old.rs -> new.rs\nA  src-tauri/Cargo.lock\n",
        );
        assert_eq!(
            paths,
            vec![
                "src/lib.rs".to_string(),
                "new.rs".to_string(),
                "src-tauri/Cargo.lock".to_string(),
            ]
        );
    }

    #[test]
    fn main_checkout_dirty_gate_allows_only_regenerable_lockfile_churn() {
        use crate::services::testing::MockGitClient;

        let mut clean = MockGitClient::new();
        clean.expect_status().returning(|_| Ok(String::new()));
        assert!(assert_main_checkout_clean_for_default_merge(&clean, "/repo").is_ok());

        let mut lockfile_only = MockGitClient::new();
        lockfile_only
            .expect_status()
            .returning(|_| Ok(" M src-tauri/Cargo.lock".to_string()));
        assert!(assert_main_checkout_clean_for_default_merge(&lockfile_only, "/repo").is_ok());

        let mut real_edit = MockGitClient::new();
        real_edit
            .expect_status()
            .returning(|_| Ok(" M src-tauri/Cargo.lock\n M src/lib.rs".to_string()));
        let error = assert_main_checkout_clean_for_default_merge(&real_edit, "/repo").unwrap_err();
        assert!(error.contains("Refusing to merge"), "{error}");
        assert!(error.contains("/repo"), "{error}");
        assert!(error.contains("src/lib.rs"), "{error}");
        assert!(!error.contains("src-tauri/Cargo.lock"), "{error}");
    }

    #[test]
    fn main_checkout_reconcile_runs_only_for_default_branch_advances() {
        assert!(should_reconcile_main_checkout_after_merge("main", "main"));
        assert!(!should_reconcile_main_checkout_after_merge(
            "agent/CAIRN-1-coordinator-0",
            "main"
        ));
    }

    /// A GitHub 405/409 refusal points at the PR (no local markers exist); any
    /// other failure is reported as a plain GitHub merge error.
    #[test]
    fn map_github_merge_error_distinguishes_refusal_from_other_failures() {
        let refusal = map_github_merge_error(
            "feature",
            "main",
            "Failed to merge PR: 405 - not mergeable".to_string(),
        );
        assert!(refusal.contains("GitHub refused"), "{refusal}");
        assert!(
            refusal.contains("feature") && refusal.contains("main"),
            "{refusal}"
        );

        let conflict = map_github_merge_error(
            "feature",
            "main",
            "Failed to merge PR: 409 - head changed".to_string(),
        );
        assert!(conflict.contains("GitHub refused"), "{conflict}");

        let other = map_github_merge_error(
            "feature",
            "main",
            "Failed to merge PR: 500 - server error".to_string(),
        );
        assert!(!other.contains("GitHub refused"), "{other}");
        assert!(other.contains("via GitHub"), "{other}");
    }
}
