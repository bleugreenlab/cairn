//! Proactive base-branch advance notifications for downstream jobs.
//!
//! When a base branch advances (a Cairn PR merges, or a remote default branch
//! moves externally), every in-flight sibling branched from that base is
//! auto-rebased onto the new tip over the shared jj store. Each rebased sibling
//! is then told its branch moved, split by outcome:
//!
//! - A sibling whose rebase recorded a **conflict** gets a **Steer** system direct
//!   (naming the conflicting files) so an idle agent wakes and an active agent sees
//!   it at the next tool boundary without having its current tool call cancelled.
//!   A conflicted commit can neither push nor merge, so this is stop-the-line work,
//!   but it should steer the agent rather than interrupt the active turn.
//! - A sibling that rebased **cleanly** gets a **passive** (non-waking) note that
//!   rides along into its next natural run — its work moved underneath it but
//!   there is nothing to resolve, so it is never mechanically resumed.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::execution::routing::{owning_db_for_job, owning_db_for_project};
use crate::messages::delivery::{latest_run_for_job, queue_system_direct};
use crate::messages::queued::DeliveryUrgency;
use crate::models::ExecutionSnapshot;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_db::turso::params;

#[derive(Debug)]
struct MergedJob {
    id: String,
    project_id: String,
    issue_id: Option<String>,
    base_branch: Option<String>,
    worktree_path: Option<String>,
}

#[derive(Debug)]
struct SiblingJob {
    id: String,
    worktree_path: String,
    branch: Option<String>,
}

#[derive(Debug)]
struct MergeRequestInfo {
    pr_number: Option<i64>,
}

#[derive(Debug)]
struct IssueInfo {
    project_key: String,
    number: i64,
}

struct BaseAdvanceNotes {
    conflict: String,
    clean: String,
}

#[derive(Debug)]
struct DefaultReconcileProject {
    id: String,
    repo_path: String,
    default_branch: String,
}

/// Queue non-waking notifications for in-flight siblings whose changes overlap
/// a merged job that advanced their shared base branch.
pub async fn notify_downstream_of_base_advance(
    orch: &Orchestrator,
    merged_job_id: &str,
) -> Result<(), String> {
    let db = owning_db_for_job(&orch.db, merged_job_id)
        .await
        .map_err(|error| {
            log::warn!(
                "Skipping base advance notify for owner {merged_job_id}: failed to route owning database: {error}"
            );
            error.to_string()
        })?;
    let Some(merged_job) = load_merged_job_for_owner(&db, merged_job_id).await? else {
        log::debug!(
            "Skipping base advance notify: no implementation job found for owner {}",
            merged_job_id
        );
        return Ok(());
    };
    let Some(base_branch) = merged_job.base_branch.as_deref() else {
        log::debug!(
            "Skipping base advance notify for job {}: no base_branch",
            merged_job.id
        );
        return Ok(());
    };

    // jj is the only substrate: a base advance is reconciled by a non-blocking
    // auto-rebase of in-flight siblings over the shared store. The advance
    // propagates through the commit graph itself; conflicts are recorded (not
    // blocking) and no sibling rebase/force-push is required.
    let Some(repo_path) = load_project_repo_path(&db, &merged_job.project_id).await? else {
        log::debug!(
            "Skipping base advance reconcile for job {}: no project repo_path",
            merged_job.id
        );
        return Ok(());
    };
    reconcile_jj_downstream(
        orch,
        &db,
        merged_job_id,
        &merged_job,
        base_branch,
        &repo_path,
    )
    .await
}

/// Sentinel for `load_sibling_jobs` when there is no merged job to exclude — an
/// external default-branch advance has no Cairn-tracked owner, so every in-flight
/// sibling on the branch is a reconcile candidate. No job row carries an empty
/// id, so `j.id != ''` excludes nothing.
const EXCLUDE_NONE: &str = "";

/// Reconcile in-flight siblings of a merged jj job by auto-rebasing each onto
/// the locally-advanced integration tip over the shared store and pushing the
/// cleanly-rebased ones so their PR heads advance. Non-blocking: conflicts are
/// recorded for the agent to resolve, and a conflicted sibling is woken (via a
/// `Steer` system direct) to resolve and re-seal so its PR can advance.
/// Cleanly-rebased siblings get a passive (non-waking) note that their branch
/// moved.
async fn reconcile_jj_downstream(
    orch: &Orchestrator,
    db: &LocalDb,
    merged_job_id: &str,
    merged_job: &MergedJob,
    base_branch: &str,
    repo_path: &str,
) -> Result<(), String> {
    // Serialize every base-advance store mutation on this project store behind
    // one async mutex, held across the whole body (on-branch advance + sibling
    // reconcile). A single Cairn merge fires this spawn AND a GitHub push webhook
    // for the same advance, and the 60s local sweep can overlap either;
    // concurrent jj rebase/import ops on the shared store mint divergent
    // conflicted copies of the integration tip. The inner helpers take no lock
    // (`TokioMutex` is not reentrant).
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    let store_lock = orch.jj_store_lock(&store);
    let _store_guard = store_lock.lock().await;

    // Advance the workspace that sits ON the merged branch (a Coordinator on its
    // integration bookmark) onto the freshly-folded tip. This is asymmetric to
    // the sibling reconcile below: `reconcile_siblings` rebases the *children*
    // (branched FROM the branch); nobody otherwise re-parents the workspace whose
    // branch IS the branch, so the fold moves the bookmark out from under its `@`
    // and a later edit+seal would orphan off the advanced branch. Runs
    // independently of (and before) the sibling reconcile — a coordinator must be
    // advanced even when it has no other in-flight siblings.
    advance_on_branch_workspaces(orch, db, &merged_job.project_id, base_branch, repo_path).await;

    let siblings =
        load_sibling_jobs(db, &merged_job.project_id, base_branch, &merged_job.id).await?;
    if siblings.is_empty() {
        log::debug!(
            "jj base advance for merged job {}: no in-flight siblings to reconcile",
            merged_job.id
        );
        return Ok(());
    }

    // The store already owns the merge (the child's commit was folded into the
    // integration bookmark), so the rebase dest is the bare local integration
    // bookmark — no fetch needed.
    let issue_info = match merged_job.issue_id.as_deref() {
        Some(issue_id) => load_issue_info(db, issue_id).await?,
        None => None,
    };
    let pr_number = load_merge_request_info(db, merged_job_id, &merged_job.id)
        .await?
        .and_then(|info| info.pr_number);
    let notes = BaseAdvanceNotes {
        conflict: build_jj_conflict_note(base_branch, pr_number, issue_info.as_ref()),
        clean: build_jj_clean_note(base_branch, pr_number, issue_info.as_ref()),
    };
    reconcile_base_advance(
        orch,
        db,
        &format!("merged job {}", merged_job.id),
        repo_path,
        base_branch,
        siblings,
        notes,
    )
    .await
}

fn remote_default_revset(default_branch: &str) -> String {
    format!("{default_branch}@origin")
}

/// Reconcile in-flight siblings after the project's default branch advanced
/// **outside Cairn** (a non-Cairn PR merged in the GitHub UI, or a direct push to
/// the default branch), detected via the GitHub `push` webhook. Thin wrapper over
/// [`reconcile_default_advance`] with the `Remote` source.
pub async fn reconcile_external_default_advance(
    orch: &Orchestrator,
    project_id: &str,
    default_branch: &str,
) -> Result<(), String> {
    reconcile_default_advance(orch, project_id, default_branch).await
}

/// Shared body for live default-branch-advance reconcile. Mirrors the Cairn-merge
/// path: gate on in-flight siblings, bring the advanced tip into the shared store,
/// then auto-rebase every in-flight sibling on that branch onto the new tip over
/// the shared store — push the cleanly-rebased ones, record conflicts
/// non-blocking, and notify the siblings this reconcile actually rewrote — a
/// waking `Steer` note to a conflicted sibling, a passive ride-along note to a
/// cleanly-rebased one (the before/after commit-id guard in
/// `reconcile_base_advance` gates both). Runs regardless of the project's
/// `pull_on_merge` setting: that gates the user's main-checkout pull, not
/// agent-workspace reconciliation. Non-fatal end to end — every failure is logged
/// and swallowed so the webhook handler does not error on it.
async fn reconcile_default_advance(
    orch: &Orchestrator,
    project_id: &str,
    default_branch: &str,
) -> Result<(), String> {
    let db = owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| {
            log::warn!(
                "Skipping external advance reconcile for project {project_id}: failed to route owning database: {error}"
            );
            error.to_string()
        })?;
    let Some(repo_path) = load_project_repo_path(&db, project_id).await? else {
        log::debug!("Skipping external advance reconcile: no repo_path for project {project_id}");
        return Ok(());
    };
    let siblings = load_sibling_jobs(&db, project_id, default_branch, EXCLUDE_NONE).await?;
    if siblings.is_empty() {
        log::debug!("external advance on {default_branch}: no in-flight siblings to reconcile");
        return Ok(());
    }

    // Bring the advanced tip into the shared store. `ensure_project_store` runs
    // `jj git import`, which imports the backing git's refs — including the local
    // `<default>` ref a local-only advance or a manual `git pull` moved — so the
    // rebase dest resolves regardless of which branch the main checkout sits on.
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    // Serialize the store import/fetch + sibling reconcile behind the per-store
    // mutex, held across the rest of the body. The live webhook, startup catch-up,
    // and Cairn-merge reconcile paths can overlap for the same advance; without
    // this they race jj ops on the shared store and mint divergent conflicted
    // copies. The inner helpers take no lock.
    let store_lock = orch.jj_store_lock(&store);
    let _store_guard = store_lock.lock().await;
    if let Err(error) = crate::jj::ensure_project_store(&jj, &store, Path::new(&repo_path)) {
        log::warn!("external advance on {default_branch}: ensure store failed: {error}");
        return Ok(());
    }
    // The webhook advance lives on origin, not in the local checkout, so fetch
    // origin to advance the `<default>@origin` tracking bookmark before rebasing.
    if let Err(error) = crate::jj::fetch_remote(&jj, &store, "origin") {
        log::warn!("external advance on {default_branch}: jj git fetch failed: {error}");
        return Ok(());
    }

    let notes = BaseAdvanceNotes {
        conflict: build_external_advance_conflict_note(default_branch),
        clean: build_external_advance_clean_note(default_branch),
    };
    reconcile_base_advance(
        orch,
        &db,
        &format!("external advance on {default_branch}"),
        &repo_path,
        &remote_default_revset(default_branch),
        siblings,
        notes,
    )
    .await
}

/// One-time startup catch-up for remote default-branch advances that landed while
/// Cairn was closed. This is intentionally not a sweep: no-remote projects are
/// skipped because nothing outside Cairn can advance them, and remote projects
/// only reconcile when fetching `origin` actually changes the stored remote
/// default tip. An unchanged base never reaches the sibling rebase path.
pub async fn reconcile_startup_remote_default_advances(orch: &Orchestrator) {
    let projects = match load_projects_for_default_reconcile(orch).await {
        Ok(projects) => projects,
        Err(error) => {
            log::warn!("startup default-advance catch-up: failed to load projects: {error}");
            return;
        }
    };
    for (db, project) in projects {
        if !project_has_origin(orch, Path::new(&project.repo_path)) {
            log::debug!(
                "startup default-advance catch-up: skipping project {} with no origin remote",
                project.id
            );
            continue;
        }
        if let Err(error) = reconcile_startup_remote_default_advance(orch, &db, &project).await {
            log::warn!(
                "startup default-advance catch-up for project {} failed: {error}",
                project.id
            );
        }
    }
}

async fn reconcile_startup_remote_default_advance(
    orch: &Orchestrator,
    db: &LocalDb,
    project: &DefaultReconcileProject,
) -> Result<(), String> {
    let siblings =
        load_sibling_jobs(db, &project.id, &project.default_branch, EXCLUDE_NONE).await?;
    if siblings.is_empty() {
        log::debug!(
            "startup remote advance on {}: no in-flight siblings to reconcile",
            project.default_branch
        );
        return Ok(());
    }

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let repo_path = Path::new(&project.repo_path);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let store_lock = orch.jj_store_lock(&store);
    let _store_guard = store_lock.lock().await;
    if let Err(error) = crate::jj::ensure_project_store(&jj, &store, repo_path) {
        log::warn!(
            "startup remote advance on {}: ensure store failed: {error}",
            project.default_branch
        );
        return Ok(());
    }

    let remote_default = remote_default_revset(&project.default_branch);
    let before = crate::jj::revset_commit(&jj, &store, &remote_default);
    if let Err(error) = crate::jj::fetch_remote(&jj, &store, "origin") {
        log::warn!(
            "startup remote advance on {}: jj git fetch failed: {error}",
            project.default_branch
        );
        return Ok(());
    }
    let after = crate::jj::revset_commit(&jj, &store, &remote_default);
    if before == after {
        log::debug!(
            "startup remote advance on {}: origin tip unchanged; skipping sibling reconcile",
            project.default_branch
        );
        return Ok(());
    }
    if after.is_none() {
        log::debug!(
            "startup remote advance on {}: origin tip did not resolve after fetch; skipping",
            project.default_branch
        );
        return Ok(());
    }

    let notes = BaseAdvanceNotes {
        conflict: build_external_advance_conflict_note(&project.default_branch),
        clean: build_external_advance_clean_note(&project.default_branch),
    };
    reconcile_base_advance(
        orch,
        db,
        &format!("startup external advance on {}", project.default_branch),
        &project.repo_path,
        &remote_default,
        siblings,
        notes,
    )
    .await
}

fn project_has_origin(orch: &Orchestrator, repo_path: &Path) -> bool {
    orch.services
        .git
        .remote_get_url(repo_path)
        .ok()
        .is_some_and(|url| !url.trim().is_empty())
}

/// Shared reconcile body for both base-advance paths (Cairn merge and external
/// default-branch advance): build the `(branch, workspace)` specs, snapshot each
/// sibling's pre-reconcile commit id, run the non-blocking auto-rebase onto
/// `rebase_dest`, then notify each sibling this reconcile actually rewrote — a
/// **waking** `Steer` note for a conflicted sibling (resolve the markers), a
/// **passive** ride-along note for a cleanly-rebased one (its branch moved, with
/// nothing to resolve).
///
/// The before/after commit-id guard makes both paths idempotent against their
/// double-fires and applies to both outcomes: a Cairn merge into the default
/// branch fires the merge path AND a GitHub `push` webhook for the same advance,
/// and a second reconcile at the same dest tip is a `jj rebase` no-op (the commit
/// id is unchanged), so `after == before` → no redundant notification, conflicted
/// or clean.
async fn reconcile_base_advance(
    orch: &Orchestrator,
    db: &LocalDb,
    label: &str,
    repo_path: &str,
    rebase_dest: &str,
    siblings: Vec<SiblingJob>,
    notes: BaseAdvanceNotes,
) -> Result<(), String> {
    let specs: Vec<(String, std::path::PathBuf)> = siblings
        .iter()
        .filter_map(|sibling| {
            let branch = sibling_branch(sibling)?;
            Some((branch, std::path::PathBuf::from(&sibling.worktree_path)))
        })
        .collect();
    if specs.is_empty() {
        log::debug!("jj base advance ({label}): no in-flight siblings with a branch to reconcile");
        return Ok(());
    }

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));

    // Store-truth precheck BEFORE any per-sibling jj work. `load_sibling_jobs`
    // returns rows straight from the DB, including long-dead `agent/…` siblings
    // whose worktrees were reclaimed but whose `jobs` / `merge_requests` rows
    // linger. List every existing bookmark ONCE and drop the siblings whose
    // bookmark is gone, so the divergence-collapse, before-snapshot, and rebase
    // loops below never spawn a `jj` subprocess per dead branch — the
    // startup/base-advance subprocess storm. A failed list disables the filter
    // (proceed with all): liveness over strictness, matching the reconcile
    // primitives.
    let existing_bookmarks = crate::jj::list_local_bookmarks(&jj, &store).ok();
    let (specs, skipped_missing) = retain_present_siblings(specs, existing_bookmarks.as_ref());
    if skipped_missing > 0 {
        log::info!(
            "jj base advance ({label}): skipped {skipped_missing} sibling(s) with missing bookmarks before reconcile"
        );
    }
    if specs.is_empty() {
        log::debug!("jj base advance ({label}): all in-flight siblings had missing bookmarks");
        return Ok(());
    }

    // Heal any PRE-EXISTING divergent twin on a sibling bookmark BEFORE the
    // sibling rebase. New divergence is already prevented upstream (the per-store
    // mutex + the idempotent `reconcile_siblings` skip); this collapses a twin
    // that forked before that serialization landed, or via an external `jj` op.
    // It is the locus of the #162 thrash: an orphaned conflicted twin keeps
    // tripping `sealed_commit_is_lost` on every re-seal. A deterministic tangle
    // (one clean twin) self-heals silently; an ambiguous one holds the store
    // untouched and interrupts the sibling for manual resolution (never a
    // force-push). Runs under the per-store lock every caller holds, so the
    // collapse cannot itself race/fork.
    let mut ambiguous: Vec<AmbiguousDivergence> = Vec::new();
    for (branch, _) in &specs {
        match crate::jj::collapse_divergent_bookmark(&jj, &store, branch) {
            Ok(crate::jj::CollapseOutcome::NotDivergent) => {}
            Ok(crate::jj::CollapseOutcome::Collapsed { kept, abandoned }) => {
                log::info!(
                    "jj collapse ({label}): sibling {branch} converged to {kept}; abandoned {}",
                    abandoned.join(", ")
                );
            }
            Ok(crate::jj::CollapseOutcome::Ambiguous { change_id, twins }) => {
                log::warn!(
                    "jj collapse ({label}): sibling {branch} divergent change {change_id} is ambiguous (twins {}); holding the store untouched for manual resolution",
                    twins.join(", ")
                );
                ambiguous.push(AmbiguousDivergence {
                    branch: branch.clone(),
                    change_id,
                    twins,
                });
            }
            Err(error) => {
                log::warn!("jj collapse ({label}): sibling {branch} failed: {error}");
            }
        }
    }
    if !ambiguous.is_empty() {
        notify_ambiguous_divergence(orch, db, &siblings, &ambiguous)?;
    }

    // Snapshot each sibling's commit id BEFORE the rebase, so we notify only
    // those this reconcile actually moved (the double-fire guard).
    let before: HashMap<String, String> = specs
        .iter()
        .filter_map(|(branch, _)| {
            crate::jj::bookmark_commit(&jj, &store, branch).map(|commit| (branch.clone(), commit))
        })
        .collect();

    let report = match crate::jj::reconcile_siblings(&jj, &store, rebase_dest, &specs) {
        Ok(report) => report,
        Err(error) => {
            log::warn!("jj sibling reconcile ({label}) failed: {error}");
            return Ok(());
        }
    };
    log::info!(
        "jj reconcile ({label}): {} rebased clean, {} recorded a conflict",
        report.rebased_clean.len(),
        report.conflicted.len()
    );

    // Re-read each touched sibling's commit id AFTER the rebase — conflicted and
    // cleanly-rebased alike — so we notify only the ones whose commit actually
    // changed (a no-op double-fire leaves it equal).
    let after: HashMap<String, String> = report
        .conflicted
        .iter()
        .chain(report.rebased_clean.iter())
        .filter_map(|branch| {
            crate::jj::bookmark_commit(&jj, &store, branch).map(|commit| (branch.clone(), commit))
        })
        .collect();

    // Conflicted siblings: a conflicted commit can never push, so the sibling
    // is steered to resolve the markers and re-seal. Idle recipients wake;
    // active recipients receive it at the next tool boundary without cancellation.
    let conflicted_rewritten = siblings_rewritten(&report.conflicted, &before, &after);
    if conflicted_rewritten.is_empty() {
        log::debug!("jj reconcile ({label}): conflicts unchanged since a prior reconcile; no redundant wake");
    } else {
        // Enumerate the conflicting files per branch here, where the jj env and
        // store are already resolved and each sibling's worktree path is in hand.
        // Keeping the jj call out of `notify_conflicted_siblings` leaves that
        // function pure and unit-testable with a synthetic file map.
        let files_by_branch: HashMap<String, Vec<String>> = conflicted_rewritten
            .iter()
            .filter_map(|branch| {
                let sibling = siblings
                    .iter()
                    .find(|sibling| sibling_branch(sibling).as_deref() == Some(branch.as_str()))?;
                let files = crate::jj::conflicted_files(&jj, Path::new(&sibling.worktree_path));
                Some((branch.clone(), files))
            })
            .collect();
        notify_conflicted_siblings(
            orch,
            db,
            &siblings,
            &conflicted_rewritten,
            &notes.conflict,
            &files_by_branch,
        )?;
    }

    // Cleanly-rebased siblings: nothing to resolve, but their branch moved — a
    // passive (non-waking) note rides along into their next natural run.
    let clean_rewritten = siblings_rewritten(&report.rebased_clean, &before, &after);
    if clean_rewritten.is_empty() {
        log::debug!("jj reconcile ({label}): clean rebases unchanged since a prior reconcile; no redundant note");
    } else {
        notify_clean_siblings(orch, db, &siblings, &clean_rewritten, &notes.clean)?;
    }

    Ok(())
}

/// Filter a set of reconciled sibling branches down to those this reconcile
/// actually rewrote: a branch whose commit id changed between the before/after
/// snapshots. A double-fire reconcile at the same dest tip is a `jj rebase` no-op,
/// so the commit id is unchanged → the branch is filtered out and not re-notified
/// (conflicted or clean). When either snapshot is missing (an unexpected resolve
/// failure), notify conservatively rather than silently dropping a real change.
fn siblings_rewritten(
    branches: &[String],
    before: &HashMap<String, String>,
    after: &HashMap<String, String>,
) -> Vec<String> {
    branches
        .iter()
        .filter(
            |branch| match (before.get(branch.as_str()), after.get(branch.as_str())) {
                (Some(before_commit), Some(after_commit)) => before_commit != after_commit,
                _ => true,
            },
        )
        .cloned()
        .collect()
}

/// Drop siblings whose branch bookmark no longer exists in `existing`, returning
/// the retained specs and how many were dropped (for one summary log line). This
/// is the store-truth guard on the DB-sourced sibling set: `load_sibling_jobs`
/// yields stale rows for long-dead `agent/…` branches, and filtering them here —
/// before the divergence-collapse and before-snapshot loops — keeps a base advance
/// from spawning a `jj` subprocess per dead sibling. `None` (the store-wide
/// bookmark list failed) disables the filter: proceed with all, liveness over
/// strictness.
fn retain_present_siblings(
    specs: Vec<(String, std::path::PathBuf)>,
    existing: Option<&std::collections::HashSet<String>>,
) -> (Vec<(String, std::path::PathBuf)>, usize) {
    let Some(existing) = existing else {
        return (specs, 0);
    };
    let total = specs.len();
    let retained: Vec<_> = specs
        .into_iter()
        .filter(|(branch, _)| existing.contains(branch))
        .collect();
    let dropped = total - retained.len();
    (retained, dropped)
}

/// The sibling's jj bookmark: the job row's `branch`, or the workspace marker.
fn sibling_branch(sibling: &SiblingJob) -> Option<String> {
    sibling
        .branch
        .clone()
        .or_else(|| crate::jj::read_branch_marker(Path::new(&sibling.worktree_path)))
}

/// The note for a sibling whose auto-rebase recorded a conflict. It carries no
/// rebase commands — the rebase already happened over the shared store; the agent
/// only resolves the materialized conflict markers in its workspace, then lets it
/// re-seal/push. A recorded conflict is STOP-THE-LINE: jj refuses to push or merge
/// a conflicted commit, so this branch is wedged until it is resolved. Delivered
/// via a `Steer` system direct that wakes idle agents and lands at the next tool
/// boundary without stopping an active turn (see `notify_conflicted_siblings`).
fn build_jj_conflict_note(
    base_branch: &str,
    pr_number: Option<i64>,
    issue_info: Option<&IssueInfo>,
) -> String {
    let pr_fragment = pr_number
        .map(|number| format!("PR #{} merged", number))
        .unwrap_or_else(|| "A PR merged".to_string());
    let issue_fragment = issue_info
        .map(|issue| format!(" (cairn://p/{}/{})", issue.project_key, issue.number))
        .unwrap_or_default();
    format!(
        "⛔ BLOCKING [Base branch update] Your base branch `{base_branch}` advanced — {pr_fragment}{issue_fragment}. Your work was auto-rebased onto the new tip over the shared store and the rebase recorded a conflict. This branch cannot push or merge until you resolve it — jj refuses to push a conflicted commit. Resolve the conflict markers in your workspace now, verify build + tests, and re-seal before continuing other work."
    )
}

/// The note for a sibling whose auto-rebase landed cleanly — its branch moved onto
/// the advanced base with no conflict, so there is nothing to resolve. Delivered
/// passively (a non-waking `queue_system_direct`) so it rides along into the
/// agent's next natural run rather than mechanically resuming an idle agent (see
/// `notify_clean_siblings`).
fn build_jj_clean_note(
    base_branch: &str,
    pr_number: Option<i64>,
    issue_info: Option<&IssueInfo>,
) -> String {
    let pr_fragment = pr_number
        .map(|number| format!("PR #{} merged", number))
        .unwrap_or_else(|| "A PR merged".to_string());
    let issue_fragment = issue_info
        .map(|issue| format!(" (cairn://p/{}/{})", issue.project_key, issue.number))
        .unwrap_or_default();
    format!(
        "[Base branch update] Your base branch `{base_branch}` advanced — {pr_fragment}{issue_fragment}. Your work was auto-rebased cleanly onto the new tip; nothing to resolve. No manual rebase or force-push is needed."
    )
}

/// Advance the workspace(s) ON the merged branch (the Coordinator on its
/// integration bookmark) onto the freshly-folded tip. The merge fold
/// (`merge_into_bookmark`) advanced the integration bookmark out from under the
/// coordinator's `@`; `reconcile_siblings` only rebases the *children* (branched
/// FROM integration), never the coordinator (whose branch IS integration). Each
/// matching workspace has its `@` re-parented onto the new tip via
/// `crate::jj::advance_workspace_onto`. Best-effort and idempotent: a no-op when
/// `@` already sits on the tip, so it is safe under the merge/webhook
/// double-fire. A recorded conflict (effectively impossible for an idle
/// coordinator's empty `@`, but handled defensively) wakes the workspace with a
/// non-blocking note rather than leaving it idle on a conflicted `@`.
///
/// `branch == default_branch` needs no handling here: the workspace on the
/// default branch is the user's main checkout, refreshed by the default-branch
/// merge reconcile, and no agent job carries `branch = <default>` (jobs always
/// branch as `agent/...`), so the on-branch query returns nothing for it.
async fn advance_on_branch_workspaces(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    repo_path: &str,
) {
    let on_branch = match load_on_branch_workspaces(db, project_id, branch).await {
        Ok(workspaces) => workspaces,
        Err(error) => {
            log::warn!("on-branch advance: failed to load workspaces on {branch}: {error}");
            return;
        }
    };
    if on_branch.is_empty() {
        log::debug!("on-branch advance on {branch}: no in-flight workspace sits on the branch");
        return;
    }

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));

    // Collapse a pre-existing divergent twin on the integration bookmark ITSELF
    // before re-parenting the on-branch coordinator onto it. A divergent dest is
    // exactly what makes the coordinator's own re-seal trip `sealed_commit_is_lost`,
    // and `reconcile_siblings` only ever heals the *children*, never the bookmark
    // the coordinator sits on. A deterministic tangle self-heals; an ambiguous one
    // interrupts every on-branch workspace and skips the advance — we must not
    // advance onto an unresolved divergence. Runs under the per-store lock the
    // caller (`reconcile_jj_downstream`) holds across this call.
    match crate::jj::collapse_divergent_bookmark(&jj, &store, branch) {
        Ok(crate::jj::CollapseOutcome::NotDivergent) => {}
        Ok(crate::jj::CollapseOutcome::Collapsed { kept, abandoned }) => {
            log::info!(
                "jj collapse (on-branch {branch}): converged to {kept}; abandoned {}",
                abandoned.join(", ")
            );
        }
        Ok(crate::jj::CollapseOutcome::Ambiguous { change_id, twins }) => {
            log::warn!(
                "jj collapse (on-branch {branch}): divergent change {change_id} is ambiguous (twins {}); interrupting the on-branch workspace and skipping the advance",
                twins.join(", ")
            );
            for workspace in &on_branch {
                let Some(run_id) = latest_run_for_job(db, &workspace.id) else {
                    continue;
                };
                let message = build_ambiguous_divergence_note(branch, &change_id, &twins);
                if let Err(error) =
                    queue_system_direct(orch, &run_id, &message, DeliveryUrgency::Interrupt)
                {
                    log::warn!(
                        "on-branch advance: failed to interrupt {} for ambiguous divergence: {error}",
                        workspace.id
                    );
                }
            }
            return;
        }
        Err(error) => log::warn!("jj collapse (on-branch {branch}): failed: {error}"),
    }

    let Some(dest) = crate::jj::bookmark_commit(&jj, &store, branch) else {
        log::debug!("on-branch advance: bookmark {branch} did not resolve in store; skipping");
        return;
    };

    let mut seen = std::collections::HashSet::new();
    for workspace in &on_branch {
        // Inheritance fan-out: several jobs can share one worktree path. Advance
        // each physical workspace once.
        if !seen.insert(workspace.worktree_path.clone()) {
            continue;
        }
        let Some(ws_branch) = sibling_branch(workspace) else {
            continue;
        };
        // Snapshot the workspace `@` before the advance: `advance_workspace_onto`
        // is idempotent (a no-op when `@` already sits on the tip), so a
        // merge/webhook double-fire leaves the commit id unchanged. We notify the
        // clean case only when `@` actually moved.
        let before_at = crate::jj::workspace_head_commit(&jj, &store, &ws_branch);
        match crate::jj::advance_workspace_onto(
            &jj,
            &store,
            Path::new(&workspace.worktree_path),
            &ws_branch,
            &dest,
        ) {
            Ok(false) => {
                let after_at = crate::jj::workspace_head_commit(&jj, &store, &ws_branch);
                // Notify only on a genuine move. A missing snapshot (resolve
                // failure) falls toward notifying rather than silently dropping it.
                let moved = match (&before_at, &after_at) {
                    (Some(b), Some(a)) => b != a,
                    _ => true,
                };
                if moved {
                    log::info!(
                        "Advanced on-branch workspace {} onto the {} tip",
                        workspace.worktree_path,
                        branch
                    );
                    if let Some(run_id) = latest_run_for_job(db, &workspace.id) {
                        let note = build_on_branch_advance_clean_note(branch);
                        if let Err(error) =
                            queue_system_direct(orch, &run_id, &note, DeliveryUrgency::Passive)
                        {
                            log::warn!(
                                "on-branch advance: failed to notify {}: {error}",
                                workspace.id
                            );
                        }
                    }
                } else {
                    log::debug!(
                        "on-branch advance: workspace {} already on the {} tip; no-op",
                        workspace.worktree_path,
                        branch
                    );
                }
            }
            Ok(true) => {
                log::warn!(
                    "on-branch advance of {} recorded a conflict; steering it",
                    workspace.worktree_path
                );
                if let Some(run_id) = latest_run_for_job(db, &workspace.id) {
                    let note = build_on_branch_advance_conflict_note(branch);
                    let files =
                        crate::jj::conflicted_files(&jj, Path::new(&workspace.worktree_path));
                    let message = append_conflicting_files(&note, Some(&files));
                    if let Err(error) =
                        queue_system_direct(orch, &run_id, &message, DeliveryUrgency::Steer)
                    {
                        log::warn!(
                            "on-branch advance: failed to steer {}: {error}",
                            workspace.id
                        );
                    }
                }
            }
            Err(error) => log::warn!(
                "on-branch advance of {} failed: {error}",
                workspace.worktree_path
            ),
        }
    }
}

/// The note for a workspace ON the advanced branch (the Coordinator) whose
/// re-parent onto the folded tip recorded a conflict. Like the sibling note it
/// carries no rebase commands — the advance already happened over the shared
/// store; the agent only resolves the materialized markers in its workspace. A
/// recorded conflict is STOP-THE-LINE (jj refuses to push or merge it), delivered
/// via a `Steer` system direct that wakes idle agents and lands at the next tool
/// boundary without stopping an active turn.
fn build_on_branch_advance_conflict_note(branch: &str) -> String {
    format!(
        "⛔ BLOCKING [Base branch update] Your branch `{branch}` advanced — a child merged into it. Your workspace was advanced onto the new tip over the shared store and the re-parent recorded a conflict. This branch cannot push or merge until you resolve it — jj refuses to push a conflicted commit. Resolve the conflict markers in your workspace now, verify build + tests, and re-seal before continuing other work."
    )
}

/// The clean-advance counterpart to `build_on_branch_advance_conflict_note`: the
/// workspace ON the advanced branch (the Coordinator) was re-parented onto the
/// folded tip with no conflict. Needs no action; delivered passively so it rides
/// along into the agent's next natural run.
fn build_on_branch_advance_clean_note(branch: &str) -> String {
    format!(
        "[Base branch update] Your branch `{branch}` advanced — a child merged into it. Your workspace was advanced cleanly onto the new tip; nothing to resolve. No manual rebase or force-push is needed."
    )
}

/// The note for a sibling whose auto-rebase recorded a conflict after the default
/// branch advanced **outside Cairn** (a non-Cairn merge or direct push detected
/// via the GitHub `push` webhook). Same shape as `build_jj_conflict_note` but
/// carries no PR number — there is no Cairn-tracked owner for the advance.
fn build_external_advance_conflict_note(default_branch: &str) -> String {
    format!(
        "⛔ BLOCKING [Base branch update] Your base branch `{default_branch}` advanced (changes landed outside Cairn). Your work was auto-rebased onto the new tip over the shared store and the rebase recorded a conflict. This branch cannot push or merge until you resolve it — jj refuses to push a conflicted commit. Resolve the conflict markers in your workspace now, verify build + tests, and re-seal before continuing other work."
    )
}

/// The clean-rebase counterpart to `build_external_advance_conflict_note`: the
/// default branch advanced outside Cairn and the sibling's auto-rebase landed
/// cleanly. Carries no PR number (no Cairn-tracked owner) and needs no action;
/// delivered passively.
fn build_external_advance_clean_note(default_branch: &str) -> String {
    format!(
        "[Base branch update] Your base branch `{default_branch}` advanced (changes landed outside Cairn). Your work was auto-rebased cleanly onto the new tip; nothing to resolve. No manual rebase or force-push is needed."
    )
}

/// Append a `Conflicting files: a, b, c.` line to a conflict note when the file
/// list is non-empty. An empty or absent list (the enumeration failed or jj
/// reported none) leaves the note unchanged — the file list is advisory detail.
fn append_conflicting_files(note: &str, files: Option<&Vec<String>>) -> String {
    match files {
        Some(files) if !files.is_empty() => {
            format!("{note}\nConflicting files: {}.", files.join(", "))
        }
        _ => note.to_string(),
    }
}

/// Steer every sibling whose auto-rebase recorded a conflict: a conflicted
/// sibling's PR can never advance (jj refuses to push a conflicted commit), so the
/// branch is wedged until the agent resolves the materialized markers and
/// re-seals. `queue_system_direct` enqueues a `Steer` delivery — it wakes an idle
/// recipient and lands at an active recipient's next tool boundary without
/// cancelling the tool call in progress.
/// `files_by_branch` supplies the conflicting file paths per branch, appended to
/// the note so the agent knows exactly where to look. Cleanly-rebased siblings
/// are not in `conflicted`; they receive a passive note via `notify_clean_siblings`.
fn notify_conflicted_siblings(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    conflicted: &[String],
    note: &str,
    files_by_branch: &HashMap<String, Vec<String>>,
) -> Result<(), String> {
    for sibling in siblings {
        let Some(branch) = sibling_branch(sibling) else {
            continue;
        };
        if !conflicted.contains(&branch) {
            continue;
        }
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj reconcile: no run for conflicted sibling {} to steer",
                sibling.id
            );
            continue;
        };
        let message = append_conflicting_files(note, files_by_branch.get(&branch));
        queue_system_direct(orch, &run_id, &message, DeliveryUrgency::Steer)?;
        log::info!(
            "Steered jj sibling job {} to resolve a recorded conflict",
            sibling.id
        );
    }
    Ok(())
}

/// One bookmark whose divergent change the collapse step refused to resolve
/// automatically, carried from the collapse loop to the interrupt layer.
struct AmbiguousDivergence {
    branch: String,
    change_id: String,
    twins: Vec<String>,
}

/// The stop-the-line note for a bookmark carrying an AMBIGUOUS divergent change
/// Cairn declined to collapse (every twin conflicts, or more than one carries
/// edits — picking one automatically could lose work). Names the bookmark, the
/// change-id, and the twin commit ids, and instructs MANUAL resolution +
/// escalation, never a force-push: Cairn owns the deterministic collapse, and a
/// genuinely ambiguous tangle is a human's call, not something the agent papers
/// over by pushing a hand-picked twin.
fn build_ambiguous_divergence_note(branch: &str, change_id: &str, twins: &[String]) -> String {
    format!(
        "⛔ BLOCKING [Divergent change] Your bookmark `{branch}` carries a divergent change `{change_id}` with multiple visible commits ({}) that Cairn could not safely collapse automatically — either every copy still conflicts or more than one carries edits, so picking one could lose work. Resolve it by hand over the shared store (keep the correct commit, abandon the rest), then verify build + tests. Do NOT force-push; if you cannot resolve it cleanly, escalate to a human.",
        twins.join(", ")
    )
}

/// Interrupt every sibling whose bookmark carries an ambiguous divergent change
/// the collapse step refused to resolve. Mirror of `notify_conflicted_siblings`:
/// map each ambiguous branch -> its sibling job -> latest run -> a stop-the-line
/// `Interrupt`. The store was left untouched, so the message names the divergent
/// twins and asks for manual resolution + escalation (never a force-push).
fn notify_ambiguous_divergence(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    ambiguous: &[AmbiguousDivergence],
) -> Result<(), String> {
    for divergence in ambiguous {
        let Some(sibling) = siblings
            .iter()
            .find(|sibling| sibling_branch(sibling).as_deref() == Some(divergence.branch.as_str()))
        else {
            continue;
        };
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj collapse: no run for ambiguous sibling {} to interrupt",
                sibling.id
            );
            continue;
        };
        let message = build_ambiguous_divergence_note(
            &divergence.branch,
            &divergence.change_id,
            &divergence.twins,
        );
        queue_system_direct(orch, &run_id, &message, DeliveryUrgency::Interrupt)?;
        log::info!(
            "Interrupted jj sibling job {} for an ambiguous divergent change on {}",
            sibling.id,
            divergence.branch
        );
    }
    Ok(())
}

/// Notify every sibling whose auto-rebase landed cleanly that its branch moved.
/// Unlike the conflict path this needs no action from the agent: the rebase is
/// done and the cleanly-rebased PR head already advanced. `queue_system_direct`
/// enqueues a `Passive` delivery, which never wakes an idle recipient — the note
/// rides along into the agent's next natural run so the silent rebase is on the
/// record without mechanically resuming an idle agent.
fn notify_clean_siblings(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    clean: &[String],
    note: &str,
) -> Result<(), String> {
    for sibling in siblings {
        let Some(branch) = sibling_branch(sibling) else {
            continue;
        };
        if !clean.contains(&branch) {
            continue;
        }
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj reconcile: no run for cleanly-rebased sibling {} to notify",
                sibling.id
            );
            continue;
        };
        queue_system_direct(orch, &run_id, note, DeliveryUrgency::Passive)?;
        log::info!(
            "Passively notified jj sibling job {} of a clean base-advance rebase",
            sibling.id
        );
    }
    Ok(())
}

async fn load_merged_job_for_owner(
    db: &LocalDb,
    owner_id: &str,
) -> Result<Option<MergedJob>, String> {
    if let Some(job) = load_job_by_id(db, owner_id).await? {
        return Ok(Some(job));
    }

    let Some(action_run) = load_action_run_pr_owner(db, owner_id).await? else {
        return Ok(None);
    };

    if let Some(parent_job_id) = action_run.parent_job_id.as_deref() {
        if let Some(job) = load_job_by_id(db, parent_job_id).await? {
            if job.worktree_path.is_some() && job.base_branch.is_some() {
                return Ok(Some(job));
            }
        }
    }

    if let Some(job) =
        find_context_source_job(db, &action_run.execution_id, &action_run.recipe_node_id).await?
    {
        return Ok(Some(job));
    }

    latest_complete_implementation_job(db, &action_run.execution_id).await
}

#[derive(Debug)]
struct ActionRunOwner {
    execution_id: String,
    recipe_node_id: String,
    parent_job_id: Option<String>,
}

async fn load_job_by_id(db: &LocalDb, job_id: &str) -> Result<Option<MergedJob>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { load_job_by_id_conn(conn, &job_id).await })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_job_by_id_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<MergedJob>> {
    let mut rows = conn
        .query(
            "SELECT id, project_id, issue_id, base_branch, worktree_path
             FROM jobs
             WHERE id = ?1",
            params![job_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            Ok(MergedJob {
                id: row.text(0)?,
                project_id: row.text(1)?,
                issue_id: row.opt_text(2)?,
                base_branch: row.opt_text(3)?,
                worktree_path: row.opt_text(4)?,
            })
        })
        .transpose()
}

async fn load_action_run_pr_owner(
    db: &LocalDb,
    owner_id: &str,
) -> Result<Option<ActionRunOwner>, String> {
    let owner_id = owner_id.to_string();
    db.read(|conn| {
        let owner_id = owner_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT execution_id, recipe_node_id, parent_job_id
                         FROM action_runs
                         WHERE id = ?1",
                    params![owner_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(ActionRunOwner {
                        execution_id: row.text(0)?,
                        recipe_node_id: row.text(1)?,
                        parent_job_id: row.opt_text(2)?,
                    })
                })
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn find_context_source_job(
    db: &LocalDb,
    execution_id: &str,
    pr_node_id: &str,
) -> Result<Option<MergedJob>, String> {
    let execution_id = execution_id.to_string();
    let pr_node_id = pr_node_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let pr_node_id = pr_node_id.clone();
        Box::pin(async move {
            let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
            for edge in snapshot.recipe.edges.iter().filter(|edge| {
                edge.edge_type.to_string() == "context" && edge.target_node_id == pr_node_id
            }) {
                let mut rows = conn
                    .query(
                        "SELECT id, project_id, issue_id, base_branch, worktree_path
                             FROM jobs
                             WHERE execution_id = ?1
                               AND recipe_node_id = ?2
                               AND worktree_path IS NOT NULL
                               AND branch IS NOT NULL
                               AND status <> 'cancelled'
                             ORDER BY created_at DESC
                             LIMIT 1",
                        params![execution_id.as_str(), edge.source_node_id.as_str()],
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    return Ok(Some(MergedJob {
                        id: row.text(0)?,
                        project_id: row.text(1)?,
                        issue_id: row.opt_text(2)?,
                        base_branch: row.opt_text(3)?,
                        worktree_path: row.opt_text(4)?,
                    }));
                }
            }
            Ok(None)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn latest_complete_implementation_job(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<MergedJob>, String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, project_id, issue_id, base_branch, worktree_path
                         FROM jobs
                         WHERE execution_id = ?1
                           AND worktree_path IS NOT NULL
                           AND branch IS NOT NULL
                           AND status = 'complete'
                         ORDER BY completed_at DESC, updated_at DESC
                         LIMIT 1",
                    params![execution_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(MergedJob {
                        id: row.text(0)?,
                        project_id: row.text(1)?,
                        issue_id: row.opt_text(2)?,
                        base_branch: row.opt_text(3)?,
                        worktree_path: row.opt_text(4)?,
                    })
                })
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_execution_snapshot_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
) -> DbResult<ExecutionSnapshot> {
    let mut rows = conn
        .query(
            "SELECT snapshot FROM executions WHERE id = ?1",
            params![execution_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Err(DbError::Row("execution not found".to_string()));
    };
    let Some(snapshot_json) = row.opt_text(0)? else {
        return Err(DbError::Row("execution has no snapshot".to_string()));
    };
    crate::config::snapshot_migrate::load(&snapshot_json)
        .map_err(|error| DbError::Row(error.to_string()))
}

/// In-flight siblings on the same base that may need rebasing after a merge.
/// Beyond the status filter (still-running jobs), this also enumerates a
/// **completed** sibling that still has an **open** PR (`merge_requests.status`
/// not merged/closed): a child whose build job finished but whose PR is awaiting
/// merge is exactly the sibling that must auto-rebase onto the advanced base.
async fn load_sibling_jobs(
    db: &LocalDb,
    project_id: &str,
    base_branch: &str,
    merged_job_id: &str,
) -> Result<Vec<SiblingJob>, String> {
    let project_id = project_id.to_string();
    let base_branch = base_branch.to_string();
    let merged_job_id = merged_job_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let base_branch = base_branch.clone();
        let merged_job_id = merged_job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.id, j.worktree_path, j.branch
                         FROM jobs j
                         WHERE j.project_id = ?1
                           AND j.base_branch = ?2
                           AND j.id != ?3
                           AND j.worktree_path IS NOT NULL
                           AND ( j.status NOT IN ('complete', 'failed', 'cancelled')
                                 OR EXISTS (
                                   SELECT 1 FROM merge_requests mr
                                   WHERE mr.source_branch = j.branch
                                     AND mr.project_id = j.project_id
                                     AND mr.status NOT IN ('merged', 'closed')
                                 ) )",
                    params![
                        project_id.as_str(),
                        base_branch.as_str(),
                        merged_job_id.as_str()
                    ],
                )
                .await?;
            let mut siblings = Vec::new();
            while let Some(row) = rows.next().await? {
                siblings.push(SiblingJob {
                    id: row.text(0)?,
                    worktree_path: row.text(1)?,
                    branch: row.opt_text(2)?,
                });
            }
            Ok(siblings)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The active workspace(s) whose `branch` *is* `branch` itself — the Coordinator
/// sitting ON its integration bookmark, as opposed to the siblings branched
/// *from* it that [`load_sibling_jobs`] returns. After a child folds into the
/// branch, the bookmark advances out from under this workspace's `@`; the sibling
/// auto-rebase never touches it (it rebases branches based ON this one), so it
/// must be advanced explicitly. Same in-flight predicate as `load_sibling_jobs`
/// (still running, or completed with an open PR). Callers dedup by
/// `worktree_path` for the inheritance fan-out.
async fn load_on_branch_workspaces(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<SiblingJob>, String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.id, j.worktree_path, j.branch
                         FROM jobs j
                         WHERE j.project_id = ?1
                           AND j.branch = ?2
                           AND j.worktree_path IS NOT NULL
                           AND ( j.status NOT IN ('complete', 'failed', 'cancelled')
                                 OR EXISTS (
                                   SELECT 1 FROM merge_requests mr
                                   WHERE mr.source_branch = j.branch
                                     AND mr.project_id = j.project_id
                                     AND mr.status NOT IN ('merged', 'closed')
                                 ) )",
                    params![project_id.as_str(), branch.as_str()],
                )
                .await?;
            let mut workspaces = Vec::new();
            while let Some(row) = rows.next().await? {
                workspaces.push(SiblingJob {
                    id: row.text(0)?,
                    worktree_path: row.text(1)?,
                    branch: row.opt_text(2)?,
                });
            }
            Ok(workspaces)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_merge_request_info(
    db: &LocalDb,
    owner_id: &str,
    implementation_job_id: &str,
) -> Result<Option<MergeRequestInfo>, String> {
    let owner_id = owner_id.to_string();
    let implementation_job_id = implementation_job_id.to_string();
    db.read(|conn| {
        let owner_id = owner_id.clone();
        let implementation_job_id = implementation_job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT github_pr_number
                         FROM merge_requests
                         WHERE job_id = ?1 OR job_id = ?2
                         ORDER BY CASE WHEN job_id = ?1 THEN 0 ELSE 1 END
                         LIMIT 1",
                    params![owner_id.as_str(), implementation_job_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(MergeRequestInfo {
                        pr_number: row.opt_i64(0)?,
                    })
                })
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The git-backed checkout path for a project (the source of the jj-managed
/// signal and the anchor for the shared jj store).
async fn load_project_repo_path(db: &LocalDb, project_id: &str) -> Result<Option<String>, String> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT repo_path FROM projects WHERE id = ?1",
                    params![project_id.as_str()],
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Projects eligible for startup default-advance catch-up: those with a local git
/// checkout (`repo_path`) and a known `default_branch`. Remote presence is checked
/// against live git config after this query so local-only projects stay cheap to
/// skip and cloud-only projects never enter the path.
async fn load_projects_for_default_reconcile(
    orch: &Orchestrator,
) -> Result<Vec<(Arc<LocalDb>, DefaultReconcileProject)>, String> {
    let mut all_projects = Vec::new();
    for db in orch.db.all_dbs().await {
        let mut projects = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT id, repo_path, default_branch FROM projects
                         WHERE repo_path IS NOT NULL AND repo_path != ''
                           AND default_branch IS NOT NULL AND default_branch != ''",
                            (),
                        )
                        .await?;
                    let mut projects = Vec::new();
                    while let Some(row) = rows.next().await? {
                        projects.push(DefaultReconcileProject {
                            id: row.text(0)?,
                            repo_path: row.text(1)?,
                            default_branch: row.text(2)?,
                        });
                    }
                    Ok(projects)
                })
            })
            .await
            .map_err(|error| error.to_string())?;
        all_projects.extend(projects.drain(..).map(|project| (db.clone(), project)));
    }
    Ok(all_projects)
}

async fn load_issue_info(db: &LocalDb, issue_id: &str) -> Result<Option<IssueInfo>, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.key, i.number
                         FROM issues i
                         JOIN projects p ON p.id = i.project_id
                         WHERE i.id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(IssueInfo {
                        project_key: row.text(0)?,
                        number: row.i64(1)?,
                    })
                })
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The store-truth sibling filter drops branches whose bookmark is gone (the
    /// stale-`jobs`-row case) and counts them, but a `None` bookmark set (the
    /// store-wide list failed) disables the filter and proceeds with all.
    #[test]
    fn retain_present_siblings_drops_missing_and_honors_none() {
        let live = (
            "agent/CAIRN-1-builder-0".to_string(),
            std::path::PathBuf::from("/w/live"),
        );
        let ghost = (
            "agent/CAIRN-1-ghost-0".to_string(),
            std::path::PathBuf::from("/w/ghost"),
        );
        let existing: std::collections::HashSet<String> = [live.0.clone()].into_iter().collect();

        let (retained, dropped) =
            retain_present_siblings(vec![live.clone(), ghost.clone()], Some(&existing));
        assert_eq!(
            retained,
            vec![live.clone()],
            "the missing-bookmark sibling is dropped before any per-sibling jj work"
        );
        assert_eq!(dropped, 1);

        // A failed store list (None) disables the filter: proceed with all.
        let (all, none_dropped) = retain_present_siblings(vec![live.clone(), ghost.clone()], None);
        assert_eq!(all, vec![live, ghost]);
        assert_eq!(none_dropped, 0);
    }
    use crate::db::DbState;
    use crate::services::testing::{MockGitClient, TestServicesBuilder};
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("base-advance-test.db").await
    }

    fn test_orchestrator(db: LocalDb, git: MockGitClient) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().with_git(git).build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    async fn seed_base_advance_fixture(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'PROJ', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                for (id, number) in [
                    ("issue-1", 1_i64),
                    ("issue-2", 2_i64),
                    ("issue-3", 3_i64),
                    ("issue-4", 4_i64),
                ] {
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                         VALUES (?1, 'proj-1', ?2, 'Issue', 'active', 1, 1)",
                        params![id, number],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                         VALUES (?1, 'recipe-default', ?2, 'proj-1', 'running', 1, 1)",
                        params![format!("exec-{}", number).as_str(), id],
                    )
                    .await?;
                }
                for (job, exec, issue, status, worktree) in [
                    ("job-merged", "exec-1", "issue-1", "complete", "/wt/merged"),
                    ("job-overlap", "exec-2", "issue-2", "running", "/wt/overlap"),
                    ("job-clean", "exec-3", "issue-3", "running", "/wt/clean"),
                    ("job-complete", "exec-4", "issue-4", "complete", "/wt/complete"),
                ] {
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, worktree_path, base_branch, created_at, updated_at)
                         VALUES (?1, ?2, 'node', ?3, 'proj-1', ?4, ?5, 'integration', 1, 1)",
                        params![job, exec, issue, status, worktree],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
                         VALUES (?1, ?2, 'proj-1', ?3, 'live', 1, 1)",
                        params![format!("run-{}", job).as_str(), issue, job],
                    )
                    .await?;
                }
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, github_pr_number)
                     VALUES ('mr-1', 'job-merged', 'proj-1', 'issue-1', 'PR', 'feature', 'integration', 'merged', 1, 1, 42)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn migrated_team_db(path: &Path) -> Arc<LocalDb> {
        let db = Arc::new(LocalDb::open(path).await.unwrap());
        crate::storage::MigrationRunner::new(crate::storage::TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_team_base_advance_notification_fixture(
        db: &LocalDb,
        team_id: &str,
        job_id: &str,
        run_id: &str,
    ) {
        let project_id = format!("{team_id}~00000000-0000-4000-8000-200000000001");
        let issue_id = format!("{team_id}~00000000-0000-4000-8000-200000000002");
        let execution_id = format!("{team_id}~00000000-0000-4000-8000-200000000003");
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES (?1, 'default', 'Team Project', 'TEAM', '/repo/team', 'main', 1, 1)",
            params![project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES (?1, ?2, 9, 'Team Issue', 'active', 1, 1)",
            params![issue_id.as_str(), project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES (?1, 'recipe-default', ?2, ?3, 'running', 1, 1)",
            params![
                execution_id.as_str(),
                issue_id.as_str(),
                project_id.as_str()
            ],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, worktree_path, branch, base_branch, uri_segment, created_at, updated_at)
             VALUES (?1, ?2, 'node', ?3, ?4, 'running', '/wt/team-overlap', 'agent/TEAM-9-builder-0', 'integration', 'builder', 1, 1)",
            params![job_id, execution_id.as_str(), issue_id.as_str(), project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'live', 1, 1)",
            params![run_id, issue_id.as_str(), project_id.as_str(), job_id],
        )
        .await
        .unwrap();
    }

    async fn message_count(db: &LocalDb) -> i64 {
        db.query_one("SELECT COUNT(*) FROM messages", (), |row| row.i64(0))
            .await
            .unwrap()
    }

    #[test]
    fn jj_conflict_note_carries_no_rebase_commands() {
        let issue = IssueInfo {
            project_key: "PROJ".to_string(),
            number: 7,
        };
        let note = build_jj_conflict_note("agent/CAIRN-1940-coordinator-0", Some(42), Some(&issue));
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("PR #42 merged"));
        assert!(note.contains("cairn://p/PROJ/7"));
        assert!(note.contains("auto-rebased"));
        // Stop-the-line: the note names the conflict as blocking, not optional.
        assert!(note.contains("BLOCKING"));
        assert!(note.contains("cannot push or merge"));
        // The note must not instruct a manual rebase/force-push.
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
    }

    #[test]
    fn jj_clean_note_describes_clean_rebase_with_no_action() {
        let issue = IssueInfo {
            project_key: "PROJ".to_string(),
            number: 7,
        };
        let note = build_jj_clean_note("agent/CAIRN-1940-coordinator-0", Some(42), Some(&issue));
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("agent/CAIRN-1940-coordinator-0"));
        assert!(note.contains("PR #42 merged"));
        assert!(note.contains("cairn://p/PROJ/7"));
        assert!(note.contains("cleanly"));
        assert!(note.contains("nothing to resolve"));
        // A clean rebase needs no manual git work.
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
        assert!(note.contains("No manual rebase or force-push is needed"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enumerates_completed_sibling_with_open_pr() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        // The completed sibling has an OPEN merge request on the same base — the
        // live-bug case where reconcile previously found zero siblings because the
        // status filter excluded a `complete` job awaiting merge.
        db.execute_script(
            "UPDATE jobs SET branch = 'agent/PROJ-4-builder-0' WHERE id = 'job-complete';
             INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-complete', 'job-complete', 'proj-1', 'issue-4', 'PR', 'agent/PROJ-4-builder-0', 'integration', 'open', 1, 1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = load_sibling_jobs(&orch.db.local, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        let ids: std::collections::HashSet<&str> = siblings.iter().map(|s| s.id.as_str()).collect();

        assert!(
            ids.contains("job-complete"),
            "a completed sibling with an open PR must be enumerated for rebase"
        );
        assert!(
            ids.contains("job-overlap"),
            "an in-flight sibling is still enumerated"
        );
        assert!(
            ids.contains("job-clean"),
            "an in-flight sibling is still enumerated"
        );
        assert!(
            !ids.contains("job-merged"),
            "the merged job itself is excluded"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_branch_query_selects_coordinator_distinct_from_siblings() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        // The Coordinator: a running job whose BRANCH *is* the integration branch
        // (it sits ON it), branched FROM 'main'. The fixture's other jobs have
        // base_branch = 'integration' and a NULL branch (children branched FROM
        // it). The two queries must be disjoint.
        db.execute_script(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-coord', 'proj-1', 5, 'Coord', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-coord', 'recipe-default', 'issue-coord', 'proj-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, worktree_path, branch, base_branch, created_at, updated_at)
             VALUES ('job-coord', 'exec-coord', 'node', 'issue-coord', 'proj-1', 'running', '/wt/coord', 'integration', 'main', 1, 1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let on_branch = load_on_branch_workspaces(&orch.db.local, "proj-1", "integration")
            .await
            .unwrap();
        let on_ids: std::collections::HashSet<&str> =
            on_branch.iter().map(|s| s.id.as_str()).collect();
        assert!(
            on_ids.contains("job-coord"),
            "the workspace ON the integration branch (the coordinator) is selected"
        );
        assert_eq!(
            on_ids.len(),
            1,
            "only the on-branch workspace; children branched FROM it are excluded"
        );

        // The sibling query (branches based ON integration) must NOT include it.
        let siblings = load_sibling_jobs(&orch.db.local, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        let sib_ids: std::collections::HashSet<&str> =
            siblings.iter().map(|s| s.id.as_str()).collect();
        assert!(
            !sib_ids.contains("job-coord"),
            "the coordinator is not a sibling of itself"
        );
        assert!(
            sib_ids.contains("job-overlap"),
            "siblings are still the children branched from integration"
        );
    }

    #[test]
    fn on_branch_advance_note_carries_no_rebase_commands() {
        let note = build_on_branch_advance_conflict_note("agent/CAIRN-1987-coordinator-0");
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("agent/CAIRN-1987-coordinator-0"));
        assert!(note.contains("a child merged into it"));
        // Stop-the-line: the note names the conflict as blocking, not optional.
        assert!(note.contains("BLOCKING"));
        assert!(note.contains("cannot push or merge"));
        // The advance already happened over the store; no manual rebase commands.
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
    }

    #[test]
    fn on_branch_advance_clean_note_carries_no_rebase_commands() {
        let note = build_on_branch_advance_clean_note("agent/CAIRN-1987-coordinator-0");
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("agent/CAIRN-1987-coordinator-0"));
        assert!(note.contains("a child merged into it"));
        assert!(note.contains("cleanly"));
        assert!(note.contains("nothing to resolve"));
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
        assert!(note.contains("No manual rebase or force-push is needed"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn excludes_completed_sibling_without_open_pr() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = load_sibling_jobs(&orch.db.local, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        let ids: std::collections::HashSet<&str> = siblings.iter().map(|s| s.id.as_str()).collect();

        // job-complete is `complete` with no MR: still excluded.
        assert!(!ids.contains("job-complete"));
        assert!(ids.contains("job-overlap"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn external_advance_enumerates_all_in_flight_siblings_with_no_exclusion() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        // An external default-branch advance has no merged job to exclude, so the
        // sentinel excludes nothing. Every in-flight sibling on the branch is a
        // reconcile candidate; a completed job without an open PR is still out.
        let siblings = load_sibling_jobs(&orch.db.local, "proj-1", "integration", EXCLUDE_NONE)
            .await
            .unwrap();
        let ids: std::collections::HashSet<&str> = siblings.iter().map(|s| s.id.as_str()).collect();

        assert!(
            ids.contains("job-overlap"),
            "in-flight sibling is enumerated"
        );
        assert!(ids.contains("job-clean"), "in-flight sibling is enumerated");
        // job-merged is `complete` with a MERGED merge request; job-complete is
        // `complete` with no MR. Both are excluded even with no job to exclude.
        assert!(
            !ids.contains("job-merged"),
            "a completed job whose PR already merged is not a reconcile candidate"
        );
        assert!(
            !ids.contains("job-complete"),
            "a completed job without an open PR is not a reconcile candidate"
        );
    }

    #[test]
    fn external_advance_note_carries_no_pr_or_rebase_commands() {
        let note = build_external_advance_conflict_note("main");
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("`main`"));
        assert!(note.contains("outside Cairn"));
        assert!(note.contains("auto-rebased"));
        // Stop-the-line: the note names the conflict as blocking, not optional.
        assert!(note.contains("BLOCKING"));
        assert!(note.contains("cannot push or merge"));
        // No Cairn-tracked owner: the note must not reference a PR number.
        assert!(!note.contains("PR #"));
        // The note must not instruct a manual rebase/force-push/fetch.
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
    }

    #[test]
    fn external_advance_clean_note_carries_no_pr_or_rebase_commands() {
        let note = build_external_advance_clean_note("main");
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("`main`"));
        assert!(note.contains("outside Cairn"));
        assert!(note.contains("cleanly"));
        assert!(note.contains("nothing to resolve"));
        // No Cairn-tracked owner: no PR number.
        assert!(!note.contains("PR #"));
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
        assert!(note.contains("No manual rebase or force-push is needed"));
    }

    #[test]
    fn siblings_rewritten_skips_unchanged_commits() {
        let branches = vec![
            "agent/rewritten".to_string(),
            "agent/unchanged".to_string(),
            "agent/missing-after".to_string(),
        ];
        let before: HashMap<String, String> = [
            ("agent/rewritten".to_string(), "commit-a".to_string()),
            ("agent/unchanged".to_string(), "commit-b".to_string()),
            ("agent/missing-after".to_string(), "commit-c".to_string()),
        ]
        .into_iter()
        .collect();
        // `rewritten` moved (this reconcile rewrote it), `unchanged` is a
        // double-fire no-op, `missing-after` failed to resolve post-rebase.
        let after: HashMap<String, String> = [
            ("agent/rewritten".to_string(), "commit-a2".to_string()),
            ("agent/unchanged".to_string(), "commit-b".to_string()),
        ]
        .into_iter()
        .collect();

        let rewritten = siblings_rewritten(&branches, &before, &after);

        assert!(
            rewritten.contains(&"agent/rewritten".to_string()),
            "a sibling this reconcile actually rewrote is notified"
        );
        assert!(
            !rewritten.contains(&"agent/unchanged".to_string()),
            "a double-fire no-op at the same tip is not re-notified"
        );
        assert!(
            rewritten.contains(&"agent/missing-after".to_string()),
            "an unresolved snapshot notifies conservatively rather than dropping a change"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wakes_only_conflicted_jj_siblings() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = vec![
            SiblingJob {
                id: "job-overlap".to_string(),
                worktree_path: "/wt/overlap".to_string(),
                branch: Some("agent/PROJ-2-builder-0".to_string()),
            },
            SiblingJob {
                id: "job-clean".to_string(),
                worktree_path: "/wt/clean".to_string(),
                branch: Some("agent/PROJ-3-builder-0".to_string()),
            },
        ];
        let conflicted = vec!["agent/PROJ-2-builder-0".to_string()];
        let note = build_jj_conflict_note("integration", Some(42), None);
        let mut files_by_branch: HashMap<String, Vec<String>> = HashMap::new();
        files_by_branch.insert(
            "agent/PROJ-2-builder-0".to_string(),
            vec!["shared.rs".to_string(), "lib.rs".to_string()],
        );

        notify_conflicted_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &conflicted,
            &note,
            &files_by_branch,
        )
        .unwrap();

        // Only the conflicted sibling receives a message, and that message names
        // the conflicting files threaded through from `files_by_branch`.
        let messages: Vec<(String, String)> = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT recipient_run_id, content FROM messages ORDER BY created_at",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(row) = rows.next().await? {
                        v.push((row.text(0)?, row.text(1)?));
                    }
                    Ok::<_, DbError>(v)
                })
            })
            .await
            .unwrap();

        assert_eq!(
            messages.len(),
            1,
            "only the conflicted sibling is messaged; the cleanly-rebased one is not"
        );
        assert_eq!(messages[0].0, "run-job-overlap");
        assert!(
            messages[0]
                .1
                .contains("Conflicting files: shared.rs, lib.rs."),
            "the conflict note names the conflicting files: {}",
            messages[0].1
        );

        // The push is waking but non-interrupting: it steers the agent at the next
        // boundary instead of cancelling an active tool call, and remains distinct
        // from the `passive` clean-rebase note asserted in
        // `notify_clean_siblings_passively`.
        let wake: String = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT wake FROM attention_pushes WHERE recipient = 'job-overlap'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("no push for job-overlap".to_string()))?;
                    row.text(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            wake, "wake",
            "a base-advance conflict wakes or steers the agent without cancelling the active turn"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn team_conflicted_sibling_notification_lands_in_team_replica() {
        let private = migrated_db().await;
        let orch = test_orchestrator(private, MockGitClient::new());
        let team_temp = tempfile::tempdir().unwrap();
        let team_id = "teambase";
        let team_db = migrated_team_db(&team_temp.path().join("team-base-test.db")).await;
        let job_id = "teambase~00000000-0000-4000-8000-200000000010";
        let run_id = "teambase~00000000-0000-4000-8000-200000000011";
        seed_team_base_advance_notification_fixture(&team_db, team_id, job_id, run_id).await;
        orch.db
            .insert_team_db_for_test(team_id, team_db.clone())
            .await;

        let siblings = vec![SiblingJob {
            id: job_id.to_string(),
            worktree_path: "/wt/team-overlap".to_string(),
            branch: Some("agent/TEAM-9-builder-0".to_string()),
        }];
        let conflicted = vec!["agent/TEAM-9-builder-0".to_string()];
        let note = build_jj_conflict_note("integration", Some(42), None);
        let mut files_by_branch: HashMap<String, Vec<String>> = HashMap::new();
        files_by_branch.insert(
            "agent/TEAM-9-builder-0".to_string(),
            vec!["team.rs".to_string()],
        );

        notify_conflicted_siblings(
            &orch,
            &team_db,
            &siblings,
            &conflicted,
            &note,
            &files_by_branch,
        )
        .unwrap();

        assert_eq!(
            message_count(&team_db).await,
            1,
            "the base-advance direct message must be written to the team replica"
        );
        assert_eq!(
            message_count(&orch.db.local).await,
            0,
            "a team base-advance notification must not fall back to the private database"
        );
        let wake: String = team_db
            .query_one(
                "SELECT wake FROM attention_pushes WHERE recipient = ?1",
                params![job_id],
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            wake, "wake",
            "team base-advance conflicts steer without interrupting active turns"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ambiguous_divergence_interrupts_only_the_affected_sibling() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = vec![
            SiblingJob {
                id: "job-overlap".to_string(),
                worktree_path: "/wt/overlap".to_string(),
                branch: Some("agent/PROJ-2-builder-0".to_string()),
            },
            SiblingJob {
                id: "job-clean".to_string(),
                worktree_path: "/wt/clean".to_string(),
                branch: Some("agent/PROJ-3-builder-0".to_string()),
            },
        ];
        let ambiguous = vec![AmbiguousDivergence {
            branch: "agent/PROJ-2-builder-0".to_string(),
            change_id: "qpvuntsmxyzw".to_string(),
            twins: vec!["aaaa1111".to_string(), "bbbb2222".to_string()],
        }];

        notify_ambiguous_divergence(&orch, &orch.db.local, &siblings, &ambiguous).unwrap();

        // Only the ambiguous sibling is messaged, and the note names the
        // change-id, both twin commit ids, and the no-force-push instruction.
        let messages: Vec<(String, String)> = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT recipient_run_id, content FROM messages ORDER BY created_at",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(row) = rows.next().await? {
                        v.push((row.text(0)?, row.text(1)?));
                    }
                    Ok::<_, DbError>(v)
                })
            })
            .await
            .unwrap();

        assert_eq!(
            messages.len(),
            1,
            "only the ambiguous sibling is interrupted; the healthy one is not"
        );
        assert_eq!(messages[0].0, "run-job-overlap");
        assert!(
            messages[0].1.contains("qpvuntsmxyzw"),
            "names the change-id: {}",
            messages[0].1
        );
        assert!(messages[0].1.contains("aaaa1111") && messages[0].1.contains("bbbb2222"));
        assert!(messages[0].1.contains("Do NOT force-push"));

        // Delivered as a stop-the-line interrupt (a divergent tangle wedges the
        // branch the same way a recorded conflict does).
        let wake: String = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT wake FROM attention_pushes WHERE recipient = 'job-overlap'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("no push for job-overlap".to_string()))?;
                    row.text(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            wake, "interrupt",
            "an ambiguous divergence interrupts the agent — stop-the-line"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn notify_clean_siblings_passively() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = vec![SiblingJob {
            id: "job-clean".to_string(),
            worktree_path: "/wt/clean".to_string(),
            branch: Some("agent/PROJ-3-builder-0".to_string()),
        }];
        let clean = vec!["agent/PROJ-3-builder-0".to_string()];
        let note = build_jj_clean_note("integration", Some(42), None);

        notify_clean_siblings(&orch, &orch.db.local, &siblings, &clean, &note).unwrap();

        // (a) the cleanly-rebased sibling receives a direct note.
        let recipients: Vec<String> = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT recipient_run_id FROM messages ORDER BY created_at",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(row) = rows.next().await? {
                        v.push(row.text(0)?);
                    }
                    Ok::<_, DbError>(v)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            recipients,
            vec!["run-job-clean".to_string()],
            "the cleanly-rebased sibling receives a direct note"
        );

        // (b) its attention push is non-waking (`passive`), so an idle agent is
        // never resumed by it — the note rides along on the next natural run.
        let wake: String = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT wake FROM attention_pushes WHERE recipient = 'job-clean'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("no push for job-clean".to_string()))?;
                    row.text(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            wake, "passive",
            "a clean base-advance note is delivered passively and never wakes an idle agent"
        );
    }

    #[test]
    fn remote_default_revset_targets_origin() {
        assert_eq!(remote_default_revset("main"), "main@origin");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn project_has_origin_uses_live_git_config() {
        let db = migrated_db().await;
        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(mockall::predicate::function(|path: &Path| {
                path == Path::new("/repo/remote")
            }))
            .returning(|_| Ok("https://github.com/acme/repo.git".to_string()));
        let orch = test_orchestrator(db, git);

        assert!(project_has_origin(&orch, Path::new("/repo/remote")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn project_has_origin_skips_missing_or_empty_origin() {
        let db = migrated_db().await;
        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(mockall::predicate::function(|path: &Path| {
                path == Path::new("/repo/no-remote")
            }))
            .returning(|_| Err("No such remote 'origin'".to_string()));
        git.expect_remote_get_url()
            .with(mockall::predicate::function(|path: &Path| {
                path == Path::new("/repo/empty")
            }))
            .returning(|_| Ok("   ".to_string()));
        let orch = test_orchestrator(db, git);

        assert!(!project_has_origin(&orch, Path::new("/repo/no-remote")));
        assert!(!project_has_origin(&orch, Path::new("/repo/empty")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn default_reconcile_projects_skip_cloud_only_and_branchless() {
        let db = migrated_db().await;
        // p-ok: a local checkout with a default branch — eligible. p-no-repo: no
        // local checkout (cloud-only) — nothing to advance. p-no-branch: no
        // default branch — nothing to reconcile onto.
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('p-ok', 'default', 'Ok', 'OK', '/repo/ok', 'main', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('p-no-repo', 'default', 'NoRepo', 'NR', '', 'main', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('p-no-branch', 'default', 'NoBranch', 'NB', '/repo/nb', NULL, 1, 1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let projects = load_projects_for_default_reconcile(&orch).await.unwrap();
        let ids: std::collections::HashSet<&str> = projects
            .iter()
            .map(|(_, project)| project.id.as_str())
            .collect();

        assert!(
            ids.contains("p-ok"),
            "a project with a local checkout and default branch is eligible"
        );
        assert!(
            !ids.contains("p-no-repo"),
            "a cloud-only project with no local checkout is skipped"
        );
        assert!(
            !ids.contains("p-no-branch"),
            "a project with no default branch is skipped"
        );
        let ok = projects
            .iter()
            .find(|(_, project)| project.id == "p-ok")
            .unwrap();
        assert_eq!(
            ok.1.repo_path, "/repo/ok",
            "the repository path is returned for live remote detection"
        );
        assert_eq!(
            ok.1.default_branch, "main",
            "the default branch is returned alongside the id"
        );
    }
}
