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
use crate::messages::delivery::{
    latest_run_for_job, queue_system_direct, queue_system_direct_once,
    queue_system_direct_once_confirmed, DirectQueueDisposition,
};
use crate::messages::queued::DeliveryUrgency;
use crate::models::ExecutionSnapshot;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::executor_protocol::{LifetimeLeaseOperation, LifetimeLeaseResult};
use cairn_db::turso::params;

#[derive(Debug)]
struct MergedJob {
    id: String,
    project_id: String,
    issue_id: Option<String>,
    base_branch: Option<String>,
    worktree_path: Option<String>,
}

struct DurableReconcileWork {
    claim: ReconcileClaim,
    store: std::path::PathBuf,
    target_branch: String,
    destination_commit: String,
    sources: Vec<String>,
}

fn on_branch_ambiguous_delivery_key(
    project_id: &str,
    branch: &str,
    fingerprint: &str,
    run_id: &str,
) -> String {
    format!("on-branch:{project_id}:{branch}:{fingerprint}:{run_id}:ambiguous")
}

async fn activate_notified_quarantines(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    pending: &[PendingReconcileQuarantine],
    notified: &[String],
) -> Result<(), String> {
    for quarantine in pending {
        if !notified.contains(&quarantine.bookmark) {
            continue;
        }
        upsert_reconcile_quarantine(
            db,
            project_id,
            store,
            &quarantine.bookmark,
            &quarantine.failure_kind,
            &quarantine.fingerprint,
            quarantine.diagnostic.as_deref(),
        )
        .await?;
    }

    Ok(())
}

struct PendingReconcileQuarantine {
    bookmark: String,
    failure_kind: String,
    fingerprint: String,
    diagnostic: Option<String>,
}

fn reconcile_has_transient_failures(failed: &[crate::jj::ReconcileFailure]) -> bool {
    failed.iter().any(|failure| {
        !crate::jj::reconcile_failure_is_permanent(crate::jj::reconcile_failure_kind(
            &failure.error,
        ))
    })
}

#[derive(Debug, Clone)]
struct ReconcileQuarantine {
    failure_kind: String,
    fingerprint: String,
    last_diagnostic: Option<String>,
}

async fn load_reconcile_quarantine(
    db: &LocalDb,
    project_id: &str,
    store_path: &Path,
    bookmark: &str,
) -> Result<Option<ReconcileQuarantine>, String> {
    let project_id = project_id.to_string();
    let store_path = store_path.to_string_lossy().into_owned();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT failure_kind, fingerprint, last_diagnostic
                     FROM jj_reconcile_quarantines
                     WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3",
                    params![project_id, store_path, bookmark],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(ReconcileQuarantine {
                failure_kind: row.text(0)?,
                fingerprint: row.text(1)?,
                last_diagnostic: row.opt_text(2)?,
            }))
        })
    })
    .await
    .map_err(|error| format!("load reconcile quarantine: {error}"))
}

async fn upsert_reconcile_quarantine(
    db: &LocalDb,
    project_id: &str,
    store_path: &Path,
    bookmark: &str,
    failure_kind: &str,
    fingerprint: &str,
    diagnostic: Option<&str>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO jj_reconcile_quarantines
         (project_id, store_path, bookmark, failure_kind, fingerprint,
          last_diagnostic, strike_count, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?7)
         ON CONFLICT(project_id, store_path, bookmark) DO UPDATE SET
           failure_kind = excluded.failure_kind,
           fingerprint = excluded.fingerprint,
           last_diagnostic = excluded.last_diagnostic,
           strike_count = jj_reconcile_quarantines.strike_count + 1,
           updated_at = excluded.updated_at",
        params![
            project_id,
            store_path.to_string_lossy().as_ref(),
            bookmark,
            failure_kind,
            fingerprint,
            diagnostic,
            now
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("persist reconcile quarantine: {error}"))
}

async fn release_reconcile_quarantine(
    db: &LocalDb,
    project_id: &str,
    store_path: &Path,
    bookmark: &str,
) -> Result<(), String> {
    db.execute(
        "DELETE FROM jj_reconcile_quarantines
         WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3",
        params![project_id, store_path.to_string_lossy().as_ref(), bookmark],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("release reconcile quarantine: {error}"))
}

fn divergence_fingerprint(twins: &[String]) -> String {
    let mut twins = twins.to_vec();
    twins.sort();
    twins.join("+")
}

async fn heartbeat_reconcile_intent(db: &LocalDb, claim: &ReconcileClaim) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    let changed = db
        .execute(
            "UPDATE jj_reconcile_intents SET lease_expires_at = ?3, updated_at = ?4
         WHERE id = ?1 AND lease_owner = ?2 AND status = 'running'",
            params![
                claim.id.as_str(),
                claim.owner.as_str(),
                now + RECONCILE_LEASE_SECONDS,
                now
            ],
        )
        .await
        .map_err(|error| format!("heartbeat reconcile intent: {error}"))?;
    if changed == 0 {
        return Err("reconcile intent lease ownership was lost".into());
    }

    Ok(())
}

async fn mark_reconcile_delivered(db: &LocalDb, intent_id: &str) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items
         SET status = CASE WHEN status = 'graph_moved' THEN 'completed' ELSE status END,
             notification_sent = 1, updated_at = ?2
         WHERE intent_id = ?1 AND status IN ('graph_moved', 'suppressed')",
        params![intent_id, chrono::Utc::now().timestamp()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("persist reconcile delivery: {error}"))
}

#[derive(Debug)]
struct SiblingJob {
    id: String,
    worktree_path: String,
    branch: Option<String>,
    base_commit: Option<String>,
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
pub(crate) async fn notify_downstream_of_base_advance(
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BranchAdvanceOutcome {
    eligible: usize,
    rebased_clean: usize,
    conflicted: usize,
    failed: usize,
}

/// Canonical propagation seam for a managed branch whose local bookmark advanced.
/// The destination is pinned to the observed commit so a later bookmark movement
/// cannot silently change the base being adopted by downstream jobs.
pub(crate) async fn reconcile_managed_branch_advance(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &str,
    advanced_branch: &str,
    new_tip: &str,
    source_job_to_exclude: Option<&str>,
) -> Result<BranchAdvanceOutcome, String> {
    let db = owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    {
        let store_guard = orch
            .acquire_jj_store_lock(
                &store,
                format!("managed branch {advanced_branch} on-branch advance to {new_tip}"),
            )
            .await;
        let _phase = store_guard.phase("on-branch workspace advance");
        advance_on_branch_workspaces(orch, &db, project_id, advanced_branch, repo_path).await;
    }
    let siblings = load_sibling_jobs(
        &db,
        project_id,
        advanced_branch,
        source_job_to_exclude.unwrap_or(EXCLUDE_NONE),
    )
    .await?;
    let eligible = siblings.len();
    let mut outcome = if siblings.is_empty() {
        BranchAdvanceOutcome::default()
    } else {
        let notes = BaseAdvanceNotes {
            conflict: build_jj_conflict_note(advanced_branch, None, None),
            clean: build_jj_clean_note(advanced_branch, None, None),
        };
        reconcile_base_advance(
            orch,
            &db,
            project_id,
            &format!("managed branch {advanced_branch} advanced to {new_tip}"),
            repo_path,
            advanced_branch,
            new_tip,
            siblings,
            notes,
        )
        .await?
    };
    outcome.eligible = eligible;
    outcome.failed +=
        refresh_terminal_leases_for_branch(orch, &db, project_id, advanced_branch, new_tip).await;
    Ok(outcome)
}

async fn refresh_terminal_leases_for_branch(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    new_tip: &str,
) -> usize {
    let rows = load_live_terminal_leases(db, project_id, branch).await;
    let Ok(leases) = rows else {
        log::error!("committed branch advance could not enumerate terminal leases: {rows:?}");
        return 1;
    };
    let mut failed = 0;
    for (lease_id, incarnation_id, lease_epoch, job_id) in leases {
        let result = orch
            .fleet
            .operate_lifetime_lease(
                orch,
                LifetimeLeaseOperation::RefreshCheckout {
                    fence: cairn_common::executor_protocol::LifetimeLeaseFence {
                        lease_id: lease_id.clone(),
                        owner: cairn_common::executor_protocol::LifetimeLeaseOwner {
                            kind: cairn_common::executor_protocol::LifetimeLeaseOwnerKind::Terminal,
                            owner_id: job_id.clone(),
                        },
                        incarnation_id: incarnation_id.clone(),
                        lease_epoch,
                    },
                    base_commit: new_tip.to_string(),
                },
            )
            .await;
        if let LifetimeLeaseResult::Failed {
            kind, diagnostic, ..
        } = result
        {
            if kind == cairn_common::executor_protocol::LifetimeLeaseFailureKind::Unavailable {
                failed += 1;
                log::warn!(
                    "terminal lease {lease_id} could not be refreshed while its executor is disconnected: {diagnostic}"
                );
                continue;
            }
            if kind == cairn_common::executor_protocol::LifetimeLeaseFailureKind::NotFound {
                match crate::terminal_host::resolve_missing_terminal_lease(
                    db,
                    &lease_id,
                    &incarnation_id,
                    lease_epoch,
                )
                .await
                {
                    Ok(true) => {
                        log::warn!("terminal lease {lease_id} no longer exists on an executor; cleared its persisted fence");
                        if let Some(run_id) = latest_run_for_job(db, &job_id) {
                            let note = format!(
                                "[Terminal ended] Cairn's executor no longer reports terminal lease {lease_id}. Its stale lease binding was cleared; restart the terminal to acquire a fresh checkout."
                            );
                            if let Err(error) =
                                queue_system_direct(orch, &run_id, &note, DeliveryUrgency::Passive)
                            {
                                log::error!("could not notify terminal owner {job_id} after its lease ended: {error}");
                            }
                        }
                    }
                    Ok(false) => {}
                    Err(error) => {
                        failed += 1;
                        log::error!("could not clear missing terminal lease {lease_id}: {error}");
                    }
                }
                continue;
            }
            failed += 1;
            log::error!("committed branch advance could not refresh terminal lease {lease_id}: {diagnostic}");
            if let Some(run_id) = latest_run_for_job(db, &job_id) {
                let note = format!(
                    "⛔ BLOCKING [Terminal head reconciliation] The branch commit succeeded, but Cairn could not advance terminal lease {lease_id} to {new_tip}. The committed branch was not rolled back. Exact executor diagnostic: {diagnostic}"
                );
                if let Err(error) =
                    queue_system_direct(orch, &run_id, &note, DeliveryUrgency::Steer)
                {
                    log::error!(
                        "could not notify terminal owner {job_id} after refresh failure: {error}"
                    );
                }
            }
        }
    }
    failed
}

async fn load_live_terminal_leases(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> crate::storage::DbResult<Vec<(String, String, u64, String)>> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT t.lease_id, t.lease_incarnation_id, t.lease_epoch, t.job_id
             FROM job_terminals t JOIN jobs j ON j.id = t.job_id
             WHERE j.project_id = ?1 AND j.branch = ?2
               AND t.status = 'running' AND t.lease_id IS NOT NULL",
                    (project_id.as_str(), branch.as_str()),
                )
                .await?;
            let mut leases = Vec::new();
            while let Some(row) = rows.next().await? {
                leases.push((
                    row.text(0)?,
                    row.text(1)?,
                    row.get::<i64>(2)? as u64,
                    row.text(3)?,
                ));
            }
            Ok(leases)
        })
    })
    .await
}

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
    // Serialize each bounded jj mutation on the project store. The on-branch
    // advance is one transaction; downstream reconciliation later reacquires per
    // sibling and yields between them. Merge, webhook, and startup triggers can
    // overlap, so the durable intent lease coalesces them while the shared mutex
    // remains the sole jj/ref/history writer.
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    // Advance the workspace that sits ON the merged branch (a Coordinator on its
    // integration bookmark) onto the freshly-folded tip. This is asymmetric to
    // the sibling reconcile below: `reconcile_siblings` rebases the *children*
    // (branched FROM the branch); nobody otherwise re-parents the workspace whose
    // branch IS the branch, so the fold moves the bookmark out from under its `@`
    // and a later edit+seal would orphan off the advanced branch. Runs
    // independently of (and before) the sibling reconcile — a coordinator must be
    // advanced even when it has no other in-flight siblings.
    {
        let guard = orch
            .acquire_jj_store_lock(&store, format!("jj on-branch advance for {merged_job_id}"))
            .await;
        let _phase = guard.phase("on-branch workspace advance");
        advance_on_branch_workspaces(orch, db, &merged_job.project_id, base_branch, repo_path)
            .await;
    }

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
        &merged_job.project_id,
        &format!("merged job {}", merged_job.id),
        repo_path,
        base_branch,
        base_branch,
        siblings,
        notes,
    )
    .await
    .map(|_| ())
}

fn remote_default_revset(default_branch: &str) -> String {
    format!("{default_branch}@origin")
}

async fn fetch_origin_outside_store_lock(
    orch: &Orchestrator,
    repo_path: &Path,
) -> Result<(), String> {
    let git = orch.services.git.clone();
    let repo_path = repo_path.to_path_buf();
    tokio::task::spawn_blocking(move || git.fetch_origin(&repo_path))
        .await
        .map_err(|error| format!("git fetch origin task failed: {error}"))?
}

/// Reconcile in-flight siblings after the project's default branch advanced
/// **outside Cairn** (a non-Cairn PR merged in the GitHub UI, or a direct push to
/// the default branch), detected via the GitHub `push` webhook. Thin wrapper over
/// [`reconcile_default_advance`] with the `Remote` source.
pub(crate) async fn reconcile_external_default_advance(
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
    let repo_path_path = Path::new(&repo_path);
    // Transfer objects and update the ordinary Git repository's origin-tracking
    // refs before entering the jj critical section. Git object writes are
    // content-addressed and additive; jj observes the fetched refs only when the
    // locked import below runs, preserving the store's single-writer discipline.
    if let Err(error) = fetch_origin_outside_store_lock(orch, repo_path_path).await {
        log::warn!("external advance on {default_branch}: git fetch origin failed: {error}");
        return Ok(());
    }

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path_path);
    {
        let store_guard = orch
            .acquire_jj_store_lock(&store, format!("external import on {default_branch}"))
            .await;
        let _phase = store_guard.phase("git ref import");
        if let Err(error) = crate::jj::ensure_project_store(&jj, &store, repo_path_path) {
            log::warn!("external advance on {default_branch}: ensure store failed: {error}");
            return Ok(());
        }
    }

    let notes = BaseAdvanceNotes {
        conflict: build_external_advance_conflict_note(default_branch),
        clean: build_external_advance_clean_note(default_branch),
    };
    reconcile_base_advance(
        orch,
        &db,
        project_id,
        &format!("external advance on {default_branch}"),
        &repo_path,
        default_branch,
        &remote_default_revset(default_branch),
        siblings,
        notes,
    )
    .await
    .map(|_| ())
}

/// One-time startup catch-up for remote default-branch advances that landed while
/// Cairn was closed. This is intentionally not a sweep: no-remote projects are
/// skipped because nothing outside Cairn can advance them, and remote projects
/// only reconcile when fetching `origin` actually changes the stored remote
/// default tip. An unchanged base never reaches the sibling rebase path.
pub(crate) async fn reconcile_startup_remote_default_advances(orch: &Orchestrator) {
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

    let repo_path = Path::new(&project.repo_path);
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let remote_default = remote_default_revset(&project.default_branch);
    let before = {
        let _store_guard = orch
            .acquire_jj_store_lock(
                &store,
                format!(
                    "startup remote advance snapshot on {}",
                    project.default_branch
                ),
            )
            .await;
        if let Err(error) = crate::jj::ensure_project_store(&jj, &store, repo_path) {
            log::warn!(
                "startup remote advance on {}: ensure store failed: {error}",
                project.default_branch
            );
            return Ok(());
        }
        crate::jj::revset_commit(&jj, &store, &remote_default)
    };

    if let Err(error) = fetch_origin_outside_store_lock(orch, repo_path).await {
        log::warn!(
            "startup remote advance on {}: git fetch origin failed: {error}",
            project.default_branch
        );
        return Ok(());
    }

    let after = {
        let store_guard = orch
            .acquire_jj_store_lock(
                &store,
                format!(
                    "startup remote advance import on {}",
                    project.default_branch
                ),
            )
            .await;
        let _phase = store_guard.phase("git ref import");
        if let Err(error) = crate::jj::ensure_project_store(&jj, &store, repo_path) {
            log::warn!(
                "startup remote advance on {}: ensure store failed after fetch: {error}",
                project.default_branch
            );
            return Ok(());
        }
        crate::jj::revset_commit(&jj, &store, &remote_default)
    };
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
        &project.id,
        &format!("startup external advance on {}", project.default_branch),
        &project.repo_path,
        &project.default_branch,
        &remote_default,
        siblings,
        notes,
    )
    .await
    .map(|_| ())
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
struct ReconcileClaim {
    id: String,
    owner: String,
    project_id: String,
}

const RECONCILE_LEASE_SECONDS: i64 = 600;

async fn release_reconcile_claim(db: &LocalDb, claim: &ReconcileClaim) {
    if let Err(error) = db
        .execute(
            "UPDATE jj_reconcile_intents
             SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL, updated_at = ?3
             WHERE id = ?1 AND lease_owner = ?2",
            params![
                claim.id.as_str(),
                claim.owner.as_str(),
                chrono::Utc::now().timestamp()
            ],
        )
        .await
    {
        log::warn!(
            "failed to release reconcile claim {} for retry: {error}",
            claim.id
        );
    }
}

async fn claim_next_reconcile_intent(db: &LocalDb) -> Result<Option<DurableReconcileWork>, String> {
    db.write(|conn| {
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let mut rows = conn
                .query(
                    "SELECT id, project_id, store_path, target_branch, destination_commit,
                            trigger_sources_json
                     FROM jj_reconcile_intents
                     WHERE status = 'pending'
                        OR (status = 'running' AND COALESCE(lease_expires_at, 0) <= ?1)
                     ORDER BY updated_at ASC LIMIT 1",
                    (now,),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let id = row.text(0)?;
            let project_id = row.text(1)?;
            let store = std::path::PathBuf::from(row.text(2)?);
            let target_branch = row.text(3)?;
            let destination_commit = row.text(4)?;
            let sources = serde_json::from_str(&row.text(5)?).unwrap_or_default();
            let owner = uuid::Uuid::new_v4().to_string();
            let changed = conn
                .execute(
                    "UPDATE jj_reconcile_intents
                     SET status = 'running', lease_owner = ?2, lease_expires_at = ?3,
                         updated_at = ?4
                     WHERE id = ?1 AND (status = 'pending'
                        OR (status = 'running' AND COALESCE(lease_expires_at, 0) <= ?4))",
                    params![
                        id.as_str(),
                        owner.as_str(),
                        now + RECONCILE_LEASE_SECONDS,
                        now
                    ],
                )
                .await?;
            if changed == 0 {
                return Ok(None);
            }
            Ok(Some(DurableReconcileWork {
                claim: ReconcileClaim {
                    id,
                    owner,
                    project_id,
                },
                store,
                target_branch,
                destination_commit,
                sources,
            }))
        })
    })
    .await
    .map_err(|error| format!("claim pending reconcile intent: {error}"))
}

async fn execute_durable_reconcile_work(
    orch: &Orchestrator,
    db: &LocalDb,
    work: DurableReconcileWork,
) -> Result<(), String> {
    let project_id = work.claim.project_id.clone();
    let repo_path = load_project_repo_path(db, &project_id)
        .await?
        .ok_or_else(|| format!("project {project_id} has no repository path"))?;
    let siblings = load_sibling_jobs(db, &project_id, &work.target_branch, EXCLUDE_NONE).await?;
    let specs = siblings
        .iter()
        .filter_map(|sibling| {
            Some((
                sibling_branch(sibling)?,
                std::path::PathBuf::from(&sibling.worktree_path),
            ))
        })
        .collect::<Vec<_>>();
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let candidate_names = specs
        .iter()
        .map(|(branch, _)| branch.clone())
        .collect::<Vec<_>>();
    let existing_bookmarks =
        crate::jj::query_local_bookmarks(&jj, &work.store, &candidate_names).ok();
    let external = work
        .sources
        .iter()
        .any(|source| source.contains("external advance"));
    let rebase_dest = if external {
        remote_default_revset(&work.target_branch)
    } else {
        work.target_branch.clone()
    };
    let notes = BaseAdvanceNotes {
        conflict: build_jj_conflict_note(&work.target_branch, None, None),
        clean: build_jj_clean_note(&work.target_branch, None, None),
    };
    let label = format!("durable retry on {}", work.target_branch);
    execute_reconcile_claim(
        orch,
        db,
        &project_id,
        &label,
        &repo_path,
        &rebase_dest,
        siblings,
        notes,
        specs,
        existing_bookmarks,
        work.destination_commit,
        work.claim,
    )
    .await
    .map(|_| ())
}

pub(crate) async fn sweep_reconcile_intents(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        loop {
            let work = match claim_next_reconcile_intent(&db).await {
                Ok(Some(work)) => work,
                Ok(None) => break,
                Err(error) => {
                    log::warn!("jj reconcile worker failed to claim durable work: {error}");
                    break;
                }
            };
            let claim = ReconcileClaim {
                id: work.claim.id.clone(),
                owner: work.claim.owner.clone(),
                project_id: work.claim.project_id.clone(),
            };
            if let Err(error) = execute_durable_reconcile_work(orch, &db, work).await {
                log::warn!(
                    "jj reconcile worker failed durable intent {}: {error}",
                    claim.id
                );
                release_reconcile_claim(&db, &claim).await;
                break;
            }
        }
    }
}

async fn claim_reconcile_intent(
    db: &LocalDb,
    repo_path: &str,
    store: &Path,
    target_branch: &str,
    destination: &str,
    source: &str,
) -> Result<Option<ReconcileClaim>, String> {
    let repo_path = repo_path.to_string();
    let store = store.to_string_lossy().into_owned();
    let target_branch = target_branch.to_string();
    let destination = destination.to_string();
    let source = source.to_string();
    db.write(|conn| {
        let repo_path = repo_path.clone();
        let store = store.clone();
        let target_branch = target_branch.clone();
        let destination = destination.clone();
        let source = source.clone();
        Box::pin(async move {
            let mut project_rows = conn
                .query(
                    "SELECT id FROM projects WHERE repo_path = ?1 LIMIT 1",
                    (repo_path.as_str(),),
                )
                .await?;
            let Some(project) = project_rows.next().await? else {
                return Ok(None);
            };
            let project_id = project.text(0)?;
            let mut rows = conn
                .query(
                    "SELECT id, trigger_sources_json, status, lease_expires_at
                     FROM jj_reconcile_intents
                     WHERE project_id = ?1 AND store_path = ?2
                       AND target_branch = ?3 AND destination_commit = ?4",
                    params![
                        project_id.as_str(),
                        store.as_str(),
                        target_branch.as_str(),
                        destination.as_str()
                    ],
                )
                .await?;
            let now = chrono::Utc::now().timestamp();
            if let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                let mut sources: Vec<String> =
                    serde_json::from_str(&row.text(1)?).unwrap_or_default();
                if !sources.contains(&source) {
                    sources.push(source);
                }
                let status = row.text(2)?;
                let lease_expires_at = row.get::<Option<i64>>(3)?.unwrap_or(0);
                let sources = serde_json::to_string(&sources).unwrap_or_else(|_| "[]".into());
                conn.execute(
                    "UPDATE jj_reconcile_intents
                     SET trigger_sources_json = ?2, updated_at = ?3
                     WHERE id = ?1",
                    params![id.as_str(), sources.as_str(), now],
                )
                .await?;
                if status == "completed" || (status == "running" && lease_expires_at > now) {
                    return Ok(None);
                }
                let owner = uuid::Uuid::new_v4().to_string();
                let claimed = conn.execute(
                    "UPDATE jj_reconcile_intents
                     SET status = 'running', lease_owner = ?2, lease_expires_at = ?3,
                         updated_at = ?4 WHERE id = ?1 AND (status != 'running' OR lease_expires_at <= ?4)",
                    params![id.as_str(), owner.as_str(), now + RECONCILE_LEASE_SECONDS, now],
                )
                .await?;
                if claimed == 0 {
                    return Ok(None);
                }
                return Ok(Some(ReconcileClaim {
                    id,
                    owner,
                    project_id,
                }));
            }

            let id = uuid::Uuid::new_v4().to_string();
            let owner = uuid::Uuid::new_v4().to_string();
            let sources = serde_json::to_string(&vec![source]).unwrap();
            conn.execute(
                "INSERT INTO jj_reconcile_intents
                 (id, project_id, store_path, target_branch, destination_commit,
                  trigger_sources_json, status, lease_owner, lease_expires_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7, ?8, ?9, ?9)",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    store.as_str(),
                    target_branch.as_str(),
                    destination.as_str(),
                    sources.as_str(),
                    owner.as_str(),
                    now + RECONCILE_LEASE_SECONDS,
                    now
                ],
            )
            .await?;
            Ok(Some(ReconcileClaim {
                id,
                owner,
                project_id,
            }))
        })
    })
    .await
    .map_err(|error| format!("claim reconcile intent: {error}"))
}

#[derive(Debug)]
struct ReconcileItemProgress {
    status: String,
    observed_tip: Option<String>,
    fingerprint: Option<String>,
    failure_kind: Option<String>,
    outcome_kind: Option<String>,
    notification_sent: bool,
}

async fn reconcile_item_status(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
) -> Result<Option<ReconcileItemProgress>, String> {
    let intent_id = intent_id.to_string();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn.query(
            "SELECT status, observed_tip, suppression_fingerprint, failure_kind, outcome_kind, notification_sent
             FROM jj_reconcile_items WHERE intent_id = ?1 AND bookmark = ?2",
            params![intent_id.as_str(), bookmark.as_str()],
        ).await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(ReconcileItemProgress {
                status: row.text(0)?,
                observed_tip: row.get::<Option<String>>(1)?,
                fingerprint: row.get::<Option<String>>(2)?,
                failure_kind: row.get::<Option<String>>(3)?,
                outcome_kind: row.get::<Option<String>>(4)?,
                notification_sent: row.get::<i64>(5)? != 0,
            }))
        })
    })
    .await
    .map_err(|error| format!("load reconcile item progress: {error}"))
}

struct ReconcileItemUpdate<'a> {
    intent_id: &'a str,
    bookmark: &'a str,
    workspace_path: &'a Path,
    observed_tip: Option<&'a str>,
    status: &'a str,
    failure_kind: Option<&'a str>,
    outcome_kind: Option<&'a str>,
    fingerprint: Option<&'a str>,
    diagnostic: Option<&'a str>,
}

async fn persist_reconcile_item(db: &LocalDb, item: ReconcileItemUpdate<'_>) -> Result<(), String> {
    db.execute(
        "INSERT INTO jj_reconcile_items
         (intent_id, bookmark, workspace_path, observed_tip, status, failure_kind,
          outcome_kind, suppression_fingerprint, last_diagnostic, attempt_count, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10)
         ON CONFLICT(intent_id, bookmark) DO UPDATE SET
           workspace_path = excluded.workspace_path,
           observed_tip = excluded.observed_tip,
           status = excluded.status,
           failure_kind = excluded.failure_kind,
           outcome_kind = excluded.outcome_kind,
           suppression_fingerprint = excluded.suppression_fingerprint,
           last_diagnostic = excluded.last_diagnostic,
           attempt_count = jj_reconcile_items.attempt_count + 1,
           updated_at = excluded.updated_at",
        params![
            item.intent_id,
            item.bookmark,
            item.workspace_path.to_string_lossy().as_ref(),
            item.observed_tip,
            item.status,
            item.failure_kind,
            item.outcome_kind,
            item.fingerprint,
            item.diagnostic,
            chrono::Utc::now().timestamp()
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("persist reconcile item progress: {error}"))
}

async fn finish_reconcile_intent(
    db: &LocalDb,
    intent_id: &str,
    owner: &str,
    retry_transient: bool,
) -> Result<(), String> {
    let status = if retry_transient {
        "pending"
    } else {
        "completed"
    };
    db.execute(
        "UPDATE jj_reconcile_intents
         SET status = ?3, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?4
         WHERE id = ?1 AND lease_owner = ?2",
        params![intent_id, owner, status, chrono::Utc::now().timestamp()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("complete reconcile intent: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_base_advance(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    label: &str,
    repo_path: &str,
    sibling_base_branch: &str,
    rebase_dest: &str,
    siblings: Vec<SiblingJob>,
    notes: BaseAdvanceNotes,
) -> Result<BranchAdvanceOutcome, String> {
    let specs: Vec<(String, std::path::PathBuf)> = siblings
        .iter()
        .filter_map(|sibling| {
            let branch = sibling_branch(sibling)?;
            Some((branch, std::path::PathBuf::from(&sibling.worktree_path)))
        })
        .collect();
    if specs.is_empty() {
        log::debug!("jj base advance ({label}): no in-flight siblings with a branch to reconcile");
        return Ok(BranchAdvanceOutcome::default());
    }

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    let (pinned_dest, existing_bookmarks) = {
        let guard = orch
            .acquire_jj_store_lock(&store, format!("sibling reconcile preparation ({label})"))
            .await;
        let _phase = guard.phase(format!(
            "bookmark listing and destination resolution candidate_count={} queried_count={}",
            specs.len(),
            specs.len()
        ));
        let pinned_dest = crate::jj::revset_commit(&jj, &store, rebase_dest).ok_or_else(|| {
            format!("jj base advance ({label}): destination `{rebase_dest}` did not resolve")
        })?;
        let candidate_names = specs
            .iter()
            .map(|(branch, _)| branch.clone())
            .collect::<Vec<_>>();
        // Failure deliberately falls back to processing every candidate so a
        // read optimization can never become a liveness gate.
        let bookmarks = crate::jj::query_local_bookmarks(&jj, &store, &candidate_names).ok();
        (pinned_dest, bookmarks)
    };

    let Some(claim) = claim_reconcile_intent(
        db,
        repo_path,
        &store,
        sibling_base_branch,
        &pinned_dest,
        label,
    )
    .await?
    else {
        log::debug!(
            "jj base advance ({label}): coalesced with an existing intent for {pinned_dest}"
        );
        return Ok(BranchAdvanceOutcome::default());
    };
    let eligible = siblings.len();
    let worker_orch = orch.clone();
    let worker_project_id = project_id.to_string();
    let worker_label = label.to_string();
    let worker_repo_path = repo_path.to_string();
    let worker_rebase_dest = rebase_dest.to_string();
    tokio::spawn(async move {
        let worker_db = match owning_db_for_project(&worker_orch.db, &worker_project_id).await {
            Ok(db) => db,
            Err(error) => {
                log::warn!("jj reconcile worker ({worker_label}) could not reopen its owning database: {error}");
                return;
            }
        };
        let retry_claim = ReconcileClaim {
            id: claim.id.clone(),
            owner: claim.owner.clone(),
            project_id: claim.project_id.clone(),
        };
        if let Err(error) = execute_reconcile_claim(
            &worker_orch,
            worker_db.as_ref(),
            &worker_project_id,
            &worker_label,
            &worker_repo_path,
            &worker_rebase_dest,
            siblings,
            notes,
            specs,
            existing_bookmarks,
            pinned_dest,
            claim,
        )
        .await
        {
            log::warn!("jj reconcile worker ({worker_label}) failed: {error}");
            release_reconcile_claim(worker_db.as_ref(), &retry_claim).await;
        }
    });

    Ok(BranchAdvanceOutcome {
        eligible,
        ..BranchAdvanceOutcome::default()
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_reconcile_claim(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    label: &str,
    repo_path: &str,
    rebase_dest: &str,
    siblings: Vec<SiblingJob>,
    notes: BaseAdvanceNotes,
    specs: Vec<(String, std::path::PathBuf)>,
    existing_bookmarks: Option<std::collections::HashSet<String>>,
    pinned_dest: String,
    claim: ReconcileClaim,
) -> Result<BranchAdvanceOutcome, String> {
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    let intent_id = claim.id.as_str();

    // List bookmarks once, then transact only live siblings.
    let (specs, skipped_missing) = retain_present_siblings(specs, existing_bookmarks.as_ref());
    if skipped_missing > 0 {
        log::info!(
                "jj base advance ({label}): skipped {skipped_missing} sibling(s) with missing bookmarks before reconcile"
            );
    }
    if specs.is_empty() {
        finish_reconcile_intent(db, intent_id, &claim.owner, false).await?;
        return Ok(BranchAdvanceOutcome::default());
    }

    let mut ambiguous: Vec<AmbiguousDivergence> = Vec::new();
    let mut pending_quarantines: Vec<PendingReconcileQuarantine> = Vec::new();
    let mut before: HashMap<String, String> = HashMap::new();
    let mut after: HashMap<String, String> = HashMap::new();
    let mut report = crate::jj::ReconcileReport::default();
    let origin_presence = crate::jj::discover_origin_presence(&jj, &store);

    for (branch, workspace_path) in &specs {
        heartbeat_reconcile_intent(db, &claim).await?;
        let progress = reconcile_item_status(db, intent_id, branch).await?;
        if progress.as_ref().is_some_and(|progress| {
            progress.status == "completed"
                || (progress.status == "graph_moved" && progress.notification_sent)
        }) {
            log::debug!("jj reconcile ({label}): resumed past completed {branch}");
            continue;
        }
        let current_tip = {
            let guard = orch
                .acquire_jj_store_lock(
                    &store,
                    format!("sibling reconcile inspect ({label}): {branch}"),
                )
                .await;
            let _phase = guard.phase(format!("bookmark suppression probe branch={branch}"));
            crate::jj::bookmark_commit(&jj, &store, branch)
        };
        let mut quarantine =
            load_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
        if let Some(existing) = quarantine
            .as_ref()
            .filter(|existing| existing.failure_kind != "ambiguous_divergence")
        {
            let current_fingerprint = current_tip.as_deref().unwrap_or("missing");
            if existing.fingerprint == current_fingerprint {
                let diagnostic = format!(
                    "quarantined: {}",
                    existing
                        .last_diagnostic
                        .as_deref()
                        .unwrap_or("permanent reconcile failure")
                );
                persist_reconcile_item(
                    db,
                    ReconcileItemUpdate {
                        intent_id,
                        bookmark: branch,
                        workspace_path,
                        observed_tip: current_tip.as_deref(),
                        status: "suppressed",
                        failure_kind: Some(&existing.failure_kind),
                        outcome_kind: Some("quarantined"),
                        fingerprint: Some(&existing.fingerprint),
                        diagnostic: Some(&diagnostic),
                    },
                )
                .await?;
                log::debug!(
                    "jj reconcile ({label}): skipped quarantined unchanged bookmark {branch}"
                );
                continue;
            }
            release_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
            quarantine = None;
        }
        if let Some(progress) = progress.as_ref() {
            if progress.status == "suppressed" && progress.notification_sent {
                let prefix = format!(
                    "{pinned_dest}:{branch}:{}:",
                    current_tip.as_deref().unwrap_or("missing")
                );
                if progress
                    .fingerprint
                    .as_deref()
                    .is_some_and(|value| value.starts_with(&prefix))
                {
                    log::debug!("jj reconcile ({label}): suppressed unchanged {branch}");
                    continue;
                }
            }
        }

        let mut ambiguous_item = None;
        let mut divergence_resolved = false;
        let mut item_report = if let Some(progress) =
            progress.as_ref().filter(|p| p.status == "graph_moved")
        {
            let mut resumed = crate::jj::ReconcileReport::default();
            match progress.outcome_kind.as_deref() {
                Some("conflicted") => resumed.conflicted.push(branch.clone()),
                Some("rebased_clean") => resumed.rebased_clean.push(branch.clone()),
                Some("preserved_dirty") => resumed.preserved_dirty.push(branch.clone()),
                Some("silent") => resumed.silent.push(branch.clone()),
                Some("failed") => resumed.failed.push(crate::jj::ReconcileFailure {
                    branch: branch.clone(),
                    workspace_path: workspace_path.clone(),
                    error: progress
                        .failure_kind
                        .clone()
                        .unwrap_or_else(|| "resumed reconcile failure".into()),
                }),
                _ => {}
            }
            if let Some(tip) = progress.observed_tip.clone() {
                after.insert(branch.clone(), tip);
            }
            resumed
        } else {
            let guard = orch
                .acquire_jj_store_lock(&store, format!("sibling reconcile ({label}): {branch}"))
                .await;
            let _phase = guard.phase(format!("bookmark transaction branch={branch}"));

            // Revalidate that the named destination still identifies the pinned
            // commit before mutating this bookmark.
            let observed_dest = crate::jj::revset_commit(&jj, &store, rebase_dest);
            if observed_dest.as_deref() != Some(pinned_dest.as_str()) {
                return Err(format!(
                        "jj base advance ({label}): destination moved while intent was running (pinned {pinned_dest}, observed {observed_dest:?})"
                    ));
            }

            match crate::jj::collapse_divergent_bookmark(&jj, &store, branch) {
                Ok(crate::jj::CollapseOutcome::NotDivergent) => {
                    divergence_resolved = true;
                }
                Ok(crate::jj::CollapseOutcome::Collapsed { kept, abandoned }) => {
                    divergence_resolved = true;
                    log::info!(
                        "jj collapse ({label}): sibling {branch} converged to {kept}; abandoned {}",
                        abandoned.join(", ")
                    );
                }
                Ok(crate::jj::CollapseOutcome::Ambiguous { change_id, twins }) => {
                    ambiguous_item = Some(AmbiguousDivergence {
                        branch: branch.clone(),
                        change_id,
                        twins,
                    });
                }
                Err(error) => {
                    log::warn!("jj collapse ({label}): sibling {branch} failed: {error}");
                }
            }

            if ambiguous_item.is_none() {
                if let Some(commit) = crate::jj::bookmark_commit(&jj, &store, branch) {
                    before.insert(branch.clone(), commit);
                }
            }
            let item_report = if ambiguous_item.is_some() {
                crate::jj::ReconcileReport::default()
            } else {
                let item = vec![(branch.clone(), workspace_path.clone())];
                crate::jj::reconcile_siblings_without_publication(&jj, &store, &pinned_dest, &item)
                    .map_err(|error| format!("jj sibling reconcile ({label}) failed: {error}"))?
            };
            if ambiguous_item.is_none() {
                if let Some(commit) = crate::jj::bookmark_commit(&jj, &store, branch) {
                    after.insert(branch.clone(), commit);
                }
            }
            item_report
        };

        if divergence_resolved
            && quarantine
                .as_ref()
                .is_some_and(|existing| existing.failure_kind == "ambiguous_divergence")
        {
            release_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
            quarantine = None;
        }

        if let Some(item) = ambiguous_item {
            let fingerprint = divergence_fingerprint(&item.twins);
            let already_quarantined = quarantine.as_ref().is_some_and(|existing| {
                existing.failure_kind == "ambiguous_divergence"
                    && existing.fingerprint == fingerprint
            });
            let diagnostic = "bookmark divergence has no unique canonical tip";
            persist_reconcile_item(
                db,
                ReconcileItemUpdate {
                    intent_id,
                    bookmark: branch,
                    workspace_path,
                    observed_tip: current_tip.as_deref(),
                    status: "suppressed",
                    failure_kind: Some("ambiguous_divergence"),
                    outcome_kind: Some("ambiguous"),
                    fingerprint: Some(&fingerprint),
                    diagnostic: Some(diagnostic),
                },
            )
            .await?;
            if !already_quarantined {
                pending_quarantines.push(PendingReconcileQuarantine {
                    bookmark: branch.clone(),
                    failure_kind: "ambiguous_divergence".to_string(),
                    fingerprint,
                    diagnostic: Some(diagnostic.to_string()),
                });
                ambiguous.push(item);
            }
            continue;
        }

        // Origin transfer and durable lineage persistence are deliberately
        // outside the jj mutex.
        let publish = (item_report.rebased_clean.contains(branch)
            || item_report.preserved_dirty.contains(branch))
            && !item_report.silent.contains(branch);
        if publish {
            if let Err(error) =
                crate::jj::publish_reconciled_bookmark(&jj, &store, branch, origin_presence)
            {
                item_report
                    .rebased_clean
                    .retain(|candidate| candidate != branch);
                item_report
                    .preserved_dirty
                    .retain(|candidate| candidate != branch);
                item_report.failed.push(crate::jj::ReconcileFailure {
                    branch: branch.clone(),
                    workspace_path: workspace_path.clone(),
                    error: format!("origin push failed: {error}"),
                });
            }
        }
        let touched = item_report.rebased_clean.contains(branch)
            || item_report.conflicted.contains(branch)
            || item_report.preserved_dirty.contains(branch);
        if touched {
            if let Some(sibling) = siblings
                .iter()
                .find(|candidate| sibling_branch(candidate).as_deref() == Some(branch.as_str()))
            {
                if let Err(error) =
                    advance_sibling_durable_base(db, &jj, &store, sibling, &pinned_dest).await
                {
                    item_report
                        .rebased_clean
                        .retain(|candidate| candidate != branch);
                    item_report
                        .conflicted
                        .retain(|candidate| candidate != branch);
                    item_report
                        .preserved_dirty
                        .retain(|candidate| candidate != branch);
                    item_report.silent.retain(|candidate| candidate != branch);
                    item_report.failed.push(crate::jj::ReconcileFailure {
                        branch: branch.clone(),
                        workspace_path: workspace_path.clone(),
                        error: format!("durable base advancement failed: {error}"),
                    });
                }
            }
        }

        let item_diagnostic = item_report
            .failed
            .iter()
            .find(|failure| failure.branch == *branch)
            .map(|failure| failure.error.as_str());
        let failure_kind = item_diagnostic.map(crate::jj::reconcile_failure_kind);
        let permanent = failure_kind.is_some_and(crate::jj::reconcile_failure_is_permanent);
        let quarantine_fingerprint = after
            .get(branch)
            .or_else(|| before.get(branch))
            .map_or("missing", String::as_str);
        if permanent {
            pending_quarantines.push(PendingReconcileQuarantine {
                bookmark: branch.clone(),
                failure_kind: failure_kind.unwrap_or("unknown").to_string(),
                fingerprint: quarantine_fingerprint.to_string(),
                diagnostic: item_diagnostic.map(str::to_string),
            });
        }
        let suppression_fingerprint = permanent.then(|| {
            format!(
                "{}:{}:{}:{}",
                pinned_dest,
                branch,
                after
                    .get(branch)
                    .or_else(|| before.get(branch))
                    .map_or("missing", String::as_str),
                failure_kind.unwrap_or("unknown")
            )
        });
        let outcome_kind = if item_diagnostic.is_some() {
            "failed"
        } else if item_report.conflicted.contains(branch) {
            "conflicted"
        } else if item_report.rebased_clean.contains(branch) {
            "rebased_clean"
        } else if item_report.preserved_dirty.contains(branch) {
            "preserved_dirty"
        } else if item_report.silent.contains(branch) {
            "silent"
        } else {
            "unchanged"
        };
        persist_reconcile_item(
            db,
            ReconcileItemUpdate {
                intent_id,
                bookmark: branch,
                workspace_path,
                observed_tip: after
                    .get(branch)
                    .or_else(|| before.get(branch))
                    .map(String::as_str),
                status: if permanent {
                    "suppressed"
                } else if item_diagnostic.is_some() {
                    "pending"
                } else {
                    "graph_moved"
                },
                failure_kind,
                outcome_kind: Some(outcome_kind),
                fingerprint: suppression_fingerprint.as_deref(),
                diagnostic: item_diagnostic,
            },
        )
        .await?;

        report.rebased_clean.append(&mut item_report.rebased_clean);
        report.conflicted.append(&mut item_report.conflicted);
        report
            .preserved_dirty
            .append(&mut item_report.preserved_dirty);
        report.silent.append(&mut item_report.silent);
        report.held.append(&mut item_report.held);
        report.failed.append(&mut item_report.failed);
        heartbeat_reconcile_intent(db, &claim).await?;
        tokio::task::yield_now().await;
    }

    let ambiguous_notified = if ambiguous.is_empty() {
        Vec::new()
    } else {
        notify_ambiguous_divergence(orch, db, &siblings, &ambiguous, intent_id)?
    };

    log::info!(
        "jj reconcile ({label}): {} rebased clean, {} recorded a conflict, {} failed",
        report.rebased_clean.len(),
        report.conflicted.len(),
        report.failed.len()
    );

    let failed_notified = if report.failed.is_empty() {
        Vec::new()
    } else {
        notify_failed_siblings(orch, db, &siblings, &report.failed, label, intent_id)?
    };
    let notified: Vec<String> = ambiguous_notified
        .into_iter()
        .chain(failed_notified)
        .collect();
    activate_notified_quarantines(
        db,
        &claim.project_id,
        &store,
        &pending_quarantines,
        &notified,
    )
    .await?;

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
            intent_id,
        )?;
    }

    // Cleanly-rebased siblings: nothing to resolve, but their branch moved — a
    // passive (non-waking) note rides along into their next natural run.
    let mut clean_rewritten = siblings_rewritten(&report.rebased_clean, &before, &after);
    clean_rewritten.retain(|branch| !report.silent.contains(branch));
    if clean_rewritten.is_empty() {
        log::debug!("jj reconcile ({label}): clean rebases unchanged since a prior reconcile; no redundant note");
    } else {
        notify_clean_siblings(
            orch,
            db,
            &siblings,
            &clean_rewritten,
            &notes.clean,
            intent_id,
        )?;
    }

    // Delivery is a separate durable step from graph movement. A restart before
    // this write resumes graph_moved items without replaying their jj mutation.
    mark_reconcile_delivered(db, intent_id).await?;

    let retry_transient = reconcile_has_transient_failures(&report.failed);
    // Fan a terminal-checkout refresh out to every sibling this reconcile actually
    // rewrote. A running terminal on a rebased job branch must follow its workspace
    // to the new tip (conflicted or clean alike) or it keeps serving pre-rebase
    // source. This is the sibling analogue of the advanced-branch fan-out
    // `reconcile_managed_branch_advance` performs, and it reaches every caller of
    // this shared body — including the external and startup default-advance paths
    // that previously skipped it. The store lock is released by now.
    let mut terminal_failed = 0;
    for branch in conflicted_rewritten.iter().chain(clean_rewritten.iter()) {
        if let Some(new_tip) = after.get(branch) {
            terminal_failed +=
                refresh_terminal_leases_for_branch(orch, db, project_id, branch, new_tip).await;
        }
    }

    finish_reconcile_intent(db, intent_id, &claim.owner, retry_transient).await?;
    Ok(BranchAdvanceOutcome {
        eligible: siblings.len(),
        rebased_clean: clean_rewritten.len(),
        conflicted: conflicted_rewritten.len(),
        failed: report.failed.len() + terminal_failed,
    })
}

/// Filter a set of reconciled sibling branches down to those this reconcile
/// actually rewrote: a branch whose commit id changed between the before/after
/// snapshots. A double-fire reconcile at the same dest tip is a `jj rebase` no-op,
/// so the commit id is unchanged → the branch is filtered out and not re-notified
/// (conflicted or clean). When either snapshot is missing (an unexpected resolve
/// failure), notify conservatively rather than silently dropping a real change.
async fn advance_sibling_durable_base(
    db: &LocalDb,
    jj: &crate::jj::JjEnv,
    store: &Path,
    sibling: &SiblingJob,
    new_base: &str,
) -> Result<(), String> {
    let mut recorded_base = sibling.base_commit.clone().ok_or_else(|| {
        format!(
            "job {} has no recorded base_commit; cannot advance to {new_base}",
            sibling.id
        )
    })?;
    let worktree = Path::new(&sibling.worktree_path);
    let mut marker = crate::jj::read_workspace_identity(worktree).ok_or_else(|| {
        format!(
            "workspace {} has no .jj/cairn-workspace-identity marker (recorded base {recorded_base}, new base {new_base})",
            sibling.worktree_path
        )
    })?;
    if marker.project_id.is_empty() || marker.worktree_path != worktree {
        return Err(format!(
            "workspace identity coordinate mismatch for job {}: marker owner={}, path={}; expected path={}; refused base {new_base}",
            sibling.id,
            marker.owner_job_id,
            marker.worktree_path.display(),
            sibling.worktree_path,
        ));
    }

    // A pending marker is authoritative. Complete it before considering a
    // finalized mismatch, including a normalization interrupted before the later
    // transition to this invocation's target.
    if let Some(pending) = marker.pending_base_transition.clone() {
        if marker.base_commit != pending.old_base && marker.base_commit != pending.new_base {
            return Err(format!(
                "pending base transition {} -> {} disagrees with marker base {}",
                pending.old_base, pending.new_base, marker.base_commit
            ));
        }
        if recorded_base != pending.old_base && recorded_base != pending.new_base {
            return Err(format!(
                "database base {recorded_base} is neither endpoint of pending base transition {} -> {}",
                pending.old_base, pending.new_base
            ));
        }
        crate::execution::jobs::workspace_identity::apply_base_transition(
            db,
            worktree,
            &mut marker,
            &pending.old_base,
            &pending.new_base,
        )
        .await?;
        recorded_base = pending.new_base;
    }

    if marker.base_commit != recorded_base {
        let lineage = crate::jj::classify_durable_base_lineage(
            jj,
            store,
            &marker.base_commit,
            &recorded_base,
            new_base,
        );
        if !lineage.repairable() {
            return Err(durable_base_mismatch_diagnostic(
                sibling,
                &marker.owner_job_id,
                &marker.base_commit,
                &recorded_base,
                new_base,
                &lineage,
            ));
        }
        let chosen = lineage.newer_base.clone().ok_or_else(|| {
            durable_base_mismatch_diagnostic(
                sibling,
                &marker.owner_job_id,
                &marker.base_commit,
                &recorded_base,
                new_base,
                &lineage,
            )
        })?;
        log::warn!(
            "self-healing durable base mismatch: workspace={}, owner_job={}, marker_base={}, database_base={}, resolved_marker={}, resolved_database={}, chosen_base={}, target={}, relationship={}",
            sibling.worktree_path,
            sibling.id,
            marker.base_commit,
            recorded_base,
            lineage.marker_resolved().unwrap_or("unresolved"),
            lineage.database_resolved().unwrap_or("unresolved"),
            chosen,
            new_base,
            lineage.relationship.label(),
        );

        if marker.base_commit != chosen {
            marker.pending_base_transition = Some(crate::jj::WorkspaceBaseTransition {
                old_base: marker.base_commit.clone(),
                new_base: chosen.clone(),
            });
            crate::jj::write_workspace_identity(worktree, &marker)?;
            marker.base_commit = chosen.clone();
            marker.pending_base_transition = None;
            crate::jj::write_workspace_identity(worktree, &marker)?;
        }
        if recorded_base != chosen {
            crate::execution::jobs::workspace_identity::apply_base_transition(
                db,
                worktree,
                &mut marker,
                &recorded_base,
                &chosen,
            )
            .await?;
        }
        recorded_base = chosen;
    }

    if recorded_base == new_base && marker.base_commit == new_base {
        return Ok(());
    }
    crate::execution::jobs::workspace_identity::apply_base_transition(
        db,
        worktree,
        &mut marker,
        &recorded_base,
        new_base,
    )
    .await
}

fn durable_base_mismatch_diagnostic(
    sibling: &SiblingJob,
    owner_job_id: &str,
    marker_base: &str,
    database_base: &str,
    target: &str,
    lineage: &crate::jj::DurableBaseLineage,
) -> String {
    let resolution = |resolved: Option<&str>, on_target: bool| match resolved {
        Some(commit) => format!("resolved={commit}, ancestor_or_equal_to_target={on_target}"),
        None => "resolved=false, ancestor_or_equal_to_target=false".to_string(),
    };
    format!(
        "durable base lineage mismatch for managed workspace {} (owner job {}): marker={marker_base} [{}]; database={database_base} [{}]; target={target}; relationship={}. Inspect these commits and confirm the workspace assignment, then run `cairn:~/workspace-recovery action=rebind` for this workspace. Do not force-push or use a destructive reset; all workspace files were preserved.",
        sibling.worktree_path,
        owner_job_id,
        resolution(lineage.marker_resolved(), lineage.marker_on_target),
        resolution(lineage.database_resolved(), lineage.database_on_target),
        lineage.relationship.label(),
    )
}

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
        Ok(crate::jj::CollapseOutcome::NotDivergent) => {
            if let Err(error) = release_reconcile_quarantine(db, project_id, &store, branch).await {
                log::warn!("on-branch advance: failed to release quarantine for {branch}: {error}");
            }
        }
        Ok(crate::jj::CollapseOutcome::Collapsed { kept, abandoned }) => {
            if let Err(error) = release_reconcile_quarantine(db, project_id, &store, branch).await {
                log::warn!("on-branch advance: failed to release quarantine for {branch}: {error}");
            }
            log::info!(
                "jj collapse (on-branch {branch}): converged to {kept}; abandoned {}",
                abandoned.join(", ")
            );
        }
        Ok(crate::jj::CollapseOutcome::Ambiguous { change_id, twins }) => {
            let fingerprint = divergence_fingerprint(&twins);
            let already_quarantined =
                match load_reconcile_quarantine(db, project_id, &store, branch).await {
                    Ok(Some(existing)) => {
                        existing.failure_kind == "ambiguous_divergence"
                            && existing.fingerprint == fingerprint
                    }
                    Ok(None) => false,
                    Err(error) => {
                        log::warn!(
                            "on-branch advance: failed to inspect quarantine for {branch}: {error}"
                        );
                        false
                    }
                };
            if already_quarantined {
                log::debug!(
                    "jj collapse (on-branch {branch}): unchanged ambiguous divergence remains quarantined"
                );
                return;
            }
            log::warn!(
                "jj collapse (on-branch {branch}): divergent change {change_id} is ambiguous (twins {}); interrupting the on-branch workspace and skipping the advance",
                twins.join(", ")
            );
            let mut all_notified = true;
            for workspace in &on_branch {
                let Some(run_id) = latest_run_for_job(db, &workspace.id) else {
                    all_notified = false;
                    continue;
                };
                let message = build_ambiguous_divergence_note(branch, &change_id, &twins);
                let key =
                    on_branch_ambiguous_delivery_key(project_id, branch, &fingerprint, &run_id);
                match queue_system_direct_once_confirmed(
                    orch,
                    &run_id,
                    &message,
                    DeliveryUrgency::Interrupt,
                    &key,
                ) {
                    Ok(DirectQueueDisposition::QueuedOrPresent) => {}
                    Ok(DirectQueueDisposition::Undeliverable) => all_notified = false,
                    Err(error) => {
                        all_notified = false;
                        log::warn!(
                            "on-branch advance: failed to interrupt {} for ambiguous divergence: {error}",
                            workspace.id
                        );
                    }
                }
            }
            if all_notified {
                if let Err(error) = upsert_reconcile_quarantine(
                    db,
                    project_id,
                    &store,
                    branch,
                    "ambiguous_divergence",
                    &fingerprint,
                    Some("bookmark divergence has no unique canonical tip"),
                )
                .await
                {
                    log::warn!("on-branch advance: failed to quarantine {branch}: {error}");
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
fn notify_failed_siblings(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    failed: &[crate::jj::ReconcileFailure],
    label: &str,
    delivery_scope: &str,
) -> Result<Vec<String>, String> {
    notify_failed_siblings_with(
        orch,
        db,
        siblings,
        failed,
        label,
        delivery_scope,
        queue_system_direct_once_confirmed,
    )
}

fn notify_failed_siblings_with<F>(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    failed: &[crate::jj::ReconcileFailure],
    label: &str,
    delivery_scope: &str,
    enqueue: F,
) -> Result<Vec<String>, String>
where
    F: Fn(
        &Orchestrator,
        &str,
        &str,
        DeliveryUrgency,
        &str,
    ) -> Result<DirectQueueDisposition, String>,
{
    let mut notified = Vec::new();
    for failure in failed {
        let Some(sibling) = siblings
            .iter()
            .find(|sibling| sibling_branch(sibling).as_deref() == Some(failure.branch.as_str()))
        else {
            continue;
        };
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj reconcile: no run for failed sibling {} to steer",
                sibling.id
            );
            continue;
        };
        let failure_kind = crate::jj::reconcile_failure_kind(&failure.error);
        let quarantine_note = if crate::jj::reconcile_failure_is_permanent(failure_kind) {
            let guidance = match failure_kind {
                "immutable_commit" => "The bookmark points at an immutable (typically already-merged) commit. If this work already landed, close the PR/issue so the workspace retires; otherwise move the bookmark onto a mutable head.",
                "conflicted_bookmark" => "The bookmark name itself is conflicted in jj; resolve it with `jj bookmark` in the workspace.",
                "missing_bookmark" => "Re-create or move the missing bookmark onto the workspace's intended mutable head.",
                _ => "Repair the bookmark state described by the diagnostic.",
            };
            format!(
                "\nThis branch is now quarantined from base-advance reconciliation. Future advances will skip it silently until the branch changes. {guidance}"
            )
        } else {
            String::new()
        };
        let note = format!(
            "⛔ BLOCKING [Base branch update] Cairn failed to reconcile the agent's managed jj workspace after the base advanced ({label}).\nManaged workspace: `{}`\nExact reconciliation diagnostic:\n{}\nYour work was preserved. Follow the diagnostic's named recovery action only after confirming the workspace assignment; do not force-push or use a destructive reset.{quarantine_note}",
            failure.workspace_path.display(),
            failure.error
        );
        let key = format!("{delivery_scope}:{}:failed", failure.branch);
        if enqueue(orch, &run_id, &note, DeliveryUrgency::Steer, &key)?
            == DirectQueueDisposition::Undeliverable
        {
            continue;
        }
        notified.push(failure.branch.clone());
        log::info!(
            "Steered jj sibling job {} after automatic reconcile failed",
            sibling.id
        );
    }
    Ok(notified)
}

fn notify_conflicted_siblings(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    conflicted: &[String],
    note: &str,
    files_by_branch: &HashMap<String, Vec<String>>,
    delivery_scope: &str,
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
        let key = format!("{delivery_scope}:{branch}:conflicted");
        queue_system_direct_once(orch, &run_id, &message, DeliveryUrgency::Steer, &key)?;
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
    delivery_scope: &str,
) -> Result<Vec<String>, String> {
    let mut notified = Vec::new();
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
        let key = format!("{delivery_scope}:{}:ambiguous", divergence.branch);
        if queue_system_direct_once_confirmed(
            orch,
            &run_id,
            &message,
            DeliveryUrgency::Interrupt,
            &key,
        )? == DirectQueueDisposition::Undeliverable
        {
            continue;
        }
        notified.push(divergence.branch.clone());
        log::info!(
            "Interrupted jj sibling job {} for an ambiguous divergent change on {}",
            sibling.id,
            divergence.branch
        );
    }
    Ok(notified)
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
    delivery_scope: &str,
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
        let key = format!("{delivery_scope}:{branch}:clean");
        queue_system_direct_once(orch, &run_id, note, DeliveryUrgency::Passive, &key)?;
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
                    "SELECT j.id, j.worktree_path, j.branch, j.base_commit
                         FROM jobs j
                         WHERE j.project_id = ?1
                           AND j.base_branch = ?2
                           AND j.id != ?3
                           AND j.worktree_path IS NOT NULL
                           AND NOT EXISTS (
                             SELECT 1 FROM issues i
                             WHERE i.id = j.issue_id AND i.status IN ('merged', 'closed')
                           )
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
                    base_commit: row.opt_text(3)?,
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
                    "SELECT j.id, j.worktree_path, j.branch, j.base_commit
                         FROM jobs j
                         WHERE j.project_id = ?1
                           AND j.branch = ?2
                           AND j.worktree_path IS NOT NULL
                           AND NOT EXISTS (
                             SELECT 1 FROM issues i
                             WHERE i.id = j.issue_id AND i.status IN ('merged', 'closed')
                           )
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
                    base_commit: row.opt_text(3)?,
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
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn run_test_git(repo: &Path, args: &[&str]) -> bool {
        crate::env::git()
            .arg("-C")
            .arg(repo)
            .args(args)
            .status()
            .is_ok_and(|status| status.success())
    }

    fn jj_test_env() -> Option<(TempDir, TempDir, TempDir, crate::jj::JjEnv, PathBuf)> {
        let bin = std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                Command::new("which")
                    .arg("jj")
                    .output()
                    .ok()
                    .filter(|output| output.status.success())
                    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            })?;
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        let workspaces = TempDir::new().unwrap();
        for args in [
            vec!["init", "-q", "-b", "main"],
            vec!["config", "user.email", "base-advance@cairn.test"],
            vec!["config", "user.name", "Base Advance Test"],
        ] {
            assert!(run_test_git(project.path(), &args));
        }
        std::fs::write(project.path().join("base.txt"), "base\n").unwrap();
        assert!(run_test_git(project.path(), &["add", "."]));
        assert!(run_test_git(
            project.path(),
            &["commit", "-q", "-m", "base"]
        ));
        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("store");
        crate::jj::ensure_project_store(&jj, &store, project.path()).unwrap();
        Some((home, project, workspaces, jj, store))
    }

    #[test]
    #[serial_test::serial(jj)]
    fn managed_workspace_reconcile_fresh_workspace_is_unchanged() {
        let Some((_home, _project, workspaces, jj, store)) = jj_test_env() else {
            return;
        };
        let branch = "agent/PROJ-2817-builder-0";
        let workspace = workspaces.path().join("fresh");
        crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
        let outcome =
            crate::jj::reconcile_managed_workspace(&jj, &store, &workspace, branch, None).unwrap();
        assert_eq!(
            outcome,
            crate::jj::ManagedWorkspaceReconcileOutcome::Unchanged
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn managed_workspace_reconcile_preserves_dirty_local_work() {
        let Some((_home, _project, workspaces, jj, store)) = jj_test_env() else {
            return;
        };
        let branch = "agent/PROJ-2817-builder-1";
        let workspace = workspaces.path().join("dirty");
        crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
        std::fs::write(workspace.join("wip.txt"), "local work\n").unwrap();
        let outcome =
            crate::jj::reconcile_managed_workspace(&jj, &store, &workspace, branch, None).unwrap();
        assert_eq!(
            outcome,
            crate::jj::ManagedWorkspaceReconcileOutcome::PreservedDirty
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join("wip.txt")).unwrap(),
            "local work\n"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn managed_workspace_reconcile_advances_clean_behind_workspace() {
        let Some((_home, _project, workspaces, jj, store)) = jj_test_env() else {
            return;
        };
        let branch = "agent/PROJ-2817-builder-2";
        let workspace = workspaces.path().join("behind");
        crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
        jj.run(&store, &["new", branch], "advance test branch")
            .unwrap();
        std::fs::write(store.join("advanced.txt"), "advanced\n").unwrap();
        jj.run(&store, &["describe", "-m", "advance"], "describe advance")
            .unwrap();
        jj.run(
            &store,
            &[
                "bookmark",
                "set",
                branch,
                "-r",
                "@",
                "--ignore-working-copy",
            ],
            "advance bookmark",
        )
        .unwrap();
        let tip = crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
        let outcome =
            crate::jj::reconcile_managed_workspace(&jj, &store, &workspace, branch, Some(&tip))
                .unwrap();
        assert_eq!(
            outcome,
            crate::jj::ManagedWorkspaceReconcileOutcome::AdvancedClean
        );
        assert!(workspace.join("advanced.txt").exists());
    }

    #[tokio::test]
    async fn reconcile_intents_coalesce_and_stale_claims_resume() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");

        let claim = claim_reconcile_intent(&db, "/repo", store, "main", "dest-a", "local_merge")
            .await
            .unwrap()
            .expect("first trigger claims the pinned intent");
        assert!(
            claim_reconcile_intent(&db, "/repo", store, "main", "dest-a", "webhook")
                .await
                .unwrap()
                .is_none(),
            "duplicate delivery coalesces while the worker owns the intent"
        );

        db.execute(
            "UPDATE jj_reconcile_intents SET lease_expires_at = 0 WHERE id = ?1",
            (claim.id.as_str(),),
        )
        .await
        .unwrap();
        let durable_work = claim_next_reconcile_intent(&db)
            .await
            .unwrap()
            .expect("the runner worker reclaims an expired lease without a duplicate trigger");
        let resumed = durable_work.claim;
        assert_eq!(resumed.id, claim.id);
        assert_ne!(resumed.owner, claim.owner);
        assert_eq!(durable_work.target_branch, "main");
        assert_eq!(durable_work.destination_commit, "dest-a");

        persist_reconcile_item(
            &db,
            ReconcileItemUpdate {
                intent_id: &resumed.id,
                bookmark: "agent/test",
                workspace_path: Path::new("/worktree"),
                observed_tip: Some("tip"),
                status: "graph_moved",
                failure_kind: None,
                outcome_kind: Some("unchanged"),
                fingerprint: None,
                diagnostic: None,
            },
        )
        .await
        .unwrap();
        let moved = reconcile_item_status(&db, &resumed.id, "agent/test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.status, "graph_moved");
        assert!(!moved.notification_sent);

        // A stale owner cannot complete a lease reclaimed by another worker.
        finish_reconcile_intent(&db, &claim.id, &claim.owner, false)
            .await
            .unwrap();
        assert_eq!(
            db.query_text(
                "SELECT status FROM jj_reconcile_intents WHERE id = ?1",
                (resumed.id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("running")
        );

        mark_reconcile_delivered(&db, &resumed.id).await.unwrap();
        let delivered = reconcile_item_status(&db, &resumed.id, "agent/test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.status, "completed");
        assert!(delivered.notification_sent);

        finish_reconcile_intent(&db, &resumed.id, &resumed.owner, false)
            .await
            .unwrap();
        assert!(
            claim_reconcile_intent(&db, "/repo", store, "main", "dest-a", "webhook")
                .await
                .unwrap()
                .is_none(),
            "completed duplicate delivery remains acknowledged"
        );
        assert!(
            claim_reconcile_intent(&db, "/repo", store, "main", "dest-b", "webhook")
                .await
                .unwrap()
                .is_some(),
            "a new pinned destination creates new work"
        );
    }

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

    #[tokio::test]
    async fn reconcile_quarantine_upsert_load_and_release() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");

        upsert_reconcile_quarantine(
            &db,
            "proj-1",
            store,
            "agent/test",
            "immutable_commit",
            "tip-a",
            Some("immutable commit"),
        )
        .await
        .unwrap();
        let first = load_reconcile_quarantine(&db, "proj-1", store, "agent/test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.failure_kind, "immutable_commit");
        assert_eq!(first.fingerprint, "tip-a");

        upsert_reconcile_quarantine(
            &db,
            "proj-1",
            store,
            "agent/test",
            "conflicted_bookmark",
            "tip-b",
            Some("name is conflicted"),
        )
        .await
        .unwrap();
        assert_eq!(db.query_opt_i64(
            "SELECT strike_count FROM jj_reconcile_quarantines WHERE project_id = 'proj-1' AND store_path = '/store' AND bookmark = 'agent/test'",
            (),
        ).await.unwrap(), Some(2));
        release_reconcile_quarantine(&db, "proj-1", store, "agent/test")
            .await
            .unwrap();
        assert!(
            load_reconcile_quarantine(&db, "proj-1", store, "agent/test")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn failed_notification_does_not_activate_quarantine_and_retry_can_activate_once() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let pending = vec![PendingReconcileQuarantine {
            bookmark: "agent/test".to_string(),
            failure_kind: "immutable_commit".to_string(),
            fingerprint: "tip-a".to_string(),
            diagnostic: Some("immutable commit".to_string()),
        }];
        let orch = test_orchestrator(db, MockGitClient::new());
        let siblings = vec![SiblingJob {
            id: "job-overlap".to_string(),
            worktree_path: "/wt/overlap".to_string(),
            branch: Some("agent/test".to_string()),
            base_commit: None,
        }];
        let failures = vec![crate::jj::ReconcileFailure {
            branch: "agent/test".to_string(),
            workspace_path: "/wt/overlap".into(),
            error: "commit is immutable".to_string(),
        }];

        let first_notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &failures,
            "test",
            "intent-1",
            |_, _, _, _, _| Ok(DirectQueueDisposition::Undeliverable),
        )
        .unwrap();
        activate_notified_quarantines(&orch.db.local, "proj-1", store, &pending, &first_notified)
            .await
            .unwrap();
        assert!(
            load_reconcile_quarantine(&orch.db.local, "proj-1", store, "agent/test")
                .await
                .unwrap()
                .is_none(),
            "an undeliverable notification must leave the branch eligible for retry"
        );

        let retried_notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &failures,
            "test",
            "intent-2",
            |_, _, _, _, _| Ok(DirectQueueDisposition::QueuedOrPresent),
        )
        .unwrap();
        activate_notified_quarantines(&orch.db.local, "proj-1", store, &pending, &retried_notified)
            .await
            .unwrap();
        assert_eq!(
            orch.db
                .local
                .query_opt_i64(
                    "SELECT strike_count FROM jj_reconcile_quarantines
                 WHERE project_id = 'proj-1' AND store_path = '/store'
                   AND bookmark = 'agent/test'",
                    (),
                )
                .await
                .unwrap(),
            Some(1),
            "the successful retry activates quarantine exactly once"
        );
    }

    #[test]
    fn divergence_quarantine_fingerprint_is_order_independent() {
        assert_eq!(
            divergence_fingerprint(&["bbb".to_string(), "aaa".to_string()]),
            "aaa+bbb"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_branch_ambiguous_delivery_is_idempotent_per_recipient() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let branch = "agent/PROJ-integration";
        let fingerprint = "aaa+bbb";
        let message = "ambiguous divergence";
        let recipients = ["run-job-overlap", "run-job-clean"];

        for run_id in recipients {
            let key = on_branch_ambiguous_delivery_key("proj-1", branch, fingerprint, run_id);
            assert_eq!(
                queue_system_direct_once_confirmed(
                    &orch,
                    run_id,
                    message,
                    DeliveryUrgency::Interrupt,
                    &key,
                )
                .unwrap(),
                DirectQueueDisposition::QueuedOrPresent
            );
        }
        let count_messages = || async {
            orch.db
                .local
                .query_opt_i64(
                    "SELECT COUNT(*) FROM messages
                     WHERE recipient_run_id IN ('run-job-overlap', 'run-job-clean')
                       AND content = 'ambiguous divergence'",
                    (),
                )
                .await
                .unwrap()
        };
        assert_eq!(
            count_messages().await,
            Some(2),
            "each on-branch workspace receives its own direct"
        );

        for run_id in recipients {
            let key = on_branch_ambiguous_delivery_key("proj-1", branch, fingerprint, run_id);
            queue_system_direct_once_confirmed(
                &orch,
                run_id,
                message,
                DeliveryUrgency::Interrupt,
                &key,
            )
            .unwrap();
        }
        assert_eq!(
            count_messages().await,
            Some(2),
            "retrying the same twin fingerprint does not duplicate either direct"
        );
    }

    #[test]
    fn only_transient_failures_keep_an_intent_pending() {
        let failure = |error: &str| crate::jj::ReconcileFailure {
            branch: "agent/test".to_string(),
            workspace_path: PathBuf::from("/worktree"),
            error: error.to_string(),
        };
        assert!(!reconcile_has_transient_failures(&[failure(
            "commit is immutable"
        )]));
        assert!(!reconcile_has_transient_failures(&[failure(
            "bookmark name is conflicted"
        )]));
        assert!(reconcile_has_transient_failures(&[failure(
            "process exited unexpectedly"
        )]));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn merged_issue_jobs_are_excluded_but_null_issue_jobs_remain() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "UPDATE issues SET status = 'merged' WHERE id = 'issue-2';
             INSERT INTO jobs
               (id, execution_id, recipe_node_id, issue_id, project_id, status,
                worktree_path, branch, base_branch, created_at, updated_at)
             VALUES
               ('job-no-issue', NULL, 'node', NULL, 'proj-1', 'running',
                '/wt/no-issue', 'agent/PROJ-null-builder-0', 'integration', 1, 1);",
        )
        .await
        .unwrap();

        let siblings = load_sibling_jobs(&db, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        let ids: std::collections::HashSet<&str> =
            siblings.iter().map(|job| job.id.as_str()).collect();
        assert!(!ids.contains("job-overlap"));
        assert!(ids.contains("job-no-issue"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_lease_enumeration_includes_only_running_terminals() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "UPDATE jobs SET branch = 'agent/PROJ-2-builder-0' WHERE id = 'job-overlap';
             INSERT INTO job_terminals
                 (id, job_id, session_id, command, status, created_at, slug,
                  lease_id, lease_incarnation_id, lease_epoch)
             VALUES
                 ('live-terminal', 'job-overlap', 'live-session', 'true', 'running', 1, 'live', 'live-lease', 'live-inc', 7),
                 ('exited-terminal', 'job-overlap', 'exited-session', 'true', 'exited', 1, 'exited', 'exited-lease', 'exited-inc', 8);",
        )
        .await
        .unwrap();

        let leases = load_live_terminal_leases(&db, "proj-1", "agent/PROJ-2-builder-0")
            .await
            .unwrap();
        assert_eq!(
            leases,
            vec![(
                "live-lease".to_string(),
                "live-inc".to_string(),
                7,
                "job-overlap".to_string(),
            )]
        );
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

    fn advance_test_commit(
        jj: &crate::jj::JjEnv,
        store: &Path,
        parent: &str,
        name: &str,
    ) -> String {
        jj.run(store, &["new", parent], "create durable-base test commit")
            .unwrap();
        std::fs::write(store.join(format!("{name}.txt")), format!("{name}\n")).unwrap();
        jj.run(
            store,
            &["describe", "-m", name],
            "describe durable-base test commit",
        )
        .unwrap();
        crate::jj::revset_commit(jj, store, "@").unwrap()
    }

    async fn durable_base_fixture(
        db: &LocalDb,
        workspace: &Path,
        database_base: &str,
        marker_base: &str,
    ) -> SiblingJob {
        let workspace_path = workspace.to_string_lossy().to_string();
        db.execute(
            "UPDATE jobs SET worktree_path = ?1, base_commit = ?2 WHERE id = 'job-overlap'",
            params![workspace_path.as_str(), database_base],
        )
        .await
        .unwrap();
        std::fs::create_dir_all(workspace.join(".jj")).unwrap();
        let identity = crate::jj::WorkspaceIdentity::new(
            "job-overlap",
            "job-overlap",
            "proj-1",
            PathBuf::from("/repo"),
            workspace.to_path_buf(),
            "branch-overlap",
            "branch-overlap",
            marker_base,
        );
        crate::jj::write_workspace_identity(workspace, &identity).unwrap();
        SiblingJob {
            id: "job-overlap".to_string(),
            worktree_path: workspace_path,
            branch: Some("branch-overlap".to_string()),
            base_commit: Some(database_base.to_string()),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(jj)]
    async fn two_consecutive_durable_advances_converge_without_pending_state() {
        let Some((_home, _project, workspaces, jj, store)) = jj_test_env() else {
            return;
        };
        let a = crate::jj::revset_commit(&jj, &store, "main").unwrap();
        let b = advance_test_commit(&jj, &store, &a, "consecutive-b");
        let c = advance_test_commit(&jj, &store, &b, "consecutive-c");
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let workspace = workspaces.path().join("consecutive");
        let sibling_a = durable_base_fixture(&db, &workspace, &a, &a).await;

        advance_sibling_durable_base(&db, &jj, &store, &sibling_a, &b)
            .await
            .unwrap();
        let sibling_b = SiblingJob {
            base_commit: Some(b.clone()),
            ..sibling_a
        };
        advance_sibling_durable_base(&db, &jj, &store, &sibling_b, &c)
            .await
            .unwrap();

        let marker = loop {
            let marker = crate::jj::read_workspace_identity(&workspace).unwrap();
            if marker.base_commit == c && marker.pending_base_transition.is_none() {
                break marker;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        assert_eq!(marker.base_commit, c);
        assert!(marker.pending_base_transition.is_none());
        assert_eq!(
            db.query_text("SELECT base_commit FROM jobs WHERE id = 'job-overlap'", ())
                .await
                .unwrap()
                .as_deref(),
            Some(c.as_str())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(jj)]
    async fn consecutive_external_advances_with_preflight_preserve_dirty_bytes() {
        let Some((home, project, workspaces, _, _)) = jj_test_env() else {
            return;
        };
        let remote = TempDir::new().unwrap();
        assert!(crate::env::git()
            .args(["init", "--bare", "-q"])
            .arg(remote.path())
            .status()
            .is_ok_and(|status| status.success()));
        assert!(run_test_git(
            project.path(),
            &["remote", "add", "origin", &remote.path().to_string_lossy()]
        ));
        assert!(run_test_git(
            project.path(),
            &["push", "-q", "-u", "origin", "main"]
        ));

        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let mut git = MockGitClient::new();
        git.expect_fetch_origin().times(2).returning(|repo| {
            run_test_git(repo, &["fetch", "-q", "origin"])
                .then_some(())
                .ok_or_else(|| "test git fetch failed".to_string())
        });
        let mut orch = test_orchestrator(db, git);
        let bin = std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                Command::new("which")
                    .arg("jj")
                    .output()
                    .ok()
                    .filter(|output| output.status.success())
                    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            })
            .unwrap();
        orch.jj_binary_path = bin.clone();
        let repo_path = project.path().to_string_lossy().to_string();
        orch.db
            .local
            .execute(
                "UPDATE projects SET repo_path = ?1 WHERE id = 'proj-1'",
                params![repo_path.as_str()],
            )
            .await
            .unwrap();

        let jj = crate::jj::JjEnv::resolve(&bin, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, project.path());
        crate::jj::ensure_project_store(&jj, &store, project.path()).unwrap();
        let workspace = workspaces.path().join("external-interleaving");
        let branch = "agent/PROJ-2-builder-0";
        crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
        let a = crate::jj::revset_commit(&jj, &store, "main").unwrap();
        let workspace_path = workspace.to_string_lossy().to_string();
        orch.db
            .local
            .execute(
                "UPDATE jobs SET worktree_path = ?1, branch = ?2, base_commit = ?3, base_branch = 'main' WHERE id = 'job-overlap'",
                params![workspace_path.as_str(), branch, a.as_str()],
            )
            .await
            .unwrap();
        crate::jj::write_project_root_marker(&workspace, project.path()).unwrap();
        let identity = crate::jj::WorkspaceIdentity::new(
            "job-overlap",
            "job-overlap",
            "proj-1",
            project.path().to_path_buf(),
            workspace.clone(),
            branch,
            crate::jj::workspace_name_for_branch(branch),
            a,
        );
        crate::jj::write_workspace_identity(&workspace, &identity).unwrap();

        std::fs::write(project.path().join("base.txt"), "base b\n").unwrap();
        assert!(run_test_git(project.path(), &["add", "."]));
        assert!(run_test_git(
            project.path(),
            &["commit", "-q", "-m", "base b"]
        ));
        assert!(run_test_git(
            project.path(),
            &["push", "-q", "origin", "main"]
        ));
        reconcile_external_default_advance(&orch, "proj-1", "main")
            .await
            .unwrap();
        let b = crate::jj::revset_commit(&jj, &store, "main@origin").unwrap();

        let local_bytes = b"exact local bytes\n\0not text\n";
        std::fs::write(workspace.join("local.bin"), local_bytes).unwrap();
        let request = cairn_common::protocol::CallbackRequest {
            thread_id: None,
            cwd: workspace_path.clone(),
            run_id: Some("run-job-overlap".to_string()),
            tool: "run".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        };
        crate::mcp::vcs::prepare_managed_workspace(&orch, &request)
            .await
            .unwrap()
            .expect("managed workspace preflight");
        assert_eq!(
            std::fs::read(workspace.join("local.bin")).unwrap(),
            local_bytes
        );

        std::fs::write(project.path().join("base.txt"), "base c\n").unwrap();
        assert!(run_test_git(project.path(), &["add", "."]));
        assert!(run_test_git(
            project.path(),
            &["commit", "-q", "-m", "base c"]
        ));
        assert!(run_test_git(
            project.path(),
            &["push", "-q", "origin", "main"]
        ));
        reconcile_external_default_advance(&orch, "proj-1", "main")
            .await
            .unwrap();
        let c = crate::jj::revset_commit(&jj, &store, "main@origin").unwrap();
        assert_ne!(b, c);

        assert_eq!(
            std::fs::read(workspace.join("local.bin")).unwrap(),
            local_bytes
        );
        let marker = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let marker = crate::jj::read_workspace_identity(&workspace).unwrap();
                if marker.base_commit == c && marker.pending_base_transition.is_none() {
                    break marker;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("reconcile worker advances durable base");
        assert_eq!(marker.base_commit, c);
        assert!(marker.pending_base_transition.is_none());
        let active_bases = orch
            .db
            .local
            .read(|conn| {
                let workspace_path = workspace_path.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT base_commit FROM jobs WHERE worktree_path = ?1 AND status IN ('pending', 'running', 'blocked', 'idle')",
                            params![workspace_path.as_str()],
                        )
                        .await?;
                    let mut bases = Vec::new();
                    while let Some(row) = rows.next().await? {
                        bases.push(row.text(0)?);
                    }
                    Ok::<_, DbError>(bases)
                })
            })
            .await
            .unwrap();
        assert!(!active_bases.is_empty());
        assert!(active_bases.iter().all(|base| base == &c));
        let failures = orch
            .db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM messages WHERE content LIKE '%durable base advancement failed%'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(failures, Some(0));
        drop(home);
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(jj)]
    async fn finalized_on_lineage_mismatches_normalize_forward() {
        let Some((_home, _project, workspaces, jj, store)) = jj_test_env() else {
            return;
        };
        let a = crate::jj::revset_commit(&jj, &store, "main").unwrap();
        let b = advance_test_commit(&jj, &store, &a, "base-b");
        let c = advance_test_commit(&jj, &store, &b, "base-c");

        for (name, marker_base, database_base) in [
            ("database-ahead", a.as_str(), b.as_str()),
            ("marker-ahead", b.as_str(), a.as_str()),
        ] {
            let db = migrated_db().await;
            seed_base_advance_fixture(&db).await;
            let workspace = workspaces.path().join(name);
            let sibling = durable_base_fixture(&db, &workspace, database_base, marker_base).await;
            advance_sibling_durable_base(&db, &jj, &store, &sibling, &c)
                .await
                .unwrap();

            let marker = crate::jj::read_workspace_identity(&workspace).unwrap();
            assert_eq!(marker.base_commit, c);
            assert!(marker.pending_base_transition.is_none());
            assert_eq!(
                db.query_text("SELECT base_commit FROM jobs WHERE id = 'job-overlap'", (),)
                    .await
                    .unwrap()
                    .as_deref(),
                Some(c.as_str())
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial(jj)]
    async fn incomparable_finalized_mismatch_refuses_without_mutation() {
        let Some((_home, _project, workspaces, jj, store)) = jj_test_env() else {
            return;
        };
        let a = crate::jj::revset_commit(&jj, &store, "main").unwrap();
        let marker_branch = advance_test_commit(&jj, &store, &a, "marker-branch");
        let database_branch = advance_test_commit(&jj, &store, &a, "database-branch");
        jj.run(
            &store,
            &["new", &marker_branch, &database_branch],
            "create incomparable merge target",
        )
        .unwrap();
        std::fs::write(store.join("target.txt"), "target\n").unwrap();
        jj.run(
            &store,
            &["describe", "-m", "target"],
            "describe incomparable merge target",
        )
        .unwrap();
        let target = crate::jj::revset_commit(&jj, &store, "@").unwrap();
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let workspace = workspaces.path().join("incomparable");
        let sibling = durable_base_fixture(&db, &workspace, &database_branch, &marker_branch).await;
        std::fs::write(workspace.join("local.txt"), "preserved bytes\n").unwrap();

        let error = advance_sibling_durable_base(&db, &jj, &store, &sibling, &target)
            .await
            .unwrap_err();
        assert!(error.contains(&marker_branch), "{error}");
        assert!(error.contains(&database_branch), "{error}");
        assert!(error.contains(&target), "{error}");
        assert!(
            error.contains("off-target") || error.contains("divergent/incomparable"),
            "{error}"
        );
        assert!(
            error.contains("cairn:~/workspace-recovery action=rebind"),
            "{error}"
        );
        assert!(error.contains("Do not force-push"), "{error}");
        assert_eq!(
            std::fs::read_to_string(workspace.join("local.txt")).unwrap(),
            "preserved bytes\n"
        );
        let marker = crate::jj::read_workspace_identity(&workspace).unwrap();
        assert_eq!(marker.base_commit, marker_branch);
        assert!(marker.pending_base_transition.is_none());
        assert_eq!(
            db.query_text("SELECT base_commit FROM jobs WHERE id = 'job-overlap'", (),)
                .await
                .unwrap()
                .as_deref(),
            Some(database_branch.as_str())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_base_transition_completes_after_database_cas_interruption() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let workspace = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(workspace.path().join(".jj")).unwrap();
        let workspace_path = workspace.path().to_string_lossy().to_string();
        db.execute(
            "UPDATE jobs SET worktree_path = ?1, base_commit = 'new-base' WHERE id = 'job-overlap'",
            params![workspace_path.as_str()],
        )
        .await
        .unwrap();
        let mut identity = crate::jj::WorkspaceIdentity::new(
            "job-overlap",
            "job-overlap",
            "proj-1",
            std::path::PathBuf::from("/repo"),
            workspace.path().to_path_buf(),
            "branch-overlap",
            "branch-overlap",
            "old-base",
        );
        identity.pending_base_transition = Some(crate::jj::WorkspaceBaseTransition {
            old_base: "old-base".to_string(),
            new_base: "new-base".to_string(),
        });
        crate::jj::write_workspace_identity(workspace.path(), &identity).unwrap();

        let sibling = SiblingJob {
            id: "job-overlap".to_string(),
            worktree_path: workspace_path,
            branch: Some("branch-overlap".to_string()),
            base_commit: Some("new-base".to_string()),
        };
        let jj = crate::jj::JjEnv::resolve("", workspace.path());
        advance_sibling_durable_base(&db, &jj, workspace.path(), &sibling, "new-base")
            .await
            .unwrap();

        let finalized = crate::jj::read_workspace_identity(workspace.path()).unwrap();
        assert_eq!(finalized.base_commit, "new-base");
        assert!(finalized.pending_base_transition.is_none());
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
    async fn failed_reconcile_queues_waking_steer_note() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let siblings = vec![SiblingJob {
            id: "job-overlap".to_string(),
            worktree_path: "/wt/overlap".to_string(),
            branch: Some("agent/PROJ-2-builder-0".to_string()),
            base_commit: None,
        }];
        let failed = vec![crate::jj::ReconcileFailure {
            branch: "agent/PROJ-2-builder-0".to_string(),
            workspace_path: "/wt/overlap".into(),
            error: "jj workspace update-stale failed: exact stderr".to_string(),
        }];

        notify_failed_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &failed,
            "external advance on main",
            "test-failed",
        )
        .unwrap();

        let (content, wake): (String, String) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut messages = conn
                        .query("SELECT content FROM messages WHERE recipient_run_id = 'run-job-overlap'", ())
                        .await?;
                    let content = messages.next().await?.expect("failed sibling message").text(0)?;
                    let mut pushes = conn
                        .query("SELECT wake FROM attention_pushes WHERE recipient = 'job-overlap'", ())
                        .await?;
                    let wake = pushes.next().await?.expect("failed sibling attention push").text(0)?;
                    Ok::<_, DbError>((content, wake))
                })
            })
            .await
            .unwrap();
        assert!(content.contains("agent's managed jj workspace"));
        assert!(content.contains("/wt/overlap"));
        assert!(content.contains("jj workspace update-stale failed: exact stderr"));
        assert!(content.contains("Your work was preserved"));
        assert!(!content.to_lowercase().contains("retry"));
        assert!(content.contains("do not force-push"));
        assert_eq!(wake, "wake");
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
                base_commit: None,
            },
            SiblingJob {
                id: "job-clean".to_string(),
                worktree_path: "/wt/clean".to_string(),
                branch: Some("agent/PROJ-3-builder-0".to_string()),
                base_commit: None,
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
            "test-conflicted",
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
            base_commit: None,
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
            "test-team-conflicted",
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
                base_commit: None,
            },
            SiblingJob {
                id: "job-clean".to_string(),
                worktree_path: "/wt/clean".to_string(),
                branch: Some("agent/PROJ-3-builder-0".to_string()),
                base_commit: None,
            },
        ];
        let ambiguous = vec![AmbiguousDivergence {
            branch: "agent/PROJ-2-builder-0".to_string(),
            change_id: "qpvuntsmxyzw".to_string(),
            twins: vec!["aaaa1111".to_string(), "bbbb2222".to_string()],
        }];

        notify_ambiguous_divergence(
            &orch,
            &orch.db.local,
            &siblings,
            &ambiguous,
            "test-ambiguous",
        )
        .unwrap();

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
            base_commit: None,
        }];
        let clean = vec!["agent/PROJ-3-builder-0".to_string()];
        let note = build_jj_clean_note("integration", Some(42), None);

        notify_clean_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &clean,
            &note,
            "test-clean",
        )
        .unwrap();
        // A crash after enqueue but before the reconcile checkpoint retries the
        // same deterministic delivery key. It must not append a second message.
        notify_clean_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &clean,
            &note,
            "test-clean",
        )
        .unwrap();

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_fetch_does_not_block_publication_lock_during_external_reconcile() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let mut git = MockGitClient::new();
        git.expect_fetch_origin()
            .with(mockall::predicate::eq(PathBuf::from("/repo")))
            .return_once(move |_| {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                Ok(())
            });
        let orch = Arc::new(test_orchestrator(db, git));
        let reconcile_orch = orch.clone();
        let reconcile = tokio::spawn(async move {
            reconcile_external_default_advance(&reconcile_orch, "proj-1", "integration").await
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("production reconcile reached the injected fetch stall");

        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new("/repo"));
        orch.acquire_jj_store_lock_with_timeout(
            &store,
            "test run commit barrier publication",
            Some(std::time::Duration::from_millis(100)),
        )
        .await
        .expect("publication acquires the canonical store lock while reconcile fetch is stalled");

        release_tx.send(()).unwrap();
        reconcile.await.unwrap().unwrap();
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
