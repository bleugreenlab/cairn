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

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::execution::routing::{owning_db_for_job, owning_db_for_project};
use crate::messages::delivery::{
    latest_run_for_job, queue_system_direct, queue_system_direct_once,
    queue_system_direct_once_confirmed, DirectQueueDisposition,
};
use crate::messages::queued::DeliveryUrgency;
use crate::models::ExecutionSnapshot;
use crate::orchestrator::conflict_session::{
    close_open_sessions_for_branch, record_conflict_session, record_marker_state,
    supersede_stale_sessions, IncomingIdentity, MarkerState, ReplayDecision,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::executor_protocol::{ResidencyOperation, ResidencyResult};
use cairn_db::turso::params;

/// Publish a managed store-authoritative branch, recovering exactly once when jj
/// proves origin moved unexpectedly. Network transfer occurs without the store
/// mutex; import and graph convergence occur under it; the verified retry is
/// again outside it. This boundary prevents slow network I/O from blocking store
/// writers while ensuring no jj mutation can fork the operation log.
pub(crate) async fn publish_managed_branch(
    orch: &Orchestrator,
    store: &Path,
    branch: &str,
) -> Result<(), String> {
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    match crate::jj::publish_branch_to_origin(&jj, store, branch).await {
        Ok(()) => return Ok(()),
        Err(crate::jj::StoreBookmarkPushError::Failed(error)) => return Err(error),
        Err(crate::jj::StoreBookmarkPushError::StaleRemote(error)) => {
            log::warn!("publish `{branch}` found stale origin state; fetching and converging once: {error}");
        }
    }

    let fetch_store = store.to_path_buf();
    let fetch_branch = branch.to_string();
    tokio::task::spawn_blocking(move || {
        crate::jj::fetch_remote_branch_via_git(&fetch_store, "origin", &fetch_branch)
    })
    .await
    .map_err(|error| format!("stale publication fetch worker failed: {error}"))??;

    {
        let guard = orch
            .acquire_jj_store_lock(store, format!("stale publication recovery: {branch}"))
            .await;
        let _phase = guard.phase(format!("import and converge branch={branch}"));
        crate::jj::converge_managed_branch_after_remote_rewrite(&jj, store, branch)?;
    }

    crate::jj::publish_branch_to_origin(&jj, store, branch)
        .await
        .map_err(|error| format!("stale publication retry failed: {error}"))
}

/// Invalidate every cached GitHub verdict for a branch that moved locally but
/// did not reach origin. The live artifact compares GitHub's head with the store
/// and names both commits; this cache downgrade closes the window before anyone
/// opens that artifact, so the desktop cannot continue presenting the old head's
/// green mergeability as current.
async fn mark_publication_unconfirmed(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "UPDATE merge_requests
         SET github_mergeable = 'UNKNOWN', github_fetched_at = NULL, updated_at = ?3
         WHERE project_id = ?1 AND source_branch = ?2
           AND status NOT IN ('merged', 'closed')
           AND (github_state IS NULL OR lower(github_state) NOT IN ('merged', 'closed'))",
        params![project_id, branch, now],
    )
    .await
    .map_err(|error| format!("invalidate stale pull-request verdict for `{branch}`: {error}"))?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );
    Ok(())
}

async fn release_reminted_claim_after_failure(
    db: &LocalDb,
    original_intent_id: &str,
    effective_claim: &ReconcileClaim,
) {
    if effective_claim.id != original_intent_id {
        release_reconcile_claim(db, effective_claim).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn refresh_stale_durable_intent(
    db: &LocalDb,
    repo_path: &str,
    store: &Path,
    target_branch: &str,
    stored_destination: &str,
    current_destination: &str,
    refresh_source: &str,
    claim: ReconcileClaim,
) -> Result<Option<ReconcileClaim>, String> {
    if stored_destination == current_destination {
        return Ok(Some(claim));
    }

    let diagnostic = format!(
        "superseded stale destination pin {stored_destination} with current destination {current_destination}"
    );
    let changed = db
        .execute(
            "UPDATE jj_reconcile_intents
             SET status = 'superseded', lease_owner = NULL, lease_expires_at = NULL,
                 last_diagnostic = ?3, updated_at = ?4
             WHERE id = ?1 AND lease_owner = ?2 AND status = 'running'",
            params![
                claim.id.as_str(),
                claim.owner.as_str(),
                diagnostic.as_str(),
                chrono::Utc::now().timestamp()
            ],
        )
        .await
        .map_err(|error| format!("supersede stale reconcile intent: {error}"))?;
    if changed == 0 {
        return Err(format!(
            "durable reconcile intent {} lost its lease while refreshing destination",
            claim.id
        ));
    }
    log::info!(
        "jj reconcile worker superseded durable intent {}: {diagnostic}",
        claim.id
    );

    let reminted = claim_reconcile_intent(
        db,
        repo_path,
        store,
        target_branch,
        current_destination,
        refresh_source,
    )
    .await?;
    if reminted.is_none() {
        log::info!(
            "jj reconcile worker reaped durable intent {}: current destination {} is already owned or completed",
            claim.id,
            current_destination
        );
    }
    Ok(reminted)
}

/// A base advance must not move a job's branch while one of its process batches
/// is still executing off-turn. The suspension row is the durable execution
/// bracket: it is created before the provider turn yields and resolved only
/// after the executor result has been published. Moving the branch inside that
/// bracket can leave the resolver publishing against a coordinate that no
/// longer names the running batch.
async fn jobs_have_inflight_run_batches(
    db: &LocalDb,
    siblings: &[SiblingJob],
) -> Result<bool, String> {
    for sibling in siblings {
        let job_id = sibling.id.clone();
        let active = db
            .read(|conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT condition_json FROM agent_waits
                             WHERE job_id = ?1 AND state IN ('pending', 'resolving')",
                            params![job_id.as_str()],
                        )
                        .await?;
                    while let Some(row) = rows.next().await? {
                        let condition = serde_json::from_str::<
                            crate::mcp::handlers::durable_suspend::Condition,
                        >(&row.text(0)?);
                        if matches!(
                            condition,
                            Ok(crate::mcp::handlers::durable_suspend::Condition::RunBatch { .. })
                        ) {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })
            })
            .await
            .map_err(|error| format!("inspect in-flight run batches: {error}"))?;
        if active {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Debug)]
struct MergedJob {
    id: String,
    project_id: String,
    issue_id: Option<String>,
    /// The branch that just merged, and is about to be deleted. Anything cut
    /// from it is stranded the moment it goes.
    branch: Option<String>,
    base_branch: Option<String>,
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

#[derive(Debug, Clone)]
struct SiblingJob {
    id: String,
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

#[derive(Clone)]
struct BaseAdvanceNotes {
    conflict: String,
    clean: String,
    /// What advanced the base, carried through to the durable resolution session
    /// so `cairn:~/rebase` can name the incoming change instead of describing an
    /// anonymous "the base moved".
    incoming: IncomingIdentity,
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

/// Sentinel for sibling selection when no merged job should be excluded.
const EXCLUDE_NONE: &str = "";

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct BranchAdvanceOutcome {
    eligible: usize,
    rebased_clean: usize,
    conflicted: usize,
    failed: usize,
    coalesced_destination: Option<String>,
}

/// Project conflict markers into every live checkout holding this branch, and
/// report back only what the executor CONFIRMED.
///
/// The return value is the marker state to persist. It is deliberately
/// pessimistic: with no live checkout there is nothing to mark, and an executor
/// that could not be reached leaves the session `Pending` so the retry pass picks
/// it up rather than a wake claiming markers exist.
async fn materialize_markers_for_branch(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    diagnostic: &crate::jj::ConflictDiagnostic,
) -> (MarkerState, Option<String>, Vec<(String, String)>) {
    let paths = diagnostic.conflicting_paths();
    if paths.is_empty() {
        return (
            MarkerState::NotMaterialized,
            Some("the rebase named no conflicting paths".to_string()),
            Vec::new(),
        );
    }
    let (Some(ours), Some(theirs)) = (diagnostic.ours.as_deref(), diagnostic.theirs.as_deref())
    else {
        return (
            MarkerState::NotMaterialized,
            Some("the conflict did not record both merge sides".to_string()),
            Vec::new(),
        );
    };
    let Ok(leases) = load_live_terminal_leases(db, project_id, branch).await else {
        return (
            MarkerState::Pending,
            Some("could not enumerate this branch's live checkouts".to_string()),
            Vec::new(),
        );
    };
    if leases.is_empty() {
        return (
            MarkerState::NotMaterialized,
            Some("this branch has no live checkout to project markers into".to_string()),
            Vec::new(),
        );
    }

    let mut confirmed: Vec<(String, String)> = Vec::new();
    let mut last_error = None;
    let mut confirmations = 0usize;
    let mut attempts = 0usize;
    let mut retryable = false;
    for (residency_holder, incarnation_id, cell_epoch, _job_id) in leases {
        attempts += 1;
        let Some(holder) =
            cairn_common::executor_protocol::ResidencyHolder::parse_storage_key(&residency_holder)
        else {
            last_error = Some(format!(
                "unreadable execution environment {residency_holder}"
            ));
            continue;
        };
        let result = orch
            .fleet
            .operate_residency(
                orch,
                ResidencyOperation::MaterializeConflict {
                    fence: cairn_common::executor_protocol::ResidencyFence {
                        holder,
                        incarnation_id,
                        cell_epoch,
                    },
                    request: cairn_common::executor_protocol::ConflictMaterializationRequest {
                        expected_head: ours.to_string(),
                        base_commit: diagnostic.base.clone(),
                        ours_commit: ours.to_string(),
                        theirs_commit: theirs.to_string(),
                        paths: paths.clone(),
                    },
                },
            )
            .await;
        match result {
            ResidencyResult::ConflictMaterialized { outcome, .. } => {
                confirmations += 1;
                for path in outcome.paths {
                    confirmed.push((path.path, path.disposition.as_str().to_string()));
                }
            }
            ResidencyResult::Failed {
                kind, diagnostic, ..
            } => {
                // An unreachable executor is a fact about the link, not about
                // this branch: keep the request alive for the retry pass.
                retryable |= matches!(
                    kind,
                    cairn_common::executor_protocol::ResidencyFailureKind::Unavailable
                );
                last_error = Some(diagnostic);
            }
            other => last_error = Some(format!("unexpected materialization reply: {other:?}")),
        }
    }

    // Marker state is stored once per branch, but materialization happens per
    // CHECKOUT. So a partial success cannot be recorded as `Materialized`: the
    // resource would then tell an agent whose checkout got nothing that markers
    // are present in it, which is precisely the false claim this whole design
    // exists to prevent. Only unanimity is confirmation; anything short of it
    // stays retryable and reads as "not confirmed".
    if confirmations == attempts && confirmations > 0 {
        (MarkerState::Materialized, last_error, confirmed)
    } else if confirmations > 0 {
        (
            MarkerState::Pending,
            Some(format!(
                "markers landed in {confirmations} of {attempts} live checkouts{}",
                last_error
                    .map(|error| format!("; last failure: {error}"))
                    .unwrap_or_default()
            )),
            confirmed,
        )
    } else if retryable {
        (MarkerState::Pending, last_error, Vec::new())
    } else {
        (MarkerState::Failed, last_error, Vec::new())
    }
}

async fn refresh_residencies_for_branch(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    new_tip: &str,
) -> usize {
    let rows = load_live_terminal_leases(db, project_id, branch).await;
    let Ok(leases) = rows else {
        log::error!("committed branch advance could not enumerate residencies: {rows:?}");
        return 1;
    };
    let mut failed = 0;
    for (residency_holder, incarnation_id, cell_epoch, job_id) in leases {
        let Some(holder) =
            cairn_common::executor_protocol::ResidencyHolder::parse_storage_key(&residency_holder)
        else {
            failed += 1;
            log::error!(
                "terminal row records an unreadable execution environment {residency_holder}"
            );
            continue;
        };
        let result = orch
            .fleet
            .operate_residency(
                orch,
                ResidencyOperation::RefreshCheckout {
                    fence: cairn_common::executor_protocol::ResidencyFence {
                        holder,
                        incarnation_id: incarnation_id.clone(),
                        cell_epoch,
                    },
                    base_commit: new_tip.to_string(),
                    require_clean: false,
                },
            )
            .await;
        if let ResidencyResult::Failed {
            kind, diagnostic, ..
        } = result
        {
            if kind == cairn_common::executor_protocol::ResidencyFailureKind::Unavailable {
                failed += 1;
                log::warn!(
                    "terminal lease {residency_holder} could not be refreshed while its executor is disconnected: {diagnostic}"
                );
                continue;
            }
            if kind == cairn_common::executor_protocol::ResidencyFailureKind::NotFound {
                match crate::terminal_host::resolve_missing_terminal_lease(
                    db,
                    &residency_holder,
                    &incarnation_id,
                    cell_epoch,
                )
                .await
                {
                    Ok(true) => {
                        log::warn!("terminal lease {residency_holder} no longer exists on an executor; cleared its persisted fence");
                        if let Some(run_id) = latest_run_for_job(db, &job_id) {
                            let note = format!(
                                "[Terminal ended] Cairn's executor no longer reports terminal lease {residency_holder}. Its stale lease binding was cleared; restart the terminal to acquire a fresh checkout."
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
                        log::error!(
                            "could not clear missing terminal lease {residency_holder}: {error}"
                        );
                    }
                }
                continue;
            }
            failed += 1;
            log::error!("committed branch advance could not refresh terminal lease {residency_holder}: {diagnostic}");
            if let Some(run_id) = latest_run_for_job(db, &job_id) {
                let note = format!(
                    "⛔ BLOCKING [Terminal head reconciliation] The branch commit succeeded, but Cairn could not advance terminal lease {residency_holder} to {new_tip}. The committed branch was not rolled back. Exact executor diagnostic: {diagnostic}"
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
    failed += crate::dev_instances::sync_live_branch_instances(orch, project_id, branch, new_tip)
        .await
        .len();
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
                    "SELECT DISTINCT t.residency_holder, t.residency_incarnation_id, t.cell_epoch, t.job_id
             FROM job_terminals t JOIN jobs j ON j.id = t.job_id
             WHERE j.project_id = ?1 AND j.branch = ?2
               AND t.status = 'running' AND t.residency_holder IS NOT NULL",
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
    // Validate the advanced integration bookmark and refresh any authorized
    // held cells on that exact coordinate before reconciling descendants.
    {
        let guard = orch
            .acquire_jj_store_lock(&store, format!("jj on-branch advance for {merged_job_id}"))
            .await;
        let _phase = guard.phase("on-branch coordinate advance");
        refresh_advanced_branch_cells(orch, db, &merged_job.project_id, base_branch, repo_path)
            .await;
    }

    // The store owns the LOCAL fold, but it does not own what origin did with the
    // pull request. An operator squash-merging on GitHub mints a commit id the
    // store has never seen, so origin moves and the store's local default
    // bookmark does not — and this path used to neither fetch nor import, on the
    // reasoning that a Cairn-driven fold needs neither. That reasoning holds for
    // the fold and fails for the merge, which is why nearly every merge left the
    // bookmark to conflict. Reconcile before the sibling gate, and fetch first so
    // the commit origin holds is actually in the store to reconcile onto.
    if branch_is_project_default(db, &merged_job.project_id, base_branch).await {
        let repo = Path::new(repo_path);
        if project_has_origin(orch, repo) {
            let ctx = format!("merged job {}", merged_job.id);
            match fetch_origin_outside_store_lock(orch, repo).await {
                Ok(()) => {
                    reconcile_store_default_bookmark(
                        orch,
                        db,
                        &merged_job.project_id,
                        repo,
                        base_branch,
                        &ctx,
                    )
                    .await
                }
                Err(error) => log::warn!(
                    "{ctx}: git fetch origin failed; skipping default bookmark reconcile: {error}"
                ),
            }
        }
        crate::execution::checks_main::spawn(
            orch,
            merged_job.project_id.clone(),
            repo_path.to_string(),
            base_branch.to_string(),
            Some(merged_job.id.clone()),
        );
    }

    // Anything cut FROM the branch that just merged loses its base the moment
    // that branch is deleted. Re-point it before the sibling set is read, so
    // those children inherit this advance in the same pass instead of being
    // stranded by it.
    if let Some(merged_branch) = merged_job
        .branch
        .as_deref()
        .filter(|branch| !branch.is_empty() && *branch != base_branch)
    {
        if let Err(error) = repoint_children_of_merged_branch(
            db,
            &merged_job.project_id,
            merged_branch,
            base_branch,
        )
        .await
        {
            log::warn!(
                "merged job {}: could not re-point work cut from `{merged_branch}` onto \
                 `{base_branch}`: {error}",
                merged_job.id
            );
        }
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
    // bookmark.
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
        incoming: IncomingIdentity {
            base_branch: base_branch.to_string(),
            pr_number,
            issue: issue_info
                .as_ref()
                .map(|issue| format!("{}/{}", issue.project_key, issue.number)),
        },
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

/// Bring the store's local bookmark for the project's default branch back into
/// agreement with origin, under the per-store lock.
///
/// Every caller runs this BEFORE deciding whether any sibling needs rebasing.
/// That ordering is the whole point: the import and repair used to be reachable
/// only through store provisioning, downstream of an `if siblings.is_empty()`
/// return, so the overwhelmingly common shape — a PR merges with nothing else in
/// flight — reconciled nothing at all and left the tracked bookmark to conflict
/// on the next operation that touched it. Reconciliation is not a step the
/// sibling rebase needs; it is what keeps the store able to answer for the
/// default branch.
///
/// The backward repair is gated HERE rather than at each call site, so no caller
/// can reach it with a branch origin does not own. `branch` must be the
/// project's configured default branch; an integration or agent branch
/// legitimately holds sealed work origin has not seen and is left untouched.
///
/// Non-fatal end to end. A reconciliation failure is operator-facing diagnostic
/// output; it never becomes a message to an agent and never fails the caller.
async fn reconcile_store_default_bookmark(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    repo_path: &Path,
    branch: &str,
    ctx: &str,
) {
    if !branch_is_project_default(db, project_id, branch).await {
        log::debug!(
            "{ctx}: `{branch}` is not project {project_id}'s default branch; not reconciling it onto origin"
        );
        return;
    }
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let guard = orch
        .acquire_jj_store_lock(&store, format!("default bookmark reconcile: {ctx}"))
        .await;
    let _phase = guard.phase("default bookmark reconcile");
    if let Err(error) = crate::jj::ensure_store_initialized(&jj, &store, repo_path) {
        log::warn!("{ctx}: ensure jj store failed; skipping default bookmark reconcile: {error}");
        return;
    }
    match crate::jj::reconcile_tracked_bookmark(&jj, &store, branch) {
        Ok(outcome) => log::debug!("{ctx}: default bookmark `{branch}` reconcile: {outcome:?}"),
        Err(error) => log::error!(
            "{ctx}: default bookmark `{branch}` did not reconcile against origin: {error}"
        ),
    }
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
/// `reconcile_base_advance` gates both). This reconciliation is driven by
/// an observed remote advance, independently of main-checkout lifecycle. Non-fatal
/// end to end — every failure is logged
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
    // Arm independently of sibling reconciliation. Its own retry refreshes and
    // reconciles the canonical bookmark, so a transient failure below cannot
    // erase the observed main advance.
    crate::execution::checks_main::spawn(
        orch,
        project_id.to_string(),
        repo_path.clone(),
        default_branch.to_string(),
        None,
    );
    // Bring the advanced tip into the shared store and reconcile the default
    // bookmark onto it — BEFORE looking at siblings, because this is owed whether
    // or not anything downstream needs rebasing.
    let repo_path_path = Path::new(&repo_path);
    // Transfer objects and update the ordinary Git repository's origin-tracking
    // refs before entering the jj critical section. Git object writes are
    // content-addressed and additive; jj observes the fetched refs only when the
    // locked import below runs, preserving the store's single-writer discipline.
    if let Err(error) = fetch_origin_outside_store_lock(orch, repo_path_path).await {
        log::warn!("external advance on {default_branch}: git fetch origin failed: {error}");
        return Ok(());
    }
    // Provision and import the store before anything downstream depends on it
    // resolving. A store that cannot be provisioned aborts the whole reconcile
    // rather than letting the sibling pass fail on an unresolvable dest.
    {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, repo_path_path);
        let store_guard = orch
            .acquire_jj_store_lock(&store, format!("external import on {default_branch}"))
            .await;
        let _phase = store_guard.phase("git ref import");
        if let Err(error) = crate::jj::ensure_project_store(&jj, &store, repo_path_path) {
            log::warn!("external advance on {default_branch}: ensure store failed: {error}");
            return Ok(());
        }
    }
    reconcile_store_default_bookmark(
        orch,
        &db,
        project_id,
        repo_path_path,
        default_branch,
        &format!("external advance on {default_branch}"),
    )
    .await;
    let siblings = load_sibling_jobs(&db, project_id, default_branch, EXCLUDE_NONE).await?;
    if siblings.is_empty() {
        log::debug!("external advance on {default_branch}: no in-flight siblings to reconcile");
        return Ok(());
    }

    let notes = BaseAdvanceNotes {
        conflict: build_external_advance_conflict_note(default_branch),
        clean: build_external_advance_clean_note(default_branch),
        incoming: IncomingIdentity {
            base_branch: default_branch.to_string(),
            ..IncomingIdentity::default()
        },
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
    let repo_path = Path::new(&project.repo_path);
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let remote_default = remote_default_revset(&project.default_branch);
    let ctx = format!("startup remote advance on {}", project.default_branch);
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
            log::warn!("{ctx}: ensure store failed: {error}");
            return Ok(());
        }
        crate::jj::revset_commit(&jj, &store, &remote_default)
    };

    if let Err(error) = fetch_origin_outside_store_lock(orch, repo_path).await {
        log::warn!("{ctx}: git fetch origin failed: {error}");
        return Ok(());
    }

    // Reconcile the default bookmark unconditionally on the way through. A
    // restart is exactly when the store is most likely to be holding a bookmark
    // that conflicts with what origin did while Cairn was closed, and the old
    // shape — gated on in-flight siblings, gated again on the tip having moved —
    // meant a restart could not heal it either.
    reconcile_store_default_bookmark(
        orch,
        db,
        &project.id,
        repo_path,
        &project.default_branch,
        &ctx,
    )
    .await;

    let after = {
        let _store_guard = orch
            .acquire_jj_store_lock(
                &store,
                format!(
                    "startup remote advance snapshot on {}",
                    project.default_branch
                ),
            )
            .await;
        crate::jj::revset_commit(&jj, &store, &remote_default)
    };
    if before == after {
        log::debug!("{ctx}: origin tip unchanged; skipping sibling reconcile");
        return Ok(());
    }
    if after.is_none() {
        log::debug!("{ctx}: origin tip did not resolve after fetch; skipping");
        return Ok(());
    }

    let siblings =
        load_sibling_jobs(db, &project.id, &project.default_branch, EXCLUDE_NONE).await?;
    if siblings.is_empty() {
        log::debug!("{ctx}: no in-flight siblings to reconcile");
        return Ok(());
    }

    let notes = BaseAdvanceNotes {
        conflict: build_external_advance_conflict_note(&project.default_branch),
        clean: build_external_advance_clean_note(&project.default_branch),
        incoming: IncomingIdentity {
            base_branch: project.default_branch.clone(),
            ..IncomingIdentity::default()
        },
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
/// default-branch advance): build the branch specs, snapshot each
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

/// How often the durable sweep revisits queued reconcile work.
pub(crate) const RECONCILE_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

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
    let original_intent_id = work.claim.id.clone();
    let repo_path = load_project_repo_path(db, &project_id)
        .await?
        .ok_or_else(|| format!("project {project_id} has no repository path"))?;
    let siblings = load_sibling_jobs(db, &project_id, &work.target_branch, EXCLUDE_NONE).await?;
    let specs = siblings
        .iter()
        .filter_map(sibling_branch)
        .collect::<Vec<_>>();
    if specs.is_empty() {
        log::info!(
            "jj reconcile worker reaped durable intent {}: no live sibling bookmarks remain for {}",
            work.claim.id,
            work.target_branch
        );
        finish_reconcile_intent(db, &work.claim.id, &work.claim.owner, false).await?;
        return Ok(());
    }
    let external = work
        .sources
        .iter()
        .any(|source| source.contains("external advance"));
    let rebase_dest = if external {
        remote_default_revset(&work.target_branch)
    } else {
        work.target_branch.clone()
    };
    let refresh_source = if external {
        "external advance durable destination refresh"
    } else {
        "durable destination refresh"
    };
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let (pinned_dest, claim, existing_bookmarks) = {
        let _guard = orch
            .acquire_jj_store_lock(
                &work.store,
                format!(
                    "durable reconcile destination refresh ({})",
                    work.target_branch
                ),
            )
            .await;
        let current_dest =
            crate::jj::revset_commit(&jj, &work.store, &rebase_dest).ok_or_else(|| {
                format!("durable reconcile destination `{rebase_dest}` did not resolve")
            })?;
        let Some(claim) = refresh_stale_durable_intent(
            db,
            &repo_path,
            &work.store,
            &work.target_branch,
            &work.destination_commit,
            &current_dest,
            refresh_source,
            work.claim,
        )
        .await?
        else {
            return Ok(());
        };
        let existing_bookmarks = crate::jj::query_local_bookmarks(&jj, &work.store, &specs).ok();
        (current_dest, claim, existing_bookmarks)
    };
    let notes = BaseAdvanceNotes {
        conflict: build_jj_conflict_note(&work.target_branch, None, None),
        clean: build_jj_clean_note(&work.target_branch, None, None),
        // A durable retry resumes work whose original trigger is no longer in
        // hand, so the session records the base it is landing on and nothing it
        // cannot vouch for.
        incoming: IncomingIdentity {
            base_branch: work.target_branch.clone(),
            ..IncomingIdentity::default()
        },
    };
    let label = format!("durable retry on {}", work.target_branch);
    let effective_claim = ReconcileClaim {
        id: claim.id.clone(),
        owner: claim.owner.clone(),
        project_id: claim.project_id.clone(),
    };
    let result = execute_reconcile_claim(
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
        pinned_dest,
        claim,
    )
    .await
    .map(|_| ());
    if result.is_err() {
        release_reminted_claim_after_failure(db, &original_intent_id, &effective_claim).await;
    }
    result
}

fn first_claim_this_sweep(claimed: &mut HashSet<String>, intent_id: &str) -> bool {
    claimed.insert(intent_id.to_owned())
}

/// Run one sweep pass so that a panic inside it becomes a value here instead of
/// unwinding the caller.
///
/// The sweep loop is the only thing that ever revisits deferred reconcile work,
/// so its liveness is the liveness of the whole recovery path, and a panic that
/// escapes into the loop retires that path for the lifetime of the process
/// without a log line. Joining a child task converts the panic into an `Err` the
/// loop can report and continue past.
pub(crate) async fn supervised_sweep_pass<F>(pass: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(pass)
        .await
        .map_err(|error| format!("sweep pass did not complete: {error}"))
}

pub(crate) async fn sweep_reconcile_intents(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        let mut claimed_this_pass = HashSet::new();
        loop {
            let work = match claim_next_reconcile_intent(&db).await {
                Ok(Some(work)) => work,
                Ok(None) => break,
                Err(error) => {
                    log::warn!("jj reconcile worker failed to claim durable work: {error}");
                    break;
                }
            };
            if !first_claim_this_sweep(&mut claimed_this_pass, &work.claim.id) {
                release_reconcile_claim(&db, &work.claim).await;
                break;
            }
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
        retry_pending_materializations(orch, &db).await;
    }
}

/// Re-attempt marker materializations left pending, typically because the
/// executor holding the checkout was unreachable at conflict time.
///
/// Pending is deliberately a state rather than a silent failure: the wake told
/// the agent markers were not confirmed, and this is what eventually makes that
/// sentence change. A session whose markers land here is not re-notified — the
/// agent already has the coordinates and the resource — so this only upgrades
/// what `cairn:~/rebase` reports.
async fn retry_pending_materializations(orch: &Orchestrator, db: &LocalDb) {
    let pending = match load_pending_materializations(db).await {
        Ok(pending) => pending,
        Err(error) => {
            log::warn!("could not enumerate pending marker materializations: {error}");
            return;
        }
    };
    for (intent_id, bookmark) in pending {
        let session =
            match crate::orchestrator::conflict_session::load_active_session(db, &bookmark).await {
                Ok(Some(session)) if session.intent_id == intent_id => session,
                // The session closed or was superseded while this was queued. There
                // is nothing left to mark, and marking it would be scaffolding for a
                // merge nobody will perform.
                Ok(_) => continue,
                Err(error) => {
                    log::warn!("could not reload pending session for {bookmark}: {error}");
                    continue;
                }
            };
        let diagnostic = crate::jj::ConflictDiagnostic {
            base: session.base.clone(),
            ours: session.ours.clone(),
            theirs: session.theirs.clone(),
            conflicted_tip: session.conflicted_tip.clone(),
            condition: Default::default(),
            incoming: session
                .files
                .iter()
                .filter(|file| file.is_conflicting())
                .map(|file| crate::jj::IncomingFile {
                    path: file.path.clone(),
                    status: file.status.clone(),
                    classification: crate::jj::IncomingClassification::Conflicting,
                })
                .collect(),
        };
        let (state, marker_diagnostic, dispositions) =
            materialize_markers_for_branch(orch, db, &session.project_id, &bookmark, &diagnostic)
                .await;
        if let Err(error) = record_marker_state(
            db,
            &intent_id,
            &bookmark,
            state,
            marker_diagnostic.as_deref(),
            &dispositions,
        )
        .await
        {
            log::warn!("could not record retried marker state for {bookmark}: {error}");
        }
    }
}

async fn load_pending_materializations(db: &LocalDb) -> Result<Vec<(String, String)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT intent_id, bookmark FROM jj_reconcile_items
                     WHERE marker_state = 'pending' AND resolution_state = 'open'",
                    (),
                )
                .await?;
            let mut pending = Vec::new();
            while let Some(row) = rows.next().await? {
                pending.push((row.text(0)?, row.text(1)?));
            }
            Ok(pending)
        })
    })
    .await
    .map_err(|error| format!("load pending materializations: {error}"))
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
         (intent_id, bookmark, observed_tip, status, failure_kind,
          outcome_kind, suppression_fingerprint, last_diagnostic, attempt_count, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9)
         ON CONFLICT(intent_id, bookmark) DO UPDATE SET
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

async fn wait_for_reconcile_slot(
    db: &LocalDb,
    store: &Path,
    target_branch: &str,
    destination: &str,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let store = store.to_string_lossy().into_owned();
    loop {
        let running = db
            .query_text(
                "SELECT status FROM jj_reconcile_intents
                 WHERE store_path = ?1 AND target_branch = ?2 AND destination_commit = ?3",
                params![store.as_str(), target_branch, destination],
            )
            .await
            .map_err(|error| format!("inspect reconcile intent: {error}"))?
            .as_deref()
            == Some("running");
        if !running {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "The reconcile already running for `{target_branch}` at `{destination}` did not \
                 yield a follow-on slot before its lease deadline. No replay was silently accepted; \
                 request it again after the running reconcile is recovered."
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
    let specs: Vec<String> = siblings.iter().filter_map(sibling_branch).collect();
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
        let candidate_names = specs.to_vec();
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
        return Ok(BranchAdvanceOutcome {
            coalesced_destination: Some(pinned_dest),
            ..BranchAdvanceOutcome::default()
        });
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

/// The sanctioned store-side replay: ask the shared jj store to move one
/// branch's ancestry onto the base it is supposed to sit on.
///
/// This is the only surface that can do it. An agent's slot is a plain git
/// worktree whose refs are downstream EXPORTS of the runner's private jj store,
/// so no ref move made there survives — which is why the request is enqueued as
/// durable reconcile work rather than run in the caller's slot. It reuses the
/// whole existing path: per-store lock, pinned-destination validation, intent
/// lease, rollback on conflict, verified export, residency refresh.
///
/// It is needed after EVERY content conflict, not only under a base-drift
/// classification. Resolving the conflicting files with ordinary writes fixes the
/// branch's content; its ancestry is still rooted at the old base, because the
/// rebase was rolled back and nothing replays it afterwards.
///
/// An open conflict session is NOT a precondition. A session is the artifact of
/// an advance that was attempted and hit a conflict, so requiring one made the
/// remedy reachable only in the case where the automatic path had already got
/// far enough to describe the problem — and unreachable in the case that needs
/// it most, where the advance never ran at all and the branch sits silently
/// behind its base with an unmergeable PR. Without a session this replays the
/// branch onto its base as recorded on the job; if that replay conflicts, the
/// reconcile worker opens the session on the way through, which is how a branch
/// the automatic path skipped gets one.
pub(crate) async fn request_branch_replay(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    branch: &str,
    expected_fingerprint: Option<&str>,
    take_committed_tip: bool,
    drop_incoming_reason: Option<&str>,
) -> Result<String, String> {
    let session = crate::orchestrator::conflict_session::load_active_session(db, branch).await?;

    // Both of these name a session's own artifacts. A request carrying one
    // without a session describes a world that does not exist, so it is answered
    // with which artifact is missing rather than accepted and quietly
    // reinterpreted as something else.
    if session.is_none() {
        if expected_fingerprint.is_some() {
            return Err(format!(
                "`{branch}` has no open rebase session, so there are no session coordinates for a \
                 fingerprint to pin. Request the replay without one."
            ));
        }
        if take_committed_tip {
            return Err(format!(
                "`resolution:\"take-committed-tip\"` restores an open session's CONFLICTING paths \
                 from your branch's committed tip, and `{branch}` has no open session, so there \
                 is no such set of paths. Request a plain `{{action:\"replay\"}}`: it moves your \
                 branch onto the current base without taking content from either side."
            ));
        }
    }

    // A request quoting coordinates that have since moved was composed against a
    // view of the world that no longer holds. Refuse and hand back the current
    // one rather than acting on the stale intent.
    if let (Some(session), Some(expected)) = (session.as_ref(), expected_fingerprint) {
        let current = session.fingerprint();
        if expected != current {
            return Err(format!(
                "This rebase session has moved on: you named fingerprint `{expected}`, but it is \
                 now `{current}`. Re-read cairn:~/rebase and request the replay again."
            ));
        }
    }

    // `take-committed-tip` restores each conflicting path WHOLE from the
    // branch's committed tip, so every incoming hunk in such a file that lives
    // OUTSIDE the region the agent resolved is discarded along with it. Refuse
    // rather than warn: this request is asynchronous and it lands on branch
    // ancestry, so a warning is read after a compiler finds the damage rather
    // than before it is done. The refusal is not a dead end — it hands back the
    // exact content that makes the same request correct.
    let mut caveats: Vec<String> = Vec::new();
    if let Some(session) = session.as_ref().filter(|_| take_committed_tip) {
        // Read-only, and deliberately outside the store lock: every object it
        // reads is immutable, and the one mutable thing (the bookmark) is
        // re-resolved by the replay itself. Holding the lock across dozens of
        // subprocess reads would stall the reconcile worker to no purpose.
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let store = Path::new(&session.store_path);
        // EXHAUSTIVE, not the read path's capped form. The restore covers every
        // conflicting path, so the decision has to as well; assessing a prefix
        // and accepting on its silence is the same silent loss wearing a guard's
        // clothes.
        //
        // The decision is a total match on a typed outcome rather than an
        // `if let`. Every way of NOT having a proof — base drift aside — has to
        // reach a refusal, and an `Option` here is what let "could not check"
        // fall through as "nothing to report" twice over.
        let proof = crate::orchestrator::conflict_session::assess_session_tip_exhaustively(
            &jj, store, session,
        );
        match crate::orchestrator::conflict_session::decide_replay(&proof, drop_incoming_reason) {
            ReplayDecision::Proceed => {}
            ReplayDecision::Refuse(refusal) => return Err(refusal),
            ReplayDecision::ProceedOnStatedReason(caveat) => {
                log::warn!(
                    "replay request for `{branch}`: proceeding with an unproven \
                     take-committed-tip restore on the requester's stated reason. {caveat}"
                );
                caveats.push(caveat);
            }
        }
    }

    let project = load_replay_project(db, job_id).await?;
    let siblings = vec![SiblingJob {
        id: job_id.to_string(),
        branch: Some(branch.to_string()),
        base_commit: project.base_commit.clone(),
    }];

    // With a session, the base it recorded; without one, the base the job was
    // cut from; and if that name is gone from the store, whatever this work is
    // still going to merge into. Resolve the CURRENT head of it, not the
    // destination this session was opened against: the request is to land on the
    // base as it is now.
    let session_base = session.as_ref().map(|session| {
        if session.incoming.base_branch.is_empty() {
            session.target_branch.as_str()
        } else {
            session.incoming.base_branch.as_str()
        }
    });
    let candidates = crate::orchestrator::replay_base::load_base_candidates_for_job(db, job_id)
        .await?
        .recorded_from_session(session_base);
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&project.repo_path));
    let resolved = {
        let guard = orch
            .acquire_jj_store_lock(&store, format!("replay request for {branch}"))
            .await;
        let _phase = guard.phase(format!("resolve replay destination branch={branch}"));
        crate::orchestrator::replay_base::resolve_base(&jj, &store, branch, &candidates)
            .map_err(|error| error.to_string())?
    };
    let base_branch = resolved.branch.clone();
    let destination = resolved.commit.clone();

    // The recorded name has been proven gone, so it is corrected here rather
    // than at whatever surface next trips over it — and before the
    // already-carried early return, because a branch that needs no replay still
    // needs a base it can be diffed and merged against. The preamble goes out
    // with EVERY answer for the same reason: an agent told only "nothing to
    // replay" learns nothing about the base that just changed underneath it.
    let mut preamble = String::new();
    if let Some(superseded) = resolved.superseded.as_deref() {
        crate::orchestrator::replay_base::repoint_recorded_base(db, job_id, &base_branch).await;
        preamble = format!(
            "The base `{superseded}` this branch recorded no longer exists in the store — the \
             usual cause is that its parent merged and the branch was deleted with it. So it is \
             measured against `{base_branch}`, {}, and the recorded base has been re-pointed \
             there so the surfaces that read it agree.\n\n",
            resolved.source.describe()
        );
    }

    // Only for a sessionless request. A session exists precisely because a
    // rebase was attempted and rolled back, so its branch is behind by
    // construction; a bare request is the one that can arrive with nothing to
    // do, and saying so beats queueing a no-op the requester then waits on.
    if session.is_none() && crate::jj::branch_carries_commit(&jj, &store, branch, &destination) {
        return Ok(format!(
            "{preamble}`{branch}` already carries `{base_branch}` at `{destination}` in its \
             ancestry, so there is nothing to replay. If its pull request still reports a \
             conflict, that is a different problem from a stale base — file it rather than \
             replaying again."
        ));
    }

    let incoming = match session.as_ref() {
        Some(session) => session.incoming.clone(),
        // A bare request vouches for the base it is landing on and nothing else:
        // no PR and no issue carried the advance, because as far as this branch
        // is concerned no advance was ever announced.
        None => IncomingIdentity {
            base_branch: base_branch.clone(),
            ..IncomingIdentity::default()
        },
    };
    let notes = BaseAdvanceNotes {
        conflict: build_jj_conflict_note(&base_branch, incoming.pr_number, None),
        clean: build_jj_clean_note(&base_branch, incoming.pr_number, None),
        incoming,
    };
    let label = if take_committed_tip {
        format!("resolved replay requested for {branch}")
    } else {
        format!("replay requested for {branch}")
    };

    // Candidate resolution happens before the destination intent is claimed, so
    // an already-running intent has a frozen work list and cannot absorb this
    // request. Wait for that pass, reopen the completed coordinate, and retry as
    // a follow-on pass. Returning success is reserved for a pass that actually
    // accepted this branch; timeout and database failures remain observable to
    // the requester. One deadline spans every attempt, including attempts made
    // after the base moves to a new destination.
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs((RECONCILE_LEASE_SECONDS + 30) as u64);
    let mut reopen_destination = destination.clone();
    // Every destination this request was re-aimed at, so the summary can name a
    // race instead of leaving it to look like a failed resolution.
    let mut retargets: Vec<(String, String)> = Vec::new();
    loop {
        let attempted_destination = reopen_destination.clone();
        reopen_reconcile_intent(db, &store, &base_branch, &reopen_destination, branch).await?;
        let outcome = reconcile_base_advance(
            orch,
            db,
            &project.id,
            &label,
            &project.repo_path,
            &base_branch,
            &base_branch,
            siblings.clone(),
            notes.clone(),
        )
        .await?;
        if outcome.eligible > 0 {
            break;
        }
        reopen_destination = outcome.coalesced_destination.ok_or_else(|| {
            "Replay reconciliation returned no accepted branch and no coalesced destination."
                .to_string()
        })?;
        if reopen_destination != attempted_destination {
            // Another merge landed on the base while this request sat in the
            // queue, so it was re-aimed. Named explicitly because the resulting
            // page churn reads exactly like a failed resolution otherwise, and
            // an agent who cannot tell them apart re-does content work that was
            // already correct.
            retargets.push((attempted_destination, reopen_destination.clone()));
        }
        wait_for_reconcile_slot(db, &store, &base_branch, &reopen_destination, deadline).await?;
    }

    // The request itself is the one fact nothing else records, and without it a
    // read of cairn:~/rebase looks identical before and after asking. A
    // sessionless request has no item row to carry the timestamp yet; the
    // reconcile pass it just queued writes one.
    if let Some(session) = session.as_ref() {
        mark_replay_requested(db, &session.intent_id, branch).await;
    }

    let mut summary = preamble;
    if let (Some(first), Some(last)) = (retargets.first(), retargets.last()) {
        let (from, to) = (&first.0, &last.1);
        summary.push_str(&format!(
            "The replay target advanced while your request was queued, from `{from}` to `{to}`, so \
             it was re-aimed at the current base. This is a race with another merge landing, not a \
             failed resolution — your content resolution still stands.\n\n"
        ));
    }
    summary.push_str(&format!(
        "Queued a store-side replay of `{branch}` onto `{base_branch}` at `{}`. The durable \
         reconcile worker performs it under the store lock; nothing runs in your slot. A clean \
         replay publishes your branch, refreshes your checkout, and closes this session. A replay \
         that still conflicts leaves your branch untouched and refreshes cairn:~/rebase with fresh \
         coordinates.",
        reopen_destination
    ));
    for caveat in &caveats {
        summary.push_str("\n\n⚠️ ");
        summary.push_str(caveat);
    }
    Ok(summary)
}

/// Record that an agent asked for a replay, so the resource can say one is
/// outstanding. Advisory: a failure here must not fail a request the reconcile
/// worker has already accepted.
async fn mark_replay_requested(db: &LocalDb, intent_id: &str, bookmark: &str) {
    if let Err(error) = db
        .execute(
            "UPDATE jj_reconcile_items SET replay_requested_at = ?3
             WHERE intent_id = ?1 AND bookmark = ?2",
            // Unix seconds, matching every other timestamp on this table so the
            // resource can render it with the same `clock::stamp`.
            params![intent_id, bookmark, chrono::Utc::now().timestamp()],
        )
        .await
    {
        log::warn!("replay request for `{bookmark}`: could not record the request time: {error}");
    }
}

struct ReplayProject {
    id: String,
    repo_path: String,
    base_commit: Option<String>,
}

async fn load_replay_project(db: &LocalDb, job_id: &str) -> Result<ReplayProject, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.id, p.repo_path, j.base_commit
                     FROM jobs j JOIN projects p ON j.project_id = p.id
                     WHERE j.id = ?1 LIMIT 1",
                    params![job_id],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(ReplayProject {
                id: row.text(0)?,
                repo_path: row.text(1)?,
                base_commit: row.opt_text(2)?,
            }))
        })
    })
    .await
    .map_err(|error| format!("load replay project: {error}"))?
    .ok_or_else(|| "This node has no project repository to replay against.".to_string())
}

/// Re-open an intent an earlier pass already finished, so an explicitly
/// requested replay is not swallowed as a duplicate.
///
/// Coalescing exists to stop AUTOMATIC work piling up at one destination. An
/// agent asking for a replay is not that: it is a new fact about the branch —
/// usually that a content conflict has just been resolved — and landing on the
/// same destination is exactly the point.
async fn reopen_reconcile_intent(
    db: &LocalDb,
    store: &Path,
    target_branch: &str,
    destination: &str,
    bookmark: &str,
) -> Result<(), String> {
    let store = store.to_string_lossy().into_owned();
    db.execute(
        "UPDATE jj_reconcile_intents
         SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
         WHERE store_path = ?1 AND target_branch = ?2 AND destination_commit = ?3
           AND status = 'completed'",
        params![store.as_str(), target_branch, destination],
    )
    .await
    .map_err(|error| format!("reopen reconcile intent: {error}"))?;
    db.execute(
        "UPDATE jj_reconcile_items SET status = 'pending', notification_sent = 0
         WHERE bookmark = ?1 AND intent_id IN (
             SELECT id FROM jj_reconcile_intents
             WHERE store_path = ?2 AND target_branch = ?3 AND destination_commit = ?4
         )",
        params![bookmark, store.as_str(), target_branch, destination],
    )
    .await
    .map_err(|error| format!("reopen reconcile item: {error}"))?;
    Ok(())
}

fn siblings_for_branch(siblings: &[SiblingJob], branch: &str) -> Vec<SiblingJob> {
    siblings
        .iter()
        .filter(|sibling| sibling.branch.as_deref() == Some(branch))
        .cloned()
        .collect()
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
    specs: Vec<String>,
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
    let mut marker_states: HashMap<String, MarkerState> = HashMap::new();
    let mut pending_quarantines: Vec<PendingReconcileQuarantine> = Vec::new();
    let mut before: HashMap<String, String> = HashMap::new();
    let mut after: HashMap<String, String> = HashMap::new();
    let mut report = crate::jj::ReconcileReport::default();
    let mut any_branch_deferred = false;

    for branch in &specs {
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

        // A requested replay is the explicit resolution operation: take the
        // branch's committed tip bytes for this session's conflicting paths.
        // Automatic base-advance reconciliation never receives this authority
        // and therefore retains the fail-closed rollback. A previous failed retry
        // may already name the resolved tip as `ours`, so tip inequality cannot
        // identify resolution intent reliably (the live CAIRN-3412 shape).
        let resolution_session = if label.starts_with("resolved replay requested for ") {
            crate::orchestrator::conflict_session::load_active_session(db, branch).await?
        } else {
            None
        };

        let mut ambiguous_item = None;
        let mut divergence_resolved = false;
        let mut item_report = if let Some(progress) =
            progress.as_ref().filter(|p| p.status == "graph_moved")
        {
            let mut resumed = crate::jj::ReconcileReport::default();
            match progress.outcome_kind.as_deref() {
                Some("conflicted") => resumed.conflicted.push(branch.clone()),
                Some("rebased_clean") => resumed.rebased_clean.push(branch.clone()),
                Some("silent") => resumed.silent.push(branch.clone()),
                Some("failed") => resumed.failed.push(crate::jj::ReconcileFailure {
                    branch: branch.clone(),
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

            // RunBatch suspension admission takes this same store lock while
            // inserting its durable row. Revalidating under the guard makes the
            // row check and bookmark mutation one exclusive bracket.
            let branch_siblings = siblings_for_branch(&siblings, branch);
            if jobs_have_inflight_run_batches(db, &branch_siblings).await? {
                log::info!(
                    "jj base advance ({label}): deferred {branch} at the mutation boundary for an in-flight run batch"
                );
                drop(guard);
                // Record the deferral rather than only counting it. A deferred
                // branch owes a rebase that has not been attempted, which is a
                // different state from both "rebased" and "never advanced" and
                // is the one an agent is least able to infer: no bookmark moved,
                // no note was sent, and cairn:~/rebase would otherwise report a
                // branch that had never met a base advance at all. The row is
                // what lets the resource say an advance is owed, and it survives
                // a restart that loses this pass.
                persist_reconcile_item(
                    db,
                    ReconcileItemUpdate {
                        intent_id,
                        bookmark: branch,
                        observed_tip: current_tip.as_deref(),
                        status: "pending",
                        failure_kind: None,
                        outcome_kind: Some("deferred"),
                        fingerprint: None,
                        diagnostic: Some(
                            "deferred at the mutation boundary: this branch had a run batch in \
                             flight, so its rebase was not attempted. It stays queued for the \
                             durable sweep.",
                        ),
                    },
                )
                .await?;
                any_branch_deferred = true;
                continue;
            }

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
            let resolution_paths = resolution_session
                .as_ref()
                .map(|session| {
                    session
                        .conflicting()
                        .map(|file| file.path.clone())
                        .collect::<Vec<_>>()
                })
                .filter(|paths| !paths.is_empty());
            let item_report = if ambiguous_item.is_some() {
                crate::jj::ReconcileReport::default()
            } else if let Some(paths) = resolution_paths.as_deref() {
                crate::jj::reconcile_resolved_sibling_without_publication(
                    &jj,
                    &store,
                    &pinned_dest,
                    branch,
                    paths,
                )
                .map_err(|error| format!("jj resolved sibling replay ({label}) failed: {error}"))?
            } else {
                let item = vec![branch.clone()];
                crate::jj::reconcile_siblings_without_publication(&jj, &store, &pinned_dest, &item)
                    .map_err(|error| format!("jj sibling reconcile ({label}) failed: {error}"))?
            };
            if ambiguous_item.is_none() {
                if let Some(commit) = crate::jj::bookmark_commit(&jj, &store, branch) {
                    after.insert(branch.clone(), commit);
                }
            }
            if before.get(branch) != after.get(branch) {
                if let Some(sibling) = siblings
                    .iter()
                    .find(|sibling| sibling.branch.as_deref() == Some(branch.as_str()))
                {
                    orch.invalidate_node_check_status(&sibling.id, "base-or-fork-point-advance");
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
        let publish =
            item_report.rebased_clean.contains(branch) && !item_report.silent.contains(branch);
        if publish {
            if let Err(error) = publish_managed_branch(orch, &store, branch).await {
                item_report
                    .rebased_clean
                    .retain(|candidate| candidate != branch);
                let cache_diagnostic =
                    mark_publication_unconfirmed(orch, db, &claim.project_id, branch)
                        .await
                        .err()
                        .map(|cache_error| format!("; additionally, {cache_error}"))
                        .unwrap_or_default();
                item_report.failed.push(crate::jj::ReconcileFailure {
                    branch: branch.clone(),
                    error: format!(
                        "origin push failed: {error}. GitHub still holds the previous branch head; the pull-request artifact has been downgraded to UNKNOWN until publication recovers{cache_diagnostic}"
                    ),
                });
            }
        }
        // Bookkeeping that could not be persisted does not change what the
        // graph did. The branch keeps its real classification and its agent
        // keeps whatever notification that classification earned; the
        // unpersisted coordinate is recorded on the reconcile item and logged
        // for the operator, and reaches nobody else.
        let mut internal_diagnostic: Option<String> = None;
        let touched =
            item_report.rebased_clean.contains(branch) || item_report.conflicted.contains(branch);
        if touched {
            if let Some(sibling) = siblings
                .iter()
                .find(|candidate| sibling_branch(candidate).as_deref() == Some(branch.as_str()))
            {
                if let Err(error) = advance_sibling_durable_base(db, sibling, &pinned_dest).await {
                    log::warn!(
                        "jj reconcile ({label}): durable base advancement for {branch} did not \
                         persist: {error}"
                    );
                    internal_diagnostic = Some(format!("durable base advancement failed: {error}"));
                }
            }
        }

        let reported_failure = item_report
            .failed
            .iter()
            .find(|failure| failure.branch == *branch)
            .map(|failure| failure.error.as_str());
        let item_diagnostic = reported_failure.or(internal_diagnostic.as_deref());
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
        let outcome_kind = if reported_failure.is_some() {
            "failed"
        } else if item_report.conflicted.contains(branch) {
            "conflicted"
        } else if item_report.rebased_clean.contains(branch) {
            "rebased_clean"
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

        // A conflicted branch opens (or refreshes) a durable resolution session,
        // then — and only then — markers are projected into its checkout. The
        // order matters: the session row must exist before the diagnostic can
        // land on it, and the marker state must be recorded from what the
        // executor confirmed before any wake is composed.
        if let Some(diagnostic) = item_report.conflict_diagnostics.get(branch) {
            record_conflict_session(db, intent_id, branch, diagnostic, &notes.incoming).await?;
            supersede_stale_sessions(db, branch, intent_id).await?;
            let (state, marker_diagnostic, dispositions) =
                materialize_markers_for_branch(orch, db, &claim.project_id, branch, diagnostic)
                    .await;
            record_marker_state(
                db,
                intent_id,
                branch,
                state,
                marker_diagnostic.as_deref(),
                &dispositions,
            )
            .await?;
            marker_states.insert(branch.clone(), state);
        } else if item_report.rebased_clean.contains(branch) {
            // The branch absorbed the incoming change, so whatever session it had
            // is genuinely finished. This is the only truthful close: an ordinary
            // commit resolves CONTENT, but a branch is not reconciled until its
            // ancestry actually moves, which is exactly what just happened.
            close_open_sessions_for_branch(db, branch).await?;
        }

        report.rebased_clean.append(&mut item_report.rebased_clean);
        report.conflicted.append(&mut item_report.conflicted);
        report.silent.append(&mut item_report.silent);
        report.held.append(&mut item_report.held);
        report.failed.append(&mut item_report.failed);
        report
            .conflict_diagnostics
            .extend(std::mem::take(&mut item_report.conflict_diagnostics));
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

    // Re-read each cleanly-rebased sibling's commit id AFTER the rebase, so we
    // notify only the ones whose commit actually changed (a no-op double-fire
    // leaves it equal). Conflicted siblings are deliberately absent: their rebase
    // was rolled back, so their commit is unchanged BY DESIGN and the same test
    // would mean the opposite thing.
    let after: HashMap<String, String> = report
        .rebased_clean
        .iter()
        .filter_map(|branch| {
            crate::jj::bookmark_commit(&jj, &store, branch).map(|commit| (branch.clone(), commit))
        })
        .collect();

    // Conflicted siblings are notified WITHOUT the rewritten filter, deliberately.
    // A conflicting rebase is now rolled back, so the branch's commit is exactly
    // what it was — the very condition `siblings_rewritten` reads as "nothing
    // happened". Filtering here would silence every conflict this reconcile found.
    // Redundancy is instead handled where it belongs: the delivery key is scoped
    // to this intent, so a resumed or double-fired reconcile at the same dest
    // cannot re-wake the agent, while a genuinely new base advance (a new intent)
    // correctly does.
    if report.conflicted.is_empty() {
        log::debug!("jj reconcile ({label}): no sibling conflicted with the advanced base");
    } else {
        // The conflicting files come from the reconcile report, captured inside
        // the rebase before it was rolled back. Nothing can enumerate them here:
        // the branch is clean again by now, so a fresh probe would report none.
        // (The old probe was worse than useless — `conflicted_files` runs
        // `jj resolve --list`, which is working-copy scoped, so aimed at the bare
        // store it answered for the store's scratch `@` rather than the sibling.)
        let evidence = ConflictEvidence {
            diagnostics: report
                .conflicted
                .iter()
                .filter_map(|branch| {
                    siblings.iter().find(|sibling| {
                        sibling_branch(sibling).as_deref() == Some(branch.as_str())
                    })?;
                    let diagnostic = report.conflict_diagnostics.get(branch).cloned()?;
                    Some((branch.clone(), diagnostic))
                })
                .collect(),
            marker_states,
        };
        notify_conflicted_siblings(
            orch,
            db,
            &siblings,
            &report.conflicted,
            &notes.conflict,
            &evidence,
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

    let retry_transient = any_branch_deferred || reconcile_has_transient_failures(&report.failed);
    // Fan a lifetime-cell refresh out to every sibling this reconcile actually
    // rewrote. A running cell on a rebased job branch must follow the logical
    // coordinate to the new tip or it keeps serving pre-rebase source. This is the
    // sibling analogue of the advanced-branch fan-out, and reaches every caller of
    // this shared body — including the external and startup default-advance paths
    // that previously skipped it. The store lock is released by now.
    // Only cleanly-rebased siblings moved; a conflicted sibling was rolled back to
    // the coordinate its cells already hold, so there is nothing to follow.
    let mut terminal_failed = 0;
    for branch in clean_rewritten.iter() {
        if let Some(new_tip) = after.get(branch) {
            terminal_failed +=
                refresh_residencies_for_branch(orch, db, project_id, branch, new_tip).await;
        }
    }

    finish_reconcile_intent(db, intent_id, &claim.owner, retry_transient).await?;
    Ok(BranchAdvanceOutcome {
        eligible: siblings.len(),
        rebased_clean: clean_rewritten.len(),
        conflicted: report.conflicted.len(),
        failed: report.failed.len() + terminal_failed,
        coalesced_destination: None,
    })
}

/// Filter a set of reconciled sibling branches down to those this reconcile
/// actually rewrote: a branch whose commit id changed between the before/after
/// snapshots. A double-fire reconcile at the same dest tip is a `jj rebase` no-op,
/// so the commit id is unchanged → the branch is filtered out and not re-notified
/// (conflicted or clean). When either snapshot is missing (an unexpected resolve
/// failure), notify conservatively rather than silently dropping a real change.
/// Record where a sibling's branch now sits after this reconcile rebased it.
///
/// `jobs.base_commit` is a **record of where a branch was last anchored**, not a
/// coordinate any surface resolves against: every reader that presents or acts
/// on a base coordinate derives it live from the store (CAIRN-3224). What is
/// left of the row is archival — a seed for `pack_anchor` and a degraded
/// fallback — and archival values do not need ordering.
///
/// This deliberately does not compare-and-swap, because the CAS it replaces
/// never provided ordering to begin with (CAIRN-3226). Its lost-race branch
/// re-read the row and then swapped against whatever it found, writing this
/// call's value unconditionally: two reconciles racing on one job always ended
/// with the later writer's value, exactly as a plain write does. The ceremony
/// bought nothing and cost two real things — a null row aborted the write with a
/// manufactured error, and a doubly-lost race produced an operator-facing
/// diagnostic for a non-event, the same shape that turned zero-delta planner
/// branches into ⛔ BLOCKING directs (CAIRN-3094 comment #4).
///
/// A write that matches no row means the job was deleted mid-reconcile. That is
/// the graph moving on, not a failure of this bookkeeping, so it is logged and
/// swallowed.
async fn advance_sibling_durable_base(
    db: &LocalDb,
    sibling: &SiblingJob,
    new_base: &str,
) -> Result<(), String> {
    if sibling.base_commit.as_deref() == Some(new_base) {
        return Ok(());
    }
    let changed = db
        .execute(
            "UPDATE jobs SET base_commit = ?2, updated_at = ?3 WHERE id = ?1",
            params![
                sibling.id.as_str(),
                new_base,
                chrono::Utc::now().timestamp()
            ],
        )
        .await
        .map_err(|error| {
            format!(
                "record durable base coordinate for job {}: {error}",
                sibling.id
            )
        })?;
    if changed == 0 {
        log::debug!(
            "jj reconcile: job {} no longer exists; its durable base record was not updated to \
             {new_base}",
            sibling.id
        );
    }
    Ok(())
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
    specs: Vec<String>,
    existing: Option<&std::collections::HashSet<String>>,
) -> (Vec<String>, usize) {
    let Some(existing) = existing else {
        return (specs, 0);
    };
    let total = specs.len();
    let retained: Vec<_> = specs
        .into_iter()
        .filter(|branch| existing.contains(branch))
        .collect();
    let dropped = total - retained.len();
    (retained, dropped)
}

/// The sibling's runner-owned logical bookmark.
fn sibling_branch(sibling: &SiblingJob) -> Option<String> {
    sibling.branch.clone()
}

/// The IDENTITY half of the note for a sibling whose auto-rebase could not be
/// applied cleanly: which base advanced, which PR and issue carried it, and the
/// fact that the rebase was rolled back.
///
/// It deliberately stops short of telling the agent what to do. What to do
/// depends on the condition the rebase actually found — a content conflict and
/// base drift call for opposite actions — and that is
/// [`append_conflict_diagnostic`]'s to say, from evidence, per branch. This half
/// is the part that is true regardless.
///
/// The branch was NOT rebased: a conflicting rebase is rolled back before it can
/// reach git, so the branch still sits on its own content at its own base, and no
/// conflict markers exist anywhere — not in the store, not in any cell. That is
/// what makes this actionable, and it is why the note never asks the agent to
/// "resolve markers" that were never materialized. It is still STOP-THE-LINE:
/// until the branch carries the new
/// base's content, its merge will keep being refused. Delivered via a `Steer`
/// system direct that wakes idle agents and lands at the next tool boundary
/// without stopping an active turn (see `notify_conflicted_siblings`).
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
        "⛔ BLOCKING [Base branch update] Your base branch `{base_branch}` advanced — {pr_fragment}{issue_fragment}. The automatic rebase could not be applied cleanly, so it was rolled back: your branch is untouched, on its own content, and nothing was lost. Your work cannot merge until it carries the new base."
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

/// Validate the runner-owned bookmark that advanced and refresh matching
/// held cells. Agent jobs have no follower workspace to update.
async fn refresh_advanced_branch_cells(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    repo_path: &str,
) {
    let on_branch = match load_on_branch_jobs(db, project_id, branch).await {
        Ok(jobs) => jobs,
        Err(error) => {
            log::warn!("on-branch advance: failed to load jobs on {branch}: {error}");
            return;
        }
    };
    if on_branch.is_empty() {
        log::debug!("on-branch advance on {branch}: no in-flight job uses the branch");
        return;
    }

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));

    // Collapse a pre-existing divergent twin on the integration bookmark ITSELF
    // before refreshing cells on that coordinate. A deterministic tangle
    // self-heals; an ambiguous one interrupts every on-branch job and skips the
    // refresh. Runs under the per-store lock the
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
                "jj collapse (on-branch {branch}): divergent change {change_id} is ambiguous (twins {}); interrupting on-branch jobs and skipping the refresh",
                twins.join(", ")
            );
            let mut all_notified = true;
            for job in &on_branch {
                let Some(run_id) = latest_run_for_job(db, &job.id) else {
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
                            job.id
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

    let refresh_failures =
        refresh_residencies_for_branch(orch, db, project_id, branch, &dest).await;
    if refresh_failures > 0 {
        log::warn!(
            "on-branch advance: {refresh_failures} held cell(s) failed to refresh for {branch}"
        );
    }
}

/// The note for a sibling whose auto-rebase recorded a conflict after the default
/// branch advanced **outside Cairn** (a non-Cairn merge or direct push detected
/// via the GitHub `push` webhook). Same shape as `build_jj_conflict_note` but
/// carries no PR number — there is no Cairn-tracked owner for the advance.
fn build_external_advance_conflict_note(default_branch: &str) -> String {
    format!(
        "⛔ BLOCKING [Base branch update] Your base branch `{default_branch}` advanced (changes landed outside Cairn). The automatic rebase could not be applied cleanly, so it was rolled back: your branch is untouched, on its own content, and nothing was lost. Your work cannot merge until it carries the new base."
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

/// How many incoming files a wake lists by name before summarizing the rest. A
/// base advance can legitimately carry hundreds; the point is to make the shape
/// of the incoming change obvious, not to paste a manifest into a message.
const MAX_LISTED_INCOMING: usize = 40;

/// Render one classification's file list, capped and counted.
fn render_incoming_group(label: &str, paths: &[String]) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let listed: Vec<&str> = paths
        .iter()
        .take(MAX_LISTED_INCOMING)
        .map(String::as_str)
        .collect();
    let elided = paths.len().saturating_sub(listed.len());
    let more = if elided > 0 {
        format!(" (and {elided} more)")
    } else {
        String::new()
    };
    format!("\n{label} ({}): {}{more}", paths.len(), listed.join(", "))
}

/// Append the conflict diagnostic to a base-advance note: the incoming change's
/// whole file set, the immutable three-way coordinates, and guidance specific to
/// the condition that was actually found.
///
/// The two lists are the point. A merged PR is one coordinated change across
/// many files, and a report naming only the conflicting subset is how an agent
/// comes to resolve the file it was told about, stop compiling, and have no idea
/// why (CAIRN-3337). Naming the clean-on-retry siblings makes it visible that
/// the tree is mid-change.
///
/// No patch text goes in here. The coordinates are immutable objects, so both
/// sides of the merge are recomputable on demand and a wake stays a wake.
/// What the agent may be told about markers, given only what was confirmed.
///
/// The standing rule is that machinery never instructs an agent to act on state
/// it has not made true. A wake saying "resolve the markers" when none exist is a
/// defect, so every branch here is keyed to the executor's own answer.
/// The condition decides whether editing files is even the right instruction, so
/// it is threaded in: under base drift the two sides already agree, and telling
/// that agent to "write the merged result" is the exact wasted round CAIRN-3327
/// and CAIRN-3328 each burned.
fn marker_guidance(state: MarkerState, condition: crate::jj::ConflictCondition) -> &'static str {
    let drift = condition == crate::jj::ConflictCondition::BaseDrift;
    match state {
        MarkerState::Materialized if drift => {
            "\nAny marker-bearing file in your checkout is scaffolding from this diagnostic, not \
             work to do: the two sides already agree. Request the replay above."
        }
        MarkerState::Materialized => {
            "\nConflict markers have been written into your checkout for the conflicting files \
             above. Resolve them with ordinary file writes; a file still containing marker syntax \
             cannot be committed, so nothing half-resolved can reach history."
        }
        MarkerState::Pending => {
            "\nMarkers were requested but are NOT confirmed present yet, so do not go looking for \
             them. Read both sides of the merge through `cairn:~/rebase` instead; it says so when \
             they land."
        }
        _ if drift => {
            "\nThere are no conflict markers in your checkout, and there is nothing to merge into \
             one. `cairn:~/rebase` carries the coordinates if you want to confirm that for \
             yourself."
        }
        _ => {
            "\nThere are no conflict markers in your checkout. Read both sides of the merge \
             through `cairn:~/rebase` and write the merged result yourself."
        }
    }
}

fn append_conflict_diagnostic(
    note: &str,
    diagnostic: Option<&crate::jj::ConflictDiagnostic>,
    marker_state: MarkerState,
) -> String {
    let Some(diagnostic) = diagnostic else {
        return note.to_string();
    };
    let conflicting = diagnostic.conflicting_paths();
    let clean = diagnostic.clean_on_retry_paths();

    let coordinates = match (
        diagnostic.base.as_deref(),
        diagnostic.ours.as_deref(),
        diagnostic.theirs.as_deref(),
    ) {
        (Some(base), Some(ours), Some(theirs)) => format!(
            "\nThree-way coordinates — base {base}, yours {ours}, incoming {theirs}. These are \
             immutable commits: read any file as of either side with `?branch=<commit>` to see \
             both versions without reconstructing them by hand."
        ),
        _ => String::new(),
    };

    let guidance = match diagnostic.condition {
        crate::jj::ConflictCondition::ContentConflict => {
            "\nThis is a CONTENT CONFLICT: the two sides genuinely disagree. Read both versions of \
             each conflicting file, write the merged result with ordinary edits, and commit it on \
             your branch. Pull the clean-on-retry files across too if your resolution depends on \
             them — they are part of the same coordinated change and your branch will not compile \
             without them. Never rebase or force-push by hand."
        }
        crate::jj::ConflictCondition::BaseDrift => {
            "\nThis is BASE DRIFT, not a content conflict: every conflicting file is already \
             byte-identical between your branch and the new base, so there is nothing to merge and \
             editing will not clear it. What is stale is your branch's ANCESTRY, and you cannot \
             repair that from here — your checkout is a plain git worktree whose refs are \
             downstream exports of the runner's private jj store, so no local ref move survives. \
             Ask the store to replay your branch instead: \
             write({changes:[{target:\"cairn:~/rebase\",mode:\"patch\",payload:{action:\"replay\"}}]})."
        }
    };

    format!(
        "{note}{}{}{coordinates}{guidance}{}\nThe complete session — both sides of the merge as \
         browsable patches, the whole incoming file set, and the sanctioned replay action — is at \
         `cairn:~/rebase`.",
        render_incoming_group("Conflicting files, yours to merge", &conflicting),
        render_incoming_group("Also arriving with this change, cleanly on retry", &clean),
        marker_guidance(marker_state, diagnostic.condition),
    )
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
        // Runner-internal bookkeeping never addresses an agent. The
        // `persistence` kind covers the durable-base compare-and-swap and
        // genuine database persist errors; an agent can act on neither. The CAS
        // path already declines to report these as failures at all — this is the
        // backstop that keeps any future internal error from reaching a builder.
        let failure_kind = crate::jj::reconcile_failure_kind(&failure.error);
        if failure_kind == "persistence" {
            log::warn!(
                "jj reconcile: internal {failure_kind} failure on {} withheld from job {}: {}",
                failure.branch,
                sibling.id,
                failure.error
            );
            continue;
        }
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj reconcile: no run for failed sibling {} to steer",
                sibling.id
            );
            continue;
        };
        let quarantine_note = if crate::jj::reconcile_failure_is_permanent(failure_kind) {
            let guidance = match failure_kind {
                "immutable_commit" => "This branch points at a commit that can no longer be changed, which usually means the work already landed. If it did, close the PR and the issue; otherwise report the diagnostic above.",
                "conflicted_bookmark" => "This branch's name is in a conflicted state and needs a maintainer; report the diagnostic above.",
                "missing_bookmark" => "This branch no longer exists and needs to be re-created; report the diagnostic above.",
                _ => "Report the diagnostic above.",
            };
            format!(
                "\nFuture base-branch updates will skip this branch until it changes. {guidance}"
            )
        } else {
            String::new()
        };
        let note = format!(
            "⛔ BLOCKING [Base branch update] Cairn could not update branch `{}` after the base branch advanced ({label}).\nDiagnostic:\n{}\nYour commits are intact. Do not force-push or reset; this branch needs repair before it can move.{quarantine_note}",
            failure.branch,
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

/// What the reconcile pass learned about each conflicted branch: the diagnostic
/// captured before its rollback, and the marker state an executor confirmed.
/// They travel together because a wake is only truthful when composed from both.
#[derive(Default)]
struct ConflictEvidence {
    diagnostics: HashMap<String, crate::jj::ConflictDiagnostic>,
    marker_states: HashMap<String, MarkerState>,
}

fn notify_conflicted_siblings(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    conflicted: &[String],
    note: &str,
    evidence: &ConflictEvidence,
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
        let diagnostic = evidence.diagnostics.get(&branch);
        let marker_state = evidence
            .marker_states
            .get(&branch)
            .copied()
            .unwrap_or_default();
        let message = append_conflict_diagnostic(note, diagnostic, marker_state);
        // The delivery key carries the diagnostic's fingerprint, so a resumed or
        // double-fired reconcile at the SAME base cannot re-wake the agent, while
        // a genuinely new base advance — different coordinates — correctly does.
        // Without it, a second advance arriving inside one intent would be
        // silently swallowed as a duplicate of the first.
        let key = match diagnostic {
            Some(diagnostic) => format!(
                "{delivery_scope}:{branch}:conflicted:{}",
                diagnostic.fingerprint()
            ),
            None => format!("{delivery_scope}:{branch}:conflicted"),
        };
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
        "⛔ BLOCKING [Divergent history] Your branch `{branch}` has more than one version of the same change ({}) that Cairn could not reconcile automatically — either every copy still conflicts or more than one carries edits, so choosing for you could lose work. Sort it out by hand (keep the correct commit, discard the rest), then verify the build and tests. Do NOT force-push; if you cannot resolve it cleanly, escalate to a human. Change id: `{change_id}`.",
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
            if job.base_branch.is_some() {
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
            "SELECT id, project_id, issue_id, branch, base_branch
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
                branch: row.opt_text(3)?,
                base_branch: row.opt_text(4)?,
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
                        "SELECT id, project_id, issue_id, branch, base_branch
                             FROM jobs
                             WHERE execution_id = ?1
                               AND recipe_node_id = ?2
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
                        branch: row.opt_text(3)?,
                        base_branch: row.opt_text(4)?,
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
                    "SELECT id, project_id, issue_id, branch, base_branch
                         FROM jobs
                         WHERE execution_id = ?1
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
                        branch: row.opt_text(3)?,
                        base_branch: row.opt_text(4)?,
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
/// A job that can still act on the base it records: it is not on a resolved
/// issue, and it is either still running or still holding an open pull request.
///
/// Shared by the sibling selection and the child re-point, because the two have
/// to name one population. Every branch that inherits a base advance is a branch
/// whose recorded base must survive that advance, and a predicate written twice
/// is a population that eventually differs.
const LIVE_JOB_PREDICATE: &str = "NOT EXISTS (
              SELECT 1 FROM issues i
               WHERE i.id = j.issue_id AND i.status IN ('merged', 'closed')
            )
            AND ( j.status NOT IN ('complete', 'failed', 'cancelled')
                  OR EXISTS (
                    SELECT 1 FROM merge_requests mr
                     WHERE mr.source_branch = j.branch
                       AND mr.project_id = j.project_id
                       AND mr.status NOT IN ('merged', 'closed')
                  ) )";

/// Re-point everything cut from a branch that has just merged.
///
/// A child issue's execution records its parent's integration branch in
/// `jobs.base_branch`, and merging the parent deletes that branch. From that
/// moment the record names nothing, and it is the name through which the child's
/// diff range, its pull request's target, the next base advance, and the replay
/// that would rescue it all resolve. What the parent merged INTO is where those
/// children were always going — it is what GitHub itself retargets their open
/// pull requests onto — and it is knowable at exactly this moment rather than
/// reconstructable later from a name that no longer resolves.
///
/// Both records that hold it are corrected, because both are read: the job's
/// base branch, and the target of any open pull request still naming the merged
/// branch. The jobs re-pointed here land in the same pass's sibling set, so they
/// inherit this advance rather than being stranded by it.
async fn repoint_children_of_merged_branch(
    db: &LocalDb,
    project_id: &str,
    merged_branch: &str,
    new_base: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    // The update target is not aliased: the liveness predicate is evaluated in a
    // subselect that owns the `j` alias, which keeps one predicate usable from
    // both a SELECT and an UPDATE.
    let jobs_sql = format!(
        "UPDATE jobs SET base_branch = ?3, updated_at = ?4
          WHERE id IN (
            SELECT j.id FROM jobs j
             WHERE j.project_id = ?1 AND j.base_branch = ?2 AND {LIVE_JOB_PREDICATE}
          )"
    );
    let jobs = db
        .execute(&jobs_sql, params![project_id, merged_branch, new_base, now])
        .await
        .map_err(|error| format!("re-point jobs cut from `{merged_branch}`: {error}"))?;
    let pull_requests = db
        .execute(
            "UPDATE merge_requests SET target_branch = ?3, updated_at = ?4
              WHERE project_id = ?1 AND target_branch = ?2
                AND status NOT IN ('merged', 'closed')",
            params![project_id, merged_branch, new_base, now],
        )
        .await
        .map_err(|error| format!("re-target pull requests aimed at `{merged_branch}`: {error}"))?;

    if jobs > 0 || pull_requests > 0 {
        log::info!(
            "merged branch `{merged_branch}` was folded into `{new_base}`: re-pointed {jobs} job(s) \
             and {pull_requests} open pull request(s) that were cut from it"
        );
    }
    Ok(())
}

async fn load_sibling_jobs(
    db: &LocalDb,
    project_id: &str,
    base_branch: &str,
    merged_job_id: &str,
) -> Result<Vec<SiblingJob>, String> {
    let project_id = project_id.to_string();
    let base_branch = base_branch.to_string();
    let merged_job_id = merged_job_id.to_string();
    let sql = format!(
        "SELECT j.id, j.branch, j.base_commit
           FROM jobs j
          WHERE j.project_id = ?1
            AND j.base_branch = ?2
            AND j.id != ?3
            AND {LIVE_JOB_PREDICATE}"
    );
    db.read(|conn| {
        let project_id = project_id.clone();
        let base_branch = base_branch.clone();
        let merged_job_id = merged_job_id.clone();
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &sql,
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
                    branch: row.opt_text(1)?,
                    base_commit: row.opt_text(2)?,
                });
            }
            Ok(siblings)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Active jobs whose durable branch is the branch that just advanced.
async fn load_on_branch_jobs(
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
                    "SELECT j.id, j.branch, j.base_commit
                         FROM jobs j
                         WHERE j.project_id = ?1
                           AND j.branch = ?2
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
            let mut jobs = Vec::new();
            while let Some(row) = rows.next().await? {
                jobs.push(SiblingJob {
                    id: row.text(0)?,
                    branch: row.opt_text(1)?,
                    base_commit: row.opt_text(2)?,
                });
            }
            Ok(jobs)
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

/// Whether `branch` is the project's configured default branch, and so the one
/// branch for which origin is the sole authority.
///
/// The gate on the backward repair in `reconcile_tracked_bookmark`: a Coordinator
/// integration branch or an agent branch legitimately holds sealed work origin
/// has not seen, and must never be reset onto origin. An unreadable project row
/// answers `false` — declining to reconcile is always safe, resetting the wrong
/// branch is not.
async fn branch_is_project_default(db: &LocalDb, project_id: &str, branch: &str) -> bool {
    match load_project_default_branch(db, project_id).await {
        Ok(Some(default_branch)) => default_branch == branch,
        Ok(None) => false,
        Err(error) => {
            log::warn!(
                "could not read default_branch for project {project_id}; skipping default bookmark reconcile: {error}"
            );
            false
        }
    }
}

async fn load_project_default_branch(
    db: &LocalDb,
    project_id: &str,
) -> Result<Option<String>, String> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT default_branch FROM projects WHERE id = ?1",
                    params![project_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.get::<Option<String>>(0)?.filter(|s| !s.is_empty())),
                None => Ok(None),
            }
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
        let live = "agent/cairn-1-builder-0".to_string();
        let ghost = "agent/cairn-1-ghost-0".to_string();
        let existing: std::collections::HashSet<String> = [live.clone()].into_iter().collect();

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
    use std::sync::Arc;

    #[tokio::test]
    async fn base_advance_defers_while_a_sibling_run_batch_is_in_flight() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        for sql in [
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-batch','job-overlap','open','backend',1,1)",
            "UPDATE runs SET session_id='session-batch' WHERE id='run-job-overlap'",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-batch','session-batch','run-job-overlap','job-overlap',1,'yielded','initial',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        let root = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();
        let sibling = SiblingJob {
            id: "job-overlap".into(),
            branch: Some("agent/overlap".into()),
            base_commit: None,
        };

        // Reconcile wins the store lock first and reaches its final mutation
        // boundary. A batch that starts between the initial claim query and this
        // point cannot insert its bracket row underneath that mutation.
        let store = PathBuf::from("/store");
        let mutation_guard = orch
            .acquire_jj_store_lock(&store, "test reconcile mutation")
            .await;
        let admission_orch = orch.clone();
        let admission_db = db.clone();
        let admission_store = store.clone();
        let mut admission = tokio::spawn(async move {
            let _guard = admission_orch
                .acquire_jj_store_lock(&admission_store, "test run batch admission")
                .await;
            admission_db
                .execute(
                    "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-batch','job-overlap','run-job-overlap','session-batch','turn-batch','tool-batch','{\"kind\":\"run_batch\",\"request_id\":\"request-1\",\"commits\":false,\"label\":\"focused test\"}','pending',1)",
                    (),
                )
                .await
                .unwrap();
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut admission)
                .await
                .is_err(),
            "run-batch admission must wait while bookmark mutation owns the store lock"
        );
        assert!(
            !jobs_have_inflight_run_batches(&db, std::slice::from_ref(&sibling))
                .await
                .unwrap()
        );

        drop(mutation_guard);
        admission.await.unwrap();
        let _revalidation_guard = orch
            .acquire_jj_store_lock(&store, "test reconcile revalidation")
            .await;
        assert!(jobs_have_inflight_run_batches(&db, &[sibling])
            .await
            .unwrap());
    }

    /// The sweep is the only mechanism that revisits a deferred base advance, so
    /// a pass that panics must cost one cycle and not the loop. Before this was
    /// supervised, a single panic retired the sweep for the lifetime of the
    /// process: work queued for retry was never claimed again, and the branches
    /// waiting on it sat silently behind their base.
    #[tokio::test]
    async fn a_panicking_sweep_pass_does_not_retire_the_loop() {
        let passes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for attempt in 0..3 {
            let counter = passes.clone();
            let outcome = supervised_sweep_pass(async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if attempt == 0 {
                    panic!("dirty pages should be empty for read txn");
                }
            })
            .await;
            assert_eq!(
                outcome.is_err(),
                attempt == 0,
                "the panicking pass reports, and only it reports"
            );
        }
        assert_eq!(
            passes.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "every pass after the panic still ran"
        );
    }

    /// A branch deferred at the mutation boundary owes a rebase nobody has
    /// attempted. That is neither "done" nor "never advanced", and the resume
    /// and delivery passes must both leave it alone so the next sweep picks it
    /// up rather than treating it as serviced.
    #[tokio::test]
    async fn a_deferred_sibling_stays_queued_rather_than_reading_as_serviced() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let claim =
            claim_reconcile_intent(&db, "/repo", Path::new("/store"), "main", "dest-a", "test")
                .await
                .unwrap()
                .unwrap();

        persist_reconcile_item(
            &db,
            ReconcileItemUpdate {
                intent_id: &claim.id,
                bookmark: "agent/overlap",
                observed_tip: Some("tip-a"),
                status: "pending",
                failure_kind: None,
                outcome_kind: Some("deferred"),
                fingerprint: None,
                diagnostic: Some("deferred at the mutation boundary"),
            },
        )
        .await
        .unwrap();

        let progress = reconcile_item_status(&db, &claim.id, "agent/overlap")
            .await
            .unwrap()
            .expect("the deferral is recorded rather than left invisible");
        assert_eq!(progress.status, "pending");
        assert_eq!(progress.outcome_kind.as_deref(), Some("deferred"));
        assert!(
            !progress.notification_sent,
            "nothing was delivered, because nothing was attempted"
        );

        // Delivery only closes out graph movement and suppression. A deferral is
        // neither, so it must survive untouched.
        mark_reconcile_delivered(&db, &claim.id).await.unwrap();
        let after = reconcile_item_status(&db, &claim.id, "agent/overlap")
            .await
            .unwrap()
            .expect("the deferral survives the delivery pass");
        assert_eq!(after.status, "pending");
        assert!(!after.notification_sent);
    }

    /// The refusal that used to greet every sessionless replay is gone, but the
    /// two payload keys that only mean something ALONGSIDE a session must still
    /// be refused, and each must say which artifact is missing rather than
    /// silently doing a different thing.
    ///
    /// `take-committed-tip` matters most: it restores a session's CONFLICTING
    /// paths from the branch tip, so with no session there is no such set, and
    /// accepting it would mean choosing some other set on the requester's
    /// behalf.
    #[tokio::test]
    async fn a_sessionless_replay_refuses_the_keys_that_need_a_session() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        let root = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();

        let fingerprinted = request_branch_replay(
            &orch,
            &db,
            "job-clean",
            "agent/clean",
            Some("base:ours:theirs"),
            false,
            None,
        )
        .await
        .expect_err("a fingerprint pins session coordinates that do not exist");
        assert!(
            fingerprinted.contains("no open rebase session")
                && fingerprinted.contains("without one"),
            "the refusal names the missing artifact and the way forward: {fingerprinted}"
        );

        let restored =
            request_branch_replay(&orch, &db, "job-clean", "agent/clean", None, true, None)
                .await
                .expect_err("there is no set of conflicting paths to restore");
        assert!(
            restored.contains("CONFLICTING paths") && restored.contains("Request a plain"),
            "the refusal explains the scope it cannot resolve and offers the plain replay: \
             {restored}"
        );
    }

    /// A job with no base branch has nothing to be replayed onto, and that is a
    /// different answer from "no session" — it must not fall through to a
    /// destination resolved from an empty string.
    #[tokio::test]
    async fn a_sessionless_replay_needs_a_base_branch_to_land_on() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        db.execute(
            "UPDATE jobs SET base_branch = NULL WHERE id = 'job-clean'",
            (),
        )
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();

        let error =
            request_branch_replay(&orch, &db, "job-clean", "agent/clean", None, false, None)
                .await
                .expect_err("a branch with no base has no destination");
        assert!(
            error.contains("no base branch recorded"),
            "the refusal names the missing base rather than a resolution failure: {error}"
        );
    }

    /// Read one text column keyed by id.
    async fn text_field(db: &LocalDb, sql: &'static str, id: &str) -> Option<String> {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn.query(sql, params![id]).await?;
                match rows.next().await? {
                    Some(row) => row.opt_text(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .unwrap()
    }

    const JOB_BASE: &str = "SELECT base_branch FROM jobs WHERE id = ?1";
    const MR_TARGET: &str = "SELECT target_branch FROM merge_requests WHERE id = ?1";

    #[tokio::test]
    async fn failed_publication_invalidates_the_open_pull_requests_cached_verdict() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-2', 'default', 'Other', 'other', '/other', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-other', 'proj-2', 1, 'Other', 'active', 1, 1);
             INSERT INTO jobs (id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
               VALUES ('job-other', 'issue-other', 'proj-2', 'running', 'agent/clean', 'main', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch,
                status, github_state, github_mergeable, github_fetched_at, opened_at, updated_at)
             VALUES
               ('mr-open', 'job-clean', 'proj-1', 'issue-3', 'Open',
                'agent/clean', 'main', 'open', 'OPEN', 'MERGEABLE', 100, 1, 1),
               ('mr-github-resolved', 'job-overlap', 'proj-1', 'issue-2', 'Resolved remotely',
                'agent/clean', 'main', 'open', 'MERGED', 'MERGEABLE', 100, 1, 1),
               ('mr-other-project', 'job-other', 'proj-2', 'issue-other', 'Other project',
                'agent/clean', 'main', 'open', 'OPEN', 'MERGEABLE', 100, 1, 1);",
        )
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();

        mark_publication_unconfirmed(&orch, &db, "proj-1", "agent/clean")
            .await
            .unwrap();

        let open: (String, Option<i64>) = db
            .query_one(
                "SELECT github_mergeable, github_fetched_at FROM merge_requests WHERE id = 'mr-open'",
                (),
                |row| Ok((row.text(0)?, row.opt_i64(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(open, ("UNKNOWN".to_string(), None));
        for (id, reason) in [
            (
                "mr-github-resolved",
                "GitHub-resolved pull requests are historical records",
            ),
            (
                "mr-other-project",
                "the same branch name in another project is unrelated",
            ),
        ] {
            assert_eq!(
                text_field(
                    &db,
                    "SELECT github_mergeable FROM merge_requests WHERE id = ?1",
                    id,
                )
                .await
                .as_deref(),
                Some("MERGEABLE"),
                "{reason} and must not be rewritten"
            );
        }
    }

    /// A child issue's branch is cut from its parent's, and the parent's merge
    /// deletes that branch. Re-pointing is what keeps the child placeable
    /// afterwards, so it has to reach BOTH records that hold the dead name and
    /// leave the child in the same pass's sibling set — which is how it inherits
    /// the advance instead of being stranded by it.
    #[tokio::test]
    async fn merging_a_parent_re_points_the_work_cut_from_its_branch() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
              VALUES ('issue-child', 'proj-1', 5, 'Child', 'active', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES ('exec-5', 'recipe-default', 'issue-child', 'proj-1', 'running', 1, 1);
            INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
              VALUES ('job-child', 'exec-5', 'node', 'issue-child', 'proj-1', 'running', 'agent/child', 'agent/merged', 1, 1);
            INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
              VALUES ('mr-child', 'job-child', 'proj-1', 'issue-child', 'PR', 'agent/child', 'agent/merged', 'open', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
              VALUES ('issue-done', 'proj-1', 6, 'Done', 'merged', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES ('exec-6', 'recipe-default', 'issue-done', 'proj-1', 'complete', 1, 1);
            INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
              VALUES ('job-done', 'exec-6', 'node', 'issue-done', 'proj-1', 'complete', 'agent/done', 'agent/merged', 1, 1);
            ",
        )
        .await
        .unwrap();

        repoint_children_of_merged_branch(&db, "proj-1", "agent/merged", "integration")
            .await
            .unwrap();

        assert_eq!(
            text_field(&db, JOB_BASE, "job-child").await.as_deref(),
            Some("integration"),
            "the child is cut from what its parent merged into"
        );
        assert_eq!(
            text_field(&db, MR_TARGET, "mr-child").await.as_deref(),
            Some("integration"),
            "the open pull request follows, so nothing later tries to merge into a deleted branch"
        );
        assert_eq!(
            text_field(&db, JOB_BASE, "job-done").await.as_deref(),
            Some("agent/merged"),
            "work that resolved alongside its parent keeps the record of where it was cut from"
        );

        let siblings = load_sibling_jobs(&db, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        assert!(
            siblings.iter().any(|sibling| sibling.id == "job-child"),
            "the re-pointed child joins this same advance rather than waiting for the next one"
        );
    }

    /// End to end over a real store: a branch whose recorded base was deleted
    /// when its parent merged asks for a replay and gets an answer — the base
    /// it is actually measured against, and a corrected record — instead of the
    /// resolution error that used to be the end of the road.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_replay_after_its_parent_merged_lands_on_the_surviving_base() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            eprintln!(
                "skipping a_replay_after_its_parent_merged_lands_on_the_surviving_base: no jj"
            );
            return;
        };
        let home = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        crate::jj::tests::init_project(project.path());
        crate::jj::tests::git(project.path(), &["checkout", "-q", "-b", "agent/child"]);
        std::fs::write(project.path().join("child.rs"), "child\n").unwrap();
        crate::jj::tests::git(project.path(), &["add", "-A"]);
        crate::jj::tests::git(project.path(), &["commit", "-q", "-m", "child"]);
        crate::jj::tests::git(project.path(), &["checkout", "-q", "main"]);

        let db = Arc::new(migrated_db().await);
        db.execute_script(&format!(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
              VALUES ('proj-1', 'default', 'Project', 'proj', '{repo}', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
              VALUES ('issue-child', 'proj-1', 1, 'Child', 'active', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, base_branch, created_at, updated_at)
              VALUES ('job-child', 'proj-1', 'issue-child', 'running', 'agent/child', 'agent/deleted-parent', 1, 1);
            ",
            repo = project.path().display()
        ))
        .await
        .unwrap();

        let root = home.path().to_path_buf();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root.clone(),
        )
        .jj_binary_path(bin.clone())
        .build();
        let jj = crate::jj::JjEnv::resolve(&bin, &root);
        let store = crate::jj::project_store_dir(&root, project.path());
        crate::jj::ensure_project_store(&jj, &store, project.path()).unwrap();

        let summary =
            request_branch_replay(&orch, &db, "job-child", "agent/child", None, false, None)
                .await
                .expect("a deleted base is not a dead end");

        assert!(
            summary.contains("agent/deleted-parent")
                && summary.contains("no longer exists")
                && summary.contains("`main`"),
            "the answer names the base that vanished and the one it is measured against: {summary}"
        );
        assert_eq!(
            text_field(&db, JOB_BASE, "job-child").await.as_deref(),
            Some("main"),
            "the correction is durable, so the next surface to ask gets the same answer"
        );
    }

    #[tokio::test]
    async fn reconcile_sweep_bounds_requeued_work_to_one_claim_per_pass() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let claim =
            claim_reconcile_intent(&db, "/repo", Path::new("/store"), "main", "dest-a", "test")
                .await
                .unwrap()
                .unwrap();
        let mut claimed = HashSet::new();

        assert!(first_claim_this_sweep(&mut claimed, &claim.id));
        release_reconcile_claim(&db, &claim).await;
        let retried = claim_next_reconcile_intent(&db)
            .await
            .unwrap()
            .expect("released work remains retryable");
        assert_eq!(retried.claim.id, claim.id);
        assert_ne!(retried.claim.owner, claim.owner);
        assert!(
            !first_claim_this_sweep(&mut claimed, &retried.claim.id),
            "the retry is deferred to the next sweep instead of spinning in this one"
        );
        release_reconcile_claim(&db, &retried.claim).await;
        assert!(
            claim_next_reconcile_intent(&db).await.unwrap().is_some(),
            "bounding the pass must not consume the retry"
        );
    }

    #[tokio::test]
    async fn durable_retry_supersedes_a_fossilized_pin_and_remints_current_destination() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let stale = claim_reconcile_intent(
            &db,
            "/repo",
            store,
            "main",
            "destination-a",
            "pre-sweep trigger",
        )
        .await
        .unwrap()
        .unwrap();

        let current = refresh_stale_durable_intent(
            &db,
            "/repo",
            store,
            "main",
            "destination-a",
            "destination-b",
            "durable destination refresh",
            stale,
        )
        .await
        .unwrap()
        .expect("the stale intent is reminted against the live destination");

        let stale_state: (String, String) = db
            .query_one(
                "SELECT status, last_diagnostic FROM jj_reconcile_intents
                 WHERE destination_commit = 'destination-a'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(stale_state.0, "superseded");
        assert!(stale_state.1.contains("destination-b"));
        assert_eq!(
            db.query_text(
                "SELECT destination_commit FROM jj_reconcile_intents WHERE id = ?1",
                (current.id.clone(),),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("destination-b")
        );

        release_reminted_claim_after_failure(&db, "fossil-intent", &current).await;
        assert_eq!(
            db.query_text(
                "SELECT status FROM jj_reconcile_intents WHERE id = ?1",
                (current.id.clone(),),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("pending"),
            "a failed execution releases the reminted destination immediately"
        );
        let retried = claim_next_reconcile_intent(&db)
            .await
            .unwrap()
            .expect("the replacement is retryable without waiting for lease expiry");
        finish_reconcile_intent(&db, &retried.claim.id, &retried.claim.owner, false)
            .await
            .unwrap();
        assert!(
            claim_next_reconcile_intent(&db).await.unwrap().is_none(),
            "one sweep can leave both the fossil and its replacement terminal"
        );
    }

    #[tokio::test]
    async fn run_batch_deferral_is_scoped_to_the_owning_branch() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        for sql in [
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-local','job-overlap','open','backend',1,1)",
            "UPDATE runs SET session_id='session-local' WHERE id='run-job-overlap'",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-local','session-local','run-job-overlap','job-overlap',1,'yielded','initial',1,1)",
            "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-local','job-overlap','run-job-overlap','session-local','turn-local','tool-local','{\"kind\":\"run_batch\",\"request_id\":\"request-local\",\"commits\":false,\"label\":\"branch-local test\"}','pending',1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        let siblings = vec![
            SiblingJob {
                id: "job-overlap".into(),
                branch: Some("agent/overlap".into()),
                base_commit: None,
            },
            SiblingJob {
                id: "job-clean".into(),
                branch: Some("agent/clean".into()),
                base_commit: None,
            },
        ];
        let blocked = siblings_for_branch(&siblings, "agent/overlap");
        let clear = siblings_for_branch(&siblings, "agent/clean");

        assert_eq!(
            blocked.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["job-overlap"]
        );
        assert_eq!(
            clear.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["job-clean"]
        );
        assert!(jobs_have_inflight_run_batches(&db, &blocked).await.unwrap());
        assert!(
            !jobs_have_inflight_run_batches(&db, &clear).await.unwrap(),
            "a run batch on one branch must not defer its clear sibling"
        );
    }

    /// The diagnostic exists only inside the rebase, between the recorded
    /// conflict and the rollback. If it does not survive that window in storage,
    /// nothing downstream can ever show it: the branch is clean again and a later
    /// probe finds nothing. So the round-trip is the whole feature.
    #[tokio::test]
    async fn a_conflict_diagnostic_outlives_the_rollback_that_erased_it() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let claim = claim_reconcile_intent(&db, "/repo", store, "main", "dest-a", "local_merge")
            .await
            .unwrap()
            .unwrap();
        let branch = "agent/test";
        persist_reconcile_item(
            &db,
            ReconcileItemUpdate {
                intent_id: &claim.id,
                bookmark: branch,
                observed_tip: Some("tip"),
                status: "graph_moved",
                failure_kind: None,
                outcome_kind: Some("conflicted"),
                fingerprint: None,
                diagnostic: None,
            },
        )
        .await
        .unwrap();

        let diagnostic = crate::jj::ConflictDiagnostic {
            base: Some("b".repeat(40)),
            ours: Some("o".repeat(40)),
            theirs: Some("t".repeat(40)),
            conflicted_tip: Some("c".repeat(40)),
            condition: crate::jj::ConflictCondition::ContentConflict,
            incoming: vec![
                crate::jj::IncomingFile {
                    path: "shared.rs".to_string(),
                    status: "M".to_string(),
                    classification: crate::jj::IncomingClassification::Conflicting,
                },
                crate::jj::IncomingFile {
                    path: "storage.rs".to_string(),
                    status: "D".to_string(),
                    classification: crate::jj::IncomingClassification::CleanOnRetry,
                },
            ],
        };
        let incoming = IncomingIdentity {
            base_branch: "main".to_string(),
            pr_number: Some(2893),
            issue: Some("cairn-3337".to_string()),
        };
        record_conflict_session(&db, &claim.id, branch, &diagnostic, &incoming)
            .await
            .unwrap();

        let session = crate::orchestrator::conflict_session::load_active_session(&db, branch)
            .await
            .unwrap()
            .expect("the session survives the pass that recorded it");

        assert_eq!(session.base.as_deref(), Some("b".repeat(40).as_str()));
        assert_eq!(session.theirs.as_deref(), Some("t".repeat(40).as_str()));
        assert_eq!(
            session.fingerprint(),
            diagnostic.fingerprint(),
            "the stored fingerprint is the one the wake deduplicated on"
        );
        assert_eq!(session.incoming.pr_number, Some(2893));
        assert_eq!(session.incoming.issue.as_deref(), Some("cairn-3337"));

        // The cross-file trap: a report naming only the conflicting path lets a
        // coordinated change land half-applied, so the clean-on-retry sibling
        // must be stored too, distinguishably.
        let conflicting: Vec<&str> = session.conflicting().map(|f| f.path.as_str()).collect();
        let clean: Vec<&str> = session.clean_on_retry().map(|f| f.path.as_str()).collect();
        assert_eq!(conflicting, vec!["shared.rs"]);
        assert_eq!(clean, vec!["storage.rs"]);
        assert_eq!(
            session
                .files
                .iter()
                .find(|f| f.path == "storage.rs")
                .unwrap()
                .status,
            "D",
            "an add/delete/rename is distinguishable, not flattened to 'changed'"
        );

        // Markers start unclaimed. Nothing may say otherwise until an executor does.
        assert_eq!(session.marker_state, MarkerState::NotMaterialized);
    }

    /// A base that advances twice leaves the first session describing a merge
    /// nobody will perform again. Exactly one session is active, and closing on a
    /// clean rebase is what ends it.
    #[tokio::test]
    async fn a_newer_session_supersedes_the_old_and_a_clean_rebase_closes_it() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let branch = "agent/test";

        let mut fingerprints = Vec::new();
        for dest in ["dest-a", "dest-b"] {
            let claim = claim_reconcile_intent(&db, "/repo", store, "main", dest, "local_merge")
                .await
                .unwrap()
                .unwrap();
            persist_reconcile_item(
                &db,
                ReconcileItemUpdate {
                    intent_id: &claim.id,
                    bookmark: branch,
                    observed_tip: Some("tip"),
                    status: "graph_moved",
                    failure_kind: None,
                    outcome_kind: Some("conflicted"),
                    fingerprint: None,
                    diagnostic: None,
                },
            )
            .await
            .unwrap();
            let diagnostic = crate::jj::ConflictDiagnostic {
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some(dest.to_string()),
                conflicted_tip: None,
                condition: crate::jj::ConflictCondition::ContentConflict,
                incoming: Vec::new(),
            };
            fingerprints.push(diagnostic.fingerprint());
            record_conflict_session(
                &db,
                &claim.id,
                branch,
                &diagnostic,
                &IncomingIdentity::default(),
            )
            .await
            .unwrap();
            supersede_stale_sessions(&db, branch, &claim.id)
                .await
                .unwrap();
            // Mark it claimed so the next destination is not coalesced away.
            db.execute(
                "UPDATE jj_reconcile_intents SET status = 'completed' WHERE id = ?1",
                (claim.id.as_str(),),
            )
            .await
            .unwrap();
        }

        let session = crate::orchestrator::conflict_session::load_active_session(&db, branch)
            .await
            .unwrap()
            .expect("one session is active");
        assert_eq!(
            session.fingerprint(),
            fingerprints[1],
            "the active session describes the CURRENT merge, not the superseded one"
        );

        close_open_sessions_for_branch(&db, branch).await.unwrap();
        assert!(
            crate::orchestrator::conflict_session::load_active_session(&db, branch)
                .await
                .unwrap()
                .is_none(),
            "absorbing the base closes the session"
        );
    }

    #[tokio::test]
    async fn replay_waits_for_resolved_intent_then_claims_follow_on_pass() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let first = claim_reconcile_intent(
            &db,
            "/repo",
            store,
            "main",
            "dest-a",
            "resolved replay requested for agent/first",
        )
        .await
        .unwrap()
        .expect("the first resolved candidate list owns the intent");

        assert!(
            claim_reconcile_intent(
                &db,
                "/repo",
                store,
                "main",
                "dest-a",
                "resolved replay requested for agent/second",
            )
            .await
            .unwrap()
            .is_none(),
            "the second request initially encounters the live frozen intent"
        );

        let waiting_db = db.clone();
        let waiter = tokio::spawn(async move {
            wait_for_reconcile_slot(
                &waiting_db,
                Path::new("/store"),
                "main",
                "dest-a",
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the second request waits for the first pass"
        );

        finish_reconcile_intent(&db, &first.id, &first.owner, false)
            .await
            .unwrap();
        waiter.await.unwrap().unwrap();
        reopen_reconcile_intent(&db, store, "main", "dest-a", "agent/second")
            .await
            .unwrap();
        let second = claim_reconcile_intent(
            &db,
            "/repo",
            store,
            "main",
            "dest-a",
            "resolved replay requested for agent/second",
        )
        .await
        .unwrap()
        .expect("the second request claims a follow-on pass instead of disappearing");
        assert_ne!(second.owner, first.owner);

        // If the base advances between attempts, the failed claim reports the
        // newly pinned destination. Waiting on that coordinate must not be
        // satisfied by the older pass releasing its different destination.
        let moved = claim_reconcile_intent(
            &db,
            "/repo",
            store,
            "main",
            "dest-b",
            "resolved replay requested for agent/other",
        )
        .await
        .unwrap()
        .expect("the moved base owns a distinct intent");
        let moved_waiting_db = Arc::clone(&db);
        let moved_waiter = tokio::spawn(async move {
            wait_for_reconcile_slot(
                &moved_waiting_db,
                Path::new("/store"),
                "main",
                "dest-b",
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
        });
        tokio::task::yield_now().await;
        finish_reconcile_intent(&db, &second.id, &second.owner, false)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert!(
            !moved_waiter.is_finished(),
            "releasing the stale destination does not satisfy the moved-base waiter"
        );
        finish_reconcile_intent(&db, &moved.id, &moved.owner, false)
            .await
            .unwrap();
        moved_waiter.await.unwrap().unwrap();
        reopen_reconcile_intent(&db, store, "main", "dest-b", "agent/second")
            .await
            .unwrap();
        let moved_follow_on = claim_reconcile_intent(
            &db,
            "/repo",
            store,
            "main",
            "dest-b",
            "resolved replay requested for agent/second",
        )
        .await
        .unwrap()
        .expect("the moved destination is reopened and claimed after its owner finishes");
        assert_ne!(moved_follow_on.owner, moved.owner);
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
                     VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1)",
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
                for (job, exec, issue, status, branch) in [
                    ("job-merged", "exec-1", "issue-1", "complete", "agent/merged"),
                    ("job-overlap", "exec-2", "issue-2", "running", "agent/overlap"),
                    ("job-clean", "exec-3", "issue-3", "running", "agent/clean"),
                    ("job-complete", "exec-4", "issue-4", "complete", "agent/complete"),
                ] {
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
                         VALUES (?1, ?2, 'node', ?3, 'proj-1', ?4, ?5, 'integration', 1, 1)",
                        params![job, exec, issue, status, branch],
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
            branch: Some("agent/test".to_string()),
            base_commit: None,
        }];
        let failures = vec![crate::jj::ReconcileFailure {
            branch: "agent/test".to_string(),
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

    /// The durable base record this reconcile leaves on a job.
    async fn recorded_durable_base(db: &LocalDb, job_id: &str) -> Option<String> {
        db.query_opt_text(
            "SELECT base_commit FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
        )
        .await
        .unwrap()
    }

    /// A concurrent reconcile that wrote the same value is agreement.
    ///
    /// `load_sibling_jobs` snapshots every sibling's coordinate once at the top
    /// of a reconcile, so a later branch routinely finds a row another reconcile
    /// has already advanced to exactly this target. The row holds what the call
    /// wanted to put there; calling that a failure is what turned zero-delta
    /// planner branches into ⛔ BLOCKING directs that resumed deliberately
    /// parked sessions (CAIRN-3094 comment #4).
    #[tokio::test]
    async fn a_durable_base_already_at_the_target_is_agreement() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute(
            "UPDATE jobs SET base_commit = 'new-tip' WHERE id = 'job-overlap'",
            (),
        )
        .await
        .unwrap();
        let sibling = SiblingJob {
            id: "job-overlap".to_string(),
            branch: Some("agent/overlap".to_string()),
            base_commit: Some("stale-snapshot".to_string()),
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("a row already holding the target is not a lost race");

        assert_eq!(
            recorded_durable_base(&db, "job-overlap").await.as_deref(),
            Some("new-tip")
        );
    }

    /// A snapshot that lost to a writer heading somewhere ELSE still lands on
    /// this reconcile's target.
    ///
    /// This is the assertion the retired compare-and-swap also made, and making
    /// it identically against a plain write is the evidence that the CAS never
    /// ordered anything (CAIRN-3226): its retry swapped against the freshly read
    /// value and wrote this call's target regardless of what it found.
    #[tokio::test]
    async fn a_stale_snapshot_still_records_this_reconciles_target() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute(
            "UPDATE jobs SET base_commit = 'somebody-elses-tip' WHERE id = 'job-overlap'",
            (),
        )
        .await
        .unwrap();
        let sibling = SiblingJob {
            id: "job-overlap".to_string(),
            branch: Some("agent/overlap".to_string()),
            base_commit: Some("stale-snapshot".to_string()),
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("a stale snapshot does not block the record");

        assert_eq!(
            recorded_durable_base(&db, "job-overlap").await.as_deref(),
            Some("new-tip")
        );
    }

    /// A job with no recorded base is recorded, not refused.
    ///
    /// The retired CAS needed a non-null expected value and aborted with a
    /// manufactured error without one, so a job whose bookkeeping had never been
    /// written could never acquire it — substrate state failing a step that has
    /// nothing to fail about.
    #[tokio::test]
    async fn a_job_with_no_recorded_base_is_recorded_not_refused() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute(
            "UPDATE jobs SET base_commit = NULL WHERE id = 'job-overlap'",
            (),
        )
        .await
        .unwrap();
        let sibling = SiblingJob {
            id: "job-overlap".to_string(),
            branch: Some("agent/overlap".to_string()),
            base_commit: None,
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("an unrecorded base is not a reason to refuse the record");

        assert_eq!(
            recorded_durable_base(&db, "job-overlap").await.as_deref(),
            Some("new-tip")
        );
    }

    /// A job deleted mid-reconcile is the graph moving on, not a failure of this
    /// bookkeeping.
    #[tokio::test]
    async fn a_vanished_job_does_not_fail_the_record() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let sibling = SiblingJob {
            id: "job-that-was-deleted".to_string(),
            branch: Some("agent/gone".to_string()),
            base_commit: Some("stale-snapshot".to_string()),
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("a missing job row is logged, not raised");
    }

    /// Runner-internal bookkeeping never addresses an agent, and the control
    /// half proves the filter is narrow: a genuine reconcile failure still
    /// steers the branch's job.
    #[tokio::test]
    async fn a_persistence_failure_is_withheld_while_a_real_one_still_steers() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let siblings = vec![SiblingJob {
            id: "job-overlap".to_string(),
            branch: Some("agent/overlap".to_string()),
            base_commit: None,
        }];
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let enqueue = |_: &Orchestrator, _: &str, _: &str, _: DeliveryUrgency, _: &str| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(DirectQueueDisposition::QueuedOrPresent)
        };

        let internal = vec![crate::jj::ReconcileFailure {
            branch: "agent/overlap".to_string(),
            error: "durable base advancement failed: durable base coordinate for job \
                    job-overlap changed concurrently twice (last observed abc); refused to \
                    overwrite it"
                .to_string(),
        }];
        let notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &internal,
            "test",
            "intent-1",
            enqueue,
        )
        .unwrap();
        assert!(
            notified.is_empty(),
            "an unpersisted coordinate is not something an agent can act on"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "and nothing may even be enqueued for it"
        );

        let genuine = vec![crate::jj::ReconcileFailure {
            branch: "agent/overlap".to_string(),
            error: "origin push failed: connection reset".to_string(),
        }];
        let notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &genuine,
            "test",
            "intent-2",
            enqueue,
        )
        .unwrap();
        assert_eq!(
            notified,
            vec!["agent/overlap".to_string()],
            "a branch that genuinely cannot move still wakes its agent"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
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
        let branch = "agent/proj-integration";
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
            "each on-branch job receives its own direct"
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
            error: error.to_string(),
        };
        assert!(!reconcile_has_transient_failures(&[failure(
            "commit is immutable"
        )]));
        // A conflicted bookmark is now TRANSIENT, and deliberately so: it is a
        // reconciliation TODO the next base advance repairs, not a terminal
        // state. Holding it as permanent meant it was never retried and never
        // repaired, which is the agent-unreachable dead end this arc forbids.
        assert!(reconcile_has_transient_failures(&[failure(
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
                branch, base_branch, created_at, updated_at)
             VALUES
               ('job-no-issue', NULL, 'node', NULL, 'proj-1', 'running',
                'agent/proj-null-builder-0', 'integration', 1, 1);",
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
            "UPDATE jobs SET branch = 'agent/proj-2-builder-0' WHERE id = 'job-overlap';
             INSERT INTO job_terminals
                 (id, job_id, session_id, command, status, created_at, slug,
                  residency_holder, residency_incarnation_id, cell_epoch)
             VALUES
                 ('live-terminal', 'job-overlap', 'live-session', 'true', 'running', 1, 'live', 'live-lease', 'live-inc', 7),
                 ('exited-terminal', 'job-overlap', 'exited-session', 'true', 'exited', 1, 'exited', 'exited-lease', 'exited-inc', 8);",
        )
        .await
        .unwrap();

        let leases = load_live_terminal_leases(&db, "proj-1", "agent/proj-2-builder-0")
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
            "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
             VALUES (?1, ?2, 'node', ?3, ?4, 'running', 'agent/TEAM-9-builder-0', 'integration', 'builder', 1, 1)",
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

    /// The two conditions must give OPPOSITE advice, and the base-drift branch
    /// must not send an agent to edit files that already agree — the exact round
    /// CAIRN-3327 and CAIRN-3328 each burned.
    #[test]
    fn base_drift_and_content_conflict_give_opposite_guidance() {
        let note = build_jj_conflict_note("main", Some(42), None);
        let incoming = vec![crate::jj::IncomingFile {
            path: "shared.rs".to_string(),
            status: "M".to_string(),
            classification: crate::jj::IncomingClassification::Conflicting,
        }];

        let drift = append_conflict_diagnostic(
            &note,
            Some(&crate::jj::ConflictDiagnostic {
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some("t".repeat(40)),
                conflicted_tip: None,
                condition: crate::jj::ConflictCondition::BaseDrift,
                incoming: incoming.clone(),
            }),
            MarkerState::NotMaterialized,
        );
        assert!(drift.contains("BASE DRIFT"), "{drift}");
        assert!(
            drift.contains("nothing to merge"),
            "drift must say the content work may already be done: {drift}"
        );
        assert!(
            drift.contains("downstream exports"),
            "drift must explain why a local ref move cannot stick: {drift}"
        );
        assert!(
            !drift.contains("write the merged result"),
            "drift must NOT ask for content edits: {drift}"
        );

        let content = append_conflict_diagnostic(
            &note,
            Some(&crate::jj::ConflictDiagnostic {
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some("t".repeat(40)),
                conflicted_tip: None,
                condition: crate::jj::ConflictCondition::ContentConflict,
                incoming,
            }),
            MarkerState::NotMaterialized,
        );
        assert!(content.contains("CONTENT CONFLICT"), "{content}");
        assert!(content.contains("write the merged result"), "{content}");
        assert!(
            !content.contains("BASE DRIFT"),
            "the two conditions never both appear: {content}"
        );

        // Neither may ever authorize the one action that cannot be undone, and
        // each must name the route it DOES sanction — the drift branch by handing
        // over the replay request verbatim, the content branch by forbidding a
        // hand rebase while pointing at the session.
        for message in [&drift, &content] {
            assert!(
                message.contains("cairn:~/rebase"),
                "each condition routes to the session: {message}"
            );
            assert!(!message.contains("git rebase"), "{message}");
        }
        assert!(
            drift.contains("action:\"replay\""),
            "drift hands over the exact request rather than describing it: {drift}"
        );
        assert!(
            content.contains("force-push"),
            "the content branch still forbids hand-rebasing: {content}"
        );
    }

    /// The standing rule: machinery never instructs an agent to act on state it
    /// has not made true. Only a CONFIRMED materialization may put "resolve the
    /// markers" in front of an agent; every other state has to say plainly that
    /// there is nothing on disk to resolve, and point at the session instead.
    #[test]
    fn only_a_confirmed_materialization_tells_the_agent_to_resolve_markers() {
        let note = build_jj_conflict_note("main", Some(42), None);
        let diagnostic = crate::jj::ConflictDiagnostic {
            base: Some("b".repeat(40)),
            ours: Some("o".repeat(40)),
            theirs: Some("t".repeat(40)),
            conflicted_tip: None,
            condition: crate::jj::ConflictCondition::ContentConflict,
            incoming: vec![crate::jj::IncomingFile {
                path: "shared.rs".to_string(),
                status: "M".to_string(),
                classification: crate::jj::IncomingClassification::Conflicting,
            }],
        };

        let confirmed =
            append_conflict_diagnostic(&note, Some(&diagnostic), MarkerState::Materialized);
        assert!(
            confirmed.contains("Conflict markers have been written into your checkout"),
            "{confirmed}"
        );

        for state in [
            MarkerState::NotMaterialized,
            MarkerState::Pending,
            MarkerState::Failed,
        ] {
            let message = append_conflict_diagnostic(&note, Some(&diagnostic), state);
            assert!(
                !message.contains("Conflict markers have been written"),
                "{state:?} must not claim markers exist: {message}"
            );
            assert!(
                message.contains("cairn:~/rebase"),
                "{state:?} must route the agent to both sides of the merge: {message}"
            );
        }

        // Pending is specifically NOT the same as absent: an agent told markers
        // are absent would stop looking, while the retry may still land them.
        let pending = append_conflict_diagnostic(&note, Some(&diagnostic), MarkerState::Pending);
        assert!(pending.contains("NOT confirmed present"), "{pending}");
        let absent =
            append_conflict_diagnostic(&note, Some(&diagnostic), MarkerState::NotMaterialized);
        assert!(
            absent.contains("no conflict markers in your checkout"),
            "{absent}"
        );

        // Under base drift the two sides already agree, so no marker state may
        // ever produce "write the merged result" — including the confirmed one,
        // where any marker on disk is scaffolding rather than work to do.
        let drift = crate::jj::ConflictDiagnostic {
            condition: crate::jj::ConflictCondition::BaseDrift,
            ..diagnostic.clone()
        };
        for state in [
            MarkerState::NotMaterialized,
            MarkerState::Pending,
            MarkerState::Materialized,
            MarkerState::Failed,
        ] {
            let message = append_conflict_diagnostic(&note, Some(&drift), state);
            assert!(
                !message.contains("write the merged result"),
                "{state:?} must not send a drifted branch to edit agreeing files: {message}"
            );
        }
    }

    /// A wake that carries no diagnostic (nothing enumerated) degrades to the
    /// identity half rather than rendering an empty file table.
    #[test]
    fn a_wake_without_a_diagnostic_is_left_unchanged() {
        let note = build_jj_conflict_note("main", Some(42), None);
        assert_eq!(
            append_conflict_diagnostic(&note, None, MarkerState::NotMaterialized),
            note
        );
    }

    #[test]
    fn jj_conflict_note_carries_no_rebase_commands() {
        let issue = IssueInfo {
            project_key: "proj".to_string(),
            number: 7,
        };
        let note = build_jj_conflict_note("agent/cairn-1940-coordinator-0", Some(42), Some(&issue));
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("PR #42 merged"));
        assert!(note.contains("cairn://p/proj/7"));
        // Stop-the-line: the note names the conflict as blocking, not optional.
        assert!(note.contains("BLOCKING"));
        // The rebase was ROLLED BACK, and saying so is what makes the note
        // actionable — the agent is being asked to merge into an untouched
        // branch, not to repair a rewritten one.
        assert!(note.contains("rolled back"), "{note}");
        assert!(note.contains("untouched"), "{note}");
        // Agents hold detached git worktrees, so conflict markers are never
        // materialized anywhere. A note that points at them is unanswerable.
        assert!(!note.to_lowercase().contains("marker"), "{note}");
        assert!(!note.contains("resolve them"), "{note}");
        // The note must not instruct a manual rebase/force-push.
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
    }

    #[test]
    fn jj_clean_note_describes_clean_rebase_with_no_action() {
        let issue = IssueInfo {
            project_key: "proj".to_string(),
            number: 7,
        };
        let note = build_jj_clean_note("agent/cairn-1940-coordinator-0", Some(42), Some(&issue));
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("agent/cairn-1940-coordinator-0"));
        assert!(note.contains("PR #42 merged"));
        assert!(note.contains("cairn://p/proj/7"));
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
            "UPDATE jobs SET branch = 'agent/proj-4-builder-0' WHERE id = 'job-complete';
             INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-complete', 'job-complete', 'proj-1', 'issue-4', 'PR', 'agent/proj-4-builder-0', 'integration', 'open', 1, 1);",
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
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('job-coord', 'exec-coord', 'node', 'issue-coord', 'proj-1', 'running', 'integration', 'main', 1, 1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let on_branch = load_on_branch_jobs(&orch.db.local, "proj-1", "integration")
            .await
            .unwrap();
        let on_ids: std::collections::HashSet<&str> =
            on_branch.iter().map(|s| s.id.as_str()).collect();
        assert!(
            on_ids.contains("job-coord"),
            "the job ON the integration branch (the coordinator) is selected"
        );
        assert_eq!(
            on_ids.len(),
            1,
            "only the on-branch job; children branched FROM it are excluded"
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
        // Stop-the-line: the note names the conflict as blocking, not optional.
        assert!(note.contains("BLOCKING"));
        // Same contract as the Cairn-owned advance: rolled back, untouched, and
        // no markers to point at.
        assert!(note.contains("rolled back"), "{note}");
        assert!(note.contains("untouched"), "{note}");
        assert!(!note.to_lowercase().contains("marker"), "{note}");
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
            branch: Some("agent/proj-2-builder-0".to_string()),
            base_commit: None,
        }];
        let failed = vec![crate::jj::ReconcileFailure {
            branch: "agent/proj-2-builder-0".to_string(),
            error: "jj bookmark rebase failed: exact stderr".to_string(),
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
        // The agent is told its work is safe and what not to do; the exact
        // reconciliation stderr rides along for the human who repairs it.
        assert!(content.contains("Your commits are intact"));
        assert!(content.contains("jj bookmark rebase failed: exact stderr"));
        assert!(!content.to_lowercase().contains("retry"));
        assert!(content.to_lowercase().contains("do not force-push"));
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
                branch: Some("agent/proj-2-builder-0".to_string()),
                base_commit: None,
            },
            SiblingJob {
                id: "job-clean".to_string(),
                branch: Some("agent/proj-3-builder-0".to_string()),
                base_commit: None,
            },
        ];
        let conflicted = vec!["agent/proj-2-builder-0".to_string()];
        let note = build_jj_conflict_note("integration", Some(42), None);
        // The cross-file trap this diagnostic exists for: the incoming change
        // conflicts on two files and ALSO carries a third that arrives cleanly.
        // An agent told only about the first two resolves them, stops compiling,
        // and has no idea why (CAIRN-3337).
        let mut diagnostics: HashMap<String, crate::jj::ConflictDiagnostic> = HashMap::new();
        diagnostics.insert(
            "agent/proj-2-builder-0".to_string(),
            crate::jj::ConflictDiagnostic {
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some("t".repeat(40)),
                conflicted_tip: None,
                condition: crate::jj::ConflictCondition::ContentConflict,
                incoming: vec![
                    crate::jj::IncomingFile {
                        path: "shared.rs".to_string(),
                        status: "M".to_string(),
                        classification: crate::jj::IncomingClassification::Conflicting,
                    },
                    crate::jj::IncomingFile {
                        path: "lib.rs".to_string(),
                        status: "M".to_string(),
                        classification: crate::jj::IncomingClassification::Conflicting,
                    },
                    crate::jj::IncomingFile {
                        path: "storage.rs".to_string(),
                        status: "M".to_string(),
                        classification: crate::jj::IncomingClassification::CleanOnRetry,
                    },
                ],
            },
        );

        notify_conflicted_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &conflicted,
            &note,
            &ConflictEvidence {
                diagnostics,
                ..ConflictEvidence::default()
            },
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
        let wake = &messages[0].1;
        assert!(
            wake.contains("Conflicting files, yours to merge (2): shared.rs, lib.rs"),
            "the note names the conflicting files: {wake}"
        );
        // The load-bearing half: the sibling file that is NOT conflicting still
        // has to be named, or the agent resolves two files and ships a branch
        // that does not compile.
        assert!(
            wake.contains("cleanly on retry (1): storage.rs"),
            "the note names the incoming change's clean-on-retry siblings: {wake}"
        );
        assert!(
            wake.contains("CONTENT CONFLICT"),
            "the note names which condition this is: {wake}"
        );
        assert!(
            wake.contains(&"t".repeat(40)) && wake.contains(&"b".repeat(40)),
            "the immutable three-way coordinates ride out, so both sides are recomputable: {wake}"
        );
        // Markers were not confirmed for this branch, so the wake must say they
        // are absent rather than tell the agent to resolve state nothing made
        // true. It may still MENTION markers — saying "there are none" is the
        // honest thing — but never in the imperative.
        assert!(
            wake.contains("no conflict markers in your checkout"),
            "an unconfirmed materialization must say markers are absent: {wake}"
        );
        assert!(
            !wake.contains("Conflict markers have been written"),
            "the wake must never claim markers exist without confirmation: {wake}"
        );
        assert!(
            wake.contains("cairn:~/rebase"),
            "the wake links the session that carries both sides of the merge: {wake}"
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
            branch: Some("agent/TEAM-9-builder-0".to_string()),
            base_commit: None,
        }];
        let conflicted = vec!["agent/TEAM-9-builder-0".to_string()];
        let note = build_jj_conflict_note("integration", Some(42), None);
        let mut diagnostics: HashMap<String, crate::jj::ConflictDiagnostic> = HashMap::new();
        diagnostics.insert(
            "agent/TEAM-9-builder-0".to_string(),
            crate::jj::ConflictDiagnostic::from_paths(vec!["team.rs".to_string()]),
        );

        notify_conflicted_siblings(
            &orch,
            &team_db,
            &siblings,
            &conflicted,
            &note,
            &ConflictEvidence {
                diagnostics,
                ..ConflictEvidence::default()
            },
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
                branch: Some("agent/proj-2-builder-0".to_string()),
                base_commit: None,
            },
            SiblingJob {
                id: "job-clean".to_string(),
                branch: Some("agent/proj-3-builder-0".to_string()),
                base_commit: None,
            },
        ];
        let ambiguous = vec![AmbiguousDivergence {
            branch: "agent/proj-2-builder-0".to_string(),
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
            branch: Some("agent/proj-3-builder-0".to_string()),
            base_commit: None,
        }];
        let clean = vec!["agent/proj-3-builder-0".to_string()];
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

    /// THE REGRESSION. A merge lands on origin with NOTHING else in flight, which
    /// is the overwhelmingly common shape. Every base-advance path used to return
    /// early on `siblings.is_empty()` BEFORE it imported anything, so the store's
    /// default bookmark was never reconciled and was left holding a target that
    /// no longer agreed with origin — which the next operation to touch it
    /// reported as a conflicted name, killing every `main`-resolving verb.
    ///
    /// This asserts the store converges on origin with zero siblings. It fails on
    /// the code this change replaces, for the reason the whole slice exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn default_bookmark_reconciles_with_zero_in_flight_siblings() {
        let Some(_bin) = crate::jj::tests::jj_bin() else {
            eprintln!("skipping default_bookmark_reconciles_with_zero_siblings: jj not resolvable");
            return;
        };
        use crate::jj::tests::{git, git_stdout, init_project};

        let origin = tempfile::tempdir().unwrap();
        let proj = tempfile::tempdir().unwrap();
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);
        let repo_path = proj.path().to_string_lossy().into_owned();

        let db = migrated_db().await;
        let seed_repo = repo_path.clone();
        db.write(|conn| {
            let seed_repo = seed_repo.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-jj', 'default', 'Project', 'PJJ', ?1, 'main', 1, 1)",
                    params![seed_repo.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let mut git_client = MockGitClient::new();
        git_client.expect_fetch_origin().returning(|repo| {
            crate::env::git()
                .args(["fetch", "-q", "origin"])
                .current_dir(repo)
                .status()
                .map_err(|e| e.to_string())?;
            Ok(())
        });
        let orch = test_orchestrator(db, git_client);

        // The store is provisioned and tracking origin, exactly as a live project's is.
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
        crate::jj::ensure_project_store(&jj, &store, Path::new(&repo_path)).unwrap();

        // A PR merges on GitHub: origin's default branch advances onto a commit
        // the store has never seen. No sibling job exists anywhere.
        git(proj.path(), &["checkout", "-q", "--detach"]);
        std::fs::write(proj.path().join("merged.rs"), "landed\n").unwrap();
        git(proj.path(), &["add", "-A"]);
        git(proj.path(), &["commit", "-q", "-m", "squash merge (#1)"]);
        let landed = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
        git(proj.path(), &["push", "-q", "-f", "origin", "HEAD:main"]);

        reconcile_external_default_advance(&orch, "proj-jj", "main")
            .await
            .unwrap();

        assert_eq!(
            crate::jj::bookmark_commit(&jj, &store, "main").as_deref(),
            Some(landed.as_str()),
            "the store's default bookmark must equal origin after a merge, with no siblings in flight and no human in the loop"
        );
        assert_eq!(
            git_stdout(proj.path(), &["rev-parse", "refs/heads/main"]),
            landed,
            "the backing git ref must follow, or the next import re-conflicts the bookmark"
        );

        // A `?branch=main` read resolves the name and returns the merged content.
        // Resolving to the right commit id is not the same as being readable: the
        // conflicted-name state fails at resolution, so this is the assertion that
        // the branch is actually usable again.
        let content = crate::jj::file_show(&jj, &store, "main", "merged.rs")
            .expect("a read at `main` must resolve once the bookmark is reconciled");
        assert_eq!(String::from_utf8_lossy(&content), "landed\n");

        // And a fresh job provisions off it — the verb a conflicted `main` kills.
        let wts = tempfile::tempdir().unwrap();
        crate::jj::add_workspace(
            &jj,
            &store,
            &wts.path().join("job"),
            "agent/cairn-3192-builder-0",
            "main",
            None,
        )
        .expect("a fresh job must spawn off `main` with no human in the loop");
    }

    /// A branch that is NOT the project default is never reset onto origin: a
    /// Coordinator integration branch and an agent branch legitimately hold sealed
    /// work origin has not seen. The gate lives in the reconciler so no call site
    /// can bypass it — the GitHub-merge caller passes a PR's target branch and
    /// only asserted in a comment that it is always the default.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_non_default_branch_is_never_reconciled_onto_origin() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;

        assert!(branch_is_project_default(&db, "proj-1", "main").await);
        assert!(!branch_is_project_default(&db, "proj-1", "integration").await);
        assert!(!branch_is_project_default(&db, "proj-1", "agent/cairn-1-builder-0").await);
        assert!(
            !branch_is_project_default(&db, "no-such-project", "main").await,
            "an unreadable project must decline to reconcile rather than guess"
        );
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
