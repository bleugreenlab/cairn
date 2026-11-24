//! What every landing route shares about its source: carrying it onto the
//! target it lands on (`prepare_landing_source`) and the final check before it
//! publishes (`prepared_landing_holds`). Default landing candidates are computed
//! on an unpublished immutable index, never by moving and undoing live refs.

use crate::orchestrator::base_advance::preparation::{
    prepare_branch, PreparationPurpose, PreparationRefusal, PreparationRequest, PreparedBranch,
};
use crate::orchestrator::{JjStoreGuard, Orchestrator};
use std::path::Path;

use super::conflict::conflicted_history_detail;
use super::context::MergeMrContext;

#[cfg(test)]
mod tests;

/// Carry a landing's source onto the target it actually lands on, by the
/// canonical branch preparer, as its owner's merge. Every landing route starts
/// here: no landing carries a source onto its target by any other means, and
/// what it binds, validates and publishes is the returned coordinate.
///
/// `reservation` is the landing's own reservation of its source and target,
/// inherited by the preparation: it installs the source's advance under it
/// instead of reserving the source again, which from the landing's task
/// would be re-entrant.
pub(super) async fn prepare_landing_source(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: &MergeMrContext,
    reservation: &std::sync::Arc<crate::orchestrator::BranchReservation>,
) -> Result<PreparedBranch, String> {
    let mut request = landing_preparation_request(orch, job_id, merge_context);
    request.reservation = Some(reservation.clone());
    request.landing_issue = merge_context.issue_id.clone();
    prepare_branch(orch, request)
        .await
        .map_err(|refusal| landing_preparation_refusal(merge_context, &refusal))
}

/// The owner's-merge preparation of a landing's source onto the target it
/// lands on, under the orchestrator's landing preparation budget.
fn landing_preparation_request(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: &MergeMrContext,
) -> PreparationRequest {
    let mut request = PreparationRequest::new(
        merge_context.project_id.clone(),
        merge_context.source_branch.clone(),
        Some(job_id.to_string()),
        PreparationPurpose::OwnerMerge,
    );
    request.target_override = Some(merge_context.target_branch.clone());
    request.compute_budget = orch.landing_preparation_budget;
    request
}

fn landing_preparation_refusal(
    merge_context: &MergeMrContext,
    refusal: &PreparationRefusal,
) -> String {
    let source = &merge_context.source_branch;
    // A conflict is the one refusal the merger cannot retry past: say where it
    // is resolved. The owner's merge opened (or kept) its conflict session.
    let recovery = match refusal {
        PreparationRefusal::Conflict(_) => format!(
            " Recovery: resolve the conflict in `{source}`'s conflict session (`cairn:~/rebase` \
             on its owner's node), then merge again."
        ),
        _ => String::new(),
    };
    format!(
        "Refusing to merge `{source}` into `{target}`: it could not be prepared onto `{target}` \
         ({kind}): {refusal}. `{target}` was not advanced.{recovery}",
        target = merge_context.target_branch,
        kind = refusal.kind(),
    )
}

/// The final comparison before a landing publishes, under the caller's writer
/// guard: the prepared source head, the prepared target commit and the declared
/// relationship all still hold. `Ok(false)` when any of them moved, which
/// restarts the landing from preparation; any other refusal refuses it.
pub(super) async fn prepared_landing_holds(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    prepared: &PreparedBranch,
    guard: &JjStoreGuard,
) -> Result<bool, String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &merge_context.project_id)
        .await
        .map_err(|error| format!("the project's database is unavailable: {error}"))?;
    // The landing's target is part of its relationship: a pull request
    // retargeted since this merge read it (its parent merged, say) must not
    // land where it used to point. The loop cannot change target, so this
    // refuses rather than preparing again onto the stale one.
    let target_now = db
        .query_opt_text(
            "SELECT target_branch FROM merge_requests WHERE id = ?1",
            (merge_context.mr.mr_id.clone(),),
        )
        .await
        .map_err(|error| format!("the pull request's target is unreadable: {error}"))?;
    if target_now.as_deref().map(str::trim) != Some(merge_context.target_branch.as_str()) {
        return Err(format!(
            "Refusing to merge `{}` into `{}`: its pull request now targets {}. Nothing was \
             published; merge again to land it on its current target.",
            merge_context.source_branch,
            merge_context.target_branch,
            target_now
                .as_deref()
                .map(|target| format!("`{target}`"))
                .unwrap_or_else(|| "nothing".to_string())
        ));
    }
    // A target that has itself landed takes nothing more: its children belong
    // on what it landed on. This holds even when re-pointing them failed or
    // has not run yet.
    let target_landed = db
        .query_opt_text(
            "SELECT id FROM merge_requests
              WHERE project_id = ?1 AND source_branch = ?2 AND status = 'merged'
              LIMIT 1",
            (
                merge_context.project_id.clone(),
                merge_context.target_branch.clone(),
            ),
        )
        .await
        .map_err(|error| format!("whether the target already landed is unreadable: {error}"))?;
    if target_landed.is_some() {
        return Err(format!(
            "Refusing to merge `{}` into `{}`: `{}` has itself already merged, so this pull \
             request belongs on the branch it merged into. Nothing was published.",
            merge_context.source_branch, merge_context.target_branch, merge_context.target_branch
        ));
    }
    match prepared.revalidate(&db, guard).await {
        Ok(()) => Ok(true),
        Err(PreparationRefusal::Moved { what }) => {
            log::info!(
                "landing: the prepared coordinate of `{}` moved: {what}",
                merge_context.source_branch
            );
            Ok(false)
        }
        Err(refusal) => Err(landing_preparation_refusal(merge_context, &refusal)),
    }
}

#[derive(Debug, Clone)]
pub(super) struct ProspectiveMerge {
    pub source_tip: String,
    pub target_tip: String,
    pub commit_id: String,
    pub operation: String,
    pub tree_hash: String,
    pub tree_entries: Vec<(String, String)>,
    pub changed_files: Vec<crate::jj::GraphFileChange>,
}

/// Compute and index the exact landing without publishing a live operation.
/// All subsequent reads use this unpublished operation, not the live view.
#[allow(clippy::too_many_arguments)]
pub(super) async fn prospective_default_landing(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    target_tip: &str,
    source_tip: &str,
    guard: &JjStoreGuard,
    reservation: &crate::orchestrator::BranchReservation,
    edits: &[crate::jj::LandingTreeEdit],
    message: &str,
) -> Result<ProspectiveMerge, String> {
    super::default_landing::admit_landing_writers(orch, guard, reservation, merge_context).await?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store =
        crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.mr.repo_path));
    if method != "squash"
        && !matches!(
            crate::jj::flatten_state(&jj, &store, target_tip, &merge_context.source_branch)?,
            crate::jj::FlattenState::Clean
        )
    {
        return Err(format!(
            "Refusing to preserve conflict-flagged history on `{}`.{}",
            merge_context.target_branch,
            conflicted_history_detail(
                &jj,
                &store,
                &format!("{target_tip}..{source_tip}"),
                &merge_context.source_branch,
                Some(&merge_context.target_branch),
            ),
        ));
    }
    let candidate = crate::jj::compute_default_landing(
        &jj,
        &store,
        &merge_context.source_branch,
        source_tip,
        target_tip,
        message,
        method == "squash",
        edits,
    )?;
    let jj = jj.at_operation(&candidate.operation);
    let commit_id = candidate.commit;
    let tree_hash = crate::jj::sealed_tree_hash_via_git(&jj, &store, &commit_id)?;
    let tree_entries = crate::jj::tree_entries(&jj, &store, &commit_id)?;
    let patch = jj.run(
        &store,
        &[
            "diff",
            "--ignore-working-copy",
            "--git",
            "--from",
            target_tip,
            "--to",
            &commit_id,
        ],
        "jj diff prospective landing",
    )?;
    Ok(ProspectiveMerge {
        source_tip: source_tip.to_string(),
        target_tip: target_tip.to_string(),
        commit_id,
        operation: candidate.operation,
        tree_hash,
        tree_entries,
        changed_files: crate::jj::parse_git_diff(&patch),
    })
}
