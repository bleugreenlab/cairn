//! What every landing route shares about its source: carrying it onto the
//! target it lands on (`prepare_landing_source`) and the final check before it
//! publishes (`prepared_landing_holds`). Default landing candidates are computed
//! on an unpublished immutable index, never by moving and undoing live refs.
//!
//! A source is carried onto its target only by the canonical branch preparer,
//! as its owner's merge. A request branch, whose head's only parent is the
//! target's tip, needs no carrying: `exact_landing_source` admits it here, and
//! preparation never advances it. Every landing route still starts here.

use crate::orchestrator::base_advance::preparation::{
    prepare_branch, PreparationPurpose, PreparationRefusal, PreparationRequest, PreparedBranch,
};
use crate::orchestrator::{JjStoreGuard, Orchestrator};
use std::path::Path;

use super::conflict::conflicted_history_detail;
use super::context::LandingSubject;

#[cfg(test)]
mod tests;

/// Why a landing's source was not admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SourceRefusal {
    /// A request branch whose commit no longer sits on the target's tip: the
    /// target moved after the seal. Nothing carries it, so its owner seals
    /// again on the new tip rather than retrying this landing.
    BaseMoved(String),
    /// Anything else, as the refusal the landing reports.
    Refused(String),
}

impl From<SourceRefusal> for String {
    fn from(refusal: SourceRefusal) -> Self {
        match refusal {
            SourceRefusal::BaseMoved(why) | SourceRefusal::Refused(why) => why,
        }
    }
}

/// Carry a landing's source onto the target it actually lands on, by the
/// canonical branch preparer, as its owner's merge, or for a request branch
/// admit it exactly as it stands ([`exact_landing_source`]). What the landing
/// binds, validates and publishes is the returned coordinate.
///
/// `reservation` is the landing's own reservation of its source and target,
/// inherited by the preparation: it installs the source's advance under it
/// instead of reserving the source again, which from the landing's task
/// would be re-entrant.
pub(super) async fn prepare_landing_source(
    orch: &Orchestrator,
    owner_id: &str,
    merge_context: &LandingSubject,
    reservation: &std::sync::Arc<crate::orchestrator::BranchReservation>,
) -> Result<PreparedBranch, SourceRefusal> {
    if crate::jj::is_request_branch(&merge_context.source_branch) {
        return exact_landing_source(orch, merge_context).await;
    }
    if merge_context.is_operator_request() {
        return Err(SourceRefusal::Refused(format!(
            "Refusing to land `{}`: an operator request lands only its own `{}` branch.",
            merge_context.source_branch,
            crate::jj::REQUEST_BRANCH_PREFIX
        )));
    }
    let mut request = landing_preparation_request(orch, owner_id, merge_context);
    request.reservation = Some(reservation.clone());
    request.landing_issue = merge_context.issue_id.clone();
    prepare_branch(orch, request).await.map_err(|refusal| {
        SourceRefusal::Refused(landing_preparation_refusal(merge_context, &refusal))
    })
}

/// Admit a request branch's source exactly as it stands, with nothing
/// carried: only when its head's only parent is the target's tip. Any other
/// shape is [`SourceRefusal::BaseMoved`]. The landing's final check holds the
/// returned coordinate to that tip and head (`prepared_landing_holds`).
async fn exact_landing_source(
    orch: &Orchestrator,
    merge_context: &LandingSubject,
) -> Result<PreparedBranch, SourceRefusal> {
    use crate::orchestrator::base_advance::preparation::{PreparedExport, Provenance};
    let (source, target) = (
        merge_context.source_branch.as_str(),
        merge_context.target_branch.as_str(),
    );
    let refused = |why: String| {
        SourceRefusal::Refused(format!(
            "Refusing to land `{source}` on `{target}`: {why}. `{target}` was not advanced."
        ))
    };
    let store = super::default_landing::store_of(orch, merge_context);
    let head = crate::jj::bookmark_commit(&store.jj, &store.path, source)
        .ok_or_else(|| refused(format!("the store has no `{source}`")))?;
    let tip = crate::jj::bookmark_commit(&store.jj, &store.path, target)
        .ok_or_else(|| refused(format!("the store has no `{target}`")))?;
    let shape = super::default_landing::store_commit_shape(&store, &head).map_err(refused)?;
    if shape.parents != [tip.clone()] {
        return Err(SourceRefusal::BaseMoved(format!(
            "`{source}`'s commit {head} sits on {}, but `{target}` is at {tip}: the base moved \
             after the seal, so it is sealed again on the new tip. `{target}` was not advanced.",
            shape.parents.join(", ")
        )));
    }
    Ok(PreparedBranch {
        project_id: merge_context.project_id.clone(),
        store: store.path,
        branch: source.to_string(),
        head_before: head.clone(),
        head_after: head,
        tree: shape.tree,
        provenance: Provenance::Exact {
            target: target.to_string(),
            tip: tip.clone(),
        },
        base_commit: tip,
        pinned_base: false,
        advanced: false,
        export: PreparedExport::NotNeeded,
        attempts: 1,
        session: None,
        warnings: Vec::new(),
        advance_pending: None,
    })
}

/// The owner's-merge preparation of a landing's source onto the target it
/// lands on, under the orchestrator's landing preparation budget.
fn landing_preparation_request(
    orch: &Orchestrator,
    job_id: &str,
    merge_context: &LandingSubject,
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
    merge_context: &LandingSubject,
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
    merge_context: &LandingSubject,
    prepared: &PreparedBranch,
    guard: &JjStoreGuard,
) -> Result<bool, String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &merge_context.project_id)
        .await
        .map_err(|error| format!("the project's database is unavailable: {error}"))?;
    let Some(mr_id) = merge_context.merge_request_id() else {
        // An operator request has no pull request to retarget: its target is
        // the subject's own, fixed from the act, and the exact coordinate was
        // admitted against exactly it.
        if prepared.base_branch() != merge_context.target_branch {
            return Err(format!(
                "Refusing to land `{}`: it was admitted against `{}`, not `{}`. Nothing was \
                 published.",
                merge_context.source_branch,
                prepared.base_branch(),
                merge_context.target_branch
            ));
        }
        return holds_or_moved(&db, guard, merge_context, prepared).await;
    };
    // The landing's target is part of its relationship: a pull request
    // retargeted since this merge read it (its parent merged, say) must not
    // land where it used to point. The loop cannot change target, so this
    // refuses rather than preparing again onto the stale one.
    let target_now = db
        .query_opt_text(
            "SELECT target_branch FROM merge_requests WHERE id = ?1",
            (mr_id.to_string(),),
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
    holds_or_moved(&db, guard, merge_context, prepared).await
}

/// The prepared coordinate's own revalidation: `Ok(false)` when it moved.
async fn holds_or_moved(
    db: &crate::storage::LocalDb,
    guard: &JjStoreGuard,
    merge_context: &LandingSubject,
    prepared: &PreparedBranch,
) -> Result<bool, String> {
    match prepared.revalidate(db, guard).await {
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
    merge_context: &LandingSubject,
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
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.repo_path));
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
