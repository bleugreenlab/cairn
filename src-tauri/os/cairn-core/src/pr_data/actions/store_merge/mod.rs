//! What every landing route shares about its source: carrying it onto the
//! target it lands on (`prepare_landing_source`) and the final check before it
//! publishes (`prepared_landing_holds`). And the store fold a default-branch
//! landing builds its landing commit with, onto an exact prepared target, with
//! its transactional rollback.

use crate::orchestrator::base_advance::preparation::{
    prepare_branch, PreparationPurpose, PreparationRefusal, PreparationRequest, PreparedBranch,
};
use crate::orchestrator::{JjStoreGuard, Orchestrator};
use std::path::Path;

use super::conflict::conflicted_history_detail;
use super::context::MergeMrContext;

#[cfg(test)]
mod tests;

/// The exact target commit a default-branch landing folds onto, and, once it
/// was judged, the tree it must produce. The fold never fetches a target of its
/// own: the landing took origin's copy into the store before preparing.
#[derive(Clone, Copy)]
pub(super) struct ExactTarget<'a> {
    pub target: &'a str,
    pub source: &'a str,
    pub tree: Option<&'a str>,
}

/// [`fold_onto_exact_target`] for a default-branch landing whose tree was already
/// validated and judged off the store lock: under the lock the fold proves it
/// is folding `validated.tree` onto exactly `validated.target`, rather than
/// re-reading every changed file for the parse gate or fetching a target that
/// may have moved since. It moves only the store: publishing the folded commit
/// to origin is the caller's, with `validated.target` as the push's expected
/// old value. The caller holds the store lock and has checked that the target
/// bookmark (and, with a remote, `<target>@origin`) is at `validated.target`.
pub(super) async fn fold_validated_default(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    validated: ExactTarget<'_>,
    guard: &JjStoreGuard,
    reservation: &crate::orchestrator::BranchReservation,
) -> Result<String, String> {
    fold_onto_exact_target(orch, merge_context, method, validated, guard, reservation).await
}

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
    let mut request = PreparationRequest::new(
        merge_context.project_id.clone(),
        merge_context.source_branch.clone(),
        Some(job_id.to_string()),
        PreparationPurpose::OwnerMerge,
    );
    request.target_override = Some(merge_context.target_branch.clone());
    request.reservation = Some(reservation.clone());
    prepare_branch(orch, request)
        .await
        .map_err(|refusal| landing_preparation_refusal(merge_context, &refusal))
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
    pub tree_hash: String,
    pub tree_entries: Vec<(String, String)>,
    pub changed_files: Vec<crate::jj::GraphFileChange>,
}

/// The commit a landing on the project DEFAULT branch would produce on exactly
/// `target_tip`, the prepared target, built in the store and then undone, so
/// the landing can validate the merge result before anything moves. Nothing
/// is fetched: the landing took origin's copy of the target into the store
/// before it prepared. The caller holds the store lock.
pub(super) async fn prospective_default_landing(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    target_tip: &str,
    source_tip: &str,
    guard: &JjStoreGuard,
    reservation: &crate::orchestrator::BranchReservation,
) -> Result<ProspectiveMerge, String> {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store =
        crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.mr.repo_path));
    let operation = guard.capture_operation(&jj)?;
    let source_tip = source_tip.to_string();
    let exact = ExactTarget {
        target: target_tip,
        source: &source_tip,
        tree: None,
    };
    let transformed =
        fold_onto_exact_target(orch, merge_context, method, exact, guard, reservation).await;
    let result = transformed.and_then(|commit_id| {
        let target_tip = target_tip.to_string();
        let tree_hash = crate::jj::sealed_tree_hash_via_git(&jj, &store, &commit_id)?;
        let tree_entries = crate::jj::tree_entries(&jj, &store, &commit_id)?;
        let patch = jj.run(
            &store,
            &[
                "diff",
                "--ignore-working-copy",
                "--git",
                "--from",
                &target_tip,
                "--to",
                &commit_id,
            ],
            "jj diff prospective landing",
        )?;
        Ok(ProspectiveMerge {
            source_tip: source_tip.clone(),
            target_tip,
            commit_id,
            tree_hash,
            tree_entries,
            changed_files: crate::jj::parse_git_diff(&patch),
        })
    });
    let restore = guard.undo_own_operations(&jj, &operation);
    match (result, restore) {
        (Ok(plan), Ok(())) => Ok(plan),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(format!(
            "failed to restore prospective landing operation: {error}"
        )),
        (Err(error), Err(restore_error)) => Err(format!(
            "{error} (also failed to restore prospective landing operation: {restore_error})"
        )),
    }
}

/// Fold the source into the default branch in the store, onto exactly
/// `exact.target`: the target the landing prepared the source onto, judged, and
/// rechecked under the store lock the caller holds. Nothing is fetched and
/// nothing is published; the landing publishes the folded commit itself, leased
/// on `exact.target`. Returns the commit the default bookmark now holds.
///
/// The `squash` method (the default) collapses the source to one commit on the
/// target, so the default branch gains exactly one commit per pull request. The
/// `merge` method (workspace pull requests) keeps every commit, and so refuses
/// any recorded conflict in the history it would preserve. The fold parse-gates
/// what it folds or, given `exact.tree`, proves it folds that already-judged
/// tree. A failed step rolls the store back to its state before the fold.
async fn fold_onto_exact_target(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    exact: ExactTarget<'_>,
    guard: &JjStoreGuard,
    reservation: &crate::orchestrator::BranchReservation,
) -> Result<String, String> {
    // Every fold moves the source and the target, the prospective one too
    // (before it is undone): both writers are admitted under this guard.
    super::default_landing::admit_landing_writers(orch, guard, reservation, merge_context).await?;
    let fold_onto_default =
        |jj: &crate::jj::JjEnv, store: &Path, target: &str, source: &str| match exact.tree {
            Some(tree) => crate::jj::fold_validated_into_bookmark(jj, store, target, source, tree),
            None => crate::jj::merge_into_bookmark(jj, store, target, source),
        };
    let target_branch = merge_context.target_branch.as_str();
    let source_branch = merge_context.source_branch.as_str();
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store =
        crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.mr.repo_path));
    let dest = exact.target;
    // A later fold failure rolls the store back to exactly this state: every
    // operation since is this fold's own, under the caller's same hold.
    let op_id = guard.capture_operation(&jj)?;

    // Preparation is the only catch-up operation. A fold verifies its
    // prerequisite rather than replaying the source a second time.
    let source_head = crate::jj::bookmark_commit(&jj, &store, source_branch)
        .ok_or_else(|| format!("the prepared source `{source_branch}` is unreadable"))?;
    if source_head != exact.source {
        return Err(format!(
            "Refusing to fold `{source_branch}`: the prepared source moved from {} to \
             {source_head}; prepare the landing again",
            exact.source
        ));
    }

    // This holds for replay and merge catch-up alike: preparation, not the
    // fold, must have made the exact target an ancestor of the source.
    if !crate::jj::revset_descends_from(&jj, &store, &source_head, dest) {
        return Err(format!(
            "Refusing to fold `{source_branch}` into `{target_branch}`: the prepared source \
             does not contain the exact target {dest}; prepare the landing again"
        ));
    }

    if method == "squash" {
        // Idempotence: a source that already resolves to the default bookmark
        // was folded by an earlier attempt whose resolution failed. Squashing
        // again would mint an empty commit on the default tip per retry.
        let already_landed = matches!(
            (
                crate::jj::bookmark_commit(&jj, &store, source_branch),
                crate::jj::bookmark_commit(&jj, &store, target_branch),
            ),
            (Some(source_tip), Some(target_tip)) if source_tip == target_tip
        );
        if !already_landed {
            // One commit whose parent is the target and whose tree is the
            // source's, through the branch-only native factory: a clean tip over
            // conflicted intermediates is recovered (the squash discards them),
            // and a wrong-base or wrong-tip collapse is refused, not landed.
            if let Err(e) = crate::jj::squash_branch_onto(
                &jj,
                &store,
                source_branch,
                exact.source,
                dest,
                &merge_context.title,
            ) {
                return Err(rollback_merge(
                    guard,
                    &jj,
                    &op_id,
                    format!(
                        "Refusing to merge: could not safely flatten `{source_branch}` onto the \
                         default branch `{target_branch}` ({e})."
                    ),
                ));
            }
            if let Err(e) = fold_onto_default(&jj, &store, target_branch, source_branch) {
                return Err(rollback_merge(guard, &jj, &op_id, e));
            }
        }
    } else {
        // `merge` (workspace pull requests) exists to PRESERVE every commit, so
        // it cannot flatten a conflict-flagged commit away: it refuses ANY
        // recorded conflict in the history it would carry onto the default
        // branch, tip or intermediate, including one older than `dest`.
        let clean = matches!(
            crate::jj::flatten_state(&jj, &store, dest, source_branch).map_err(|error| {
                format!(
                    "Refusing to preserve `{source_branch}`'s history: its conflict state could \
                     not be established ({error}); no fold was installed"
                )
            })?,
            crate::jj::FlattenState::Clean
        );
        if !clean {
            return Err(format!(
                "Refusing to merge: `{source_branch}`'s own history contains conflict-flagged \
                 commit(s), and this PR preserves every commit, so they cannot be flattened away. \
                 `{target_branch}` was not advanced. Rebuild `{source_branch}` on clean content \
                 before merging again.{detail}",
                detail = conflicted_history_detail(
                    &jj,
                    &store,
                    &format!(
                        "bookmarks(exact:{target_branch:?})..bookmarks(exact:{source_branch:?})"
                    ),
                    source_branch,
                    Some(target_branch),
                ),
            ));
        }
        // The source descends from `dest`, so this fast-forward never goes
        // backwards.
        if let Err(e) = fold_onto_default(&jj, &store, target_branch, source_branch) {
            return Err(rollback_merge(guard, &jj, &op_id, e));
        }
    }

    crate::jj::bookmark_commit(&jj, &store, target_branch)
        .ok_or_else(|| format!("target bookmark `{target_branch}` did not resolve after the fold"))
}

/// Roll a partially applied fold back to its snapshot `op_id`, captured under
/// this same hold (so every operation since it is the fold's own), and say so,
/// so the caller knows a clean retry is safe.
fn rollback_merge(
    guard: &JjStoreGuard,
    jj: &crate::jj::JjEnv,
    op_id: &crate::orchestrator::GuardOperation,
    base_err: String,
) -> String {
    if let Err(e) = guard.undo_own_operations(jj, op_id) {
        log::warn!("jj store merge: op restore during rollback failed: {e}");
    }
    format!(
        "{base_err} All local bookmarks were restored to their pre-merge state; the merge is safe to retry."
    )
}
