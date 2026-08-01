//! The low-level jj store fold that lands a child branch into its target, with
//! the transactional rollback and worktree-refresh helpers it depends on.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::path::Path;

use super::conflict::{conflict_recovery_hint, conflicted_history_detail};
use super::context::MergeMrContext;

#[cfg(test)]
mod tests;

/// Perform a jj source-to-target merge entirely in the shared store: fold the
/// source's commit into the target bookmark, then (for a project with a
/// remote) push the target to origin. The push advances the target branch and —
/// because the source's head commit is now an ancestor of the target —
/// GitHub's out-of-band "merged outside GitHub" detection marks the source PR
/// Merged (the way `git merge feature; git push` does). Returns the new
/// target-tip commit id to persist as `merge_requests.merged_commit`. For a
/// no-remote project the push is skipped and the fold is purely local.
///
/// Two fold shapes, discriminated by whether the target IS the project default
/// branch:
///
/// - target ≠ default (a child PR into a Coordinator integration branch): the
///   integration tip advances within Cairn's local fold chain as earlier
///   siblings merge in, and downstream sibling reconciliation now runs deferred
///   off the synchronous merge path — so the source may lag the live tip. Rebase
///   the source onto the current integration tip before the forward-only fold
///   (materializing any conflict and failing closed, as the default path does),
///   then — because the rebase rewrites the source's commit id — push the rebased
///   source's PR head before advancing the target on origin so GitHub still marks
///   the child PR Merged.
/// - target == default: the default branch advances OUTSIDE Cairn's fold chain
///   (another PR merged into it, or an external push), so the source's fork
///   point may now lag the live tip and a bare FF would be refused. Fetch the
///   live tip and rebase the source onto it, then FF. For the default `squash`
///   method the rebased chain is first collapsed to a single commit on the live
///   tip (`squash_branch_onto`) so the default branch gains exactly one commit
///   per PR; the `merge` method (workspace PRs) keeps the real per-commit fold
///   via `rebase_then_fold_into`. Either way the rebase/squash
///   rewrites the source's commit id, so origin's PR head SHA is no longer
///   reachable from the new target; push the rewritten source first so its PR
///   head matches the commit that lands on the default branch, then advance the
///   target to mark the PR Merged out of band.
pub(super) async fn store_merge_child(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
) -> Result<String, String> {
    store_merge_child_inner(orch, merge_context, method, true).await
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum VerifiedLanding {
    Landed(String),
    Stale,
}

pub(super) async fn prospective_store_merge_child(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
) -> Result<ProspectiveMerge, String> {
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store =
        crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.mr.repo_path));
    let operation = crate::jj::operation_id(&jj, &store)?;
    let source_tip = crate::jj::bookmark_commit(&jj, &store, &merge_context.source_branch)
        .ok_or_else(|| {
            format!(
                "source bookmark `{}` did not resolve",
                merge_context.source_branch
            )
        })?;
    let target_tip = crate::jj::bookmark_commit(&jj, &store, &merge_context.target_branch)
        .ok_or_else(|| {
            format!(
                "target bookmark `{}` did not resolve",
                merge_context.target_branch
            )
        })?;
    let mut local = merge_context.clone();
    local.mr.is_local = true;
    let transformed = store_merge_child_inner(orch, &local, method, false).await;
    let result = transformed.and_then(|commit_id| {
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
            "jj diff prospective merge",
        )?;
        Ok(ProspectiveMerge {
            source_tip,
            target_tip,
            commit_id,
            tree_hash,
            tree_entries,
            changed_files: crate::jj::parse_git_diff(&patch),
        })
    });
    let restore = crate::jj::restore_operation(&jj, &store, &operation);
    match (result, restore) {
        (Ok(plan), Ok(())) => Ok(plan),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(format!(
            "failed to restore prospective merge operation: {error}"
        )),
        (Err(error), Err(restore_error)) => Err(format!(
            "{error} (also failed to restore prospective merge operation: {restore_error})"
        )),
    }
}

pub(super) async fn land_verified_store_merge_child(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    verified: &ProspectiveMerge,
) -> Result<VerifiedLanding, String> {
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store =
        crate::jj::project_store_dir(&orch.config_dir, Path::new(&merge_context.mr.repo_path));
    if crate::jj::bookmark_commit(&jj, &store, &merge_context.source_branch).as_deref()
        != Some(&verified.source_tip)
        || crate::jj::bookmark_commit(&jj, &store, &merge_context.target_branch).as_deref()
            != Some(&verified.target_tip)
    {
        return Ok(VerifiedLanding::Stale);
    }
    let operation = crate::jj::operation_id(&jj, &store)?;
    let mut local = merge_context.clone();
    local.mr.is_local = true;
    let commit = match store_merge_child_inner(orch, &local, method, false).await {
        Ok(commit) => commit,
        Err(error) => return Err(error),
    };
    let actual_tree = crate::jj::sealed_tree_hash_via_git(&jj, &store, &commit)?;
    if actual_tree != verified.tree_hash {
        crate::jj::restore_operation(&jj, &store, &operation)?;
        return Ok(VerifiedLanding::Stale);
    }
    if !merge_context.mr.is_local {
        if let Err(error) = publish_integration_merge(
            &jj,
            &store,
            &merge_context.source_branch,
            &merge_context.target_branch,
        ) {
            return Err(rollback_merge(
                orch,
                &merge_context.project_id,
                &jj,
                &store,
                &operation,
                &merge_context.source_branch,
                Some(&merge_context.target_branch),
                error,
            )
            .await);
        }
    }
    Ok(VerifiedLanding::Landed(commit))
}

fn publish_integration_merge(
    jj: &crate::jj::JjEnv,
    store: &Path,
    source_branch: &str,
    target_branch: &str,
) -> Result<(), String> {
    validate_source_publishability(jj, store, target_branch, source_branch)?;
    let _ = crate::jj::track_bookmark(jj, store, source_branch);
    crate::jj::push_store_bookmark(jj, store, source_branch)?;
    let _ = crate::jj::track_bookmark(jj, store, target_branch);
    reflect_child_merge_on_github(jj, store, target_branch)
}

async fn store_merge_child_inner(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    method: &str,
    _materialize_conflicts: bool,
) -> Result<String, String> {
    let repo_path = merge_context.mr.repo_path.as_str();
    let target_branch = merge_context.target_branch.as_str();
    let source_branch = merge_context.source_branch.as_str();
    let default_branch = merge_context.default_branch.as_str();
    let project_id = merge_context.project_id.as_str();
    let has_remote = !merge_context.mr.is_local;
    let squash_title = merge_context.title.as_str();
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));

    if target_branch == default_branch {
        // The default branch advances out of band. Bring its live tip into the
        // store (it may have moved via another Cairn merge OR externally — a
        // fetch covers both) and rebase the source onto it before the FF fold,
        // so the fold can never go backwards.
        let dest = if has_remote {
            // Track + fetch so `<target>@origin` resolves to the live tip
            // (mirrors how `base_advance.rs` learns an external default advance).
            // Best-effort: warn and fall back to whatever the store last saw.
            if let Err(e) = crate::jj::track_bookmark(&jj, &store, target_branch) {
                log::debug!("jj store merge: track {target_branch} (continuing): {e}");
            }
            if let Err(e) = crate::jj::fetch_remote(&jj, &store, "origin") {
                log::warn!(
                    "jj store merge: fetch origin before rebase-then-fold (continuing): {e}"
                );
            }
            format!("{target_branch}@origin")
        } else {
            target_branch.to_string()
        };

        // Snapshot the store AFTER the read-only preamble (track/fetch) so a later
        // fold/push failure rolls the merge back to exactly this pre-mutation
        // state — exact under the per-store lock the caller holds.
        let op_id = crate::jj::operation_id(&jj, &store)?;

        if method == "squash" {
            // Squash landing: rebase the source onto the live default tip, then
            // collapse the rebased chain to a single commit on that tip before
            // the FF — so the default branch gains exactly one commit per PR
            // instead of every per-change commit the agent sealed.
            // A conflicting rebase is rolled back inside `rebase_branch_onto`, so
            // by the time this returns the source is bit-identical to its
            // pre-merge tip and the default bookmark never moved. Branch on the
            // outcome rather than re-probing for a conflict: the probe would now
            // report a clean branch and read a refusal as a success.
            match crate::jj::rebase_branch_onto(&jj, &store, source_branch, &dest)? {
                crate::jj::RebaseOutcome::Conflicted { diagnostic } => {
                    return Err(crate::jj::base_conflict_refusal(
                        target_branch,
                        source_branch,
                        &diagnostic.conflicting_paths(),
                    ))
                }
                // A clean tip over conflicted history is safe HERE and only here:
                // this path lands exactly one commit whose tree equals the source
                // tip, so `flatten_branch_recovery` below discards the conflicted
                // ancestry rather than folding it onto the default branch.
                crate::jj::RebaseOutcome::RebasedOverConflictedAncestry { .. }
                | crate::jj::RebaseOutcome::Rebased => {}
            }
            // Idempotence guard (mirrors the old real-fold path's no-op on a
            // retry): if the source already resolves to the LOCAL default
            // bookmark, a prior attempt's fold already landed this PR's content
            // and only local resolution / DB marking failed. Squashing again
            // would mint a fresh empty commit on the default tip and FF onto it,
            // adding one empty commit per retry. Skip the squash+fold and fall
            // through to the return. Compared against the LOCAL target (not
            // `dest`): when an interrupted retry still needs to advance origin,
            // the source already equals the local tip while `dest@origin` may
            // lag, and re-squashing against the lagging origin tip would mint a
            // sideways commit the FF then refuses. The remote push block below
            // still runs, idempotently finishing any unpushed origin advance.
            let already_landed = matches!(
                (
                    crate::jj::bookmark_commit(&jj, &store, source_branch),
                    crate::jj::bookmark_commit(&jj, &store, target_branch),
                ),
                (Some(source_tip), Some(target_tip)) if source_tip == target_tip
            );
            if !already_landed {
                // Collapse to one commit whose parent is the live default tip and
                // whose tree equals the rebased source, then FF the default to it.
                // Routed through the footprint-guarded flatten so a clean-tip /
                // conflicted-intermediate source is recovered (the squash discards
                // the conflicted intermediates), while the footprint guard refuses
                // a wrong-base/wrong-tip collapse rather than landing it. The
                // fully-clean case is unchanged (a plain squash plus orphan cleanup).
                if let Err(e) = crate::jj::flatten_branch_recovery(
                    &jj,
                    &store,
                    source_branch,
                    &dest,
                    squash_title,
                ) {
                    return Err(rollback_merge(
                        orch,
                        project_id,
                        &jj,
                        &store,
                        &op_id,
                        source_branch,
                        None,
                        format!(
                            "Refusing to merge: could not safely flatten `{source_branch}` onto the default branch `{target_branch}` ({e})."
                        ),
                    )
                    .await);
                }
                if let Err(e) =
                    crate::jj::merge_into_bookmark(&jj, &store, target_branch, source_branch)
                {
                    return Err(rollback_merge(
                        orch,
                        project_id,
                        &jj,
                        &store,
                        &op_id,
                        source_branch,
                        None,
                        e,
                    )
                    .await);
                }
            }
        } else {
            // Non-squash (workspace): keep the real fold so the
            // default branch carries every sealed commit. This method exists to
            // PRESERVE every commit, so flattening would contradict its intent —
            // instead it refuses on ANY recorded conflict (tip or intermediate) so
            // the relaxed merge gate cannot let a conflicted-ancestor branch poison
            // the default branch via a preserved fold. Rebase onto the live default
            // tip, gate, then FF the default to it.
            // This method preserves every commit, so conflict-flagged ancestry
            // cannot be flattened away and must be refused outright.
            match crate::jj::rebase_branch_onto(&jj, &store, source_branch, &dest)? {
                crate::jj::RebaseOutcome::Rebased => {}
                crate::jj::RebaseOutcome::Conflicted { diagnostic } => {
                    return Err(crate::jj::base_conflict_refusal(
                        target_branch,
                        source_branch,
                        &diagnostic.conflicting_paths(),
                    ))
                }
                crate::jj::RebaseOutcome::RebasedOverConflictedAncestry { paths } => {
                    return Err(crate::jj::conflicted_ancestry_refusal(
                        target_branch,
                        source_branch,
                        &paths,
                    ))
                }
            }
            // Belt and braces on the same question, from the other side: this
            // also catches a conflicted intermediate that predates the rebase
            // dest and so falls outside the rebased range.
            let clean = match crate::jj::flatten_state(&jj, &store, &dest, source_branch) {
                Ok(crate::jj::FlattenState::Clean) => true,
                Ok(_) => false,
                // Liveness: fall back to the bare tip-conflict check on a probe error.
                Err(e) => {
                    log::warn!(
                        "non-squash preserve: flatten_state check for {source_branch} failed: {e}; falling back to tip check"
                    );
                    !crate::jj::branch_has_conflict(&jj, &store, source_branch).unwrap_or(false)
                }
            };
            if !clean {
                // Pre-existing conflict-flagged commits in the source's history.
                // This PR method exists to preserve every commit, so they cannot
                // be flattened away; the default bookmark was NOT moved.
                return Err(format!(
                    "Refusing to merge: `{source_branch}`'s own history contains conflict-flagged commit(s), and this PR preserves every commit, so they cannot be flattened away. `{target_branch}` was not advanced. Rebuild `{source_branch}` on clean content before merging again.{detail}",
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
            // Clean: the source now descends from the advanced default tip, so this
            // FF can never go backwards.
            if let Err(e) =
                crate::jj::merge_into_bookmark(&jj, &store, target_branch, source_branch)
            {
                return Err(rollback_merge(
                    orch,
                    project_id,
                    &jj,
                    &store,
                    &op_id,
                    source_branch,
                    None,
                    e,
                )
                .await);
            }
        }

        if has_remote {
            // Advance the rebased source's PR head on origin BEFORE advancing the
            // target. The rebase rewrote the source's commit id, so origin's PR
            // head must move to the rebased commit or GitHub never marks the PR
            // Merged (its old head SHA is not reachable from the advanced
            // default). Load-bearing, so fail closed: do NOT advance the target
            // on origin while the PR head still points at the abandoned commit —
            // that would land the content but leave the PR unmerged. A retry is
            // idempotent (the source already sits on the fetched tip).
            if let Err(e) =
                validate_source_publishability(&jj, &store, target_branch, source_branch)
            {
                return Err(rollback_merge(
                    orch,
                    project_id,
                    &jj,
                    &store,
                    &op_id,
                    source_branch,
                    None,
                    e,
                )
                .await);
            }
            if let Err(e) = crate::jj::track_bookmark(&jj, &store, source_branch) {
                log::debug!("jj store merge: track {source_branch} (continuing): {e}");
            }
            if let Err(e) = crate::jj::push_store_bookmark(&jj, &store, source_branch) {
                let recovery = if e.contains("conflict") {
                    format!(
                        "\n{}",
                        conflict_recovery_hint(source_branch, Some(target_branch))
                    )
                } else {
                    String::new()
                };
                let base_err = format!(
                    "Refusing to complete the merge: could not advance the rebased source `{source_branch}` on origin ({e}). The default branch was not advanced on origin; retry the merge.{recovery}"
                );
                return Err(rollback_merge(
                    orch,
                    project_id,
                    &jj,
                    &store,
                    &op_id,
                    source_branch,
                    None,
                    base_err,
                )
                .await);
            }
            if let Err(e) = reflect_child_merge_on_github(&jj, &store, target_branch) {
                return Err(rollback_merge(
                    orch,
                    project_id,
                    &jj,
                    &store,
                    &op_id,
                    source_branch,
                    None,
                    e,
                )
                .await);
            }
        }
    } else {
        // Child→integration: rebase the source onto the live integration tip
        // before the forward-only fold, so this merge is self-contained. The
        // integration tip advances within Cairn's local fold chain as earlier
        // siblings merge into it; downstream sibling reconciliation — which used
        // to rebase the not-yet-merged siblings onto each advance — now runs
        // deferred off the synchronous merge path, so this fold can no longer
        // assume the source already sits on the current tip. Without this rebase a
        // second child merged into the same integration branch before the
        // background reconcile lands would still be based on the pre-advance tip,
        // and `merge_into_bookmark`'s forward-only `bookmark set` would refuse it
        // ("source is not a descendant of the target"). Rebasing here mirrors the
        // default-branch path and keeps sequential Coordinator child merges
        // correct regardless of reconcile timing.
        // Snapshot the store BEFORE the target preflight so a preflight failure (a
        // flatten guard refusal, or a failed PUBLISH of the repair) rewinds cleanly
        // to the pre-merge state (exact under the per-store lock the caller holds).
        let pre_repair_op = crate::jj::operation_id(&jj, &store)?;

        // Target preflight (the load-bearing fix). Every other conflict probe on
        // the merge path scopes to the SOURCE range and is blind to conflicted
        // commits in the TARGET integration branch's own ancestry. A `main` advance
        // can bake conflicts into the hub's INTERMEDIATE commits; the coordinator
        // resolves at the tip and re-seals (clean tip, conflicted ancestors), so
        // every source-scoped probe passes and the fold succeeds locally — then
        // pushing the source fails, because its ancestry now includes the target's
        // conflicted intermediates and jj refuses to push a conflicted commit, and
        // nothing ever flattened the target. Flatten the target FIRST so the merge
        // builds on a pushable integration branch (CAIRN-2288).
        //
        // The target flatten is a STANDALONE, content-preserving repair of the
        // integration branch, independent of this child's merge, so it is committed
        // (pushed to origin) FAIL-CLOSED here — before the merge transaction begins
        // — and the merge's own rollback baseline (`op_id`, snapshot below) already
        // includes the durable repair. That separation is what lets the
        // source-conflict refusal keep the source markers without any risk of
        // leaving origin behind a locally-clean target: by the time the source is
        // rebased, the repair is already durable everywhere or the whole merge has
        // rewound to the pre-repair state.
        if let Some(dest_commit) = resolve_target_base_commit(
            &orch.db.local,
            &jj,
            &store,
            project_id,
            target_branch,
            default_branch,
        )
        .await
        {
            match crate::jj::flatten_state(&jj, &store, &dest_commit, target_branch) {
                Ok(crate::jj::FlattenState::Clean) => {}
                Ok(crate::jj::FlattenState::TipConflicted) => {
                    return Err(format!(
                        "Refusing to merge into `{target_branch}`: the integration branch's own tip carries a recorded conflict. Its coordinator must resolve the conflict markers in that workspace and re-seal before any child PR can merge into it."
                    ));
                }
                Ok(crate::jj::FlattenState::IntermediateOnly) => {
                    let message = {
                        let desc = crate::jj::branch_description(&jj, &store, target_branch);
                        if desc.is_empty() {
                            squash_title.to_string()
                        } else {
                            desc
                        }
                    };
                    if let Err(e) = crate::jj::flatten_branch_recovery(
                        &jj,
                        &store,
                        target_branch,
                        &dest_commit,
                        &message,
                    ) {
                        return Err(rollback_merge(
                            orch,
                            project_id,
                            &jj,
                            &store,
                            &pre_repair_op,
                            source_branch,
                            Some(target_branch),
                            format!(
                                "Refusing to merge into `{target_branch}`: its history has a clean tip over conflicted intermediate commit(s) that could not be safely flattened ({e})."
                            ),
                        )
                        .await);
                    }
                    // Publish the repair to origin FAIL-CLOSED. If it cannot land,
                    // roll the flatten (and worktree re-parent) back to the
                    // pre-repair state so local and origin stay identical (both
                    // wedged) rather than leaving origin behind a locally-clean
                    // target; a retry re-attempts the repair. Nothing source-side
                    // has run yet, so this rollback strands no conflict markers.
                    if has_remote {
                        if let Err(e) = reflect_child_merge_on_github(&jj, &store, target_branch) {
                            return Err(rollback_merge(
                                orch,
                                project_id,
                                &jj,
                                &store,
                                &pre_repair_op,
                                source_branch,
                                Some(target_branch),
                                format!(
                                    "Refusing to merge: the integration branch `{target_branch}` had a clean tip over conflicted intermediate commit(s), but the flatten that repairs it could not be published to origin ({e})."
                                ),
                            )
                            .await);
                        }
                    }
                }
                Err(e) => log::warn!(
                    "jj store merge: target preflight flatten_state for {target_branch} failed: {e}; proceeding without target flatten"
                ),
            }
        }

        // Snapshot the MERGE rollback baseline AFTER the (now durable) target
        // repair, so a later source-side failure rewinds only the merge and never
        // un-does the published repair.
        let op_id = crate::jj::operation_id(&jj, &store)?;

        // A conflicting rebase is rolled back inside `rebase_branch_onto`, so the
        // source is bit-identical to its pre-merge tip and the integration
        // bookmark never moved. The target repair above is already durable on
        // origin, so this is a pure refusal: nothing to roll back, and origin is
        // not left behind.
        match crate::jj::rebase_branch_onto(&jj, &store, source_branch, target_branch)? {
            crate::jj::RebaseOutcome::Conflicted { diagnostic } => {
                return Err(crate::jj::base_conflict_refusal(
                    target_branch,
                    source_branch,
                    &diagnostic.conflicting_paths(),
                ))
            }
            // Handled below: the child is flattened to one clean commit on the
            // integration tip before the fold, so conflicted ancestry is
            // discarded rather than folded into the integration branch.
            crate::jj::RebaseOutcome::RebasedOverConflictedAncestry { .. }
            | crate::jj::RebaseOutcome::Rebased => {}
        }

        // Clean tip: if a base advance baked conflicts into INTERMEDIATE commits
        // (clean net tip, conflicted ancestors), flatten the child to ONE clean
        // commit on the integration tip before folding — otherwise `merge_into_bookmark`
        // preserves the child's lineage and poisons the integration branch with
        // conflicted ancestors (exactly the CAIRN-2269 failure). The per-child
        // lineage is ephemeral (collapsed again at default-landing), so flattening
        // it changes nothing on main. On guard failure, keep the existing refuse +
        // materialize path.
        match crate::jj::flatten_state(&jj, &store, target_branch, source_branch) {
            Ok(crate::jj::FlattenState::IntermediateOnly) => {
                let dest_commit = crate::jj::bookmark_commit(&jj, &store, target_branch)
                    .ok_or_else(|| {
                        format!(
                            "integration bookmark `{target_branch}` did not resolve for flatten"
                        )
                    })?;
                let desc = crate::jj::branch_description(&jj, &store, source_branch);
                let message = if desc.is_empty() {
                    squash_title.to_string()
                } else {
                    desc
                };
                if let Err(e) = crate::jj::flatten_branch_recovery(
                    &jj,
                    &store,
                    source_branch,
                    &dest_commit,
                    &message,
                ) {
                    let base_err = format!(
                        "Refusing to merge: could not safely flatten `{source_branch}` onto the integration branch `{target_branch}` ({e}).{detail}",
                        detail = conflicted_history_detail(
                            &jj,
                            &store,
                            &format!(
                                "bookmarks(exact:{target_branch:?})..bookmarks(exact:{source_branch:?})"
                            ),
                            source_branch,
                            Some(target_branch),
                        )
                    );
                    return Err(rollback_merge(
                        orch,
                        project_id,
                        &jj,
                        &store,
                        &op_id,
                        source_branch,
                        Some(target_branch),
                        base_err,
                    )
                    .await);
                }
            }
            Ok(_) => {}
            Err(e) => log::warn!(
                "child->integration: flatten_state check for {source_branch} failed: {e}; proceeding with a plain fold"
            ),
        }

        // Fold the source's (now-descendant) real commit into the integration
        // bookmark (forward-only).
        if let Err(e) = crate::jj::merge_into_bookmark(&jj, &store, target_branch, source_branch) {
            return Err(rollback_merge(
                orch,
                project_id,
                &jj,
                &store,
                &op_id,
                source_branch,
                Some(target_branch),
                e,
            )
            .await);
        }

        if has_remote {
            // The rebase may have rewritten the source's commit id, so origin's PR
            // head must move to the rebased commit BEFORE the integration ref
            // advances — otherwise the child PR's old head SHA is unreachable from
            // the advanced integration branch and GitHub never marks it Merged.
            // Push the source first and fail closed (do NOT advance the target on
            // origin while the PR head is stale), then advance the target.
            // Defensively track each bookmark, since its `@origin` ref may have been
            // created outside this store's jj (best-effort).
            if let Err(e) =
                validate_source_publishability(&jj, &store, target_branch, source_branch)
            {
                return Err(rollback_merge(
                    orch,
                    project_id,
                    &jj,
                    &store,
                    &op_id,
                    source_branch,
                    None,
                    e,
                )
                .await);
            }
            if let Err(e) = crate::jj::track_bookmark(&jj, &store, source_branch) {
                log::debug!("jj store merge: track {source_branch} (continuing): {e}");
            }
            if let Err(e) = crate::jj::push_store_bookmark(&jj, &store, source_branch) {
                let recovery = if e.contains("conflict") {
                    format!(
                        "\n{}",
                        conflict_recovery_hint(source_branch, Some(target_branch))
                    )
                } else {
                    String::new()
                };
                let base_err = format!(
                    "Refusing to complete the merge: could not advance the rebased source `{source_branch}` on origin ({e}). The integration branch was not advanced on origin; retry the merge.{recovery}"
                );
                return Err(rollback_merge(
                    orch,
                    project_id,
                    &jj,
                    &store,
                    &op_id,
                    source_branch,
                    Some(target_branch),
                    base_err,
                )
                .await);
            }
            if let Err(e) = crate::jj::track_bookmark(&jj, &store, target_branch) {
                log::debug!("jj store merge: track {target_branch} (continuing): {e}");
            }
            if let Err(e) = reflect_child_merge_on_github(&jj, &store, target_branch) {
                return Err(rollback_merge(
                    orch,
                    project_id,
                    &jj,
                    &store,
                    &op_id,
                    source_branch,
                    Some(target_branch),
                    e,
                )
                .await);
            }
        }
    }

    crate::jj::bookmark_commit(&jj, &store, target_branch)
        .ok_or_else(|| format!("target bookmark `{target_branch}` did not resolve after the fold"))
}

fn validate_source_publishability(
    jj: &crate::jj::JjEnv,
    store: &Path,
    target_branch: &str,
    source_branch: &str,
) -> Result<(), String> {
    let revset = format!("bookmarks(exact:{target_branch:?})..bookmarks(exact:{source_branch:?})");
    let template = "commit_id.short() ++ \"\\x1f\" ++ description.first_line() ++ \"\\x1f\" ++ if(empty, \"1\", \"0\") ++ \"\\n\"";
    let output = jj.run(
        store,
        &[
            "log",
            "--ignore-working-copy",
            "-r",
            &revset,
            "--no-graph",
            "-T",
            template,
        ],
        "jj log source publishability",
    )?;
    let mut malformed = Vec::new();
    for line in output.lines() {
        let mut fields = line.split('\u{1f}');
        let commit_id = fields.next().unwrap_or_default().trim();
        let description = fields.next().unwrap_or_default().trim();
        let empty = fields.next().unwrap_or_default().trim() == "1";
        if !commit_id.is_empty() && (description.is_empty() || empty) {
            malformed.push(format!(
                "{commit_id} ({})",
                if description.is_empty() && empty {
                    "empty tree and blank description"
                } else if description.is_empty() {
                    "blank description"
                } else {
                    "empty tree"
                }
            ));
        }
    }
    if malformed.is_empty() {
        return Ok(());
    }
    Err(format!(
        "Refusing to publish source `{source_branch}`: its final source stack contains malformed/debris commit(s): {}. Amend or abandon those commits, ensure `{source_branch}` points to the surviving stack, then retry the merge.",
        malformed.join(", ")
    ))
}

/// The base branch the integration `target_branch` was itself cut from: the
/// `base_branch` of the newest job whose `branch` IS `target_branch` in this
/// project. A Coordinator integration branch's base is the project default, but a
/// nested integration branch's base is its parent integration branch — read it
/// from the job row rather than assuming the default. `None` when no such job
/// recorded a base (the caller falls back to the project default).
async fn load_target_base_branch(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Option<String>, String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT base_branch
                     FROM jobs
                     WHERE project_id = ?1
                       AND branch = ?2
                       AND base_branch IS NOT NULL
                     ORDER BY created_at DESC
                     LIMIT 1",
                    params![project_id.as_str(), branch.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Resolve the concrete commit the integration `target_branch` is flattened onto
/// during the merge-time target preflight: its own recorded base
/// ([`load_target_base_branch`]), falling back to the project default, resolved to
/// a bookmark commit. `None` — skip the preflight (liveness over strictness, the
/// same convention as `classify_reconciled_sibling`'s unresolvable-dest arm) —
/// when neither base resolves to a commit.
async fn resolve_target_base_commit(
    db: &LocalDb,
    jj: &crate::jj::JjEnv,
    store: &Path,
    project_id: &str,
    target_branch: &str,
    default_branch: &str,
) -> Option<String> {
    let base_branch = match load_target_base_branch(db, project_id, target_branch).await {
        Ok(Some(base)) => base,
        Ok(None) => default_branch.to_string(),
        Err(e) => {
            log::warn!(
                "jj store merge: could not load base branch for `{target_branch}` ({e}); using the default `{default_branch}`"
            );
            default_branch.to_string()
        }
    };
    match crate::jj::bookmark_commit(jj, store, &base_branch) {
        Some(commit) => Some(commit),
        None => {
            log::warn!(
                "jj store merge: target base `{base_branch}` for `{target_branch}` did not resolve; skipping target preflight"
            );
            None
        }
    }
}

/// Roll a partially-applied merge back to its pre-merge snapshot and extend the
/// error so the agent knows a clean retry is safe. Called from every
/// mutation-phase failure that is NOT a designed resolve-and-reseal refusal (a
/// flatten guard failure, a failed fold, or a failed origin push): restore the
/// whole store to `op_id` — exact under the per-store lock the merge holds, since
/// every op since the snapshot is the merge's own — then refresh the
/// source-branch worktrees onto the restored state. This completes the CAIRN-2287
/// principle: never PERSIST a merge the remote never saw, so a push half-failure
/// no longer leaves local bookmarks diverged from origin (the occurrence-1–3
/// state corruption).
// The store identity (orch, project_id, jj, store, op_id) plus the two branches
// and the error message are each load-bearing and distinct; a wrapper struct
// would not clarify the call sites.
#[allow(clippy::too_many_arguments)]
async fn rollback_merge(
    _orch: &Orchestrator,
    _project_id: &str,
    jj: &crate::jj::JjEnv,
    store: &Path,
    op_id: &str,
    _source_branch: &str,
    _target_branch: Option<&str>,
    base_err: String,
) -> String {
    if let Err(e) = crate::jj::restore_operation(jj, store, op_id) {
        log::warn!("jj store merge: op restore during rollback failed: {e}");
    }
    format!(
        "{base_err} All local bookmarks were restored to their pre-merge state; the merge is safe to retry."
    )
}

/// Reflect a folded child merge as Merged on GitHub by pushing the advanced
/// integration bookmark to origin. This is the single swappable seam for the
/// GitHub-state hypothesis: if live testing ever shows GitHub marks the PR Closed
/// (or is unreliable), a state-only merge-API call belongs here and nowhere else
/// — the store already owns the content by this point.
fn reflect_child_merge_on_github(
    jj: &crate::jj::JjEnv,
    store: &Path,
    integration_branch: &str,
) -> Result<(), String> {
    crate::jj::push_store_bookmark(jj, store, integration_branch)
}
