//! The low-level jj store fold that lands a child branch into its target, with
//! the transactional rollback and worktree-refresh helpers it depends on.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use std::path::Path;
use turso::params;

use super::conflict::{conflict_recovery_hint, conflicted_history_detail};
use super::context::MergeMrContext;

#[cfg(test)]
mod tests;

/// Perform a jj source→target merge entirely in the shared store: fold the
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
    let repo_path = merge_context.mr.repo_path.as_str();
    let target_branch = merge_context.target_branch.as_str();
    let source_branch = merge_context.source_branch.as_str();
    let default_branch = merge_context.default_branch.as_str();
    let project_id = merge_context.project_id.as_str();
    let has_remote = !merge_context.mr.is_local;
    let squash_title = merge_context.title.as_str();
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));

    // Capture the source's clean pre-merge tip up front so a conflicted-rebase
    // refusal can name the seal the merge-time re-rebase overwrites (recoverable
    // via `jj restore --from <tip>` without `jj evolog`).
    let pre_source_tip = crate::jj::bookmark_commit(&jj, &store, source_branch);

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
            crate::jj::rebase_branch_onto(&jj, &store, source_branch, &dest)?;
            if crate::jj::branch_has_conflict(&jj, &store, source_branch)? {
                // The rebase recorded a conflict and the default bookmark was NOT
                // moved. The source's live workspace `@` was rebased out from
                // under it and is now stale: materialize the markers (as
                // `reconcile_siblings` does) so resolve-and-retry is actionable,
                // and let the merge gate keep blocking until they are resolved.
                refresh_worktrees_on_branch(orch, project_id, &jj, source_branch).await;
                return Err(format!(
                    "Refusing to merge: rebasing `{source_branch}` onto the advanced default branch `{target_branch}` recorded a conflict.{detail}{hint}",
                    detail = conflicted_history_detail(
                        &jj,
                        &store,
                        &format!("bookmarks(exact:{source_branch:?})"),
                        source_branch,
                        Some(target_branch),
                    ),
                    hint = pre_merge_tip_hint(source_branch, &pre_source_tip),
                ));
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
            crate::jj::rebase_branch_onto(&jj, &store, source_branch, &dest)?;
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
                // A conflict (tip or intermediate) survives the rebase and the
                // default bookmark was NOT moved. The source's live workspace `@`
                // is now stale: materialize the markers so resolve-and-retry is
                // actionable, and keep the merge blocked until they are resolved.
                refresh_worktrees_on_branch(orch, project_id, &jj, source_branch).await;
                return Err(format!(
                    "Refusing to merge: rebasing `{source_branch}` onto the advanced default branch `{target_branch}` recorded a conflict, and this PR preserves every commit (its history cannot be flattened).{detail}{hint}",
                    detail = conflicted_history_detail(
                        &jj,
                        &store,
                        &format!(
                            "bookmarks(exact:{target_branch:?})..bookmarks(exact:{source_branch:?})"
                        ),
                        source_branch,
                        Some(target_branch),
                    ),
                    hint = pre_merge_tip_hint(source_branch, &pre_source_tip),
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
                    // Re-parent every workspace on the integration branch onto the
                    // flattened tip so the coordinator's `@` follows the collapse.
                    if let (Ok(worktrees), Some(flattened)) = (
                        load_worktrees_on_branch(&orch.db.local, project_id, target_branch).await,
                        crate::jj::bookmark_commit(&jj, &store, target_branch),
                    ) {
                        let mut seen = std::collections::HashSet::new();
                        for wt in worktrees {
                            if !seen.insert(wt.clone()) {
                                continue;
                            }
                            if let Err(e) = crate::jj::advance_workspace_onto(
                                &jj,
                                &store,
                                Path::new(&wt),
                                target_branch,
                                &flattened,
                            ) {
                                log::warn!(
                                    "jj store merge: re-parent integration workspace {wt} onto flattened tip failed: {e}"
                                );
                            }
                        }
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

        crate::jj::rebase_branch_onto(&jj, &store, source_branch, target_branch)?;
        if crate::jj::branch_has_conflict(&jj, &store, source_branch)? {
            // The rebase recorded a conflict and the integration bookmark was NOT
            // moved. The source's live workspace `@` was rebased out from under it
            // and is now stale: materialize the markers (as `reconcile_siblings`
            // does) so resolve-and-retry is actionable, and let the merge gate keep
            // blocking until they are resolved. The target repair is already durable
            // on origin (published in the preflight above), so this is a pure KEEP
            // refusal: no rollback (the conflicted rebased source IS the
            // resolve-and-reseal artifact) and no target push — origin is not left
            // behind.
            refresh_worktrees_on_branch(orch, project_id, &jj, source_branch).await;
            return Err(format!(
                "Refusing to merge: rebasing `{source_branch}` onto the advanced integration branch `{target_branch}` recorded a conflict.{detail}{hint}",
                detail = conflicted_history_detail(
                    &jj,
                    &store,
                    &format!("bookmarks(exact:{source_branch:?})"),
                    source_branch,
                    Some(target_branch),
                ),
                hint = pre_merge_tip_hint(source_branch, &pre_source_tip),
            ));
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

/// `update-stale` every live workspace on `branch` so its on-disk files match the
/// store after a store-driven rewrite left `@` stale. Two callers: a
/// conflicted-rebase refusal (the source `@` was rebased out from under it, so
/// this materializes the conflict markers the agent must resolve) and
/// [`rollback_merge`] (an op-restore rewound bookmarks a preflight had advanced,
/// so the source AND target workspaces must be refreshed back to the restored
/// state). Best-effort — a refresh failure only means the agent must run
/// `jj workspace update-stale` itself; it never blocks the (already-failed) merge.
async fn refresh_worktrees_on_branch(
    orch: &Orchestrator,
    project_id: &str,
    jj: &crate::jj::JjEnv,
    branch: &str,
) {
    let worktrees = match load_worktrees_on_branch(&orch.db.local, project_id, branch).await {
        Ok(worktrees) => worktrees,
        Err(e) => {
            log::warn!(
                "jj store merge: could not load workspaces on {branch} to refresh them: {e}"
            );
            return;
        }
    };
    let mut seen = std::collections::HashSet::new();
    for worktree in worktrees {
        // Several jobs can share one physical worktree; refresh each once.
        if !seen.insert(worktree.clone()) {
            continue;
        }
        if let Err(e) = crate::jj::update_stale(jj, Path::new(&worktree)) {
            log::warn!("jj store merge: update-stale {worktree} failed: {e}");
        }
    }
}

/// Worktree paths of in-flight jobs whose branch IS `branch` (the source branch
/// of a merge). Mirrors `base_advance::load_on_branch_workspaces`' status guard
/// so a just-finished Coordinator (status `complete`) whose PR is not yet marked
/// merged is still found.
async fn load_worktrees_on_branch(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<String>, String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.worktree_path
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
            let mut worktrees = Vec::new();
            while let Some(row) = rows.next().await? {
                worktrees.push(row.text(0)?);
            }
            Ok(worktrees)
        })
    })
    .await
    .map_err(|error| error.to_string())
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
    orch: &Orchestrator,
    project_id: &str,
    jj: &crate::jj::JjEnv,
    store: &Path,
    op_id: &str,
    source_branch: &str,
    target_branch: Option<&str>,
    base_err: String,
) -> String {
    if let Err(e) = crate::jj::restore_operation(jj, store, op_id) {
        log::warn!("jj store merge: op restore during rollback failed: {e}");
    }
    // Refresh BOTH the source-branch worktrees and (integration path) the
    // target-branch worktrees onto the restored (pre-merge) `@`. The target
    // preflight may have flattened the integration branch and re-parented its
    // coordinator workspace via `advance_workspace_onto`; the op-restore rewinds
    // that in the store, so without this the target workspace stays on the
    // flattened/re-parented files on disk (stale) even though the bookmark is
    // rolled back — leaving the "all local state restored" guarantee incomplete.
    refresh_worktrees_on_branch(orch, project_id, jj, source_branch).await;
    if let Some(target_branch) = target_branch {
        if target_branch != source_branch {
            refresh_worktrees_on_branch(orch, project_id, jj, target_branch).await;
        }
    }
    format!(
        "{base_err} All local bookmarks were restored to their pre-merge state; the merge is safe to retry."
    )
}

/// Recovery hint appended to a conflicted-rebase refusal. The merge rebases the
/// source onto the current target tip before folding, which re-records the
/// conflict inside the source's tip — overwriting a clean seal the agent produced
/// at the current base (occurrence 3). Naming the pre-merge tip makes that seal's
/// tree recoverable with a plain `jj restore --from <tip>` instead of `jj evolog`.
fn pre_merge_tip_hint(source_branch: &str, pre_source_tip: &Option<String>) -> String {
    match pre_source_tip {
        Some(tip) => format!(
            "\nThe pre-merge tip of `{source_branch}` was `{tip}`; if the merge-time re-rebase overwrote a clean seal, recover its tree with `jj restore --from {tip}`."
        ),
        None => String::new(),
    }
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
