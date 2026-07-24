//! Guarded flatten recovery and sibling reconcile onto an advanced tip.
use super::*;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedWorkspaceReconcileOutcome {
    Unchanged,
    AdvancedClean,
    AdvancedConflicted,
    PreservedDirty,
}

/// Read-only optimistic probe used before entering the project-wide store lock.
/// The full reconciler revalidates after acquisition, so a concurrent advance can
/// only cause a harmless false negative that seal-time stale recovery catches.
pub(crate) fn managed_workspace_needs_reconcile(
    jj: &JjEnv,
    store: &Path,
    workspace_path: &Path,
    branch: &str,
) -> Result<bool, String> {
    if branch_has_conflict(jj, store, branch).unwrap_or(true) {
        return Ok(false);
    }
    let Some(tip) = bookmark_commit(jj, store, branch) else {
        return Ok(false);
    };
    let source = format!("{}@", workspace_name_for_branch(branch));
    let _ = workspace_path;
    Ok(!revset_descends_from(jj, store, &source, &tip))
}

/// Bring one managed workspace's working copy into agreement with its bookmark.
/// The caller must hold the project's owner-tagged jj store lock.
pub(crate) fn reconcile_managed_workspace(
    jj: &JjEnv,
    store: &Path,
    workspace_path: &Path,
    branch: &str,
    pinned_destination_tip: Option<&str>,
) -> Result<ManagedWorkspaceReconcileOutcome, String> {
    update_stale(jj, workspace_path)?;

    if is_working_copy_dirty(jj, workspace_path)? {
        return Ok(ManagedWorkspaceReconcileOutcome::PreservedDirty);
    }

    let tip = match pinned_destination_tip {
        Some(tip) => tip.to_string(),
        None => {
            // Run/write preflight must preserve the conflicted-branch recovery
            // boundary: a clean workspace behind a conflicted bookmark stays
            // untouched so ordinary command preparation cannot materialize the
            // conflict into `@`. Probe failures are conservative for the same
            // reason. Pinned base-advance callers intentionally continue because
            // they own surfacing conflicts recorded by that reconciliation.
            if branch_has_conflict(jj, store, branch).unwrap_or(true) {
                return Ok(ManagedWorkspaceReconcileOutcome::Unchanged);
            }
            match bookmark_commit(jj, store, branch) {
                Some(tip) => tip,
                None => return Ok(ManagedWorkspaceReconcileOutcome::Unchanged),
            }
        }
    };
    let source = format!("{}@", workspace_name_for_branch(branch));
    if revset_descends_from(jj, store, &source, &tip) {
        return Ok(ManagedWorkspaceReconcileOutcome::Unchanged);
    }

    let conflicted = advance_workspace_onto(jj, store, workspace_path, branch, &tip)?;
    Ok(if conflicted {
        ManagedWorkspaceReconcileOutcome::AdvancedConflicted
    } else {
        ManagedWorkspaceReconcileOutcome::AdvancedClean
    })
}

fn record_failure(
    report: &mut ReconcileReport,
    branch: &str,
    workspace_path: &Path,
    error: String,
) {
    report.failed.push(ReconcileFailure {
        branch: branch.to_string(),
        workspace_path: workspace_path.to_path_buf(),
        error,
    });
}

/// The set of paths a branch changes relative to a dest, plus which of those are
/// deletions. Captured before a flatten and re-checked after: [`squash_branch_onto`]
/// restores the exact clean-tip tree, so the post-flatten footprint MUST match
/// the pre-flatten one. A footprint mismatch — above all a NEW deletion of a dest
/// file — means the flatten re-parented a stale tree (wrong base/tip) and would
/// silently revert base files, so the guard rejects it.
#[derive(Debug, Default, PartialEq, Eq)]
struct BranchFootprint {
    changed: std::collections::BTreeSet<String>,
    deletions: std::collections::BTreeSet<String>,
}

/// Compute the tree diff footprint of `dest_commit..tip_commit` over the store.
/// Uses the same `jj diff --git` → [`parse_git_diff`] path as `node_changed_files`,
/// so a rename is decomposed into an added new path plus a deleted old path.
fn branch_footprint(
    jj: &JjEnv,
    store: &Path,
    dest_commit: &str,
    tip_commit: &str,
) -> Result<BranchFootprint, String> {
    let out = jj.run(
        store,
        &[
            "diff",
            "--ignore-working-copy",
            "--git",
            "--from",
            dest_commit,
            "--to",
            tip_commit,
        ],
        "jj diff (flatten footprint)",
    )?;
    let mut footprint = BranchFootprint::default();
    for change in parse_git_diff(&out) {
        footprint.changed.insert(change.path.clone());
        if change.status == "deleted" {
            footprint.deletions.insert(change.path.clone());
        }
        if let Some(prev) = change.previous_path {
            // A rename removes the old path from the tree.
            footprint.changed.insert(prev.clone());
            footprint.deletions.insert(prev);
        }
    }
    Ok(footprint)
}

/// The full description of a bookmark's tip over the store, trimmed, or empty when
/// the bookmark does not resolve. A reconcile-time flatten preserves the branch's
/// own seal message (not a PR title) by passing this as the squash description.
pub(crate) fn branch_description(jj: &JjEnv, store: &Path, branch: &str) -> String {
    jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("bookmarks(exact:{branch:?})"),
            "--no-graph",
            "-T",
            "description",
            "--ignore-working-copy",
        ],
        "branch description",
    )
    .map(|s| s.trim().to_string())
    .unwrap_or_default()
}

/// Reset `branch` back to `tip` (a deliberate sideways/backwards move) and
/// re-export the ref to git. Used to UNDO a [`squash_branch_onto`] when a
/// post-squash guard in [`flatten_branch_recovery`] refuses the result: the squash
/// has already moved the bookmark, so without this a rejected flatten would leave
/// the visible branch rewritten. The git export is best-effort (a stale ref
/// self-heals on the next seal); the bookmark move is load-bearing and propagated.
pub(crate) fn restore_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    tip: &str,
) -> Result<(), String> {
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            tip,
            "--allow-backwards",
            "--ignore-working-copy",
        ],
        "flatten: restore bookmark after guard failure",
    )?;
    let _ = export_git_preserving_checkout(jj, store, true, "flatten: git export after restore");
    Ok(())
}

/// The store's current operation id (the newest entry in the op log). Paired with
/// [`restore_operation`] to snapshot store state before a multi-step mutation and
/// rewind to it if a later step fails.
///
/// EXACT ONLY UNDER THE PER-STORE LOCK: the id is only a faithful "pre-mutation"
/// marker if no other writer interleaves an op before the matching restore. The
/// caller MUST hold the per-store lock (as the merge fold and every Cairn jj
/// writer do via `resolve_store_lock`), under which every op between snapshot and
/// restore is the caller's own.
pub(crate) fn operation_id(jj: &JjEnv, store: &Path) -> Result<String, String> {
    jj.run(
        store,
        &[
            "op",
            "log",
            "--no-graph",
            "-n",
            "1",
            "-T",
            "id",
            "--ignore-working-copy",
        ],
        "jj op id",
    )
    .map(|s| s.trim().to_string())
}

/// Rewind the whole store to a prior operation `op_id` (an exact undo of every
/// bookmark move and commit rewrite since it), then re-export the backing git
/// refs so `refs/heads/*` realign with the restored bookmarks. Used to roll a
/// partially-applied merge back to its pre-merge snapshot so a push failure never
/// leaves local bookmarks diverged from origin.
///
/// EXACT ONLY UNDER THE PER-STORE LOCK: `jj op restore` restores whole-store
/// state, so any op another writer interleaved between the [`operation_id`]
/// snapshot and this restore would also be undone. The caller MUST hold the
/// per-store lock; under it every op since the snapshot is the caller's own and
/// the restore is precise.
pub(crate) fn restore_operation(jj: &JjEnv, store: &Path, op_id: &str) -> Result<(), String> {
    jj.run(
        store,
        &["op", "restore", op_id, "--ignore-working-copy"],
        "jj op restore",
    )?;
    export_git_preserving_checkout(jj, store, true, "jj git export (after op restore)")
}

/// Local bookmarks that ride a commit inside `range_revset`, excluding `exclude`.
/// A base advance that bakes conflicts into a branch's intermediate commits can
/// leave a SIBLING bookmark pointing at one of those intermediates (its own tip
/// is one of this branch's lineage commits, so it has no seals of its own). When
/// this branch is flattened, that rider is orphaned onto an abandoned lineage;
/// [`flatten_branch_recovery`] re-points each such rider onto the flattened
/// commit. `local_bookmarks` is the jj 0.42 template keyword for a commit's local
/// bookmarks; `--ignore-working-copy` keeps the enumeration store-driven.
fn local_bookmarks_in_range(
    jj: &JjEnv,
    store: &Path,
    range_revset: &str,
    exclude: &str,
) -> Vec<String> {
    jj.run(
        store,
        &[
            "log",
            "-r",
            range_revset,
            "--no-graph",
            "-T",
            "local_bookmarks.map(|b| b.name()).join(\"\\n\") ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (rider bookmarks in range)",
    )
    .unwrap_or_default()
    .lines()
    .map(str::trim)
    .filter(|name| !name.is_empty() && *name != exclude)
    .map(ToOwned::to_owned)
    .collect::<std::collections::BTreeSet<_>>()
    .into_iter()
    .collect()
}

/// The outcome of a guarded flatten recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlattenReport {
    /// The single commit the branch now points at: its tree equals the clean
    /// rebased tip and its parent is `dest_commit`.
    pub(crate) flattened_commit: String,
    /// How many conflicted intermediate commits the flatten collapsed away
    /// (advisory count).
    pub(crate) collapsed_conflicted_commits: usize,
    /// Orphaned twins of the PRE-flatten change-id that were abandoned in the
    /// twin-cleanup pass.
    pub(crate) abandoned_twins: Vec<String>,
    /// Sibling bookmarks that rode a flattened-away intermediate commit and were
    /// re-pointed onto the flattened commit (so a later `reconcile_siblings` does
    /// not resurrect the orphaned lineage).
    pub(crate) repointed_bookmarks: Vec<String>,
}

/// Guarded flatten recovery for a clean-tip / conflicted-intermediate branch.
///
/// A base advance that re-applies a whole lineage onto a new base can bake
/// conflicts into intermediate commits while a later commit resolves them, so the
/// NET tip is clean but jj still refuses to push/fold the branch (its history
/// contains conflicted commits). This collapses the branch to ONE commit on
/// `dest_commit` whose tree equals the clean rebased tip — clearing the conflicted
/// history while preserving the exact net tree — behind two guards:
///
/// - **Pre-guard:** the branch tip must genuinely descend from `dest_commit`.
///   Otherwise the squash would re-parent a stale tree onto dest and revert dest's
///   own files (the wrong-base reversion). Fails with a typed guard error.
/// - **Post-guard (footprint):** after the squash, the flattened tree must equal
///   the pre-flatten tip tree AND the flatten must delete no dest path that was
///   not already a deletion in the pre-flatten footprint. A violation is a
///   wrong-base/wrong-tip bug; the caller escalates rather than landing a
///   footprint-unsafe flatten.
///
/// Then a **twin-cleanup** pass: the squash mints a fresh change-id, so any commit
/// still sharing the PRE-flatten change-id is an orphaned twin (including the
/// "every twin conflicted" divergence that [`collapse_divergent_bookmark`] cannot
/// resolve). Now that a clean flattened commit exists they are safe to abandon.
///
/// The caller MUST hold the per-store lock (like [`collapse_divergent_bookmark`])
/// so the flatten cannot itself fork the op log. `message` becomes the flattened
/// commit's description (a PR title at a default landing, the branch's own seal
/// message at a reconcile).
pub(crate) fn flatten_branch_recovery(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest_commit: &str,
    message: &str,
) -> Result<FlattenReport, String> {
    let branch_revset = format!("bookmarks(exact:{branch:?})");

    // Pre-guard: the branch must genuinely descend from the dest it is about to be
    // flattened onto, or the squash re-parents a stale tree and reverts base files.
    if !revset_descends_from(jj, store, &branch_revset, dest_commit) {
        return Err(format!(
            "flatten guard: branch `{branch}` does not descend from dest `{dest_commit}`; refusing to flatten (would re-parent a stale tree and revert base files)"
        ));
    }

    let pre_tip = bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("flatten: branch `{branch}` did not resolve"))?;
    let pre_change_id = change_id_of(jj, store, branch);
    let pre_footprint = branch_footprint(jj, store, dest_commit, &pre_tip)?;
    let pre_tree = sealed_tree_hash_via_git(jj, store, &pre_tip).ok();
    let collapsed_conflicted_commits =
        conflicted_commits(jj, store, &format!("{dest_commit}..{branch_revset}")).len();

    // Enumerate sibling bookmarks riding the about-to-be-flattened lineage BEFORE
    // the squash (afterwards the range collapses and their commits leave it). Any
    // bookmark in `dest..branch` other than `branch` itself sits on one of this
    // branch's own lineage commits and would be orphaned onto an abandoned line by
    // the flatten; it is re-pointed onto the flattened commit once the guards pass.
    let riders = local_bookmarks_in_range(
        jj,
        store,
        &format!("{dest_commit}..{branch_revset}"),
        branch,
    );

    // Collapse the branch to one commit on dest whose tree = the clean rebased tip.
    squash_branch_onto(jj, store, branch, dest_commit, message)?;

    // From here on the bookmark has ALREADY been rewritten by the squash, so every
    // post-squash failure path — a footprint mismatch, a transient tree-hash read,
    // or an unresolved post tip — must first restore the bookmark to `pre_tip`, or a
    // refused flatten would leave the visible branch mutated despite reporting a
    // refusal (the load-bearing safety guarantee). `fail` performs that restore and
    // returns the reason to hand back as the `Err`.
    let fail = |reason: String| -> String {
        if let Err(e) = restore_bookmark(jj, store, branch, &pre_tip) {
            log::warn!(
                "flatten: failed to restore bookmark {branch} to {pre_tip} after a post-squash guard failure: {e}"
            );
        }
        reason
    };

    let post_tip = match bookmark_commit(jj, store, branch) {
        Some(commit) => commit,
        None => {
            return Err(fail(format!(
                "flatten: branch `{branch}` did not resolve after squash"
            )))
        }
    };

    // Post-guard (footprint): a wrong-base/wrong-tip squash would delete dest files
    // the branch never touched. Reject any NEW deletion, naming the offending paths.
    let post_footprint = match branch_footprint(jj, store, dest_commit, &post_tip) {
        Ok(footprint) => footprint,
        Err(e) => return Err(fail(e)),
    };
    let new_deletions: Vec<String> = post_footprint
        .deletions
        .difference(&pre_footprint.deletions)
        .cloned()
        .collect();
    if !new_deletions.is_empty() {
        return Err(fail(format!(
            "flatten footprint guard: flattening `{branch}` onto `{dest_commit}` would delete base file(s) the branch did not delete: {}. Refusing (wrong-base/wrong-tip flatten).",
            new_deletions.join(", ")
        )));
    }
    // The restore should have copied the tip tree exactly; verify byte-for-byte via
    // the git tree object when it resolves (advisory fallback: skip on git hiccup).
    if let Some(pre_tree) = pre_tree {
        let post_tree = match sealed_tree_hash_via_git(jj, store, &post_tip) {
            Ok(tree) => tree,
            Err(e) => return Err(fail(e)),
        };
        if post_tree != pre_tree {
            return Err(fail(format!(
                "flatten footprint guard: flattened `{branch}` tree {post_tree} does not match the pre-flatten tip tree {pre_tree}. Refusing (wrong-base/wrong-tip flatten)."
            )));
        }
    }

    // Re-point every rider sibling onto the flattened commit BEFORE the twin
    // cleanup. Bookmark preservation is fail-closed: abandoning a bookmark head
    // and its parent together can delete the bookmark, so no candidate lineage is
    // abandoned unless every affected bookmark has a proven surviving target.
    let mut repointed_bookmarks = Vec::new();
    for rider in riders {
        match jj.run(
            store,
            &[
                "bookmark",
                "set",
                &rider,
                "-r",
                &post_tip,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "flatten: re-point rider bookmark",
        ) {
            Ok(_) => {
                let _ = export_git_preserving_checkout(
                    jj,
                    store,
                    true,
                    "flatten: git export after rider re-point",
                );
                repointed_bookmarks.push(rider);
            }
            Err(e) => {
                return Err(fail(format!(
                    "flatten: could not safely re-point bookmark `{rider}` to surviving commit {post_tip}; no orphaned lineage was abandoned: {e}"
                )))
            }
        }
    }

    // Twin cleanup: the squash minted a fresh change-id, so any commit still
    // sharing the PRE-flatten change-id is an orphaned twin (the old lineage tip,
    // or every twin of an ambiguous conflicted divergence). Drop them now that a
    // clean flattened commit exists.
    let flattened_change_id = change_id_of(jj, store, branch);
    let mut abandoned_twins = Vec::new();
    if !pre_change_id.is_empty() && pre_change_id != flattened_change_id {
        for commit in visible_commit_ids_for_change(jj, store, &pre_change_id) {
            if commit == post_tip {
                continue;
            }
            let affected = local_bookmarks_in_range(jj, store, &commit, branch);
            for bookmark in &affected {
                jj.run(
                    store,
                    &[
                        "bookmark",
                        "set",
                        bookmark,
                        "-r",
                        &post_tip,
                        "--allow-backwards",
                        "--ignore-working-copy",
                    ],
                    "flatten: preserve twin bookmark",
                )
                .map_err(|error| {
                    fail(format!(
                        "flatten: refusing to abandon {commit}: bookmark `{bookmark}` could not be moved to proven survivor {post_tip}: {error}"
                    ))
                })?;
                if bookmark_commit(jj, store, bookmark).as_deref() != Some(post_tip.as_str()) {
                    return Err(fail(format!(
                        "flatten: refusing to abandon {commit}: bookmark `{bookmark}` did not resolve to proven survivor {post_tip} after re-point"
                    )));
                }
                if !repointed_bookmarks.contains(bookmark) {
                    repointed_bookmarks.push(bookmark.clone());
                }
            }
            match jj.run(
                store,
                &["abandon", &commit, "--ignore-working-copy"],
                "flatten: abandon orphaned twin",
            ) {
                Ok(_) => abandoned_twins.push(commit),
                Err(e) => log::warn!("flatten: abandoning orphaned twin {commit} failed: {e}"),
            }
        }
    }

    Ok(FlattenReport {
        flattened_commit: post_tip,
        collapsed_conflicted_commits,
        abandoned_twins,
        repointed_bookmarks,
    })
}

/// Whether this project has an `origin` remote. Publication sites use absence as a
/// successful nothing-to-publish result. If discovery fails, callers preserve the
/// configured-origin failure signal rather than silently treating unreadable remote
/// configuration as local-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OriginPresence {
    Present,
    Absent,
    Unknown,
}

pub(crate) fn discover_origin_presence(jj: &JjEnv, repo: &Path) -> OriginPresence {
    match jj.run(
        repo,
        &["git", "remote", "list", "--ignore-working-copy"],
        "jj git remote list",
    ) {
        Ok(remotes)
            if remotes.lines().any(|line| {
                line.split_whitespace()
                    .next()
                    .is_some_and(|name| name == "origin")
            }) =>
        {
            OriginPresence::Present
        }
        Ok(_) => OriginPresence::Absent,
        Err(error) => {
            log::warn!("jj reconcile: origin discovery failed; attempting publication conservatively: {error}");
            OriginPresence::Unknown
        }
    }
}

pub(crate) fn publish_reconciled_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    origin: OriginPresence,
) -> Result<(), String> {
    if origin == OriginPresence::Absent {
        return Ok(());
    }
    push_store_bookmark(jj, store, branch)
}

#[derive(Clone, Copy)]
struct ReconcilePublication {
    clean: bool,
    flattened: bool,
}

/// Classify one sibling that is already positioned on `dest_commit` (either it was
/// just rebased there, or it already descended from it) into the reconcile report,
/// applying proactive flatten recovery for the clean-tip / conflicted-intermediate
/// case. `dest_commit` is `None` only when the reconcile dest was unresolvable, in
/// which case this falls back to the bare tip-conflict check (liveness over
/// strictness). `publication.clean` advances a cleanly-rebased sibling's PR head
/// when an `origin` remote is configured. A local-only project has nothing to publish and
/// remains clean; failure to reach a configured origin is reported as blocking so
/// the PR head cannot silently remain stale. A FLATTENED sibling always attempts
/// publication because its commit id changed. The caller holds the per-store lock,
/// so the flatten cannot fork.
fn classify_reconciled_sibling(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    ws_path: &Path,
    dest_commit: Option<&str>,
    publication: ReconcilePublication,
    report: &mut ReconcileReport,
) {
    let state = dest_commit.and_then(|dest| flatten_state(jj, store, dest, branch).ok());
    match state {
        // No concrete dest to classify against: fall back to the tip-conflict check.
        None => match branch_has_conflict(jj, store, branch) {
            Ok(true) => report.conflicted.push(branch.to_string()),
            Ok(false) => {
                if publication.clean {
                    if let Err(e) = publish_reconciled_bookmark(
                        jj,
                        store,
                        branch,
                        discover_origin_presence(jj, store),
                    ) {
                        log::warn!("jj reconcile: push rebased sibling {branch} failed: {e}");
                        record_failure(report, branch, ws_path, e);
                        return;
                    }
                }
                report.rebased_clean.push(branch.to_string());
            }
            Err(e) => {
                log::warn!("jj reconcile: conflict check for {branch} failed: {e}");
                record_failure(report, branch, ws_path, e);
            }
        },
        Some(FlattenState::Clean) => {
            if publication.clean {
                if let Err(e) = publish_reconciled_bookmark(
                    jj,
                    store,
                    branch,
                    discover_origin_presence(jj, store),
                ) {
                    log::warn!("jj reconcile: push rebased sibling {branch} failed: {e}");
                    record_failure(report, branch, ws_path, e);
                    return;
                }
            }
            report.rebased_clean.push(branch.to_string());
        }
        // A genuinely-conflicted tip needs the agent to resolve markers; a
        // conflicted commit can never push, so it is stop-the-line.
        Some(FlattenState::TipConflicted) => report.conflicted.push(branch.to_string()),
        // Clean tip, conflicted intermediates: heal it in place so the branch stays
        // pushable/mergeable at all times (no silent seal-push failure, no
        // misleading "clean" note while the merge is actually blocked).
        Some(FlattenState::IntermediateOnly) => {
            let dest = dest_commit.expect("IntermediateOnly implies a concrete dest");
            let desc = branch_description(jj, store, branch);
            let message = if desc.is_empty() {
                format!("Flatten {branch} onto base (auto-recovery)")
            } else {
                desc
            };
            match flatten_branch_recovery(jj, store, branch, dest, &message) {
                Ok(recovered) => {
                    // Re-parent the sibling's live workspace onto the flattened
                    // commit so its `@` no longer sits on the abandoned conflicted
                    // line; this refreshes the on-disk files via update-stale.
                    if let Err(e) = reconcile_managed_workspace(
                        jj,
                        store,
                        ws_path,
                        branch,
                        Some(&recovered.flattened_commit),
                    ) {
                        log::warn!(
                            "jj reconcile: re-parent workspace {branch} onto flattened tip failed: {e}"
                        );
                        record_failure(report, branch, ws_path, e);
                        return;
                    }
                    // The flatten rewrote the commit id, so the PR head must move
                    // even when a plain clean rebase would have been skipped.
                    if publication.flattened {
                        if let Err(e) = publish_reconciled_bookmark(
                            jj,
                            store,
                            branch,
                            discover_origin_presence(jj, store),
                        ) {
                            log::warn!("jj reconcile: push flattened sibling {branch} failed: {e}");
                            record_failure(report, branch, ws_path, e);
                            return;
                        }
                    }
                    log::info!(
                        "jj reconcile: flattened {branch} ({} conflicted intermediate(s) collapsed, {} twin(s) abandoned)",
                        recovered.collapsed_conflicted_commits,
                        recovered.abandoned_twins.len()
                    );
                    report.rebased_clean.push(branch.to_string());
                }
                Err(e) => {
                    // Guard failure: a footprint-unsafe flatten. Fall back to a
                    // conflicted classification so the agent is interrupted rather
                    // than the branch silently wedged.
                    log::warn!(
                        "jj reconcile: flatten of {branch} refused by guard ({e}); classifying conflicted"
                    );
                    report.conflicted.push(branch.to_string());
                }
            }
        }
    }
}

/// Stable failure classification consumed by durable reconcile progress. String
/// matching is quarantined here at the jj adapter boundary; orchestration stores
/// only this closed vocabulary.
pub(crate) fn reconcile_failure_is_permanent(kind: &str) -> bool {
    matches!(
        kind,
        "immutable_commit" | "conflicted_bookmark" | "ambiguous_divergence" | "missing_bookmark"
    )
}

pub(crate) fn reconcile_failure_kind(error: &str) -> &'static str {
    let error = error.to_ascii_lowercase();
    if error.contains("immutable") {
        "immutable_commit"
    } else if error.contains("name") && error.contains("conflicted") {
        "conflicted_bookmark"
    } else if error.contains("ambiguous") || error.contains("divergent") {
        "ambiguous_divergence"
    } else if error.contains("bookmark")
        && (error.contains("missing") || error.contains("not found"))
    {
        "missing_bookmark"
    } else if error.contains("durable base") || error.contains("persist") {
        "persistence"
    } else if error.contains("notify") || error.contains("message") {
        "notification"
    } else {
        "transient_process"
    }
}

/// Reconcile in-flight siblings onto the locally-advanced integration tip: the
/// store already owns the merge (the child's commit was folded into the
/// integration bookmark by `merge_into_bookmark`), so there is no fetch or origin
/// round-trip — each sibling bookmark rebases non-blockingly onto the local
/// integration bookmark, its workspace refreshes, and a cleanly-rebased sibling
/// is pushed so its PR head advances on origin. This replaces the git "notify
/// each sibling to rebase + force-push" tax: conflicts are recorded (not
/// blocking), change-IDs are preserved, and no force-push is needed.
///
/// `siblings` pairs each in-flight sibling's bookmark with its workspace dir.
/// Every step is best-effort per sibling: a failure on one is logged and the
/// rest proceed. A conflicted sibling is not pushed (and `jj git push` would
/// refuse it anyway — the self-enforcing backstop). Returns which siblings landed
/// clean versus conflicted.
pub fn reconcile_siblings(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    siblings: &[(String, PathBuf)],
) -> Result<ReconcileReport, String> {
    reconcile_siblings_with_publication(jj, store, integration_branch, siblings, true)
}

pub(crate) fn reconcile_siblings_without_publication(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    siblings: &[(String, PathBuf)],
) -> Result<ReconcileReport, String> {
    reconcile_siblings_with_publication(jj, store, integration_branch, siblings, false)
}

fn reconcile_siblings_with_publication(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    siblings: &[(String, PathBuf)],
    publish: bool,
) -> Result<ReconcileReport, String> {
    let mut report = ReconcileReport::default();
    // Resolve the rebase dest to a concrete commit id ONCE up front: it may be a
    // bookmark name or a `<default>@origin` remote ref, and pinning it keeps the
    // dest from moving mid-loop and lets each sibling check whether it already
    // descends from this exact commit. None (unresolvable dest) disables the
    // skip and falls back to the unconditional rebase below.
    let dest_commit = jj
        .run(
            store,
            &[
                "log",
                "-r",
                integration_branch,
                "--no-graph",
                "-T",
                "commit_id",
            ],
            "jj resolve reconcile dest commit",
        )
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Stop-the-line guard: never hand a conflicted base to clean siblings. If the
    // rebase dest itself carries a recorded conflict, hold every sibling on its
    // prior clean commit (no rewrite, nothing pushed) and classify them `held` —
    // neither rebased_clean nor conflicted, so the notify layer fires nothing for
    // them. The conflict must be resolved AT THE BASE first; the next reconcile
    // sees a clean dest, the guard does not fire, and the per-sibling descends
    // skip handles the rest (self-clearing). A transient check error falls to
    // `false` (proceed) — the same liveness-over-strictness convention as
    // `revset_descends_from`; only a confirmed conflicted dest holds the line, so
    // a flaky check never wedges every reconcile.
    if revset_has_conflict(jj, store, integration_branch).unwrap_or(false) {
        log::warn!(
            "jj reconcile: rebase dest {integration_branch} carries a conflict; holding {} sibling(s) off the conflicted base",
            siblings.len()
        );
        report.held = siblings.iter().map(|(branch, _)| branch.clone()).collect();
        return Ok(report);
    }

    // Missing-bookmark siblings are filtered upstream in `reconcile_base_advance`
    // (one store-wide bookmark list before ANY per-sibling jj work), so every
    // sibling reaching this loop has a live bookmark. See `retain_present_siblings`.
    for (branch, ws_path) in siblings {
        let already_on_dest = dest_commit
            .as_deref()
            .map(|dest| branch_descends_from(jj, store, branch, dest))
            .unwrap_or(false);
        let mut push_clean = !already_on_dest;
        let mut trivial_fast_forward = false;

        if !already_on_dest {
            if let Some(dest) = dest_commit.as_deref() {
                if branch_is_ancestor_of(jj, store, branch, dest) {
                    if let Err(error) = fast_forward_bookmark(jj, store, branch, dest) {
                        log::warn!("jj reconcile: fast-forward {branch} to {dest} failed: {error}");
                        record_failure(&mut report, branch, ws_path, error);
                        continue;
                    }
                    push_clean = false;
                    trivial_fast_forward = true;
                } else if let Err(error) = rebase_branch_onto(jj, store, branch, integration_branch)
                {
                    log::warn!(
                        "jj reconcile: rebase {branch} onto {integration_branch} failed: {error}"
                    );
                    record_failure(&mut report, branch, ws_path, error);
                    continue;
                }
            } else if let Err(error) = rebase_branch_onto(jj, store, branch, integration_branch) {
                log::warn!(
                    "jj reconcile: rebase {branch} onto {integration_branch} failed: {error}"
                );
                record_failure(&mut report, branch, ws_path, error);
                continue;
            }
        }

        let workspace_outcome =
            match reconcile_managed_workspace(jj, store, ws_path, branch, dest_commit.as_deref()) {
                Ok(outcome) => outcome,
                Err(error) => {
                    log::warn!(
                        "jj reconcile: managed workspace {} failed: {error}",
                        ws_path.display()
                    );
                    record_failure(&mut report, branch, ws_path, error);
                    continue;
                }
            };

        if workspace_outcome == ManagedWorkspaceReconcileOutcome::AdvancedConflicted {
            report.conflicted.push(branch.clone());
            continue;
        }
        if trivial_fast_forward {
            report.rebased_clean.push(branch.clone());
            report.silent.push(branch.clone());
            continue;
        }

        let clean_len = report.rebased_clean.len();
        classify_reconciled_sibling(
            jj,
            store,
            branch,
            ws_path,
            dest_commit.as_deref(),
            ReconcilePublication {
                clean: push_clean && publish,
                flattened: publish,
            },
            &mut report,
        );
        if workspace_outcome == ManagedWorkspaceReconcileOutcome::PreservedDirty
            && report.rebased_clean.len() > clean_len
        {
            report.rebased_clean.retain(|candidate| candidate != branch);
            report.preserved_dirty.push(branch.clone());
        }
    }
    Ok(report)
}

/// Advance an active workspace that sits ON `dest`'s branch (a Coordinator on its
/// integration bookmark) after that bookmark was folded forward out from under
/// the workspace's working copy. The sibling auto-rebase ([`reconcile_siblings`])
/// only touches workspaces branched *from* the branch; the workspace *on* the
/// branch has its `@` re-parented onto the new tip here, then its on-disk files
/// refreshed — the jj-native form of the old git "post-merge fast-forward of
/// active worktrees".
///
/// Store-driven for consistency with [`reconcile_siblings`]: `jj rebase -s
/// <name>@ -o <dest>` over the store re-parents the workspace's working-copy
/// commit (`<name>` is [`workspace_name_for_branch`]), then [`update_stale`]
/// materializes the new files (and any conflict markers) on disk.
/// `--ignore-working-copy` because the rebase is driven from the store, not the
/// workspace. Idempotent: when `@` already sits on `dest`, jj reports "Nothing
/// changed" and this is a no-op, so it is safe under the merge/webhook
/// double-fire.
///
/// Guaranteed-safe by the forward-only fold: a successful `merge_into_bookmark`
/// means the new tip is a descendant of the prior integration bookmark, hence of
/// every coordinator seal, so re-parenting the coordinator's empty/idle `@` onto
/// it never drops coordinator work.
///
/// Returns whether the re-parent recorded a conflict (non-blocking: jj
/// materializes it for the agent rather than failing). A Coordinator's `@` is
/// empty/idle, so re-parenting it never conflicts in practice; the signal exists
/// so a caller can wake a workspace that somehow lands on a conflicted `@` rather
/// than leaving it idle there.
pub(crate) fn advance_workspace_onto(
    jj: &JjEnv,
    store: &Path,
    ws_path: &Path,
    ws_branch: &str,
    dest: &str,
) -> Result<bool, String> {
    // Skip a workspace whose directory is gone: the rebase drives from the store,
    // but `update_stale` (and the conflict check below) operate inside `ws_path`,
    // so a missing workspace can only fail. A base advance that still lists a
    // long-reclaimed on-branch workspace would otherwise spawn a doomed rebase.
    if !ws_path.exists() {
        log::debug!(
            "jj advance: workspace {} no longer exists; skipping",
            ws_path.display()
        );
        return Ok(false);
    }
    let source = format!("{}@", workspace_name_for_branch(ws_branch));
    // Idempotent skip: when `@` already descends from `dest`, a re-rebase would
    // re-rewrite the working-copy commit (and could mint a divergent copy under
    // the merge/webhook double-fire). `dest` here is a concrete commit id
    // (resolved by the caller via `bookmark_commit`). Nothing to move, no
    // conflict to surface — report a clean no-op.
    if revset_descends_from(jj, store, &source, dest) {
        return Ok(false);
    }
    jj.run(
        store,
        &["rebase", "-s", &source, "-o", dest, "--ignore-working-copy"],
        "jj rebase (advance workspace onto branch tip)",
    )?;
    // Refresh the live workspace: the rebase rewrote its `@` out from under it.
    update_stale(jj, ws_path)?;
    // A conflict from the re-parent lands in the rewritten working-copy commit.
    let out = jj.run(
        ws_path,
        &["log", "-r", "@", "--no-graph", "-T", "self.conflict()"],
        "jj advance conflict check",
    )?;
    Ok(out.contains("true"))
}
