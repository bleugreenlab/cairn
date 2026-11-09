//! Guarded flatten recovery and sibling reconcile onto an advanced tip.
use super::*;
use std::path::Path;

fn record_failure(report: &mut ReconcileReport, branch: &str, error: String) {
    report.failed.push(ReconcileFailure {
        branch: branch.to_string(),
        error,
    });
}

/// Record a rolled-back conflicting rebase, carrying the paths captured inside
/// the rebase out to the notification layer.
///
/// The caller MUST `continue` past `classify_reconciled_sibling` after this. That
/// is load-bearing: the rebase was undone, so the branch is genuinely clean
/// again, and classification would read it as `Clean`, publish it, and push it —
/// silently reporting a successful reconcile for a branch that never moved.
fn record_conflict(report: &mut ReconcileReport, branch: &str, diagnostic: ConflictDiagnostic) {
    let paths = diagnostic.conflicting_paths();
    log::warn!(
        "jj reconcile: {branch} hit a {} against the advanced base; the rebase was rolled back and \
         the branch is unchanged. Conflicting file(s): {}. The incoming change also carries {} \
         file(s) that arrive cleanly on retry.",
        diagnostic.condition.as_str(),
        if paths.is_empty() {
            "<none enumerated>".to_string()
        } else {
            paths.join(", ")
        },
        diagnostic.clean_on_retry_paths().len()
    );
    report.conflicted.push(branch.to_string());
    report
        .conflict_diagnostics
        .insert(branch.to_string(), diagnostic);
}

/// Restore only the explicitly observed source after a failed squash export.
/// A concurrent owner commit wins; its head is never replaced by rollback.
pub(crate) fn restore_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    expected_current: &str,
    tip: &str,
) -> Result<(), String> {
    super::packed_refs::with_packed_refs_patience_for(
        jj,
        PackedRefs::JjStore(store),
        "restore exact squash source",
        || {
            crate::repoint_bookmark_if_unchanged(
                store,
                branch,
                &[expected_current.to_string()],
                tip,
                "restore exact source after refused squash export",
            )
        },
    )?;
    super::advance::export_accepted_branch(jj, store, branch, tip)
}

/// The store's current operation id (the newest entry in the op log). Paired with
/// [`restore_operation`] to snapshot the store's local view before a multi-step
/// mutation and rewind it if a later step fails.
///
/// The caller MUST hold the per-store lock (as the merge fold and every Cairn jj
/// writer of commits and local bookmarks do via `resolve_store_lock`), so every
/// LOCAL-VIEW op between snapshot and restore is the caller's own. Pushes and
/// fetches deliberately run without the lock and may interleave; they write only
/// remote-tracking state, which [`restore_operation`] leaves alone.
///
/// Not a pure read: like every unpinned jj command it merges a split operation
/// log first and reports the merge. A read that must not settle the store uses
/// [`crate::settled_operation`] instead.
///
/// In a budgeted environment the read also snapshots every local bookmark and
/// backing Git head at that operation, so a later [`restore_operation`] to it
/// can be verified rather than assumed.
pub fn operation_id(jj: &JjEnv, store: &Path) -> Result<String, String> {
    let op_id = jj
        .run(
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
        .map(|s| s.trim().to_string())?;
    if jj.is_budgeted() {
        let snapshot = super::recovery::RollbackSnapshot::capture(store, &op_id);
        jj.ledger().remember(snapshot);
    }
    Ok(op_id)
}

/// Rewind the store's LOCAL VIEW to a prior operation `op_id` (an exact undo of
/// every commit rewrite and local bookmark move since it), then re-export the
/// backing git refs so `refs/heads/*` realign with the restored bookmarks. Used
/// to roll a partially-applied merge, rebase, or seal back to its snapshot.
///
/// Remote-tracking bookmarks (`X@origin`, their targets and tracked state) are
/// deliberately NOT restored (`--what repo`). They record facts about origin: a
/// push that landed between the snapshot and this restore really is on origin,
/// and a local rollback cannot take it back. Rewinding them anyway is what made a
/// merge preview's rollback erase a concurrent PR push's tracking — jj forgot the
/// branch was on origin, the export deleted `refs/remotes/origin/<branch>`, and
/// every later push of that branch was refused as stale and then as untracked.
/// Pushes and fetches run without the per-store lock by design, so this is the
/// only form of restore that stays exact while they interleave.
///
/// The caller MUST hold the per-store lock; under it every local-view op since
/// the snapshot is the caller's own.
///
/// In a budgeted environment the restore is verified against the snapshot
/// [`operation_id`] took: every local bookmark back at its target and every
/// touched bookmark's Git head with it. An unverified restore returns a
/// [`rollback_incomplete`] error AND leaves typed recovery coordinates in the
/// transaction's ledger (see [`JjEnv::take_recoveries`]).
pub fn restore_operation(jj: &JjEnv, store: &Path, op_id: &str) -> Result<(), String> {
    if jj.is_budgeted() {
        return super::recovery::restore_recorded(jj, store, op_id);
    }
    // Patient, because every rollback depends on it. Restoring to the same
    // operation twice yields the same view, so a repeat is idempotent.
    super::packed_refs::with_packed_refs_patience_for(
        jj,
        PackedRefs::JjStore(store),
        "jj op restore",
        || {
            jj.run(
                store,
                &[
                    "op",
                    "restore",
                    "--what",
                    "repo",
                    op_id,
                    "--ignore-working-copy",
                ],
                "jj op restore",
            )
        },
    )?;
    // The one export with no expectation to name: an op restore rewinds every
    // local bookmark at once, so the set it realigns is not known here and there
    // is no single (branch, commit) invariant to assert. Every other export site
    // advances a specific bookmark and passes it.
    export_git_preserving_checkout(jj, store, true, "jj git export (after op restore)")
}

/// [`restore_operation`], then the principal bookmark's verified export. A
/// budgeted restore already proved every touched bookmark's Git head, so the
/// second export is only for unbudgeted callers, which have no snapshot.
pub(crate) fn restore_operation_and_export(
    jj: &JjEnv,
    store: &Path,
    op_id: &str,
    branch: &str,
    ctx: &str,
) -> Result<(), String> {
    restore_operation(jj, store, op_id)?;
    if jj.is_budgeted() {
        return Ok(());
    }
    super::export_bookmark_advance(jj, store, true, branch, ctx)
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
) -> Result<(), StoreBookmarkPushError> {
    if origin == OriginPresence::Absent {
        return Ok(());
    }
    push_store_bookmark_classified(jj, store, branch)
}

#[derive(Clone, Copy)]
struct ReconcilePublication {
    clean: bool,
    healed: bool,
}

/// Where a branch this pass rebased stood immediately before the rebase, so a
/// classification that cannot keep the rebased result can put it back.
pub(crate) struct RebaseRollback {
    pub(crate) pre_op: String,
    pub(crate) pre_tip: Option<String>,
}

/// Whether a heal refusal came from one of its guards — a positive finding that
/// this history cannot be healed exactly — rather than from jj failing to run.
/// The prefix is the one [`heal_conflicted_ancestry`] writes for its guards; the
/// heal's verified export refusing a tree that carries conflict scaffolding is
/// the same kind of finding, reached through the export guard.
fn heal_refusal_is_guard(error: &str) -> bool {
    error.starts_with(ANCESTRY_HEAL_GUARD) || is_conflict_scaffolding_export_refusal(error)
}

/// Undo this pass's rebase of a branch whose conflicted history could not be
/// healed, and say truthfully why.
///
/// Left in place, the branch would sit on a rebased tip with conflict-flagged
/// ancestors under a `conflicted` label that names no path, and nothing would
/// open a session for it. A guard refusal is a finding about the tree, so it is
/// recorded as a conflict with coordinates captured before the rollback erases
/// them. Anything else is jj failing to act — a store lock, a transient process
/// error — and is recorded as a failure, which the durable worker retries.
///
/// The rollback is what keeps an agent's committed resolution intact: the
/// branch returns to the tip the agent committed, never to a collapsed copy.
pub(crate) fn roll_back_refused_heal(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
    rollback: &RebaseRollback,
    refusal: String,
    report: &mut ReconcileReport,
) {
    let diagnostic = heal_refusal_is_guard(&refusal).then(|| {
        let paths: Vec<String> =
            conflicted_commits(jj, store, &format!("({dest})..bookmarks(exact:{branch:?})"))
                .into_iter()
                .flat_map(|commit| commit.files)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
        capture_conflict_diagnostic(jj, store, branch, dest, rollback.pre_tip.as_deref(), paths)
    });
    let cleanup = jj.cleanup_env();
    let jj = &cleanup;
    if let Err(error) = restore_operation(jj, store, &rollback.pre_op) {
        log::error!(
            "jj reconcile: heal of {branch} was refused ({refusal}) and the rebase could not be \
             rolled back to {}: {error}",
            rollback.pre_op
        );
        record_failure(
            report,
            branch,
            rollback_incomplete(&format!(
                "healing the rebased branch's conflict-flagged history was refused ({refusal}), \
                 and rolling the rebase back to operation {} also failed: {error}. The branch may \
                 sit on conflict-flagged history; do not publish it.",
                rollback.pre_op
            )),
        );
        return;
    }
    // A budgeted restore already proved every touched Git head.
    let exported = if jj.is_budgeted() {
        Ok(())
    } else {
        export_bookmark_advance(
            jj,
            store,
            true,
            branch,
            "jj git export (refused heal rolled back)",
        )
    };
    if let Err(error) = exported {
        record_failure(
            report,
            branch,
            rollback_incomplete(&format!(
                "heal refused ({refusal}); the store was rolled back, but exporting the \
                 restored branch failed, so git may still name the rebased tip: {error}"
            )),
        );
        return;
    }
    match diagnostic {
        Some(diagnostic) => {
            log::warn!(
                "jj reconcile: heal of {branch} refused by guard ({refusal}); rebase rolled \
                 back and recorded as a conflict"
            );
            record_conflict(report, branch, diagnostic);
        }
        None => {
            log::warn!(
                "jj reconcile: heal of {branch} could not run ({refusal}); rebase rolled back \
                 and left for retry"
            );
            record_failure(
                report,
                branch,
                format!(
                    "the rebased branch carries conflict-flagged history that must be healed, \
                     and the heal could not run: {refusal}. The rebase was rolled back, so the \
                     branch is unchanged; it is retried."
                ),
            );
        }
    }
}

/// Record a heal refused for a branch that already sat on its dest before
/// this pass, so there is no rebase of this pass's to roll back.
///
/// A guard refusal is a finding about the tree, and the agent is interrupted
/// rather than the branch silently wedged. Anything else is jj failing to act
/// — above all a lost `packed-refs.lock` — and is a retried failure, never a
/// content conflict: there is nothing in the tree for the agent to resolve.
pub(crate) fn record_unmoved_heal_refusal(
    report: &mut ReconcileReport,
    branch: &str,
    refusal: String,
) {
    if refusal.contains(ROLLBACK_INCOMPLETE) {
        log::error!("jj reconcile: heal of {branch} failed and was not fully undone: {refusal}");
        record_failure(
            report,
            branch,
            format!("healing this branch's conflict-flagged history failed: {refusal}"),
        );
        return;
    }
    if heal_refusal_is_guard(&refusal) {
        log::warn!(
            "jj reconcile: heal of {branch} refused by guard ({refusal}); classifying conflicted"
        );
        report.conflicted.push(branch.to_string());
        return;
    }
    log::warn!("jj reconcile: heal of {branch} could not run ({refusal}); left for retry");
    record_failure(
        report,
        branch,
        format!(
            "the branch carries conflict-flagged history that must be healed, and the heal \
             could not run: {refusal}. Nothing was rebased this pass, so the branch is unchanged; \
             it is retried."
        ),
    );
}

/// Classify one sibling that is already positioned on `dest_commit` (either it was
/// just rebased there, or it already descended from it) into the reconcile report,
/// healing the clean-tip / conflicted-intermediate case in place with
/// [`heal_conflicted_ancestry`]. `dest_commit` is `None` only when the reconcile dest was unresolvable, in
/// which case this falls back to the bare tip-conflict check (liveness over
/// strictness). `publication.clean` advances a cleanly-rebased sibling's PR head
/// when an `origin` remote is configured. A local-only project has nothing to publish and
/// remains clean; failure to reach a configured origin is reported as blocking so
/// the PR head cannot silently remain stale. A HEALED sibling always attempts
/// publication because its commit ids changed. The caller holds the per-store lock,
/// so the heal cannot fork.
fn classify_reconciled_sibling(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest_commit: Option<&str>,
    publication: ReconcilePublication,
    rollback: Option<&RebaseRollback>,
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
                        record_failure(report, branch, e.to_string());
                        return;
                    }
                }
                report.rebased_clean.push(branch.to_string());
            }
            Err(e) => {
                log::warn!("jj reconcile: conflict check for {branch} failed: {e}");
                record_failure(report, branch, e.to_string());
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
                    record_failure(report, branch, e.to_string());
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
        // misleading "clean" note while the merge is actually blocked). The heal
        // resolves each conflict-flagged commit where it stands; it never
        // collapses the branch, so an integration branch keeps every commit it
        // carries, child-PR landings included.
        Some(FlattenState::IntermediateOnly) => {
            let dest = dest_commit.expect("IntermediateOnly implies a concrete dest");
            match heal_conflicted_ancestry(jj, store, branch, dest) {
                Ok(heal) => {
                    // The heal rewrote commit ids, so the PR head must move even
                    // when a plain clean rebase would have been skipped.
                    if publication.healed {
                        if let Err(e) = publish_reconciled_bookmark(
                            jj,
                            store,
                            branch,
                            discover_origin_presence(jj, store),
                        ) {
                            log::warn!("jj reconcile: push healed sibling {branch} failed: {e}");
                            record_failure(report, branch, e.to_string());
                            return;
                        }
                    }
                    log::info!(
                        "jj reconcile: healed {branch} in place ({} conflict-flagged commit(s) resolved, {} emptied commit(s) dropped)",
                        heal.healed.len(),
                        heal.dropped.len()
                    );
                    report.rebased_clean.push(branch.to_string());
                }
                Err(e) => match rollback {
                    // This pass moved the branch, so it puts it back.
                    Some(rollback) => {
                        roll_back_refused_heal(jj, store, branch, dest, rollback, e, report)
                    }
                    None => record_unmoved_heal_refusal(report, branch, e),
                },
            }
        }
    }
}

/// Stable failure classification consumed by durable reconcile progress. String
/// matching is quarantined here at the jj adapter boundary; orchestration stores
/// only this closed vocabulary.
/// A permanent failure is never retried and never repaired. `conflicted_bookmark`
/// is deliberately NOT in this set: a conflicted tracked bookmark is a
/// reconciliation TODO that [`crate::jj::reconcile_tracked_bookmark`] now clears,
/// so recording it as terminal was the exact shape this system must not have —
/// runner-owned state failing into a dead end no agent can reach. It is retried
/// instead, and the next base advance repairs the bookmark on its way through.
pub fn reconcile_failure_is_permanent(kind: &str) -> bool {
    matches!(
        kind,
        "immutable_commit" | "ambiguous_divergence" | "missing_bookmark" | MERGE_CATCH_UP_REFUSED
    )
}

///
/// A lost `packed-refs.lock` is checked first, because the lock error's own
/// text can carry words the later arms key on. It is infrastructure, never a
/// finding about the branch: `ref_lock_stale` when patience gave up on an
/// abandoned lock it could not remove, `ref_lock_contention` otherwise.
///
/// One cause outranks even that: a rollback that did not complete. Such an
/// error often quotes a lost lock as its cause, and filing it under the lock
/// would tell everyone the branch is unchanged when it may not be.
pub fn reconcile_failure_kind(error: &str) -> &'static str {
    if error.contains(ROLLBACK_INCOMPLETE) {
        return ROLLBACK_FAILED;
    }
    if is_jj_timeout_error(error) {
        return RECONCILE_TIMEOUT;
    }
    if error.contains(STALE_DIAGNOSIS) {
        return REF_LOCK_STALE;
    }
    // Before the word arms below: the refusal names an ambiguous merge base,
    // which is not a divergent bookmark.
    if error.starts_with(MERGE_CATCH_UP_REFUSAL) {
        return MERGE_CATCH_UP_REFUSED;
    }
    if error.contains(CONTENTION_DIAGNOSIS) || packed_refs_lock_error(error) {
        return REF_LOCK_CONTENTION;
    }
    let error = error.to_ascii_lowercase();
    if error.contains("immutable") {
        "immutable_commit"
    } else if error.contains(crate::jj::CONFLICTED_NAME_REFUSAL)
        || (error.contains("name `") && error.contains("` is conflicted"))
    {
        // The store's own refusal to settle a conflicted name, or jj's
        // `Name `<branch>` is conflicted` from a verb that met one.
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

/// Forward work exhausted its shared deadline or a subprocess timeout.
pub const RECONCILE_TIMEOUT: &str = "reconcile_timeout";
pub const REF_LOCK_CONTENTION: &str = "ref_lock_contention";
pub const REF_LOCK_STALE: &str = "ref_lock_stale";
/// A failure whose undo did not complete, so the store may not be where the
/// pass found it. Retried, and — unlike a lost lock — always reported.
pub const ROLLBACK_FAILED: &str = "rollback_failed";

/// The marker every failure carries when its rollback did not complete. Kept
/// in one place so no such failure can be mistaken for a clean retry.
pub const ROLLBACK_INCOMPLETE: &str = "rollback incomplete:";

pub(crate) fn rollback_incomplete(detail: &str) -> String {
    format!("{ROLLBACK_INCOMPLETE} {detail}")
}

/// Whether a failure kind is a lost `packed-refs.lock`: retried by the worker,
/// withheld from the agent, who can act on neither.
pub fn reconcile_failure_is_ref_lock(kind: &str) -> bool {
    kind == REF_LOCK_CONTENTION || kind == REF_LOCK_STALE
}

/// The give-up line of a lost-lock error — who held the lock, or that it was
/// stale — or `None` when `error` is not a lost lock at all. A bare lock error
/// that carries no line (a step outside patience) is named as contention.
pub fn ref_lock_diagnosis(error: &str) -> Option<String> {
    if !reconcile_failure_is_ref_lock(reconcile_failure_kind(error)) {
        return None;
    }
    Some(
        error
            .lines()
            .find(|line| {
                line.starts_with(CONTENTION_DIAGNOSIS) || line.starts_with(STALE_DIAGNOSIS)
            })
            .map(str::to_string)
            .unwrap_or_else(|| {
                format!("{CONTENTION_DIAGNOSIS} — lost outside patience, without a diagnosis")
            }),
    )
}

/// The failure to record, without an attempt, for each branch a pass has not
/// reached yet once `error` showed the ref store locked past patience.
///
/// Every rewrite may now wait out the lock while holding the per-store lock,
/// so one long contention spell would otherwise cost the full wait per branch
/// — twenty siblings, ten minutes of everyone else queued behind the store.
/// The remainder is left for the sweep, which retries the whole set shortly.
/// `None` when `error` is not a lost lock. The deferral carries the same
/// diagnosis line, so it classifies as the same kind.
pub fn ref_lock_deferral(error: &str) -> Option<String> {
    ref_lock_diagnosis(error).map(|diagnosis| {
        format!(
            "deferred without an attempt: the ref store stayed locked past patience earlier in \
             this pass, and is retried automatically.\n{diagnosis}"
        )
    })
}

/// Rebase one sibling onto the reconcile dest, recording the outcome. Returns
/// where the branch stood before the rebase when the caller should go on to
/// classify the sibling, and `None` when the outcome is already recorded.
fn rebase_sibling(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    integration_branch: &str,
    resolution: Option<&ResolutionCarry>,
    report: &mut ReconcileReport,
) -> Option<RebaseRollback> {
    // Captured before the rebase so a classification that cannot keep the
    // result can return the store to exactly here. Exact under the per-store
    // lock every caller holds.
    let rollback = match operation_id(jj, store) {
        Ok(pre_op) => RebaseRollback {
            pre_op,
            pre_tip: bookmark_commit(jj, store, branch),
        },
        Err(error) => {
            record_failure(report, branch, error);
            return None;
        }
    };
    let proceed =
        match rebase_branch_onto_with_resolution(jj, store, branch, integration_branch, resolution)
        {
            Ok(RebaseOutcome::Rebased) => true,
            Ok(RebaseOutcome::Conflicted { diagnostic }) => {
                record_conflict(report, branch, *diagnostic);
                false
            }
            Err(error) => {
                log::warn!(
                    "jj reconcile: rebase {branch} onto {integration_branch} failed: {error}"
                );
                record_failure(report, branch, error);
                false
            }
        };
    proceed.then_some(rollback)
}

/// Reconcile runner-owned sibling bookmarks onto the locally advanced integration
/// tip. Graph advancement never reads, refreshes, or follows a workspace.
pub fn reconcile_siblings(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    siblings: &[String],
) -> Result<ReconcileReport, String> {
    reconcile_siblings_with_options(
        jj,
        store,
        integration_branch,
        siblings,
        &ReconcileOptions {
            publish: true,
            ..ReconcileOptions::default()
        },
    )
}

pub fn reconcile_siblings_without_publication(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    siblings: &[String],
) -> Result<ReconcileReport, String> {
    reconcile_siblings_with_options(
        jj,
        store,
        integration_branch,
        siblings,
        &ReconcileOptions::default(),
    )
}

pub fn reconcile_resolved_sibling_without_publication(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    sibling: &str,
    resolution: &ResolutionCarry,
) -> Result<ReconcileReport, String> {
    let siblings = vec![sibling.to_string()];
    reconcile_siblings_with_options(
        jj,
        store,
        integration_branch,
        &siblings,
        &ReconcileOptions {
            resolution: Some((sibling.to_string(), resolution.clone())),
            ..ReconcileOptions::default()
        },
    )
}

/// How one reconcile pass treats its siblings.
#[derive(Debug, Clone, Default)]
pub struct ReconcileOptions {
    /// Publish each moved sibling to origin as part of the pass.
    pub publish: bool,
    /// An explicitly requested committed resolution for one named sibling.
    pub resolution: Option<(String, ResolutionCarry)>,
    /// Siblings that catch up by ONE merge commit of the destination and are
    /// never rebased (see [`merge_base_into_branch`]). Every other sibling is
    /// rebased exactly as before.
    pub merge_mode: std::collections::BTreeSet<String>,
    /// The destination's branch name, for the merge commit's description; the
    /// pass itself is usually handed a pinned commit id.
    pub target_name: Option<String>,
    /// For a resolved merge-mode sibling: the destination the resolution was
    /// made against. The resolution is merged there first, and the branch is
    /// then merged with the pass's destination if that is newer.
    pub resolution_dest: Option<String>,
}

pub fn reconcile_siblings_with_options(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    siblings: &[String],
    options: &ReconcileOptions,
) -> Result<ReconcileReport, String> {
    let publish = options.publish;
    let resolution = options
        .resolution
        .as_ref()
        .map(|(branch, carry)| (branch.as_str(), carry));
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
        report.held = siblings.to_vec();
        return Ok(report);
    }

    // Missing-bookmark siblings are filtered upstream in `reconcile_base_advance`
    // (one store-wide bookmark list before ANY per-sibling jj work), so every
    // sibling reaching this loop has a live bookmark. See `retain_present_siblings`.
    let mut deferral: Option<String> = None;
    for branch in siblings {
        if let Some(deferred) = deferral.as_ref() {
            record_failure(&mut report, branch, deferred.clone());
            continue;
        }
        if options.merge_mode.contains(branch) {
            merge_one_sibling(
                jj,
                store,
                integration_branch,
                branch,
                dest_commit.as_deref(),
                options,
                &mut report,
            );
        } else {
            reconcile_one_sibling(
                jj,
                store,
                integration_branch,
                branch,
                dest_commit.as_deref(),
                publish,
                resolution,
                &mut report,
            );
        }
        deferral = report
            .failed
            .iter()
            .find(|failure| failure.branch == *branch)
            .and_then(|failure| ref_lock_deferral(&failure.error));
    }
    Ok(report)
}

/// One merge-mode sibling's slice of the pass: never a rebase. A clean merge
/// is reported as `rebased_clean` (plus its `merged_clean` detail), a conflict
/// as an ordinary rolled-back conflict whose coordinates are (merge base, tip,
/// dest), and a refusal as a failure of kind [`MERGE_CATCH_UP_REFUSED`].
fn merge_one_sibling(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    branch: &str,
    dest_commit: Option<&str>,
    options: &ReconcileOptions,
    report: &mut ReconcileReport,
) {
    let Some(dest) = dest_commit else {
        record_failure(
            report,
            branch,
            format!("catch-up merge: destination `{integration_branch}` did not resolve"),
        );
        return;
    };
    let target = options.target_name.as_deref().unwrap_or(integration_branch);
    let carry = options
        .resolution
        .as_ref()
        .filter(|(resolved, _)| resolved == branch)
        .map(|(_, carry)| carry);
    // A resolution is merged against the destination it was made against, so
    // its carried paths are exact there; the pass's newer destination follows
    // as an ordinary merge.
    let mut steps: Vec<(&str, Option<&ResolutionCarry>)> = Vec::new();
    match (carry, options.resolution_dest.as_deref()) {
        (Some(carry), Some(resolved_on)) if resolved_on != dest => {
            steps.push((resolved_on, Some(carry)));
            steps.push((dest, None));
        }
        (carry, _) => steps.push((dest, carry)),
    }
    let mut merged: Option<MergedCatchUp> = None;
    for (step_dest, step_carry) in steps {
        let message = catch_up_merge_message(target, step_dest, branch);
        match merge_base_into_branch(jj, store, branch, step_dest, &message, step_carry) {
            Ok(MergeCatchUp::AlreadyCurrent) => {}
            Ok(MergeCatchUp::FastForwarded { .. }) => {
                if merged.is_none() {
                    report.rebased_clean.push(branch.to_string());
                    report.silent.push(branch.to_string());
                    return;
                }
            }
            Ok(MergeCatchUp::Merged {
                merge,
                old_base,
                new_base,
            }) => {
                let resolved =
                    step_carry.is_some() || merged.as_ref().is_some_and(|earlier| earlier.resolved);
                let old_base = merged
                    .take()
                    .map(|earlier| earlier.old_base)
                    .unwrap_or(old_base);
                merged = Some(MergedCatchUp {
                    merge,
                    old_base,
                    new_base,
                    resolved,
                });
            }
            Ok(MergeCatchUp::Conflicted { diagnostic }) => {
                record_conflict(report, branch, diagnostic);
                return;
            }
            Ok(MergeCatchUp::Refused { reason }) => {
                report
                    .merge_refused
                    .insert(branch.to_string(), reason.clone());
                record_failure(report, branch, reason);
                return;
            }
            Err(error) => {
                log::warn!("catch-up[{branch}] merge onto {step_dest} failed: {error}");
                record_failure(report, branch, error);
                return;
            }
        }
    }
    let Some(merged) = merged else {
        return;
    };
    if options.publish {
        if let Err(error) =
            publish_reconciled_bookmark(jj, store, branch, discover_origin_presence(jj, store))
        {
            log::warn!("catch-up[{branch}] publishing the merge failed: {error}");
            record_failure(report, branch, error.to_string());
            return;
        }
    }
    report.rebased_clean.push(branch.to_string());
    report.merged_clean.insert(branch.to_string(), merged);
}

#[expect(
    clippy::too_many_arguments,
    reason = "one sibling's slice of the pass, carrying the pass's pinned coordinates"
)]
fn reconcile_one_sibling(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    branch: &str,
    dest_commit: Option<&str>,
    publish: bool,
    resolution: Option<(&str, &ResolutionCarry)>,
    report: &mut ReconcileReport,
) {
    let already_on_dest = dest_commit
        .map(|dest| branch_descends_from(jj, store, branch, dest))
        .unwrap_or(false);
    let mut push_clean = !already_on_dest;
    let mut trivial_fast_forward = false;
    let mut rollback = None;
    let carry = resolution
        .filter(|(resolved, _)| *resolved == branch)
        .map(|(_, carry)| carry);

    if !already_on_dest {
        let fast_forward_to =
            dest_commit.filter(|dest| branch_is_ancestor_of(jj, store, branch, dest));
        if let Some(dest) = fast_forward_to {
            if let Err(error) = fast_forward_bookmark(jj, store, branch, dest) {
                log::warn!("jj reconcile: fast-forward {branch} to {dest} failed: {error}");
                record_failure(report, branch, error);
                return;
            }
            push_clean = false;
            trivial_fast_forward = true;
        } else {
            match rebase_sibling(jj, store, branch, integration_branch, carry, report) {
                Some(before) => rollback = Some(before),
                None => return,
            }
        }
    }

    if trivial_fast_forward {
        report.rebased_clean.push(branch.to_string());
        report.silent.push(branch.to_string());
        return;
    }

    reached_step("reconcile: before classification");
    classify_reconciled_sibling(
        jj,
        store,
        branch,
        dest_commit,
        ReconcilePublication {
            clean: push_clean && publish,
            healed: publish,
        },
        rollback.as_ref(),
        report,
    );
    // Automatic reconcile does not publish. Its later classification is still
    // part of the same locked transaction: expiry there must undo the rebase,
    // not leave a moved branch reported as a failed retry.
    if !publish {
        if let (Some(rollback), Err(error)) = (
            rollback.as_ref(),
            jj.check_operation_budget("jj reconcile classification"),
        ) {
            report.rebased_clean.retain(|name| name != branch);
            report.conflicted.retain(|name| name != branch);
            report.failed.retain(|failure| failure.branch != branch);
            report.conflict_diagnostics.remove(branch);
            record_failure(
                report,
                branch,
                super::merge::roll_back_rebase(jj, store, branch, &rollback.pre_op, error),
            );
        }
    }
}
