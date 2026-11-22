//! Operation recovery and failure classification for branch advancement.
use super::*;
use std::path::Path;

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
