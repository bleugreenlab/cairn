//! Classification of the jj error families the commit barrier routes on
//! (stale, lost-seal, conflicted-branch) plus the stale-recovery op.
use super::*;
use std::path::Path;

/// Classify a jj error as the STALE-working-copy refusal family.
///
/// jj refuses every working-copy-touching command on a stale workspace (one
/// whose `@` a sibling workspace rewrote over the shared store) with the stable,
/// documented `working copy is stale` message. Both the seal (`jj commit`) and
/// the discard (`jj restore`) hit it, so the commit barrier's rollback must
/// classify and self-heal it rather than dead-end. Also classify the `seal_paths`
/// pre-commit "behind its branch tip" refusal: it is the same family (the
/// bookmark advanced past a rewritten `@`), and the write path recovers from it
/// the same way. The DISTINCT conflicted-branch refusal (a divergent `@` over a
/// bookmark whose tip carries a recorded conflict) is split off into
/// [`is_conflicted_branch_seal_error`] and deliberately NOT matched here: it must
/// preserve the working copy, not discard it.
///
/// Detection is by error-string because jj 0.42 exposes no non-snapshotting
/// staleness probe (`jj debug workingcopy` is gone; `--ignore-working-copy`
/// skips the check entirely). Centralized here with the jj phrasing cited so a
/// future jj rewording is a one-line change.
pub fn is_stale_error(msg: &str) -> bool {
    msg.contains("working copy is stale") || msg.contains("behind its branch tip")
}

/// Stable marker phrase for a seal that captured no change because the working
/// copy was reset under a concurrent store advance — the empty/divergent-seal
/// data-loss mode. Carried in the `Err` [`seal_paths`] returns when its
/// post-commit anomaly check fires, so the routing sites can recognize it.
pub(crate) const LOST_SEAL_MSG: &str =
    "seal captured no change (the working copy was reset under a concurrent store advance)";

/// Classify a jj error as the LOST-SEAL family: a `jj commit` that returned a sha
/// but sealed an empty or divergent commit because a concurrent op reset `@` out
/// from under it (silent data loss reported as a real commit). Kept distinct from
/// [`is_stale_error`] — the cause and jj phrasing differ — and OR'd with it at the
/// routing sites, because both are recoverable the same way: re-apply the batch
/// against the current base and re-seal.
pub fn is_lost_seal_error(msg: &str) -> bool {
    msg.contains(LOST_SEAL_MSG)
}

/// Stable marker phrase for a seal refused because the workspace head diverged
/// from a branch bookmark whose tip carries a recorded CONFLICT — the deliberate
/// resolve-at-base FLATTEN case. The agent moved `@` onto a fresh resolved line
/// off the current base while the bookmark still points at the conflicted
/// intermediate stack tip it is escaping; jj will not fold that conflicted
/// history, so sealing forward is refused. Unlike the clean "behind its branch
/// tip" refusal (genuine stale / coordinator-advance, recovered by discard +
/// update-stale), this MUST NOT discard: `@` holds real resolved work the discard
/// would destroy. Deliberately omits the "behind its branch tip" phrase so
/// [`is_stale_error`] does not match it. `pub(crate)` so the cross-module barrier
/// tests can reference the exact string without drift.
pub(crate) const CONFLICTED_BRANCH_SEAL_MSG: &str =
    "seal refused: branch tip carries a recorded conflict; sealing forward would advance onto the conflict";

/// Classify a jj error as the CONFLICTED-BRANCH seal refusal: the fast-forward
/// guard refused a seal because the branch bookmark tip carries a recorded
/// conflict and `@` has diverged from it (a deliberate resolve-at-base flatten).
/// Kept DISTINCT from [`is_stale_error`] and [`is_lost_seal_error`] — those
/// recover by discarding / re-sealing, but this one must PRESERVE the working
/// copy, because discarding destroys the resolved flatten and advancing lands
/// back on the conflict. The routing sites give it its own non-destructive arm
/// that preserves `@` and points at the git-workflow resolve-at-base flatten.
pub fn is_conflicted_branch_seal_error(msg: &str) -> bool {
    msg.contains(CONFLICTED_BRANCH_SEAL_MSG)
}

/// Refresh a workspace whose `@` was rebased out from under it. A rebased live
/// workspace goes stale; `update-stale` updates the on-disk files and
/// materializes any conflict markers for the agent to resolve.
pub fn update_stale(jj: &JjEnv, ws: &Path) -> Result<(), String> {
    jj.run(
        ws,
        &["workspace", "update-stale"],
        "jj workspace update-stale",
    )
    .map(|_| ())
}
