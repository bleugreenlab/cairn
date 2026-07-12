//! Store-side merge folds, rebases, squashes, and bookmark advancement
//! primitives the sibling reconcile builds on.
use super::*;
use std::path::Path;

// ── Sibling reconcile (auto-rebase onto an advanced integration tip) ─────────

/// Outcome of reconciling in-flight siblings onto an advanced integration tip:
/// which sibling bookmarks rebased cleanly, which recorded a conflict, and which
/// were held back untouched. A recorded conflict is STOP-THE-LINE, not a
/// convenience item: jj refuses to push or merge a conflicted commit, so a
/// conflicted branch destined for GitHub is wedged until the agent resolves the
/// markers and re-seals. The reconcile also never hands a conflicted base down to
/// clean siblings — when the rebase dest itself carries a conflict, every sibling
/// is `held` on its prior clean commit rather than rebased onto the conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileFailure {
    pub branch: String,
    pub workspace_path: std::path::PathBuf,
    pub error: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Sibling bookmarks that rebased with no conflict.
    pub rebased_clean: Vec<String>,
    /// Sibling bookmarks whose rebase recorded a conflict.
    pub conflicted: Vec<String>,
    /// Successful reconciliations that preserve loose local work and therefore
    /// should not produce a clean-rebase notification.
    pub preserved_dirty: Vec<String>,
    /// Trivial bookmark/workspace advances that are current but intentionally
    /// silent because the sibling had no branch work to announce.
    pub silent: Vec<String>,
    /// Sibling bookmarks held UNrebased because the rebase dest itself carries
    /// a recorded conflict — never handed a conflicted base. Cleared on the next
    /// reconcile once the base re-seals conflict-free.
    pub held: Vec<String>,
    /// Exact per-workspace failures from graph movement, workspace reconciliation,
    /// flatten recovery, or publication.
    pub failed: Vec<ReconcileFailure>,
}

/// Fold a child's real commit into the integration bookmark over the shared
/// store — the local "merge" of a child PR. `jj bookmark set` is forward-only (it
/// refuses a backwards/sideways move), so the child must already sit on the
/// current integration tip; callers establish that by rebasing the source onto
/// the current tip before folding (`store_merge_child`, `rebase_then_fold_into`).
/// A refusal here means that rebase did not run or did not take — surface it
/// loudly rather than silently regressing the tip.
/// `--ignore-working-copy` because the fold is driven from the store, not a
/// workspace (Gotcha A: the store's default `@` may be stale after a prior
/// `--ignore-working-copy` rebase).
///
/// A backwards/sideways refusal is mapped to a safe, actionable message: jj's
/// raw stderr hints `--allow-backwards`, which would move the bookmark BACKWARD
/// and clobber the commits that advanced it. That hint must never reach an
/// agent, so it is never echoed. For a fold whose target advances out of band
/// (the project default branch), callers use `rebase_then_fold_into`, which
/// rebases first so this path is never reached.
pub fn merge_into_bookmark(
    jj: &JjEnv,
    store: &Path,
    integration_branch: &str,
    child_branch: &str,
) -> Result<(), String> {
    let child_rev = format!("bookmarks(exact:{child_branch:?})");
    if let Err(e) = jj.run(
        store,
        &[
            "bookmark",
            "set",
            integration_branch,
            "-r",
            &child_rev,
            "--ignore-working-copy",
        ],
        "jj bookmark set (merge fold)",
    ) {
        // Sanitize jj's raw backwards/sideways refusal: its stderr hints
        // `--allow-backwards`, which would move the bookmark BACKWARD and clobber
        // the commits that advanced it. Map it to a message that names the real
        // cause (the source is not a descendant of the target) and the safe
        // remedy (rebase first), and NEVER echo the dangerous hint.
        let lowered = e.to_lowercase();
        if lowered.contains("backwards") || lowered.contains("sideways") {
            return Err(format!(
                "Refusing to fold `{child_branch}` into `{integration_branch}`: the source is not a descendant of the target (the target advanced past the source's fork point). Rebase the source onto the current target tip and let it re-seal, then merge again."
            ));
        }
        return Err(e);
    }
    // Export the advanced bookmark to the backing git repo so the project's
    // `refs/heads/<integration>` tracks the fold (as `seal` does after a sealed
    // commit). Without this the store bookmark is advanced but the project git
    // ref lags, and a later child provisioned off the integration branch
    // resolves its base via that stale ref (`execution/jobs/worktrees.rs`) and
    // would start from the pre-merge tip — breaking the store-owns-merge
    // invariant. Load-bearing, so it fails the fold rather than silently leaving
    // a stale ref.
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "jj git export (merge fold)",
    )
    .map(|_| ())
}

/// Merge a source bookmark into a target whose tip may have advanced out of band
/// (the project default branch). Unlike `merge_into_bookmark`'s forward-only fold
/// — which assumes Cairn's reconcile keeps the source on an integration tip — the
/// default branch advances OUTSIDE the fold chain (another PR merged, or an
/// external push), so the source is first rebased onto the current target tip,
/// exactly as `reconcile_siblings` rebases siblings, then the target FFs to it.
/// A recorded conflict returns a safe, actionable error and NEVER the
/// `--allow-backwards` hint (which would move the default branch backward and
/// clobber it). `dest` is the resolved live target tip (`<target>@origin` for a
/// remote project after a fetch, else the local bookmark). Idempotent when the
/// source already sits on `dest` (the rebase is a `jj rebase` no-op).
pub fn rebase_then_fold_into(
    jj: &JjEnv,
    store: &Path,
    target_branch: &str,
    source_branch: &str,
    dest: &str,
) -> Result<(), String> {
    rebase_branch_onto(jj, store, source_branch, dest)?;
    if branch_has_conflict(jj, store, source_branch)? {
        return Err(format!(
            "Refusing to merge: rebasing `{source_branch}` onto the advanced default branch `{target_branch}` recorded a conflict. Resolve the conflict markers in the workspace and let it re-seal, then merge again."
        ));
    }
    // The source is now a descendant of `dest` (and thus of the local target
    // bookmark, which `dest` advanced from), so this FF can never go backwards.
    merge_into_bookmark(jj, store, target_branch, source_branch)
}

/// Collapse a (possibly multi-commit) branch into a single commit on top of
/// `base_rev`, preserving its current tree. This restores the squash *shape* at
/// a default-branch landing: after the source is rebased onto the live default
/// tip, this rewrites the source bookmark to one commit whose parent is that tip
/// and whose tree equals the rebased source tree, so the FF fold lands exactly
/// one commit on the default branch instead of every per-change commit the agent
/// sealed. `message` becomes that commit's description (the PR title).
///
/// Operates entirely over the shared store with `--ignore-working-copy`
/// discipline (the store's `@` is a scratch working copy that must never be
/// snapshotted — Gotcha A, matching `merge_into_bookmark`/`rebase_branch_onto`).
/// Crucially the store's `@` is also never *moved*: `jj new --no-edit` creates
/// the squashed commit WITHOUT checking it out, so the working copy stays on its
/// scratch commit and a later plain (non-`--ignore-working-copy`) read — e.g.
/// `bookmark_commit` at the end of the fold — does not trip jj's stale-working-
/// copy guard.
///
/// Steps: capture the rebased tip (it carries the full source tree); create an
/// empty commit as a child of `base_rev`, addressing it by the set difference of
/// `base_rev`'s children before and after (`jj new` prints no machine-readable
/// id); repoint the bookmark to that empty commit; then `restore` the captured
/// tree INTO the bookmark. The restore mints a fresh commit id, so the bookmark
/// is moved FIRST and the restore targets the bookmark revset so it follows the
/// rewrite. The repoint is a deliberate sideways move — the squashed commit is
/// NOT a descendant of the old branch tip — so it passes `--allow-backwards`;
/// that hint is legitimate here (we are replacing the branch's own history with
/// an equivalent-tree single commit), unlike `merge_into_bookmark`, where the
/// same hint would clobber commits that advanced a shared target.
pub fn squash_branch_onto(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    base_rev: &str,
    message: &str,
) -> Result<(), String> {
    // The rebased tip still carries the complete source tree; capture it before
    // the bookmark is moved off it.
    let source_tree_rev = bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("squash: branch `{branch}` did not resolve"))?;

    // Create an empty commit as a child of the live default tip, WITHOUT moving
    // `@`. `jj new` emits no machine-readable id, so address the new commit by
    // the set difference of `base_rev`'s children before and after.
    let before = base_children(jj, store, base_rev)?;
    jj.run(
        store,
        &[
            "new",
            "--no-edit",
            "-r",
            base_rev,
            "-m",
            message,
            "--ignore-working-copy",
        ],
        "jj new (squash base)",
    )?;
    let after = base_children(jj, store, base_rev)?;
    let mut added: Vec<String> = after.difference(&before).cloned().collect();
    let squashed = match added.len() {
        1 => added.remove(0),
        n => {
            return Err(format!(
                "squash: expected exactly one new commit on `{base_rev}`, found {n}"
            ))
        }
    };

    // Repoint the branch at the empty commit FIRST, then restore the source tree
    // INTO the bookmark so it follows the rewrite (`restore` mints a new id).
    // The repoint is a deliberate sideways move, so `--allow-backwards` is
    // correct here.
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            &squashed,
            "--ignore-working-copy",
            "--allow-backwards",
        ],
        "jj bookmark set (squash)",
    )?;
    let branch_rev = format!("bookmarks(exact:{branch:?})");
    jj.run(
        store,
        &[
            "restore",
            "--from",
            &source_tree_rev,
            "--into",
            &branch_rev,
            "--ignore-working-copy",
        ],
        "jj restore (squash tree)",
    )?;
    // Export the rewritten bookmark to the backing git, as the fold path does,
    // so the project's `refs/heads/<branch>` tracks the squashed commit.
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "jj git export (squash)",
    )
    .map(|_| ())
}

/// Commit ids of the direct children of `rev` in the shared store. Used to
/// address a freshly-created `jj new --no-edit` commit by set difference, since
/// `jj new` emits no machine-readable id.
fn base_children(
    jj: &JjEnv,
    store: &Path,
    rev: &str,
) -> Result<std::collections::HashSet<String>, String> {
    let revset = format!("children({rev})");
    let out = jj.run(
        store,
        &[
            "log",
            "-r",
            &revset,
            "--no-graph",
            "--ignore-working-copy",
            "-T",
            "commit_id ++ \"\\n\"",
        ],
        "jj log (base children)",
    )?;
    Ok(out
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

/// Idempotently mark a remote bookmark as jj-tracked so a local push of it is
/// accepted. jj refuses to push a local bookmark whose `@origin` counterpart is
/// untracked ("Non-tracking remote bookmark … exists"), which happens when
/// origin's ref was created outside this store's jj. A no-op when already
/// tracked; errors (best-effort for the caller) when there is no such remote
/// bookmark, e.g. a no-remote project.
pub fn track_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    let remote_ref = format!("{branch}@origin");
    jj.run(
        store,
        &["bookmark", "track", &remote_ref],
        "jj bookmark track",
    )
    .map(|_| ())
}

/// Push an already-advanced store bookmark to origin with `--ignore-working-copy`
/// (Gotcha A: the store's default `@` may be stale after a fold/rebase). Used to
/// advance both the integration tip after a fold and a cleanly-rebased sibling's
/// PR head; jj's remote-tracking model accepts a rewritten bookmark without a
/// force-push.
pub fn push_store_bookmark(jj: &JjEnv, store: &Path, branch: &str) -> Result<(), String> {
    jj.run_with_timeout(
        store,
        &[
            "git",
            "push",
            "--ignore-working-copy",
            "--remote",
            "origin",
            "--bookmark",
            branch,
        ],
        "jj git push store bookmark",
        JJ_NETWORK_TIMEOUT,
    )
    .map(|_| ())
}

/// Rebase a whole branch onto a destination over the shared store, non-blocking.
/// `--ignore-working-copy` because this is driven from the store, not the
/// sibling's workspace. A resulting conflict is recorded in the rebased commit
/// (the command still succeeds); the sibling's descendant `@` auto-rebases.
///
/// After the rebase, export the store's bookmarks back to git immediately. jj
/// moves the local bookmark during the rebase, and leaving the backing git ref at
/// the old commit produces a local-vs-`@git` conflicted bookmark; once conflicted,
/// idempotent descendant checks stop being reliable and later reconciles can keep
/// rewriting the branch. Exporting here keeps the two ref views in lockstep.
pub fn rebase_branch_onto(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
) -> Result<(), String> {
    jj.run(
        store,
        &["rebase", "-b", branch, "-o", dest, "--ignore-working-copy"],
        "jj rebase",
    )?;
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "jj git export (rebase)",
    )
    .map(|_| ())
}

/// Fast-forward a branch bookmark to a concrete destination commit over the shared
/// store, then export the move to git immediately so jj's bookmark and backing git
/// ref stay in lockstep. This is the no-work sibling analogue of
/// [`rebase_branch_onto`]: there is no branch commit to rebase, only an idle
/// bookmark to move onto the advanced base.
pub fn fast_forward_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
) -> Result<(), String> {
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            dest,
            "--ignore-working-copy",
        ],
        "jj bookmark fast-forward",
    )?;
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "jj git export (bookmark fast-forward)",
    )
    .map(|_| ())
}
