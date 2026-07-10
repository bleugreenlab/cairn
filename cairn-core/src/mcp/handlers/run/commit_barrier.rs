//! The post-batch commit barrier: enforce the worktree==HEAD invariant.

use crate::mcp::git::GitAuthor;

/// Outcome of the post-batch commit barrier.
pub(super) struct CommitBarrierOutcome {
    /// Text to append to the run output (may be empty).
    pub message: String,
    /// Whether the worktree was mutated (committed or restored) so the caller
    /// should emit a `worktree-changed` event.
    pub worktree_changed: bool,
    /// Whether a real commit (or amend) landed. True only when the seal
    /// succeeded — not on a restore, a clean no-op, or a missing commit_msg.
    /// Part of the barrier's result contract, asserted by the commit-hygiene
    /// tests, and read by `handle_run` to gate the synchronous when:write check
    /// runner on an actually-sealed commit.
    pub committed: bool,
    /// The working-copy patch (`jj diff --git`) captured just before the seal,
    /// carried out ONLY on a successful commit (`committed == true`).
    /// `handle_run` parses it to record the sealed commit's file changes so the
    /// run path populates the same `file_changes` cache the write path does.
    /// `None` on a restore, a clean no-op, a missing commit_msg, or when the
    /// backend produces no patch.
    pub committed_patch: Option<String>,
}

/// Enforce the worktree==HEAD invariant after a `run` batch.
///
/// The single decision point for what happens to the worktree once a batch
/// finishes: commit it, restore it to HEAD, or leave it alone. It touches git
/// only inside `worktree_path` and returns the user-facing message plus whether
/// the worktree changed, so it is testable without an `Orchestrator`.
///
/// - `Some(msg)`: commit the worktree if dirty, even on partial item failure;
///   a commit failure restores the worktree to HEAD.
/// - `None`: when the batch fully succeeded and changed the worktree, restore
///   it to HEAD — no commit_msg means the new dirt must not persist.
pub(super) fn run_commit_barrier(
    vcs: &dyn crate::mcp::vcs::WorktreeVcs,
    worktree_path: &std::path::Path,
    commit_msg: Option<&str>,
    all_ok: bool,
    before: Option<&crate::mcp::vcs::VcsSnapshot>,
    author: Option<&GitAuthor>,
) -> CommitBarrierOutcome {
    let mut message = String::new();
    let mut worktree_changed = false;
    let mut committed = false;
    let mut committed_patch: Option<String> = None;

    match commit_msg {
        Some(commit_msg) => {
            // Commit the worktree even when some items failed: a partial-success
            // batch must not silently leave the successful items' dirt behind.
            if !matches!(vcs.is_dirty(worktree_path), Ok(false)) {
                // Capture the working-copy patch BEFORE the seal empties `@`, so a
                // successful commit can record its file changes on the run path.
                let patch = vcs.capture_patch(worktree_path);
                match vcs.seal_all(worktree_path, commit_msg, author) {
                    Ok(commit_result) => {
                        worktree_changed = true;
                        committed = true;
                        committed_patch = patch;
                        let pr_suffix = commit_result
                            .pr_number
                            .map(|pr| format!(" updated PR#{}", pr))
                            .unwrap_or_default();
                        message.push_str(&format!(
                            "\u{2713} Committed changes ({}){}",
                            commit_result.sha, pr_suffix
                        ));
                        // Surface an amend that was converted to a child commit
                        // because the target commit is shared with a sibling
                        // bookmark, so the agent's `^` intent visibly landed as a
                        // new commit rather than a rewrite of shared history.
                        if let Some(note) = &commit_result.amend_note {
                            message.push_str(&format!(" — {note}"));
                        }
                    }
                    Err(e) if e.contains("nothing to commit") => {
                        // Tree was (or became) clean; already equals HEAD.
                        log::info!("run commit_msg given but nothing to commit: {}", e);
                    }
                    Err(e) if crate::mcp::vcs::is_workspace_lineage_mismatch(&e) => {
                        // Ownership changed after commands ran. Preserve every byte;
                        // stale recovery and generic discard are forbidden here.
                        worktree_changed = false;
                        committed = false;
                        message.push_str(&format!(
                            "⚠️ Seal refused: {e}. The working copy was PRESERVED exactly; no discard or update-stale recovery was attempted."
                        ));
                    }
                    Err(e) if crate::jj::is_conflicted_branch_seal_error(&e) => {
                        // The seal was refused because the branch bookmark tip
                        // carries a recorded conflict and `@` has diverged from it
                        // — a deliberate resolve-at-base flatten away from a
                        // conflicted intermediate stack. Unlike a stale/lost-seal
                        // advance, discarding here would DESTROY the agent's
                        // resolved work: jj will not fold a conflicted history, so
                        // advancing onto the bookmark lands back on the conflict.
                        // The only safe automatic action is to PRESERVE the working
                        // copy exactly as the agent arranged it, so this does NOT
                        // discard. That deliberately leaves `@` dirty (worktree !=
                        // bookmark HEAD); the flatten the message points to
                        // converges the invariant — its final `jj new` leaves `@`
                        // clean on the moved bookmark.
                        worktree_changed = false;
                        committed = false;
                        message.push_str(&format!(
                            "\u{26a0}\u{fe0f} Seal refused: this branch has conflicted intermediate commits jj will not fold, so sealing `@` forward can't clear them: {}. The working copy was PRESERVED (not discarded). To land a flattened resolution, run the pure-jj resolve-at-base flatten with NO commit_msg (see the git-workflow skill); do not retry with commit_msg.",
                            e
                        ));
                    }
                    Err(e)
                        if crate::jj::is_lost_seal_error(&e) || crate::jj::is_stale_error(&e) =>
                    {
                        // A concurrent store advance reset `@` out from under the
                        // seal (the lost-seal case, already backed out in
                        // `seal_paths`) or left the workspace stale. The run can't
                        // re-derive command side effects, so revert-and-retry is the
                        // ceiling: discard to HEAD and tell the agent to re-run.
                        let restore = vcs.discard(worktree_path);
                        worktree_changed = true;
                        match restore {
                            Ok(()) => message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Hit a concurrent store advance: {}; the worktree was restored to HEAD and nothing was committed. Retry the run with commit_msg to land the changes.",
                                e
                            )),
                            Err(re) => message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Hit a concurrent store advance: {}; additionally failed to restore the worktree to HEAD: {}",
                                e, re
                            )),
                        }
                    }
                    Err(e) => {
                        let restore = vcs.discard(worktree_path);
                        worktree_changed = true;
                        match restore {
                            Ok(()) => message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Failed to commit: {}; the worktree was restored to HEAD.",
                                e
                            )),
                            Err(re) => message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Failed to commit: {}; additionally failed to restore the worktree to HEAD: {}",
                                e, re
                            )),
                        }
                    }
                }
            }
        }
        None => {
            // No commit_msg: the run must not leave new dirt. When the whole
            // batch succeeded and changed the worktree, restore to HEAD. Gate on
            // `all_ok` — a failed batch's own error is the headline, and the
            // hygiene gate must not mask it.
            if all_ok {
                if let Some(before) = before {
                    // A stale `@` blocks `changed_since` too (jj's diff snapshots,
                    // and snapshotting is what staleness refuses), so it returns
                    // `Err`, not `Ok(true)`. Treat a stale read as "changed": the
                    // batch's loose edits are real dirt that must not persist, and
                    // the stale-resilient `discard` self-heals them to HEAD.
                    match vcs.changed_since(worktree_path, before) {
                        Ok(true) => {
                            // A non-worktree backend (the project's live checkout)
                            // cannot revert: rolling it back would destroy the
                            // user's own uncommitted work. Changes can only happen
                            // in a worktree, so the stray dirt is left in place and
                            // the agent is warned loudly instead of (falsely) told
                            // it was reverted. See `docs/worktree-fence.md`.
                            if !vcs.can_revert() {
                                message.push_str(
                                    "\u{26a0}\u{fe0f} This run wrote into the project's live checkout, but changes can only be made in a worktree. The changes were left in place — the live checkout is not reverted, because that could destroy your own uncommitted work — so review and clean them up (`git status` / `git restore`). To make changes that persist, run inside a worktree.",
                                );
                                return CommitBarrierOutcome {
                                    message,
                                    worktree_changed,
                                    committed,
                                    committed_patch,
                                };
                            }
                            let reset_ok = vcs.discard(worktree_path);
                            worktree_changed = true;
                            if let Err(e) = reset_ok {
                                message.push_str(&format!(
                                    "\u{26a0}\u{fe0f} Run changed the worktree but no commit_msg was given. Failed to restore the worktree to HEAD: {}. Run with commit_msg like `run({{commands:[…], commit_msg)`, then retry.",
                                    e
                                ));
                            } else {
                                message.push_str(
                                    "\u{26a0}\u{fe0f} Run reverted: it changed the worktree but no commit_msg was given. Run with commit_msg like `run({commands:[…], commit_msg)`, then retry.",
                                );
                            }
                        }
                        Ok(false) => {}
                        Err(e) if crate::jj::is_stale_error(&e) && vcs.can_revert() => {
                            // A sibling advanced `@` out from under this run mid-batch.
                            // The dirt can't be inspected (jj won't snapshot a stale
                            // copy), so reconcile to the fresh HEAD via the
                            // stale-resilient discard and tell the agent to retry.
                            let _ = vcs.discard(worktree_path);
                            worktree_changed = true;
                            message.push_str(
                                "\u{26a0}\u{fe0f} Run hit a concurrent worktree advance and was reconciled to HEAD; no commit_msg was given. Re-run with commit_msg to keep changes.",
                            );
                        }
                        // A non-stale read error (or a non-revertable backend) is
                        // best-effort: leave the worktree as-is, as before.
                        Err(_) => {}
                    }
                }
            }
        }
    }

    CommitBarrierOutcome {
        message,
        worktree_changed,
        committed,
        committed_patch,
    }
}

#[cfg(test)]
mod commit_barrier_tests {
    use super::*;
    use crate::mcp::vcs::{FakeVcs, VcsSnapshot};
    use std::path::Path;

    // The barrier touches the VCS only through the `WorktreeVcs` seam, so a
    // FakeVcs double covers its commit/restore/no-op control flow deterministically
    // and without a VCS binary. The worktree path is never dereferenced.
    fn wt() -> &'static Path {
        Path::new("/tmp/fake-worktree")
    }

    #[test]
    fn commit_msg_commits_dirty_worktree_even_on_partial_failure() {
        // Some(msg) + dirty: a partial-success batch (all_ok=false) still seals
        // its dirt rather than stranding the successful items' changes.
        let vcs = FakeVcs::new().dirty(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), Some("add file"), false, None, None);
        assert_eq!(vcs.seals(), 1, "a dirty worktree must be sealed");
        assert_eq!(vcs.discards(), 0);
        assert!(out.worktree_changed);
        assert!(out.committed, "a real commit must set committed");
        assert!(out.message.contains("Committed"), "got: {}", out.message);
    }

    #[test]
    fn commit_msg_with_clean_worktree_is_noop() {
        let vcs = FakeVcs::new().dirty(Ok(false));
        let out = run_commit_barrier(&vcs, wt(), Some("nothing"), true, None, None);
        assert_eq!(vcs.seals(), 0, "a clean worktree is not sealed");
        assert_eq!(vcs.discards(), 0);
        assert!(!out.worktree_changed);
        assert!(!out.committed, "a clean no-op must not set committed");
        assert!(out.message.is_empty());
    }

    #[test]
    fn commit_failure_restores_worktree_to_head() {
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .seal(Err("pre-commit hook failed".to_string()));
        let out = run_commit_barrier(&vcs, wt(), Some("will fail"), true, None, None);
        assert_eq!(vcs.seals(), 1);
        assert_eq!(
            vcs.discards(),
            1,
            "a failed seal restores the worktree to HEAD"
        );
        assert!(!out.committed, "a failed commit must not set committed");
        assert!(
            out.message.contains("Failed to commit"),
            "got: {}",
            out.message
        );
        assert!(
            out.message.contains("restored to HEAD"),
            "got: {}",
            out.message
        );
    }

    #[test]
    fn commit_msg_nothing_to_commit_is_clean_noop() {
        // The seal reports "nothing to commit" when the tree became clean; that is
        // already==HEAD, not a failure — no restore, no message.
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .seal(Err("nothing to commit, working tree clean".to_string()));
        let out = run_commit_barrier(&vcs, wt(), Some("noop"), true, None, None);
        assert_eq!(vcs.discards(), 0, "nothing-to-commit must not restore");
        assert!(!out.committed);
        assert!(out.message.is_empty(), "got: {}", out.message);
    }

    #[test]
    fn committed_patch_carried_only_on_successful_seal() {
        // The pre-seal working-copy patch rides out on the outcome ONLY when a
        // real commit landed, so `handle_run` can record its file changes. On a
        // restore, a clean no-op, and a no-commit_msg run it is `None`.
        let patch = "diff --git a/x.rs b/x.rs\n";

        // Success: the captured patch is carried out.
        let ok = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch.to_string()));
        let out = run_commit_barrier(&ok, wt(), Some("add x"), true, None, None);
        assert!(out.committed);
        assert_eq!(out.committed_patch.as_deref(), Some(patch));

        // Seal fails → restore → no patch even though one was captured.
        let fail = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch.to_string()))
            .seal(Err("pre-commit hook failed".to_string()));
        let out = run_commit_barrier(&fail, wt(), Some("add x"), true, None, None);
        assert!(!out.committed);
        assert_eq!(out.committed_patch, None, "a restore carries no patch");

        // Clean no-op: nothing captured, nothing carried.
        let clean = FakeVcs::new()
            .dirty(Ok(false))
            .capture(Some(patch.to_string()));
        let out = run_commit_barrier(&clean, wt(), Some("noop"), true, None, None);
        assert!(!out.committed);
        assert_eq!(out.committed_patch, None);

        // No commit_msg: the barrier never seals, so it never carries a patch.
        let before = VcsSnapshot("entry".to_string());
        let none = FakeVcs::new()
            .changed(Ok(true))
            .capture(Some(patch.to_string()));
        let out = run_commit_barrier(&none, wt(), None, true, Some(&before), None);
        assert!(!out.committed);
        assert_eq!(out.committed_patch, None);
    }

    #[test]
    fn none_commit_msg_reverts_changed_worktree() {
        // No commit_msg + a fully-successful batch that changed the worktree must
        // restore to HEAD: new dirt must not persist across calls.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None);
        assert_eq!(vcs.seals(), 0);
        assert_eq!(vcs.discards(), 1, "new dirt without commit_msg is reverted");
        assert!(out.worktree_changed);
        assert!(!out.committed, "a restore must not set committed");
        assert!(out.message.contains("reverted"), "got: {}", out.message);
        assert!(
            out.message
                .contains("Run with commit_msg like `run({commands:[…], commit_msg)`"),
            "got: {}",
            out.message
        );
    }

    #[test]
    fn none_commit_msg_leaves_unchanged_worktree_alone() {
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(false));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None);
        assert_eq!(vcs.discards(), 0);
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty());
    }

    #[test]
    fn none_commit_msg_warns_without_reverting_when_backend_cannot_revert() {
        // A backend that cannot revert (the project's live checkout) must NOT
        // discard on a no-commit_msg run that left dirt: reverting the checkout
        // would destroy the user's own uncommitted work. It warns instead.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true)).can_revert(false);
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None);
        assert_eq!(vcs.discards(), 0, "the live checkout is never reverted");
        assert_eq!(vcs.seals(), 0);
        assert!(!out.worktree_changed, "Cairn mutated nothing");
        assert!(!out.committed);
        assert!(
            out.message.contains("live checkout") && out.message.contains("worktree"),
            "the agent is warned it crossed the worktree boundary: {}",
            out.message
        );
    }

    #[test]
    fn none_commit_msg_leaves_failed_batch_dirt_for_inspection() {
        // Deliberate boundary: a failed batch (all_ok=false) with no commit_msg
        // keeps the hygiene gate out of the way so the failure's side effects stay
        // visible. The `all_ok` guard lives only here.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), None, false, Some(&before), None);
        assert_eq!(vcs.discards(), 0, "a failed batch's dirt is not reverted");
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty());
    }

    /// Over the read-only non-worktree sentinel the barrier is a clean no-op in
    /// both directions: with commit_msg it never seals (is_dirty=false), and
    /// without commit_msg it never discards (changed_since=false) — so an agent
    /// on the project's live checkout never has its working copy sealed or
    /// reverted by Cairn.
    #[test]
    fn non_worktree_barrier_is_a_safe_noop_both_directions() {
        use crate::mcp::vcs::NonWorktreeVcs;
        let before = VcsSnapshot(String::new());

        let with_msg = run_commit_barrier(&NonWorktreeVcs, wt(), Some("work"), true, None, None);
        assert!(!with_msg.worktree_changed);
        assert!(!with_msg.committed);
        assert!(with_msg.message.is_empty());

        let no_msg = run_commit_barrier(&NonWorktreeVcs, wt(), None, true, Some(&before), None);
        assert!(!no_msg.worktree_changed);
        assert!(no_msg.message.is_empty());
    }

    #[test]
    fn commit_msg_stale_seal_restores_worktree_to_head() {
        // A stale-`@` seal failure routes through the (now stale-resilient)
        // discard exactly like any other seal failure: the barrier restores and
        // never claims a commit. The self-heal itself lives in `jj::discard`; here
        // the FakeVcs feeds a genuine stale string so the path is exercised.
        let vcs = FakeVcs::new().dirty(Ok(true)).seal(Err(
            "Error: The working copy is stale (not updated since operation abc).".to_string(),
        ));
        let out = run_commit_barrier(&vcs, wt(), Some("write batch"), true, None, None);
        assert_eq!(vcs.seals(), 1);
        assert_eq!(vcs.discards(), 1, "a stale seal failure restores to HEAD");
        assert!(!out.committed, "a failed stale seal must not set committed");
        assert!(
            out.message.contains("restored to HEAD"),
            "got: {}",
            out.message
        );
    }

    #[test]
    fn commit_msg_conflicted_branch_seal_preserves_worktree() {
        // The explicit regression guard for the silent-data-loss bug: a seal
        // refused because the branch bookmark tip carries a recorded conflict (a
        // deliberate resolve-at-base flatten) must NOT discard — discarding would
        // destroy the agent's resolved flatten — and must NOT advise "retry with
        // commit_msg". The barrier preserves the worktree and points at the
        // pure-jj flatten procedure instead.
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .seal(Err(crate::jj::CONFLICTED_BRANCH_SEAL_MSG.to_string()));
        let out = run_commit_barrier(&vcs, wt(), Some("flatten"), true, None, None);
        assert_eq!(vcs.seals(), 1, "the seal is attempted");
        assert_eq!(
            vcs.discards(),
            0,
            "a conflicted-branch refusal must NOT discard the resolved flatten"
        );
        assert!(!out.committed, "a refused seal must not set committed");
        assert!(
            !out.worktree_changed,
            "the worktree is preserved as the agent arranged it"
        );
        assert!(
            out.message.contains("PRESERVED"),
            "the message names that the working copy was preserved: {}",
            out.message
        );
        assert!(
            out.message.contains("NO commit_msg")
                && !out.message.contains("Retry the run with commit_msg"),
            "the message points at the no-commit_msg flatten, not a futile retry: {}",
            out.message
        );
    }

    #[test]
    fn commit_msg_lineage_mismatch_preserves_worktree() {
        let vcs = FakeVcs::new().dirty(Ok(true)).seal(Err(format!(
            "{} marker changed; recovery=cairn:~/workspace-recovery",
            crate::mcp::vcs::WORKSPACE_LINEAGE_MISMATCH_PREFIX
        )));
        let out = run_commit_barrier(&vcs, wt(), Some("write"), true, None, None);
        assert_eq!(vcs.seals(), 1);
        assert_eq!(vcs.discards(), 0, "lineage mismatch must never discard");
        assert!(!out.committed);
        assert!(!out.worktree_changed);
        assert!(out.message.contains("PRESERVED"));
        assert!(out.message.contains("workspace-recovery"));
    }

    #[test]
    fn none_commit_msg_reconciles_when_changed_since_is_stale() {
        // No commit_msg + a stale `changed_since` read (jj can't snapshot a stale
        // copy, so it errors rather than returning Ok(true)) must still reconcile
        // to HEAD via the stale-resilient discard, not skip the revert and orphan
        // the dirt. Classification flows through the real `crate::jj::is_stale_error`.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Err(
            "Error: The working copy is stale (not updated since operation abc).".to_string(),
        ));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None);
        assert_eq!(vcs.discards(), 1, "a stale read reconciles to HEAD");
        assert!(out.worktree_changed);
        assert!(!out.committed);
        assert!(
            out.message.contains("concurrent worktree advance"),
            "got: {}",
            out.message
        );
    }

    #[test]
    fn none_commit_msg_stale_read_left_alone_when_backend_cannot_revert() {
        // The non-revertable live checkout must NOT discard even on a stale read:
        // reverting it could destroy the user's own work. The stale arm is gated
        // on `can_revert`, so a false backend leaves the worktree untouched.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new()
            .changed(Err(
                "Error: The working copy is stale (not updated since operation abc).".to_string(),
            ))
            .can_revert(false);
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None);
        assert_eq!(vcs.discards(), 0, "the live checkout is never reverted");
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty(), "got: {}", out.message);
    }
}
