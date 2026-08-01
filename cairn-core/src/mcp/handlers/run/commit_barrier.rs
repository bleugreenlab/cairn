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
}

/// Why a barrier that would otherwise roll a checkout back left it dirty
/// instead. Shared by all three non-revertable arms because it states one fact:
/// a revert is a destructive publication (it takes every uncommitted change
/// with it, not just this batch's), so it is only Cairn's to perform in a
/// checkout Cairn provisioned — as judged BEFORE the batch ran. See
/// [`crate::mcp::vcs::WorktreeVcs::can_revert`].
const UNOWNED_CHECKOUT_NOTE: &str = "the changes were left in place, because this checkout is not \
     one Cairn provisioned and reverting it would take every uncommitted change with it, not just \
     this run's. Review them (`git status`) and commit or discard them yourself.";

/// The request field an agent sets to commit deliberate literal conflict
/// markers from a `run` batch.
pub(super) const MARKER_ESCAPE_KEY: &str = "conflict_markers_reason";

/// Every conflict-marker line in the COMPLETE current content of the files this
/// batch is about to seal.
///
/// The paths come from the captured working-copy patch, but the content is read
/// back off disk rather than out of the patch's added lines. That is the whole
/// point: the dangerous shape is a marker the batch did not itself author — a
/// materialized conflict a script edited around, or a generated file that
/// inherited one — and a patch-scoped scan waves both through. A file the patch
/// names but that no longer exists (a deletion) simply contributes nothing.
fn working_tree_conflict_markers(
    checkout_path: &std::path::Path,
    patch: Option<&str>,
) -> Vec<cairn_common::conflict_scaffolding::ConflictMarkerHit> {
    let Some(patch) = patch else {
        return Vec::new();
    };
    crate::jj::parse_git_patch(patch)
        .iter()
        .map(|change| change.path.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter_map(|path| {
            let content = std::fs::read(checkout_path.join(&path)).ok()?;
            Some(cairn_common::conflict_scaffolding::conflict_markers_in_content(&path, &content))
        })
        .flatten()
        .collect()
}

/// Aggregate `(additions, deletions)` across a captured working-copy patch. The
/// commit barrier appends this to the committed-changes message so the run's
/// commit row can show lines-changed the way a write's file rows do.
fn aggregate_diff_stat(patch: &str) -> (i32, i32) {
    crate::jj::parse_git_patch(patch)
        .iter()
        .fold((0, 0), |(add, del), change| {
            (add + change.additions, del + change.deletions)
        })
}

/// Enforce the worktree==HEAD invariant after a `run` batch.
///
/// The single decision point for what happens to the worktree once a batch
/// finishes: commit it, restore it to HEAD, or leave it alone. It touches git
/// only inside `checkout_path` and returns the user-facing message plus whether
/// the worktree changed, so it is testable without an `Orchestrator`.
///
/// - `Some(msg)`: commit the worktree if dirty, even on partial item failure;
///   a commit failure restores the worktree to HEAD.
/// - `None`: when the batch fully succeeded and changed the worktree, restore
///   it to HEAD — no commit_msg means the new dirt must not persist.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_commit_barrier(
    vcs: &dyn crate::mcp::vcs::WorktreeVcs,
    checkout_path: &std::path::Path,
    commit_msg: Option<&str>,
    all_ok: bool,
    before: Option<&crate::mcp::vcs::VcsSnapshot>,
    author: Option<&GitAuthor>,
    marker_escape: Option<&str>,
) -> CommitBarrierOutcome {
    let mut message = String::new();
    let mut worktree_changed = false;
    let mut committed = false;

    match commit_msg {
        Some(commit_msg) => {
            // Commit the worktree even when some items failed: a partial-success
            // batch must not silently leave the successful items' dirt behind.
            if !matches!(vcs.is_dirty(checkout_path), Ok(false)) {
                // Capture the working-copy patch BEFORE the seal empties `@`, so a
                // successful commit can record its file changes on the run path.
                let patch = vcs.capture_patch(checkout_path);

                // The durable boundary for conflict scaffolding, on the carrier
                // where markers genuinely sit on disk. The refusal deliberately
                // does NOT discard: rolling the tree back here would erase the
                // resolution session the markers belong to, which is the one
                // outcome worse than refusing the commit.
                let marker_hits = working_tree_conflict_markers(checkout_path, patch.as_deref());
                if !marker_hits.is_empty() {
                    match marker_escape {
                        Some(reason) => {
                            log::warn!(
                                "run: conflict-marker guard bypassed on {} — {reason}",
                                marker_hits
                                    .iter()
                                    .map(|hit| hit.path.as_str())
                                    .collect::<std::collections::BTreeSet<_>>()
                                    .into_iter()
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            );
                            message.push_str(
                                &cairn_common::conflict_scaffolding::conflict_marker_bypass_note(
                                    &marker_hits,
                                    reason,
                                ),
                            );
                            message.push('\n');
                        }
                        None => {
                            message.push_str(
                                &cairn_common::conflict_scaffolding::conflict_marker_refusal(
                                    "commit this batch",
                                    &marker_hits,
                                    MARKER_ESCAPE_KEY,
                                    cairn_common::conflict_scaffolding::MarkerSource::WorkingTree,
                                ),
                            );
                            return CommitBarrierOutcome {
                                message,
                                worktree_changed,
                                committed,
                            };
                        }
                    }
                }

                match vcs.seal_all(checkout_path, commit_msg, author) {
                    Ok(commit_result) => {
                        worktree_changed = true;
                        committed = true;
                        let committed_patch = patch;
                        let pr_suffix = commit_result
                            .pr_number
                            .map(|pr| format!(" updated PR#{}", pr))
                            .unwrap_or_default();
                        // Annotate the committed-changes line with the sealed
                        // commit's aggregate `+adds/-dels`, kept OUTSIDE the
                        // `(sha)` parens so the sha parsers (frontend and archival
                        // `run_commit_sha`, both anchored on `(<sha>)`) stay
                        // intact. The run's commit-msg row parses this back out to
                        // render a diff stat like a write's per-file rows. Omitted
                        // for a zero-line change (a pure rename or mode change),
                        // where the row's DiffStat renders nothing anyway.
                        let stat_suffix = committed_patch
                            .as_deref()
                            .map(aggregate_diff_stat)
                            .filter(|&(add, del)| add > 0 || del > 0)
                            .map(|(add, del)| format!(" +{add}/-{del}"))
                            .unwrap_or_default();
                        message.push_str(&format!(
                            "\u{2713} Committed changes ({}){}{}",
                            commit_result.sha, stat_suffix, pr_suffix
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
                        if !vcs.can_revert() {
                            message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Hit a concurrent store advance: {e}; {UNOWNED_CHECKOUT_NOTE}"
                            ));
                            return CommitBarrierOutcome {
                                message,
                                worktree_changed,
                                committed,
                            };
                        }
                        let restore = vcs.discard(checkout_path);
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
                        if !vcs.can_revert() {
                            message.push_str(&format!(
                                "\u{26a0}\u{fe0f} Failed to commit: {e}; {UNOWNED_CHECKOUT_NOTE}"
                            ));
                            return CommitBarrierOutcome {
                                message,
                                worktree_changed,
                                committed,
                            };
                        }
                        let restore = vcs.discard(checkout_path);
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
                    match vcs.changed_since(checkout_path, before) {
                        Ok(true) => {
                            // A checkout Cairn does not own — the project's live
                            // checkout, or a jj repo the user colocated themselves
                            // — is never rolled back: the revert would take the
                            // user's own uncommitted work with it. The stray dirt is
                            // left in place and the agent is warned loudly instead
                            // of (falsely) told it was reverted. See
                            // `docs/worktree-fence.md`.
                            if !vcs.can_revert() {
                                message.push_str(&format!(
                                    "\u{26a0}\u{fe0f} Run changed this checkout but no commit_msg was given; {UNOWNED_CHECKOUT_NOTE} To make changes that persist, run in a worktree Cairn provisioned and pass commit_msg."
                                ));
                                return CommitBarrierOutcome {
                                    message,
                                    worktree_changed,
                                    committed,
                                };
                            }
                            let reset_ok = vcs.discard(checkout_path);
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
                            let _ = vcs.discard(checkout_path);
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

    /// A patch naming `path`, enough for the barrier to learn which files to
    /// re-read off disk. The barrier scans CONTENT, never this text.
    fn patch_naming(path: &str) -> String {
        format!(
            "diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n@@ -1 +1 @@\n-old\n+new\n"
        )
    }

    /// The conflicted file an agent is mid-resolution on.
    const CONFLICTED: &str =
        "fn main() {\n<<<<<<< HEAD\n    ours();\n=======\n    theirs();\n>>>>>>> main\n}\n";

    /// The load-bearing property of CAIRN-3197 as it now stands: markers may sit
    /// in a working tree, and may never be sealed. The refusal must ALSO leave
    /// the tree alone — discarding here would destroy the resolution the markers
    /// belong to, which is strictly worse than refusing the commit.
    #[test]
    fn marker_bearing_files_cannot_be_sealed_and_are_left_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("src/main.rs");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, CONFLICTED).unwrap();

        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch_naming("src/main.rs")));
        let out = run_commit_barrier(
            &vcs,
            dir.path(),
            Some("resolve conflict"),
            true,
            None,
            None,
            None,
        );

        assert_eq!(vcs.seals(), 0, "a marker-bearing tree is never sealed");
        assert_eq!(
            vcs.discards(),
            0,
            "the refusal must not discard the resolution session"
        );
        assert!(!out.committed);
        assert!(!out.worktree_changed, "Cairn mutated nothing");
        assert!(out.message.contains("src/main.rs"), "got: {}", out.message);
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            CONFLICTED,
            "the marker-bearing file must survive the refusal untouched"
        );
    }

    /// The scan is CONTENT-scoped, not patch-scoped, and this is the shape that
    /// proves it: the batch's own patch adds an ordinary line, while the marker
    /// it must catch was already sitting in the file. A guard reading only added
    /// patch lines seals this and publishes a half-resolved merge.
    #[test]
    fn a_pre_existing_marker_the_batch_did_not_author_still_refuses() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), CONFLICTED).unwrap();
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch_naming("a.rs")));
        let out = run_commit_barrier(&vcs, dir.path(), Some("tidy"), true, None, None, None);
        assert_eq!(vcs.seals(), 0);
        assert!(out.message.contains("a.rs"), "got: {}", out.message);
    }

    #[test]
    fn every_marker_bearing_file_is_named_not_just_the_first() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), CONFLICTED).unwrap();
        std::fs::write(dir.path().join("b.rs"), CONFLICTED).unwrap();
        let vcs = FakeVcs::new().dirty(Ok(true)).capture(Some(format!(
            "{}{}",
            patch_naming("a.rs"),
            patch_naming("b.rs")
        )));
        let out = run_commit_barrier(&vcs, dir.path(), Some("tidy"), true, None, None, None);
        assert_eq!(vcs.seals(), 0);
        assert!(out.message.contains("a.rs"), "got: {}", out.message);
        assert!(out.message.contains("b.rs"), "got: {}", out.message);
    }

    /// A clean tree seals exactly as before. The guard costs a re-read of the
    /// batch's own changed files and nothing else.
    #[test]
    fn marker_free_content_seals_normally() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "fn main() {}\n").unwrap();
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch_naming("a.rs")));
        let out = run_commit_barrier(&vcs, dir.path(), Some("edit a"), true, None, None, None);
        assert_eq!(vcs.seals(), 1);
        assert!(out.committed);
    }

    /// The escape exists for literal markers in docs and fixtures. It commits,
    /// and it is never silent: the reason and the affected files ride out on the
    /// result so a reader of the transcript sees what landed and why.
    #[test]
    fn the_reason_bearing_escape_commits_and_is_audited() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("docs/x.md"), "").ok();
        std::fs::create_dir_all(dir.path().join("docs")).unwrap();
        std::fs::write(dir.path().join("docs/x.md"), CONFLICTED).unwrap();
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch_naming("docs/x.md")));
        let out = run_commit_barrier(
            &vcs,
            dir.path(),
            Some("document marker syntax"),
            true,
            None,
            None,
            Some("documenting conflict-marker syntax"),
        );
        assert_eq!(vcs.seals(), 1, "an explained bypass commits");
        assert!(out.committed);
        assert!(
            out.message.contains("guard bypassed")
                && out.message.contains("documenting conflict-marker syntax")
                && out.message.contains("docs/x.md"),
            "the bypass must be visible in the result: {}",
            out.message
        );
    }

    /// A deleted file the patch still names contributes nothing rather than
    /// erroring: the guard must not turn an unreadable path into a refusal.
    #[test]
    fn a_path_that_no_longer_exists_is_not_a_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch_naming("gone.rs")));
        let out = run_commit_barrier(&vcs, dir.path(), Some("delete"), true, None, None, None);
        assert_eq!(vcs.seals(), 1);
        assert!(out.committed);
    }

    #[test]
    fn commit_msg_commits_dirty_worktree_even_on_partial_failure() {
        // Some(msg) + dirty: a partial-success batch (all_ok=false) still seals
        // its dirt rather than stranding the successful items' changes.
        let vcs = FakeVcs::new().dirty(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), Some("add file"), false, None, None, None);
        assert_eq!(vcs.seals(), 1, "a dirty worktree must be sealed");
        assert_eq!(vcs.discards(), 0);
        assert!(out.worktree_changed);
        assert!(out.committed, "a real commit must set committed");
        assert!(out.message.contains("Committed"), "got: {}", out.message);
    }

    #[test]
    fn commit_message_carries_aggregate_diff_stat() {
        // A successful seal annotates the committed-changes line with the sealed
        // commit's aggregate `+adds/-dels`, kept outside the `(sha)` parens. This
        // is what the run's commit-msg row parses back out to render lines-changed
        // like a write's file rows.
        let patch =
            "diff --git a/x.rs b/x.rs\n--- a/x.rs\n+++ b/x.rs\n@@ -1,2 +1,3 @@\n a\n-b\n+c\n+d\n";
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch.to_string()));
        let out = run_commit_barrier(&vcs, wt(), Some("edit x"), true, None, None, None);
        assert!(out.committed);
        // The stat sits after the sha's closing paren so `run_commit_sha` and the
        // frontend hash parser (both anchored on `(<sha>)`) still work.
        assert!(
            out.message.contains(") +2/-1"),
            "message must carry the aggregate stat after the sha: {}",
            out.message
        );
    }

    #[test]
    fn commit_message_omits_stat_for_zero_line_change() {
        // A hunkless patch (a pure rename or mode change) has no `+`/`-` lines, so
        // no stat suffix is appended — the row's DiffStat would render nothing.
        let patch = "diff --git a/x.rs b/y.rs\nrename from x.rs\nrename to y.rs\n";
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .capture(Some(patch.to_string()));
        let out = run_commit_barrier(&vcs, wt(), Some("rename x"), true, None, None, None);
        assert!(out.committed);
        assert!(
            out.message.contains("Committed changes"),
            "got: {}",
            out.message
        );
        assert!(
            !out.message.contains('+') && !out.message.contains("/-"),
            "a zero-line change carries no stat suffix: {}",
            out.message
        );
    }

    #[test]
    fn commit_msg_with_clean_worktree_is_noop() {
        let vcs = FakeVcs::new().dirty(Ok(false));
        let out = run_commit_barrier(&vcs, wt(), Some("nothing"), true, None, None, None);
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
        let out = run_commit_barrier(&vcs, wt(), Some("will fail"), true, None, None, None);
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
        let out = run_commit_barrier(&vcs, wt(), Some("noop"), true, None, None, None);
        assert_eq!(vcs.discards(), 0, "nothing-to-commit must not restore");
        assert!(!out.committed);
        assert!(out.message.is_empty(), "got: {}", out.message);
    }

    #[test]
    fn none_commit_msg_reverts_changed_worktree() {
        // No commit_msg + a fully-successful batch that changed the worktree must
        // restore to HEAD: new dirt must not persist across calls.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None, None);
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
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None, None);
        assert_eq!(vcs.discards(), 0);
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty());
    }

    #[test]
    fn none_commit_msg_warns_without_reverting_when_backend_cannot_revert() {
        // A backend that does not own its checkout must NOT discard on a
        // no-commit_msg run that left dirt: the revert would take the user's own
        // uncommitted work with it. It warns instead.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true)).can_revert(false);
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None, None);
        assert_eq!(
            vcs.discards(),
            0,
            "a checkout Cairn does not own is never reverted"
        );
        assert_eq!(vcs.seals(), 0);
        assert!(!out.worktree_changed, "Cairn mutated nothing");
        assert!(!out.committed);
        assert!(
            out.message.contains("left in place") && out.message.contains("worktree"),
            "the agent is told the dirt stayed and where changes do persist: {}",
            out.message
        );
    }

    /// The seal-failure arms carry the same hazard as the hygiene revert and are
    /// gated the same way: a failed seal in a checkout Cairn does not own must
    /// not be "recovered" by discarding it, because `discard` takes every
    /// uncommitted change with it, not just the batch's.
    #[test]
    fn seal_failure_warns_without_reverting_when_backend_cannot_revert() {
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .seal(Err("pre-commit hook failed".to_string()))
            .can_revert(false);
        let out = run_commit_barrier(&vcs, wt(), Some("will fail"), true, None, None, None);
        assert_eq!(vcs.seals(), 1, "the seal is still attempted");
        assert_eq!(
            vcs.discards(),
            0,
            "a failed seal must not roll back a checkout Cairn does not own"
        );
        assert!(!out.committed);
        assert!(!out.worktree_changed, "Cairn mutated nothing");
        assert!(
            out.message.contains("Failed to commit") && out.message.contains("left in place"),
            "got: {}",
            out.message
        );
        assert!(
            !out.message.contains("restored to HEAD"),
            "the barrier must not claim a restore it did not perform: {}",
            out.message
        );
    }

    /// The concurrent-store-advance arm is gated too. Its normal recovery is a
    /// discard-and-retry, which is exactly the destructive action an unowned
    /// checkout must not receive.
    #[test]
    fn stale_seal_failure_warns_without_reverting_when_backend_cannot_revert() {
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .seal(Err(
                "Error: The working copy is stale (not updated since operation abc).".to_string(),
            ))
            .can_revert(false);
        let out = run_commit_barrier(&vcs, wt(), Some("write batch"), true, None, None, None);
        assert_eq!(vcs.seals(), 1);
        assert_eq!(vcs.discards(), 0);
        assert!(!out.committed);
        assert!(!out.worktree_changed);
        assert!(
            out.message.contains("concurrent store advance")
                && out.message.contains("left in place"),
            "got: {}",
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
        let out = run_commit_barrier(&vcs, wt(), None, false, Some(&before), None, None);
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

        let with_msg =
            run_commit_barrier(&NonWorktreeVcs, wt(), Some("work"), true, None, None, None);
        assert!(!with_msg.worktree_changed);
        assert!(!with_msg.committed);
        assert!(with_msg.message.is_empty());

        let no_msg =
            run_commit_barrier(&NonWorktreeVcs, wt(), None, true, Some(&before), None, None);
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
        let out = run_commit_barrier(&vcs, wt(), Some("write batch"), true, None, None, None);
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
        let out = run_commit_barrier(&vcs, wt(), Some("flatten"), true, None, None, None);
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
    fn none_commit_msg_reconciles_when_changed_since_is_stale() {
        // No commit_msg + a stale `changed_since` read (jj can't snapshot a stale
        // copy, so it errors rather than returning Ok(true)) must still reconcile
        // to HEAD via the stale-resilient discard, not skip the revert and orphan
        // the dirt. Classification flows through the real `crate::jj::is_stale_error`.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Err(
            "Error: The working copy is stale (not updated since operation abc).".to_string(),
        ));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None, None);
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
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None, None);
        assert_eq!(vcs.discards(), 0, "the live checkout is never reverted");
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty(), "got: {}", out.message);
    }
}
