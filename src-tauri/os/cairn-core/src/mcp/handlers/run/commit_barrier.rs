//! The post-batch commit barrier: enforce the worktree==HEAD invariant.

use cairn_vcs::PublicationIdentity;

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

/// Why the barrier leaves an ambient checkout as the batch left it: a revert
/// would take every uncommitted change with it, not just this batch's, and
/// none of these checkouts is Cairn's to roll back.
const LEFT_IN_PLACE_NOTE: &str = "the changes were left in place, because Cairn never reverts \
     this checkout: a revert would take every uncommitted change with it, not just this run's. \
     Review them (`git status`) and commit or discard them yourself.";

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

/// Settle an ambient `run` batch's physical checkout: commit it, or report what
/// the batch left behind.
///
/// It touches the checkout only through `vcs` and returns the user-facing
/// message plus whether a commit landed, so it is testable without an
/// `Orchestrator`. It never rolls the checkout back; see [`LEFT_IN_PLACE_NOTE`].
///
/// - `Some(msg)`: commit the checkout if dirty, even on partial item failure. A
///   refused or failed commit leaves the checkout exactly as it was.
/// - `None`: when the whole batch succeeded and left new changes, warn that they
///   were left in place.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_commit_barrier(
    vcs: &dyn crate::mcp::vcs::WorktreeVcs,
    checkout_path: &std::path::Path,
    commit_msg: Option<&str>,
    all_ok: bool,
    before: Option<&crate::mcp::vcs::VcsSnapshot>,
    author: Option<&PublicationIdentity>,
    marker_escape: Option<&str>,
) -> CommitBarrierOutcome {
    let mut message = String::new();
    let mut committed = false;

    match commit_msg {
        Some(commit_msg) => {
            // Commit even when some items failed: a partial-success batch must
            // not silently leave the successful items' changes behind.
            if !matches!(vcs.is_dirty(checkout_path), Ok(false)) {
                // Captured BEFORE the seal empties `@`, so a successful commit
                // can report its line counts.
                let patch = vcs.capture_patch(checkout_path);

                // The durable boundary for conflict scaffolding, on the carrier
                // where markers genuinely sit on disk. A refusal leaves the tree
                // exactly as it is, mid-resolution markers included.
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
                                worktree_changed: false,
                                committed: false,
                            };
                        }
                    }
                }

                match vcs.seal_all(checkout_path, commit_msg, author) {
                    Ok(commit_result) => {
                        committed = true;
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
                        let stat_suffix = patch
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
                    Err(e) if crate::mcp::vcs::is_cairn_residence_refusal(&e) => {
                        // Refused before jj ran: nothing in the checkout or the
                        // shared store moved.
                        message.push_str(&format!("\u{26a0}\u{fe0f} {e}"));
                    }
                    Err(e) => message.push_str(&format!(
                        "\u{26a0}\u{fe0f} Failed to commit: {e}; {LEFT_IN_PLACE_NOTE}"
                    )),
                }
            }
        }
        None => {
            // Gated on `all_ok`: a failed batch's own error is the headline, and
            // the hygiene warning must not mask it.
            if let (true, Some(before)) = (all_ok, before) {
                match vcs.changed_since(checkout_path, before) {
                    Ok(true) => message.push_str(&format!(
                        "\u{26a0}\u{fe0f} Run changed this checkout but no commit_msg was given; {LEFT_IN_PLACE_NOTE}"
                    )),
                    Err(e) if crate::mcp::vcs::is_cairn_residence_refusal(&e) => {
                        message.push_str(&format!("\u{26a0}\u{fe0f} {e}"));
                    }
                    // Detection is advisory: an unreadable checkout produces no
                    // warning rather than a failed run.
                    Ok(false) | Err(_) => {}
                }
            }
        }
    }

    CommitBarrierOutcome {
        message,
        worktree_changed: committed,
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
        assert!(!out.worktree_changed);
        assert!(!out.committed, "a clean no-op must not set committed");
        assert!(out.message.is_empty());
    }

    /// A failed seal is reported and the checkout is left exactly as it was,
    /// whatever the failure; the barrier never claims a restore.
    #[test]
    fn a_failed_seal_is_reported_and_left_in_place() {
        for failure in [
            "pre-commit hook failed",
            "Error: The working copy is stale (not updated since operation abc).",
            crate::jj::CONFLICTED_BRANCH_SEAL_MSG,
        ] {
            let vcs = FakeVcs::new()
                .dirty(Ok(true))
                .seal(Err(failure.to_string()));
            let out = run_commit_barrier(&vcs, wt(), Some("will fail"), true, None, None, None);
            assert_eq!(vcs.seals(), 1, "{failure}");
            assert!(!out.committed, "{failure}");
            assert!(!out.worktree_changed, "{failure}: Cairn mutated nothing");
            assert!(
                out.message.contains("Failed to commit")
                    && out.message.contains(failure)
                    && out.message.contains("left in place"),
                "got: {}",
                out.message
            );
        }
    }

    #[test]
    fn commit_msg_nothing_to_commit_is_clean_noop() {
        // The seal reports "nothing to commit" when the tree became clean; that is
        // already==HEAD, not a failure, so there is no message.
        let vcs = FakeVcs::new()
            .dirty(Ok(true))
            .seal(Err("nothing to commit, working tree clean".to_string()));
        let out = run_commit_barrier(&vcs, wt(), Some("noop"), true, None, None, None);
        assert!(!out.committed);
        assert!(out.message.is_empty(), "got: {}", out.message);
    }

    /// A residence refusal is reported as itself, not as a failed commit in
    /// somebody else's checkout.
    #[test]
    fn a_residence_refusal_is_reported_verbatim() {
        let vcs = FakeVcs::new().dirty(Ok(true)).seal(Err(
            "This checkout is a Cairn-managed agent workspace, and this run carries no agent \
             identity, so it cannot commit here."
                .to_string(),
        ));
        let out = run_commit_barrier(&vcs, wt(), Some("ambient"), true, None, None, None);
        assert!(!out.committed);
        assert!(!out.message.contains("Failed to commit"), "{}", out.message);
        assert!(
            out.message.contains("Cairn-managed agent workspace"),
            "{}",
            out.message
        );
    }

    /// No commit_msg and new changes: the run is warned, and nothing is
    /// reverted.
    #[test]
    fn none_commit_msg_warns_about_new_changes_and_leaves_them() {
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None, None);
        assert_eq!(vcs.seals(), 0);
        assert!(!out.worktree_changed, "Cairn mutated nothing");
        assert!(!out.committed);
        assert!(
            out.message.contains("no commit_msg") && out.message.contains("left in place"),
            "got: {}",
            out.message
        );
    }

    #[test]
    fn none_commit_msg_leaves_unchanged_worktree_alone() {
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(false));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None, None);
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty());
    }

    /// A change set the backend could not read is advisory: no warning, no
    /// failure, nothing touched.
    #[test]
    fn none_commit_msg_with_an_unreadable_checkout_says_nothing() {
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Err(
            "Error: The working copy is stale (not updated since operation abc).".to_string(),
        ));
        let out = run_commit_barrier(&vcs, wt(), None, true, Some(&before), None, None);
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty(), "got: {}", out.message);
    }

    #[test]
    fn none_commit_msg_leaves_failed_batch_dirt_for_inspection() {
        // Deliberate boundary: a failed batch (all_ok=false) with no commit_msg
        // keeps the hygiene gate out of the way so the failure's side effects stay
        // visible. The `all_ok` guard lives only here.
        let before = VcsSnapshot("entry".to_string());
        let vcs = FakeVcs::new().changed(Ok(true));
        let out = run_commit_barrier(&vcs, wt(), None, false, Some(&before), None, None);
        assert!(!out.worktree_changed);
        assert!(out.message.is_empty());
    }

    /// Over the read-only non-worktree sentinel the barrier is a clean no-op in
    /// both directions: with commit_msg it never seals (is_dirty=false), and
    /// without it there is nothing new to report (changed_since=false).
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
}
