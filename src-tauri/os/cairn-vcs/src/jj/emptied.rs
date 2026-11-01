//! Commits a rewrite left with no changes.
//!
//! A managed branch carries commits Cairn wrote as well as its agent's: a
//! write check's formatting fix, a structural merge after a base advance, a
//! bind commit. Once the base carries the same change, a replay onto it leaves
//! such a commit empty. An empty commit has no content to lose, and dropping it
//! leaves the tree of every commit after it as it was, so it is never a choice
//! for anyone to make. Every rewrite that can empty a commit drops it:
//!
//! - the base-advance rebase skips the commits it empties (`jj rebase
//!   --skip-emptied` in [`super::rebase_branch_onto_with_resolution`]) and
//!   logs what it dropped;
//! - the conflict heal abandons the commits its restores empty, and names them
//!   in the healed commit's note;
//! - publication drops whatever empty commit got past both
//!   ([`drop_empty_commits`]), such as one from a store older than these rules
//!   or one jj made outside Cairn.
//!
//! A merge commit is never dropped: its bookmarks would have no single parent
//! to move to. What is dropped is always named as `<short id> "<subject>"`.

use super::heal::{same_tree, scope_commits, undo_branch_rewrite, ScopeCommit};
use super::*;
use std::collections::BTreeSet;
use std::path::Path;

/// How a dropped commit is named in a log line, a note, or a merge record.
pub(super) fn dropped_label(commit_id: &str, subject: &str) -> String {
    format!("{} \"{subject}\"", &commit_id[..commit_id.len().min(12)])
}

/// Abandon `commits`, moving any bookmark on one to its parent, and name them.
/// Without `--retain-bookmarks` jj deletes the bookmark of an abandoned commit,
/// which would take a branch whose tip is empty off its own history.
pub(super) fn abandon_empty(
    jj: &JjEnv,
    store: &Path,
    commits: &[&ScopeCommit],
    ctx: &str,
) -> Result<Vec<String>, String> {
    if commits.is_empty() {
        return Ok(Vec::new());
    }
    let mut args = vec!["abandon", "--retain-bookmarks", "--ignore-working-copy"];
    args.extend(commits.iter().map(|commit| commit.commit_id.as_str()));
    run_store_rewrite(jj, store, &args, ctx)?;
    Ok(commits
        .iter()
        .map(|commit| dropped_label(&commit.commit_id, &commit.subject))
        .collect())
}

fn own_range(dest: &str, branch: &str) -> String {
    format!("({dest})..bookmarks(exact:{branch:?})")
}

/// The branch's own commits above `dest`, as `(change id, label)`, read before
/// a rewrite that may drop some so [`dropped_since`] can name them after. A
/// failed read answers nothing: the report is a log line, never a reason to
/// refuse the rewrite.
pub(super) fn own_commits(
    jj: &JjEnv,
    store: &Path,
    dest: &str,
    branch: &str,
) -> Vec<(String, String)> {
    match scope_commits(jj, store, &own_range(dest, branch)) {
        Ok(commits) => commits
            .into_iter()
            .map(|commit| {
                let label = dropped_label(&commit.commit_id, &commit.subject);
                (commit.change_id, label)
            })
            .collect(),
        Err(error) => {
            log::debug!("emptied commits: could not read `{branch}`'s own commits: {error}");
            Vec::new()
        }
    }
}

/// The labels of the commits in `before` the branch no longer carries above
/// `dest`.
pub(super) fn dropped_since(
    jj: &JjEnv,
    store: &Path,
    dest: &str,
    branch: &str,
    before: &[(String, String)],
) -> Vec<String> {
    if before.is_empty() {
        return Vec::new();
    }
    let after: BTreeSet<String> = match scope_commits(jj, store, &own_range(dest, branch)) {
        Ok(commits) => commits.into_iter().map(|commit| commit.change_id).collect(),
        Err(error) => {
            log::debug!("emptied commits: could not re-read `{branch}`'s own commits: {error}");
            return Vec::new();
        }
    };
    before
        .iter()
        .filter(|(change, _)| !after.contains(change))
        .map(|(_, label)| label.clone())
        .collect()
}

/// The outcome of [`drop_empty_commits`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DroppedEmpty {
    /// The branch tip afterwards. Its tree is the tree the tip held before.
    pub tip: String,
    /// The commits dropped, oldest first, as `<short id> "<subject>"`. Empty
    /// when the branch carried none, in which case nothing was rewritten.
    pub dropped: Vec<String>,
}

/// Drop every empty commit `branch` carries above the `onto` bookmark, before
/// the branch is published or merged.
///
/// The bookmark stays on the surviving stack: a dropped tip moves it to its
/// parent, and the commits above a dropped one are rebased onto its parent.
/// Before anything outside the store sees the result it is proven to hold the
/// tip's tree, and the branch's git ref is exported onto it; any failure
/// restores the store to the operation before the drop.
///
/// A branch on which every commit is empty is refused rather than emptied: it
/// has nothing to land, and dropping its commits would only move it onto
/// `onto`. The caller MUST hold the per-store lock.
pub fn drop_empty_commits(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    onto: &str,
) -> Result<DroppedEmpty, String> {
    let pre_tip = bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("branch `{branch}` did not resolve"))?;
    let commits = scope_commits(
        jj,
        store,
        &own_range(&format!("bookmarks(exact:{onto:?})"), branch),
    )?;
    let empty: Vec<&ScopeCommit> = commits
        .iter()
        .filter(|commit| commit.empty && !commit.merge)
        .collect();
    if empty.is_empty() {
        return Ok(DroppedEmpty {
            tip: pre_tip,
            dropped: Vec::new(),
        });
    }
    if empty.len() == commits.len() {
        return Err(format!(
            "`{branch}` carries no changes beyond `{onto}`: every commit on it is empty ({}), so \
             there is nothing to land",
            empty
                .iter()
                .map(|commit| dropped_label(&commit.commit_id, &commit.subject))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let pre_op = operation_id(jj, store)?;
    let dropped = abandon_empty(
        jj,
        store,
        &empty,
        "jj abandon (empty commits before publication)",
    )?;
    let proven = (|| -> Result<String, String> {
        let tip = bookmark_commit(jj, store, branch).ok_or_else(|| {
            format!("branch `{branch}` did not resolve after its empty commits were dropped")
        })?;
        if !same_tree(jj, store, &pre_tip, &tip)? {
            return Err(format!(
                "dropping the empty commits of `{branch}` left its tip {tip} holding a tree other \
                 than its previous tip {pre_tip}'s; refusing to publish it"
            ));
        }
        export_bookmark_advance(
            jj,
            store,
            true,
            branch,
            "jj git export (empty commits dropped)",
        )?;
        Ok(tip)
    })();
    match proven {
        Ok(tip) => {
            log::info!(
                "dropped {} empty commit(s) from `{branch}` before publishing it: {}",
                dropped.len(),
                dropped.join(", ")
            );
            Ok(DroppedEmpty { tip, dropped })
        }
        Err(reason) => Err(undo_branch_rewrite(
            jj,
            store,
            branch,
            &pre_op,
            "empty-commit drop",
            reason,
        )),
    }
}
