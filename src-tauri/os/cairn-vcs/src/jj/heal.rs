//! Healing a branch whose tip is clean but whose own history carries
//! conflict-flagged commits, without collapsing that history.
//!
//! The shape arises whenever a resolution lands on a branch's tip while the
//! commit that originally conflicted stays below it: a take-committed-tip
//! replay restores the resolution into the tip only, and the older
//! resolve-on-top-and-re-seal sequence adds a resolving commit above the
//! conflicted one. jj refuses to push or fold such a branch, so something has
//! to clear the flag. Collapsing the whole branch onto its base clears it too,
//! but erases every commit the branch carried, child-PR landings on an
//! integration branch included, under a message that describes only the tip.
//!
//! The heal here resolves each conflict-flagged commit in place instead. The
//! commits below the first conflict keep their exact ids, every other path of
//! every commit keeps its content, and the tip's tree is proven unchanged.

use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// The prefix every guard refusal of [`heal_conflicted_ancestry`] starts with:
/// a positive finding that the heal could not be made exact, as distinct from
/// jj failing to run. Callers classify a refusal by it.
pub const ANCESTRY_HEAL_GUARD: &str = "ancestry heal guard:";

/// One conflict-flagged commit the heal resolved in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealedCommit {
    /// The commit's change id, which the rewrite preserves.
    pub change_id: String,
    /// The paths that now hold the branch's committed resolution.
    pub paths: Vec<String>,
}

/// The outcome of [`heal_conflicted_ancestry`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AncestryHeal {
    /// The branch tip after the heal. Its tree equals the tip's tree before it.
    pub tip: String,
    /// The branch's conflict-flagged commits resolved in place, oldest first.
    /// Empty when the branch had none.
    pub healed: Vec<HealedCommit>,
    /// Commits the heal left with no changes and abandoned, as
    /// `<short id> "<subject>"`. Typically a resolution commit, once its
    /// content lives in the commit that conflicted.
    pub dropped: Vec<String>,
}

/// A commit the heal may rewrite, as it needs to see it.
struct ScopeCommit {
    commit_id: String,
    change_id: String,
    empty: bool,
    conflict: bool,
    merge: bool,
    subject: String,
    /// The paths jj records as conflicted in this commit.
    files: Vec<String>,
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// The commits of `revset`, oldest first: every commit is listed after all of
/// its ancestors in the set.
fn scope_commits(jj: &JjEnv, store: &Path, revset: &str) -> Result<Vec<ScopeCommit>, String> {
    const TEMPLATE: &str = "commit_id ++ \"\\x1f\" ++ change_id ++ \"\\x1f\" ++ if(self.empty(), \"1\", \"0\") ++ \"\\x1f\" ++ if(self.conflict(), \"1\", \"0\") ++ \"\\x1f\" ++ if(parents.len() > 1, \"1\", \"0\") ++ \"\\x1f\" ++ self.conflicted_files().map(|f| f.path()).join(\"\\x1e\") ++ \"\\x1f\" ++ description.first_line() ++ \"\\n\"";
    let out = jj.run(
        store,
        &[
            "log",
            "-r",
            revset,
            "--reversed",
            "--no-graph",
            "-T",
            TEMPLATE,
            "--ignore-working-copy",
        ],
        "jj log (ancestry heal scope)",
    )?;
    Ok(out
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('\u{1f}');
            let commit_id = fields.next()?.trim().to_string();
            if commit_id.is_empty() {
                return None;
            }
            let change_id = fields.next()?.trim().to_string();
            let empty = fields.next()? == "1";
            let conflict = fields.next()? == "1";
            let merge = fields.next()? == "1";
            let files = fields
                .next()?
                .split('\u{1e}')
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(String::from)
                .collect();
            let subject = fields.next().unwrap_or_default().trim().to_string();
            Some(ScopeCommit {
                commit_id,
                change_id,
                empty,
                conflict,
                merge,
                subject,
                files,
            })
        })
        .collect())
}

/// Every visible conflict-flagged change in the store, so the heal can prove
/// no commit anywhere became conflicted because of it.
fn store_conflicted_changes(jj: &JjEnv, store: &Path) -> Result<BTreeSet<String>, String> {
    let out = jj.run(
        store,
        &[
            "log",
            "-r",
            "conflicts()",
            "--no-graph",
            "-T",
            "change_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (store conflicts)",
    )?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(String::from)
        .collect())
}

fn same_tree(jj: &JjEnv, store: &Path, from: &str, to: &str) -> Result<bool, String> {
    jj.run(
        store,
        &[
            "diff",
            "--from",
            from,
            "--to",
            to,
            "--summary",
            "--ignore-working-copy",
        ],
        "jj diff (ancestry heal tree check)",
    )
    .map(|out| out.trim().is_empty())
}

fn full_description(jj: &JjEnv, store: &Path, rev: &str) -> Result<String, String> {
    jj.run(
        store,
        &[
            "log",
            "-r",
            rev,
            "--no-graph",
            "-T",
            "description",
            "--ignore-working-copy",
        ],
        "jj log (ancestry heal description)",
    )
}

/// The paragraph appended to a healed commit's message. A commit whose content
/// the heal changed says so, and says where the new content came from, so its
/// message never reads as the author's change alone.
fn heal_note(
    dest: &str,
    tip: &str,
    tip_subject: &str,
    paths: &[String],
    dropped: &[String],
) -> String {
    let paths = paths
        .iter()
        .map(|path| format!("`{path}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut note = format!(
        "Cairn conflict heal: rebased onto {}, this commit carried a recorded conflict in \
         {paths}. Those paths now hold the branch's committed resolution from {} \
         (\"{tip_subject}\"); the rest of the commit is unchanged.",
        short(dest),
        short(tip),
    );
    if !dropped.is_empty() {
        note.push_str(&format!(
            "\nCommits this left with no changes were dropped: {}.",
            dropped.join(", ")
        ));
    }
    note
}

/// Clear the conflict flags from a clean-tip branch's history by resolving each
/// conflict-flagged commit in place, never by collapsing the branch.
///
/// Each pass takes the oldest conflict-flagged commit among the branch's
/// commits and everything built on them, and restores only its conflicted
/// paths:
///
/// - A commit of the branch (`dest..branch`) that was conflict-flagged before
///   the heal takes them from the branch's clean tip, which is the committed
///   resolution.
/// - A commit that was clean before the heal, and that the heal's own rewrite
///   re-conflicted, takes them from its own pre-heal version, so its tree comes
///   back unchanged. That covers the resolution commit above the healed one,
///   and a child branch built on this one that carries its own resolution.
/// - A descendant outside the branch that was already conflict-flagged is left
///   as it was; it is its own branch's to resolve.
///
/// jj rebases descendants after every restore, and the loop repeats until
/// nothing it owns is conflicted. What it preserves, and proves before it
/// exports anything:
///
/// - Commits below the first conflict are never rewritten, so they keep their
///   ids.
/// - Only conflicted paths change, so every other path of every commit keeps
///   its content, and every commit keeps its place and its author.
/// - The tip's tree equals the pre-heal tip's tree.
/// - No commit anywhere in the store became conflict-flagged because of it.
/// - No bookmark on the branch or anything built on it is left on a
///   conflict-flagged commit. Descendant bookmarks follow their commits through
///   every rewrite, so they end on the healed history, never on an abandoned
///   one; one whose own conflict the heal cannot clear refuses the heal.
///
/// Every healed commit's message gets a paragraph naming the paths and where
/// their content came from. A commit the heal leaves with no changes is
/// abandoned (an empty commit is debris that a later fold refuses) with its
/// bookmarks moved to its parent, and the first healed commit's paragraph
/// names it.
///
/// Any failure restores the store to the operation before the heal. A guard
/// refusal starts with [`ANCESTRY_HEAL_GUARD`]; an undo that does not complete
/// carries a [`rollback_incomplete`] line. The caller MUST hold the per-store
/// lock.
pub fn heal_conflicted_ancestry(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest_commit: &str,
) -> Result<AncestryHeal, String> {
    let branch_revset = format!("bookmarks(exact:{branch:?})");
    if !revset_descends_from(jj, store, &branch_revset, dest_commit) {
        return Err(format!(
            "{ANCESTRY_HEAL_GUARD} branch `{branch}` does not descend from `{dest_commit}`, so its \
             history is not measured against the base it sits on; refusing to rewrite it"
        ));
    }
    if branch_has_conflict(jj, store, branch)? {
        return Err(format!(
            "{ANCESTRY_HEAL_GUARD} the tip of `{branch}` is itself conflict-flagged, so there is no \
             committed resolution to carry into its history"
        ));
    }
    let pre_tip = bookmark_commit(jj, store, branch)
        .ok_or_else(|| format!("ancestry heal: branch `{branch}` did not resolve"))?;
    let range = format!("({dest_commit})..{branch_revset}");
    let branch_commits = scope_commits(jj, store, &range)?;
    if !branch_commits.iter().any(|commit| commit.conflict) {
        return Ok(AncestryHeal {
            tip: pre_tip,
            ..AncestryHeal::default()
        });
    }
    let scope = format!("({range})::");
    let before = scope_commits(jj, store, &scope)?;
    let pre_op = operation_id(jj, store)?;
    let conflicts_before = store_conflicted_changes(jj, store)?;
    let plan = HealPlan {
        branch,
        dest: dest_commit,
        range: &range,
        scope: &scope,
        pre_tip: &pre_tip,
        branch_commits: &branch_commits,
        before: &before,
        conflicts_before: &conflicts_before,
    };
    plan.run(jj, store)
        .map_err(|reason| undo_heal(jj, store, branch, &pre_op, reason))
}

/// Everything the heal pinned before its first rewrite.
struct HealPlan<'a> {
    branch: &'a str,
    dest: &'a str,
    /// `dest..branch`: the branch's own commits.
    range: &'a str,
    /// The branch's commits and every descendant of them.
    scope: &'a str,
    pre_tip: &'a str,
    branch_commits: &'a [ScopeCommit],
    before: &'a [ScopeCommit],
    conflicts_before: &'a BTreeSet<String>,
}

impl HealPlan<'_> {
    fn run(&self, jj: &JjEnv, store: &Path) -> Result<AncestryHeal, String> {
        let branch = self.branch;
        let on_branch: BTreeSet<&str> = self
            .branch_commits
            .iter()
            .map(|commit| commit.change_id.as_str())
            .collect();
        let pre_commit_of: BTreeMap<&str, &str> = self
            .before
            .iter()
            .map(|commit| (commit.change_id.as_str(), commit.commit_id.as_str()))
            .collect();
        let pre_conflicted: BTreeSet<&str> = self
            .before
            .iter()
            .filter(|commit| commit.conflict)
            .map(|commit| commit.change_id.as_str())
            .collect();
        let tip_subject = self
            .branch_commits
            .iter()
            .find(|commit| commit.commit_id == self.pre_tip)
            .map(|commit| commit.subject.clone())
            .unwrap_or_default();

        // Each pass clears one commit for good: a restored commit is clean, and
        // the rebase of its descendants never rewrites it again. So two passes
        // per commit (its own, and one if the heal re-conflicts it) bound the
        // loop.
        let budget = self.before.len() * 2 + 1;
        let mut healed: Vec<HealedCommit> = Vec::new();
        let mut converged = false;
        for _ in 0..budget {
            let next = scope_commits(jj, store, self.scope)?
                .into_iter()
                .find(|commit| {
                    commit.conflict
                        && (on_branch.contains(commit.change_id.as_str())
                            || !pre_conflicted.contains(commit.change_id.as_str()))
                });
            let Some(next) = next else {
                converged = true;
                break;
            };
            if next.files.is_empty() {
                return Err(format!(
                    "{ANCESTRY_HEAL_GUARD} commit {} is conflict-flagged but jj names no \
                     conflicted path in it, so there is nothing exact to restore",
                    short(&next.commit_id)
                ));
            }
            let takes_resolution = on_branch.contains(next.change_id.as_str())
                && pre_conflicted.contains(next.change_id.as_str());
            let source = if takes_resolution {
                self.pre_tip
            } else if let Some(own) = pre_commit_of.get(next.change_id.as_str()) {
                own
            } else {
                return Err(format!(
                    "{ANCESTRY_HEAL_GUARD} commit {} became conflict-flagged on `{branch}`'s \
                     history but did not exist before the heal",
                    short(&next.commit_id)
                ));
            };
            let mut args = vec![
                "restore",
                "--from",
                source,
                "--into",
                next.commit_id.as_str(),
                "--ignore-working-copy",
                "--",
            ];
            args.extend(next.files.iter().map(String::as_str));
            run_store_rewrite(jj, store, &args, "jj restore (ancestry heal)")?;
            reached_step("ancestry heal: commit resolved");
            if takes_resolution {
                match healed
                    .iter_mut()
                    .find(|entry| entry.change_id == next.change_id)
                {
                    Some(entry) => {
                        for path in next.files {
                            if !entry.paths.contains(&path) {
                                entry.paths.push(path);
                            }
                        }
                    }
                    None => healed.push(HealedCommit {
                        change_id: next.change_id,
                        paths: next.files,
                    }),
                }
            }
        }
        if !converged {
            return Err(format!(
                "{ANCESTRY_HEAL_GUARD} `{branch}` still carries conflict-flagged commits after \
                 {budget} restores; refusing to publish a partial heal"
            ));
        }

        let dropped = self.drop_emptied(jj, store, &pre_commit_of, &mut healed)?;
        for (index, entry) in healed.iter().enumerate() {
            let original = full_description(jj, store, &entry.change_id)?;
            let note = heal_note(
                self.dest,
                self.pre_tip,
                &tip_subject,
                &entry.paths,
                if index == 0 { &dropped } else { &[] },
            );
            let message = match original.trim_end() {
                "" => note,
                original => format!("{original}\n\n{note}"),
            };
            run_store_rewrite(
                jj,
                store,
                &[
                    "describe",
                    entry.change_id.as_str(),
                    "-m",
                    &message,
                    "--ignore-working-copy",
                ],
                "jj describe (ancestry heal note)",
            )?;
        }

        self.prove(jj, store)?;
        let tip = bookmark_commit(jj, store, branch).ok_or_else(|| {
            format!("ancestry heal: branch `{branch}` did not resolve after the heal")
        })?;
        export_bookmark_advance(jj, store, true, branch, "jj git export (ancestry heal)")?;
        log::info!(
            "ancestry heal: resolved {} conflict-flagged commit(s) of `{branch}` in place on {}, \
             dropped {} emptied commit(s)",
            healed.len(),
            short(self.dest),
            dropped.len()
        );
        Ok(AncestryHeal {
            tip,
            healed,
            dropped,
        })
    }

    /// Abandon the commits the heal left with no changes, moving their
    /// bookmarks to their parents, and return them for the note. A merge is
    /// kept, since its bookmarks would have no single parent to move to, and a
    /// commit that was already empty is not the heal's to remove.
    fn drop_emptied(
        &self,
        jj: &JjEnv,
        store: &Path,
        pre_commit_of: &BTreeMap<&str, &str>,
        healed: &mut Vec<HealedCommit>,
    ) -> Result<Vec<String>, String> {
        let pre_empty: BTreeSet<&str> = self
            .before
            .iter()
            .filter(|commit| commit.empty)
            .map(|commit| commit.change_id.as_str())
            .collect();
        let emptied: Vec<ScopeCommit> = scope_commits(jj, store, self.scope)?
            .into_iter()
            .filter(|commit| {
                commit.empty
                    && !commit.merge
                    && pre_commit_of.contains_key(commit.change_id.as_str())
                    && !pre_empty.contains(commit.change_id.as_str())
            })
            .collect();
        if emptied.is_empty() {
            return Ok(Vec::new());
        }
        let mut args = vec!["abandon", "--retain-bookmarks", "--ignore-working-copy"];
        args.extend(emptied.iter().map(|commit| commit.commit_id.as_str()));
        run_store_rewrite(
            jj,
            store,
            &args,
            "jj abandon (ancestry heal emptied commits)",
        )?;
        healed.retain(|entry| {
            !emptied
                .iter()
                .any(|commit| commit.change_id == entry.change_id)
        });
        let dropped: Vec<String> = emptied
            .iter()
            .map(|commit| format!("{} \"{}\"", short(&commit.commit_id), commit.subject))
            .collect();
        if healed.is_empty() {
            log::warn!(
                "ancestry heal: every commit of `{}` the resolution touched ended up empty and was \
                 dropped ({}); no surviving commit carries the note",
                self.branch,
                dropped.join(", ")
            );
        }
        Ok(dropped)
    }

    /// The heal's postconditions, checked before anything is exported.
    fn prove(&self, jj: &JjEnv, store: &Path) -> Result<(), String> {
        let branch = self.branch;
        if let Some(commit) = scope_commits(jj, store, self.range)?
            .into_iter()
            .find(|commit| commit.conflict)
        {
            return Err(format!(
                "{ANCESTRY_HEAL_GUARD} commit {} of `{branch}` is still conflict-flagged after the \
                 heal",
                short(&commit.commit_id)
            ));
        }
        let tip = bookmark_commit(jj, store, branch).ok_or_else(|| {
            format!("ancestry heal: branch `{branch}` did not resolve after the heal")
        })?;
        if !same_tree(jj, store, self.pre_tip, &tip)? {
            return Err(format!(
                "{ANCESTRY_HEAL_GUARD} the healed tip {} of `{branch}` does not hold the tree of \
                 its pre-heal tip {}; refusing to publish it",
                short(&tip),
                short(self.pre_tip)
            ));
        }
        // A bookmark anywhere on this history that is still conflict-flagged — a
        // child that edited the same path itself — would be exported as conflict
        // scaffolding. The heal is refused rather than publish it; the caller
        // rolls the replay back, child bookmarks included.
        let flagged = jj.run(
            store,
            &[
                "log",
                "-r",
                &format!("({}) & conflicts() & bookmarks()", self.scope),
                "--no-graph",
                "-T",
                "local_bookmarks.map(|b| b.name()).join(\"\\n\") ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "jj log (ancestry heal flagged bookmarks)",
        )?;
        let flagged: BTreeSet<&str> = flagged
            .lines()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .collect();
        if !flagged.is_empty() {
            return Err(format!(
                "{ANCESTRY_HEAL_GUARD} healing `{branch}` would leave bookmark(s) {} on \
                 conflict-flagged commits; their conflicts are their own, so the replay is rolled \
                 back rather than publish them",
                flagged
                    .iter()
                    .map(|name| format!("`{name}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let newly: Vec<String> = store_conflicted_changes(jj, store)?
            .difference(self.conflicts_before)
            .map(|change| short(change).to_string())
            .collect();
        if !newly.is_empty() {
            return Err(format!(
                "{ANCESTRY_HEAL_GUARD} healing `{branch}` would leave commit(s) {} newly \
                 conflict-flagged; refusing",
                newly.join(", ")
            ));
        }
        Ok(())
    }
}

/// Put the store back where the heal found it and prove the branch's git ref
/// followed. The reason still leads, so a guard refusal stays recognizable.
fn undo_heal(jj: &JjEnv, store: &Path, branch: &str, pre_op: &str, reason: String) -> String {
    let undone = restore_operation(jj, store, pre_op).and_then(|()| {
        export_bookmark_advance(
            jj,
            store,
            true,
            branch,
            "jj git export (refused ancestry heal undone)",
        )
    });
    match undone {
        Ok(()) => reason,
        Err(error) => {
            log::error!("ancestry heal: the refusal for {branch} could not be undone: {error}");
            format!(
                "{reason}\n{}",
                rollback_incomplete(&format!(
                    "the refused heal of `{branch}` could not be undone to operation {pre_op}: \
                     {error}"
                ))
            )
        }
    }
}
