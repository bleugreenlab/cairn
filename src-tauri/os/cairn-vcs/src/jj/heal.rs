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
//! Each commit's tree is built explicitly from its own pre-heal tree rather
//! than by re-applying its diff onto a repaired parent, so a commit the rebase
//! left clean keeps exactly the tree it had.

use super::*;
use std::path::Path;

/// The prefix every [`HealRefusal`] renders with: a positive finding that the
/// heal could not be made exact, as distinct from jj failing to run. Callers
/// classify by the type, never by this text.
pub const ANCESTRY_HEAL_GUARD: &str = "ancestry heal guard:";

/// Which of the heal's guards refused.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HealGuard {
    /// The healed tip does not hold the pre-heal tip's tree.
    TipTreeChanged,
    /// Annotating the healed commits' messages moved the tip off its tree.
    AnnotatedTipTreeChanged,
    /// A conflict-flagged commit names no conflicted path to repair.
    NoConflictedPaths { commit: String },
    /// A commit is still conflict-flagged after its conflicted paths took the
    /// clean tip's content.
    RemainsConflicted { commit: String },
    /// A conflict-flagged commit belongs to another branch's history.
    ForeignConflictedHistory { commit: String },
    /// The branch does not descend from the base it is healed against.
    NotDescendedFromBase,
    /// The tip itself is conflict-flagged, so there is no clean tree to keep.
    TipConflicted,
    /// The healed tip's tree root carried jj conflict scaffolding, so its
    /// verified export refused it.
    ExportScaffolding { detail: String },
}

/// A heal refused by one of its guards, with the coordinates that show why.
///
/// The tips are commits and the trees are tree objects, all immutable, so the
/// refusal can be checked later by anyone who reads it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HealRefusal {
    pub guard: HealGuard,
    /// The clean tip whose tree the heal must keep.
    pub pre_heal_tip: String,
    pub pre_heal_tree: String,
    /// The tip the heal produced, when it got that far.
    #[serde(default)]
    pub healed_tip: Option<String>,
    #[serde(default)]
    pub healed_tree: Option<String>,
    /// The paths at which the two trees differ, or the conflicted paths the
    /// refusal names.
    #[serde(default)]
    pub paths: Vec<String>,
}

impl HealRefusal {
    /// The guard's finding as one sentence, without [`ANCESTRY_HEAL_GUARD`].
    pub fn sentence(&self) -> String {
        let pre = format!(
            "the pre-heal tip `{}` (tree `{}`)",
            self.pre_heal_tip, self.pre_heal_tree
        );
        let healed = format!(
            "`{}` (tree `{}`)",
            self.healed_tip.as_deref().unwrap_or("?"),
            self.healed_tree.as_deref().unwrap_or("?")
        );
        let listed = |lead: &str| {
            if self.paths.is_empty() {
                String::new()
            } else {
                format!("; {lead}: {}", self.paths.join(", "))
            }
        };
        match &self.guard {
            HealGuard::TipTreeChanged => format!(
                "the healed tip {healed} does not hold the tree `{}` of the pre-heal tip `{}`{}",
                self.pre_heal_tree,
                self.pre_heal_tip,
                listed("differs in")
            ),
            HealGuard::AnnotatedTipTreeChanged => format!(
                "annotating the healed history moved the tip off its tree: {healed} does not hold \
                 the tree `{}` of the pre-heal tip `{}`{}",
                self.pre_heal_tree,
                self.pre_heal_tip,
                listed("differs in")
            ),
            HealGuard::NoConflictedPaths { commit } => format!(
                "commit `{commit}` below {pre} is conflict-flagged but names no conflicted path, \
                 so it cannot be repaired exactly"
            ),
            HealGuard::RemainsConflicted { commit } => format!(
                "commit `{commit}` below {pre} is still conflict-flagged after its conflicted \
                 paths took the tip's content{}",
                listed("conflicted in")
            ),
            HealGuard::ForeignConflictedHistory { commit } => format!(
                "commit `{commit}` below {pre} carries a recorded conflict and belongs to another \
                 branch's history, so this branch cannot heal it"
            ),
            HealGuard::NotDescendedFromBase => {
                format!("{pre} does not descend from the base it is healed against")
            }
            HealGuard::TipConflicted => format!(
                "{pre} is itself conflict-flagged, and only a clean tip's history can be healed"
            ),
            HealGuard::ExportScaffolding { detail } => format!(
                "the healed tip {healed} of {pre} carries jj conflict scaffolding, so its export \
                 was refused: {detail}"
            ),
        }
    }
}

impl std::fmt::Display for HealRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{ANCESTRY_HEAL_GUARD} {}", self.sentence())
    }
}

/// Why [`heal_conflicted_ancestry`] did not heal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealError {
    /// A guard's finding about the tree: the history cannot be healed exactly.
    Refused(Box<HealRefusal>),
    /// jj or the store failed to act; nothing was learned about the tree.
    Failed(String),
}

impl std::fmt::Display for HealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => refusal.fmt(f),
            Self::Failed(error) => f.write_str(error),
        }
    }
}

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
    /// Commits replay or healing left newly empty and abandoned, as
    /// `<short id> "<subject>"`. Typically a resolution commit, once its
    /// content lives in the commit that conflicted.
    pub dropped: Vec<String>,
}

/// A commit a store rewrite may change, as it needs to see it.
pub(super) struct ScopeCommit {
    pub(super) commit_id: String,
    pub(super) empty: bool,
    pub(super) merge: bool,
    pub(super) subject: String,
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// The commits of `revset`, oldest first: every commit is listed after all of
/// its ancestors in the set.
pub(super) fn scope_commits(
    jj: &JjEnv,
    store: &Path,
    revset: &str,
) -> Result<Vec<ScopeCommit>, String> {
    const TEMPLATE: &str = "commit_id ++ \"\\x1f\" ++ if(self.empty(), \"1\", \"0\") ++ \"\\x1f\" ++ if(parents.len() > 1, \"1\", \"0\") ++ \"\\x1f\" ++ description.first_line() ++ \"\\n\"";
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
            let empty = fields.next()? == "1";
            let merge = fields.next()? == "1";
            let subject = fields.next().unwrap_or_default().trim().to_string();
            Some(ScopeCommit {
                commit_id,
                empty,
                merge,
                subject,
            })
        })
        .collect())
}

pub(super) fn same_tree(jj: &JjEnv, store: &Path, from: &str, to: &str) -> Result<bool, String> {
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

/// The paragraph appended to a healed commit's message. A commit whose content
/// the heal changed says so, and says where the new content came from, so its
/// message never reads as the author's change alone.
pub(super) fn heal_note(
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

/// Heal a clean-tip branch through the canonical detached replay engine.
/// Only branch-owned history is rewritten; child bookmarks retain their own
/// coordinates and prepare independently. The caller holds the store writer
/// lock through capture, compute, installation and verified export.
pub fn heal_conflicted_ancestry(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest_commit: &str,
) -> Result<AncestryHeal, HealError> {
    super::advance::heal_branch_locked(jj, store, branch, dest_commit)
}

/// Put the store back where a refused `rewrite` of `branch` found it and prove
/// the branch's git ref followed. The reason still leads, so a guard refusal
/// stays recognizable.
pub(super) fn undo_branch_rewrite(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    pre_op: &str,
    rewrite: &str,
    reason: String,
) -> String {
    let cleanup = jj.cleanup_env();
    let jj = &cleanup;
    let undone = super::reconcile::restore_operation_and_export(
        jj,
        store,
        pre_op,
        branch,
        &format!("jj git export (refused {rewrite} undone)"),
    );
    match undone {
        Ok(()) => reason,
        Err(error) => {
            log::error!("{rewrite}: the refusal for {branch} could not be undone: {error}");
            format!(
                "{reason}\n{}",
                rollback_incomplete(&format!(
                    "the refused {rewrite} of `{branch}` could not be undone to operation \
                     {pre_op}: {error}"
                ))
            )
        }
    }
}
