//! Conflicted-history reporting for a PR source branch: detection, bullet
//! formatting, and the shared recovery hint.

use std::path::{Path, PathBuf};

/// One immutable view of a project's branch store, captured once and shared by
/// every read in a pass, so the head, the base and the conflict verdict that
/// pass reports all describe the same moment.
///
/// A read through the bare jj CLI is not a read: loading the repository at its
/// head merges a split operation log and writes the merge, outside the store
/// writer's lock. A view is pinned to the store's one settled operation, found
/// without running jj, so nothing read through it can do that.
pub(crate) enum StoreView {
    /// Every command loads at the store's one settled operation.
    Pinned {
        jj: crate::jj::JjEnv,
        store: PathBuf,
    },
    /// The project has no branch store, so nothing can be recorded in one.
    Absent,
    /// The store could not be pinned: a split operation log, or a store that
    /// would not load. Every answer read through it is unknown.
    Unknown(String),
}

impl StoreView {
    pub(crate) fn capture(jj_binary_path: &str, config_dir: &Path, repo_path: &str) -> Self {
        let store = crate::jj::project_store_dir(config_dir, Path::new(repo_path));
        if !crate::jj::is_jj_dir(&store) {
            return Self::Absent;
        }
        match crate::jj::environment(jj_binary_path, config_dir).pin_settled_read(&store) {
            Ok(jj) => Self::Pinned { jj, store },
            Err(reason) => Self::Unknown(reason),
        }
    }

    /// The pinned environment and store, or why this view cannot answer.
    pub(crate) fn pinned(&self) -> Result<(&crate::jj::JjEnv, &Path), String> {
        match self {
            Self::Pinned { jj, store } => Ok((jj, store.as_path())),
            Self::Absent => {
                Err("this project has no branch store yet, so there is nothing to read".to_string())
            }
            Self::Unknown(reason) => Err(reason.clone()),
        }
    }

    /// The commit `branch` names in this view; `None` when it is absent or the
    /// view cannot answer.
    pub(crate) fn bookmark_commit(&self, branch: &str) -> Option<String> {
        match self {
            Self::Pinned { jj, store } if !branch.is_empty() => {
                crate::jj::bookmark_commit_checked(jj, store, branch)
                    .ok()
                    .flatten()
            }
            _ => None,
        }
    }

    /// Whether `ancestor` is `tip_revset` or one of its ancestors in this view;
    /// `false` when the view cannot answer.
    pub(crate) fn descends_from(&self, tip_revset: &str, ancestor: &str) -> bool {
        match self {
            Self::Pinned { jj, store } => {
                crate::jj::revset_descends_from(jj, store, tip_revset, ancestor)
            }
            Self::Absent | Self::Unknown(_) => false,
        }
    }
}

/// A read of exact commit IDs in `store`, pinned to an operation whose index
/// holds them all. Such a read needs no bookmark view, so a split operation log
/// does not refuse it; an absent store is refused rather than created.
pub(crate) fn immutable_store_read(
    jj_binary_path: &str,
    config_dir: &Path,
    store: &Path,
    commits: &[&str],
) -> Result<crate::jj::JjEnv, String> {
    if !crate::jj::is_jj_dir(store) {
        return Err(format!("no branch store exists at {}", store.display()));
    }
    crate::jj::environment(jj_binary_path, config_dir).pin_immutable_read(store, commits)
}

/// What a PR source branch's history says about conflicts, read through one
/// [`StoreView`].
pub(crate) enum SourceConflicts {
    /// No recorded conflict in the source's range, or no store to record one.
    Clean,
    Conflicted(SourceConflictReport),
    /// The store could not be read without changing it, or a probe failed. Not
    /// clean: nothing may be inferred from it, least of all mergeability.
    Unknown(String),
}

impl SourceConflicts {
    pub(crate) fn report(&self) -> Option<&SourceConflictReport> {
        match self {
            Self::Conflicted(report) => Some(report),
            Self::Clean | Self::Unknown(_) => None,
        }
    }

    pub(crate) fn tip_conflicted(&self) -> bool {
        self.report().is_some_and(|report| report.tip_conflicted)
    }

    pub(crate) fn unknown(&self) -> Option<&str> {
        match self {
            Self::Unknown(reason) => Some(reason),
            Self::Clean | Self::Conflicted(_) => None,
        }
    }
}

/// The sentence every surface renders for [`SourceConflicts::Unknown`].
pub(crate) fn unknown_conflict_note(reason: &str) -> String {
    format!(
        "⚠️ Conflict state unknown: the branch store could not be read without changing it \
         ({reason}). Mergeability is not reported until a later read can see it; a merge checks \
         the store itself."
    )
}

/// A conflicted-history report for a PR source branch: the conflicted commits
/// (with files) that block the merge. Built only when the source actually
/// carries a conflict, so `commits` is never empty.
pub(crate) struct SourceConflictReport {
    pub commits: Vec<crate::jj::ConflictedCommit>,
    /// Whether the source bookmark's TIP carries a recorded conflict. A conflicted
    /// tip is a HARD block — a flatten preserves the tip tree, so only the agent
    /// resolving markers and re-sealing clears it. A clean tip with conflicted
    /// INTERMEDIATE commits (this is `false`) is AUTO-RECOVERABLE: the guarded
    /// flatten at merge time collapses the branch to its clean tip. The merge gate
    /// and the mergeable override key off this, not off the mere presence of a
    /// conflict somewhere in the range.
    pub tip_conflicted: bool,
}

/// The conflicts recorded in a PR source branch's history, read through `view`.
/// Scopes enumeration to `<target>..bookmarks(exact:source)` when
/// `target_branch` is known (excluding commits already on the target), else the
/// source bookmark alone, which still catches the conflict because jj
/// propagates it to the tip.
///
/// jj records merge conflicts *inside* the commit, which GitHub still reports as
/// mergeable (and renders as garbage), so every PR read trusts this over the
/// GitHub mergeable bit. A read-side advisory: an unpinnable store or a failed
/// probe is [`SourceConflicts::Unknown`], never clean, and the merge boundary
/// decides from the store itself.
pub(crate) fn source_conflict_report(
    view: &StoreView,
    source_branch: &str,
    target_branch: Option<&str>,
) -> SourceConflicts {
    let (jj, store) = match view {
        StoreView::Pinned { jj, store } => (jj, store),
        StoreView::Absent => return SourceConflicts::Clean,
        StoreView::Unknown(reason) => return SourceConflicts::Unknown(reason.clone()),
    };
    let source_revset = format!("bookmarks(exact:{source_branch:?})");
    let range = match target_branch {
        Some(target) => format!("bookmarks(exact:{target:?})..{source_revset}"),
        None => source_revset,
    };
    let commits = match crate::jj::conflicted_commits_checked(jj, store, &range) {
        Ok(commits) if commits.is_empty() => return SourceConflicts::Clean,
        Ok(commits) => commits,
        Err(error) => return SourceConflicts::Unknown(error),
    };
    match crate::jj::branch_has_conflict(jj, store, source_branch) {
        Ok(tip_conflicted) => SourceConflicts::Conflicted(SourceConflictReport {
            commits,
            tip_conflicted,
        }),
        Err(error) => SourceConflicts::Unknown(error),
    }
}

/// Render a conflicted-commit list as compact markdown bullets, one per commit,
/// naming the short commit id, description, and conflicted files. Shared by every
/// surface so the wording is identical wherever a conflicted history is reported.
pub(crate) fn format_conflicted_commits(commits: &[crate::jj::ConflictedCommit]) -> String {
    commits
        .iter()
        .map(|c| {
            let desc = if c.description.is_empty() {
                String::new()
            } else {
                format!(" ({:?})", c.description)
            };
            let files = if c.files.is_empty() {
                String::new()
            } else {
                format!(": {}", c.files.join(", "))
            };
            format!("- {}{}{}", c.commit_id, desc, files)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The concrete recovery sentence shared across all conflicted-history surfaces:
/// resolve the markers in the workspace and re-seal, or rebuild the branch
/// conflict-free onto the current target tip (resolve-at-base).
pub(crate) fn conflict_recovery_hint(source_branch: &str, target_branch: Option<&str>) -> String {
    let target = target_branch.unwrap_or("target");
    format!(
        "Recovery: resolve the conflict markers in `{source_branch}`'s workspace and let it re-seal, or rebuild `{source_branch}` conflict-free onto the current `{target}` tip (resolve-at-base)."
    )
}

/// Build the conflicted-history detail block (commit list + recovery hint) from
/// an already-resolved jj env + store. Used by the in-fold merge refusals where
/// `jj` and `store` are in hand; enumerates `range_revset & conflicts()`.
pub(super) fn conflicted_history_detail(
    jj: &crate::jj::JjEnv,
    store: &Path,
    range_revset: &str,
    source_branch: &str,
    target_branch: Option<&str>,
) -> String {
    let commits = crate::jj::conflicted_commits(jj, store, range_revset);
    let listing = if commits.is_empty() {
        String::new()
    } else {
        format!("\n{}", format_conflicted_commits(&commits))
    };
    format!(
        "{listing}\n{}",
        conflict_recovery_hint(source_branch, target_branch)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every branch-store read on the PR advisory surfaces goes through one
    /// pinned [`StoreView`]. None builds a bare jj environment of its own, whose
    /// first command would merge a split operation log outside the writer lock.
    #[test]
    fn advisory_store_reads_go_through_one_pinned_view() {
        for (name, source) in [
            ("refresh.rs", include_str!("refresh.rs")),
            ("node.rs", include_str!("../../resources/node.rs")),
            ("rebase.rs", include_str!("../../resources/rebase.rs")),
        ] {
            for unpinned in [
                "crate::jj::environment(",
                "crate::jj::bookmark_commit(",
                "crate::jj::revset_descends_from(",
                "crate::jj::conflicted_commits(",
                "crate::jj::branch_has_conflict(",
            ] {
                assert!(
                    !source.contains(unpinned),
                    "{name} reads the store through `{unpinned}` instead of a StoreView"
                );
            }
        }
        let own = include_str!("conflict.rs");
        let production = own.split("#[cfg(test)]").next().unwrap();
        assert_eq!(
            production.matches("crate::jj::environment(").count(),
            2,
            "only StoreView::capture and immutable_store_read build an environment"
        );
        assert!(production.contains(".pin_settled_read(&store)"));
        assert!(production.contains(".pin_immutable_read(store, commits)"));
    }

    /// The shared formatters render a conflicted-commit list as bullets (commit,
    /// description, files) and a concrete resolve-at-base recovery sentence — the
    /// identical wording every surface (summary, artifact, merge error) emits.
    /// Pure: no jj, always runs.
    #[test]
    fn conflict_formatters_render_bullets_and_recovery() {
        let commits = vec![
            crate::jj::ConflictedCommit {
                commit_id: "27e4383e".into(),
                change_id: "ppvnsuwu".into(),
                description: "merged".into(),
                files: vec!["f.txt".into(), "g.rs".into()],
            },
            crate::jj::ConflictedCommit {
                commit_id: "deadbeef".into(),
                change_id: "zzzzzzzz".into(),
                description: String::new(),
                files: vec![],
            },
        ];
        assert_eq!(
            format_conflicted_commits(&commits),
            "- 27e4383e (\"merged\"): f.txt, g.rs\n- deadbeef"
        );

        let recovery = conflict_recovery_hint("agent/cairn-1-builder-0", Some("main"));
        assert!(recovery.contains("agent/cairn-1-builder-0"));
        assert!(recovery.contains("`main`"));
        assert!(recovery.contains("resolve-at-base"));
        // An unknown target falls back to a generic placeholder, never panics.
        assert!(conflict_recovery_hint("src", None).contains("`target`"));
    }
}
