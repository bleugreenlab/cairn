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

    fn expect_conflicted(conflicts: SourceConflicts, why: &str) -> SourceConflictReport {
        match conflicts {
            SourceConflicts::Conflicted(report) => report,
            SourceConflicts::Clean => panic!("{why}: reported clean"),
            SourceConflicts::Unknown(reason) => panic!("{why}: reported unknown: {reason}"),
        }
    }

    fn op_head_count(store: &std::path::Path) -> usize {
        std::fs::read_dir(store.join(".jj/repo/op_heads/heads"))
            .unwrap()
            .count()
    }

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

    /// The merge gate trusts jj's recorded conflict state: a sibling whose
    /// auto-rebase recorded a conflict is reported (with its commits and files),
    /// a cleanly-rebased sibling is not, and a non-jj project never gates.
    /// Records a jj-unavailable skip when jj is unresolvable.
    ///
    /// Serialized on the shared `jj` key: like every test that drives the real
    /// jj binary, it must not run concurrently with another, or the spawned jj
    /// subprocesses contend and a `jj config set --repo` intermittently panics.
    #[test]
    #[serial_test::serial(jj)]
    fn source_conflict_report_gates_and_enumerates_recorded_conflicts() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let run_git = |dir: &std::path::Path, args: &[&str]| {
            assert!(
                crate::env::git()
                    .args(args)
                    .current_dir(dir)
                    .status()
                    .unwrap()
                    .success(),
                "git {args:?}"
            );
        };
        let home = tempfile::tempdir().unwrap();
        let proj = tempfile::tempdir().unwrap();
        let wts = tempfile::tempdir().unwrap();

        run_git(proj.path(), &["init", "-q", "-b", "main"]);
        run_git(proj.path(), &["config", "user.email", "t@e.com"]);
        run_git(proj.path(), &["config", "user.name", "T"]);
        std::fs::write(proj.path().join("shared.rs"), "base\n").unwrap();
        run_git(proj.path(), &["add", "-A"]);
        run_git(proj.path(), &["commit", "-q", "-m", "base"]);

        // Build the shared store at the path the gate computes from config_dir.
        let jj = crate::jj::environment(&bin, home.path());
        let store = crate::jj::project_store_dir(home.path(), proj.path());
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
        let cfg = home.path().join("jj").join("config.toml");
        let jj_cfg = |cwd: &std::path::Path, args: &[&str]| {
            let out = crate::env::command(&bin)
                .args(args)
                .current_dir(cwd)
                .env("JJ_CONFIG", &cfg)
                .env("EDITOR", "true")
                .env("JJ_EDITOR", "true")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "jj {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        let int = "agent/cairn-1940-coordinator-0";
        jj_cfg(&store, &["bookmark", "create", "-r", "main", int]);

        let overlap = "agent/cairn-1-builder-0";
        let clean = "agent/cairn-2-builder-0";
        let ws_o = wts.path().join("o");
        let ws_c = wts.path().join("c");
        crate::jj::add_workspace(&jj, &store, &ws_o, overlap, int, None).unwrap();
        crate::jj::add_workspace(&jj, &store, &ws_c, clean, int, None).unwrap();
        std::fs::write(ws_o.join("shared.rs"), "overlap-change\n").unwrap();
        crate::jj::seal(&jj, &ws_o, "overlap", None).unwrap();
        std::fs::write(ws_c.join("other.rs"), "clean-change\n").unwrap();
        crate::jj::seal(&jj, &ws_c, "clean", None).unwrap();

        // Advance the integration tip conflictingly, then reconcile (rebase).
        jj_cfg(&store, &["new", int]);
        std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
        jj_cfg(&store, &["describe", "-m", "advance"]);
        jj_cfg(&store, &["bookmark", "set", int, "-r", "@"]);
        crate::jj::reconcile_siblings(&jj, &store, int, &[clean.to_string()]).unwrap();
        // The overlapping sibling's conflicted commit is built with a raw rebase.
        // A Cairn reconcile no longer produces one — it rolls a conflicting rebase
        // back — but this READ-side gate still has to report on branches that
        // carry conflicted commits from older stores or from `jj` run outside
        // Cairn, which is exactly the shape being constructed here.
        jj_cfg(
            &store,
            &["rebase", "-b", overlap, "-o", int, "--ignore-working-copy"],
        );

        let proj_path = proj.path().to_string_lossy().to_string();
        let capture = || StoreView::capture(&bin, home.path(), &proj_path);
        let report = expect_conflicted(
            source_conflict_report(&capture(), overlap, Some(int)),
            "a recorded-conflict sibling must gate the merge",
        );
        assert!(
            report.tip_conflicted,
            "the overlapping sibling's rebase records the conflict on its tip (a hard block)"
        );
        // The report names the offending commit(s) and conflicted file(s).
        assert!(
            !report.commits.is_empty(),
            "the conflicted source enumerates its commits"
        );
        assert!(
            report
                .commits
                .iter()
                .any(|c| c.files.iter().any(|f| f == "shared.rs")),
            "the conflicted file is named in the report"
        );
        // The shared formatters turn the report into the diagnostic surfaces all
        // share: a commit bullet and a concrete recovery path.
        let listing = format_conflicted_commits(&report.commits);
        assert!(
            listing.starts_with("- ") && listing.contains("shared.rs"),
            "the formatted listing bullets the conflicted commit and file: {listing}"
        );
        let recovery = conflict_recovery_hint(overlap, Some(int));
        assert!(
            recovery.contains("resolve-at-base") && recovery.contains(int),
            "the recovery hint names the resolve-at-base path: {recovery}"
        );

        assert!(
            matches!(
                source_conflict_report(&capture(), clean, Some(int)),
                SourceConflicts::Clean
            ),
            "a cleanly-rebased sibling must not gate"
        );

        // One refresh pass reads its head comparison and its conflicts through
        // one view: the PR at the conflicted tip is the branch head, and the
        // conflict verdict describes that same head.
        let conflicted_head = capture().bookmark_commit(overlap).unwrap();
        let (heads, conflicts) =
            super::super::refresh::probe_store(&capture(), overlap, int, &conflicted_head, true);
        assert_eq!(
            heads,
            crate::pr_data::publication::HeadComparison::Equal(conflicted_head.clone())
        );
        assert!(conflicts.tip_conflicted());
        let before_resolution = capture();

        // Resolve the overlapping sibling's conflict on top: the rebased
        // (intermediate) commit stays conflicted, but the TIP is now clean. The
        // report still fires (a conflict remains in the range) yet marks the tip
        // clean — so the relaxed merge gate treats it as auto-recoverable (the
        // guarded flatten) rather than a hard block.
        crate::jj::update_stale(&jj, &ws_o).unwrap();
        std::fs::write(ws_o.join("shared.rs"), "resolved\n").unwrap();
        crate::jj::seal(&jj, &ws_o, "resolve conflict", None).unwrap();
        let intermediate = expect_conflicted(
            source_conflict_report(&capture(), overlap, Some(int)),
            "a conflicted intermediate still enumerates in the report",
        );
        assert!(
            !intermediate.commits.is_empty(),
            "the conflicted intermediate is still enumerated"
        );
        assert!(
            !intermediate.tip_conflicted,
            "a clean tip over a conflicted intermediate is not a tip conflict (auto-recoverable)"
        );

        // The branch moved after the earlier view was captured. That view still
        // answers for its own moment, head and conflicts together, so a pass
        // never pairs the new head with the old verdict or the reverse.
        let resolved_head = capture().bookmark_commit(overlap).unwrap();
        assert_ne!(resolved_head, conflicted_head);
        assert_eq!(
            before_resolution.bookmark_commit(overlap),
            Some(conflicted_head.clone())
        );
        assert!(source_conflict_report(&before_resolution, overlap, Some(int)).tip_conflicted());
        let (heads, conflicts) =
            super::super::refresh::probe_store(&capture(), overlap, int, &conflicted_head, true);
        assert!(
            matches!(&heads, crate::pr_data::publication::HeadComparison::Divergent(d)
                if d.branch_head == resolved_head && d.pr_is_behind),
            "{heads:?}"
        );
        assert!(!conflicts.tip_conflicted());

        // A split operation log: two writers forked from one operation. The
        // pass reports unknown for both answers and leaves both heads.
        let cairn_vcs::SettledOperation::Single(operation) =
            cairn_vcs::settled_operation(&store).unwrap()
        else {
            panic!("the fixture store is settled before the fork");
        };
        for name in ["split-a", "split-b"] {
            jj_cfg(
                &store,
                &[
                    "--at-op",
                    operation.as_str(),
                    "bookmark",
                    "create",
                    "-r",
                    int,
                    name,
                    "--ignore-working-copy",
                ],
            );
        }
        assert_eq!(op_head_count(&store), 2, "fixture must fork the op log");
        let split = capture();
        assert!(matches!(split, StoreView::Unknown(_)));
        assert_eq!(split.bookmark_commit(overlap), None);
        assert!(source_conflict_report(&split, overlap, Some(int))
            .unknown()
            .is_some_and(|reason| reason.contains("2 concurrent operation heads")));
        let (heads, conflicts) =
            super::super::refresh::probe_store(&split, overlap, int, &resolved_head, true);
        assert_eq!(heads, crate::pr_data::publication::HeadComparison::Unknown);
        assert!(conflicts.unknown().is_some());
        assert!(unknown_conflict_note(conflicts.unknown().unwrap()).contains("unknown"));
        // A view captured before the fork still reads, pinned to its operation.
        assert_eq!(
            before_resolution.bookmark_commit(overlap),
            Some(conflicted_head)
        );
        assert_eq!(op_head_count(&store), 2, "no read settled the fork");

        // A non-jj project (no store) never gates.
        let git_proj = tempfile::tempdir().unwrap();
        assert!(matches!(
            source_conflict_report(
                &StoreView::capture(&bin, home.path(), &git_proj.path().to_string_lossy()),
                overlap,
                Some(int),
            ),
            SourceConflicts::Clean
        ));
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
