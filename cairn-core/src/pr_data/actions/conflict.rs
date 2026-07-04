//! Conflicted-history reporting for a PR source branch: detection, bullet
//! formatting, and the shared recovery hint.

use std::path::Path;

/// A conflicted-history report for a PR source branch: the conflicted commits
/// (with files) that block the merge. Built only when the source actually
/// carries a conflict — `source_conflict_report` returns `None` for a clean
/// source, so `commits` is never empty.
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

/// `None` when the source branch is clean; `Some(report)` when its history
/// carries one or more recorded conflicts. Scopes enumeration to
/// `<target>..bookmarks(exact:source)` when `target_branch` is known (excluding
/// commits already on the target), else the source bookmark alone — which still
/// catches the conflict because jj propagates it to the tip.
///
/// jj records merge conflicts *inside* the commit, which GitHub still reports as
/// mergeable (and renders as garbage), so every PR read and merge boundary
/// trusts this over the GitHub mergeable bit. Read-side advisory: returns `None`
/// on any jj error (fail open), so it never weakens the load-bearing boolean
/// gate ([`jj_source_branch_conflicted`]) underneath.
pub(crate) fn source_conflict_report(
    jj_binary_path: &str,
    config_dir: &Path,
    repo_path: &str,
    source_branch: &str,
    target_branch: Option<&str>,
) -> Option<SourceConflictReport> {
    let jj = crate::jj::JjEnv::resolve(jj_binary_path, config_dir);
    let store = crate::jj::project_store_dir(config_dir, Path::new(repo_path));
    let source_revset = format!("bookmarks(exact:{source_branch:?})");
    let range = match target_branch {
        Some(target) => format!("bookmarks(exact:{target:?})..{source_revset}"),
        None => source_revset,
    };
    let commits = crate::jj::conflicted_commits(&jj, &store, &range);
    if commits.is_empty() {
        return None;
    }
    let tip_conflicted =
        crate::jj::branch_has_conflict(&jj, &store, source_branch).unwrap_or(false);
    Some(SourceConflictReport {
        commits,
        tip_conflicted,
    })
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

    /// The merge gate trusts jj's recorded conflict state: a sibling whose
    /// auto-rebase recorded a conflict is reported (with its commits and files),
    /// a cleanly-rebased sibling is not, and a non-jj project never gates.
    /// Self-skips when jj is unresolvable.
    ///
    /// Serialized on the shared `jj` key: like every test that drives the real
    /// jj binary, it must not run concurrently with another, or the spawned jj
    /// subprocesses contend and a `jj config set --repo` intermittently panics.
    #[test]
    #[serial_test::serial(jj)]
    fn source_conflict_report_gates_and_enumerates_recorded_conflicts() {
        let bin = match std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                crate::env::command("jj")
                    .arg("--version")
                    .output()
                    .ok()
                    .filter(|o| o.status.success())
                    .map(|_| "jj".to_string())
            }) {
            Some(bin) => bin,
            None => {
                eprintln!("skipping jj_source_branch_conflicted_gates_only_recorded_conflicts: jj not resolvable");
                return;
            }
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
        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
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
        let int = "agent/CAIRN-1940-coordinator-0";
        jj_cfg(&store, &["bookmark", "create", "-r", "main", int]);

        let overlap = "agent/CAIRN-1-builder-0";
        let clean = "agent/CAIRN-2-builder-0";
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
        crate::jj::reconcile_siblings(
            &jj,
            &store,
            int,
            &[
                (overlap.to_string(), ws_o.clone()),
                (clean.to_string(), ws_c.clone()),
            ],
        )
        .unwrap();

        let proj_path = proj.path().to_string_lossy().to_string();
        let report = source_conflict_report(&bin, home.path(), &proj_path, overlap, Some(int))
            .expect("a recorded-conflict sibling must gate the merge");
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
            source_conflict_report(&bin, home.path(), &proj_path, clean, Some(int)).is_none(),
            "a cleanly-rebased sibling must not gate"
        );

        // Resolve the overlapping sibling's conflict on top: the rebased
        // (intermediate) commit stays conflicted, but the TIP is now clean. The
        // report still fires (a conflict remains in the range) yet marks the tip
        // clean — so the relaxed merge gate treats it as auto-recoverable (the
        // guarded flatten) rather than a hard block.
        crate::jj::update_stale(&jj, &ws_o).unwrap();
        std::fs::write(ws_o.join("shared.rs"), "resolved\n").unwrap();
        crate::jj::seal(&jj, &ws_o, "resolve conflict", None).unwrap();
        let intermediate =
            source_conflict_report(&bin, home.path(), &proj_path, overlap, Some(int))
                .expect("a conflicted intermediate still enumerates in the report");
        assert!(
            !intermediate.commits.is_empty(),
            "the conflicted intermediate is still enumerated"
        );
        assert!(
            !intermediate.tip_conflicted,
            "a clean tip over a conflicted intermediate is not a tip conflict (auto-recoverable)"
        );

        // A non-jj project (no config marker) never gates.
        let git_proj = tempfile::tempdir().unwrap();
        assert!(source_conflict_report(
            &bin,
            home.path(),
            &git_proj.path().to_string_lossy(),
            overlap,
            Some(int),
        )
        .is_none());
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

        let recovery = conflict_recovery_hint("agent/CAIRN-1-builder-0", Some("main"));
        assert!(recovery.contains("agent/CAIRN-1-builder-0"));
        assert!(recovery.contains("`main`"));
        assert!(recovery.contains("resolve-at-base"));
        // An unknown target falls back to a generic placeholder, never panics.
        assert!(conflict_recovery_hint("src", None).contains("`target`"));
    }
}
