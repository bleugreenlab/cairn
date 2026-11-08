//! Merge-shaped catch-up: bring a branch current with its base by ONE merge
//! commit whose parents are the branch tip and the base tip, instead of
//! replaying the branch's stack onto the base.
//!
//! A replay rewrites every commit the branch carries, so its cost grows with
//! the stack; on a long-lived branch with hundreds of commits it holds the
//! store lock for minutes and can conflict at every commit. A merge costs one
//! three-way tree merge whatever the stack length, and rewrites nothing: every
//! commit of the stack keeps its id, and the bookmark fast-forwards onto a new
//! child of its old tip.
//!
//! The merge is jj's own (`jj new --no-edit <tip> <dest>`), so the tree that
//! is judged is the tree that is committed, and for fixed parents it is
//! deterministic: a landing can redo it and prove it got the same tree.
//!
//! Which branches catch up this way is the orchestrator's decision (an
//! operator-set per-branch mode); this module only performs one merge, and
//! fails closed: a conflict, an unparseable result, a scaffolded tree, or an
//! ambiguous merge base leaves the branch exactly where it was.
use super::*;
use std::collections::BTreeSet;
use std::path::Path;

/// The marker every merge catch-up refusal carries, so the reconcile failure
/// classifier files it under [`MERGE_CATCH_UP_REFUSED`] and nowhere else.
pub const MERGE_CATCH_UP_REFUSAL: &str = "catch-up refused:";

/// Failure kind of a merge catch-up the store refused without trying (an
/// ambiguous merge base). Retrying cannot help until the tip or the base
/// changes, so it is parked on those coordinates.
pub const MERGE_CATCH_UP_REFUSED: &str = "merge_catch_up_refused";

/// What one merge-shaped catch-up did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeCatchUp {
    /// The destination is already in the branch's ancestry. Nothing moved.
    AlreadyCurrent,
    /// The branch had no commit of its own past the destination, so its
    /// bookmark was fast-forwarded to `to`; no merge commit was made.
    FastForwarded { to: String },
    /// One two-parent commit `merge` (parents: the old tip, then `new_base`)
    /// was created and the bookmark fast-forwarded onto it. `old_base` is the
    /// merge base the three-way merge used: the range `old_base..new_base` is
    /// what the merge brought in.
    Merged {
        merge: String,
        old_base: String,
        new_base: String,
    },
    /// The merge recorded a conflict, or produced a tree that does not parse
    /// or carries conflict scaffolding. It was undone: the branch is exactly
    /// where it was. The diagnostic's coordinates are (merge base, tip, dest).
    Conflicted { diagnostic: ConflictDiagnostic },
    /// The merge was not attempted, and nothing moved. `reason` begins with
    /// [`MERGE_CATCH_UP_REFUSAL`].
    Refused { reason: String },
}

/// The description of a catch-up merge commit. Publication requires every
/// commit to be described, and the subject says what the commit is.
pub fn catch_up_merge_message(target_name: &str, dest_commit: &str, branch: &str) -> String {
    let short: String = dest_commit.chars().take(12).collect();
    format!("Merge {target_name}@{short} into {branch} (catch-up by merge)")
}

/// Every commit id `revset` resolves to, in jj's log order.
fn revset_commit_ids(jj: &JjEnv, store: &Path, revset: &str) -> Result<Vec<String>, String> {
    Ok(jj
        .run(
            store,
            &[
                "log",
                "-r",
                revset,
                "--no-graph",
                "--ignore-working-copy",
                "-T",
                "commit_id ++ \"\\n\"",
            ],
            "jj log (catch-up merge revset)",
        )?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// The merge bases of two commits: `heads(::a & ::b)`. Exactly one for any
/// history a single three-way merge can judge; more is a criss-cross.
pub fn merge_bases(jj: &JjEnv, store: &Path, a: &str, b: &str) -> Result<Vec<String>, String> {
    revset_commit_ids(jj, store, &format!("heads(::({a}) & ::({b}))"))
}

/// The refusal for a branch whose tip and destination share more than one
/// merge base. One merge would have to pick a base, and a wrong pick reopens
/// or silently settles conflicts, so nothing is guessed.
pub fn ambiguous_merge_base_refusal(branch: &str, dest: &str, bases: &[String]) -> String {
    if bases.is_empty() {
        return format!(
            "{MERGE_CATCH_UP_REFUSAL} `{branch}` and {dest} share no merge base, so they cannot \
             be merged. The branch was left untouched."
        );
    }
    format!(
        "{MERGE_CATCH_UP_REFUSAL} `{branch}` and {dest} share {} merge bases ({}); one merge \
         would be ambiguous. The branch was left untouched. Resolve the merge by hand against the \
         base you intend and import it with `fast-forward-from-origin`, or clear the branch's \
         catch-up mode.",
        bases.len(),
        bases.join(", ")
    )
}

/// Paths changed between two revisions, by name only. Advisory: an error
/// yields an empty set.
fn changed_paths(jj: &JjEnv, store: &Path, from: &str, to: &str) -> BTreeSet<String> {
    diff_name_status(jj, store, from, to)
        .into_iter()
        .map(|(_, path)| path)
        .collect()
}

/// The paths `old_base..new_base` changed: what a catch-up merge brought in
/// from the base side, sorted.
pub fn base_side_paths(jj: &JjEnv, store: &Path, old_base: &str, new_base: &str) -> Vec<String> {
    changed_paths(jj, store, old_base, new_base)
        .into_iter()
        .collect()
}

/// Bring `branch` current with `dest_commit` by one merge commit.
///
/// The caller holds the store lock and, for an automatic pass, the operation
/// budget. `carry` is an explicitly requested resolution: the branch tip is
/// the owner's committed resolution (a single-parent commit on top of the tip
/// that conflicted), and each carried path is taken whole from it into the
/// merge. Only [`CarriedContent::FromTip`] is meaningful here, because the
/// owner resolved against this very destination.
///
/// Steps, all fail-closed:
/// 1. `dest_commit` already in `::tip`: [`MergeCatchUp::AlreadyCurrent`].
/// 2. The merge base `heads(::tip & ::dest)` must be exactly one commit, or
///    the merge is refused untouched.
/// 3. `jj new --no-edit <tip> <dest>` creates the merge without moving `@`.
/// 4. A carry restores its paths into the merge from the tip.
/// 5. A recorded conflict, a path both sides changed that parsed on each side
///    and fails in the merge, or conflict scaffolding at the tree root undoes
///    the operation and reports a conflict.
/// 6. The bookmark is moved forward onto the merge and exported through the
///    guarded export.
pub fn merge_base_into_branch(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest_commit: &str,
    message: &str,
    carry: Option<&ResolutionCarry>,
) -> Result<MergeCatchUp, String> {
    let tip = bookmark_commit_checked(jj, store, branch)?
        .ok_or_else(|| format!("catch-up merge: bookmark `{branch}` does not resolve"))?;
    if let Some(carry) = carry {
        if carry.expected_pre_tip != tip {
            return Err(format!(
                "resolved catch-up merge of `{branch}` refused before moving anything: it was \
                 requested with the branch at `{}`, and it is now at `{tip}`. Re-read \
                 cairn:~/rebase and request it again.",
                carry.expected_pre_tip
            ));
        }
        if carry
            .paths
            .iter()
            .any(|(_, content)| *content != CarriedContent::FromTip)
        {
            return Err(format!(
                "resolved catch-up merge of `{branch}`: a merge takes each resolved path whole \
                 from the committed tip; a carried text was offered, and nothing moved"
            ));
        }
    }
    if revset_descends_from(jj, store, &tip, dest_commit) {
        return Ok(MergeCatchUp::AlreadyCurrent);
    }
    if branch_is_ancestor_of(jj, store, branch, dest_commit) {
        fast_forward_bookmark(jj, store, branch, dest_commit)?;
        return Ok(MergeCatchUp::FastForwarded {
            to: dest_commit.to_string(),
        });
    }

    let bases = merge_bases(jj, store, &tip, dest_commit)?;
    if bases.len() != 1 {
        let reason = ambiguous_merge_base_refusal(branch, dest_commit, &bases);
        log::warn!("catch-up[{branch}] {reason}");
        return Ok(MergeCatchUp::Refused { reason });
    }
    let old_base = bases[0].clone();

    // Exact under the store lock: every operation after this one is ours.
    let pre_op = operation_id(jj, store)?;
    let pair = format!("children({tip}) & children({dest_commit})");
    let before: BTreeSet<String> = revset_commit_ids(jj, store, &pair)?.into_iter().collect();
    let new_merge = |before: &BTreeSet<String>| -> Result<String, String> {
        let after: BTreeSet<String> = revset_commit_ids(jj, store, &pair)?.into_iter().collect();
        let mut added: Vec<String> = after.difference(before).cloned().collect();
        match added.len() {
            1 => Ok(added.remove(0)),
            n => Err(format!(
                "catch-up merge: expected exactly one new merge of {tip} and {dest_commit}, found {n}"
            )),
        }
    };
    // One transaction: a lost lock repeats it safely and adds no second child.
    run_store_rewrite(
        jj,
        store,
        &[
            "new",
            "--no-edit",
            "--ignore-working-copy",
            "-m",
            message,
            &tip,
            dest_commit,
        ],
        "jj new (catch-up merge)",
    )
    .map_err(|error| roll_back_rebase(jj, store, branch, &pre_op, error))?;
    let mut merge =
        new_merge(&before).map_err(|error| roll_back_rebase(jj, store, branch, &pre_op, error))?;

    if let Some(carry) = carry.filter(|carry| !carry.paths.is_empty()) {
        let mut args = vec![
            "restore",
            "--from",
            tip.as_str(),
            "--into",
            merge.as_str(),
            "--ignore-working-copy",
            "--",
        ];
        let quoted: Vec<String> = carry
            .paths
            .iter()
            .map(|(path, _)| quote_fileset(path))
            .collect();
        args.extend(quoted.iter().map(String::as_str));
        run_store_rewrite(jj, store, &args, "jj restore (catch-up merge resolution)")
            .map_err(|error| roll_back_rebase(jj, store, branch, &pre_op, error))?;
        // The restore rewrote the merge, so it has a new id; the old one is
        // hidden and no longer a child of either parent.
        merge = new_merge(&before)
            .map_err(|error| roll_back_rebase(jj, store, branch, &pre_op, error))?;
        log::info!(
            "catch-up[{branch}] carried the committed resolution of {} path(s) into the merge",
            carry.paths.len()
        );
    }

    let conflicted = revset_has_conflict(jj, store, &merge)
        .map_err(|error| roll_back_rebase(jj, store, branch, &pre_op, error))?;
    if conflicted {
        let paths: Vec<String> = conflicted_commits(jj, store, &merge)
            .into_iter()
            .flat_map(|commit| commit.files)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut diagnostic =
            capture_conflict_diagnostic(jj, store, branch, dest_commit, Some(&tip), paths.clone());
        diagnostic.conflicted_tip = Some(merge.clone());
        roll_back_conflicting_rebase(jj, store, branch, dest_commit, &pre_op)?;
        log::info!(
            "catch-up[{branch}] merge of {tip} with {dest_commit} (base {old_base}) conflicted on \
             {} path(s) and was undone; the branch is unchanged: {}",
            paths.len(),
            if paths.is_empty() {
                "<none enumerated>".to_string()
            } else {
                paths.join(", ")
            }
        );
        return Ok(MergeCatchUp::Conflicted { diagnostic });
    }

    // Clean in jj's terms is a statement about lines. A path both sides
    // changed must still parse, or the merge is refused like a conflict.
    let own = changed_paths(jj, store, &old_base, &tip);
    let overlapping: Vec<String> = changed_paths(jj, store, &old_base, dest_commit)
        .into_iter()
        .filter(|path| own.contains(path))
        .collect();
    let failures = introduced_parse_failures(jj, store, &merge, &[&tip, dest_commit], &overlapping);
    if !failures.is_empty() {
        let paths: Vec<String> = failures
            .iter()
            .map(|failure| failure.path.clone())
            .collect();
        let mut diagnostic =
            capture_conflict_diagnostic(jj, store, branch, dest_commit, Some(&tip), paths);
        diagnostic.condition = ConflictCondition::Unparseable;
        diagnostic.conflicted_tip = Some(merge.clone());
        diagnostic.validation = Some(ValidationRefusal {
            commit: merge.clone(),
            failures: failures.clone(),
        });
        roll_back_conflicting_rebase(jj, store, branch, dest_commit, &pre_op)?;
        log::warn!(
            "catch-up[{branch}] merge {merge} recorded no conflict but does not parse; undone.\n{}",
            render_failures(&failures)
        );
        return Ok(MergeCatchUp::Conflicted { diagnostic });
    }

    // Judge the tree before the bookmark moves, not only at export: a moved
    // bookmark whose export is then refused is partial state.
    let scaffolding = git_backend_root(jj, store)
        .and_then(|git_dir| root_conflict_scaffolding(Some(jj), Path::new(&git_dir), &merge))
        .map_err(|error| roll_back_rebase(jj, store, branch, &pre_op, error))?;
    if !scaffolding.is_empty() {
        let mut diagnostic = capture_conflict_diagnostic(
            jj,
            store,
            branch,
            dest_commit,
            Some(&tip),
            overlapping.clone(),
        );
        diagnostic.condition = ConflictCondition::ContentConflict;
        diagnostic.conflicted_tip = Some(merge.clone());
        diagnostic.scaffolding = Some(ScaffoldingRefusal {
            commit: merge.clone(),
            entries: scaffolding.clone(),
        });
        roll_back_conflicting_rebase(jj, store, branch, dest_commit, &pre_op)?;
        log::error!(
            "catch-up[{branch}] merge {merge} recorded no conflict but its tree root carries jj \
             conflict scaffolding ({}); undone before the bookmark moved",
            scaffolding.join(", ")
        );
        return Ok(MergeCatchUp::Conflicted { diagnostic });
    }

    // A forward move: the merge is a child of the tip.
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            &merge,
            "--ignore-working-copy",
        ],
        "jj bookmark set (catch-up merge)",
    )
    .map_err(|error| roll_back_rebase(jj, store, branch, &pre_op, error))?;
    match export_bookmark_advance_guarded(jj, store, true, branch, "jj git export (catch-up merge)")
    {
        Ok(()) => {}
        Err(ExportError::ConflictScaffolding {
            commit, entries, ..
        }) => {
            let mut diagnostic = capture_conflict_diagnostic(
                jj,
                store,
                branch,
                dest_commit,
                Some(&tip),
                overlapping,
            );
            diagnostic.condition = ConflictCondition::ContentConflict;
            diagnostic.conflicted_tip = Some(merge.clone());
            diagnostic.scaffolding = Some(ScaffoldingRefusal { commit, entries });
            roll_back_conflicting_rebase(jj, store, branch, dest_commit, &pre_op)?;
            return Ok(MergeCatchUp::Conflicted { diagnostic });
        }
        Err(ExportError::Failed(error)) => {
            return Err(roll_back_rebase(
                jj,
                store,
                branch,
                &pre_op,
                format!("catch-up merge of `{branch}` moved the bookmark, and exporting it failed: {error}"),
            ));
        }
    }
    jj.check_operation_budget("catch-up merge follow-through")
        .map_err(|error| roll_back_rebase(jj, store, branch, &pre_op, error))?;
    log::info!(
        "catch-up[{branch}] merged {dest_commit} into {tip} as {merge} (base {old_base}); no \
         commit of the branch was rewritten"
    );
    Ok(MergeCatchUp::Merged {
        merge,
        old_base,
        new_base: dest_commit.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::*;
    use tempfile::TempDir;

    struct Fixture {
        _home: TempDir,
        _proj: TempDir,
        jj: JjEnv,
        store: std::path::PathBuf,
    }

    fn fixture(bin: &str) -> Fixture {
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        Fixture {
            _home: home,
            _proj: proj,
            jj,
            store,
        }
    }

    const BRANCH: &str = "agent/cairn-4113-builder-0";

    impl Fixture {
        /// One commit on `parents` writing `file`, with `bookmark` moved onto
        /// it. Built through the store's own working copy, as other store
        /// fixtures are.
        fn commit(&self, parents: &[&str], bookmark: &str, file: &str, content: &str) -> String {
            let mut args = vec!["new"];
            args.extend_from_slice(parents);
            self.jj.run(&self.store, &args, "fixture new").unwrap();
            if !file.is_empty() {
                std::fs::write(self.store.join(file), content).unwrap();
            }
            self.jj
                .run(
                    &self.store,
                    &["describe", "-m", &format!("{bookmark}: {file}")],
                    "fixture describe",
                )
                .unwrap();
            self.jj
                .run(
                    &self.store,
                    &["bookmark", "set", bookmark, "-r", "@", "--allow-backwards"],
                    "fixture bookmark",
                )
                .unwrap();
            self.tip(bookmark)
        }

        fn tip(&self, bookmark: &str) -> String {
            bookmark_commit(&self.jj, &self.store, bookmark).unwrap()
        }

        fn parents(&self, commit: &str) -> Vec<String> {
            self.jj
                .run(
                    &self.store,
                    &[
                        "log",
                        "-r",
                        commit,
                        "--no-graph",
                        "--ignore-working-copy",
                        "-T",
                        "parents.map(|c| c.commit_id()).join(\",\")",
                    ],
                    "fixture parents",
                )
                .unwrap()
                .split(',')
                .map(ToOwned::to_owned)
                .collect()
        }

        fn file(&self, rev: &str, path: &str) -> String {
            String::from_utf8_lossy(&file_show(&self.jj, &self.store, rev, path).unwrap())
                .into_owned()
        }

        fn operations(&self) -> Vec<String> {
            self.jj
                .run(
                    &self.store,
                    &[
                        "op",
                        "log",
                        "--no-graph",
                        "--ignore-working-copy",
                        "-T",
                        "description ++ \"\\n\"",
                    ],
                    "fixture op log",
                )
                .unwrap()
                .lines()
                .map(ToOwned::to_owned)
                .collect()
        }

        /// The descriptions of the operations recorded since `before`, newest
        /// first.
        fn operations_since(&self, before: &[String]) -> Vec<String> {
            let after = self.operations();
            after[..after.len() - before.len()].to_vec()
        }
    }

    fn message(dest: &str) -> String {
        catch_up_merge_message("main", dest, BRANCH)
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_disjoint_advance_adds_one_two_parent_commit_and_rewrites_nothing() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let base = fx.tip("main");
        fx.commit(&["main"], BRANCH, "one.rs", "one\n");
        fx.commit(&[BRANCH], BRANCH, "two.rs", "two\n");
        let tip = fx.commit(&[BRANCH], BRANCH, "three.rs", "three\n");
        let stack_before = fx
            .jj
            .run(
                &fx.store,
                &[
                    "log",
                    "-r",
                    &format!("{base}..{tip}"),
                    "--no-graph",
                    "--ignore-working-copy",
                    "-T",
                    "commit_id ++ \"\\n\"",
                ],
                "stack",
            )
            .unwrap();
        let dest = fx.commit(&["main"], "main", "main-only.rs", "main\n");
        let ops = fx.operations();

        let outcome =
            merge_base_into_branch(&fx.jj, &fx.store, BRANCH, &dest, &message(&dest), None)
                .unwrap();
        let MergeCatchUp::Merged {
            merge,
            old_base,
            new_base,
        } = outcome
        else {
            panic!("a disjoint advance merges cleanly: {outcome:?}");
        };
        assert_eq!(old_base, base);
        assert_eq!(new_base, dest);
        assert_eq!(fx.tip(BRANCH), merge, "the bookmark moved onto the merge");
        assert_eq!(fx.parents(&merge), vec![tip.clone(), dest.clone()]);
        assert_eq!(fx.file(BRANCH, "main-only.rs"), "main\n");
        assert_eq!(fx.file(BRANCH, "three.rs"), "three\n");
        let stack_after = fx
            .jj
            .run(
                &fx.store,
                &[
                    "log",
                    "-r",
                    &format!("{base}..{tip}"),
                    "--no-graph",
                    "--ignore-working-copy",
                    "-T",
                    "commit_id ++ \"\\n\"",
                ],
                "stack",
            )
            .unwrap();
        assert_eq!(stack_before, stack_after, "every stack commit keeps its id");
        let since = fx.operations_since(&ops);
        assert!(
            since
                .iter()
                .all(|op| !op.contains("rebase") && !op.contains("abandon")),
            "no rebase and no abandon ran: {since:?}"
        );
        assert_eq!(
            git_stdout(
                &fx.store,
                &[
                    "--git-dir",
                    &git_backend_root(&fx.jj, &fx.store).unwrap(),
                    "rev-parse",
                    &format!("refs/heads/{BRANCH}")
                ]
            ),
            merge,
            "the git ref was exported onto the merge"
        );
        let message = fx
            .jj
            .run(
                &fx.store,
                &[
                    "log",
                    "-r",
                    &merge,
                    "--no-graph",
                    "--ignore-working-copy",
                    "-T",
                    "description",
                ],
                "merge message",
            )
            .unwrap();
        assert!(message.starts_with("Merge main@"), "{message}");

        assert_eq!(
            merge_base_into_branch(&fx.jj, &fx.store, BRANCH, &dest, &message, None).unwrap(),
            MergeCatchUp::AlreadyCurrent,
            "a second pass at the same destination does nothing"
        );

        // The next advance merges against the TRUE base: the previous dest.
        let next = fx.commit(&["main"], "main", "later.rs", "later\n");
        let MergeCatchUp::Merged { old_base, .. } =
            merge_base_into_branch(&fx.jj, &fx.store, BRANCH, &next, &message, None).unwrap()
        else {
            panic!("the follow-up advance merges cleanly");
        };
        assert_eq!(
            old_base, dest,
            "after one merge the base is the merged dest"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_conflicting_advance_leaves_the_branch_untouched_with_merge_coordinates() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let base = fx.tip("main");
        let tip = fx.commit(&["main"], BRANCH, "shared.rs", "BRANCH\n");
        let dest = fx.commit(&["main"], "main", "shared.rs", "MAIN\n");
        let ops = fx.operations();

        let outcome =
            merge_base_into_branch(&fx.jj, &fx.store, BRANCH, &dest, &message(&dest), None)
                .unwrap();
        let MergeCatchUp::Conflicted { diagnostic } = outcome else {
            panic!("the same-hunk edit conflicts: {outcome:?}");
        };
        assert_eq!(fx.tip(BRANCH), tip, "the bookmark did not move");
        assert_eq!(diagnostic.base.as_deref(), Some(base.as_str()));
        assert_eq!(diagnostic.ours.as_deref(), Some(tip.as_str()));
        assert_eq!(diagnostic.theirs.as_deref(), Some(dest.as_str()));
        assert_eq!(
            diagnostic.conflicting_paths(),
            vec!["shared.rs".to_string()]
        );
        assert_eq!(fx.file(BRANCH, "shared.rs"), "BRANCH\n");
        let since = fx.operations_since(&ops);
        assert!(
            since.iter().all(|op| !op.contains("rebase")),
            "no rebase ran: {since:?}"
        );
        // Undone: no merge of the pair survives.
        assert!(
            revset_commit_ids(
                &fx.jj,
                &fx.store,
                &format!("children({tip}) & children({dest})")
            )
            .unwrap()
            .is_empty(),
            "the conflicted merge was undone"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_criss_cross_is_refused_untouched_naming_both_bases() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let a1 = fx.commit(&["main"], "side-a", "a.rs", "a\n");
        let b1 = fx.commit(&["main"], BRANCH, "b.rs", "b\n");
        let dest = fx.commit(&[&a1, &b1], "main", "", "");
        let tip = fx.commit(&[&b1, &a1], BRANCH, "", "");

        let outcome =
            merge_base_into_branch(&fx.jj, &fx.store, BRANCH, &dest, &message(&dest), None)
                .unwrap();
        let MergeCatchUp::Refused { reason } = outcome else {
            panic!("two merge bases are refused: {outcome:?}");
        };
        assert!(reason.starts_with(MERGE_CATCH_UP_REFUSAL), "{reason}");
        assert!(reason.contains(&a1) && reason.contains(&b1), "{reason}");
        assert_eq!(fx.tip(BRANCH), tip, "nothing moved");
        assert_eq!(
            reconcile_failure_kind(&reason),
            MERGE_CATCH_UP_REFUSED,
            "the refusal is its own failure kind, not a divergence"
        );
        assert!(reconcile_failure_is_permanent(MERGE_CATCH_UP_REFUSED));
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_committed_resolution_lands_as_the_merge_commit() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        fx.commit(&["main"], BRANCH, "shared.rs", "BRANCH\n");
        let dest = fx.commit(&["main"], "main", "shared.rs", "MAIN\n");
        fx.commit(&["main"], "main", "main-only.rs", "main\n");
        let dest_after = fx.tip("main");
        let MergeCatchUp::Conflicted { diagnostic } =
            merge_base_into_branch(&fx.jj, &fx.store, BRANCH, &dest, &message(&dest), None)
                .unwrap()
        else {
            panic!("precondition: the first merge conflicts");
        };

        // The owner resolves the markers and commits T' on top of the tip.
        let resolved = fx.commit(&[BRANCH], BRANCH, "shared.rs", "RESOLVED\n");
        let carry = ResolutionCarry::from_tip(&resolved, &diagnostic.conflicting_paths());
        let ops = fx.operations();
        let MergeCatchUp::Merged { merge, .. } = merge_base_into_branch(
            &fx.jj,
            &fx.store,
            BRANCH,
            &dest,
            &message(&dest),
            Some(&carry),
        )
        .unwrap() else {
            panic!("the resolution merges cleanly");
        };
        assert_eq!(fx.parents(&merge), vec![resolved.clone(), dest.clone()]);
        assert_eq!(fx.file(BRANCH, "shared.rs"), "RESOLVED\n");
        assert!(fx
            .operations_since(&ops)
            .iter()
            .all(|op| !op.contains("rebase")));

        // A carry computed from another tip is refused before anything moves.
        let stale = ResolutionCarry::from_tip(&resolved, &["shared.rs".to_string()]);
        assert!(merge_base_into_branch(
            &fx.jj,
            &fx.store,
            BRANCH,
            &dest_after,
            &message(&dest_after),
            Some(&stale)
        )
        .is_err());
        assert_eq!(fx.tip(BRANCH), merge);

        // Without a carry, the newer main merges in on top as usual.
        let MergeCatchUp::Merged { old_base, .. } = merge_base_into_branch(
            &fx.jj,
            &fx.store,
            BRANCH,
            &dest_after,
            &message(&dest_after),
            None,
        )
        .unwrap() else {
            panic!("the newer main merges cleanly over the resolution");
        };
        assert_eq!(old_base, dest);
        assert_eq!(fx.file(BRANCH, "main-only.rs"), "main\n");
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_merge_mode_reconcile_merges_and_a_plain_sibling_still_rebases() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let plain = "agent/cairn-1-builder-0";
        let merge_tip = fx.commit(&["main"], BRANCH, "one.rs", "one\n");
        let plain_tip = fx.commit(&["main"], plain, "plain.rs", "plain\n");
        let dest = fx.commit(&["main"], "main", "main-only.rs", "main\n");
        let ops = fx.operations();

        let report = reconcile_siblings_with_options(
            &fx.jj,
            &fx.store,
            &dest,
            &[BRANCH.to_string(), plain.to_string()],
            &ReconcileOptions {
                merge_mode: [BRANCH.to_string()].into_iter().collect(),
                target_name: Some("main".to_string()),
                ..ReconcileOptions::default()
            },
        )
        .unwrap();
        assert!(report.rebased_clean.contains(&BRANCH.to_string()));
        assert!(report.rebased_clean.contains(&plain.to_string()));
        let merged = report.merged_clean.get(BRANCH).expect("merge detail");
        assert_eq!(fx.parents(&merged.merge), vec![merge_tip, dest.clone()]);
        assert!(!merged.resolved);
        assert!(!report.merged_clean.contains_key(plain));
        assert_ne!(fx.tip(plain), plain_tip, "the plain sibling was rebased");
        assert_eq!(fx.parents(&fx.tip(plain)), vec![dest]);
        let since = fx.operations_since(&ops);
        assert_eq!(
            since.iter().filter(|op| op.contains("rebase")).count(),
            1,
            "exactly one rebase ran, the plain sibling's: {since:?}"
        );
    }

    /// A child cut from a merge-mode branch is not in the merge-mode set, so
    /// once its parent catches up by merge it still replays onto the parent's
    /// merge tip, carrying only its own commits and rewriting none of the
    /// parent's.
    #[test]
    #[serial_test::serial(jj)]
    fn a_child_of_a_merge_mode_branch_still_replays_onto_its_merge_tip() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let child = "agent/cairn-4114-builder-0";
        let base = fx.tip("main");
        fx.commit(&["main"], BRANCH, "one.rs", "one\n");
        let parent_tip = fx.commit(&[BRANCH], BRANCH, "two.rs", "two\n");
        let child_tip = fx.commit(&[BRANCH], child, "child.rs", "child\n");
        let dest = fx.commit(&["main"], "main", "main-only.rs", "main\n");
        let options = ReconcileOptions {
            merge_mode: [BRANCH.to_string()].into_iter().collect(),
            target_name: Some("main".to_string()),
            ..ReconcileOptions::default()
        };

        let parent_pass = reconcile_siblings_with_options(
            &fx.jj,
            &fx.store,
            &dest,
            &[BRANCH.to_string()],
            &options,
        )
        .unwrap();
        let merge = parent_pass
            .merged_clean
            .get(BRANCH)
            .expect("parent merged")
            .merge
            .clone();
        assert_eq!(fx.parents(&merge), vec![parent_tip.clone(), dest.clone()]);

        let ops = fx.operations();
        let child_pass = reconcile_siblings_with_options(
            &fx.jj,
            &fx.store,
            &merge,
            &[child.to_string()],
            &options,
        )
        .unwrap();
        assert!(child_pass.rebased_clean.contains(&child.to_string()));
        assert!(
            !child_pass.merged_clean.contains_key(child),
            "the child is not merged"
        );
        let rebased = fx.tip(child);
        assert_ne!(rebased, child_tip, "the child was replayed");
        assert_eq!(fx.parents(&rebased), vec![merge.clone()]);
        assert_eq!(
            revset_commit_ids(&fx.jj, &fx.store, &format!("{merge}..{rebased}")).unwrap(),
            vec![rebased.clone()],
            "only the child's own commit was carried"
        );
        assert_eq!(fx.file(child, "main-only.rs"), "main\n");
        assert_eq!(fx.file(child, "child.rs"), "child\n");
        assert_eq!(fx.tip(BRANCH), merge, "the parent did not move");
        assert_eq!(
            revset_commit_ids(&fx.jj, &fx.store, &format!("{base}..{parent_tip}"))
                .unwrap()
                .len(),
            2,
            "the parent's stack is intact"
        );
        let since = fx.operations_since(&ops);
        assert_eq!(
            since.iter().filter(|op| op.contains("rebase")).count(),
            1,
            "exactly one rebase ran, the child's: {since:?}"
        );
    }
}
