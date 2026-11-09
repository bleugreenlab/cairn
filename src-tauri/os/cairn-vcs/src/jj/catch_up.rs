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
//! The canonical kernel uses jj's own tree merge and writes only hidden objects.
//! The tree judged is the tree committed. Detached preparation and the locked
//! adapter share this kernel; only installation publishes the accepted candidate.
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
    /// or carries conflict scaffolding. It was not installed: the branch is exactly
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
/// Uses the same hidden-object kernel as detached preparation, then installs
/// through the two-coordinate CAS and guarded export. Conflicts and refusals
/// publish no operation and require no live mutation or undo.
pub fn merge_base_into_branch(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    dest_commit: &str,
    message: &str,
    carry: Option<&ResolutionCarry>,
) -> Result<MergeCatchUp, String> {
    let snapshot = super::advance::capture_replay(store, branch, dest_commit, false)?;
    let coordinate = snapshot.coordinate().clone();
    let budget = jj
        .operation_deadline
        .map(|deadline| deadline.saturating_duration_since(std::time::Instant::now()))
        .unwrap_or(super::env::JJ_DEFAULT_TIMEOUT);
    let candidate = match super::advance::compute_branch_merge_with_message(
        jj, snapshot, carry, message, budget,
    ) {
        Ok(candidate) => candidate,
        Err(AdvanceComputeError::Conflict(diagnostic)) => {
            return Ok(MergeCatchUp::Conflicted {
                diagnostic: *diagnostic,
            });
        }
        Err(AdvanceComputeError::Repository(reason))
            if reason.starts_with(MERGE_CATCH_UP_REFUSAL) =>
        {
            return Ok(MergeCatchUp::Refused { reason });
        }
        Err(error) => return Err(error.to_string()),
    };
    let head = candidate.head();
    if head == coordinate.head {
        return Ok(MergeCatchUp::AlreadyCurrent);
    }
    let old_base = if head == coordinate.base {
        None
    } else {
        // The kernel already proved uniqueness at the captured operation.
        Some(
            merge_bases(jj, store, &coordinate.head, &coordinate.base)?
                .into_iter()
                .next()
                .ok_or("catch-up merge lost its merge base")?,
        )
    };
    let installed =
        super::advance::install_legacy_replay(jj, candidate).map_err(|error| error.to_string())?;
    if let Err(error) = installed.export {
        return Err(roll_back_rebase(
            jj,
            store,
            branch,
            &coordinate.operation,
            error,
        ));
    }
    match old_base {
        Some(old_base) => Ok(MergeCatchUp::Merged {
            merge: head,
            old_base,
            new_base: coordinate.base,
        }),
        None => Ok(MergeCatchUp::FastForwarded { to: head }),
    }
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

    #[test]
    #[serial_test::serial(jj)]
    fn detached_merge_is_hidden_until_cas_install_and_keeps_exact_source_graph() {
        use jj_lib::object_id::ObjectId as _;
        use jj_lib::repo::Repo as _;
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let source = fx.commit(&["main"], BRANCH, "own.txt", "own\n");
        let dest = fx.commit(&["main"], "main", "base.txt", "base\n");
        export_bookmark_advance_guarded(&fx.jj, &fx.store, true, BRANCH, "fixture export").unwrap();
        let git_dir = git_backend_root(&fx.jj, &fx.store).unwrap();
        let git_tip = || {
            git_stdout(
                &fx.store,
                &[
                    "--git-dir",
                    &git_dir,
                    "rev-parse",
                    &format!("refs/heads/{BRANCH}"),
                ],
            )
        };
        let before = operation_id(&fx.jj, &fx.store).unwrap();
        let snapshot = capture_branch_advance(&fx.store, BRANCH, "main").unwrap();
        let candidate = compute_branch_merge_advance(
            &fx.jj,
            snapshot,
            None,
            std::time::Duration::from_secs(30),
        )
        .unwrap();
        let head = candidate.head();
        let tree = candidate.tree().unwrap();
        assert!(candidate.dropped_commit_ids().is_empty());
        assert_eq!(fx.tip(BRANCH), source);
        assert_eq!(git_tip(), source);
        assert_eq!(operation_id(&fx.jj, &fx.store).unwrap(), before);
        let repo = futures::executor::block_on(crate::load_repo_at_head(&fx.store)).unwrap();
        let object = repo
            .store()
            .get_commit(&jj_lib::backend::CommitId::try_from_hex(&head).unwrap())
            .unwrap();
        assert_eq!(
            object
                .parent_ids()
                .iter()
                .map(|id| id.hex())
                .collect::<Vec<_>>(),
            vec![source.clone(), dest.clone()]
        );
        assert!(
            !repo.index().has_id(object.id()).unwrap(),
            "hidden objects are not published in the live index"
        );
        let local = install_branch_advance_local(&fx.jj, candidate).unwrap();
        assert_eq!(local.tree, tree);
        assert_eq!(fx.tip(BRANCH), head);
        assert_eq!(git_tip(), source, "local acceptance does not export");
        let installed = export_installed_branch_advance(&fx.jj, local);
        assert!(installed.export.is_ok(), "{:?}", installed.export);
        assert_eq!(git_tip(), head);
        assert_eq!(fx.parents(&head), vec![source, dest]);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn detached_merge_refuses_either_moved_coordinate_and_expired_budget() {
        let Some(bin) = jj_bin() else { return };
        for moved in [BRANCH, "main"] {
            let fx = fixture(&bin);
            fx.commit(&["main"], BRANCH, "own.txt", "own\n");
            fx.commit(&["main"], "main", "base.txt", "base\n");
            let snapshot = capture_branch_advance(&fx.store, BRANCH, "main").unwrap();
            assert!(matches!(
                compute_branch_merge_advance(&fx.jj, snapshot, None, std::time::Duration::ZERO),
                Err(AdvanceComputeError::TimedOut)
            ));
            let snapshot = capture_branch_advance(&fx.store, BRANCH, "main").unwrap();
            let candidate = compute_branch_merge_advance(
                &fx.jj,
                snapshot,
                None,
                std::time::Duration::from_secs(30),
            )
            .unwrap();
            fx.commit(&[moved], moved, "later.txt", "later\n");
            let before = operation_id(&fx.jj, &fx.store).unwrap();
            assert!(matches!(
                install_branch_advance_local(&fx.jj, candidate),
                Err(AdvanceInstallError::CoordinatesChanged)
            ));
            assert_eq!(operation_id(&fx.jj, &fx.store).unwrap(), before);
        }
    }

    #[test]
    #[serial_test::serial(jj)]
    fn detached_merge_conflict_does_not_publish_or_replay() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let source = fx.commit(&["main"], BRANCH, "same.txt", "own\n");
        fx.commit(&["main"], "main", "same.txt", "base\n");
        let before = operation_id(&fx.jj, &fx.store).unwrap();
        let snapshot = capture_branch_advance(&fx.store, BRANCH, "main").unwrap();
        let result = compute_branch_merge_advance(
            &fx.jj,
            snapshot,
            None,
            std::time::Duration::from_secs(30),
        );
        assert!(matches!(result, Err(AdvanceComputeError::Conflict(_))));
        assert_eq!(operation_id(&fx.jj, &fx.store).unwrap(), before);
        assert_eq!(fx.tip(BRANCH), source);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn detached_merge_resolution_keeps_session_destination_when_main_moves() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let source = fx.commit(&["main"], BRANCH, "same.txt", "own\n");
        let dest = fx.commit(&["main"], "main", "same.txt", "base\n");
        let resolved = fx.commit(&[BRANCH], BRANCH, "same.txt", "resolved\n");
        let carry = ResolutionCarry::from_tip(&resolved, &["same.txt".to_string()]);
        assert!(capture_branch_merge_resolution(&fx.store, BRANCH, "main").is_err());
        assert!(capture_branch_merge_resolution(&fx.store, BRANCH, &dest[..12]).is_err());
        let snapshot = capture_branch_merge_resolution(&fx.store, BRANCH, &dest).unwrap();
        let candidate = compute_branch_merge_advance(
            &fx.jj,
            snapshot,
            Some(&carry),
            std::time::Duration::from_secs(30),
        )
        .unwrap();
        let later = fx.commit(&["main"], "main", "later.txt", "later\n");
        let local = install_branch_advance_local(&fx.jj, candidate).unwrap();
        let merge = local.head.clone();
        assert!(export_installed_branch_advance(&fx.jj, local)
            .export
            .is_ok());
        assert_eq!(fx.parents(&merge), vec![resolved.clone(), dest]);
        assert_eq!(fx.parents(&resolved), vec![source]);
        assert_eq!(fx.file(BRANCH, "same.txt"), "resolved\n");
        assert_eq!(fx.tip("main"), later);
        let snapshot = capture_branch_advance(&fx.store, BRANCH, "main").unwrap();
        let candidate = compute_branch_merge_advance(
            &fx.jj,
            snapshot,
            None,
            std::time::Duration::from_secs(30),
        )
        .unwrap();
        let local = install_branch_advance_local(&fx.jj, candidate).unwrap();
        let next = local.head.clone();
        assert!(export_installed_branch_advance(&fx.jj, local)
            .export
            .is_ok());
        assert_eq!(fx.parents(&next), vec![merge, later]);
        assert_eq!(fx.file(BRANCH, "same.txt"), "resolved\n");
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
        assert!(
            since
                .iter()
                .all(|op| !op.contains("rebase") && !op.contains("abandon")),
            "both paths install detached candidates without CLI rewrites: {since:?}"
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
            since
                .iter()
                .filter(|op| op.starts_with(&format!("advance `{child}` onto ")))
                .count(),
            1,
            "exactly one advance ran for the child: {since:?}"
        );
        assert!(
            since
                .iter()
                .all(|op| !op.starts_with(&format!("advance `{BRANCH}` onto "))),
            "the parent was not replayed: {since:?}"
        );
    }
}
