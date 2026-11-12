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
//! The canonical kernel (`compute_branch_merge_advance`) uses jj's own tree
//! merge and writes only hidden objects. The tree judged is the tree committed,
//! and only installation publishes the accepted candidate.
//!
//! Its destination is always captured from the store's own view of the base
//! bookmark, in the same operation as the branch tip, so a base tip origin has
//! but the store has not yet imported is never a coordinate: the pass catches
//! up to what the store holds, and the next pass after the import takes the
//! rest. There is no pinned destination that can fail to resolve.
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

        /// Merge the detached candidate for `BRANCH` onto the store's `main`,
        /// expecting the kernel to refuse it.
        fn refused_merge(&self) -> AdvanceComputeError {
            let snapshot = capture_branch_advance(&self.store, BRANCH, "main").unwrap();
            match compute_branch_merge_advance(
                &self.jj,
                snapshot,
                None,
                std::time::Duration::from_secs(30),
            ) {
                Ok(candidate) => panic!("the merge computed {}", candidate.head()),
                Err(error) => error,
            }
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
    fn a_conflicting_merge_names_its_merge_coordinates_and_leaves_the_branch() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let base = fx.tip("main");
        let tip = fx.commit(&["main"], BRANCH, "shared.rs", "BRANCH\n");
        let dest = fx.commit(&["main"], "main", "shared.rs", "MAIN\n");

        let AdvanceComputeError::Conflict(diagnostic) = fx.refused_merge() else {
            panic!("the same-hunk edit conflicts");
        };
        assert_eq!(diagnostic.base.as_deref(), Some(base.as_str()));
        assert_eq!(diagnostic.ours.as_deref(), Some(tip.as_str()));
        assert_eq!(diagnostic.theirs.as_deref(), Some(dest.as_str()));
        assert_eq!(
            diagnostic.conflicting_paths(),
            vec!["shared.rs".to_string()]
        );
        assert_eq!(fx.tip(BRANCH), tip, "the bookmark did not move");
        assert_eq!(fx.file(BRANCH, "shared.rs"), "BRANCH\n");
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_criss_cross_is_refused_untouched_naming_both_bases() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let a1 = fx.commit(&["main"], "side-a", "a.rs", "a\n");
        let b1 = fx.commit(&["main"], BRANCH, "b.rs", "b\n");
        fx.commit(&[&a1, &b1], "main", "", "");
        let tip = fx.commit(&[&b1, &a1], BRANCH, "", "");

        let AdvanceComputeError::Repository(reason) = fx.refused_merge() else {
            panic!("two merge bases are refused");
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

    /// A base tip that has reached the store's backing Git repository but not
    /// its jj view (the window between a sync's fetch and its import) is not a
    /// destination yet. A pass in that window merges the base the store holds
    /// and fails nothing; the first pass after the import takes the new tip.
    #[test]
    #[serial_test::serial(jj)]
    fn a_base_tip_not_yet_imported_is_never_the_destination() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin);
        let source = fx.commit(&["main"], BRANCH, "own.txt", "own\n");
        let imported = fx.commit(&["main"], "main", "base.txt", "base\n");
        let git_dir = git_backend_root(&fx.jj, &fx.store).unwrap();
        let git = |args: &[&str]| {
            let mut full = vec![
                "-c",
                "user.name=Upstream",
                "-c",
                "user.email=upstream@example.com",
                "--git-dir",
                &git_dir,
            ];
            full.extend_from_slice(args);
            git_stdout(&fx.store, &full)
        };
        // Upstream advances main, and the fetch lands it in the backing Git
        // repository; nothing has imported it.
        let tree = git(&["rev-parse", &format!("{imported}^{{tree}}")]);
        let pending = git(&[
            "commit-tree",
            &tree,
            "-p",
            &imported,
            "-m",
            "upstream advance",
        ]);
        git(&["update-ref", "refs/heads/main", &pending]);

        let snapshot = capture_branch_advance(&fx.store, BRANCH, "main").unwrap();
        assert_eq!(snapshot.coordinate().head, source);
        assert_eq!(
            snapshot.coordinate().base,
            imported,
            "the destination is the base the store has imported"
        );
        let candidate = compute_branch_merge_advance(
            &fx.jj,
            snapshot,
            None,
            std::time::Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("the in-flight import fails nothing: {error}"));
        assert_eq!(candidate.coordinate().base, imported);
        drop(candidate);

        fx.jj
            .run(&fx.store, &["git", "import"], "fixture import")
            .unwrap();
        let snapshot = capture_branch_advance(&fx.store, BRANCH, "main").unwrap();
        assert_eq!(
            snapshot.coordinate().base,
            pending,
            "once imported, the new tip is the destination"
        );
        compute_branch_merge_advance(&fx.jj, snapshot, None, std::time::Duration::from_secs(30))
            .unwrap_or_else(|error| panic!("the imported tip merges: {error}"));
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
}
