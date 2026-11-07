//! Explicit, loss-checked recovery of a conflicted branch from a clean source.
//! No commits are rewritten: descendants and sibling bookmarks stay untouched.
use futures::StreamExt as _;
use jj_lib::backend::CommitId;
use jj_lib::commit::Commit;
use jj_lib::matchers::EverythingMatcher;
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::RefTarget;
use jj_lib::ref_name::RefName;
use jj_lib::repo::Repo as _;
use std::path::Path;

fn full_id(value: &str) -> Result<CommitId, String> {
    if value.len() != 40 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("rebuild requires full immutable 40-digit commit IDs".to_string());
    }
    CommitId::try_from_hex(value).ok_or_else(|| "invalid commit ID".to_string())
}

fn pin_ref(jj: &super::JjEnv, git_dir: &Path, name: &str, id: &str) -> Result<(), String> {
    let output = jj.command_output(
        cairn_platform::env::git()
            .arg("--git-dir")
            .arg(git_dir)
            .args([
                "update-ref",
                name,
                id,
                "0000000000000000000000000000000000000000",
            ]),
        std::time::Duration::from_secs(30),
        "pin rebuild recovery",
    )?;
    if !output.status.success() {
        let read = jj.command_output(
            cairn_platform::env::git()
                .arg("--git-dir")
                .arg(git_dir)
                .args(["rev-parse", "--verify", name]),
            std::time::Duration::from_secs(30),
            "verify rebuild pin",
        )?;
        if !read.status.success() || String::from_utf8_lossy(&read.stdout).trim() != id {
            return Err(format!(
                "pin rebuild recovery: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    Ok(())
}

async fn apply(
    destination: MergedTree,
    before: MergedTree,
    after: MergedTree,
) -> Result<MergedTree, String> {
    MergedTree::merge(Merge::from_vec(vec![
        (destination, "current base".to_string()),
        (before, "delta base".to_string()),
        (after, "branch delta".to_string()),
    ]))
    .await
    .map_err(|error| format!("merge rebuild delta: {error}"))
}

fn parent(commit: &Commit) -> Result<Commit, String> {
    if commit.parent_ids().len() != 1 {
        return Err(format!(
            "rebuild cannot prove a delta across merge/root commit {}",
            commit.id().hex()
        ));
    }
    commit
        .store()
        .get_commit(&commit.parent_ids()[0])
        .map_err(|e| e.to_string())
}

/// Native publication is authoritative even when the subsequent Git export fails.
/// Callers must accept branch metadata for `Accepted` before reporting export errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuildOutcome {
    Preview {
        summary: String,
    },
    Accepted {
        previous_tip: String,
        rebuilt_tip: String,
        base_commit: String,
        summary: String,
        export_error: Option<String>,
    },
}

/// Rebuild only the owning bookmark, under the caller's store lock. The source
/// must be a clean incarnation of a change still on the flagged tip's linear
/// ancestry. The owner explicitly attests the original source_base boundary;
/// every source commit above it must match its complete rewritten counterpart. Later deltas may change only resolved, ordinary
/// paths; native conflict edits and scaffold edits require human recovery.
/// A recovery Git ref is pinned before publication, outside heads/tags so no
/// ordinary import/export can turn the flagged tree into a publishable branch.
#[allow(clippy::too_many_arguments)]
pub fn rebuild_flagged_branch(
    jj: &super::JjEnv,
    store: &Path,
    branch: &str,
    base_branch: &str,
    source_commit: &str,
    source_base: &str,
    source_base_reason: &str,
    expected_tip: &str,
    dry_run: bool,
) -> Result<RebuildOutcome, String> {
    if source_base_reason.trim().is_empty() {
        return Err(
            "rebuild requires source_base_reason explaining the original branch cut provenance"
                .to_string(),
        );
    }
    let recovery = format!("refs/cairn/rebuild/{branch}/{expected_tip}");
    let request_record = serde_json::json!({
        "branch": branch, "source_commit": source_commit, "source_base": source_base,
        "source_base_reason": source_base_reason, "expected_tip": expected_tip
    })
    .to_string();
    let source_id = full_id(source_commit)?;
    let source_base_id = full_id(source_base)?;
    let expected_id = full_id(expected_tip)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(async {
        let repo = crate::load_repo_at_head(store).await.map_err(|e| e.to_string())?;
        let target = repo.view().get_local_bookmark(RefName::new(branch));
        if branch == base_branch {
            return Err("rebuild refuses to replace the base branch".to_string());
        }
        if target.as_normal() != Some(&expected_id) {
            // A retry of a failed export must not create another rebuilt head.
            if let Some(actual) = target.as_normal() {
                let current = repo.store().get_commit(actual).map_err(|e| e.to_string())?;
                let marker = format!("Cairn rebuild request: {request_record}");
                if current.description().lines().any(|line| line == marker) {
                    crate::require_clean_logical_tree(&current.tree(), "retry rebuilt export").await?;
                    let git_repo = jj_lib::git::get_git_repo(repo.store()).map_err(|e| e.to_string())?;
                    let pin = jj.command_output(
                        cairn_platform::env::git().arg("--git-dir").arg(git_repo.path())
                            .args(["rev-parse", "--verify", &recovery]),
                        std::time::Duration::from_secs(30), "verify rebuild recovery pin"
                    )?;
                    if !pin.status.success() || String::from_utf8_lossy(&pin.stdout).trim() != expected_tip {
                        return Err("rebuild export retry lacks its exact recovery pin".to_string());
                    }
                    let id = actual.hex();
                    let receipt = format!("refs/cairn/rebuild-result/{branch}/{id}");
                    let pin = jj.command_output(
                        cairn_platform::env::git().arg("--git-dir").arg(git_repo.path())
                            .args(["rev-parse", "--verify", &receipt]),
                        std::time::Duration::from_secs(30), "verify rebuilt result receipt"
                    )?;
                    if !pin.status.success() || String::from_utf8_lossy(&pin.stdout).trim() != id {
                        return Err("rebuild refused: branch advanced beyond the original rebuilt result; re-read its tip".to_string());
                    }
                    let base_commit = parent(&current)?.id().hex();
                    if dry_run {
                        return Ok(RebuildOutcome::Preview { summary: format!("Would retry only the export of already rebuilt `{branch}` at {id}; original flagged state: {recovery}") });
                    }
                    let export_error = super::export::export_selected_bookmarks_inner(jj, store, &[(branch, &id)]).await.err();
                    return Ok(RebuildOutcome::Accepted {
                        previous_tip: expected_tip.to_string(),
                        rebuilt_tip: id.clone(),
                        base_commit,
                        summary: format!("Retried export of already rebuilt `{branch}` at {id}; no new native head; recovery {recovery}"),
                        export_error,
                    });
                }
            }
            return Err("rebuild refused: branch moved or its bookmark is ambiguous; re-read its tip".to_string());
        }
        let head = repo.store().get_commit(&expected_id).map_err(|e| e.to_string())?;
        let head_tree = head.tree();
        if !head_tree.has_conflict() {
            let roots = head_tree.trees().await.map_err(|e| format!("inspect rebuild tip: {e}"))?;
            let root = roots.as_resolved().ok_or_else(|| "unresolved rebuild tip".to_string())?;
            if !root.entries_non_recursive().any(|entry| cairn_common::conflict_scaffolding::is_conflict_scaffolding_entry(entry.name().as_internal_str())) {
                return Err("rebuild is only for a conflict-bearing/scaffolded tip".to_string());
            }
        }
        let source = repo.store().get_commit(&source_id).map_err(|e| e.to_string())?;
        crate::require_clean_logical_tree(&source.tree(), "rebuild source").await?;
        let base_id = repo.view().get_local_bookmark(RefName::new(base_branch)).as_normal()
            .ok_or_else(|| "rebuild base is absent or ambiguous".to_string())?.clone();
        let base = repo.store().get_commit(&base_id).map_err(|e| e.to_string())?;
        crate::require_clean_logical_tree(&base.tree(), "rebuild base").await?;
        let fork = repo.store().get_commit(&source_base_id).map_err(|e| e.to_string())?;
        crate::require_clean_logical_tree(&fork.tree(), "owner-attested source base").await?;
        if repo.index().common_ancestors(std::slice::from_ref(&source_base_id), std::slice::from_ref(&source_id))
            .map_err(|e| e.to_string())? != vec![source_base_id.clone()] {
            return Err("rebuild source_base is not an ancestor of source_commit".to_string());
        }

        let mut cursor = head.clone();
        let mut later = Vec::new();
        while cursor.change_id() != source.change_id() {
            let previous = parent(&cursor)?;
            later.push(cursor);
            cursor = previous;
            if later.len() > 10000 {
                return Err("rebuild source was not found in the bounded linear branch ancestry".to_string());
            }
        }
        let mut original = source.clone();
        let mut rewritten = cursor;
        let mut prefix_length = 0;
        while original.id() != &source_base_id {
            crate::require_clean_logical_tree(&original.tree(), "clean source prefix").await?;
            if original.change_id() != rewritten.change_id() {
                return Err("rebuild cannot pair the complete source prefix with rewritten branch history; nothing moved".to_string());
            }
            let original_parent = parent(&original)?;
            let rewritten_parent = parent(&rewritten)?;
            let proven = apply(rewritten_parent.tree(), original_parent.tree(), original.tree()).await?;
            if proven.tree_ids() != rewritten.tree_ids() {
                return Err(format!("rebuild cannot prove {} is the unedited rewrite of source {}; nothing moved", rewritten.id().hex(), original.id().hex()));
            }
            original = original_parent;
            rewritten = rewritten_parent;
            prefix_length += 1;
            if prefix_length > 10000 {
                return Err("rebuild source range exceeds the bounded proof".to_string());
            }
        }
        let fork_tree = fork.tree();
        let source_tree = source.tree();
        let mut source_diff = fork_tree.diff_stream(&source_tree, &EverythingMatcher);
        let mut recovered_paths = Vec::new();
        while let Some(entry) = source_diff.next().await {
            entry.values.map_err(|e| e.to_string())?;
            recovered_paths.push(entry.path.into_internal_string());
        }
        let recovered_paths = recovered_paths.join(", ");
        let mut tree = apply(base.tree(), fork.tree(), source.tree()).await?;
        crate::require_clean_logical_tree(&tree, "rebuilt source on current base").await?;
        for commit in later.iter().rev() {
            let previous = parent(commit)?;
            let mut before = MergedTreeBuilder::new(tree.clone());
            let mut after = MergedTreeBuilder::new(tree.clone());
            let previous_tree = previous.tree();
            let commit_tree = commit.tree();
            let mut diff = previous_tree.diff_stream(&commit_tree, &EverythingMatcher);
            while let Some(entry) = diff.next().await {
                let values = entry.values.map_err(|e| e.to_string())?;
                let path = entry.path.as_internal_file_string();
                if path.split('/').any(cairn_common::conflict_scaffolding::is_conflict_scaffolding_entry)
                    || !values.before.is_resolved() || !values.after.is_resolved()
                {
                    return Err(format!("rebuild cannot prove later edit at `{path}` in {}; nothing moved", commit.id().hex()));
                }
                before.set_or_remove(entry.path.clone(), values.before);
                after.set_or_remove(entry.path, values.after);
            }
            tree = apply(tree, before.write_tree().await.map_err(|e| e.to_string())?,
                after.write_tree().await.map_err(|e| e.to_string())?).await?;
            crate::require_clean_logical_tree(&tree, "rebuilt later delta").await?;
        }

        if dry_run {
            return Ok(RebuildOutcome::Preview { summary: format!("Would rebuild `{branch}` onto {} from owner-attested {source_base}..{source_commit} (paths: {recovered_paths}), preserving {} later commits; proved {prefix_length} source pairs; provenance: {source_base_reason}; source diff: cairn:@/vcs/diff?from={source_base}&to={source_commit}&base=from&view=patch; flagged tip would be pinned at {recovery}", base_id.hex(), later.len()) });
        }
        // A failed publication may leave this extra safety pin, never lose one.
        let git_repo = jj_lib::git::get_git_repo(repo.store()).map_err(|e| e.to_string())?;
        pin_ref(jj, git_repo.path(), &recovery, expected_tip)?;
        let mut tx = repo.start_transaction();
        let rebuilt = tx.repo_mut().new_commit(vec![base_id.clone()], tree)
            .set_description(format!("Rebuild {branch} from clean {source_commit}\n\nRecovery: {recovery}\nSource base: {source_base}\nRebuild base: {}\nLater commits preserved: {}\nSource base provenance: {source_base_reason}\nCairn rebuild request: {request_record}", base_id.hex(), later.len()))
            .write().await.map_err(|e| e.to_string())?;
        pin_ref(jj, git_repo.path(), &format!("refs/cairn/rebuild-result/{branch}/{}", rebuilt.id().hex()), &rebuilt.id().hex())?;
        tx.repo_mut().set_local_bookmark_target(RefName::new(branch), RefTarget::normal(rebuilt.id().clone()));
        // Native authority commits first. A failed publication must never leave
        // Git ahead of an uncommitted native bookmark.
        #[cfg(test)]
        if FAIL_BEFORE_NATIVE_COMMIT.with(|fail| fail.replace(false)) {
            return Err("injected native rebuild commit failure".to_string());
        }
        tx.commit(format!("rebuild flagged branch {branch}; preserve {recovery}")).await
            .map_err(|e| format!("commit rebuild (recovery {recovery}): {e}"))?;
        let id = rebuilt.id().hex();
        let summary = format!("Rebuilt `{branch}` at {id} onto {} from owner-attested {source_base}..{source_commit} (paths: {recovered_paths}); preserved {} later commits. Source base provenance: {source_base_reason}. Flagged state: {recovery}. Descendant bookmarks were not moved.", base_id.hex(), later.len());
        let export_error = super::export::export_selected_bookmarks_inner(jj, store, &[(branch, &id)]).await.err();
        Ok(RebuildOutcome::Accepted {
            previous_tip: expected_tip.to_string(),
            rebuilt_tip: id,
            base_commit: base_id.hex(),
            summary,
            export_error,
        })
    })
}

#[cfg(test)]
thread_local! {
    static FAIL_BEFORE_NATIVE_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::super::*;
    use super::*;
    use tempfile::TempDir;

    struct Fixture {
        _home: TempDir,
        project: TempDir,
        jj: JjEnv,
        store: std::path::PathBuf,
        source: String,
        source_base: String,
        flagged: String,
    }

    impl Fixture {
        fn new() -> Option<Self> {
            Self::with_history(false, false)
        }

        fn with_history(rewritten_parent: bool, owned_prefix: bool) -> Option<Self> {
            let bin = jj_bin()?;
            let home = TempDir::new().unwrap();
            let project = TempDir::new().unwrap();
            let p = project.path();
            init_project(p);
            std::fs::write(p.join("second.rs"), "base\n").unwrap();
            git(p, &["add", "-A"]);
            git(p, &["commit", "-q", "-m", "second base file"]);
            if rewritten_parent {
                git(p, &["checkout", "-q", "-b", "old-parent"]);
                std::fs::write(p.join("inherited.txt"), "old parent content\n").unwrap();
                git(p, &["add", "-A"]);
                git(p, &["commit", "-q", "-m", "old parent work"]);
            }
            let root = git_stdout(p, &["rev-parse", "HEAD"]);
            git(p, &["checkout", "-q", "-b", "agent/test"]);
            if owned_prefix {
                std::fs::write(p.join("prefix.txt"), "original own prefix\n").unwrap();
                git(p, &["add", "-A"]);
                git(p, &["commit", "-q", "-m", "own prefix"]);
            }
            std::fs::write(p.join("shared.rs"), "branch\n").unwrap();
            std::fs::write(p.join("second.rs"), "branch second\n").unwrap();
            std::fs::write(p.join("owned.txt"), "owned\n").unwrap();
            git(p, &["add", "-A"]);
            git(p, &["commit", "-q", "-m", "branch work"]);
            let source = git_stdout(p, &["rev-parse", "HEAD"]);
            git(p, &["checkout", "-q", "-b", "bad", &root]);
            std::fs::write(p.join("shared.rs"), "wrong parent\n").unwrap();
            std::fs::write(p.join("second.rs"), "wrong second\n").unwrap();
            git(p, &["add", "-A"]);
            git(p, &["commit", "-q", "-m", "wrong base"]);
            git(p, &["checkout", "-q", "main"]);
            std::fs::write(p.join("incoming.txt"), "incoming\n").unwrap();
            git(p, &["add", "-A"]);
            git(p, &["commit", "-q", "-m", "new base work"]);
            git(p, &["checkout", "-q", "-b", "edited-prefix", &root]);
            std::fs::write(p.join("legitimate.txt"), "legitimate amended prefix\n").unwrap();
            git(p, &["add", "-A"]);
            git(p, &["commit", "-q", "-m", "prefix amendment content"]);
            git(p, &["checkout", "-q", "main"]);
            let jj = JjEnv::resolve(&bin, home.path());
            let store = home.path().join("store");
            ensure_project_store(&jj, &store, p).unwrap();
            rebase_recording_conflict(&jj, &store, "agent/test", "bad");
            let flagged = bookmark_commit_checked(&jj, &store, "agent/test")
                .unwrap()
                .unwrap();
            assert!(branch_has_conflict(&jj, &store, "agent/test").unwrap());
            Some(Self {
                _home: home,
                project,
                jj,
                store,
                source,
                source_base: root,
                flagged,
            })
        }

        fn rebuild(&self, preview: bool) -> Result<RebuildOutcome, String> {
            rebuild_flagged_branch(
                &self.jj,
                &self.store,
                "agent/test",
                "main",
                &self.source,
                &self.source_base,
                "fixture original branch cut",
                &self.flagged,
                preview,
            )
        }

        fn add_later(&mut self, path: &str, content: &str) {
            let child = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let repo = crate::load_repo_at_head(&self.store).await.unwrap();
                    let head = repo
                        .store()
                        .get_commit(&full_id(&self.flagged).unwrap())
                        .unwrap();
                    let mut tree = MergedTreeBuilder::new(head.tree());
                    let path = jj_lib::repo_path::RepoPathBuf::from_internal_string(path).unwrap();
                    let id = repo
                        .store()
                        .write_file(&path, &mut futures::io::Cursor::new(content.as_bytes()))
                        .await
                        .unwrap();
                    tree.set_or_remove(
                        path,
                        Merge::normal(jj_lib::backend::TreeValue::File {
                            id,
                            executable: false,
                            copy_id: jj_lib::backend::CopyId::placeholder(),
                        }),
                    );
                    let tree = tree.write_tree().await.unwrap();
                    let mut tx = repo.start_transaction();
                    let child = tx
                        .repo_mut()
                        .new_commit(vec![head.id().clone()], tree)
                        .set_description("later work")
                        .write()
                        .await
                        .unwrap();
                    tx.repo_mut().set_local_bookmark_target(
                        RefName::new("agent/test"),
                        jj_lib::op_store::RefTarget::normal(child.id().clone()),
                    );
                    tx.commit("later fixture edit").await.unwrap();
                    child.id().hex()
                });
            self.flagged = child;
        }
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_preserves_source_current_base_and_later_edits_and_pins_flagged_tip() {
        let Some(mut f) = Fixture::new() else { return };
        f.add_later("later.txt", "legitimate later work\n");
        // A sibling pinned at the flagged tip must not follow this recovery.
        f.jj.run(
            &f.store,
            &[
                "bookmark",
                "create",
                "child",
                "-r",
                &f.flagged,
                "--ignore-working-copy",
            ],
            "pin child",
        )
        .unwrap();
        let RebuildOutcome::Accepted {
            summary,
            previous_tip,
            rebuilt_tip,
            base_commit,
            export_error,
        } = f.rebuild(false).unwrap()
        else {
            panic!("expected accepted rebuild")
        };
        assert!(summary.contains("preserved 1 later commits"), "{summary}");
        assert_eq!(previous_tip, f.flagged);
        assert_eq!(
            bookmark_commit(&f.jj, &f.store, "agent/test").as_deref(),
            Some(rebuilt_tip.as_str())
        );
        assert_eq!(
            bookmark_commit(&f.jj, &f.store, "main").as_deref(),
            Some(base_commit.as_str())
        );
        assert_eq!(export_error, None);
        let rebuilt = bookmark_commit_checked(&f.jj, &f.store, "agent/test")
            .unwrap()
            .unwrap();
        assert!(!branch_has_conflict(&f.jj, &f.store, "agent/test").unwrap());
        assert_eq!(
            bookmark_commit_checked(&f.jj, &f.store, "child")
                .unwrap()
                .unwrap(),
            f.flagged
        );
        for (path, content) in [
            ("shared.rs", "branch"),
            ("owned.txt", "owned"),
            ("incoming.txt", "incoming"),
            ("later.txt", "legitimate later work"),
        ] {
            assert_eq!(
                git_stdout(f.project.path(), &["show", &format!("{rebuilt}:{path}")]),
                content
            );
        }
        let base = bookmark_commit_checked(&f.jj, &f.store, "main")
            .unwrap()
            .unwrap();
        assert_eq!(
            git_stdout(f.project.path(), &["rev-parse", &format!("{rebuilt}^")]),
            base
        );
        assert_eq!(
            git_stdout(
                f.project.path(),
                &[
                    "rev-parse",
                    &format!("refs/cairn/rebuild/agent/test/{}", f.flagged)
                ]
            ),
            f.flagged
        );
        assert!(root_conflict_scaffolding(None, f.project.path(), &rebuilt)
            .unwrap()
            .is_empty());
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_preview_and_stale_tip_leave_every_ref_unchanged() {
        let Some(f) = Fixture::new() else { return };
        let before = crate::read_local_bookmark_view(&f.store).unwrap();
        let RebuildOutcome::Preview { summary } = f.rebuild(true).unwrap() else {
            panic!("expected preview")
        };
        assert!(summary.contains("Would rebuild"));
        assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
        assert!(rebuild_flagged_branch(
            &f.jj,
            &f.store,
            "agent/test",
            "main",
            &f.source,
            &f.source_base,
            "fixture original branch cut",
            &f.source,
            false
        )
        .unwrap_err()
        .contains("branch moved"));
        assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
        let refs = git_stdout(
            f.project.path(),
            &["for-each-ref", "--format=%(refname)", "refs/cairn/rebuild"],
        );
        assert!(refs.is_empty());
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_refuses_edits_to_native_conflicts_or_scaffold_directories() {
        for path in ["shared.rs", ".jjconflict-side-0/owned.txt"] {
            let Some(mut f) = Fixture::new() else { return };
            f.add_later(path, "unproven resolution\n");
            let before = crate::read_local_bookmark_view(&f.store).unwrap();
            let error = f.rebuild(false).unwrap_err();
            assert!(error.contains("cannot prove later edit"), "{path}: {error}");
            assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
        }
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_refuses_a_new_base_that_conflicts_instead_of_flattening_over_it() {
        let Some(f) = Fixture::new() else { return };
        f.jj.run(
            &f.store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                "bad",
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "base moved",
        )
        .unwrap();
        let before = crate::read_local_bookmark_view(&f.store).unwrap();
        assert!(f
            .rebuild(false)
            .unwrap_err()
            .contains("native tree conflicts"));
        assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_refuses_unrecorded_edits_to_the_rewritten_source() {
        let Some(f) = Fixture::new() else { return };
        f.jj.run(
            &f.store,
            &[
                "restore",
                "--from",
                "main",
                "--into",
                &f.flagged,
                "incoming.txt",
                "--ignore-working-copy",
            ],
            "edit rewrite",
        )
        .unwrap();
        let tip = bookmark_commit_checked(&f.jj, &f.store, "agent/test")
            .unwrap()
            .unwrap();
        let before = crate::read_local_bookmark_view(&f.store).unwrap();
        let error = rebuild_flagged_branch(
            &f.jj,
            &f.store,
            "agent/test",
            "main",
            &f.source,
            &f.source_base,
            "fixture original branch cut",
            &tip,
            false,
        )
        .unwrap_err();
        assert!(error.contains("unedited rewrite"), "{error}");
        assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_refuses_conflicting_later_edits_and_preserves_the_flagged_tip() {
        let Some(mut f) = Fixture::new() else { return };
        f.add_later("incoming.txt", "later competing addition\n");
        let before = crate::read_local_bookmark_view(&f.store).unwrap();
        let error = f.rebuild(false).unwrap_err();
        assert!(error.contains("native tree conflicts"), "{error}");
        assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_scoped_export_does_not_export_flagged_sibling_bookmarks() {
        let Some(f) = Fixture::new() else { return };
        let repo = f.project.path();
        // Make the sibling exist only in native state.
        f.jj.run(
            &f.store,
            &[
                "bookmark",
                "create",
                "flagged-sibling",
                "-r",
                &f.flagged,
                "--ignore-working-copy",
            ],
            "create sibling",
        )
        .unwrap();
        git(repo, &["update-ref", "-d", "refs/heads/flagged-sibling"]);
        f.rebuild(false).unwrap();
        assert!(git_stdout(
            repo,
            &[
                "for-each-ref",
                "--format=%(refname)",
                "refs/heads/flagged-sibling"
            ]
        )
        .is_empty());
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_export_failure_reports_committed_native_state_and_keeps_recovery() {
        let Some(f) = Fixture::new() else { return };
        // Git moved independently. Scoped export must not overwrite it.
        let other = bookmark_commit_checked(&f.jj, &f.store, "main")
            .unwrap()
            .unwrap();
        git(
            f.project.path(),
            &["update-ref", "refs/heads/agent/test", &other],
        );
        let RebuildOutcome::Accepted {
            previous_tip,
            rebuilt_tip,
            base_commit,
            export_error,
            ..
        } = f.rebuild(false).unwrap()
        else {
            panic!("export failure must retain accepted native outcome")
        };
        assert_eq!(previous_tip, f.flagged);
        assert_eq!(base_commit, other);
        assert!(export_error.is_some());
        let rebuilt = bookmark_commit_checked(&f.jj, &f.store, "agent/test")
            .unwrap()
            .unwrap();
        assert_eq!(rebuilt_tip, rebuilt);
        assert_ne!(rebuilt, f.flagged);
        assert!(!branch_has_conflict(&f.jj, &f.store, "agent/test").unwrap());
        assert_eq!(
            git_stdout(f.project.path(), &["rev-parse", "refs/heads/agent/test"]),
            other
        );
        assert_eq!(
            git_stdout(
                f.project.path(),
                &[
                    "rev-parse",
                    &format!("refs/cairn/rebuild/agent/test/{}", f.flagged)
                ]
            ),
            f.flagged
        );
        // Clear only the independently moved Git ref back to jj's prior export.
        // The same request must then export the existing native head, not rebuild.
        let prior_git = git_stdout(f.project.path(), &["rev-parse", &f.source]);
        git(
            f.project.path(),
            &["update-ref", "refs/heads/agent/test", &prior_git],
        );
        let RebuildOutcome::Accepted {
            previous_tip,
            rebuilt_tip,
            base_commit,
            export_error,
            summary,
        } = f.rebuild(false).unwrap()
        else {
            panic!("expected accepted export retry")
        };
        assert_eq!(previous_tip, f.flagged);
        assert_eq!(rebuilt_tip, rebuilt);
        assert_eq!(base_commit, other);
        assert_eq!(export_error, None);
        assert!(summary.contains("Retried export"), "{summary}");
        assert_eq!(
            bookmark_commit_checked(&f.jj, &f.store, "agent/test")
                .unwrap()
                .unwrap(),
            rebuilt
        );
        assert_eq!(
            git_stdout(f.project.path(), &["rev-parse", "refs/heads/agent/test"]),
            rebuilt
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_native_commit_failure_never_exports_ahead_of_authority() {
        let Some(f) = Fixture::new() else { return };
        let before = crate::read_local_bookmark_view(&f.store).unwrap();
        let git_before = git_stdout(f.project.path(), &["rev-parse", "refs/heads/agent/test"]);
        FAIL_BEFORE_NATIVE_COMMIT.with(|fail| fail.set(true));
        assert!(f
            .rebuild(false)
            .unwrap_err()
            .contains("injected native rebuild commit failure"));
        assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
        assert_eq!(
            git_stdout(f.project.path(), &["rev-parse", "refs/heads/agent/test"]),
            git_before
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_original_cut_does_not_resurrect_rewritten_parent_content() {
        let Some(f) = Fixture::with_history(true, false) else {
            return;
        };
        let RebuildOutcome::Preview { summary } = f.rebuild(true).unwrap() else {
            panic!("expected preview")
        };
        assert!(!summary.contains("paths: inherited.txt"), "{summary}");
        f.rebuild(false).unwrap();
        let rebuilt = bookmark_commit_checked(&f.jj, &f.store, "agent/test")
            .unwrap()
            .unwrap();
        let paths = git_stdout(
            f.project.path(),
            &["ls-tree", "-r", "--name-only", &rebuilt],
        );
        assert!(
            !paths.lines().any(|path| path == "inherited.txt"),
            "{paths}"
        );
        assert_eq!(
            git_stdout(f.project.path(), &["show", &format!("{rebuilt}:owned.txt")]),
            "owned"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_checks_the_entire_owned_prefix_and_refuses_ancestor_amendments() {
        let Some(mut f) = Fixture::with_history(false, true) else {
            return;
        };
        let prefix =
            f.jj.run(
                &f.store,
                &[
                    "log",
                    "-r",
                    &format!("{}-", f.flagged),
                    "--no-graph",
                    "-T",
                    "commit_id",
                    "--ignore-working-copy",
                ],
                "find rewritten prefix",
            )
            .unwrap();
        f.jj.run(
            &f.store,
            &[
                "restore",
                "--from",
                "edited-prefix",
                "--into",
                prefix.trim(),
                "legitimate.txt",
                "--ignore-working-copy",
            ],
            "amend prefix",
        )
        .unwrap();
        f.flagged = bookmark_commit_checked(&f.jj, &f.store, "agent/test")
            .unwrap()
            .unwrap();
        let before = crate::read_local_bookmark_view(&f.store).unwrap();
        let error = f.rebuild(false).unwrap_err();
        assert!(error.contains("unedited rewrite"), "{error}");
        assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_reattaches_a_non_default_checkout_after_scoped_export() {
        let Some(f) = Fixture::new() else { return };
        git(f.project.path(), &["checkout", "-q", "agent/test"]);
        f.rebuild(false).unwrap();
        assert_eq!(
            git_stdout(f.project.path(), &["symbolic-ref", "--short", "HEAD"]),
            "agent/test"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn rebuild_export_retry_refuses_a_later_commit_copying_the_description() {
        let Some(mut f) = Fixture::new() else { return };
        let original_flagged = f.flagged.clone();
        f.rebuild(false).unwrap();
        let rebuilt = bookmark_commit_checked(&f.jj, &f.store, "agent/test")
            .unwrap()
            .unwrap();
        let description =
            f.jj.run(
                &f.store,
                &[
                    "log",
                    "-r",
                    &rebuilt,
                    "--no-graph",
                    "-T",
                    "description",
                    "--ignore-working-copy",
                ],
                "read rebuild provenance",
            )
            .unwrap();
        f.flagged = rebuilt;
        f.add_later("newer.txt", "newer work\n");
        f.jj.run(
            &f.store,
            &[
                "describe",
                "-r",
                &f.flagged,
                "-m",
                &description,
                "--ignore-working-copy",
            ],
            "copy rebuild description",
        )
        .unwrap();
        let before = crate::read_local_bookmark_view(&f.store).unwrap();
        let error = rebuild_flagged_branch(
            &f.jj,
            &f.store,
            "agent/test",
            "main",
            &f.source,
            &f.source_base,
            "fixture original branch cut",
            &original_flagged,
            false,
        )
        .unwrap_err();
        assert!(
            error.contains("advanced beyond the original rebuilt result"),
            "{error}"
        );
        assert_eq!(crate::read_local_bookmark_view(&f.store).unwrap(), before);
    }

    #[test]
    fn rebuild_coordinates_are_immutable_full_ids() {
        for bad in [
            "main",
            "abcdef",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "",
        ] {
            assert!(full_id(bad).is_err());
        }
        assert!(full_id("0123456789abcdef0123456789abcdef01234567").is_ok());
    }
}
