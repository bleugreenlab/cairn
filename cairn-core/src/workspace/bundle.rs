use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{mirror_bundle_resources_with_fs, BUNDLE_RESOURCE_DIRS};
use crate::services::{FileSystem, GitClient, GitOutput};

use super::repo::ensure_workspace_repo;

const BUNDLE_BRANCH: &str = "bundle";
const DEFAULT_BRANCH: &str = "main";
const BUNDLE_SYNC_MARKER: &str = ".bundle-sync";
const BUNDLE_WORKTREE_DIR: &str = ".bundle-sync-worktree";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BundleSyncResult {
    pub updated: bool,
    pub skipped_conflicts: Vec<String>,
}

pub fn sync_workspace_bundle(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
    app_version: &str,
) -> Result<BundleSyncResult, String> {
    fs.create_dir_all(config_dir)?;

    let marker_path = config_dir.join(BUNDLE_SYNC_MARKER);
    let bundle_hash = bundle_content_hash(resource_dir)?;
    let repo_exists = git.is_repo(config_dir)?;
    let bundle_exists = repo_exists && git.branch_exists(config_dir, BUNDLE_BRANCH)?;

    if bundle_exists && marker_matches(fs, &marker_path, &bundle_hash) {
        return Ok(BundleSyncResult::default());
    }

    if !repo_exists {
        mirror_bundle_resources_with_fs(fs, resource_dir, config_dir)?;
        ensure_workspace_repo(git, fs, config_dir, DEFAULT_BRANCH)?;
        git.create_branch_at(config_dir, BUNDLE_BRANCH, "HEAD")?;
        write_marker(fs, &marker_path, &bundle_hash)?;
        return Ok(BundleSyncResult {
            updated: true,
            skipped_conflicts: Vec::new(),
        });
    }

    ensure_workspace_repo(git, fs, config_dir, DEFAULT_BRANCH)?;

    if git.root_commit(config_dir, DEFAULT_BRANCH).is_err() {
        mirror_bundle_resources_with_fs(fs, resource_dir, config_dir)?;
        git.add_all(config_dir)?;
        git.commit(config_dir, "Initialize Cairn workspace config")?;
        git.create_branch_at(config_dir, BUNDLE_BRANCH, "HEAD")?;
        write_marker(fs, &marker_path, &bundle_hash)?;
        return Ok(BundleSyncResult {
            updated: true,
            skipped_conflicts: Vec::new(),
        });
    }

    if !git.branch_exists(config_dir, BUNDLE_BRANCH)? {
        let root = git.root_commit(config_dir, DEFAULT_BRANCH)?;
        git.create_branch_at(config_dir, BUNDLE_BRANCH, &root)?;
    }

    snapshot_pending_user_edits(git, config_dir)?;
    advance_bundle_branch(git, fs, resource_dir, config_dir, app_version)?;

    let mut result = BundleSyncResult::default();
    if !git.is_ancestor(config_dir, BUNDLE_BRANCH, DEFAULT_BRANCH)? {
        result = merge_bundle(git, config_dir)?;
    }

    write_marker(fs, &marker_path, &bundle_hash)?;
    Ok(result)
}

fn marker_matches(fs: &dyn FileSystem, marker_path: &Path, bundle_hash: &str) -> bool {
    fs.exists(marker_path)
        && fs
            .read_to_string(marker_path)
            .map(|value| value.trim() == bundle_hash)
            .unwrap_or(false)
}

fn write_marker(fs: &dyn FileSystem, marker_path: &Path, bundle_hash: &str) -> Result<(), String> {
    fs.write_str(marker_path, &format!("{bundle_hash}\n"))
}

fn bundle_content_hash(resource_dir: &Path) -> Result<String, String> {
    let mut entries = Vec::new();
    for dir_name in BUNDLE_RESOURCE_DIRS {
        let dir = resource_dir.join(dir_name);
        if dir.exists() {
            collect_bundle_files(resource_dir, &dir, &mut entries)?;
        }
    }

    entries.sort_by(|(left_path, _), (right_path, _)| left_path.cmp(right_path));

    let mut hasher = Sha256::new();
    for (relative_path, bytes) in entries {
        let path_bytes = relative_path.as_bytes();
        hasher.update((path_bytes.len() as u64).to_le_bytes());
        hasher.update(path_bytes);
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

fn collect_bundle_files(
    root: &Path,
    dir: &Path,
    entries: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), String> {
    let mut children = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read bundled resource directory {:?}: {}", dir, e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read bundled resource entry: {}", e))?;
    children.sort_by_key(|entry| entry.path());

    for child in children {
        let path = child.path();
        let file_type = child
            .file_type()
            .map_err(|e| format!("Failed to inspect bundled resource {:?}: {}", path, e))?;
        if file_type.is_dir() {
            collect_bundle_files(root, &path, entries)?;
        } else if file_type.is_file() {
            let relative_path = path
                .strip_prefix(root)
                .map_err(|e| format!("Failed to relativize bundled resource {:?}: {}", path, e))?
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let bytes = std::fs::read(&path)
                .map_err(|e| format!("Failed to read bundled resource {:?}: {}", path, e))?;
            entries.push((relative_path, bytes));
        }
    }

    Ok(())
}

fn snapshot_pending_user_edits(git: &dyn GitClient, config_dir: &Path) -> Result<(), String> {
    if !git.status(config_dir)?.trim().is_empty() {
        git.add_all(config_dir)?;
        git.commit(config_dir, "Snapshot workspace config")?;
    }
    Ok(())
}

fn advance_bundle_branch(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
    app_version: &str,
) -> Result<(), String> {
    let worktree_path = bundle_worktree_path(config_dir);
    let _ = git.worktree_prune(config_dir);
    if fs.exists(&worktree_path) {
        fs.remove_dir_all(&worktree_path)?;
    }

    git.worktree_add_existing_branch(config_dir, &worktree_path, BUNDLE_BRANCH)?;
    let advance_result = (|| {
        mirror_bundle_resources_with_fs(fs, resource_dir, &worktree_path)?;
        git.add_all(&worktree_path)?;
        git.commit(
            &worktree_path,
            &format!("Update bundled defaults to {app_version}"),
        )
    })();

    let remove_result = git.worktree_remove(config_dir, &worktree_path, true);
    advance_result?;
    remove_result?;
    Ok(())
}

fn bundle_worktree_path(config_dir: &Path) -> PathBuf {
    config_dir.join(BUNDLE_WORKTREE_DIR)
}

fn merge_bundle(git: &dyn GitClient, config_dir: &Path) -> Result<BundleSyncResult, String> {
    let output = git.merge_no_edit(config_dir, BUNDLE_BRANCH)?;
    if output.success {
        return Ok(BundleSyncResult {
            updated: true,
            skipped_conflicts: Vec::new(),
        });
    }

    let skipped_conflicts = git.list_unmerged(config_dir)?;
    if skipped_conflicts.is_empty() {
        return Err(format_git_failure("git merge failed", &output));
    }

    git.checkout_ours(config_dir, skipped_conflicts.clone())?;
    git.add_all(config_dir)?;
    let commit_output = git.run(
        config_dir,
        vec![
            "-c".to_string(),
            "user.name=Cairn".to_string(),
            "-c".to_string(),
            "user.email=cairn@local.invalid".to_string(),
            "commit".to_string(),
            "--no-edit".to_string(),
        ],
    )?;
    if !commit_output.success {
        return Err(format_git_failure(
            "git commit --no-edit failed",
            &commit_output,
        ));
    }

    Ok(BundleSyncResult {
        updated: true,
        skipped_conflicts,
    })
}

fn format_git_failure(prefix: &str, output: &GitOutput) -> String {
    let detail = if output.stderr.trim().is_empty() {
        output.stdout.trim()
    } else {
        output.stderr.trim()
    };
    format!("{prefix}: {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::{MockFileSystem, MockGitClient};
    use mockall::predicate::eq;
    use std::path::Path;
    use tempfile::TempDir;

    fn ok_merge() -> GitOutput {
        GitOutput {
            success: true,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    #[test]
    fn fresh_install_mirrors_initial_commit_and_creates_bundle() {
        let repo = Path::new("/home/user/.cairn");
        let resources = Path::new("/app/resources");
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        git.expect_is_repo().with(eq(repo)).returning(|_| Ok(false));
        fs.expect_exists()
            .returning(|path| path.starts_with("/app/resources"));
        fs.expect_remove_dir_all().times(0);
        fs.expect_copy_dir_recursive()
            .times(3)
            .returning(|_, _| Ok(()));
        git.expect_init_repo().times(1).returning(|_, _| Ok(()));
        fs.expect_read_to_string().returning(|_| Ok(String::new()));
        fs.expect_write_str().returning(|_, _| Ok(()));
        git.expect_add_all()
            .with(eq(repo))
            .times(1)
            .returning(|_| Ok(()));
        git.expect_commit()
            .withf(|_, msg| msg == "Initialize Cairn workspace config")
            .times(1)
            .returning(|_, _| Ok(()));
        git.expect_create_branch_at()
            .withf(|_, name, start| name == BUNDLE_BRANCH && start == "HEAD")
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = sync_workspace_bundle(&git, &fs, resources, repo, "1.0.0").unwrap();
        assert!(result.updated);
        assert!(result.skipped_conflicts.is_empty());
    }

    #[test]
    fn matching_marker_and_bundle_branch_short_circuits() {
        let repo = Path::new("/home/user/.cairn");
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all()
            .with(eq(repo))
            .returning(|_| Ok(()));
        git.expect_is_repo().with(eq(repo)).returning(|_| Ok(true));
        git.expect_branch_exists()
            .with(eq(repo), eq(BUNDLE_BRANCH))
            .returning(|_, _| Ok(true));
        let bundle_hash = bundle_content_hash(Path::new("/resources")).unwrap();
        fs.expect_exists()
            .with(eq(repo.join(BUNDLE_SYNC_MARKER)))
            .returning(|_| true);
        fs.expect_read_to_string()
            .with(eq(repo.join(BUNDLE_SYNC_MARKER)))
            .returning(move |_| Ok(format!("{bundle_hash}\n")));
        git.expect_status().times(0);

        let result =
            sync_workspace_bundle(&git, &fs, Path::new("/resources"), repo, "1.0.0").unwrap();
        assert_eq!(result, BundleSyncResult::default());
    }

    #[test]
    fn existing_install_without_bundle_creates_at_root_advances_and_merges() {
        let repo = Path::new("/home/user/.cairn");
        let resources = Path::new("/app/resources");
        let worktree = repo.join(BUNDLE_WORKTREE_DIR);
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        git.expect_is_repo().with(eq(repo)).returning(|_| Ok(true));
        git.expect_branch_exists()
            .with(eq(repo), eq(BUNDLE_BRANCH))
            .returning(|_, _| Ok(false));
        git.expect_init_repo().times(0);
        fs.expect_exists()
            .returning(|path| path.ends_with(".gitignore") || path.starts_with("/app/resources"));
        fs.expect_read_to_string()
            .returning(|_| Ok(super::super::repo::WORKSPACE_GITIGNORE.to_string()));
        fs.expect_write_str().returning(|_, _| Ok(()));
        git.expect_root_commit()
            .with(eq(repo), eq(DEFAULT_BRANCH))
            .returning(|_, _| Ok("root".to_string()));
        git.expect_branch_exists()
            .with(eq(repo), eq(BUNDLE_BRANCH))
            .returning(|_, _| Ok(false));
        git.expect_create_branch_at()
            .with(eq(repo), eq(BUNDLE_BRANCH), eq("root"))
            .times(1)
            .returning(|_, _, _| Ok(()));
        git.expect_status()
            .with(eq(repo))
            .returning(|_| Ok(String::new()));
        git.expect_worktree_prune()
            .with(eq(repo))
            .returning(|_| Ok(()));
        fs.expect_remove_dir_all().returning(|_| Ok(()));
        git.expect_worktree_add_existing_branch()
            .with(eq(repo), eq(worktree.clone()), eq(BUNDLE_BRANCH))
            .returning(|_, _, _| Ok(()));
        fs.expect_copy_dir_recursive()
            .times(3)
            .returning(|_, _| Ok(()));
        git.expect_add_all()
            .with(eq(worktree.clone()))
            .returning(|_| Ok(()));
        git.expect_commit()
            .withf(|path, msg| {
                path.ends_with(BUNDLE_WORKTREE_DIR) && msg == "Update bundled defaults to 2.0.0"
            })
            .returning(|_, _| Ok(()));
        git.expect_worktree_remove()
            .with(eq(repo), eq(worktree.clone()), eq(true))
            .returning(|_, _, _| Ok(()));
        git.expect_is_ancestor()
            .with(eq(repo), eq(BUNDLE_BRANCH), eq(DEFAULT_BRANCH))
            .returning(|_, _, _| Ok(false));
        git.expect_merge_no_edit()
            .with(eq(repo), eq(BUNDLE_BRANCH))
            .returning(|_, _| Ok(ok_merge()));

        let result = sync_workspace_bundle(&git, &fs, resources, repo, "2.0.0").unwrap();
        assert!(result.updated);
        assert!(result.skipped_conflicts.is_empty());
    }

    #[test]
    fn conflict_keeps_ours_and_reports_skipped_paths() {
        let repo = Path::new("/home/user/.cairn");
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        git.expect_is_repo().returning(|_| Ok(true));
        git.expect_branch_exists().returning(|_, _| Ok(true));
        fs.expect_exists()
            .returning(|path| path.starts_with("/app/resources"));
        fs.expect_read_to_string().returning(|_| Ok(String::new()));
        fs.expect_write_str().returning(|_, _| Ok(()));
        git.expect_init_repo().times(0);
        git.expect_root_commit()
            .returning(|_, _| Ok("root".to_string()));
        git.expect_status().returning(|_| Ok(String::new()));
        git.expect_worktree_prune().returning(|_| Ok(()));
        fs.expect_remove_dir_all().returning(|_| Ok(()));
        fs.expect_copy_dir_recursive()
            .times(3)
            .returning(|_, _| Ok(()));
        git.expect_worktree_add_existing_branch()
            .returning(|_, _, _| Ok(()));
        git.expect_add_all().returning(|_| Ok(()));
        git.expect_commit().returning(|_, _| Ok(()));
        git.expect_worktree_remove().returning(|_, _, _| Ok(()));
        git.expect_is_ancestor().returning(|_, _, _| Ok(false));
        git.expect_merge_no_edit().returning(|_, _| {
            Ok(GitOutput {
                success: false,
                stdout: String::new(),
                stderr: "conflict".to_string(),
            })
        });
        git.expect_list_unmerged()
            .returning(|_| Ok(vec!["agents/explore.md".to_string()]));
        git.expect_checkout_ours()
            .with(eq(repo), eq(vec!["agents/explore.md".to_string()]))
            .returning(|_, _| Ok(()));
        git.expect_run().returning(|_, _| Ok(ok_merge()));

        let result =
            sync_workspace_bundle(&git, &fs, Path::new("/app/resources"), repo, "2.0.0").unwrap();
        assert_eq!(result.skipped_conflicts, vec!["agents/explore.md"]);
    }

    #[test]
    fn real_temp_repo_merges_non_conflicts_and_keeps_user_conflict() {
        use crate::services::{RealFileSystem, RealGitClient};

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources_a = temp.path().join("resources-a");
        let resources_b = temp.path().join("resources-b");
        write_resources_a(&resources_a);
        write_resources_b(&resources_b);

        let git = RealGitClient;
        let fs = RealFileSystem;
        sync_workspace_bundle(&git, &fs, &resources_a, &repo, "1.0.0").unwrap();

        std::fs::write(repo.join("agents/explore.md"), "user edit\n").unwrap();
        git.add_all(&repo).unwrap();
        git.commit(&repo, "User edits explore").unwrap();

        let result = sync_workspace_bundle(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert!(repo.join("agents/new-agent.md").exists());
        assert!(repo.join("recipes/memory-triage.yaml").exists());
        assert!(repo.join("skills/example/SKILL.md").exists());
        assert_eq!(result.skipped_conflicts, vec!["agents/explore.md"]);
    }

    #[test]
    fn same_version_changed_bundle_content_resyncs_and_preserves_user_files() {
        use crate::services::{RealFileSystem, RealGitClient};

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources_a = temp.path().join("resources-a");
        let resources_b = temp.path().join("resources-b");
        write_resources_a(&resources_a);
        write_resources_b(&resources_b);

        let git = RealGitClient;
        let fs = RealFileSystem;
        sync_workspace_bundle(&git, &fs, &resources_a, &repo, "1.0.0").unwrap();
        let original_marker = std::fs::read_to_string(repo.join(BUNDLE_SYNC_MARKER)).unwrap();
        assert_eq!(
            original_marker.trim(),
            bundle_content_hash(&resources_a).unwrap()
        );

        std::fs::write(repo.join("agents/explore.md"), "user edit\n").unwrap();
        git.add_all(&repo).unwrap();
        git.commit(&repo, "User edits explore").unwrap();

        let result = sync_workspace_bundle(&git, &fs, &resources_b, &repo, "1.0.0").unwrap();

        assert!(result.updated);
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert!(repo.join("recipes/memory-triage.yaml").exists());
        assert!(repo.join("agents/new-agent.md").exists());
        assert_eq!(
            std::fs::read_to_string(repo.join(BUNDLE_SYNC_MARKER))
                .unwrap()
                .trim(),
            bundle_content_hash(&resources_b).unwrap()
        );
    }

    fn write_resources_a(root: &Path) {
        std::fs::create_dir_all(root.join("agents")).unwrap();
        std::fs::create_dir_all(root.join("recipes")).unwrap();
        std::fs::create_dir_all(root.join("skills")).unwrap();
        std::fs::write(root.join("agents/explore.md"), "bundle a\n").unwrap();
        std::fs::write(root.join("recipes/default.yaml"), "name: default\n").unwrap();
    }

    fn write_resources_b(root: &Path) {
        std::fs::create_dir_all(root.join("agents")).unwrap();
        std::fs::create_dir_all(root.join("recipes")).unwrap();
        std::fs::create_dir_all(root.join("skills/example")).unwrap();
        std::fs::write(root.join("agents/explore.md"), "bundle b\n").unwrap();
        std::fs::write(root.join("agents/new-agent.md"), "new\n").unwrap();
        std::fs::write(root.join("recipes/default.yaml"), "name: default\n").unwrap();
        std::fs::write(
            root.join("recipes/memory-triage.yaml"),
            "name: memory-triage\n",
        )
        .unwrap();
        std::fs::write(
            root.join("skills/example/SKILL.md"),
            "---\nname: Example\ndescription: Example skill\n---\nBody\n",
        )
        .unwrap();
    }
}
