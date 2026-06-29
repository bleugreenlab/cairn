use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::BUNDLE_RESOURCE_DIRS;
use crate::services::{FileSystem, GitClient};

use super::repo::ensure_workspace_repo;

const DEFAULT_BRANCH: &str = "main";
const BUNDLE_SYNC_MARKER: &str = ".bundle-sync";

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
    _app_version: &str,
) -> Result<BundleSyncResult, String> {
    fs.create_dir_all(config_dir)?;

    let marker_path = config_dir.join(BUNDLE_SYNC_MARKER);
    let bundle_hash = bundle_content_hash(resource_dir)?;
    let repo_exists = git.is_repo(config_dir)?;

    if !repo_exists {
        copy_missing_bundle_resources(fs, resource_dir, config_dir)?;
        ensure_workspace_repo(git, fs, config_dir, DEFAULT_BRANCH)?;
        write_marker(fs, &marker_path, &bundle_hash)?;
        return Ok(BundleSyncResult {
            updated: true,
            skipped_conflicts: Vec::new(),
        });
    }

    ensure_workspace_repo(git, fs, config_dir, DEFAULT_BRANCH)?;

    if git.root_commit(config_dir, DEFAULT_BRANCH).is_err() {
        copy_missing_bundle_resources(fs, resource_dir, config_dir)?;
        git.add_all(config_dir)?;
        git.commit(config_dir, "Initialize Cairn workspace config")?;
        write_marker(fs, &marker_path, &bundle_hash)?;
        return Ok(BundleSyncResult {
            updated: true,
            skipped_conflicts: Vec::new(),
        });
    }

    let marker_was_current = marker_matches(fs, &marker_path, &bundle_hash);
    let copied = copy_missing_bundle_resources(fs, resource_dir, config_dir)?;
    if marker_was_current && !copied {
        return Ok(BundleSyncResult::default());
    }

    snapshot_pending_user_edits(git, config_dir)?;
    if copied {
        git.add_all(config_dir)?;
        git.commit(config_dir, "Add missing bundled workspace defaults")?;
    }

    write_marker(fs, &marker_path, &bundle_hash)?;
    Ok(BundleSyncResult {
        updated: copied,
        skipped_conflicts: Vec::new(),
    })
}

fn copy_missing_bundle_resources(
    fs: &dyn FileSystem,
    resource_dir: &Path,
    target_dir: &Path,
) -> Result<bool, String> {
    let mut copied_any = false;

    for dir_name in BUNDLE_RESOURCE_DIRS {
        let source_dir = resource_dir.join(dir_name);
        let dest_dir = target_dir.join(dir_name);
        fs.create_dir_all(&dest_dir)?;

        if !source_dir.exists() {
            log::debug!(
                "Bundled {} directory not found at {:?}; leaving workspace tree unchanged",
                dir_name,
                source_dir
            );
            continue;
        }

        let mut entries = std::fs::read_dir(&source_dir)
            .map_err(|e| {
                format!(
                    "Failed to read bundled {dir_name} directory {:?}: {e}",
                    source_dir
                )
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Failed to read bundled {dir_name} entry: {e}"))?;
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let source = entry.path();
            let file_name = entry.file_name();
            let dest = dest_dir.join(file_name);

            if dir_name == "skills" {
                if source.is_dir() && source.join("SKILL.md").exists() && !fs.exists(&dest) {
                    fs.copy_dir_recursive(&source, &dest)?;
                    copied_any = true;
                }
            } else if source.is_file()
                && bundled_file_matches_dir(dir_name, &source)
                && !fs.exists(&dest)
            {
                fs.copy_file(&source, &dest)?;
                copied_any = true;
            }
        }
    }

    Ok(copied_any)
}

fn bundled_file_matches_dir(dir_name: &str, path: &Path) -> bool {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    match dir_name {
        "agents" => ext == "md",
        "recipes" => ext == "yaml" || ext == "yml",
        _ => false,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::{MockFileSystem, MockGitClient};
    use mockall::predicate::eq;
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn fresh_install_copies_missing_defaults_and_initializes_repo() {
        let repo = Path::new("/home/user/.cairn");
        let resources = Path::new("/app/resources");
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        git.expect_is_repo().with(eq(repo)).returning(|_| Ok(false));
        fs.expect_exists().returning(|path| {
            path.ends_with(".gitignore") || path.starts_with("/home/user/.cairn")
        });
        fs.expect_remove_dir_all().times(0);
        fs.expect_copy_file().times(0);
        fs.expect_copy_dir_recursive().times(0);
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

        let result = sync_workspace_bundle(&git, &fs, resources, repo, "1.0.0").unwrap();
        assert!(result.updated);
        assert!(result.skipped_conflicts.is_empty());
    }

    #[test]
    fn matching_marker_and_no_missing_resources_short_circuits() {
        let repo = Path::new("/home/user/.cairn");
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        git.expect_is_repo().with(eq(repo)).returning(|_| Ok(true));
        let bundle_hash = bundle_content_hash(Path::new("/resources")).unwrap();
        fs.expect_exists().returning(|path| {
            path == Path::new("/home/user/.cairn/.bundle-sync")
                || path == Path::new("/home/user/.cairn/.gitignore")
        });
        fs.expect_read_to_string().returning(move |path| {
            if path.ends_with(".bundle-sync") {
                Ok(format!("{bundle_hash}\n"))
            } else {
                Ok(super::super::repo::WORKSPACE_GITIGNORE.to_string())
            }
        });
        git.expect_root_commit()
            .with(eq(repo), eq(DEFAULT_BRANCH))
            .returning(|_, _| Ok("root".to_string()));
        git.expect_status().times(0);

        let result =
            sync_workspace_bundle(&git, &fs, Path::new("/resources"), repo, "1.0.0").unwrap();
        assert_eq!(result, BundleSyncResult::default());
    }

    #[test]
    fn existing_install_copies_missing_bundled_files_without_merge_branch() {
        let repo = Path::new("/home/user/.cairn");
        let temp = TempDir::new().unwrap();
        let resources = temp.path().join("resources");
        write_resources_a(&resources);
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        git.expect_is_repo().with(eq(repo)).returning(|_| Ok(true));
        git.expect_init_repo().times(0);
        fs.expect_exists().returning(|path| {
            path == Path::new("/home/user/.cairn/.gitignore")
                || path == Path::new("/home/user/.cairn/recipes/default.yaml")
        });
        fs.expect_read_to_string()
            .returning(|_| Ok(super::super::repo::WORKSPACE_GITIGNORE.to_string()));
        fs.expect_write_str().returning(|_, _| Ok(()));
        git.expect_root_commit()
            .with(eq(repo), eq(DEFAULT_BRANCH))
            .returning(|_, _| Ok("root".to_string()));
        fs.expect_copy_file().times(1).returning(|_, _| Ok(()));
        fs.expect_copy_dir_recursive().times(0);
        git.expect_status()
            .with(eq(repo))
            .returning(|_| Ok(String::new()));
        git.expect_add_all().with(eq(repo)).returning(|_| Ok(()));
        git.expect_commit()
            .withf(|path, msg| {
                path == Path::new("/home/user/.cairn")
                    && msg == "Add missing bundled workspace defaults"
            })
            .returning(|_, _| Ok(()));

        let result = sync_workspace_bundle(&git, &fs, &resources, repo, "2.0.0").unwrap();
        assert!(result.updated);
        assert!(result.skipped_conflicts.is_empty());
    }

    #[test]
    fn real_temp_repo_adds_missing_bundles_without_touching_existing_files() {
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
        assert!(result.skipped_conflicts.is_empty());
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
