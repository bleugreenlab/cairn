use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::BUNDLE_RESOURCE_DIRS;
use crate::services::{FileSystem, GitClient, GitOutput};

use super::repo::ensure_workspace_repo;

const DEFAULT_BRANCH: &str = "main";
const BUNDLE_SYNC_MARKER: &str = ".bundle-sync";

/// Commit subjects the bundle sync itself authors. A bundled file whose most
/// recent commit carries one of these has not been edited by the user since it
/// was last shipped, so it is safe to overwrite in place when the bundle updates
/// it. Any other last commit (a snapshotted user edit or an external commit)
/// marks the file user-owned, and it is preserved. The list includes the two
/// historical bundle-commit subjects so files shipped by earlier versions are
/// still recognized as bundle-owned after an upgrade.
const BUNDLE_COMMIT_SUBJECTS: &[&str] = &[
    "Initialize Cairn workspace config",
    "Add missing bundled workspace defaults",
    "Sync bundled workspace defaults",
];

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BundleSyncResult {
    updated: bool,
    pub skipped_conflicts: Vec<String>,
}

struct SyncOutcome {
    changed: bool,
    copied_paths: Vec<String>,
    skipped_conflicts: Vec<String>,
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

    // Materializing missing defaults is deliberately independent of Git. A
    // missing or undiscoverable Git binary may prevent history management, but
    // it must not leave a fresh workspace without usable bundled resources.
    let provisioned = sync_bundle_resources(git, fs, resource_dir, config_dir, false)?;
    let repo_exists = git.is_repo(config_dir)?;

    if !repo_exists {
        git.init_repo(config_dir, DEFAULT_BRANCH)?;
        ensure_workspace_repo(git, fs, config_dir, DEFAULT_BRANCH)?;
        initialize_bundle_history(git, fs, resource_dir, config_dir)?;
        write_marker(fs, &marker_path, &bundle_hash)?;
        return Ok(BundleSyncResult {
            updated: true,
            skipped_conflicts: provisioned.skipped_conflicts,
        });
    }

    ensure_workspace_repo(git, fs, config_dir, DEFAULT_BRANCH)?;

    if git.root_commit(config_dir, DEFAULT_BRANCH).is_err() {
        // Recovery may have been interrupted after repository initialization.
        // Rebuild the unborn repository's history with the same ownership
        // classification used on the first recovery attempt.
        initialize_bundle_history(git, fs, resource_dir, config_dir)?;
        write_marker(fs, &marker_path, &bundle_hash)?;
        return Ok(BundleSyncResult {
            updated: true,
            skipped_conflicts: provisioned.skipped_conflicts,
        });
    }

    // Keep newly materialized defaults bundle-owned without folding unrelated
    // pending user edits into the bundle commit.
    commit_copied_defaults(git, config_dir, &provisioned.copied_paths)?;

    let marker_was_current = marker_matches(fs, &marker_path, &bundle_hash);

    if !marker_was_current {
        // The bundle content changed since the last sync, so an in-place update
        // may overwrite bundled files. Snapshot any uncommitted user edits first
        // so an overwrite can never lose unsaved work, and so an edited file is
        // committed under a non-bundle subject that marks it user-owned.
        snapshot_pending_user_edits(git, config_dir)?;
    }

    let updated = sync_bundle_resources(git, fs, resource_dir, config_dir, !marker_was_current)?;

    if !updated.changed {
        if !marker_was_current {
            write_marker(fs, &marker_path, &bundle_hash)?;
        }
        return Ok(BundleSyncResult {
            updated: provisioned.changed,
            skipped_conflicts: updated.skipped_conflicts,
        });
    }

    if marker_was_current {
        snapshot_pending_user_edits(git, config_dir)?;
    }
    git.add_all(config_dir)?;
    git.commit(config_dir, "Sync bundled workspace defaults")?;

    write_marker(fs, &marker_path, &bundle_hash)?;
    Ok(BundleSyncResult {
        updated: true,
        skipped_conflicts: updated.skipped_conflicts,
    })
}

/// Copy bundled resources into the workspace config tree. Always copies files
/// whose destination is missing. When `allow_update` is set (an established repo
/// whose bundle content changed), a bundled file whose content differs from the
/// shipped source is overwritten **only if it is still bundle-owned** (see
/// `bundled_file_is_user_owned`); a user-customized file is left untouched and
/// reported as a skipped conflict. Skill packages are multi-file directories and
/// are only ever copy-when-missing.
fn sync_bundle_resources(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    resource_dir: &Path,
    target_dir: &Path,
    allow_update: bool,
) -> Result<SyncOutcome, String> {
    let mut changed = false;
    let mut copied_paths = Vec::new();
    let mut skipped_conflicts = Vec::new();

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
            let dest = dest_dir.join(&file_name);

            // Skill and workflow packages are versioned directory trees keyed by
            // a manifest file. In-place update of a multi-file package is out of
            // scope, so they are copy-when-missing only — which also means a
            // user's edited copy is never overwritten on a later sync.
            if let Some(manifest) = package_manifest_file(dir_name) {
                if source.is_dir() && source.join(manifest).exists() && !fs.exists(&dest) {
                    fs.copy_dir_recursive(&source, &dest)?;
                    changed = true;
                    copied_paths.push(format!("{dir_name}/{}", file_name.to_string_lossy()));
                }
                continue;
            }

            if !(source.is_file() && bundled_file_matches_dir(dir_name, &source)) {
                continue;
            }

            if !fs.exists(&dest) {
                fs.copy_file(&source, &dest)?;
                changed = true;
                copied_paths.push(format!("{dir_name}/{}", file_name.to_string_lossy()));
                continue;
            }

            if !allow_update {
                continue;
            }

            // Dest exists and the bundle changed. Overwrite only an unmodified
            // (bundle-owned) file whose content actually differs from the source.
            let source_content = fs.read_to_string(&source)?;
            let dest_content = fs.read_to_string(&dest)?;
            if source_content == dest_content {
                continue;
            }

            let rel_path = format!("{dir_name}/{}", file_name.to_string_lossy());
            if bundled_file_is_user_owned(git, target_dir, &rel_path)? {
                skipped_conflicts.push(rel_path);
            } else {
                fs.copy_file(&source, &dest)?;
                changed = true;
            }
        }
    }

    Ok(SyncOutcome {
        changed,
        copied_paths,
        skipped_conflicts,
    })
}

fn commit_copied_defaults(
    git: &dyn GitClient,
    config_dir: &Path,
    copied_paths: &[String],
) -> Result<(), String> {
    if copied_paths.is_empty() {
        return Ok(());
    }
    let mut paths = vec![".gitignore".to_string()];
    paths.extend(copied_paths.iter().cloned());
    let mut add_args = vec!["add".to_string(), "--".to_string()];
    add_args.extend(paths.iter().cloned());
    let output = git.run(config_dir, add_args)?;
    if !output.success {
        return Err(format!(
            "Failed to stage bundled workspace defaults: {}",
            output.stderr
        ));
    }
    commit_only_paths(
        git,
        config_dir,
        "Add missing bundled workspace defaults",
        &paths,
    )
}

fn commit_only_paths(
    git: &dyn GitClient,
    config_dir: &Path,
    subject: &str,
    paths: &[String],
) -> Result<(), String> {
    let mut args = vec![
        "-c".to_string(),
        "user.name=Cairn".to_string(),
        "-c".to_string(),
        "user.email=cairn@local.invalid".to_string(),
        "commit".to_string(),
        "--only".to_string(),
        "-m".to_string(),
        subject.to_string(),
        "--".to_string(),
    ];
    args.extend(paths.iter().cloned());
    let output = git.run(config_dir, args)?;
    // A path-scoped commit that finds nothing to commit is a legitimate no-op:
    // restoring a byte-identical default (a working-tree deletion reverted with
    // identical content) leaves the named paths already matching HEAD. Git
    // reports this with one of several phrasings depending on the surrounding
    // tree state, all of which must succeed so a no-op restore never fails the
    // whole bundle sync — and, with `--only`, an unrelated staged change is left
    // untouched.
    if output.success || is_nothing_to_commit(&output) {
        Ok(())
    } else {
        Err(format!("git commit failed: {}", output.stderr))
    }
}

/// Whether a failed `git commit` failed only because the requested scope had
/// nothing to commit. Git emits one of several phrasings depending on tree
/// state; all mean the working tree already matches HEAD for that scope.
fn is_nothing_to_commit(output: &GitOutput) -> bool {
    let combined = format!("{}\n{}", output.stdout, output.stderr);
    combined.contains("nothing to commit")
        || combined.contains("nothing added to commit")
        || combined.contains("no changes added to commit")
}

fn initialize_bundle_history(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
) -> Result<(), String> {
    // Defaults may have been edited after an earlier Git-less provisioning
    // pass. Build the baseline without those paths, then snapshot everything
    // else under a user-owned subject so future upgrades cannot overwrite it.
    let (bundle_owned_paths, _) = classify_untracked_bundle_paths(fs, resource_dir, config_dir)?;
    git.add_all(config_dir)?;
    commit_only_paths(
        git,
        config_dir,
        "Initialize Cairn workspace config",
        &bundle_owned_paths,
    )?;
    snapshot_pending_user_edits(git, config_dir)
}

fn classify_untracked_bundle_paths(
    fs: &dyn FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
) -> Result<(Vec<String>, Vec<String>), String> {
    let mut bundle_owned = vec![".gitignore".to_string()];
    let mut user_owned = Vec::new();

    for dir_name in BUNDLE_RESOURCE_DIRS {
        let source_dir = resource_dir.join(dir_name);
        if !source_dir.exists() {
            continue;
        }
        let mut entries = std::fs::read_dir(&source_dir)
            .map_err(|error| format!("Failed to read bundled {dir_name} directory: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("Failed to read bundled {dir_name} entry: {error}"))?;
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let source = entry.path();
            let file_name = entry.file_name();
            let relative = format!("{dir_name}/{}", file_name.to_string_lossy());
            let destination = config_dir.join(&relative);
            if !fs.exists(&destination) {
                continue;
            }
            if let Some(manifest) = package_manifest_file(dir_name) {
                if source.is_dir() && source.join(manifest).exists() {
                    bundle_owned.push(relative);
                }
                continue;
            }
            if source.is_file() && bundled_file_matches_dir(dir_name, &source) {
                if fs.read_to_string(&source)? == fs.read_to_string(&destination)? {
                    bundle_owned.push(relative);
                } else {
                    user_owned.push(relative);
                }
            }
        }
    }

    Ok((bundle_owned, user_owned))
}

/// Whether a tracked bundled file has been edited by the user since it was last
/// shipped. True when the most recent commit touching `rel_path` carries a
/// subject the bundle sync did not author. An untracked file (no commit history)
/// or an unreadable log is treated as user-owned so local work is never lost.
fn bundled_file_is_user_owned(
    git: &dyn GitClient,
    config_dir: &Path,
    rel_path: &str,
) -> Result<bool, String> {
    let output = git.run(
        config_dir,
        vec![
            "log".to_string(),
            "-1".to_string(),
            "--format=%s".to_string(),
            "--".to_string(),
            rel_path.to_string(),
        ],
    )?;
    if !output.success {
        return Ok(true);
    }
    let subject = output.stdout.trim();
    if subject.is_empty() {
        return Ok(true);
    }
    Ok(!BUNDLE_COMMIT_SUBJECTS.contains(&subject))
}

/// The manifest filename that marks a bundled directory-package for `dir_name`,
/// or `None` for a flat (single-file) resource dir. A package dir is copied
/// whole when missing and never updated in place, so an edited copy is
/// preserved.
fn package_manifest_file(dir_name: &str) -> Option<&'static str> {
    match dir_name {
        "skills" => Some("SKILL.md"),
        "workflows" => Some("workflow.yaml"),
        _ => None,
    }
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

/// Subdirectory of `CAIRN_HOME` (and of the app's bundled resources) holding the
/// Cairn-owned workflow runtime: `runtime/node_modules/@cairn/{harness,sdk}`.
const RUNTIME_DIR: &str = "runtime";
/// Marker recording the content hash of the last-provisioned runtime, so a
/// version change re-syncs and staleness is detectable (bundle-sync precedent).
const RUNTIME_MARKER: &str = ".runtime-version";

/// Provision the Cairn-owned workflow runtime (`@cairn/harness` + `@cairn/sdk`)
/// into `<cairn_home>/runtime` from the app's bundled `runtime/` resource
/// (CAIRN-2504). A workflow spawned from ANY project sets `NODE_PATH` to this
/// runtime first (see `backends::workflow`), so the harness resolves regardless
/// of the invoking project's own `node_modules`.
///
/// No-op when the bundle ships no `runtime/` (a dev build resolves the harness
/// from the Cairn repo's own `node_modules` instead). The runtime is
/// Cairn-owned and never user-edited, so a content-hash change replaces the tree
/// wholesale — the same marker precedent the workspace bundle uses, making
/// version skew self-healing on the next startup. Returns whether it (re)synced.
pub fn provision_workflow_runtime(
    fs: &dyn FileSystem,
    resource_dir: &Path,
    cairn_home: &Path,
) -> Result<bool, String> {
    let source = resource_dir.join(RUNTIME_DIR);
    if !fs.exists(&source) {
        return Ok(false);
    }
    let hash = runtime_content_hash(&source)?;
    let dest = cairn_home.join(RUNTIME_DIR);
    let marker_path = dest.join(RUNTIME_MARKER);
    if marker_matches(fs, &marker_path, &hash) {
        return Ok(false);
    }
    if fs.exists(&dest) {
        fs.remove_dir_all(&dest)?;
    }
    fs.copy_dir_recursive(&source, &dest)?;
    write_marker(fs, &marker_path, &hash)?;
    Ok(true)
}

/// Content hash of every file under the bundled runtime tree, so a shipped
/// change (a new harness protocol) forces a re-provision.
fn runtime_content_hash(source: &Path) -> Result<String, String> {
    let mut entries = Vec::new();
    collect_bundle_files(source, source, &mut entries)?;
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
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
        use crate::services::{RealFileSystem, RealGitClient};

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);

        let result =
            sync_workspace_bundle(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert!(result.updated);
        assert!(result.skipped_conflicts.is_empty());
        assert!(repo.join(".git").exists());
        assert!(repo.join(BUNDLE_SYNC_MARKER).exists());
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
        // A current marker with nothing missing short-circuits before any status,
        // snapshot, or update scan.
        git.expect_status().times(0);

        let result =
            sync_workspace_bundle(&git, &fs, Path::new("/resources"), repo, "1.0.0").unwrap();
        assert_eq!(result, BundleSyncResult::default());
    }

    #[test]
    fn existing_install_copies_missing_bundled_files() {
        let repo = Path::new("/home/user/.cairn");
        let temp = TempDir::new().unwrap();
        let resources = temp.path().join("resources");
        write_resources_a(&resources);
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        git.expect_is_repo().with(eq(repo)).returning(|_| Ok(true));
        git.expect_init_repo().times(0);
        let agent_exists = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let exists_state = agent_exists.clone();
        fs.expect_exists().returning(move |path| {
            path == Path::new("/home/user/.cairn/.gitignore")
                || path == Path::new("/home/user/.cairn/recipes/default.yaml")
                || (path == Path::new("/home/user/.cairn/agents/explore.md")
                    && exists_state.load(std::sync::atomic::Ordering::SeqCst))
        });
        fs.expect_read_to_string()
            .returning(|_| Ok(super::super::repo::WORKSPACE_GITIGNORE.to_string()));
        fs.expect_write_str().returning(|_, _| Ok(()));
        git.expect_root_commit()
            .with(eq(repo), eq(DEFAULT_BRANCH))
            .returning(|_, _| Ok("root".to_string()));
        fs.expect_copy_file().times(1).returning(move |_, _| {
            agent_exists.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        fs.expect_copy_dir_recursive().times(0);
        git.expect_run()
            .withf(|path, args| {
                path == Path::new("/home/user/.cairn")
                    && args.len() == 4
                    && args[0] == "add"
                    && args[1] == "--"
                    && args[2] == ".gitignore"
                    && args[3] == "agents/explore.md"
            })
            .returning(|_, _| {
                Ok(crate::services::GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });
        git.expect_run()
            .withf(|path, args| {
                path == Path::new("/home/user/.cairn")
                    && args.iter().any(|arg| arg == "commit")
                    && args.iter().any(|arg| arg == "--only")
                    && args
                        .iter()
                        .any(|arg| arg == "Add missing bundled workspace defaults")
                    && args.iter().any(|arg| arg == "agents/explore.md")
            })
            .returning(|_, _| {
                Ok(crate::services::GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });
        git.expect_status()
            .with(eq(repo))
            .returning(|_| Ok(String::new()));

        let result = sync_workspace_bundle(&git, &fs, &resources, repo, "2.0.0").unwrap();
        assert!(result.updated);
        assert!(result.skipped_conflicts.is_empty());
    }

    #[test]
    fn real_temp_repo_adds_missing_bundles_and_preserves_user_edits() {
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
        // The user-edited file is preserved and surfaced as a skipped conflict
        // even though the bundle shipped a new version of it.
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert_eq!(
            result.skipped_conflicts,
            vec!["agents/explore.md".to_string()]
        );
        // Genuinely new bundled resources are still installed.
        assert!(repo.join("agents/new-agent.md").exists());
        assert!(repo.join("recipes/memory-triage.yaml").exists());
        assert!(repo.join("skills/example/SKILL.md").exists());
    }

    #[test]
    fn changed_bundle_file_propagates_to_unmodified_install() {
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
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "bundle a\n"
        );

        // The user never touched explore.md, so the bundle's new version of it
        // must reach this install on upgrade.
        let result = sync_workspace_bundle(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        assert!(result.updated);
        assert!(result.skipped_conflicts.is_empty());
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "bundle b\n"
        );
        // A repeated sync of the same bundle is a no-op (marker is current).
        let again = sync_workspace_bundle(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        assert!(!again.updated);
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

    #[test]
    fn missing_git_still_provisions_all_defaults_and_recovers_later() {
        use crate::services::{RealFileSystem, RealGitClient};

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);

        let mut unavailable_git = MockGitClient::new();
        unavailable_git
            .expect_is_repo()
            .with(eq(repo.clone()))
            .times(1)
            .returning(|_| Err("Failed to run git: No such file or directory".to_string()));

        let error = sync_workspace_bundle(
            &unavailable_git,
            &RealFileSystem,
            &resources,
            &repo,
            "1.0.0",
        )
        .unwrap_err();
        assert!(error.contains("No such file or directory"));
        assert!(repo.join("agents/explore.md").exists());
        assert!(repo.join("recipes/default.yaml").exists());
        assert!(repo.join("skills/example/SKILL.md").exists());
        assert!(repo.join("workflows/example/workflow.yaml").exists());
        assert!(!repo.join(BUNDLE_SYNC_MARKER).exists());
        assert!(!repo.join(".git").exists());

        // A user edit made while Git is unavailable survives recovery,
        // including when a previous recovery stopped after repository init.
        std::fs::write(repo.join("agents/explore.md"), "user edit\n").unwrap();

        let git = RealGitClient;
        git.init_repo(&repo, DEFAULT_BRANCH).unwrap();
        assert!(git.root_commit(&repo, DEFAULT_BRANCH).is_err());

        let recovered =
            sync_workspace_bundle(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(recovered.updated);
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert!(repo.join(".git").exists());
        assert!(repo.join(BUNDLE_SYNC_MARKER).exists());
        assert!(git.root_commit(&repo, DEFAULT_BRANCH).is_ok());
        let tracked = git.run(&repo, vec!["ls-files".to_string()]).unwrap();
        for path in [
            "agents/explore.md",
            "recipes/default.yaml",
            "skills/example/SKILL.md",
            "workflows/example/workflow.yaml",
        ] {
            assert!(tracked.stdout.lines().any(|tracked| tracked == path));
        }

        let idempotent =
            sync_workspace_bundle(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(!idempotent.updated);

        std::fs::write(resources.join("agents/explore.md"), "bundle upgrade\n").unwrap();
        let upgraded =
            sync_workspace_bundle(&git, &RealFileSystem, &resources, &repo, "2.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("agents/explore.md")).unwrap(),
            "user edit\n"
        );
        assert_eq!(
            upgraded.skipped_conflicts,
            vec!["agents/explore.md".to_string()]
        );
    }

    #[test]
    fn restoring_defaults_does_not_consume_an_unrelated_staged_change() {
        use crate::services::{RealFileSystem, RealGitClient};

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        let git = RealGitClient;
        sync_workspace_bundle(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();

        std::fs::write(repo.join("AGENTS.md"), "staged user change\n").unwrap();
        git.run(
            &repo,
            vec!["add".to_string(), "--".to_string(), "AGENTS.md".to_string()],
        )
        .unwrap();
        std::fs::remove_file(repo.join("agents/explore.md")).unwrap();

        let restored =
            sync_workspace_bundle(&git, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(restored.updated);
        let staged = git
            .run(
                &repo,
                vec![
                    "diff".to_string(),
                    "--cached".to_string(),
                    "--name-only".to_string(),
                ],
            )
            .unwrap();
        assert_eq!(staged.stdout.trim(), "AGENTS.md");
    }

    #[test]
    fn provision_runtime_syncs_then_short_circuits_then_resyncs_on_change() {
        use crate::services::RealFileSystem;

        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        let cairn_home = temp.path().join("home");
        let harness = resource_dir.join("runtime/node_modules/@cairn/harness");
        std::fs::create_dir_all(harness.join("src")).unwrap();
        std::fs::write(
            harness.join("package.json"),
            "{\"name\":\"@cairn/harness\"}",
        )
        .unwrap();
        std::fs::write(harness.join("src/index.ts"), "export const v = 1;\n").unwrap();

        let fs = RealFileSystem;
        // First provision installs the runtime and its marker.
        assert!(provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());
        let dest_harness = cairn_home.join("runtime/node_modules/@cairn/harness/src/index.ts");
        assert!(dest_harness.exists());
        assert!(cairn_home.join("runtime/.runtime-version").exists());

        // A repeat with unchanged content is a no-op (marker current).
        assert!(!provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());

        // A shipped change re-syncs the tree.
        std::fs::write(harness.join("src/index.ts"), "export const v = 2;\n").unwrap();
        assert!(provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());
        assert_eq!(
            std::fs::read_to_string(&dest_harness).unwrap(),
            "export const v = 2;\n"
        );
    }

    #[test]
    fn provision_runtime_is_noop_without_a_bundled_runtime() {
        use crate::services::RealFileSystem;

        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        std::fs::create_dir_all(&resource_dir).unwrap();
        let cairn_home = temp.path().join("home");

        // No `runtime/` in the bundle (the dev case): nothing provisioned.
        assert!(!provision_workflow_runtime(&RealFileSystem, &resource_dir, &cairn_home).unwrap());
        assert!(!cairn_home.join("runtime").exists());
    }

    /// A fresh workspace seeds the bundled `fan-out` workflow package, the loader
    /// then lists it, and a later sync of a CHANGED bundle preserves the user's
    /// edit to their copy (directory packages are copy-when-missing, never
    /// overwritten). This is the loader-level proof that a fresh workspace lists
    /// `fan-out` out of the box — the built-in workflow's whole provisioning
    /// contract — without dogfooding through the running host.
    #[test]
    fn bundled_workflow_seeds_missing_lists_and_preserves_user_edits() {
        use crate::services::{RealFileSystem, RealGitClient};

        fn write_workflow_bundle(root: &Path, body: &str) {
            std::fs::create_dir_all(root.join("workflows/fan-out")).unwrap();
            std::fs::write(
                root.join("workflows/fan-out/workflow.yaml"),
                "name: Fan Out\ndescription: Zero-authoring ad-hoc agent batch.\n",
            )
            .unwrap();
            std::fs::write(root.join("workflows/fan-out/main.ts"), body).unwrap();
        }

        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources_a = temp.path().join("resources-a");
        let resources_b = temp.path().join("resources-b");
        write_resources_a(&resources_a);
        write_resources_b(&resources_b);
        write_workflow_bundle(&resources_a, "// v1\n");
        write_workflow_bundle(&resources_b, "// v2\n");

        let git = RealGitClient;
        let fs = RealFileSystem;

        // Fresh workspace: the package is seeded and the loader lists it.
        sync_workspace_bundle(&git, &fs, &resources_a, &repo, "1.0.0").unwrap();
        assert!(repo.join("workflows/fan-out/workflow.yaml").exists());
        let listed = crate::config::workflows::list_workflows(&repo, None).unwrap();
        assert!(
            listed.iter().any(|r| matches!(
                r,
                crate::config::ConfigResult::Ok(w) if w.id == "fan-out"
            )),
            "fresh workspace must list the bundled fan-out workflow"
        );

        // The user edits their copy, then a CHANGED bundle syncs: the edit stands
        // (copy-when-missing never overwrites an existing package).
        std::fs::write(repo.join("workflows/fan-out/main.ts"), "// user edit\n").unwrap();
        git.add_all(&repo).unwrap();
        git.commit(&repo, "User edits fan-out").unwrap();
        sync_workspace_bundle(&git, &fs, &resources_b, &repo, "2.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("workflows/fan-out/main.ts")).unwrap(),
            "// user edit\n",
            "a user's edited workflow copy must never be clobbered by a re-sync"
        );
    }

    fn write_complete_resources(root: &Path) {
        std::fs::create_dir_all(root.join("agents")).unwrap();
        std::fs::create_dir_all(root.join("recipes")).unwrap();
        std::fs::create_dir_all(root.join("skills/example")).unwrap();
        std::fs::create_dir_all(root.join("workflows/example")).unwrap();
        std::fs::write(root.join("agents/explore.md"), "bundle agent\n").unwrap();
        std::fs::write(root.join("recipes/default.yaml"), "name: default\n").unwrap();
        std::fs::write(
            root.join("skills/example/SKILL.md"),
            "---\nname: Example\ndescription: Example skill\n---\n",
        )
        .unwrap();
        std::fs::write(
            root.join("workflows/example/workflow.yaml"),
            "name: Example\n",
        )
        .unwrap();
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
