use super::*;

/// Result of populating a worktree with gitignored content.
#[derive(Debug, Default)]
pub struct PopulateResult {
    pub copied: usize,
    pub symlinked: usize,
    pub skipped: usize,
    pub failed: usize,
}

use crate::config::project_settings::PopulateConfig;

/// Determine the population strategy for a given path.
///
/// Returns `Some("copy")` if it matches a copy pattern, `Some("symlink")` if
/// it matches a symlink pattern, or `None` if unmatched (skip).
/// Copy wins when a path matches both.
fn classify_path(
    path: &str,
    copy_set: &globset::GlobSet,
    symlink_set: &globset::GlobSet,
) -> Option<&'static str> {
    if copy_set.is_match(path) {
        Some("copy")
    } else if symlink_set.is_match(path) {
        Some("symlink")
    } else {
        None
    }
}

/// Build a GlobSet from a list of pattern strings.
fn build_glob_set(patterns: &[String]) -> Result<globset::GlobSet, String> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        let glob = globset::GlobBuilder::new(pattern)
            .literal_separator(false)
            .build()
            .map_err(|e| format!("Invalid glob pattern '{}': {}", pattern, e))?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|e| format!("Failed to build glob set: {}", e))
}

/// Populate a worktree with gitignored content according to explicit rules.
///
/// Discovers gitignored paths via `git ls-files --others --ignored --exclude-standard --directory`,
/// then applies the configured strategy:
/// - Paths matching `config.copy` patterns are copied (isolated per worktree).
/// - Paths matching `config.symlink` patterns are symlinked (shared with main repo).
/// - Unmatched paths are skipped.
/// - If a path matches both copy and symlink, copy wins (safer — isolation over sharing).
///
/// Best-effort: individual failures are logged as warnings and don't block.
pub fn populate_worktree(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    repo_path: &Path,
    worktree_path: &Path,
    config: &PopulateConfig,
) -> Result<PopulateResult, String> {
    if config.is_empty() {
        return Ok(PopulateResult::default());
    }

    let copy_set = build_glob_set(&config.copy)?;
    let symlink_set = build_glob_set(&config.symlink)?;

    // Discover all gitignored content in the main repo
    let output = git.run(
        repo_path,
        vec![
            "ls-files".to_string(),
            "--others".to_string(),
            "--ignored".to_string(),
            "--exclude-standard".to_string(),
            "--directory".to_string(),
        ],
    )?;

    if !output.success {
        return Err(format!("git ls-files failed: {}", output.stderr.trim()));
    }

    let entries: Vec<&str> = output
        .stdout
        .lines()
        .filter(|line| !line.is_empty())
        .collect();

    if entries.is_empty() {
        log::info!("No gitignored content discovered");
        return Ok(PopulateResult::default());
    }

    log::info!(
        "Evaluating {} gitignored entries against populate rules",
        entries.len()
    );

    let mut result = PopulateResult::default();

    for entry in entries {
        // Always skip .cairn/ directory
        if entry == ".cairn/" || entry.starts_with(".cairn/") {
            result.skipped += 1;
            continue;
        }

        let entry_path = entry.trim_end_matches('/');

        // Match against the entry as given by git (with trailing slash for dirs)
        let strategy = classify_path(entry, &copy_set, &symlink_set)
            .or_else(|| classify_path(entry_path, &copy_set, &symlink_set));

        let strategy = match strategy {
            Some(s) => s,
            None => {
                result.skipped += 1;
                continue;
            }
        };

        let src = repo_path.join(entry_path);
        let dst = worktree_path.join(entry_path);

        // Skip if source doesn't exist
        if !fs.exists(&src) {
            result.skipped += 1;
            continue;
        }

        // Skip if destination already exists
        if fs.exists(&dst) || fs.is_symlink(&dst) {
            result.skipped += 1;
            continue;
        }

        // Ensure parent directory exists
        if let Some(parent) = dst.parent() {
            if let Err(e) = fs.create_dir_all(parent) {
                log::warn!("Failed to create parent dir for {}: {}", entry, e);
                result.failed += 1;
                continue;
            }
        }

        let is_dir = entry.ends_with('/');

        match strategy {
            "copy" => {
                if is_dir {
                    match fs.copy_dir_recursive(&src, &dst) {
                        Ok(()) => {
                            result.copied += 1;
                        }
                        Err(e) => {
                            log::warn!("Recursive copy failed for {}: {}", entry, e);
                            result.failed += 1;
                        }
                    }
                } else {
                    match fs.copy_file(&src, &dst) {
                        Ok(()) => {
                            result.copied += 1;
                        }
                        Err(e) => {
                            log::warn!("Copy failed for {}: {}", entry, e);
                            result.failed += 1;
                        }
                    }
                }
            }
            "symlink" => match fs.symlink(&src, &dst) {
                Ok(()) => {
                    result.symlinked += 1;
                }
                Err(e) => {
                    log::warn!("Symlink failed for {}: {}", entry, e);
                    result.failed += 1;
                }
            },
            _ => unreachable!(),
        }
    }

    // Explicitly-populated gitignored content is kept UNCOMMITTED by the jj
    // store's `snapshot.auto-track` exclude, set before this populate runs and
    // verified by the security backstop in `prepare_worktree_for_job`. (The old
    // `.git/info/exclude` seeding was a no-op in a `.jj`-only workspace, which
    // has no `.git`.)
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::{MockFileSystem, MockGitClient};
    use crate::services::GitOutput;
    // Tests for populate_worktree

    fn git_ls_files_output(entries: &[&str]) -> GitOutput {
        GitOutput {
            success: true,
            stdout: entries.join("\n"),
            stderr: String::new(),
        }
    }

    fn populate_with(
        git: &MockGitClient,
        fs: &MockFileSystem,
        config: &PopulateConfig,
    ) -> Result<PopulateResult, String> {
        populate_worktree(git, fs, Path::new("/repo"), Path::new("/worktree"), config)
    }

    fn populate_ok(
        git: &MockGitClient,
        fs: &MockFileSystem,
        config: &PopulateConfig,
    ) -> PopulateResult {
        let result = populate_with(git, fs, config);
        assert!(result.is_ok());
        result.unwrap()
    }

    fn expect_available_candidate(fs: &mut MockFileSystem, rel_path: &str) {
        let source = std::path::PathBuf::from(format!("/repo/{rel_path}"));
        fs.expect_exists()
            .withf(move |p| p == source.as_path())
            .returning(|_| true);

        let destination = std::path::PathBuf::from(format!("/worktree/{rel_path}"));
        fs.expect_exists()
            .withf(move |p| p == destination.as_path())
            .returning(|_| false);

        let destination = std::path::PathBuf::from(format!("/worktree/{rel_path}"));
        fs.expect_is_symlink()
            .withf(move |p| p == destination.as_path())
            .returning(|_| false);
    }

    #[test]
    fn test_populate_empty_config_returns_immediately() {
        let git = MockGitClient::new();
        let fs = MockFileSystem::new();
        let config = PopulateConfig::default();

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 0);
        assert_eq!(r.symlinked, 0);
        assert_eq!(r.skipped, 0);
        assert_eq!(r.failed, 0);
    }

    #[test]
    fn test_populate_no_gitignored_content() {
        let mut git = MockGitClient::new();
        let fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec![".env".to_string()],
            symlink: vec![],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[])));

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 0);
        assert_eq!(r.skipped, 0);
    }

    #[test]
    fn test_populate_excludes_cairn_directory() {
        let mut git = MockGitClient::new();
        let fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec![],
            symlink: vec![".cairn/".to_string()],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".cairn/", ".cairn/config.yaml"])));

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.symlinked, 0);
        assert_eq!(r.skipped, 2);
    }

    #[test]
    fn test_populate_copies_files_symlinks_dirs() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec![".env".to_string()],
            symlink: vec!["target/".to_string()],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".env", "target/"])));

        expect_available_candidate(&mut fs, ".env");
        expect_available_candidate(&mut fs, "target");

        fs.expect_create_dir_all().returning(|_| Ok(()));

        // .env should be copied
        fs.expect_copy_file()
            .withf(|src, dst| src == Path::new("/repo/.env") && dst == Path::new("/worktree/.env"))
            .times(1)
            .returning(|_, _| Ok(()));

        // target should be symlinked
        fs.expect_symlink()
            .withf(|target, link| {
                target == Path::new("/repo/target") && link == Path::new("/worktree/target")
            })
            .times(1)
            .returning(|_, _| Ok(()));

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 1);
        assert_eq!(r.symlinked, 1);
        assert_eq!(r.skipped, 0);
        assert_eq!(r.failed, 0);
    }

    #[test]
    fn test_populate_unmatched_paths_skipped() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();
        // Only .env in copy list, but git reports node_modules too
        let config = PopulateConfig {
            copy: vec![".env".to_string()],
            symlink: vec![],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&["node_modules/", ".env"])));

        // .env source doesn't exist (so it's skipped for a different reason)
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env"))
            .returning(|_| false);

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 0);
        assert_eq!(r.symlinked, 0);
        // node_modules skipped (no matching pattern) + .env skipped (source missing)
        assert_eq!(r.skipped, 2);
    }

    #[test]
    fn test_populate_copy_wins_over_symlink() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();
        // .env matches both copy and symlink — copy should win
        let config = PopulateConfig {
            copy: vec![".env*".to_string()],
            symlink: vec![".env*".to_string()],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".env"])));

        expect_available_candidate(&mut fs, ".env");
        fs.expect_create_dir_all().returning(|_| Ok(()));

        // Should copy, NOT symlink
        fs.expect_copy_file().times(1).returning(|_, _| Ok(()));

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 1);
        assert_eq!(r.symlinked, 0);
    }

    #[test]
    fn test_populate_git_failure_is_fatal() {
        let mut git = MockGitClient::new();
        let fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec![".env".to_string()],
            symlink: vec![],
        };

        git.expect_run()
            .returning(|_, _| Err("git not found".to_string()));

        let result = populate_with(&git, &fs, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_populate_skips_existing_destinations() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec![".env".to_string()],
            symlink: vec![],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".env"])));

        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env"))
            .returning(|_| true);
        fs.expect_exists()
            .withf(|p| p == Path::new("/worktree/.env"))
            .returning(|_| true);

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 0);
        assert_eq!(r.skipped, 1);
    }

    #[test]
    fn test_populate_copy_dir_recursive_for_directory_entries() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();
        // Pattern matches a directory entry — should use copy_dir_recursive, not copy_file
        let config = PopulateConfig {
            copy: vec!["node_modules/".to_string()],
            symlink: vec![],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&["node_modules/"])));

        expect_available_candidate(&mut fs, "node_modules");
        fs.expect_create_dir_all().returning(|_| Ok(()));

        // Must call copy_dir_recursive (not copy_file) for directory entries
        fs.expect_copy_dir_recursive()
            .withf(|src, dst| {
                src == Path::new("/repo/node_modules") && dst == Path::new("/worktree/node_modules")
            })
            .times(1)
            .returning(|_, _| Ok(()));

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 1);
        assert_eq!(r.symlinked, 0);
        assert_eq!(r.failed, 0);
    }

    #[test]
    fn test_populate_invalid_glob_pattern_returns_error() {
        let git = MockGitClient::new();
        let fs = MockFileSystem::new();
        // An unclosed bracket is an invalid glob
        let config = PopulateConfig {
            copy: vec!["[invalid".to_string()],
            symlink: vec![],
        };

        let result = populate_with(&git, &fs, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid glob pattern"));
    }

    #[test]
    fn test_populate_git_ls_files_non_zero_exit() {
        let mut git = MockGitClient::new();
        let fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec![".env".to_string()],
            symlink: vec![],
        };

        git.expect_run().returning(|_, _| {
            Ok(GitOutput {
                success: false,
                stdout: String::new(),
                stderr: "fatal: not a git repository".to_string(),
            })
        });

        let result = populate_with(&git, &fs, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not a git repository"));
    }

    #[test]
    fn test_populate_best_effort_continues_after_individual_failure() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec![".env*".to_string()],
            symlink: vec![],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".env", ".env.local"])));

        expect_available_candidate(&mut fs, ".env");
        expect_available_candidate(&mut fs, ".env.local");

        fs.expect_create_dir_all().returning(|_| Ok(()));

        // First copy fails, second succeeds
        let mut call_count = 0;
        fs.expect_copy_file().times(2).returning(move |_, _| {
            call_count += 1;
            if call_count == 1 {
                Err("permission denied".to_string())
            } else {
                Ok(())
            }
        });

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 1);
        assert_eq!(r.failed, 1);
    }

    #[test]
    fn test_populate_skips_dangling_symlinks_at_destination() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec![],
            symlink: vec!["node_modules/".to_string()],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&["node_modules/"])));

        // Source exists
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/node_modules"))
            .returning(|_| true);
        // Destination doesn't "exist" (dangling symlink reports false for exists)
        fs.expect_exists()
            .withf(|p| p == Path::new("/worktree/node_modules"))
            .returning(|_| false);
        // But IS a symlink (dangling)
        fs.expect_is_symlink()
            .withf(|p| p == Path::new("/worktree/node_modules"))
            .returning(|_| true);

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.symlinked, 0);
        assert_eq!(r.skipped, 1);
    }

    #[test]
    fn test_populate_copy_dir_failure_is_best_effort() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();
        let config = PopulateConfig {
            copy: vec!["build/".to_string()],
            symlink: vec![],
        };

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&["build/"])));

        expect_available_candidate(&mut fs, "build");
        fs.expect_create_dir_all().returning(|_| Ok(()));

        // copy_dir_recursive fails
        fs.expect_copy_dir_recursive()
            .returning(|_, _| Err("disk full".to_string()));

        let r = populate_ok(&git, &fs, &config);
        assert_eq!(r.copied, 0);
        assert_eq!(r.failed, 1);
    }
}
