use crate::services::{
    FileSystem, GitClient, ProcessSpawner, RealFileSystem, RealGitClient, SpawnConfig,
};
use std::path::{Path, PathBuf};

/// Generate branch name from project key, issue number, and title.
/// Uses "agent" as the default prefix.
/// Example: generate_branch_name("CAI", 42, "Add user authentication")
///          -> "agent/CAI-42-add-user-authentication"
#[allow(dead_code)]
pub fn generate_branch_name(project_key: &str, issue_number: i32, issue_title: &str) -> String {
    generate_branch_name_with_slug("agent", project_key, issue_number, issue_title, None)
}

/// Generate branch name with optional agent-provided slug and configurable prefix.
/// If branch_slug is provided and not empty, uses it instead of auto-generating from issue_title.
/// Example: generate_branch_name_with_slug("agent", "CAI", 42, "Add user authentication", Some("user-auth"))
///          -> "agent/CAI-42-user-auth"
pub fn generate_branch_name_with_slug(
    branch_prefix: &str,
    project_key: &str,
    issue_number: i32,
    issue_title: &str,
    branch_slug: Option<&str>,
) -> String {
    let prefix = if branch_prefix.is_empty() {
        "agent"
    } else {
        branch_prefix.trim_end_matches('/')
    };
    let display_id = format!("{}-{}", project_key, issue_number);
    let slug = match branch_slug {
        Some(s) if !s.is_empty() => sanitize_branch_slug(s, 30), // longer limit for agent slugs
        _ => sanitize_branch_slug(issue_title, 20),
    };
    if slug.is_empty() {
        format!("{}/{}", prefix, display_id)
    } else {
        format!("{}/{}-{}", prefix, display_id, slug)
    }
}

/// Sanitize title into URL-safe slug (lowercase, dashes, max length).
pub fn sanitize_branch_slug(title: &str, max_len: usize) -> String {
    title
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("-")
        .chars()
        .take(max_len)
        .collect::<String>()
        .trim_end_matches('-')
        .to_string()
}

/// Base directory for all worktrees: ~/.cairn/worktrees/
#[allow(dead_code)] // Used by worktree_path_from_branch and other helpers
pub fn worktree_base_dir() -> Result<PathBuf, String> {
    let home = dirs::home_dir().ok_or("Cannot find home directory")?;
    Ok(home.join(".cairn").join("worktrees"))
}

/// Worktree path derived from branch name: ~/.cairn/worktrees/CAI-42-add-user-auth/
/// Strips prefix (anything before the first '/') from branch name
#[allow(dead_code)] // Will be used when job-based execution is wired up
pub fn worktree_path_from_branch(branch_name: &str) -> Result<PathBuf, String> {
    // Strip any prefix before the first '/' (e.g., "agent/CAI-42" -> "CAI-42")
    let dir_name = branch_name
        .split_once('/')
        .map(|(_, rest)| rest)
        .unwrap_or(branch_name);
    Ok(worktree_base_dir()?.join(dir_name))
}

/// Worktree path for an arbitrary name (e.g., for plan worktrees)
/// Example: worktree_path_for_name("CAI-42-plan-0") -> ~/.cairn/worktrees/CAI-42-plan-0/
#[allow(dead_code)] // Will be used when job-based execution is wired up
pub fn worktree_path_for_name(name: &str) -> Result<PathBuf, String> {
    Ok(worktree_base_dir()?.join(name))
}

/// Check if a git branch exists in the repository (injectable version).
pub fn branch_exists_with_git(
    git: &dyn GitClient,
    repo_path: &Path,
    branch_name: &str,
) -> Result<bool, String> {
    git.branch_exists(repo_path, branch_name)
}

/// Parse worktree list output to find worktree path for a branch.
/// This is a pure function for testability.
pub fn parse_worktree_list_for_branch(output: &str, branch_name: &str) -> Option<PathBuf> {
    let mut current_worktree: Option<PathBuf> = None;

    for line in output.lines() {
        if let Some(path) = line.strip_prefix("worktree ") {
            current_worktree = Some(PathBuf::from(path));
        } else if let Some(branch) = line.strip_prefix("branch refs/heads/") {
            if branch == branch_name {
                return current_worktree;
            }
        }
    }

    None
}

/// Get the worktree path for a branch if it's already checked out (injectable version).
pub fn get_worktree_for_branch_with_git(
    git: &dyn GitClient,
    repo_path: &Path,
    branch_name: &str,
) -> Result<Option<PathBuf>, String> {
    let output = git.worktree_list(repo_path)?;
    Ok(parse_worktree_list_for_branch(&output, branch_name))
}

/// Check if error is due to a stale worktree entry.
fn is_stale_worktree_error(error_msg: &str) -> bool {
    error_msg.contains("is a missing but already registered worktree")
        || error_msg.contains("already registered worktree")
}

/// Attempt to create a worktree, with automatic recovery if stale entry is detected.
fn try_add_worktree(
    git: &dyn GitClient,
    repo_path: &Path,
    worktree_path: &Path,
    branch_name: &str,
    base_branch: Option<&str>,
) -> Result<(), String> {
    let result = if let Some(base) = base_branch {
        git.worktree_add_new_branch(repo_path, worktree_path, branch_name, base)
    } else {
        git.worktree_add_existing_branch(repo_path, worktree_path, branch_name)
    };

    match result {
        Ok(()) => Ok(()),
        Err(e) if is_stale_worktree_error(&e) => {
            log::warn!(
                "Detected stale worktree entry, attempting to prune and retry: {}",
                e
            );

            // Prune stale worktree entries
            git.worktree_prune(repo_path)?;
            log::info!("Pruned stale worktree entries, retrying creation");

            // Retry the operation
            if let Some(base) = base_branch {
                git.worktree_add_new_branch(repo_path, worktree_path, branch_name, base)
            } else {
                git.worktree_add_existing_branch(repo_path, worktree_path, branch_name)
            }
        }
        Err(e) => Err(e),
    }
}

/// Create a new worktree with a new branch based on base_branch (injectable version).
pub fn create_worktree_with_services(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    repo_path: &Path,
    worktree_path: &Path,
    branch_name: &str,
    base_branch: &str,
) -> Result<(), String> {
    // Ensure base dir exists
    if let Some(parent) = worktree_path.parent() {
        fs.create_dir_all(parent)
            .map_err(|e| format!("Failed to create worktree directory: {}", e))?;
    }

    // Check if worktree path already exists
    if fs.exists(worktree_path) {
        return Err(format!(
            "Worktree path already exists: {}. Clean up first.",
            worktree_path.display()
        ));
    }

    // Check if branch already exists
    let branch_exists_val = branch_exists_with_git(git, repo_path, branch_name)?;

    if branch_exists_val {
        // Branch exists - check if it's already checked out in another worktree
        if let Some(existing_wt) = get_worktree_for_branch_with_git(git, repo_path, branch_name)? {
            return Err(format!(
                "Branch '{}' is already checked out in worktree: {}. \
                 Clean up the existing implementation first.",
                branch_name,
                existing_wt.display()
            ));
        }

        // Branch exists but not checked out - create worktree using existing branch
        log::info!(
            "Branch {} exists, creating worktree with existing branch",
            branch_name
        );
        try_add_worktree(git, repo_path, worktree_path, branch_name, None)?;
    } else {
        // Create new branch and worktree
        log::info!(
            "Creating new branch {} from {} with worktree",
            branch_name,
            base_branch
        );
        try_add_worktree(
            git,
            repo_path,
            worktree_path,
            branch_name,
            Some(base_branch),
        )?;
    }

    log::info!("Created worktree at {}", worktree_path.display());
    Ok(())
}

/// Remove a worktree (injectable version).
pub fn remove_worktree_with_services(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    repo_path: &Path,
    worktree_path: &Path,
    force: bool,
) -> Result<(), String> {
    if !fs.exists(worktree_path) {
        log::info!(
            "Worktree path {} does not exist, skipping removal",
            worktree_path.display()
        );
        return Ok(());
    }

    git.worktree_remove(repo_path, worktree_path, force)?;

    log::info!("Removed worktree at {}", worktree_path.display());
    Ok(())
}

/// Remove a worktree (optionally force if dirty).
pub fn remove_worktree(repo_path: &str, worktree_path: &Path, force: bool) -> Result<(), String> {
    remove_worktree_with_services(
        &RealGitClient,
        &RealFileSystem,
        Path::new(repo_path),
        worktree_path,
        force,
    )
}

/// Delete a local branch (injectable version).
/// Silently succeeds if the branch doesn't exist.
pub fn delete_branch_with_services(
    git: &dyn GitClient,
    repo_path: &Path,
    branch_name: &str,
) -> Result<(), String> {
    // Safety check: ensure branch exists
    if !branch_exists_with_git(git, repo_path, branch_name)? {
        // Already deleted, silently succeed
        log::info!("Branch {} does not exist, skipping deletion", branch_name);
        return Ok(());
    }

    // Delete the branch (force=true since we're cleaning up after merge/close)
    git.delete_branch(repo_path, branch_name, true)?;

    log::info!("Deleted local branch {}", branch_name);
    Ok(())
}

/// Create a detached HEAD worktree (no branch) at a specific commit.
/// Used for plan nodes which are read-only.
pub fn create_detached_worktree_with_services(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    repo_path: &Path,
    worktree_path: &Path,
    commit_ref: &str,
) -> Result<(), String> {
    // Ensure base dir exists
    if let Some(parent) = worktree_path.parent() {
        fs.create_dir_all(parent)
            .map_err(|e| format!("Failed to create worktree directory: {}", e))?;
    }

    // Remove existing worktree if it exists (plans are ephemeral)
    if fs.exists(worktree_path) {
        log::info!(
            "Removing existing worktree at {} before creating new one",
            worktree_path.display()
        );
        remove_worktree_with_services(git, fs, repo_path, worktree_path, true)?;
    }

    log::info!(
        "Creating detached worktree at {} from {}",
        worktree_path.display(),
        commit_ref
    );

    // Try to create detached worktree, with automatic recovery for stale entries
    let result = git.worktree_add_detached(repo_path, worktree_path, commit_ref);
    match result {
        Ok(()) => Ok(()),
        Err(e) if is_stale_worktree_error(&e) => {
            log::warn!(
                "Detected stale worktree entry, attempting to prune and retry: {}",
                e
            );
            git.worktree_prune(repo_path)?;
            log::info!("Pruned stale worktree entries, retrying detached worktree creation");
            git.worktree_add_detached(repo_path, worktree_path, commit_ref)
        }
        Err(e) => Err(e),
    }?;

    log::info!("Created detached worktree at {}", worktree_path.display());
    Ok(())
}

/// Create a detached HEAD worktree (no branch) at a specific commit.
#[allow(dead_code)] // Will be used when job-based execution is wired up
pub fn create_detached_worktree(
    repo_path: &str,
    worktree_path: &Path,
    commit_ref: &str,
) -> Result<(), String> {
    create_detached_worktree_with_services(
        &RealGitClient,
        &RealFileSystem,
        Path::new(repo_path),
        worktree_path,
        commit_ref,
    )
}

/// Get platform-appropriate shell command and flag.
fn get_shell_command() -> (&'static str, &'static str) {
    if cfg!(windows) {
        ("cmd.exe", "/c")
    } else {
        ("sh", "-c")
    }
}

/// Run setup commands in a worktree directory (injectable version).
/// Commands are executed sequentially in the worktree directory.
/// If any command fails, execution stops and returns an error.
pub fn run_setup_commands_with_process(
    process: &dyn ProcessSpawner,
    worktree_path: &Path,
    commands: &[String],
) -> Result<(), String> {
    if commands.is_empty() {
        return Ok(());
    }

    log::info!(
        "Running {} setup command(s) in {}",
        commands.len(),
        worktree_path.display()
    );

    let (shell, flag) = get_shell_command();

    for (idx, cmd) in commands.iter().enumerate() {
        log::info!("Setup command {}/{}: {}", idx + 1, commands.len(), cmd);

        let config = SpawnConfig::new(shell)
            .arg(flag)
            .arg(cmd)
            .cwd(&worktree_path.to_string_lossy());

        let output = process
            .run(config)
            .map_err(|e| format!("Failed to execute setup command '{}': {}", cmd, e))?;

        if !output.success {
            return Err(format!(
                "Setup command '{}' failed with exit code {:?}\nStdout: {}\nStderr: {}",
                cmd,
                output.exit_code,
                output.stdout.trim(),
                output.stderr.trim()
            ));
        }

        if !output.stdout.trim().is_empty() {
            log::info!("Command output: {}", output.stdout.trim());
        }
    }

    log::info!("All setup commands completed successfully");
    Ok(())
}

/// Copy files from the main repository to a worktree directory (injectable version).
/// Files that don't exist in the source are logged as warnings and skipped.
/// Existing files in the destination are overwritten.
pub fn copy_files_to_worktree_with_services(
    fs: &dyn FileSystem,
    repo_path: &Path,
    worktree_path: &Path,
    files: &[String],
) -> Result<(), String> {
    if files.is_empty() {
        return Ok(());
    }

    log::info!(
        "Copying {} file(s) from {} to {}",
        files.len(),
        repo_path.display(),
        worktree_path.display()
    );

    for file in files {
        let src = repo_path.join(file);
        let dst = worktree_path.join(file);

        if !fs.exists(&src) {
            log::warn!("Copy file not found in main repo: {}", file);
            continue;
        }

        fs.copy_file(&src, &dst)?;
        log::info!("Copied {} to worktree", file);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::{MockFileSystem, MockGitClient, MockProcessSpawner};
    use crate::services::CommandOutput;

    // Tests for branch name generation

    #[test]
    fn test_generate_branch_name_basic() {
        let result = generate_branch_name("CAI", 42, "Add user authentication");
        // max_len is 20, "add-user-authentication" truncates to "add-user-authenticat"
        assert_eq!(result, "agent/CAI-42-add-user-authenticat");
    }

    #[test]
    fn test_generate_branch_name_empty_title() {
        let result = generate_branch_name("CAI", 42, "");
        assert_eq!(result, "agent/CAI-42");
    }

    #[test]
    fn test_generate_branch_name_with_slug_uses_slug() {
        // Dashes in slug are stripped (only alphanumeric + whitespace kept)
        let result = generate_branch_name_with_slug(
            "agent",
            "CAI",
            42,
            "Add user authentication",
            Some("user-auth"),
        );
        assert_eq!(result, "agent/CAI-42-userauth");
    }

    #[test]
    fn test_generate_branch_name_with_slug_empty_uses_title() {
        let result =
            generate_branch_name_with_slug("agent", "CAI", 42, "Add user authentication", Some(""));
        assert_eq!(result, "agent/CAI-42-add-user-authenticat");
    }

    #[test]
    fn test_generate_branch_name_with_slug_none_uses_title() {
        let result =
            generate_branch_name_with_slug("agent", "CAI", 42, "Add user authentication", None);
        assert_eq!(result, "agent/CAI-42-add-user-authenticat");
    }

    #[test]
    fn test_generate_branch_name_custom_prefix() {
        let result =
            generate_branch_name_with_slug("feature", "CAI", 42, "Add user authentication", None);
        assert_eq!(result, "feature/CAI-42-add-user-authenticat");
    }

    #[test]
    fn test_generate_branch_name_empty_prefix_defaults_to_agent() {
        let result = generate_branch_name_with_slug("", "CAI", 42, "Add user authentication", None);
        assert_eq!(result, "agent/CAI-42-add-user-authenticat");
    }

    #[test]
    fn test_sanitize_branch_slug_basic() {
        let result = sanitize_branch_slug("Add user authentication", 20);
        // "add-user-authentication" -> truncate to 20 -> "add-user-authenticat"
        assert_eq!(result, "add-user-authenticat");
    }

    #[test]
    fn test_sanitize_branch_slug_special_chars() {
        let result = sanitize_branch_slug("Fix bug #123: crash on startup!", 30);
        assert_eq!(result, "fix-bug-123-crash-on-startup");
    }

    #[test]
    fn test_sanitize_branch_slug_respects_max_len() {
        let result = sanitize_branch_slug("This is a very long title that should be truncated", 10);
        assert_eq!(result, "this-is-a");
    }

    #[test]
    fn test_sanitize_branch_slug_trims_trailing_dash() {
        let result = sanitize_branch_slug("Add feature-", 30);
        assert_eq!(result, "add-feature");
    }

    #[test]
    fn test_generate_branch_name_short_title() {
        assert_eq!(
            generate_branch_name("CAI", 1, "Fix bug"),
            "agent/CAI-1-fix-bug"
        );
    }

    #[test]
    fn test_generate_branch_name_special_chars() {
        assert_eq!(
            generate_branch_name("CAI", 5, "Fix: bug (urgent)!"),
            "agent/CAI-5-fix-bug-urgent"
        );
    }

    #[test]
    fn test_sanitize_branch_slug_combined() {
        assert_eq!(sanitize_branch_slug("Hello World", 20), "hello-world");
        assert_eq!(
            sanitize_branch_slug("This is a very long title that should be truncated", 20),
            "this-is-a-very-long"
        );
        assert_eq!(sanitize_branch_slug("Fix: bug!", 20), "fix-bug");
        assert_eq!(sanitize_branch_slug("", 20), "");
    }

    #[test]
    fn test_generate_branch_name_with_slug_provided_spaces() {
        // When slug is provided with spaces, sanitize converts to dashes
        assert_eq!(
            generate_branch_name_with_slug(
                "agent",
                "CAI",
                42,
                "Add user authentication",
                Some("user auth")
            ),
            "agent/CAI-42-user-auth"
        );
    }

    #[test]
    fn test_generate_branch_name_with_slug_longer_limit() {
        // Agent slugs get 30 char limit instead of 20
        assert_eq!(
            generate_branch_name_with_slug(
                "agent",
                "CAI",
                42,
                "Ignored title",
                Some("this is a very long branch slug name")
            ),
            "agent/CAI-42-this-is-a-very-long-branch-slu"
        );
    }

    #[test]
    fn test_generate_branch_name_with_prefix_trailing_slash() {
        // Trailing slash in prefix should be stripped
        assert_eq!(
            generate_branch_name_with_slug("feature/", "CAI", 42, "Fix bug", None),
            "feature/CAI-42-fix-bug"
        );
    }

    #[test]
    fn test_worktree_path_from_branch_strips_prefix() {
        // Various prefixes should be stripped
        let path1 = worktree_path_from_branch("agent/CAI-42-fix-bug").unwrap();
        let path2 = worktree_path_from_branch("feature/CAI-42-fix-bug").unwrap();
        let path3 = worktree_path_from_branch("CAI-42-fix-bug").unwrap();

        // All should end up at the same worktree directory name
        assert!(path1.ends_with("CAI-42-fix-bug"));
        assert!(path2.ends_with("CAI-42-fix-bug"));
        assert!(path3.ends_with("CAI-42-fix-bug"));
    }

    #[test]
    fn test_run_setup_commands_empty() {
        let mock = MockProcessSpawner::new();
        let result = run_setup_commands_with_process(&mock, Path::new("/tmp/test"), &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_setup_commands_single_success() {
        let mut mock = MockProcessSpawner::new();
        mock.expect_run()
            .withf(|config| {
                config.program == "sh"
                    && config.args == vec!["-c", "bun install"]
                    && config.cwd == Some("/tmp/worktree".to_string())
            })
            .times(1)
            .returning(|_| {
                Ok(CommandOutput {
                    success: true,
                    exit_code: Some(0),
                    stdout: "installed packages".to_string(),
                    stderr: String::new(),
                })
            });

        let result = run_setup_commands_with_process(
            &mock,
            Path::new("/tmp/worktree"),
            &["bun install".to_string()],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_setup_commands_multiple_success() {
        let mut mock = MockProcessSpawner::new();

        // Expect two calls in sequence
        mock.expect_run().times(2).returning(|_| {
            Ok(CommandOutput {
                success: true,
                exit_code: Some(0),
                stdout: String::new(),
                stderr: String::new(),
            })
        });

        let commands = vec!["bun install".to_string(), "cargo build".to_string()];
        let result = run_setup_commands_with_process(&mock, Path::new("/tmp/worktree"), &commands);
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_setup_commands_first_fails() {
        let mut mock = MockProcessSpawner::new();
        mock.expect_run().times(1).returning(|_| {
            Ok(CommandOutput {
                success: false,
                exit_code: Some(1),
                stdout: String::new(),
                stderr: "command not found".to_string(),
            })
        });

        let commands = vec!["bad-command".to_string(), "good-command".to_string()];
        let result = run_setup_commands_with_process(&mock, Path::new("/tmp/worktree"), &commands);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("bad-command"));
        assert!(err.contains("command not found"));
    }

    #[test]
    fn test_run_setup_commands_spawn_error() {
        let mut mock = MockProcessSpawner::new();
        mock.expect_run()
            .times(1)
            .returning(|_| Err("spawn failed".to_string()));

        let result = run_setup_commands_with_process(
            &mock,
            Path::new("/tmp/worktree"),
            &["echo hello".to_string()],
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("spawn failed"));
    }

    // Tests for parse_worktree_list_for_branch

    #[test]
    fn test_parse_worktree_list_finds_branch() {
        let output = r#"worktree /Users/dev/main-repo
HEAD abc123
branch refs/heads/main

worktree /Users/dev/.cairn/worktrees/feature-x
HEAD def456
branch refs/heads/agent/CAI-42-feature-x

worktree /Users/dev/.cairn/worktrees/other
HEAD ghi789
branch refs/heads/agent/CAI-99-other
"#;
        let result = parse_worktree_list_for_branch(output, "agent/CAI-42-feature-x");
        assert_eq!(
            result,
            Some(PathBuf::from("/Users/dev/.cairn/worktrees/feature-x"))
        );
    }

    #[test]
    fn test_parse_worktree_list_branch_not_found() {
        let output = r#"worktree /Users/dev/main-repo
HEAD abc123
branch refs/heads/main
"#;
        let result = parse_worktree_list_for_branch(output, "agent/CAI-42-feature-x");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_worktree_list_empty() {
        let result = parse_worktree_list_for_branch("", "agent/CAI-42-feature-x");
        assert_eq!(result, None);
    }

    // Tests for branch_exists_with_git

    #[test]
    fn test_branch_exists_true() {
        let mut mock = MockGitClient::new();
        mock.expect_branch_exists()
            .withf(|path, branch| path == Path::new("/repo") && branch == "feature-branch")
            .returning(|_, _| Ok(true));

        let result = branch_exists_with_git(&mock, Path::new("/repo"), "feature-branch");
        assert_eq!(result, Ok(true));
    }

    #[test]
    fn test_branch_exists_false() {
        let mut mock = MockGitClient::new();
        mock.expect_branch_exists().returning(|_, _| Ok(false));

        let result = branch_exists_with_git(&mock, Path::new("/repo"), "nonexistent");
        assert_eq!(result, Ok(false));
    }

    // Tests for get_worktree_for_branch_with_git

    #[test]
    fn test_get_worktree_for_branch_found() {
        let mut mock = MockGitClient::new();
        mock.expect_worktree_list().returning(|_| {
            Ok(r#"worktree /path/to/worktree
HEAD abc123
branch refs/heads/agent/CAI-42-test
"#
            .to_string())
        });

        let result =
            get_worktree_for_branch_with_git(&mock, Path::new("/repo"), "agent/CAI-42-test");
        assert_eq!(result, Ok(Some(PathBuf::from("/path/to/worktree"))));
    }

    #[test]
    fn test_get_worktree_for_branch_not_found() {
        let mut mock = MockGitClient::new();
        mock.expect_worktree_list()
            .returning(|_| Ok("worktree /main\nHEAD abc\nbranch refs/heads/main\n".to_string()));

        let result =
            get_worktree_for_branch_with_git(&mock, Path::new("/repo"), "agent/CAI-42-test");
        assert_eq!(result, Ok(None));
    }

    // Tests for create_worktree_with_services

    #[test]
    fn test_create_worktree_new_branch() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        // Parent dir creation succeeds
        fs.expect_create_dir_all().returning(|_| Ok(()));
        // Worktree path doesn't exist
        fs.expect_exists().returning(|_| false);

        // Branch doesn't exist
        git.expect_branch_exists().returning(|_, _| Ok(false));
        // Create worktree with new branch
        git.expect_worktree_add_new_branch()
            .withf(|repo, wt, branch, base| {
                repo == Path::new("/repo")
                    && wt == Path::new("/worktrees/CAI-42")
                    && branch == "agent/CAI-42-test"
                    && base == "main"
            })
            .returning(|_, _, _, _| Ok(()));

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            "agent/CAI-42-test",
            "main",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_existing_branch_not_checked_out() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // Branch exists
        git.expect_branch_exists().returning(|_, _| Ok(true));
        // Not checked out in any worktree
        git.expect_worktree_list()
            .returning(|_| Ok("worktree /main\nHEAD abc\nbranch refs/heads/main\n".to_string()));
        // Create worktree with existing branch
        git.expect_worktree_add_existing_branch()
            .returning(|_, _, _| Ok(()));

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            "agent/CAI-42-test",
            "main",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_branch_already_checked_out() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // Branch exists
        git.expect_branch_exists().returning(|_, _| Ok(true));
        // Already checked out elsewhere
        git.expect_worktree_list().returning(|_| {
            Ok(
                "worktree /other/location\nHEAD abc\nbranch refs/heads/agent/CAI-42-test\n"
                    .to_string(),
            )
        });

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            "agent/CAI-42-test",
            "main",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("already checked out"));
        assert!(err.contains("/other/location"));
    }

    #[test]
    fn test_create_worktree_path_exists() {
        let git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        // Worktree path already exists
        fs.expect_exists().returning(|_| true);

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            "agent/CAI-42-test",
            "main",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_create_worktree_recovers_from_stale_entry_new_branch() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // Branch doesn't exist
        git.expect_branch_exists().returning(|_, _| Ok(false));

        // First attempt fails with stale worktree error
        git.expect_worktree_add_new_branch()
            .times(1)
            .returning(|_, _, _, _| {
                Err("fatal: '/Users/mitch/.cairn/worktrees/CAI-3-planner-0' is a missing but already registered worktree".to_string())
            });

        // Prune is called
        git.expect_worktree_prune().times(1).returning(|_| Ok(()));

        // Second attempt succeeds
        git.expect_worktree_add_new_branch()
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            "agent/CAI-42-test",
            "main",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_recovers_from_stale_entry_existing_branch() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // Branch exists
        git.expect_branch_exists().returning(|_, _| Ok(true));
        // Not checked out in any worktree
        git.expect_worktree_list()
            .returning(|_| Ok("worktree /main\nHEAD abc\nbranch refs/heads/main\n".to_string()));

        // First attempt fails with stale worktree error
        git.expect_worktree_add_existing_branch()
            .times(1)
            .returning(|_, _, _| {
                Err(
                    "fatal: '/path/to/worktree' is a missing but already registered worktree"
                        .to_string(),
                )
            });

        // Prune is called
        git.expect_worktree_prune().times(1).returning(|_| Ok(()));

        // Second attempt succeeds
        git.expect_worktree_add_existing_branch()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            "agent/CAI-42-test",
            "main",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_fails_after_recovery_attempt() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // Branch doesn't exist
        git.expect_branch_exists().returning(|_, _| Ok(false));

        // First attempt fails with stale worktree error
        git.expect_worktree_add_new_branch()
            .times(1)
            .returning(|_, _, _, _| {
                Err(
                    "fatal: '/path/to/worktree' is a missing but already registered worktree"
                        .to_string(),
                )
            });

        // Prune is called
        git.expect_worktree_prune().times(1).returning(|_| Ok(()));

        // Second attempt still fails (different error)
        git.expect_worktree_add_new_branch()
            .times(1)
            .returning(|_, _, _, _| Err("some other error".to_string()));

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            "agent/CAI-42-test",
            "main",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("some other error"));
    }

    // Tests for remove_worktree_with_services

    #[test]
    fn test_remove_worktree_success() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_exists().returning(|_| true);
        git.expect_worktree_remove().returning(|_, _, _| Ok(()));

        let result = remove_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_worktree_not_exists() {
        let git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        // Path doesn't exist - should succeed silently
        fs.expect_exists().returning(|_| false);

        let result = remove_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_worktree_force() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_exists().returning(|_| true);
        git.expect_worktree_remove()
            .withf(|_, _, force| *force)
            .returning(|_, _, _| Ok(()));

        let result = remove_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            true,
        );
        assert!(result.is_ok());
    }

    // Tests for copy_files_to_worktree_with_services

    #[test]
    fn test_copy_files_empty_list() {
        let fs = MockFileSystem::new();

        let result = copy_files_to_worktree_with_services(
            &fs,
            Path::new("/repo"),
            Path::new("/worktree"),
            &[],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_copy_files_copies_existing_files() {
        let mut fs = MockFileSystem::new();

        // First file exists
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env"))
            .returning(|_| true);
        fs.expect_copy_file()
            .withf(|src, dst| src == Path::new("/repo/.env") && dst == Path::new("/worktree/.env"))
            .returning(|_, _| Ok(()));

        // Second file exists
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/config/local.yaml"))
            .returning(|_| true);
        fs.expect_copy_file()
            .withf(|src, dst| {
                src == Path::new("/repo/config/local.yaml")
                    && dst == Path::new("/worktree/config/local.yaml")
            })
            .returning(|_, _| Ok(()));

        let result = copy_files_to_worktree_with_services(
            &fs,
            Path::new("/repo"),
            Path::new("/worktree"),
            &[".env".to_string(), "config/local.yaml".to_string()],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_copy_files_skips_missing_files() {
        let mut fs = MockFileSystem::new();

        // First file doesn't exist - should be skipped
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env"))
            .returning(|_| false);

        // Second file exists and should be copied
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/config.yaml"))
            .returning(|_| true);
        fs.expect_copy_file()
            .withf(|src, dst| {
                src == Path::new("/repo/config.yaml") && dst == Path::new("/worktree/config.yaml")
            })
            .returning(|_, _| Ok(()));

        let result = copy_files_to_worktree_with_services(
            &fs,
            Path::new("/repo"),
            Path::new("/worktree"),
            &[".env".to_string(), "config.yaml".to_string()],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_copy_files_propagates_copy_error() {
        let mut fs = MockFileSystem::new();

        fs.expect_exists().returning(|_| true);
        fs.expect_copy_file()
            .returning(|_, _| Err("Permission denied".to_string()));

        let result = copy_files_to_worktree_with_services(
            &fs,
            Path::new("/repo"),
            Path::new("/worktree"),
            &[".env".to_string()],
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Permission denied"));
    }
}
