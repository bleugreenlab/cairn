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

/// Attempt to create a worktree tracking a remote branch, with automatic recovery if stale entry is detected.
fn try_add_worktree_tracking(
    git: &dyn GitClient,
    repo_path: &Path,
    worktree_path: &Path,
    branch_name: &str,
) -> Result<(), String> {
    let result = git.worktree_add_tracking_branch(repo_path, worktree_path, branch_name);

    match result {
        Ok(()) => Ok(()),
        Err(e) if is_stale_worktree_error(&e) => {
            log::warn!(
                "Detected stale worktree entry, attempting to prune and retry: {}",
                e
            );
            git.worktree_prune(repo_path)?;
            log::info!("Pruned stale worktree entries, retrying creation");
            git.worktree_add_tracking_branch(repo_path, worktree_path, branch_name)
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
    } else if git.remote_branch_exists(repo_path, branch_name)? {
        // No local branch, but exists on remote - create worktree tracking remote
        log::info!(
            "Branch {} exists on remote, creating worktree with tracking branch",
            branch_name
        );
        try_add_worktree_tracking(git, repo_path, worktree_path, branch_name)?;
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

/// Result of seeding a worktree with gitignored content.
#[derive(Debug, Default)]
pub struct SeedResult {
    pub seeded: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Seed a worktree with all gitignored content from the main repo.
///
/// **Directories** are symlinked (instant, zero disk, shared with main repo).
/// **Files** are copied (tiny .env-style files, instant).
///
/// Best-effort: individual failures are logged as warnings and don't block.
pub fn seed_worktree_ignored(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    repo_path: &Path,
    worktree_path: &Path,
) -> Result<SeedResult, String> {
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
        log::info!("No gitignored content to seed");
        return Ok(SeedResult::default());
    }

    log::info!(
        "Seeding worktree with {} gitignored entries from {}",
        entries.len(),
        repo_path.display()
    );

    let mut result = SeedResult::default();
    let mut seeded_paths: Vec<String> = Vec::new();

    for entry in entries {
        // Filter out .cairn/ directory
        if entry == ".cairn/" || entry.starts_with(".cairn/") {
            result.skipped += 1;
            continue;
        }

        let is_dir = entry.ends_with('/');
        let entry_path = entry.trim_end_matches('/');

        let src = repo_path.join(entry_path);
        let dst = worktree_path.join(entry_path);

        // Skip if source doesn't exist
        if !fs.exists(&src) {
            result.skipped += 1;
            continue;
        }

        // Skip if destination already exists (or is already a symlink)
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

        if is_dir {
            // Directories: symlink to main repo's copy (instant, shared)
            match fs.symlink(&src, &dst) {
                Ok(()) => {
                    result.seeded += 1;
                    seeded_paths.push(entry_path.to_string());
                }
                Err(e) => {
                    log::warn!("Symlink failed for {}: {}", entry, e);
                    result.failed += 1;
                }
            }
        } else {
            // Files: copy (small .env-style files)
            match fs.copy_file(&src, &dst) {
                Ok(()) => {
                    result.seeded += 1;
                    seeded_paths.push(entry_path.to_string());
                }
                Err(e) => {
                    log::warn!("Copy failed for {}: {}", entry, e);
                    result.failed += 1;
                }
            }
        }
    }

    // Write seeded paths to the worktree's local git exclude so they can
    // never be committed — .gitignore patterns with trailing slashes don't
    // match symlinks, and the worktree branch may have an older .gitignore.
    if !seeded_paths.is_empty() {
        if let Err(e) = write_seeded_excludes(git, fs, worktree_path, &seeded_paths) {
            log::warn!("Failed to write git excludes for seeded paths: {}", e);
        }
    }

    Ok(result)
}

/// Append seeded paths to the worktree's local git exclude file.
///
/// Uses `git rev-parse --git-dir` from the worktree to locate the correct
/// exclude file (works for both main repos and linked worktrees).
fn write_seeded_excludes(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    worktree_path: &Path,
    paths: &[String],
) -> Result<(), String> {
    let git_dir = git
        .rev_parse(worktree_path, vec!["--git-dir".to_string()])?
        .trim()
        .to_string();

    let git_dir_path = if Path::new(&git_dir).is_absolute() {
        PathBuf::from(&git_dir)
    } else {
        worktree_path.join(&git_dir)
    };

    let exclude_path = git_dir_path.join("info").join("exclude");

    // Ensure info/ directory exists
    if let Some(parent) = exclude_path.parent() {
        fs.create_dir_all(parent)?;
    }

    // Read existing content, append our entries under a marker
    let existing = fs.read_to_string(&exclude_path).unwrap_or_default();

    let marker = "# seeded by cairn (do not edit)";
    if existing.contains(marker) {
        // Already written — skip to avoid duplicates
        return Ok(());
    }

    let mut new_content = existing;
    if !new_content.is_empty() && !new_content.ends_with('\n') {
        new_content.push('\n');
    }
    new_content.push_str(marker);
    new_content.push('\n');
    for p in paths {
        new_content.push_str(p);
        new_content.push('\n');
    }

    fs.write_str(&exclude_path, &new_content)?;

    log::info!(
        "Wrote {} seeded paths to {}",
        paths.len(),
        exclude_path.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::{MockFileSystem, MockGitClient, MockProcessSpawner};
    use crate::services::{CommandOutput, GitOutput};

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

        // Branch doesn't exist locally or remotely
        git.expect_branch_exists().returning(|_, _| Ok(false));
        git.expect_remote_branch_exists()
            .returning(|_, _| Ok(false));
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

        // Branch doesn't exist locally or remotely
        git.expect_branch_exists().returning(|_, _| Ok(false));
        git.expect_remote_branch_exists()
            .returning(|_, _| Ok(false));

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

        // Branch doesn't exist locally or remotely
        git.expect_branch_exists().returning(|_, _| Ok(false));
        git.expect_remote_branch_exists()
            .returning(|_, _| Ok(false));

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

    #[test]
    fn test_create_worktree_tracks_remote_branch() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // No local branch
        git.expect_branch_exists().returning(|_, _| Ok(false));
        // But exists on remote
        git.expect_remote_branch_exists().returning(|_, _| Ok(true));
        // Should create tracking worktree
        git.expect_worktree_add_tracking_branch()
            .withf(|repo, wt, branch| {
                repo == Path::new("/repo")
                    && wt == Path::new("/worktrees/feature-x")
                    && branch == "feature/webview"
            })
            .returning(|_, _, _| Ok(()));

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/feature-x"),
            "feature/webview",
            "main",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_no_local_no_remote_creates_new() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // No local branch
        git.expect_branch_exists().returning(|_, _| Ok(false));
        // No remote branch either
        git.expect_remote_branch_exists()
            .returning(|_, _| Ok(false));
        // Should create new branch from base
        git.expect_worktree_add_new_branch()
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
    fn test_create_worktree_tracking_recovers_from_stale_entry() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // No local branch
        git.expect_branch_exists().returning(|_, _| Ok(false));
        // Exists on remote
        git.expect_remote_branch_exists().returning(|_, _| Ok(true));

        // First tracking attempt fails with stale worktree error
        git.expect_worktree_add_tracking_branch()
            .times(1)
            .returning(|_, _, _| {
                Err(
                    "fatal: '/worktrees/feature-x' is a missing but already registered worktree"
                        .to_string(),
                )
            });

        // Prune is called
        git.expect_worktree_prune().times(1).returning(|_| Ok(()));

        // Retry succeeds
        git.expect_worktree_add_tracking_branch()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/feature-x"),
            "feature/webview",
            "main",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_tracking_non_stale_error_passes_through() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(|_| false);

        // No local branch
        git.expect_branch_exists().returning(|_, _| Ok(false));
        // Exists on remote
        git.expect_remote_branch_exists().returning(|_, _| Ok(true));

        // Tracking attempt fails with a non-stale error
        git.expect_worktree_add_tracking_branch()
            .returning(|_, _, _| {
                Err("git worktree add --track failed: permission denied".to_string())
            });

        let result = create_worktree_with_services(
            &git,
            &fs,
            Path::new("/repo"),
            Path::new("/worktrees/feature-x"),
            "feature/webview",
            "main",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("permission denied"));
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

    // Tests for seed_worktree_ignored

    fn git_ls_files_output(entries: &[&str]) -> GitOutput {
        GitOutput {
            success: true,
            stdout: entries.join("\n"),
            stderr: String::new(),
        }
    }

    #[test]
    fn test_seed_worktree_no_gitignored_content() {
        let mut git = MockGitClient::new();
        let fs = MockFileSystem::new();

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[])));

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.seeded, 0);
        assert_eq!(r.skipped, 0);
        assert_eq!(r.failed, 0);
    }

    #[test]
    fn test_seed_worktree_excludes_cairn_directory() {
        let mut git = MockGitClient::new();
        let fs = MockFileSystem::new();

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".cairn/", ".cairn/config.yaml"])));

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.seeded, 0);
        assert_eq!(r.skipped, 2);
    }

    #[test]
    fn test_seed_worktree_symlinks_dirs_copies_files() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".env", "node_modules/"])));

        // .env: source exists, dest doesn't exist, not a symlink
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env"))
            .returning(|_| true);
        fs.expect_exists()
            .withf(|p| p == Path::new("/worktree/.env"))
            .returning(|_| false);
        fs.expect_is_symlink()
            .withf(|p| p == Path::new("/worktree/.env"))
            .returning(|_| false);

        // node_modules: source exists, dest doesn't exist, not a symlink
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/node_modules"))
            .returning(|_| true);
        fs.expect_exists()
            .withf(|p| p == Path::new("/worktree/node_modules"))
            .returning(|_| false);
        fs.expect_is_symlink()
            .withf(|p| p == Path::new("/worktree/node_modules"))
            .returning(|_| false);

        // Parent dir creation
        fs.expect_create_dir_all().returning(|_| Ok(()));

        // .env should be copied (file)
        fs.expect_copy_file()
            .withf(|src, dst| src == Path::new("/repo/.env") && dst == Path::new("/worktree/.env"))
            .times(1)
            .returning(|_, _| Ok(()));

        // node_modules should be symlinked (directory)
        fs.expect_symlink()
            .withf(|target, link| {
                target == Path::new("/repo/node_modules")
                    && link == Path::new("/worktree/node_modules")
            })
            .times(1)
            .returning(|_, _| Ok(()));

        // Exclude file writing: rev_parse returns git dir, write_str records it
        git.expect_rev_parse()
            .withf(|repo, args| repo == Path::new("/worktree") && args == &["--git-dir"])
            .returning(|_, _| Ok("/repo/.git/worktrees/worktree\n".to_string()));
        fs.expect_read_to_string().returning(|_| Ok(String::new()));
        fs.expect_write_str()
            .withf(|path, contents| {
                path == Path::new("/repo/.git/worktrees/worktree/info/exclude")
                    && contents.contains("# seeded by cairn")
                    && contents.contains(".env")
                    && contents.contains("node_modules")
            })
            .times(1)
            .returning(|_, _| Ok(()));

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.seeded, 2);
        assert_eq!(r.skipped, 0);
        assert_eq!(r.failed, 0);
    }

    #[test]
    fn test_seed_worktree_skips_existing_destinations() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".env"])));

        // Source exists, destination already exists
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env"))
            .returning(|_| true);
        fs.expect_exists()
            .withf(|p| p == Path::new("/worktree/.env"))
            .returning(|_| true);

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.seeded, 0);
        assert_eq!(r.skipped, 1);
    }

    #[test]
    fn test_seed_worktree_skips_existing_symlinks() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&["node_modules/"])));

        // Source exists, destination doesn't exist but IS a symlink (dangling)
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/node_modules"))
            .returning(|_| true);
        fs.expect_exists()
            .withf(|p| p == Path::new("/worktree/node_modules"))
            .returning(|_| false);
        fs.expect_is_symlink()
            .withf(|p| p == Path::new("/worktree/node_modules"))
            .returning(|_| true);

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.seeded, 0);
        assert_eq!(r.skipped, 1);
    }

    #[test]
    fn test_seed_worktree_best_effort_on_failure() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".env", ".env.local"])));

        // Both sources exist, neither dest exists or is symlink
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env"))
            .returning(|_| true);
        fs.expect_exists()
            .withf(|p| p == Path::new("/worktree/.env"))
            .returning(|_| false);
        fs.expect_is_symlink()
            .withf(|p| p == Path::new("/worktree/.env"))
            .returning(|_| false);
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env.local"))
            .returning(|_| true);
        fs.expect_exists()
            .withf(|p| p == Path::new("/worktree/.env.local"))
            .returning(|_| false);
        fs.expect_is_symlink()
            .withf(|p| p == Path::new("/worktree/.env.local"))
            .returning(|_| false);

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

        // Exclude file writing for the one successfully seeded path
        git.expect_rev_parse()
            .returning(|_, _| Ok("/repo/.git/worktrees/worktree\n".to_string()));
        fs.expect_read_to_string().returning(|_| Ok(String::new()));
        fs.expect_write_str().returning(|_, _| Ok(()));

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.seeded, 1);
        assert_eq!(r.failed, 1);
    }

    #[test]
    fn test_seed_worktree_git_failure_is_fatal() {
        let mut git = MockGitClient::new();
        let fs = MockFileSystem::new();

        git.expect_run()
            .returning(|_, _| Err("git not found".to_string()));

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_err());
    }

    #[test]
    fn test_seed_worktree_git_ls_files_non_zero_exit() {
        let mut git = MockGitClient::new();
        let fs = MockFileSystem::new();

        git.expect_run().returning(|_, _| {
            Ok(GitOutput {
                success: false,
                stdout: String::new(),
                stderr: "fatal: not a git repository".to_string(),
            })
        });

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not a git repository"));
    }

    #[test]
    fn test_seed_worktree_skips_missing_sources() {
        let mut git = MockGitClient::new();
        let mut fs = MockFileSystem::new();

        git.expect_run()
            .returning(|_, _| Ok(git_ls_files_output(&[".env"])));

        // Source doesn't exist
        fs.expect_exists()
            .withf(|p| p == Path::new("/repo/.env"))
            .returning(|_| false);

        let result = seed_worktree_ignored(&git, &fs, Path::new("/repo"), Path::new("/worktree"));
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.seeded, 0);
        assert_eq!(r.skipped, 1);
    }
}
