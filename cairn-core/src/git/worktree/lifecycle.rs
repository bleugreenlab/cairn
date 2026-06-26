use super::*;

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
        // Create new branch and worktree from the first existing base ref.
        let start_point = resolve_worktree_start_point(git, repo_path, base_branch)?;
        log::info!(
            "Creating new branch {} from {} with worktree",
            branch_name,
            start_point
        );
        try_add_worktree(
            git,
            repo_path,
            worktree_path,
            branch_name,
            Some(&start_point),
        )?;
    }

    log::info!("Created worktree at {}", worktree_path.display());
    Ok(())
}

fn resolve_worktree_start_point(
    git: &dyn GitClient,
    repo_path: &Path,
    base_branch: &str,
) -> Result<String, String> {
    ensure_head_commit_for_worktree(git, repo_path)?;

    if git.branch_exists(repo_path, base_branch)? {
        return Ok(base_branch.to_string());
    }

    if git.remote_branch_exists(repo_path, base_branch)? {
        return Ok(format!("origin/{base_branch}"));
    }

    let current = git.current_branch(repo_path)?.trim().to_string();
    if current.is_empty() || current == "HEAD" {
        return Err(format!(
            "No usable base branch found: '{}' does not exist locally or on origin, and current branch is '{}'. Ensure the repository has at least one commit and a checked-out branch before starting work.",
            base_branch,
            if current.is_empty() { "<empty>" } else { current.as_str() }
        ));
    }

    Ok(current)
}

fn ensure_head_commit_for_worktree(git: &dyn GitClient, repo_path: &Path) -> Result<(), String> {
    if git
        .rev_parse(repo_path, vec!["--verify".to_string(), "HEAD".to_string()])
        .is_ok()
    {
        return Ok(());
    }

    let output = git.run(
        repo_path,
        vec![
            "-c".to_string(),
            "user.name=Cairn".to_string(),
            "-c".to_string(),
            "user.email=cairn@local.invalid".to_string(),
            "commit".to_string(),
            "--allow-empty".to_string(),
            "-m".to_string(),
            "Initial commit".to_string(),
        ],
    )?;

    if output.success {
        log::info!(
            "Created initial commit in unborn repository {} before creating worktree",
            repo_path.display()
        );
        Ok(())
    } else {
        Err(format!(
            "Cannot create a worktree because repository {} has no commits and Cairn could not create an initial commit: {}. Create an initial commit, then start work again.",
            repo_path.display(),
            output.stderr
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::{MockFileSystem, MockGitClient};
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

    fn create_worktree_fs(path_exists: bool) -> MockFileSystem {
        let mut fs = MockFileSystem::new();
        fs.expect_create_dir_all().returning(|_| Ok(()));
        fs.expect_exists().returning(move |_| path_exists);
        fs
    }

    fn create_worktree_test(path_exists: bool) -> (MockGitClient, MockFileSystem) {
        (MockGitClient::new(), create_worktree_fs(path_exists))
    }

    fn expect_local_branch(git: &mut MockGitClient, exists: bool) {
        git.expect_branch_exists().returning(move |_, _| Ok(exists));
    }

    fn expect_remote_branch(git: &mut MockGitClient, exists: bool) {
        git.expect_remote_branch_exists()
            .returning(move |_, _| Ok(exists));
    }

    fn expect_no_local_or_remote_branch(git: &mut MockGitClient) {
        expect_local_branch(git, false);
        expect_remote_branch(git, false);
    }

    fn expect_remote_only_branch(git: &mut MockGitClient) {
        expect_local_branch(git, false);
        expect_remote_branch(git, true);
    }

    fn expect_existing_branch_not_checked_out(git: &mut MockGitClient) {
        expect_local_branch(git, true);
        git.expect_worktree_list()
            .returning(|_| Ok("worktree /main\nHEAD abc\nbranch refs/heads/main\n".to_string()));
    }

    fn expect_head_commit(git: &mut MockGitClient) {
        git.expect_rev_parse()
            .returning(|_, _| Ok("abc123".to_string()));
    }

    fn expect_stale_prune(git: &mut MockGitClient) {
        git.expect_worktree_prune().times(1).returning(|_| Ok(()));
    }

    fn stale_worktree_error(path: &str) -> String {
        format!("fatal: '{path}' is a missing but already registered worktree")
    }

    fn create_standard_worktree(git: &MockGitClient, fs: &MockFileSystem) -> Result<(), String> {
        create_worktree_with_services(
            git,
            fs,
            Path::new("/repo"),
            Path::new("/worktrees/CAI-42"),
            "agent/CAI-42-test",
            "main",
        )
    }

    fn create_feature_worktree(git: &MockGitClient, fs: &MockFileSystem) -> Result<(), String> {
        create_worktree_with_services(
            git,
            fs,
            Path::new("/repo"),
            Path::new("/worktrees/feature-x"),
            "feature/webview",
            "main",
        )
    }

    #[test]
    fn test_create_worktree_new_branch() {
        let (mut git, fs) = create_worktree_test(false);

        // Target branch is absent; base branch exists locally.
        git.expect_branch_exists()
            .returning(|_, branch| Ok(branch == "main"));
        git.expect_remote_branch_exists()
            .returning(|_, _| Ok(false));
        expect_head_commit(&mut git);
        // Create worktree with new branch
        git.expect_worktree_add_new_branch()
            .withf(|repo, wt, branch, base| {
                repo == Path::new("/repo")
                    && wt == Path::new("/worktrees/CAI-42")
                    && branch == "agent/CAI-42-test"
                    && base == "main"
            })
            .returning(|_, _, _, _| Ok(()));

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_existing_branch_not_checked_out() {
        let (mut git, fs) = create_worktree_test(false);

        // Branch exists
        expect_existing_branch_not_checked_out(&mut git);
        // Create worktree with existing branch
        git.expect_worktree_add_existing_branch()
            .returning(|_, _, _| Ok(()));

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_branch_already_checked_out() {
        let (mut git, fs) = create_worktree_test(false);

        // Branch exists
        expect_local_branch(&mut git, true);
        // Already checked out elsewhere
        git.expect_worktree_list().returning(|_| {
            Ok(
                "worktree /other/location\nHEAD abc\nbranch refs/heads/agent/CAI-42-test\n"
                    .to_string(),
            )
        });

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("already checked out"));
        assert!(err.contains("/other/location"));
    }

    #[test]
    fn test_create_worktree_path_exists() {
        let (git, fs) = create_worktree_test(true);

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_create_worktree_recovers_from_stale_entry_new_branch() {
        let (mut git, fs) = create_worktree_test(false);

        // Target branch is absent; base branch exists locally.
        git.expect_branch_exists()
            .returning(|_, branch| Ok(branch == "main"));
        git.expect_remote_branch_exists()
            .returning(|_, _| Ok(false));
        expect_head_commit(&mut git);

        // First attempt fails with stale worktree error
        git.expect_worktree_add_new_branch()
            .times(1)
            .returning(|_, _, _, _| {
                Err(stale_worktree_error(
                    "/Users/mitch/.cairn/worktrees/CAI-3-planner-0",
                ))
            });

        // Prune is called
        expect_stale_prune(&mut git);

        // Second attempt succeeds
        git.expect_worktree_add_new_branch()
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_recovers_from_stale_entry_existing_branch() {
        let (mut git, fs) = create_worktree_test(false);

        // Branch exists
        expect_existing_branch_not_checked_out(&mut git);

        // First attempt fails with stale worktree error
        git.expect_worktree_add_existing_branch()
            .times(1)
            .returning(|_, _, _| Err(stale_worktree_error("/path/to/worktree")));

        // Prune is called
        expect_stale_prune(&mut git);

        // Second attempt succeeds
        git.expect_worktree_add_existing_branch()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_fails_after_recovery_attempt() {
        let (mut git, fs) = create_worktree_test(false);

        // Target branch is absent; base branch exists locally.
        git.expect_branch_exists()
            .returning(|_, branch| Ok(branch == "main"));
        git.expect_remote_branch_exists()
            .returning(|_, _| Ok(false));
        expect_head_commit(&mut git);

        // First attempt fails with stale worktree error
        git.expect_worktree_add_new_branch()
            .times(1)
            .returning(|_, _, _, _| Err(stale_worktree_error("/path/to/worktree")));

        // Prune is called
        expect_stale_prune(&mut git);

        // Second attempt still fails (different error)
        git.expect_worktree_add_new_branch()
            .times(1)
            .returning(|_, _, _, _| Err("some other error".to_string()));

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("some other error"));
    }

    #[test]
    fn test_create_worktree_tracks_remote_branch() {
        let (mut git, fs) = create_worktree_test(false);

        // No local branch
        expect_remote_only_branch(&mut git);
        // Should create tracking worktree
        git.expect_worktree_add_tracking_branch()
            .withf(|repo, wt, branch| {
                repo == Path::new("/repo")
                    && wt == Path::new("/worktrees/feature-x")
                    && branch == "feature/webview"
            })
            .returning(|_, _, _| Ok(()));

        let result = create_feature_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_no_local_no_remote_creates_new() {
        let (mut git, fs) = create_worktree_test(false);

        // No local or remote base branch: fall back to the current branch.
        expect_no_local_or_remote_branch(&mut git);
        expect_head_commit(&mut git);
        git.expect_current_branch()
            .returning(|_| Ok("trunk".to_string()));
        // New branch is created from the current-branch fallback.
        git.expect_worktree_add_new_branch()
            .withf(|_, _, _, base| base == "trunk")
            .returning(|_, _, _, _| Ok(()));

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_new_branch_uses_remote_base_when_local_missing() {
        let (mut git, fs) = create_worktree_test(false);

        git.expect_branch_exists().returning(|_, _| Ok(false));
        git.expect_remote_branch_exists()
            .returning(|_, branch| Ok(branch == "main"));
        expect_head_commit(&mut git);
        git.expect_worktree_add_new_branch()
            .withf(|_, _, _, base| base == "origin/main")
            .returning(|_, _, _, _| Ok(()));

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_unborn_head_creates_initial_commit_before_branching() {
        let (mut git, fs) = create_worktree_test(false);

        git.expect_branch_exists()
            .returning(|_, branch| Ok(branch == "main"));
        git.expect_remote_branch_exists()
            .returning(|_, _| Ok(false));
        git.expect_rev_parse()
            .returning(|_, _| Err("fatal: Needed a single revision".to_string()));
        git.expect_run()
            .withf(|repo, args| {
                repo == Path::new("/repo")
                    && args.iter().any(|arg| arg == "--allow-empty")
                    && args.iter().any(|arg| arg == "Initial commit")
            })
            .returning(|_, _| {
                Ok(crate::services::GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });
        git.expect_worktree_add_new_branch()
            .withf(|_, _, _, base| base == "main")
            .returning(|_, _, _, _| Ok(()));

        let result = create_standard_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_tracking_recovers_from_stale_entry() {
        let (mut git, fs) = create_worktree_test(false);

        // No local branch
        expect_remote_only_branch(&mut git);

        // First tracking attempt fails with stale worktree error
        git.expect_worktree_add_tracking_branch()
            .times(1)
            .returning(|_, _, _| Err(stale_worktree_error("/worktrees/feature-x")));

        // Prune is called
        expect_stale_prune(&mut git);

        // Retry succeeds
        git.expect_worktree_add_tracking_branch()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let result = create_feature_worktree(&git, &fs);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_worktree_tracking_non_stale_error_passes_through() {
        let (mut git, fs) = create_worktree_test(false);

        // No local branch
        expect_remote_only_branch(&mut git);

        // Tracking attempt fails with a non-stale error
        git.expect_worktree_add_tracking_branch()
            .returning(|_, _, _| {
                Err("git worktree add --track failed: permission denied".to_string())
            });

        let result = create_feature_worktree(&git, &fs);
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
}
