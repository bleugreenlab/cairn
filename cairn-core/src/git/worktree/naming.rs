use super::*;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_worktree_list_main_repo_entry_resolves_to_repo_path() {
        // `git worktree list --porcelain` lists the main working tree first.
        // The post-merge fast-forward relies on this: a merge into the default
        // branch resolves to the main repo path and is then skipped (the
        // main-repo pull owns it), while an integration-branch worktree resolves
        // to its own distinct path.
        let output = "\
worktree /repo
HEAD abc123
branch refs/heads/main

worktree /home/user/.cairn/worktrees/CAI-42
HEAD def456
branch refs/heads/agent/CAI-42-coordinator-0
";
        assert_eq!(
            parse_worktree_list_for_branch(output, "main"),
            Some(PathBuf::from("/repo"))
        );
        assert_eq!(
            parse_worktree_list_for_branch(output, "agent/CAI-42-coordinator-0"),
            Some(PathBuf::from("/home/user/.cairn/worktrees/CAI-42"))
        );
        assert_eq!(parse_worktree_list_for_branch(output, "nonexistent"), None);
    }
}
