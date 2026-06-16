use super::*;

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

#[cfg(test)]
mod tests {
    use super::*;
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
