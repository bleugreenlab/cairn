//! Git helper functions for MCP handlers.

use std::path::{Path, PathBuf};

/// Result from a git commit operation
#[derive(Debug)]
pub struct CommitResult {
    pub sha: String,
    pub pr_number: Option<i32>,
}

/// Check if a PR exists for the current branch. Returns PR number if found.
pub fn get_pr_for_branch(worktree_path: &Path) -> Option<i32> {
    let output = crate::env::gh()
        .args(["pr", "view", "--json", "number"])
        .current_dir(worktree_path)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    json.get("number")?.as_i64().map(|n| n as i32)
}

/// Validate that a file path stays within the worktree (no path traversal).
pub fn validate_file_path(worktree_path: &Path, file_path: &str) -> Result<PathBuf, String> {
    // Join to get the target path
    let full_path = worktree_path.join(file_path);

    let worktree_canonical = worktree_path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve worktree path: {}", e))?;

    if full_path.exists() {
        // For existing files, canonicalize and verify within worktree
        let canonical = full_path
            .canonicalize()
            .map_err(|e| format!("Failed to resolve path: {}", e))?;

        if !canonical.starts_with(&worktree_canonical) {
            return Err("Path escapes worktree".to_string());
        }

        Ok(canonical)
    } else {
        // For new files, check that parent exists and is within worktree
        let parent = full_path
            .parent()
            .ok_or_else(|| "Invalid file path: no parent directory".to_string())?;

        // Create parent dirs if needed
        if !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directories: {}", e))?;
        }

        let canonical_parent = parent
            .canonicalize()
            .map_err(|e| format!("Failed to resolve parent path: {}", e))?;

        if !canonical_parent.starts_with(&worktree_canonical) {
            return Err("Path escapes worktree".to_string());
        }

        // Return the full path (not canonicalized since file doesn't exist yet)
        Ok(full_path)
    }
}

/// Validate file path for read operations.
/// Allows absolute paths anywhere on the filesystem.
/// Blocks relative path traversal attacks (../.., etc.)
pub fn validate_read_path(worktree_path: &Path, file_path: &str) -> Result<PathBuf, String> {
    let path = Path::new(file_path);

    // If it's an absolute path, allow it directly
    if path.is_absolute() {
        if !path.exists() {
            return Err(format!("Path does not exist: {}", file_path));
        }
        let canonical = path
            .canonicalize()
            .map_err(|e| format!("Failed to resolve path: {}", e))?;
        return Ok(canonical);
    }

    // For relative paths, treat as relative to worktree
    // Check for path traversal attempts (../ components)
    if file_path.contains("..") {
        return Err("Path traversal not allowed for relative paths".to_string());
    }

    let full_path = worktree_path.join(file_path);

    if !full_path.exists() {
        return Err(format!("Path does not exist: {}", file_path));
    }

    let canonical = full_path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve path: {}", e))?;

    // Verify relative path stayed within worktree after canonicalization
    let worktree_canonical = worktree_path
        .canonicalize()
        .map_err(|e| format!("Failed to resolve worktree path: {}", e))?;

    if !canonical.starts_with(&worktree_canonical) {
        return Err("Path escapes worktree".to_string());
    }

    Ok(canonical)
}

/// Git add and commit (or amend) all changes in the worktree.
pub fn git_commit_all(worktree_path: &Path, commit_msg: &str) -> Result<CommitResult, String> {
    // Stage all changes
    let add_output = crate::env::git()
        .args(["add", "-A"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("Failed to run git add: {}", e))?;

    if !add_output.status.success() {
        let stderr = String::from_utf8_lossy(&add_output.stderr);
        return Err(format!("git add failed: {}", stderr));
    }

    // Check if there are changes to commit
    let status_output = crate::env::git()
        .args(["diff", "--cached", "--quiet"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("Failed to check git status: {}", e))?;

    // If exit code is 0, there are no changes
    if status_output.status.success() {
        return Err("nothing to commit, working tree clean".to_string());
    }

    // Commit or amend
    let commit_output = if commit_msg == "^" {
        // Check if there's a commit to amend
        let log_output = crate::env::git()
            .args(["log", "-1", "--format=%H"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| format!("Failed to check git log: {}", e))?;

        if !log_output.status.success() || log_output.stdout.is_empty() {
            // No previous commit, create initial commit
            crate::env::git()
                .args(["commit", "-m", "Initial changes"])
                .current_dir(worktree_path)
                .output()
                .map_err(|e| format!("Failed to run git commit: {}", e))?
        } else {
            // Amend previous commit
            crate::env::git()
                .args(["commit", "--amend", "--no-edit"])
                .current_dir(worktree_path)
                .output()
                .map_err(|e| format!("Failed to run git commit --amend: {}", e))?
        }
    } else {
        // New commit with message
        crate::env::git()
            .args(["commit", "-m", commit_msg])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| format!("Failed to run git commit: {}", e))?
    };

    if !commit_output.status.success() {
        let stderr = String::from_utf8_lossy(&commit_output.stderr);
        return Err(format!("git commit failed: {}", stderr));
    }

    // Get the commit SHA for confirmation
    let sha = crate::env::git()
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(worktree_path)
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    // Push to origin (don't fail if push fails - commit succeeded locally)
    push_to_origin(worktree_path);

    // Check if a PR exists for this branch
    let pr_number = get_pr_for_branch(worktree_path);

    Ok(CommitResult { sha, pr_number })
}

/// Git add and commit (or amend) a file.
pub fn git_commit_file(
    worktree_path: &Path,
    file_path: &str,
    commit_msg: &str,
) -> Result<CommitResult, String> {
    // Stage the file
    let add_output = crate::env::git()
        .args(["add", file_path])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("Failed to run git add: {}", e))?;

    if !add_output.status.success() {
        let stderr = String::from_utf8_lossy(&add_output.stderr);
        return Err(format!("git add failed: {}", stderr));
    }

    // Commit or amend
    let commit_output = if commit_msg == "^" {
        // Check if there's a commit to amend
        let log_output = crate::env::git()
            .args(["log", "-1", "--format=%H"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| format!("Failed to check git log: {}", e))?;

        if !log_output.status.success() || log_output.stdout.is_empty() {
            // No previous commit, create initial commit
            crate::env::git()
                .args(["commit", "-m", "Initial changes"])
                .current_dir(worktree_path)
                .output()
                .map_err(|e| format!("Failed to run git commit: {}", e))?
        } else {
            // Amend previous commit
            crate::env::git()
                .args(["commit", "--amend", "--no-edit"])
                .current_dir(worktree_path)
                .output()
                .map_err(|e| format!("Failed to run git commit --amend: {}", e))?
        }
    } else {
        // New commit with message
        crate::env::git()
            .args(["commit", "-m", commit_msg])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| format!("Failed to run git commit: {}", e))?
    };

    if !commit_output.status.success() {
        let stderr = String::from_utf8_lossy(&commit_output.stderr);
        return Err(format!("git commit failed: {}", stderr));
    }

    // Get the commit SHA for confirmation
    let sha = crate::env::git()
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(worktree_path)
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    // Push to origin (don't fail if push fails - commit succeeded locally)
    push_to_origin(worktree_path);

    // Check if a PR exists for this branch
    let pr_number = get_pr_for_branch(worktree_path);

    Ok(CommitResult { sha, pr_number })
}

/// Push current branch to origin. Logs errors but doesn't fail.
pub fn push_to_origin(worktree_path: &Path) {
    // Get current branch name
    let branch_output = crate::env::git()
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(worktree_path)
        .output();

    let branch = match branch_output {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => {
            log::warn!("Could not determine branch name for push");
            return;
        }
    };

    // Skip push for detached HEAD or main/master branches
    if branch == "HEAD" || branch == "main" || branch == "master" {
        log::debug!("Skipping push for branch: {}", branch);
        return;
    }

    // Push to origin (force-with-lease handles amended commits safely)
    let push_output = crate::env::git()
        .args(["push", "--force-with-lease", "origin", &branch])
        .current_dir(worktree_path)
        .output();

    match push_output {
        Ok(o) if o.status.success() => {
            log::info!("Pushed to origin/{}", branch);
        }
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            log::warn!("Push failed (commit succeeded locally): {}", stderr);
        }
        Err(e) => {
            log::warn!("Push failed (commit succeeded locally): {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_validate_file_path_allows_simple_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Create a file
        std::fs::write(worktree.join("test.txt"), "content").unwrap();

        let result = validate_file_path(worktree, "test.txt");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_file_path_allows_nested_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Create nested directory and file
        std::fs::create_dir_all(worktree.join("src/lib")).unwrap();
        std::fs::write(worktree.join("src/lib/mod.rs"), "content").unwrap();

        let result = validate_file_path(worktree, "src/lib/mod.rs");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_file_path_blocks_traversal() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Create a file inside worktree, then try to access via traversal
        std::fs::write(worktree.join("safe.txt"), "content").unwrap();

        // Try to traverse out - this should fail
        let result = validate_file_path(worktree, "../../../etc/passwd");
        assert!(result.is_err());
        // Error message varies based on whether parent exists on system
        let err = result.unwrap_err();
        assert!(
            err.contains("escapes worktree") || err.contains("Failed to"),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_validate_file_path_blocks_absolute_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Try an absolute path outside worktree
        let result = validate_file_path(worktree, "/etc/passwd");
        // This will fail because /etc/passwd doesn't start with worktree
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_file_path_creates_parent_dirs_for_new_file() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // New file in nested directory that doesn't exist
        let result = validate_file_path(worktree, "new/nested/dir/file.txt");
        assert!(result.is_ok());
        assert!(worktree.join("new/nested/dir").exists());
    }

    #[test]
    fn test_validate_file_path_blocks_hidden_traversal() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Create a legitimate nested structure
        std::fs::create_dir_all(worktree.join("src")).unwrap();

        // Try traversal hidden in path
        let result = validate_file_path(worktree, "src/../../outside.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_read_path_allows_absolute_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Create a file outside worktree
        let outside_dir = tempdir().unwrap();
        let outside_file = outside_dir.path().join("test.txt");
        std::fs::write(&outside_file, "content").unwrap();

        let result = validate_read_path(worktree, outside_file.to_str().unwrap());
        assert!(
            result.is_ok(),
            "Should allow absolute path outside worktree"
        );
    }

    #[test]
    fn test_validate_read_path_allows_relative_path_in_worktree() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        std::fs::write(worktree.join("file.txt"), "content").unwrap();

        let result = validate_read_path(worktree, "file.txt");
        assert!(result.is_ok(), "Should allow relative path in worktree");
    }

    #[test]
    fn test_validate_read_path_blocks_relative_path_traversal() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_read_path(worktree, "../../../etc/passwd");
        assert!(result.is_err(), "Should block relative path traversal");
        assert!(result.unwrap_err().contains("Path traversal not allowed"));
    }

    #[test]
    fn test_validate_read_path_rejects_nonexistent_absolute_path() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_read_path(worktree, "/nonexistent/file.txt");
        assert!(result.is_err(), "Should reject nonexistent absolute path");
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_validate_read_path_blocks_hidden_traversal() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        let result = validate_read_path(worktree, "src/../../outside.txt");
        assert!(result.is_err(), "Should block hidden path traversal");
    }

    #[test]
    fn test_git_commit_all_commits_changes() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Initialize a git repo
        let _ = crate::env::git()
            .args(["init"])
            .current_dir(worktree)
            .output()
            .unwrap();

        // Configure git user (required for commits)
        let _ = crate::env::git()
            .args(["config", "user.email", "test@test.com"])
            .current_dir(worktree)
            .output()
            .unwrap();
        let _ = crate::env::git()
            .args(["config", "user.name", "Test User"])
            .current_dir(worktree)
            .output()
            .unwrap();

        // Create and modify a file
        std::fs::write(worktree.join("test.txt"), "content").unwrap();

        // Commit all changes
        let result = git_commit_all(worktree, "Test commit");
        assert!(result.is_ok(), "Should successfully commit changes");

        // Verify commit was created
        let log_output = crate::env::git()
            .args(["log", "--oneline"])
            .current_dir(worktree)
            .output()
            .unwrap();
        assert!(log_output.status.success());
        let log_str = String::from_utf8_lossy(&log_output.stdout);
        assert!(log_str.contains("Test commit"));
    }

    #[test]
    fn test_git_commit_all_fails_when_nothing_to_commit() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Initialize a git repo
        let _ = crate::env::git()
            .args(["init"])
            .current_dir(worktree)
            .output()
            .unwrap();

        // Configure git user
        let _ = crate::env::git()
            .args(["config", "user.email", "test@test.com"])
            .current_dir(worktree)
            .output()
            .unwrap();
        let _ = crate::env::git()
            .args(["config", "user.name", "Test User"])
            .current_dir(worktree)
            .output()
            .unwrap();

        // Try to commit with no changes
        let result = git_commit_all(worktree, "Test commit");
        assert!(result.is_err(), "Should fail when nothing to commit");
        assert!(
            result.unwrap_err().contains("nothing to commit"),
            "Error should mention nothing to commit"
        );
    }

    #[test]
    fn test_git_commit_all_amends_previous_commit() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();

        // Initialize a git repo
        let _ = crate::env::git()
            .args(["init"])
            .current_dir(worktree)
            .output()
            .unwrap();

        // Configure git user
        let _ = crate::env::git()
            .args(["config", "user.email", "test@test.com"])
            .current_dir(worktree)
            .output()
            .unwrap();
        let _ = crate::env::git()
            .args(["config", "user.name", "Test User"])
            .current_dir(worktree)
            .output()
            .unwrap();

        // Create initial commit
        std::fs::write(worktree.join("test.txt"), "content").unwrap();
        let _ = git_commit_all(worktree, "Initial commit").unwrap();

        // Get commit count
        let log_output = crate::env::git()
            .args(["rev-list", "--count", "HEAD"])
            .current_dir(worktree)
            .output()
            .unwrap();
        let initial_count = String::from_utf8_lossy(&log_output.stdout)
            .trim()
            .parse::<u32>()
            .unwrap();

        // Make another change and amend
        std::fs::write(worktree.join("test2.txt"), "more content").unwrap();
        let result = git_commit_all(worktree, "^");
        assert!(result.is_ok(), "Should successfully amend commit");

        // Verify commit count is still the same (amend doesn't create new commit)
        let log_output = crate::env::git()
            .args(["rev-list", "--count", "HEAD"])
            .current_dir(worktree)
            .output()
            .unwrap();
        let final_count = String::from_utf8_lossy(&log_output.stdout)
            .trim()
            .parse::<u32>()
            .unwrap();
        assert_eq!(
            initial_count, final_count,
            "Amend should not increase commit count"
        );
    }
}
