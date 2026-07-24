//! Git helper functions for MCP handlers.

use std::path::Path;
use std::process::Output;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitAuthor {
    pub(crate) name: String,
    pub(crate) email: String,
}

impl GitAuthor {
    pub(crate) fn new(name: impl Into<String>, email: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            email: email.into(),
        }
    }
}

fn run_git_output(
    worktree_path: &Path,
    args: &[&str],
    failure_context: &str,
) -> Result<Output, String> {
    crate::env::git()
        .args(args)
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("{failure_context}: {e}"))
}

fn git_stdout(
    worktree_path: &Path,
    args: &[&str],
    failure_context: &str,
    command_name: &str,
) -> Result<String, String> {
    let output = run_git_output(worktree_path, args, failure_context)?;
    if !output.status.success() {
        return Err(format!(
            "{command_name} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Result from a commit/seal operation.
#[derive(Debug, Clone)]
pub struct CommitResult {
    pub sha: String,
    pub(crate) pr_number: Option<i32>,
    /// Set when a `^` amend was CONVERTED into a regular child commit because the
    /// commit it would have rewritten is shared with a sibling bookmark. Carries a
    /// human-readable note the barrier/commit report surfaces so the agent's `^`
    /// intent visibly landed as a child commit instead of silently rewriting
    /// shared history. `None` for every ordinary seal/amend.
    pub(crate) amend_note: Option<String>,
}

pub fn current_commit(worktree_path: &Path) -> Result<String, String> {
    git_stdout(
        worktree_path,
        &["rev-parse", "HEAD"],
        "Failed to run git rev-parse HEAD",
        "git rev-parse HEAD",
    )
}

pub fn current_branch(worktree_path: &Path) -> Result<String, String> {
    git_stdout(
        worktree_path,
        &["rev-parse", "--abbrev-ref", "HEAD"],
        "Failed to run git rev-parse --abbrev-ref HEAD",
        "git rev-parse --abbrev-ref HEAD",
    )
}

/// Return true when the worktree has a configured, non-empty `origin` remote.
pub(crate) fn has_remote(worktree_path: &Path) -> bool {
    crate::env::git()
        .args(["remote", "get-url", "origin"])
        .current_dir(worktree_path)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| !String::from_utf8_lossy(&output.stdout).trim().is_empty())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn has_remote_detects_origin_presence() {
        let temp = tempdir().unwrap();
        let worktree = temp.path();
        crate::env::git()
            .args(["init"])
            .current_dir(worktree)
            .output()
            .unwrap();

        assert!(!has_remote(worktree));

        crate::env::git()
            .args(["remote", "add", "origin", "https://example.com/repo.git"])
            .current_dir(worktree)
            .output()
            .unwrap();

        assert!(has_remote(worktree));
    }
}
