//! Git checkout probes.

use std::path::Path;
use std::process::Output;

fn run_git_output(
    repository_path: &Path,
    args: &[&str],
    failure_context: &str,
) -> Result<Output, String> {
    cairn_platform::env::git()
        .args(args)
        .current_dir(repository_path)
        .output()
        .map_err(|e| format!("{failure_context}: {e}"))
}

fn git_stdout(
    repository_path: &Path,
    args: &[&str],
    failure_context: &str,
    command_name: &str,
) -> Result<String, String> {
    let output = run_git_output(repository_path, args, failure_context)?;
    if !output.status.success() {
        return Err(format!(
            "{command_name} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn current_commit(repository_path: &Path) -> Result<String, String> {
    git_stdout(
        repository_path,
        &["rev-parse", "HEAD"],
        "Failed to run git rev-parse HEAD",
        "git rev-parse HEAD",
    )
}

pub fn current_branch(repository_path: &Path) -> Result<String, String> {
    git_stdout(
        repository_path,
        &["rev-parse", "--abbrev-ref", "HEAD"],
        "Failed to run git rev-parse --abbrev-ref HEAD",
        "git rev-parse --abbrev-ref HEAD",
    )
}

/// Return true when the worktree has a configured, non-empty `origin` remote.
pub fn has_remote(repository_path: &Path) -> bool {
    cairn_platform::env::git()
        .args(["remote", "get-url", "origin"])
        .current_dir(repository_path)
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
        cairn_platform::env::git()
            .args(["init"])
            .current_dir(worktree)
            .output()
            .unwrap();

        assert!(!has_remote(worktree));

        cairn_platform::env::git()
            .args(["remote", "add", "origin", "https://example.com/repo.git"])
            .current_dir(worktree)
            .output()
            .unwrap();

        assert!(has_remote(worktree));
    }
}
