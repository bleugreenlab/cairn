//! Git client service for version control operations.
//!
//! Abstracts git CLI calls to enable testing without real git repositories.

use std::path::Path;

#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;

/// Output from a git command.
#[derive(Debug, Clone)]
pub struct GitOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

/// Trait for git operations.
///
/// This abstraction allows tests to mock git behavior
/// without requiring real repositories or the git CLI.
///
/// Note: Uses owned Strings to avoid lifetime complexity with mockall.
#[cfg_attr(any(test, feature = "test-utils"), automock)]
pub trait GitClient: Send + Sync {
    /// Run git rev-parse with the given arguments.
    fn rev_parse(&self, repo: &Path, args: Vec<String>) -> Result<String, String>;

    /// Check if a branch exists in the repository.
    fn branch_exists(&self, repo: &Path, branch_name: &str) -> Result<bool, String>;

    /// List worktrees in porcelain format.
    fn worktree_list(&self, repo: &Path) -> Result<String, String>;

    /// Add a new worktree with a new branch.
    fn worktree_add_new_branch(
        &self,
        repo: &Path,
        worktree_path: &Path,
        branch_name: &str,
        base_branch: &str,
    ) -> Result<(), String>;

    /// Add a worktree for an existing branch.
    fn worktree_add_existing_branch(
        &self,
        repo: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> Result<(), String>;

    /// Remove a worktree.
    fn worktree_remove(&self, repo: &Path, worktree_path: &Path, force: bool)
        -> Result<(), String>;

    /// Prune stale worktree administrative files.
    fn worktree_prune(&self, repo: &Path) -> Result<(), String>;

    /// Add a worktree at a detached HEAD (no branch).
    fn worktree_add_detached(
        &self,
        repo: &Path,
        worktree_path: &Path,
        commit_ref: &str,
    ) -> Result<(), String>;

    /// Push a branch to origin.
    fn push_branch(&self, repo: &Path, branch_name: &str) -> Result<(), String>;

    /// Get git status in porcelain format.
    fn status(&self, repo: &Path) -> Result<String, String>;

    /// Get the remote URL for origin.
    fn remote_get_url(&self, repo: &Path) -> Result<String, String>;

    /// Run an arbitrary git command (for operations not covered above).
    fn run(&self, repo: &Path, args: Vec<String>) -> Result<GitOutput, String>;

    /// Get the current branch name.
    fn current_branch(&self, repo: &Path) -> Result<String, String>;

    /// Stash changes with an optional message.
    fn stash_push(&self, repo: &Path, message: Option<String>) -> Result<(), String>;

    /// Pop the most recent stash.
    fn stash_pop(&self, repo: &Path) -> Result<(), String>;

    /// Pull from a remote branch.
    fn pull(&self, repo: &Path, remote: &str, branch: &str) -> Result<(), String>;

    /// Delete a local branch.
    fn delete_branch(&self, repo: &Path, branch_name: &str, force: bool) -> Result<(), String>;

    /// Check if a branch exists on the remote (origin).
    fn remote_branch_exists(&self, repo: &Path, branch_name: &str) -> Result<bool, String>;

    /// Add a worktree tracking a remote branch (creates local branch from origin/<branch>).
    fn worktree_add_tracking_branch(
        &self,
        repo: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> Result<(), String>;
}

/// Production git client using the git CLI.
pub struct RealGitClient;

impl RealGitClient {
    fn git_command(&self) -> std::process::Command {
        crate::env::git()
    }

    fn run_git(&self, repo: &Path, args: &[String]) -> Result<GitOutput, String> {
        let output = self
            .git_command()
            .args(args)
            .current_dir(repo)
            .output()
            .map_err(|e| format!("Failed to run git: {}", e))?;

        Ok(GitOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        })
    }

    fn run_git_strs(&self, repo: &Path, args: &[&str]) -> Result<GitOutput, String> {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        self.run_git(repo, &args)
    }
}

impl GitClient for RealGitClient {
    fn rev_parse(&self, repo: &Path, args: Vec<String>) -> Result<String, String> {
        let mut full_args = vec!["rev-parse".to_string()];
        full_args.extend(args);

        let output = self.run_git(repo, &full_args)?;
        if output.success {
            Ok(output.stdout)
        } else {
            Err(format!("git rev-parse failed: {}", output.stderr))
        }
    }

    fn branch_exists(&self, repo: &Path, branch_name: &str) -> Result<bool, String> {
        let output = self.run_git_strs(
            repo,
            &[
                "rev-parse",
                "--verify",
                &format!("refs/heads/{}", branch_name),
            ],
        )?;
        Ok(output.success)
    }

    fn worktree_list(&self, repo: &Path) -> Result<String, String> {
        let output = self.run_git_strs(repo, &["worktree", "list", "--porcelain"])?;
        if output.success {
            Ok(output.stdout)
        } else {
            Err(format!("git worktree list failed: {}", output.stderr))
        }
    }

    fn worktree_add_new_branch(
        &self,
        repo: &Path,
        worktree_path: &Path,
        branch_name: &str,
        base_branch: &str,
    ) -> Result<(), String> {
        let wt_path = worktree_path.to_string_lossy();
        let output = self.run_git_strs(
            repo,
            &["worktree", "add", "-b", branch_name, &wt_path, base_branch],
        )?;

        if output.success {
            Ok(())
        } else {
            Err(format!("git worktree add failed: {}", output.stderr))
        }
    }

    fn worktree_add_existing_branch(
        &self,
        repo: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> Result<(), String> {
        let wt_path = worktree_path.to_string_lossy();
        let output = self.run_git_strs(repo, &["worktree", "add", &wt_path, branch_name])?;

        if output.success {
            Ok(())
        } else {
            Err(format!("git worktree add failed: {}", output.stderr))
        }
    }

    fn worktree_remove(
        &self,
        repo: &Path,
        worktree_path: &Path,
        force: bool,
    ) -> Result<(), String> {
        let wt_path = worktree_path.to_string_lossy();
        let mut args = vec!["worktree", "remove"];
        if force {
            args.push("--force");
        }
        args.push(&wt_path);

        let output = self.run_git_strs(repo, &args)?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git worktree remove failed: {}", output.stderr))
        }
    }

    fn worktree_prune(&self, repo: &Path) -> Result<(), String> {
        let output = self.run_git_strs(repo, &["worktree", "prune"])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git worktree prune failed: {}", output.stderr))
        }
    }

    fn worktree_add_detached(
        &self,
        repo: &Path,
        worktree_path: &Path,
        commit_ref: &str,
    ) -> Result<(), String> {
        let wt_path = worktree_path.to_string_lossy();
        // --detach creates worktree at detached HEAD
        let output =
            self.run_git_strs(repo, &["worktree", "add", "--detach", &wt_path, commit_ref])?;

        if output.success {
            Ok(())
        } else {
            Err(format!(
                "git worktree add --detach failed: {}",
                output.stderr
            ))
        }
    }

    fn push_branch(&self, repo: &Path, branch_name: &str) -> Result<(), String> {
        let output = self.run_git_strs(repo, &["push", "-u", "origin", branch_name])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git push failed: {}", output.stderr))
        }
    }

    fn status(&self, repo: &Path) -> Result<String, String> {
        let output = self.run_git_strs(repo, &["status", "--porcelain"])?;
        if output.success {
            Ok(output.stdout)
        } else {
            Err(format!("git status failed: {}", output.stderr))
        }
    }

    fn remote_get_url(&self, repo: &Path) -> Result<String, String> {
        let output = self.run_git_strs(repo, &["remote", "get-url", "origin"])?;
        if output.success {
            Ok(output.stdout)
        } else {
            Err(format!("git remote get-url failed: {}", output.stderr))
        }
    }

    fn run(&self, repo: &Path, args: Vec<String>) -> Result<GitOutput, String> {
        self.run_git(repo, &args)
    }

    fn current_branch(&self, repo: &Path) -> Result<String, String> {
        let output = self.run_git_strs(repo, &["branch", "--show-current"])?;
        if output.success {
            Ok(output.stdout)
        } else {
            Err(format!(
                "git branch --show-current failed: {}",
                output.stderr
            ))
        }
    }

    fn stash_push(&self, repo: &Path, message: Option<String>) -> Result<(), String> {
        let mut args = vec!["stash", "push"];
        if let Some(msg) = &message {
            args.push("-m");
            args.push(msg.as_str());
        }
        let output = self.run_git_strs(repo, &args)?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git stash push failed: {}", output.stderr))
        }
    }

    fn stash_pop(&self, repo: &Path) -> Result<(), String> {
        let output = self.run_git_strs(repo, &["stash", "pop"])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git stash pop failed: {}", output.stderr))
        }
    }

    fn pull(&self, repo: &Path, remote: &str, branch: &str) -> Result<(), String> {
        let output = self.run_git_strs(repo, &["pull", remote, branch])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git pull failed: {}", output.stderr))
        }
    }

    fn delete_branch(&self, repo: &Path, branch_name: &str, force: bool) -> Result<(), String> {
        let flag = if force { "-D" } else { "-d" };
        let output = self.run_git_strs(repo, &["branch", flag, branch_name])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git branch {} failed: {}", flag, output.stderr))
        }
    }

    fn remote_branch_exists(&self, repo: &Path, branch_name: &str) -> Result<bool, String> {
        let output = self.run_git_strs(
            repo,
            &[
                "rev-parse",
                "--verify",
                &format!("refs/remotes/origin/{}", branch_name),
            ],
        )?;
        Ok(output.success)
    }

    fn worktree_add_tracking_branch(
        &self,
        repo: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> Result<(), String> {
        let wt_path = worktree_path.to_string_lossy();
        let remote_ref = format!("origin/{}", branch_name);
        let output = self.run_git_strs(
            repo,
            &[
                "worktree",
                "add",
                "--track",
                "-b",
                branch_name,
                &wt_path,
                &remote_ref,
            ],
        )?;

        if output.success {
            Ok(())
        } else {
            Err(format!(
                "git worktree add --track failed: {}",
                output.stderr
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // GitOutput tests
    // =========================================================================

    #[test]
    fn git_output_struct_success() {
        let output = GitOutput {
            success: true,
            stdout: "abc123".to_string(),
            stderr: "".to_string(),
        };
        assert!(output.success);
        assert_eq!(output.stdout, "abc123");
        assert_eq!(output.stderr, "");
    }

    #[test]
    fn git_output_struct_failure() {
        let output = GitOutput {
            success: false,
            stdout: "".to_string(),
            stderr: "fatal: not a git repository".to_string(),
        };
        assert!(!output.success);
        assert_eq!(output.stdout, "");
        assert!(output.stderr.contains("not a git repository"));
    }

    #[test]
    fn git_output_clone() {
        let output = GitOutput {
            success: true,
            stdout: "data".to_string(),
            stderr: "".to_string(),
        };
        let cloned = output.clone();
        assert_eq!(cloned.success, output.success);
        assert_eq!(cloned.stdout, output.stdout);
        assert_eq!(cloned.stderr, output.stderr);
    }
}
