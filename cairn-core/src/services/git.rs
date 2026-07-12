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

    /// Push a branch to origin.
    fn push_branch(&self, repo: &Path, branch_name: &str) -> Result<(), String>;

    /// Get git status in porcelain format.
    fn status(&self, repo: &Path) -> Result<String, String>;

    /// Get the remote URL for origin.
    fn remote_get_url(&self, repo: &Path) -> Result<String, String>;

    /// Fetch origin into the ordinary Git repository.
    fn fetch_origin(&self, repo: &Path) -> Result<(), String>;

    /// Clone a remote repository into the target directory.
    fn clone(&self, remote_url: &str, target_dir: &Path) -> Result<(), String>;

    /// Add or update a named git remote.
    fn set_remote(&self, repo: &Path, name: &str, url: &str) -> Result<(), String>;

    /// Run an arbitrary git command (for operations not covered above).
    fn run(&self, repo: &Path, args: Vec<String>) -> Result<GitOutput, String>;

    /// Get the current branch name.
    fn current_branch(&self, repo: &Path) -> Result<String, String>;

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

    /// Check whether `path` is inside a git work tree.
    fn is_repo(&self, path: &Path) -> Result<bool, String>;

    /// Initialize a git repository and set HEAD to the desired initial branch.
    fn init_repo(&self, path: &Path, initial_branch: &str) -> Result<(), String>;

    /// Stage all changes in the repository.
    fn add_all(&self, repo: &Path) -> Result<(), String>;

    /// Commit staged changes with Cairn's inline identity.
    fn commit(&self, repo: &Path, message: &str) -> Result<(), String>;

    /// Create a branch at a specific start point.
    fn create_branch_at(&self, repo: &Path, name: &str, start_point: &str) -> Result<(), String>;

    /// Return the root commit for a revision.
    fn root_commit(&self, repo: &Path, rev: &str) -> Result<String, String>;

    /// Check whether `ancestor` is an ancestor of `descendant`.
    fn is_ancestor(&self, repo: &Path, ancestor: &str, descendant: &str) -> Result<bool, String>;

    /// Merge a ref with git's default merge message.
    fn merge_no_edit(&self, repo: &Path, merge_ref: &str) -> Result<GitOutput, String>;

    /// List paths currently in an unmerged/conflicted state.
    fn list_unmerged(&self, repo: &Path) -> Result<Vec<String>, String>;

    /// Resolve paths by checking out our side.
    fn checkout_ours(&self, repo: &Path, paths: Vec<String>) -> Result<(), String>;
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
    fn clone(&self, remote_url: &str, target_dir: &Path) -> Result<(), String> {
        let parent = target_dir.parent().unwrap_or_else(|| Path::new("."));
        let target = target_dir.to_string_lossy().to_string();
        let output = self.run_git_strs(parent, &["clone", remote_url, &target])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git clone failed: {}", output.stderr))
        }
    }

    fn is_repo(&self, path: &Path) -> Result<bool, String> {
        let output = self.run_git_strs(path, &["rev-parse", "--is-inside-work-tree"])?;
        Ok(output.success && output.stdout.trim() == "true")
    }

    fn init_repo(&self, path: &Path, initial_branch: &str) -> Result<(), String> {
        let output = self.run_git_strs(path, &["init"])?;
        if !output.success {
            return Err(format!("git init failed: {}", output.stderr));
        }

        let head_ref = format!("refs/heads/{initial_branch}");
        let output = self.run_git_strs(path, &["symbolic-ref", "HEAD", &head_ref])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git symbolic-ref failed: {}", output.stderr))
        }
    }

    fn add_all(&self, repo: &Path) -> Result<(), String> {
        let output = self.run_git_strs(repo, &["add", "-A"])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git add failed: {}", output.stderr))
        }
    }

    fn commit(&self, repo: &Path, message: &str) -> Result<(), String> {
        let output = self.run_git_strs(
            repo,
            &[
                "-c",
                "user.name=Cairn",
                "-c",
                "user.email=cairn@local.invalid",
                "commit",
                "-m",
                message,
            ],
        )?;
        if output.success
            || output.stderr.contains("nothing to commit")
            || output.stdout.contains("nothing to commit")
        {
            Ok(())
        } else {
            Err(format!("git commit failed: {}", output.stderr))
        }
    }

    fn create_branch_at(&self, repo: &Path, name: &str, start_point: &str) -> Result<(), String> {
        let output = self.run_git_strs(repo, &["branch", name, start_point])?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git branch failed: {}", output.stderr))
        }
    }

    fn root_commit(&self, repo: &Path, rev: &str) -> Result<String, String> {
        let output = self.run_git_strs(repo, &["rev-list", "--max-parents=0", rev])?;
        if output.success {
            output
                .stdout
                .lines()
                .last()
                .map(|line| line.trim().to_string())
                .filter(|line| !line.is_empty())
                .ok_or_else(|| format!("git rev-list returned no root commit for {rev}"))
        } else {
            Err(format!("git rev-list failed: {}", output.stderr))
        }
    }

    fn is_ancestor(&self, repo: &Path, ancestor: &str, descendant: &str) -> Result<bool, String> {
        let output =
            self.run_git_strs(repo, &["merge-base", "--is-ancestor", ancestor, descendant])?;
        if output.success {
            Ok(true)
        } else if output.stderr.trim().is_empty() {
            Ok(false)
        } else {
            Err(format!("git merge-base failed: {}", output.stderr))
        }
    }

    fn merge_no_edit(&self, repo: &Path, merge_ref: &str) -> Result<GitOutput, String> {
        self.run_git_strs(repo, &["merge", "--no-edit", merge_ref])
    }

    fn list_unmerged(&self, repo: &Path) -> Result<Vec<String>, String> {
        let output = self.run_git_strs(repo, &["diff", "--name-only", "--diff-filter=U"])?;
        if output.success {
            Ok(output
                .stdout
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(ToOwned::to_owned)
                .collect())
        } else {
            Err(format!("git diff unmerged failed: {}", output.stderr))
        }
    }

    fn checkout_ours(&self, repo: &Path, paths: Vec<String>) -> Result<(), String> {
        if paths.is_empty() {
            return Ok(());
        }
        let mut args = vec![
            "checkout".to_string(),
            "--ours".to_string(),
            "--".to_string(),
        ];
        args.extend(paths);
        let output = self.run_git(repo, &args)?;
        if output.success {
            Ok(())
        } else {
            Err(format!("git checkout --ours failed: {}", output.stderr))
        }
    }

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

    fn fetch_origin(&self, repo: &Path) -> Result<(), String> {
        let output = crate::jj::bounded_command_output(
            self.git_command()
                .args(["fetch", "origin"])
                .current_dir(repo),
            crate::jj::JJ_NETWORK_TIMEOUT,
            "git fetch origin",
        )?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "git fetch origin failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }

    fn set_remote(&self, repo: &Path, name: &str, url: &str) -> Result<(), String> {
        let set_output = self.run_git_strs(repo, &["remote", "set-url", name, url])?;
        if set_output.success {
            return Ok(());
        }

        let add_output = self.run_git_strs(repo, &["remote", "add", name, url])?;
        if add_output.success {
            Ok(())
        } else {
            Err(format!(
                "git remote set-url failed: {}; git remote add failed: {}",
                set_output.stderr, add_output.stderr
            ))
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

    #[test]
    fn real_git_set_remote_adds_then_updates_remote() {
        let temp = tempfile::tempdir().unwrap();
        let git = RealGitClient;
        git.init_repo(temp.path(), "main").unwrap();

        let one = temp.path().join("one.git").to_string_lossy().to_string();
        let two = temp.path().join("two.git").to_string_lossy().to_string();

        git.set_remote(temp.path(), "origin", &one).unwrap();
        assert_eq!(git.remote_get_url(temp.path()).unwrap(), one);

        git.set_remote(temp.path(), "origin", &two).unwrap();
        assert_eq!(git.remote_get_url(temp.path()).unwrap(), two);
    }
}
