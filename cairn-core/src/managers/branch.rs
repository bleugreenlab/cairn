//! Branch staleness detection for managers.
//!
//! Checks how far a manager's feature branch has diverged from the project's
//! default branch (typically `main`). Used to wake managers when their branch
//! falls behind and to display branch status in manager context.

use std::path::Path;
use std::process::Command;

/// How far a feature branch has diverged from the default branch.
#[derive(Debug, Clone)]
pub struct BranchStatus {
    pub commits_behind: i32,
    pub commits_ahead: i32,
}

/// Default number of commits behind before a manager is considered stale.
pub const DEFAULT_STALENESS_THRESHOLD: i32 = 10;

/// Check how far a feature branch has diverged from the default branch.
///
/// Uses `git rev-list --count --left-right origin/{default}...origin/{feature}`.
/// Caller must ensure `git fetch origin` has been run beforehand.
///
/// Returns `None` if the feature branch doesn't exist on origin yet.
pub fn check_staleness(
    repo_path: &Path,
    feature_branch: &str,
    default_branch: &str,
) -> Result<Option<BranchStatus>, String> {
    // First check if the feature branch exists on origin
    let check = Command::new("git")
        .args([
            "rev-parse",
            "--verify",
            &format!("origin/{}", feature_branch),
        ])
        .current_dir(repo_path)
        .output()
        .map_err(|e| format!("Failed to run git rev-parse: {}", e))?;

    if !check.status.success() {
        // Branch doesn't exist on origin yet — not stale, just not pushed
        return Ok(None);
    }

    let output = Command::new("git")
        .args([
            "rev-list",
            "--count",
            "--left-right",
            &format!("origin/{}...origin/{}", default_branch, feature_branch),
        ])
        .current_dir(repo_path)
        .output()
        .map_err(|e| format!("Failed to run git rev-list: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git rev-list failed: {}", stderr.trim()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = stdout.trim().split('\t').collect();
    if parts.len() != 2 {
        return Err(format!(
            "Unexpected git rev-list output: {:?}",
            stdout.trim()
        ));
    }

    let behind = parts[0]
        .parse::<i32>()
        .map_err(|e| format!("Failed to parse behind count: {}", e))?;
    let ahead = parts[1]
        .parse::<i32>()
        .map_err(|e| format!("Failed to parse ahead count: {}", e))?;

    Ok(Some(BranchStatus {
        commits_behind: behind,
        commits_ahead: ahead,
    }))
}

/// Fetch origin for a repo, suppressing output.
pub fn fetch_origin(repo_path: &Path) -> Result<(), String> {
    let output = Command::new("git")
        .args(["fetch", "origin", "--quiet"])
        .current_dir(repo_path)
        .output()
        .map_err(|e| format!("Failed to run git fetch: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git fetch origin failed: {}", stderr.trim()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    /// Create a git repo with a bare "origin" remote, a main branch, and optionally
    /// a feature branch. Returns (work_dir, origin_dir) — both TempDirs that must
    /// be kept alive for the duration of the test.
    fn setup_git_repo() -> (tempfile::TempDir, tempfile::TempDir) {
        let origin_dir = tempfile::tempdir().unwrap();
        let work_dir = tempfile::tempdir().unwrap();

        // Create bare origin
        run_git(&origin_dir, &["init", "--bare"]);

        // Init working repo
        run_git(&work_dir, &["init"]);
        run_git(
            &work_dir,
            &[
                "remote",
                "add",
                "origin",
                origin_dir.path().to_str().unwrap(),
            ],
        );
        run_git(&work_dir, &["config", "user.email", "test@test.com"]);
        run_git(&work_dir, &["config", "user.name", "Test"]);

        // Initial commit on main
        std::fs::write(work_dir.path().join("init.txt"), "initial").unwrap();
        run_git(&work_dir, &["add", "."]);
        run_git(&work_dir, &["commit", "-m", "initial"]);
        run_git(&work_dir, &["branch", "-M", "main"]);
        run_git(&work_dir, &["push", "-u", "origin", "main"]);

        (work_dir, origin_dir)
    }

    fn run_git(dir: &tempfile::TempDir, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn test_branch_status_fields() {
        let status = BranchStatus {
            commits_behind: 10,
            commits_ahead: 3,
        };
        assert_eq!(status.commits_behind, 10);
        assert_eq!(status.commits_ahead, 3);
    }

    #[test]
    fn test_default_threshold() {
        assert_eq!(DEFAULT_STALENESS_THRESHOLD, 10);
    }

    #[test]
    fn test_check_staleness_branch_not_on_origin() {
        let (work_dir, _origin_dir) = setup_git_repo();

        // Create a local-only branch (not pushed)
        run_git(&work_dir, &["checkout", "-b", "feature/local-only"]);
        std::fs::write(work_dir.path().join("local.txt"), "local").unwrap();
        run_git(&work_dir, &["add", "."]);
        run_git(&work_dir, &["commit", "-m", "local commit"]);

        let result = check_staleness(work_dir.path(), "feature/local-only", "main").unwrap();
        assert!(result.is_none(), "Expected None for branch not on origin");
    }

    #[test]
    fn test_check_staleness_up_to_date() {
        let (work_dir, _origin_dir) = setup_git_repo();

        // Create feature from main and push — both at same commit
        run_git(&work_dir, &["checkout", "-b", "feature/current"]);
        run_git(&work_dir, &["push", "-u", "origin", "feature/current"]);
        run_git(&work_dir, &["fetch", "origin"]);

        let status = check_staleness(work_dir.path(), "feature/current", "main")
            .unwrap()
            .expect("Expected Some(BranchStatus)");
        assert_eq!(status.commits_behind, 0);
        assert_eq!(status.commits_ahead, 0);
    }

    #[test]
    fn test_check_staleness_behind_and_ahead() {
        let (work_dir, _origin_dir) = setup_git_repo();

        // Create feature branch with 1 unique commit
        run_git(&work_dir, &["checkout", "-b", "feature/stale"]);
        std::fs::write(work_dir.path().join("feature.txt"), "feature work").unwrap();
        run_git(&work_dir, &["add", "."]);
        run_git(&work_dir, &["commit", "-m", "feature commit"]);
        run_git(&work_dir, &["push", "-u", "origin", "feature/stale"]);

        // Advance main by 5 commits
        run_git(&work_dir, &["checkout", "main"]);
        for i in 0..5 {
            std::fs::write(
                work_dir.path().join(format!("main_{}.txt", i)),
                format!("commit {}", i),
            )
            .unwrap();
            run_git(&work_dir, &["add", "."]);
            run_git(&work_dir, &["commit", "-m", &format!("main commit {}", i)]);
        }
        run_git(&work_dir, &["push", "origin", "main"]);
        run_git(&work_dir, &["fetch", "origin"]);

        let status = check_staleness(work_dir.path(), "feature/stale", "main")
            .unwrap()
            .expect("Expected Some(BranchStatus)");
        assert_eq!(status.commits_behind, 5);
        assert_eq!(status.commits_ahead, 1);
    }

    #[test]
    fn test_check_staleness_only_behind() {
        let (work_dir, _origin_dir) = setup_git_repo();

        // Feature branch from main, no unique commits
        run_git(&work_dir, &["checkout", "-b", "feature/no-work"]);
        run_git(&work_dir, &["push", "-u", "origin", "feature/no-work"]);

        // Advance main by 3 commits
        run_git(&work_dir, &["checkout", "main"]);
        for i in 0..3 {
            std::fs::write(
                work_dir.path().join(format!("advance_{}.txt", i)),
                format!("{}", i),
            )
            .unwrap();
            run_git(&work_dir, &["add", "."]);
            run_git(&work_dir, &["commit", "-m", &format!("advance {}", i)]);
        }
        run_git(&work_dir, &["push", "origin", "main"]);
        run_git(&work_dir, &["fetch", "origin"]);

        let status = check_staleness(work_dir.path(), "feature/no-work", "main")
            .unwrap()
            .expect("Expected Some(BranchStatus)");
        assert_eq!(status.commits_behind, 3);
        assert_eq!(status.commits_ahead, 0);
    }

    #[test]
    fn test_check_staleness_invalid_repo() {
        let dir = tempfile::tempdir().unwrap();
        // Not a git repo at all
        let result = check_staleness(dir.path(), "feature", "main");
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
