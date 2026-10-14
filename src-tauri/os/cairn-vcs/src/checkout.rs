//! Repair mechanics for a checkout detached by an export.
use cairn_platform::services::GitClient;
use std::path::Path;

/// Tracked generated paths the host permits checkout repair to discard.
/// The default preserves every tracked edit.
#[derive(Clone, Copy, Debug, Default)]
pub struct CheckoutRepairPolicy {
    pub regenerable_paths: &'static [&'static str],
}

pub fn dirty_tracked_paths_from_porcelain(status: &str) -> Vec<String> {
    status
        .lines()
        .filter_map(|line| {
            if line.len() < 4 || line.starts_with("??") || line.starts_with("!!") {
                return None;
            }
            let path = line[3..].rsplit(" -> ").next().unwrap_or_default().trim();
            if path.is_empty() {
                None
            } else {
                Some(path.to_string())
            }
        })
        .collect()
}

/// Outcome of a [`reattach_checkout_head`] attempt, made explicit so callers can
/// distinguish a completed repair from a guard that declined it (leaving HEAD
/// detached). The two must not be conflated: a declined repair must NOT be
/// followed by a `git pull` into the still-detached, dirty checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReattachOutcome {
    /// HEAD was re-attached to the branch and the tree fast-forwarded to its tip.
    Reattached,
    /// The dirty-tree guard declined the repair; HEAD is LEFT detached and the
    /// user's uncommitted edits preserved (a warning named the manual repair).
    DeclinedDirty,
}

/// Re-attach a detached checkout HEAD to `branch` and fast-forward the working
/// tree to that branch's tip — the ONE canonical repair for the detach a
/// `jj git export` inflicts when it moves the branch HEAD was a symref to. Used
/// both by the export wrapper (`crate::jj::export_git_preserving_checkout`,
/// synchronously at the export choke point) and by host checkout reconciliation
/// (for detaches caused by an export from a different machine/process).
///
/// Clean-tree guard: if the working tree carries uncommitted tracked changes
/// beyond the host's regenerable-path policy, a hard reset would destroy user
/// work, so HEAD is LEFT detached and a warning names the exact two commands to
/// self-repair. Callers that pre-gate the checkout clean never hit the guard;
/// callers on paths with no such gate are protected by it. A failing
/// `symbolic-ref`/`reset` on the clean path surfaces as an error
/// (the export wrapper downgrades it to a warning; the merge reconcile
/// propagates it).
pub fn reattach_checkout_head(
    git: &dyn GitClient,
    repo: &Path,
    branch: &str,
    policy: CheckoutRepairPolicy,
) -> Result<ReattachOutcome, String> {
    let blocking: Vec<String> = dirty_tracked_paths_from_porcelain(&git.status(repo)?)
        .into_iter()
        .filter(|path| !policy.regenerable_paths.contains(&path.as_str()))
        .collect();
    if !blocking.is_empty() {
        log::warn!(
            "Export detached HEAD in checkout {repo} but the working tree has uncommitted changes \
             to {files}; leaving HEAD detached to preserve them. Re-attach manually with \
             `git -C {repo} symbolic-ref HEAD refs/heads/{branch}` then \
             `git -C {repo} reset --hard {branch}`.",
            repo = repo.display(),
            files = blocking.join(", "),
        );
        return Ok(ReattachOutcome::DeclinedDirty);
    }

    run_git_checked(
        git,
        repo,
        &["symbolic-ref", "HEAD", &format!("refs/heads/{branch}")],
        "re-attach HEAD to branch",
    )?;
    run_git_checked(
        git,
        repo,
        &["reset", "--hard", branch],
        "fast-forward checkout to branch tip",
    )?;
    log::info!("Re-attached checkout HEAD to '{branch}' and fast-forwarded to its tip");
    Ok(ReattachOutcome::Reattached)
}

/// Run a git subcommand and fail (with stderr) on a non-zero exit.
fn run_git_checked(
    git: &dyn GitClient,
    repo: &Path,
    args: &[&str],
    ctx: &str,
) -> Result<(), String> {
    let out = git.run(repo, args.iter().map(|arg| arg.to_string()).collect())?;
    if out.success {
        Ok(())
    } else {
        Err(format!("{ctx} failed: {}", out.stderr.trim()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_platform::services::MockGitClient;

    #[test]
    fn default_policy_preserves_generated_files_too() {
        let mut git = MockGitClient::new();
        git.expect_status()
            .returning(|_| Ok(" M generated.txt\n".into()));
        git.expect_run().never();
        assert_eq!(
            reattach_checkout_head(&git, Path::new("/repo"), "main", Default::default()),
            Ok(ReattachOutcome::DeclinedDirty)
        );
    }

    #[test]
    fn configured_generated_path_does_not_allow_an_unrelated_edit() {
        let mut git = MockGitClient::new();
        git.expect_status()
            .returning(|_| Ok(" M generated.txt\n M source.rs\n".into()));
        git.expect_run().never();
        assert_eq!(
            reattach_checkout_head(
                &git,
                Path::new("/repo"),
                "main",
                CheckoutRepairPolicy {
                    regenerable_paths: &["generated.txt"]
                }
            ),
            Ok(ReattachOutcome::DeclinedDirty)
        );
    }
    #[test]
    fn dirty_tracked_paths_parses_only_tracked_porcelain_entries() {
        let paths = dirty_tracked_paths_from_porcelain(
            " M src/lib.rs\n?? scratch.txt\n!! ignored.log\nR  old.rs -> new.rs\nA  src-tauri/Cargo.lock\n",
        );
        assert_eq!(
            paths,
            vec![
                "src/lib.rs".to_string(),
                "new.rs".to_string(),
                "src-tauri/Cargo.lock".to_string(),
            ]
        );
    }
}
