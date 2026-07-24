//! Shared PR helper logic — pure computation and HTTP-composed helpers.
//!
//! Used by both the Tauri app and the headless server for PR state computation,
//! GitHub API fetching, and CI log extraction.

use crate::github::api;
use crate::github::credentials::GitHubAppCredentials;
use crate::models::{Check, CheckState, ChecksStatus, MergeableState, PrState, ReviewDecision};
use crate::services::{GitClient, HttpClient};
use std::path::Path;
use std::time::Duration;

// ── Parsed PR Details ──────────────────────────────────────────

/// Intermediate struct returned by `fetch_pr_via_api`.
pub(crate) struct ParsedPrDetails {
    pub(crate) title: Option<String>,
    pub(crate) body: Option<String>,
    pub(crate) state: PrState,
    pub(crate) is_draft: bool,
    pub(crate) review_decision: Option<ReviewDecision>,
    pub(crate) mergeable: MergeableState,
    pub(crate) additions: Option<i32>,
    pub(crate) deletions: Option<i32>,
    pub(crate) head_sha: String,
}

// ── Pure Computation ───────────────────────────────────────────

/// Compute PR state from API response fields.
fn compute_pr_state(merged: bool, state_str: &str) -> PrState {
    if merged {
        PrState::Merged
    } else if state_str == "closed" {
        PrState::Closed
    } else {
        PrState::Open
    }
}

/// Compute mergeable state from API response fields.
fn compute_mergeable_state(
    mergeable: Option<bool>,
    mergeable_state_str: Option<&str>,
) -> MergeableState {
    match (mergeable, mergeable_state_str) {
        (Some(true), _) => MergeableState::Mergeable,
        (Some(false), _) => MergeableState::Conflicting,
        (None, Some("dirty")) => MergeableState::Conflicting,
        (None, Some("clean")) => MergeableState::Mergeable,
        _ => MergeableState::Unknown,
    }
}

/// Compute review decision from reviews.
fn compute_review_decision(reviews: &[api::Review]) -> Option<ReviewDecision> {
    if reviews.is_empty() {
        return None;
    }

    // Get the latest review per user
    let mut latest_reviews: std::collections::HashMap<&str, &str> =
        std::collections::HashMap::new();
    for review in reviews {
        latest_reviews.insert(&review.user.login, &review.state);
    }

    let has_approved = latest_reviews.values().any(|&s| s == "APPROVED");
    let has_changes_requested = latest_reviews.values().any(|&s| s == "CHANGES_REQUESTED");

    if has_changes_requested {
        Some(ReviewDecision::ChangesRequested)
    } else if has_approved {
        Some(ReviewDecision::Approved)
    } else {
        Some(ReviewDecision::ReviewRequired)
    }
}

/// Compute overall checks status from a list of checks.
pub(crate) fn compute_checks_status(checks: &[Check]) -> Option<ChecksStatus> {
    if checks.is_empty() {
        return None;
    }

    let has_failure = checks.iter().any(|c| c.state == CheckState::Failure);
    let has_pending = checks.iter().any(|c| c.state == CheckState::Pending);
    let all_success = checks
        .iter()
        .all(|c| matches!(c.state, CheckState::Success | CheckState::Skipped));

    if has_failure {
        Some(ChecksStatus::Failure)
    } else if has_pending {
        Some(ChecksStatus::Pending)
    } else if all_success {
        Some(ChecksStatus::Success)
    } else {
        Some(ChecksStatus::Error)
    }
}

/// Extract workflow run ID from a GitHub Actions URL.
pub fn extract_run_id(url: &str) -> Result<i64, String> {
    let parts: Vec<&str> = url.split('/').collect();

    for (i, part) in parts.iter().enumerate() {
        if *part == "runs" && i + 1 < parts.len() {
            let run_id_str = parts[i + 1].split('/').next().unwrap_or(parts[i + 1]);
            return run_id_str
                .parse()
                .map_err(|_| format!("Invalid run ID: {}", run_id_str));
        }
    }

    Err(format!("Could not extract run ID from URL: {}", url))
}

// ── HTTP-Composed Helpers ──────────────────────────────────────

/// Number of times to re-poll GitHub for a PR's mergeability after the first
/// fetch returns a still-computing `null`.
///
/// GitHub computes a PR's `mergeable` boolean asynchronously: a freshly opened
/// or just-pushed PR's first GET returns `mergeable: null` until the background
/// merge check finishes. Polling past that window keeps a null-window `UNKNOWN`
/// out of the cache. Bounded so a genuinely indeterminate PR still resolves.
const MERGEABILITY_POLL_ATTEMPTS: usize = 4;

/// Delay between mergeability re-polls (~2.4s worst case across all attempts).
const MERGEABILITY_POLL_BACKOFF: Duration = Duration::from_millis(600);

/// Fetch and parse PR details from GitHub, including reviews.
///
/// Polls past GitHub's asynchronous mergeability-compute window (see
/// [`MERGEABILITY_POLL_ATTEMPTS`]) so an open PR never resolves to a
/// null-window `UNKNOWN` that would otherwise stick in the cache.
pub(crate) async fn fetch_pr_via_api(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<ParsedPrDetails, String> {
    fetch_pr_via_api_with_backoff(
        http,
        creds,
        owner,
        repo,
        pr_number,
        MERGEABILITY_POLL_BACKOFF,
    )
    .await
}

async fn fetch_pr_via_api_with_backoff(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    pr_number: i32,
    backoff: Duration,
) -> Result<ParsedPrDetails, String> {
    let mut pr = api::fetch_pr(http, creds, owner, repo, pr_number).await?;

    // GitHub returns `mergeable: null` for an open PR while it computes the merge
    // check in the background. Re-poll until it settles or the attempt budget is
    // exhausted; a still-null result then falls through to `Unknown` gracefully.
    for _ in 0..MERGEABILITY_POLL_ATTEMPTS {
        if pr.mergeable.is_some() || compute_pr_state(pr.merged, &pr.state) != PrState::Open {
            break;
        }
        tokio::time::sleep(backoff).await;
        pr = api::fetch_pr(http, creds, owner, repo, pr_number).await?;
    }

    let state = compute_pr_state(pr.merged, &pr.state);
    let mergeable = compute_mergeable_state(pr.mergeable, pr.mergeable_state.as_deref());

    // Fetch reviews to determine review decision
    let reviews = api::fetch_reviews(http, creds, owner, repo, pr_number)
        .await
        .unwrap_or_default();

    let review_decision = compute_review_decision(&reviews);

    Ok(ParsedPrDetails {
        title: Some(pr.title),
        body: pr.body,
        state,
        is_draft: pr.draft,
        review_decision,
        mergeable,
        additions: Some(pr.additions),
        deletions: Some(pr.deletions),
        head_sha: pr.head.sha,
    })
}

/// Fetch check runs for a commit and convert to domain Check model.
pub(crate) async fn fetch_checks_via_api(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    sha: &str,
) -> Result<Vec<Check>, String> {
    let check_runs = api::fetch_check_runs(http, creds, owner, repo, sha).await?;

    Ok(check_runs
        .check_runs
        .into_iter()
        .map(|c| {
            let state = match (c.status.as_str(), c.conclusion.as_deref()) {
                ("completed", Some("success")) => CheckState::Success,
                ("completed", Some("failure")) => CheckState::Failure,
                ("completed", Some("skipped")) => CheckState::Skipped,
                ("completed", Some("cancelled")) => CheckState::Cancelled,
                ("completed", _) => CheckState::Failure,
                _ => CheckState::Pending,
            };

            Check {
                name: c.name,
                state,
                description: c.output.summary,
                workflow_name: None,
                link: Some(c.html_url),
            }
        })
        .collect())
}

/// Fetch the name of the failed step from job details via REST API.
pub async fn fetch_failed_step_via_api(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    run_id: i64,
    job_name: &str,
) -> Result<String, String> {
    let jobs_response = api::fetch_run_jobs(http, creds, owner, repo, run_id).await?;

    let job = jobs_response
        .jobs
        .iter()
        .find(|j| j.name == job_name || j.name.contains(job_name))
        .ok_or_else(|| format!("Job '{}' not found in run", job_name))?;

    let failed_step = job
        .steps
        .as_ref()
        .and_then(|steps| {
            steps
                .iter()
                .find(|s| s.conclusion.as_deref() == Some("failure"))
                .map(|s| s.name.clone())
        })
        .ok_or("No failed step found")?;

    Ok(failed_step)
}

/// Fetch failure logs from a workflow run via REST API.
/// Returns `(log_excerpt, full_log_available)`.
pub async fn fetch_failure_logs_via_api(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    run_id: i64,
    job_name: &str,
) -> Result<(String, bool), String> {
    let logs_data = api::fetch_run_logs(http, creds, owner, repo, run_id).await?;

    let cursor = std::io::Cursor::new(logs_data);
    let mut archive =
        zip::ZipArchive::new(cursor).map_err(|e| format!("Failed to read logs archive: {}", e))?;

    let mut job_logs = String::new();

    // Try to find logs matching the job name
    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| format!("Failed to read log file: {}", e))?;

        let name = file.name().to_string();

        if name.starts_with(job_name) || name.contains(job_name) {
            use std::io::Read;
            let mut contents = String::new();
            file.read_to_string(&mut contents)
                .map_err(|e| format!("Failed to read log contents: {}", e))?;
            job_logs.push_str(&contents);
            job_logs.push('\n');
        }
    }

    // Fallback: if no matching logs, return all logs
    if job_logs.is_empty() {
        for i in 0..archive.len() {
            let mut file = archive
                .by_index(i)
                .map_err(|e| format!("Failed to read log file: {}", e))?;

            use std::io::Read;
            let mut contents = String::new();
            file.read_to_string(&mut contents)
                .map_err(|e| format!("Failed to read log contents: {}", e))?;
            job_logs.push_str(&contents);
            job_logs.push('\n');
        }
    }

    if job_logs.is_empty() {
        return Err("No failure logs found".to_string());
    }

    Ok((job_logs, false))
}

// ── Main Repo Update ───────────────────────────────────────────

// The main checkout can carry persistent tracked lockfile churn while the Dev
// terminal or `bun dev:instance` runs from the project checkout. It is generated
// by Cargo's two-workspace lockfile behavior and is safe to discard when a merge
// advances the default branch. Keep this list deliberately tiny: every other
// tracked change should stop or skip checkout mutation before reconcile can
// hard-reset it.
const REGENERABLE_DIRTY_CHECKOUT_PATHS: &[&str] = &["src-tauri/Cargo.lock"];

pub(crate) fn dirty_tracked_paths_from_porcelain(status: &str) -> Vec<String> {
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

pub(crate) fn assert_main_checkout_clean_for_default_merge(
    git: &dyn GitClient,
    repo_path: &str,
) -> Result<(), String> {
    let dirty_paths = dirty_tracked_paths_from_porcelain(&git.status(Path::new(repo_path))?);
    let blocking_paths: Vec<_> = dirty_paths
        .into_iter()
        .filter(|path| !REGENERABLE_DIRTY_CHECKOUT_PATHS.contains(&path.as_str()))
        .collect();

    if blocking_paths.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Refusing to merge: your project checkout at {repo_path} has uncommitted changes to {files}. Commit or discard them, then retry.",
            files = blocking_paths.join(", ")
        ))
    }
}

/// Run a git subcommand and fail (with stderr) on a non-zero exit.
fn run_git_checked(
    git: &dyn GitClient,
    repo: &Path,
    args: &[&str],
    ctx: &str,
) -> Result<(), String> {
    let out = run_git(git, repo, args)?;
    if out.success {
        Ok(())
    } else {
        Err(format!("{ctx} failed: {}", out.stderr.trim()))
    }
}

/// Restore the user's project checkout after a merge folded the source into the
/// default branch and exported it to the backing `.git`.
///
/// The shared jj store's git backend IS the project's `.git`, so the merge fold's
/// `jj git export` advances `refs/heads/<default>` to the merged tip but DETACHES
/// the checkout's git HEAD: jj cannot leave HEAD a symref to a branch it is
/// moving, so it pins HEAD at the pre-merge tip (which keeps the working tree
/// clean). Left alone, the user's main checkout sits in detached HEAD after every
/// merge into the default branch — for both local and remote merges, and the old
/// pull-only path could not see it (`git branch --show-current` is empty when
/// detached, so it read "not on default" and skipped).
///
/// Repair: when HEAD is detached, re-attach it to `refs/heads/<default>` and
/// hard-reset the working tree to the merged tip the export already wrote to that
/// ref. A checkout deliberately on a *different* branch is never detached by the
/// export (jj only rewrites HEAD when it points at the branch being moved), so it
/// is left untouched. `pull` (remote + `pull_on_merge`) adds a `git pull origin
/// <default>` to also absorb an external advance; it never gates the re-attach,
/// which must restore the invariant regardless of the pull preference.
///
/// This path deliberately does not stash. Merges that advance the default branch
/// pass through a pre-merge dirty-checkout gate, so any remaining tracked dirt is
/// the allowlisted, regenerable lockfile churn from the two-lockfile dev-build
/// workflow and can be discarded by the hard reset.
pub(crate) fn reconcile_main_checkout_after_merge(
    git: &dyn GitClient,
    repo_path: &str,
    default_branch: &str,
    pull: bool,
) -> Result<(), String> {
    let repo = Path::new(repo_path);

    let current_branch = git.current_branch(repo)?;
    let detached = current_branch.is_empty();

    // A checkout deliberately on a non-default branch is never detached by the
    // export, so there is nothing to repair and a pull would fight the user's
    // branch choice.
    if !detached && current_branch != default_branch {
        log::info!(
            "Main repo on '{}', not default '{}'; leaving checkout untouched",
            current_branch,
            default_branch
        );
        return Ok(());
    }

    // Already attached to the default branch and no pull requested: nothing to do.
    if !detached && !pull {
        return Ok(());
    }

    let reconciled = reconcile_checkout_inner(git, repo, default_branch, detached, pull)?;
    if reconciled {
        log::info!(
            "Reconciled main repo at {} onto {}",
            repo_path,
            default_branch
        );
    } else {
        // The dirty guard declined the repair (it already warned with the manual
        // re-attach commands); the checkout is intentionally left detached, so do
        // not claim a reconcile that did not happen, and do not pull into it.
        log::warn!(
            "Left main repo at {} detached to preserve uncommitted changes; see the re-attach warning above",
            repo_path
        );
    }
    Ok(())
}

/// The HEAD-mutating core. Re-attaches a detached HEAD to the default branch
/// (delegating to the canonical [`reattach_checkout_head`] repair) and then
/// optionally pulls, returning whether the checkout ended ATTACHED. The re-attach
/// runs before the pull so the invariant is restored even if the pull (network)
/// fails; and the pull is skipped entirely when the dirty guard DECLINED the
/// repair, so it never merges into a still-detached, dirty checkout.
fn reconcile_checkout_inner(
    git: &dyn GitClient,
    repo: &Path,
    default_branch: &str,
    detached: bool,
    pull: bool,
) -> Result<bool, String> {
    // `attached` = the checkout ends attached to the default branch. A detached
    // checkout whose dirty guard declined the repair stays detached, and we must
    // NOT pull into it: pulling into a detached, dirty checkout fights the edits
    // the guard deliberately preserved (and can fast-forward or fail against a
    // detached HEAD). Only pull once the checkout is genuinely on the branch.
    let attached = if detached {
        matches!(
            reattach_checkout_head(git, repo, default_branch)?,
            ReattachOutcome::Reattached
        )
    } else {
        true
    };

    if pull && attached {
        git.pull(repo, "origin", default_branch)?;
    }

    Ok(attached)
}

/// Outcome of a [`reattach_checkout_head`] attempt, made explicit so callers can
/// distinguish a completed repair from a guard that declined it (leaving HEAD
/// detached). The two must not be conflated: a declined repair must NOT be
/// followed by a `git pull` into the still-detached, dirty checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReattachOutcome {
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
/// synchronously at the export choke point) and by
/// [`reconcile_main_checkout_after_merge`] (for detaches caused by an export from
/// a different machine/process, e.g. the push-webhook path).
///
/// Clean-tree guard: if the working tree carries uncommitted tracked changes
/// beyond the regenerable-lockfile allowlist, a hard reset would destroy user
/// work, so HEAD is LEFT detached and a warning names the exact two commands to
/// self-repair. Callers that pre-gate the checkout clean (the default-branch
/// merge, via `assert_main_checkout_clean_for_default_merge`) never hit the
/// guard; callers on paths with no such gate (non-merge exports) are protected by
/// it. A failing `symbolic-ref`/`reset` on the clean path surfaces as an error
/// (the export wrapper downgrades it to a warning; the merge reconcile
/// propagates it).
pub(crate) fn reattach_checkout_head(
    git: &dyn GitClient,
    repo: &Path,
    branch: &str,
) -> Result<ReattachOutcome, String> {
    let blocking: Vec<String> = dirty_tracked_paths_from_porcelain(&git.status(repo)?)
        .into_iter()
        .filter(|path| !REGENERABLE_DIRTY_CHECKOUT_PATHS.contains(&path.as_str()))
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

fn run_git(
    git: &dyn GitClient,
    repo: &Path,
    args: &[&str],
) -> Result<crate::services::GitOutput, String> {
    git.run(repo, args.iter().map(|arg| arg.to_string()).collect())
}

/// Compute local mergeability without touching the working tree.
pub(crate) fn compute_local_mergeable(
    git: &dyn GitClient,
    repo_path: &Path,
    target_branch: &str,
    source_branch: &str,
) -> MergeableState {
    match run_git(
        git,
        repo_path,
        &["merge-tree", "--write-tree", target_branch, source_branch],
    ) {
        Ok(output) if output.success => MergeableState::Mergeable,
        Ok(_) => MergeableState::Conflicting,
        Err(_) => MergeableState::Unknown,
    }
}

/// Capture local changed files for a PR-equivalent using git diff --numstat.
pub(crate) fn local_pr_files(
    git: &dyn GitClient,
    repo_path: &Path,
    target_branch: &str,
    source_branch: &str,
) -> Result<Vec<api::PrFile>, String> {
    // PR semantics: compare the source branch against the merge base with the
    // target branch, excluding target-only changes that landed after fork.
    let range = format!("{target_branch}...{source_branch}");
    let output = run_git(git, repo_path, &["diff", "--numstat", &range])?;
    if !output.success {
        return Err(format!("git diff --numstat failed: {}", output.stderr));
    }
    let mut files = Vec::new();
    for line in output.stdout.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 3 {
            continue;
        }
        let additions = parts[0].parse::<i32>().unwrap_or(0);
        let deletions = parts[1].parse::<i32>().unwrap_or(0);
        files.push(api::PrFile {
            filename: parts[2].to_string(),
            status: "modified".to_string(),
            additions,
            deletions,
            changes: additions + deletions,
            patch: None,
            previous_filename: None,
        });
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(path: &std::path::Path, args: &[&str]) {
        let output = crate::env::git()
            .args(args)
            .current_dir(path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn local_pr_files_excludes_target_only_changes_after_fork() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        git(repo, &["init"]);
        git(repo, &["config", "user.email", "test@test.com"]);
        git(repo, &["config", "user.name", "Test User"]);
        git(repo, &["checkout", "-B", "main"]);

        std::fs::write(repo.join("file.txt"), "base\n").unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-m", "base"]);

        git(repo, &["checkout", "-b", "feature"]);
        std::fs::write(repo.join("file.txt"), "base\nfeature\n").unwrap();
        git(repo, &["commit", "-am", "feature"]);

        git(repo, &["checkout", "main"]);
        std::fs::write(repo.join("file.txt"), "base\ntarget\n").unwrap();
        git(repo, &["commit", "-am", "target"]);

        let files =
            local_pr_files(&crate::services::RealGitClient, repo, "main", "feature").unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "file.txt");
        assert_eq!(files[0].additions, 1);
        assert_eq!(files[0].deletions, 0);
    }

    #[test]
    fn extract_run_id_from_full_url() {
        let url = "https://github.com/owner/repo/actions/runs/12345678/job/87654321";
        let result = extract_run_id(url).unwrap();
        assert_eq!(result, 12345678);
    }

    #[test]
    fn extract_run_id_without_job() {
        let url = "https://github.com/owner/repo/actions/runs/12345678";
        let result = extract_run_id(url).unwrap();
        assert_eq!(result, 12345678);
    }

    #[test]
    fn test_compute_pr_state_merged() {
        assert_eq!(compute_pr_state(true, "closed"), PrState::Merged);
        assert_eq!(compute_pr_state(true, "open"), PrState::Merged);
    }

    #[test]
    fn test_compute_pr_state_closed() {
        assert_eq!(compute_pr_state(false, "closed"), PrState::Closed);
    }

    #[test]
    fn test_compute_pr_state_open() {
        assert_eq!(compute_pr_state(false, "open"), PrState::Open);
    }

    #[test]
    fn test_compute_mergeable_state() {
        assert_eq!(
            compute_mergeable_state(Some(true), None),
            MergeableState::Mergeable
        );
        assert_eq!(
            compute_mergeable_state(Some(false), None),
            MergeableState::Conflicting
        );
        assert_eq!(
            compute_mergeable_state(None, Some("dirty")),
            MergeableState::Conflicting
        );
        assert_eq!(
            compute_mergeable_state(None, Some("clean")),
            MergeableState::Mergeable
        );
        assert_eq!(compute_mergeable_state(None, None), MergeableState::Unknown);
        // The boolean wins over `mergeable_state`: a settled `Some(true)` is
        // MERGEABLE even when the state is "unstable" (e.g. checks failing). The
        // webhook path now shares this fn, so both paths agree on this case.
        assert_eq!(
            compute_mergeable_state(Some(true), Some("unstable")),
            MergeableState::Mergeable
        );
    }

    #[test]
    fn test_compute_review_decision_empty() {
        let reviews: Vec<api::Review> = vec![];
        assert_eq!(compute_review_decision(&reviews), None);
    }

    #[test]
    fn test_compute_review_decision_approved() {
        let reviews = vec![api::Review {
            user: api::User {
                login: "reviewer".to_string(),
            },
            state: "APPROVED".to_string(),
        }];
        assert_eq!(
            compute_review_decision(&reviews),
            Some(ReviewDecision::Approved)
        );
    }

    #[test]
    fn test_compute_review_decision_changes_requested() {
        let reviews = vec![
            api::Review {
                user: api::User {
                    login: "reviewer1".to_string(),
                },
                state: "APPROVED".to_string(),
            },
            api::Review {
                user: api::User {
                    login: "reviewer2".to_string(),
                },
                state: "CHANGES_REQUESTED".to_string(),
            },
        ];
        assert_eq!(
            compute_review_decision(&reviews),
            Some(ReviewDecision::ChangesRequested)
        );
    }

    #[test]
    fn test_compute_checks_status_empty() {
        assert_eq!(compute_checks_status(&[]), None);
    }

    #[test]
    fn test_compute_checks_status_all_success() {
        let checks = vec![
            Check {
                name: "build".into(),
                state: CheckState::Success,
                description: None,
                workflow_name: None,
                link: None,
            },
            Check {
                name: "test".into(),
                state: CheckState::Skipped,
                description: None,
                workflow_name: None,
                link: None,
            },
        ];
        assert_eq!(compute_checks_status(&checks), Some(ChecksStatus::Success));
    }

    #[test]
    fn test_compute_checks_status_with_failure() {
        let checks = vec![
            Check {
                name: "build".into(),
                state: CheckState::Failure,
                description: None,
                workflow_name: None,
                link: None,
            },
            Check {
                name: "test".into(),
                state: CheckState::Success,
                description: None,
                workflow_name: None,
                link: None,
            },
        ];
        assert_eq!(compute_checks_status(&checks), Some(ChecksStatus::Failure));
    }

    #[test]
    fn test_compute_checks_status_with_pending() {
        let checks = vec![Check {
            name: "build".into(),
            state: CheckState::Pending,
            description: None,
            workflow_name: None,
            link: None,
        }];
        assert_eq!(compute_checks_status(&checks), Some(ChecksStatus::Pending));
    }

    #[test]
    fn test_compute_checks_status_cancelled_only_returns_error() {
        let checks = vec![Check {
            name: "build".into(),
            state: CheckState::Cancelled,
            description: None,
            workflow_name: None,
            link: None,
        }];
        assert_eq!(compute_checks_status(&checks), Some(ChecksStatus::Error));
    }

    // ── compute_review_decision: latest review per user wins ────

    #[test]
    fn test_compute_review_decision_same_user_latest_wins() {
        // Same user first approves, then requests changes — latest should win
        let reviews = vec![
            api::Review {
                user: api::User {
                    login: "alice".to_string(),
                },
                state: "APPROVED".to_string(),
            },
            api::Review {
                user: api::User {
                    login: "alice".to_string(),
                },
                state: "CHANGES_REQUESTED".to_string(),
            },
        ];
        assert_eq!(
            compute_review_decision(&reviews),
            Some(ReviewDecision::ChangesRequested)
        );
    }

    #[test]
    fn test_compute_review_decision_same_user_changes_then_approves() {
        // Same user first requests changes, then approves — latest should win
        let reviews = vec![
            api::Review {
                user: api::User {
                    login: "alice".to_string(),
                },
                state: "CHANGES_REQUESTED".to_string(),
            },
            api::Review {
                user: api::User {
                    login: "alice".to_string(),
                },
                state: "APPROVED".to_string(),
            },
        ];
        assert_eq!(
            compute_review_decision(&reviews),
            Some(ReviewDecision::Approved)
        );
    }

    #[test]
    fn test_compute_review_decision_comment_only() {
        let reviews = vec![api::Review {
            user: api::User {
                login: "bob".to_string(),
            },
            state: "COMMENTED".to_string(),
        }];
        assert_eq!(
            compute_review_decision(&reviews),
            Some(ReviewDecision::ReviewRequired)
        );
    }

    // ── extract_run_id: error cases ─────────────────────────────

    #[test]
    fn extract_run_id_no_runs_segment() {
        let url = "https://github.com/owner/repo/actions/workflows/ci.yml";
        assert!(extract_run_id(url).is_err());
    }

    #[test]
    fn extract_run_id_invalid_number() {
        let url = "https://github.com/owner/repo/actions/runs/not-a-number";
        assert!(extract_run_id(url).is_err());
    }

    // ── fetch_checks_via_api: check-run-to-Check mapping ────────

    #[tokio::test]
    async fn fetch_checks_via_api_maps_states_correctly() {
        use crate::github::credentials::GitHubAppCredentials;
        use crate::services::{testing::MockHttpClient, HttpResponse};

        let token_body = serde_json::json!({
            "token": "ghs_test",
            "expires_at": "2099-01-01T00:00:00Z"
        });
        let checks_body = serde_json::json!({
            "check_runs": [
                { "name": "success-check", "status": "completed", "conclusion": "success", "html_url": "https://example.com/1", "output": { "summary": null } },
                { "name": "failure-check", "status": "completed", "conclusion": "failure", "html_url": "https://example.com/2", "output": { "summary": "Failed" } },
                { "name": "skipped-check", "status": "completed", "conclusion": "skipped", "html_url": "https://example.com/3", "output": { "summary": null } },
                { "name": "cancelled-check", "status": "completed", "conclusion": "cancelled", "html_url": "https://example.com/4", "output": { "summary": null } },
                { "name": "unknown-conclusion", "status": "completed", "conclusion": "timed_out", "html_url": "https://example.com/5", "output": { "summary": null } },
                { "name": "in-progress", "status": "in_progress", "conclusion": null, "html_url": "https://example.com/6", "output": { "summary": null } }
            ]
        });
        let http = MockHttpClient::new()
            .respond_to(
                "access_tokens",
                HttpResponse {
                    status: 201,
                    body: serde_json::to_vec(&token_body).unwrap(),
                },
            )
            .respond_to(
                "check-runs",
                HttpResponse {
                    status: 200,
                    body: serde_json::to_vec(&checks_body).unwrap(),
                },
            );
        let creds = GitHubAppCredentials {
            app_id: 12345,
            private_key: include_str!("../../tests/fixtures/test_rsa_key.pem").to_string(),
            installation_id: 99999,
        };

        let checks = fetch_checks_via_api(&http, &creds, "owner", "repo", "sha123")
            .await
            .unwrap();
        assert_eq!(checks.len(), 6);
        assert_eq!(checks[0].state, CheckState::Success);
        assert_eq!(checks[1].state, CheckState::Failure);
        assert_eq!(checks[1].description, Some("Failed".to_string()));
        assert_eq!(checks[2].state, CheckState::Skipped);
        assert_eq!(checks[3].state, CheckState::Cancelled);
        assert_eq!(checks[4].state, CheckState::Failure); // unknown conclusion → Failure
        assert_eq!(checks[5].state, CheckState::Pending); // in_progress → Pending
    }

    // ── fetch_pr_via_api: mergeability poll ─────────────────────

    fn mergeability_poll_creds() -> crate::github::credentials::GitHubAppCredentials {
        crate::github::credentials::GitHubAppCredentials {
            app_id: 12345,
            private_key: include_str!("../../tests/fixtures/test_rsa_key.pem").to_string(),
            installation_id: 99999,
        }
    }

    fn pr_response(
        mergeable: serde_json::Value,
        mergeable_state: serde_json::Value,
    ) -> crate::services::HttpResponse {
        let body = serde_json::json!({
            "title": "Poll PR",
            "body": null,
            "state": "open",
            "draft": false,
            "mergeable": mergeable,
            "mergeable_state": mergeable_state,
            "additions": 3,
            "deletions": 1,
            "merged": false,
            "head": { "sha": "headsha" }
        });
        crate::services::HttpResponse {
            status: 200,
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    fn token_response() -> crate::services::HttpResponse {
        let token_body = serde_json::json!({
            "token": "ghs_test",
            "expires_at": "2099-01-01T00:00:00Z"
        });
        crate::services::HttpResponse {
            status: 201,
            body: serde_json::to_vec(&token_body).unwrap(),
        }
    }

    fn empty_reviews_response() -> crate::services::HttpResponse {
        crate::services::HttpResponse {
            status: 200,
            body: serde_json::to_vec(&serde_json::json!([])).unwrap(),
        }
    }

    #[tokio::test]
    async fn fetch_pr_via_api_polls_past_null_mergeable_window() {
        use crate::services::testing::MockHttpClient;

        // First GET lands inside GitHub's compute window (`mergeable: null`);
        // the next resolves to a real value. The poll must surface MERGEABLE.
        let http = MockHttpClient::new()
            .respond_to("access_tokens", token_response())
            .respond_to("reviews", empty_reviews_response())
            .respond_to_sequence(
                "/pulls/7",
                vec![
                    pr_response(serde_json::Value::Null, serde_json::json!("unknown")),
                    pr_response(serde_json::json!(true), serde_json::json!("clean")),
                ],
            );

        let creds = mergeability_poll_creds();
        let parsed =
            fetch_pr_via_api_with_backoff(&http, &creds, "owner", "repo", 7, Duration::ZERO)
                .await
                .unwrap();

        assert_eq!(parsed.mergeable, MergeableState::Mergeable);
    }

    #[tokio::test]
    async fn fetch_pr_via_api_returns_unknown_when_mergeable_never_settles() {
        use crate::services::testing::MockHttpClient;

        // GitHub never finishes computing within the budget: every GET returns
        // `mergeable: null`. The poll must give up and return UNKNOWN, not hang.
        let http = MockHttpClient::new()
            .respond_to("access_tokens", token_response())
            .respond_to("reviews", empty_reviews_response())
            .respond_to(
                "/pulls/9",
                pr_response(serde_json::Value::Null, serde_json::json!("unknown")),
            );

        let creds = mergeability_poll_creds();
        let parsed =
            fetch_pr_via_api_with_backoff(&http, &creds, "owner", "repo", 9, Duration::ZERO)
                .await
                .unwrap();

        assert_eq!(parsed.mergeable, MergeableState::Unknown);
    }

    // ── reconcile_main_checkout_after_merge ─────────────────────

    use crate::services::GitOutput;

    fn ok_output() -> GitOutput {
        GitOutput {
            success: true,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    /// Expect exactly one `git` invocation whose first arg equals `verb`.
    fn expect_git_verb(git: &mut crate::services::testing::MockGitClient, verb: &'static str) {
        git.expect_run()
            .withf(move |_, args| args.first().map(String::as_str) == Some(verb))
            .times(1)
            .returning(|_, _| Ok(ok_output()));
    }

    #[test]
    fn reconcile_skips_if_on_non_default_branch() {
        use crate::services::testing::MockGitClient;

        // A checkout deliberately on a feature branch is never detached by the
        // export, so nothing is touched even when a pull was requested.
        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("feature-branch".to_string()));
        git.expect_pull().never();
        git.expect_run().never();

        let result = reconcile_main_checkout_after_merge(&git, "/repo", "main", true);
        assert!(result.is_ok());
    }

    #[test]
    fn reconcile_attached_no_pull_is_noop() {
        use crate::services::testing::MockGitClient;

        // Already on default, no pull requested (e.g. a child→integration merge
        // that never detached the default checkout): do nothing.
        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("main".to_string()));
        git.expect_pull().never();
        git.expect_run().never();
        git.expect_status().never();

        let result = reconcile_main_checkout_after_merge(&git, "/repo", "main", false);
        assert!(result.is_ok());
    }

    #[test]
    fn reconcile_attached_pulls_clean_tree() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("main".to_string()));
        git.expect_status().never();
        git.expect_pull().returning(|_, _, _| Ok(()));
        // Not detached: no HEAD re-attach.
        git.expect_run().never();

        let result = reconcile_main_checkout_after_merge(&git, "/repo", "main", true);
        assert!(result.is_ok());
    }

    #[test]
    fn reconcile_reattaches_detached_head_local_merge() {
        use crate::services::testing::MockGitClient;

        // The core regression: after the fold's `jj git export`, HEAD is detached
        // (`current_branch` empty). A LOCAL merge passes `pull = false`. The
        // checkout must be re-attached to the default branch and fast-forwarded,
        // with NO network pull. Before the fix this path was never reached.
        let mut git = MockGitClient::new();
        git.expect_current_branch().returning(|_| Ok(String::new()));
        // The canonical repair probes the tree; a clean tree proceeds to reset.
        git.expect_status().returning(|_| Ok(String::new()));
        expect_git_verb(&mut git, "symbolic-ref");
        expect_git_verb(&mut git, "reset");
        git.expect_pull().never();

        let result = reconcile_main_checkout_after_merge(&git, "/repo", "main", false);
        assert!(result.is_ok());
    }

    #[test]
    fn reconcile_reattaches_detached_head_then_pulls_remote_merge() {
        use crate::services::testing::MockGitClient;

        // Remote merge with `pull_on_merge`: re-attach AND pull. The re-attach is
        // not gated on the pull preference.
        let mut git = MockGitClient::new();
        git.expect_current_branch().returning(|_| Ok(String::new()));
        git.expect_status().returning(|_| Ok(String::new()));
        expect_git_verb(&mut git, "symbolic-ref");
        expect_git_verb(&mut git, "reset");
        git.expect_pull().times(1).returning(|_, _, _| Ok(()));

        let result = reconcile_main_checkout_after_merge(&git, "/repo", "main", true);
        assert!(result.is_ok());
    }

    #[test]
    fn reconcile_detached_dirty_reset_does_not_stash() {
        use crate::services::testing::MockGitClient;

        // The dirty-checkout gate runs before default-branch merges, so only the
        // allowlisted lockfile churn can remain. The canonical repair treats it as
        // clean and resets away, without touching the stash stack.
        let mut git = MockGitClient::new();
        git.expect_current_branch().returning(|_| Ok(String::new()));
        git.expect_status()
            .returning(|_| Ok(" M src-tauri/Cargo.lock\n".to_string()));
        expect_git_verb(&mut git, "symbolic-ref");
        expect_git_verb(&mut git, "reset");
        git.expect_pull().never();

        let result = reconcile_main_checkout_after_merge(&git, "/repo", "main", false);
        assert!(result.is_ok());
    }

    #[test]
    fn reconcile_fails_loud_when_reattach_command_fails() {
        use crate::services::testing::MockGitClient;

        // A failing `git symbolic-ref` (non-zero exit) must surface as an error,
        // not be silently swallowed.
        let mut git = MockGitClient::new();
        git.expect_current_branch().returning(|_| Ok(String::new()));
        git.expect_status().returning(|_| Ok(String::new()));
        git.expect_run()
            .withf(|_, args| args.first().map(String::as_str) == Some("symbolic-ref"))
            .returning(|_, _| {
                Ok(GitOutput {
                    success: false,
                    stdout: String::new(),
                    stderr: "fatal: boom".to_string(),
                })
            });

        let result = reconcile_main_checkout_after_merge(&git, "/repo", "main", false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("re-attach HEAD"));
    }

    // ── reattach_checkout_head clean-tree guard ─────────────────

    #[test]
    fn reattach_leaves_detached_and_skips_reset_when_tree_dirty() {
        use crate::services::testing::MockGitClient;

        // A tracked edit outside the regenerable-lockfile allowlist must block the
        // hard reset: HEAD stays detached (no symbolic-ref / reset) and the user's
        // work is preserved. This is the guard the non-merge export paths rely on,
        // where no pre-merge clean gate runs.
        let mut git = MockGitClient::new();
        git.expect_status()
            .returning(|_| Ok(" M src/main.rs\n".to_string()));
        git.expect_run().never();

        let result = reattach_checkout_head(&git, Path::new("/repo"), "main");
        assert_eq!(
            result,
            Ok(ReattachOutcome::DeclinedDirty),
            "the guard declines the repair, leaving HEAD detached"
        );
    }

    #[test]
    fn reattach_resets_when_only_allowlisted_lockfile_dirty() {
        use crate::services::testing::MockGitClient;

        // Allowlisted lockfile churn is treated as clean, so the repair proceeds.
        let mut git = MockGitClient::new();
        git.expect_status()
            .returning(|_| Ok(" M src-tauri/Cargo.lock\n".to_string()));
        expect_git_verb(&mut git, "symbolic-ref");
        expect_git_verb(&mut git, "reset");

        let result = reattach_checkout_head(&git, Path::new("/repo"), "main");
        assert_eq!(result, Ok(ReattachOutcome::Reattached));
    }

    #[test]
    fn reconcile_detached_dirty_with_pull_skips_pull_and_leaves_detached() {
        use crate::services::testing::MockGitClient;

        // Reachable when the checkout is dirtied AFTER the pre-merge clean gate but
        // BEFORE the reconcile (the merge path includes store + network work in
        // that window). The export wrapper leaves HEAD detached to preserve the
        // edits; the dirty guard then declines the re-attach here too, so the
        // reconcile must NOT pull into the still-detached, dirty checkout even
        // though `pull = true`.
        let mut git = MockGitClient::new();
        git.expect_current_branch().returning(|_| Ok(String::new()));
        git.expect_status()
            .returning(|_| Ok(" M src/main.rs\n".to_string()));
        git.expect_run().never(); // no symbolic-ref / reset
        git.expect_pull().never(); // and crucially, no pull into a detached dirty checkout

        let result = reconcile_main_checkout_after_merge(&git, "/repo", "main", true);
        assert!(result.is_ok());
    }
}
