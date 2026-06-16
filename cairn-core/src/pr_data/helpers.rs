//! Shared PR helper logic — pure computation and HTTP-composed helpers.
//!
//! Used by both the Tauri app and the headless server for PR state computation,
//! GitHub API fetching, and CI log extraction.

use crate::github::api;
use crate::github::credentials::GitHubAppCredentials;
use crate::models::{Check, CheckState, ChecksStatus, MergeableState, PrState, ReviewDecision};
use crate::services::{GitClient, HttpClient};
use std::path::{Path, PathBuf};
use std::time::Duration;

// ── Parsed PR Details ──────────────────────────────────────────

/// Intermediate struct returned by `fetch_pr_via_api`.
pub struct ParsedPrDetails {
    pub title: Option<String>,
    pub body: Option<String>,
    pub state: PrState,
    pub is_draft: bool,
    pub review_decision: Option<ReviewDecision>,
    pub mergeable: MergeableState,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
    pub head_sha: String,
}

// ── Pure Computation ───────────────────────────────────────────

/// Compute PR state from API response fields.
pub fn compute_pr_state(merged: bool, state_str: &str) -> PrState {
    if merged {
        PrState::Merged
    } else if state_str == "closed" {
        PrState::Closed
    } else {
        PrState::Open
    }
}

/// Compute mergeable state from API response fields.
pub fn compute_mergeable_state(
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
pub fn compute_review_decision(reviews: &[api::Review]) -> Option<ReviewDecision> {
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
pub fn compute_checks_status(checks: &[Check]) -> Option<ChecksStatus> {
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
pub async fn fetch_pr_via_api(
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
pub async fn fetch_checks_via_api(
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

/// Update main repository after PR merge by pulling latest changes.
///
/// Only pulls if the main repo is on the default branch.
/// Stashes any uncommitted changes before pulling, then pops them back.
pub fn update_main_repo_after_merge(
    git: &dyn GitClient,
    repo_path: &str,
    default_branch: &str,
) -> Result<(), String> {
    let repo = Path::new(repo_path);

    let current_branch = git.current_branch(repo)?;

    if current_branch != default_branch {
        log::info!(
            "Skipping pull: main repo is on '{}', not '{}'",
            current_branch,
            default_branch
        );
        return Ok(());
    }

    let status = git.status(repo)?;
    let has_changes = !status.trim().is_empty();

    let stashed = if has_changes {
        log::info!("Main repo has uncommitted changes, stashing...");
        match git.stash_push(repo, None) {
            Ok(_) => true,
            Err(e) => {
                log::warn!("Failed to stash changes: {}", e);
                return Err(format!("Could not stash changes in main repo: {}", e));
            }
        }
    } else {
        false
    };

    let pull_result = git.pull(repo, "origin", default_branch);

    if stashed {
        match git.stash_pop(repo) {
            Ok(_) => {
                log::info!("Successfully popped stash after pull");
            }
            Err(e) => {
                log::error!("Failed to pop stash after pull: {}", e);
                return Err(format!(
                    "Pulled changes but stash pop failed (conflicts?). \
                     Your changes are in 'git stash'. Error: {}",
                    e
                ));
            }
        }
    }

    pull_result?;
    log::info!(
        "Updated main repo at {} to latest {}",
        repo_path,
        default_branch
    );
    Ok(())
}

// ── Active Worktree Fast-Forward ───────────────────────────────

/// Fast-forward an active worktree checked out on `branch` to the merged tip.
///
/// This is the general case of "a checkout has the merged base branch and
/// should advance": a Coordinator's integration-branch worktree, into which
/// child PRs merge, is left stale otherwise. (The main repo's default-branch
/// checkout is the special case already handled by
/// `update_main_repo_after_merge`.)
///
/// Safe by construction: fetch, then `merge --ff-only`. If the worktree is on a
/// different branch, has uncommitted changes, or its branch has diverged (so a
/// fast-forward is impossible), this warns and leaves the worktree untouched —
/// it never force-updates or clobbers local work.
pub fn update_worktree_after_merge(
    git: &dyn GitClient,
    worktree_path: &Path,
    branch: &str,
) -> Result<(), String> {
    let current = git.current_branch(worktree_path)?;
    if current != branch {
        log::warn!(
            "Skipping worktree fast-forward: {} is on '{}', not '{}'",
            worktree_path.display(),
            current,
            branch
        );
        return Ok(());
    }

    let status = git.status(worktree_path)?;
    if !status.trim().is_empty() {
        log::warn!(
            "Skipping worktree fast-forward for {}: working tree has uncommitted changes",
            worktree_path.display()
        );
        return Ok(());
    }

    let fetch = git.run(
        worktree_path,
        vec![
            "fetch".to_string(),
            "origin".to_string(),
            branch.to_string(),
        ],
    )?;
    if !fetch.success {
        return Err(format!("git fetch failed: {}", fetch.stderr));
    }

    let ff = git.run(
        worktree_path,
        vec![
            "merge".to_string(),
            "--ff-only".to_string(),
            format!("origin/{branch}"),
        ],
    )?;
    if !ff.success {
        return Err(format!(
            "fast-forward failed (branch diverged?): {}",
            ff.stderr
        ));
    }

    log::info!(
        "Fast-forwarded worktree {} to merged {}",
        worktree_path.display(),
        branch
    );
    Ok(())
}

fn run_git(
    git: &dyn GitClient,
    repo: &Path,
    args: &[&str],
) -> Result<crate::services::GitOutput, String> {
    git.run(repo, args.iter().map(|arg| arg.to_string()).collect())
}

fn cleanup_local_merge(git: &dyn GitClient, repo_path: &Path, worktree: &Path, ephemeral: bool) {
    let _ = run_git(git, worktree, &["merge", "--abort"]);
    let _ = run_git(git, worktree, &["reset", "--hard"]);
    if ephemeral {
        let _ = git.worktree_remove(repo_path, worktree, true);
    }
}

#[derive(Debug, Clone)]
struct LocalMergeStash {
    sha: String,
    label: String,
}

fn stash_label_for_sha(git: &dyn GitClient, worktree: &Path, sha: &str) -> Result<String, String> {
    let list = run_git(git, worktree, &["stash", "list", "--format=%gd %H"])?;
    if !list.success {
        return Err(format!("git stash list failed: {}", list.stderr));
    }

    for line in list.stdout.lines() {
        let mut parts = line.split_whitespace();
        let Some(label) = parts.next() else {
            continue;
        };
        let Some(entry_sha) = parts.next() else {
            continue;
        };
        if entry_sha == sha {
            return Ok(label.to_string());
        }
    }

    Err(format!(
        "could not find local merge stash {sha} in git stash list"
    ))
}

fn stash_dirty_local_merge_target(
    git: &dyn GitClient,
    worktree: &Path,
    target_branch: &str,
) -> Result<Option<LocalMergeStash>, String> {
    let status = git.status(worktree)?;
    if status.trim().is_empty() {
        return Ok(None);
    }

    let message = format!("cairn-local-merge {target_branch}");
    log::info!(
        "Target branch {} has uncommitted changes in {}; stashing before local merge",
        target_branch,
        worktree.display()
    );
    let push = run_git(
        git,
        worktree,
        &["stash", "push", "--include-untracked", "-m", &message],
    )?;
    if !push.success {
        return Err(format!(
            "Could not stash uncommitted changes in target branch {target_branch} at {} before local merge: {}",
            worktree.display(),
            if push.stderr.is_empty() { push.stdout } else { push.stderr }
        ));
    }

    let stash_sha = git.rev_parse(worktree, vec!["refs/stash".to_string()])?;
    let label = stash_label_for_sha(git, worktree, &stash_sha)?;
    log::info!(
        "Stashed target branch changes for local merge as {} ({})",
        label,
        stash_sha
    );

    Ok(Some(LocalMergeStash {
        sha: stash_sha,
        label,
    }))
}

fn pop_local_merge_stash(
    git: &dyn GitClient,
    worktree: &Path,
    stash: &LocalMergeStash,
) -> Result<(), String> {
    let label = stash_label_for_sha(git, worktree, &stash.sha)?;
    let pop = run_git(git, worktree, &["stash", "pop", &label])?;
    if pop.success {
        log::info!("Restored local merge target changes from {label}");
        return Ok(());
    }

    Err(format!(
        "git stash pop {label} failed: {}",
        if pop.stderr.is_empty() {
            pop.stdout
        } else {
            pop.stderr
        }
    ))
}

fn restore_local_merge_stash_after_failure(
    git: &dyn GitClient,
    worktree: &Path,
    stash: &Option<LocalMergeStash>,
    failure_context: &str,
) -> Result<(), String> {
    let Some(stash) = stash else {
        return Ok(());
    };

    pop_local_merge_stash(git, worktree, stash).map_err(|error| {
        log::error!(
            "{}; additionally failed to restore local merge stash {} ({}): {}",
            failure_context,
            stash.label,
            stash.sha,
            error
        );
        format!(
            "{failure_context}; attempted to restore stashed target changes, but {error}. Your changes remain in the git stash entry originally captured as {} ({}); recover manually with `git stash apply {}` in {}.",
            stash.label,
            stash.sha,
            stash.label,
            worktree.display()
        )
    })
}

fn local_merge_worktree_path() -> Result<PathBuf, String> {
    let mut base = crate::git::worktree::worktree_base_dir()?;
    base.push(format!("local-merge-{}", uuid::Uuid::new_v4()));
    Ok(base)
}

/// Compute local mergeability without touching the working tree.
pub fn compute_local_mergeable(
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
pub fn local_pr_files(
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

/// Merge a local PR-equivalent into its target branch.
pub fn local_merge(
    git: &dyn GitClient,
    repo_path: &Path,
    target_branch: &str,
    source_branch: &str,
    method: &str,
    title: &str,
) -> Result<String, String> {
    log::info!(
        "Preparing local merge: {} -> {} using {} in {}",
        source_branch,
        target_branch,
        method,
        repo_path.display()
    );
    let existing =
        crate::git::worktree::get_worktree_for_branch_with_git(git, repo_path, target_branch)?;
    let (worktree, ephemeral, stashed_target_changes) = if let Some(path) = existing {
        let stash = stash_dirty_local_merge_target(git, &path, target_branch)?;
        (path, false, stash)
    } else {
        let path = local_merge_worktree_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create local merge worktree dir: {e}"))?;
        }
        git.worktree_add_existing_branch(repo_path, &path, target_branch)?;
        (path, true, None)
    };

    log::info!(
        "Running local merge in worktree {} (ephemeral={})",
        worktree.display(),
        ephemeral
    );

    let merge_result = match method {
        "merge" | "merge-commit" => run_git(
            git,
            &worktree,
            &[
                "-c",
                "user.name=Cairn",
                "-c",
                "user.email=cairn@localhost",
                "merge",
                "--no-ff",
                source_branch,
                "-m",
                title,
            ],
        ),
        "squash" => {
            let squash = run_git(git, &worktree, &["merge", "--squash", source_branch]);
            match squash {
                Ok(output) if output.success => run_git(
                    git,
                    &worktree,
                    &[
                        "-c",
                        "user.name=Cairn",
                        "-c",
                        "user.email=cairn@localhost",
                        "commit",
                        "-m",
                        title,
                    ],
                ),
                other => other,
            }
        }
        "rebase" => {
            cleanup_local_merge(git, repo_path, &worktree, ephemeral);
            restore_local_merge_stash_after_failure(
                git,
                &worktree,
                &stashed_target_changes,
                "Local rebase merge is not supported yet",
            )?;
            return Err("Local rebase merge is not supported yet".to_string());
        }
        other => {
            let error = format!("Unsupported local merge method: {other}");
            cleanup_local_merge(git, repo_path, &worktree, ephemeral);
            restore_local_merge_stash_after_failure(
                git,
                &worktree,
                &stashed_target_changes,
                &error,
            )?;
            return Err(error);
        }
    };

    match merge_result {
        Ok(output) if output.success => {}
        Ok(output) => {
            cleanup_local_merge(git, repo_path, &worktree, ephemeral);
            let error = format!(
                "local merge conflict while merging {source_branch} into {target_branch}: {}",
                if output.stderr.is_empty() {
                    output.stdout
                } else {
                    output.stderr
                }
            );
            restore_local_merge_stash_after_failure(
                git,
                &worktree,
                &stashed_target_changes,
                &error,
            )?;
            return Err(error);
        }
        Err(error) => {
            cleanup_local_merge(git, repo_path, &worktree, ephemeral);
            let error = format!(
                "local merge conflict while merging {source_branch} into {target_branch}: {error}"
            );
            restore_local_merge_stash_after_failure(
                git,
                &worktree,
                &stashed_target_changes,
                &error,
            )?;
            return Err(error);
        }
    }

    let merged_commit = git.rev_parse(&worktree, vec!["HEAD".to_string()])?;
    if let Some(stash) = &stashed_target_changes {
        if let Err(error) = pop_local_merge_stash(git, &worktree, stash) {
            log::error!(
                "Local merge succeeded at {}, but restoring target changes from {} ({}) failed: {}",
                merged_commit,
                stash.label,
                stash.sha,
                error
            );
            return Err(format!(
                "local merge succeeded at {merged_commit}, but restoring stashed target changes failed: {error}. The stash was preserved as {} ({}); recover manually with `git stash apply {}` in {}.",
                stash.label,
                stash.sha,
                stash.label,
                worktree.display()
            ));
        }
    }
    if ephemeral {
        git.worktree_remove(repo_path, &worktree, false).map_err(|error| {
            format!(
                "local merge succeeded at {merged_commit}, but failed to remove temporary worktree {}: {error}",
                worktree.display()
            )
        })?;
    }
    log::info!(
        "Completed local merge: {} -> {} at {}",
        source_branch,
        target_branch,
        merged_commit
    );
    Ok(merged_commit)
}

/// Locate an active worktree checked out on `branch` and fast-forward it.
///
/// Skips the main repository checkout at `repo_path` — its default-branch
/// update is handled by `update_main_repo_after_merge`, and
/// `git worktree list --porcelain` lists the main working tree first, so a
/// merge into the default branch resolves here to `repo_path` and is a no-op.
/// Non-fatal: returns `Err` on a lookup/fast-forward failure for the caller to
/// log, but never force-updates.
pub fn fast_forward_active_worktree(
    git: &dyn GitClient,
    repo_path: &Path,
    branch: &str,
) -> Result<(), String> {
    match crate::git::worktree::get_worktree_for_branch_with_git(git, repo_path, branch)? {
        Some(worktree) if worktree.as_path() != repo_path => {
            update_worktree_after_merge(git, &worktree, branch)
        }
        _ => {
            log::debug!(
                "No separate active worktree on '{}' to fast-forward (main repo handled by pull)",
                branch
            );
            Ok(())
        }
    }
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

    fn git_stdout(path: &std::path::Path, args: &[&str]) -> String {
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
        String::from_utf8_lossy(&output.stdout).trim().to_string()
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
    fn local_merge_squash_merges_non_remote_repo() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        git(repo, &["init"]);
        git(repo, &["checkout", "-B", "main"]);

        std::fs::write(repo.join("file.txt"), "base\n").unwrap();
        git(repo, &["add", "-A"]);
        git(
            repo,
            &[
                "-c",
                "user.email=test@test.com",
                "-c",
                "user.name=Test User",
                "commit",
                "-m",
                "base",
            ],
        );

        git(repo, &["checkout", "-b", "agent/test-local-pr"]);
        std::fs::write(repo.join("file.txt"), "base\nfeature\n").unwrap();
        git(repo, &["add", "-A"]);
        git(
            repo,
            &[
                "-c",
                "user.email=test@test.com",
                "-c",
                "user.name=Test User",
                "commit",
                "-m",
                "feature",
            ],
        );
        git(repo, &["checkout", "main"]);

        let merged_commit = local_merge(
            &crate::services::RealGitClient,
            repo,
            "main",
            "agent/test-local-pr",
            "squash",
            "Local PR",
        )
        .unwrap();

        assert!(!merged_commit.is_empty());
        assert_eq!(
            std::fs::read_to_string(repo.join("file.txt")).unwrap(),
            "base\nfeature\n"
        );
        let status = crate::services::RealGitClient.status(repo).unwrap();
        assert!(status.is_empty(), "local merge left dirty status: {status}");
        let branch = crate::services::RealGitClient.current_branch(repo).unwrap();
        assert_eq!(branch, "main");
    }

    #[test]
    fn local_merge_stashes_dirty_target_and_restores_changes() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        git(repo, &["init"]);
        git(repo, &["checkout", "-B", "main"]);

        std::fs::write(repo.join("file.txt"), "base\n").unwrap();
        std::fs::write(repo.join("target.txt"), "clean\n").unwrap();
        git(repo, &["add", "-A"]);
        git(
            repo,
            &[
                "-c",
                "user.email=test@test.com",
                "-c",
                "user.name=Test User",
                "commit",
                "-m",
                "base",
            ],
        );

        git(repo, &["checkout", "-b", "agent/test-local-pr"]);
        std::fs::write(repo.join("file.txt"), "base\nfeature\n").unwrap();
        git(repo, &["add", "-A"]);
        git(
            repo,
            &[
                "-c",
                "user.email=test@test.com",
                "-c",
                "user.name=Test User",
                "commit",
                "-m",
                "feature",
            ],
        );
        git(repo, &["checkout", "main"]);

        std::fs::write(repo.join("target.txt"), "dirty target change\n").unwrap();
        std::fs::write(repo.join("untracked.txt"), "untracked target change\n").unwrap();

        let merged_commit = local_merge(
            &crate::services::RealGitClient,
            repo,
            "main",
            "agent/test-local-pr",
            "squash",
            "Local PR",
        )
        .unwrap();

        assert!(!merged_commit.is_empty());
        assert_eq!(
            std::fs::read_to_string(repo.join("file.txt")).unwrap(),
            "base\nfeature\n"
        );
        assert_eq!(
            std::fs::read_to_string(repo.join("target.txt")).unwrap(),
            "dirty target change\n"
        );
        assert_eq!(
            std::fs::read_to_string(repo.join("untracked.txt")).unwrap(),
            "untracked target change\n"
        );
        let status = crate::services::RealGitClient.status(repo).unwrap();
        assert!(
            status.contains("target.txt"),
            "missing dirty tracked file: {status}"
        );
        assert!(
            status.contains("untracked.txt"),
            "missing dirty untracked file: {status}"
        );
        let stash_list = git_stdout(repo, &["stash", "list"]);
        assert!(
            !stash_list.contains("cairn-local-merge main"),
            "local merge stash was not dropped: {stash_list}"
        );
    }

    #[test]
    fn local_merge_pop_conflict_preserves_stash_and_reports_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        git(repo, &["init"]);
        git(repo, &["checkout", "-B", "main"]);

        std::fs::write(repo.join("conflict.txt"), "base\n").unwrap();
        git(repo, &["add", "-A"]);
        git(
            repo,
            &[
                "-c",
                "user.email=test@test.com",
                "-c",
                "user.name=Test User",
                "commit",
                "-m",
                "base",
            ],
        );

        git(repo, &["checkout", "-b", "agent/test-local-pr"]);
        std::fs::write(repo.join("conflict.txt"), "feature\n").unwrap();
        git(repo, &["add", "-A"]);
        git(
            repo,
            &[
                "-c",
                "user.email=test@test.com",
                "-c",
                "user.name=Test User",
                "commit",
                "-m",
                "feature",
            ],
        );
        git(repo, &["checkout", "main"]);
        std::fs::write(repo.join("conflict.txt"), "dirty target change\n").unwrap();

        let error = local_merge(
            &crate::services::RealGitClient,
            repo,
            "main",
            "agent/test-local-pr",
            "squash",
            "Local PR",
        )
        .unwrap_err();

        assert!(
            error.contains("local merge succeeded at"),
            "missing merge-success context: {error}"
        );
        assert!(
            error.contains("restoring stashed target changes failed"),
            "missing restore-failure context: {error}"
        );
        assert!(
            error.contains("stash was preserved as stash@{"),
            "missing named stash recovery: {error}"
        );
        let stash_list = git_stdout(repo, &["stash", "list"]);
        assert!(
            stash_list.contains("cairn-local-merge main"),
            "conflicted pop dropped the stash: {stash_list}"
        );
        let conflict_file = std::fs::read_to_string(repo.join("conflict.txt")).unwrap();
        assert!(
            conflict_file.contains("<<<<<<<"),
            "expected pop conflict markers, got: {conflict_file}"
        );
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

    // ── update_main_repo_after_merge ────────────────────────────

    #[test]
    fn update_main_repo_skips_if_not_on_default_branch() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("feature-branch".to_string()));
        git.expect_pull().never();

        let result = update_main_repo_after_merge(&git, "/repo", "main");
        assert!(result.is_ok());
    }

    #[test]
    fn update_main_repo_pulls_clean_tree() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("main".to_string()));
        git.expect_status().returning(|_| Ok("".to_string()));
        git.expect_pull().returning(|_, _, _| Ok(()));

        let result = update_main_repo_after_merge(&git, "/repo", "main");
        assert!(result.is_ok());
    }

    #[test]
    fn update_main_repo_stashes_and_pops_dirty_tree() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("main".to_string()));
        git.expect_status()
            .returning(|_| Ok(" M dirty-file.rs".to_string()));
        git.expect_stash_push().returning(|_, _| Ok(()));
        git.expect_pull().returning(|_, _, _| Ok(()));
        git.expect_stash_pop().returning(|_| Ok(()));

        let result = update_main_repo_after_merge(&git, "/repo", "main");
        assert!(result.is_ok());
    }

    #[test]
    fn update_main_repo_returns_error_on_stash_pop_failure() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("main".to_string()));
        git.expect_status()
            .returning(|_| Ok(" M dirty-file.rs".to_string()));
        git.expect_stash_push().returning(|_, _| Ok(()));
        git.expect_pull().returning(|_, _, _| Ok(()));
        git.expect_stash_pop()
            .returning(|_| Err("conflict".to_string()));

        let result = update_main_repo_after_merge(&git, "/repo", "main");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("stash pop failed"));
    }

    #[test]
    fn update_main_repo_returns_error_on_stash_push_failure() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("main".to_string()));
        git.expect_status()
            .returning(|_| Ok(" M dirty-file.rs".to_string()));
        git.expect_stash_push()
            .returning(|_, _| Err("stash failed".to_string()));

        let result = update_main_repo_after_merge(&git, "/repo", "main");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Could not stash"));
    }

    // ── update_worktree_after_merge ──────────────────────────

    #[test]
    fn update_worktree_fast_forwards_clean_matching_branch() {
        use crate::services::testing::MockGitClient;
        use crate::services::GitOutput;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("agent/CAI-1-coordinator-0".to_string()));
        git.expect_status().returning(|_| Ok(String::new()));
        // Asserts both halves fire exactly once: fetch, then ff-only merge.
        git.expect_run()
            .withf(|_, args| args.first().map(String::as_str) == Some("fetch"))
            .times(1)
            .returning(|_, _| {
                Ok(GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });
        git.expect_run()
            .withf(|_, args| {
                args.first().map(String::as_str) == Some("merge")
                    && args.iter().any(|a| a == "--ff-only")
            })
            .times(1)
            .returning(|_, _| {
                Ok(GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });

        let result =
            update_worktree_after_merge(&git, Path::new("/wt/coord"), "agent/CAI-1-coordinator-0");
        assert!(result.is_ok());
    }

    #[test]
    fn update_worktree_skips_dirty_tree() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("integration".to_string()));
        git.expect_status()
            .returning(|_| Ok(" M src/lib.rs".to_string()));
        // A dirty tree must never trigger fetch/merge.
        git.expect_run().never();

        let result = update_worktree_after_merge(&git, Path::new("/wt/coord"), "integration");
        assert!(result.is_ok());
    }

    #[test]
    fn update_worktree_skips_wrong_branch() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("some-other-branch".to_string()));
        // A branch mismatch short-circuits before status/run.
        git.expect_status().never();
        git.expect_run().never();

        let result = update_worktree_after_merge(&git, Path::new("/wt/coord"), "integration");
        assert!(result.is_ok());
    }

    #[test]
    fn update_worktree_errors_when_fast_forward_impossible() {
        use crate::services::testing::MockGitClient;
        use crate::services::GitOutput;

        let mut git = MockGitClient::new();
        git.expect_current_branch()
            .returning(|_| Ok("integration".to_string()));
        git.expect_status().returning(|_| Ok(String::new()));
        git.expect_run()
            .withf(|_, args| args.first().map(String::as_str) == Some("fetch"))
            .returning(|_, _| {
                Ok(GitOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            });
        git.expect_run()
            .withf(|_, args| args.first().map(String::as_str) == Some("merge"))
            .returning(|_, _| {
                Ok(GitOutput {
                    success: false,
                    stdout: String::new(),
                    stderr: "Not possible to fast-forward".to_string(),
                })
            });

        let result = update_worktree_after_merge(&git, Path::new("/wt/coord"), "integration");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("fast-forward failed"));
    }

    // ── fast_forward_active_worktree: dispatch + main-repo skip ──

    const PORCELAIN_TWO_WORKTREES: &str = "\
worktree /repo
HEAD abc
branch refs/heads/main

worktree /wt/coord
HEAD def
branch refs/heads/integration
";

    #[test]
    fn fast_forward_active_worktree_advances_separate_worktree() {
        use crate::services::testing::MockGitClient;
        use crate::services::GitOutput;

        let mut git = MockGitClient::new();
        git.expect_worktree_list()
            .returning(|_| Ok(PORCELAIN_TWO_WORKTREES.to_string()));
        // The `integration` worktree is distinct from /repo → it is fast-forwarded.
        git.expect_current_branch()
            .returning(|_| Ok("integration".to_string()));
        git.expect_status().returning(|_| Ok(String::new()));
        git.expect_run().returning(|_, _| {
            Ok(GitOutput {
                success: true,
                stdout: String::new(),
                stderr: String::new(),
            })
        });

        let result = fast_forward_active_worktree(&git, Path::new("/repo"), "integration");
        assert!(result.is_ok());
    }

    #[test]
    fn fast_forward_active_worktree_skips_main_repo_on_default_branch() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_worktree_list()
            .returning(|_| Ok(PORCELAIN_TWO_WORKTREES.to_string()));
        // `main` resolves to the main repo entry (/repo), so no fast-forward runs:
        // the existing main-repo pull owns the default branch (no regression).
        git.expect_current_branch().never();
        git.expect_status().never();
        git.expect_run().never();

        let result = fast_forward_active_worktree(&git, Path::new("/repo"), "main");
        assert!(result.is_ok());
    }

    #[test]
    fn fast_forward_active_worktree_skips_when_branch_not_checked_out() {
        use crate::services::testing::MockGitClient;

        let mut git = MockGitClient::new();
        git.expect_worktree_list()
            .returning(|_| Ok("worktree /repo\nHEAD abc\nbranch refs/heads/main\n".to_string()));
        git.expect_current_branch().never();
        git.expect_run().never();

        let result = fast_forward_active_worktree(&git, Path::new("/repo"), "integration");
        assert!(result.is_ok());
    }
}
