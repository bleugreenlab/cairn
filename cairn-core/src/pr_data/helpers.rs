//! Shared PR helper logic — pure computation and HTTP-composed helpers.
//!
//! Used by both the Tauri app and the headless server for PR state computation,
//! GitHub API fetching, and CI log extraction.

use crate::github::api;
use crate::github::credentials::GitHubAppCredentials;
use crate::models::{Check, CheckState, ChecksStatus, MergeableState, PrState, ReviewDecision};
use crate::services::{GitClient, HttpClient};
use std::path::Path;

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

/// Fetch and parse PR details from GitHub, including reviews.
pub async fn fetch_pr_via_api(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<ParsedPrDetails, String> {
    let pr = api::fetch_pr(http, creds, owner, repo, pr_number).await?;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
