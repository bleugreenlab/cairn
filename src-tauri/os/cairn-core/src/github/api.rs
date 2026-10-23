//! GitHub REST API client — shared between Tauri and cairn-server.
//!
//! Contains the subset of GitHub API operations needed by cairn-core: repo URL
//! parsing, PR reads and merges, branch deletion, and workflow log retrieval.
//!
//! It holds no credential and never sees one. Every operation takes an
//! authority — an [`AppAuthority`] or an [`InstallationAuthority`] minted by
//! `security::broker::github` — and asks *it* to send the request, so the URL
//! and the credential are bound in one call and no header ever comes back here.
//! Signing the app JWT and exchanging it for an installation token used to live
//! in this file, which is how a model-callable handler came to pass an RSA
//! private key around by reference.

use crate::security::broker::github::{AppAuthority, InstallationAuthority, API_BASE};
use crate::services::{HttpClient, HttpResponse};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::process::Command;

// Named as it always was at the call sites below; the constant itself now lives
// beside the broker that authenticates against it.
use API_BASE as GITHUB_API_BASE;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AppInstallation {
    pub id: i64,
    pub account: AppInstallationAccount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseMergeCheckSpec {
    pub candidate_oid: String,
    pub intent_digest_sha256: String,
    pub details_url: Option<String>,
}

impl BaseMergeCheckSpec {
    fn request_body(&self) -> Result<serde_json::Value, String> {
        if !matches!(self.candidate_oid.len(), 40 | 64)
            || !self
                .candidate_oid
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("candidate oid must be a canonical Git object id".to_string());
        }
        if self.intent_digest_sha256.len() != 64
            || !self
                .intent_digest_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("intent digest must be lowercase SHA-256 hex".to_string());
        }
        Ok(serde_json::json!({
            "name": HUMAN_BASE_MERGE_CHECK_NAME,
            "head_sha": self.candidate_oid,
            "status": "completed",
            "conclusion": "success",
            "external_id": self.intent_digest_sha256,
            "details_url": self.details_url,
            "output": {
                "title": "Provably-human base merge approved",
                "summary": "Cairn accepted a one-shot human approval bound to this exact merge candidate."
            }
        }))
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PublishedCheckRun {
    pub id: i64,
    pub node_id: String,
    pub name: String,
    pub head_sha: String,
    pub external_id: Option<String>,
}

pub async fn create_base_merge_check_run(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    spec: &BaseMergeCheckSpec,
) -> Result<PublishedCheckRun, String> {
    let response = auth
        .post(
            http,
            &format!("{GITHUB_API_BASE}/repos/{owner}/{repo}/check-runs"),
            spec.request_body()?,
        )
        .await?;
    ensure_github_api_success(&response)?;
    let published: PublishedCheckRun = response.json()?;
    if published.name != HUMAN_BASE_MERGE_CHECK_NAME
        || published.head_sha != spec.candidate_oid
        || published.external_id.as_deref() != Some(spec.intent_digest_sha256.as_str())
    {
        return Err(
            "GitHub returned a check run that does not match the approved candidate".into(),
        );
    }
    Ok(published)
}

pub const HUMAN_BASE_MERGE_RULESET_NAME: &str = "Cairn provably-human base merge";
pub const HUMAN_BASE_MERGE_CHECK_NAME: &str = "Cairn / provably-human base merge";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseMergeRulesetSpec {
    pub base_branch: String,
    pub required_check_name: String,
    pub github_app_integration_id: i64,
}

impl BaseMergeRulesetSpec {
    pub fn request_body(&self) -> Result<serde_json::Value, String> {
        let base_branch = self.base_branch.trim();
        let check_name = self.required_check_name.trim();
        if base_branch.is_empty() || base_branch.starts_with("refs/") {
            return Err("base branch must be a non-empty short branch name".to_string());
        }
        if check_name.is_empty() {
            return Err("required check name must not be blank".to_string());
        }
        if self.github_app_integration_id <= 0 {
            return Err("GitHub App integration id must be positive".to_string());
        }
        Ok(serde_json::json!({
            "name": HUMAN_BASE_MERGE_RULESET_NAME,
            "target": "branch",
            "enforcement": "active",
            "bypass_actors": [],
            "conditions": {
                "ref_name": {
                    "include": [format!("refs/heads/{base_branch}")],
                    "exclude": []
                }
            },
            "rules": [
                {"type": "deletion"},
                {"type": "non_fast_forward"},
                {
                    "type": "pull_request",
                    "parameters": {
                        "dismiss_stale_reviews_on_push": true,
                        "require_code_owner_review": false,
                        "require_last_push_approval": false,
                        "required_approving_review_count": 0,
                        "required_review_thread_resolution": false,
                        "allowed_merge_methods": ["merge", "squash", "rebase"]
                    }
                },
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": true,
                        "do_not_enforce_on_create": false,
                        "required_status_checks": [{
                            "context": check_name,
                            "integration_id": self.github_app_integration_id
                        }]
                    }
                }
            ]
        }))
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RepositoryRulesetSummary {
    pub id: i64,
    pub name: String,
    pub target: String,
    pub enforcement: String,
}

pub async fn list_repository_rulesets(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
) -> Result<Vec<RepositoryRulesetSummary>, String> {
    github_get_json(
        http,
        auth,
        &format!("{GITHUB_API_BASE}/repos/{owner}/{repo}/rulesets"),
    )
    .await
}

pub async fn get_repository_ruleset(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    ruleset_id: i64,
) -> Result<serde_json::Value, String> {
    github_get_json(
        http,
        auth,
        &format!("{GITHUB_API_BASE}/repos/{owner}/{repo}/rulesets/{ruleset_id}"),
    )
    .await
}

pub async fn create_base_merge_ruleset(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    spec: &BaseMergeRulesetSpec,
) -> Result<RepositoryRulesetSummary, String> {
    let response = auth
        .post(
            http,
            &format!("{GITHUB_API_BASE}/repos/{owner}/{repo}/rulesets"),
            spec.request_body()?,
        )
        .await?;
    ensure_github_api_success(&response)?;
    response.json()
}

pub async fn update_base_merge_ruleset(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    ruleset_id: i64,
    spec: &BaseMergeRulesetSpec,
) -> Result<RepositoryRulesetSummary, String> {
    let response = auth
        .put(
            http,
            &format!("{GITHUB_API_BASE}/repos/{owner}/{repo}/rulesets/{ruleset_id}"),
            spec.request_body()?,
        )
        .await?;
    ensure_github_api_success(&response)?;
    response.json()
}

pub fn verify_base_merge_ruleset(
    ruleset: &serde_json::Value,
    spec: &BaseMergeRulesetSpec,
) -> Result<(), String> {
    let expected = spec.request_body()?;
    for key in [
        "name",
        "target",
        "enforcement",
        "bypass_actors",
        "conditions",
        "rules",
    ] {
        if ruleset.get(key) != expected.get(key) {
            return Err(format!("provider ruleset drifted at {key}"));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AppInstallationAccount {
    pub login: String,
    #[serde(rename = "type")]
    pub account_type: String,
}

/// List the installations currently authorized for this GitHub App.
pub async fn list_app_installations(
    http: &dyn HttpClient,
    app: &AppAuthority,
) -> Result<Vec<AppInstallation>, String> {
    let mut installations = Vec::new();
    for page in 1.. {
        let resp = app
            .get(
                http,
                &format!("{GITHUB_API_BASE}/app/installations?per_page=100&page={page}"),
            )
            .await?;
        ensure_github_api_success(&resp)?;
        let mut page_installations: Vec<AppInstallation> = resp.json()?;
        let is_last_page = page_installations.len() < 100;
        installations.append(&mut page_installations);
        if is_last_page {
            return Ok(installations);
        }
    }
    unreachable!("installation pagination always returns from a finite page")
}

/// Revoke exactly one GitHub App installation.
pub async fn revoke_app_installation(
    http: &dyn HttpClient,
    app: &AppAuthority,
    installation_id: i64,
) -> Result<(), String> {
    let resp = app
        .delete(
            http,
            &format!("{GITHUB_API_BASE}/app/installations/{installation_id}"),
        )
        .await?;
    ensure_github_api_success(&resp)
}

// Authentication — signing the app JWT, exchanging it for an installation
// token, and holding that token between calls — lives in
// `security::broker::github`. It used to live here, which is how a
// model-callable handler came to pass an RSA private key around by reference,
// and how minted tokens came to sit in a process-global cache nothing could
// revoke.

// ── Rate limit ──────────────────────────────────────────────────

/// Parse rate limit error from GitHub API response.
fn parse_rate_limit_error(resp: &HttpResponse) -> Option<u64> {
    if let Ok(body) = resp.json::<serde_json::Value>() {
        if body
            .get("message")
            .and_then(|m| m.as_str())
            .map(|m| m.to_lowercase().contains("rate limit"))
            .unwrap_or(false)
        {
            return Some(60);
        }
    }
    None
}

fn check_rate_limit(resp: &HttpResponse) -> Result<(), String> {
    if resp.status == 429 {
        let wait_secs = parse_rate_limit_error(resp).unwrap_or(60);
        return Err(format!(
            "GitHub API rate limit exceeded. Please wait {} seconds before retrying.",
            wait_secs
        ));
    }
    Ok(())
}

fn github_api_error(resp: &HttpResponse) -> String {
    format!("GitHub API error: {} - {}", resp.status, resp.text())
}

fn ensure_github_api_success(resp: &HttpResponse) -> Result<(), String> {
    check_rate_limit(resp)?;
    if !resp.is_success() {
        return Err(github_api_error(resp));
    }
    Ok(())
}

async fn github_get_response(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    url: &str,
) -> Result<HttpResponse, String> {
    let resp = auth.get(http, url).await?;
    ensure_github_api_success(&resp)?;
    Ok(resp)
}

async fn github_get_json<T: DeserializeOwned>(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    url: &str,
) -> Result<T, String> {
    github_get_response(http, auth, url).await?.json()
}

// ── API Response Types ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PullRequest {
    pub(crate) title: String,
    pub(crate) body: Option<String>,
    pub(crate) state: String,
    pub(crate) draft: bool,
    pub(crate) mergeable: Option<bool>,
    pub(crate) mergeable_state: Option<String>,
    pub(crate) additions: i32,
    pub(crate) deletions: i32,
    pub(crate) merged: bool,
    pub(crate) head: PrHead,
}

#[derive(Debug, Deserialize)]
pub struct PrHead {
    pub(crate) sha: String,
}

#[derive(Debug, Deserialize)]
pub struct CheckRunsResponse {
    pub(crate) check_runs: Vec<CheckRun>,
}

#[derive(Debug, Deserialize)]
pub struct CheckRun {
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) conclusion: Option<String>,
    pub(crate) html_url: String,
    pub(crate) output: CheckRunOutput,
}

#[derive(Debug, Deserialize)]
pub struct CheckRunOutput {
    pub(crate) summary: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Review {
    pub(crate) state: String,
    pub(crate) user: User,
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub(crate) login: String,
}

#[derive(Debug, Deserialize)]
pub struct JobsResponse {
    pub(crate) jobs: Vec<Job>,
}

#[derive(Debug, Deserialize)]
pub struct Job {
    pub(crate) name: String,
    pub(crate) steps: Option<Vec<Step>>,
}

#[derive(Debug, Deserialize)]
pub struct Step {
    pub(crate) name: String,
    pub(crate) conclusion: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PrFile {
    pub(crate) filename: String,
    pub(crate) status: String,
    pub(crate) additions: i32,
    pub(crate) deletions: i32,
    pub(crate) changes: i32,
    pub(crate) patch: Option<String>,
    pub(crate) previous_filename: Option<String>,
}

// ── URL Parsing ─────────────────────────────────────────────────

/// Extract owner and repo from a GitHub URL or repo path.
pub(crate) fn parse_repo_from_url(url: &str) -> Result<(String, String), String> {
    let url = url.trim_end_matches(".git");

    if url.contains("github.com") {
        let parts: Vec<&str> = url.split('/').collect();
        if parts.len() >= 2 {
            let repo = parts[parts.len() - 1];
            let owner = parts[parts.len() - 2]
                .split(':')
                .next_back()
                .unwrap_or(parts[parts.len() - 2]);
            return Ok((owner.to_string(), repo.to_string()));
        }
    } else if url.contains('/') {
        let parts: Vec<&str> = url.split('/').collect();
        if parts.len() == 2 {
            return Ok((parts[0].to_string(), parts[1].to_string()));
        }
    }

    Err(format!("Could not parse owner/repo from: {}", url))
}

/// Get repo remote URL from git directory.
pub(crate) fn get_repo_remote(repo_path: &str) -> Result<String, String> {
    let output = Command::new("git")
        .args(["remote", "get-url", "origin"])
        .current_dir(repo_path)
        .output()
        .map_err(|e| format!("Failed to get git remote: {}", e))?;

    if !output.status.success() {
        return Err("Failed to get git remote URL".to_string());
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

// ── API Operations ──────────────────────────────────────────────

/// Merge a PR via REST API.
///
/// `expected_head_sha` binds the merge to one head: GitHub refuses (409) when
/// the PR's head is no longer that commit, so a branch that moved after the
/// caller verified it cannot land in its place.
pub(crate) async fn merge_pr(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    pr_number: i32,
    merge_method: &str,
    expected_head_sha: Option<&str>,
) -> Result<Option<String>, String> {
    let url = format!(
        "{}/repos/{}/{}/pulls/{}/merge",
        GITHUB_API_BASE, owner, repo, pr_number
    );

    let mut body = serde_json::json!({ "merge_method": merge_method });
    if let Some(sha) = expected_head_sha {
        body["sha"] = serde_json::Value::String(sha.to_string());
    }
    let resp = auth.put(http, &url, body).await?;

    check_rate_limit(&resp)?;

    if !resp.is_success() {
        return Err(format!(
            "Failed to merge PR: {} - {}",
            resp.status,
            resp.text()
        ));
    }

    // The merge commit GitHub just wrote to the base branch. Absent only if
    // the response body is not what GitHub documents; the merge still stands.
    Ok(resp
        .json::<serde_json::Value>()
        .ok()
        .and_then(|body| body["sha"].as_str().map(str::to_string)))
}

/// Delete a branch via REST API.
pub(crate) async fn delete_branch(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    branch: &str,
) -> Result<(), String> {
    let url = format!(
        "{}/repos/{}/{}/git/refs/heads/{}",
        GITHUB_API_BASE, owner, repo, branch
    );

    let resp = auth.delete(http, &url).await?;

    check_rate_limit(&resp)?;

    // 204 No Content is success, 422 means already deleted
    if !resp.is_success() && resp.status != 422 {
        return Err(format!(
            "Failed to delete branch: {} - {}",
            resp.status,
            resp.text()
        ));
    }

    Ok(())
}

/// Delete remote branches via GitHub API (non-fatal, logs warnings).
pub(crate) async fn delete_remote_branches(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    branches: &[String],
) {
    for branch in branches {
        match delete_branch(http, auth, owner, repo, branch).await {
            Ok(()) => log::info!("Deleted remote branch: {}", branch),
            Err(e) => log::warn!("Failed to delete remote branch {}: {}", branch, e),
        }
    }
}

// ── PR API Operations ──────────────────────────────────────────

/// Fetch PR details via REST API.
pub(crate) async fn fetch_pr(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<PullRequest, String> {
    let url = format!(
        "{}/repos/{}/{}/pulls/{}",
        GITHUB_API_BASE, owner, repo, pr_number
    );
    github_get_json(http, auth, &url).await
}

/// Fetch check runs for a commit via REST API.
pub(crate) async fn fetch_check_runs(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    sha: &str,
) -> Result<CheckRunsResponse, String> {
    let url = format!(
        "{}/repos/{}/{}/commits/{}/check-runs",
        GITHUB_API_BASE, owner, repo, sha
    );
    github_get_json(http, auth, &url).await
}

/// Fetch PR reviews via REST API.
pub(crate) async fn fetch_reviews(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<Vec<Review>, String> {
    let url = format!(
        "{}/repos/{}/{}/pulls/{}/reviews",
        GITHUB_API_BASE, owner, repo, pr_number
    );
    github_get_json(http, auth, &url).await
}

/// Fetch PR files (changed files with diffs) via REST API.
pub async fn fetch_pr_files(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<Vec<PrFile>, String> {
    let url = format!(
        "{}/repos/{}/{}/pulls/{}/files",
        GITHUB_API_BASE, owner, repo, pr_number
    );
    github_get_json(http, auth, &url).await
}

#[derive(Debug, Deserialize)]
pub struct PrCommit {
    pub commit: PrCommitDetails,
}

#[derive(Debug, Deserialize)]
pub struct PrCommitDetails {
    pub message: String,
}

/// Close a PR via REST API.
pub(crate) async fn close_pr(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<(), String> {
    let url = format!(
        "{}/repos/{}/{}/pulls/{}",
        GITHUB_API_BASE, owner, repo, pr_number
    );

    let body = serde_json::json!({ "state": "closed" });
    let resp = auth.patch(http, &url, body).await?;
    check_rate_limit(&resp)?;

    if !resp.is_success() {
        return Err(format!(
            "Failed to close PR: {} - {}",
            resp.status,
            resp.text()
        ));
    }

    Ok(())
}

/// Fetch workflow run jobs via REST API.
pub(crate) async fn fetch_run_jobs(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    run_id: i64,
) -> Result<JobsResponse, String> {
    let url = format!(
        "{}/repos/{}/{}/actions/runs/{}/jobs",
        GITHUB_API_BASE, owner, repo, run_id
    );
    github_get_json(http, auth, &url).await
}

/// Fetch workflow run logs via REST API. Returns the raw log content as bytes.
pub(crate) async fn fetch_run_logs(
    http: &dyn HttpClient,
    auth: &InstallationAuthority,
    owner: &str,
    repo: &str,
    run_id: i64,
) -> Result<Vec<u8>, String> {
    let url = format!(
        "{}/repos/{}/{}/actions/runs/{}/logs",
        GITHUB_API_BASE, owner, repo, run_id
    );
    log::info!("Fetching workflow logs from: {}", url);

    let resp = auth.get(http, &url).await?;
    log::info!("Logs response status: {}", resp.status);
    check_rate_limit(&resp)?;

    if !resp.is_success() {
        return Err(format!(
            "GitHub API error: {} - {}",
            resp.status,
            resp.text()
        ));
    }

    Ok(resp.body)
}

/// Update GitHub App's webhook URL via REST API.
///
/// Requires App-level JWT authentication (not installation token).
pub async fn update_app_webhook_url(
    http: &dyn HttpClient,
    app: &AppAuthority,
    new_webhook_url: &str,
) -> Result<(), String> {
    let url = format!("{}/app/hook/config", GITHUB_API_BASE);

    let body = serde_json::json!({
        "url": new_webhook_url,
        "content_type": "json"
    });

    let resp = app.patch(http, &url, body).await?;
    check_rate_limit(&resp)?;

    if !resp.is_success() {
        return Err(format!(
            "Failed to update webhook URL: {} - {}",
            resp.status,
            resp.text()
        ));
    }

    log::info!("Updated GitHub App webhook URL to: {}", new_webhook_url);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{testing::MockHttpClient, HttpResponse};

    fn base_merge_ruleset_spec() -> BaseMergeRulesetSpec {
        BaseMergeRulesetSpec {
            base_branch: "main".into(),
            required_check_name: "Cairn / provably-human base merge".into(),
            github_app_integration_id: 42,
        }
    }

    #[test]
    fn base_merge_ruleset_has_no_bypass_and_binds_exact_app_check() {
        let spec = base_merge_ruleset_spec();
        let body = spec.request_body().unwrap();
        assert_eq!(body["enforcement"], "active");
        assert_eq!(body["bypass_actors"], serde_json::json!([]));
        assert_eq!(
            body["conditions"]["ref_name"]["include"],
            serde_json::json!(["refs/heads/main"])
        );
        assert!(body["rules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|rule| rule["type"] == "pull_request"));
        let checks = body["rules"]
            .as_array()
            .unwrap()
            .iter()
            .find(|rule| rule["type"] == "required_status_checks")
            .unwrap();
        assert_eq!(
            checks["parameters"]["required_status_checks"][0],
            serde_json::json!({
                "context": "Cairn / provably-human base merge",
                "integration_id": 42
            })
        );
        verify_base_merge_ruleset(&body, &spec).unwrap();

        let mut weakened = body;
        weakened["bypass_actors"] = serde_json::json!([{"actor_type":"OrganizationAdmin"}]);
        assert_eq!(
            verify_base_merge_ruleset(&weakened, &spec).unwrap_err(),
            "provider ruleset drifted at bypass_actors"
        );
    }

    #[test]
    fn base_merge_check_binds_candidate_and_intent_digest() {
        let spec = BaseMergeCheckSpec {
            candidate_oid: "ab".repeat(20),
            intent_digest_sha256: "cd".repeat(32),
            details_url: Some("https://cairn.example/approvals/1".into()),
        };
        let body = spec.request_body().unwrap();
        assert_eq!(body["name"], HUMAN_BASE_MERGE_CHECK_NAME);
        assert_eq!(body["head_sha"], "ab".repeat(20));
        assert_eq!(body["external_id"], "cd".repeat(32));
        assert_eq!(body["status"], "completed");
        assert_eq!(body["conclusion"], "success");

        let mut invalid = spec.clone();
        invalid.candidate_oid = "ABC".into();
        assert!(invalid.request_body().is_err());
        let mut invalid = spec;
        invalid.intent_digest_sha256 = "short".into();
        assert!(invalid.request_body().is_err());
    }

    #[test]
    fn base_merge_ruleset_rejects_ambiguous_identity_inputs() {
        let mut spec = base_merge_ruleset_spec();
        spec.base_branch = "refs/heads/main".into();
        assert!(spec.request_body().is_err());
        let mut spec = base_merge_ruleset_spec();
        spec.github_app_integration_id = 0;
        assert!(spec.request_body().is_err());
    }

    #[tokio::test]
    async fn base_merge_ruleset_mutations_use_authenticated_provider_endpoints() {
        let token_body = serde_json::json!({
            "token": "ghs_test_token",
            "expires_at": "2099-01-01T00:00:00Z"
        });
        let ruleset = serde_json::json!({
            "id": 17,
            "name": HUMAN_BASE_MERGE_RULESET_NAME,
            "target": "branch",
            "enforcement": "active"
        });
        let http = MockHttpClient::new()
            .respond_to(
                "access_tokens",
                HttpResponse::new(201, serde_json::to_vec(&token_body).unwrap()),
            )
            .respond_to(
                "/repos/owner/repo/rulesets",
                HttpResponse::new(201, serde_json::to_vec(&ruleset).unwrap()),
            )
            .respond_to(
                "/repos/owner/repo/rulesets/17",
                HttpResponse::new(200, serde_json::to_vec(&ruleset).unwrap()),
            );
        let auth = test_authority();
        let spec = base_merge_ruleset_spec();

        let created = create_base_merge_ruleset(&http, &auth, "owner", "repo", &spec)
            .await
            .unwrap();
        assert_eq!(created.id, 17);
        let updated = update_base_merge_ruleset(&http, &auth, "owner", "repo", 17, &spec)
            .await
            .unwrap();
        assert_eq!(updated.id, 17);

        let requests = http.requests();
        let provider_requests = requests
            .iter()
            .filter(|request| request.url.contains("/repos/owner/repo/rulesets"))
            .collect::<Vec<_>>();
        assert_eq!(provider_requests.len(), 2);
        assert_eq!(
            provider_requests[0].method,
            crate::services::HttpMethod::Post
        );
        assert_eq!(
            provider_requests[1].method,
            crate::services::HttpMethod::Put
        );
        assert!(provider_requests
            .iter()
            .all(|request| request.authorization() == Some("Bearer ghs_test_token")));
    }

    #[tokio::test]
    async fn base_merge_ruleset_mutation_fails_closed_on_provider_refusal() {
        let http = mock_with_token_and("/repos/owner/repo/rulesets", 403);
        let error = create_base_merge_ruleset(
            &http,
            &test_authority(),
            "owner",
            "repo",
            &base_merge_ruleset_spec(),
        )
        .await
        .unwrap_err();
        assert!(error.contains("403"));
    }

    #[tokio::test]
    async fn lists_app_installations_with_account_facts() {
        let body = serde_json::json!([
            {"id": 42, "account": {"login": "Acme", "type": "Organization"}},
            {"id": 7, "account": {"login": "mitch", "type": "User"}}
        ]);
        let http = MockHttpClient::new().respond_to(
            "/app/installations?",
            HttpResponse::new(200, serde_json::to_vec(&body).unwrap()),
        );
        let installations = list_app_installations(&http, &test_app_authority())
            .await
            .unwrap();
        assert_eq!(installations[0].id, 42);
        assert_eq!(installations[0].account.login, "Acme");
        assert_eq!(installations[0].account.account_type, "Organization");
    }

    #[tokio::test]
    async fn scoped_revocation_reports_github_failure() {
        let http = MockHttpClient::new().respond_to(
            "/app/installations/42",
            HttpResponse::new(403, br#"{"message":"forbidden"}"#.to_vec()),
        );
        let error = revoke_app_installation(&http, &test_app_authority(), 42)
            .await
            .unwrap_err();
        assert!(error.contains("403"));
    }

    // ── parse_repo_from_url ──────────────────────────────────────

    #[test]
    fn parse_https_url() {
        let (owner, repo) = parse_repo_from_url("https://github.com/owner/repo").unwrap();
        assert_eq!(owner, "owner");
        assert_eq!(repo, "repo");
    }

    #[test]
    fn parse_https_url_with_git_suffix() {
        let (owner, repo) = parse_repo_from_url("https://github.com/owner/repo.git").unwrap();
        assert_eq!(owner, "owner");
        assert_eq!(repo, "repo");
    }

    #[test]
    fn parse_ssh_url() {
        let (owner, repo) = parse_repo_from_url("git@github.com:owner/repo.git").unwrap();
        assert_eq!(owner, "owner");
        assert_eq!(repo, "repo");
    }

    #[test]
    fn parse_ssh_url_without_git_suffix() {
        let (owner, repo) = parse_repo_from_url("git@github.com:owner/repo").unwrap();
        assert_eq!(owner, "owner");
        assert_eq!(repo, "repo");
    }

    #[test]
    fn parse_simple_owner_repo() {
        let (owner, repo) = parse_repo_from_url("owner/repo").unwrap();
        assert_eq!(owner, "owner");
        assert_eq!(repo, "repo");
    }

    #[test]
    fn parse_url_with_special_chars() {
        let (owner, repo) = parse_repo_from_url("https://github.com/my-org/my_repo").unwrap();
        assert_eq!(owner, "my-org");
        assert_eq!(repo, "my_repo");
    }

    #[test]
    fn parse_invalid_url_returns_error() {
        assert!(parse_repo_from_url("not-a-valid-format").is_err());
    }

    #[test]
    fn parse_empty_string_returns_error() {
        assert!(parse_repo_from_url("").is_err());
    }

    // ── parse_rate_limit_error ──────────────────────────────────

    #[test]
    fn parse_rate_limit_error_with_rate_limit_message() {
        let body = serde_json::json!({
            "message": "API rate limit exceeded for installation ID 12345."
        });
        let resp = HttpResponse::new(429, serde_json::to_vec(&body).unwrap());
        assert_eq!(parse_rate_limit_error(&resp), Some(60));
    }

    #[test]
    fn parse_rate_limit_error_without_rate_limit_message() {
        let body = serde_json::json!({
            "message": "Not Found"
        });
        let resp = HttpResponse::new(404, serde_json::to_vec(&body).unwrap());
        assert_eq!(parse_rate_limit_error(&resp), None);
    }

    #[test]
    fn parse_rate_limit_error_with_empty_body() {
        let resp = HttpResponse::new(429, vec![]);
        assert_eq!(parse_rate_limit_error(&resp), None);
    }

    // ── check_rate_limit ─────────────────────────────────────────

    #[test]
    fn rate_limit_429_returns_error() {
        let resp = HttpResponse::new(429, vec![]);
        assert!(check_rate_limit(&resp).is_err());
    }

    #[test]
    fn rate_limit_429_error_message_includes_wait_seconds() {
        let body = serde_json::json!({
            "message": "API rate limit exceeded"
        });
        let resp = HttpResponse::new(429, serde_json::to_vec(&body).unwrap());
        let err = check_rate_limit(&resp).unwrap_err();
        assert!(
            err.contains("60 seconds"),
            "Error should mention wait time: {}",
            err
        );
    }

    #[test]
    fn rate_limit_200_ok() {
        let resp = HttpResponse::new(200, vec![]);
        assert!(check_rate_limit(&resp).is_ok());
    }

    // ── merge_pr ─────────────────────────────────────────────────

    const TEST_KEY: &str = include_str!("../../tests/fixtures/test_rsa_key.pem");

    fn test_app_authority() -> crate::security::broker::github::AppAuthority {
        crate::security::broker::github::app_authority_from_key(12345, TEST_KEY).unwrap()
    }

    fn test_authority() -> InstallationAuthority {
        // A fixture key, signed with here exactly as production signs with the
        // stored one — the difference production makes is where the key came
        // from, which is the broker's business rather than this client's.
        crate::security::broker::github::installation_authority_from_key(12345, 99999, TEST_KEY)
            .unwrap()
    }

    fn mock_with_token_and(url_pattern: &str, status: u16) -> MockHttpClient {
        let token_body = serde_json::json!({
            "token": "ghs_test_token",
            "expires_at": "2099-01-01T00:00:00Z"
        });
        MockHttpClient::new()
            .respond_to(
                "access_tokens",
                HttpResponse::new(201, serde_json::to_vec(&token_body).unwrap()),
            )
            .respond_to(url_pattern, HttpResponse::new(status, vec![]))
    }

    #[tokio::test]
    async fn merge_pr_success() {
        let http = mock_with_token_and("pulls/42/merge", 200);
        let auth = test_authority();
        let result = merge_pr(&http, &auth, "owner", "repo", 42, "squash", None).await;
        assert_eq!(result, Ok(None), "an empty body still merged");
        let merge = http
            .requests()
            .into_iter()
            .find(|request| request.url.contains("pulls/42/merge"))
            .unwrap();
        assert_eq!(
            merge.body,
            Some(serde_json::json!({"merge_method": "squash"})),
            "no expected head means no `sha` binding"
        );
    }

    #[tokio::test]
    async fn merge_pr_binds_the_expected_head_through_sha() {
        let http = mock_with_token_and("pulls/42/merge", 200);
        let auth = test_authority();
        merge_pr(
            &http,
            &auth,
            "owner",
            "repo",
            42,
            "squash",
            Some("0123abcd"),
        )
        .await
        .unwrap();
        let merge = http
            .requests()
            .into_iter()
            .find(|request| request.url.contains("pulls/42/merge"))
            .unwrap();
        assert_eq!(
            merge.body,
            Some(serde_json::json!({"merge_method": "squash", "sha": "0123abcd"}))
        );
    }

    #[tokio::test]
    async fn merge_pr_reports_the_merge_commit() {
        let http = mock_with_token_and_body(
            "pulls/42/merge",
            200,
            serde_json::json!({"sha": "9e77a275", "merged": true, "message": "Pull Request successfully merged"}),
        );
        let auth = test_authority();
        let result = merge_pr(&http, &auth, "owner", "repo", 42, "squash", None).await;
        assert_eq!(result, Ok(Some("9e77a275".to_string())));
    }

    #[tokio::test]
    async fn merge_pr_failure_returns_error() {
        let http = mock_with_token_and("pulls/42/merge", 405);
        let auth = test_authority();
        let result = merge_pr(&http, &auth, "owner", "repo", 42, "merge", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn merge_pr_rate_limited() {
        let http = mock_with_token_and("pulls/1/merge", 429);
        let auth = test_authority();
        let result = merge_pr(&http, &auth, "owner", "repo", 1, "merge", None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("rate limit"));
    }

    // ── delete_branch ────────────────────────────────────────────

    #[tokio::test]
    async fn delete_branch_success() {
        let http = mock_with_token_and("refs/heads/feature", 204);
        let auth = test_authority();
        let result = delete_branch(&http, &auth, "owner", "repo", "feature").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_branch_already_deleted_is_ok() {
        // 422 means branch already deleted — should not error
        let http = mock_with_token_and("refs/heads/old", 422);
        let auth = test_authority();
        let result = delete_branch(&http, &auth, "owner", "repo", "old").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_branch_forbidden_returns_error() {
        let http = mock_with_token_and("refs/heads/protected", 403);
        let auth = test_authority();
        let result = delete_branch(&http, &auth, "owner", "repo", "protected").await;
        assert!(result.is_err());
    }

    // ── fetch_pr ─────────────────────────────────────────────────

    fn mock_with_token_and_body(
        url_pattern: &str,
        status: u16,
        body: serde_json::Value,
    ) -> MockHttpClient {
        let token_body = serde_json::json!({
            "token": "ghs_test_token",
            "expires_at": "2099-01-01T00:00:00Z"
        });
        MockHttpClient::new()
            .respond_to(
                "access_tokens",
                HttpResponse::new(201, serde_json::to_vec(&token_body).unwrap()),
            )
            .respond_to(
                url_pattern,
                HttpResponse::new(status, serde_json::to_vec(&body).unwrap()),
            )
    }

    #[tokio::test]
    async fn fetch_pr_success() {
        let pr_json = serde_json::json!({
            "title": "Fix bug",
            "body": "Fixes #123",
            "state": "open",
            "draft": false,
            "mergeable": true,
            "mergeable_state": "clean",
            "additions": 10,
            "deletions": 5,
            "merged": false,
            "head": { "sha": "abc123" }
        });
        let http = mock_with_token_and_body("pulls/42", 200, pr_json);
        let auth = test_authority();
        let pr = fetch_pr(&http, &auth, "owner", "repo", 42).await.unwrap();
        assert_eq!(pr.title, "Fix bug");
        assert_eq!(pr.head.sha, "abc123");
        assert!(!pr.merged);
    }

    #[tokio::test]
    async fn fetch_pr_not_found() {
        let http = mock_with_token_and("pulls/999", 404);
        let auth = test_authority();
        let result = fetch_pr(&http, &auth, "owner", "repo", 999).await;
        assert!(result.is_err());
    }

    // ── fetch_check_runs ─────────────────────────────────────────

    #[tokio::test]
    async fn fetch_check_runs_success() {
        let body = serde_json::json!({
            "check_runs": [{
                "name": "CI",
                "status": "completed",
                "conclusion": "success",
                "html_url": "https://github.com/owner/repo/runs/1",
                "output": { "summary": null }
            }]
        });
        let http = mock_with_token_and_body("check-runs", 200, body);
        let auth = test_authority();
        let result = fetch_check_runs(&http, &auth, "owner", "repo", "abc123")
            .await
            .unwrap();
        assert_eq!(result.check_runs.len(), 1);
        assert_eq!(result.check_runs[0].name, "CI");
    }

    // ── close_pr ─────────────────────────────────────────────────

    #[tokio::test]
    async fn close_pr_success() {
        let http = mock_with_token_and("pulls/42", 200);
        let auth = test_authority();
        let result = close_pr(&http, &auth, "owner", "repo", 42).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn close_pr_failure() {
        let http = mock_with_token_and("pulls/42", 422);
        let auth = test_authority();
        let result = close_pr(&http, &auth, "owner", "repo", 42).await;
        assert!(result.is_err());
    }

    // ── fetch_run_jobs ───────────────────────────────────────────

    #[tokio::test]
    async fn fetch_run_jobs_success() {
        let body = serde_json::json!({
            "jobs": [{
                "name": "build",
                "steps": [{ "name": "Run tests", "conclusion": "success" }]
            }]
        });
        let http = mock_with_token_and_body("runs/100/jobs", 200, body);
        let auth = test_authority();
        let result = fetch_run_jobs(&http, &auth, "owner", "repo", 100)
            .await
            .unwrap();
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.jobs[0].name, "build");
    }
}
