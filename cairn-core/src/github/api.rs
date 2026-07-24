//! GitHub REST API client — shared between Tauri and cairn-server.
//!
//! Contains the subset of GitHub API operations needed by cairn-core:
//! - Repo URL parsing
//! - PR merge
//! - Branch deletion
//! - Auth header generation (JWT + installation tokens)

use super::credentials::GitHubAppCredentials;
use crate::services::{HttpClient, HttpResponse};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, USER_AGENT};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::process::Command;
use std::sync::{LazyLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const GITHUB_API_BASE: &str = "https://api.github.com";

/// Cached installation token with expiry.
struct CachedToken {
    token: String,
    expires_at: u64,
}

/// Global token cache keyed by installation_id.
static TOKEN_CACHE: LazyLock<RwLock<HashMap<i64, CachedToken>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

// ── JWT ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct JwtClaims {
    iat: u64,
    exp: u64,
    iss: i64,
}

fn generate_app_jwt(app_id: i64, private_key: &str) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs();

    let claims = JwtClaims {
        iat: now - 60,
        exp: now + 10 * 60,
        iss: app_id,
    };

    let key = EncodingKey::from_rsa_pem(private_key.as_bytes())
        .map_err(|e| format!("Invalid private key: {}", e))?;

    encode(&Header::new(Algorithm::RS256), &claims, &key)
        .map_err(|e| format!("Failed to generate JWT: {}", e))
}

// ── Token management ────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct InstallationTokenResponse {
    token: String,
    expires_at: String,
}

async fn get_installation_token(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
) -> Result<String, String> {
    // Check cache
    {
        let cache = TOKEN_CACHE.read().map_err(|e| e.to_string())?;
        if let Some(cached) = cache.get(&creds.installation_id) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_secs();
            if cached.expires_at > now + 300 {
                return Ok(cached.token.clone());
            }
        }
    }

    let jwt = generate_app_jwt(creds.app_id, &creds.private_key)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", jwt)).map_err(|e| e.to_string())?,
    );
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    headers.insert(USER_AGENT, HeaderValue::from_static("Cairn"));
    headers.insert(
        "X-GitHub-Api-Version",
        HeaderValue::from_static("2022-11-28"),
    );

    let url = format!(
        "{}/app/installations/{}/access_tokens",
        GITHUB_API_BASE, creds.installation_id
    );

    let resp = http.post(&url, serde_json::json!({}), headers).await?;

    if !resp.is_success() {
        return Err(format!(
            "GitHub API error: {} - {}",
            resp.status,
            resp.text()
        ));
    }

    let token_resp: InstallationTokenResponse = resp.json()?;

    let expires_at = chrono::DateTime::parse_from_rfc3339(&token_resp.expires_at)
        .map(|dt| dt.timestamp() as u64)
        .unwrap_or(0);

    {
        let mut cache = TOKEN_CACHE.write().map_err(|e| e.to_string())?;
        cache.insert(
            creds.installation_id,
            CachedToken {
                token: token_resp.token.clone(),
                expires_at,
            },
        );
    }

    Ok(token_resp.token)
}

/// Create authenticated headers for GitHub API requests.
async fn auth_headers(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
) -> Result<HeaderMap, String> {
    let token = get_installation_token(http, creds).await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token)).map_err(|e| e.to_string())?,
    );
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    headers.insert(USER_AGENT, HeaderValue::from_static("Cairn"));
    headers.insert(
        "X-GitHub-Api-Version",
        HeaderValue::from_static("2022-11-28"),
    );

    Ok(headers)
}

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
    creds: &GitHubAppCredentials,
    url: &str,
) -> Result<HttpResponse, String> {
    let headers = auth_headers(http, creds).await?;
    let resp = http.get(url, headers).await?;
    ensure_github_api_success(&resp)?;
    Ok(resp)
}

async fn github_get_json<T: DeserializeOwned>(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    url: &str,
) -> Result<T, String> {
    github_get_response(http, creds, url).await?.json()
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
pub(crate) async fn merge_pr(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    pr_number: i32,
    merge_method: &str,
) -> Result<(), String> {
    let headers = auth_headers(http, creds).await?;
    let url = format!(
        "{}/repos/{}/{}/pulls/{}/merge",
        GITHUB_API_BASE, owner, repo, pr_number
    );

    let body = serde_json::json!({ "merge_method": merge_method });
    let resp = http.put(&url, body, headers).await?;

    check_rate_limit(&resp)?;

    if !resp.is_success() {
        return Err(format!(
            "Failed to merge PR: {} - {}",
            resp.status,
            resp.text()
        ));
    }

    Ok(())
}

/// Delete a branch via REST API.
pub(crate) async fn delete_branch(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    branch: &str,
) -> Result<(), String> {
    let headers = auth_headers(http, creds).await?;
    let url = format!(
        "{}/repos/{}/{}/git/refs/heads/{}",
        GITHUB_API_BASE, owner, repo, branch
    );

    let resp = http.delete(&url, headers).await?;

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
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    branches: &[String],
) {
    for branch in branches {
        match delete_branch(http, creds, owner, repo, branch).await {
            Ok(()) => log::info!("Deleted remote branch: {}", branch),
            Err(e) => log::warn!("Failed to delete remote branch {}: {}", branch, e),
        }
    }
}

// ── PR API Operations ──────────────────────────────────────────

/// Fetch PR details via REST API.
pub(crate) async fn fetch_pr(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<PullRequest, String> {
    let url = format!(
        "{}/repos/{}/{}/pulls/{}",
        GITHUB_API_BASE, owner, repo, pr_number
    );
    github_get_json(http, creds, &url).await
}

/// Fetch check runs for a commit via REST API.
pub(crate) async fn fetch_check_runs(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    sha: &str,
) -> Result<CheckRunsResponse, String> {
    let url = format!(
        "{}/repos/{}/{}/commits/{}/check-runs",
        GITHUB_API_BASE, owner, repo, sha
    );
    github_get_json(http, creds, &url).await
}

/// Fetch PR reviews via REST API.
pub(crate) async fn fetch_reviews(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<Vec<Review>, String> {
    let url = format!(
        "{}/repos/{}/{}/pulls/{}/reviews",
        GITHUB_API_BASE, owner, repo, pr_number
    );
    github_get_json(http, creds, &url).await
}

/// Fetch PR files (changed files with diffs) via REST API.
pub async fn fetch_pr_files(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<Vec<PrFile>, String> {
    let url = format!(
        "{}/repos/{}/{}/pulls/{}/files",
        GITHUB_API_BASE, owner, repo, pr_number
    );
    github_get_json(http, creds, &url).await
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
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    pr_number: i32,
) -> Result<(), String> {
    let headers = auth_headers(http, creds).await?;
    let url = format!(
        "{}/repos/{}/{}/pulls/{}",
        GITHUB_API_BASE, owner, repo, pr_number
    );

    let body = serde_json::json!({ "state": "closed" });
    let resp = http.patch(&url, body, headers).await?;
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
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    run_id: i64,
) -> Result<JobsResponse, String> {
    let url = format!(
        "{}/repos/{}/{}/actions/runs/{}/jobs",
        GITHUB_API_BASE, owner, repo, run_id
    );
    github_get_json(http, creds, &url).await
}

/// Fetch workflow run logs via REST API. Returns the raw log content as bytes.
pub(crate) async fn fetch_run_logs(
    http: &dyn HttpClient,
    creds: &GitHubAppCredentials,
    owner: &str,
    repo: &str,
    run_id: i64,
) -> Result<Vec<u8>, String> {
    let headers = auth_headers(http, creds).await?;
    let url = format!(
        "{}/repos/{}/{}/actions/runs/{}/logs",
        GITHUB_API_BASE, owner, repo, run_id
    );
    log::info!("Fetching workflow logs from: {}", url);

    let resp = http.get(&url, headers).await?;
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
    app_id: i64,
    private_key: &str,
    new_webhook_url: &str,
) -> Result<(), String> {
    let jwt = generate_app_jwt(app_id, private_key)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", jwt)).map_err(|e| e.to_string())?,
    );
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    headers.insert(USER_AGENT, HeaderValue::from_static("Cairn"));
    headers.insert(
        "X-GitHub-Api-Version",
        HeaderValue::from_static("2022-11-28"),
    );

    let url = format!("{}/app/hook/config", GITHUB_API_BASE);

    let body = serde_json::json!({
        "url": new_webhook_url,
        "content_type": "json"
    });

    let resp = http.patch(&url, body, headers).await?;
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
        let resp = HttpResponse {
            status: 429,
            body: serde_json::to_vec(&body).unwrap(),
        };
        assert_eq!(parse_rate_limit_error(&resp), Some(60));
    }

    #[test]
    fn parse_rate_limit_error_without_rate_limit_message() {
        let body = serde_json::json!({
            "message": "Not Found"
        });
        let resp = HttpResponse {
            status: 404,
            body: serde_json::to_vec(&body).unwrap(),
        };
        assert_eq!(parse_rate_limit_error(&resp), None);
    }

    #[test]
    fn parse_rate_limit_error_with_empty_body() {
        let resp = HttpResponse {
            status: 429,
            body: vec![],
        };
        assert_eq!(parse_rate_limit_error(&resp), None);
    }

    // ── check_rate_limit ─────────────────────────────────────────

    #[test]
    fn rate_limit_429_returns_error() {
        let resp = HttpResponse {
            status: 429,
            body: vec![],
        };
        assert!(check_rate_limit(&resp).is_err());
    }

    #[test]
    fn rate_limit_429_error_message_includes_wait_seconds() {
        let body = serde_json::json!({
            "message": "API rate limit exceeded"
        });
        let resp = HttpResponse {
            status: 429,
            body: serde_json::to_vec(&body).unwrap(),
        };
        let err = check_rate_limit(&resp).unwrap_err();
        assert!(
            err.contains("60 seconds"),
            "Error should mention wait time: {}",
            err
        );
    }

    #[test]
    fn rate_limit_200_ok() {
        let resp = HttpResponse {
            status: 200,
            body: vec![],
        };
        assert!(check_rate_limit(&resp).is_ok());
    }

    // ── merge_pr ─────────────────────────────────────────────────

    fn test_creds() -> GitHubAppCredentials {
        // Use a minimal RSA key for JWT generation in tests
        GitHubAppCredentials {
            app_id: 12345,
            private_key: include_str!("../../tests/fixtures/test_rsa_key.pem").to_string(),
            installation_id: 99999,
        }
    }

    fn mock_with_token_and(url_pattern: &str, status: u16) -> MockHttpClient {
        let token_body = serde_json::json!({
            "token": "ghs_test_token",
            "expires_at": "2099-01-01T00:00:00Z"
        });
        MockHttpClient::new()
            .respond_to(
                "access_tokens",
                HttpResponse {
                    status: 201,
                    body: serde_json::to_vec(&token_body).unwrap(),
                },
            )
            .respond_to(
                url_pattern,
                HttpResponse {
                    status,
                    body: vec![],
                },
            )
    }

    #[tokio::test]
    async fn merge_pr_success() {
        let http = mock_with_token_and("pulls/42/merge", 200);
        let creds = test_creds();
        let result = merge_pr(&http, &creds, "owner", "repo", 42, "squash").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn merge_pr_failure_returns_error() {
        let http = mock_with_token_and("pulls/42/merge", 405);
        let creds = test_creds();
        let result = merge_pr(&http, &creds, "owner", "repo", 42, "merge").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn merge_pr_rate_limited() {
        let http = mock_with_token_and("pulls/1/merge", 429);
        let creds = test_creds();
        let result = merge_pr(&http, &creds, "owner", "repo", 1, "merge").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("rate limit"));
    }

    // ── delete_branch ────────────────────────────────────────────

    #[tokio::test]
    async fn delete_branch_success() {
        let http = mock_with_token_and("refs/heads/feature", 204);
        let creds = test_creds();
        let result = delete_branch(&http, &creds, "owner", "repo", "feature").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_branch_already_deleted_is_ok() {
        // 422 means branch already deleted — should not error
        let http = mock_with_token_and("refs/heads/old", 422);
        let creds = test_creds();
        let result = delete_branch(&http, &creds, "owner", "repo", "old").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_branch_forbidden_returns_error() {
        let http = mock_with_token_and("refs/heads/protected", 403);
        let creds = test_creds();
        let result = delete_branch(&http, &creds, "owner", "repo", "protected").await;
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
                HttpResponse {
                    status: 201,
                    body: serde_json::to_vec(&token_body).unwrap(),
                },
            )
            .respond_to(
                url_pattern,
                HttpResponse {
                    status,
                    body: serde_json::to_vec(&body).unwrap(),
                },
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
        let creds = test_creds();
        let pr = fetch_pr(&http, &creds, "owner", "repo", 42).await.unwrap();
        assert_eq!(pr.title, "Fix bug");
        assert_eq!(pr.head.sha, "abc123");
        assert!(!pr.merged);
    }

    #[tokio::test]
    async fn fetch_pr_not_found() {
        let http = mock_with_token_and("pulls/999", 404);
        let creds = test_creds();
        let result = fetch_pr(&http, &creds, "owner", "repo", 999).await;
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
        let creds = test_creds();
        let result = fetch_check_runs(&http, &creds, "owner", "repo", "abc123")
            .await
            .unwrap();
        assert_eq!(result.check_runs.len(), 1);
        assert_eq!(result.check_runs[0].name, "CI");
    }

    // ── close_pr ─────────────────────────────────────────────────

    #[tokio::test]
    async fn close_pr_success() {
        let http = mock_with_token_and("pulls/42", 200);
        let creds = test_creds();
        let result = close_pr(&http, &creds, "owner", "repo", 42).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn close_pr_failure() {
        let http = mock_with_token_and("pulls/42", 422);
        let creds = test_creds();
        let result = close_pr(&http, &creds, "owner", "repo", 42).await;
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
        let creds = test_creds();
        let result = fetch_run_jobs(&http, &creds, "owner", "repo", 100)
            .await
            .unwrap();
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.jobs[0].name, "build");
    }
}
