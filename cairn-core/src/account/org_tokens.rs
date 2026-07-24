//! Org-scoped JWT cache for remote server access.
//!
//! Desktop apps exchange their device JWT for org-scoped JWTs via `POST /tokens/issue`.
//! This module caches those tokens and handles refresh.

use serde::Deserialize;
use std::collections::HashMap;

/// How close to expiry before we consider a token stale (5 minutes).
const REFRESH_MARGIN_SECS: i64 = 5 * 60;

#[derive(Debug, Clone)]
struct CachedOrgToken {
    token: String,
    expires_at: i64,
}

/// In-memory cache of org-scoped JWTs, keyed by org_id.
#[derive(Debug, Default)]
pub struct OrgTokenCache {
    tokens: HashMap<String, CachedOrgToken>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IssueTokenResponse {
    token: String,
    expires_at: String,
}

impl OrgTokenCache {
    pub(crate) fn new() -> Self {
        Self {
            tokens: HashMap::new(),
        }
    }

    /// Get a cached token if it's still valid (not within refresh margin of expiry).
    pub fn get(&self, org_id: &str) -> Option<&str> {
        let cached = self.tokens.get(org_id)?;
        let now = chrono::Utc::now().timestamp();
        if cached.expires_at - now > REFRESH_MARGIN_SECS {
            Some(&cached.token)
        } else {
            None
        }
    }

    /// Store a token in the cache.
    pub fn set(&mut self, org_id: &str, token: String, expires_at: i64) {
        self.tokens
            .insert(org_id.to_string(), CachedOrgToken { token, expires_at });
    }

    /// Clear all cached tokens (e.g. on logout).
    pub(crate) fn clear(&mut self) {
        self.tokens.clear();
    }

    /// Get all org IDs with cached tokens that are near expiry (within margin).
    pub fn expiring_org_ids(&self) -> Vec<String> {
        let now = chrono::Utc::now().timestamp();
        self.tokens
            .iter()
            .filter(|(_, cached)| cached.expires_at - now <= REFRESH_MARGIN_SECS)
            .map(|(org_id, _)| org_id.clone())
            .collect()
    }
}

/// Fetch a new org-scoped JWT from the API using a device JWT.
pub async fn fetch_org_token(
    client: &reqwest::Client,
    device_jwt: &str,
    org_id: &str,
    api_url: &str,
) -> Result<(String, i64), String> {
    let url = format!("{}/tokens/issue", api_url.trim_end_matches('/'));

    let resp = client
        .post(&url)
        .bearer_auth(device_jwt)
        .json(&serde_json::json!({ "orgId": org_id }))
        .send()
        .await
        .map_err(|e| format!("Failed to request org token: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Org token request failed ({status}): {body}"));
    }

    let body: IssueTokenResponse = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse org token response: {e}"))?;

    let expires_at = chrono::DateTime::parse_from_rfc3339(&body.expires_at)
        .map(|dt| dt.timestamp())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp() + 3600);

    Ok((body.token, expires_at))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_get_returns_valid_token() {
        let mut cache = OrgTokenCache::new();
        let future = chrono::Utc::now().timestamp() + 3600;
        cache.set("org-1", "token-1".to_string(), future);
        assert_eq!(cache.get("org-1"), Some("token-1"));
    }

    #[test]
    fn cache_get_returns_none_for_expired() {
        let mut cache = OrgTokenCache::new();
        let past = chrono::Utc::now().timestamp() - 100;
        cache.set("org-1", "token-1".to_string(), past);
        assert_eq!(cache.get("org-1"), None);
    }

    #[test]
    fn cache_get_returns_none_for_near_expiry() {
        let mut cache = OrgTokenCache::new();
        // Within 5-minute margin
        let near = chrono::Utc::now().timestamp() + 60;
        cache.set("org-1", "token-1".to_string(), near);
        assert_eq!(cache.get("org-1"), None);
    }

    #[test]
    fn cache_get_returns_none_for_missing() {
        let cache = OrgTokenCache::new();
        assert_eq!(cache.get("org-1"), None);
    }

    #[test]
    fn cache_clear_removes_all() {
        let mut cache = OrgTokenCache::new();
        let future = chrono::Utc::now().timestamp() + 3600;
        cache.set("org-1", "token-1".to_string(), future);
        cache.set("org-2", "token-2".to_string(), future);
        cache.clear();
        assert_eq!(cache.get("org-1"), None);
        assert_eq!(cache.get("org-2"), None);
    }

    #[test]
    fn expiring_org_ids_returns_near_expiry_tokens() {
        let mut cache = OrgTokenCache::new();
        let future = chrono::Utc::now().timestamp() + 3600;
        let near = chrono::Utc::now().timestamp() + 60;
        cache.set("valid-org", "token-1".to_string(), future);
        cache.set("expiring-org", "token-2".to_string(), near);

        let expiring = cache.expiring_org_ids();
        assert_eq!(expiring.len(), 1);
        assert_eq!(expiring[0], "expiring-org");
    }

    #[test]
    fn set_overwrites_existing_token_for_same_org() {
        let mut cache = OrgTokenCache::new();
        let future = chrono::Utc::now().timestamp() + 3600;
        cache.set("org-1", "old-token".to_string(), future);
        cache.set("org-1", "new-token".to_string(), future);
        assert_eq!(cache.get("org-1"), Some("new-token"));
    }

    #[test]
    fn expiring_org_ids_includes_already_expired_tokens() {
        let mut cache = OrgTokenCache::new();
        let future = chrono::Utc::now().timestamp() + 3600;
        let past = chrono::Utc::now().timestamp() - 100;
        cache.set("valid-org", "token-1".to_string(), future);
        cache.set("expired-org", "token-2".to_string(), past);

        let expiring = cache.expiring_org_ids();
        assert_eq!(expiring.len(), 1);
        assert_eq!(expiring[0], "expired-org");
    }
}
