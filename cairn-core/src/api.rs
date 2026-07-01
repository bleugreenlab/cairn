//! Cloud API configuration.
//!
//! All cloud features (account, sync, bug reporting) are optional.
//! These URLs are only used when the user opts into account connection.

/// Cloud API endpoint configuration.
///
/// Centralizes all `api.cairn.computer` URLs. Override the base URL
/// via the `CAIRN_API_URL` environment variable for development or
/// self-hosted deployments.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub base_url: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        let base = std::env::var("CAIRN_API_URL")
            .unwrap_or_else(|_| "https://api.cairn.computer".to_string());
        Self { base_url: base }
    }
}

impl ApiConfig {
    /// Device JWT refresh endpoint.
    pub fn device_refresh_url(&self) -> String {
        format!("{}/tokens/device/refresh", self.base_url)
    }

    /// Device token status endpoint.
    pub fn device_url(&self, device_id: &str) -> String {
        format!("{}/tokens/device/{}", self.base_url, device_id)
    }

    /// Read-only device org-memberships endpoint (GET, device-JWT-authed). Mints
    /// no token, so the account-refresh loop can poll membership each tick to
    /// discover newly-joined teams without minting a JWT every time.
    pub fn device_orgs_url(&self) -> String {
        format!("{}/tokens/device/orgs", self.base_url)
    }

    /// Anonymous (account-less) device registration endpoint.
    /// Returns a long-lived, user-less device JWT usable only on `/embed`.
    pub fn anon_device_url(&self) -> String {
        format!("{}/tokens/device/anonymous", self.base_url)
    }

    /// Org-scoped token issuance endpoint.
    pub fn org_token_url(&self) -> String {
        format!("{}/tokens/issue", self.base_url)
    }

    /// Bug report submission endpoint.
    pub fn bug_report_url(&self) -> String {
        format!("{}/bugs/reports", self.base_url)
    }

    /// Embedding gateway endpoint (Bedrock Cohere Embed v4).
    pub fn embed_url(&self) -> String {
        format!("{}/embed", self.base_url)
    }

    /// Team sync-config endpoint (GET, member-authed). Returns the broker
    /// `syncUrl` plus bootstrap metadata, or 404/503 when not yet provisioned.
    pub fn team_sync_config_url(&self, team_id: &str) -> String {
        format!("{}/teams/{}/sync-config", self.base_url, team_id)
    }

    /// Team sync-token endpoint (POST, member-authed). Mints a short-lived,
    /// member- and team-scoped token the desktop presents to the sync broker.
    pub fn team_sync_token_url(&self, team_id: &str) -> String {
        format!("{}/teams/{}/sync-token", self.base_url, team_id)
    }

    /// Per-team content-addressed store endpoint (PUT/GET, sync-token-authed).
    /// The broker proxies `hash`-keyed archival bytes to/from per-team object
    /// storage on the same auth boundary as team sync.
    pub fn team_cas_url(&self, team_id: &str, hash: &str) -> String {
        format!("{}/teams/{}/cas/{}", self.base_url, team_id, hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_production_base_url() {
        let config = ApiConfig {
            base_url: "https://api.cairn.computer".to_string(),
        };
        assert_eq!(
            config.device_refresh_url(),
            "https://api.cairn.computer/tokens/device/refresh"
        );
        assert_eq!(
            config.device_url("dev-123"),
            "https://api.cairn.computer/tokens/device/dev-123"
        );
        assert_eq!(
            config.device_orgs_url(),
            "https://api.cairn.computer/tokens/device/orgs"
        );
        assert_eq!(
            config.anon_device_url(),
            "https://api.cairn.computer/tokens/device/anonymous"
        );
        assert_eq!(
            config.org_token_url(),
            "https://api.cairn.computer/tokens/issue"
        );
        assert_eq!(
            config.bug_report_url(),
            "https://api.cairn.computer/bugs/reports"
        );
        assert_eq!(
            config.team_sync_config_url("team-1"),
            "https://api.cairn.computer/teams/team-1/sync-config"
        );
        assert_eq!(
            config.team_sync_token_url("team-1"),
            "https://api.cairn.computer/teams/team-1/sync-token"
        );
    }

    #[test]
    fn test_custom_base_url() {
        let config = ApiConfig {
            base_url: "http://localhost:3000".to_string(),
        };
        assert_eq!(
            config.device_refresh_url(),
            "http://localhost:3000/tokens/device/refresh"
        );
        assert_eq!(config.org_token_url(), "http://localhost:3000/tokens/issue");
    }
}
