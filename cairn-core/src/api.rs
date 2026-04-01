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

    /// Org-scoped token issuance endpoint.
    pub fn org_token_url(&self) -> String {
        format!("{}/tokens/issue", self.base_url)
    }

    /// WebSocket URL for remote sync.
    pub fn ws_url(&self, device_id: &str, jwt: &str) -> String {
        let ws_base = self
            .base_url
            .replace("https://", "wss://")
            .replace("http://", "ws://");
        format!("{}/remote/ws/{}?token={}", ws_base, device_id, jwt)
    }

    /// Remote sync HTTP endpoint.
    pub fn sync_url(&self, path: &str) -> String {
        format!("{}/remote/{}", self.base_url, path)
    }

    /// Bug report submission endpoint.
    pub fn bug_report_url(&self) -> String {
        format!("{}/bugs/reports", self.base_url)
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
            config.org_token_url(),
            "https://api.cairn.computer/tokens/issue"
        );
        assert_eq!(
            config.ws_url("dev-abc", "jwt-tok"),
            "wss://api.cairn.computer/remote/ws/dev-abc?token=jwt-tok"
        );
        assert_eq!(
            config.sync_url("events"),
            "https://api.cairn.computer/remote/events"
        );
        assert_eq!(
            config.bug_report_url(),
            "https://api.cairn.computer/bugs/reports"
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
        assert_eq!(
            config.ws_url("dev-1", "tok"),
            "ws://localhost:3000/remote/ws/dev-1?token=tok"
        );
        assert_eq!(config.sync_url("sync"), "http://localhost:3000/remote/sync");
    }
}
