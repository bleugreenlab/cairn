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
    pub(crate) base_url: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        let base = std::env::var("CAIRN_API_URL")
            .unwrap_or_else(|_| "https://api.cairn.computer".to_string());
        Self { base_url: base }
    }
}

impl ApiConfig {
    /// The relay every install's channel lives on.
    pub fn relay_url(&self) -> String {
        format!("{}/relay", self.base_url.trim_end_matches('/'))
    }

    /// Runner registration endpoint (member-authenticated).
    pub fn runner_registration_url(&self) -> String {
        format!("{}/runners/register", self.base_url.trim_end_matches('/'))
    }

    /// Runner-authenticated runtime-admission collection endpoint.
    pub fn runner_admission_url(&self, operation: &str) -> String {
        format!(
            "{}/runners/admission/{}",
            self.base_url.trim_end_matches('/'),
            operation.trim_start_matches('/')
        )
    }

    /// Runner-authenticated endpoint for one runtime-admission request.
    pub fn runner_admission_request_url(&self, request_id: &str) -> String {
        self.runner_admission_url(request_id)
    }
    /// A runner-authenticated approvals-service endpoint, `path` below
    /// `/approvals/`.
    pub fn approvals_url(&self, path: &str) -> String {
        format!(
            "{}/approvals/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    /// The API's published signing keys.
    pub fn jwks_url(&self) -> String {
        format!(
            "{}/.well-known/jwks.json",
            self.base_url.trim_end_matches('/')
        )
    }

    /// The base URL, which keys per-deployment caches.
    pub fn base_url(&self) -> &str {
        self.base_url.trim_end_matches('/')
    }

    /// Device JWT refresh endpoint.
    pub(crate) fn device_refresh_url(&self) -> String {
        format!("{}/tokens/device/refresh", self.base_url)
    }

    /// Device token status endpoint.
    pub(crate) fn device_url(&self, device_id: &str) -> String {
        format!("{}/tokens/device/{}", self.base_url, device_id)
    }

    /// Read-only device org-memberships endpoint (GET, device-JWT-authed). Mints
    /// no token, so the account-refresh loop can poll membership each tick to
    /// discover newly-joined teams without minting a JWT every time.
    pub(crate) fn device_orgs_url(&self) -> String {
        format!("{}/tokens/device/orgs", self.base_url)
    }

    /// Anonymous (account-less) device registration endpoint.
    /// Returns a long-lived, user-less device JWT usable only on `/embed`.
    pub(crate) fn anon_device_url(&self) -> String {
        format!("{}/tokens/device/anonymous", self.base_url)
    }

    /// Org-scoped token issuance endpoint.
    pub fn org_token_url(&self) -> String {
        format!("{}/tokens/issue", self.base_url)
    }

    /// Bug report submission endpoint.
    pub(crate) fn bug_report_url(&self) -> String {
        format!("{}/bugs/reports", self.base_url)
    }

    /// Team sync-config endpoint (GET, member-authed). Returns the broker
    /// `syncUrl` plus bootstrap metadata, or 404/503 when not yet provisioned.
    pub(crate) fn team_sync_config_url(&self, team_id: &str) -> String {
        format!("{}/teams/{}/sync-config", self.base_url, team_id)
    }

    /// Team sync-token endpoint (POST, member-authed). Mints a short-lived,
    /// member- and team-scoped token the desktop presents to the sync broker.
    pub(crate) fn team_sync_token_url(&self, team_id: &str) -> String {
        format!("{}/teams/{}/sync-token", self.base_url, team_id)
    }

    /// Device-JWT-authenticated exact-object S3 grant endpoint.
    pub(crate) fn team_object_grants_url(&self, team_id: &str) -> String {
        format!("{}/teams/{}/object-grants", self.base_url, team_id)
    }

    /// Device-JWT-authenticated team attention push endpoint.
    pub(crate) fn push_notify_url(&self) -> String {
        format!("{}/push/notify", self.base_url)
    }

    /// Per-team content-addressed store endpoint (PUT/GET, sync-token-authed).
    /// The broker proxies `hash`-keyed archival bytes to/from per-team object
    /// storage on the same auth boundary as team sync.
    pub(crate) fn team_cas_url(&self, team_id: &str, hash: &str) -> String {
        format!("{}/teams/{}/cas/{}", self.base_url, team_id, hash)
    }
}

/// This runner's credential for the cloud API: the bearer token, and the runner
/// and organization it was issued to.
///
/// The token is our own service's credential, so its payload is read without
/// verifying the signature: the runner needs to know whom it speaks as (the
/// approval audience is `cairn-runner:<runner_id>`), and the service verifies
/// the token on every request anyway.
#[derive(Clone, PartialEq, Eq)]
pub struct RunnerCredential {
    token: String,
    runner_id: String,
    org_id: String,
}

impl std::fmt::Debug for RunnerCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunnerCredential")
            .field("runner_id", &self.runner_id)
            .field("org_id", &self.org_id)
            .finish_non_exhaustive()
    }
}

impl RunnerCredential {
    /// Read a runner token's identity claims.
    pub fn parse(token: &str) -> Result<Self, String> {
        use base64::Engine as _;
        let token = token.trim();
        let payload = token
            .split('.')
            .nth(1)
            .filter(|_| token.split('.').count() == 3)
            .ok_or_else(|| "the runner token is not a JWT".to_string())?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| "the runner token's payload is not base64url".to_string())?;
        #[derive(serde::Deserialize)]
        struct Claims {
            sub: String,
            org_id: String,
        }
        let claims: Claims = serde_json::from_slice(&bytes)
            .map_err(|_| "the runner token names no runner and organization".to_string())?;
        let runner_id = claims
            .sub
            .strip_prefix("runner:")
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| "the runner token's subject is not a runner".to_string())?;
        if claims.org_id.trim().is_empty() {
            return Err("the runner token names no organization".to_string());
        }
        Ok(Self {
            token: token.to_string(),
            runner_id: runner_id.to_string(),
            org_id: claims.org_id,
        })
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn runner_id(&self) -> &str {
        &self.runner_id
    }

    pub fn org_id(&self) -> &str {
        &self.org_id
    }

    /// The audience an approval intent and its Human assertion are bound to.
    pub fn audience(&self) -> String {
        format!("cairn-runner:{}", self.runner_id)
    }
}

/// This runner's credential, from `CAIRN_RUNNER_TOKEN`. The one place the
/// token is read.
pub fn runner_credential() -> Result<RunnerCredential, String> {
    let token = std::env::var("CAIRN_RUNNER_TOKEN")
        .ok()
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| "CAIRN_RUNNER_TOKEN is unavailable".to_string())?;
    RunnerCredential::parse(&token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(claims: serde_json::Value) -> String {
        use base64::Engine as _;
        let encode = |value: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value);
        format!(
            "{}.{}.sig",
            encode(br#"{"alg":"EdDSA"}"#),
            encode(claims.to_string().as_bytes())
        )
    }

    #[test]
    fn a_runner_token_names_its_runner_organization_and_audience() {
        let credential = RunnerCredential::parse(&token(serde_json::json!({
            "sub": "runner:runner-1", "org_id": "org-1", "type": "runner"
        })))
        .unwrap();
        assert_eq!(credential.runner_id(), "runner-1");
        assert_eq!(credential.org_id(), "org-1");
        assert_eq!(credential.audience(), "cairn-runner:runner-1");
        assert!(!format!("{credential:?}").contains("sig"));

        for bad in [
            token(serde_json::json!({"sub": "user-1", "org_id": "org-1"})),
            token(serde_json::json!({"sub": "runner:", "org_id": "org-1"})),
            token(serde_json::json!({"sub": "runner:r", "org_id": ""})),
            token(serde_json::json!({"sub": "runner:r"})),
            "not-a-jwt".to_string(),
        ] {
            assert!(RunnerCredential::parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn approvals_and_jwks_urls_hang_off_the_base() {
        let config = ApiConfig {
            base_url: "https://api.cairn.computer/".to_string(),
        };
        assert_eq!(
            config.approvals_url("/challenges/apr_1"),
            "https://api.cairn.computer/approvals/challenges/apr_1"
        );
        assert_eq!(
            config.jwks_url(),
            "https://api.cairn.computer/.well-known/jwks.json"
        );
    }

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
            config.runner_registration_url(),
            "https://api.cairn.computer/runners/register"
        );
        assert_eq!(
            config.runner_admission_url("enqueue"),
            "https://api.cairn.computer/runners/admission/enqueue"
        );
        assert_eq!(
            config.runner_admission_request_url("adm_1"),
            "https://api.cairn.computer/runners/admission/adm_1"
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
        assert_eq!(
            config.team_object_grants_url("team-1"),
            "https://api.cairn.computer/teams/team-1/object-grants"
        );
        assert_eq!(
            config.push_notify_url(),
            "https://api.cairn.computer/push/notify"
        );
        assert_eq!(config.relay_url(), "https://api.cairn.computer/relay");
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
