//! OS-keychain persistence for MCP OAuth credentials and tokens.
//!
//! Mirrors [`crate::config::secrets`] but in a separate keychain namespace
//! (`computer.cairn.mcp-oauth.{env}`) so OAuth state never collides with the
//! `${VAR}` secrets store. The account is the scoped MCP credential key
//! (workspace server name, or `project/{project_path}/{server}`); the value is a single
//! JSON blob ([`StoredAuth`]) holding the resolved endpoints, client
//! credentials, the RFC8707 resource, granted scopes, and the access/refresh
//! tokens.
//!
//! Access/refresh tokens and any client secret are secret and live only here,
//! never in `settings.yaml`. The non-secret `client_id`/`scopes` may also live
//! in the settings `oauth` block; this store is the source of truth at runtime.

use keyring::{Entry, Error};
use serde::{Deserialize, Serialize};

use super::{refresh_access_token, TokenSet, EXPIRY_SKEW_SECS};

/// Everything needed to attach a bearer token and refresh it without re-running
/// discovery: the resolved endpoints, the client, the canonical resource, the
/// granted scopes, and the tokens.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredAuth {
    /// Canonical RFC8707 resource identifier bound on every token request.
    pub resource: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_endpoint: Option<String>,
    pub client_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// Absolute access-token expiry (unix seconds), if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    /// Scopes actually granted (or requested when the server is silent).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    /// Scopes a `403 insufficient_scope` asked for but we have not yet been
    /// granted. Surfaced as a step-up prompt; the next authorize widens to these.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs_scopes: Vec<String>,
}

impl StoredAuth {
    /// Whether the access token is expired (or within the refresh skew window).
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => chrono::Utc::now().timestamp() + EXPIRY_SKEW_SECS >= expires_at,
            None => false,
        }
    }

    /// Fold a freshly obtained/refreshed token set into the stored record,
    /// preserving endpoints and client identity.
    pub fn apply_tokens(&mut self, tokens: TokenSet) {
        self.access_token = tokens.access_token;
        if tokens.refresh_token.is_some() {
            self.refresh_token = tokens.refresh_token;
        }
        self.expires_at = tokens.expires_at;
        if let Some(scope) = tokens.scope {
            let granted: Vec<String> = scope.split_whitespace().map(str::to_string).collect();
            if !granted.is_empty() {
                self.scopes = granted;
            }
        }
    }
}

/// The runtime authorization status of a server, for the settings UI.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OAuthStatus {
    /// `authorized`, `needs_auth`, or `error`.
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    /// Scopes a step-up (`403 insufficient_scope`) is waiting on, if any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs_scopes: Vec<String>,
}

impl OAuthStatus {
    fn authorized(scopes: Vec<String>, needs_scopes: Vec<String>) -> Self {
        let (state, detail) = if needs_scopes.is_empty() {
            ("authorized", None)
        } else {
            ("authorized", Some("Additional scopes required".to_string()))
        };
        Self {
            state: state.to_string(),
            detail,
            scopes,
            needs_scopes,
        }
    }

    /// The server requires OAuth but has no stored token yet.
    pub fn needs_auth() -> Self {
        Self {
            state: "needs_auth".to_string(),
            detail: None,
            scopes: Vec::new(),
            needs_scopes: Vec::new(),
        }
    }

    /// The server does not require OAuth (no authorization affordance shown).
    pub fn none() -> Self {
        Self {
            state: "none".to_string(),
            detail: None,
            scopes: Vec::new(),
            needs_scopes: Vec::new(),
        }
    }
}

/// Keychain service name for the OAuth namespace, scoped to dev/prod.
fn service() -> String {
    format!(
        "computer.cairn.mcp-oauth.{}",
        cairn_common::paths::env_str()
    )
}

fn entry(server: &str) -> Result<Entry, String> {
    Entry::new(&service(), server).map_err(|e| format!("Failed to open keychain entry: {e}"))
}

/// Load the stored OAuth record for `server`, if one exists.
pub fn load(server: &str) -> Option<StoredAuth> {
    let raw = match entry(server).ok()?.get_password() {
        Ok(value) => value,
        Err(Error::NoEntry) => return None,
        Err(e) => {
            log::warn!("Failed to read OAuth tokens for {server} from keychain: {e}");
            return None;
        }
    };
    match serde_json::from_str(&raw) {
        Ok(auth) => Some(auth),
        Err(e) => {
            log::warn!("Stored OAuth record for {server} is corrupt: {e}");
            None
        }
    }
}

/// Store (or replace) the OAuth record for `server`.
pub fn save(server: &str, auth: &StoredAuth) -> Result<(), String> {
    let blob = serde_json::to_string(auth)
        .map_err(|e| format!("Failed to serialize OAuth record: {e}"))?;
    entry(server)?
        .set_password(&blob)
        .map_err(|e| format!("Failed to store OAuth tokens in keychain: {e}"))
}

/// Remove the stored OAuth record for `server`. Succeeds when none exists.
pub fn delete(server: &str) -> Result<(), String> {
    match entry(server)?.delete_credential() {
        Ok(()) | Err(Error::NoEntry) => Ok(()),
        Err(e) => Err(format!("Failed to delete OAuth tokens from keychain: {e}")),
    }
}

/// The authorization status for `server`. A stored token reports `authorized`
/// (the gateway refreshes it on use when a refresh token is present); a stored
/// token that is already expired AND has no refresh token reports `needs_auth`,
/// because it cannot be made usable without a fresh authorize. Reporting such a
/// dead token as `authorized` is what showed a green "Authorized" badge while
/// every connection failed. Surfaces any pending step-up scopes.
pub fn status(server: &str) -> OAuthStatus {
    match load(server) {
        Some(auth) if auth.is_expired() && auth.refresh_token.is_none() => {
            OAuthStatus::needs_auth()
        }
        Some(auth) => OAuthStatus::authorized(auth.scopes, auth.needs_scopes),
        None => OAuthStatus::needs_auth(),
    }
}

/// Record that a `403 insufficient_scope` asked for `required` scopes, so the
/// settings UI can prompt for a re-authorize that widens the scope set.
pub fn record_scope_step_up(server: &str, required: &[String]) {
    if required.is_empty() {
        return;
    }
    if let Some(mut auth) = load(server) {
        let mut union = auth.scopes.clone();
        for scope in required {
            if !union.contains(scope) {
                union.push(scope.clone());
            }
        }
        auth.needs_scopes = union
            .into_iter()
            .filter(|s| !auth.scopes.contains(s))
            .collect();
        let _ = save(server, &auth);
    }
}

/// Drop the cached record for `server` from memory of validity by clearing its
/// expiry so the next [`get_valid_access_token`] forces a refresh. Used by the
/// gateway after a `401` to recover from a server-side revocation.
pub fn force_expire(server: &str) {
    if let Some(mut auth) = load(server) {
        auth.expires_at = Some(0);
        let _ = save(server, &auth);
    }
}

/// Resolve a currently-valid access token for `server`, refreshing via the
/// refresh-token grant when the stored token is expired (or within the skew
/// window). Returns `None` when the server has no stored authorization or a
/// refresh is impossible/failed — the gateway then surfaces "authorization
/// required".
pub async fn get_valid_access_token(server: &str) -> Option<String> {
    let mut auth = load(server)?;
    if !auth.is_expired() {
        return Some(auth.access_token);
    }
    let refresh_token = auth.refresh_token.clone()?;
    match refresh_access_token(
        &auth.token_endpoint,
        &auth.client_id,
        auth.client_secret.as_deref(),
        &refresh_token,
        &auth.resource,
        scope_param(&auth.scopes).as_deref(),
    )
    .await
    {
        Ok(tokens) => {
            auth.apply_tokens(tokens);
            let access = auth.access_token.clone();
            if let Err(e) = save(server, &auth) {
                log::warn!("Failed to persist refreshed OAuth tokens for {server}: {e}");
            }
            Some(access)
        }
        Err(e) => {
            log::warn!("OAuth token refresh for {server} failed: {e}");
            None
        }
    }
}

fn scope_param(scopes: &[String]) -> Option<String> {
    if scopes.is_empty() {
        None
    } else {
        Some(scopes.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::secrets::mock_keychain;

    fn sample(server_token: &str) -> StoredAuth {
        StoredAuth {
            resource: "https://api.example.com/mcp".to_string(),
            authorization_endpoint: "https://auth.example.com/authorize".to_string(),
            token_endpoint: "https://auth.example.com/token".to_string(),
            registration_endpoint: None,
            client_id: "client-123".to_string(),
            client_secret: None,
            access_token: server_token.to_string(),
            refresh_token: Some("refresh-abc".to_string()),
            expires_at: Some(chrono::Utc::now().timestamp() + 3600),
            scopes: vec!["read".to_string()],
            needs_scopes: Vec::new(),
        }
    }

    #[test]
    fn save_load_delete_roundtrip() {
        mock_keychain::install();
        let server = "store-roundtrip";
        assert!(load(server).is_none());
        assert_eq!(status(server).state, "needs_auth");

        let auth = sample("access-1");
        save(server, &auth).unwrap();
        assert_eq!(load(server), Some(auth.clone()));
        let st = status(server);
        assert_eq!(st.state, "authorized");
        assert_eq!(st.scopes, vec!["read".to_string()]);

        delete(server).unwrap();
        assert!(load(server).is_none());
        // Deleting an absent record is a no-op success.
        delete(server).unwrap();
    }

    #[test]
    fn status_reflects_token_usability_not_mere_presence() {
        mock_keychain::install();

        // Fresh token -> authorized.
        let server = "store-status-valid";
        save(server, &sample("good")).unwrap();
        assert_eq!(status(server).state, "authorized");

        // Expired but refreshable -> still authorized (gateway refreshes on use).
        let server = "store-status-expired-refreshable";
        let mut auth = sample("stale");
        auth.expires_at = Some(chrono::Utc::now().timestamp() - 10);
        assert!(auth.refresh_token.is_some());
        save(server, &auth).unwrap();
        assert_eq!(status(server).state, "authorized");

        // Expired AND no refresh token -> needs_auth: the token is dead and
        // cannot be made usable without a fresh authorize, so the UI must offer
        // it rather than show a green "Authorized" that fails on every connect.
        let server = "store-status-expired-dead";
        let mut auth = sample("dead");
        auth.expires_at = Some(chrono::Utc::now().timestamp() - 10);
        auth.refresh_token = None;
        save(server, &auth).unwrap();
        assert_eq!(status(server).state, "needs_auth");
    }

    #[test]
    fn records_are_scoped_per_server() {
        mock_keychain::install();
        save("store-a", &sample("a-token")).unwrap();
        save("store-b", &sample("b-token")).unwrap();
        assert_eq!(load("store-a").unwrap().access_token, "a-token");
        assert_eq!(load("store-b").unwrap().access_token, "b-token");
    }

    #[test]
    fn is_expired_respects_skew() {
        let mut auth = sample("t");
        auth.expires_at = Some(chrono::Utc::now().timestamp() + EXPIRY_SKEW_SECS - 5);
        assert!(auth.is_expired());
        auth.expires_at = Some(chrono::Utc::now().timestamp() + EXPIRY_SKEW_SECS + 120);
        assert!(!auth.is_expired());
        auth.expires_at = None;
        assert!(!auth.is_expired());
    }

    #[test]
    fn apply_tokens_keeps_prior_refresh_and_updates_scopes() {
        let mut auth = sample("old-access");
        auth.apply_tokens(TokenSet {
            access_token: "new-access".to_string(),
            refresh_token: None,
            expires_at: Some(123),
            scope: Some("read write".to_string()),
            token_type: "Bearer".to_string(),
        });
        assert_eq!(auth.access_token, "new-access");
        assert_eq!(auth.refresh_token.as_deref(), Some("refresh-abc"));
        assert_eq!(auth.expires_at, Some(123));
        assert_eq!(auth.scopes, vec!["read".to_string(), "write".to_string()]);
    }

    #[test]
    fn step_up_records_only_missing_scopes() {
        mock_keychain::install();
        let server = "store-stepup";
        save(server, &sample("t")).unwrap();
        record_scope_step_up(server, &["read".to_string(), "admin".to_string()]);
        let auth = load(server).unwrap();
        // `read` already granted; only `admin` is pending.
        assert_eq!(auth.needs_scopes, vec!["admin".to_string()]);
        assert_eq!(status(server).needs_scopes, vec!["admin".to_string()]);
    }

    #[tokio::test]
    async fn valid_token_returned_without_refresh() {
        mock_keychain::install();
        let server = "store-valid";
        save(server, &sample("still-good")).unwrap();
        assert_eq!(
            get_valid_access_token(server).await.as_deref(),
            Some("still-good")
        );
    }

    #[tokio::test]
    async fn missing_server_yields_none() {
        mock_keychain::install();
        assert!(get_valid_access_token("store-absent").await.is_none());
    }
}
