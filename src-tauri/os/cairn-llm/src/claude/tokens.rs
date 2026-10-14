//! The data side of a Cairn-managed Claude subscription credential.
//!
//! A managed account's OAuth tokens live in Cairn's credential store, never in
//! the CLI's home: a Claude CLI is launched with only the access token
//! (`CLAUDE_CODE_OAUTH_TOKEN`), and the refresh token is spent by Cairn's
//! single refresh authority alone. Spending it is subscription mechanics and
//! lives behind [`crate::subscription::Subscriptions`]; this module holds the
//! values either side needs around that: the stored token shape, which
//! rotation of it a process was fed, and what a launch receives.
//!
//! No HTTP and no endpoints live here.

use serde_json::{Map, Value};

use crate::accounts::{ApiProvider, ProviderAccount, ProviderAuth};
use crate::interface::session::SecretEnv;

/// Cairn-owned field in the stored token object: which rotation of the
/// account's credential it holds. The first harvest is generation 1, and every
/// publication by the refresh authority advances it by one. It names a
/// rotation without comparing secrets to do it.
pub const GENERATION_FIELD: &str = "cairn_refresh_generation";

/// Cairn-owned field: when this generation was published, in milliseconds
/// since the epoch. A 401 on a generation this young is a token that is valid
/// but cannot do what was asked, not an expired one, so it is not renewed.
pub const PUBLISHED_AT_FIELD: &str = "cairn_published_at";

/// The environment variable a Claude CLI authenticates with.
pub const OAUTH_TOKEN_ENV: &str = "CLAUDE_CODE_OAUTH_TOKEN";

/// How long the CLI waits, on an inference 401, for its environment to carry a
/// different token before failing the turn. Cairn pushes a renewed token into
/// that wait, so a rotation the process missed costs at most this.
pub const OAUTH_401_WAIT_ENV: &str = "CLAUDE_CODE_OAUTH_401_WAIT_MS";
pub const OAUTH_401_WAIT_MS: u64 = 20_000;

/// One stored Claude OAuth credential: the CLI's `claudeAiOauth` object held
/// verbatim (`accessToken`, `refreshToken`, `expiresAt`, `scopes`,
/// `subscriptionType`, `rateLimitTier`, and anything a later CLI adds), plus
/// Cairn's own rotation fields.
///
/// `Debug` names the generation and expiry and never a token.
#[derive(Clone, PartialEq)]
pub struct ClaudeOAuth(Map<String, Value>);

impl ClaudeOAuth {
    /// Parse the stored form. An object without an access token is not a
    /// credential.
    pub fn parse(json: &str) -> Result<Self, String> {
        let value: Value = serde_json::from_str(json)
            .map_err(|e| format!("Claude OAuth credential is not JSON: {e}"))?;
        Self::from_value(value)
    }

    /// Parse what the CLI itself stores — the `.credentials.json` file or its
    /// Keychain item, both `{"claudeAiOauth": {...}}`.
    pub fn from_cli_credentials(json: &str) -> Result<Self, String> {
        let value: Value = serde_json::from_str(json)
            .map_err(|e| format!("Claude CLI credentials are not JSON: {e}"))?;
        let oauth = value
            .get("claudeAiOauth")
            .cloned()
            .ok_or_else(|| "Claude CLI credentials have no claudeAiOauth entry".to_string())?;
        Self::from_value(oauth)
    }

    fn from_value(value: Value) -> Result<Self, String> {
        let Value::Object(map) = value else {
            return Err("Claude OAuth credential is not an object".to_string());
        };
        let oauth = Self(map);
        if oauth.access_token().is_empty() {
            return Err("Claude OAuth credential has no access token".to_string());
        }
        Ok(oauth)
    }

    pub fn to_json(&self) -> String {
        Value::Object(self.0.clone()).to_string()
    }

    fn str_field(&self, name: &str) -> Option<&str> {
        self.0
            .get(name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    }

    pub fn access_token(&self) -> &str {
        self.str_field("accessToken").unwrap_or_default()
    }

    pub fn refresh_token(&self) -> Option<&str> {
        self.str_field("refreshToken")
    }

    /// When the access token expires, in milliseconds since the epoch.
    pub fn expires_at_ms(&self) -> Option<i64> {
        self.0.get("expiresAt").and_then(Value::as_i64)
    }

    pub fn scopes(&self) -> Vec<String> {
        self.0
            .get("scopes")
            .and_then(Value::as_array)
            .map(|scopes| {
                scopes
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Which rotation this is. A credential Cairn has not stamped reads as 0.
    pub fn generation(&self) -> u64 {
        self.0
            .get(GENERATION_FIELD)
            .and_then(Value::as_u64)
            .unwrap_or(0)
    }

    pub fn published_at_ms(&self) -> Option<i64> {
        self.0.get(PUBLISHED_AT_FIELD).and_then(Value::as_i64)
    }

    /// Milliseconds of validity left at `now_ms`. `None` when the credential
    /// does not say when it expires, which callers treat as "renew".
    pub fn remaining_ms(&self, now_ms: i64) -> Option<i64> {
        self.expires_at_ms().map(|expires| expires - now_ms)
    }

    /// This credential stamped as `generation`, published at `now_ms`.
    pub fn stamped(mut self, generation: u64, now_ms: i64) -> Self {
        self.0
            .insert(GENERATION_FIELD.into(), Value::from(generation));
        self.0
            .insert(PUBLISHED_AT_FIELD.into(), Value::from(now_ms));
        self
    }

    /// The next rotation of this credential: the provider's new tokens laid
    /// over the durable object, so every field the CLI or an earlier Cairn
    /// wrote survives. A response without a refresh token keeps the current
    /// one, because the provider is keeping it alive.
    pub fn rotated(
        &self,
        access_token: &str,
        refresh_token: Option<&str>,
        expires_at_ms: i64,
        scopes: Option<Vec<String>>,
        now_ms: i64,
    ) -> Self {
        let mut next = self.clone();
        next.0
            .insert("accessToken".into(), Value::from(access_token));
        if let Some(refresh) = refresh_token.filter(|r| !r.is_empty()) {
            next.0.insert("refreshToken".into(), Value::from(refresh));
        }
        next.0
            .insert("expiresAt".into(), Value::from(expires_at_ms));
        if let Some(scopes) = scopes.filter(|s| !s.is_empty()) {
            next.0.insert("scopes".into(), Value::from(scopes));
        }
        next.stamped(self.generation().saturating_add(1), now_ms)
    }
}

impl std::fmt::Debug for ClaudeOAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaudeOAuth")
            .field("generation", &self.generation())
            .field("expires_at_ms", &self.expires_at_ms())
            .field("has_refresh_token", &self.refresh_token().is_some())
            .finish()
    }
}

/// A managed Claude account's stored credential, if it is one and has been
/// harvested. `Some(Err)` is a stored credential that no longer parses.
pub fn stored_claude_oauth(account: &ProviderAccount) -> Option<Result<ClaudeOAuth, String>> {
    match (&account.api_provider, &account.auth) {
        (ApiProvider::Anthropic, ProviderAuth::ClaudeProfile { oauth: Some(json) }) => {
            Some(ClaudeOAuth::parse(json))
        }
        _ => None,
    }
}

/// What a Claude process launched on a managed account receives, and which
/// rotation that was, so a later push can tell whether the process is behind.
#[derive(Debug)]
pub struct ClaudeSessionCredential {
    pub env: SecretEnv,
    pub generation: u64,
}

impl ClaudeSessionCredential {
    /// The launch environment for `oauth`, running in the CLI home `home`.
    ///
    /// The home is kept for the CLI's transcripts and state; it holds no
    /// secret. The 401 wait lets a process that missed a rotation recover the
    /// same turn when the renewed token is pushed.
    pub fn for_launch(oauth: &ClaudeOAuth, home: &std::path::Path) -> Self {
        Self {
            env: SecretEnv::new()
                .with(OAUTH_TOKEN_ENV, oauth.access_token())
                .with("CLAUDE_CONFIG_DIR", home.to_string_lossy())
                .with(OAUTH_401_WAIT_ENV, OAUTH_401_WAIT_MS.to_string()),
            generation: oauth.generation(),
        }
    }

    /// Only the token, for pushing into a live process.
    pub fn for_push(oauth: &ClaudeOAuth) -> Self {
        Self {
            env: SecretEnv::new().with(OAUTH_TOKEN_ENV, oauth.access_token()),
            generation: oauth.generation(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLI_FILE: &str = r#"{"claudeAiOauth":{"accessToken":"at-1","refreshToken":"rt-1","expiresAt":1000,"scopes":["user:inference","user:profile"],"subscriptionType":"max","rateLimitTier":"default_claude_max_20x"}}"#;

    #[test]
    fn harvests_the_cli_object_verbatim() {
        let oauth = ClaudeOAuth::from_cli_credentials(CLI_FILE).unwrap();
        assert_eq!(oauth.access_token(), "at-1");
        assert_eq!(oauth.refresh_token(), Some("rt-1"));
        assert_eq!(oauth.expires_at_ms(), Some(1000));
        assert_eq!(oauth.scopes(), vec!["user:inference", "user:profile"]);
        assert_eq!(oauth.generation(), 0);
        let stored = ClaudeOAuth::parse(&oauth.clone().stamped(1, 5).to_json()).unwrap();
        assert_eq!(stored.generation(), 1);
        assert_eq!(stored.published_at_ms(), Some(5));
        assert!(stored.to_json().contains("default_claude_max_20x"));
    }

    #[test]
    fn a_rotation_keeps_unknown_fields_and_a_refresh_token_the_provider_kept() {
        let oauth = ClaudeOAuth::from_cli_credentials(CLI_FILE)
            .unwrap()
            .stamped(3, 0);
        let next = oauth.rotated("at-2", None, 9000, None, 50);
        assert_eq!(next.access_token(), "at-2");
        assert_eq!(next.refresh_token(), Some("rt-1"));
        assert_eq!(next.expires_at_ms(), Some(9000));
        assert_eq!(next.generation(), 4);
        assert_eq!(next.published_at_ms(), Some(50));
        assert!(next.to_json().contains("subscriptionType"));
        let next = next.rotated("at-3", Some("rt-3"), 10_000, None, 60);
        assert_eq!(next.refresh_token(), Some("rt-3"));
    }

    #[test]
    fn debug_and_the_launch_env_never_show_the_refresh_token() {
        let oauth = ClaudeOAuth::from_cli_credentials(CLI_FILE).unwrap();
        assert!(!format!("{oauth:?}").contains("rt-1"));
        assert!(!format!("{oauth:?}").contains("at-1"));
        let launch = ClaudeSessionCredential::for_launch(&oauth, std::path::Path::new("/h"));
        assert!(launch.env.contains_value("at-1"));
        assert!(!launch.env.contains_value("rt-1"));
        assert!(launch.env.names().any(|n| n == OAUTH_401_WAIT_ENV));
    }

    #[test]
    fn rejects_what_is_not_a_credential() {
        assert!(ClaudeOAuth::parse("{}").is_err());
        assert!(ClaudeOAuth::parse("[]").is_err());
        assert!(ClaudeOAuth::from_cli_credentials(r#"{"mcpOAuth":{}}"#).is_err());
    }
}
