//! User identity types and local identity store.
//!
//! Two-layer architecture:
//! - `IdentityStore` is the new multi-account model (v2 format).
//!   Credentials belong to **API providers** (Anthropic, OpenAI, Google, GitHub),
//!   not backends. Multiple accounts per provider, reorderable by priority.
//! - `UserIdentity` is the backward-compatible runtime type used everywhere —
//!   session startup, MCP handlers, action attribution, git commits.
//!   Resolution converts `IdentityStore` → `UserIdentity` for downstream code.

pub mod crypto;
pub mod local;

use serde::{Deserialize, Serialize};

/// Universal user identity — populated from local config OR auth service JWT claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserIdentity {
    /// Unique ID (local: generated UUID, auth service: service-issued)
    pub user_id: String,
    /// User's email address (used for git identity)
    pub email: String,
    /// User's display name (used for git identity)
    pub name: String,
    /// Claude/Anthropic authentication (optional — only present in local identity, never in JWT claims)
    pub claude_auth: Option<ClaudeAuth>,
    /// Codex/OpenAI authentication (optional — stores full auth.json contents or API key)
    pub codex_auth: Option<CodexAuth>,
    /// GitHub personal access token (optional — separate from GitHub App auth)
    pub github_token: Option<String>,
}

/// Claude authentication method.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ClaudeAuth {
    /// User's own OAuth token (Max subscriber using `claude setup-token`)
    OAuthToken(String),
    /// API key (personal or org-provided)
    ApiKey(String),
}

impl ClaudeAuth {
    /// Get the raw token/key value.
    pub fn value(&self) -> &str {
        match self {
            ClaudeAuth::OAuthToken(v) | ClaudeAuth::ApiKey(v) => v,
        }
    }
}

/// Codex/OpenAI authentication method.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum CodexAuth {
    /// ChatGPT OAuth — stores full auth.json contents (JSON string)
    OAuthToken(String),
    /// OpenAI API key
    ApiKey(String),
}

impl CodexAuth {
    /// Get the raw token/key/json value.
    pub fn value(&self) -> &str {
        match self {
            CodexAuth::OAuthToken(v) | CodexAuth::ApiKey(v) => v,
        }
    }
}

// === Multi-account provider model (v2) ===

/// API provider that a credential authenticates with.
///
/// Credentials belong to providers, not backends. A single Anthropic API key
/// works across both the Claude CLI backend and the Native HTTP backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ApiProvider {
    /// Anthropic APIs (Claude)
    #[serde(rename = "anthropic")]
    Anthropic,
    /// OpenAI APIs (GPT, Codex)
    #[serde(rename = "openai", alias = "open_ai", alias = "open_a_i")]
    OpenAI,
    /// Google APIs (Gemini)
    #[serde(rename = "google")]
    Google,
    /// GitHub APIs
    #[serde(rename = "github", alias = "git_hub")]
    GitHub,
}

impl ApiProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            ApiProvider::Anthropic => "anthropic",
            ApiProvider::OpenAI => "openai",
            ApiProvider::Google => "google",
            ApiProvider::GitHub => "github",
        }
    }

    /// All providers in display order.
    pub fn all() -> &'static [ApiProvider] {
        &[
            ApiProvider::Anthropic,
            ApiProvider::OpenAI,
            ApiProvider::Google,
            ApiProvider::GitHub,
        ]
    }
}

impl std::fmt::Display for ApiProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiProvider::Anthropic => write!(f, "Anthropic"),
            ApiProvider::OpenAI => write!(f, "OpenAI"),
            ApiProvider::Google => write!(f, "Google"),
            ApiProvider::GitHub => write!(f, "GitHub"),
        }
    }
}

/// How an account was discovered/configured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountSource {
    /// User explicitly configured via settings UI
    Configured,
    /// Auto-detected from installed CLI (not persisted)
    LocalCli,
    /// Provided by cairn-server (future — team credentials)
    Server,
}

/// Authentication credential for a provider account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ProviderAuth {
    /// API key — works with both CLI and Native backends
    #[serde(rename = "api_key")]
    ApiKey { value: String },
    /// OAuth token — CLI-specific (Claude OAuth, ChatGPT OAuth)
    #[serde(rename = "oauth_token", alias = "o_auth_token")]
    OAuthToken { value: String },
    /// Ambient CLI auth — no stored credential, uses locally installed CLI
    #[serde(rename = "local_cli")]
    LocalCli,
}

impl ProviderAuth {
    /// Get the raw credential value, if any.
    pub fn credential_value(&self) -> Option<&str> {
        match self {
            ProviderAuth::ApiKey { value } | ProviderAuth::OAuthToken { value } => Some(value),
            ProviderAuth::LocalCli => None,
        }
    }

    /// Short description of auth type for UI display.
    pub fn auth_type_label(&self) -> &'static str {
        match self {
            ProviderAuth::ApiKey { .. } => "api_key",
            ProviderAuth::OAuthToken { .. } => "oauth_token",
            ProviderAuth::LocalCli => "local_cli",
        }
    }
}

/// A named credential for an API provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAccount {
    pub id: String,
    pub label: String,
    pub api_provider: ApiProvider,
    pub source: AccountSource,
    pub auth: ProviderAuth,
    /// Position = priority (lower = higher priority)
    pub sort_order: i32,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

impl ProviderAccount {
    /// Which agent backends can consume this credential.
    pub fn compatible_backends(&self) -> Vec<&'static str> {
        match (&self.api_provider, &self.auth) {
            // API keys work with both CLI and Native
            (ApiProvider::Anthropic, ProviderAuth::ApiKey { .. }) => vec!["claude", "native"],
            (ApiProvider::OpenAI, ProviderAuth::ApiKey { .. }) => vec!["codex", "native"],
            (ApiProvider::Google, ProviderAuth::ApiKey { .. }) => vec!["native"],
            (ApiProvider::GitHub, ProviderAuth::ApiKey { .. }) => vec![],
            // OAuth is CLI-specific
            (ApiProvider::Anthropic, ProviderAuth::OAuthToken { .. }) => vec!["claude"],
            (ApiProvider::OpenAI, ProviderAuth::OAuthToken { .. }) => vec!["codex"],
            (ApiProvider::Google, ProviderAuth::OAuthToken { .. }) => vec![],
            (ApiProvider::GitHub, ProviderAuth::OAuthToken { .. }) => vec![],
            // Local CLI is CLI-specific
            (ApiProvider::Anthropic, ProviderAuth::LocalCli) => vec!["claude"],
            (ApiProvider::OpenAI, ProviderAuth::LocalCli) => vec!["codex"],
            _ => vec![],
        }
    }
}

/// Separate git commit identity (name + email).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitIdentity {
    pub id: String,
    pub label: String,
    pub name: String,
    pub email: String,
    /// First = default
    pub sort_order: i32,
}

/// The full identity store — multi-account model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityStore {
    pub user_id: String,
    /// Sorted by (api_provider, sort_order)
    pub accounts: Vec<ProviderAccount>,
    /// Sorted by sort_order
    pub git_identities: Vec<GitIdentity>,
    /// Per-project account overrides, keyed by project ID.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub project_overrides: std::collections::HashMap<String, AccountOverrides>,
}

impl IdentityStore {
    /// Build an IdentityStore from a UserIdentity (server-side convenience).
    ///
    /// Converts the flat UserIdentity fields into the structured IdentityStore
    /// format with ProviderAccounts and GitIdentities.
    pub fn from_user_identity(identity: &UserIdentity) -> Self {
        let mut accounts = Vec::new();
        let now = chrono::Utc::now().timestamp();

        if let Some(auth) = &identity.claude_auth {
            let provider_auth = match auth {
                ClaudeAuth::ApiKey(v) => ProviderAuth::ApiKey { value: v.clone() },
                ClaudeAuth::OAuthToken(v) => ProviderAuth::OAuthToken { value: v.clone() },
            };
            accounts.push(ProviderAccount {
                id: format!("server_{}", uuid::Uuid::new_v4()),
                label: "Server".to_string(),
                api_provider: ApiProvider::Anthropic,
                source: AccountSource::Server,
                auth: provider_auth,
                sort_order: 0,
                created_at: now,
                last_used_at: None,
            });
        }
        if let Some(auth) = &identity.codex_auth {
            let provider_auth = match auth {
                CodexAuth::ApiKey(v) => ProviderAuth::ApiKey { value: v.clone() },
                CodexAuth::OAuthToken(v) => ProviderAuth::OAuthToken { value: v.clone() },
            };
            accounts.push(ProviderAccount {
                id: format!("server_{}", uuid::Uuid::new_v4()),
                label: "Server".to_string(),
                api_provider: ApiProvider::OpenAI,
                source: AccountSource::Server,
                auth: provider_auth,
                sort_order: 0,
                created_at: now,
                last_used_at: None,
            });
        }
        if let Some(token) = &identity.github_token {
            accounts.push(ProviderAccount {
                id: format!("server_{}", uuid::Uuid::new_v4()),
                label: "Server".to_string(),
                api_provider: ApiProvider::GitHub,
                source: AccountSource::Server,
                auth: ProviderAuth::ApiKey {
                    value: token.clone(),
                },
                sort_order: 0,
                created_at: now,
                last_used_at: None,
            });
        }

        Self {
            user_id: identity.user_id.clone(),
            accounts,
            git_identities: vec![GitIdentity {
                id: format!("gi_{}", uuid::Uuid::new_v4()),
                label: "Server".to_string(),
                name: identity.name.clone(),
                email: identity.email.clone(),
                sort_order: 0,
            }],
            project_overrides: Default::default(),
        }
    }

    /// Get accounts for a specific provider, sorted by priority.
    pub fn accounts_for_provider(&self, provider: ApiProvider) -> Vec<&ProviderAccount> {
        let mut accounts: Vec<_> = self
            .accounts
            .iter()
            .filter(|a| a.api_provider == provider)
            .collect();
        accounts.sort_by_key(|a| a.sort_order);
        accounts
    }

    /// Find the highest-priority account for a provider that's compatible with a backend.
    fn best_account_for(
        &self,
        provider: ApiProvider,
        backend: &str,
        override_id: Option<&str>,
    ) -> Option<&ProviderAccount> {
        let accounts = self.accounts_for_provider(provider);

        // If there's an explicit override, use only that account — no fallback
        if let Some(id) = override_id {
            return accounts
                .iter()
                .find(|a| a.id == id && a.compatible_backends().contains(&backend))
                .copied();
        }

        // No override — first compatible account wins
        accounts
            .into_iter()
            .find(|a| a.compatible_backends().contains(&backend))
    }

    /// Get the default git identity (first by sort order).
    pub fn default_git_identity(&self) -> Option<&GitIdentity> {
        self.git_identities.iter().min_by_key(|g| g.sort_order)
    }

    /// Resolve the multi-account store into a backward-compatible `UserIdentity`.
    ///
    /// This finds the highest-priority compatible account per provider/backend
    /// and maps them to the fields `UserIdentity` expects.
    pub fn resolve(&self, overrides: Option<&AccountOverrides>) -> UserIdentity {
        let anthropic_override = overrides.and_then(|o| o.anthropic_account_id.as_deref());
        let openai_override = overrides.and_then(|o| o.openai_account_id.as_deref());
        let github_override = overrides.and_then(|o| o.github_account_id.as_deref());
        let git_override = overrides.and_then(|o| o.git_identity_id.as_deref());

        // Resolve Claude auth from best Anthropic account
        let claude_auth = self
            .best_account_for(ApiProvider::Anthropic, "claude", anthropic_override)
            .and_then(|a| match &a.auth {
                ProviderAuth::ApiKey { value } => Some(ClaudeAuth::ApiKey(value.clone())),
                ProviderAuth::OAuthToken { value } => Some(ClaudeAuth::OAuthToken(value.clone())),
                ProviderAuth::LocalCli => None, // Local CLI doesn't need stored auth
            });

        // Resolve Codex auth from best OpenAI account
        let codex_auth = self
            .best_account_for(ApiProvider::OpenAI, "codex", openai_override)
            .and_then(|a| match &a.auth {
                ProviderAuth::ApiKey { value } => Some(CodexAuth::ApiKey(value.clone())),
                ProviderAuth::OAuthToken { value } => Some(CodexAuth::OAuthToken(value.clone())),
                ProviderAuth::LocalCli => None,
            });

        // Resolve GitHub token
        let github_token = self
            .accounts_for_provider(ApiProvider::GitHub)
            .into_iter()
            .find(|a| {
                if let Some(id) = github_override {
                    a.id == id
                } else {
                    true
                }
            })
            .and_then(|a| a.auth.credential_value().map(|v| v.to_string()));

        // Resolve git identity
        let git_identity = if let Some(id) = git_override {
            self.git_identities
                .iter()
                .find(|g| g.id == id)
                .or_else(|| self.default_git_identity())
        } else {
            self.default_git_identity()
        };

        let (name, email) = match git_identity {
            Some(gi) => (gi.name.clone(), gi.email.clone()),
            None => (String::new(), String::new()),
        };

        UserIdentity {
            user_id: self.user_id.clone(),
            email,
            name,
            claude_auth,
            codex_auth,
            github_token,
        }
    }

    /// Check if any Anthropic account has local CLI auth.
    pub fn has_local_cli(&self, provider: ApiProvider) -> bool {
        self.accounts
            .iter()
            .any(|a| a.api_provider == provider && matches!(a.auth, ProviderAuth::LocalCli))
    }
}

/// Per-project overrides for account selection.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anthropic_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub openai_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub google_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_identity_id: Option<String>,
}

/// Frontend-safe account info (no credential values exposed).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    pub id: String,
    pub label: String,
    pub api_provider: ApiProvider,
    pub source: AccountSource,
    pub auth_type: String,
    pub compatible_backends: Vec<String>,
    pub sort_order: i32,
    pub last_used_at: Option<i64>,
}

impl From<&ProviderAccount> for AccountInfo {
    fn from(account: &ProviderAccount) -> Self {
        Self {
            id: account.id.clone(),
            label: account.label.clone(),
            api_provider: account.api_provider,
            source: account.source.clone(),
            auth_type: account.auth.auth_type_label().to_string(),
            compatible_backends: account
                .compatible_backends()
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            sort_order: account.sort_order,
            last_used_at: account.last_used_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_auth_value_oauth() {
        let auth = CodexAuth::OAuthToken("oauth-json".to_string());
        assert_eq!(auth.value(), "oauth-json");
    }

    #[test]
    fn codex_auth_value_api_key() {
        let auth = CodexAuth::ApiKey("sk-key".to_string());
        assert_eq!(auth.value(), "sk-key");
    }

    #[test]
    fn codex_auth_serde_oauth_roundtrip() {
        let auth = CodexAuth::OAuthToken("token-data".to_string());
        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains(r#""type":"o_auth_token""#), "got: {json}");
        assert!(json.contains(r#""value":"token-data""#), "got: {json}");
        let deserialized: CodexAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.value(), "token-data");
    }

    #[test]
    fn codex_auth_serde_api_key_roundtrip() {
        let auth = CodexAuth::ApiKey("sk-test".to_string());
        let json = serde_json::to_string(&auth).unwrap();
        assert!(json.contains(r#""type":"api_key""#), "got: {json}");
        assert!(json.contains(r#""value":"sk-test""#), "got: {json}");
        let deserialized: CodexAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.value(), "sk-test");
    }

    // === Multi-account provider tests ===

    fn test_account(provider: ApiProvider, auth: ProviderAuth) -> ProviderAccount {
        ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label: "Test".to_string(),
            api_provider: provider,
            source: AccountSource::Configured,
            auth,
            sort_order: 0,
            created_at: 0,
            last_used_at: None,
        }
    }

    fn test_store() -> IdentityStore {
        IdentityStore {
            user_id: "local-test".to_string(),
            accounts: vec![],
            git_identities: vec![GitIdentity {
                id: "gi_1".to_string(),
                label: "Test".to_string(),
                name: "Test User".to_string(),
                email: "test@example.com".to_string(),
                sort_order: 0,
            }],
            project_overrides: Default::default(),
        }
    }

    #[test]
    fn compatible_backends_anthropic_api_key() {
        let account = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "sk-ant-test".to_string(),
            },
        );
        assert_eq!(account.compatible_backends(), vec!["claude", "native"]);
    }

    #[test]
    fn compatible_backends_anthropic_oauth() {
        let account = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::OAuthToken {
                value: "oauth-token".to_string(),
            },
        );
        assert_eq!(account.compatible_backends(), vec!["claude"]);
    }

    #[test]
    fn compatible_backends_anthropic_local_cli() {
        let account = test_account(ApiProvider::Anthropic, ProviderAuth::LocalCli);
        assert_eq!(account.compatible_backends(), vec!["claude"]);
    }

    #[test]
    fn compatible_backends_openai_api_key() {
        let account = test_account(
            ApiProvider::OpenAI,
            ProviderAuth::ApiKey {
                value: "sk-test".to_string(),
            },
        );
        assert_eq!(account.compatible_backends(), vec!["codex", "native"]);
    }

    #[test]
    fn compatible_backends_google_api_key() {
        let account = test_account(
            ApiProvider::Google,
            ProviderAuth::ApiKey {
                value: "goog-key".to_string(),
            },
        );
        assert_eq!(account.compatible_backends(), vec!["native"]);
    }

    #[test]
    fn resolve_empty_store() {
        let store = test_store();
        let identity = store.resolve(None);
        assert_eq!(identity.user_id, "local-test");
        assert_eq!(identity.name, "Test User");
        assert_eq!(identity.email, "test@example.com");
        assert!(identity.claude_auth.is_none());
        assert!(identity.codex_auth.is_none());
        assert!(identity.github_token.is_none());
    }

    #[test]
    fn resolve_with_anthropic_api_key() {
        let mut store = test_store();
        store.accounts.push(test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "sk-ant-key".to_string(),
            },
        ));
        let identity = store.resolve(None);
        match &identity.claude_auth {
            Some(ClaudeAuth::ApiKey(key)) => assert_eq!(key, "sk-ant-key"),
            other => panic!("Expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn resolve_with_overrides() {
        let mut store = test_store();
        let mut acc1 = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "primary-key".to_string(),
            },
        );
        acc1.id = "acc_primary".to_string();
        acc1.sort_order = 0;

        let mut acc2 = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "secondary-key".to_string(),
            },
        );
        acc2.id = "acc_secondary".to_string();
        acc2.sort_order = 1;

        store.accounts.push(acc1);
        store.accounts.push(acc2);

        // Without override: primary wins
        let identity = store.resolve(None);
        match &identity.claude_auth {
            Some(ClaudeAuth::ApiKey(key)) => assert_eq!(key, "primary-key"),
            other => panic!("Expected primary key, got {:?}", other),
        }

        // With override: secondary wins
        let overrides = AccountOverrides {
            anthropic_account_id: Some("acc_secondary".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(Some(&overrides));
        match &identity.claude_auth {
            Some(ClaudeAuth::ApiKey(key)) => assert_eq!(key, "secondary-key"),
            other => panic!("Expected secondary key, got {:?}", other),
        }
    }

    #[test]
    fn resolve_local_cli_yields_no_auth() {
        let mut store = test_store();
        store
            .accounts
            .push(test_account(ApiProvider::Anthropic, ProviderAuth::LocalCli));
        let identity = store.resolve(None);
        // LocalCli doesn't produce a stored auth value
        assert!(identity.claude_auth.is_none());
    }

    #[test]
    fn api_provider_serde_roundtrip() {
        let provider = ApiProvider::Anthropic;
        let json = serde_json::to_string(&provider).unwrap();
        assert_eq!(json, r#""anthropic""#);
        let deserialized: ApiProvider = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, provider);
    }

    #[test]
    fn account_info_from_provider_account() {
        let account = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "secret".to_string(),
            },
        );
        let info = AccountInfo::from(&account);
        assert_eq!(info.api_provider, ApiProvider::Anthropic);
        assert_eq!(info.auth_type, "api_key");
        assert_eq!(info.compatible_backends, vec!["claude", "native"]);
    }

    #[test]
    fn accounts_for_provider_sorted() {
        let mut store = test_store();
        let mut acc_b = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "b".to_string(),
            },
        );
        acc_b.sort_order = 1;
        acc_b.label = "B".to_string();
        let mut acc_a = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "a".to_string(),
            },
        );
        acc_a.sort_order = 0;
        acc_a.label = "A".to_string();

        store.accounts.push(acc_b);
        store.accounts.push(acc_a);

        let accounts = store.accounts_for_provider(ApiProvider::Anthropic);
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0].label, "A");
        assert_eq!(accounts[1].label, "B");
    }

    // === Additional coverage for unverified claims ===

    #[test]
    fn compatible_backends_openai_oauth() {
        let account = test_account(
            ApiProvider::OpenAI,
            ProviderAuth::OAuthToken {
                value: "chatgpt-oauth".to_string(),
            },
        );
        assert_eq!(account.compatible_backends(), vec!["codex"]);
    }

    #[test]
    fn compatible_backends_openai_local_cli() {
        let account = test_account(ApiProvider::OpenAI, ProviderAuth::LocalCli);
        assert_eq!(account.compatible_backends(), vec!["codex"]);
    }

    #[test]
    fn compatible_backends_github_api_key_empty() {
        let account = test_account(
            ApiProvider::GitHub,
            ProviderAuth::ApiKey {
                value: "ghp_test".to_string(),
            },
        );
        // GitHub API keys don't map to any agent backend
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn compatible_backends_google_oauth_empty() {
        let account = test_account(
            ApiProvider::Google,
            ProviderAuth::OAuthToken {
                value: "goog-oauth".to_string(),
            },
        );
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn compatible_backends_github_oauth_empty() {
        let account = test_account(
            ApiProvider::GitHub,
            ProviderAuth::OAuthToken {
                value: "gh-oauth".to_string(),
            },
        );
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn resolve_with_openai_api_key() {
        let mut store = test_store();
        store.accounts.push(test_account(
            ApiProvider::OpenAI,
            ProviderAuth::ApiKey {
                value: "sk-openai-key".to_string(),
            },
        ));
        let identity = store.resolve(None);
        match &identity.codex_auth {
            Some(CodexAuth::ApiKey(key)) => assert_eq!(key, "sk-openai-key"),
            other => panic!("Expected CodexAuth::ApiKey, got {:?}", other),
        }
        // Anthropic should remain unset
        assert!(identity.claude_auth.is_none());
    }

    #[test]
    fn resolve_with_openai_oauth() {
        let mut store = test_store();
        store.accounts.push(test_account(
            ApiProvider::OpenAI,
            ProviderAuth::OAuthToken {
                value: "chatgpt-oauth-json".to_string(),
            },
        ));
        let identity = store.resolve(None);
        match &identity.codex_auth {
            Some(CodexAuth::OAuthToken(val)) => assert_eq!(val, "chatgpt-oauth-json"),
            other => panic!("Expected CodexAuth::OAuthToken, got {:?}", other),
        }
    }

    #[test]
    fn resolve_with_github_token() {
        let mut store = test_store();
        store.accounts.push(test_account(
            ApiProvider::GitHub,
            ProviderAuth::ApiKey {
                value: "ghp_my_token".to_string(),
            },
        ));
        let identity = store.resolve(None);
        assert_eq!(identity.github_token, Some("ghp_my_token".to_string()));
    }

    #[test]
    fn resolve_github_token_with_override() {
        let mut store = test_store();
        let mut acc1 = test_account(
            ApiProvider::GitHub,
            ProviderAuth::ApiKey {
                value: "ghp_primary".to_string(),
            },
        );
        acc1.id = "gh_1".to_string();
        acc1.sort_order = 0;
        let mut acc2 = test_account(
            ApiProvider::GitHub,
            ProviderAuth::ApiKey {
                value: "ghp_secondary".to_string(),
            },
        );
        acc2.id = "gh_2".to_string();
        acc2.sort_order = 1;

        store.accounts.push(acc1);
        store.accounts.push(acc2);

        // Without override: first account wins
        let identity = store.resolve(None);
        assert_eq!(identity.github_token, Some("ghp_primary".to_string()));

        // With override: selected account wins
        let overrides = AccountOverrides {
            github_account_id: Some("gh_2".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(Some(&overrides));
        assert_eq!(identity.github_token, Some("ghp_secondary".to_string()));
    }

    #[test]
    fn resolve_git_identity_override() {
        let mut store = test_store();
        // test_store() already has gi_1 with sort_order 0
        store.git_identities.push(GitIdentity {
            id: "gi_2".to_string(),
            label: "Work".to_string(),
            name: "Work Name".to_string(),
            email: "work@corp.com".to_string(),
            sort_order: 1,
        });

        // Without override: default (sort_order 0) wins
        let identity = store.resolve(None);
        assert_eq!(identity.name, "Test User");
        assert_eq!(identity.email, "test@example.com");

        // With override: selected git identity wins
        let overrides = AccountOverrides {
            git_identity_id: Some("gi_2".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(Some(&overrides));
        assert_eq!(identity.name, "Work Name");
        assert_eq!(identity.email, "work@corp.com");
    }

    #[test]
    fn resolve_invalid_override_no_fallback() {
        // When an explicit override points to a nonexistent account,
        // resolve returns None for that auth — it does NOT fall back.
        let mut store = test_store();
        store.accounts.push(test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "real-key".to_string(),
            },
        ));

        // Without override: works fine
        let identity = store.resolve(None);
        assert!(identity.claude_auth.is_some());

        // With invalid override: no fallback
        let overrides = AccountOverrides {
            anthropic_account_id: Some("nonexistent_id".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(Some(&overrides));
        assert!(
            identity.claude_auth.is_none(),
            "Explicit override to nonexistent account should not fall back"
        );
    }

    #[test]
    fn resolve_invalid_git_identity_override_falls_back() {
        // Git identity override with invalid ID falls back to default
        // (different behavior from account overrides — see line 322-326)
        let store = test_store();
        let overrides = AccountOverrides {
            git_identity_id: Some("nonexistent_gi".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(Some(&overrides));
        // Falls back to default git identity
        assert_eq!(identity.name, "Test User");
        assert_eq!(identity.email, "test@example.com");
    }

    #[test]
    fn resolve_no_git_identity_yields_empty_strings() {
        let store = IdentityStore {
            user_id: "test".to_string(),
            accounts: vec![],
            git_identities: vec![],
            project_overrides: Default::default(),
        };
        let identity = store.resolve(None);
        assert_eq!(identity.name, "");
        assert_eq!(identity.email, "");
    }

    #[test]
    fn has_local_cli_true_when_present() {
        let mut store = test_store();
        store
            .accounts
            .push(test_account(ApiProvider::Anthropic, ProviderAuth::LocalCli));
        assert!(store.has_local_cli(ApiProvider::Anthropic));
        assert!(!store.has_local_cli(ApiProvider::OpenAI));
    }

    #[test]
    fn has_local_cli_false_when_absent() {
        let store = test_store();
        assert!(!store.has_local_cli(ApiProvider::Anthropic));
    }

    #[test]
    fn default_git_identity_picks_min_sort_order() {
        let store = IdentityStore {
            user_id: "test".to_string(),
            accounts: vec![],
            git_identities: vec![
                GitIdentity {
                    id: "gi_high".to_string(),
                    label: "High".to_string(),
                    name: "High Priority".to_string(),
                    email: "high@example.com".to_string(),
                    sort_order: 5,
                },
                GitIdentity {
                    id: "gi_low".to_string(),
                    label: "Low".to_string(),
                    name: "Low Priority".to_string(),
                    email: "low@example.com".to_string(),
                    sort_order: 1,
                },
            ],
            project_overrides: Default::default(),
        };
        let default = store.default_git_identity().unwrap();
        assert_eq!(default.id, "gi_low");
    }

    #[test]
    fn provider_auth_credential_value() {
        let api_key = ProviderAuth::ApiKey {
            value: "key-val".to_string(),
        };
        assert_eq!(api_key.credential_value(), Some("key-val"));

        let oauth = ProviderAuth::OAuthToken {
            value: "oauth-val".to_string(),
        };
        assert_eq!(oauth.credential_value(), Some("oauth-val"));

        let local = ProviderAuth::LocalCli;
        assert_eq!(local.credential_value(), None);
    }

    #[test]
    fn provider_auth_type_labels() {
        assert_eq!(
            ProviderAuth::ApiKey {
                value: "x".to_string()
            }
            .auth_type_label(),
            "api_key"
        );
        assert_eq!(
            ProviderAuth::OAuthToken {
                value: "x".to_string()
            }
            .auth_type_label(),
            "oauth_token"
        );
        assert_eq!(ProviderAuth::LocalCli.auth_type_label(), "local_cli");
    }

    #[test]
    fn api_provider_as_str() {
        assert_eq!(ApiProvider::Anthropic.as_str(), "anthropic");
        assert_eq!(ApiProvider::OpenAI.as_str(), "openai");
        assert_eq!(ApiProvider::Google.as_str(), "google");
        assert_eq!(ApiProvider::GitHub.as_str(), "github");
    }

    #[test]
    fn api_provider_display() {
        assert_eq!(format!("{}", ApiProvider::Anthropic), "Anthropic");
        assert_eq!(format!("{}", ApiProvider::OpenAI), "OpenAI");
        assert_eq!(format!("{}", ApiProvider::Google), "Google");
        assert_eq!(format!("{}", ApiProvider::GitHub), "GitHub");
    }

    #[test]
    fn api_provider_all_returns_four() {
        let all = ApiProvider::all();
        assert_eq!(all.len(), 4);
        assert_eq!(all[0], ApiProvider::Anthropic);
        assert_eq!(all[1], ApiProvider::OpenAI);
        assert_eq!(all[2], ApiProvider::Google);
        assert_eq!(all[3], ApiProvider::GitHub);
    }

    // === Serde alias tests (critical for disk compatibility) ===

    #[test]
    fn api_provider_serde_aliases() {
        // Canonical forms
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""anthropic""#).unwrap(),
            ApiProvider::Anthropic
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""openai""#).unwrap(),
            ApiProvider::OpenAI
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""github""#).unwrap(),
            ApiProvider::GitHub
        );

        // Aliases from serde's automatic mangling
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""open_ai""#).unwrap(),
            ApiProvider::OpenAI,
            "open_ai alias should work"
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""open_a_i""#).unwrap(),
            ApiProvider::OpenAI,
            "open_a_i alias should work (serde rename_all mangling)"
        );
        assert_eq!(
            serde_json::from_str::<ApiProvider>(r#""git_hub""#).unwrap(),
            ApiProvider::GitHub,
            "git_hub alias should work"
        );
    }

    #[test]
    fn api_provider_serializes_canonical() {
        // Serialization should always use canonical form
        assert_eq!(
            serde_json::to_string(&ApiProvider::OpenAI).unwrap(),
            r#""openai""#
        );
        assert_eq!(
            serde_json::to_string(&ApiProvider::GitHub).unwrap(),
            r#""github""#
        );
    }

    #[test]
    fn provider_auth_serde_oauth_alias() {
        // ProviderAuth::OAuthToken can be deserialized from alias "o_auth_token"
        let json = r#"{"type": "o_auth_token", "value": "tok"}"#;
        let auth: ProviderAuth = serde_json::from_str(json).unwrap();
        assert_eq!(auth.credential_value(), Some("tok"));
        assert_eq!(auth.auth_type_label(), "oauth_token");
    }

    #[test]
    fn provider_auth_serde_roundtrip() {
        let api_key = ProviderAuth::ApiKey {
            value: "key".to_string(),
        };
        let json = serde_json::to_string(&api_key).unwrap();
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.credential_value(), Some("key"));

        let oauth = ProviderAuth::OAuthToken {
            value: "tok".to_string(),
        };
        let json = serde_json::to_string(&oauth).unwrap();
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.credential_value(), Some("tok"));

        let local = ProviderAuth::LocalCli;
        let json = serde_json::to_string(&local).unwrap();
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.credential_value(), None);
    }

    #[test]
    fn account_source_serde_roundtrip() {
        for source in [
            AccountSource::Configured,
            AccountSource::LocalCli,
            AccountSource::Server,
        ] {
            let json = serde_json::to_string(&source).unwrap();
            let parsed: AccountSource = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, source);
        }
    }

    // === IdentityStore::from_user_identity tests ===

    fn test_user_identity() -> UserIdentity {
        UserIdentity {
            user_id: "user-abc".to_string(),
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            claude_auth: Some(ClaudeAuth::ApiKey("sk-ant-test".to_string())),
            codex_auth: None,
            github_token: Some("ghp_test123".to_string()),
        }
    }

    #[test]
    fn from_user_identity_claude_api_key_creates_anthropic_account() {
        let store = IdentityStore::from_user_identity(&test_user_identity());
        let anthropic = store.accounts_for_provider(ApiProvider::Anthropic);
        assert_eq!(anthropic.len(), 1);
        assert_eq!(anthropic[0].source, AccountSource::Server);
        match &anthropic[0].auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "sk-ant-test"),
            other => panic!("expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn from_user_identity_claude_oauth_creates_anthropic_account() {
        let identity = UserIdentity {
            claude_auth: Some(ClaudeAuth::OAuthToken("oauth-tok".to_string())),
            ..test_user_identity()
        };
        let store = IdentityStore::from_user_identity(&identity);
        let anthropic = store.accounts_for_provider(ApiProvider::Anthropic);
        assert_eq!(anthropic.len(), 1);
        match &anthropic[0].auth {
            ProviderAuth::OAuthToken { value } => assert_eq!(value, "oauth-tok"),
            other => panic!("expected OAuthToken, got {:?}", other),
        }
    }

    #[test]
    fn from_user_identity_codex_creates_openai_account() {
        let identity = UserIdentity {
            codex_auth: Some(CodexAuth::ApiKey("sk-openai".to_string())),
            ..test_user_identity()
        };
        let store = IdentityStore::from_user_identity(&identity);
        let openai = store.accounts_for_provider(ApiProvider::OpenAI);
        assert_eq!(openai.len(), 1);
        match &openai[0].auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "sk-openai"),
            other => panic!("expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn from_user_identity_github_token_creates_github_account() {
        let store = IdentityStore::from_user_identity(&test_user_identity());
        let github = store.accounts_for_provider(ApiProvider::GitHub);
        assert_eq!(github.len(), 1);
        match &github[0].auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "ghp_test123"),
            other => panic!("expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn from_user_identity_no_auth_produces_no_accounts() {
        let identity = UserIdentity {
            claude_auth: None,
            codex_auth: None,
            github_token: None,
            ..test_user_identity()
        };
        let store = IdentityStore::from_user_identity(&identity);
        assert!(store.accounts.is_empty());
    }

    #[test]
    fn from_user_identity_populates_git_identity() {
        let store = IdentityStore::from_user_identity(&test_user_identity());
        assert_eq!(store.git_identities.len(), 1);
        assert_eq!(store.git_identities[0].name, "Alice");
        assert_eq!(store.git_identities[0].email, "alice@example.com");
    }

    #[test]
    fn from_user_identity_sets_user_id() {
        let store = IdentityStore::from_user_identity(&test_user_identity());
        assert_eq!(store.user_id, "user-abc");
    }

    #[test]
    fn account_info_does_not_expose_credentials() {
        let account = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ApiKey {
                value: "super-secret-key".to_string(),
            },
        );
        let info = AccountInfo::from(&account);
        let json = serde_json::to_string(&info).unwrap();
        assert!(
            !json.contains("super-secret-key"),
            "AccountInfo should not contain credential values"
        );
    }
}
