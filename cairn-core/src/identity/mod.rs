//! User identity types and local identity store.
//!
//! Two-layer architecture:
//! - `IdentityStore` is the new multi-account model (v2 format).
//!   Credentials belong to **API providers** (Anthropic, OpenAI, Google, GitHub),
//!   not backends. Multiple accounts per provider, reorderable by priority.
//! - `UserIdentity` is the backward-compatible runtime type used everywhere —
//!   session startup, MCP handlers, action attribution, git commits.
//!   Resolution converts `IdentityStore` → `UserIdentity` for downstream code.

pub mod claude_profile;
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
///
/// Subscription auth is a Cairn-managed profile and nothing else. Every
/// claude-backend session runs against an explicit `CLAUDE_CONFIG_DIR`, so no
/// session can land on whichever account happens to be signed in at the user
/// level — that ambient account is invisible to usage routing and cannot be
/// switched away from when it runs out. An API key stays a first-class
/// alternative because it is a credential Cairn holds and can attribute.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ClaudeAuth {
    /// API key (personal or org-provided)
    ApiKey(String),
    /// Cairn-managed Claude CLI profile directory.
    ConfigDir(std::path::PathBuf),
}

impl ClaudeAuth {
    /// Get the raw token/key value.
    pub fn value(&self) -> &str {
        match self {
            ClaudeAuth::ApiKey(v) => v,
            ClaudeAuth::ConfigDir(path) => path.to_str().unwrap_or_default(),
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
    /// OpenRouter APIs
    #[serde(rename = "openrouter", alias = "open_router")]
    OpenRouter,
    /// OpenCode Zen APIs. One key spans the Zen surfaces; the Go subscription is
    /// the one Cairn serves today, which is why the credential is named for the
    /// account and the backend is named for the subscription.
    #[serde(rename = "opencode", alias = "open_code")]
    OpenCode,
    /// Ollama APIs
    #[serde(rename = "ollama")]
    Ollama,
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
            ApiProvider::OpenRouter => "openrouter",
            ApiProvider::OpenCode => "opencode",
            ApiProvider::Ollama => "ollama",
            ApiProvider::GitHub => "github",
        }
    }

    /// All providers in display order.
    pub fn all() -> &'static [ApiProvider] {
        &[
            ApiProvider::Anthropic,
            ApiProvider::OpenAI,
            ApiProvider::Google,
            ApiProvider::OpenRouter,
            ApiProvider::OpenCode,
            ApiProvider::Ollama,
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
            ApiProvider::OpenRouter => write!(f, "OpenRouter"),
            ApiProvider::OpenCode => write!(f, "OpenCode"),
            ApiProvider::Ollama => write!(f, "Ollama"),
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
    /// Provider host URL. This is connection metadata, not a secret.
    #[serde(rename = "base_url")]
    BaseUrl { url: String },
    /// Cairn-managed Claude CLI profile. Its path is derived from the account id.
    #[serde(rename = "claude_profile")]
    ClaudeProfile,
}

impl ProviderAuth {
    /// Parse and canonicalize provider connection metadata at its input boundary.
    pub fn base_url(value: &str) -> Result<Self, String> {
        let value = value.trim();
        let parsed = reqwest::Url::parse(value)
            .map_err(|error| format!("invalid base URL '{value}': {error}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(format!(
                "invalid base URL '{value}': expected http or https scheme"
            ));
        }
        Ok(Self::BaseUrl {
            url: parsed.to_string().trim_end_matches('/').to_string(),
        })
    }

    /// Get the raw credential value, if any.
    fn credential_value(&self) -> Option<&str> {
        match self {
            ProviderAuth::ApiKey { value } | ProviderAuth::OAuthToken { value } => Some(value),
            ProviderAuth::BaseUrl { .. } | ProviderAuth::ClaudeProfile => None,
        }
    }

    /// Short description of auth type for UI display.
    fn auth_type_label(&self) -> &'static str {
        match self {
            ProviderAuth::ApiKey { .. } => "api_key",
            ProviderAuth::OAuthToken { .. } => "oauth_token",
            ProviderAuth::BaseUrl { .. } => "base_url",
            ProviderAuth::ClaudeProfile => "claude_profile",
        }
    }
}

/// Persisted availability and subscription usage for one provider account.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAccountHealth {
    #[serde(default)]
    pub windows: Vec<crate::models::ProviderUsageWindow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_until: Option<i64>,
    pub captured_at: i64,
}

impl ProviderAccountHealth {
    /// Whether the provider has told Cairn this account cannot run yet.
    fn is_blocked(&self, now: i64) -> bool {
        self.blocked_until.is_some_and(|until| until > now)
    }

    /// The tightest remaining headroom this snapshot still speaks for at `now`,
    /// or `None` when it says nothing measurable.
    ///
    /// A window whose reset time has passed has rolled over since it was
    /// measured, so its number is history rather than evidence of exhaustion.
    /// Without that, one measured-empty five-hour window would strand an
    /// account out of rotation until somebody re-probed it by hand — snapshots
    /// arrive on sign-in, on a manual refresh, and from live session events,
    /// none of which fire for an account nothing is routing sessions to.
    fn live_headroom(&self, now: i64) -> Option<f64> {
        let tightest = self
            .windows
            .iter()
            .filter(|window| window.resets_at.is_none_or(|resets_at| resets_at > now))
            .map(|window| window.remaining_percent)
            .fold(f64::INFINITY, f64::min);
        tightest.is_finite().then_some(tightest)
    }
}

/// A named credential for an API provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAccount {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) api_provider: ApiProvider,
    pub(crate) source: AccountSource,
    pub(crate) auth: ProviderAuth,
    /// None = shared account; Some(project_id) = private to that project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) project_id: Option<String>,
    /// Position = priority (lower = higher priority)
    pub(crate) sort_order: i32,
    pub(crate) created_at: i64,
    pub(crate) last_used_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) health: Option<ProviderAccountHealth>,
}

impl ProviderAccount {
    /// Which agent backends can consume this credential.
    pub(crate) fn compatible_backends(&self) -> Vec<&'static str> {
        match (&self.api_provider, &self.auth) {
            // API keys work with both CLI and Native
            (ApiProvider::Anthropic, ProviderAuth::ApiKey { .. }) => vec!["claude", "native"],
            (ApiProvider::OpenAI, ProviderAuth::ApiKey { .. }) => vec!["codex", "native"],
            (ApiProvider::Google, ProviderAuth::ApiKey { .. }) => vec!["native"],
            (ApiProvider::OpenRouter, ProviderAuth::ApiKey { .. }) => vec!["openrouter"],
            (ApiProvider::OpenCode, ProviderAuth::ApiKey { .. }) => vec!["opencode-go"],
            (ApiProvider::Ollama, ProviderAuth::BaseUrl { .. }) => vec!["ollama"],
            (ApiProvider::GitHub, ProviderAuth::ApiKey { .. }) => vec![],
            // OAuth is CLI-specific
            (ApiProvider::OpenAI, ProviderAuth::OAuthToken { .. }) => vec!["codex"],
            (ApiProvider::Google, ProviderAuth::OAuthToken { .. }) => vec![],
            (ApiProvider::OpenRouter, ProviderAuth::OAuthToken { .. }) => vec![],
            // OpenCode Zen issues API keys from its console; there is no OAuth.
            (ApiProvider::OpenCode, ProviderAuth::OAuthToken { .. }) => vec![],
            (ApiProvider::GitHub, ProviderAuth::OAuthToken { .. }) => vec![],
            // A Cairn-managed profile is the only subscription credential the
            // Claude backend accepts. A pasted `claude setup-token` value
            // (`OAuthToken`) authenticates a session Cairn can neither route by
            // remaining usage nor sign out of, so it is not a usable Anthropic
            // credential; stored ones are retired when the store loads.
            (ApiProvider::Anthropic, ProviderAuth::OAuthToken { .. }) => vec![],
            (ApiProvider::Anthropic, ProviderAuth::ClaudeProfile) => vec!["claude"],
            _ => vec![],
        }
    }
}

/// Separate git commit identity (name + email).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitIdentity {
    pub(crate) id: String,
    pub(crate) label: String,
    pub name: String,
    pub(crate) email: String,
    /// First = default
    pub(crate) sort_order: i32,
}

/// An Anthropic login that the profiles-only model no longer accepts, kept
/// until the user has been told why it disappeared.
///
/// Dropping a credential the user deliberately added is not something to do
/// silently: the account simply vanishing from settings reads as a bug, and the
/// only recovery — signing in again as a managed profile — is not guessable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetiredLogin {
    pub(crate) label: String,
    /// The auth shape it used to have (`local_cli` or `oauth_token`).
    pub(crate) auth_type: String,
    pub(crate) retired_at: i64,
}

/// Headroom assumed for a managed account Cairn has no usage snapshot for.
/// Full, so an account that just signed in competes normally instead of
/// being sorted below every account with a measured window.
const UNKNOWN_HEADROOM: f64 = 100.0;

/// A provider whose subscription logins Cairn routes sessions across by
/// remaining usage.
///
/// Subscriptions are inventory — interchangeable token faucets picked by
/// headroom — so what identifies one here is the provider plus the credential
/// shape that carries a metered window. An API key bills per token and has no
/// window to route around, so it is never a routing candidate; it remains the
/// deliberate fallback a caller reaches for when no subscription is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutedProvider {
    Claude,
    Codex,
}

impl RoutedProvider {
    /// The routed provider a backend draws its subscription credential from,
    /// or `None` for a backend Cairn does not hold subscriptions for.
    pub fn for_backend(backend: &str) -> Option<Self> {
        match backend {
            "claude" => Some(Self::Claude),
            "codex" => Some(Self::Codex),
            _ => None,
        }
    }

    fn api_provider(self) -> ApiProvider {
        match self {
            Self::Claude => ApiProvider::Anthropic,
            Self::Codex => ApiProvider::OpenAI,
        }
    }

    pub fn backend(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }

    /// Whether this credential is the metered subscription shape for the
    /// provider: a Cairn-managed Claude profile, or a ChatGPT OAuth login whose
    /// `auth.json` Cairn owns and refreshes.
    fn routes(self, auth: &ProviderAuth) -> bool {
        match self {
            Self::Claude => matches!(auth, ProviderAuth::ClaudeProfile),
            Self::Codex => matches!(auth, ProviderAuth::OAuthToken { .. }),
        }
    }
}

/// The full identity store — multi-account model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityStore {
    user_id: String,
    /// Sorted by (api_provider, sort_order)
    pub(crate) accounts: Vec<ProviderAccount>,
    /// Sorted by sort_order
    pub(crate) git_identities: Vec<GitIdentity>,
    /// Per-project account overrides, keyed by project ID.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub(crate) project_overrides: std::collections::HashMap<String, AccountOverrides>,
    /// Anthropic logins dropped by the profiles-only migration, pending the
    /// user acknowledging them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) retired_logins: Vec<RetiredLogin>,
    /// The configuration root this store was loaded from, and therefore the one
    /// its managed Claude profiles live under. Not content, so not persisted —
    /// but carried, because a store that cannot say where it lives cannot
    /// resolve a profile path, and inferring one is how sign-in and session
    /// resolution came to disagree.
    #[serde(skip)]
    pub(crate) config_dir: std::path::PathBuf,
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
                ClaudeAuth::ConfigDir(_) => ProviderAuth::ClaudeProfile,
            };
            accounts.push(ProviderAccount {
                id: format!("server_{}", uuid::Uuid::new_v4()),
                label: "Server".to_string(),
                api_provider: ApiProvider::Anthropic,
                source: AccountSource::Server,
                auth: provider_auth,
                project_id: None,
                sort_order: 0,
                created_at: now,
                last_used_at: None,
                email: None,
                plan: None,
                health: None,
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
                project_id: None,
                sort_order: 0,
                created_at: now,
                last_used_at: None,
                email: None,
                plan: None,
                health: None,
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
                project_id: None,
                sort_order: 0,
                created_at: now,
                last_used_at: None,
                email: None,
                plan: None,
                health: None,
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
            retired_logins: Vec::new(),
            // A server-provided identity carries credentials, not a local
            // configuration root; it never resolves a managed profile.
            config_dir: std::path::PathBuf::new(),
        }
    }

    /// Get accounts for a specific provider, sorted by priority.
    pub(crate) fn accounts_for_provider(
        &self,
        provider: ApiProvider,
        project_id: Option<&str>,
    ) -> Vec<&ProviderAccount> {
        let mut private_accounts: Vec<_> = self
            .accounts
            .iter()
            .filter(|a| {
                a.api_provider == provider
                    && project_id.is_some()
                    && a.project_id.as_deref() == project_id
            })
            .collect();
        private_accounts.sort_by_key(|a| a.sort_order);

        let mut shared_accounts: Vec<_> = self
            .accounts
            .iter()
            .filter(|a| a.api_provider == provider && a.project_id.is_none())
            .collect();
        shared_accounts.sort_by_key(|a| a.sort_order);

        let mut accounts = private_accounts;
        accounts.extend(shared_accounts);
        accounts
    }

    /// Find the highest-priority account for a provider that's compatible with a backend.
    fn best_account_for(
        &self,
        provider: ApiProvider,
        backend: &str,
        override_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Option<&ProviderAccount> {
        let accounts = self.accounts_for_provider(provider, project_id);

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

    /// Whether a session's pinned subscription account can still run.
    ///
    /// An account with no snapshot reads as available: health is captured after
    /// the fact, so its absence is ignorance, not exhaustion.
    pub(crate) fn routed_account_is_available(
        &self,
        provider: RoutedProvider,
        account_id: &str,
        now: i64,
    ) -> bool {
        self.accounts.iter().any(|account| {
            account.id == account_id
                && provider.routes(&account.auth)
                && account.health.as_ref().is_none_or(|health| {
                    !health.is_blocked(now)
                        && health
                            .live_headroom(now)
                            .is_none_or(|headroom| headroom > 0.0)
                })
        })
    }

    /// Select an available subscription account by tightest-window headroom.
    /// Assignments since each account's snapshot break equal-headroom bursts.
    ///
    /// An account Cairn has no usage snapshot for is a candidate at full
    /// headroom rather than a skipped one. Snapshots only ever arrive after a
    /// probe or a rate-limit event, so demanding one would make a login that
    /// just completed permanently unselectable and push every session onto
    /// whatever else happened to resolve.
    pub(crate) fn select_routed_account(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        override_id: Option<&str>,
        excluded_id: Option<&str>,
        assignments: &[(String, i64)],
        now: i64,
    ) -> Option<&ProviderAccount> {
        let backend = provider.backend();
        let accounts = self.accounts_for_provider(provider.api_provider(), project_id);
        if let Some(id) = override_id {
            return accounts.into_iter().find(|account| {
                account.id == id
                    && excluded_id != Some(account.id.as_str())
                    && account.compatible_backends().contains(&backend)
            });
        }

        accounts
            .into_iter()
            .filter(|account| {
                excluded_id != Some(account.id.as_str())
                    && provider.routes(&account.auth)
                    && account.compatible_backends().contains(&backend)
            })
            .filter_map(|account| {
                let (headroom, since) = match account.health.as_ref() {
                    Some(health) => {
                        if health.is_blocked(now) {
                            return None;
                        }
                        match health.live_headroom(now) {
                            // A snapshot with no window still standing says
                            // nothing about headroom, the same as no snapshot.
                            None => (UNKNOWN_HEADROOM, health.captured_at),
                            Some(tightest) if tightest <= 0.0 => return None,
                            Some(tightest) => (tightest, health.captured_at),
                        }
                    }
                    None => (UNKNOWN_HEADROOM, account.created_at),
                };
                let burst = assignments
                    .iter()
                    .filter(|(id, created_at)| id == &account.id && *created_at >= since)
                    .count();
                Some((account, headroom, burst))
            })
            .max_by(
                |(left, left_headroom, left_burst), (right, right_headroom, right_burst)| {
                    left_headroom
                        .total_cmp(right_headroom)
                        .then_with(|| right_burst.cmp(left_burst))
                        .then_with(|| right.sort_order.cmp(&left.sort_order))
                },
            )
            .map(|(account, _, _)| account)
    }

    /// Resolve the runtime identity with one provider's account pinned,
    /// leaving the project's other overrides (git identity, the other
    /// provider's pin) in force.
    pub(crate) fn resolve_with_routed_account(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        account_id: &str,
    ) -> UserIdentity {
        let mut overrides = project_id
            .and_then(|id| self.project_overrides.get(id).cloned())
            .unwrap_or_default();
        match provider {
            RoutedProvider::Claude => overrides.anthropic_account_id = Some(account_id.to_string()),
            RoutedProvider::Codex => overrides.openai_account_id = Some(account_id.to_string()),
        }
        self.resolve(project_id, Some(&overrides))
    }

    pub(crate) fn resolve_with_provider_account(
        &self,
        backend: &str,
        account_id: &str,
    ) -> Option<UserIdentity> {
        let (provider, overrides) = match backend {
            "claude" => (
                ApiProvider::Anthropic,
                AccountOverrides {
                    anthropic_account_id: Some(account_id.to_string()),
                    ..Default::default()
                },
            ),
            "codex" => (
                ApiProvider::OpenAI,
                AccountOverrides {
                    openai_account_id: Some(account_id.to_string()),
                    ..Default::default()
                },
            ),
            _ => return None,
        };
        self.accounts_for_provider(provider, None)
            .into_iter()
            .find(|account| {
                account.id == account_id && account.compatible_backends().contains(&backend)
            })?;
        Some(self.resolve(None, Some(&overrides)))
    }

    /// Get the default git identity (first by sort order).
    fn default_git_identity(&self) -> Option<&GitIdentity> {
        self.git_identities.iter().min_by_key(|g| g.sort_order)
    }

    /// Resolve the multi-account store into a backward-compatible `UserIdentity`.
    ///
    /// This finds the highest-priority compatible account per provider/backend
    /// and maps them to the fields `UserIdentity` expects.
    pub(crate) fn resolve(
        &self,
        project_id: Option<&str>,
        overrides: Option<&AccountOverrides>,
    ) -> UserIdentity {
        let anthropic_override = overrides.and_then(|o| o.anthropic_account_id.as_deref());
        let openai_override = overrides.and_then(|o| o.openai_account_id.as_deref());
        let github_override = overrides.and_then(|o| o.github_account_id.as_deref());
        let git_override = overrides.and_then(|o| o.git_identity_id.as_deref());

        // Resolve Claude auth from best Anthropic account
        let claude_auth = self
            .best_account_for(
                ApiProvider::Anthropic,
                "claude",
                anthropic_override,
                project_id,
            )
            .and_then(|a| match &a.auth {
                ProviderAuth::ApiKey { value } => Some(ClaudeAuth::ApiKey(value.clone())),
                // Without a configuration root there is no honest profile path,
                // and a guessed one points a session at an empty directory that
                // the CLI reports as signed out. Resolving to nothing instead
                // makes the backend refuse the session and say so.
                ProviderAuth::ClaudeProfile if self.config_dir.as_os_str().is_empty() => {
                    log::error!(
                        "Claude profile {} cannot be resolved: identity store has no config dir",
                        a.id
                    );
                    None
                }
                ProviderAuth::ClaudeProfile => Some(ClaudeAuth::ConfigDir(
                    crate::identity::claude_profile::profile_dir_in(&self.config_dir, &a.id),
                )),
                // A retired setup token is not Claude auth, and neither is a
                // host URL. `best_account_for` already filters on
                // `compatible_backends`, so neither reaches here.
                ProviderAuth::OAuthToken { .. } | ProviderAuth::BaseUrl { .. } => None,
            });

        // Resolve Codex auth from best OpenAI account
        let codex_auth = self
            .best_account_for(ApiProvider::OpenAI, "codex", openai_override, project_id)
            .and_then(|a| match &a.auth {
                ProviderAuth::ApiKey { value } => Some(CodexAuth::ApiKey(value.clone())),
                ProviderAuth::OAuthToken { value } => Some(CodexAuth::OAuthToken(value.clone())),
                ProviderAuth::BaseUrl { .. } | ProviderAuth::ClaudeProfile => None,
            });

        // Resolve GitHub token
        let github_token = self
            .accounts_for_provider(ApiProvider::GitHub, project_id)
            .into_iter()
            .find(|a| {
                if let Some(id) = github_override {
                    a.id == id
                } else {
                    true
                }
            })
            .and_then(|a| a.auth.credential_value().map(|v| v.to_string()));

        // Resolve git identity: inline project values first, then legacy gitIdentityId, then default.
        let inline_git = overrides.and_then(|o| match (&o.git_name, &o.git_email) {
            (Some(name), Some(email)) if !name.trim().is_empty() && !email.trim().is_empty() => {
                Some((name.clone(), email.clone()))
            }
            _ => None,
        });

        let git_identity = if inline_git.is_none() && git_override.is_some() {
            self.git_identities
                .iter()
                .find(|g| Some(g.id.as_str()) == git_override)
                .or_else(|| self.default_git_identity())
        } else {
            self.default_git_identity()
        };

        let (name, email) = match inline_git {
            Some(pair) => pair,
            None => match git_identity {
                Some(gi) => (gi.name.clone(), gi.email.clone()),
                None => (String::new(), String::new()),
            },
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
}

/// Per-project overrides for account selection.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) anthropic_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) openai_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) github_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) git_identity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) git_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) git_email: Option<String>,
}

/// Frontend-safe account info (no credential values exposed).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    /// Public because callers outside this crate thread the id of a returned
    /// account back into the orchestrator's account APIs. The remaining fields
    /// stay crate-private and reach consumers by serialization.
    pub id: String,
    pub(crate) label: String,
    pub(crate) api_provider: ApiProvider,
    pub(crate) source: AccountSource,
    pub(crate) auth_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    base_url: Option<String>,
    compatible_backends: Vec<String>,
    project_id: Option<String>,
    sort_order: i32,
    last_used_at: Option<i64>,
    email: Option<String>,
    plan: Option<String>,
    health: Option<ProviderAccountHealth>,
}

impl From<&ProviderAccount> for AccountInfo {
    fn from(account: &ProviderAccount) -> Self {
        Self {
            id: account.id.clone(),
            label: account.label.clone(),
            api_provider: account.api_provider,
            source: account.source.clone(),
            auth_type: account.auth.auth_type_label().to_string(),
            base_url: match &account.auth {
                ProviderAuth::BaseUrl { url } => Some(url.clone()),
                _ => None,
            },
            compatible_backends: account
                .compatible_backends()
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            project_id: account.project_id.clone(),
            sort_order: account.sort_order,
            last_used_at: account.last_used_at,
            email: account.email.clone(),
            plan: account.plan.clone(),
            health: account.health.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_provider_account_is_exact_and_never_falls_back() {
        let mut store = test_store();
        let mut first = test_account(ApiProvider::OpenAI, api_key_auth("first-key"));
        first.id = "first".into();
        let mut requested = test_account(ApiProvider::OpenAI, api_key_auth("requested-key"));
        requested.id = "requested".into();
        store.accounts = vec![first, requested];

        let identity = store
            .resolve_with_provider_account("codex", "requested")
            .unwrap();
        assert!(
            matches!(identity.codex_auth, Some(CodexAuth::ApiKey(ref key)) if key == "requested-key")
        );
        assert!(store
            .resolve_with_provider_account("codex", "missing")
            .is_none());
    }

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
            project_id: None,
            sort_order: 0,
            created_at: 0,
            last_used_at: None,
            email: None,
            plan: None,
            health: None,
        }
    }

    fn api_key_auth(value: &str) -> ProviderAuth {
        ProviderAuth::ApiKey {
            value: value.to_string(),
        }
    }

    fn oauth_auth(value: &str) -> ProviderAuth {
        ProviderAuth::OAuthToken {
            value: value.to_string(),
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
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::from("/test-config-root"),
        }
    }

    fn healthy_claude_account(id: &str, remaining: f64, captured_at: i64) -> ProviderAccount {
        let mut account = test_account(ApiProvider::Anthropic, ProviderAuth::ClaudeProfile);
        account.id = id.to_string();
        account.health = Some(ProviderAccountHealth {
            windows: vec![crate::models::ProviderUsageWindow {
                id: "five-hour".to_string(),
                label: "5h".to_string(),
                scope: crate::models::ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0 - remaining,
                remaining_percent: remaining,
                resets_at: None,
                reset_at_text: None,
                window_duration_mins: Some(300),
            }],
            blocked_until: None,
            captured_at,
        });
        account
    }

    #[test]
    fn claude_selection_prefers_headroom_and_skips_unavailable() {
        let mut store = test_store();
        store.accounts = vec![
            healthy_claude_account("busy", 20.0, 100),
            healthy_claude_account("free", 80.0, 100),
        ];
        let selected = store
            .select_routed_account(RoutedProvider::Claude, None, None, None, &[], 200)
            .unwrap();
        assert_eq!(selected.id, "free");

        store.accounts[1].health.as_mut().unwrap().blocked_until = Some(300);
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, &[], 200)
                .unwrap()
                .id,
            "busy"
        );
    }

    #[test]
    fn claude_selection_spreads_equal_headroom_bursts() {
        let mut store = test_store();
        store.accounts = vec![
            healthy_claude_account("first", 80.0, 100),
            healthy_claude_account("second", 80.0, 100),
        ];
        let assignments = vec![("first".to_string(), 101)];
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, &assignments, 200)
                .unwrap()
                .id,
            "second"
        );
    }

    #[test]
    fn explicit_claude_override_wins_even_without_health() {
        let mut store = test_store();
        let mut explicit = test_account(ApiProvider::Anthropic, ProviderAuth::ClaudeProfile);
        explicit.id = "explicit".to_string();
        store.accounts = vec![explicit, healthy_claude_account("healthy", 90.0, 100)];
        assert_eq!(
            store
                .select_routed_account(
                    RoutedProvider::Claude,
                    None,
                    Some("explicit"),
                    None,
                    &[],
                    200
                )
                .unwrap()
                .id,
            "explicit"
        );
    }

    #[test]
    fn compatible_backends_claude_profile() {
        let account = test_account(ApiProvider::Anthropic, ProviderAuth::ClaudeProfile);
        assert_eq!(account.compatible_backends(), vec!["claude"]);
    }

    #[test]
    fn compatible_backends_anthropic_api_key() {
        let account = test_account(ApiProvider::Anthropic, api_key_auth("sk-ant-test"));
        assert_eq!(account.compatible_backends(), vec!["claude", "native"]);
    }

    #[test]
    fn a_managed_profile_resolves_under_the_stores_own_root() {
        // Sign-in writes the profile under the orchestrator's config dir. When
        // resolution rebuilt that path from the environment instead, every
        // Cairn running on a non-default root (each dev instance) spawned its
        // sessions against an empty directory and the CLI answered "Not logged
        // in", while the usage probe read the same empty directory.
        let mut store = test_store();
        let mut account = test_account(ApiProvider::Anthropic, ProviderAuth::ClaudeProfile);
        account.id = "acc_profile".to_string();
        store.accounts.push(account);

        match store.resolve(None, None).claude_auth {
            Some(ClaudeAuth::ConfigDir(path)) => assert_eq!(
                path,
                std::path::Path::new("/test-config-root/claude-profiles/acc_profile")
            ),
            other => panic!("expected a managed profile directory, got {other:?}"),
        }
    }

    #[test]
    fn a_store_with_no_root_resolves_no_profile_rather_than_guessing_one() {
        let mut store = test_store();
        store.config_dir = std::path::PathBuf::new();
        store.accounts.push(test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile,
        ));

        // Fails closed: the backend refuses a session with no credential, which
        // is honest, where a relative guess would point at some other profile.
        assert!(store.resolve(None, None).claude_auth.is_none());
    }

    #[test]
    fn a_profile_with_no_snapshot_yet_is_selectable() {
        // The acceptance case for a fresh sign-in: a profile connected moments
        // ago has no usage snapshot, and must still be able to run a session.
        let mut store = test_store();
        let mut fresh = test_account(ApiProvider::Anthropic, ProviderAuth::ClaudeProfile);
        fresh.id = "fresh".to_string();
        store.accounts = vec![healthy_claude_account("measured", 40.0, 100), fresh];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, &[], 200)
                .unwrap()
                .id,
            "fresh",
            "an unmeasured profile competes at full headroom"
        );
        assert!(store.routed_account_is_available(RoutedProvider::Claude, "fresh", 200));
    }

    #[test]
    fn a_snapshot_with_no_windows_reads_as_unknown_not_exhausted() {
        let mut store = test_store();
        let mut blank = healthy_claude_account("blank", 90.0, 100);
        blank.health.as_mut().unwrap().windows.clear();
        store.accounts = vec![blank];

        assert!(store
            .select_routed_account(RoutedProvider::Claude, None, None, None, &[], 200)
            .is_some());
        assert!(store.routed_account_is_available(RoutedProvider::Claude, "blank", 200));
    }

    #[test]
    fn a_blocked_profile_is_unavailable_however_stale_its_snapshot() {
        let mut store = test_store();
        let mut blocked = healthy_claude_account("blocked", 90.0, 100);
        blocked.health.as_mut().unwrap().blocked_until = Some(300);
        store.accounts = vec![blocked];

        assert!(store
            .select_routed_account(RoutedProvider::Claude, None, None, None, &[], 200)
            .is_none());
        assert!(!store.routed_account_is_available(RoutedProvider::Claude, "blocked", 200));
    }

    /// A subscription account measured empty is out of rotation only while
    /// that window stands. Once its reset time passes the number is history,
    /// not exhaustion — otherwise nothing would ever route to the account
    /// again, because every source of a fresh snapshot (sign-in, a manual
    /// refresh, a live session event) needs the account to be in use.
    #[test]
    fn an_exhausted_window_stops_excluding_once_it_has_reset() {
        let mut store = test_store();
        let mut spent = healthy_claude_account("spent", 0.0, 100);
        spent.health.as_mut().unwrap().windows[0].resets_at = Some(500);
        store.accounts = vec![spent];

        assert!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, &[], 200)
                .is_none(),
            "an empty window still standing excludes the account"
        );
        assert!(!store.routed_account_is_available(RoutedProvider::Claude, "spent", 200));

        assert!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, &[], 600)
                .is_some(),
            "after the window resets the account competes at unknown headroom again"
        );
        assert!(store.routed_account_is_available(RoutedProvider::Claude, "spent", 600));
    }

    // === Codex subscription routing ===

    fn codex_account(id: &str, remaining: Option<f64>, captured_at: i64) -> ProviderAccount {
        let mut account = test_account(ApiProvider::OpenAI, oauth_auth(&format!("{id}-auth-json")));
        account.id = id.to_string();
        account.health = remaining.map(|remaining| ProviderAccountHealth {
            windows: vec![crate::models::ProviderUsageWindow {
                id: "primary".to_string(),
                label: "5-hour window".to_string(),
                scope: crate::models::ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0 - remaining,
                remaining_percent: remaining,
                resets_at: None,
                reset_at_text: None,
                window_duration_mins: Some(300),
            }],
            blocked_until: None,
            captured_at,
        });
        account
    }

    #[test]
    fn codex_selection_prefers_headroom_and_skips_blocked() {
        let mut store = test_store();
        store.accounts = vec![
            codex_account("busy", Some(15.0), 100),
            codex_account("free", Some(70.0), 100),
        ];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, &[], 200)
                .unwrap()
                .id,
            "free"
        );

        store.accounts[1].health.as_mut().unwrap().blocked_until = Some(300);
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, &[], 200)
                .unwrap()
                .id,
            "busy",
            "a rate-limited account is passed over while its block stands"
        );
        assert!(!store.routed_account_is_available(RoutedProvider::Codex, "free", 200));
        assert!(
            store.routed_account_is_available(RoutedProvider::Codex, "free", 400),
            "the block lifts on its own once it expires"
        );
    }

    #[test]
    fn a_fresh_codex_account_is_selectable_before_any_usage_snapshot() {
        // The acceptance case for a sign-in: the account connected moments ago
        // has no snapshot, and must still be able to run a session.
        let mut store = test_store();
        store.accounts = vec![
            codex_account("measured", Some(40.0), 100),
            codex_account("fresh", None, 100),
        ];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, &[], 200)
                .unwrap()
                .id,
            "fresh",
            "an unmeasured account competes at full headroom"
        );
        assert!(store.routed_account_is_available(RoutedProvider::Codex, "fresh", 200));
    }

    #[test]
    fn an_exhausted_codex_account_is_passed_over() {
        let mut store = test_store();
        store.accounts = vec![
            codex_account("spent", Some(0.0), 100),
            codex_account("left", Some(5.0), 100),
        ];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, &[], 200)
                .unwrap()
                .id,
            "left"
        );
        assert!(!store.routed_account_is_available(RoutedProvider::Codex, "spent", 200));
    }

    /// An OpenAI API key bills per token, so it has no window to route around
    /// and is never a routing candidate — it stays the deliberate fallback a
    /// session reaches for when no subscription is available.
    #[test]
    fn an_openai_api_key_is_not_routed_by_usage() {
        let mut store = test_store();
        let mut key = test_account(ApiProvider::OpenAI, api_key_auth("sk-test"));
        key.id = "key".to_string();
        store.accounts = vec![key];

        assert!(store
            .select_routed_account(RoutedProvider::Codex, None, None, None, &[], 200)
            .is_none());
        assert!(!store.routed_account_is_available(RoutedProvider::Codex, "key", 200));
    }

    /// Selection stays inside its own provider: a Codex session must never
    /// resolve onto an Anthropic profile, nor the reverse.
    #[test]
    fn routing_never_crosses_providers() {
        let mut store = test_store();
        let mut profile = test_account(ApiProvider::Anthropic, ProviderAuth::ClaudeProfile);
        profile.id = "claude".to_string();
        store.accounts = vec![profile, codex_account("codex", Some(50.0), 100)];

        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Codex, None, None, None, &[], 200)
                .unwrap()
                .id,
            "codex"
        );
        assert_eq!(
            store
                .select_routed_account(RoutedProvider::Claude, None, None, None, &[], 200)
                .unwrap()
                .id,
            "claude"
        );
    }

    /// Resolving onto a chosen Codex account pins that account's credential and
    /// leaves the other provider's resolution alone.
    #[test]
    fn resolving_a_codex_account_pins_only_that_provider() {
        let mut store = test_store();
        let mut profile = test_account(ApiProvider::Anthropic, ProviderAuth::ClaudeProfile);
        profile.id = "claude".to_string();
        let mut first = codex_account("first", Some(50.0), 100);
        first.sort_order = 0;
        let mut second = codex_account("second", Some(90.0), 100);
        second.sort_order = 1;
        store.accounts = vec![profile, first, second];

        let identity = store.resolve_with_routed_account(RoutedProvider::Codex, None, "second");

        assert!(
            matches!(identity.codex_auth, Some(CodexAuth::OAuthToken(ref json)) if json == "second-auth-json"),
            "got {:?}",
            identity.codex_auth
        );
        assert!(
            matches!(identity.claude_auth, Some(ClaudeAuth::ConfigDir(_))),
            "the Anthropic side still resolves normally"
        );
    }

    #[test]
    fn anthropic_setup_tokens_are_not_usable_credentials() {
        // `claude setup-token` output authenticates a session Cairn cannot
        // route by usage or sign out of, so it buys nothing the profile model
        // does not already own.
        let account = test_account(ApiProvider::Anthropic, oauth_auth("oauth-token"));
        assert!(account.compatible_backends().is_empty());

        let mut store = test_store();
        store.accounts.push(account);
        assert!(store.resolve(None, None).claude_auth.is_none());
    }

    #[test]
    fn compatible_backends_openai_api_key() {
        let account = test_account(ApiProvider::OpenAI, api_key_auth("sk-test"));
        assert_eq!(account.compatible_backends(), vec!["codex", "native"]);
    }

    #[test]
    fn compatible_backends_google_api_key() {
        let account = test_account(ApiProvider::Google, api_key_auth("goog-key"));
        assert_eq!(account.compatible_backends(), vec!["native"]);
    }

    #[test]
    fn resolve_empty_store() {
        let store = test_store();
        let identity = store.resolve(None, None);
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
            api_key_auth("sk-ant-key"),
        ));
        let identity = store.resolve(None, None);
        match &identity.claude_auth {
            Some(ClaudeAuth::ApiKey(key)) => assert_eq!(key, "sk-ant-key"),
            other => panic!("Expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn resolve_with_overrides() {
        let mut store = test_store();
        let mut acc1 = test_account(ApiProvider::Anthropic, api_key_auth("primary-key"));
        acc1.id = "acc_primary".to_string();
        acc1.sort_order = 0;

        let mut acc2 = test_account(ApiProvider::Anthropic, api_key_auth("secondary-key"));
        acc2.id = "acc_secondary".to_string();
        acc2.sort_order = 1;

        store.accounts.push(acc1);
        store.accounts.push(acc2);

        // Without override: primary wins
        let identity = store.resolve(None, None);
        match &identity.claude_auth {
            Some(ClaudeAuth::ApiKey(key)) => assert_eq!(key, "primary-key"),
            other => panic!("Expected primary key, got {:?}", other),
        }

        // With override: secondary wins
        let overrides = AccountOverrides {
            anthropic_account_id: Some("acc_secondary".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(None, Some(&overrides));
        match &identity.claude_auth {
            Some(ClaudeAuth::ApiKey(key)) => assert_eq!(key, "secondary-key"),
            other => panic!("Expected secondary key, got {:?}", other),
        }
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
        let account = test_account(ApiProvider::Anthropic, api_key_auth("secret"));
        let info = AccountInfo::from(&account);
        assert_eq!(info.api_provider, ApiProvider::Anthropic);
        assert_eq!(info.auth_type, "api_key");
        assert_eq!(info.compatible_backends, vec!["claude", "native"]);
    }

    #[test]
    fn project_private_accounts_are_private_first_and_scoped() {
        let mut store = test_store();
        let mut shared = test_account(ApiProvider::Anthropic, api_key_auth("shared-key"));
        shared.id = "shared".to_string();
        shared.sort_order = 0;
        let mut private_p = test_account(ApiProvider::Anthropic, api_key_auth("project-key"));
        private_p.id = "private-p".to_string();
        private_p.project_id = Some("project-p".to_string());
        private_p.sort_order = 10;
        let mut private_q = test_account(ApiProvider::Anthropic, api_key_auth("other-key"));
        private_q.project_id = Some("project-q".to_string());

        store.accounts.extend([shared, private_p, private_q]);

        let p_accounts = store.accounts_for_provider(ApiProvider::Anthropic, Some("project-p"));
        assert_eq!(
            p_accounts.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(),
            vec!["private-p", "shared"]
        );
        let qless_accounts = store.accounts_for_provider(ApiProvider::Anthropic, None);
        assert_eq!(
            qless_accounts
                .iter()
                .map(|a| a.id.as_str())
                .collect::<Vec<_>>(),
            vec!["shared"]
        );

        let identity = store.resolve(Some("project-p"), None);
        assert_eq!(
            identity.claude_auth.as_ref().map(|a| a.value()),
            Some("project-key")
        );
        let identity = store.resolve(Some("project-q"), None);
        assert_eq!(
            identity.claude_auth.as_ref().map(|a| a.value()),
            Some("other-key")
        );
        let identity = store.resolve(Some("project-r"), None);
        assert_eq!(
            identity.claude_auth.as_ref().map(|a| a.value()),
            Some("shared-key")
        );
    }

    #[test]
    fn override_pin_must_be_in_project_scope() {
        let mut store = test_store();
        let mut private_q = test_account(ApiProvider::OpenAI, api_key_auth("q-key"));
        private_q.id = "private-q".to_string();
        private_q.project_id = Some("project-q".to_string());
        store.accounts.push(private_q);

        let overrides = AccountOverrides {
            openai_account_id: Some("private-q".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(Some("project-p"), Some(&overrides));
        assert!(identity.codex_auth.is_none());
        let identity = store.resolve(Some("project-q"), Some(&overrides));
        assert_eq!(
            identity.codex_auth.as_ref().map(|a| a.value()),
            Some("q-key")
        );
    }

    #[test]
    fn inline_git_identity_takes_precedence() {
        let store = test_store();
        let overrides = AccountOverrides {
            git_identity_id: Some("gi_1".to_string()),
            git_name: Some("Project User".to_string()),
            git_email: Some("project@example.com".to_string()),
            ..Default::default()
        };

        let identity = store.resolve(Some("project-p"), Some(&overrides));
        assert_eq!(identity.name, "Project User");
        assert_eq!(identity.email, "project@example.com");
    }

    #[test]
    fn accounts_for_provider_sorted() {
        let mut store = test_store();
        let mut acc_b = test_account(ApiProvider::Anthropic, api_key_auth("b"));
        acc_b.sort_order = 1;
        acc_b.label = "B".to_string();
        let mut acc_a = test_account(ApiProvider::Anthropic, api_key_auth("a"));
        acc_a.sort_order = 0;
        acc_a.label = "A".to_string();

        store.accounts.push(acc_b);
        store.accounts.push(acc_a);

        let accounts = store.accounts_for_provider(ApiProvider::Anthropic, None);
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[0].label, "A");
        assert_eq!(accounts[1].label, "B");
    }

    // === Additional coverage for unverified claims ===

    #[test]
    fn compatible_backends_openai_oauth() {
        let account = test_account(ApiProvider::OpenAI, oauth_auth("chatgpt-oauth"));
        assert_eq!(account.compatible_backends(), vec!["codex"]);
    }

    #[test]
    fn compatible_backends_github_api_key_empty() {
        let account = test_account(ApiProvider::GitHub, api_key_auth("ghp_test"));
        // GitHub API keys don't map to any agent backend
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn compatible_backends_google_oauth_empty() {
        let account = test_account(ApiProvider::Google, oauth_auth("goog-oauth"));
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn compatible_backends_github_oauth_empty() {
        let account = test_account(ApiProvider::GitHub, oauth_auth("gh-oauth"));
        assert!(account.compatible_backends().is_empty());
    }

    #[test]
    fn resolve_with_openai_api_key() {
        let mut store = test_store();
        store.accounts.push(test_account(
            ApiProvider::OpenAI,
            api_key_auth("sk-openai-key"),
        ));
        let identity = store.resolve(None, None);
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
            oauth_auth("chatgpt-oauth-json"),
        ));
        let identity = store.resolve(None, None);
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
            api_key_auth("ghp_my_token"),
        ));
        let identity = store.resolve(None, None);
        assert_eq!(identity.github_token, Some("ghp_my_token".to_string()));
    }

    #[test]
    fn resolve_github_token_with_override() {
        let mut store = test_store();
        let mut acc1 = test_account(ApiProvider::GitHub, api_key_auth("ghp_primary"));
        acc1.id = "gh_1".to_string();
        acc1.sort_order = 0;
        let mut acc2 = test_account(ApiProvider::GitHub, api_key_auth("ghp_secondary"));
        acc2.id = "gh_2".to_string();
        acc2.sort_order = 1;

        store.accounts.push(acc1);
        store.accounts.push(acc2);

        // Without override: first account wins
        let identity = store.resolve(None, None);
        assert_eq!(identity.github_token, Some("ghp_primary".to_string()));

        // With override: selected account wins
        let overrides = AccountOverrides {
            github_account_id: Some("gh_2".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(None, Some(&overrides));
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
        let identity = store.resolve(None, None);
        assert_eq!(identity.name, "Test User");
        assert_eq!(identity.email, "test@example.com");

        // With override: selected git identity wins
        let overrides = AccountOverrides {
            git_identity_id: Some("gi_2".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(None, Some(&overrides));
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
            api_key_auth("real-key"),
        ));

        // Without override: works fine
        let identity = store.resolve(None, None);
        assert!(identity.claude_auth.is_some());

        // With invalid override: no fallback
        let overrides = AccountOverrides {
            anthropic_account_id: Some("nonexistent_id".to_string()),
            ..Default::default()
        };
        let identity = store.resolve(None, Some(&overrides));
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
        let identity = store.resolve(None, Some(&overrides));
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
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::new(),
        };
        let identity = store.resolve(None, None);
        assert_eq!(identity.name, "");
        assert_eq!(identity.email, "");
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
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::new(),
        };
        let default = store.default_git_identity().unwrap();
        assert_eq!(default.id, "gi_low");
    }

    #[test]
    fn provider_auth_credential_value() {
        let api_key = api_key_auth("key-val");
        assert_eq!(api_key.credential_value(), Some("key-val"));

        let oauth = oauth_auth("oauth-val");
        assert_eq!(oauth.credential_value(), Some("oauth-val"));

        assert_eq!(ProviderAuth::ClaudeProfile.credential_value(), None);
    }

    #[test]
    fn provider_auth_type_labels() {
        assert_eq!(api_key_auth("x").auth_type_label(), "api_key");
        assert_eq!(oauth_auth("x").auth_type_label(), "oauth_token");
        assert_eq!(
            ProviderAuth::ClaudeProfile.auth_type_label(),
            "claude_profile"
        );
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
    fn api_provider_all_returns_every_provider() {
        assert_eq!(
            ApiProvider::all(),
            &[
                ApiProvider::Anthropic,
                ApiProvider::OpenAI,
                ApiProvider::Google,
                ApiProvider::OpenRouter,
                ApiProvider::OpenCode,
                ApiProvider::Ollama,
                ApiProvider::GitHub,
            ]
        );
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
            serde_json::from_str::<ApiProvider>(r#""opencode""#).unwrap(),
            ApiProvider::OpenCode
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
        let api_key = api_key_auth("key");
        let json = serde_json::to_string(&api_key).unwrap();
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.credential_value(), Some("key"));

        let oauth = oauth_auth("tok");
        let json = serde_json::to_string(&oauth).unwrap();
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.credential_value(), Some("tok"));

        let base_url = ProviderAuth::BaseUrl {
            url: "http://localhost:11434".to_string(),
        };
        let json = serde_json::to_string(&base_url).unwrap();
        assert_eq!(
            json,
            r#"{"type":"base_url","url":"http://localhost:11434"}"#
        );
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            parsed,
            ProviderAuth::BaseUrl { url } if url == "http://localhost:11434"
        ));
        assert_eq!(base_url.credential_value(), None);
        assert_eq!(base_url.auth_type_label(), "base_url");

        let profile = ProviderAuth::ClaudeProfile;
        let json = serde_json::to_string(&profile).unwrap();
        assert_eq!(json, r#"{"type":"claude_profile"}"#);
        let parsed: ProviderAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ProviderAuth::ClaudeProfile));
    }

    #[test]
    fn account_source_serde_roundtrip() {
        for source in [AccountSource::Configured, AccountSource::Server] {
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
        let anthropic = store.accounts_for_provider(ApiProvider::Anthropic, None);
        assert_eq!(anthropic.len(), 1);
        assert_eq!(anthropic[0].source, AccountSource::Server);
        match &anthropic[0].auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "sk-ant-test"),
            other => panic!("expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn from_user_identity_codex_creates_openai_account() {
        let identity = UserIdentity {
            codex_auth: Some(CodexAuth::ApiKey("sk-openai".to_string())),
            ..test_user_identity()
        };
        let store = IdentityStore::from_user_identity(&identity);
        let openai = store.accounts_for_provider(ApiProvider::OpenAI, None);
        assert_eq!(openai.len(), 1);
        match &openai[0].auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "sk-openai"),
            other => panic!("expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn from_user_identity_github_token_creates_github_account() {
        let store = IdentityStore::from_user_identity(&test_user_identity());
        let github = store.accounts_for_provider(ApiProvider::GitHub, None);
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
        let account = test_account(ApiProvider::Anthropic, api_key_auth("super-secret-key"));
        let info = AccountInfo::from(&account);
        let json = serde_json::to_string(&info).unwrap();
        assert!(
            !json.contains("super-secret-key"),
            "AccountInfo should not contain credential values"
        );
    }
}
