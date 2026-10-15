//! Runtime identity projection and author display resolution.

pub mod display;
pub mod local;

use crate::credentials::*;
pub use crate::models::GitIdentity;
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

/// Assemble the application runtime context from stored accounts and author values.
/// Persistence and provider selection remain in `cairn-credentials`.
pub trait CredentialStoreExt {
    fn from_user_identity(identity: &UserIdentity) -> Self;
    /// The provider's API-key account a routed launch falls back to, and the
    /// identity it resolves to.
    fn resolve_routed_api_key_fallback(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
    ) -> Option<(String, UserIdentity)>;
    fn resolve_with_routed_account(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        account_id: &str,
    ) -> UserIdentity;
    fn resolve_with_provider_account(
        &self,
        backend: &str,
        account_id: &str,
    ) -> Option<UserIdentity>;
    fn default_git_identity(&self) -> Option<&GitIdentity>;
    fn resolve(
        &self,
        project_id: Option<&str>,
        overrides: Option<&AccountOverrides>,
    ) -> UserIdentity;
}

impl CredentialStoreExt for CredentialStore {
    fn from_user_identity(identity: &UserIdentity) -> Self {
        let mut accounts = Vec::new();
        let now = chrono::Utc::now().timestamp();

        if let Some(auth) = &identity.claude_auth {
            let provider_auth = match auth {
                ClaudeAuth::ApiKey(v) => ProviderAuth::ApiKey { value: v.clone() },
                ClaudeAuth::ManagedProfile { .. } => ProviderAuth::ClaudeProfile { oauth: None },
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
    fn resolve_routed_api_key_fallback(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
    ) -> Option<(String, UserIdentity)> {
        let account_id = self
            .accounts_for_provider(provider.api_provider(), project_id)
            .into_iter()
            .find(|account| {
                matches!(account.auth, ProviderAuth::ApiKey { .. })
                    && account.serves_backend(provider.backend())
            })?
            .id
            .clone();
        let identity = self.resolve_with_routed_account(provider, project_id, &account_id);
        Some((account_id, identity))
    }
    fn resolve_with_routed_account(
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

    fn resolve_with_provider_account(
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
            .find(|account| account.id == account_id && account.serves_backend(backend))?;
        Some(self.resolve(None, Some(&overrides)))
    }

    fn default_git_identity(&self) -> Option<&GitIdentity> {
        self.git_identities.iter().min_by_key(|g| g.sort_order)
    }

    fn resolve(
        &self,
        project_id: Option<&str>,
        overrides: Option<&AccountOverrides>,
    ) -> UserIdentity {
        let ProviderCredentials {
            claude_auth,
            codex_auth,
            github_token,
        } = self.resolve_auth(project_id, overrides);
        let git_override = overrides.and_then(|o| o.git_identity_id.as_deref());
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

    fn test_store() -> CredentialStore {
        CredentialStore {
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
        let mut account = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
        account.id = id.to_string();
        account.health = Some(ProviderAccountHealth {
            windows: vec![cairn_llm::types::usage::ProviderUsageWindow {
                id: "five-hour".to_string(),
                label: "5h".to_string(),
                scope: cairn_llm::types::usage::ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0 - remaining,
                remaining_percent: remaining,
                resets_at: None,
                reset_at_text: None,
                window_duration_mins: Some(300),
            }],
            blocked_until: None,
            depleted: None,
            captured_at,
        });
        account
    }

    #[tokio::test]
    async fn live_two_profile_shape_persists_the_opus_selection() {
        let mut store = test_store();
        let mut terminal_429 = healthy_claude_account("quarantined", 0.0, 100);
        terminal_429.health.as_mut().unwrap().windows[0].resets_at = Some(500);
        let mut usable_for_opus = healthy_claude_account("opus-profile", 24.0, 100);
        usable_for_opus.health.as_mut().unwrap().windows.push(
            cairn_llm::types::usage::ProviderUsageWindow {
                id: "weekly_scoped-fable".to_string(),
                label: "Current week (Fable)".to_string(),
                scope: cairn_llm::types::usage::ProviderUsageScope::Weekly,
                scope_target: Some("Fable".to_string()),
                used_percent: 100.0,
                remaining_percent: 0.0,
                resets_at: Some(800),
                reset_at_text: None,
                window_duration_mins: None,
            },
        );
        store.accounts = vec![terminal_429, usable_for_opus];
        let selected_id = store
            .select_routed_account(
                RoutedProvider::Claude,
                None,
                None,
                None,
                Some("opus"),
                &[],
                200,
            )
            .expect("Opus has one available managed profile")
            .id
            .clone();

        let db = crate::storage::migrated_test_db("model-aware-claude-account-pin.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
               VALUES ('w', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'w', 'Project', 'PROJ', '/tmp/proj', 1, 1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES ('j', 'p', 'running', 's', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('s', 'j', 'claude', 'open', 1, 1, 1);",
        )
        .await
        .expect("seed an unpinned Claude session");

        crate::sessions::queries::set_account_id(&db, "s", &selected_id)
            .await
            .expect("persist the selected account");
        let persisted = crate::sessions::queries::get(&db, "s")
            .await
            .expect("read the pinned session");
        assert_eq!(persisted.account_id.as_deref(), Some("opus-profile"));
        assert!(persisted.account_id.is_some());
    }

    #[test]
    fn a_managed_profile_resolves_to_its_account() {
        // Resolution names the account; where its home lives is asked of the
        // installed subscription support at spawn.
        let mut store = test_store();
        let mut account = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
        account.id = "acc_profile".to_string();
        store.accounts.push(account);

        match store.resolve(None, None).claude_auth {
            Some(ClaudeAuth::ManagedProfile { account_id }) => {
                assert_eq!(account_id, "acc_profile")
            }
            other => panic!("expected a managed profile, got {other:?}"),
        }
    }

    fn codex_account(id: &str, remaining: Option<f64>, captured_at: i64) -> ProviderAccount {
        let mut account = test_account(ApiProvider::OpenAI, oauth_auth(&format!("{id}-auth-json")));
        account.id = id.to_string();
        account.health = remaining.map(|remaining| ProviderAccountHealth {
            windows: vec![cairn_llm::types::usage::ProviderUsageWindow {
                id: "primary".to_string(),
                label: "5-hour window".to_string(),
                scope: cairn_llm::types::usage::ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0 - remaining,
                remaining_percent: remaining,
                resets_at: None,
                reset_at_text: None,
                window_duration_mins: Some(300),
            }],
            blocked_until: None,
            depleted: None,
            captured_at,
        });
        account
    }

    #[test]
    fn resolving_a_codex_account_pins_only_that_provider() {
        let mut store = test_store();
        let mut profile = test_account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile { oauth: None },
        );
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
            matches!(
                identity.claude_auth,
                Some(ClaudeAuth::ManagedProfile { .. })
            ),
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
        let store = CredentialStore {
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
    fn from_user_identity_claude_api_key_creates_anthropic_account() {
        let store = CredentialStore::from_user_identity(&test_user_identity());
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
        let store = CredentialStore::from_user_identity(&identity);
        let openai = store.accounts_for_provider(ApiProvider::OpenAI, None);
        assert_eq!(openai.len(), 1);
        match &openai[0].auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "sk-openai"),
            other => panic!("expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn from_user_identity_github_token_creates_github_account() {
        let store = CredentialStore::from_user_identity(&test_user_identity());
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
        let store = CredentialStore::from_user_identity(&identity);
        assert!(store.accounts.is_empty());
    }

    #[test]
    fn from_user_identity_populates_git_identity() {
        let store = CredentialStore::from_user_identity(&test_user_identity());
        assert_eq!(store.git_identities.len(), 1);
        assert_eq!(store.git_identities[0].name, "Alice");
        assert_eq!(store.git_identities[0].email, "alice@example.com");
    }

    #[test]
    fn from_user_identity_sets_user_id() {
        let store = CredentialStore::from_user_identity(&test_user_identity());
        assert_eq!(store.user_id, "user-abc");
    }

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
    fn default_git_identity_picks_min_sort_order() {
        let store = CredentialStore {
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
}
