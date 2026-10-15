//! Runtime identity adapters over the credential store.

use super::{CredentialStoreExt, GitIdentity, UserIdentity};
use crate::credentials::*;
use cairn_models::accounts::store::*;
use std::path::Path;

/// Seed author metadata for a new credential store from the host Git config.
pub fn credential_store_from_git_config(config_dir: &Path) -> CredentialStore {
    let name = git_config_value("user.name").unwrap_or_default();
    let email = git_config_value("user.email").unwrap_or_default();

    CredentialStore {
        config_dir: config_dir.to_path_buf(),
        user_id: format!("local-{}", uuid::Uuid::new_v4()),
        accounts: vec![],
        git_identities: vec![GitIdentity {
            id: format!("gi_{}", uuid::Uuid::new_v4()),
            label: "Default".to_string(),
            name,
            email,
            sort_order: 0,
        }],
        project_overrides: Default::default(),
        retired_logins: Vec::new(),
    }
}

fn git_config_value(key: &str) -> Option<String> {
    std::process::Command::new("git")
        .args(["config", "--global", key])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if value.is_empty() {
                    None
                } else {
                    Some(value)
                }
            } else {
                None
            }
        })
}

// === Backward-compatible API (delegates to store) ===

/// Load identity from `<config_dir>/credentials.yaml` (backward-compatible).
///
/// Returns the resolved `UserIdentity` from the store (v1 or v2).
pub fn load_local_identity(config_dir: &Path) -> Result<Option<UserIdentity>, String> {
    match load_credential_store(config_dir)? {
        Some(store) => Ok(Some(store.resolve(None, None))),
        None => Ok(None),
    }
}

/// Save identity to `<config_dir>/credentials.yaml` (backward-compatible).
///
/// Merges the `UserIdentity` fields into the existing store, or creates a new v2 store.
pub fn save_local_identity(config_dir: &Path, identity: &UserIdentity) -> Result<(), String> {
    let mut store = load_credential_store(config_dir)?.unwrap_or_else(|| CredentialStore {
        user_id: identity.user_id.clone(),
        accounts: vec![],
        git_identities: vec![],
        project_overrides: Default::default(),
        retired_logins: Vec::new(),
        config_dir: config_dir.to_path_buf(),
    });

    // Update git identity
    if store.git_identities.is_empty() {
        store.git_identities.push(GitIdentity {
            id: format!("gi_{}", uuid::Uuid::new_v4()),
            label: "Default".to_string(),
            name: identity.name.clone(),
            email: identity.email.clone(),
            sort_order: 0,
        });
    } else {
        let gi = &mut store.git_identities[0];
        gi.name = identity.name.clone();
        gi.email = identity.email.clone();
    }

    // Update Claude auth
    update_account_from_auth(
        &mut store.accounts,
        ApiProvider::Anthropic,
        identity.claude_auth.as_ref().map(|a| match a {
            ClaudeAuth::ApiKey(v) => ProviderAuth::ApiKey { value: v.clone() },
            ClaudeAuth::ManagedProfile { .. } => ProviderAuth::ClaudeProfile { oauth: None },
        }),
    );

    // Update Codex auth
    update_account_from_auth(
        &mut store.accounts,
        ApiProvider::OpenAI,
        identity.codex_auth.as_ref().map(|a| match a {
            CodexAuth::OAuthToken(v) => ProviderAuth::OAuthToken { value: v.clone() },
            CodexAuth::ApiKey(v) => ProviderAuth::ApiKey { value: v.clone() },
        }),
    );

    // Update GitHub token
    update_account_from_auth(
        &mut store.accounts,
        ApiProvider::GitHub,
        identity
            .github_token
            .as_ref()
            .map(|v| ProviderAuth::ApiKey { value: v.clone() }),
    );

    save_credential_store(config_dir, &store)
}

// === Internal helpers ===

/// Update or insert the first configured account for a provider.
fn update_account_from_auth(
    accounts: &mut Vec<ProviderAccount>,
    provider: ApiProvider,
    auth: Option<ProviderAuth>,
) {
    // Find first configured account for this provider
    let existing_idx = accounts
        .iter()
        .position(|a| a.api_provider == provider && a.source == AccountSource::Configured);

    match (existing_idx, auth) {
        // A `UserIdentity` can say "managed profile" but carries none of its
        // tokens, so it must not overwrite the ones the store holds.
        (Some(idx), Some(ProviderAuth::ClaudeProfile { oauth: None }))
            if matches!(accounts[idx].auth, ProviderAuth::ClaudeProfile { .. }) => {}
        (Some(idx), Some(new_auth)) => {
            accounts[idx].auth = new_auth;
        }
        (Some(idx), None) => {
            accounts.remove(idx);
        }
        (None, Some(new_auth)) => {
            let now = chrono::Utc::now().timestamp();
            accounts.push(ProviderAccount {
                id: format!("acc_{}", uuid::Uuid::new_v4()),
                label: format!("{}", provider),
                api_provider: provider,
                source: AccountSource::Configured,
                auth: new_auth,
                project_id: None,
                sort_order: 0,
                created_at: now,
                last_used_at: None,
                email: None,
                plan: None,
                health: None,
            });
        }
        (None, None) => {} // Nothing to do
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    fn roundtrip_saved<T>(
        dir: &TempDir,
        save: impl FnOnce(&Path) -> Result<(), String>,
        load: impl FnOnce(&Path) -> Result<Option<T>, String>,
    ) -> T {
        save(dir.path()).unwrap();
        load(dir.path()).unwrap().unwrap()
    }

    fn configured_account(
        id: &str,
        label: &str,
        api_provider: ApiProvider,
        auth: ProviderAuth,
        sort_order: i32,
        created_at: i64,
        last_used_at: Option<i64>,
    ) -> ProviderAccount {
        ProviderAccount {
            id: id.to_string(),
            label: label.to_string(),
            api_provider,
            source: AccountSource::Configured,
            auth,
            project_id: None,
            sort_order,
            created_at,
            last_used_at,
            email: None,
            plan: None,
            health: None,
        }
    }

    fn api_key_account(
        id: &str,
        label: &str,
        api_provider: ApiProvider,
        value: &str,
        sort_order: i32,
        created_at: i64,
        last_used_at: Option<i64>,
    ) -> ProviderAccount {
        configured_account(
            id,
            label,
            api_provider,
            ProviderAuth::ApiKey {
                value: value.to_string(),
            },
            sort_order,
            created_at,
            last_used_at,
        )
    }

    fn user_identity(
        user_id: &str,
        email: &str,
        name: &str,
        claude_auth: Option<ClaudeAuth>,
        codex_auth: Option<CodexAuth>,
        github_token: Option<&str>,
    ) -> UserIdentity {
        UserIdentity {
            user_id: user_id.to_string(),
            email: email.to_string(),
            name: name.to_string(),
            claude_auth,
            codex_auth,
            github_token: github_token.map(str::to_string),
        }
    }

    #[test]
    fn test_backward_compat_save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();

        let identity = user_identity(
            "local-test-123",
            "test@example.com",
            "Test User",
            Some(ClaudeAuth::ApiKey("sk-ant-test-key".to_string())),
            None,
            Some("ghp_test_token"),
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_local_identity(path, &identity),
            load_local_identity,
        );

        assert_eq!(loaded.user_id, identity.user_id);
        assert_eq!(loaded.email, identity.email);
        assert_eq!(loaded.name, identity.name);
        assert_eq!(
            loaded.claude_auth.as_ref().map(|a| a.value()),
            identity.claude_auth.as_ref().map(|a| a.value())
        );
        assert_eq!(loaded.github_token, identity.github_token);
    }

    #[test]
    fn test_backward_compat_without_credentials() {
        let dir = TempDir::new().unwrap();

        let identity = user_identity(
            "local-no-creds",
            "user@example.com",
            "Plain User",
            None,
            None,
            None,
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_local_identity(path, &identity),
            load_local_identity,
        );

        assert_eq!(loaded.name, "Plain User");
        assert!(loaded.claude_auth.is_none());
        assert!(loaded.github_token.is_none());
    }

    #[test]
    fn test_load_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = load_local_identity(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_runtime_identity_creates_credential_store() {
        let dir = TempDir::new().unwrap();
        assert!(!credential_store_exists(dir.path()));

        let identity = user_identity("test", "test@test.com", "Test", None, None, None);
        save_local_identity(dir.path(), &identity).unwrap();
        assert!(credential_store_exists(dir.path()));
    }

    #[test]
    fn test_claude_profile_roundtrip() {
        let dir = TempDir::new().unwrap();

        let identity = user_identity(
            "test",
            "test@test.com",
            "Test",
            Some(ClaudeAuth::ManagedProfile {
                account_id: "ignored".into(),
            }),
            None,
            None,
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_local_identity(path, &identity),
            load_local_identity,
        );

        // The stored shape is the account, not a path: resolution names the
        // account, and its home is placed at spawn.
        assert!(matches!(
            loaded.claude_auth,
            Some(ClaudeAuth::ManagedProfile { .. })
        ));
    }

    #[test]
    fn test_codex_oauth_roundtrip() {
        let dir = TempDir::new().unwrap();
        let auth_json = r#"{"auth_mode":"chatgpt","tokens":{"id_token":"id","access_token":"at","refresh_token":"rt"}}"#;

        let identity = user_identity(
            "test",
            "test@test.com",
            "Test",
            None,
            Some(CodexAuth::OAuthToken(auth_json.to_string())),
            None,
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_local_identity(path, &identity),
            load_local_identity,
        );

        match loaded.codex_auth {
            Some(CodexAuth::OAuthToken(json)) => assert_eq!(json, auth_json),
            other => panic!("Expected CodexAuth::OAuthToken, got {:?}", other),
        }
    }

    #[test]
    fn test_codex_api_key_roundtrip() {
        let dir = TempDir::new().unwrap();

        let identity = user_identity(
            "test",
            "test@test.com",
            "Test",
            None,
            Some(CodexAuth::ApiKey("sk-openai-test-key".to_string())),
            None,
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_local_identity(path, &identity),
            load_local_identity,
        );

        match loaded.codex_auth {
            Some(CodexAuth::ApiKey(key)) => assert_eq!(key, "sk-openai-test-key"),
            other => panic!("Expected CodexAuth::ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn test_update_account_from_auth_updates_existing() {
        let mut accounts = vec![api_key_account(
            "acc_1",
            "Anthropic",
            ApiProvider::Anthropic,
            "old-key",
            0,
            0,
            None,
        )];

        update_account_from_auth(
            &mut accounts,
            ApiProvider::Anthropic,
            Some(ProviderAuth::ApiKey {
                value: "new-key".to_string(),
            }),
        );

        assert_eq!(accounts.len(), 1);
        assert!(matches!(&accounts[0].auth, ProviderAuth::ApiKey { value } if value == "new-key"));
    }

    #[test]
    fn test_update_account_from_auth_removes_when_none() {
        let mut accounts = vec![api_key_account(
            "acc_1",
            "Anthropic",
            ApiProvider::Anthropic,
            "key",
            0,
            0,
            None,
        )];

        update_account_from_auth(&mut accounts, ApiProvider::Anthropic, None);

        assert!(accounts.is_empty());
    }

    #[test]
    fn test_update_account_from_auth_inserts_new() {
        let mut accounts: Vec<ProviderAccount> = vec![];

        update_account_from_auth(
            &mut accounts,
            ApiProvider::OpenAI,
            Some(ProviderAuth::ApiKey {
                value: "sk-new".to_string(),
            }),
        );

        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].api_provider, ApiProvider::OpenAI);
        assert!(matches!(&accounts[0].auth, ProviderAuth::ApiKey { value } if value == "sk-new"));
        assert_eq!(accounts[0].source, AccountSource::Configured);
    }

    #[test]
    fn test_update_account_from_auth_noop_when_both_none() {
        let mut accounts: Vec<ProviderAccount> = vec![];
        update_account_from_auth(&mut accounts, ApiProvider::GitHub, None);
        assert!(accounts.is_empty());
    }

    #[test]
    fn test_backward_compat_save_merges_into_existing_store() {
        let dir = TempDir::new().unwrap();

        // First save creates a store with Claude auth
        let identity1 = user_identity(
            "user-1",
            "test@test.com",
            "Test",
            Some(ClaudeAuth::ApiKey("sk-claude".to_string())),
            None,
            None,
        );
        save_local_identity(dir.path(), &identity1).unwrap();

        // Second save adds Codex auth — should merge, not replace
        let identity2 = user_identity(
            "user-1",
            "test@test.com",
            "Test",
            Some(ClaudeAuth::ApiKey("sk-claude".to_string())),
            Some(CodexAuth::ApiKey("sk-openai".to_string())),
            None,
        );
        save_local_identity(dir.path(), &identity2).unwrap();

        // Load the store directly to verify structure
        let store = load_credential_store(dir.path()).unwrap().unwrap();
        let anthropic_accs: Vec<_> = store
            .accounts
            .iter()
            .filter(|a| a.api_provider == ApiProvider::Anthropic)
            .collect();
        let openai_accs: Vec<_> = store
            .accounts
            .iter()
            .filter(|a| a.api_provider == ApiProvider::OpenAI)
            .collect();

        assert_eq!(anthropic_accs.len(), 1);
        assert_eq!(openai_accs.len(), 1);
    }
}
