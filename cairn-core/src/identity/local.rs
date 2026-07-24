//! Local identity store — file I/O for `~/.cairn/identity.yaml`.
//!
//! Supports two on-disk formats:
//! - **v1** (legacy): flat `userId/email/name/claudeAuth/codexAuth/githubToken`
//! - **v2** (current): `version: 2` with `gitIdentities[]` and `accounts[]`
//!
//! v1 files are auto-migrated to v2 on load. Sensitive fields (API keys, tokens)
//! are encrypted at rest using a machine-derived key.

use serde::{Deserialize, Serialize};
use std::path::Path;

use super::crypto::{decrypt_credential, encrypt_credential, get_machine_id};
use super::{
    AccountOverrides, AccountSource, ApiProvider, ClaudeAuth, CodexAuth, GitIdentity,
    IdentityStore, ProviderAccount, ProviderAuth, UserIdentity,
};

const IDENTITY_FILENAME: &str = "identity.yaml";

// === V1 (legacy) on-disk format ===

/// On-disk representation of identity (v1 format). Detected by absence of `version` field.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityFileV1 {
    user_id: String,
    email: String,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    claude_auth: Option<ClaudeAuthFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    codex_auth: Option<CodexAuthFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    github_token: Option<String>, // encrypted
}

/// On-disk representation of Claude auth with encrypted token value.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClaudeAuthFile {
    OAuthToken { encrypted: String },
    ApiKey { encrypted: String },
}

/// On-disk representation of Codex auth with encrypted value.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CodexAuthFile {
    OAuthToken { encrypted: String },
    ApiKey { encrypted: String },
}

// === V2 (current) on-disk format ===

/// On-disk representation of the identity store (v2 format).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityStoreFile {
    version: u32,
    user_id: String,
    #[serde(default)]
    git_identities: Vec<GitIdentityFile>,
    #[serde(default)]
    accounts: Vec<AccountFile>,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    project_overrides: std::collections::HashMap<String, AccountOverrides>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GitIdentityFile {
    id: String,
    label: String,
    name: String,
    email: String,
    sort_order: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountFile {
    id: String,
    label: String,
    api_provider: ApiProvider,
    source: AccountSource,
    auth: AuthFile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    project_id: Option<String>,
    sort_order: i32,
    created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_used_at: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum AuthFile {
    #[serde(rename = "api_key")]
    ApiKey { encrypted: String },
    #[serde(rename = "oauth_token", alias = "o_auth_token")]
    OAuthToken { encrypted: String },
    #[serde(rename = "local_cli")]
    LocalCli,
}

/// Intermediate struct to detect format version during load.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VersionProbe {
    #[serde(default)]
    version: Option<u32>,
}

// === Public API ===

/// Load identity store from `<config_dir>/identity.yaml`.
///
/// Returns `None` if the file doesn't exist. Automatically migrates v1 → v2.
pub fn load_identity_store(config_dir: &Path) -> Result<Option<IdentityStore>, String> {
    let path = config_dir.join(IDENTITY_FILENAME);

    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read identity file: {}", e))?;

    let probe: VersionProbe = serde_yaml::from_str(&content)
        .map_err(|e| format!("Failed to parse identity file: {}", e))?;

    let machine_id = get_machine_id();

    match probe.version {
        Some(2) => {
            // V2 format — parse directly
            let file: IdentityStoreFile = serde_yaml::from_str(&content)
                .map_err(|e| format!("Failed to parse v2 identity file: {}", e))?;
            let store = store_from_v2_file(file, &machine_id)?;
            Ok(Some(store))
        }
        None | Some(1) => {
            // V1 or unversioned — migrate
            let file: IdentityFileV1 = serde_yaml::from_str(&content)
                .map_err(|e| format!("Failed to parse v1 identity file: {}", e))?;
            let store = migrate_v1_to_store(file, &machine_id)?;

            // Save as v2 for future loads
            if let Err(e) = save_identity_store(config_dir, &store) {
                log::warn!("Failed to migrate identity file to v2: {}", e);
            } else {
                log::info!("Migrated identity.yaml from v1 to v2 format");
            }

            Ok(Some(store))
        }
        Some(v) => Err(format!("Unknown identity file version: {}", v)),
    }
}

/// Save identity store to `<config_dir>/identity.yaml` in v2 format.
pub(crate) fn save_identity_store(config_dir: &Path, store: &IdentityStore) -> Result<(), String> {
    let machine_id = get_machine_id();
    let file = store_to_v2_file(store, &machine_id)?;

    // Ensure directory exists
    std::fs::create_dir_all(config_dir)
        .map_err(|e| format!("Failed to create config directory: {}", e))?;

    let yaml =
        serde_yaml::to_string(&file).map_err(|e| format!("Failed to serialize identity: {}", e))?;
    let content = format!("# Cairn Identity Store (v2)\n{}", yaml);

    let path = config_dir.join(IDENTITY_FILENAME);
    std::fs::write(&path, content).map_err(|e| format!("Failed to write identity file: {}", e))?;

    // Set restrictive permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("Failed to set permissions: {}", e))?;
    }

    Ok(())
}

/// Check whether an identity file exists in the config directory.
pub fn identity_exists(config_dir: &Path) -> bool {
    config_dir.join(IDENTITY_FILENAME).exists()
}

/// Detect locally installed CLIs and return ephemeral accounts.
///
/// These are NOT persisted — they're merged at load time with sort_order
/// after all configured accounts (lowest priority by default).
pub fn detect_local_accounts() -> Vec<ProviderAccount> {
    let mut accounts = Vec::new();

    if crate::env::find_binary("claude").is_ok() {
        accounts.push(ProviderAccount {
            id: "local_cli_anthropic".to_string(),
            label: "Local CLI".to_string(),
            api_provider: ApiProvider::Anthropic,
            source: AccountSource::LocalCli,
            auth: ProviderAuth::LocalCli,
            project_id: None,
            sort_order: 1000, // Low priority — after all configured accounts
            created_at: 0,
            last_used_at: None,
        });
    }

    accounts
}

/// Auto-populate a new identity store from git config (for first-run).
pub(crate) fn identity_store_from_git_config() -> IdentityStore {
    let name = git_config_value("user.name").unwrap_or_default();
    let email = git_config_value("user.email").unwrap_or_default();

    IdentityStore {
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
    }
}

// === Backward-compatible API (delegates to store) ===

/// Load identity from `<config_dir>/identity.yaml` (backward-compatible).
///
/// Returns the resolved `UserIdentity` from the store (v1 or v2).
pub fn load_local_identity(config_dir: &Path) -> Result<Option<UserIdentity>, String> {
    match load_identity_store(config_dir)? {
        Some(store) => Ok(Some(store.resolve(None, None))),
        None => Ok(None),
    }
}

/// Save identity to `<config_dir>/identity.yaml` (backward-compatible).
///
/// Merges the `UserIdentity` fields into the existing store, or creates a new v2 store.
pub fn save_local_identity(config_dir: &Path, identity: &UserIdentity) -> Result<(), String> {
    let mut store = load_identity_store(config_dir)?.unwrap_or_else(|| IdentityStore {
        user_id: identity.user_id.clone(),
        accounts: vec![],
        git_identities: vec![],
        project_overrides: Default::default(),
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
            ClaudeAuth::OAuthToken(v) => ProviderAuth::OAuthToken { value: v.clone() },
            ClaudeAuth::ApiKey(v) => ProviderAuth::ApiKey { value: v.clone() },
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

    save_identity_store(config_dir, &store)
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
            });
        }
        (None, None) => {} // Nothing to do
    }
}

fn migrate_v1_to_store(file: IdentityFileV1, machine_id: &str) -> Result<IdentityStore, String> {
    let mut accounts = Vec::new();
    let now = chrono::Utc::now().timestamp();

    // Migrate Claude auth
    if let Some(claude_file) = file.claude_auth {
        let auth = match claude_file {
            ClaudeAuthFile::OAuthToken { encrypted } => ProviderAuth::OAuthToken {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
            ClaudeAuthFile::ApiKey { encrypted } => ProviderAuth::ApiKey {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
        };
        accounts.push(ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label: "Anthropic".to_string(),
            api_provider: ApiProvider::Anthropic,
            source: AccountSource::Configured,
            auth,
            project_id: None,
            sort_order: 0,
            created_at: now,
            last_used_at: None,
        });
    }

    // Migrate Codex auth
    if let Some(codex_file) = file.codex_auth {
        let auth = match codex_file {
            CodexAuthFile::OAuthToken { encrypted } => ProviderAuth::OAuthToken {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
            CodexAuthFile::ApiKey { encrypted } => ProviderAuth::ApiKey {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
        };
        accounts.push(ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label: "OpenAI".to_string(),
            api_provider: ApiProvider::OpenAI,
            source: AccountSource::Configured,
            auth,
            project_id: None,
            sort_order: 0,
            created_at: now,
            last_used_at: None,
        });
    }

    // Migrate GitHub token
    if let Some(encrypted) = file.github_token {
        let token = decrypt_credential(&encrypted, machine_id)?;
        accounts.push(ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label: "GitHub".to_string(),
            api_provider: ApiProvider::GitHub,
            source: AccountSource::Configured,
            auth: ProviderAuth::ApiKey { value: token },
            project_id: None,
            sort_order: 0,
            created_at: now,
            last_used_at: None,
        });
    }

    Ok(IdentityStore {
        user_id: file.user_id,
        accounts,
        git_identities: vec![GitIdentity {
            id: format!("gi_{}", uuid::Uuid::new_v4()),
            label: "Default".to_string(),
            name: file.name,
            email: file.email,
            sort_order: 0,
        }],
        project_overrides: Default::default(),
    })
}

fn store_from_v2_file(file: IdentityStoreFile, machine_id: &str) -> Result<IdentityStore, String> {
    let mut accounts = Vec::new();

    for acc_file in file.accounts {
        let auth = match acc_file.auth {
            AuthFile::ApiKey { encrypted } => ProviderAuth::ApiKey {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
            AuthFile::OAuthToken { encrypted } => ProviderAuth::OAuthToken {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
            AuthFile::LocalCli => ProviderAuth::LocalCli,
        };
        if acc_file.api_provider == ApiProvider::OpenAI
            && acc_file.source == AccountSource::LocalCli
        {
            continue;
        }
        accounts.push(ProviderAccount {
            id: acc_file.id,
            label: acc_file.label,
            api_provider: acc_file.api_provider,
            source: acc_file.source,
            auth,
            project_id: acc_file.project_id,
            sort_order: acc_file.sort_order,
            created_at: acc_file.created_at,
            last_used_at: acc_file.last_used_at,
        });
    }

    let git_identities = file
        .git_identities
        .into_iter()
        .map(|gi| GitIdentity {
            id: gi.id,
            label: gi.label,
            name: gi.name,
            email: gi.email,
            sort_order: gi.sort_order,
        })
        .collect();

    Ok(IdentityStore {
        user_id: file.user_id,
        accounts,
        git_identities,
        project_overrides: file.project_overrides,
    })
}

fn store_to_v2_file(store: &IdentityStore, machine_id: &str) -> Result<IdentityStoreFile, String> {
    let mut accounts = Vec::new();

    for account in &store.accounts {
        if account.api_provider == ApiProvider::OpenAI && account.source == AccountSource::LocalCli
        {
            continue;
        }

        // Skip ephemeral LocalCli accounts that haven't been reordered by the user.
        // If the user reordered them (sort_order != 1000), persist so the preference sticks.
        if account.source == AccountSource::LocalCli && account.sort_order == 1000 {
            continue;
        }

        let auth = match &account.auth {
            ProviderAuth::ApiKey { value } => AuthFile::ApiKey {
                encrypted: encrypt_credential(value, machine_id)?,
            },
            ProviderAuth::OAuthToken { value } => AuthFile::OAuthToken {
                encrypted: encrypt_credential(value, machine_id)?,
            },
            ProviderAuth::LocalCli => AuthFile::LocalCli,
        };

        accounts.push(AccountFile {
            id: account.id.clone(),
            label: account.label.clone(),
            api_provider: account.api_provider,
            source: account.source.clone(),
            auth,
            project_id: account.project_id.clone(),
            sort_order: account.sort_order,
            created_at: account.created_at,
            last_used_at: account.last_used_at,
        });
    }

    let git_identities = store
        .git_identities
        .iter()
        .map(|gi| GitIdentityFile {
            id: gi.id.clone(),
            label: gi.label.clone(),
            name: gi.name.clone(),
            email: gi.email.clone(),
            sort_order: gi.sort_order,
        })
        .collect();

    Ok(IdentityStoreFile {
        version: 2,
        user_id: store.user_id.clone(),
        git_identities,
        accounts,
        project_overrides: store.project_overrides.clone(),
    })
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

    fn oauth_account(
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
            ProviderAuth::OAuthToken {
                value: value.to_string(),
            },
            sort_order,
            created_at,
            last_used_at,
        )
    }

    fn local_cli_account(
        id: &str,
        label: &str,
        api_provider: ApiProvider,
        sort_order: i32,
    ) -> ProviderAccount {
        ProviderAccount {
            id: id.to_string(),
            label: label.to_string(),
            api_provider,
            source: AccountSource::LocalCli,
            auth: ProviderAuth::LocalCli,
            project_id: None,
            sort_order,
            created_at: 0,
            last_used_at: None,
        }
    }

    fn git_identity(
        id: &str,
        label: &str,
        name: &str,
        email: &str,
        sort_order: i32,
    ) -> GitIdentity {
        GitIdentity {
            id: id.to_string(),
            label: label.to_string(),
            name: name.to_string(),
            email: email.to_string(),
            sort_order,
        }
    }

    fn identity_store(
        user_id: &str,
        accounts: Vec<ProviderAccount>,
        git_identities: Vec<GitIdentity>,
    ) -> IdentityStore {
        IdentityStore {
            user_id: user_id.to_string(),
            accounts,
            git_identities,
            project_overrides: Default::default(),
        }
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

    // === V2 format tests ===

    #[test]
    fn test_v2_save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();

        let store = identity_store(
            "local-test-123",
            vec![api_key_account(
                "acc_1",
                "Test Anthropic",
                ApiProvider::Anthropic,
                "sk-ant-test-key",
                0,
                1000,
                None,
            )],
            vec![git_identity(
                "gi_1",
                "Personal",
                "Test User",
                "test@example.com",
                0,
            )],
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );

        assert_eq!(loaded.user_id, store.user_id);
        assert_eq!(loaded.accounts.len(), 1);
        assert_eq!(loaded.accounts[0].label, "Test Anthropic");
        assert_eq!(loaded.accounts[0].api_provider, ApiProvider::Anthropic);
        match &loaded.accounts[0].auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "sk-ant-test-key"),
            other => panic!("Expected ApiKey, got {:?}", other),
        }
        assert_eq!(loaded.git_identities.len(), 1);
        assert_eq!(loaded.git_identities[0].name, "Test User");
    }

    #[test]
    fn test_v2_multiple_accounts() {
        let dir = TempDir::new().unwrap();

        let store = identity_store(
            "local-multi",
            vec![
                oauth_account(
                    "acc_1",
                    "Personal",
                    ApiProvider::Anthropic,
                    "oauth-token",
                    0,
                    1000,
                    None,
                ),
                api_key_account(
                    "acc_2",
                    "Work",
                    ApiProvider::Anthropic,
                    "sk-work-key",
                    1,
                    2000,
                    Some(3000),
                ),
                api_key_account(
                    "acc_3",
                    "OpenAI",
                    ApiProvider::OpenAI,
                    "sk-openai",
                    0,
                    1000,
                    None,
                ),
            ],
            vec![git_identity(
                "gi_1",
                "Default",
                "User",
                "user@example.com",
                0,
            )],
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );

        assert_eq!(loaded.accounts.len(), 3);
        assert_eq!(loaded.accounts[0].label, "Personal");
        assert_eq!(loaded.accounts[1].label, "Work");
        assert_eq!(loaded.accounts[1].last_used_at, Some(3000));
        assert_eq!(loaded.accounts[2].api_provider, ApiProvider::OpenAI);
    }

    #[test]
    fn test_v2_credentials_encrypted_on_disk() {
        let dir = TempDir::new().unwrap();
        let secret = "sk-ant-my-secret-token";

        let store = identity_store(
            "test",
            vec![api_key_account(
                "acc_1",
                "Test",
                ApiProvider::Anthropic,
                secret,
                0,
                0,
                None,
            )],
            vec![],
        );

        save_identity_store(dir.path(), &store).unwrap();

        let raw = std::fs::read_to_string(dir.path().join(IDENTITY_FILENAME)).unwrap();
        assert!(!raw.contains(secret), "Secret should be encrypted on disk");
        assert!(raw.contains("version: 2"), "Should be v2 format");
    }

    #[test]
    fn test_v2_local_cli_accounts_not_persisted() {
        let dir = TempDir::new().unwrap();

        let store = identity_store(
            "test",
            vec![
                api_key_account(
                    "acc_1",
                    "Configured",
                    ApiProvider::Anthropic,
                    "key",
                    0,
                    0,
                    None,
                ),
                local_cli_account(
                    "local_cli_anthropic",
                    "Local CLI",
                    ApiProvider::Anthropic,
                    1000,
                ),
            ],
            vec![],
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );

        // LocalCli accounts should not be persisted
        assert_eq!(loaded.accounts.len(), 1);
        assert_eq!(loaded.accounts[0].label, "Configured");
    }

    // === V1 migration tests ===

    #[test]
    fn test_v1_migration_preserves_credentials() {
        let dir = TempDir::new().unwrap();

        // Write a v1 format file by saving with the old API
        let identity = user_identity(
            "local-v1-user",
            "v1@example.com",
            "V1 User",
            Some(ClaudeAuth::ApiKey("sk-ant-v1-key".to_string())),
            Some(CodexAuth::OAuthToken("codex-oauth-json".to_string())),
            Some("ghp_test_token"),
        );

        // Write v1 format directly
        let machine_id = get_machine_id();
        let v1_file = IdentityFileV1 {
            user_id: identity.user_id.clone(),
            email: identity.email.clone(),
            name: identity.name.clone(),
            claude_auth: Some(ClaudeAuthFile::ApiKey {
                encrypted: encrypt_credential("sk-ant-v1-key", &machine_id).unwrap(),
            }),
            codex_auth: Some(CodexAuthFile::OAuthToken {
                encrypted: encrypt_credential("codex-oauth-json", &machine_id).unwrap(),
            }),
            github_token: Some(encrypt_credential("ghp_test_token", &machine_id).unwrap()),
        };
        let yaml = serde_yaml::to_string(&v1_file).unwrap();
        let content = format!("# Cairn User Identity\n{}", yaml);
        std::fs::write(dir.path().join(IDENTITY_FILENAME), content).unwrap();

        // Load should migrate to v2
        let store = load_identity_store(dir.path()).unwrap().unwrap();

        assert_eq!(store.user_id, "local-v1-user");
        assert_eq!(store.git_identities.len(), 1);
        assert_eq!(store.git_identities[0].name, "V1 User");
        assert_eq!(store.git_identities[0].email, "v1@example.com");

        // Should have 3 accounts (Anthropic, OpenAI, GitHub)
        assert_eq!(store.accounts.len(), 3);

        let anthropic = store
            .accounts
            .iter()
            .find(|a| a.api_provider == ApiProvider::Anthropic)
            .unwrap();
        match &anthropic.auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "sk-ant-v1-key"),
            other => panic!("Expected ApiKey, got {:?}", other),
        }

        let openai = store
            .accounts
            .iter()
            .find(|a| a.api_provider == ApiProvider::OpenAI)
            .unwrap();
        match &openai.auth {
            ProviderAuth::OAuthToken { value } => assert_eq!(value, "codex-oauth-json"),
            other => panic!("Expected OAuthToken, got {:?}", other),
        }

        let github = store
            .accounts
            .iter()
            .find(|a| a.api_provider == ApiProvider::GitHub)
            .unwrap();
        match &github.auth {
            ProviderAuth::ApiKey { value } => assert_eq!(value, "ghp_test_token"),
            other => panic!("Expected ApiKey, got {:?}", other),
        }

        // Verify the file was migrated on disk to v2
        let raw = std::fs::read_to_string(dir.path().join(IDENTITY_FILENAME)).unwrap();
        assert!(raw.contains("version: 2"));
    }

    // === Backward-compatible API tests ===

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
    fn test_identity_exists() {
        let dir = TempDir::new().unwrap();
        assert!(!identity_exists(dir.path()));

        let identity = user_identity("test", "test@test.com", "Test", None, None, None);
        save_local_identity(dir.path(), &identity).unwrap();
        assert!(identity_exists(dir.path()));
    }

    #[test]
    fn test_oauth_token_roundtrip() {
        let dir = TempDir::new().unwrap();

        let identity = user_identity(
            "test",
            "test@test.com",
            "Test",
            Some(ClaudeAuth::OAuthToken("oauth-token-xyz".to_string())),
            None,
            None,
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_local_identity(path, &identity),
            load_local_identity,
        );

        match loaded.claude_auth {
            Some(ClaudeAuth::OAuthToken(token)) => assert_eq!(token, "oauth-token-xyz"),
            other => panic!("Expected OAuthToken, got {:?}", other),
        }
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

    // === Additional coverage ===

    #[test]
    fn test_v2_project_overrides_roundtrip() {
        let dir = TempDir::new().unwrap();

        let mut overrides_map = std::collections::HashMap::new();
        overrides_map.insert(
            "proj_1".to_string(),
            AccountOverrides {
                anthropic_account_id: Some("acc_work".to_string()),
                github_account_id: Some("gh_work".to_string()),
                ..Default::default()
            },
        );

        let mut store = identity_store(
            "test",
            vec![],
            vec![git_identity("gi_1", "Default", "Test", "test@test.com", 0)],
        );
        store.project_overrides = overrides_map;

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );

        assert_eq!(loaded.project_overrides.len(), 1);
        let ov = loaded.project_overrides.get("proj_1").unwrap();
        assert_eq!(ov.anthropic_account_id, Some("acc_work".to_string()));
        assert_eq!(ov.github_account_id, Some("gh_work".to_string()));
        assert!(ov.openai_account_id.is_none());
        assert!(ov.git_identity_id.is_none());
    }

    #[test]
    fn test_v2_local_cli_with_custom_sort_order_is_persisted() {
        // When user reorders a LocalCli account (sort_order != 1000),
        // it should be persisted so the preference sticks.
        let dir = TempDir::new().unwrap();

        let store = identity_store(
            "test",
            vec![
                local_cli_account(
                    "local_cli_anthropic",
                    "Local CLI",
                    ApiProvider::Anthropic,
                    0,
                ),
                api_key_account(
                    "acc_1",
                    "API Key",
                    ApiProvider::Anthropic,
                    "sk-key",
                    1,
                    0,
                    None,
                ),
            ],
            vec![],
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );

        // Both accounts should be persisted since LocalCli has non-1000 sort_order
        assert_eq!(loaded.accounts.len(), 2);
        let local = loaded
            .accounts
            .iter()
            .find(|a| a.source == AccountSource::LocalCli)
            .expect("LocalCli account should be persisted");
        assert_eq!(local.sort_order, 0);
    }

    #[test]
    fn test_unknown_version_returns_error() {
        let dir = TempDir::new().unwrap();
        let content = "version: 99\nuserId: test\n";
        std::fs::write(dir.path().join(IDENTITY_FILENAME), content).unwrap();

        let result = load_identity_store(dir.path());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Unknown identity file version: 99"));
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
        assert_eq!(accounts[0].auth.credential_value(), Some("new-key"));
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
        assert_eq!(accounts[0].auth.credential_value(), Some("sk-new"));
        assert_eq!(accounts[0].source, AccountSource::Configured);
    }

    #[test]
    fn test_update_account_from_auth_noop_when_both_none() {
        let mut accounts: Vec<ProviderAccount> = vec![];
        update_account_from_auth(&mut accounts, ApiProvider::GitHub, None);
        assert!(accounts.is_empty());
    }

    #[test]
    fn detect_local_accounts_does_not_include_openai_local_cli() {
        let accounts = detect_local_accounts();
        assert!(!accounts.iter().any(|a| {
            a.api_provider == ApiProvider::OpenAI && a.source == AccountSource::LocalCli
        }));
    }

    #[test]
    fn test_v2_openai_local_cli_accounts_are_ignored_on_load() {
        let dir = TempDir::new().unwrap();
        let content = r#"# Cairn Identity Store (v2)
version: 2
userId: test-user
gitIdentities: []
accounts:
  - id: local_cli_openai
    label: Local CLI
    apiProvider: openai
    source: local_cli
    auth:
      type: local_cli
    sortOrder: 0
    createdAt: 0
"#;
        std::fs::write(dir.path().join(IDENTITY_FILENAME), content).unwrap();

        let store = load_identity_store(dir.path()).unwrap().unwrap();
        assert!(store.accounts.is_empty());
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
        let store = load_identity_store(dir.path()).unwrap().unwrap();
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

    #[test]
    fn test_load_identity_store_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = load_identity_store(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_v2_oauth_token_alias_on_disk() {
        // AuthFile::OAuthToken should deserialize from "o_auth_token" alias
        // (matches what older versions might write)
        let dir = TempDir::new().unwrap();
        let machine_id = get_machine_id();
        let encrypted = encrypt_credential("my-oauth-token", &machine_id).unwrap();

        let content = format!(
            r#"# Cairn Identity Store (v2)
version: 2
userId: test-user
gitIdentities: []
accounts:
  - id: acc_1
    label: Test
    apiProvider: anthropic
    source: configured
    auth:
      type: o_auth_token
      encrypted: {encrypted}
    sortOrder: 0
    createdAt: 1000
"#
        );
        std::fs::write(dir.path().join(IDENTITY_FILENAME), content).unwrap();

        // Should parse without error (alias handling)
        let store = load_identity_store(dir.path())
            .expect("Failed to parse o_auth_token alias")
            .expect("Store should not be None");
        assert_eq!(store.accounts.len(), 1);
        match &store.accounts[0].auth {
            ProviderAuth::OAuthToken { value } => assert_eq!(value, "my-oauth-token"),
            other => panic!("Expected OAuthToken, got {:?}", other),
        }
    }
}
