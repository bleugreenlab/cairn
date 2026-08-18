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
    AccountOverrides, AccountSource, ActualTargetKind, ApiProvider, ClaudeAuth, CodexAuth,
    GitIdentity, IdentityStore, ProviderAccount, ProviderAuth, RetiredLogin, UserIdentity,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    retired_logins: Vec<RetiredLogin>,
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
    source: SourceFile,
    auth: AuthFile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    project_id: Option<String>,
    sort_order: i32,
    created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_used_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    health: Option<super::ProviderAccountHealth>,
}

/// On-disk account provenance.
///
/// `local_cli` is a retired shape: ambient CLI accounts used to be written
/// here. The runtime model has no such source any more, so those rows are
/// dropped when the file loads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SourceFile {
    Configured,
    LocalCli,
    Server,
}

/// On-disk credential shape.
///
/// `local_cli` is likewise retired and readable only so an existing file still
/// parses; nothing writes it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum AuthFile {
    #[serde(rename = "api_key")]
    ApiKey { encrypted: String },
    #[serde(rename = "oauth_token", alias = "o_auth_token")]
    OAuthToken { encrypted: String },
    #[serde(rename = "base_url")]
    BaseUrl { url: String },
    /// One Actual target. The endpoint and the cluster pin are plain connection
    /// metadata; the relay credential is encrypted at rest exactly like any
    /// other secret, so a target that gains a key does not become a plaintext
    /// credential on disk.
    // The rest of `identity.yaml` is camelCase, so these multi-word fields say
    // so explicitly; without it they would be the only snake_case keys in the
    // file.
    #[serde(rename = "actual_target", rename_all = "camelCase")]
    ActualTarget {
        kind: ActualTargetKind,
        base_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_api_key: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cluster_id: Option<String>,
    },
    #[serde(rename = "local_cli")]
    LocalCli,
    #[serde(rename = "claude_profile")]
    ClaudeProfile,
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
            let (mut store, retired) = store_from_v2_file(file, &machine_id)?;
            store.config_dir = config_dir.to_path_buf();
            // Write the pruned store straight back. The retired logins are the
            // only record that those accounts existed, and they have to outlive
            // this process for the UI to explain the change even once.
            if retired {
                if let Err(e) = save_identity_store(config_dir, &store) {
                    log::warn!("Failed to persist retired Anthropic logins: {e}");
                } else {
                    log::info!("Retired non-profile Anthropic logins from identity.yaml");
                }
            }
            Ok(Some(store))
        }
        None | Some(1) => {
            // V1 or unversioned — migrate
            let file: IdentityFileV1 = serde_yaml::from_str(&content)
                .map_err(|e| format!("Failed to parse v1 identity file: {}", e))?;
            let mut store = migrate_v1_to_store(file, &machine_id)?;
            store.config_dir = config_dir.to_path_buf();

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

/// Auto-populate a new identity store from git config (for first-run).
///
/// Takes the configuration root rather than inferring one: this store resolves
/// managed profile paths, and it has to name the same directory the sign-in
/// that creates them will use.
pub(crate) fn identity_store_from_git_config(config_dir: &Path) -> IdentityStore {
    let name = git_config_value("user.name").unwrap_or_default();
    let email = git_config_value("user.email").unwrap_or_default();

    IdentityStore {
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
            ClaudeAuth::ConfigDir(_) => ProviderAuth::ClaudeProfile,
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
                email: None,
                plan: None,
                health: None,
            });
        }
        (None, None) => {} // Nothing to do
    }
}

fn migrate_v1_to_store(file: IdentityFileV1, machine_id: &str) -> Result<IdentityStore, String> {
    let mut accounts = Vec::new();
    let mut retired_logins = Vec::new();
    let now = chrono::Utc::now().timestamp();

    // Migrate Claude auth. A v1 setup token is the oldest form of the shape
    // this model dropped, so it retires here rather than migrating forward
    // into an account nothing can use.
    match file.claude_auth {
        Some(ClaudeAuthFile::OAuthToken { .. }) => retired_logins.push(RetiredLogin {
            label: "Anthropic".to_string(),
            auth_type: "oauth_token".to_string(),
            retired_at: now,
        }),
        Some(ClaudeAuthFile::ApiKey { encrypted }) => accounts.push(ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label: "Anthropic".to_string(),
            api_provider: ApiProvider::Anthropic,
            source: AccountSource::Configured,
            auth: ProviderAuth::ApiKey {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
            project_id: None,
            sort_order: 0,
            created_at: now,
            last_used_at: None,
            email: None,
            plan: None,
            health: None,
        }),
        None => {}
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
            email: None,
            plan: None,
            health: None,
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
            email: None,
            plan: None,
            health: None,
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
        retired_logins,
        // Stamped by `load_identity_store`, which is the only caller and the
        // only place that knows which root this file came from.
        config_dir: std::path::PathBuf::new(),
    })
}

/// Load a v2 file, dropping credential shapes the model no longer accepts.
///
/// Returns the store and whether anything was dropped, so the caller can write
/// the pruned file back. Two shapes go: ambient CLI rows (any provider) and
/// Anthropic setup tokens. Anthropic ones leave a `RetiredLogin` behind — the
/// user chose that credential, and needs to be told it is gone and that signing
/// in again creates a managed profile. OpenAI's ambient row was already
/// unusable before this and leaves nothing.
fn store_from_v2_file(
    file: IdentityStoreFile,
    machine_id: &str,
) -> Result<(IdentityStore, bool), String> {
    let mut accounts = Vec::new();
    let mut retired_logins = file.retired_logins;
    let already_retired = retired_logins.len();
    let mut dropped_openai_ambient = false;
    let now = chrono::Utc::now().timestamp();

    for acc_file in file.accounts {
        let is_anthropic = acc_file.api_provider == ApiProvider::Anthropic;
        let retired_shape = match &acc_file.auth {
            _ if acc_file.source == SourceFile::LocalCli => Some("local_cli"),
            AuthFile::LocalCli => Some("local_cli"),
            AuthFile::OAuthToken { .. } if is_anthropic => Some("oauth_token"),
            _ => None,
        };
        if let Some(auth_type) = retired_shape {
            if is_anthropic {
                retired_logins.push(RetiredLogin {
                    label: acc_file.label,
                    auth_type: auth_type.to_string(),
                    retired_at: now,
                });
            } else {
                dropped_openai_ambient = true;
            }
            continue;
        }

        let auth = match acc_file.auth {
            AuthFile::ApiKey { encrypted } => ProviderAuth::ApiKey {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
            AuthFile::OAuthToken { encrypted } => ProviderAuth::OAuthToken {
                value: decrypt_credential(&encrypted, machine_id)?,
            },
            AuthFile::BaseUrl { url } => ProviderAuth::BaseUrl { url },
            AuthFile::ActualTarget {
                kind,
                base_url,
                encrypted_api_key,
                cluster_id,
            } => ProviderAuth::ActualTarget {
                kind,
                base_url,
                api_key: encrypted_api_key
                    .map(|encrypted| decrypt_credential(&encrypted, machine_id))
                    .transpose()?,
                cluster_id,
            },
            AuthFile::ClaudeProfile => ProviderAuth::ClaudeProfile,
            // Both retired shapes are handled above; a `local_cli` row never
            // reaches here.
            AuthFile::LocalCli => continue,
        };
        accounts.push(ProviderAccount {
            id: acc_file.id,
            label: acc_file.label,
            api_provider: acc_file.api_provider,
            source: match acc_file.source {
                SourceFile::Server => AccountSource::Server,
                SourceFile::Configured | SourceFile::LocalCli => AccountSource::Configured,
            },
            auth,
            project_id: acc_file.project_id,
            sort_order: acc_file.sort_order,
            created_at: acc_file.created_at,
            last_used_at: acc_file.last_used_at,
            email: acc_file.email,
            plan: acc_file.plan,
            health: acc_file.health,
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

    let migrated = dropped_openai_ambient || retired_logins.len() > already_retired;
    Ok((
        IdentityStore {
            user_id: file.user_id,
            accounts,
            git_identities,
            project_overrides: file.project_overrides,
            retired_logins,
            // Stamped by `load_identity_store`, which knows the root.
            config_dir: std::path::PathBuf::new(),
        },
        migrated,
    ))
}

fn store_to_v2_file(store: &IdentityStore, machine_id: &str) -> Result<IdentityStoreFile, String> {
    let mut accounts = Vec::new();

    for account in &store.accounts {
        let auth = match &account.auth {
            ProviderAuth::ApiKey { value } => AuthFile::ApiKey {
                encrypted: encrypt_credential(value, machine_id)?,
            },
            ProviderAuth::OAuthToken { value } => AuthFile::OAuthToken {
                encrypted: encrypt_credential(value, machine_id)?,
            },
            ProviderAuth::BaseUrl { url } => AuthFile::BaseUrl { url: url.clone() },
            ProviderAuth::ActualTarget {
                kind,
                base_url,
                api_key,
                cluster_id,
            } => AuthFile::ActualTarget {
                kind: *kind,
                base_url: base_url.clone(),
                encrypted_api_key: api_key
                    .as_deref()
                    .map(|value| encrypt_credential(value, machine_id))
                    .transpose()?,
                cluster_id: cluster_id.clone(),
            },
            ProviderAuth::ClaudeProfile => AuthFile::ClaudeProfile,
        };

        accounts.push(AccountFile {
            id: account.id.clone(),
            label: account.label.clone(),
            api_provider: account.api_provider,
            source: match account.source {
                AccountSource::Configured => SourceFile::Configured,
                AccountSource::Server => SourceFile::Server,
            },
            auth,
            project_id: account.project_id.clone(),
            sort_order: account.sort_order,
            created_at: account.created_at,
            last_used_at: account.last_used_at,
            email: account.email.clone(),
            plan: account.plan.clone(),
            health: account.health.clone(),
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
        retired_logins: store.retired_logins.clone(),
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
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::new(),
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
    fn test_v2_base_url_roundtrip_is_plaintext() {
        let dir = TempDir::new().unwrap();
        let url = "http://localhost:11434";
        let store = identity_store(
            "local-ollama",
            vec![configured_account(
                "acc_ollama",
                "Local Ollama",
                ApiProvider::Ollama,
                ProviderAuth::BaseUrl {
                    url: url.to_string(),
                },
                0,
                1000,
                None,
            )],
            vec![],
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );
        assert!(matches!(
            &loaded.accounts[0].auth,
            ProviderAuth::BaseUrl { url: loaded_url } if loaded_url == url
        ));

        let raw = std::fs::read_to_string(dir.path().join(IDENTITY_FILENAME)).unwrap();
        assert!(raw.contains("type: base_url"), "got: {raw}");
        assert!(raw.contains("url: http://localhost:11434"), "got: {raw}");
        assert!(!raw.contains("encrypted:"), "got: {raw}");
    }

    /// The endpoint and cluster pin stay readable (they are connection
    /// metadata), while the relay credential is encrypted like any other
    /// secret. Both halves are asserted against the bytes on disk, because a
    /// round-trip alone would pass just as happily if the key were plaintext.
    #[test]
    fn an_actual_relay_target_encrypts_only_its_credential_on_disk() {
        let dir = TempDir::new().unwrap();
        let store = identity_store(
            "local-actual",
            vec![configured_account(
                "acc_actual",
                "Actual Relay",
                ApiProvider::Actual,
                ProviderAuth::ActualTarget {
                    kind: ActualTargetKind::Relay,
                    base_url: "https://api.actual.inc".to_string(),
                    api_key: Some("ac_live_supersecret".to_string()),
                    cluster_id: Some("cluster-7".to_string()),
                },
                0,
                1000,
                None,
            )],
            vec![],
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );
        let ProviderAuth::ActualTarget {
            kind,
            base_url,
            api_key,
            cluster_id,
        } = &loaded.accounts[0].auth
        else {
            panic!(
                "expected an Actual target, got {:?}",
                loaded.accounts[0].auth
            );
        };
        assert_eq!(*kind, ActualTargetKind::Relay);
        assert_eq!(base_url, "https://api.actual.inc");
        assert_eq!(api_key.as_deref(), Some("ac_live_supersecret"));
        assert_eq!(cluster_id.as_deref(), Some("cluster-7"));

        let raw = std::fs::read_to_string(dir.path().join(IDENTITY_FILENAME)).unwrap();
        assert!(raw.contains("type: actual_target"), "got: {raw}");
        assert!(
            !raw.contains("ac_live_supersecret"),
            "the relay credential must not be written in plaintext: {raw}"
        );
        assert!(raw.contains("encryptedApiKey:"), "got: {raw}");
        assert!(raw.contains("clusterId: cluster-7"), "got: {raw}");
        // Endpoint and pin are not secrets, and hiding them would make a
        // misconfigured target impossible to diagnose from the file.
        assert!(raw.contains("https://api.actual.inc"), "got: {raw}");
        assert!(raw.contains("cluster-7"), "got: {raw}");
    }

    #[test]
    fn an_actual_local_target_stores_no_credential_field_at_all() {
        let dir = TempDir::new().unwrap();
        let store = identity_store(
            "local-actual-loopback",
            vec![configured_account(
                "acc_actual_local",
                "Studio",
                ApiProvider::Actual,
                ProviderAuth::ActualTarget {
                    kind: ActualTargetKind::Local,
                    base_url: "http://127.0.0.1:8080".to_string(),
                    api_key: None,
                    cluster_id: None,
                },
                0,
                1000,
                None,
            )],
            vec![],
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );
        assert!(matches!(
            &loaded.accounts[0].auth,
            ProviderAuth::ActualTarget {
                kind: ActualTargetKind::Local,
                api_key: None,
                cluster_id: None,
                ..
            }
        ));
        let raw = std::fs::read_to_string(dir.path().join(IDENTITY_FILENAME)).unwrap();
        assert!(!raw.contains("encryptedApiKey"), "got: {raw}");
        assert!(!raw.contains("clusterId"), "got: {raw}");
    }

    #[test]
    fn test_v2_multiple_accounts() {
        let dir = TempDir::new().unwrap();

        let store = identity_store(
            "local-multi",
            vec![
                configured_account(
                    "acc_1",
                    "Personal",
                    ApiProvider::Anthropic,
                    ProviderAuth::ClaudeProfile,
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
    fn legacy_anthropic_logins_are_retired_on_load_and_pruned_from_disk() {
        // The acceptance case for an existing install: a store carrying an
        // ambient CLI row and a setup token loads cleanly, keeps the accounts
        // that are still credentials, and can explain the two that went.
        let dir = TempDir::new().unwrap();
        let machine_id = get_machine_id();
        let encrypted = encrypt_credential("sk-ant-setup-token", &machine_id).unwrap();
        let content = format!(
            r#"# Cairn Identity Store (v2)
version: 2
userId: test-user
gitIdentities: []
accounts:
  - id: local_cli_anthropic
    label: Claude Code
    apiProvider: anthropic
    source: local_cli
    auth:
      type: local_cli
    sortOrder: 0
    createdAt: 0
  - id: acc_token
    label: Max subscription
    apiProvider: anthropic
    source: configured
    auth:
      type: oauth_token
      encrypted: {encrypted}
    sortOrder: 1
    createdAt: 0
  - id: acc_profile
    label: work@example.com
    apiProvider: anthropic
    source: configured
    auth:
      type: claude_profile
    sortOrder: 2
    createdAt: 0
"#
        );
        std::fs::write(dir.path().join(IDENTITY_FILENAME), content).unwrap();

        let store = load_identity_store(dir.path()).unwrap().unwrap();
        assert_eq!(
            store
                .accounts
                .iter()
                .map(|a| a.id.as_str())
                .collect::<Vec<_>>(),
            vec!["acc_profile"]
        );
        assert_eq!(
            store
                .retired_logins
                .iter()
                .map(|r| (r.label.as_str(), r.auth_type.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("Claude Code", "local_cli"),
                ("Max subscription", "oauth_token"),
            ]
        );

        // The pruned store is written back, so the notice survives a restart
        // and the dropped rows do not come back.
        let reloaded = load_identity_store(dir.path()).unwrap().unwrap();
        assert_eq!(reloaded.accounts.len(), 1);
        assert_eq!(reloaded.retired_logins.len(), 2);
        // The retired entries name the shapes they used to have, so look for
        // the account fields specifically rather than the words.
        let raw = std::fs::read_to_string(dir.path().join(IDENTITY_FILENAME)).unwrap();
        assert!(!raw.contains("type: local_cli"), "got: {raw}");
        assert!(!raw.contains("source: local_cli"), "got: {raw}");
        assert!(!raw.contains("type: oauth_token"), "got: {raw}");
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
    fn test_claude_profile_roundtrip() {
        let dir = TempDir::new().unwrap();

        let identity = user_identity(
            "test",
            "test@test.com",
            "Test",
            Some(ClaudeAuth::ConfigDir("/tmp/ignored".into())),
            None,
            None,
        );

        let loaded = roundtrip_saved(
            &dir,
            |path| save_local_identity(path, &identity),
            load_local_identity,
        );

        // The stored shape is the account, not the path: the profile directory
        // is derived from the account id on resolve.
        assert!(matches!(loaded.claude_auth, Some(ClaudeAuth::ConfigDir(_))));
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
    fn a_loaded_store_knows_the_root_it_came_from() {
        // Managed profiles live under this root, so a store that could not name
        // it resolved sessions to a different directory than sign-in wrote.
        let dir = TempDir::new().unwrap();
        let store = identity_store("test", vec![], vec![]);
        save_identity_store(dir.path(), &store).unwrap();

        let loaded = load_identity_store(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.config_dir, dir.path());
    }

    #[test]
    fn retired_logins_survive_a_save_until_they_are_acknowledged() {
        let dir = TempDir::new().unwrap();
        let mut store = identity_store("test", vec![], vec![]);
        store.retired_logins.push(RetiredLogin {
            label: "Max subscription".to_string(),
            auth_type: "oauth_token".to_string(),
            retired_at: 42,
        });

        let loaded = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );
        assert_eq!(loaded.retired_logins, store.retired_logins);

        store.retired_logins.clear();
        let cleared = roundtrip_saved(
            &dir,
            |path| save_identity_store(path, &store),
            load_identity_store,
        );
        assert!(cleared.retired_logins.is_empty());
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
        // An ambient OpenAI row was never a usable credential, so it goes
        // without a notice.
        assert!(store.retired_logins.is_empty());
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
    apiProvider: openai
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
