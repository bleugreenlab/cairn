//! Credential store — file I/O for `~/.cairn/credentials.yaml`.
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
    AccountOverrides, AccountSource, ActualTargetKind, ApiProvider, CredentialStore, CustomHeader,
    CustomModelPolicy, CustomProtocol, CustomSampling, GitIdentity, ProviderAccount, ProviderAuth,
    RetiredLogin,
};
use std::ops::Not;

pub const CREDENTIALS_FILENAME: &str = "credentials.yaml";
const LEGACY_FILENAME: &str = "identity.yaml";
/// Keep the existing scope so migration serializes with credential refresh writers.
const CREDENTIAL_STORE_LEASE: &str = "identity-store";

/// Distinguishes the staged files of concurrent writes within one process.
static STAGE_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn default_custom_max_output_tokens() -> i64 {
    super::DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS
}

// === V1 (legacy) on-disk format ===

/// On-disk representation of identity (v1 format). Detected by absence of `version` field.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityFileV1 {
    pub user_id: String,
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

/// On-disk representation of the credential store (v2 format).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CredentialStoreFile {
    version: u32,
    pub user_id: String,
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
    plan_choice: Option<String>,
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
    // The rest of `credentials.yaml` is camelCase, so these multi-word fields say
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
    /// One named custom provider. Where to reach it, what it speaks, and how
    /// its models are known are plain configuration; the bearer key and every
    /// operator header VALUE are encrypted at rest, because the only reason to
    /// add a header to an inference endpoint is to authenticate to it.
    ///
    /// Header values are sealed together as one JSON blob rather than one
    /// ciphertext each. Encrypting them individually would publish how many
    /// there are and how long each one is, and would let a file editor reorder
    /// or drop one without the seal noticing.
    #[serde(rename = "custom_endpoint", rename_all = "camelCase")]
    CustomEndpoint {
        base_url: String,
        protocol: CustomProtocol,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_api_key: Option<String>,
        /// Header names in order, paired positionally with the decrypted
        /// values. Names are not secret and stay readable so an operator can
        /// see what a provider sends without decrypting anything.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        header_names: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_header_values: Option<String>,
        models: CustomModelPolicy,
        #[serde(default, skip_serializing_if = "CustomSampling::is_empty")]
        sampling: CustomSampling,
        #[serde(default = "default_custom_max_output_tokens")]
        max_output_tokens: i64,
        /// Declared embedding models; not secret.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        embeddings: Vec<crate::CustomEmbeddingModel>,
        /// Not secret: what an image server can draw.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        images: Option<Box<cairn_models_types::image::ImageCapabilities>>,
    },
    #[serde(rename = "local_cli")]
    LocalCli,
    /// A managed Claude account. A row written before Cairn held its tokens
    /// has no `encryptedOauth` and still parses, as "not yet harvested".
    #[serde(rename = "claude_profile", rename_all = "camelCase")]
    ClaudeProfile {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_oauth: Option<String>,
    },
}

/// Intermediate struct to detect format version during load.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VersionProbe {
    #[serde(default)]
    version: Option<u32>,
}

// === Public API ===

/// Presence, including an unreadable or dangling canonical entry. Such an
/// entry must produce an error, never cause fallback to retired credentials.
fn file_entry_exists(path: &Path) -> Result<bool, String> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("Failed to inspect credential store: {error}")),
    }
}

/// Load credential store from `<config_dir>/credentials.yaml`.
///
/// Returns `None` if the file doesn't exist. Automatically migrates v1 → v2.
pub fn load_credential_store(config_dir: &Path) -> Result<Option<CredentialStore>, String> {
    let canonical = config_dir.join(CREDENTIALS_FILENAME);
    if file_entry_exists(&canonical)? {
        let (store, needs_write) = decode_store(config_dir, &canonical)?;
        // Atomic replacement makes ordinary reads safe without a writer lease.
        if !needs_write {
            return Ok(Some(store));
        }
    } else if !file_entry_exists(&config_dir.join(LEGACY_FILENAME))? {
        return Ok(None);
    }
    // Re-read after acquiring the lease: a writer may have rotated a token
    // since the initial inspection. Only the newly read state can be migrated.
    let lease = store_lease(config_dir);
    read_credential_store(config_dir, lease.is_some())
}

/// Load under the writer's existing lease, including filename migration.
fn read_credential_store(
    config_dir: &Path,
    leased: bool,
) -> Result<Option<CredentialStore>, String> {
    let canonical = config_dir.join(CREDENTIALS_FILENAME);
    let legacy = config_dir.join(LEGACY_FILENAME);
    // An existing canonical file is authoritative, including when it is invalid.
    // Never resurrect stale credentials from the old filename after a rotation.
    if file_entry_exists(&canonical)? {
        let (store, needs_write) = decode_store(config_dir, &canonical)?;
        if needs_write {
            if let Err(error) = write_credential_store(config_dir, &store) {
                log::warn!("Failed to persist credential format migration: {error}");
            }
        }
        return Ok(Some(store));
    }
    if !file_entry_exists(&legacy)? {
        return Ok(None);
    }
    if !leased {
        return Err("Cannot migrate identity.yaml without the credential store lease".into());
    }
    // Decode before publishing. A malformed or undecryptable legacy file stays
    // intact; v1 conversion and retired-login pruning are persisted once below.
    let (store, _) = decode_store(config_dir, &legacy)?;
    write_credential_store(config_dir, &store)?;
    // Publication completed atomically before retiring the old source.
    // If removal fails, canonical precedence still prevents stale fallback.
    if let Err(error) = std::fs::remove_file(&legacy) {
        log::warn!("Migrated credentials but could not remove identity.yaml: {error}");
    }
    Ok(Some(store))
}

/// Decode the file and report whether its schema or retired accounts need a write.
/// Parsing never mutates files; callers choose when they hold the writer lease.
fn decode_store(config_dir: &Path, path: &Path) -> Result<(CredentialStore, bool), String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read credential file: {}", e))?;

    let probe: VersionProbe = serde_yaml::from_str(&content)
        .map_err(|e| format!("Failed to parse credential file: {}", e))?;

    let machine_id = get_machine_id();

    match probe.version {
        Some(2) => {
            // V2 format — parse directly
            let file: CredentialStoreFile = serde_yaml::from_str(&content)
                .map_err(|e| format!("Failed to parse v2 credential file: {}", e))?;
            let (mut store, retired) = store_from_v2_file(file, &machine_id)?;
            store.config_dir = config_dir.to_path_buf();
            Ok((store, retired))
        }
        None | Some(1) => {
            // V1 or unversioned — migrate
            let file: IdentityFileV1 = serde_yaml::from_str(&content)
                .map_err(|e| format!("Failed to parse v1 credential file: {}", e))?;
            let mut store = migrate_v1_to_store(file, &machine_id)?;
            store.config_dir = config_dir.to_path_buf();

            Ok((store, true))
        }
        Some(v) => Err(format!("Unknown credential file version: {}", v)),
    }
}

/// Save credential store to `<config_dir>/credentials.yaml` in v2 format.
///
/// Serialized machine-wide against every other writer of this file, because
/// several Cairn processes share one `CAIRN_HOME`.
pub fn save_credential_store(config_dir: &Path, store: &CredentialStore) -> Result<(), String> {
    let _lease = store_lease(config_dir);
    write_credential_store(config_dir, store)
}

/// Read the store, change it, and write it back without leaving a window in
/// which another process can publish between the read and the write.
///
/// This is the only correct way to change one field of the credential store from a
/// process that may be holding a stale copy. Plain `save_credential_store` writes
/// a whole store, so a caller that loaded it minutes ago silently reverts
/// everything another process published since — including a rotating OAuth token
/// whose old value is already spent and unrecoverable.
///
/// The mutator's `Ok` return is handed back alongside the store as it was
/// written, so a caller can publish and learn the published state in one step.
/// An `Err` from the mutator leaves the file untouched.
pub fn update_credential_store<T>(
    config_dir: &Path,
    fallback: Option<CredentialStore>,
    mutate: impl FnOnce(&mut CredentialStore) -> Result<T, String>,
) -> Result<(T, CredentialStore), String> {
    let lease = store_lease(config_dir);
    // The file is authoritative whenever there is one. `fallback` covers the
    // home that has not written one yet, where the caller's copy is all there
    // is and there is nothing newer to have missed.
    let mut store = read_credential_store(config_dir, lease.is_some())?
        .or(fallback)
        .ok_or_else(|| "No credential store to update".to_string())?;
    let outcome = mutate(&mut store)?;
    write_credential_store(config_dir, &store)?;
    Ok((outcome, store))
}

/// Take the machine-wide lease that serializes writers of `credentials.yaml`.
///
/// Best-effort by design: a home where the lock file cannot be created is
/// unusual, and an credential store that refuses to save is worse than one written
/// unserialized. The write itself is atomic either way, so the cost of losing
/// the lease is a lost update, not a corrupt file.
fn store_lease(config_dir: &Path) -> Option<super::lease::CredentialLease> {
    match super::lease::acquire_credential_lease(
        config_dir,
        CREDENTIAL_STORE_LEASE,
        super::lease::DEFAULT_LEASE_WAIT,
    ) {
        Ok(lease) => Some(lease),
        Err(e) => {
            log::warn!("Could not acquire the credential store lease: {e}");
            None
        }
    }
}

/// Write the store without taking the lease. Callers must already hold it.
///
/// Written to a sibling temp file and renamed into place, so a concurrent reader
/// sees either the whole previous store or the whole new one. A reader is not
/// asked to take the lease at all; atomic replacement is what makes that safe.
fn write_credential_store(config_dir: &Path, store: &CredentialStore) -> Result<(), String> {
    let machine_id = get_machine_id();
    let file = store_to_v2_file(store, &machine_id)?;

    // Ensure directory exists
    std::fs::create_dir_all(config_dir)
        .map_err(|e| format!("Failed to create config directory: {}", e))?;

    let yaml =
        serde_yaml::to_string(&file).map_err(|e| format!("Failed to serialize identity: {}", e))?;
    let content = format!("# Cairn Credentials (v2)\n{}", yaml);

    let path = config_dir.join(CREDENTIALS_FILENAME);
    // Unique per write, not merely per process. The lease normally keeps writers
    // apart, but it is best-effort, and two threads sharing one staged path
    // would interleave their bytes and rename the result into place — turning
    // the documented cost of losing the lease from a lost update into a corrupt
    // store.
    let staged = config_dir.join(format!(
        "{CREDENTIALS_FILENAME}.{}.{}.tmp",
        std::process::id(),
        STAGE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::write(&staged, content)
        .map_err(|e| format!("Failed to write credential file: {}", e))?;

    // Set restrictive permissions on Unix, before the file is reachable under
    // its real name rather than after.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o600)) {
            let _ = std::fs::remove_file(&staged);
            return Err(format!("Failed to set permissions: {}", e));
        }
    }

    if let Err(e) = std::fs::rename(&staged, &path) {
        let _ = std::fs::remove_file(&staged);
        return Err(format!("Failed to write credential file: {}", e));
    }

    Ok(())
}

/// Check whether an credential file exists in the config directory.
pub fn credential_store_exists(config_dir: &Path) -> bool {
    config_dir.join(CREDENTIALS_FILENAME).exists() || config_dir.join(LEGACY_FILENAME).exists()
}

fn migrate_v1_to_store(file: IdentityFileV1, machine_id: &str) -> Result<CredentialStore, String> {
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
            plan_choice: None,
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
            plan_choice: None,
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
            plan_choice: None,
            health: None,
        });
    }

    Ok(CredentialStore {
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
        // Stamped by `load_credential_store`, which is the only caller and the
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
    file: CredentialStoreFile,
    machine_id: &str,
) -> Result<(CredentialStore, bool), String> {
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
            AuthFile::CustomEndpoint {
                base_url,
                protocol,
                encrypted_api_key,
                header_names,
                encrypted_header_values,
                models,
                sampling,
                max_output_tokens,
                embeddings,
                images,
            } => {
                let values: Vec<String> = match &encrypted_header_values {
                    Some(sealed) => serde_json::from_str(&decrypt_credential(sealed, machine_id)?)
                        .map_err(|error| format!("custom endpoint headers unreadable: {error}"))?,
                    None => Vec::new(),
                };
                if values.len() != header_names.len() {
                    return Err(format!(
                        "custom endpoint '{base_url}' has {} header names and {} header values",
                        header_names.len(),
                        values.len()
                    ));
                }
                ProviderAuth::CustomEndpoint {
                    base_url,
                    protocol,
                    api_key: encrypted_api_key
                        .map(|encrypted| decrypt_credential(&encrypted, machine_id))
                        .transpose()?,
                    headers: header_names
                        .into_iter()
                        .zip(values)
                        .map(|(name, value)| CustomHeader { name, value })
                        .collect(),
                    models,
                    sampling,
                    max_output_tokens,
                    embeddings,
                    images,
                }
            }
            AuthFile::ClaudeProfile { encrypted_oauth } => ProviderAuth::ClaudeProfile {
                oauth: encrypted_oauth
                    .map(|encrypted| decrypt_credential(&encrypted, machine_id))
                    .transpose()?,
            },
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
            plan_choice: acc_file.plan_choice,
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
        CredentialStore {
            user_id: file.user_id,
            accounts,
            git_identities,
            project_overrides: file.project_overrides,
            retired_logins,
            // Stamped by `load_credential_store`, which knows the root.
            config_dir: std::path::PathBuf::new(),
        },
        migrated,
    ))
}

fn store_to_v2_file(
    store: &CredentialStore,
    machine_id: &str,
) -> Result<CredentialStoreFile, String> {
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
            ProviderAuth::CustomEndpoint {
                base_url,
                protocol,
                api_key,
                headers,
                models,
                sampling,
                max_output_tokens,
                embeddings,
                images,
            } => AuthFile::CustomEndpoint {
                base_url: base_url.clone(),
                protocol: *protocol,
                encrypted_api_key: api_key
                    .as_deref()
                    .map(|value| encrypt_credential(value, machine_id))
                    .transpose()?,
                header_names: headers.iter().map(|header| header.name.clone()).collect(),
                encrypted_header_values: headers
                    .is_empty()
                    .not()
                    .then(|| {
                        let values: Vec<&str> =
                            headers.iter().map(|header| header.value.as_str()).collect();
                        let json = serde_json::to_string(&values).map_err(|error| {
                            format!("custom endpoint headers unserializable: {error}")
                        })?;
                        encrypt_credential(&json, machine_id)
                    })
                    .transpose()?,
                models: models.clone(),
                sampling: sampling.clone(),
                max_output_tokens: *max_output_tokens,
                embeddings: embeddings.clone(),
                images: images.clone(),
            },
            ProviderAuth::ClaudeProfile { oauth } => AuthFile::ClaudeProfile {
                encrypted_oauth: oauth
                    .as_deref()
                    .map(|value| encrypt_credential(value, machine_id))
                    .transpose()?,
            },
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
            plan_choice: account.plan_choice.clone(),
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

    Ok(CredentialStoreFile {
        version: 2,
        user_id: store.user_id.clone(),
        git_identities,
        accounts,
        project_overrides: store.project_overrides.clone(),
        retired_logins: store.retired_logins.clone(),
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
            plan_choice: None,
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

    fn credential_store(
        user_id: &str,
        accounts: Vec<ProviderAccount>,
        git_identities: Vec<GitIdentity>,
    ) -> CredentialStore {
        CredentialStore {
            user_id: user_id.to_string(),
            accounts,
            git_identities,
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::new(),
        }
    }

    // === V2 format tests ===

    #[test]
    fn test_v2_save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();

        let store = credential_store(
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
            |path| save_credential_store(path, &store),
            load_credential_store,
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
        let store = credential_store(
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
            |path| save_credential_store(path, &store),
            load_credential_store,
        );
        assert!(matches!(
            &loaded.accounts[0].auth,
            ProviderAuth::BaseUrl { url: loaded_url } if loaded_url == url
        ));

        let raw = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
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
        let store = credential_store(
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
            |path| save_credential_store(path, &store),
            load_credential_store,
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

        let raw = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
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
        let store = credential_store(
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
            |path| save_credential_store(path, &store),
            load_credential_store,
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
        let raw = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
        assert!(!raw.contains("encryptedApiKey"), "got: {raw}");
        assert!(!raw.contains("clusterId"), "got: {raw}");
    }

    #[test]
    fn test_v2_multiple_accounts() {
        let dir = TempDir::new().unwrap();

        let store = credential_store(
            "local-multi",
            vec![
                configured_account(
                    "acc_1",
                    "Personal",
                    ApiProvider::Anthropic,
                    ProviderAuth::ClaudeProfile { oauth: None },
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
            |path| save_credential_store(path, &store),
            load_credential_store,
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

        let store = credential_store(
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

        save_credential_store(dir.path(), &store).unwrap();

        let raw = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
        assert!(!raw.contains(secret), "Secret should be encrypted on disk");
        assert!(raw.contains("version: 2"), "Should be v2 format");
    }

    #[test]
    fn a_claude_profile_row_without_tokens_still_loads_as_unharvested() {
        let dir = TempDir::new().unwrap();
        let content = r#"version: 2
userId: test-user
gitIdentities: []
accounts:
  - id: acc_profile
    label: work@example.com
    apiProvider: anthropic
    source: configured
    auth:
      type: claude_profile
    sortOrder: 0
    createdAt: 0
"#;
        std::fs::write(dir.path().join(CREDENTIALS_FILENAME), content).unwrap();
        let store = load_credential_store(dir.path()).unwrap().unwrap();
        assert!(matches!(
            store.accounts[0].auth,
            ProviderAuth::ClaudeProfile { oauth: None }
        ));
    }

    #[test]
    fn claude_profile_tokens_are_encrypted_on_disk_and_round_trip() {
        let dir = TempDir::new().unwrap();
        let oauth = r#"{"accessToken":"at-SENTINEL","refreshToken":"rt-SENTINEL"}"#;
        let mut store = credential_store("test", vec![], vec![]);
        store.accounts.push(ProviderAccount {
            id: "acc_profile".into(),
            label: "Claude".into(),
            api_provider: ApiProvider::Anthropic,
            source: AccountSource::Configured,
            auth: ProviderAuth::ClaudeProfile {
                oauth: Some(oauth.into()),
            },
            project_id: None,
            sort_order: 0,
            created_at: 0,
            last_used_at: None,
            email: None,
            plan: None,
            plan_choice: None,
            health: None,
        });
        save_credential_store(dir.path(), &store).unwrap();

        let raw = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
        assert!(raw.contains("type: claude_profile"), "{raw}");
        assert!(raw.contains("encryptedOauth"), "{raw}");
        assert!(!raw.contains("SENTINEL"), "tokens must be encrypted: {raw}");

        let loaded = load_credential_store(dir.path()).unwrap().unwrap();
        assert!(matches!(
            &loaded.accounts[0].auth,
            ProviderAuth::ClaudeProfile { oauth: Some(json) } if json == oauth
        ));
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
            r#"# Cairn Credentials (v2)
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
        std::fs::write(dir.path().join(CREDENTIALS_FILENAME), content).unwrap();

        let store = load_credential_store(dir.path()).unwrap().unwrap();
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
        let reloaded = load_credential_store(dir.path()).unwrap().unwrap();
        assert_eq!(reloaded.accounts.len(), 1);
        assert_eq!(reloaded.retired_logins.len(), 2);
        // The retired entries name the shapes they used to have, so look for
        // the account fields specifically rather than the words.
        let raw = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
        assert!(!raw.contains("type: local_cli"), "got: {raw}");
        assert!(!raw.contains("source: local_cli"), "got: {raw}");
        assert!(!raw.contains("type: oauth_token"), "got: {raw}");
    }

    // === V1 migration tests ===

    // === Backward-compatible API tests ===

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

        let mut store = credential_store(
            "test",
            vec![],
            vec![git_identity("gi_1", "Default", "Test", "test@test.com", 0)],
        );
        store.project_overrides = overrides_map;

        let loaded = roundtrip_saved(
            &dir,
            |path| save_credential_store(path, &store),
            load_credential_store,
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
        let store = credential_store("test", vec![], vec![]);
        save_credential_store(dir.path(), &store).unwrap();

        let loaded = load_credential_store(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.config_dir, dir.path());
    }

    #[test]
    fn retired_logins_survive_a_save_until_they_are_acknowledged() {
        let dir = TempDir::new().unwrap();
        let mut store = credential_store("test", vec![], vec![]);
        store.retired_logins.push(RetiredLogin {
            label: "Max subscription".to_string(),
            auth_type: "oauth_token".to_string(),
            retired_at: 42,
        });

        let loaded = roundtrip_saved(
            &dir,
            |path| save_credential_store(path, &store),
            load_credential_store,
        );
        assert_eq!(loaded.retired_logins, store.retired_logins);

        store.retired_logins.clear();
        let cleared = roundtrip_saved(
            &dir,
            |path| save_credential_store(path, &store),
            load_credential_store,
        );
        assert!(cleared.retired_logins.is_empty());
    }

    #[test]
    fn test_unknown_version_returns_error() {
        let dir = TempDir::new().unwrap();
        let content = "version: 99\nuserId: test\n";
        std::fs::write(dir.path().join(CREDENTIALS_FILENAME), content).unwrap();

        let result = load_credential_store(dir.path());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Unknown credential file version: 99"));
    }

    #[test]
    fn an_image_endpoint_keeps_its_declared_capabilities_on_disk() {
        let declared = cairn_models_types::image::ImageCapabilities::openai_gpt_image();
        let auth = ProviderAuth::custom_endpoint(
            "https://api.openai.com/v1",
            CustomProtocol::OpenAiImages,
            Some("sk-secret-value"),
            &[],
            CustomModelPolicy::Explicit {
                ids: vec!["gpt-image-2".into()],
            },
            CustomSampling::default(),
            crate::DEFAULT_CUSTOM_MAX_OUTPUT_TOKENS,
        )
        .unwrap()
        .with_image_capabilities(declared.clone());
        let store = CredentialStore {
            user_id: "local-1".to_string(),
            accounts: vec![configured_account(
                "acc_openai",
                "OpenAI Images",
                ApiProvider::Custom,
                auth,
                0,
                0,
                None,
            )],
            git_identities: vec![],
            project_overrides: Default::default(),
            retired_logins: vec![],
            config_dir: std::path::PathBuf::new(),
        };
        let yaml = serde_yaml::to_string(&store_to_v2_file(&store, "m").unwrap()).unwrap();
        assert!(yaml.contains("openai-images"), "{yaml}");
        let (restored, _) = store_from_v2_file(serde_yaml::from_str(&yaml).unwrap(), "m").unwrap();
        assert_eq!(
            restored.accounts[0].auth.image_capabilities(),
            Some(declared)
        );
    }

    #[test]
    fn a_custom_endpoint_round_trips_with_its_secrets_sealed() {
        let machine = "test-machine";
        let auth = ProviderAuth::custom_endpoint(
            "http://10.10.10.83:8000",
            CustomProtocol::OpenAiChatCompletions,
            Some("sk-secret-value"),
            &[
                ("x-tenant".to_string(), "tenant-secret".to_string()),
                ("x-route".to_string(), "route-secret".to_string()),
            ],
            CustomModelPolicy::Explicit {
                ids: vec!["qwen3.8-27b".into()],
            },
            CustomSampling {
                repetition_penalty: Some(1.1),
                ..Default::default()
            },
            65_536,
        )
        .unwrap()
        .with_embedding_models(vec![crate::CustomEmbeddingModel {
            id: "qwen/qwen3-embedding-4b".into(),
            dims: 2560,
            widths: Default::default(),
        }])
        .unwrap();
        let account = configured_account(
            "acc_spark",
            "DGX Spark",
            ApiProvider::Custom,
            auth,
            0,
            0,
            None,
        );
        let store = CredentialStore {
            user_id: "local-1".to_string(),
            accounts: vec![account],
            git_identities: vec![],
            project_overrides: Default::default(),
            retired_logins: vec![],
            config_dir: std::path::PathBuf::new(),
        };

        let file = store_to_v2_file(&store, machine).unwrap();
        let yaml = serde_yaml::to_string(&file).unwrap();
        // Nothing secret is legible on disk; the header NAMES are, so an
        // operator can see what a provider sends without decrypting anything.
        assert!(!yaml.contains("sk-secret-value"), "{yaml}");
        assert!(!yaml.contains("tenant-secret"), "{yaml}");
        assert!(!yaml.contains("route-secret"), "{yaml}");
        assert!(yaml.contains("x-tenant"), "{yaml}");
        assert!(yaml.contains("http://10.10.10.83:8000/v1"), "{yaml}");

        let parsed: CredentialStoreFile = serde_yaml::from_str(&yaml).unwrap();
        let (restored, _) = store_from_v2_file(parsed, machine).unwrap();
        let restored = &restored.accounts[0];
        assert_eq!(restored.label, "DGX Spark");
        assert_eq!(restored.api_provider, ApiProvider::Custom);
        let ProviderAuth::CustomEndpoint {
            base_url,
            protocol,
            api_key,
            headers,
            models,
            sampling,
            max_output_tokens,
            embeddings,
            images,
        } = &restored.auth
        else {
            panic!("expected a custom endpoint");
        };
        // Declared embedding models are not secret, and survive the round trip.
        assert!(yaml.contains("qwen/qwen3-embedding-4b"), "{yaml}");
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].dims, 2560);
        assert_eq!(*images, None);
        assert_eq!(base_url, "http://10.10.10.83:8000/v1");
        assert_eq!(*protocol, CustomProtocol::OpenAiChatCompletions);
        assert_eq!(api_key.as_deref(), Some("sk-secret-value"));
        assert_eq!(
            headers,
            &[
                CustomHeader {
                    name: "x-tenant".into(),
                    value: "tenant-secret".into()
                },
                CustomHeader {
                    name: "x-route".into(),
                    value: "route-secret".into()
                },
            ]
        );
        assert_eq!(
            models,
            &CustomModelPolicy::Explicit {
                ids: vec!["qwen3.8-27b".into()]
            }
        );
        assert_eq!(sampling.repetition_penalty, Some(1.1));
        assert_eq!(*max_output_tokens, 65_536);
    }

    #[test]
    fn a_custom_endpoint_with_no_credentials_stores_none() {
        let machine = "test-machine";
        let account = configured_account(
            "acc_open",
            "Laptop",
            ApiProvider::Custom,
            ProviderAuth::custom_endpoint(
                "http://localhost:8000/v1",
                CustomProtocol::OpenAiChatCompletions,
                None,
                &[],
                CustomModelPolicy::Discover,
                CustomSampling::default(),
                super::default_custom_max_output_tokens(),
            )
            .unwrap(),
            0,
            0,
            None,
        );
        let store = CredentialStore {
            user_id: "local-1".to_string(),
            accounts: vec![account],
            git_identities: vec![],
            project_overrides: Default::default(),
            retired_logins: vec![],
            config_dir: std::path::PathBuf::new(),
        };
        let yaml = serde_yaml::to_string(&store_to_v2_file(&store, machine).unwrap()).unwrap();
        assert!(!yaml.contains("encryptedApiKey"), "{yaml}");
        assert!(!yaml.contains("encryptedHeaderValues"), "{yaml}");
        let parsed: CredentialStoreFile = serde_yaml::from_str(&yaml).unwrap();
        let (restored, _) = store_from_v2_file(parsed, machine).unwrap();
        assert!(matches!(
            &restored.accounts[0].auth,
            ProviderAuth::CustomEndpoint { api_key: None, headers, models: CustomModelPolicy::Discover, .. }
                if headers.is_empty()
        ));
    }

    #[test]
    fn a_tampered_custom_endpoint_refuses_to_load() {
        // Header names are readable on disk and their values are sealed as one
        // blob. Adding a name by hand therefore desynchronizes the pair, and a
        // provider that silently sent the wrong header value would be worse
        // than one that refuses to load.
        let machine = "test-machine";
        let account = configured_account(
            "acc_spark",
            "DGX Spark",
            ApiProvider::Custom,
            ProviderAuth::custom_endpoint(
                "http://host:8000/v1",
                CustomProtocol::OpenAiChatCompletions,
                None,
                &[("x-tenant".to_string(), "tenant-secret".to_string())],
                CustomModelPolicy::Discover,
                CustomSampling::default(),
                super::default_custom_max_output_tokens(),
            )
            .unwrap(),
            0,
            0,
            None,
        );
        let store = CredentialStore {
            user_id: "local-1".to_string(),
            accounts: vec![account],
            git_identities: vec![],
            project_overrides: Default::default(),
            retired_logins: vec![],
            config_dir: std::path::PathBuf::new(),
        };
        let mut file = store_to_v2_file(&store, machine).unwrap();
        if let AuthFile::CustomEndpoint { header_names, .. } = &mut file.accounts[0].auth {
            header_names.push("x-injected".to_string());
        }
        let error = store_from_v2_file(file, machine).expect_err("mismatched pairing");
        assert!(error.contains("header names"), "{error}");
    }

    #[test]
    fn test_v2_openai_local_cli_accounts_are_ignored_on_load() {
        let dir = TempDir::new().unwrap();
        let content = r#"# Cairn Credentials (v2)
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
        std::fs::write(dir.path().join(CREDENTIALS_FILENAME), content).unwrap();

        let store = load_credential_store(dir.path()).unwrap().unwrap();
        assert!(store.accounts.is_empty());
        // An ambient OpenAI row was never a usable credential, so it goes
        // without a notice.
        assert!(store.retired_logins.is_empty());
    }

    #[test]
    fn test_load_credential_store_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = load_credential_store(dir.path()).unwrap();
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
            r#"# Cairn Credentials (v2)
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
        std::fs::write(dir.path().join(CREDENTIALS_FILENAME), content).unwrap();

        // Should parse without error (alias handling)
        let store = load_credential_store(dir.path())
            .expect("Failed to parse o_auth_token alias")
            .expect("Store should not be None");
        assert_eq!(store.accounts.len(), 1);
        match &store.accounts[0].auth {
            ProviderAuth::OAuthToken { value } => assert_eq!(value, "my-oauth-token"),
            other => panic!("Expected OAuthToken, got {:?}", other),
        }
    }
    #[test]
    fn test_v1_migration_preserves_credentials() {
        let dir = TempDir::new().unwrap();

        // Write v1 format directly
        let machine_id = get_machine_id();
        let v1_file = IdentityFileV1 {
            user_id: "local-v1-user".into(),
            email: "v1@example.com".into(),
            name: "V1 User".into(),
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
        std::fs::write(dir.path().join(CREDENTIALS_FILENAME), content).unwrap();

        // Load should migrate to v2
        let store = load_credential_store(dir.path()).unwrap().unwrap();

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
        let raw = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
        assert!(raw.contains("version: 2"));
    }
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    use tempfile::TempDir;

    fn legacy_v2(dir: &Path, token: &str) -> Vec<u8> {
        let encrypted = encrypt_credential(token, &get_machine_id()).unwrap();
        let yaml = format!(
            r#"version: 2
userId: local-stable
gitIdentities:
  - id: gi-stable
    label: Work
    name: Author
    email: author@example.com
    sortOrder: 2
accounts:
  - id: account-stable
    label: Work
    apiProvider: openai
    source: configured
    auth:
      type: oauth_token
      encrypted: {encrypted}
    projectId: project-stable
    sortOrder: 3
    createdAt: 100
projectOverrides:
  project-stable:
    openaiAccountId: account-stable
"#
        );
        std::fs::write(dir.join(LEGACY_FILENAME), &yaml).unwrap();
        yaml.into_bytes()
    }

    #[test]
    fn migrates_existing_accounts_and_attribution_without_rekeying_or_reidentifying() {
        let dir = TempDir::new().unwrap();
        legacy_v2(dir.path(), "rotating-oauth-secret");
        let store = load_credential_store(dir.path()).unwrap().unwrap();
        assert_eq!(store.user_id, "local-stable");
        assert_eq!(store.git_identities[0].email, "author@example.com");
        assert_eq!(store.accounts[0].id, "account-stable");
        assert_eq!(
            store.accounts[0].project_id.as_deref(),
            Some("project-stable")
        );
        assert_eq!(
            store.project_overrides["project-stable"]
                .openai_account_id
                .as_deref(),
            Some("account-stable")
        );
        assert_eq!(
            store.accounts[0].auth.credential_value(),
            Some("rotating-oauth-secret")
        );
        assert!(!dir.path().join(LEGACY_FILENAME).exists());
        let bytes = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
        assert!(!bytes.contains("rotating-oauth-secret"));
        let reloaded = load_credential_store(dir.path()).unwrap().unwrap();
        assert_eq!(reloaded.accounts[0].id, "account-stable");
        assert_eq!(
            reloaded.accounts[0].auth.credential_value(),
            Some("rotating-oauth-secret")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.path().join(CREDENTIALS_FILENAME))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn migrates_v1_filename_and_format_together() {
        let dir = TempDir::new().unwrap();
        let encrypted = encrypt_credential("legacy-openai-key", &get_machine_id()).unwrap();
        std::fs::write(
            dir.path().join(LEGACY_FILENAME),
            format!(
                r#"userId: v1-user
name: Original Author
email: original@example.com
codexAuth:
  type: api_key
  encrypted: {encrypted}
"#
            ),
        )
        .unwrap();
        let store = load_credential_store(dir.path()).unwrap().unwrap();
        assert_eq!(store.user_id, "v1-user");
        assert_eq!(store.git_identities[0].name, "Original Author");
        assert_eq!(
            store.resolve_auth(None, None).codex_auth.unwrap().value(),
            "legacy-openai-key"
        );
        assert!(!dir.path().join(LEGACY_FILENAME).exists());
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            &std::fs::read_to_string(dir.path().join(CREDENTIALS_FILENAME)).unwrap(),
        )
        .unwrap();
        assert_eq!(yaml["version"].as_u64(), Some(2));
    }

    #[test]
    fn canonical_store_wins_over_a_stale_legacy_token() {
        let dir = TempDir::new().unwrap();
        legacy_v2(dir.path(), "old-token");
        let mut store = load_credential_store(dir.path()).unwrap().unwrap();
        store.accounts[0].auth = ProviderAuth::OAuthToken {
            value: "new-token".into(),
        };
        save_credential_store(dir.path(), &store).unwrap();
        legacy_v2(dir.path(), "old-token");
        assert_eq!(
            load_credential_store(dir.path()).unwrap().unwrap().accounts[0]
                .auth
                .credential_value(),
            Some("new-token")
        );
    }

    #[test]
    fn invalid_canonical_store_does_not_resurrect_legacy_credentials() {
        let dir = TempDir::new().unwrap();
        let original = legacy_v2(dir.path(), "spent-token");
        std::fs::write(dir.path().join(CREDENTIALS_FILENAME), "version: 999\n").unwrap();
        assert!(load_credential_store(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(LEGACY_FILENAME)).unwrap(),
            original
        );
    }

    #[test]
    fn invalid_legacy_store_is_left_intact() {
        let dir = TempDir::new().unwrap();
        let original = b"version: 999\n";
        std::fs::write(dir.path().join(LEGACY_FILENAME), original).unwrap();
        assert!(load_credential_store(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(LEGACY_FILENAME)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILENAME).exists());
    }

    #[test]
    fn migration_and_updates_share_one_store_lease() {
        let dir = TempDir::new().unwrap();
        legacy_v2(dir.path(), "before-refresh");
        let mut threads = Vec::new();
        for index in 0..8 {
            let path = dir.path().to_path_buf();
            threads.push(std::thread::spawn(move || {
                update_credential_store(&path, None, |store| {
                    store
                        .project_overrides
                        .insert(format!("project-{index}"), AccountOverrides::default());
                    Ok(())
                })
                .unwrap();
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let store = load_credential_store(dir.path()).unwrap().unwrap();
        assert_eq!(store.project_overrides.len(), 9);
        assert!(!dir.path().join(LEGACY_FILENAME).exists());
        assert_eq!(
            store.accounts[0].auth.credential_value(),
            Some("before-refresh")
        );
    }

    #[test]
    fn failed_mutation_does_not_replace_the_published_token() {
        let dir = TempDir::new().unwrap();
        legacy_v2(dir.path(), "current-token");
        load_credential_store(dir.path()).unwrap();
        let before = std::fs::read(dir.path().join(CREDENTIALS_FILENAME)).unwrap();
        let result: Result<((), CredentialStore), String> =
            update_credential_store(dir.path(), None, |store| {
                store.accounts.clear();
                Err("reject mutation".into())
            });
        assert!(result.is_err());
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILENAME)).unwrap(),
            before
        );
    }
    #[test]
    fn migration_without_a_lease_does_not_publish_or_remove_the_source() {
        let dir = TempDir::new().unwrap();
        let before = legacy_v2(dir.path(), "current-token");
        assert!(read_credential_store(dir.path(), false).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(LEGACY_FILENAME)).unwrap(),
            before
        );
        assert!(!dir.path().join(CREDENTIALS_FILENAME).exists());
    }

    #[cfg(unix)]
    #[test]
    fn dangling_canonical_entry_does_not_fall_back_to_a_legacy_token() {
        let dir = TempDir::new().unwrap();
        let before = legacy_v2(dir.path(), "spent-token");
        std::os::unix::fs::symlink("missing-store", dir.path().join(CREDENTIALS_FILENAME)).unwrap();
        assert!(load_credential_store(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(LEGACY_FILENAME)).unwrap(),
            before
        );
    }

    #[test]
    fn ordinary_reads_do_not_wait_for_a_writer_lease() {
        let dir = TempDir::new().unwrap();
        legacy_v2(dir.path(), "published-token");
        load_credential_store(dir.path()).unwrap();
        let lease = store_lease(dir.path()).unwrap();
        let path = dir.path().to_path_buf();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            tx.send(load_credential_store(&path)).unwrap();
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(2));
        // Release before asserting, so a regressed reader can finish and join.
        drop(lease);
        reader.join().unwrap();
        let store = result
            .expect("read must complete while the writer lease is held")
            .unwrap()
            .unwrap();
        assert_eq!(
            store.accounts[0].auth.credential_value(),
            Some("published-token")
        );
    }
}
