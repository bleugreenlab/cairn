//! GitHub credential management — DB operations with transparent at-rest
//! encryption.
//!
//! `private_key`, `webhook_secret`, and `relay_secret` are encrypted at rest
//! (see [`crypto`]). [`get_github_credentials`] returns them decrypted, and
//! [`update_github_credentials`] re-encrypts on write, so callers always work
//! with plaintext. Legacy plaintext values migrate to ciphertext on the next
//! write. The `relay_private_key_encrypted` field is encrypted separately by the
//! relay manager and is stored as-is here.

use super::crypto;
use crate::storage::{LocalDb, RowExt};
use turso::params;

/// GitHub App credentials needed for API auth.
#[derive(Debug, Clone)]
pub struct GitHubAppCredentials {
    pub app_id: i64,
    pub private_key: String,
    pub installation_id: i64,
}

/// GitHub credentials stored in DB.
#[derive(Debug, Clone, Default)]
pub struct GitHubCredentials {
    pub app_id: Option<i64>,
    pub app_name: Option<String>,
    pub app_slug: Option<String>,
    pub private_key: Option<String>,
    pub webhook_secret: Option<String>,
    pub installation_id: Option<i64>,
    pub relay_channel_id: Option<String>,
    pub relay_secret: Option<String>,
    pub last_event_sync: Option<String>,
    pub relay_public_key: Option<String>,
    pub relay_private_key_encrypted: Option<String>,
}

/// Get GitHub credentials from DB.
pub async fn get_github_credentials(db: &LocalDb) -> Result<GitHubCredentials, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT app_id, app_name, app_slug, private_key, webhook_secret,
                            installation_id, relay_channel_id, relay_secret, last_event_sync,
                            relay_public_key, relay_private_key_encrypted
                     FROM github_app
                     WHERE id = 'default'",
                    (),
                )
                .await?;

            let Some(row) = rows.next().await? else {
                return Ok(GitHubCredentials::default());
            };

            Ok(GitHubCredentials {
                app_id: row.opt_i64(0)?,
                app_name: row.opt_text(1)?,
                app_slug: row.opt_text(2)?,
                private_key: row.opt_text(3)?,
                webhook_secret: row.opt_text(4)?,
                installation_id: row.opt_i64(5)?,
                relay_channel_id: row.opt_text(6)?,
                relay_secret: row.opt_text(7)?,
                last_event_sync: row.opt_text(8)?,
                relay_public_key: row.opt_text(9)?,
                relay_private_key_encrypted: row.opt_text(10)?,
            })
        })
    })
    .await
    .map(|mut creds| {
        decrypt_at_rest_fields(&mut creds);
        creds
    })
    .map_err(|e| e.to_string())
}

/// Decrypt the at-rest fields in place. A field that is in our ciphertext format
/// but fails to decrypt (wrong machine or tampering) is treated as unusable and
/// cleared to `None` so callers report it as unconfigured rather than handing
/// out ciphertext. Legacy plaintext passes through unchanged.
fn decrypt_at_rest_fields(creds: &mut GitHubCredentials) {
    let machine_id = crypto::get_machine_id();
    creds.private_key = decrypt_field(
        creds.private_key.take(),
        &machine_id,
        crypto::APP_PRIVATE_KEY_DOMAIN,
        "private_key",
    );
    creds.webhook_secret = decrypt_field(
        creds.webhook_secret.take(),
        &machine_id,
        crypto::WEBHOOK_SECRET_DOMAIN,
        "webhook_secret",
    );
    creds.relay_secret = decrypt_field(
        creds.relay_secret.take(),
        &machine_id,
        crypto::RELAY_SECRET_DOMAIN,
        "relay_secret",
    );
}

fn decrypt_field(
    value: Option<String>,
    machine_id: &str,
    domain: &[u8],
    label: &str,
) -> Option<String> {
    let value = value?;
    match crypto::decrypt_at_rest(&value, machine_id, domain) {
        Ok(plaintext) => Some(plaintext),
        Err(e) => {
            log::warn!("Failed to decrypt github_app.{label}: {e}");
            None
        }
    }
}

fn encrypt_field(
    value: Option<&str>,
    machine_id: &str,
    domain: &[u8],
) -> Result<Option<String>, String> {
    value
        .map(|v| crypto::encrypt_at_rest(v, machine_id, domain))
        .transpose()
}

/// Read-modify-write the single `github_app` row, encrypting the at-rest fields.
///
/// `update_fn` operates on the decrypted (plaintext) credentials. On write,
/// `private_key`, `webhook_secret`, and `relay_secret` are re-encrypted, which
/// also migrates any legacy plaintext values to ciphertext.
pub async fn update_github_credentials<F>(db: &LocalDb, update_fn: F) -> Result<(), String>
where
    F: FnOnce(&mut GitHubCredentials),
{
    let mut creds = get_github_credentials(db).await?;
    update_fn(&mut creds);

    let machine_id = crypto::get_machine_id();
    let private_key = encrypt_field(
        creds.private_key.as_deref(),
        &machine_id,
        crypto::APP_PRIVATE_KEY_DOMAIN,
    )?;
    let webhook_secret = encrypt_field(
        creds.webhook_secret.as_deref(),
        &machine_id,
        crypto::WEBHOOK_SECRET_DOMAIN,
    )?;
    let relay_secret = encrypt_field(
        creds.relay_secret.as_deref(),
        &machine_id,
        crypto::RELAY_SECRET_DOMAIN,
    )?;

    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let creds = creds.clone();
        let private_key = private_key.clone();
        let webhook_secret = webhook_secret.clone();
        let relay_secret = relay_secret.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO github_app (
                    id, app_id, app_name, app_slug, private_key, webhook_secret,
                    installation_id, relay_channel_id, relay_secret, last_event_sync,
                    created_at, updated_at, relay_public_key, relay_private_key_encrypted
                 )
                 VALUES ('default', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, ?12)
                 ON CONFLICT(id) DO UPDATE SET
                    app_id = excluded.app_id,
                    app_name = excluded.app_name,
                    app_slug = excluded.app_slug,
                    private_key = excluded.private_key,
                    webhook_secret = excluded.webhook_secret,
                    installation_id = excluded.installation_id,
                    relay_channel_id = excluded.relay_channel_id,
                    relay_secret = excluded.relay_secret,
                    last_event_sync = excluded.last_event_sync,
                    updated_at = excluded.updated_at,
                    relay_public_key = excluded.relay_public_key,
                    relay_private_key_encrypted = excluded.relay_private_key_encrypted",
                params![
                    creds.app_id,
                    creds.app_name,
                    creds.app_slug,
                    private_key,
                    webhook_secret,
                    creds.installation_id,
                    creds.relay_channel_id,
                    relay_secret,
                    creds.last_event_sync,
                    now,
                    creds.relay_public_key,
                    creds.relay_private_key_encrypted,
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Get installation ID for a repository owner (user or org).
pub async fn get_installation_for_owner(db: &LocalDb, owner: &str) -> Result<Option<i64>, String> {
    let owner = owner.to_string();
    db.read(|conn| {
        let owner = owner.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT installation_id
                     FROM github_installations
                     WHERE account_login = ?1",
                    params![owner.as_str()],
                )
                .await?;
            crate::storage::next_i64(&mut rows, 0).await
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Get GitHub App credentials for a specific repository owner.
///
/// Looks up the installation by owner first, falls back to default installation_id.
pub async fn get_credentials_for_owner(
    db: &LocalDb,
    owner: &str,
) -> Result<GitHubAppCredentials, String> {
    let creds = get_github_credentials(db).await?;

    let app_id = creds.app_id.ok_or("GitHub App ID not configured")?;
    let private_key = creds
        .private_key
        .ok_or("GitHub App private key not configured")?;

    let installation_id = get_installation_for_owner(db, owner)
        .await?
        .or(creds.installation_id)
        .ok_or_else(|| {
            format!(
                "GitHub App not installed for '{}'. Install the app on this account/org.",
                owner
            )
        })?;

    Ok(GitHubAppCredentials {
        app_id,
        private_key,
        installation_id,
    })
}

/// Get owner/repo from a repository path using git remote.
pub fn get_owner_repo(repo_path: &str) -> Result<(String, String), String> {
    let remote_url = super::api::get_repo_remote(repo_path)?;
    super::api::parse_repo_from_url(&remote_url)
}
