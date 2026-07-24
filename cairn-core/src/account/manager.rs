//! AccountManager — manages the desktop's connection to cairn.computer.
//!
//! Handles device code auth flow, JWT storage/refresh, and connection lifecycle.
//! Replaces TeamManager for the single-account model.

use std::{future::Future, sync::Arc};
use tokio::sync::{watch, Mutex};

use super::jwt::{decrypt_jwt_from_storage, encrypt_jwt_for_storage};
use super::org_tokens::OrgTokenCache;
use crate::db::DbState;
use crate::services::EventEmitter;
use crate::storage::{DbError, RowExt};

use super::connection::{canonical_plan, AccountConnection, DbAccount, OrgMembership};

const CHECK_INTERVAL_SECS: u64 = 30 * 60; // 30 minutes
                                          // Must exceed CHECK_INTERVAL_SECS: with a 1-hour token, the T+30 tick then sees
                                          // ~30 min of validity left (below this threshold) and refreshes while the token
                                          // is still good, instead of the T+60 tick refreshing an already-expired token
                                          // and leaning on the API's short post-expiry grace window.
const REFRESH_THRESHOLD_SECS: i64 = 45 * 60; // 45 minutes before expiry

/// Manages the desktop's connection to cairn.computer.
pub struct AccountManager {
    db: Arc<DbState>,
    emitter: Arc<dyn EventEmitter>,
    refresh_handle: Mutex<Option<RefreshHandle>>,
    org_token_cache: Mutex<OrgTokenCache>,
    api_config: crate::api::ApiConfig,
}

struct RefreshHandle {
    cancel_tx: watch::Sender<bool>,
}

impl Drop for RefreshHandle {
    fn drop(&mut self) {
        let _ = self.cancel_tx.send(true);
    }
}

impl AccountManager {
    pub(crate) fn new(db: Arc<DbState>, emitter: Arc<dyn EventEmitter>) -> Self {
        Self {
            db,
            emitter,
            refresh_handle: Mutex::new(None),
            org_token_cache: Mutex::new(OrgTokenCache::new()),
            api_config: crate::api::ApiConfig::default(),
        }
    }

    /// Connect with a JWT received via deep link callback.
    /// The server registers the device and issues the JWT; the deep link
    /// passes back device_id, plan, and user info.
    pub async fn connect_with_jwt(
        &self,
        jwt: &str,
        device_id: &str,
        plan: &str,
        user_name: &str,
        user_email: &str,
        orgs_json: Option<&str>,
    ) -> Result<AccountConnection, String> {
        // Clear any cached org tokens from a previous session/user
        self.org_token_cache.lock().await.clear();

        let claims = super::jwt::decode_jwt_claims(jwt).or_else(|_| {
            // Device JWTs may not have org_id/org_role — parse manually
            decode_device_jwt_claims(jwt)
        })?;

        let org_memberships =
            orgs_json.and_then(|s| serde_json::from_str::<Vec<OrgMembership>>(s).ok());

        let encrypted_jwt = encrypt_jwt_for_storage(jwt)?;
        let now = chrono::Utc::now().timestamp() as i32;

        // Canonicalize the deep-link plan at the write boundary so a malformed or
        // legacy callback (or a direct invocation with a retired literal) never
        // persists `pro`/`remote` into SQLite or returns it in the connection
        // result/event. Mirrors the read-boundary guard in AccountConnection::from.
        let plan = canonical_plan(plan);

        let db_account = DbAccount {
            user_id: claims.sub.clone(),
            email: user_email.to_string(),
            name: user_name.to_string(),
            device_id: device_id.to_string(),
            plan: plan.clone(),
            jwt_encrypted: Some(encrypted_jwt),
            jwt_expires_at: Some(claims.exp as i32),
            org_memberships: org_memberships
                .as_ref()
                .and_then(|m| serde_json::to_string(m).ok()),
            connected_at: now,
            updated_at: now,
        };

        upsert_account(&self.db, db_account).await?;
        self.db.set_team_sync_authorized(true).await;

        let connection = AccountConnection {
            user_id: claims.sub,
            email: user_email.to_string(),
            name: user_name.to_string(),
            device_id: device_id.to_string(),
            plan,
            org_memberships: org_memberships.unwrap_or_default(),
            connected_at: now as i64,
        };

        let _ = self.emitter.emit(
            "db-change",
            serde_json::json!({"table": "account", "action": "upsert"}),
        );

        // Start JWT refresh
        self.start_refresh().await;

        Ok(connection)
    }

    /// Disconnect from the account. Deactivates device on server, stops refresh, removes from DB.
    pub async fn disconnect(&self) -> Result<(), String> {
        // Stop refresh
        let mut handle = self.refresh_handle.lock().await;
        *handle = None;

        // Attempt API deactivation (best-effort — local state clears regardless)
        if let Ok(Some(jwt)) = get_jwt_from_db(&self.db).await {
            if let Ok(Some(connection)) = get_account_connection(&self.db).await {
                let url = self.api_config.device_url(&connection.device_id);
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(5))
                    .build()
                    .ok();

                if let Some(client) = client {
                    match client.delete(&url).bearer_auth(&jwt).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            log::info!("Device deactivated on server: {}", connection.device_id);
                        }
                        Ok(resp) => {
                            log::warn!(
                                "Device deactivation returned {}: {}",
                                resp.status(),
                                resp.text().await.unwrap_or_default()
                            );
                        }
                        Err(e) => {
                            log::warn!(
                                "Device deactivation request failed (proceeding with local disconnect): {}",
                                e
                            );
                        }
                    }
                }
            }
        }

        // Clear org token cache
        self.org_token_cache.lock().await.clear();

        self.db.set_team_sync_authorized(false).await;
        if let Err(error) = delete_account(&self.db).await {
            // The account is still connected, so roll back the runtime gate.
            self.db.set_team_sync_authorized(true).await;
            return Err(error);
        }

        let _ = self.emitter.emit(
            "db-change",
            serde_json::json!({"table": "account", "action": "delete"}),
        );
        let _ = self
            .emitter
            .emit("account-disconnected", serde_json::json!({}));

        Ok(())
    }

    /// Get current connection (if any).
    pub fn get_connection(&self) -> Result<Option<AccountConnection>, String> {
        let db = self.db.clone();
        block_on_account_db(async move { get_account_connection(&db).await })
    }

    /// Get the decrypted JWT for API calls, or `None` when no token is stored
    /// or the stored token has expired. Mirrors
    /// `AnonDeviceManager::get_anon_jwt`: an expired token is useless for API
    /// calls (every gateway 401s it) and would shadow the anonymous `/embed`
    /// fallback, so it is treated as absent. The background refresh loop reads
    /// the raw token via `get_jwt_data`, so it can still renew an expired token.
    pub(crate) fn get_jwt(&self) -> Result<Option<String>, String> {
        let db = self.db.clone();
        block_on_account_db(async move { get_jwt_from_db(&db).await })
    }

    /// Start the JWT refresh background task.
    async fn start_refresh(&self) {
        let mut handle = self.refresh_handle.lock().await;

        // Stop existing task
        *handle = None;

        let (cancel_tx, cancel_rx) = watch::channel(false);
        *handle = Some(RefreshHandle { cancel_tx });

        let db = self.db.clone();
        let emitter = self.emitter.clone();
        let api_config = self.api_config.clone();
        tokio::spawn(refresh_loop(db, emitter, cancel_rx, api_config));
    }

    /// Start refresh if an account exists. Called on app startup.
    pub async fn start_refresh_if_connected(&self) {
        let has_account = match get_account_connection(&self.db).await {
            Ok(account) => account.is_some(),
            Err(error) => {
                log::error!("Failed to load account connection: {}", error);
                return;
            }
        };

        if has_account {
            self.start_refresh().await;
            log::info!("Started account JWT refresh task");
        }
    }

    /// Stop the refresh task (for shutdown).
    pub async fn stop(&self) {
        let mut handle = self.refresh_handle.lock().await;
        *handle = None;
    }
}

async fn refresh_loop(
    db: Arc<DbState>,
    emitter: Arc<dyn EventEmitter>,
    mut cancel_rx: watch::Receiver<bool>,
    api_config: crate::api::ApiConfig,
) {
    let mut consecutive_failures: u32 = 0;
    // Tracked separately from generic failures: a 401 is auth-definitive (the
    // stored token will never be accepted again), so it drives a hard clear of
    // local account state rather than the soft log-and-retry that transient
    // network/parse failures get.
    let mut consecutive_401s: u32 = 0;
    let client = reqwest::Client::new();
    const MAX_FAILURES: u32 = 3;

    loop {
        let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(CHECK_INTERVAL_SECS));
        tokio::select! {
            _ = sleep => {},
            _ = cancel_rx.changed() => {
                if *cancel_rx.borrow() {
                    log::info!("Account JWT refresh task cancelled");
                    return;
                }
            }
        }

        // Load JWT from DB
        let jwt_data = get_jwt_data(&db).await;

        let (encrypted_jwt, expires_at_opt) = match jwt_data {
            Ok(Some(data)) => data,
            Ok(None) => {
                log::debug!("No account JWT found, skipping refresh");
                continue;
            }
            Err(e) => {
                log::warn!("Account refresh: failed to load JWT: {}", e);
                consecutive_failures += 1;
                continue;
            }
        };

        let jwt = match decrypt_jwt_from_storage(&encrypted_jwt) {
            Ok(j) => j,
            Err(e) => {
                log::warn!("Account refresh: failed to decrypt JWT: {}", e);
                consecutive_failures += 1;
                continue;
            }
        };

        // Reconcile org memberships each connected tick, independent of the
        // token-expiry gate below. The token refresh only fires near expiry, so
        // it alone can't bound how quickly a team joined mid-session becomes
        // visible; polling the read-only orgs endpoint here caps that latency at
        // one interval. Best-effort: any error leaves the stored set untouched.
        reconcile_org_memberships(&db, &emitter, &client, &api_config, &jwt).await;

        let expires_at = match expires_at_opt {
            Some(exp) => exp,
            None => continue,
        };

        let now = chrono::Utc::now().timestamp();
        let time_until_expiry = expires_at - now;

        if time_until_expiry > REFRESH_THRESHOLD_SECS {
            log::debug!(
                "Account JWT still valid for {} minutes, no refresh needed",
                time_until_expiry / 60
            );
            consecutive_failures = 0;
            consecutive_401s = 0;
            continue;
        }

        log::info!(
            "Account JWT expires in {} minutes, refreshing...",
            time_until_expiry / 60
        );

        match client
            .post(api_config.device_refresh_url())
            .bearer_auth(&jwt)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<serde_json::Value>().await {
                    Ok(body) => {
                        if let (Some(new_token), Some(expires_at_str)) =
                            (body["token"].as_str(), body["expires_at"].as_str())
                        {
                            let new_expires_at =
                                chrono::DateTime::parse_from_rfc3339(expires_at_str)
                                    .map(|dt| dt.timestamp())
                                    .unwrap_or(now + 3600);

                            match encrypt_jwt_for_storage(new_token) {
                                Ok(enc) => {
                                    if let Err(e) = update_jwt(&db, enc, new_expires_at).await {
                                        log::error!("Account refresh: failed to save JWT: {}", e);
                                        consecutive_failures += 1;
                                    } else {
                                        log::info!("Account JWT refreshed successfully");
                                        consecutive_failures = 0;
                                        consecutive_401s = 0;
                                    }
                                }
                                Err(e) => {
                                    log::error!(
                                        "Account refresh: failed to encrypt new JWT: {}",
                                        e
                                    );
                                    consecutive_failures += 1;
                                }
                            }
                        } else {
                            // A 200 with no token/expires_at is a malformed
                            // success: we can't advance the stored token, so count
                            // it as a failure rather than silently no-op'ing and
                            // letting the token drift to expiry unnoticed.
                            log::warn!("Account refresh: 200 response missing token/expires_at");
                            consecutive_failures += 1;
                        }
                    }
                    Err(e) => {
                        log::warn!("Account refresh: failed to parse response: {}", e);
                        consecutive_failures += 1;
                    }
                }
            }
            Ok(resp) if resp.status() == reqwest::StatusCode::UNAUTHORIZED => {
                let body = resp.text().await.unwrap_or_default();
                log::warn!("Account JWT refresh rejected (401): {}", body);
                consecutive_401s += 1;
                if consecutive_401s >= MAX_FAILURES {
                    // Auth-definitive dead token: it will never be accepted
                    // again, so stop the zombie 401-loop by clearing local
                    // account state (mirrors the local clear in `disconnect`; the
                    // server-side device deactivation is skipped — it can't
                    // succeed with a dead token). DB and UI now agree the user is
                    // signed out, and the refresh task ends until a reconnect
                    // starts a fresh one.
                    log::error!(
                        "Account refresh: {} consecutive 401s, clearing account and disconnecting",
                        consecutive_401s
                    );
                    db.set_team_sync_authorized(false).await;
                    if let Err(e) = delete_account(&db).await {
                        log::error!("Account refresh: failed to clear account row: {}", e);
                    }
                    let _ = emitter.emit(
                        "db-change",
                        serde_json::json!({"table": "account", "action": "delete"}),
                    );
                    let _ = emitter.emit("account-disconnected", serde_json::json!({}));
                    return;
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                log::warn!("Account JWT refresh failed: {} - {}", status, body);
                consecutive_failures += 1;
            }
            Err(e) => {
                log::warn!("Account JWT refresh request failed: {}", e);
                consecutive_failures += 1;
            }
        }

        if consecutive_failures >= MAX_FAILURES {
            log::error!(
                "Account refresh: {} consecutive failures, emitting disconnect",
                consecutive_failures
            );
            let _ = emitter.emit("account-disconnected", serde_json::json!({}));
            consecutive_failures = 0;
        }
    }
}

/// Fetch the account's current org memberships and, when they differ from the
/// stored set, persist the new set and emit `account-updated` so the frontend
/// can auto-connect newly-joined teams (and drop ones the user left). Compares
/// full `(org_id, org_name, role)` tuples so a rename also refreshes the cached
/// account view. Best-effort and side-effect-free on error or when unchanged.
async fn reconcile_org_memberships(
    db: &DbState,
    emitter: &Arc<dyn EventEmitter>,
    client: &reqwest::Client,
    api_config: &crate::api::ApiConfig,
    device_jwt: &str,
) {
    let fetched = match fetch_org_memberships(client, api_config, device_jwt).await {
        Ok(memberships) => memberships,
        Err(error) => {
            log::debug!("Account refresh: org membership fetch skipped: {error}");
            return;
        }
    };

    let stored = match get_account_connection(db).await {
        Ok(Some(conn)) => conn.org_memberships,
        // No account (raced with a disconnect) or a read error: nothing to do.
        _ => return,
    };

    if memberships_match(&stored, &fetched) {
        return;
    }

    let json = match serde_json::to_string(&fetched) {
        Ok(json) => json,
        Err(error) => {
            log::warn!("Account refresh: failed to serialize memberships: {error}");
            return;
        }
    };

    if let Err(error) = update_org_memberships(db, &json).await {
        log::warn!("Account refresh: failed to persist memberships: {error}");
        return;
    }

    log::info!(
        "Account org memberships changed ({} team(s)); emitting account-updated",
        fetched.len()
    );
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "account", "action": "upsert"}),
    );
    let _ = emitter.emit("account-updated", serde_json::json!({}));
}

/// Fetch the account's org memberships from the read-only `/tokens/device/orgs`
/// endpoint. Returns an error (not an empty list) on any non-success status or
/// transport failure, so the caller leaves the stored set untouched rather than
/// wiping memberships on a transient blip.
async fn fetch_org_memberships(
    client: &reqwest::Client,
    api_config: &crate::api::ApiConfig,
    device_jwt: &str,
) -> Result<Vec<OrgMembership>, String> {
    let resp = client
        .get(api_config.device_orgs_url())
        .bearer_auth(device_jwt)
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("orgs endpoint returned {}", resp.status()));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("parse failed: {e}"))?;
    let orgs = body
        .get("orgs")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    serde_json::from_value(orgs).map_err(|e| format!("deserialize orgs failed: {e}"))
}

/// Order-insensitive equality of two membership sets on `(org_id, org_name,
/// role)`, so a reorder from the api doesn't spuriously churn the stored set.
fn memberships_match(a: &[OrgMembership], b: &[OrgMembership]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a: Vec<(&str, &str, &str)> = a
        .iter()
        .map(|m| (m.org_id.as_str(), m.org_name.as_str(), m.role.as_str()))
        .collect();
    let mut b: Vec<(&str, &str, &str)> = b
        .iter()
        .map(|m| (m.org_id.as_str(), m.org_name.as_str(), m.role.as_str()))
        .collect();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

/// Persist a new `org_memberships` JSON blob on the account row.
async fn update_org_memberships(db: &DbState, memberships_json: &str) -> Result<(), String> {
    let json = memberships_json.to_string();
    db.local
        .write(|conn| {
            let json = json.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE account SET org_memberships = ?1, updated_at = ?2",
                    (json.as_str(), chrono::Utc::now().timestamp()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| account_db_error("Failed to update org memberships", error))
}

async fn upsert_account(db: &DbState, account: DbAccount) -> Result<(), String> {
    db.local
        .write(|conn| {
            let account = account.clone();
            Box::pin(async move {
                conn.execute("DELETE FROM account", ()).await?;
                conn.execute(
                    "INSERT INTO account (
                        user_id,
                        email,
                        name,
                        device_id,
                        plan,
                        jwt_encrypted,
                        jwt_expires_at,
                        org_memberships,
                        connected_at,
                        updated_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    (
                        account.user_id.as_str(),
                        account.email.as_str(),
                        account.name.as_str(),
                        account.device_id.as_str(),
                        account.plan.as_str(),
                        account.jwt_encrypted.as_deref(),
                        account.jwt_expires_at.map(i64::from),
                        account.org_memberships.as_deref(),
                        i64::from(account.connected_at),
                        i64::from(account.updated_at),
                    ),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| account_db_error("Failed to upsert account", error))
}

async fn delete_account(db: &DbState) -> Result<(), String> {
    db.local
        .write(|conn| {
            Box::pin(async move {
                conn.execute("DELETE FROM account", ()).await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| account_db_error("Failed to delete account", error))
}

async fn get_account_connection(db: &DbState) -> Result<Option<AccountConnection>, String> {
    db.local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT
                            user_id,
                            email,
                            name,
                            device_id,
                            plan,
                            jwt_encrypted,
                            jwt_expires_at,
                            org_memberships,
                            connected_at,
                            updated_at
                         FROM account
                         LIMIT 1",
                        (),
                    )
                    .await?;

                rows.next()
                    .await?
                    .map(|row| DbAccount::from_row(&row).map(AccountConnection::from))
                    .transpose()
            })
        })
        .await
        .map_err(|error| account_db_error("Failed to get account", error))
}

async fn get_jwt_data(db: &DbState) -> Result<Option<(String, Option<i64>)>, String> {
    db.local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT jwt_encrypted, jwt_expires_at
                         FROM account
                         LIMIT 1",
                        (),
                    )
                    .await?;

                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };

                let Some(jwt) = row.opt_text(0)? else {
                    return Ok(None);
                };
                Ok(Some((jwt, row.opt_i64(1)?)))
            })
        })
        .await
        .map_err(|error| account_db_error("Failed to get account JWT data", error))
}

async fn get_jwt_from_db(db: &DbState) -> Result<Option<String>, String> {
    match get_jwt_data(db).await? {
        // An expired token is treated as absent: callers get `None` rather than
        // a credential every gateway rejects with 401. Critically, this lets
        // `resolve_embed_token` fall through to the anonymous device token
        // instead of letting a lapsed account JWT shadow it and silently kill
        // vibe coloring. A token with no recorded expiry is returned as-is (the
        // legacy posture — we can't prove it stale).
        Some((_encrypted, Some(exp))) if exp <= chrono::Utc::now().timestamp() => Ok(None),
        Some((encrypted, _)) => decrypt_jwt_from_storage(&encrypted).map(Some),
        None => Ok(None),
    }
}

async fn update_jwt(db: &DbState, encrypted_jwt: String, expires_at: i64) -> Result<(), String> {
    db.local
        .write(|conn| {
            let encrypted_jwt = encrypted_jwt.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE account
                     SET jwt_encrypted = ?1,
                         jwt_expires_at = ?2",
                    (encrypted_jwt.as_str(), expires_at),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| account_db_error("Failed to update account JWT", error))
}

fn account_db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

fn block_on_account_db<T>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run_account_db_future(future))
            .join()
            .map_err(|_| "Account DB runtime thread panicked".to_string())?
    } else {
        run_account_db_future(future)
    }
}

fn run_account_db_future<T>(future: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Failed to create account DB runtime: {error}"))?
        .block_on(future)
}

/// Decode JWT claims for device tokens (no org_id/org_role required).
fn decode_device_jwt_claims(jwt: &str) -> Result<super::jwt::JwtClaims, String> {
    use base64::Engine as _;

    let parts: Vec<&str> = jwt.split('.').collect();
    if parts.len() != 3 {
        return Err("Invalid JWT format".to_string());
    }

    let payload_bytes = base64::engine::general_purpose::STANDARD
        .decode(parts[1])
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1]))
        .map_err(|e| format!("Failed to decode JWT payload: {}", e))?;

    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|e| format!("Failed to parse JWT payload: {}", e))?;

    Ok(super::jwt::JwtClaims {
        sub: payload["sub"]
            .as_str()
            .ok_or("Missing 'sub' claim")?
            .to_string(),
        org_id: payload["org_id"].as_str().unwrap_or("").to_string(),
        org_role: payload["org_role"].as_str().unwrap_or("").to_string(),
        exp: payload["exp"].as_i64().ok_or("Missing 'exp' claim")?,
        name: payload["name"].as_str().map(|s| s.to_string()),
        email: payload["email"].as_str().map(|s| s.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    fn make_jwt(payload: &serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"EdDSA","typ":"JWT"}"#);
        let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(payload).unwrap());
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("fake-sig");
        format!("{}.{}.{}", header, payload_b64, signature)
    }

    #[test]
    fn refresh_threshold_exceeds_check_interval() {
        // Invariant: the near-expiry threshold must sit ABOVE the tick interval so
        // a 1-hour token is refreshed on the T+30 tick while still valid, rather
        // than on the T+60 tick after it has already expired (relying on the API's
        // short post-expiry grace window). If the threshold ever drops below the
        // interval, every refresh lands post-expiry and a sleep/wake across the
        // boundary can strand the token.
        assert!(REFRESH_THRESHOLD_SECS > CHECK_INTERVAL_SECS as i64);
    }

    #[test]
    fn decode_device_jwt_with_all_fields() {
        let payload = serde_json::json!({
            "sub": "user-123",
            "exp": 9999999999_i64,
            "name": "Alice",
            "email": "alice@example.com",
            "type": "device",
            "device_id": "dev-1",
        });

        let jwt = make_jwt(&payload);
        let claims = decode_device_jwt_claims(&jwt).unwrap();

        assert_eq!(claims.sub, "user-123");
        assert_eq!(claims.exp, 9999999999);
        assert_eq!(claims.name, Some("Alice".to_string()));
        assert_eq!(claims.email, Some("alice@example.com".to_string()));
        // Device JWTs don't have org_id/org_role — defaults to empty
        assert_eq!(claims.org_id, "");
        assert_eq!(claims.org_role, "");
    }

    #[test]
    fn decode_device_jwt_missing_sub_fails() {
        let payload = serde_json::json!({
            "exp": 9999999999_i64,
        });
        let jwt = make_jwt(&payload);
        let result = decode_device_jwt_claims(&jwt);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("sub"));
    }

    #[test]
    fn decode_device_jwt_missing_exp_fails() {
        let payload = serde_json::json!({
            "sub": "user-123",
        });
        let jwt = make_jwt(&payload);
        let result = decode_device_jwt_claims(&jwt);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exp"));
    }

    #[test]
    fn decode_device_jwt_invalid_format_fails() {
        assert!(decode_device_jwt_claims("not-a-jwt").is_err());
        assert!(decode_device_jwt_claims("only.two").is_err());
        assert!(decode_device_jwt_claims("").is_err());
    }

    #[test]
    fn decode_device_jwt_invalid_base64_fails() {
        let result = decode_device_jwt_claims("header.!!!invalid!!!.sig");
        assert!(result.is_err());
    }

    fn membership(id: &str, name: &str, role: &str) -> OrgMembership {
        OrgMembership {
            org_id: id.to_string(),
            org_name: name.to_string(),
            role: role.to_string(),
        }
    }

    #[test]
    fn memberships_match_is_order_insensitive() {
        let a = vec![
            membership("o1", "One", "member"),
            membership("o2", "Two", "owner"),
        ];
        let b = vec![
            membership("o2", "Two", "owner"),
            membership("o1", "One", "member"),
        ];
        assert!(memberships_match(&a, &b));
    }

    #[test]
    fn memberships_match_detects_join_leave_and_rename() {
        let base = vec![membership("o1", "One", "member")];
        // A newly-joined team.
        assert!(!memberships_match(
            &base,
            &[
                membership("o1", "One", "member"),
                membership("o2", "Two", "member")
            ]
        ));
        // A left team.
        assert!(!memberships_match(&base, &[]));
        // A rename (same id, different name) still counts as a change.
        assert!(!memberships_match(
            &base,
            &[membership("o1", "Renamed", "member")]
        ));
        // A role change counts as a change.
        assert!(!memberships_match(
            &base,
            &[membership("o1", "One", "owner")]
        ));
    }

    #[test]
    fn decode_device_jwt_standard_base64_also_works() {
        // Standard base64 (with +/= padding) should also decode
        let payload = serde_json::json!({
            "sub": "user-std",
            "exp": 1234567890_i64,
        });
        let payload_b64 =
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&payload).unwrap());
        let jwt = format!("header.{}.sig", payload_b64);

        let claims = decode_device_jwt_claims(&jwt).unwrap();
        assert_eq!(claims.sub, "user-std");
    }
}
