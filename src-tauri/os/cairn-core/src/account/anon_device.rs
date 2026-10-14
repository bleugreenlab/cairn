//! AnonDeviceManager — a user-less device identity for the cloud `/embed`
//! gateway, so embedding (vibe coloring, recommender, …) works with no account
//! connected.
//!
//! Mirrors the relay posture: the app self-registers an anonymous device JWT
//! (no login) and sends it as the Bearer token to `/embed`. The token is
//! long-lived (~30 days), persisted locally in the single-row `anon_device`
//! table, and re-registered lazily near expiry. When an account *is* connected,
//! the account JWT takes precedence (see `Orchestrator::embed_token_provider`).

use std::{future::Future, sync::Arc};

use super::jwt::{decrypt_jwt_from_storage, encrypt_jwt_for_storage};
use crate::api::ApiConfig;
use crate::db::DbState;
use crate::storage::RowExt;

/// Re-register when the stored JWT is within this window of expiring.
const REFRESH_THRESHOLD_SECS: i64 = 2 * 24 * 3600; // 2 days

/// Default token lifetime assumed when the API omits/!parses `expiresAt`.
const DEFAULT_TTL_SECS: i64 = 30 * 24 * 3600; // 30 days

/// Manages the anonymous device JWT used for the `/embed` gateway.
pub struct AnonDeviceManager {
    db: Arc<DbState>,
    api_config: ApiConfig,
    /// Process cache of the resolved stable machine `device_id`, so the
    /// team-ownership guard/sweep don't hit the DB on every claim and so the id
    /// stays fixed for the life of the process even if the row were disturbed.
    device_id_cache: std::sync::Mutex<Option<String>>,
}

impl AnonDeviceManager {
    pub(crate) fn new(db: Arc<DbState>, api_config: ApiConfig) -> Self {
        Self {
            db,
            api_config,
            device_id_cache: std::sync::Mutex::new(None),
        }
    }

    /// The stable per-machine `device_id` used as the team-execution OWNERSHIP
    /// key (CAIRN-2629).
    ///
    /// This is the same UUID `ensure_registered` persists for the `/embed`
    /// gateway: generated once on first run, kept across sign-in/out and account
    /// switches (no code path clears or regenerates it). Unlike the account/cloud
    /// device id — which the server caps at one per USER and so cannot tell two
    /// machines apart — this id distinguishes any two machines, exactly what
    /// runner ownership needs.
    ///
    /// Sync, and self-healing: if the row does not exist yet (called before
    /// `ensure_registered`), it generates and persists one so ownership is never
    /// keyed on an empty id. Cached after first resolution.
    pub fn device_id(&self) -> String {
        {
            let guard = self.device_id_cache.lock().unwrap();
            if let Some(id) = guard.as_ref() {
                return id.clone();
            }
        }
        let db = self.db.clone();
        let resolved = block_on_anon_db(async move {
            if let Some(row) = get_anon_device(&db).await? {
                return Ok(row.device_id);
            }
            let id = uuid::Uuid::new_v4().to_string();
            insert_anon_device(&db, &id).await?;
            Ok(id)
        })
        .unwrap_or_else(|e| {
            log::warn!("resolving machine device_id failed, using a process-local id: {e}");
            uuid::Uuid::new_v4().to_string()
        });
        // get_or_insert makes concurrent first-callers converge on one value.
        self.device_id_cache
            .lock()
            .unwrap()
            .get_or_insert(resolved)
            .clone()
    }

    /// Ensure an anonymous device JWT is registered and fresh.
    ///
    /// Generates and persists a stable `device_id` on first run, then registers
    /// (or re-registers near expiry) an anonymous token via the API.
    /// Best-effort: failures are logged and swallowed so embedding simply stays
    /// neutral until the next attempt.
    pub(crate) async fn ensure_registered(&self) {
        if let Err(e) = self.ensure_registered_inner().await {
            log::warn!("anon device registration failed: {e}");
        }
    }

    async fn ensure_registered_inner(&self) -> Result<(), String> {
        let row = get_anon_device(&self.db).await?;

        // Resolve a stable device_id, persisting a fresh one on first run.
        let device_id = match row.as_ref() {
            Some(r) => r.device_id.clone(),
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                insert_anon_device(&self.db, &id).await?;
                id
            }
        };

        // Skip if the stored JWT is still comfortably valid.
        let now = chrono::Utc::now().timestamp();
        if let Some(r) = row.as_ref() {
            if let (Some(_), Some(exp)) = (r.jwt_encrypted.as_ref(), r.jwt_expires_at) {
                if exp - now > REFRESH_THRESHOLD_SECS {
                    return Ok(());
                }
            }
        }

        // Register (or re-register) an anonymous token, reusing the stable id.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;

        let resp = client
            .post(self.api_config.anon_device_url())
            .json(&serde_json::json!({ "device_id": device_id }))
            .send()
            .await
            .map_err(|e| format!("anon device request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("anon device endpoint returned {status}: {body}"));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("anon device response parse failed: {e}"))?;

        let token = body["token"]
            .as_str()
            .ok_or("anon device response missing token")?;
        let expires_at = body["expiresAt"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.timestamp())
            .unwrap_or(now + DEFAULT_TTL_SECS);

        let encrypted = encrypt_jwt_for_storage(token)?;
        update_anon_jwt(&self.db, &device_id, &encrypted, expires_at).await?;

        log::info!("anonymous device registered for /embed");
        Ok(())
    }

    /// Get the decrypted anonymous JWT for gateway auth.
    ///
    /// Sync, mirroring `AccountManager::get_jwt`. Returns `None` when no token
    /// is stored or it has expired (callers fall back to neutral coloring).
    pub(crate) fn get_anon_jwt(&self) -> Result<Option<String>, String> {
        let db = self.db.clone();
        block_on_anon_db(async move {
            let Some(row) = get_anon_device(&db).await? else {
                return Ok(None);
            };
            let (Some(encrypted), Some(exp)) = (row.jwt_encrypted, row.jwt_expires_at) else {
                return Ok(None);
            };
            if exp <= chrono::Utc::now().timestamp() {
                return Ok(None);
            }
            decrypt_jwt_from_storage(&encrypted).map(Some)
        })
    }
}

/// Human-readable presence name for this machine: hostname + platform. The
/// display label the runner picker and owner UI show for a device; v1 has no
/// user-editable override.
pub fn machine_device_name() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown-host".to_string());
    format!("{host} ({})", std::env::consts::OS)
}

struct AnonDeviceRow {
    device_id: String,
    jwt_encrypted: Option<String>,
    jwt_expires_at: Option<i64>,
}

async fn get_anon_device(db: &DbState) -> Result<Option<AnonDeviceRow>, String> {
    db.local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT device_id, jwt_encrypted, jwt_expires_at
                         FROM anon_device
                         LIMIT 1",
                        (),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some(AnonDeviceRow {
                    device_id: row.text(0)?,
                    jwt_encrypted: row.opt_text(1)?,
                    jwt_expires_at: row.opt_i64(2)?,
                }))
            })
        })
        .await
        .map_err(|e| format!("Failed to get anon device: {e}"))
}

async fn insert_anon_device(db: &DbState, device_id: &str) -> Result<(), String> {
    let device_id = device_id.to_string();
    db.local
        .write(|conn| {
            let device_id = device_id.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                conn.execute(
                    "INSERT INTO anon_device (device_id, created_at, updated_at)
                     VALUES (?1, ?2, ?2)",
                    (device_id.as_str(), now),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to insert anon device: {e}"))
}

async fn update_anon_jwt(
    db: &DbState,
    device_id: &str,
    encrypted_jwt: &str,
    expires_at: i64,
) -> Result<(), String> {
    let device_id = device_id.to_string();
    let encrypted_jwt = encrypted_jwt.to_string();
    db.local
        .write(|conn| {
            let device_id = device_id.clone();
            let encrypted_jwt = encrypted_jwt.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp();
                conn.execute(
                    "UPDATE anon_device
                     SET jwt_encrypted = ?1, jwt_expires_at = ?2, updated_at = ?3
                     WHERE device_id = ?4",
                    (encrypted_jwt.as_str(), expires_at, now, device_id.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to update anon device JWT: {e}"))
}

fn block_on_anon_db<T>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run_anon_db_future(future))
            .join()
            .map_err(|_| "Anon DB runtime thread panicked".to_string())?
    } else {
        run_anon_db_future(future)
    }
}

fn run_anon_db_future<T>(future: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Failed to create anon DB runtime: {error}"))?
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    async fn test_db() -> Arc<DbState> {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalDb::open(temp.keep().join("anon.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search_dir = tempfile::tempdir().unwrap();
        let search = Arc::new(SearchIndex::open_or_create(search_dir.keep()).unwrap());
        Arc::new(DbState::new(Arc::new(local), search))
    }

    fn manager(db: Arc<DbState>) -> AnonDeviceManager {
        AnonDeviceManager::new(db, ApiConfig::default())
    }

    #[tokio::test]
    async fn get_anon_jwt_none_when_no_row() {
        let db = test_db().await;
        let mgr = manager(db);
        assert_eq!(mgr.get_anon_jwt().unwrap(), None);
    }

    #[tokio::test]
    async fn insert_persists_stable_device_id() {
        let db = test_db().await;
        insert_anon_device(&db, "dev-stable").await.unwrap();

        let row = get_anon_device(&db).await.unwrap().unwrap();
        assert_eq!(row.device_id, "dev-stable");
        assert!(row.jwt_encrypted.is_none());
        assert!(row.jwt_expires_at.is_none());
    }

    #[tokio::test]
    async fn jwt_roundtrips_through_storage() {
        let db = test_db().await;
        insert_anon_device(&db, "dev-1").await.unwrap();

        let jwt = "header.payload.sig";
        let encrypted = encrypt_jwt_for_storage(jwt).unwrap();
        let future_exp = chrono::Utc::now().timestamp() + 1000;
        update_anon_jwt(&db, "dev-1", &encrypted, future_exp)
            .await
            .unwrap();

        let mgr = manager(db);
        assert_eq!(mgr.get_anon_jwt().unwrap(), Some(jwt.to_string()));
    }

    #[tokio::test]
    async fn get_anon_jwt_none_when_expired() {
        let db = test_db().await;
        insert_anon_device(&db, "dev-1").await.unwrap();

        let encrypted = encrypt_jwt_for_storage("header.payload.sig").unwrap();
        let past_exp = chrono::Utc::now().timestamp() - 10;
        update_anon_jwt(&db, "dev-1", &encrypted, past_exp)
            .await
            .unwrap();

        let mgr = manager(db);
        assert_eq!(mgr.get_anon_jwt().unwrap(), None);
    }
}
