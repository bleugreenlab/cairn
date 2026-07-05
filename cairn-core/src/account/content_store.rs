//! The production, broker-backed [`ContentStore`] implementation and its
//! factory.
//!
//! The pure content-store abstraction — the [`ContentStore`] /
//! [`ContentStoreFactory`] traits, framing helpers, [`TeamReplicaContext`], and
//! the in-memory test store — lives in [`crate::storage::content_store`]. This
//! module holds only the impl that reaches UP into the account/api layer:
//! [`BrokeredContentStore`] proxies bytes through the api/ broker on the SAME
//! auth boundary as team sync (a per-request minted sync token), backed by S3 in
//! prod and a filesystem dir in dev.

use std::sync::Arc;

use async_trait::async_trait;

use crate::account::team_token_minter::TeamTokenMinter;
use crate::api::ApiConfig;
use crate::db::TeamId;
use crate::storage::content_store::{content_hash, ContentStore, ContentStoreFactory};
use crate::storage::{LocalDb, RowExt};

/// Production [`ContentStore`]: proxies bytes through the api/ broker
/// (`PUT`/`GET /teams/:id/cas/:hash`) authed with a per-request minted team sync
/// token, and read-through caches fetched bytes in the private DB's `cas_cache`
/// so repeat reconstructs are offline.
pub struct BrokeredContentStore {
    team_id: TeamId,
    api: ApiConfig,
    minter: Arc<dyn TeamTokenMinter>,
    /// The PRIVATE database, home of the `cas_cache` read-through cache. Never the
    /// team replica (whose synced rows must stay free of fetched bytes).
    local: Arc<LocalDb>,
    client: reqwest::Client,
}

impl BrokeredContentStore {
    pub fn new(
        team_id: TeamId,
        api: ApiConfig,
        minter: Arc<dyn TeamTokenMinter>,
        local: Arc<LocalDb>,
    ) -> Self {
        Self {
            team_id,
            api,
            minter,
            local,
            client: reqwest::Client::new(),
        }
    }

    /// Read-through cache lookup. Best-effort: a cache miss or error returns
    /// `None` and the caller falls through to the broker.
    async fn cache_get(&self, hash: &str) -> Option<Vec<u8>> {
        let hash = hash.to_string();
        let rows = self
            .local
            .query_all(
                "SELECT content FROM cas_cache WHERE hash = ?1 LIMIT 1",
                (hash,),
                |row| row.opt_blob(0),
            )
            .await
            .ok()?;
        rows.into_iter().next().flatten()
    }

    /// Populate the read-through cache. Content-addressed, so `INSERT OR IGNORE`
    /// is safe and concurrent fetches converge. Best-effort; errors are ignored.
    async fn cache_put(&self, hash: &str, bytes: &[u8]) {
        let _ = self
            .local
            .execute(
                "INSERT OR IGNORE INTO cas_cache(hash, content, fetched_at)
                 VALUES (?1, ?2, unixepoch())",
                (
                    hash.to_string(),
                    cairn_db::turso::Value::Blob(bytes.to_vec()),
                ),
            )
            .await;
    }
}

#[async_trait]
impl ContentStore for BrokeredContentStore {
    async fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), String> {
        let token = self.minter.mint(&self.team_id).await?;
        let url = self.api.team_cas_url(&self.team_id, hash);
        let resp = self
            .client
            .put(url)
            .bearer_auth(token)
            .body(bytes.to_vec())
            .send()
            .await
            .map_err(|e| format!("content-store put request failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "content-store put for {hash} returned {}",
                resp.status()
            ));
        }
        Ok(())
    }

    async fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, String> {
        if let Some(bytes) = self.cache_get(hash).await {
            return Ok(Some(bytes));
        }
        let token = self.minter.mint(&self.team_id).await?;
        let url = self.api.team_cas_url(&self.team_id, hash);
        let resp = self
            .client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| format!("content-store get request failed: {e}"))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(format!(
                "content-store get for {hash} returned {}",
                resp.status()
            ));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("content-store get body read failed: {e}"))?
            .to_vec();
        // Content-addressed integrity: the object's bytes MUST hash to the key it
        // was fetched under. A mismatch (a tampered or corrupt object, a broker or
        // backend bug) is treated as absent and NEVER written to `cas_cache`, so a
        // bad object can poison neither the read-through cache nor a
        // reconstruction; the caller degrades to a stub.
        if content_hash(&bytes) != hash {
            log::warn!(
                "content-store object for {hash} failed integrity check; treating as absent"
            );
            return Ok(None);
        }
        self.cache_put(hash, &bytes).await;
        Ok(Some(bytes))
    }
}

/// The production [`ContentStoreFactory`]: mints a [`BrokeredContentStore`] per
/// team, reusing the host's private DB, [`ApiConfig`], and the SAME token minter
/// that drives sync-token rotation.
pub struct BrokeredContentStoreFactory {
    local: Arc<LocalDb>,
    api: ApiConfig,
    minter: Arc<dyn TeamTokenMinter>,
}

impl BrokeredContentStoreFactory {
    pub fn new(local: Arc<LocalDb>, api: ApiConfig, minter: Arc<dyn TeamTokenMinter>) -> Self {
        Self { local, api, minter }
    }
}

impl ContentStoreFactory for BrokeredContentStoreFactory {
    fn store_for(&self, team_id: &TeamId) -> Arc<dyn ContentStore> {
        Arc::new(BrokeredContentStore::new(
            team_id.clone(),
            self.api.clone(),
            self.minter.clone(),
            self.local.clone(),
        ))
    }
}
