//! The shared per-team content store: the canonical archival home for heavy
//! team-run content (the per-execution range pack and the content-addressed
//! system-prompt/init blobs), fetched on demand by hash.
//!
//! For a team run, [`super::rewrite::archive_target`] offloads those bytes to the
//! store keyed by their content hash instead of inlining them into the synced
//! replica; the replica keeps only rows, git coordinates, and hash pointers. On
//! read, [`super::reconstruct`] fetches the bytes back by hash. Local runs never
//! touch a store — their bytes stay inline in the private DB exactly as before.
//!
//! The single seam is the [`ContentStore`] trait: a content-addressed
//! `put(hash, bytes)` / `get(hash)`. The production implementation
//! ([`BrokeredContentStore`]) proxies bytes through the api/ broker on the SAME
//! auth boundary as team sync (a per-request minted sync token), backed by S3 in
//! prod and a filesystem dir in dev. Swapping the broker for presigned S3 URLs
//! later changes only this trait's impl and one broker route, never the archival
//! call sites. Tests inject [`InMemoryContentStore`].

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::account::team_token_minter::TeamTokenMinter;
use crate::api::ApiConfig;
use crate::db::TeamId;
use crate::storage::{LocalDb, RowExt};

/// Frame an execution's `(pack, pack_idx)` pair into ONE content-store object:
/// `u64-LE len(pack) || pack || u64-LE len(idx) || idx`. The store key is the
/// sha256 of this framed object (see [`content_hash`]), so a fetched object can
/// be verified against the `pack_hash` pointer that addressed it. One object,
/// one fetch, one column.
pub fn frame_pack(pack: &[u8], idx: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + pack.len() + idx.len());
    out.extend_from_slice(&(pack.len() as u64).to_le_bytes());
    out.extend_from_slice(pack);
    out.extend_from_slice(&(idx.len() as u64).to_le_bytes());
    out.extend_from_slice(idx);
    out
}

/// Split a framed pack object (see [`frame_pack`]) back into `(pack, pack_idx)`.
/// `None` on malformed framing (a truncated or corrupt object), which
/// reconstruction treats as a missing pack and degrades to a labeled stub.
pub fn unframe_pack(bytes: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    fn take_len(bytes: &[u8], cur: &mut usize) -> Option<usize> {
        let end = cur.checked_add(8)?;
        let slice = bytes.get(*cur..end)?;
        let mut len = [0u8; 8];
        len.copy_from_slice(slice);
        *cur = end;
        usize::try_from(u64::from_le_bytes(len)).ok()
    }
    let mut cur = 0usize;
    let pack_len = take_len(bytes, &mut cur)?;
    let pack_end = cur.checked_add(pack_len)?;
    let pack = bytes.get(cur..pack_end)?.to_vec();
    cur = pack_end;
    let idx_len = take_len(bytes, &mut cur)?;
    let idx_end = cur.checked_add(idx_len)?;
    let idx = bytes.get(cur..idx_end)?.to_vec();
    Some((pack, idx))
}

/// The content-store key for `bytes`: their sha256 as lowercase hex.
pub fn content_hash(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// A content-addressed byte store. Both methods key on a caller-computed hash;
/// the store itself never interprets the bytes.
#[async_trait]
pub trait ContentStore: Send + Sync {
    /// Store `bytes` under `hash`. Idempotent: content-addressed, so re-putting an
    /// existing key is a no-op (this is what makes cross-member dedup free).
    async fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), String>;
    /// Fetch the bytes for `hash`. `Ok(None)` means not found (reconstruction
    /// degrades to a labeled stub); `Err` is a transport failure.
    async fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, String>;
}

/// A team replica's intrinsic team identity plus its content store. Attached to a
/// [`LocalDb`] only when the handle is a team replica (the private DB carries
/// `None`), so archival and reconstruction can branch on `db.content_store()`
/// without any out-of-band team lookup — the handle knows its own scope.
pub struct TeamReplicaContext {
    pub team_id: TeamId,
    pub store: Arc<dyn ContentStore>,
    /// Private database for machine-local project route metadata. Team replicas
    /// use this to resolve `project_routes.local_repo_path` during reconstruction.
    pub private_db: Option<Arc<LocalDb>>,
}

impl std::fmt::Debug for TeamReplicaContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeamReplicaContext")
            .field("team_id", &self.team_id)
            .field("store", &"<dyn ContentStore>")
            .field("private_db", &self.private_db.as_ref().map(|_| "<LocalDb>"))
            .finish()
    }
}

/// Builds the per-team [`ContentStore`] a freshly opened team replica attaches.
/// The host installs one factory ([`DbState::set_content_store_factory`]); tests
/// attach a store directly and never go through it.
///
/// [`DbState::set_content_store_factory`]: crate::db::DbState::set_content_store_factory
pub trait ContentStoreFactory: Send + Sync {
    fn store_for(&self, team_id: &TeamId) -> Arc<dyn ContentStore>;
}

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
                (hash.to_string(), turso::Value::Blob(bytes.to_vec())),
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

/// An in-memory [`ContentStore`] for tests: a hash -> bytes map shared (via
/// `Arc`) between the archiving and reconstructing sides of a round-trip. No
/// broker, no S3, no network.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Default)]
pub struct InMemoryContentStore {
    inner: Arc<tokio::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl InMemoryContentStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test introspection: whether a hash is present in the store.
    pub async fn contains(&self, hash: &str) -> bool {
        self.inner.lock().await.contains_key(hash)
    }

    /// Test introspection: number of stored objects.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl ContentStore for InMemoryContentStore {
    async fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), String> {
        self.inner
            .lock()
            .await
            .insert(hash.to_string(), bytes.to_vec());
        Ok(())
    }

    async fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.inner.lock().await.get(hash).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_framing_round_trips() {
        let pack = b"the-pack-bytes".to_vec();
        let idx = b"idx".to_vec();
        let framed = frame_pack(&pack, &idx);
        assert_eq!(unframe_pack(&framed), Some((pack, idx)));
        // The hash addresses the FRAMED object, so a fetched object verifies.
        assert_eq!(
            content_hash(&framed),
            content_hash(&frame_pack(b"the-pack-bytes", b"idx"))
        );
    }

    #[test]
    fn unframe_rejects_truncated_or_corrupt_objects() {
        assert_eq!(unframe_pack(&[]), None);
        assert_eq!(unframe_pack(&[1, 2, 3]), None);
        // Declares a 100-byte pack but carries none.
        let mut bad = 100u64.to_le_bytes().to_vec();
        bad.extend_from_slice(b"short");
        assert_eq!(unframe_pack(&bad), None);
    }

    #[test]
    fn empty_pack_and_idx_round_trip() {
        let framed = frame_pack(&[], &[]);
        assert_eq!(unframe_pack(&framed), Some((Vec::new(), Vec::new())));
    }

    #[tokio::test]
    async fn in_memory_store_round_trips_and_is_idempotent() {
        let store = InMemoryContentStore::new();
        assert!(store.get("abc").await.unwrap().is_none());

        store.put("abc", b"hello").await.unwrap();
        assert_eq!(store.get("abc").await.unwrap().unwrap(), b"hello");
        assert_eq!(store.len().await, 1);

        // Idempotent re-put of identical content-addressed bytes is a no-op in
        // count and value.
        store.put("abc", b"hello").await.unwrap();
        assert_eq!(store.len().await, 1);
        assert_eq!(store.get("abc").await.unwrap().unwrap(), b"hello");
    }
}
