//! The shared per-team content store: the canonical archival home for heavy
//! team-run content (the per-execution range pack and the content-addressed
//! system-prompt/init blobs), fetched on demand by hash.
//!
//! For a team run, `crate::archival::rewrite::archive_target` offloads those
//! bytes to the store keyed by their content hash instead of inlining them into
//! the synced replica; the replica keeps only rows, git coordinates, and hash
//! pointers. On read, [`crate::storage::events::reconstruct`] fetches the bytes
//! back by hash. Local runs never
//! touch a store — their bytes stay inline in the private DB exactly as before.
//!
//! The single seam is the [`ContentStore`] trait: a content-addressed
//! `put(hash, bytes)` / `get(hash)`. The production implementation (a
//! broker-backed store living in the account layer) proxies bytes through the
//! api/ broker on the SAME auth boundary as team sync (a per-request minted sync
//! token), backed by S3 in prod and a filesystem dir in dev. Swapping the broker
//! for presigned S3 URLs later changes only that trait's impl and one broker
//! route, never the archival call sites. Tests inject [`InMemoryContentStore`].

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::storage::LocalDb;
use crate::storage::TeamId;

/// Frame an execution's `(pack, pack_idx)` pair into ONE content-store object:
/// `u64-LE len(pack) || pack || u64-LE len(idx) || idx`. The store key is the
/// sha256 of this framed object (see [`content_hash`]), so a fetched object can
/// be verified against the `pack_hash` pointer that addressed it. One object,
/// one fetch, one column.
pub use cairn_codec::transfer::frame_pack;

/// Split a framed pack object (see [`frame_pack`]) back into `(pack, pack_idx)`.
/// `None` on malformed framing (a truncated or corrupt object), which
/// reconstruction treats as a missing pack and degrades to a labeled stub.
pub(crate) fn unframe_pack(bytes: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    cairn_codec::transfer::unframe_pack(bytes).ok()
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
/// The cairn-core orchestrator (`DbState`) installs one factory via its
/// content-store-factory setter; tests attach a store directly and never go
/// through it.
pub trait ContentStoreFactory: Send + Sync {
    fn store_for(&self, team_id: &TeamId) -> Arc<dyn ContentStore>;
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
