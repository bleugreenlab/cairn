use super::*;

use crate::storage::pack_catalog::{PackCatalogPublication, PackKind};

/// Event UPDATEs per transaction. Bounds a single transaction's size no matter
/// how long the transcript is.
const UPDATE_CHUNK: usize = 500;

/// Apply a local (private database) archival: the content-addressed blobs and
/// the `execution_history` row commit first, then the event rewrites in chunks.
/// The ordering means a gitcoord row can never reference an absent pack.
pub(super) async fn apply(
    db: &LocalDb,
    updates: Vec<EventUpdate>,
    history: Option<ExecHistory>,
    blobs: Vec<SegmentBlob>,
) -> Result<(), String> {
    if !blobs.is_empty() || history.is_some() {
        db.write(move |conn| {
            let history = history.clone();
            let blobs = blobs.clone();
            Box::pin(async move {
                // Shared across executions, so INSERT OR IGNORE: a hash already
                // present is left untouched and the event simply references it.
                for (hash, content) in &blobs {
                    conn.execute(
                        "INSERT OR IGNORE INTO archival_blobs(hash, content, created_at)
                         VALUES (?1, ?2, unixepoch())",
                        (hash.as_str(), cairn_db::turso::Value::Blob(content.clone())),
                    )
                    .await?;
                }
                if let Some(history) = history {
                    let (pack, idx) = match history.pack {
                        Some((pack, idx)) => (
                            cairn_db::turso::Value::Blob(pack),
                            cairn_db::turso::Value::Blob(idx),
                        ),
                        None => (cairn_db::turso::Value::Null, cairn_db::turso::Value::Null),
                    };
                    conn.execute(
                        "INSERT OR REPLACE INTO execution_history
                         (execution_id, base_sha, tip_sha, pack, pack_idx, repository_id)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        (
                            history.execution_id.as_str(),
                            history.base_sha.as_str(),
                            history.tip_sha.as_str(),
                            pack,
                            idx,
                            history.repository_id.as_str(),
                        ),
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("archival history transaction failed: {e}"))?;
    }
    write_event_updates(db, updates).await
}

/// Archive a TEAM run: offload the heavy bytes to the shared per-team content
/// store by hash, then write only pointers into the synced replica.
///
/// Put-before-commit is the load-bearing ordering: every blob and the framed
/// pack are `put` to the store BEFORE any replica write, so the replica never
/// records a pointer whose object is absent. A put failure aborts here and the
/// rows stay `full`; a put that succeeds before a failed commit leaves a
/// harmless content-addressed orphan in the store.
///
/// Unlike [`apply`], this writes NO `archival_blobs` rows (the team replica has
/// no such table and must carry no blob bytes) and sets `execution_history`
/// `pack`/`pack_idx` to NULL with `pack_hash` pointing at the framed pack, whose
/// catalog entry and owner reference publish through the shared pack catalog.
pub(super) async fn apply_offloaded(
    db: &LocalDb,
    store: &dyn ContentStore,
    updates: Vec<EventUpdate>,
    history: Option<ExecHistory>,
    blobs: Vec<SegmentBlob>,
) -> Result<(), String> {
    // A blob's content hash is the sha256 of its UNCOMPRESSED bytes. The store
    // is fetched over an untrusted network, so its object must hash to its key
    // for a get to verify integrity: store the uncompressed bytes.
    for (hash, content) in &blobs {
        let original = decompress(CODEC_ZSTD_V1, content)
            .map_err(|e| format!("decompressing archival blob {hash} for offload: {e}"))?;
        store
            .put(hash, &original)
            .await
            .map_err(|e| format!("offloading archival blob {hash}: {e}"))?;
    }

    if let Some(history) = history {
        let pack_hash = match history.pack.as_ref() {
            Some((pack, idx)) => {
                let validated = crate::orchestrator::object_plane::validate_pack_bytes(
                    pack.clone(),
                )
                .map_err(|e| format!("validating execution pack before publication: {e}"))?;
                if validated.index != *idx {
                    return Err("execution pack index disagrees with independent validation".into());
                }
                crate::orchestrator::object_plane::publish_validated_pack(
                    db,
                    Some(store),
                    &validated,
                    PackCatalogPublication {
                        // Hash, size, checksum, and count are filled from the
                        // validated pack by the publisher.
                        content_hash: String::new(),
                        project_id: history.project_id.clone(),
                        repository_id: history.repository_id.clone(),
                        object_format: "sha1".into(),
                        byte_count: 0,
                        pack_checksum: String::new(),
                        object_count: 0,
                        kind: PackKind::ExecutionRange,
                        base_commit: Some(history.base_sha.clone()),
                        tip_commit: history.tip_sha.clone(),
                        owner_kind: "execution_history".into(),
                        owner_id: history.execution_id.clone(),
                    },
                )
                .await
                .map_err(|e| format!("offloading execution pack: {e}"))?;
                Some(validated.content_hash.clone())
            }
            None => None,
        };
        db.write(move |conn| {
            let history = history.clone();
            let pack_hash = pack_hash.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT OR REPLACE INTO execution_history
                     (execution_id, base_sha, tip_sha, pack, pack_idx, pack_hash, repository_id)
                     VALUES (?1, ?2, ?3, NULL, NULL, ?4, ?5)",
                    (
                        history.execution_id.as_str(),
                        history.base_sha.as_str(),
                        history.tip_sha.as_str(),
                        pack_hash.as_deref(),
                        history.repository_id.as_str(),
                    ),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("archival history transaction (offloaded) failed: {e}"))?;
    }
    write_event_updates(db, updates).await
}

/// Rewrite events through their [`ArchivedShape`] encoding, [`UPDATE_CHUNK`]
/// rows per transaction. The single writer of the six-column storage contract,
/// shared by both apply paths so they can never diverge on how a row is
/// rewritten.
///
/// The `full` guard makes every UPDATE a no-op on a row another pass already
/// archived, so concurrent teardown and sweep passes cannot double-archive.
/// `content_change_id` is cleared: no jj change id is recorded.
async fn write_event_updates(db: &LocalDb, updates: Vec<EventUpdate>) -> Result<(), String> {
    for chunk in updates.chunks(UPDATE_CHUNK) {
        let chunk = chunk.to_vec();
        db.write(move |conn| {
            let chunk = chunk.clone();
            Box::pin(async move {
                for EventUpdate { id, shape } in &chunk {
                    let cols = shape.encode();
                    let blob_value = match &cols.data_blob {
                        Some(blob) => cairn_db::turso::Value::Blob(blob.clone()),
                        None => cairn_db::turso::Value::Null,
                    };
                    conn.execute(
                        "UPDATE events SET storage_mode = ?1, content_commit = ?2,
                             content_render_sha = ?3, data = ?4, data_blob = ?5, codec = ?6,
                             content_change_id = NULL
                         WHERE id = ?7 AND (storage_mode IS NULL OR storage_mode = 'full')",
                        (
                            cols.storage_mode.as_deref(),
                            cols.content_commit.as_deref(),
                            cols.content_render_sha.as_deref(),
                            cols.data.as_str(),
                            blob_value,
                            cols.codec.as_deref(),
                            id.as_str(),
                        ),
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("archival event transaction failed: {e}"))?;
    }
    Ok(())
}
