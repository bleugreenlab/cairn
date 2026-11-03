use super::*;

use crate::storage::pack_catalog::{PackCatalogPublication, PackKind};

/// Event UPDATEs per transaction. Bounds a single transaction's size no matter
/// how long the transcript is, and how long one chunk holds the write lock and
/// the runtime worker stepping it: the embedded engine rarely suspends inside a
/// statement, so a chunk runs as one long poll.
const UPDATE_CHUNK: usize = 100;

/// Apply an archival: put the heavy bytes to the database's content store by
/// hash, then write only pointers into the database.
///
/// Put-before-commit is the load-bearing ordering: every blob and the framed
/// pack are `put` to the store BEFORE any database write, so the database never
/// records a pointer whose object is absent. A put failure aborts here and the
/// rows stay `full`; a put that succeeds before a failed commit leaves a
/// harmless content-addressed orphan in the store.
///
/// No `archival_blobs` rows are written, and `execution_history` keeps
/// `pack`/`pack_idx` NULL with `pack_hash` pointing at the framed pack, whose
/// catalog entry and owner reference publish through the pack catalog. The
/// `execution_history` row commits before the event rewrites, so a gitcoord row
/// can never reference an absent pack.
pub(super) async fn apply(
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
                store
                    .put(&validated.content_hash, &validated.framed)
                    .await
                    .map_err(|e| format!("offloading execution pack: {e}"))?;
                // The catalog entry serves object resolution, not
                // reconstruction, which reads the pack by `pack_hash`. Identical
                // pack bytes already catalogued under another execution's base
                // and tip are refused as conflicting metadata; that must not
                // cost this execution its archival.
                if let Err(error) = crate::orchestrator::object_plane::publish_validated_reference(
                    db,
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
                {
                    log::warn!(
                        "archival: execution {}'s pack {} is stored but not catalogued: {error}",
                        history.execution_id,
                        validated.content_hash
                    );
                }
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
        .map_err(|e| format!("archival history transaction failed: {e}"))?;
    }
    write_event_updates(db, updates).await
}

/// Rewrite events through their [`ArchivedShape`] encoding, [`UPDATE_CHUNK`]
/// rows per transaction. The single writer of the six-column storage contract.
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
        // Let the writers and readers queued behind this chunk run before the
        // next one takes the lock again.
        tokio::task::yield_now().await;
    }
    Ok(())
}
