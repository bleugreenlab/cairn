use super::*;

// ---------------------------------------------------------------------------
// Phase C: one transaction — execution_history insert + all event UPDATEs.
// ---------------------------------------------------------------------------

pub(super) async fn apply(
    db: &LocalDb,
    updates: Vec<EventUpdate>,
    history: Option<ExecHistory>,
    blobs: Vec<SegmentBlob>,
) -> Result<(), String> {
    db.write(move |conn| {
        let updates = updates.clone();
        let history = history.clone();
        let blobs = blobs.clone();
        Box::pin(async move {
            // Content-addressed segment blobs first. Shared across executions, so
            // INSERT OR IGNORE: a hash already present (this or another execution)
            // is left untouched and the event simply references it.
            for (hash, content) in &blobs {
                conn.execute(
                    "INSERT OR IGNORE INTO archival_blobs(hash, content, created_at)
                     VALUES (?1, ?2, unixepoch())",
                    (hash.as_str(), cairn_db::turso::Value::Blob(content.clone())),
                )
                .await?;
            }
            if let Some(history) = history {
                match history.pack {
                    Some((pack, idx)) => {
                        conn.execute(
                            "INSERT OR REPLACE INTO execution_history
                             (execution_id, base_sha, tip_sha, pack, pack_idx)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            (
                                history.execution_id.as_str(),
                                history.base_sha.as_str(),
                                history.tip_sha.as_str(),
                                pack,
                                idx,
                            ),
                        )
                        .await?;
                    }
                    None => {
                        conn.execute(
                            "INSERT OR REPLACE INTO execution_history
                             (execution_id, base_sha, tip_sha, pack, pack_idx)
                             VALUES (?1, ?2, ?3, NULL, NULL)",
                            (
                                history.execution_id.as_str(),
                                history.base_sha.as_str(),
                                history.tip_sha.as_str(),
                            ),
                        )
                        .await?;
                    }
                }
            }
            write_event_updates(conn, &updates).await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("archival transaction failed: {e}"))
}

/// Apply each event's archived-shape UPDATE within an already-open transaction.
/// The single writer of the six-column storage contract
/// ([`crate::storage::events::encoding::ArchivedShape::encode`]), shared by the inline ([`apply`])
/// and offloaded ([`apply_offloaded`]) paths so the two can never diverge on how
/// a row is rewritten. `content_change_id` rides along as orthogonal provenance.
async fn write_event_updates(
    conn: &cairn_db::turso::Connection,
    updates: &[EventUpdate],
) -> DbResult<()> {
    for EventUpdate {
        id,
        shape,
        change_id,
    } in updates
    {
        // The shape encodes to its exact six column values; one uniform UPDATE
        // binds them so the writer can never set a combination the reader's
        // `decode` would reject.
        let cols = shape.encode();
        let blob_value = match &cols.data_blob {
            Some(blob) => cairn_db::turso::Value::Blob(blob.clone()),
            None => cairn_db::turso::Value::Null,
        };
        conn.execute(
            "UPDATE events SET storage_mode = ?1, content_commit = ?2,
                 content_render_sha = ?3, data = ?4, data_blob = ?5, codec = ?6,
                 content_change_id = ?7
             WHERE id = ?8",
            (
                cols.storage_mode.as_deref(),
                cols.content_commit.as_deref(),
                cols.content_render_sha.as_deref(),
                cols.data.as_str(),
                blob_value,
                cols.codec.as_deref(),
                change_id.as_deref(),
                id.as_str(),
            ),
        )
        .await?;
    }
    Ok(())
}

/// Archive a TEAM run: offload the heavy bytes to the shared per-team content
/// store by hash, then write only pointers into the synced replica.
///
/// Put-before-commit is the load-bearing ordering: every blob and the framed
/// pack are `put` to the store BEFORE the DB transaction, so the replica never
/// records a `pack_hash`/segment-hash pointer whose object is absent. A put
/// failure aborts here and the caller leaves the rows `full` (the teardown
/// fail-soft contract); a put that succeeds before a failed commit leaves
/// harmless content-addressed orphans in the store (GC-able — store GC is out of
/// scope, matching `archival_blobs`).
///
/// Unlike [`apply`], this writes NO `archival_blobs` rows (the team replica has
/// no such table and must carry no blob bytes) and sets `execution_history`
/// `pack`/`pack_idx` to NULL with `pack_hash` pointing at the framed pack object.
pub(super) async fn apply_offloaded(
    db: &LocalDb,
    store: &dyn ContentStore,
    updates: Vec<EventUpdate>,
    history: Option<ExecHistory>,
    blobs: Vec<SegmentBlob>,
) -> Result<(), String> {
    // 1. Offload heavy bytes. A blob's content hash is the sha256 of its
    //    UNCOMPRESSED segment bytes (`archival_blobs` stores the compressed form
    //    keyed by that same hash). The content store is fetched over an untrusted
    //    network, so its object MUST hash to its key for a get to verify
    //    integrity — therefore store the uncompressed bytes (whose sha256 is the
    //    key), not the compressed `content`. Reconstruct's team path uses them
    //    directly; the local path keeps decompressing `archival_blobs`.
    for (hash, content) in &blobs {
        let original = decompress(CODEC_ZSTD_V1, content)
            .map_err(|e| format!("decompressing archival blob {hash} for offload: {e}"))?;
        store
            .put(hash, &original)
            .await
            .map_err(|e| format!("offloading archival blob {hash}: {e}"))?;
    }
    // The pack (when the range was non-empty) becomes one framed store object
    // keyed by the sha256 of its framed bytes — `pack_hash` addresses exactly
    // what is stored, so a fetch can verify integrity.
    let pack_hash = match history.as_ref().and_then(|h| h.pack.as_ref()) {
        Some((pack, idx)) => {
            let framed = frame_pack(pack, idx);
            let hash = content_hash(&framed);
            store
                .put(&hash, &framed)
                .await
                .map_err(|e| format!("offloading execution pack {hash}: {e}"))?;
            Some(hash)
        }
        None => None,
    };

    // 2. One DB transaction: the pointer row + the event UPDATEs. No
    //    `archival_blobs` insert.
    db.write(move |conn| {
        let updates = updates.clone();
        let history = history.clone();
        let pack_hash = pack_hash.clone();
        Box::pin(async move {
            if let Some(history) = history {
                conn.execute(
                    "INSERT OR REPLACE INTO execution_history
                     (execution_id, base_sha, tip_sha, pack, pack_idx, pack_hash)
                     VALUES (?1, ?2, ?3, NULL, NULL, ?4)",
                    (
                        history.execution_id.as_str(),
                        history.base_sha.as_str(),
                        history.tip_sha.as_str(),
                        pack_hash.as_deref(),
                    ),
                )
                .await?;
            }
            write_event_updates(conn, &updates).await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("archival transaction (offloaded) failed: {e}"))
}
