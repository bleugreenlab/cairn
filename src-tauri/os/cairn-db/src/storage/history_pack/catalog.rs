//! The catalog of history packs: `history_tier` and `history_tier_runs`.
//!
//! The file store cannot be enumerated, so these rows are the only list of
//! packs. [`publish`] is the one path into them, and its order is the
//! guarantee: store the object, read it back, verify it against what was
//! written, and only then write the catalog rows. The run rows go first, in
//! chunks, and the `history_tier` row last, in its own short write, which makes
//! that row the commit point. A crash before it leaves orphan run rows that the
//! read seam's join ignores and the next pack of the execution replaces, and a
//! crash before the catalog at all leaves an unreferenced object for the
//! store's collector (cairn/4648).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use turso::Value;

use super::write::BuiltPack;
use super::{HistoryPack, PackError};
use crate::storage::content_store::content_hash;
use crate::storage::{ContentStore, DbResult, LocalDb, RowExt};

/// Rows per `history_tier_runs` write.
const RUN_ROWS_PER_WRITE: usize = 500;

/// One execution's catalog row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierEntry {
    pub execution_id: String,
    pub content_hash: String,
    pub format: i64,
    pub bytes: i64,
    pub hot_bytes_estimate: i64,
    pub row_counts: BTreeMap<String, u64>,
    pub resolved_at: i64,
    pub packed_at: i64,
    pub evicted_at: Option<i64>,
}

/// When a pack was made, and what the execution holds hot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishFacts {
    /// The estimated bytes eviction would free.
    pub hot_bytes_estimate: i64,
    /// When the execution's issue resolved, epoch seconds.
    pub resolved_at: i64,
    /// When this pack was written, epoch seconds.
    pub packed_at: i64,
}

/// What a publication cost.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Published {
    /// Each catalog statement's duration, run mappings first and the
    /// `history_tier` row last.
    pub catalog_writes: Vec<Duration>,
}

/// Why a pack was not catalogued. A failed attempt can leave an unreferenced
/// object or run mappings, but cannot replace an evicted catalog.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error(
        "execution {execution_id} has already been evicted; its history pack cannot be replaced"
    )]
    Evicted { execution_id: String },
    #[error("this database has no archive store")]
    NoStore,
    #[error("the store did not return the object it just took")]
    ReadBackMissing,
    #[error("the stored pack does not match what was written: {0}")]
    Mismatch(String),
    #[error(transparent)]
    Pack(#[from] PackError),
    #[error(transparent)]
    Db(#[from] crate::storage::DbError),
}

/// Store a built pack, verify the stored copy, and catalog it.
///
/// The stored bytes must hash to the built object's content hash, open, verify
/// against their own manifest, and carry the manifest that was built, so a
/// catalog row never names an unverified pack or bytes stored under a wrong key.
/// An evicted execution's catalog is immutable, even if the same pack is offered.
/// Verification decompresses and hashes every frame, so it runs on the blocking
/// pool, inside an `archival/pack` runtime region.
///
/// Callers must exclude eviction and other publishers for this execution from
/// pack construction through publication. The SQL guards preserve an eviction
/// marker set during publication, but cannot prove the hot snapshot was complete
/// or stop an evictor from using an older catalog it read before publication.
pub async fn publish(
    db: &LocalDb,
    built: &BuiltPack,
    facts: PublishFacts,
) -> Result<Published, PublishError> {
    let store = db.content_store().ok_or(PublishError::NoStore)?;
    publish_to(db, store.as_ref(), built, facts).await
}

/// [`publish`] through a named store.
pub async fn publish_to(
    db: &LocalDb,
    store: &dyn ContentStore,
    built: &BuiltPack,
    facts: PublishFacts,
) -> Result<Published, PublishError> {
    let execution_id = &built.manifest.execution_id;
    if tier_entry(db, execution_id)
        .await?
        .is_some_and(|entry| entry.evicted_at.is_some())
    {
        return Err(PublishError::Evicted {
            execution_id: execution_id.clone(),
        });
    }
    store
        .put(&built.content_hash, &built.bytes)
        .await
        .map_err(PackError::from)?;
    let stored = store
        .get(&built.content_hash)
        .await
        .map_err(PackError::from)?
        .ok_or(PublishError::ReadBackMissing)?;
    let (expected_hash, expected_manifest) = (built.content_hash.clone(), built.manifest.clone());
    tokio::task::spawn_blocking(move || verify_stored(stored, &expected_hash, &expected_manifest))
        .await
        .map_err(|error| {
            PublishError::Mismatch(format!("the verification task failed: {error}"))
        })??;
    let catalog_writes = record(db, built, facts).await?;
    Ok(Published { catalog_writes })
}

/// The stored bytes hash to their key, open, verify against their own
/// manifest, and carry the manifest that was built.
fn verify_stored(
    stored: Vec<u8>,
    expected_hash: &str,
    expected_manifest: &super::Manifest,
) -> Result<(), PublishError> {
    let _region = cairn_platform::runtime_regions::enter("archival/pack");
    let stored_hash = content_hash(&stored);
    if stored_hash != expected_hash {
        return Err(PublishError::Mismatch(format!(
            "the stored content hash {stored_hash} differs from the built one {expected_hash}"
        )));
    }
    let pack = HistoryPack::open(stored)?;
    pack.verify()
        .map_err(|error| PublishError::Mismatch(error.to_string()))?;
    if pack.manifest() != expected_manifest {
        return Err(PublishError::Mismatch(
            "the stored manifest differs from the built one".to_string(),
        ));
    }
    Ok(())
}

/// Write the catalog rows for a pack already verified in the store, timing
/// each statement.
async fn record(
    db: &LocalDb,
    built: &BuiltPack,
    facts: PublishFacts,
) -> Result<Vec<Duration>, PublishError> {
    let mut writes = Vec::new();
    let execution_id = built.manifest.execution_id.clone();
    let run_ids: Vec<String> = built
        .manifest
        .runs
        .iter()
        .map(|run| run.id.clone())
        .collect();
    for chunk in run_ids.chunks(RUN_ROWS_PER_WRITE) {
        let placeholders = (0..chunk.len())
            .map(|index| format!("(?{})", index + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR REPLACE INTO history_tier_runs(run_id, execution_id)
             SELECT column1, ?1 FROM (VALUES {placeholders})
             WHERE NOT EXISTS (
                 SELECT 1 FROM history_tier WHERE execution_id = ?1 AND evicted_at IS NOT NULL
             )"
        );
        let mut binds = vec![Value::Text(execution_id.clone())];
        binds.extend(chunk.iter().cloned().map(Value::Text));
        let started = Instant::now();
        let changed = db.execute(sql, binds).await?;
        writes.push(started.elapsed());
        if changed == 0 {
            return Err(PublishError::Evicted { execution_id });
        }
    }
    let row_counts = serde_json::to_string(&built.stats.rows)
        .map_err(|error| crate::storage::DbError::internal(error.to_string()))?;
    let started = Instant::now();
    let changed = db
        .execute(
            "INSERT INTO history_tier
            (execution_id, content_hash, format, bytes, hot_bytes_estimate, row_counts,
             resolved_at, packed_at, evicted_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)
         ON CONFLICT(execution_id) DO UPDATE SET
             content_hash = excluded.content_hash,
             format = excluded.format,
             bytes = excluded.bytes,
             hot_bytes_estimate = excluded.hot_bytes_estimate,
             row_counts = excluded.row_counts,
             resolved_at = excluded.resolved_at,
             packed_at = excluded.packed_at
         WHERE history_tier.evicted_at IS NULL",
            vec![
                Value::Text(execution_id.clone()),
                Value::Text(built.content_hash.clone()),
                Value::Integer(i64::from(built.manifest.format)),
                Value::Integer(built.bytes.len() as i64),
                Value::Integer(facts.hot_bytes_estimate),
                Value::Text(row_counts),
                Value::Integer(facts.resolved_at),
                Value::Integer(facts.packed_at),
            ],
        )
        .await?;
    writes.push(started.elapsed());
    if changed == 0 {
        return Err(PublishError::Evicted { execution_id });
    }
    Ok(writes)
}

/// Drop the catalog row of the unevicted pack holding `event_id`, so the next
/// pack pass packs its execution again. Returns the execution whose row went.
///
/// A pack is frozen as of its pass: an event withheld then is stored as its
/// notice. When a quarantine is released, the hot row is the truth again and
/// the pack must be rebuilt before eviction can rely on it. The object stays
/// in the store for its collector (cairn/4648); its run mappings are orphans
/// the read seam ignores and the next publication replaces. An evicted
/// execution's catalog is never touched here: the hot row is gone, and
/// repacking it is eviction's concern.
pub async fn invalidate_for_event(db: &LocalDb, event_id: &str) -> DbResult<Option<String>> {
    if !db.has_history_catalog() {
        return Ok(None);
    }
    let Some(execution_id) = db
        .query_opt(
            "SELECT j.execution_id FROM events e
             JOIN runs r ON r.id = e.run_id
             JOIN jobs j ON j.id = r.job_id
             WHERE e.id = ?1",
            (event_id.to_string(),),
            |row| row.opt_text(0),
        )
        .await?
        .flatten()
    else {
        return Ok(None);
    };
    let dropped = db
        .execute(
            "DELETE FROM history_tier WHERE execution_id = ?1 AND evicted_at IS NULL",
            (execution_id.clone(),),
        )
        .await?;
    Ok((dropped > 0).then_some(execution_id))
}

/// One execution's catalog row, if it has been packed.
pub async fn tier_entry(db: &LocalDb, execution_id: &str) -> DbResult<Option<TierEntry>> {
    db.query_opt(
        "SELECT execution_id, content_hash, format, bytes, hot_bytes_estimate, row_counts,
                resolved_at, packed_at, evicted_at
         FROM history_tier WHERE execution_id = ?1",
        (execution_id.to_string(),),
        |row| {
            Ok(TierEntry {
                execution_id: row.text(0)?,
                content_hash: row.text(1)?,
                format: row.i64(2)?,
                bytes: row.i64(3)?,
                hot_bytes_estimate: row.i64(4)?,
                row_counts: serde_json::from_str(&row.text(5)?).unwrap_or_default(),
                resolved_at: row.i64(6)?,
                packed_at: row.i64(7)?,
                evicted_at: row.opt_i64(8)?,
            })
        },
    )
    .await
}
