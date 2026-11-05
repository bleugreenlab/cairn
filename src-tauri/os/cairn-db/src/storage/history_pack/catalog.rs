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
///
/// Callers must exclude eviction and other publishers for this execution from
/// pack construction through publication. The SQL guards preserve an eviction
/// marker set during publication, but cannot prove the hot snapshot was complete
/// or stop an evictor from using an older catalog it read before publication.
pub async fn publish(
    db: &LocalDb,
    built: &BuiltPack,
    facts: PublishFacts,
) -> Result<(), PublishError> {
    let store = db.content_store().ok_or(PublishError::NoStore)?;
    publish_to(db, store.as_ref(), built, facts).await
}

/// [`publish`] through a named store.
pub async fn publish_to(
    db: &LocalDb,
    store: &dyn ContentStore,
    built: &BuiltPack,
    facts: PublishFacts,
) -> Result<(), PublishError> {
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
    let stored_hash = content_hash(&stored);
    if stored_hash != built.content_hash {
        return Err(PublishError::Mismatch(format!(
            "the stored content hash {stored_hash} differs from the built one {}",
            built.content_hash
        )));
    }
    let pack = HistoryPack::open(stored)?;
    pack.verify()
        .map_err(|error| PublishError::Mismatch(error.to_string()))?;
    if pack.manifest() != &built.manifest {
        return Err(PublishError::Mismatch(
            "the stored manifest differs from the built one".to_string(),
        ));
    }
    record(db, built, facts).await?;
    Ok(())
}

/// Write the catalog rows for a pack already verified in the store.
async fn record(db: &LocalDb, built: &BuiltPack, facts: PublishFacts) -> Result<(), PublishError> {
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
        if db.execute(sql, binds).await? == 0 {
            return Err(PublishError::Evicted { execution_id });
        }
    }
    let row_counts = serde_json::to_string(&built.stats.rows)
        .map_err(|error| crate::storage::DbError::internal(error.to_string()))?;
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
    if changed == 0 {
        return Err(PublishError::Evicted { execution_id });
    }
    Ok(())
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
