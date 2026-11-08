//! The catalog of history packs: `history_tier`, keyed by scope, and its two
//! mappings, `history_tier_runs` and `history_tier_sessions`.
//!
//! The file store cannot be enumerated, so these rows are the only list of
//! packs. [`publish`] is the one path into them, and its order is the
//! guarantee: store the object, read it back, verify it against what was
//! written, and only then write the catalog rows. The run mappings go first,
//! then the session mappings, in chunks, and the `history_tier` row last, in its
//! own short write, which makes that row the commit point. A crash before it
//! leaves orphan mappings that the read seam's join ignores and the next pack of
//! the scope replaces, and a crash before the catalog at all leaves an
//! unreferenced object for the store's collector (cairn/4648).
//!
//! The mappings are the only way a read reaches a pack: a run by its own row, a
//! session by its own row, and a turn through its run (or its session, when it
//! has no run). Nothing resolves through `jobs.execution_id`, so a run or a
//! session no pack mapped reads hot whatever its execution's catalog says.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use turso::Value;

use super::write::BuiltPack;
use super::{HistoryPack, PackError, ScopeKind};
use crate::storage::content_store::content_hash;
use crate::storage::{ContentStore, DbResult, LocalDb, RowExt};

/// Rows per mapping write.
const MAPPING_ROWS_PER_WRITE: usize = 500;

/// What a catalog row is keyed by and who owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierScope {
    pub kind: ScopeKind,
    /// `history_tier.scope_id`: the execution id, or
    /// `<session job id>/<first sequence>-<last sequence>` for a thread range.
    pub id: String,
    /// The execution, or the thread's session job.
    pub owner_id: String,
    /// The compaction generation a thread range was cut at.
    pub through_generation: Option<i64>,
}

impl TierScope {
    /// An execution scope: its id is its owner.
    pub fn execution(execution_id: &str) -> TierScope {
        TierScope {
            kind: ScopeKind::Execution,
            id: execution_id.to_string(),
            owner_id: execution_id.to_string(),
            through_generation: None,
        }
    }
}

/// One scope's catalog row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierEntry {
    pub scope_id: String,
    pub kind: ScopeKind,
    pub owner_id: String,
    pub content_hash: String,
    pub format: i64,
    pub bytes: i64,
    pub hot_bytes_estimate: i64,
    pub row_counts: BTreeMap<String, u64>,
    pub resolved_at: i64,
    pub packed_at: i64,
    pub evicted_at: Option<i64>,
    pub through_generation: Option<i64>,
}

/// When a pack was made, and what the scope holds hot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishFacts {
    /// The estimated bytes eviction would free.
    pub hot_bytes_estimate: i64,
    /// When the scope stopped changing, epoch seconds: an execution's issue
    /// resolution, or a thread range's last session closing.
    pub resolved_at: i64,
    /// When this pack was written, epoch seconds.
    pub packed_at: i64,
}

/// What a publication cost.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Published {
    /// Each catalog statement's duration: run mappings, then session mappings,
    /// and the `history_tier` row last.
    pub catalog_writes: Vec<Duration>,
}

/// Why a pack was not catalogued. A failed attempt can leave an unreferenced
/// object or mappings, but cannot replace an evicted catalog or take another
/// catalogued scope's runs or sessions.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("{kind} {scope_id} has already been evicted; its history pack cannot be replaced")]
    Evicted { kind: ScopeKind, scope_id: String },
    /// A run or session the pack covers is mapped to another catalogued
    /// scope. One run or session belongs to one live scope; that scope's
    /// row must be dropped before another can take it.
    #[error("{kind} {scope_id} covers a {what} already mapped to another catalogued scope")]
    Claimed {
        kind: ScopeKind,
        scope_id: String,
        what: &'static str,
    },
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

/// Store a built pack, verify the stored copy, and catalog it under the scope
/// it was built for.
///
/// The stored bytes must hash to the built object's content hash, open, verify
/// against their own manifest, and carry the manifest that was built, so a
/// catalog row never names an unverified pack or bytes stored under a wrong key.
/// An evicted scope's catalog is immutable, even if the same pack is offered.
/// Verification decompresses and hashes every frame, so it runs on the blocking
/// pool, inside an `archival/pack` runtime region.
///
/// Callers must exclude eviction and other publishers for this scope from
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
    let scope = &built.scope;
    if tier_entry(db, &scope.id)
        .await?
        .is_some_and(|entry| entry.evicted_at.is_some())
    {
        return Err(evicted(scope));
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

fn evicted(scope: &TierScope) -> PublishError {
    PublishError::Evicted {
        kind: scope.kind,
        scope_id: scope.id.clone(),
    }
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

/// The mapping statement for one chunk of `count` ids, bound from `?2`, with
/// the scope at `?1`. It writes the whole chunk or none of it: none when the
/// scope has been evicted, and none when any id is already mapped to another
/// scope that still has a catalog row. An orphan mapping, whose scope has no
/// row, is replaced. Under execution keying a run could only ever be mapped by
/// its own execution; once many scopes can cover one job, this guard is what
/// keeps a later publication from taking an evicted range's runs, which would
/// send their reads hot to rows that are gone.
pub(crate) fn mapping_sql(table: &str, key: &str, count: usize) -> String {
    let placeholders = (0..count)
        .map(|index| format!("(?{})", index + 2))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT OR REPLACE INTO {table}({key}, scope_id)
         SELECT column1, ?1 FROM (VALUES {placeholders})
         WHERE NOT EXISTS (
             SELECT 1 FROM history_tier WHERE scope_id = ?1 AND evicted_at IS NOT NULL
         )
         AND NOT EXISTS (
             SELECT 1 FROM (VALUES {placeholders}) wanted
             JOIN {table} held ON held.{key} = wanted.column1
             JOIN history_tier owner ON owner.scope_id = held.scope_id
             WHERE held.scope_id <> ?1
         )"
    )
}

/// Why [`map_to_scope`] stopped.
enum MappingRefused {
    Evicted,
    Claimed,
}

/// Map `ids` to `scope_id` in `table`, in chunks. Returns each statement's
/// duration, or why the guard refused a chunk.
async fn map_to_scope(
    db: &LocalDb,
    table: &str,
    key: &str,
    scope_id: &str,
    ids: &[String],
) -> DbResult<Result<Vec<Duration>, MappingRefused>> {
    let mut writes = Vec::new();
    for chunk in ids.chunks(MAPPING_ROWS_PER_WRITE) {
        let mut binds = vec![Value::Text(scope_id.to_string())];
        binds.extend(chunk.iter().cloned().map(Value::Text));
        let started = Instant::now();
        let changed = db
            .execute(mapping_sql(table, key, chunk.len()), binds)
            .await?;
        writes.push(started.elapsed());
        if changed < chunk.len() as u64 {
            let evicted = tier_entry(db, scope_id)
                .await?
                .is_some_and(|entry| entry.evicted_at.is_some());
            return Ok(Err(if evicted {
                MappingRefused::Evicted
            } else {
                MappingRefused::Claimed
            }));
        }
    }
    Ok(Ok(writes))
}

/// Write the catalog rows for a pack already verified in the store, timing
/// each statement.
///
/// The sessions mapped are the scope's own, the manifest sessions with a job:
/// a session some event names but whose job lies outside the scope belongs to
/// another scope, and mapping it here would send its reads to a pack that holds
/// only part of it.
async fn record(
    db: &LocalDb,
    built: &BuiltPack,
    facts: PublishFacts,
) -> Result<Vec<Duration>, PublishError> {
    let scope = &built.scope;
    let run_ids: Vec<String> = built
        .manifest
        .runs
        .iter()
        .map(|run| run.id.clone())
        .collect();
    let session_ids: Vec<String> = built
        .manifest
        .sessions
        .iter()
        .filter(|session| session.job_id.is_some())
        .map(|session| session.id.clone())
        .collect();
    let mut writes = Vec::new();
    for (table, key, what, ids) in [
        ("history_tier_runs", "run_id", "run", &run_ids),
        (
            "history_tier_sessions",
            "session_id",
            "session",
            &session_ids,
        ),
    ] {
        match map_to_scope(db, table, key, &scope.id, ids).await? {
            Ok(timed) => writes.extend(timed),
            Err(MappingRefused::Evicted) => return Err(evicted(scope)),
            Err(MappingRefused::Claimed) => {
                return Err(PublishError::Claimed {
                    kind: scope.kind,
                    scope_id: scope.id.clone(),
                    what,
                })
            }
        }
    }
    let row_counts = serde_json::to_string(&built.stats.rows)
        .map_err(|error| crate::storage::DbError::internal(error.to_string()))?;
    let started = Instant::now();
    let changed = db
        .execute(
            "INSERT INTO history_tier
            (scope_id, kind, owner_id, content_hash, format, bytes, hot_bytes_estimate,
             row_counts, resolved_at, packed_at, evicted_at, through_generation)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?11)
         ON CONFLICT(scope_id) DO UPDATE SET
             content_hash = excluded.content_hash,
             format = excluded.format,
             bytes = excluded.bytes,
             hot_bytes_estimate = excluded.hot_bytes_estimate,
             row_counts = excluded.row_counts,
             resolved_at = excluded.resolved_at,
             packed_at = excluded.packed_at,
             through_generation = excluded.through_generation
         WHERE history_tier.evicted_at IS NULL",
            vec![
                Value::Text(scope.id.clone()),
                Value::Text(scope.kind.as_str().to_string()),
                Value::Text(scope.owner_id.clone()),
                Value::Text(built.content_hash.clone()),
                Value::Integer(i64::from(built.manifest.format)),
                Value::Integer(built.bytes.len() as i64),
                Value::Integer(facts.hot_bytes_estimate),
                Value::Text(row_counts),
                Value::Integer(facts.resolved_at),
                Value::Integer(facts.packed_at),
                scope.through_generation.map_or(Value::Null, Value::Integer),
            ],
        )
        .await?;
    writes.push(started.elapsed());
    if changed == 0 {
        return Err(evicted(scope));
    }
    Ok(writes)
}

/// Drop the catalog row of the unevicted pack holding `event_id`, and its
/// mappings, so the next pack pass packs its scope again. Returns the scope
/// whose row went.
///
/// A pack is frozen as of its pass: an event withheld then is stored as its
/// notice. When a quarantine is released, the hot row is the truth again and
/// the pack must be rebuilt before eviction can rely on it. The scope is found
/// through the event's run mapping, the same path a read takes. The object
/// stays in the store for its collector (cairn/4648). The mappings go with the
/// row, in the same transaction, because a thread range is eligible only while
/// its sessions are unmapped. An evicted scope's catalog is never touched here:
/// the hot row is gone, and repacking it is eviction's concern.
pub async fn invalidate_for_event(db: &LocalDb, event_id: &str) -> DbResult<Option<String>> {
    if !db.has_history_catalog() {
        return Ok(None);
    }
    let Some(scope_id) = db
        .query_opt(
            "SELECT m.scope_id FROM events e
             JOIN history_tier_runs m ON m.run_id = e.run_id
             WHERE e.id = ?1",
            (event_id.to_string(),),
            |row| row.text(0),
        )
        .await?
    else {
        return Ok(None);
    };
    let id = scope_id.clone();
    let dropped = db
        .write(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let dropped = conn
                    .execute(
                        "DELETE FROM history_tier WHERE scope_id = ?1 AND evicted_at IS NULL",
                        (id.as_str(),),
                    )
                    .await?;
                for table in ["history_tier_runs", "history_tier_sessions"] {
                    conn.execute(
                        &format!(
                            "DELETE FROM {table} WHERE scope_id = ?1
                             AND NOT EXISTS (SELECT 1 FROM history_tier WHERE scope_id = ?1)"
                        ),
                        (id.as_str(),),
                    )
                    .await?;
                }
                Ok(dropped)
            })
        })
        .await?;
    Ok((dropped > 0).then_some(scope_id))
}

/// One scope's catalog row, if it has been packed.
pub async fn tier_entry(db: &LocalDb, scope_id: &str) -> DbResult<Option<TierEntry>> {
    db.query_opt(
        "SELECT scope_id, kind, owner_id, content_hash, format, bytes, hot_bytes_estimate,
                row_counts, resolved_at, packed_at, evicted_at, through_generation
         FROM history_tier WHERE scope_id = ?1",
        (scope_id.to_string(),),
        |row| {
            let kind = row.text(1)?;
            Ok(TierEntry {
                scope_id: row.text(0)?,
                kind: ScopeKind::parse(&kind).ok_or_else(|| {
                    crate::storage::DbError::internal(format!(
                        "history_tier names an unknown scope kind {kind:?}"
                    ))
                })?,
                owner_id: row.text(2)?,
                content_hash: row.text(3)?,
                format: row.i64(4)?,
                bytes: row.i64(5)?,
                hot_bytes_estimate: row.i64(6)?,
                row_counts: serde_json::from_str(&row.text(7)?).unwrap_or_default(),
                resolved_at: row.i64(8)?,
                packed_at: row.i64(9)?,
                evicted_at: row.opt_i64(10)?,
                through_generation: row.opt_i64(11)?,
            })
        },
    )
    .await
}
