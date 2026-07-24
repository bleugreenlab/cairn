use serde::{Deserialize, Serialize};

use super::{DbResult, LocalDb, RowExt};
use crate::storage::content_store::content_hash;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PackKind {
    Reachable,
    ExecutionRange,
    MutationDelta,
}

/// Incrementally reconstruct catalog truth for legacy execution_history pointers.
/// Each invocation handles a bounded batch so team-open latency is capped; later
/// opens continue where the prior pass stopped.
pub async fn backfill_execution_pack_catalog(db: &LocalDb, limit: usize) -> DbResult<usize> {
    let Some(store) = db.content_store().cloned() else {
        return Ok(0);
    };
    let rows = db
        .query_all(
            "SELECT h.execution_id, h.base_sha, h.tip_sha, h.pack_hash,
                    COALESCE(e.project_id, (SELECT j.project_id FROM jobs j
                                            WHERE j.execution_id = e.id LIMIT 1)),
                    h.repository_id
             FROM execution_history h
             JOIN executions e ON e.id = h.execution_id
             LEFT JOIN pack_catalog_references r
               ON r.owner_kind = 'execution_history'
              AND r.owner_id = h.execution_id
              AND r.content_hash = h.pack_hash
              AND r.repository_id = h.repository_id
              AND r.object_format = 'sha1'
             LEFT JOIN pack_catalog_backfill_attempts a
               ON a.execution_id = h.execution_id AND a.content_hash = h.pack_hash
             WHERE h.pack_hash IS NOT NULL
               AND h.repository_id IS NOT NULL
               AND r.owner_id IS NULL
               AND a.execution_id IS NULL
             ORDER BY h.execution_id LIMIT ?1",
            (limit as i64,),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.text(5)?,
                ))
            },
        )
        .await?;
    let mut published = 0;
    for (execution_id, base, tip, hash, project_id, repository_id) in rows {
        let bytes = match store.get(&hash).await.map_err(super::DbError::internal)? {
            Some(bytes) if content_hash(&bytes) == hash => bytes,
            Some(_) => {
                record_backfill_attempt(db, &execution_id, &hash, "invalid").await?;
                continue;
            }
            None => {
                record_backfill_attempt(db, &execution_id, &hash, "missing").await?;
                continue;
            }
        };
        let (pack, _) = match cairn_codec::transfer::unframe_pack(&bytes) {
            Ok(pair) => pair,
            Err(_) => {
                record_backfill_attempt(db, &execution_id, &hash, "invalid").await?;
                continue;
            }
        };
        let validated = match cairn_codec::transfer::validate_pack(
            &pack,
            cairn_codec::transfer::PackLimits::default(),
        ) {
            Ok(validated) => validated,
            Err(_) => {
                record_backfill_attempt(db, &execution_id, &hash, "invalid").await?;
                continue;
            }
        };
        publish_pack(
            db,
            PackCatalogPublication {
                content_hash: hash,
                project_id,
                repository_id,
                object_format: "sha1".into(),
                byte_count: bytes.len() as u64,
                pack_checksum: validated.manifest.pack_checksum,
                object_count: validated.manifest.object_count,
                kind: PackKind::ExecutionRange,
                base_commit: Some(base),
                tip_commit: tip,
                owner_kind: "execution_history".into(),
                owner_id: execution_id,
            },
        )
        .await?;
        published += 1;
    }
    Ok(published)
}

async fn record_backfill_attempt(
    db: &LocalDb,
    execution_id: &str,
    content_hash: &str,
    outcome: &str,
) -> DbResult<()> {
    db.execute(
        "INSERT INTO pack_catalog_backfill_attempts
         (execution_id, content_hash, attempted_at, outcome)
         VALUES (?1, ?2, unixepoch(), ?3)
         ON CONFLICT(execution_id, content_hash) DO UPDATE SET
           attempted_at = excluded.attempted_at, outcome = excluded.outcome",
        (execution_id, content_hash, outcome),
    )
    .await?;
    Ok(())
}

impl PackKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Reachable => "reachable",
            Self::ExecutionRange => "execution_range",
            Self::MutationDelta => "mutation_delta",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackCatalogPublication {
    pub content_hash: String,
    pub project_id: String,
    pub repository_id: String,
    pub object_format: String,
    pub byte_count: u64,
    pub pack_checksum: String,
    pub object_count: u64,
    pub kind: PackKind,
    pub base_commit: Option<String>,
    pub tip_commit: String,
    pub owner_kind: String,
    pub owner_id: String,
}

/// Publish metadata only after the caller has durably put and validated the
/// bytes. Duplicate publication is idempotent, but conflicting metadata for the
/// same content hash is rejected instead of silently rewriting catalog truth.
pub async fn publish_pack(db: &LocalDb, publication: PackCatalogPublication) -> DbResult<()> {
    db.write(move |conn| {
        let publication = publication.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO pack_catalog
             (content_hash, project_id, repository_id, object_format, byte_count,
              pack_checksum, object_count, kind, base_commit, tip_commit, created_at,
              publication_state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, unixepoch(), 'published')
             ON CONFLICT(project_id, repository_id, object_format, content_hash) DO NOTHING",
                (
                    publication.content_hash.as_str(),
                    publication.project_id.as_str(),
                    publication.repository_id.as_str(),
                    publication.object_format.as_str(),
                    publication.byte_count as i64,
                    publication.pack_checksum.as_str(),
                    publication.object_count as i64,
                    publication.kind.as_str(),
                    publication.base_commit.as_deref(),
                    publication.tip_commit.as_str(),
                ),
            )
            .await?;
            let matches = conn
                .query(
                    "SELECT project_id, repository_id, object_format, byte_count, pack_checksum,
                    object_count, kind, base_commit, tip_commit
             FROM pack_catalog WHERE content_hash = ?1 AND project_id = ?2
               AND repository_id = ?3 AND object_format = ?4",
                    (
                        publication.content_hash.as_str(),
                        publication.project_id.as_str(),
                        publication.repository_id.as_str(),
                        publication.object_format.as_str(),
                    ),
                )
                .await?
                .next()
                .await?
                .is_some_and(|row| {
                    row.text(0).ok().as_deref() == Some(publication.project_id.as_str())
                        && row.text(1).ok().as_deref() == Some(publication.repository_id.as_str())
                        && row.text(2).ok().as_deref() == Some(publication.object_format.as_str())
                        && row.i64(3).ok() == Some(publication.byte_count as i64)
                        && row.text(4).ok().as_deref() == Some(publication.pack_checksum.as_str())
                        && row.i64(5).ok() == Some(publication.object_count as i64)
                        && row.text(6).ok().as_deref() == Some(publication.kind.as_str())
                        && row.opt_text(7).ok().flatten().as_deref()
                            == publication.base_commit.as_deref()
                        && row.text(8).ok().as_deref() == Some(publication.tip_commit.as_str())
                });
            if !matches {
                return Err(cairn_db_error(
                    "pack catalog hash already has conflicting metadata",
                ));
            }
            if publication.owner_kind != "sealed_commit" {
                conn.execute(
                    "DELETE FROM pack_catalog_references
                     WHERE owner_kind = ?1 AND owner_id = ?2",
                    (
                        publication.owner_kind.as_str(),
                        publication.owner_id.as_str(),
                    ),
                )
                .await?;
            }
            conn.execute(
                "INSERT INTO pack_catalog_references
             (content_hash, project_id, repository_id, object_format,
              owner_kind, owner_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, unixepoch())
             ON CONFLICT(owner_kind, owner_id, project_id, repository_id) DO UPDATE SET
               content_hash = excluded.content_hash,
               project_id = excluded.project_id,
               repository_id = excluded.repository_id,
               object_format = excluded.object_format,
               created_at = excluded.created_at",
                (
                    publication.content_hash.as_str(),
                    publication.project_id.as_str(),
                    publication.repository_id.as_str(),
                    publication.object_format.as_str(),
                    publication.owner_kind.as_str(),
                    publication.owner_id.as_str(),
                ),
            )
            .await?;
            Ok(())
        })
    })
    .await
}

fn cairn_db_error(message: &str) -> super::DbError {
    super::DbError::Internal(message.to_owned())
}
