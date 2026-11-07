//! The archive offload: moves the archival bytes a private database still holds
//! inline out of `cairn.db` and into its file archive.
//!
//! It runs on the archival sweep's interval, its first pass
//! [`FIRST_PASS_DELAY`] after boot.
//!
//! Archival writes heavy bytes to the content store and keeps only pointers in
//! the database (see [`super::rewrite`]). Executions archived before the private
//! database had a store still carry them inline: a git range pack in
//! `execution_history.pack`/`pack_idx`, and zstd segment blobs in
//! `archival_blobs`. This pass moves both, in two phases, each keyset-paged and
//! resumable from its `archival_progress` cursor:
//!
//! 1. **Inline packs**, one execution per page: validate the pack and frame it,
//!    put the framed object, read it back and compare, publish its pack-catalog
//!    entry, then one short write points the row at the object and clears the
//!    inline columns. A crash anywhere before that write leaves a duplicate
//!    object and an inline row that is still authoritative, so the row is simply
//!    redone on resume. Reconstruction prefers the inline columns while both
//!    exist, so a row reads the same in every state.
//! 2. **`archival_blobs`**, at most [`OffloadBudget::blob_page_rows`] rows or
//!    [`OffloadBudget::blob_page_bytes`] stored bytes per page: decompress each
//!    segment, check it hashes to its key, put the uncompressed bytes, read them
//!    back, then delete exactly those hashes in one short write. A crash before
//!    the delete leaves the row and the object both; reconstruction reads the
//!    object first and the row second.
//!
//! Each page takes and releases the process-wide archival permit, so a
//! teardown's archival never waits behind more than one page. Decompression
//! and pack validation run on the blocking pool. Deletes are by key, never
//! `IN (SELECT ...)` (cairn/5332). Between pages the pass folds the logical log
//! only when it is due (past [`OffloadBudget::fold_at_log_bytes`]) and no
//! earlier checkpoint attempt is still in the engine; while one is, the pass
//! keeps moving pages and skips the fold rather than waiting on it. Small pages
//! make small folds.
//!
//! A row whose bytes cannot be moved (a pack that fails validation, a blob that
//! does not hash to its key) is logged, left where it is, and passed over; the
//! cursor resets once a phase reaches its end, so the next pass retries it. A
//! store failure (an offline archive) ends the pass with its cursor where it
//! was.

use std::fmt;
use std::time::{Duration, Instant};

use super::pacing::{acquire_gate, fold_if_due, FoldTally};
use super::progress::{self, Progress};
use crate::orchestrator::object_plane::{
    publish_validated_reference, validate_pack_bytes, ValidatedObjectPack,
};
use crate::storage::content_store::{content_hash, ContentStore};
use crate::storage::pack_catalog::{PackCatalogPublication, PackKind};
use crate::storage::{decompress, LocalDb, RowExt, CODEC_ZSTD_V1, FOLD_AT_LOG_BYTES};

/// How long after boot the first pass waits, so a backlog's first pass never
/// lands in the boot convoy.
pub const FIRST_PASS_DELAY: Duration = Duration::from_secs(15 * 60);

/// The environment variable an operator sets to stop the offload without a
/// rebuild. Read once, when the archival maintenance loop is spawned; see
/// [`super::switch_enabled`] for the values that turn it off.
pub const OFFLOAD_ENV: &str = "CAIRN_ARCHIVE_OFFLOAD";

/// How large one page may grow, and when the log is folded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffloadBudget {
    pub blob_page_rows: usize,
    /// Stored (compressed) bytes; a page always takes at least one row.
    pub blob_page_bytes: usize,
    pub fold_at_log_bytes: u64,
}

impl OffloadBudget {
    pub const PRODUCTION: Self = Self {
        blob_page_rows: 100,
        blob_page_bytes: 32 * 1024 * 1024,
        fold_at_log_bytes: FOLD_AT_LOG_BYTES,
    };
}

/// The most rows the remaining-work probe counts; past it the summary says
/// "more than".
const REMAINING_PROBE: usize = 1_000;

/// What one pass did.
#[derive(Debug, Default, Clone)]
pub struct OffloadReport {
    pub packs_moved: usize,
    pub pack_bytes: u64,
    /// Packs moved without a pack-catalog entry (no owning project, or a
    /// catalog conflict). Reconstruction does not need the entry.
    pub packs_uncatalogued: usize,
    pub packs_skipped: usize,
    pub blobs_moved: usize,
    pub blob_bytes: u64,
    pub blobs_skipped: usize,
    pub pages: usize,
    /// The most rows any one page carried.
    pub page_rows_max: usize,
    /// Each data write's duration (repoint or delete), in milliseconds.
    pub write_ms: Vec<u64>,
    pub gate_wait: Duration,
    pub fold: FoldTally,
    pub remaining_packs: Option<usize>,
    pub remaining_blobs: Option<usize>,
    /// Why the pass stopped early, if it did.
    pub error: Option<String>,
    pub duration: Duration,
}

impl OffloadReport {
    fn write_percentile(&self, pct: usize) -> u64 {
        if self.write_ms.is_empty() {
            return 0;
        }
        let mut sorted = self.write_ms.clone();
        sorted.sort_unstable();
        sorted[(sorted.len() - 1) * pct / 100]
    }

    /// Whether there was nothing left to move when the pass ended.
    pub fn drained(&self) -> bool {
        self.remaining_packs == Some(0) && self.remaining_blobs == Some(0)
    }
}

fn remaining(count: Option<usize>) -> String {
    match count {
        Some(n) if n > REMAINING_PROBE => format!("more than {REMAINING_PROBE}"),
        Some(n) => n.to_string(),
        None => "unknown".into(),
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

impl fmt::Display for OffloadReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} packs ({}, {} uncatalogued, {} skipped) and {} blobs ({}, {} skipped) in {} pages \
             (max {} rows); writes p50 {} ms, max {} ms; gate wait {} ms; {} folds (max {} ms, \
             {} skipped, {} failed); log peak {}; remaining {} packs, {} blobs; {:.1} s",
            self.packs_moved,
            mib(self.pack_bytes),
            self.packs_uncatalogued,
            self.packs_skipped,
            self.blobs_moved,
            mib(self.blob_bytes),
            self.blobs_skipped,
            self.pages,
            self.page_rows_max,
            self.write_percentile(50),
            self.write_percentile(100),
            self.gate_wait.as_millis(),
            self.fold.folds,
            self.fold.max.as_millis(),
            self.fold.skipped,
            self.fold.failed,
            mib(self.fold.log_peak_bytes),
            remaining(self.remaining_packs),
            remaining(self.remaining_blobs),
            self.duration.as_secs_f64(),
        )?;
        if let Some(error) = &self.error {
            write!(f, "; stopped: {error}")?;
        }
        Ok(())
    }
}

/// Why one row did not move.
enum RowError {
    /// The row's own bytes are unusable: skip it and carry on.
    Data(String),
    /// The store or the database failed: end the pass.
    Halt(String),
}

/// Run one offload pass over the private database to completion, or until the
/// store or database fails.
pub async fn run_offload_pass(db: &LocalDb, budget: OffloadBudget) -> OffloadReport {
    let started = Instant::now();
    let mut report = OffloadReport::default();
    report.fold.observe(db);
    match (db.team_content_store(), db.content_store().cloned()) {
        (Some(_), _) => report.error = Some("a team replica holds nothing inline".into()),
        (None, None) => report.error = Some("this database has no content store".into()),
        (None, Some(store)) => {
            let outcome = match offload_packs(db, store.as_ref(), budget, &mut report).await {
                Ok(()) => offload_blobs(db, store.as_ref(), budget, &mut report).await,
                halted => halted,
            };
            if let Err(error) = outcome {
                report.error = Some(error);
            }
        }
    }
    report.remaining_packs = probe(
        db,
        "SELECT execution_id FROM execution_history WHERE pack IS NOT NULL LIMIT ?1",
    )
    .await;
    report.remaining_blobs = probe(db, "SELECT hash FROM archival_blobs LIMIT ?1").await;
    report.duration = started.elapsed();
    report
}

/// Rows left, counted to one past [`REMAINING_PROBE`] rather than by `COUNT(*)`.
async fn probe(db: &LocalDb, sql: &str) -> Option<usize> {
    db.query_all(sql, ((REMAINING_PROBE + 1) as i64,), |row| row.text(0))
        .await
        .ok()
        .map(|rows| rows.len())
}

async fn save_cursor(db: &LocalDb, pass: &str, cursor: Option<String>) -> Result<(), String> {
    progress::save(
        db,
        pass,
        &Progress {
            watermark: None,
            cursor,
        },
    )
    .await
    .map_err(|error| format!("saving the {pass} cursor: {error}"))
}

async fn load_cursor(db: &LocalDb, pass: &str) -> Result<String, String> {
    Ok(progress::load(db, pass)
        .await
        .map_err(|error| format!("loading the {pass} cursor: {error}"))?
        .cursor
        .unwrap_or_default())
}

/// One `execution_history` row still carrying its pack inline.
struct InlinePack {
    execution_id: String,
    base_sha: String,
    tip_sha: String,
    pack: Vec<u8>,
    idx: Option<Vec<u8>>,
    project_id: Option<String>,
    repository_id: Option<String>,
}

async fn next_inline_pack(db: &LocalDb, after: &str) -> Result<Option<InlinePack>, String> {
    db.query_opt(
        "SELECT h.execution_id, h.base_sha, h.tip_sha, h.pack, h.pack_idx,
                (SELECT j.project_id FROM jobs j JOIN projects p ON p.id = j.project_id
                  WHERE j.execution_id = h.execution_id LIMIT 1),
                COALESCE(h.repository_id,
                         (SELECT COALESCE(p.repository_id, p.id) FROM jobs j
                            JOIN projects p ON p.id = j.project_id
                           WHERE j.execution_id = h.execution_id LIMIT 1))
         FROM execution_history h
         WHERE h.execution_id > ?1 AND h.pack IS NOT NULL
         ORDER BY h.execution_id LIMIT 1",
        (after.to_string(),),
        |row| {
            Ok(InlinePack {
                execution_id: row.text(0)?,
                base_sha: row.text(1)?,
                tip_sha: row.text(2)?,
                pack: row.opt_blob(3)?.unwrap_or_default(),
                idx: row.opt_blob(4)?,
                project_id: row.opt_text(5)?,
                repository_id: row.opt_text(6)?,
            })
        },
    )
    .await
    .map_err(|error| format!("reading the next inline pack: {error}"))
}

async fn offload_packs(
    db: &LocalDb,
    store: &dyn ContentStore,
    budget: OffloadBudget,
    report: &mut OffloadReport,
) -> Result<(), String> {
    let mut cursor = load_cursor(db, progress::OFFLOAD_HISTORY).await?;
    loop {
        let permit = acquire_gate(&mut report.gate_wait).await?;
        let Some(row) = next_inline_pack(db, &cursor).await? else {
            drop(permit);
            // The end: start over next pass, so a skipped row is retried.
            return save_cursor(db, progress::OFFLOAD_HISTORY, None).await;
        };
        let execution_id = row.execution_id.clone();
        match move_pack(db, store, row, report).await {
            Ok(()) => {}
            Err(RowError::Data(error)) => {
                log::warn!(
                    "archive offload: execution {execution_id}'s pack stays inline: {error}"
                );
                report.packs_skipped += 1;
            }
            Err(RowError::Halt(error)) => return Err(error),
        }
        drop(permit);
        report.pages += 1;
        report.page_rows_max = report.page_rows_max.max(1);
        cursor = execution_id;
        save_cursor(db, progress::OFFLOAD_HISTORY, Some(cursor.clone())).await?;
        log::debug!("archive offload: pack page done through {cursor}");
        tokio::task::yield_now().await;
        fold_if_due(
            db,
            budget.fold_at_log_bytes,
            &mut report.fold,
            "archive offload",
        )
        .await;
    }
}

async fn move_pack(
    db: &LocalDb,
    store: &dyn ContentStore,
    row: InlinePack,
    report: &mut OffloadReport,
) -> Result<(), RowError> {
    let InlinePack {
        execution_id,
        base_sha,
        tip_sha,
        pack,
        idx,
        project_id,
        repository_id,
    } = row;
    let Some(idx) = idx else {
        return Err(RowError::Data("the pack has no index".into()));
    };
    let inline_bytes = (pack.len() + idx.len()) as u64;
    let validated: ValidatedObjectPack = tokio::task::spawn_blocking(move || {
        let validated = validate_pack_bytes(pack)?;
        if validated.index != idx {
            return Err("the stored index disagrees with independent validation".to_string());
        }
        Ok(validated)
    })
    .await
    .map_err(|error| RowError::Data(format!("pack validation task failed: {error}")))?
    .map_err(RowError::Data)?;
    let hash = validated.content_hash.clone();

    store
        .put(&hash, &validated.framed)
        .await
        .map_err(|error| RowError::Halt(format!("putting pack {hash}: {error}")))?;
    match store.get(&hash).await {
        Ok(Some(back)) if back == validated.framed => {}
        Ok(_) => {
            return Err(RowError::Halt(format!(
                "pack {hash} did not read back after its put"
            )))
        }
        Err(error) => return Err(RowError::Halt(format!("reading back pack {hash}: {error}"))),
    }

    let catalogued = match (project_id, repository_id) {
        (Some(project_id), Some(repository_id)) => publish_validated_reference(
            db,
            &validated,
            PackCatalogPublication {
                content_hash: String::new(),
                project_id,
                repository_id,
                object_format: "sha1".into(),
                byte_count: 0,
                pack_checksum: String::new(),
                object_count: 0,
                kind: PackKind::ExecutionRange,
                base_commit: Some(base_sha),
                tip_commit: tip_sha,
                owner_kind: "execution_history".into(),
                owner_id: execution_id.clone(),
            },
        )
        .await
        .map_err(|error| log::warn!("archive offload: cataloguing pack {hash}: {error}"))
        .is_ok(),
        _ => false,
    };
    if !catalogued {
        report.packs_uncatalogued += 1;
    }

    let started = Instant::now();
    db.execute(
        "UPDATE execution_history SET pack = NULL, pack_idx = NULL, pack_hash = ?2
         WHERE execution_id = ?1 AND pack IS NOT NULL",
        (execution_id.clone(), hash),
    )
    .await
    .map_err(|error| RowError::Halt(format!("repointing {execution_id}: {error}")))?;
    report.write_ms.push(started.elapsed().as_millis() as u64);
    report.packs_moved += 1;
    report.pack_bytes += inline_bytes;
    Ok(())
}

async fn next_blob_page(
    db: &LocalDb,
    after: &str,
    budget: OffloadBudget,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let after = after.to_string();
    db.read(move |conn| {
        let after = after.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT hash, content FROM archival_blobs WHERE hash > ?1
                     ORDER BY hash LIMIT ?2",
                    (after.as_str(), budget.blob_page_rows as i64),
                )
                .await?;
            let mut page = Vec::new();
            let mut bytes = 0usize;
            while let Some(row) = rows.next().await? {
                let content = row.opt_blob(1)?.unwrap_or_default();
                bytes += content.len();
                page.push((row.text(0)?, content));
                if bytes >= budget.blob_page_bytes {
                    break;
                }
            }
            Ok(page)
        })
    })
    .await
    .map_err(|error| format!("reading the next archival_blobs page: {error}"))
}

async fn offload_blobs(
    db: &LocalDb,
    store: &dyn ContentStore,
    budget: OffloadBudget,
    report: &mut OffloadReport,
) -> Result<(), String> {
    let mut cursor = load_cursor(db, progress::OFFLOAD_BLOBS).await?;
    loop {
        let permit = acquire_gate(&mut report.gate_wait).await?;
        let page = next_blob_page(db, &cursor, budget).await?;
        let Some(last) = page.last().map(|(hash, _)| hash.clone()) else {
            drop(permit);
            return save_cursor(db, progress::OFFLOAD_BLOBS, None).await;
        };
        report.page_rows_max = report.page_rows_max.max(page.len());

        let decoded = tokio::task::spawn_blocking(move || {
            page.into_iter()
                .map(|(hash, stored)| {
                    let original = decompress(CODEC_ZSTD_V1, &stored)
                        .map_err(|error| format!("does not decompress: {error}"))
                        .and_then(|bytes| {
                            if content_hash(&bytes) == hash {
                                Ok(bytes)
                            } else {
                                Err("does not hash to its key".to_string())
                            }
                        });
                    (hash, original)
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|error| format!("segment decompression task failed: {error}"))?;

        let mut moved: Vec<String> = Vec::new();
        let mut moved_bytes = 0u64;
        for (hash, original) in decoded {
            let bytes = match original {
                Ok(bytes) => bytes,
                Err(error) => {
                    log::warn!(
                        "archive offload: segment blob {hash} stays in archival_blobs: {error}"
                    );
                    report.blobs_skipped += 1;
                    continue;
                }
            };
            store
                .put(&hash, &bytes)
                .await
                .map_err(|error| format!("putting segment blob {hash}: {error}"))?;
            match store.get(&hash).await {
                Ok(Some(back)) if back == bytes => {}
                Ok(_) => {
                    return Err(format!(
                        "segment blob {hash} did not read back after its put"
                    ))
                }
                Err(error) => return Err(format!("reading back segment blob {hash}: {error}")),
            }
            moved_bytes += bytes.len() as u64;
            moved.push(hash);
        }

        if !moved.is_empty() {
            let count = moved.len();
            let started = Instant::now();
            db.write(move |conn| {
                let moved = moved.clone();
                Box::pin(async move {
                    for hash in &moved {
                        conn.execute(
                            "DELETE FROM archival_blobs WHERE hash = ?1",
                            (hash.as_str(),),
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
            .map_err(|error| format!("deleting offloaded archival_blobs rows: {error}"))?;
            report.write_ms.push(started.elapsed().as_millis() as u64);
            report.blobs_moved += count;
            report.blob_bytes += moved_bytes;
        }
        drop(permit);
        report.pages += 1;
        cursor = last;
        save_cursor(db, progress::OFFLOAD_BLOBS, Some(cursor.clone())).await?;
        log::debug!("archive offload: blob page done through {cursor}");
        tokio::task::yield_now().await;
        fold_if_due(
            db,
            budget.fold_at_log_bytes,
            &mut report.fold,
            "archive offload",
        )
        .await;
    }
}

#[cfg(test)]
#[path = "offload_tests.rs"]
mod tests;
