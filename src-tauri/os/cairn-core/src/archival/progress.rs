//! Where each archival maintenance pass resumes, persisted in
//! `archival_progress` so a restart picks up where the last process stopped.

use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

/// The sweep's resolution watermark.
pub(crate) const SWEEP: &str = "sweep";
/// The offload of inline `execution_history` packs: the last execution id
/// done.
pub(crate) const OFFLOAD_HISTORY: &str = "offload_history";
/// The offload of `archival_blobs` rows: the last hash done.
pub(crate) const OFFLOAD_BLOBS: &str = "offload_blobs";
/// The history pack pass: the `(completed_at, execution_id)` of the last
/// candidate visited, as `watermark` and `cursor`.
pub(crate) const PACK: &str = "pack";
/// Thread-range walk: last session job fully visited, independent of executions.
pub(crate) const PACK_THREAD: &str = "pack_thread";
/// The archive root this database's stored bytes live in, recorded when an
/// archive is first opened (its `cursor`). Once set, boot never creates an
/// archive in place of a missing one.
pub(crate) const ARCHIVE_ROOT: &str = "archive";

/// One pass's saved position.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Progress {
    pub watermark: Option<i64>,
    pub cursor: Option<String>,
}

pub(crate) async fn load(db: &LocalDb, pass: &str) -> DbResult<Progress> {
    Ok(db
        .query_opt(
            "SELECT watermark, cursor FROM archival_progress WHERE pass = ?1",
            (pass.to_string(),),
            |row| {
                Ok(Progress {
                    watermark: row.opt_i64(0)?,
                    cursor: row.opt_text(1)?,
                })
            },
        )
        .await?
        .unwrap_or_default())
}

pub(crate) async fn save(db: &LocalDb, pass: &str, progress: &Progress) -> DbResult<()> {
    db.execute(
        "INSERT INTO archival_progress(pass, watermark, cursor, updated_at)
         VALUES (?1, ?2, ?3, unixepoch())
         ON CONFLICT(pass) DO UPDATE SET watermark = excluded.watermark,
             cursor = excluded.cursor, updated_at = excluded.updated_at",
        params![
            pass.to_string(),
            progress.watermark,
            progress.cursor.clone()
        ],
    )
    .await
    .map(drop)
}
