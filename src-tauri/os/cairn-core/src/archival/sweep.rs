//! The recurring archival sweep: catch-up for executions teardown did not fully
//! archive.
//!
//! Teardown archives an issue's executions when it resolves. The sweep covers
//! everything else that is already immutable and still `full`: events recorded
//! after teardown ran, executions whose teardown archival failed, and history
//! from before the writer was restored. A candidate is an execution whose issue
//! is resolved (`merged` or `closed`) and was resolved more than
//! [`RESOLUTION_GRACE_SECS`] ago, so a sweep never races teardown or a late
//! event. Each one is archived with [`archive_execution`] and no range: the
//! branch is usually gone, so only the zstd and blobbed paths apply, and no
//! `execution_history` is written.
//!
//! ## Finding candidates
//!
//! `events` has no index on `storage_mode`, and most of its bytes belong to
//! terminal issues. Candidates are therefore found through `issues`, `jobs`,
//! `runs`, and `idx_events_run_id`, never by scanning `events`, and a pass only
//! considers issues resolved since the last fully successful pass in this
//! process (the watermark). The first pass after startup scans every resolved
//! issue once; later passes see only new resolutions. A reopened issue clears
//! `completed_at` and stamps a new one when it resolves again, so it re-enters
//! the window. A pass with any failure leaves the watermark in place so the
//! next pass retries. Within a pass, candidates are paged by
//! `(completed_at, execution_id)` so no page rescans an earlier one.
//!
//! The sweep only visits the private database. Team replicas are archived by
//! teardown alone.
//!
//! ## File-size reclamation: the offline `vacuum_reclaim` utility
//!
//! Compressing events only moves pages onto the freelist; the database file does
//! not shrink until it is rewritten. In-place reclamation is **not safely
//! available in this embedded Turso build** (measured during CAIRN-1555):
//!
//! - In-place `VACUUM` requires a TRUNCATE WAL checkpoint under MVCC, and that
//!   checkpoint (and the VACUUM) corrupt on the real multi-table schema with
//!   deleted rows (`MVCC delete: rowid N not found`).
//! - `auto_vacuum=INCREMENTAL` is unreachable: the SDK builder exposes no
//!   `enable_autovacuum`, and `auto_vacuum` is silently ignored on a non-empty
//!   database anyway.
//! - The one working primitive, `VACUUM INTO`, writes a fresh self-contained
//!   compacted image to a *separate* path. Reclaiming therefore means swapping a
//!   three-file MVCC set (`.db` + `-wal` + `-log`, where committed data lives in
//!   the sidecars) while the database is closed — its blast radius is
//!   whole-database loss, so the swap runs offline behind a retained backup.
//!
//! Because freed pages are reused, the file plateaus at its high-water mark
//! rather than growing unbounded, so reclamation is a one-time, offline cleanup
//! rather than in-app machinery. With the app quit the database is quiescent, so
//! the `vacuum_reclaim` cargo example (`examples/vacuum_reclaim.rs`) does the
//! whole job: `VACUUM INTO` a staged image, gate on `integrity_check`, then swap
//! the three-file set in place behind a `.vacuum-backup` set. See
//! docs/database.md. The `archival_backfill_state` table (migration 0055) is no
//! longer read: the sweep needs no completion marker.

use std::collections::HashMap;
use std::sync::LazyLock;

use super::rewrite::{archive_execution, ArchiveSummary, ArchiveTarget};
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::params;

/// How long after an issue resolves before the sweep may archive it.
pub const RESOLUTION_GRACE_SECS: i64 = 60 * 60;

/// How often the orchestrator runs a sweep pass.
pub const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(6 * 60 * 60);

/// Candidate executions fetched per page.
const PAGE_SIZE: i64 = 50;

/// Per-database watermarks (keyed by [`LocalDb::instance`]): the resolution
/// time below which every candidate has already been archived. The mutex also
/// serializes passes on one database.
static WATERMARKS: LazyLock<tokio::sync::Mutex<HashMap<u64, i64>>> =
    LazyLock::new(Default::default);

/// Aggregate outcome of one sweep pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepSummary {
    /// Executions archived (including ones that turned out to have nothing
    /// left to rewrite).
    pub executions: usize,
    /// Executions whose archival failed; their rows stay `full` for a retry.
    pub failed: usize,
    pub archive: ArchiveSummary,
}

/// Run one sweep pass over `db` at the current time.
pub async fn run_archival_sweep(db: &LocalDb) -> Result<SweepSummary, String> {
    run_archival_sweep_at(db, chrono::Utc::now().timestamp(), RESOLUTION_GRACE_SECS).await
}

/// One sweep pass at an explicit clock and grace period.
pub(crate) async fn run_archival_sweep_at(
    db: &LocalDb,
    now: i64,
    grace_secs: i64,
) -> Result<SweepSummary, String> {
    let mut watermarks = WATERMARKS.lock().await;
    let since = watermarks.get(&db.instance()).copied().unwrap_or(i64::MIN);
    let cutoff = now - grace_secs;
    let summary = sweep(db, since, cutoff).await?;
    if summary.failed == 0 {
        watermarks.insert(db.instance(), cutoff);
    }
    Ok(summary)
}

/// Archive every candidate resolved in `[since, cutoff)`.
async fn sweep(db: &LocalDb, since: i64, cutoff: i64) -> Result<SweepSummary, String> {
    let mut summary = SweepSummary::default();
    let mut after: Option<(i64, String)> = None;
    loop {
        let page = load_candidates(db, since, cutoff, after.as_ref())
            .await
            .map_err(|e| format!("loading archival sweep candidates: {e}"))?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        for (_, execution_id) in page {
            let target = ArchiveTarget {
                execution_id: execution_id.clone(),
                ranges: Vec::new(),
            };
            match archive_execution(db, target).await {
                Ok(outcome) => {
                    summary.executions += 1;
                    summary.archive.absorb(&outcome);
                }
                Err(error) => {
                    log::warn!("archival sweep: execution {execution_id} failed: {error}");
                    summary.failed += 1;
                }
            }
            tokio::task::yield_now().await;
        }
        after = Some(last);
    }
    Ok(summary)
}

/// The next page of `(completed_at, execution_id)` candidates after `after`.
async fn load_candidates(
    db: &LocalDb,
    since: i64,
    cutoff: i64,
    after: Option<&(i64, String)>,
) -> DbResult<Vec<(i64, String)>> {
    let (after_at, after_execution) = match after {
        Some((at, execution)) => (*at, execution.clone()),
        None => (since, String::new()),
    };
    db.query_all(
        "SELECT DISTINCT i.completed_at, j.execution_id
         FROM issues i JOIN jobs j ON j.issue_id = i.id
         WHERE i.status IN ('merged', 'closed')
           AND i.completed_at IS NOT NULL
           AND i.completed_at >= ?1 AND i.completed_at < ?2
           AND (i.completed_at > ?3 OR (i.completed_at = ?3 AND j.execution_id > ?4))
           AND j.execution_id IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM runs r JOIN events e ON e.run_id = r.id
               WHERE r.job_id = j.id
                 AND (e.storage_mode IS NULL OR e.storage_mode = 'full'))
         ORDER BY i.completed_at ASC, j.execution_id ASC
         LIMIT ?5",
        params![since, cutoff, after_at, after_execution, PAGE_SIZE],
        |row| Ok((row.i64(0)?, row.text(1)?)),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Event;
    use crate::runs::queries::{event_from_row, EVENT_COLUMNS};
    use crate::storage::event_fixture::{assistant_text, user_text};
    use crate::storage::{migrated_test_db, reconstruct_events};

    const NOW: i64 = 100_000;
    const GRACE: i64 = 3_600;

    async fn seed_project(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj','ws','P','p','/nonexistent','main',1,1);",
        )
        .await
        .unwrap();
    }

    /// One issue with one execution, job, run, and two full events.
    async fn seed_issue(db: &LocalDb, n: i64, status: &str, completed_at: Option<i64>) {
        let script = format!(
            "INSERT INTO issues(id, project_id, number, title, status, completed_at, created_at, updated_at)
               VALUES ('i{n}','proj',{n},'T','{status}',{completed},1,1);
             INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('x{n}','r','complete',1);
             INSERT INTO jobs(id, execution_id, project_id, issue_id, status, created_at, updated_at)
               VALUES ('j{n}','x{n}','proj','i{n}','complete',1,1);
             INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
               VALUES ('r{n}','j{n}','proj','exited',1,1);",
            completed = completed_at.map_or("NULL".to_string(), |at| at.to_string()),
        );
        db.execute_script(&script).await.unwrap();
        insert_event(
            db,
            &format!("u{n}"),
            &format!("r{n}"),
            1,
            &user_text("task"),
        )
        .await;
        insert_event(
            db,
            &format!("a{n}"),
            &format!("r{n}"),
            2,
            &assistant_text("done"),
        )
        .await;
    }

    async fn insert_event(db: &LocalDb, id: &str, run: &str, seq: i64, data: &str) {
        let event_type = if id.starts_with('u') {
            "user"
        } else {
            "assistant"
        };
        db.execute(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?3)",
            params![id, run, seq, event_type, data],
        )
        .await
        .unwrap();
    }

    async fn mode(db: &LocalDb, id: &str) -> String {
        db.query_one(
            "SELECT COALESCE(storage_mode, 'full') FROM events WHERE id = ?1",
            (id.to_string(),),
            |row| row.text(0),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn sweep_archives_only_issues_resolved_past_the_grace_period() {
        let db = migrated_test_db("archival-sweep-eligibility.db").await;
        seed_project(&db).await;
        seed_issue(&db, 1, "merged", Some(1_000)).await;
        seed_issue(&db, 2, "closed", Some(2_000)).await;
        seed_issue(&db, 3, "merged", Some(NOW - 60)).await; // inside the grace period
        seed_issue(&db, 4, "active", None).await; // open, or reopened (completed_at cleared)

        let summary = run_archival_sweep_at(&db, NOW, GRACE).await.unwrap();
        assert_eq!(summary.executions, 2);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.archive.zstd, 4);
        for id in ["u1", "a1", "u2", "a2"] {
            assert_eq!(mode(&db, id).await, "zstd", "{id}");
        }
        for id in ["u3", "a3", "u4", "a4"] {
            assert_eq!(mode(&db, id).await, "full", "{id}");
        }

        let rows: Vec<Event> = db
            .query_all(
                format!("SELECT {EVENT_COLUMNS} FROM events WHERE id = 'u1'"),
                (),
                event_from_row,
            )
            .await
            .unwrap();
        let restored = reconstruct_events(&db, rows).await;
        assert_eq!(restored[0].data, user_text("task"));
    }

    #[tokio::test]
    async fn later_passes_see_only_new_resolutions() {
        let db = migrated_test_db("archival-sweep-watermark.db").await;
        seed_project(&db).await;
        seed_issue(&db, 1, "merged", Some(1_000)).await;
        assert_eq!(
            run_archival_sweep_at(&db, NOW, GRACE)
                .await
                .unwrap()
                .executions,
            1
        );

        let again = run_archival_sweep_at(&db, NOW, GRACE).await.unwrap();
        assert_eq!(again, SweepSummary::default(), "a second pass is a no-op");

        // An issue resolved after the last cutoff is picked up once it ages past
        // the grace period.
        seed_issue(&db, 2, "closed", Some(NOW - 60)).await;
        let later = run_archival_sweep_at(&db, NOW + GRACE, GRACE)
            .await
            .unwrap();
        assert_eq!(later.executions, 1);
        assert_eq!(mode(&db, "u2").await, "zstd");
    }

    #[tokio::test]
    async fn sweep_pages_past_one_page() {
        let db = migrated_test_db("archival-sweep-pages.db").await;
        seed_project(&db).await;
        let total = PAGE_SIZE + 5;
        for n in 1..=total {
            // Shared resolution instants exercise the execution-id tie-break.
            seed_issue(&db, n, "merged", Some(1_000 + n % 3)).await;
        }
        let summary = run_archival_sweep_at(&db, NOW, GRACE).await.unwrap();
        assert_eq!(summary.executions as i64, total);
        assert_eq!(summary.archive.zstd as i64, total * 2);
    }

    /// A teardown and a sweep racing on one execution archive each row exactly
    /// once, and every row still round-trips.
    #[tokio::test]
    async fn concurrent_passes_never_double_archive() {
        let db = migrated_test_db("archival-sweep-race.db").await;
        seed_project(&db).await;
        seed_issue(&db, 1, "closed", Some(1_000)).await;
        let target = || ArchiveTarget {
            execution_id: "x1".to_string(),
            ranges: Vec::new(),
        };
        let (left, right) = tokio::join!(
            archive_execution(&db, target()),
            archive_execution(&db, target())
        );
        left.unwrap();
        right.unwrap();
        let rows: Vec<Event> = db
            .query_all(
                format!("SELECT {EVENT_COLUMNS} FROM events ORDER BY sequence"),
                (),
                event_from_row,
            )
            .await
            .unwrap();
        assert!(rows
            .iter()
            .all(|e| e.storage_mode.as_deref() == Some("zstd")));
        let restored = reconstruct_events(&db, rows).await;
        assert_eq!(restored[0].data, user_text("task"));
        assert_eq!(restored[1].data, assistant_text("done"));
    }
}
