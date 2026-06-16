//! One-time, idempotent archival backfill for already-torn-down executions.
//!
//! The teardown writer ([`super::rewrite::archive_target`]) archives an
//! execution's events at the moment its worktree is removed. That only covers
//! executions torn down *after* the writer shipped; the bulk of historical event
//! bytes belong to executions whose worktrees are already gone. The dividing line
//! between what the two writers can apply is the *addressing scheme*, not the
//! event — **git-addressed paths need the live worktree; hash-addressed paths
//! apply in both writers**:
//!
//! - **Git-addressed paths need the live worktree.** A gitcoord read or write
//!   resolves bytes from objects reachable only while the worktree (and its
//!   branch and `jobs.base_commit`) still exist. For a torn-down execution those
//!   are gone by construction, so this backfill gives everything git-addressed the
//!   **zstd backstop**: no replay, no pack, no gitcoord.
//! - **Hash-addressed paths apply in both writers.** The two content-addressed
//!   forms — the near-constant `system:init` handshake and the assembled
//!   `system:prompt` — reconstruct from blobs in `archival_blobs` keyed purely by
//!   content, needing no worktree, no branch, and no commit. They reconstruct
//!   identically whether stripped at teardown or swept up here. Both are
//!   high-volume system events, so deduping the historical backlog (not just
//!   future teardowns) is the bulk of their win. A byte-exact verify gates each
//!   one, so a row that does not reassemble exactly falls back to the zstd
//!   backstop like any other; a `system:prompt` recorded before its `raw.segments`
//!   boundary map shipped (CAIRN-1562) has no map to segment and correctly falls
//!   back too.
//!
//! The full per-path writer-coverage matrix lives in [`super`]'s module header.
//!
//! ## What counts as "unreachable by the writer"
//!
//! The writer runs from `teardown_worktrees` for jobs whose `worktree_path` is
//! still present on disk, immediately before removing the worktree. Nothing ever
//! nulls `jobs.worktree_path`, so "already torn down" is **not** a NULL path — it
//! is a path whose directory no longer exists. An execution is therefore
//! unreachable by the writer when its owning jobs either never had a worktree
//! (`worktree_path IS NULL`) or had one that has since been removed
//! (`!Path::exists`). Anything whose worktree still exists is left alone: it is
//! live or will be archived at its own teardown.
//!
//! This composes safely with the writer. The writer only rewrites `full` events
//! and removes the worktree *after* it finishes, so by the time a directory is
//! gone its events are no longer `full` and this backfill skips them. A writer
//! that failed (rows left `full`) and then lost its worktree is correctly swept
//! up here as a plain zstd fallback.
//!
//! ## Completion + reclamation
//!
//! A single backfill pass compresses every eligible-bucket event — there is no
//! per-row deferral — so the backfill is marked complete (in
//! `archival_backfill_state`) immediately after its first pass and never
//! re-scans.
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
//! rather than in-app machinery. With the app quit the database is quiescent (no
//! write-loss window), so the `vacuum_reclaim` cargo example
//! (`examples/vacuum_reclaim.rs`) does the whole job: `VACUUM INTO` a staged
//! image, gate on `integrity_check`, then swap the three-file set in place behind
//! a `.vacuum-backup` set — no startup blocking and no crash-safe swap state
//! machine. See docs/database.md. The reserved
//! `archival_backfill_state.vacuum_completed_at` column (migration 0055) is now
//! unused by app code; the offline utility does not touch it. Backfilling is
//! still the substantive win: it removes the ~2 GB of logical event bytes and
//! frees those pages for reuse, so the file stops growing even before it is
//! compacted.

use std::path::Path;

use super::compress;
use super::encoding::ArchivedShape;
use super::rewrite::{
    build_system_init_shape, build_system_prompt_shape, build_tool_map, event_tool_use_id,
    normalize_tool_name, push_blobbed_or_zstd, zstd_stub, SegmentBlob, SystemBlobKind,
    SystemBlobSink,
};
use crate::models::Event;
use crate::runs::queries::{event_from_row, EVENT_COLUMNS};
use crate::storage::{DbResult, LocalDb, RowExt};

/// Rows compressed per transaction. One batch is one transaction.
const BATCH_SIZE: i64 = 500;

/// Aggregate outcome of a backfill pass, for the summary log line.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackfillSummary {
    pub rows_compressed: usize,
    /// Near-constant `system:init` events content-addressed to a shared skeleton
    /// plus a deduped tool set instead of whole-event zstd. Counted separately so
    /// the blobbed win is visible, not buried in `rows_compressed`.
    pub system_init: usize,
    /// Assembled `system:prompt` events content-addressed to shared static-segment
    /// blobs (the per-run dynamic tail inlined) instead of whole-event zstd. Only
    /// rows carrying a `raw.segments` boundary map qualify; the rest fall to zstd.
    /// Counted separately for the same reason as `system_init`.
    pub system_prompt: usize,
    pub bytes_before: usize,
    pub bytes_after: usize,
}

/// Run the one-time archival backfill, gated so it runs once.
///
/// Best-effort and non-blocking: spawn it on a background task at startup. A
/// single pass compresses every eligible-bucket event, then marks the backfill
/// complete so it never re-scans. File-size reclamation is a separate one-time,
/// offline step run via the `vacuum_reclaim` example (see the module docs).
pub async fn run_archival_maintenance(db: &LocalDb) -> Result<(), String> {
    if backfill_complete(db)
        .await
        .map_err(|e| format!("reading archival backfill state: {e}"))?
    {
        return Ok(());
    }

    let summary = run_backfill(db).await?;
    log::info!(
        "archival backfill pass: {} rows compressed ({} system-init, {} system-prompt), {} -> {} bytes",
        summary.rows_compressed,
        summary.system_init,
        summary.system_prompt,
        summary.bytes_before,
        summary.bytes_after,
    );
    mark_backfill_complete(db)
        .await
        .map_err(|e| format!("marking archival backfill complete: {e}"))?;
    Ok(())
}

/// Drain every eligible worktree bucket and return the pass summary. A single
/// pass compresses all eligible-bucket events — there is no per-row deferral — so
/// the caller marks the backfill complete unconditionally afterward.
async fn run_backfill(db: &LocalDb) -> Result<BackfillSummary, String> {
    let buckets = candidate_buckets(db)
        .await
        .map_err(|e| format!("loading archival candidate buckets: {e}"))?;

    let mut summary = BackfillSummary::default();
    for bucket in buckets {
        if !is_eligible_bucket(bucket.as_deref()) {
            // Worktree still on disk: live, or the writer's territory at teardown.
            continue;
        }
        drain_bucket(db, bucket.as_deref(), &mut summary).await?;
    }
    Ok(summary)
}

/// A worktree path is eligible when it never existed (`None`) or its directory is
/// gone. An empty string is treated as "never had a worktree".
fn is_eligible_bucket(worktree_path: Option<&str>) -> bool {
    match worktree_path {
        None | Some("") => true,
        Some(path) => !Path::new(path).exists(),
    }
}

/// Compress every compressible event in one bucket, one transaction per batch,
/// until the bucket is empty.
async fn drain_bucket(
    db: &LocalDb,
    worktree_path: Option<&str>,
    summary: &mut BackfillSummary,
) -> Result<(), String> {
    loop {
        let batch = load_bucket_batch(db, worktree_path, BATCH_SIZE)
            .await
            .map_err(|e| format!("loading archival backfill batch: {e}"))?;
        if batch.is_empty() {
            break;
        }
        let (updates, blobs) = classify(&batch, summary)?;
        apply(db, updates, blobs).await?;
    }
    Ok(())
}

/// Classify a batch into archival updates, reusing the teardown writer's helpers
/// so backfilled rows are byte-shaped identically to teardown-archived ones.
///
/// Most rows get the zstd backstop (no git coordinates exist for a torn-down
/// execution). The exceptions are the two hash-addressed system events —
/// `system:prompt` and `system:init` — which are self-contained in
/// `archival_blobs` and need no worktree, so they are content-addressed here
/// exactly as at teardown (a byte-exact verify gates each, falling back to zstd on
/// any mismatch). This reclaims the bulk of the historical system-event backlog
/// the writer can never reach. A `system:prompt` lacking a `raw.segments` map
/// simply fails the shape and falls to zstd.
///
/// The tool-name label on a `tool_result` zstd stub is resolved from the batch's
/// own assistant events (adjacent in `created_at` order); a label that lands
/// across a batch boundary is cosmetic only — reconstruction restores the full
/// `data` from `data_blob` regardless.
fn classify(
    batch: &[Event],
    summary: &mut BackfillSummary,
) -> Result<(Vec<BackfillUpdate>, Vec<SegmentBlob>), String> {
    let tool_map = build_tool_map(batch);
    let mut updates = Vec::with_capacity(batch.len());
    let mut blobs: Vec<SegmentBlob> = Vec::new();
    for event in batch {
        summary.bytes_before += event.data.len();

        // Assembled system prompts are hash-addressed: their static segments live
        // in `archival_blobs` keyed by content, independent of git, so they dedup
        // here exactly as at teardown. Only rows recorded WITH a `raw.segments`
        // boundary map qualify; older rows without it fail the shape and fall to
        // the zstd backstop below — correct, not forced.
        if event.event_type == "system:prompt" {
            let mut sink = BackfillSystemBlobSink {
                updates: &mut updates,
                summary,
                blobs: &mut blobs,
            };
            push_blobbed_or_zstd(
                event,
                SystemBlobKind::Prompt,
                build_system_prompt_shape,
                &mut sink,
            )?;
            continue;
        }

        if event.event_type == "system:init" {
            let mut sink = BackfillSystemBlobSink {
                updates: &mut updates,
                summary,
                blobs: &mut blobs,
            };
            push_blobbed_or_zstd(
                event,
                SystemBlobKind::Init,
                build_system_init_shape,
                &mut sink,
            )?;
            continue;
        }

        let tool_name = event_tool_use_id(&event.data).and_then(|id| {
            tool_map
                .get(&id)
                .map(|(name, _)| normalize_tool_name(name).to_string())
        });
        let blob = compress(event.data.as_bytes())?;
        let data = zstd_stub(event, tool_name.as_deref());
        summary.bytes_after += data.len() + blob.len();
        summary.rows_compressed += 1;
        updates.push(BackfillUpdate {
            id: event.id.clone(),
            shape: ArchivedShape::Zstd { data, blob },
        });
    }
    Ok((updates, blobs))
}

struct BackfillUpdate {
    id: String,
    shape: ArchivedShape,
}

struct BackfillSystemBlobSink<'a> {
    updates: &'a mut Vec<BackfillUpdate>,
    summary: &'a mut BackfillSummary,
    blobs: &'a mut Vec<SegmentBlob>,
}

impl SystemBlobSink for BackfillSystemBlobSink<'_> {
    fn push_blobbed(
        &mut self,
        event: &Event,
        kind: SystemBlobKind,
        shape: ArchivedShape,
        blobs: Vec<SegmentBlob>,
    ) {
        if let ArchivedShape::Blobbed { ref data, .. } = shape {
            self.summary.bytes_after += data.len();
        }
        match kind {
            SystemBlobKind::Prompt => self.summary.system_prompt += 1,
            SystemBlobKind::Init => self.summary.system_init += 1,
        }
        self.summary.rows_compressed += 1;
        self.blobs.extend(blobs);
        self.updates.push(BackfillUpdate {
            id: event.id.clone(),
            shape,
        });
    }

    fn push_zstd(&mut self, event: &Event) -> Result<(), String> {
        let blob = compress(event.data.as_bytes())?;
        let data = zstd_stub(event, None);
        self.summary.bytes_after += data.len() + blob.len();
        self.summary.rows_compressed += 1;
        self.updates.push(BackfillUpdate {
            id: event.id.clone(),
            shape: ArchivedShape::Zstd { data, blob },
        });
        Ok(())
    }
}

/// Apply one batch's rewrites in a single transaction: insert the content-
/// addressed blobs (shared via `INSERT OR IGNORE`, exactly as the teardown
/// writer), then UPDATE each row through its [`ArchivedShape`] encoding so
/// backfilled rows are column-identical to teardown-archived ones.
async fn apply(
    db: &LocalDb,
    updates: Vec<BackfillUpdate>,
    blobs: Vec<SegmentBlob>,
) -> Result<(), String> {
    if updates.is_empty() {
        return Ok(());
    }
    db.write(move |conn| {
        let updates = updates
            .iter()
            .map(|u| (u.id.clone(), u.shape.clone()))
            .collect::<Vec<_>>();
        let blobs = blobs.clone();
        Box::pin(async move {
            for (hash, content) in &blobs {
                conn.execute(
                    "INSERT OR IGNORE INTO archival_blobs(hash, content, created_at)
                     VALUES (?1, ?2, unixepoch())",
                    turso::params![hash.as_str(), turso::Value::Blob(content.clone())],
                )
                .await?;
            }
            for (id, shape) in &updates {
                let cols = shape.encode();
                let blob_value = match &cols.data_blob {
                    Some(blob) => turso::Value::Blob(blob.clone()),
                    None => turso::Value::Null,
                };
                conn.execute(
                    "UPDATE events SET storage_mode = ?1, content_commit = ?2,
                         content_render_sha = ?3, data = ?4, data_blob = ?5, codec = ?6
                     WHERE id = ?7",
                    turso::params![
                        cols.storage_mode.as_deref(),
                        cols.content_commit.as_deref(),
                        cols.content_render_sha.as_deref(),
                        cols.data.as_str(),
                        blob_value,
                        cols.codec.as_deref(),
                        id.as_str()
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("archival backfill batch transaction failed: {e}"))
}

// ---------------------------------------------------------------------------
// Queries.
// ---------------------------------------------------------------------------

/// Distinct `worktree_path` values (NULL included) of jobs that own at least one
/// candidate event (`full`/NULL storage with no blob). Bounded by execution count.
async fn candidate_buckets(db: &LocalDb) -> DbResult<Vec<Option<String>>> {
    db.query_all(
        "SELECT DISTINCT j.worktree_path
         FROM jobs j
         WHERE EXISTS (
             SELECT 1 FROM runs r JOIN events e ON e.run_id = r.id
             WHERE r.job_id = j.id
               AND (e.storage_mode IS NULL OR e.storage_mode = 'full')
               AND e.data_blob IS NULL
         )",
        (),
        |row| row.opt_text(0),
    )
    .await
}

/// Compressible events for one bucket: `full`/NULL storage, no blob, owned by a
/// job with this worktree path.
async fn load_bucket_batch(
    db: &LocalDb,
    worktree_path: Option<&str>,
    limit: i64,
) -> DbResult<Vec<Event>> {
    // The concrete-path bucket binds the path at ?1, so the LIMIT shifts to ?2;
    // the NULL bucket has no path bind, so LIMIT is ?1.
    let (predicate, limit_placeholder) = match worktree_path {
        Some(_) => ("j.worktree_path = ?1", "?2"),
        None => ("j.worktree_path IS NULL", "?1"),
    };
    let sql = format!(
        "SELECT {EVENT_COLUMNS} FROM events e
         WHERE (e.storage_mode IS NULL OR e.storage_mode = 'full')
           AND e.data_blob IS NULL
           AND e.run_id IN (
               SELECT r.id FROM runs r JOIN jobs j ON r.job_id = j.id
               WHERE {predicate}
           )
         ORDER BY e.created_at ASC, e.sequence ASC
         LIMIT {limit_placeholder}"
    );
    match worktree_path {
        Some(path) => {
            let path = path.to_string();
            db.query_all(sql, turso::params![path, limit], event_from_row)
                .await
        }
        None => {
            db.query_all(sql, turso::params![limit], event_from_row)
                .await
        }
    }
}

// ---------------------------------------------------------------------------
// Completion markers (single-row `archival_backfill_state`).
// ---------------------------------------------------------------------------

async fn backfill_complete(db: &LocalDb) -> DbResult<bool> {
    db.query_one(
        "SELECT backfill_completed_at FROM archival_backfill_state WHERE id = 1",
        (),
        |row| Ok(row.opt_i64(0)?.is_some()),
    )
    .await
}

async fn mark_backfill_complete(db: &LocalDb) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "UPDATE archival_backfill_state SET backfill_completed_at = ?1 WHERE id = 1",
        turso::params![now],
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archival::event_fixture::{
        assistant_read, assistant_text, system_init_claude, system_prompt, tool_result, user_text,
    };
    use crate::archival::reconstruct_events;
    use crate::archival::rewrite::{classify_system_blob_columns_for_test, SystemBlobKind};
    use crate::storage::{migrated_test_db, SearchIndex};
    use turso::params;

    /// Seed a project/execution/job/run chain. `worktree_path` is the job's
    /// recorded path: `None` (never had a worktree) and a nonexistent path are
    /// both eligible buckets; an existing directory is the writer's territory.
    async fn seed(db: &LocalDb, worktree_path: Option<&str>) {
        let worktree_path = worktree_path.map(str::to_string);
        db.write(move |conn| {
            let worktree_path = worktree_path.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('ws','w',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES('proj','ws','p','PROJ','/tmp/p','main',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, status, started_at) VALUES('exec','r','running',1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, project_id, status, worktree_path, created_at, updated_at)
                     VALUES('job','exec','proj','complete',?1,1,1)",
                    params![worktree_path.as_deref()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
                     VALUES('run','job','proj','exited',1,1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    fn event_fixture(id: &str, seq: i32, event_type: &str, data: &str) -> Event {
        Event {
            id: id.to_string(),
            run_id: "run".to_string(),
            session_id: None,
            sequence: seq,
            timestamp: seq as i64,
            event_type: event_type.to_string(),
            data: data.to_string(),
            parent_tool_use_id: None,
            created_at: seq as i64,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            turn_id: None,
            thinking_tokens: None,
            storage_mode: None,
            content_commit: None,
            content_render_sha: None,
            data_blob: None,
            codec: None,
            read_segments: None,
        }
    }

    async fn insert_event(db: &LocalDb, id: &str, seq: i64, event_type: &str, data: &str) {
        let id = id.to_string();
        let event_type = event_type.to_string();
        let data = data.to_string();
        db.write(move |conn| {
            let (id, event_type, data) = (id.clone(), event_type.clone(), data.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES(?1,'run',?2,?2,?3,?4,?2)",
                    params![id.as_str(), seq, event_type.as_str(), data.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// `(storage_mode, data, data_blob present)` for one event.
    async fn row_state(db: &LocalDb, id: &str) -> (Option<String>, String, bool) {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT storage_mode, data, data_blob FROM events WHERE id = ?1",
                        params![id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                DbResult::Ok((row.opt_text(0)?, row.text(1)?, row.opt_blob(2)?.is_some()))
            })
        })
        .await
        .unwrap()
    }

    async fn all_events(db: &LocalDb) -> Vec<Event> {
        db.query_all(
            format!("SELECT {EVENT_COLUMNS} FROM events ORDER BY sequence ASC"),
            (),
            event_from_row,
        )
        .await
        .unwrap()
    }

    async fn zstd_count(db: &LocalDb) -> i64 {
        db.query_one(
            "SELECT COUNT(*) FROM events WHERE storage_mode = 'zstd'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap()
    }

    async fn blob_count(db: &LocalDb) -> i64 {
        db.query_one("SELECT COUNT(*) FROM archival_blobs", (), |row| row.i64(0))
            .await
            .unwrap()
    }

    #[test]
    fn hash_addressed_system_events_match_teardown_writer_columns() {
        let backend_base = "CLAUDE-BASE ".repeat(40);
        let cairn = "\n\nCAIRN-PROMPT body".to_string();
        let workspace = "\n\n## Workspace Instructions\n\nworkspace doctrine".to_string();
        let agent = "\n\n<agent_role>\nbuilder role body".to_string();
        let (prompt_data, _) = system_prompt(&[
            ("backend_base", &backend_base),
            ("cairn", &cairn),
            ("workspace", &workspace),
            ("agent", &agent),
            ("dynamic", "\n\ncwd=/work/run-1"),
        ]);
        let init_data = system_init_claude(
            "sess-1",
            "uuid-1",
            "/work/run-1",
            &["mcp__cairn__read", "Glob", "Grep", "mcp__cairn__write"],
        );

        for (kind, event_type, data) in [
            (SystemBlobKind::Prompt, "system:prompt", prompt_data),
            (SystemBlobKind::Init, "system:init", init_data),
        ] {
            let event = event_fixture(event_type, 1, event_type, &data);

            let (rewrite_cols, rewrite_blobs, rewrite_summary) =
                classify_system_blob_columns_for_test(&event, kind).unwrap();

            let mut backfill_summary = BackfillSummary::default();
            let (backfill_updates, backfill_blobs) =
                classify(std::slice::from_ref(&event), &mut backfill_summary).unwrap();
            assert_eq!(backfill_updates.len(), 1);
            let backfill_cols = backfill_updates[0].shape.encode();

            assert_eq!(
                backfill_cols, rewrite_cols,
                "{event_type} must encode to identical EventColumns in both writers"
            );
            assert_eq!(
                backfill_blobs, rewrite_blobs,
                "{event_type} must insert identical content-addressed blobs"
            );
            assert_eq!(backfill_summary.bytes_after, rewrite_summary.bytes_after);
            assert_eq!(backfill_summary.rows_compressed, 1);
        }
    }

    #[tokio::test]
    async fn compresses_and_round_trips_a_torn_down_execution() {
        let db = migrated_test_db("backfill-roundtrip.db").await;
        seed(&db, Some("/nonexistent/worktree-gone")).await;

        let a1 = assistant_read("t1", &["file:x"]);
        let e1 = tool_result("t1", "a large rendered file body that should be compressed");
        let u1 = user_text("hello searchable world");
        insert_event(&db, "a1", 1, "assistant", &a1).await;
        insert_event(&db, "e1", 2, "tool_result", &e1).await;
        insert_event(&db, "u1", 3, "user", &u1).await;

        run_archival_maintenance(&db).await.unwrap();

        // Every row is now a zstd stub with its blob populated.
        for id in ["a1", "e1", "u1"] {
            let (mode, data, has_blob) = row_state(&db, id).await;
            assert_eq!(mode.as_deref(), Some("zstd"), "{id} should be zstd");
            assert!(has_blob, "{id} should have a data_blob");
            assert!(
                data.contains("\"archived\":\"zstd\""),
                "{id} should be a stub"
            );
        }
        // The heavy tool_result body is gone from the inline stub, and the
        // tool name is recovered from the paired assistant in the same batch.
        let (_, e1_stub, _) = row_state(&db, "e1").await;
        assert!(!e1_stub.contains("large rendered file body"));
        assert!(e1_stub.contains("\"toolName\":\"read\""));

        // Reconstruction restores every original `data` byte-for-byte.
        let restored = reconstruct_events(&db, all_events(&db).await).await;
        let by_id: std::collections::HashMap<&str, &Event> =
            restored.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(by_id["a1"].data, a1);
        assert_eq!(by_id["e1"].data, e1);
        assert_eq!(by_id["u1"].data, u1);

        // The backfill is marked complete (one execution, fully drained).
        assert!(backfill_complete(&db).await.unwrap());
    }

    #[tokio::test]
    async fn leaves_a_live_worktree_untouched() {
        let db = migrated_test_db("backfill-live.db").await;
        let live = tempfile::tempdir().unwrap();
        seed(&db, Some(live.path().to_str().unwrap())).await;
        insert_event(&db, "u1", 1, "user", &user_text("live")).await;

        run_archival_maintenance(&db).await.unwrap();

        // The worktree still exists, so its events stay full: the teardown writer
        // owns this execution.
        let (mode, _, has_blob) = row_state(&db, "u1").await;
        assert_ne!(mode.as_deref(), Some("zstd"));
        assert!(!has_blob);
    }

    #[tokio::test]
    async fn compresses_not_yet_embedded_assistants() {
        let db = migrated_test_db("backfill-assistant.db").await;
        seed(&db, Some("/nonexistent/worktree-gone")).await;
        insert_event(&db, "a1", 1, "assistant", &assistant_text("unembedded")).await;
        insert_event(&db, "u1", 2, "user", &user_text("ready")).await;

        // No embedding gate: the assistant and user events both compress in the
        // first pass, and the backfill is marked complete immediately.
        run_archival_maintenance(&db).await.unwrap();
        assert_eq!(row_state(&db, "a1").await.0.as_deref(), Some("zstd"));
        assert_eq!(row_state(&db, "u1").await.0.as_deref(), Some("zstd"));
        assert!(backfill_complete(&db).await.unwrap());
    }

    #[tokio::test]
    async fn backfill_scan_is_idempotent() {
        let db = migrated_test_db("backfill-idempotent.db").await;
        seed(&db, None).await; // never had a worktree
        for i in 0..5 {
            insert_event(
                &db,
                &format!("u{i}"),
                i,
                "user",
                &user_text(&format!("row {i}")),
            )
            .await;
        }

        let first = run_backfill(&db).await.unwrap();
        assert_eq!(first.rows_compressed, 5);

        // A second scan finds nothing left to do.
        let second = run_backfill(&db).await.unwrap();
        assert_eq!(second.rows_compressed, 0);
    }

    #[tokio::test]
    async fn compresses_across_batch_boundaries() {
        let db = migrated_test_db("backfill-batches.db").await;
        seed(&db, None).await;
        let total = (BATCH_SIZE + 100) as usize; // forces multiple batches
        for i in 0..total {
            insert_event(&db, &format!("u{i}"), i as i64, "user", &user_text("x")).await;
        }

        let summary = run_backfill(&db).await.unwrap();
        assert_eq!(summary.rows_compressed, total);
        assert_eq!(zstd_count(&db).await, total as i64);
    }

    #[tokio::test]
    async fn maintenance_runs_once_then_short_circuits() {
        let db = migrated_test_db("backfill-once.db").await;
        seed(&db, None).await;
        insert_event(&db, "u1", 1, "user", &user_text("done")).await;

        run_archival_maintenance(&db).await.unwrap();
        assert!(backfill_complete(&db).await.unwrap());
        assert_eq!(row_state(&db, "u1").await.0.as_deref(), Some("zstd"));

        // A fresh row arriving after completion is NOT re-scanned: the backfill is
        // a one-time historical sweep, gated by the completion marker.
        insert_event(&db, "u2", 2, "user", &user_text("late")).await;
        run_archival_maintenance(&db).await.unwrap();
        assert_ne!(row_state(&db, "u2").await.0.as_deref(), Some("zstd"));
    }

    #[tokio::test]
    async fn backfilled_text_is_searchable_after_rebuild() {
        let db = migrated_test_db("backfill-fts.db").await;
        seed(&db, Some("/nonexistent/worktree-gone")).await;
        insert_event(&db, "u1", 1, "user", &user_text("zephyr backfilled needle")).await;

        let index_dir = tempfile::tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();
        assert_eq!(index.search("zephyr", None).unwrap().len(), 1);

        // Backfill rewrites the event to a stub but leaves the FTS row intact.
        run_archival_maintenance(&db).await.unwrap();
        assert_eq!(row_state(&db, "u1").await.0.as_deref(), Some("zstd"));

        // A rebuild reconstructs the archived text, so the needle is still found.
        index.rebuild(&db).await.unwrap();
        let hits = index.search("zephyr", None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "u1");
    }

    /// A torn-down execution's `system:prompt` events take the hash-addressed blob
    /// path in the backfill, not whole-event zstd: static segments dedup into
    /// shared `archival_blobs` rows, the dynamic tail stays inline, the summary
    /// counts the prompt dedups separately, and reconstruction is byte-exact.
    #[tokio::test]
    async fn backfills_system_prompt_segments_through_the_blob_path() {
        let db = migrated_test_db("backfill-sysprompt.db").await;
        seed(&db, Some("/nonexistent/worktree-gone")).await;

        // Two runs share four static segments and differ only in the dynamic tail,
        // exactly as the assembled prompt is recorded with its `raw.segments` map.
        let backend_base = "CLAUDE-BASE ".repeat(400);
        let cairn = format!("\n\n{}", "CAIRN-PROMPT ".repeat(300));
        let workspace = "\n\n## Workspace Instructions\n\nworkspace doctrine".to_string();
        let agent = "\n\n<agent_role>\nbuilder role body".to_string();
        let statics: [(&str, &str); 4] = [
            ("backend_base", &backend_base),
            ("cairn", &cairn),
            ("workspace", &workspace),
            ("agent", &agent),
        ];
        let (data1, content1) = system_prompt(
            &statics
                .iter()
                .copied()
                .chain([("dynamic", "\n\ncwd=/work/run-1")])
                .collect::<Vec<_>>(),
        );
        let (data2, content2) = system_prompt(
            &statics
                .iter()
                .copied()
                .chain([("dynamic", "\n\ncwd=/work/run-2")])
                .collect::<Vec<_>>(),
        );
        insert_event(&db, "sp1", 1, "system:prompt", &data1).await;
        insert_event(&db, "sp2", 2, "system:prompt", &data2).await;

        let summary = run_backfill(&db).await.unwrap();
        // Both prompts took the blob path; the summary counts them separately and
        // none fell back to whole-event zstd.
        assert_eq!(summary.system_prompt, 2);
        assert_eq!(summary.rows_compressed, 2);
        assert_eq!(zstd_count(&db).await, 0, "no prompt fell back to zstd");
        // Four distinct static segments shared by both events dedup to four blobs.
        assert_eq!(blob_count(&db).await, 4, "identical static segments dedup");

        // A blobbed row is hash-addressed (storage_mode 'gitcoord', no data_blob);
        // the heavy static bytes left the inline stub and the dynamic tail stayed.
        let (mode1, stub1, has_blob1) = row_state(&db, "sp1").await;
        assert_eq!(mode1.as_deref(), Some("gitcoord"));
        assert!(!has_blob1, "blobbed row carries no data_blob");
        assert!(
            !stub1.contains("CLAUDE-BASE"),
            "static bytes moved to blobs"
        );
        assert!(
            stub1.contains("cwd=/work/run-1"),
            "dynamic tail stays inline"
        );

        // Reconstruction restores both prompts byte-for-byte.
        let restored = reconstruct_events(&db, all_events(&db).await).await;
        let by_id: std::collections::HashMap<&str, &Event> =
            restored.iter().map(|e| (e.id.as_str(), e)).collect();
        let recon1: serde_json::Value = serde_json::from_str(&by_id["sp1"].data).unwrap();
        let recon2: serde_json::Value = serde_json::from_str(&by_id["sp2"].data).unwrap();
        assert_eq!(recon1["content"].as_str().unwrap(), content1);
        assert_eq!(recon2["content"].as_str().unwrap(), content2);

        // Re-running is a no-op: the rows are already blobbed, no blobs added.
        let again = run_backfill(&db).await.unwrap();
        assert_eq!(again.system_prompt, 0);
        assert_eq!(blob_count(&db).await, 4, "second pass adds zero blob rows");
    }

    /// A `system:prompt` recorded before the `raw.segments` boundary map shipped
    /// has no map to segment, so the backfill keeps it whole as plain zstd rather
    /// than forcing the blob path.
    #[tokio::test]
    async fn backfills_unsegmented_system_prompt_as_zstd() {
        let db = migrated_test_db("backfill-sysprompt-legacy.db").await;
        seed(&db, Some("/nonexistent/worktree-gone")).await;
        // No `raw.segments`: the historical shape from before CAIRN-1562.
        let data = serde_json::json!({
            "eventType": "system:prompt",
            "content": "a fully assembled prompt with no boundary map recorded",
        })
        .to_string();
        insert_event(&db, "sp1", 1, "system:prompt", &data).await;

        let summary = run_backfill(&db).await.unwrap();
        assert_eq!(summary.system_prompt, 0, "no map, no blob path");
        assert_eq!(summary.rows_compressed, 1);
        assert_eq!(row_state(&db, "sp1").await.0.as_deref(), Some("zstd"));
        assert_eq!(blob_count(&db).await, 0);

        // It still round-trips byte-for-byte through the zstd backstop.
        let restored = reconstruct_events(&db, all_events(&db).await).await;
        let sp = restored.iter().find(|e| e.id == "sp1").unwrap();
        assert_eq!(sp.data, data);
    }
}
