//! One-time retirement of the attention queue's historical backlog (CAIRN-4182).
//!
//! Migration 0196 gives a push somewhere to record that its referent resolved,
//! and ordinary drains fill it in from then on. Neither does anything about the
//! rows already in the table: in production, 4,213 undelivered pushes across
//! 1,124 recipients, the oldest `review:` row 55 days old and naming an issue
//! merged long ago. Those belong to recipients that may never drain again.
//!
//! Why this is code and not migration SQL: classifying a push means running the
//! liveness resolver across issues, merge_requests, artifacts, runs, prompts and
//! permission_requests. Expressing that a second time as SQL would put two
//! copies of a delivery predicate in the system, and they would drift. This
//! worker calls the same [`attention_push::resolve_verdicts`] every drain calls,
//! so a row is retired here for exactly the reasons it would be retired there.
//!
//! Shape: a keyset walk over a fixed upper bound, in bounded batches, with the
//! cursor committed after each one. It is detached from startup, so it never
//! blocks boot; it is restartable, so an interrupted pass repeats at most one
//! batch; and it is finite, because rows created after the bound are the
//! ordinary drain's business rather than this pass's.

use cairn_db::turso::params;

use super::attention_push::{self, Push};
use crate::storage::{DbResult, LocalDb, RowExt};

/// Rows classified per batch. Small enough that one batch's resolver call and
/// write transaction stay short, large enough that a production backlog is a
/// handful of passes rather than thousands.
const BATCH: i64 = 500;

/// Where the pass has got to, or `None` once it has finished for good.
struct Progress {
    upper_bound_rowid: i64,
    last_rowid: i64,
}

/// Run the backfill to completion, or return immediately if a previous run
/// already finished it. Safe to call on every startup.
pub async fn run_retirement_backfill(db: &LocalDb) -> DbResult<()> {
    let Some(progress) = begin_or_resume(db).await? else {
        return Ok(());
    };
    let upper = progress.upper_bound_rowid;
    let mut cursor = progress.last_rowid;
    let mut batches = 0usize;
    let mut retired_total = 0usize;
    let mut classified = 0usize;

    loop {
        let batch = next_batch(db, cursor, upper).await?;
        if batch.is_empty() {
            break;
        }
        let last_rowid = batch.last().expect("non-empty batch").0;
        let pushes: Vec<Push> = batch.into_iter().map(|(_, push)| push).collect();

        let verdicts = attention_push::resolve_verdicts(db, &pushes).await?;
        let retired = attention_push::retire_terminal(db, &pushes, &verdicts).await?;

        // The cursor advances only after the retirements it accounts for are
        // durable. Crashing in between repeats this batch, and repeating is
        // harmless: retirement is guarded on the row still being pending, so a
        // second pass over already-retired rows writes nothing.
        advance_cursor(db, last_rowid).await?;

        classified += pushes.len();
        retired_total += retired;
        batches += 1;
        cursor = last_rowid;
    }

    complete(db).await?;
    log::info!(
        "attention retirement backfill complete: {classified} rows classified in \
         {batches} batches, {retired_total} retired (rowid <= {upper})"
    );
    Ok(())
}

/// Claim the pass, capturing the upper bound the first time. `None` means a
/// previous run already completed and there is nothing left to do.
async fn begin_or_resume(db: &LocalDb) -> DbResult<Option<Progress>> {
    let existing = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT upper_bound_rowid, last_rowid, completed_at
                           FROM attention_push_retirement_backfill WHERE id=1",
                        (),
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => Some((row.i64(0)?, row.i64(1)?, row.opt_i64(2)?)),
                    None => None,
                })
            })
        })
        .await?;

    match existing {
        Some((_, _, Some(_))) => Ok(None),
        Some((upper_bound_rowid, last_rowid, None)) => Ok(Some(Progress {
            upper_bound_rowid,
            last_rowid,
        })),
        None => {
            // Capture MAX(rowid) exactly once. Fixing the finish line here is
            // what keeps the pass from chasing a table that is still growing:
            // anything inserted after this point is, by construction, recent
            // enough that ordinary drains will reach it.
            let now = crate::orchestrator::attention_push::now_ts();
            let upper = db
                .write(move |conn| {
                    Box::pin(async move {
                        let mut rows = conn
                            .query("SELECT COALESCE(MAX(rowid), 0) FROM attention_pushes", ())
                            .await?;
                        let upper = rows.next().await?.expect("aggregate row").i64(0)?;
                        drop(rows);
                        conn.execute(
                            "INSERT INTO attention_push_retirement_backfill
                               (id, upper_bound_rowid, last_rowid, started_at, completed_at)
                             VALUES (1, ?1, 0, ?2, NULL)",
                            params![upper, now],
                        )
                        .await?;
                        Ok(upper)
                    })
                })
                .await?;
            log::info!("attention retirement backfill starting (rowid <= {upper})");
            Ok(Some(Progress {
                upper_bound_rowid: upper,
                last_rowid: 0,
            }))
        }
    }
}

/// The next window of candidate rows, oldest rowid first.
///
/// Only rows that could possibly retire are read: still pending, within the
/// bound, and carrying one of the three resolvable prefixes. Every other prefix
/// is informational and unconditionally live, so loading it would be work that
/// could never produce a retirement.
async fn next_batch(db: &LocalDb, cursor: i64, upper: i64) -> DbResult<Vec<(i64, Push)>> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT rowid, id, recipient, content_ref, wake, boundary, key,
                            created_at, delivered_event_id
                       FROM attention_pushes
                      WHERE rowid > ?1 AND rowid <= ?2
                        AND delivered_event_id IS NULL AND retired_at IS NULL
                        AND (key LIKE 'review:%'
                          OR key LIKE 'question:%'
                          OR key LIKE 'permission:%')
                      ORDER BY rowid ASC
                      LIMIT ?3",
                    params![cursor, upper, BATCH],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let rowid = row.i64(0)?;
                out.push((rowid, attention_push::push_from_row_offset(&row, 1)?));
            }
            Ok(out)
        })
    })
    .await
}

async fn advance_cursor(db: &LocalDb, last_rowid: i64) -> DbResult<()> {
    db.write(move |conn| {
        Box::pin(async move {
            conn.execute(
                "UPDATE attention_push_retirement_backfill
                    SET last_rowid=?1 WHERE id=1 AND last_rowid < ?1",
                params![last_rowid],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// Mark the pass finished.
///
/// The keyset walk reaching the upper bound IS the proof: every row at or below
/// it was read and classified exactly once. Rows still pending down there are
/// the ones the resolver judged `Suspended` or `Live` -- not terminal, and so
/// correctly left alone. Deliberately not re-derived by a second query, because
/// "no terminal row remains" is only answerable by resolving them all again,
/// which is the pass itself.
async fn complete(db: &LocalDb) -> DbResult<()> {
    let now = crate::orchestrator::attention_push::now_ts();
    db.write(move |conn| {
        Box::pin(async move {
            conn.execute(
                "UPDATE attention_push_retirement_backfill
                    SET completed_at=?1 WHERE id=1 AND completed_at IS NULL",
                params![now],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::attention_push::{push, Boundary, Wake};

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("attention-retirement.db").await
    }

    /// Two issues: `issue-1` (number 2) whose review resolved and whose question
    /// was answered, and `issue-2` (number 3) which produced nothing reviewable.
    async fn seed(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('issue-1','p',2,'One','active','active','none',1,1),
                    ('issue-2','p',3,'Two','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('exec-1','default','issue-1','p','running',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('watcher','p','issue-1','running','sess',1,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
              VALUES('child-job','p','issue-1','exec-1','planner','planner','running','sess2',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('run-1','p','child-job','issue-1',1,1);
            INSERT INTO merge_requests
              (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
              VALUES('mr','child-job','p','issue-1','t','b','main','merged',1,1);
            INSERT INTO prompts(id, run_id, questions, response, created_at)
              VALUES('q','run-1','[]','answered',1);
            ",
        )
        .await
        .unwrap();
    }

    async fn count_where(db: &LocalDb, predicate: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM attention_pushes WHERE {predicate}");
        db.read(move |conn| {
            let sql = sql.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, ()).await?;
                rows.next().await?.expect("count row").i64(0)
            })
        })
        .await
        .unwrap()
    }

    /// Queue one row of every kind the historical table actually holds, then
    /// prove the pass touches exactly the terminal ones. The negatives matter
    /// more than the positives here: this worker runs unattended against a
    /// production backlog, and every row it wrongly retires is a wake nobody
    /// will ever receive.
    #[tokio::test]
    async fn a_mixed_backlog_retires_only_the_terminal_rows() {
        let db = migrated_db().await;
        seed(&db).await;

        // Terminal: the merge request merged, and the question was answered.
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/proj/2/q",
        )
        .await
        .unwrap();
        // Suspended: an issue with no merge request and no plan proves nothing.
        push(
            &db,
            "watcher",
            "cairn://p/proj/3",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/3",
        )
        .await
        .unwrap();
        // Informational: names no referent, so it is never eligible.
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "catchup:cairn://p/proj/2/1/planner",
        )
        .await
        .unwrap();
        // Missing issue: fails open, exactly as at drain time.
        push(
            &db,
            "watcher",
            "cairn://p/proj/9999",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/9999",
        )
        .await
        .unwrap();
        // Already delivered, and already retired: both out of scope.
        db.execute_script(
            "INSERT INTO attention_pushes
               (id, recipient, content_ref, wake, boundary, key, created_at, delivered_event_id)
             VALUES('done','watcher','cairn://p/proj/2','wake','event','review:done',1,'ev-1');
             INSERT INTO attention_pushes
               (id, recipient, content_ref, wake, boundary, key, created_at, retired_at, retirement_reason)
             VALUES('gone','watcher','cairn://p/proj/2','wake','event','review:gone',1,5,'review_resolved');",
        )
        .await
        .unwrap();

        run_retirement_backfill(&db).await.unwrap();

        assert_eq!(
            count_where(
                &db,
                "retirement_reason='review_resolved' AND key='review:cairn://p/proj/2'"
            )
            .await,
            1,
            "a merged merge request is proof the review resolved"
        );
        assert_eq!(
            count_where(&db, "retirement_reason='question_resolved'").await,
            1
        );
        assert_eq!(
            count_where(&db, "retired_at IS NULL AND delivered_event_id IS NULL").await,
            3,
            "the unevidenced review, the informational push, and the missing-issue \
             push all stay pending"
        );
        assert_eq!(
            count_where(
                &db,
                "id='done' AND delivered_event_id='ev-1' AND retired_at IS NULL"
            )
            .await,
            1,
            "a delivered row is never re-marked"
        );
        assert_eq!(
            count_where(&db, "id='gone' AND retired_at=5").await,
            1,
            "an already-retired row keeps its original timestamp"
        );
        assert_eq!(count_where(&db, "1=1").await, 7, "nothing is ever deleted");
    }

    /// The cursor is the whole restart story, so it has to be load-bearing
    /// rather than decorative: rows at or below it are not revisited.
    #[tokio::test]
    async fn the_pass_resumes_from_its_durable_cursor_and_settles() {
        let db = migrated_db().await;
        seed(&db).await;
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/proj/2/q",
        )
        .await
        .unwrap();

        // Claim the pass, then pretend a previous run already walked past the
        // first row before dying.
        let first_rowid = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT MIN(rowid) FROM attention_pushes", ())
                        .await?;
                    rows.next().await?.expect("aggregate row").i64(0)
                })
            })
            .await
            .unwrap();
        begin_or_resume(&db).await.unwrap().expect("a fresh pass");
        advance_cursor(&db, first_rowid).await.unwrap();

        run_retirement_backfill(&db).await.unwrap();

        assert_eq!(
            count_where(&db, "retired_at IS NOT NULL").await,
            1,
            "only the row above the cursor is classified; the skipped one is left \
             to ordinary drains rather than being walked twice"
        );

        // Completed, so a later boot is free.
        let retired_before = count_where(&db, "retired_at IS NOT NULL").await;
        run_retirement_backfill(&db).await.unwrap();
        assert_eq!(
            count_where(&db, "retired_at IS NOT NULL").await,
            retired_before,
            "a completed pass does no work on the next startup"
        );
        assert!(begin_or_resume(&db).await.unwrap().is_none());
    }

    /// Rows created after the captured bound belong to ordinary drains. Without
    /// a fixed finish line the pass could chase a growing table forever.
    #[tokio::test]
    async fn rows_created_after_the_bound_are_left_to_ordinary_drains() {
        let db = migrated_db().await;
        seed(&db).await;
        begin_or_resume(&db).await.unwrap().expect("a fresh pass");

        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();

        run_retirement_backfill(&db).await.unwrap();
        assert_eq!(
            count_where(&db, "retired_at IS NOT NULL").await,
            0,
            "the bound was captured when the table was empty"
        );
    }
}
