//! The one-time rewrite that brings `check_result_cache` rows earlier builds
//! stored down to the shape every writer now stores: the structured result
//! without its plain passes ([`without_plain_passes`]).
//!
//! Those rows held one entry per test, passes included: 12.7 GB of the column
//! on the 11-27 store, a `rust-tests` row about 4 MB. The walk goes by rowid a
//! few rows a page. Each page is one short write that compare-and-swaps every
//! row on the exact text it read, so a row a check rewrote in between keeps
//! what its writer stored, which is already the slim shape. The cursor and the
//! count advance in that same write; `cached_result_slimming_state` holds them,
//! so a restart resumes rather than rescans, and its `completed_at` makes every
//! later start a single read. The logical log is folded by the shared rule as
//! the walk goes, and it rests between pages so the writer stays free for the
//! work it serves.
//!
//! Nothing waits on it: an unrewritten row reads exactly as before, its
//! passing entries still present and unread. The freed pages stay inside the
//! database file until the operator next compacts it.

use std::sync::Arc;
use std::time::Duration;

use cairn_db::turso::params;

use crate::archival::{fold_if_due, FoldTally};
use crate::execution::check_parsers::without_plain_passes;
use crate::storage::{DbError, DbResult, LocalDb, RowExt, FOLD_AT_LOG_BYTES};

/// How much of the walk one call does.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SlimmingBudget {
    /// Rows read a page. A row from before the change runs to about 4 MB, so
    /// this bounds the text one page holds in memory and binds into its write.
    pub page_rows: usize,
    /// Pages one call walks before it stops; `None` walks to the end.
    pub pages: Option<usize>,
    /// Rest after each page.
    pub pause: Duration,
}

impl SlimmingBudget {
    pub(crate) const PRODUCTION: Self = Self {
        page_rows: 8,
        pages: None,
        pause: Duration::from_millis(50),
    };
}

/// What one [`slim_cached_results`] call did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SlimmingReport {
    pub rows_read: usize,
    pub rewritten: usize,
    /// Rows a writer changed between this walk's read and its write.
    pub contended: usize,
    /// Sizes of the rewritten rows' results, before and after.
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub folds: usize,
    /// The walk has reached the end, in this call or an earlier one.
    pub completed: bool,
}

/// One row's rewrite: the text read, and what replaces it.
struct Rewrite {
    rowid: i64,
    read: String,
    slim: String,
}

/// Rewrites written and rows contended on one page, with the written rows'
/// sizes before and after.
#[derive(Default)]
struct PageWrite {
    written: usize,
    contended: usize,
    bytes_before: u64,
    bytes_after: u64,
}

/// Walk `check_result_cache` from where the last call stopped, rewriting
/// every row whose stored result still carries plain passes, within `budget`.
pub(crate) async fn slim_cached_results(
    db: &LocalDb,
    budget: SlimmingBudget,
) -> DbResult<SlimmingReport> {
    let mut report = SlimmingReport::default();
    let (mut cursor, completed_at) = db
        .query_one(
            "SELECT cursor_rowid, completed_at FROM cached_result_slimming_state WHERE id = 1",
            (),
            |row| Ok((row.i64(0)?, row.opt_i64(1)?)),
        )
        .await?;
    if completed_at.is_some() {
        report.completed = true;
        return Ok(report);
    }
    let mut tally = FoldTally::default();
    let mut pages = 0;
    while budget.pages.is_none_or(|limit| pages < limit) {
        let page = read_page(db, cursor, budget.page_rows).await?;
        let Some(last) = page.last().map(|(rowid, _)| *rowid) else {
            report.completed = true;
            break;
        };
        let at_end = page.len() < budget.page_rows;
        report.rows_read += page.len();
        let rewrites: Vec<Rewrite> = page
            .into_iter()
            .filter_map(|(rowid, read)| {
                let slim = without_plain_passes(&read)?;
                Some(Rewrite { rowid, read, slim })
            })
            .collect();
        let written = write_page(db, Arc::new(rewrites), last).await?;
        report.rewritten += written.written;
        report.contended += written.contended;
        report.bytes_before += written.bytes_before;
        report.bytes_after += written.bytes_after;
        cursor = last;
        pages += 1;
        if at_end {
            report.completed = true;
            break;
        }
        fold_if_due(db, FOLD_AT_LOG_BYTES, &mut tally, "cached result slimming").await;
        tokio::time::sleep(budget.pause).await;
    }
    if report.completed {
        db.execute(
            "UPDATE cached_result_slimming_state SET completed_at = unixepoch() WHERE id = 1",
            (),
        )
        .await?;
    }
    report.folds = tally.folds;
    Ok(report)
}

/// Up to `rows` rows past rowid `after` that hold a stored result.
async fn read_page(db: &LocalDb, after: i64, rows: usize) -> DbResult<Vec<(i64, String)>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut found = conn
                .query(
                    "SELECT rowid, target_results_json FROM check_result_cache
                      WHERE rowid > ?1 AND target_results_json IS NOT NULL
                      ORDER BY rowid LIMIT ?2",
                    params![after, rows as i64],
                )
                .await?;
            let mut page = Vec::new();
            while let Some(row) = found.next().await? {
                page.push((row.i64(0)?, row.text(1)?));
            }
            Ok::<_, DbError>(page)
        })
    })
    .await
}

/// Apply one page's rewrites and move the cursor to `cursor`, in one write.
/// A row whose text no longer matches what the page read is left alone.
async fn write_page(db: &LocalDb, rewrites: Arc<Vec<Rewrite>>, cursor: i64) -> DbResult<PageWrite> {
    db.write(|conn| {
        let rewrites = rewrites.clone();
        Box::pin(async move {
            let mut page = PageWrite::default();
            for rewrite in rewrites.iter() {
                let changed = conn
                    .execute(
                        "UPDATE check_result_cache SET target_results_json = ?1
                          WHERE rowid = ?2 AND target_results_json = ?3",
                        params![rewrite.slim.as_str(), rewrite.rowid, rewrite.read.as_str()],
                    )
                    .await?;
                if changed == 1 {
                    page.written += 1;
                    page.bytes_before += rewrite.read.len() as u64;
                    page.bytes_after += rewrite.slim.len() as u64;
                } else {
                    page.contended += 1;
                }
            }
            conn.execute(
                "UPDATE cached_result_slimming_state
                    SET cursor_rowid = ?1, rewritten = rewritten + ?2 WHERE id = 1",
                params![cursor, page.written as i64],
            )
            .await?;
            Ok::<_, DbError>(page)
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::cache::tests::{cache_db, many_passes_result};
    use crate::execution::check_parsers::ParsedCheckResult;

    async fn insert(db: &LocalDb, input: &str, json: &str) {
        db.execute(
            "INSERT INTO check_result_cache(project_id,tree_hash,input_hash,check_name,exit_code,
                 passed,output_tail,duration_ms,ran_at,target_results_json)
             VALUES('project-a','tree',?1,'rust-tests',1,0,'',1,1,?2)",
            params![input, json],
        )
        .await
        .unwrap();
    }

    async fn stored(db: &LocalDb, input: &str) -> String {
        db.query_one(
            "SELECT target_results_json FROM check_result_cache WHERE input_hash = ?1",
            (input.to_string(),),
            |row| row.text(0),
        )
        .await
        .unwrap()
    }

    /// Rows an earlier build stored with every passing test are rewritten to
    /// counts and non-passes, a page at a time: a call stops at its page budget
    /// with the rest untouched, the next resumes from the recorded cursor, and
    /// once the walk reaches the end a start does nothing. A result with no
    /// per-test list and one that is not JSON are left exactly as they were.
    #[tokio::test]
    async fn the_walk_slims_earlier_rows_stops_at_its_budget_and_resumes() {
        let db = cache_db().await;
        let full = serde_json::to_string(&many_passes_result(500)).unwrap();
        let tsc = r#"{"parser":"tsc","passed":0,"failed":2,"skipped":0,"failures":[]}"#;
        for (input, json) in [
            ("in-1", full.as_str()),
            ("in-2", full.as_str()),
            ("in-tsc", tsc),
            ("in-broken", "{broken"),
            ("in-3", full.as_str()),
        ] {
            insert(&db, input, json).await;
        }
        let budget = SlimmingBudget {
            page_rows: 2,
            pages: Some(1),
            pause: Duration::ZERO,
        };

        let first = slim_cached_results(&db, budget).await.unwrap();
        assert_eq!(
            (first.rows_read, first.rewritten, first.completed),
            (2, 2, false)
        );
        assert_eq!(
            stored(&db, "in-3").await,
            full,
            "past the budget, untouched"
        );

        let rest = slim_cached_results(
            &db,
            SlimmingBudget {
                pages: None,
                ..budget
            },
        )
        .await
        .unwrap();
        assert_eq!(
            (
                rest.rows_read,
                rest.rewritten,
                rest.contended,
                rest.completed
            ),
            (3, 1, 0, true),
            "resumed after the first page rather than rereading it"
        );
        assert_eq!(rest.bytes_before, full.len() as u64);

        for input in ["in-1", "in-2", "in-3"] {
            let slim = stored(&db, input).await;
            assert!(slim.len() < 2_048, "{input}: {} bytes", slim.len());
            let kept: ParsedCheckResult = serde_json::from_str(&slim).unwrap();
            assert_eq!((kept.passed, kept.failed, kept.skipped), (501, 1, 1));
            assert!(kept.tests.iter().all(|test| !test.is_plain_pass()));
            assert_eq!(kept.tests.len(), 3, "{input}");
        }
        assert_eq!(stored(&db, "in-tsc").await, tsc);
        assert_eq!(stored(&db, "in-broken").await, "{broken");

        let rewritten = db
            .query_one(
                "SELECT rewritten, completed_at IS NOT NULL FROM cached_result_slimming_state",
                (),
                |row| Ok((row.i64(0)?, row.i64(1)? == 1)),
            )
            .await
            .unwrap();
        assert_eq!(rewritten, (3, true));
        insert(&db, "in-later", &full).await;
        assert_eq!(
            slim_cached_results(&db, SlimmingBudget::PRODUCTION)
                .await
                .unwrap(),
            SlimmingReport {
                completed: true,
                ..SlimmingReport::default()
            },
            "a finished walk is one read"
        );
    }

    /// A row a check rewrote between the walk's read and its write keeps what
    /// the check stored.
    #[tokio::test]
    async fn a_row_changed_under_the_walk_is_left_to_its_writer() {
        let db = cache_db().await;
        insert(&db, "in-1", "{\"written\":\"later\"}").await;
        let rowid = db
            .query_one("SELECT rowid FROM check_result_cache", (), |row| row.i64(0))
            .await
            .unwrap();
        let stale = Rewrite {
            rowid,
            read: "{\"written\":\"earlier\"}".into(),
            slim: "{}".into(),
        };
        let page = write_page(&db, Arc::new(vec![stale]), rowid).await.unwrap();
        assert_eq!((page.written, page.contended), (0, 1));
        assert_eq!(stored(&db, "in-1").await, "{\"written\":\"later\"}");
    }
}
