//! What one statement costs at Cairn's own schema size once its tables are
//! checkpointed. Each cursor a statement opens resolves its physical root page
//! to an MVCC table id, and that lookup's cost grows with the number of tables
//! and indexes unless the engine indexes it (docs/database.md, "The Turso pin
//! carries one patch"). A freshly migrated store still holds negative,
//! uncheckpointed roots, which skip the lookup altogether, so the bench
//! checkpoints and reopens the store before timing anything.
//!
//! Run: `cargo test -p cairn-db --release --lib cursor_open_cost -- --ignored --nocapture`

use super::{migrated_test_db, LocalDb, RowExt};
use std::time::Instant;

const RUNS: usize = 10_000;
const WARMUP: usize = 200;

async fn schema_shape(db: &LocalDb) -> (i64, i64, i64) {
    db.query_one(
        "SELECT (SELECT count(*) FROM sqlite_schema WHERE type = 'table'),
                (SELECT count(*) FROM sqlite_schema WHERE type = 'index'),
                (SELECT count(*) FROM sqlite_schema WHERE rootpage < 0)",
        (),
        |row| Ok((row.i64(0)?, row.i64(1)?, row.i64(2)?)),
    )
    .await
    .unwrap()
}

/// Microseconds per execution of `sql` with `param` bound, on one connection
/// and one prepared statement, after a warm-up.
async fn micros_per_statement(db: &LocalDb, sql: &str, param: &str) -> f64 {
    let conn = db.connect().await.unwrap();
    let mut statement = conn.prepare(sql).await.unwrap();
    for run in 0..WARMUP + RUNS {
        if run == WARMUP {
            return timed(&mut statement, param).await;
        }
        drain(&mut statement, param).await;
    }
    unreachable!()
}

async fn timed(statement: &mut turso::Statement, param: &str) -> f64 {
    let started = Instant::now();
    for _ in 0..RUNS {
        drain(statement, param).await;
    }
    started.elapsed().as_secs_f64() * 1e6 / RUNS as f64
}

async fn drain(statement: &mut turso::Statement, param: &str) {
    let mut rows = statement.query((param,)).await.unwrap();
    while rows.next().await.unwrap().is_some() {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "benchmark; run in release with --ignored --nocapture"]
async fn cursor_open_cost_at_the_migrated_schema() {
    let db = migrated_test_db("cursor_open_bench").await;
    let path = db.path().to_path_buf();
    let (_, _, uncheckpointed) = schema_shape(&db).await;
    {
        let conn = db.connect().await.unwrap();
        let mut rows = conn
            .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
            .await
            .unwrap();
        while rows.next().await.unwrap().is_some() {}
    }
    drop(db);
    let db = LocalDb::open(&path).await.unwrap();
    let (tables, indexes, negative) = schema_shape(&db).await;
    assert_eq!(
        negative, 0,
        "a negative root skips the root-page lookup and would hide its cost"
    );

    let point = micros_per_statement(&db, "SELECT id FROM issues WHERE id = ?1", "bench").await;
    let join = micros_per_statement(
        &db,
        "SELECT e.id, i.id FROM executions e JOIN issues i ON i.id = e.issue_id WHERE e.id = ?1",
        "bench",
    )
    .await;
    println!(
        "cursor-open bench: {tables} tables, {indexes} indexes \
         ({uncheckpointed} uncheckpointed before the checkpoint, {negative} after); \
         point lookup {point:.2} us/statement, two-table join {join:.2} us/statement \
         ({RUNS} runs each)"
    );
}
