//! Diagnostic probe: plan and time the runner-start queue expiry sweep against
//! a (copied) Cairn database. Never open a live database; APFS-clone the db
//! file (`cp -c`, on the database's own volume) and point this at the clone,
//! deleting it afterwards (docs/database.md "Taking a census").
//!
//! Usage: cargo run -p cairn-core --example explain_queue_expiry -- /path/to/db MODE...
//!
//! - `baseline`: table facts, and the whole-journal shapes the sweep used to
//!   take (each under a 60 s engine deadline; on the 10-29 production journal
//!   every one of them reached it).
//! - `incarnations`: plan the sweep's two reads, then list the journal's
//!   incarnations and time the open-entry read for each. The newest one is what
//!   a steady-state start pays; the sum is what the first start after the
//!   upgrade pays, once.
//! - `write`: create the closed-incarnations table if missing and run the
//!   sweep's statements, inserts included, twice. Only ever pass this a
//!   disposable copy.
//!
//! The statements are copies of those in `fleet::queue`; keep them in step.
//! A dev build measures the engine at full speed: the dev profile compiles
//! dependencies, Turso included, at opt-level 3.

use cairn_db::turso::{Builder, Connection, Value};
use std::collections::HashSet;
use std::time::{Duration, Instant};

fn text(v: &Value) -> String {
    match v {
        Value::Text(t) => t.clone(),
        Value::Null => "-".into(),
        Value::Integer(i) => i.to_string(),
        _ => "?".into(),
    }
}

const OLD_SELF_JOIN: &str = "SELECT live.entry_id FROM execution_queue live
  JOIN (SELECT entry_id, MAX(sequence) AS sequence FROM execution_queue GROUP BY entry_id) latest
    ON latest.entry_id = live.entry_id AND latest.sequence = live.sequence
  WHERE live.runner_incarnation != ?1 AND live.state IN ('queued', 'offered', 'claimed')";

const WHOLE_JOURNAL_NOT_EXISTS: &str =
    "SELECT live.entry_id, live.sequence FROM execution_queue live
  WHERE live.runner_incarnation != ?1 AND live.state IN ('queued', 'offered', 'claimed')
    AND NOT EXISTS (SELECT 1 FROM execution_queue newer
                    WHERE newer.entry_id = live.entry_id AND newer.sequence > live.sequence)";

const NEXT_INCARNATION: &str = "SELECT runner_incarnation FROM execution_queue
     WHERE runner_incarnation > ?1
     ORDER BY runner_incarnation
     LIMIT 1";

const OPEN_ENTRIES_OF_INCARNATION: &str = "SELECT live.entry_id, live.sequence
     FROM execution_queue live
     WHERE live.runner_incarnation = ?1
       AND live.state IN ('queued', 'offered', 'claimed')
       AND NOT EXISTS (
           SELECT 1 FROM execution_queue newer
           WHERE newer.entry_id = live.entry_id AND newer.sequence > live.sequence)";

const EXPIRE_ENTRY: &str = "INSERT OR IGNORE INTO execution_queue(
         event_id, entry_id, sequence, runner_incarnation,
         request_id, attempt_id, project_id, state,
         constraints_json, scoring_json, horizon_unix_ms, created_at, detail)
     SELECT
         live.entry_id || ':expired:restart', live.entry_id, live.sequence + 1, ?1,
         live.request_id, live.attempt_id, live.project_id, 'expired',
         live.constraints_json, live.scoring_json, live.horizon_unix_ms,
         unixepoch() * 1000, 'runner restarted'
     FROM execution_queue live
     WHERE live.entry_id = ?2
       AND live.sequence = ?3
       AND live.runner_incarnation != ?1
       AND live.state IN ('queued', 'offered', 'claimed')
       AND NOT EXISTS (
           SELECT 1 FROM execution_queue newer
           WHERE newer.entry_id = live.entry_id AND newer.sequence > live.sequence)";

const CLOSED_TABLE: &str = "CREATE TABLE IF NOT EXISTS execution_queue_closed_incarnations (
    runner_incarnation TEXT PRIMARY KEY NOT NULL,
    closed_by TEXT NOT NULL,
    expired_entries INTEGER NOT NULL,
    closed_at INTEGER NOT NULL)";

const INCARNATION: &str = "runner-9999999999999";

async fn plan(conn: &Connection, label: &str, sql: &str) {
    println!("--- plan {label}");
    let mut rows = conn
        .query(&format!("EXPLAIN QUERY PLAN {sql}"), ())
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        let cols: Vec<String> = (0..row.column_count())
            .map(|i| text(&row.get_value(i).unwrap()))
            .collect();
        println!("  {}", cols.join(" | "));
    }
}

async fn time_whole(conn: &Connection, label: &str, sql: &str) {
    conn.set_query_timeout(Duration::from_secs(60)).unwrap();
    let started = Instant::now();
    let mut n = 0usize;
    let outcome = async {
        let mut rows = conn.query(sql, (INCARNATION,)).await?;
        while rows.next().await?.is_some() {
            n += 1;
        }
        Ok::<_, cairn_db::turso::Error>(())
    }
    .await;
    conn.set_query_timeout(Duration::ZERO).unwrap();
    println!(
        "{label}: rows={n} elapsed_ms={} outcome={outcome:?}",
        started.elapsed().as_millis()
    );
}

async fn one_text(conn: &Connection, sql: &str, param: &str) -> Option<String> {
    let mut rows = conn.query(sql, (param,)).await.unwrap();
    rows.next()
        .await
        .unwrap()
        .map(|row| text(&row.get_value(0).unwrap()))
}

async fn incarnations(conn: &Connection) -> Vec<String> {
    let mut found = Vec::new();
    let mut after = String::new();
    while let Some(next) = one_text(conn, NEXT_INCARNATION, &after).await {
        after.clone_from(&next);
        found.push(next);
    }
    found
}

async fn open_entries(conn: &Connection, incarnation: &str) -> Vec<(String, i64)> {
    let mut open = Vec::new();
    let mut rows = conn
        .query(OPEN_ENTRIES_OF_INCARNATION, (incarnation,))
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        let Value::Integer(sequence) = row.get_value(1).unwrap() else {
            panic!("sequence is an integer");
        };
        open.push((text(&row.get_value(0).unwrap()), sequence));
    }
    open
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    cairn_build_id::install();
    let db_path = std::env::args()
        .nth(1)
        .expect("usage: explain_queue_expiry <db> [baseline] [incarnations] [write]");
    let flags: Vec<String> = std::env::args().skip(2).collect();
    let mode = |name: &str| flags.iter().any(|flag| flag == name);
    let started = Instant::now();
    let db = Builder::new_local(&db_path).build().await.unwrap();
    let conn = db.connect().unwrap();
    println!("open_ms={}", started.elapsed().as_millis());

    if mode("baseline") {
        for (label, sql) in [
            ("rows", "SELECT count(*) FROM execution_queue"),
            ("entries", "SELECT count(DISTINCT entry_id) FROM execution_queue"),
            (
                "open-state rows",
                "SELECT count(*) FROM execution_queue WHERE state IN ('queued', 'offered', 'claimed')",
            ),
        ] {
            let started = Instant::now();
            let mut rows = conn.query(sql, ()).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            println!(
                "{label}: {} ({} ms)",
                text(&row.get_value(0).unwrap()),
                started.elapsed().as_millis()
            );
        }
        plan(&conn, "OLD_SELF_JOIN", OLD_SELF_JOIN).await;
        plan(&conn, "WHOLE_JOURNAL_NOT_EXISTS", WHOLE_JOURNAL_NOT_EXISTS).await;
        time_whole(&conn, "WHOLE_JOURNAL_NOT_EXISTS", WHOLE_JOURNAL_NOT_EXISTS).await;
        time_whole(&conn, "OLD_SELF_JOIN", OLD_SELF_JOIN).await;
    }

    if mode("incarnations") {
        // An earlier probe run may have left an experimental index on the copy;
        // measure against the schema production has.
        conn.execute("DROP INDEX IF EXISTS idx_execution_queue_latest", ())
            .await
            .unwrap();
        plan(&conn, "NEXT_INCARNATION", NEXT_INCARNATION).await;
        plan(
            &conn,
            "OPEN_ENTRIES_OF_INCARNATION",
            OPEN_ENTRIES_OF_INCARNATION,
        )
        .await;
        let started = Instant::now();
        let all = incarnations(&conn).await;
        println!(
            "incarnations={} list_ms={}",
            all.len(),
            started.elapsed().as_millis()
        );
        let mut total = Duration::ZERO;
        for incarnation in &all {
            let started = Instant::now();
            let rows = one_text(
                &conn,
                "SELECT count(*) FROM execution_queue WHERE runner_incarnation = ?1",
                incarnation,
            )
            .await
            .unwrap_or_default();
            let counted = started.elapsed();
            let started = Instant::now();
            let open = open_entries(&conn, incarnation).await;
            let found = started.elapsed();
            total += found;
            println!(
                "  {incarnation}: rows={rows} open={} count_ms={} find_ms={}",
                open.len(),
                counted.as_millis(),
                found.as_millis()
            );
        }
        println!("sum of finds: {} ms", total.as_millis());
    }

    if mode("write") {
        conn.execute(CLOSED_TABLE, ()).await.unwrap();
        // Twice: the second pass must close nothing, since the first marked
        // every incarnation it swept.
        for pass in 1..=2 {
            let started = Instant::now();
            let mut closed = HashSet::new();
            let mut rows = conn
                .query(
                    "SELECT runner_incarnation FROM execution_queue_closed_incarnations",
                    (),
                )
                .await
                .unwrap();
            while let Some(row) = rows.next().await.unwrap() {
                closed.insert(text(&row.get_value(0).unwrap()));
            }
            drop(rows);
            let (mut swept, mut expired) = (0usize, 0u64);
            for incarnation in incarnations(&conn).await {
                if incarnation == INCARNATION || closed.contains(&incarnation) {
                    continue;
                }
                let open = open_entries(&conn, &incarnation).await;
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
                let mut here = 0;
                for (entry_id, sequence) in &open {
                    here += conn
                        .execute(EXPIRE_ENTRY, (INCARNATION, entry_id.as_str(), *sequence))
                        .await
                        .unwrap();
                }
                conn.execute(
                    "INSERT OR IGNORE INTO execution_queue_closed_incarnations(
                         runner_incarnation, closed_by, expired_entries, closed_at)
                     VALUES (?1, ?2, ?3, unixepoch() * 1000)",
                    (incarnation.as_str(), INCARNATION, here as i64),
                )
                .await
                .unwrap();
                conn.execute("COMMIT", ()).await.unwrap();
                swept += 1;
                expired += here;
            }
            println!(
                "sweep pass {pass}: incarnations_closed={swept} entries_expired={expired} total_ms={}",
                started.elapsed().as_millis()
            );
        }
    }
}
