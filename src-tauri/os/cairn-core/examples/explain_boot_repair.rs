//! Diagnostic probe: plan and time the startup stale-run repair queries
//! against a (copied) Cairn database. Never open a live database.
//!
//! Usage: cargo run -p cairn-core --example explain_boot_repair -- /path/to/db [run-slow]

use cairn_db::turso::{Builder, Value};
use std::time::Instant;

fn text(v: &Value) -> String {
    match v {
        Value::Text(t) => t.clone(),
        Value::Null => "-".into(),
        Value::Integer(i) => i.to_string(),
        _ => "?".into(),
    }
}

const OLD_ORPHAN: &str = "SELECT j.id FROM jobs j JOIN turns t ON t.id = (
    SELECT latest.id FROM turns latest WHERE latest.job_id = j.id
    ORDER BY latest.created_at DESC, latest.sequence DESC LIMIT 1)
  WHERE j.status = 'running' AND t.state = 'cancelled' AND t.end_reason = 'orphaned_pending'
    AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id AND r.status IN ('starting', 'live'))";

const NEW_WARM: &str = "SELECT j.id FROM jobs j JOIN turns t ON t.id = (
    SELECT latest.id FROM turns latest WHERE latest.job_id = j.id AND latest.start_reason != 'memory_review'
    ORDER BY latest.created_at DESC, latest.sequence DESC LIMIT 1)
  WHERE j.status = 'running' AND t.state IN ('complete', 'failed')";

const FIXED_ORPHAN: &str = "SELECT j.id FROM jobs j WHERE j.status = 'running'
  AND EXISTS (SELECT 1 FROM turns t WHERE t.id = (SELECT latest.id FROM turns latest WHERE latest.job_id = j.id
       ORDER BY latest.created_at DESC, latest.sequence DESC LIMIT 1)
       AND t.state = 'cancelled' AND t.end_reason = 'orphaned_pending')
  AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id AND r.status IN ('starting', 'live'))";

const FIXED_WARM: &str = "SELECT j.id FROM jobs j WHERE j.status = 'running'
  AND (SELECT latest.state FROM turns latest WHERE latest.job_id = j.id AND latest.start_reason != 'memory_review'
       ORDER BY latest.created_at DESC, latest.sequence DESC LIMIT 1) IN ('complete', 'failed')";

const RESOLVED_ISSUE: &str = "SELECT j.id FROM jobs j
  WHERE j.status IN ('running', 'pending', 'idle', 'blocked') AND j.thread_id IS NULL
  AND EXISTS (SELECT 1 FROM issues i WHERE i.id = j.issue_id AND i.status IN ('merged', 'closed'))
  AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id AND r.status IN ('starting', 'live'))";

const PER_JOB: &str =
    "SELECT state, end_reason FROM turns WHERE job_id = ?1 AND start_reason != 'memory_review'
    ORDER BY created_at DESC, sequence DESC LIMIT 1";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let db_path = std::env::args()
        .nth(1)
        .expect("usage: explain_boot_repair <db> [run-slow]");
    let run_slow = std::env::args().nth(2).as_deref() == Some("run-slow");
    let db = Builder::new_local(&db_path).build().await.unwrap();
    let conn = db.connect().unwrap();

    for (label, sql) in [
        ("jobs", "SELECT count(*) FROM jobs"),
        (
            "running jobs",
            "SELECT count(*) FROM jobs WHERE status='running'",
        ),
        ("turns", "SELECT count(*) FROM turns"),
        ("runs", "SELECT count(*) FROM runs"),
    ] {
        let mut rows = conn.query(sql, ()).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        println!("{label}: {}", text(&row.get_value(0).unwrap()));
    }

    for (label, sql) in [
        ("OLD_ORPHAN", OLD_ORPHAN),
        ("NEW_WARM", NEW_WARM),
        ("FIXED_ORPHAN", FIXED_ORPHAN),
        ("FIXED_WARM", FIXED_WARM),
        ("RESOLVED_ISSUE", RESOLVED_ISSUE),
        ("PER_JOB", PER_JOB),
    ] {
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

    println!("--- timing per-job approach");
    let started = Instant::now();
    let mut running = Vec::new();
    let mut rows = conn
        .query("SELECT id FROM jobs WHERE status='running'", ())
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        running.push(text(&row.get_value(0).unwrap()));
    }
    let mut hits = 0usize;
    for job_id in &running {
        let mut rows = conn.query(PER_JOB, (job_id.as_str(),)).await.unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            let state = text(&row.get_value(0).unwrap());
            if state == "complete" || state == "failed" {
                hits += 1;
            }
        }
    }
    println!(
        "running={} repair_candidates={} elapsed_ms={}",
        running.len(),
        hits,
        started.elapsed().as_millis()
    );

    let mut timed: Vec<(&str, &str)> = vec![
        ("FIXED_ORPHAN", FIXED_ORPHAN),
        ("FIXED_WARM", FIXED_WARM),
        ("RESOLVED_ISSUE", RESOLVED_ISSUE),
    ];
    if run_slow {
        timed.push(("OLD_ORPHAN", OLD_ORPHAN));
        timed.push(("NEW_WARM", NEW_WARM));
    }
    {
        for (label, sql) in timed {
            let started = Instant::now();
            let mut n = 0usize;
            let mut rows = conn.query(sql, ()).await.unwrap();
            while let Some(_row) = rows.next().await.unwrap() {
                n += 1;
            }
            println!(
                "{label}: rows={n} elapsed_ms={}",
                started.elapsed().as_millis()
            );
        }
    }
}
