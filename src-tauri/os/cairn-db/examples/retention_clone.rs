//! Clone-only rehearsal of the retention evictor (`storage::retention`).
//!
//! Point it at an APFS clone of the three-file set, never at the live file:
//!
//!   cp -c ~/.cairn/cairn.db ~/.cairn/cairn.db-log ~/.cairn/cairn.db-wal <dir>/
//!
//! Every subcommand refuses a database inside a live Cairn data directory and
//! any path not ending in `.db` (Turso names the logical log from the
//! extension; cairn/5590). Each prints one JSON document on stdout:
//!
//!   fold <db>                  TRUNCATE-checkpoint the clone's logical log
//!   sql <db> <statement>       one ad-hoc statement, rows as JSON
//!   catchup <db> <now> <rules> run the evictor (`retention::run_rules`) to
//!                              completion at clock `now` (unix seconds; use the
//!                              clone's TAKEN time) for the comma-separated rule
//!                              names, with the production fold threshold, and
//!                              report pages, rows, per-page select and write
//!                              times, the largest page, folds and the log peak
//!   logdelta <db> <statement>  fold the log, run one statement, and report how
//!                              many bytes it added to the logical log (the
//!                              write amplification of that one statement)
//!
//! Usage:
//!   cargo run --release -p cairn-db --example retention_clone -- fold <clone>/cairn.db

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cairn_db::storage::retention::{
    quantile, run_rules, Eviction, EvictorBudget, EvictorState, PassEnd,
};
use cairn_db::storage::{DrainBudget, LocalDb};
use cairn_db::turso::Value;
use serde_json::{json, Value as Json};

type Res<T> = Result<T, String>;

/// Data directories a live Cairn keeps its database in, relative to `$HOME`.
const LIVE_DIRS: &[&str] = &[".cairn", "Library/Application Support/com.cairn.desktop"];

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(path) = args.get(1) {
        if let Err(error) = check_path(path) {
            eprintln!("retention_clone: {error}");
            std::process::exit(2);
        }
    }
    let result = match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["fold", db] => fold(db).await,
        ["sql", db, sql] => adhoc(db, sql).await,
        ["catchup", db, now, rules] => catchup(db, now, rules).await,
        ["logdelta", db, sql] => logdelta(db, sql).await,
        _ => Err(
            "usage: retention_clone fold|sql|catchup|logdelta <db> ... (see the module docs)"
                .to_string(),
        ),
    };
    match result {
        Ok(doc) => println!("{}", serde_json::to_string_pretty(&doc).unwrap()),
        Err(error) => {
            eprintln!("retention_clone: {error}");
            std::process::exit(1);
        }
    }
}

/// Refuse a live database: the evictor deletes, and this tool exists to
/// rehearse it where a mistake costs a clone.
fn check_path(path: &str) -> Res<()> {
    if !path.ends_with(".db") {
        return Err(format!(
            "{path} must end in .db: Turso derives the logical log's name from the extension"
        ));
    }
    let Some(home) = std::env::var_os("HOME") else {
        return Ok(());
    };
    let dir = Path::new(path)
        .parent()
        .map(|p| {
            if p.as_os_str().is_empty() {
                Path::new(".")
            } else {
                p
            }
        })
        .and_then(|p| std::fs::canonicalize(p).ok());
    for live in LIVE_DIRS {
        let live = std::fs::canonicalize(PathBuf::from(&home).join(live)).ok();
        if live.is_some() && dir == live {
            return Err(format!(
                "{path} is in a live Cairn data directory; clone it first (see the module docs)"
            ));
        }
    }
    Ok(())
}

async fn open(path: &str) -> Res<(LocalDb, f64)> {
    let started = Instant::now();
    let db = LocalDb::open(path)
        .await
        .map_err(|e| format!("open {path}: {e}"))?;
    Ok((db, started.elapsed().as_secs_f64()))
}

async fn fold(path: &str) -> Res<Json> {
    let (db, open_s) = open(path).await?;
    let before = db.logical_log_bytes();
    let report = db
        .checkpoint(3, Duration::from_millis(250), DrainBudget::PRODUCTION)
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({
        "open_s": open_s,
        "log_bytes_before": before,
        "log_bytes_after": report.log_bytes_after,
        "checkpoint_s": report.duration.as_secs_f64(),
        "error": report.error,
    }))
}

async fn adhoc(path: &str, sql: &str) -> Res<Json> {
    let (db, _) = open(path).await?;
    let conn = db.connect().await.map_err(|e| e.to_string())?;
    let started = Instant::now();
    let mut rows = conn
        .query(sql, ())
        .await
        .map_err(|e| format!("{e}: {sql}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(|e| format!("{e}: {sql}"))? {
        let mut values = Vec::with_capacity(row.column_count());
        for idx in 0..row.column_count() {
            values.push(json_value(row.get_value(idx).map_err(|e| e.to_string())?));
        }
        out.push(Json::Array(values));
    }
    Ok(json!({ "ms": started.elapsed().as_millis() as u64, "rows": out }))
}

/// Fold first so the delta is this statement's alone, then run it once. A read
/// is run as a query and its rows counted; anything else is executed.
async fn logdelta(path: &str, sql: &str) -> Res<Json> {
    let (db, open_s) = open(path).await?;
    let fold = db
        .checkpoint(3, Duration::from_millis(250), DrainBudget::PRODUCTION)
        .await
        .map_err(|e| e.to_string())?;
    let conn = db.connect().await.map_err(|e| e.to_string())?;
    let before = db.logical_log_bytes();
    let started = Instant::now();
    let is_read = sql.trim_start().to_ascii_lowercase().starts_with("select");
    let affected = if is_read {
        let mut rows = conn
            .query(sql, ())
            .await
            .map_err(|e| format!("{e}: {sql}"))?;
        let mut count = 0u64;
        while rows
            .next()
            .await
            .map_err(|e| format!("{e}: {sql}"))?
            .is_some()
        {
            count += 1;
        }
        count
    } else {
        conn.execute(sql, ())
            .await
            .map_err(|e| format!("{e}: {sql}"))?
    };
    let ms = started.elapsed().as_millis() as u64;
    let after = db.logical_log_bytes();
    Ok(json!({
        "open_s": open_s,
        "fold_log_bytes_after": fold.log_bytes_after,
        "fold_error": fold.error,
        "ms": ms,
        "rows": affected,
        "log_bytes_before": before,
        "log_bytes_after": after,
        "log_bytes_delta": after.saturating_sub(before),
    }))
}

/// Run passes until every named rule has finished, accumulating each rule's
/// pages. Passes are unbounded in time and rows but fold the log exactly as
/// production does, so the log peak and the per-write times are the
/// production shape of a full catch-up.
async fn catchup(path: &str, now: &str, rules: &str) -> Res<Json> {
    let now: i64 = now.parse().map_err(|_| format!("bad clock {now}"))?;
    let rules = rules
        .split(',')
        .map(|name| {
            Eviction::ALL
                .into_iter()
                .find(|rule| rule.name() == name)
                .ok_or_else(|| format!("unknown rule {name}"))
        })
        .collect::<Res<Vec<_>>>()?;
    let (db, open_s) = open(path).await?;
    let budget = EvictorBudget {
        pass_time: Duration::from_secs(24 * 3600),
        pass_rows: usize::MAX,
        folds_per_pass: usize::MAX,
        ..EvictorBudget::PRODUCTION
    };
    let log_at_start = db.logical_log_bytes();
    let started = Instant::now();
    let mut out = Vec::new();
    let mut log_peak = log_at_start;
    let mut folds = Vec::new();
    let mut passes = 0;
    let mut state = EvictorState::default();
    for rule in rules {
        let rule_started = Instant::now();
        let (mut pages, mut rows, mut max_page_rows) = (0usize, 0usize, 0usize);
        let (mut select_ms, mut write_ms) = (Vec::new(), Vec::new());
        let mut failed_folds = 0;
        loop {
            passes += 1;
            let report = run_rules(&db, now, budget, &mut state, &[rule]).await;
            log_peak = log_peak.max(report.log_peak_bytes);
            folds.extend(report.folds.iter().map(|fold| {
                json!({
                    "before_mib": fold.log_bytes_before as f64 / 1048576.0,
                    "after_mib": fold.log_bytes_after as f64 / 1048576.0,
                    "ms": fold.duration.as_millis() as u64,
                    "work_ms": fold.work().as_millis() as u64,
                    "drain_ms": fold.drain.as_millis() as u64,
                    "ok": fold.succeeded,
                })
            }));
            let Some(result) = report.rules.first() else {
                break;
            };
            pages += result.pages;
            rows += result.rows;
            max_page_rows = max_page_rows.max(result.max_page_rows);
            select_ms.extend(&result.select_ms);
            write_ms.extend(&result.write_ms);
            if let Some(error) = &result.error {
                return Err(format!("{}: {error}", rule.name()));
            }
            if result.finished {
                break;
            }
            if report.end == PassEnd::FoldFailed {
                failed_folds += 1;
                if failed_folds > 3 {
                    return Err(format!("{}: folds keep failing", rule.name()));
                }
            }
        }
        let secs = rule_started.elapsed().as_secs_f64();
        let stats = |values: &[u64]| {
            json!({
                "p50": quantile(values, 0.5),
                "p90": quantile(values, 0.9),
                "p99": quantile(values, 0.99),
                "max": values.iter().max().copied().unwrap_or(0),
            })
        };
        out.push(json!({
            "rule": rule.name(),
            "pages": pages,
            "rows": rows,
            "max_page_rows": max_page_rows,
            "seconds": secs,
            "rows_per_s": rows as f64 / secs.max(1e-9),
            "select_ms": stats(&select_ms),
            "write_ms": stats(&write_ms),
            "writes_over_1s": write_ms.iter().filter(|ms| **ms > 1000).count(),
        }));
    }
    Ok(json!({
        "open_s": open_s,
        "seconds": started.elapsed().as_secs_f64(),
        "passes": passes,
        "log_mib_at_start": log_at_start as f64 / 1048576.0,
        "log_mib_peak": log_peak as f64 / 1048576.0,
        "log_mib_at_end": db.logical_log_bytes() as f64 / 1048576.0,
        "fold_count": folds.len(),
        "fold_ms_max": folds.iter().filter_map(|f| f["ms"].as_u64()).max().unwrap_or(0),
        "fold_work_ms": fold_work(&folds),
        "folds_failed": folds.iter().filter(|f| f["ok"] == json!(false)).count(),
        "rules": out,
    }))
}

fn fold_work(folds: &[Json]) -> Json {
    let work: Vec<u64> = folds.iter().filter_map(|f| f["work_ms"].as_u64()).collect();
    json!({
        "p50": quantile(&work, 0.5),
        "p99": quantile(&work, 0.99),
        "max": work.iter().max().copied().unwrap_or(0),
        "total": work.iter().sum::<u64>(),
    })
}

fn json_value(value: Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Integer(i) => json!(i),
        Value::Real(r) => json!(r),
        Value::Text(t) => json!(t),
        Value::Blob(b) => json!(format!("<blob {} bytes>", b.len())),
    }
}
