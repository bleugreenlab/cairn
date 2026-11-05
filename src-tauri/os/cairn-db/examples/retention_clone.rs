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
//!   loadfold <db> <target_mib> <cpu_threads> <fsync:on|off>
//!                              grow the log to `target_mib` in a scratch table
//!                              (`fold_probe`: 4 KiB blobs, every third deleted),
//!                              then fold it with the production checkpoint call
//!                              while `cpu_threads` busy-spin threads, an optional
//!                              1 MiB write + fsync loop in the clone's directory,
//!                              and 16 pooled readers plus 4 pooled writers run;
//!                              report the fold's throughput and what every
//!                              concurrent caller saw (calls, `database is
//!                              locked`, other errors, longest wait). Each row
//!                              carries the host's load average, because on a
//!                              busy host the storm adds to real load rather than
//!                              simulating it
//!   passes <db> <now> <n> [folds_per_pass]
//!                              run `n` production evictor passes
//!                              (`retention::run_pass`, `EvictorBudget::PRODUCTION`,
//!                              with the fold cap overridden when given) back to
//!                              back on one `EvictorState`, under loadfold's full
//!                              storm (12 spinners, fsync, pooled callers), and
//!                              report the rules each pass served, its folds and
//!                              writes, the cost per page each rule learned, and
//!                              what the concurrent callers saw
//!   foldfit <db> <now> <rules> <windows> <reps> <max_seconds>
//!                              under the same storm, for each comma-separated rule
//!                              and window size (pages, 0 for a fold of the storm's
//!                              log alone), fold the log, delete that many pages,
//!                              and fold them with the production call; report each
//!                              fold's work and the fit of work against pages as a
//!                              fixed floor plus a per-page slope, per rule. No new
//!                              window starts past `max_seconds`
//!   qosprobe                   the QoS class a plain `std::thread` gets when a
//!                              user-initiated thread spawns it, which is how a
//!                              production runtime worker spawns the checkpoint
//!                              thread (macOS only)
//!
//! Usage:
//!   cargo run --release -p cairn-db --example retention_clone -- fold <clone>/cairn.db

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_db::storage::census::refuse_live_directory;
use cairn_db::storage::retention::{
    quantile, run_pass, run_rules, Eviction, EvictorBudget, EvictorState, PassEnd,
};
use cairn_db::storage::{CheckpointReport, DrainBudget, LocalDb, RowExt};
use cairn_db::turso::Value;
use rand::RngCore;
use serde_json::{json, Value as Json};

type Res<T> = Result<T, String>;

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
        ["loadfold", db, mib, threads, fsync] => loadfold(db, mib, threads, fsync).await,
        ["qosprobe"] => Ok(qos_probe()),
        ["passes", db, now, n] => passes(db, now, n, None).await,
        ["passes", db, now, n, folds] => passes(db, now, n, Some(folds)).await,
        ["foldfit", db, now, rules, windows, reps, max_seconds] => {
            foldfit(db, now, rules, windows, reps, max_seconds).await
        }
        _ => Err(
            "usage: retention_clone fold|sql|catchup|logdelta|loadfold <db> ... (see the module \
             docs)"
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
    refuse_live_directory(Path::new(path))
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
    let rules = parse_rules(rules)?;
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

fn parse_rules(rules: &str) -> Res<Vec<Eviction>> {
    rules
        .split(',')
        .map(|name| {
            Eviction::ALL
                .into_iter()
                .find(|rule| rule.name() == name)
                .ok_or_else(|| format!("unknown rule {name}"))
        })
        .collect()
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

/// Rows the log-growing loop inserts per write transaction.
const GROW_BATCH: usize = 256;
/// The payload of one `fold_probe` row.
const GROW_ROW_BYTES: usize = 4096;
/// Pooled readers and writers running through a fold.
const LOAD_READERS: usize = 16;
const LOAD_WRITERS: usize = 4;
/// The pause each load task takes between calls. A loop over `LocalDb` calls
/// with no other await monopolises its worker, because the engine's futures
/// never return `Pending`, and enough of them starve the timer driver
/// (cairn/5644, builder finding 4). Twenty tasks at this pace offer about
/// 4,000 calls a second, far above production's ~70, so a wait a caller pays
/// here is the fold's and not the queue's.
const LOAD_PACE: Duration = Duration::from_millis(5);
/// How long the storm runs before the fold starts, so the fold meets it at
/// full strength.
const STORM_SETTLE: Duration = Duration::from_secs(2);

async fn loadfold(path: &str, mib: &str, threads: &str, fsync: &str) -> Res<Json> {
    let target_mib: u64 = mib.parse().map_err(|_| format!("bad target_mib {mib}"))?;
    let cpu_threads: usize = threads
        .parse()
        .map_err(|_| format!("bad cpu_threads {threads}"))?;
    let fsync = match fsync {
        "on" => true,
        "off" => false,
        other => return Err(format!("fsync must be on or off, not {other}")),
    };
    let dir = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .to_path_buf();

    let (db, open_s) = open(path).await?;
    let db = Arc::new(db);
    let inherited = quiet_fold(&db).await?;
    for sql in [
        "CREATE TABLE IF NOT EXISTS fold_probe (id INTEGER PRIMARY KEY, payload BLOB)",
        "CREATE TABLE IF NOT EXISTS fold_probe_load (id INTEGER PRIMARY KEY, at INTEGER)",
        "DELETE FROM fold_probe",
        "DELETE FROM fold_probe_load",
    ] {
        db.execute(sql, ())
            .await
            .map_err(|e| format!("{e}: {sql}"))?;
    }
    let reset = quiet_fold(&db).await?;

    let grow_started = Instant::now();
    let rows = grow_log(&db, target_mib * 1024 * 1024).await?;
    let grow_s = grow_started.elapsed().as_secs_f64();

    let load_avg_before = load_average();
    let load = Load::start(db.clone(), cpu_threads, fsync.then_some(dir));
    tokio::time::sleep(STORM_SETTLE).await;

    let fold_started = Instant::now();
    let report = db
        .checkpoint(3, Duration::from_millis(50), DrainBudget::PRODUCTION)
        .await
        .map_err(|e| e.to_string())?;
    let returned_ms = fold_started.elapsed().as_millis() as u64;
    // An abandoned attempt keeps folding in the engine with the gate open, and
    // the callers that meet it then are the ones this measurement is for, so
    // the load runs until the attempt has left.
    while db.checkpoint_in_flight() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let in_engine_ms = fold_started.elapsed().as_millis() as u64;
    let load_avg_during = load_average();
    let load = load.finish().await;

    let drain_ms = report.drain.waited.as_millis() as u64;
    let fold_ms = in_engine_ms.saturating_sub(drain_ms);
    let log_mib_before = report.log_bytes_before as f64 / 1048576.0;
    Ok(json!({
        "target_mib": target_mib,
        "cpu_threads": cpu_threads,
        "fsync": fsync,
        "load_avg_before": load_avg_before,
        "load_avg_during": load_avg_during,
        "open_s": open_s,
        "inherited_fold": inherited,
        "reset_fold": reset,
        "grow_rows": rows,
        "grow_s": grow_s,
        "log_mib_before": log_mib_before,
        "log_mib_after": db.logical_log_bytes() as f64 / 1048576.0,
        "drain_ms": drain_ms,
        "drained": report.drain.drained,
        "open_at_close": report.drain.open_at_close,
        "returned_ms": returned_ms,
        "fold_ms": fold_ms,
        "mib_per_s": log_mib_before / (fold_ms.max(1) as f64 / 1000.0),
        "attempts": report.attempts,
        "outcome": if report.succeeded() { "checkpointed" } else { "failed" },
        "error": report.error,
        "report": report.to_string(),
        "load": load,
    }))
}

async fn passes(path: &str, now: &str, n: &str, folds: Option<&&str>) -> Res<Json> {
    let now: i64 = now.parse().map_err(|_| format!("bad clock {now}"))?;
    let n: usize = n.parse().map_err(|_| format!("bad pass count {n}"))?;
    let budget = match folds {
        None => EvictorBudget::PRODUCTION,
        Some(folds) => EvictorBudget {
            folds_per_pass: folds
                .parse()
                .map_err(|_| format!("bad folds_per_pass {folds}"))?,
            ..EvictorBudget::PRODUCTION
        },
    };
    let dir = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let (db, open_s) = open(path).await?;
    let db = Arc::new(db);
    let inherited = quiet_fold(&db).await?;
    for sql in [
        "CREATE TABLE IF NOT EXISTS fold_probe (id INTEGER PRIMARY KEY, payload BLOB)",
        "CREATE TABLE IF NOT EXISTS fold_probe_load (id INTEGER PRIMARY KEY, at INTEGER)",
    ] {
        db.execute(sql, ())
            .await
            .map_err(|e| format!("{e}: {sql}"))?;
    }

    let load_avg_before = load_average();
    let load = Load::start(db.clone(), 12, Some(dir));
    tokio::time::sleep(STORM_SETTLE).await;
    let started = Instant::now();
    let mut state = EvictorState::default();
    let mut out = Vec::new();
    let mut abandoned = 0usize;
    let mut first_served: std::collections::BTreeMap<&str, usize> = Default::default();
    let mut all_writes = Vec::new();
    for pass in 1..=n {
        let report = run_pass(&db, now, budget, &mut state).await;
        let in_engine = db.checkpoint_in_flight();
        abandoned += usize::from(in_engine);
        while db.checkpoint_in_flight() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for rule in report.rules.iter().filter(|rule| rule.pages > 0) {
            first_served.entry(rule.eviction.name()).or_insert(pass);
        }
        let writes: Vec<u64> = report
            .rules
            .iter()
            .flat_map(|rule| rule.write_ms.iter().copied())
            .collect();
        all_writes.extend(&writes);
        out.push(json!({
            "pass": pass,
            "end": format!("{:?}", report.end),
            "rows": report.rows(),
            "duration_ms": report.duration.as_millis() as u64,
            "log_peak_mib": report.log_peak_bytes as f64 / 1048576.0,
            "attempt_left_in_engine": in_engine,
            "write_ms_p50": quantile(&writes, 0.5),
            "write_ms_max": writes.iter().max().copied().unwrap_or(0),
            "fold_work_ms_total": report.folds.iter()
                .map(|fold| fold.work().as_millis() as u64).sum::<u64>(),
            "fold_drain_ms_total": report.folds.iter()
                .map(|fold| fold.drain.as_millis() as u64).sum::<u64>(),
            "rules": report.rules.iter().map(|rule| json!({
                "rule": rule.eviction.name(),
                "pages": rule.pages,
                "rows": rule.rows,
                "write_ms_p50": quantile(&rule.write_ms, 0.5),
                "write_ms_max": rule.write_ms.iter().max().copied().unwrap_or(0),
                "select_ms_max": rule.select_ms.iter().max().copied().unwrap_or(0),
                "finished": rule.finished,
                "error": rule.error,
            })).collect::<Vec<_>>(),
            "folds": report.folds.iter().map(|fold| json!({
                "before_mib": fold.log_bytes_before as f64 / 1048576.0,
                "work_ms": fold.work().as_millis() as u64,
                "drain_ms": fold.drain.as_millis() as u64,
                "ok": fold.succeeded,
            })).collect::<Vec<_>>(),
            "line": report.to_string(),
        }));
    }
    let seconds = started.elapsed().as_secs_f64();
    let load_avg_during = load_average();
    let load = load.finish().await;
    let target = budget
        .fold_work_target
        .unwrap_or(Duration::from_millis(500));
    let learned: serde_json::Map<String, Json> = Eviction::ALL
        .iter()
        .map(|rule| {
            (
                rule.name().to_string(),
                json!({
                    "fold_ms_per_page": state.fold_ms_per_page(*rule),
                    "pages_per_fold": state.pages_per_fold(*rule, target),
                }),
            )
        })
        .collect();
    Ok(json!({
        "folds_per_pass": budget.folds_per_pass,
        "open_s": open_s,
        "inherited_fold": inherited,
        "load_avg_before": load_avg_before,
        "load_avg_during": load_avg_during,
        "seconds": seconds,
        "attempts_left_in_engine": abandoned,
        "first_served_pass": first_served,
        "write_ms_p50": quantile(&all_writes, 0.5),
        "write_ms_max": all_writes.iter().max().copied().unwrap_or(0),
        "writes_over_1s": all_writes.iter().filter(|ms| **ms > 1000).count(),
        "learned": learned,
        "passes": out,
        "load": load,
    }))
}

async fn foldfit(
    path: &str,
    now: &str,
    rules: &str,
    windows: &str,
    reps: &str,
    max_seconds: &str,
) -> Res<Json> {
    let now: i64 = now.parse().map_err(|_| format!("bad clock {now}"))?;
    let rules = parse_rules(rules)?;
    let windows = windows
        .split(',')
        .map(|w| w.parse().map_err(|_| format!("bad window {w}")))
        .collect::<Res<Vec<usize>>>()?;
    let reps: usize = reps.parse().map_err(|_| format!("bad reps {reps}"))?;
    let limit = Duration::from_secs(
        max_seconds
            .parse()
            .map_err(|_| format!("bad max_seconds {max_seconds}"))?,
    );
    let dir = Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let (db, open_s) = open(path).await?;
    let db = Arc::new(db);
    let inherited = quiet_fold(&db).await?;
    for sql in [
        "CREATE TABLE IF NOT EXISTS fold_probe (id INTEGER PRIMARY KEY, payload BLOB)",
        "CREATE TABLE IF NOT EXISTS fold_probe_load (id INTEGER PRIMARY KEY, at INTEGER)",
    ] {
        db.execute(sql, ())
            .await
            .map_err(|e| format!("{e}: {sql}"))?;
    }

    let load_avg_before = load_average();
    let load = Load::start(db.clone(), 12, Some(dir));
    tokio::time::sleep(STORM_SETTLE).await;
    let started = Instant::now();
    let mut samples: Vec<FoldSample> = Vec::new();
    'reps: for rep in 0..reps {
        for rule in &rules {
            // Rotated each rep, so a window size is not tied to one position
            // after a large window's dirty pages.
            for offset in 0..windows.len() {
                if started.elapsed() >= limit {
                    break 'reps;
                }
                let window = windows[(offset + rep) % windows.len()];
                samples.push(fold_window(&db, now, *rule, window).await?);
            }
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    let load_avg_during = load_average();
    let load = load.finish().await;

    let fits: Vec<Json> = rules
        .iter()
        .map(|rule| {
            let of_rule: Vec<&FoldSample> =
                samples.iter().filter(|s| s.rule == rule.name()).collect();
            fit_json(rule.name(), &of_rule, &windows)
        })
        .collect();
    Ok(json!({
        "open_s": open_s,
        "inherited_fold": inherited,
        "load_avg_before": load_avg_before,
        "load_avg_during": load_avg_during,
        "seconds": seconds,
        "windows_abandoned": samples.iter().filter(|s| s.abandoned).count(),
        "fits": fits,
        "samples": samples.iter().map(FoldSample::json).collect::<Vec<_>>(),
        "load": load,
    }))
}

/// One window `foldfit` deleted and folded.
struct FoldSample {
    rule: &'static str,
    wanted: usize,
    pages: usize,
    rows: usize,
    delete_ms: u64,
    write_ms_max: u64,
    log_bytes: u64,
    settle_work_ms: u64,
    work_ms: u64,
    drain_ms: u64,
    succeeded: bool,
    abandoned: bool,
    errors: Vec<String>,
}

impl FoldSample {
    fn json(&self) -> Json {
        json!({
            "rule": self.rule,
            "wanted": self.wanted,
            "pages": self.pages,
            "rows": self.rows,
            "delete_ms": self.delete_ms,
            "write_ms_max": self.write_ms_max,
            "log_mib": self.log_bytes as f64 / 1048576.0,
            "settle_work_ms": self.settle_work_ms,
            "work_ms": self.work_ms,
            "drain_ms": self.drain_ms,
            "succeeded": self.succeeded,
            "abandoned": self.abandoned,
            "errors": self.errors,
        })
    }
}

/// Fold the log, delete `wanted` pages of `rule`, and fold those with the
/// production call. A budget of one row ends each `run_rules` after one page,
/// and with no fold trigger it never folds on its own, so the window holds
/// exactly the pages asked for (fewer only when the rule runs out).
async fn fold_window(db: &LocalDb, now: i64, rule: Eviction, wanted: usize) -> Res<FoldSample> {
    let budget = EvictorBudget {
        pass_time: Duration::from_secs(3600),
        pass_rows: 1,
        fold_at_log_bytes: u64::MAX,
        fold_work_target: None,
        folds_per_pass: 0,
        slow_write: Duration::MAX,
    };
    let (settle, settle_abandoned) = production_fold(db).await?;
    let mut state = EvictorState::default();
    let (mut pages, mut rows, mut write_ms_max) = (0usize, 0usize, 0u64);
    let mut errors = Vec::new();
    let delete_started = Instant::now();
    for _ in 0..wanted {
        let report = run_rules(db, now, budget, &mut state, &[rule]).await;
        let Some(result) = report.rules.first() else {
            break;
        };
        pages += result.pages;
        rows += result.rows;
        write_ms_max = write_ms_max.max(result.write_ms.iter().max().copied().unwrap_or(0));
        errors.extend(result.error.clone());
        if result.finished || result.error.is_some() {
            break;
        }
    }
    let delete_ms = delete_started.elapsed().as_millis() as u64;
    let log_bytes = db.logical_log_bytes();
    let (fold, abandoned) = production_fold(db).await?;
    errors.extend(settle.error.clone());
    errors.extend(fold.error.clone());
    Ok(FoldSample {
        rule: rule.name(),
        wanted,
        pages,
        rows,
        delete_ms,
        write_ms_max,
        log_bytes,
        settle_work_ms: settle
            .duration
            .saturating_sub(settle.drain.waited)
            .as_millis() as u64,
        work_ms: fold.duration.saturating_sub(fold.drain.waited).as_millis() as u64,
        drain_ms: fold.drain.waited.as_millis() as u64,
        succeeded: fold.succeeded(),
        abandoned: abandoned || settle_abandoned,
        errors,
    })
}

/// The evictor's checkpoint call, waiting out an attempt the backstop gave up
/// on so the next window starts from a log nothing is folding. The flag says
/// whether that happened.
async fn production_fold(db: &LocalDb) -> Res<(CheckpointReport, bool)> {
    let report = db
        .checkpoint(3, Duration::from_millis(50), DrainBudget::PRODUCTION)
        .await
        .map_err(|e| e.to_string())?;
    let abandoned = db.checkpoint_in_flight();
    while db.checkpoint_in_flight() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok((report, abandoned))
}

/// Least squares of fold work on pages over one rule's windows, with the
/// median work at each window size beside it so a fit a line does not describe
/// shows as one.
fn fit_json(rule: &str, samples: &[&FoldSample], windows: &[usize]) -> Json {
    let n = samples.len() as f64;
    let mean_x = samples.iter().map(|s| s.pages as f64).sum::<f64>() / n.max(1.0);
    let mean_y = samples.iter().map(|s| s.work_ms as f64).sum::<f64>() / n.max(1.0);
    let (mut sxx, mut sxy, mut syy) = (0.0, 0.0, 0.0);
    for sample in samples {
        let (dx, dy) = (sample.pages as f64 - mean_x, sample.work_ms as f64 - mean_y);
        sxx += dx * dx;
        sxy += dx * dy;
        syy += dy * dy;
    }
    let slope = if sxx > 0.0 { sxy / sxx } else { 0.0 };
    let by_window: Vec<Json> = windows
        .iter()
        .map(|window| {
            let work: Vec<u64> = samples
                .iter()
                .filter(|s| s.wanted == *window)
                .map(|s| s.work_ms)
                .collect();
            let of_window = samples.iter().filter(|s| s.wanted == *window);
            json!({
                "pages": window,
                "samples": work.len(),
                "work_ms_median": quantile(&work, 0.5),
                "work_ms_max": work.iter().max().copied().unwrap_or(0),
                "rows_max": of_window.map(|s| s.rows).max().unwrap_or(0),
            })
        })
        .collect();
    let settles: Vec<u64> = samples.iter().map(|s| s.settle_work_ms).collect();
    json!({
        "rule": rule,
        "samples": samples.len(),
        "floor_ms": mean_y - slope * mean_x,
        "slope_ms_per_page": slope,
        "r2": if sxx > 0.0 && syy > 0.0 { sxy * sxy / (sxx * syy) } else { 0.0 },
        "settle_work_ms_median": quantile(&settles, 0.5),
        "by_window": by_window,
    })
}

/// Fold whatever the log holds with nothing else running, and say what it was
/// and how long the engine took. A cold first fold on a large clone can outlive
/// the pass's attempt bound with nothing else running, so an abandoned attempt
/// is waited out rather than treated as failure.
async fn quiet_fold(db: &LocalDb) -> Res<Json> {
    let started = Instant::now();
    let log_bytes_before = db.logical_log_bytes();
    let mut passes = Vec::new();
    for _ in 0..3 {
        let report = db
            .checkpoint(3, Duration::from_millis(50), DrainBudget::PRODUCTION)
            .await
            .map_err(|e| e.to_string())?;
        let succeeded = report.succeeded();
        passes.push(report.to_string());
        if succeeded {
            break;
        }
        while db.checkpoint_in_flight() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if db.logical_log_bytes() < 1024 * 1024 {
            break;
        }
    }
    if db.logical_log_bytes() >= 1024 * 1024 {
        return Err(format!("a quiet fold did not fold: {passes:?}"));
    }
    Ok(json!({
        "log_mib_before": log_bytes_before as f64 / 1048576.0,
        "ms": started.elapsed().as_millis() as u64,
        "passes": passes,
    }))
}

/// Write `fold_probe` rows until the logical log reaches `target` bytes: 4 KiB
/// random blobs, a third of each batch deleted again, the delete-heavy shape
/// production's log has. Returns the rows inserted.
async fn grow_log(db: &LocalDb, target: u64) -> Res<usize> {
    let mut next_id = 1i64;
    while db.logical_log_bytes() < target {
        let blobs: Vec<Vec<u8>> = (0..GROW_BATCH)
            .map(|_| {
                let mut blob = vec![0u8; GROW_ROW_BYTES];
                rand::thread_rng().fill_bytes(&mut blob);
                blob
            })
            .collect();
        let first = next_id;
        let last = first + GROW_BATCH as i64 - 1;
        db.write(move |conn| {
            let blobs = blobs.clone();
            Box::pin(async move {
                for (offset, blob) in blobs.into_iter().enumerate() {
                    conn.execute(
                        "INSERT INTO fold_probe (id, payload) VALUES (?1, ?2)",
                        (first + offset as i64, Value::Blob(blob)),
                    )
                    .await?;
                }
                conn.execute(
                    "DELETE FROM fold_probe WHERE id BETWEEN ?1 AND ?2 AND id % 3 = 0",
                    (first, last),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("growing the log: {e}"))?;
        next_id = last + 1;
        tokio::task::yield_now().await;
    }
    Ok((next_id - 1) as usize)
}

/// The host's one-minute load average, from `sysctl vm.loadavg` (macOS) or
/// `/proc/loadavg` (Linux).
fn load_average() -> Option<f64> {
    let text = std::process::Command::new("sysctl")
        .args(["-n", "vm.loadavg"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
        .or_else(|| std::fs::read_to_string("/proc/loadavg").ok())?;
    text.split_whitespace()
        .find_map(|field| field.parse::<f64>().ok())
}

/// What one kind of concurrent caller saw.
#[derive(Default)]
struct CallStats {
    calls: u64,
    locked: u64,
    other_errors: u64,
    first_error: Option<String>,
    waits_ms: Vec<u64>,
}

impl CallStats {
    fn record(&mut self, started: Instant, outcome: Result<(), String>) {
        self.calls += 1;
        self.waits_ms.push(started.elapsed().as_millis() as u64);
        if let Err(error) = outcome {
            if error.contains("locked") || error.to_ascii_lowercase().contains("busy") {
                self.locked += 1;
            } else {
                self.other_errors += 1;
            }
            self.first_error.get_or_insert(error);
        }
    }

    fn merge(&mut self, other: CallStats) {
        self.calls += other.calls;
        self.locked += other.locked;
        self.other_errors += other.other_errors;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
        self.waits_ms.extend(other.waits_ms);
    }

    fn json(&self) -> Json {
        json!({
            "calls": self.calls,
            "locked": self.locked,
            "other_errors": self.other_errors,
            "first_error": self.first_error,
            "wait_ms_p50": quantile(&self.waits_ms, 0.5),
            "wait_ms_p99": quantile(&self.waits_ms, 0.99),
            "wait_ms_max": self.waits_ms.iter().max().copied().unwrap_or(0),
        })
    }
}

/// A storm for a fold to run through: CPU spinners, an optional write + fsync
/// loop on the clone's volume, and pooled database callers measuring what they
/// see. Runs until [`Load::finish`].
struct Load {
    stop: Arc<AtomicBool>,
    spinners: Vec<std::thread::JoinHandle<()>>,
    fsync: Option<std::thread::JoinHandle<Result<u64, String>>>,
    readers: Vec<tokio::task::JoinHandle<CallStats>>,
    writers: Vec<tokio::task::JoinHandle<CallStats>>,
}

impl Load {
    fn start(db: Arc<LocalDb>, cpu_threads: usize, fsync_dir: Option<PathBuf>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let spinners = (0..cpu_threads)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    // The storm production's folds met was the orchestrator's
                    // own runtime workers, which run at user-initiated QoS,
                    // while a spawned checkpoint thread comes up at default
                    // (see `qosprobe`). The spinners stand in for the workers.
                    cairn_platform::priority::raise_current_thread_for_orchestration();
                    let mut x = 1u64;
                    while !stop.load(Ordering::Relaxed) {
                        for _ in 0..100_000 {
                            x = std::hint::black_box(
                                x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1),
                            );
                        }
                    }
                })
            })
            .collect();
        let fsync = fsync_dir.map(|dir| {
            let stop = stop.clone();
            std::thread::spawn(move || fsync_loop(&dir.join("fold_probe_fsync.bin"), &stop))
        });
        let readers = (0..LOAD_READERS)
            .map(|reader| {
                let (db, stop) = (db.clone(), stop.clone());
                tokio::spawn(async move {
                    let mut stats = CallStats::default();
                    let mut id = reader as i64;
                    while !stop.load(Ordering::Relaxed) {
                        id = (id * 7 + 13) % 50_000;
                        let started = Instant::now();
                        let outcome = db
                            .query_one(
                                "SELECT COUNT(*) FROM fold_probe WHERE id = ?1",
                                (id,),
                                |row| row.i64(0),
                            )
                            .await
                            .map(drop)
                            .map_err(|e| e.to_string());
                        stats.record(started, outcome);
                        tokio::time::sleep(LOAD_PACE).await;
                    }
                    stats
                })
            })
            .collect();
        let writers = (0..LOAD_WRITERS)
            .map(|_| {
                let (db, stop) = (db.clone(), stop.clone());
                tokio::spawn(async move {
                    let mut stats = CallStats::default();
                    let mut at = 0i64;
                    while !stop.load(Ordering::Relaxed) {
                        at += 1;
                        let started = Instant::now();
                        let outcome = db
                            .execute("INSERT INTO fold_probe_load (at) VALUES (?1)", (at,))
                            .await
                            .map(drop)
                            .map_err(|e| e.to_string());
                        stats.record(started, outcome);
                        tokio::time::sleep(LOAD_PACE).await;
                    }
                    stats
                })
            })
            .collect();
        Self {
            stop,
            spinners,
            fsync,
            readers,
            writers,
        }
    }

    async fn finish(self) -> Json {
        self.stop.store(true, Ordering::Relaxed);
        let mut reads = CallStats::default();
        for reader in self.readers {
            reads.merge(reader.await.unwrap_or_default());
        }
        let mut writes = CallStats::default();
        for writer in self.writers {
            writes.merge(writer.await.unwrap_or_default());
        }
        for spinner in self.spinners {
            let _ = spinner.join();
        }
        let fsyncs = self
            .fsync
            .map(|handle| handle.join().unwrap_or_else(|_| Err("panicked".into())));
        json!({
            "reads": reads.json(),
            "writes": writes.json(),
            "fsyncs": match fsyncs {
                None => Json::Null,
                Some(Ok(count)) => json!(count),
                Some(Err(error)) => json!(error),
            },
        })
    }
}

/// Write 1 MiB and fsync it, round a 64 MiB file, until `stop`; the file is
/// removed afterwards. Returns how many syncs completed.
fn fsync_loop(path: &Path, stop: &AtomicBool) -> Result<u64, String> {
    const CHUNK: usize = 1 << 20;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let chunk = vec![0xA5u8; CHUNK];
    let mut syncs = 0u64;
    let outcome = (|| {
        while !stop.load(Ordering::Relaxed) {
            file.seek(SeekFrom::Start((syncs % 64) * CHUNK as u64))?;
            file.write_all(&chunk)?;
            file.sync_all()?;
            syncs += 1;
        }
        Ok::<_, std::io::Error>(())
    })();
    drop(file);
    let _ = std::fs::remove_file(path);
    outcome.map(|()| syncs).map_err(|e| e.to_string())
}

#[cfg(target_os = "macos")]
extern "C" {
    fn qos_class_self() -> u32;
}

fn qos_probe() -> Json {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: each call reads the calling thread's own QoS class.
        let (parent, child) = std::thread::spawn(|| {
            cairn_platform::priority::raise_current_thread_for_orchestration();
            let parent = unsafe { qos_class_self() };
            let child = std::thread::spawn(|| unsafe { qos_class_self() })
                .join()
                .unwrap();
            (parent, child)
        })
        .join()
        .unwrap();
        json!({
            "parent_qos": format!("{parent:#x}"),
            "spawned_child_qos": format!("{child:#x}"),
            "user_initiated": "0x19",
            "default": "0x15",
        })
    }
    #[cfg(not(target_os = "macos"))]
    json!({ "error": "macOS only" })
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
