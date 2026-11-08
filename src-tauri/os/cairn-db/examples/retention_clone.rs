//! Retention rehearsals and a bounded synthetic migration probe.
//!
//! `synthetic <temp-root>` (Linux) creates a fresh temporary DB under
//! `<temp-root>`, applies every migration before the historical-observation
//! relink through the real MigrationRunner, seeds post-trim attribution rows
//! (no jobId, requestId, failureNames or failureKind; name and ranAt kept)
//! with unique, missed, ambiguous, undated and already-cited lanes, then times
//! the relink and checks every row against the expected result. The
//! directory is removed before the report prints. A synthetic measurement,
//! not a clone measurement.
//!
//! Point it at an APFS clone of the three-file set, never at the live file:
//!
//!   cp -c ~/.cairn/cairn.db ~/.cairn/cairn.db-log ~/.cairn/cairn.db-wal <dir>/
//!
//! `<dir>` must be on `~/.cairn`'s volume, and the clone must not be kept beside
//! the running database; docs/database.md "Taking a census" has the sequence.
//!
//! Every subcommand refuses a database inside a live Cairn data directory and
//! any path not ending in `.db` (Turso names the logical log from the
//! extension; cairn/5590). Each prints one JSON document on stdout:
//!
//!   fold <db>                  TRUNCATE-checkpoint the clone's logical log
//!   migration <db> <name>      apply one named pending migration, timed as at boot
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
    if let Some(path) = args
        .get(1)
        .filter(|_| args.first().map(String::as_str) != Some("synthetic"))
    {
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
        ["synthetic", root] => synthetic_migration(root).await,
        ["fold", db] => fold(db).await,
        ["sql", db, sql] => adhoc(db, sql).await,
        ["migration", db, name] => migration(db, name).await,
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

async fn migration(path: &str, name: &str) -> Res<Json> {
    use cairn_db::storage::{MigrationRunner, TURSO_MIGRATIONS};
    let migration = TURSO_MIGRATIONS
        .iter()
        .find(|migration| migration.name() == name)
        .ok_or_else(|| format!("unknown migration: {name}"))?;
    let (db, open_s) = open(path).await?;
    let started = Instant::now();
    let applied = MigrationRunner::new(vec![*migration])
        .run(&db)
        .await
        .map_err(|e| e.to_string())?;
    if applied.is_empty() {
        return Err(format!("{name} already applied; no migration was timed"));
    }
    Ok(json!({ "open_s": open_s, "migration": name,
        "seconds": started.elapsed().as_secs_f64(), "applied": applied }))
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

// The live store's historical attribution rows after the original 0393 ran:
// lanes keep name, ranAt, state and counts, but no jobId, requestId,
// failureNames or failureKind. The job is recovered from the attribution's
// merge request, and the observation by an exact (job, check name, ranAt)
// match.
const SYNTHETIC_ROWS: usize = 3659;
const SYNTHETIC_LANES: usize = 41;
const SYNTHETIC_OBSERVATIONS: usize = 135_000;
/// Two attribution rows per merge request (merge plus a re-journal), so a
/// job's observation catalog is shared by rows that must not cite each other.
const ROWS_PER_MR: usize = 2;
const FORWARD_MIGRATION: &str = "relink_pr_resolution_lane_observations";

/// What a historical lane offers the forward migration.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LaneCase {
    /// Exactly one observation of the row's job, this check and this ranAt.
    /// Lanes 5, 15, 25 and 35 carry `observation: null`, which is no citation.
    Unique,
    /// None of the row's job; one of another job with the same name and ranAt.
    Miss,
    /// Two observations of the row's job, this check and this ranAt.
    Ambiguous,
    /// ranAt null (even lanes) or absent (odd lanes); a same-job, same-name
    /// observation exists at another time.
    NoRanAt,
    /// Already carries a citation (`observationId`, or an `observation` object
    /// on odd lanes) and also has a unique match that must not replace it.
    Cited,
}

/// Rows with lanes are two in three; a third of those have only null or
/// absent ranAt, which puts the share of lane-bearing rows with a real ranAt
/// near the live 1,643 of 2,386.
fn row_has_lanes(row: usize) -> bool {
    row % 3 != 2
}

fn lane_case(row: usize, lane: usize) -> LaneCase {
    if (row / 3) % 10 < 3 {
        return LaneCase::NoRanAt;
    }
    match lane % 5 {
        0 => LaneCase::Unique,
        1 => LaneCase::Miss,
        2 => LaneCase::Ambiguous,
        3 => LaneCase::NoRanAt,
        _ => LaneCase::Cited,
    }
}

fn row_job(row: usize) -> String {
    format!("job-{:04}", row / ROWS_PER_MR)
}

fn lane_ran_at(row: usize, lane: usize) -> i64 {
    1_762_000_000_000 + (row as i64) * 100 + lane as i64
}

fn lane_name(lane: usize) -> String {
    format!("check-{lane:02}")
}

fn post_trim_lane(row: usize, lane: usize) -> Json {
    let case = lane_case(row, lane);
    let mut value = json!({
        "cached": false, "complete": true, "deferred": false, "durationMs": 1422,
        "failed": 1, "name": lane_name(lane), "passed": 120, "policy": "advisory",
        "ranAt": lane_ran_at(row, lane), "skipped": 0, "state": "failed",
        "suiteFailures": 1, "suppressedAfter": null, "when": "review",
        "verifiedOn": {"head": "0123456789abcdef"}
    });
    let object = value.as_object_mut().unwrap();
    match case {
        LaneCase::NoRanAt if lane.is_multiple_of(2) => {
            object.insert("ranAt".into(), Json::Null);
        }
        LaneCase::NoRanAt => {
            object.remove("ranAt");
        }
        LaneCase::Cited if lane.is_multiple_of(2) => {
            object.insert(
                "observationId".into(),
                json!(format!("cited-{row:04}-{lane:02}")),
            );
        }
        LaneCase::Cited => {
            object.insert(
                "observation".into(),
                json!({"kind": "cited", "id": format!("cited-{row:04}-{lane:02}")}),
            );
        }
        LaneCase::Unique if lane % 10 == 5 => {
            object.insert("observation".into(), Json::Null);
        }
        _ => {}
    }
    value
}

fn post_trim_snapshot(row: usize) -> Json {
    let mut value = json!({
        "execution": {"executionStatus": "complete",
            "jobs": [{"node": "builder", "status": "complete"}]}
    });
    if row_has_lanes(row) {
        value["systematicChecks"] = (0..SYNTHETIC_LANES)
            .map(|lane| post_trim_lane(row, lane))
            .collect();
    } else if row % 6 == 2 {
        value["systematicChecks"] = json!([]);
    }
    value
}

/// The observations each lane's case calls for: (id, job, check, ran_at).
fn key_observations(row: usize, lane: usize) -> Vec<(String, String, String, i64)> {
    let (job, name, ran_at) = (row_job(row), lane_name(lane), lane_ran_at(row, lane));
    let id = |tag: &str| format!("{tag}-{row:04}-{lane:02}");
    match lane_case(row, lane) {
        LaneCase::Unique => vec![
            (id("obs"), job.clone(), name.clone(), ran_at),
            // Same job and check one millisecond later: not exact.
            (id("near"), job, name, ran_at + 1),
        ],
        LaneCase::Miss => vec![(id("otherjob"), format!("other-{job}"), name, ran_at)],
        LaneCase::Ambiguous => vec![
            (id("ambig-a"), job.clone(), name.clone(), ran_at),
            (id("ambig-b"), job, name, ran_at),
        ],
        LaneCase::NoRanAt if lane.is_multiple_of(4) || (row / 3) % 10 >= 3 => {
            vec![(id("undated"), job, name, ran_at)]
        }
        LaneCase::NoRanAt => Vec::new(),
        LaneCase::Cited => vec![(id("uncited"), job, name, ran_at)],
    }
}

/// The lane snapshot the forward migration must produce: the input unchanged
/// except a unique match gains the canonical citation new rows carry.
fn expected_snapshot(row: usize) -> Json {
    let mut value = post_trim_snapshot(row);
    // `get_mut`, not `value[..]`: indexing would insert a null key into the
    // rows that have none.
    if let Some(lanes) = value
        .get_mut("systematicChecks")
        .and_then(Json::as_array_mut)
    {
        for (lane, entry) in lanes.iter_mut().enumerate() {
            if lane_case(row, lane) == LaneCase::Unique {
                entry["observation"] =
                    json!({"kind": "cited", "id": format!("obs-{row:04}-{lane:02}")});
            }
        }
    }
    value
}

fn process_cpu_ticks() -> Res<[u64; 2]> {
    let stat = std::fs::read_to_string("/proc/self/stat").map_err(|e| e.to_string())?;
    // comm (field 2) may contain spaces or parentheses; fields after its final
    // ')' start at state (3). utime/stime (14/15) include all process threads.
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .ok_or("malformed process stat")?
        .1
        .split_whitespace()
        .collect();
    let parse = |index: usize| -> Res<u64> {
        fields
            .get(index)
            .ok_or("missing CPU stat")?
            .parse()
            .map_err(|e| format!("CPU stat: {e}"))
    };
    Ok([parse(11)?, parse(12)?])
}

fn linux_loads() -> Res<[f64; 3]> {
    let text = std::fs::read_to_string("/proc/loadavg").map_err(|e| e.to_string())?;
    let mut fields = text.split_whitespace();
    let mut result = [0.0; 3];
    for field in &mut result {
        *field = fields
            .next()
            .ok_or("missing load")?
            .parse()
            .map_err(|e| format!("load: {e}"))?;
    }
    Ok(result)
}

async fn synthetic_fold(db: &LocalDb) -> Res<()> {
    let fold = db
        .checkpoint(3, Duration::from_millis(250), DrainBudget::PRODUCTION)
        .await
        .map_err(|e| e.to_string())?;
    if !fold.succeeded() {
        return Err(format!("setup checkpoint failed: {:?}", fold.error));
    }
    Ok(())
}

/// Lane-level census of the attribution table, computed by SQL so it counts
/// what the store holds rather than what the fixture intended.
async fn lane_census(db: &LocalDb) -> Res<Json> {
    let mut rows = db
        .query_all(
            "SELECT COUNT(DISTINCT a.id),
                    COUNT(lane.key),
                    SUM(CASE WHEN json_type(lane.value, '$.ranAt') IN ('integer','real') THEN 1 ELSE 0 END),
                    COUNT(DISTINCT CASE WHEN json_type(lane.value, '$.ranAt') IN ('integer','real') THEN a.id END),
                    SUM(CASE WHEN json_type(lane.value, '$.observationId') IS NOT NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN json_extract(lane.value, '$.observation.kind') = 'cited' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN json_type(lane.value, '$.requestId') IS NOT NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN json_type(lane.value, '$.jobId') IS NOT NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN json_type(lane.value, '$.failureNames') IS NOT NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN json_type(lane.value, '$.failureKind') IS NOT NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN lane.type = 'object'
                              AND json_type(lane.value, '$.ranAt') = 'integer'
                              AND json_type(lane.value, '$.name') = 'text'
                              AND COALESCE(json_type(lane.value, '$.observation'), 'null') = 'null'
                              AND COALESCE(json_type(lane.value, '$.observationId'), 'null') = 'null'
                             THEN 1 ELSE 0 END)
               FROM pr_resolution_attributions a
               JOIN json_each(a.lane_snapshot, '$.systematicChecks') AS lane
              WHERE json_valid(a.lane_snapshot)
                AND json_type(a.lane_snapshot, '$.systematicChecks') = 'array'",
            (),
            |row| {
                Ok(json!({
                    "rows_with_lanes": row.get::<i64>(0)?,
                    "lanes": row.get::<i64>(1)?,
                    "lanes_with_ranAt": row.get::<Option<i64>>(2)?.unwrap_or(0),
                    "rows_with_a_ranAt_lane": row.get::<i64>(3)?,
                    "lanes_with_observationId": row.get::<Option<i64>>(4)?.unwrap_or(0),
                    "lanes_cited": row.get::<Option<i64>>(5)?.unwrap_or(0),
                    "lanes_with_requestId": row.get::<Option<i64>>(6)?.unwrap_or(0),
                    "lanes_with_jobId": row.get::<Option<i64>>(7)?.unwrap_or(0),
                    "lanes_with_failureNames": row.get::<Option<i64>>(8)?.unwrap_or(0),
                    "lanes_with_failureKind": row.get::<Option<i64>>(9)?.unwrap_or(0),
                    // The relink's own candidate filter: uncited, integer ranAt.
                    "uncited_integer_ranAt_lanes": row.get::<Option<i64>>(10)?.unwrap_or(0),
                }))
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    let mut out = rows.pop().ok_or("missing lane census")?;
    let totals = db
        .query_all(
            "SELECT COUNT(*), COALESCE(SUM(length(CAST(lane_snapshot AS BLOB))), 0)
               FROM pr_resolution_attributions",
            (),
            |row| Ok((row.get::<i64>(0)?, row.get::<i64>(1)?)),
        )
        .await
        .map_err(|e| e.to_string())?;
    let (count, bytes) = totals.first().copied().ok_or("missing totals")?;
    out["rows"] = json!(count);
    out["lane_snapshot_bytes"] = json!(bytes);
    Ok(out)
}

async fn table_count(db: &LocalDb, table: &str) -> Res<i64> {
    db.query_opt_i64(format!("SELECT COUNT(*) FROM {table}"), ())
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("missing count for {table}"))
}

fn leftover_dirs(root: &str, prefix: &str) -> Res<Vec<String>> {
    Ok(std::fs::read_dir(root)
        .map_err(|e| format!("{root}: {e}"))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(prefix))
        .collect())
}

const SYNTHETIC_PREFIX: &str = "synthetic-forward-";

async fn synthetic_migration(root: &str) -> Res<Json> {
    let leftovers_before = leftover_dirs(root, SYNTHETIC_PREFIX)?;
    let dir = tempfile::Builder::new()
        .prefix(SYNTHETIC_PREFIX)
        .tempdir_in(root)
        .map_err(|e| e.to_string())?;
    // `dir` outlives everything that opens the database, so an early return
    // drops the database first and then removes the directory.
    let mut report = synthetic_run(&dir.path().join("synthetic.db")).await?;
    dir.close().map_err(|e| format!("cleanup: {e}"))?;
    report["cleanup"] = json!({
        "tempdir_closed": true,
        "same_prefix_dirs_before": leftovers_before,
        "same_prefix_dirs_after": leftover_dirs(root, SYNTHETIC_PREFIX)?,
    });
    Ok(report)
}

async fn synthetic_run(path: &Path) -> Res<Json> {
    use cairn_db::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use cairn_db::turso::params;
    let setup_started = Instant::now();
    let target = *TURSO_MIGRATIONS
        .iter()
        .find(|m| m.name() == FORWARD_MIGRATION)
        .ok_or_else(|| format!("{FORWARD_MIGRATION} missing from TURSO_MIGRATIONS"))?;
    let prior: Vec<_> = TURSO_MIGRATIONS
        .iter()
        .take_while(|m| m.name() != FORWARD_MIGRATION)
        .copied()
        .collect();
    if !prior
        .iter()
        .any(|m| m.name() == "trim_pr_resolution_lane_snapshot")
    {
        return Err("the trim migration must precede the forward migration".into());
    }
    check_path(path.to_str().ok_or("non-UTF8 path")?)?;
    let db = LocalDb::open(path).await.map_err(|e| e.to_string())?;
    MigrationRunner::new(prior.clone())
        .run(&db)
        .await
        .map_err(|e| e.to_string())?;
    db.execute_script(
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
         INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('p', 'w', 'P', 'p', '/tmp/p', 1, 1);
         INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
          VALUES('i', 'p', 1, 'I', 'merged', 'merged', 'none', 1, 1);",
    )
    .await
    .map_err(|e| e.to_string())?;
    let mrs = SYNTHETIC_ROWS.div_ceil(ROWS_PER_MR);
    for start in (0..mrs).step_by(256) {
        db.write(move |conn| {
            Box::pin(async move {
                for mr in start..(start + 256).min(mrs) {
                    let job = format!("job-{mr:04}");
                    conn.execute(
                        "INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
                         VALUES(?1, 'p', 'i', 'complete', 1, 1)",
                        params![job.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title,
                          source_branch, target_branch, status, opened_at, updated_at)
                         VALUES(?1, ?2, 'p', 'i', 'PR', 'feature', 'main', 'merged', 1, 2)",
                        params![format!("mr-{mr:04}").as_str(), job.as_str()],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    }

    let mut observations = Vec::new();
    let mut expected_cases = std::collections::BTreeMap::<String, usize>::new();
    for row in (0..SYNTHETIC_ROWS).filter(|row| row_has_lanes(*row)) {
        for lane in 0..SYNTHETIC_LANES {
            *expected_cases
                .entry(format!("{:?}", lane_case(row, lane)))
                .or_default() += 1;
            observations.extend(key_observations(row, lane));
        }
    }
    let key_observation_count = observations.len();
    if key_observation_count > SYNTHETIC_OBSERVATIONS {
        return Err(format!(
            "{key_observation_count} key observations exceed the catalog"
        ));
    }
    // The rest of the catalog: the same jobs and checks at other times, so the
    // match has to discriminate by ranAt within a job's own history.
    for index in key_observation_count..SYNTHETIC_OBSERVATIONS {
        observations.push((
            format!("catalog-{index}"),
            format!("job-{:04}", index % mrs),
            lane_name(index % SYNTHETIC_LANES),
            1_000 + index as i64,
        ));
    }
    for (batch, chunk) in observations.chunks(256).enumerate() {
        db.write(|conn| {
            let chunk = chunk.to_vec();
            Box::pin(async move {
                for (id, job, name, ran_at) in &chunk {
                    conn.execute(
                        "INSERT INTO check_result_observations
                         (id, project_id, commit_sha, tree_hash, check_name, input_hash,
                          environment_fingerprint, exit_code, verdict, failure_kind, complete,
                          reusable, parser_version, result_schema_version, ran_at, duration_ms,
                          cadence, output_tail, job_id)
                         VALUES (?1,'p','c','t',?2,'i','env',1,'failed','testFailure',1,0,1,1,?3,
                                 1422,'review','test failed',?4)",
                        params![id.as_str(), name.as_str(), *ran_at, job.as_str()],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())?;
        if batch % 8 == 7 {
            synthetic_fold(&db).await?;
        }
    }
    drop(observations);
    synthetic_fold(&db).await?;

    for start in (0..SYNTHETIC_ROWS).step_by(64) {
        let batch: Vec<(String, String, String)> = (start..(start + 64).min(SYNTHETIC_ROWS))
            .map(|row| {
                (
                    format!("attribution-{row:04}"),
                    format!("mr-{:04}", row / ROWS_PER_MR),
                    post_trim_snapshot(row).to_string(),
                )
            })
            .collect();
        db.write(|conn| {
            let batch = batch.clone();
            Box::pin(async move {
                for (id, mr, value) in batch {
                    conn.execute(
                        "INSERT INTO pr_resolution_attributions
                        (id, merge_request_id, action, actor_kind, surface, lane_snapshot, created_at)
                        VALUES (?1, ?2, 'merge', 'operator-ui', 'operator-ui', ?3, 2)",
                        params![id.as_str(), mr.as_str(), value.as_str()],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    }
    synthetic_fold(&db).await?;

    let before = lane_census(&db).await?;
    let observations_before = table_count(&db, "check_result_observations").await?;
    let mrs_before = table_count(&db, "merge_requests").await?;
    let unique_lanes = expected_cases.get("Unique").copied().unwrap_or(0) as i64;
    let cited_lanes = expected_cases.get("Cited").copied().unwrap_or(0) as i64;
    if before["rows"] != json!(SYNTHETIC_ROWS)
        || observations_before != SYNTHETIC_OBSERVATIONS as i64
        || before["lanes_with_requestId"] != json!(0)
        || before["lanes_with_jobId"] != json!(0)
        || before["lanes_with_failureNames"] != json!(0)
        || before["lanes_with_failureKind"] != json!(0)
    {
        return Err(format!("fixture mismatch: {before}"));
    }
    let setup_seconds = setup_started.elapsed().as_secs_f64();
    eprintln!("synthetic setup complete in {setup_seconds:.3}s: {before}");

    let ticks_per_second: f64 = {
        let clock = std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .map_err(|e| e.to_string())?;
        if !clock.status.success() {
            return Err("getconf CLK_TCK failed".into());
        }
        String::from_utf8_lossy(&clock.stdout)
            .trim()
            .parse()
            .map_err(|e| format!("CLK_TCK: {e}"))?
    };
    if ticks_per_second <= 0.0 {
        return Err("invalid CLK_TCK".into());
    }
    let loads_before = linux_loads()?;
    let migration_started_utc = chrono::Utc::now().to_rfc3339();
    eprintln!("synthetic migration starting at {migration_started_utc}, loads {loads_before:?}");
    let done = Arc::new(AtomicBool::new(false));
    let watchdog_done = done.clone();
    let watchdog = std::thread::spawn(move || {
        for _ in 0..300 {
            if watchdog_done.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if !watchdog_done.load(Ordering::Acquire) {
            eprintln!("SYNTHETIC MIGRATION EXCEEDS 30 SECONDS");
        }
    });
    let cpu_before = process_cpu_ticks()?;
    let started = Instant::now();
    let applied = MigrationRunner::new(vec![target]).run(&db).await;
    let seconds = started.elapsed().as_secs_f64();
    let cpu_after = process_cpu_ticks()?;
    let loads_after = linux_loads()?;
    done.store(true, Ordering::Release);
    watchdog.join().map_err(|_| "watchdog panicked")?;
    let cpu_user_seconds = (cpu_after[0] - cpu_before[0]) as f64 / ticks_per_second;
    let cpu_system_seconds = (cpu_after[1] - cpu_before[1]) as f64 / ticks_per_second;
    let applied = applied.map_err(|e| format!("migration failed after {seconds:.3}s: {e}"))?;
    if applied.len() != 1 {
        return Err(format!("expected one applied migration: {applied:?}"));
    }
    eprintln!("synthetic migration finished: {seconds:.3}s, loads {loads_after:?}");

    let after = lane_census(&db).await?;
    // Every row compared whole: lanes, untouched siblings, and the columns
    // beside the snapshot. Any difference other than the expected citations
    // fails the probe and names the row.
    let mut mismatches = Vec::new();
    for row in 0..SYNTHETIC_ROWS {
        let id = format!("attribution-{row:04}");
        let found = db
            .query_all(
                "SELECT lane_snapshot, merge_request_id, created_at
                   FROM pr_resolution_attributions WHERE id = ?1",
                params![id.as_str()],
                |r| Ok((r.get::<String>(0)?, r.get::<String>(1)?, r.get::<i64>(2)?)),
            )
            .await
            .map_err(|e| e.to_string())?;
        let (text, mr, created_at) = found.into_iter().next().ok_or("missing attribution")?;
        let actual: Json = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        if actual != expected_snapshot(row)
            || mr != format!("mr-{:04}", row / ROWS_PER_MR)
            || created_at != 2
        {
            mismatches.push(id);
        }
    }
    let observations_after = table_count(&db, "check_result_observations").await?;
    let mrs_after = table_count(&db, "merge_requests").await?;
    let lanes_with_ran_at = after["lanes_with_ranAt"].as_i64().unwrap_or(0);
    let candidates = before["uncited_integer_ranAt_lanes"].as_i64().unwrap_or(0);
    let modeled_candidates = ["Unique", "Miss", "Ambiguous"]
        .iter()
        .map(|case| expected_cases.get(*case).copied().unwrap_or(0) as i64)
        .sum::<i64>();
    let relinked =
        after["lanes_cited"].as_i64().unwrap_or(0) - before["lanes_cited"].as_i64().unwrap_or(0);
    let passed = mismatches.is_empty()
        && relinked == unique_lanes
        && candidates == modeled_candidates
        && after["uncited_integer_ranAt_lanes"] == json!(candidates - relinked)
        && after["lanes_with_requestId"] == json!(0)
        && after["lanes_with_jobId"] == json!(0)
        && after["lanes_with_observationId"] == before["lanes_with_observationId"]
        && after["lanes"] == before["lanes"]
        && observations_after == observations_before
        && mrs_after == mrs_before;
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname").map_err(|e| e.to_string())?;
    let report = json!({
        "measurement": "synthetic post-trim fixture, not a clone",
        "migration": FORWARD_MIGRATION,
        "prior_migrations_applied": prior.len(),
        "host": host.trim(),
        "rustc": String::from_utf8_lossy(&std::process::Command::new("rustc").arg("--version")
            .output().map_err(|e| e.to_string())?.stdout).trim(),
        "setup_seconds": setup_seconds,
        "migration_started_utc": migration_started_utc,
        "migration_wall_seconds": seconds,
        "under_30s_cap": seconds < 30.0,
        "cpu": {"method": "/proc/self/stat utime+stime, all threads, around MigrationRunner only",
            "ticks_per_second": ticks_per_second, "user_seconds": cpu_user_seconds,
            "system_seconds": cpu_system_seconds,
            "total_seconds": cpu_user_seconds + cpu_system_seconds},
        "load_before_1m_5m_15m": loads_before,
        "load_after_1m_5m_15m": loads_after,
        "applied": applied,
        "fixture": {"rows": SYNTHETIC_ROWS, "lanes_per_lane_row": SYNTHETIC_LANES,
            "rows_per_merge_request": ROWS_PER_MR, "merge_requests": mrs,
            "observations": observations_before, "key_observations": key_observation_count,
            "lane_cases": expected_cases,
            "note": "Modeled distribution, not a captured live one."},
        "before": before,
        "after": after,
        "acceptance": {
            "relinked_lanes": relinked,
            "candidate_lanes_uncited_integer_ranAt": candidates,
            "relinked_of_candidates": format!("{relinked} / {candidates}"),
            "candidates_left_uncited": after["uncited_integer_ranAt_lanes"],
            "dated_lanes_including_cited": lanes_with_ran_at,
            "relinked_of_dated_lanes": format!("{relinked} / {lanes_with_ran_at}"),
            "expected_unique": unique_lanes,
            "existing_citations_preserved": cited_lanes,
            "rows_compared_whole": SYNTHETIC_ROWS,
            "rows_mismatched": mismatches.len(),
            "first_mismatches": mismatches.iter().take(5).collect::<Vec<_>>(),
            "guessed_requestIds": after["lanes_with_requestId"],
            "observations_unchanged": observations_after == observations_before,
            "merge_requests_unchanged": mrs_after == mrs_before,
            "passed": passed,
        },
    });
    drop(db);
    if !passed {
        return Err(format!("acceptance failed: {}", report["acceptance"]));
    }
    Ok(report)
}
