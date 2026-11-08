//! Rehearsal of the history pack pass (`archival::pack`) on a disposable copy
//! of a database and its archive, never on the live ones.
//!
//! The driver owns the copy: an APFS snapshot of the live three-file set into
//! an internal temporary directory, copied at once to a scratch directory on
//! another volume, the internal snapshot deleted, the live archive copied
//! beside it, and the whole copy deleted by the batch that measures. The copy's
//! root holds a `HISTORY-PACK-CLONE` marker file; every subcommand refuses a
//! database or archive that is not below a marked root, any database path not
//! ending in `.db` (Turso names the logical log from the extension), and any
//! path inside a live Cairn data directory. The archive is opened with
//! `FileContentStore::open_existing`, which clears its `tmp/`, so it must be the
//! copy.
//!
//! Each subcommand prints one JSON document:
//!
//!   fold <db>                         TRUNCATE-checkpoint the copy's logical log
//!   snapshot <db> <archive> <n>       for the first <n> executions the pass
//!                                     would visit, the sha256 of every job's
//!                                     hot `chat/raw` JSON and app tail window
//!   compare <before.json> <after.json>
//!                                     every snapshot hash that differs
//!   pack <db> <archive> <n>           run the real pass until <n> executions
//!                                     are packed, sampling the logical log;
//!                                     report every pass line, write times,
//!                                     gate wait and hold, folds, the log peak,
//!                                     peak RSS, and `events` rows before and
//!                                     after
//!   identity <db> <archive> <n>       for the first <n> catalogued executions,
//!                                     each job's `chat/raw` JSON and app tail
//!                                     window read hot and read from its pack;
//!                                     count the differences
//!   report <db>                       catalogued pack bytes against measured hot
//!                                     bytes and the estimate: totals,
//!                                     distribution, largest
//!   decode <archive> <n>              with no database open, read the first <n>
//!                                     packs named in the archive's objects and
//!                                     verify the ones that open as packs
//!
//! `scripts/measure-history-pack.py` makes the copy, runs every step in order
//! with its memory and host load sampled, and deletes the copy.
//!
//! Usage:
//!   cargo run --release -p cairn-core --features internal-api \
//!     --example history_pack_clone -- pack <copy>/cairn.db <copy>/archive 1000

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_core::archival::pack::{run_pack_pass, PackBudget, PackPolicy};
use cairn_core::internal::mcp::handlers::read::register_archived_file_renderer;
use cairn_core::internal::resources::render_job_chat_raw_json;
use cairn_db::storage::content_store::{ContentStore, FileContentStore};
use cairn_db::storage::events::history::HistorySource;
use cairn_db::storage::history_pack::HistoryPack;
use cairn_db::storage::{DrainBudget, LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
use cairn_session::runs::queries::{list_job_transcript_window, TranscriptWindow};
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};

type Res<T> = Result<T, String>;

const MARKER: &str = "HISTORY-PACK-CLONE";
const LIVE_DIRS: &[&str] = &[
    ".cairn",
    ".cairn-dev",
    "Library/Application Support/com.cairn.desktop",
];
const DAY: i64 = 86_400;
const WINDOW_DAYS: i64 = 30;
const TAIL: usize = 500;

struct Stderr;

impl log::Log for Stderr {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("{} {}", record.level(), record.args());
        }
    }
    fn flush(&self) {}
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let level = match std::env::var("CLONE_LOG").as_deref() {
        Ok("debug") => log::LevelFilter::Debug,
        Ok("warn") => log::LevelFilter::Warn,
        _ => log::LevelFilter::Info,
    };
    let _ = log::set_logger(&Stderr).map(|()| log::set_max_level(level));
    register_archived_file_renderer();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let argv: Vec<&str> = args.iter().map(String::as_str).collect();
    let result = match argv.as_slice() {
        ["fold", db] => fold(db).await,
        ["snapshot", db, archive, n] => snapshot(db, archive, n).await,
        ["compare", before, after] => compare(before, after),
        ["pack", db, archive, n] => pack(db, archive, n).await,
        ["identity", db, archive, n] => identity(db, archive, n).await,
        ["report", db] => report(db).await,
        ["decode", archive, n] => decode(archive, n).await,
        _ => Err(
            "usage: history_pack_clone fold|snapshot|compare|pack|identity|report|decode ... (see the module docs)"
                .to_string(),
        ),
    };
    match result {
        Ok(doc) => println!("{}", serde_json::to_string_pretty(&doc).unwrap()),
        Err(error) => {
            eprintln!("history_pack_clone: {error}");
            std::process::exit(1);
        }
    }
}

/// The marked disposable root above `path`, refusing anything in a live data
/// directory.
fn marked_root(path: &Path) -> Res<PathBuf> {
    let resolved = path
        .canonicalize()
        .map_err(|e| format!("{}: {e}", path.display()))?;
    if let Some(home) = std::env::var_os("HOME") {
        for live in LIVE_DIRS {
            if let Ok(live) = PathBuf::from(&home).join(live).canonicalize() {
                if resolved.starts_with(&live) {
                    return Err(format!(
                        "{} is in the live data directory {}",
                        path.display(),
                        live.display()
                    ));
                }
            }
        }
    }
    resolved
        .ancestors()
        .find(|dir| dir.join(MARKER).is_file())
        .map(Path::to_path_buf)
        .ok_or_else(|| format!("{} is not below a {MARKER} root", path.display()))
}

fn check_db(path: &str) -> Res<PathBuf> {
    if !path.ends_with(".db") {
        return Err(format!(
            "{path} must end in .db: Turso derives the logical log's name from the extension"
        ));
    }
    marked_root(Path::new(path))
}

fn check_pair(db: &str, archive: &str) -> Res<()> {
    let root = check_db(db)?;
    if marked_root(Path::new(archive))? != root {
        return Err("the archive must be the copy beside the database copy".into());
    }
    Ok(())
}

async fn open(path: &str, archive: Option<&str>) -> Res<(LocalDb, f64)> {
    let started = Instant::now();
    let db = LocalDb::open(path)
        .await
        .map_err(|e| format!("open {path}: {e}"))?;
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .map_err(|e| format!("migrating {path}: {e}"))?;
    if let Some(archive) = archive {
        let store = FileContentStore::open_existing(archive);
        store
            .status()
            .map_err(|e| format!("the archive copy: {e}"))?;
        db.set_private_content_store(Arc::new(store))
            .map_err(|_| "the copy already has an archive installed".to_string())?;
    }
    Ok((db, started.elapsed().as_secs_f64()))
}

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

async fn count(db: &LocalDb, sql: &str) -> Res<i64> {
    db.query_one(sql, (), |row| row.i64(0))
        .await
        .map_err(|e| format!("{e}: {sql}"))
}

async fn fold(path: &str) -> Res<Json> {
    check_db(path)?;
    let (db, open_s) = open(path, None).await?;
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

fn cutoff() -> i64 {
    chrono::Utc::now().timestamp() - WINDOW_DAYS * DAY
}

/// The executions the pass visits first: resolved past the window, in its
/// order, whether or not they are packed yet.
async fn pass_order(db: &LocalDb, limit: i64) -> Res<Vec<String>> {
    db.query_all(
        "SELECT DISTINCT i.completed_at, j.execution_id
         FROM issues i JOIN jobs j ON j.issue_id = i.id
         WHERE i.status IN ('merged', 'closed') AND i.completed_at IS NOT NULL
           AND i.completed_at < ?1 AND j.execution_id IS NOT NULL
         ORDER BY i.completed_at, j.execution_id LIMIT ?2",
        (cutoff(), limit),
        |row| row.text(1),
    )
    .await
    .map_err(|e| format!("listing executions: {e}"))
}

async fn jobs_of(db: &LocalDb, execution: &str) -> Res<Vec<String>> {
    db.query_all(
        "SELECT id FROM jobs WHERE execution_id = ?1 ORDER BY created_at, id",
        (execution.to_string(),),
        |row| row.text(0),
    )
    .await
    .map_err(|e| format!("jobs of {execution}: {e}"))
}

/// `value` with every object's keys in sorted order, so equal content hashes
/// equally: the app window carries `HashMap`s, which serialize in a randomized
/// order.
fn canonical(value: Json) -> Json {
    match value {
        Json::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Json::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect(),
            )
        }
        Json::Array(items) => Json::Array(items.into_iter().map(canonical).collect()),
        other => other,
    }
}

/// One job read as the app and the resources read it: its `chat/raw` JSON, and
/// its app tail window as canonical JSON (or the error it read as).
struct JobViews {
    raw: String,
    tail: Result<Json, String>,
}

async fn job_views(db: &LocalDb, job: &str) -> JobViews {
    let raw = render_job_chat_raw_json(db, job).await;
    let tail = list_job_transcript_window(db, job, TranscriptWindow::Tail { limit: TAIL })
        .await
        .map_err(|error| error.to_string())
        .and_then(|window| serde_json::to_value(&window).map_err(|error| error.to_string()))
        .map(canonical);
    JobViews { raw, tail }
}

fn tail_hash(tail: &Result<Json, String>) -> String {
    match tail {
        Ok(value) => sha(&serde_json::to_vec(value).unwrap_or_default()),
        Err(error) => format!("error: {error}"),
    }
}

/// The sha256 of one job's `chat/raw` JSON and of its canonical app tail
/// window.
async fn job_hashes(db: &LocalDb, job: &str) -> Json {
    let views = job_views(db, job).await;
    json!({ "raw": sha(views.raw.as_bytes()), "tail": tail_hash(&views.tail) })
}

/// Where two app windows differ, by structure only: counts, the first
/// differing event's id and field names, and which cursors differ. Never
/// content.
fn window_difference(left: &Result<Json, String>, right: &Result<Json, String>) -> Json {
    let (Ok(left), Ok(right)) = (left, right) else {
        return json!({
            "left_error": left.as_ref().err(),
            "right_error": right.as_ref().err(),
        });
    };
    let empty = Vec::new();
    let events = |window: &Json| window["events"].as_array().unwrap_or(&empty).clone();
    let (l, r) = (events(left), events(right));
    let first = l.iter().zip(&r).position(|(a, b)| a != b);
    let fields: Vec<String> = first
        .map(|at| {
            let keys: std::collections::BTreeSet<&String> = l[at]
                .as_object()
                .into_iter()
                .chain(r[at].as_object())
                .flat_map(|object| object.keys())
                .collect();
            keys.into_iter()
                .filter(|key| l[at][key.as_str()] != r[at][key.as_str()])
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let cursors: Vec<&str> = ["last_rowid", "older", "run_ordinals", "streaming"]
        .into_iter()
        .filter(|key| left[*key] != right[*key])
        .collect();
    json!({
        "events": [l.len(), r.len()],
        "first_differing_index": first,
        "first_differing_event": first.map(|at| l[at]["id"].clone()),
        "fields": fields,
        "cursors_differ": cursors,
    })
}

async fn snapshot(path: &str, archive: &str, n: &str) -> Res<Json> {
    check_pair(path, archive)?;
    let n: i64 = n.parse().map_err(|_| format!("bad count {n}"))?;
    let (db, open_s) = open(path, Some(archive)).await?;
    let started = Instant::now();
    let mut hashes = BTreeMap::new();
    let mut jobs_total = 0usize;
    for execution in pass_order(&db, n).await? {
        let mut jobs = BTreeMap::new();
        for job in jobs_of(&db, &execution).await? {
            jobs.insert(job.clone(), job_hashes(&db, &job).await);
        }
        jobs_total += jobs.len();
        hashes.insert(execution, jobs);
    }
    Ok(json!({
        "open_s": open_s,
        "seconds": started.elapsed().as_secs_f64(),
        "executions": hashes.len(),
        "jobs": jobs_total,
        "hashes": hashes,
    }))
}

fn compare(before: &str, after: &str) -> Res<Json> {
    let read = |path: &str| -> Res<Json> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))
    };
    let (before, after) = (read(before)?, read(after)?);
    let empty = serde_json::Map::new();
    let left = before["hashes"].as_object().unwrap_or(&empty);
    let right = after["hashes"].as_object().unwrap_or(&empty);
    let mut differing = Vec::new();
    let mut compared = 0usize;
    for (execution, jobs) in left {
        for (job, hashes) in jobs.as_object().unwrap_or(&empty) {
            compared += 1;
            for view in ["raw", "tail"] {
                if right[execution][job][view] != hashes[view] {
                    differing.push(json!({ "execution": execution, "job": job, "view": view }));
                }
            }
        }
    }
    Ok(json!({
        "executions": left.len(),
        "jobs_compared": compared,
        "differing": differing.len(),
        "differences": differing,
    }))
}

fn quantile(values: &[u64], q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

/// This process's peak resident set, in bytes.
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes one rusage for this process.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    // SAFETY: initialized by the successful call above.
    let max = unsafe { usage.assume_init() }.ru_maxrss as u64;
    // macOS reports bytes, Linux kibibytes.
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

async fn pack(path: &str, archive: &str, n: &str) -> Res<Json> {
    check_pair(path, archive)?;
    let limit: usize = n.parse().map_err(|_| format!("bad count {n}"))?;
    let (db, open_s) = open(path, Some(archive)).await?;
    let db = Arc::new(db);
    let events_before = count(&db, "SELECT COUNT(*) FROM events").await?;
    let log_at_start = db.logical_log_bytes();

    // The pass observes the log between pages; this samples it between those
    // too, so the peak is the log's, not the pass's view of it.
    let sampled_peak = Arc::new(AtomicU64::new(log_at_start));
    let sampling = Arc::new(AtomicBool::new(true));
    let sampler = {
        let (db, peak, sampling) = (db.clone(), sampled_peak.clone(), sampling.clone());
        tokio::spawn(async move {
            while sampling.load(Ordering::Relaxed) {
                peak.fetch_max(db.logical_log_bytes(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };

    let started = Instant::now();
    let policy = PackPolicy {
        window_secs: WINDOW_DAYS * DAY,
        bytes_per_row: None,
    };
    let mut passes = Vec::new();
    let (mut write_ms, mut packed, mut pack_bytes, mut hot_bytes, mut estimate) =
        (Vec::new(), 0usize, 0u64, 0u64, 0u64);
    let (mut gate_wait_max, mut hold_max, mut hold_max_execution) =
        (Duration::ZERO, Duration::ZERO, None::<String>);
    let (mut folds, mut fold_max, mut pages) = (0usize, Duration::ZERO, 0u64);
    let mut largest: Option<(String, u64)> = None;
    let mut skipped = Vec::new();
    let mut failed = Vec::new();
    let mut bytes_per_row = 0;
    while packed < limit {
        let budget = PackBudget {
            pass_time: Duration::from_secs(24 * 60 * 60),
            max_executions: limit - packed,
            ..PackBudget::PRODUCTION
        };
        let report = run_pack_pass(&db, policy, budget).await;
        eprintln!("pass {}: {report}", passes.len() + 1);
        passes.push(report.to_string());
        write_ms.extend(&report.write_ms);
        packed += report.packed;
        pack_bytes += report.pack_bytes;
        hot_bytes += report.hot_bytes;
        estimate += report.hot_estimate_bytes;
        pages += report.pages;
        folds += report.fold.folds;
        fold_max = fold_max.max(report.fold.max);
        gate_wait_max = gate_wait_max.max(report.gate_wait_max);
        bytes_per_row = report.bytes_per_row;
        sampled_peak.fetch_max(report.fold.log_peak_bytes, Ordering::Relaxed);
        if report.hold_max > hold_max {
            hold_max = report.hold_max;
            hold_max_execution = report.hold_max_scope.clone();
        }
        if let Some((execution, bytes)) = &report.largest {
            if largest.as_ref().is_none_or(|(_, max)| bytes > max) {
                largest = Some((execution.clone(), *bytes));
            }
        }
        skipped.extend(report.skipped.iter().map(|p| json!([p.scope_id, p.reason])));
        failed.extend(report.failed.iter().map(|p| json!([p.scope_id, p.reason])));
        if report.halted || report.packed == 0 {
            break;
        }
    }
    sampling.store(false, Ordering::Relaxed);
    let _ = sampler.await;
    let events_after = count(&db, "SELECT COUNT(*) FROM events").await?;
    let mib = |bytes: u64| bytes as f64 / 1048576.0;
    Ok(json!({
        "open_s": open_s,
        "seconds": started.elapsed().as_secs_f64(),
        "passes": passes,
        "packed": packed,
        "pack_mib": mib(pack_bytes),
        "hot_mib": mib(hot_bytes),
        "estimate_mib": mib(estimate),
        "bytes_per_row": bytes_per_row,
        "largest": largest.map(|(execution, bytes)| json!({"execution": execution, "mib": mib(bytes)})),
        "pages": pages,
        "catalog_write_ms": {
            "count": write_ms.len(),
            "p50": quantile(&write_ms, 0.5),
            "p99": quantile(&write_ms, 0.99),
            "max": write_ms.iter().max().copied().unwrap_or(0),
        },
        "writes_over_1s": write_ms.iter().filter(|ms| **ms > 1000).count(),
        "gate_wait_ms_max": gate_wait_max.as_millis() as u64,
        "gate_hold_ms_max": hold_max.as_millis() as u64,
        "gate_hold_max_execution": hold_max_execution,
        "folds": folds,
        "fold_ms_max": fold_max.as_millis() as u64,
        "log_mib_at_start": mib(log_at_start),
        "log_mib_peak": mib(sampled_peak.load(Ordering::Relaxed)),
        "log_mib_at_end": mib(db.logical_log_bytes()),
        "peak_rss_mib": mib(peak_rss_bytes()),
        "events_before": events_before,
        "events_after": events_after,
        "skipped": skipped,
        "failed": failed,
    }))
}

async fn identity(path: &str, archive: &str, n: &str) -> Res<Json> {
    check_pair(path, archive)?;
    let n: i64 = n.parse().map_err(|_| format!("bad count {n}"))?;
    let (db, open_s) = open(path, Some(archive)).await?;
    let started = Instant::now();
    let executions = db
        .query_all(
            "SELECT scope_id FROM history_tier WHERE kind = 'execution'
             ORDER BY resolved_at, scope_id LIMIT ?1",
            (n,),
            |row| row.text(0),
        )
        .await
        .map_err(|e| format!("listing catalogued executions: {e}"))?;
    let mut differing = Vec::new();
    let mut unstable = Vec::new();
    let (mut jobs_compared, mut tail_events) = (0usize, 0usize);
    let (mut outside_events, mut outside_jobs) = (0usize, Vec::new());
    for execution in &executions {
        for job in jobs_of(&db, execution).await? {
            // Two hot reads first: a view that differs between them is the
            // reader's own nondeterminism, not the pack's.
            db.set_history_source_override(Some(HistorySource::Hot));
            let hot = job_views(&db, &job).await;
            let again = job_views(&db, &job).await;
            db.set_history_source_override(Some(HistorySource::Pack));
            let packed = job_views(&db, &job).await;
            db.set_history_source_override(None);
            jobs_compared += 1;
            if hot.raw != again.raw {
                unstable.push(json!({"execution": execution, "job": job, "view": "raw"}));
            } else if hot.raw != packed.raw {
                differing.push(json!({
                    "execution": execution, "job": job, "view": "raw",
                    "bytes": [hot.raw.len(), packed.raw.len()],
                }));
            }
            if hot.tail != again.tail {
                unstable.push(json!({
                    "execution": execution, "job": job, "view": "tail",
                    "where": window_difference(&hot.tail, &again.tail),
                }));
            } else if hot.tail != packed.tail {
                differing.push(json!({
                    "execution": execution, "job": job, "view": "tail",
                    "where": window_difference(&hot.tail, &packed.tail),
                }));
            }
            // The containment the pack's source rule rests on: every event of
            // a job's window belongs to a run of the job's own execution.
            if let Ok(window) = &hot.tail {
                let runs: std::collections::BTreeSet<String> = window["events"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|event| event["runId"].as_str().or(event["run_id"].as_str()))
                    .map(str::to_string)
                    .collect();
                let mut foreign = std::collections::BTreeSet::new();
                for run in &runs {
                    let owner = db
                        .query_opt(
                            "SELECT j.execution_id FROM runs r JOIN jobs j ON j.id = r.job_id
                             WHERE r.id = ?1",
                            (run.clone(),),
                            |row| row.opt_text(0),
                        )
                        .await
                        .map_err(|e| format!("owner of run {run}: {e}"))?
                        .flatten();
                    if owner.as_deref() != Some(execution.as_str()) {
                        foreign.insert(run.clone());
                    }
                }
                let events = window["events"].as_array().map_or(0, Vec::len);
                tail_events += events;
                if !foreign.is_empty() {
                    let count = window["events"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|event| {
                            event["runId"]
                                .as_str()
                                .or(event["run_id"].as_str())
                                .is_some_and(|run| foreign.contains(run))
                        })
                        .count();
                    outside_events += count;
                    outside_jobs.push(json!({"execution": execution, "job": job, "events": count, "runs": foreign.len()}));
                }
            }
        }
    }
    Ok(json!({
        "open_s": open_s,
        "seconds": started.elapsed().as_secs_f64(),
        "executions": executions.len(),
        "jobs_compared": jobs_compared,
        "tail_events_compared": tail_events,
        "differing": differing.len(),
        "differences": differing.into_iter().take(50).collect::<Vec<_>>(),
        "hot_unstable": unstable.len(),
        "hot_unstable_examples": unstable.into_iter().take(20).collect::<Vec<_>>(),
        "containment": {
            "events_outside_their_jobs_execution": outside_events,
            "jobs": outside_jobs.len(),
            "examples": outside_jobs.into_iter().take(20).collect::<Vec<_>>(),
        },
    }))
}

async fn report(path: &str) -> Res<Json> {
    check_db(path)?;
    let (db, _) = open(path, None).await?;
    let rows: Vec<(String, i64, i64, String)> = db
        .query_all(
            "SELECT scope_id, bytes, hot_bytes_estimate, row_counts FROM history_tier",
            (),
            |row| Ok((row.text(0)?, row.i64(1)?, row.i64(2)?, row.text(3)?)),
        )
        .await
        .map_err(|e| e.to_string())?;
    let bytes: Vec<u64> = rows.iter().map(|row| row.1 as u64).collect();
    let ratios: Vec<u64> = rows
        .iter()
        .filter(|row| row.2 > 0)
        .map(|row| (row.1 as f64 / row.2 as f64 * 1000.0) as u64)
        .collect();
    let largest = rows.iter().max_by_key(|row| row.1);
    let mib = |bytes: u64| bytes as f64 / 1048576.0;
    Ok(json!({
        "executions": rows.len(),
        "pack_mib": mib(bytes.iter().sum()),
        "estimate_mib": mib(rows.iter().map(|row| row.2 as u64).sum()),
        "pack_kib": {
            "p50": quantile(&bytes, 0.5) / 1024,
            "p90": quantile(&bytes, 0.9) / 1024,
            "p99": quantile(&bytes, 0.99) / 1024,
            "max": bytes.iter().max().copied().unwrap_or(0) / 1024,
        },
        "pack_over_estimate_permille": {
            "p10": quantile(&ratios, 0.1),
            "p50": quantile(&ratios, 0.5),
            "p90": quantile(&ratios, 0.9),
        },
        "largest": largest.map(|row| json!({"execution": row.0, "mib": mib(row.1 as u64), "row_counts": row.3})),
    }))
}

async fn decode(archive: &str, n: &str) -> Res<Json> {
    marked_root(Path::new(archive))?;
    let n: usize = n.parse().map_err(|_| format!("bad count {n}"))?;
    // No database is opened: the archive alone must decode.
    let store = FileContentStore::open_existing(archive);
    store.status().map_err(|e| e.to_string())?;
    let objects = Path::new(archive).join("objects");
    let mut hashes = Vec::new();
    for shard in std::fs::read_dir(&objects).map_err(|e| e.to_string())? {
        let shard = shard.map_err(|e| e.to_string())?;
        for object in std::fs::read_dir(shard.path()).map_err(|e| e.to_string())? {
            hashes.push(object.map_err(|e| e.to_string())?.file_name());
        }
    }
    hashes.sort();
    let (mut packs, mut verified, mut rows, mut failures) = (0usize, 0usize, 0u64, Vec::new());
    for hash in hashes {
        if packs >= n {
            break;
        }
        let hash = hash.to_string_lossy().to_string();
        let Some(bytes) = store.get(&hash).await.map_err(|e| e.to_string())? else {
            continue;
        };
        if !bytes.starts_with(cairn_db::storage::history_pack::MAGIC) {
            continue;
        }
        packs += 1;
        match HistoryPack::open(bytes).and_then(|pack| {
            pack.verify()?;
            Ok(pack
                .manifest()
                .runs
                .iter()
                .map(|run| run.event_rows)
                .sum::<u64>())
        }) {
            Ok(events) => {
                verified += 1;
                rows += events;
            }
            Err(error) => failures.push(json!({"hash": hash, "error": error.to_string()})),
        }
    }
    Ok(json!({ "packs": packs, "verified": verified, "event_rows": rows, "failures": failures }))
}
