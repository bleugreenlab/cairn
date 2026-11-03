//! Clone-only rehearsal of the archive offload (`archival::offload`).
//!
//! Point it at an APFS clone of the three-file set, never at the live file, and
//! at an archive directory in scratch, never at `~/.cairn/archive`:
//!
//!   cp -c ~/.cairn/cairn.db ~/.cairn/cairn.db-log ~/.cairn/cairn.db-wal <dir>/
//!
//! Every subcommand refuses a database inside a live Cairn data directory, any
//! path not ending in `.db` (Turso names the logical log from the extension;
//! cairn/5590), and an archive inside a live data directory. Each opens the
//! clone, applies any pending migrations, and prints one JSON document:
//!
//!   fold <db>                      TRUNCATE-checkpoint the clone's logical log
//!   counts <db>                    inline pack rows and `archival_blobs` rows
//!   snapshot <db> <archive> <n>    for the first <n> executions (by id) that
//!                                  hold an inline pack or a blobbed event, the
//!                                  sha256 of every job's `chat/raw` JSON and of
//!                                  the execution's archived base-tip diff
//!   compare <before.json> <after.json>
//!                                  every snapshot hash that differs
//!   offload <db> <archive>         run offload passes to completion through the
//!                                  real pass, and report pages, write times,
//!                                  folds, the log peak, and what moved
//!   probe <db> <archive> <execution>
//!                                  read one execution's diff and chat the way
//!                                  the app does, and count labeled stubs (run
//!                                  with the archive renamed to see the offline
//!                                  shape; the store's log lines go to stderr)
//!
//! Usage:
//!   cargo run --release -p cairn-core --features internal-api \
//!     --example archive_offload_clone -- fold <clone>/cairn.db

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cairn_core::archival::offload::{run_offload_pass, OffloadBudget};
use cairn_core::internal::mcp::handlers::read::register_archived_file_renderer;
use cairn_core::internal::resources::render_job_chat_raw_json;
use cairn_core::internal::storage::{
    DrainBudget, FileContentStore, LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS,
};
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};

type Res<T> = Result<T, String>;

/// Data directories a live Cairn keeps its database in, relative to `$HOME`.
const LIVE_DIRS: &[&str] = &[
    ".cairn",
    ".cairn-dev",
    "Library/Application Support/com.cairn.desktop",
];

use cairn_core::internal::storage::events::reconstruct::STUB_PREFIX;

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
    let checked = match argv.as_slice() {
        ["compare", ..] => Ok(()),
        [_, db, rest @ ..] => check_db(db).and_then(|()| match rest.first() {
            Some(archive) if !matches!(argv[0], "counts" | "fold") => check_archive(archive),
            _ => Ok(()),
        }),
        _ => Ok(()),
    };
    if let Err(error) = checked {
        eprintln!("archive_offload_clone: {error}");
        std::process::exit(2);
    }
    let result = match argv.as_slice() {
        ["fold", db] => fold(db).await,
        ["counts", db] => counts(db).await,
        ["snapshot", db, archive, limit] => snapshot(db, archive, limit).await,
        ["compare", before, after] => compare(before, after),
        ["offload", db, archive] => offload(db, archive).await,
        ["probe", db, archive, execution] => probe(db, archive, execution).await,
        _ => Err(
            "usage: archive_offload_clone fold|counts|snapshot|compare|offload|probe ... (see the module docs)"
                .to_string(),
        ),
    };
    match result {
        Ok(doc) => println!("{}", serde_json::to_string_pretty(&doc).unwrap()),
        Err(error) => {
            eprintln!("archive_offload_clone: {error}");
            std::process::exit(1);
        }
    }
}

fn live_dirs() -> Vec<PathBuf> {
    let Some(home) = std::env::var_os("HOME") else {
        return Vec::new();
    };
    LIVE_DIRS
        .iter()
        .filter_map(|live| std::fs::canonicalize(PathBuf::from(&home).join(live)).ok())
        .collect()
}

fn canonical_parent(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    std::fs::canonicalize(parent).ok()
}

/// Refuse a live database: the offload deletes rows, and this tool exists to
/// rehearse it where a mistake costs a clone.
fn check_db(path: &str) -> Res<()> {
    if !path.ends_with(".db") {
        return Err(format!(
            "{path} must end in .db: Turso derives the logical log's name from the extension"
        ));
    }
    let dir = canonical_parent(Path::new(path));
    if live_dirs().iter().any(|live| Some(live) == dir.as_ref()) {
        return Err(format!(
            "{path} is in a live Cairn data directory; clone it first (see the module docs)"
        ));
    }
    Ok(())
}

/// Refuse a live archive (`archive` in a live data directory, the default
/// root): a rehearsal's objects belong in scratch.
fn check_archive(path: &str) -> Res<()> {
    let candidate = Path::new(path);
    let resolved = std::fs::canonicalize(candidate).ok().or_else(|| {
        canonical_parent(candidate)
            .map(|parent| parent.join(candidate.file_name().unwrap_or_default()))
    });
    if let Some(resolved) = resolved {
        if live_dirs()
            .iter()
            .any(|live| resolved.starts_with(live.join("archive")))
        {
            return Err(format!(
                "{path} is a live Cairn archive; point the archive at scratch"
            ));
        }
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
        // The boot install, so the clone sees the app's archive semantics:
        // created only where the database has never had one.
        let settings_home = archive_settings_home(archive)?;
        cairn_core::internal::db::install_private_archive(&db, &settings_home).await;
    }
    Ok((db, started.elapsed().as_secs_f64()))
}

/// A throwaway settings home whose `archive.path` names `archive`, so the boot
/// install opens exactly that root.
fn archive_settings_home(archive: &str) -> Res<PathBuf> {
    let home = std::env::temp_dir().join(format!("archive-offload-clone-{}", std::process::id()));
    std::fs::create_dir_all(&home).map_err(|e| format!("{}: {e}", home.display()))?;
    let archive = std::path::absolute(archive).map_err(|e| format!("{archive}: {e}"))?;
    std::fs::write(
        home.join("settings.yaml"),
        format!("archive:\n  path: {:?}\n", archive.display().to_string()),
    )
    .map_err(|e| format!("writing the clone's settings: {e}"))?;
    Ok(home)
}

async fn fold(path: &str) -> Res<Json> {
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

async fn count(db: &LocalDb, sql: &str) -> Res<i64> {
    db.query_one(sql, (), |row| row.i64(0))
        .await
        .map_err(|e| format!("{e}: {sql}"))
}

async fn counts(path: &str) -> Res<Json> {
    let (db, _) = open(path, None).await?;
    Ok(json!({
        "inline_pack_rows": count(&db, "SELECT COUNT(*) FROM execution_history WHERE pack IS NOT NULL").await?,
        "inline_pack_bytes": count(&db, "SELECT COALESCE(SUM(length(pack) + length(pack_idx)), 0) FROM execution_history WHERE pack IS NOT NULL").await?,
        "stored_pack_rows": count(&db, "SELECT COUNT(*) FROM execution_history WHERE pack IS NULL AND pack_hash IS NOT NULL").await?,
        "archival_blobs_rows": count(&db, "SELECT COUNT(*) FROM archival_blobs").await?,
        "archival_blobs_bytes": count(&db, "SELECT COALESCE(SUM(length(content)), 0) FROM archival_blobs").await?,
        "log_bytes": db.logical_log_bytes(),
    }))
}

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// The executions the offload touches: an inline pack, or a blobbed event.
async fn candidates(db: &LocalDb, limit: i64) -> Res<Vec<String>> {
    db.query_all(
        "SELECT execution_id FROM (
             SELECT execution_id FROM execution_history WHERE pack IS NOT NULL
             UNION
             SELECT DISTINCT j.execution_id FROM events e
               JOIN runs r ON r.id = e.run_id JOIN jobs j ON j.id = r.job_id
              WHERE e.storage_mode = 'gitcoord' AND e.content_commit IS NULL
                AND j.execution_id IS NOT NULL)
         ORDER BY execution_id LIMIT ?1",
        (limit,),
        |row| row.text(0),
    )
    .await
    .map_err(|e| format!("listing candidates: {e}"))
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

/// The archived base-tip diff, read the way the app reads it when no live
/// branch resolves (no jj binary here, so it never does).
async fn archived_diff(db: &LocalDb, job: &str) -> Res<Option<String>> {
    let diff = cairn_core::diff::node_base_tip_diff(db, job, "", Path::new("/nonexistent"))
        .await
        .map_err(|e| format!("diff of {job}: {e}"))?;
    Ok(diff.map(|diff| format!("{diff:?}")))
}

async fn snapshot(path: &str, archive: &str, limit: &str) -> Res<Json> {
    let limit: i64 = limit.parse().map_err(|_| format!("bad limit {limit}"))?;
    let (db, open_s) = open(path, Some(archive)).await?;
    let started = Instant::now();
    let executions = candidates(&db, limit).await?;
    let mut hashes = BTreeMap::new();
    let (mut jobs_total, mut stubs, mut diffs) = (0usize, 0usize, 0usize);
    for execution in &executions {
        let jobs = jobs_of(&db, execution).await?;
        let mut chats = BTreeMap::new();
        for job in &jobs {
            let raw = render_job_chat_raw_json(&db, job).await;
            stubs += raw.matches(STUB_PREFIX).count();
            chats.insert(job.clone(), sha(raw.as_bytes()));
        }
        jobs_total += jobs.len();
        let diff = match jobs.first() {
            Some(job) => archived_diff(&db, job).await?,
            None => None,
        };
        diffs += usize::from(diff.is_some());
        hashes.insert(
            execution.clone(),
            json!({ "chat": chats, "diff": diff.map(|d| sha(d.as_bytes())) }),
        );
    }
    Ok(json!({
        "open_s": open_s,
        "seconds": started.elapsed().as_secs_f64(),
        "executions": executions.len(),
        "jobs": jobs_total,
        "diffs": diffs,
        "stubs": stubs,
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
    for (execution, hashes) in left {
        let Some(other) = right.get(execution) else {
            differing.push(json!({ "execution": execution, "missing_after": true }));
            continue;
        };
        if hashes["diff"] != other["diff"] {
            differing.push(json!({ "execution": execution, "what": "diff" }));
        }
        for (job, chat) in hashes["chat"].as_object().unwrap_or(&empty) {
            compared += 1;
            if other["chat"][job] != *chat {
                differing.push(json!({ "execution": execution, "job": job, "what": "chat" }));
            }
        }
    }
    Ok(json!({
        "executions": left.len(),
        "chats_compared": compared,
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

/// Passes until one moves nothing (drained, or stopped on rows it skips).
async fn offload(path: &str, archive: &str) -> Res<Json> {
    let (db, open_s) = open(path, Some(archive)).await?;
    let started = Instant::now();
    let log_at_start = db.logical_log_bytes();
    let mut passes = Vec::new();
    let (mut write_ms, mut log_peak, mut folds, mut fold_max) =
        (Vec::new(), log_at_start, 0usize, Duration::ZERO);
    let (mut packs, mut pack_bytes, mut blobs, mut blob_bytes, mut pages) = (0, 0, 0, 0, 0);
    loop {
        let report = run_offload_pass(&db, OffloadBudget::PRODUCTION).await;
        eprintln!("pass {}: {report}", passes.len() + 1);
        write_ms.extend(&report.write_ms);
        log_peak = log_peak.max(report.log_peak_bytes);
        folds += report.folds;
        fold_max = fold_max.max(report.fold_max);
        packs += report.packs_moved;
        pack_bytes += report.pack_bytes;
        blobs += report.blobs_moved;
        blob_bytes += report.blob_bytes;
        pages += report.pages;
        let moved = report.packs_moved + report.blobs_moved;
        passes.push(report.to_string());
        if let Some(error) = report.error {
            return Err(format!("pass {} stopped: {error}", passes.len()));
        }
        if moved == 0 || report.remaining_packs == Some(0) && report.remaining_blobs == Some(0) {
            break;
        }
    }
    Ok(json!({
        "open_s": open_s,
        "seconds": started.elapsed().as_secs_f64(),
        "passes": passes,
        "packs_moved": packs,
        "pack_mib": pack_bytes as f64 / 1048576.0,
        "blobs_moved": blobs,
        "blob_mib": blob_bytes as f64 / 1048576.0,
        "pages": pages,
        "write_ms": {
            "p50": quantile(&write_ms, 0.5),
            "p99": quantile(&write_ms, 0.99),
            "max": write_ms.iter().max().copied().unwrap_or(0),
        },
        "writes_over_1s": write_ms.iter().filter(|ms| **ms > 1000).count(),
        "folds": folds,
        "fold_ms_max": fold_max.as_millis() as u64,
        "log_mib_at_start": log_at_start as f64 / 1048576.0,
        "log_mib_peak": log_peak as f64 / 1048576.0,
        "log_mib_at_end": db.logical_log_bytes() as f64 / 1048576.0,
    }))
}

async fn probe(path: &str, archive: &str, execution: &str) -> Res<Json> {
    let (db, _) = open(path, Some(archive)).await?;
    let jobs = jobs_of(&db, execution).await?;
    let mut chats = Vec::new();
    for job in &jobs {
        let raw = render_job_chat_raw_json(&db, job).await;
        chats.push(json!({
            "job": job,
            "bytes": raw.len(),
            "stubs": raw.matches(STUB_PREFIX).count(),
        }));
    }
    let diff = match jobs.first() {
        Some(job) => cairn_core::diff::node_base_tip_diff(&db, job, "", Path::new("/nonexistent"))
            .await
            .map(|diff| diff.map(|diff| diff.files.len()))
            .map_err(|e| e.to_string()),
        None => Ok(None),
    };
    Ok(json!({
        "archive_online": FileContentStore::open_existing(archive).status().is_ok(),
        "jobs": chats,
        "diff_files": diff,
    }))
}
