//! Phase 0 concurrency gate (CAIRN-1133/CAIRN-1963): prove that multiple
//! *OS processes* can concurrently read and write the same local `.turso.db`
//! file without corruption, relying only on the existing busy_timeout +
//! optimistic-retry machinery in `LocalDb`.
//!
//! This is the real cross-process test the within-process unit test in
//! `local_db.rs` cannot give us: Turso/SQLite coordination across separate
//! `cairn_db::turso::Database` handles inside one process could plausibly share in-memory
//! state, masking a cross-process failure. Re-spawning ourselves as worker
//! processes exercises genuine OS-level file locking + WAL/MVCC coordination.
//!
//! Usage:
//!   cargo run --example concurrent_db_probe --features internal-api
//!   cargo run --example concurrent_db_probe --features internal-api -- [WORKERS] [ITERS]
//!   cargo run --example concurrent_db_probe --features internal-api -- \
//!     --workers 8 --iters 500 --checkpoint-every 25 --delete-heavy
//!
//! Exit code 0 = gate passed; non-zero = corruption / lost update / error.

use std::path::{Path, PathBuf};
use std::process::Child;

use cairn_core::internal::storage::{db_set_paths, db_set_size, LocalDb, RowExt};

const DEFAULT_WORKERS: usize = 6;
const DEFAULT_ITERS: usize = 200;

#[derive(Debug, Clone)]
struct Config {
    workers: usize,
    iters: usize,
    checkpoint_every: Option<usize>,
    integrity_every: Option<usize>,
    delete_heavy: bool,
    db_path: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            workers: DEFAULT_WORKERS,
            iters: DEFAULT_ITERS,
            checkpoint_every: None,
            integrity_every: None,
            delete_heavy: false,
            db_path: None,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Worker mode: `concurrent_db_probe worker <db_path> <iters> <worker_id> [--delete-heavy]`
    if args.get(1).map(String::as_str) == Some("worker") {
        let db_path = args.get(2).expect("worker: missing db path").clone();
        let iters: usize = args.get(3).expect("worker: missing iters").parse().unwrap();
        let worker_id: usize = args.get(4).expect("worker: missing id").parse().unwrap();
        let delete_heavy = args.iter().any(|arg| arg == "--delete-heavy");
        match run_worker(&db_path, iters, worker_id, delete_heavy).await {
            Ok(()) => std::process::exit(0),
            Err(e) => {
                eprintln!("[worker {worker_id}] FAILED: {e}");
                eprintln!(
                    "[worker {worker_id}] {}",
                    db_diagnostics(Path::new(&db_path))
                );
                std::process::exit(1);
            }
        }
    }

    let config = match parse_config(&args[1..]) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{e}\n\n{}", usage());
            std::process::exit(2);
        }
    };

    match run_parent(&config).await {
        Ok(()) => {
            println!(
                "GATE PASSED: {} processes x {} iters, checkpoint_every={:?}, integrity_every={:?}, delete_heavy={}",
                config.workers,
                config.iters,
                config.checkpoint_every,
                config.integrity_every,
                config.delete_heavy
            );
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("GATE FAILED: {e}");
            std::process::exit(1);
        }
    }
}

fn parse_config(args: &[String]) -> Result<Config, String> {
    let mut config = Config::default();
    let mut positional = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--workers" => {
                i += 1;
                config.workers = parse_value(args, i, "--workers")?;
            }
            "--iters" => {
                i += 1;
                config.iters = parse_value(args, i, "--iters")?;
            }
            "--checkpoint-every" => {
                i += 1;
                config.checkpoint_every = Some(parse_nonzero(args, i, "--checkpoint-every")?);
            }
            "--integrity-every" => {
                i += 1;
                config.integrity_every = Some(parse_nonzero(args, i, "--integrity-every")?);
            }
            "--delete-heavy" => config.delete_heavy = true,
            "--db" | "--keep-db" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| format!("{} requires a path", args[i - 1]))?;
                config.db_path = Some(PathBuf::from(value));
            }
            "--help" | "-h" => return Err(String::new()),
            arg if arg.starts_with('-') => return Err(format!("unknown option: {arg}")),
            arg => positional.push(arg.to_string()),
        }
        i += 1;
    }

    if let Some(workers) = positional.first() {
        config.workers = workers
            .parse()
            .map_err(|_| format!("invalid positional workers: {workers}"))?;
    }
    if let Some(iters) = positional.get(1) {
        config.iters = iters
            .parse()
            .map_err(|_| format!("invalid positional iters: {iters}"))?;
    }
    if positional.len() > 2 {
        return Err(format!("too many positional arguments: {positional:?}"));
    }
    Ok(config)
}

fn parse_value(args: &[String], index: usize, flag: &str) -> Result<usize, String> {
    args.get(index)
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse()
        .map_err(|_| format!("invalid value for {flag}"))
}

fn parse_nonzero(args: &[String], index: usize, flag: &str) -> Result<usize, String> {
    let value = parse_value(args, index, flag)?;
    if value == 0 {
        return Err(format!("{flag} must be greater than zero"));
    }
    Ok(value)
}

fn usage() -> &'static str {
    "usage: concurrent_db_probe [WORKERS] [ITERS] [--workers N] [--iters N] [--checkpoint-every N] [--integrity-every N] [--delete-heavy] [--db PATH|--keep-db PATH]"
}

async fn run_parent(config: &Config) -> Result<(), String> {
    let temp_dir;
    let db_path = if let Some(path) = &config.db_path {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create db parent: {e}"))?;
        }
        path.clone()
    } else {
        temp_dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
        temp_dir.path().join("cairn-concurrency-gate.turso.db")
    };
    let db_path_str = db_path.to_string_lossy().to_string();

    let run_result = run_parent_inner(config, &db_path, &db_path_str).await;
    if let Err(e) = run_result {
        return Err(format!(
            "{e}\nconfig: workers={}, iters={}, checkpoint_every={:?}, integrity_every={:?}, delete_heavy={}\n{}",
            config.workers,
            config.iters,
            config.checkpoint_every,
            config.integrity_every,
            config.delete_heavy,
            db_diagnostics(&db_path)
        ));
    }
    Ok(())
}

async fn run_parent_inner(
    config: &Config,
    db_path: &Path,
    db_path_str: &str,
) -> Result<(), String> {
    // Parent owns schema setup and the initial shared row.
    {
        let db = open_probe_db(db_path)
            .await
            .map_err(|e| format!("parent open: {e}"))?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS counters (id TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS worker_log (id TEXT PRIMARY KEY NOT NULL, worker INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS worker_scratch (id TEXT PRIMARY KEY NOT NULL, worker INTEGER NOT NULL, iter INTEGER NOT NULL, touched INTEGER NOT NULL DEFAULT 0);",
        )
        .await
        .map_err(|e| format!("parent schema: {e}"))?;
        db.execute(
            "INSERT OR REPLACE INTO counters(id, value) VALUES ('shared', 0)",
            (),
        )
        .await
        .map_err(|e| format!("parent seed: {e}"))?;
    }

    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;

    // Spawn worker processes.
    let mut children = Vec::new();
    for id in 0..config.workers {
        let mut command = std::process::Command::new(&exe);
        command
            .arg("worker")
            .arg(db_path_str)
            .arg(config.iters.to_string())
            .arg(id.to_string());
        if config.delete_heavy {
            command.arg("--delete-heavy");
        }
        let child = command
            .spawn()
            .map_err(|e| format!("spawn worker {id}: {e}"))?;
        children.push(Some(child));
    }

    // While workers hammer the file, the parent interleaves its own reads, and
    // optionally checkpoints / integrity-checks from separate short-lived LocalDb
    // handles. Turso local opens take a process-level file lock, so the probe
    // intentionally contends at open/write/checkpoint boundaries instead of
    // holding one parent handle for the whole run.
    monitor_workers(&mut children, db_path, config).await?;

    // Wait for all workers, collecting failures.
    let mut failed = Vec::new();
    for (id, child) in children.into_iter().enumerate() {
        if let Some(mut child) = child {
            let status = child.wait().map_err(|e| format!("wait worker {id}: {e}"))?;
            if !status.success() {
                failed.push(id);
            }
        }
    }
    if !failed.is_empty() {
        return Err(format!("workers failed: {failed:?}"));
    }

    let final_db = open_probe_db(db_path)
        .await
        .map_err(|e| format!("final open: {e}"))?;
    if config.checkpoint_every.is_some() {
        final_db
            .consume_query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .map_err(|e| format!("final checkpoint: {e}"))?;
    }
    assert_integrity(&final_db, "final").await?;
    assert_final_counts(&final_db, config).await?;
    drop(final_db);

    // Reopen the same DB file with a fresh LocalDb handle and re-check counts.
    let reopened = open_probe_db(db_path)
        .await
        .map_err(|e| format!("reopen after final checkpoint: {e}"))?;
    assert_integrity(&reopened, "reopened").await?;
    assert_final_counts(&reopened, config).await?;

    Ok(())
}

async fn monitor_workers(
    children: &mut [Option<Child>],
    db_path: &Path,
    config: &Config,
) -> Result<(), String> {
    let mut poll = 0usize;
    while children.iter().any(Option::is_some) {
        for (id, child_slot) in children.iter_mut().enumerate() {
            let Some(child) = child_slot else { continue };
            if let Some(status) = child
                .try_wait()
                .map_err(|e| format!("poll worker {id}: {e}"))?
            {
                if !status.success() {
                    return Err(format!("worker {id} failed with {status}"));
                }
                *child_slot = None;
            }
        }

        {
            let reader = open_probe_db(db_path)
                .await
                .map_err(|e| format!("parent read open poll {poll}: {e}"))?;
            let _ = read_shared(&reader)
                .await
                .map_err(|e| format!("parent read poll {poll}: {e}"))?;
        }

        if config
            .checkpoint_every
            .is_some_and(|every| poll > 0 && poll.is_multiple_of(every))
        {
            let checkpoint_db = open_probe_db(db_path)
                .await
                .map_err(|e| format!("checkpoint open poll {poll}: {e}"))?;
            checkpoint_db
                .consume_query("PRAGMA wal_checkpoint(TRUNCATE)")
                .await
                .map_err(|e| format!("checkpoint poll {poll}: {e}"))?;
        }
        if config
            .integrity_every
            .is_some_and(|every| poll > 0 && poll.is_multiple_of(every))
        {
            let integrity_db = open_probe_db(db_path)
                .await
                .map_err(|e| format!("integrity open poll {poll}: {e}"))?;
            assert_integrity(&integrity_db, &format!("poll {poll}")).await?;
        }

        poll += 1;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn assert_final_counts(db: &LocalDb, config: &Config) -> Result<(), String> {
    let final_value = read_shared(db)
        .await
        .map_err(|e| format!("final read: {e}"))?;
    let expected = (config.workers * config.iters) as i64;
    if final_value != expected {
        return Err(format!(
            "lost updates: counter = {final_value}, expected {expected}"
        ));
    }

    let log_count = query_count(db, "SELECT COUNT(*) FROM worker_log")
        .await
        .map_err(|e| format!("log count: {e}"))?;
    if log_count != expected {
        return Err(format!(
            "worker_log rows = {log_count}, expected {expected}"
        ));
    }

    if config.delete_heavy {
        let deleted_per_worker = config.iters.div_ceil(3) as i64;
        let scratch_expected = expected - (config.workers as i64 * deleted_per_worker);
        let scratch_count = query_count(db, "SELECT COUNT(*) FROM worker_scratch")
            .await
            .map_err(|e| format!("scratch count: {e}"))?;
        if scratch_count != scratch_expected {
            return Err(format!(
                "worker_scratch rows = {scratch_count}, expected {scratch_expected}"
            ));
        }
    }

    Ok(())
}

async fn assert_integrity(db: &LocalDb, label: &str) -> Result<(), String> {
    let result = query_text(db, "PRAGMA integrity_check")
        .await
        .map_err(|e| format!("integrity_check {label}: {e}"))?;
    if result != "ok" {
        return Err(format!("integrity_check {label}: {result}"));
    }
    Ok(())
}

async fn run_worker(
    db_path: &str,
    iters: usize,
    worker_id: usize,
    delete_heavy: bool,
) -> Result<(), String> {
    for i in 0..iters {
        let db = open_probe_db(Path::new(db_path))
            .await
            .map_err(|e| format!("open iter {i}: {e}"))?;
        db.write(|conn| {
            let log_id = format!("{worker_id}:{i}");
            let previous_id = i.checked_sub(1).map(|prev| format!("{worker_id}:{prev}"));
            Box::pin(async move {
                // Read-modify-write the shared counter (forces commit conflicts
                // across processes that the retry loop must resolve).
                let mut rows = conn
                    .query("SELECT value FROM counters WHERE id = 'shared'", ())
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    cairn_core::internal::storage::DbError::Row("missing shared".into())
                })?;
                let value = row.i64(0)?;
                conn.execute(
                    "UPDATE counters SET value = ?1 WHERE id = 'shared'",
                    (value + 1,),
                )
                .await?;
                conn.execute(
                    "INSERT INTO worker_log(id, worker) VALUES (?1, ?2)",
                    (log_id.as_str(), worker_id as i64),
                )
                .await?;

                if delete_heavy {
                    conn.execute(
                        "INSERT INTO worker_scratch(id, worker, iter, touched) VALUES (?1, ?2, ?3, 0)",
                        (log_id.as_str(), worker_id as i64, i as i64),
                    )
                    .await?;
                    if let Some(previous_id) = previous_id.as_deref() {
                        conn.execute(
                            "UPDATE worker_scratch SET touched = 1 WHERE id = ?1",
                            (previous_id,),
                        )
                        .await?;
                    }
                    if i % 3 == 0 {
                        conn.execute("DELETE FROM worker_scratch WHERE id = ?1", (log_id.as_str(),))
                            .await?;
                    }
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("write iter {i}: {e}"))?;
    }
    Ok(())
}

async fn open_probe_db(path: &Path) -> Result<LocalDb, cairn_core::internal::storage::DbError> {
    let mut last_error = None;
    for _ in 0..6_000 {
        match LocalDb::open(path).await {
            Ok(db) => return Ok(db),
            Err(error) if error.to_string().contains("Locking error") => {
                last_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        cairn_core::internal::storage::DbError::internal("LocalDb open retry exhausted")
    }))
}

async fn read_shared(db: &LocalDb) -> Result<i64, cairn_core::internal::storage::DbError> {
    query_count(db, "SELECT value FROM counters WHERE id = 'shared'").await
}

async fn query_count(
    db: &LocalDb,
    sql: &'static str,
) -> Result<i64, cairn_core::internal::storage::DbError> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, ()).await?;
            let row = rows.next().await?.ok_or_else(|| {
                cairn_core::internal::storage::DbError::Row("missing integer row".into())
            })?;
            row.i64(0)
        })
    })
    .await
}

async fn query_text(
    db: &LocalDb,
    sql: &'static str,
) -> Result<String, cairn_core::internal::storage::DbError> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, ()).await?;
            let row = rows.next().await?.ok_or_else(|| {
                cairn_core::internal::storage::DbError::Row("missing text row".into())
            })?;
            row.text(0)
        })
    })
    .await
}

fn db_diagnostics(path: &Path) -> String {
    let mut parts = vec![
        format!("db_path={}", path.display()),
        format!("db_set_size={}", db_set_size(path)),
    ];
    for member in db_set_paths(path) {
        let size = std::fs::metadata(&member).map(|m| m.len()).unwrap_or(0);
        parts.push(format!("{}={size}", member.display()));
    }
    parts.join(", ")
}
