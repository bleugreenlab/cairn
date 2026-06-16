//! Phase 0 concurrency gate (CAIRN-1133): prove that two *OS processes* can
//! concurrently read and write the same local `.turso.db` file without
//! corruption, relying only on the existing busy_timeout + optimistic-retry
//! machinery in `LocalDb`.
//!
//! This is the real cross-process test the within-process unit test in
//! `local_db.rs` cannot give us: Turso/SQLite coordination across separate
//! `turso::Database` handles inside one process could plausibly share in-memory
//! state, masking a cross-process failure. Re-spawning ourselves as worker
//! processes exercises genuine OS-level file locking + WAL/MVCC coordination.
//!
//! Usage:
//!   cargo run --example concurrent_db_probe --features internal-api
//!   cargo run --example concurrent_db_probe --features internal-api -- [WORKERS] [ITERS]
//!
//! Exit code 0 = gate passed; non-zero = corruption / lost update / error.

use cairn_core::internal::storage::{LocalDb, RowExt};

const DEFAULT_WORKERS: usize = 6;
const DEFAULT_ITERS: usize = 200;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Worker mode: `concurrent_db_probe worker <db_path> <iters> <worker_id>`
    if args.get(1).map(String::as_str) == Some("worker") {
        let db_path = args.get(2).expect("worker: missing db path").clone();
        let iters: usize = args.get(3).expect("worker: missing iters").parse().unwrap();
        let worker_id: usize = args.get(4).expect("worker: missing id").parse().unwrap();
        match run_worker(&db_path, iters, worker_id).await {
            Ok(()) => std::process::exit(0),
            Err(e) => {
                eprintln!("[worker {worker_id}] FAILED: {e}");
                std::process::exit(1);
            }
        }
    }

    // Parent / driver mode.
    let workers: usize = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_WORKERS);
    let iters: usize = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_ITERS);

    match run_parent(workers, iters).await {
        Ok(()) => {
            println!("GATE PASSED: {workers} processes x {iters} iters, no lost updates");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("GATE FAILED: {e}");
            std::process::exit(1);
        }
    }
}

async fn run_parent(workers: usize, iters: usize) -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let db_path = dir.path().join("cairn-concurrency-gate.turso.db");
    let db_path_str = db_path.to_string_lossy().to_string();

    // Parent owns schema setup and the initial shared row.
    {
        let db = LocalDb::open(&db_path)
            .await
            .map_err(|e| format!("parent open: {e}"))?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS counters (id TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS worker_log (id TEXT PRIMARY KEY NOT NULL, worker INTEGER NOT NULL);",
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
    for id in 0..workers {
        let child = std::process::Command::new(&exe)
            .arg("worker")
            .arg(&db_path_str)
            .arg(iters.to_string())
            .arg(id.to_string())
            .spawn()
            .map_err(|e| format!("spawn worker {id}: {e}"))?;
        children.push(child);
    }

    // While workers hammer the file, the parent interleaves its own reads to
    // confirm a separate process can read mid-contention without error.
    let reader = LocalDb::open(&db_path)
        .await
        .map_err(|e| format!("parent reader open: {e}"))?;
    for _ in 0..50 {
        let _ = read_shared(&reader)
            .await
            .map_err(|e| format!("parent read: {e}"))?;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Wait for all workers, collecting failures.
    let mut failed = Vec::new();
    for (id, mut child) in children.into_iter().enumerate() {
        let status = child.wait().map_err(|e| format!("wait worker {id}: {e}"))?;
        if !status.success() {
            failed.push(id);
        }
    }
    if !failed.is_empty() {
        return Err(format!("workers failed: {failed:?}"));
    }

    // Final invariant: every increment committed exactly once.
    let final_value = read_shared(&reader)
        .await
        .map_err(|e| format!("final read: {e}"))?;
    let expected = (workers * iters) as i64;
    if final_value != expected {
        return Err(format!(
            "lost updates: counter = {final_value}, expected {expected}"
        ));
    }

    // Worker-log row count is an independent corruption check (distinct PKs).
    let log_count = reader
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query("SELECT COUNT(*) FROM worker_log", ()).await?;
                let row = rows.next().await?.ok_or_else(|| {
                    cairn_core::internal::storage::DbError::Row("missing count".into())
                })?;
                row.i64(0)
            })
        })
        .await
        .map_err(|e| format!("log count: {e}"))?;
    if log_count != expected {
        return Err(format!(
            "worker_log rows = {log_count}, expected {expected}"
        ));
    }

    Ok(())
}

async fn run_worker(db_path: &str, iters: usize, worker_id: usize) -> Result<(), String> {
    let db = LocalDb::open(db_path)
        .await
        .map_err(|e| format!("open: {e}"))?;

    for i in 0..iters {
        db.write(|conn| {
            let log_id = format!("{worker_id}:{i}");
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
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("write iter {i}: {e}"))?;
    }
    Ok(())
}

async fn read_shared(db: &LocalDb) -> Result<i64, cairn_core::internal::storage::DbError> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT value FROM counters WHERE id = 'shared'", ())
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                cairn_core::internal::storage::DbError::Row("missing shared".into())
            })?;
            row.i64(0)
        })
    })
    .await
}
