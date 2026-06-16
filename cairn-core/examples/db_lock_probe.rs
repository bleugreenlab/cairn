//! CAIRN-1133 Phase 0 diagnostic: characterize Turso's local-file locking.
//!
//! The concurrency gate (`concurrent_db_probe`) showed cross-process `open`
//! fails with "File is locked by another process". This probe distinguishes:
//!   (a) lock held for the whole Database *lifetime* (app always holds it →
//!       any external process can never open → direct-to-DB infeasible), vs
//!   (b) lock held only momentarily during `build()` (collisions transient →
//!       feasible if `open` retries).
//!
//! Usage:
//!   cargo run --example db_lock_probe --features internal-api -- held
//!   cargo run --example db_lock_probe --features internal-api -- retry

use std::time::Duration;

use cairn_core::internal::storage::{LocalDb, RowExt};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("held");

    // Child entry point: try to open + write once, report result via exit code.
    if mode == "child" {
        let path = args.get(2).expect("child: db path").clone();
        match child_open_write(&path).await {
            Ok(()) => {
                println!("[child] OPEN+WRITE OK");
                std::process::exit(0);
            }
            Err(e) => {
                println!("[child] FAILED: {e}");
                std::process::exit(1);
            }
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lock-probe.turso.db");
    let path_str = path.to_string_lossy().to_string();
    let exe = std::env::current_exe().unwrap();

    // Parent opens + seeds, then HOLDS the handle for the rest of the test.
    let db = LocalDb::open(&path).await.unwrap();
    db.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, v INTEGER);")
        .await
        .unwrap();
    db.execute("INSERT OR REPLACE INTO t(id,v) VALUES ('x',0)", ())
        .await
        .unwrap();

    match mode {
        // Experiment (a): parent keeps `db` open; child attempts open while held.
        "held" => {
            println!("[parent] holding DB open, spawning child...");
            let status = std::process::Command::new(&exe)
                .arg("child")
                .arg(&path_str)
                .status()
                .unwrap();
            // Keep db alive until after child finishes.
            let v = read_v(&db).await;
            println!(
                "[parent] child success={}, final v={v} (lock held for lifetime => child should FAIL)",
                status.success()
            );
            std::process::exit(if status.success() { 0 } else { 2 });
        }
        // Experiment (b): parent DROPS the handle, then child opens. Confirms
        // open works once nobody holds it (rules out file-format issues).
        "retry" => {
            drop(db);
            println!("[parent] dropped DB, spawning child...");
            let status = std::process::Command::new(&exe)
                .arg("child")
                .arg(&path_str)
                .status()
                .unwrap();
            println!(
                "[parent] child success={} (no holder => should SUCCEED)",
                status.success()
            );
            std::process::exit(if status.success() { 0 } else { 2 });
        }
        other => {
            eprintln!("unknown mode: {other}");
            std::process::exit(3);
        }
    }
}

async fn child_open_write(path: &str) -> Result<(), String> {
    // Retry open a few times in case the lock is only momentary.
    let mut last_err = String::new();
    for attempt in 0..20 {
        match LocalDb::open(path).await {
            Ok(db) => {
                db.execute("UPDATE t SET v = v + 1 WHERE id = 'x'", ())
                    .await
                    .map_err(|e| format!("write: {e}"))?;
                return Ok(());
            }
            Err(e) => {
                last_err = format!("open attempt {attempt}: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(last_err)
}

async fn read_v(db: &LocalDb) -> i64 {
    db.read(|c| {
        Box::pin(async move {
            let mut rows = c.query("SELECT v FROM t WHERE id='x'", ()).await?;
            let row = rows.next().await?.unwrap();
            row.i64(0)
        })
    })
    .await
    .unwrap()
}
