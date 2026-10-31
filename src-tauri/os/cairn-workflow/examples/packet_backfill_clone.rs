//! Run the delegated-packet backfill on a COPY of a live database set and
//! measure what a packet status change costs afterwards (cairn/5597).
//!
//! Migrates the copy, folds its log, runs `delegated_packets::run_backfill`
//! and reports its time and log growth. It then takes the execution with the
//! most packets, folds again, changes one of its packets' status through
//! `delegated_packets::update_db`, and reports how many bytes that one change
//! added to the logical log.
//!
//! Writes to the database it is given. Never point it at the live set.
//!
//! Usage:
//!   cargo run -p cairn-workflow --example packet_backfill_clone -- <db>

use std::time::{Duration, Instant};

use cairn_workflow::delegated_packets::{self, PacketChange};
use cairn_workflow::models::DelegatedStatus;
use cairn_workflow::storage::{DrainBudget, LocalDb, RowExt};

#[tokio::main]
async fn main() {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: packet_backfill_clone <db>");
        std::process::exit(2);
    };
    if let Err(error) = run(&path).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn fold(db: &LocalDb) -> Result<(), String> {
    let report = db
        .checkpoint(3, Duration::from_millis(250), DrainBudget::PRODUCTION)
        .await
        .map_err(|e| e.to_string())?;
    if report.succeeded() {
        Ok(())
    } else {
        Err(format!("fold failed: {report}"))
    }
}

async fn run(path: &str) -> Result<(), String> {
    let started = Instant::now();
    let db = LocalDb::open_migrated_for_test(path)
        .await
        .map_err(|e| format!("open {path}: {e}"))?;
    println!(
        "opened and migrated in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    fold(&db).await?;

    let log_before = db.logical_log_bytes();
    let started = Instant::now();
    let report = delegated_packets::run_backfill(&db)
        .await
        .map_err(|e| format!("backfill: {e}"))?;
    println!(
        "backfill: {report:?} in {:.1}s; log {log_before} -> {} bytes",
        started.elapsed().as_secs_f64(),
        db.logical_log_bytes()
    );

    let (execution_id, packets, snapshot_bytes) = db
        .query_one(
            "SELECT p.execution_id, COUNT(*), \
                    (SELECT length(snapshot) FROM executions WHERE id = p.execution_id) \
             FROM delegated_packets p GROUP BY p.execution_id ORDER BY COUNT(*) DESC LIMIT 1",
            (),
            |row| Ok((row.text(0)?, row.i64(1)?, row.i64(2)?)),
        )
        .await
        .map_err(|e| format!("largest execution: {e}"))?;
    let packet_id = db
        .query_one(
            "SELECT id FROM delegated_packets WHERE execution_id = ?1 ORDER BY seq DESC LIMIT 1",
            (execution_id.clone(),),
            |row| row.text(0),
        )
        .await
        .map_err(|e| format!("packet: {e}"))?;

    fold(&db).await?;
    let log_before = db.logical_log_bytes();
    let change = PacketChange {
        status: Some(DelegatedStatus::Cancelled),
        ..PacketChange::default()
    };
    delegated_packets::update_db(&db, &execution_id, &packet_id, &change)
        .await
        .map_err(|e| format!("update: {e}"))?;
    println!(
        "status change on packet {packet_id} of {execution_id} \
         ({packets} packets; snapshot now {snapshot_bytes} bytes): {} bytes of log",
        db.logical_log_bytes().saturating_sub(log_before)
    );
    Ok(())
}
