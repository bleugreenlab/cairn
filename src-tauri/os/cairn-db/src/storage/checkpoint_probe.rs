//! Whether a long read stays out of the checkpoint's way, measured on a copy of
//! a real database.
//!
//! A unit test can show that a reader holds no connection between its pages;
//! only a production-sized history shows that each page is short enough. The
//! env-gated probes that use this (the digest probe in cairn-core's
//! `resources::thread_seed` and the export probe in `projects::export`) run
//! their read while a side task checkpoints every second, as `db_maintenance`
//! would under pressure, and a writer keeps the log growing.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{CheckpointReport, DrainBudget, LocalDb};

/// The forced-checkpoint ceiling the log must stay under.
pub const LOG_CEILING_BYTES: u64 = 256 * 1024 * 1024;

/// The oldest holder a pass may report.
pub const OLDEST_HOLDER_LIMIT: Duration = Duration::from_secs(5);

/// What the checkpoint saw while the work ran.
#[derive(Debug)]
pub struct PressureReport {
    /// The fold of the backlog a copy carried in, made before measuring.
    pub prefold: Option<CheckpointReport>,
    pub passes: Vec<CheckpointReport>,
    pub max_log_bytes: u64,
    pub work: Duration,
}

impl PressureReport {
    /// Every pass drained, none reported a holder older than
    /// [`OLDEST_HOLDER_LIMIT`], and the log stayed under the ceiling.
    pub fn assert_stayed_out_of_the_way(&self) {
        assert!(
            !self.passes.is_empty(),
            "no checkpoint pass ran during the work"
        );
        for pass in &self.passes {
            assert!(pass.drain.drained, "a pass could not drain: {pass}");
            if let Some((origin, age)) = pass.drain.oldest {
                assert!(
                    age < OLDEST_HOLDER_LIMIT,
                    "{origin} held a connection {} ms: {pass}",
                    age.as_millis()
                );
            }
        }
        assert!(
            self.max_log_bytes < LOG_CEILING_BYTES,
            "the log reached {} bytes",
            self.max_log_bytes
        );
    }
}

impl std::fmt::Display for PressureReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let longest_drain = self
            .passes
            .iter()
            .map(|pass| pass.drain.waited)
            .max()
            .unwrap_or_default();
        write!(
            f,
            "work_ms={} passes={} drained={} succeeded={} longest_drain_ms={} max_log_mib={:.1}",
            self.work.as_millis(),
            self.passes.len(),
            self.passes.iter().filter(|pass| pass.drain.drained).count(),
            self.passes.iter().filter(|pass| pass.succeeded()).count(),
            longest_drain.as_millis(),
            self.max_log_bytes as f64 / (1024.0 * 1024.0),
        )?;
        if let Some(prefold) = &self.prefold {
            write!(f, " prefold=[{prefold}]")?;
        }
        Ok(())
    }
}

/// Run `work` while a writer appends rows and a checkpoint pass runs every
/// second. Needs a multi-threaded runtime so the three make progress together.
pub async fn under_checkpoint_pressure<T>(
    db: Arc<LocalDb>,
    work: impl Future<Output = T>,
) -> (T, PressureReport) {
    // DDL needs an exclusive transaction under MVCC.
    db.exclusive(|conn| {
        Box::pin(async move {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS checkpoint_probe_writes(
                     id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .expect("creating the probe's write table");
    // A copy carries whatever log the live database had, which can be
    // gigabytes when checkpoints have been failing. Fold it first, as the
    // startup fold would, so the measurement starts where a healthy instance
    // does and the ceiling assertion measures the work rather than the copy.
    let prefold = if db.logical_log_bytes() >= LOG_CEILING_BYTES / 16 {
        let fold = db
            .checkpoint(
                3,
                Duration::from_millis(50),
                DrainBudget::fixed(Duration::from_secs(5)),
            )
            .await
            .expect("folding the copy's backlog");
        assert!(
            fold.succeeded(),
            "the copy's backlog would not fold: {fold}"
        );
        Some(fold)
    } else {
        None
    };
    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let db = db.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let payload = "x".repeat(1024);
            while !stop.load(Ordering::Relaxed) {
                let _ = db
                    .execute(
                        "INSERT INTO checkpoint_probe_writes(payload) VALUES (?1)",
                        (payload.as_str(),),
                    )
                    .await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    let checkpointer = {
        let db = db.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut passes = Vec::new();
            let mut max_log_bytes = db.logical_log_bytes();
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                max_log_bytes = max_log_bytes.max(db.logical_log_bytes());
                match db
                    .checkpoint(3, Duration::from_millis(50), DrainBudget::PRODUCTION)
                    .await
                {
                    Ok(pass) => passes.push(pass),
                    Err(error) => panic!("a checkpoint pass could not connect: {error}"),
                }
            }
            (passes, max_log_bytes)
        })
    };

    let started = Instant::now();
    let output = work.await;
    let work_elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    writer.await.expect("the probe writer panicked");
    let (passes, max_log_bytes) = checkpointer.await.expect("the probe checkpointer panicked");
    (
        output,
        PressureReport {
            prefold,
            passes,
            max_log_bytes,
            work: work_elapsed,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The harness itself: short reads under pressure pass its assertions,
    /// and a reader holding one transaction for the whole work does not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_harness_tells_a_held_reader_from_short_ones() {
        let path = tempfile::tempdir()
            .unwrap()
            .keep()
            .join("checkpoint-probe-harness.db");
        let db = Arc::new(LocalDb::open_migrated_for_test(path).await.unwrap());

        let (_, short) = under_checkpoint_pressure(db.clone(), async {
            for _ in 0..25 {
                let _ = db.query_opt_i64("SELECT count(*) FROM events", ()).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        short.assert_stayed_out_of_the_way();

        let (_, held) = under_checkpoint_pressure(db.clone(), async {
            let reader = db.connect().await.unwrap();
            reader.execute(db.concurrent_begin(), ()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            reader.execute("ROLLBACK", ()).await.unwrap();
        })
        .await;
        assert!(
            held.passes.iter().any(|pass| !pass.drain.drained),
            "a reader held across the work must cost at least one pass: {held}"
        );
    }
}
