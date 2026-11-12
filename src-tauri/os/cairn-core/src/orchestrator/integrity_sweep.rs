//! When the weekly integrity sweep starts: once the cadence says it is due, and
//! only when the host is not loaded.
//!
//! `PRAGMA integrity_check` reads the whole file in one statement, at about
//! 65 MB/s (8 min 9 s over 31 GB at boot load on 2026-11-09). It cannot yield,
//! and nothing can fold the logical log behind the snapshot it holds, so its
//! cost scales with the file rather than with any work and the one lever on
//! what it costs everything else is when it runs. The startup gate's quiet
//! signal never arrives on a working runner, so on its own it would start the
//! sweep at the ten-minute deadline, in the middle of whatever the fleet is
//! doing. The sweep therefore also waits for the host's own 1- and 5-minute
//! load averages to fall under its core count, looking again every
//! [`SWEEP_LOAD_CHECK`]. A postponement is never a skip: it records nothing,
//! so the sweep stays due until it runs.

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use crate::storage::{
    integrity_sweep_due, integrity_sweep_since_last, run_integrity_sweep, DbResult,
    IntegritySweepOutcome, LocalDb,
};

/// How often a postponed sweep looks at the host's load again.
pub(crate) const SWEEP_LOAD_CHECK: Duration = Duration::from_secs(5 * 60);

/// How often a sweep that stays postponed says so.
const POSTPONED_LINE_EVERY: Duration = Duration::from_secs(60 * 60);

/// Past this since the last sweep, a postponement line is a WARN: two
/// cadences without verification means the host has not cooled down for a
/// week, and that should be visible rather than silent.
const OVERDUE_AFTER: Duration = Duration::from_secs(14 * 24 * 60 * 60);

/// Whether the host's load lets the sweep start.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SweepAdmission {
    Admit {
        one: f64,
        five: f64,
        limit: usize,
    },
    /// The platform has no load average (Windows). Nothing else in the runtime
    /// gates on load there, and inventing a value would be worse than
    /// admitting.
    AdmitNoLoadSignal,
    Postpone {
        one: f64,
        five: f64,
        limit: usize,
    },
}

/// Admit when the 1- and 5-minute load averages are both under `cores`, the
/// host's logical core count. On the 16-core production host that is the
/// clone procedure's "both under 15" rule, scaled to whatever host this is.
pub(crate) fn sweep_admission(load: Option<[f64; 3]>, cores: usize) -> SweepAdmission {
    let Some([one, five, _]) = load else {
        return SweepAdmission::AdmitNoLoadSignal;
    };
    let limit = cores.max(1);
    if one < limit as f64 && five < limit as f64 {
        SweepAdmission::Admit { one, five, limit }
    } else {
        SweepAdmission::Postpone { one, five, limit }
    }
}

/// How one wait for the sweep ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SweepRun {
    /// The cadence did not call for a sweep; one row was read.
    NotDue,
    /// The process began shutting down while the sweep was postponed.
    ShutDown,
    Swept {
        outcome: IntegritySweepOutcome,
        /// Load checks that postponed it before it was admitted.
        postponed_checks: usize,
    },
}

/// When a postponement is worth a line: the first, then once an hour.
#[derive(Debug, Default)]
struct PostponedLine {
    last: Option<Instant>,
}

impl PostponedLine {
    fn due(&mut self, now: Instant) -> bool {
        if self
            .last
            .is_some_and(|at| now.saturating_duration_since(at) < POSTPONED_LINE_EVERY)
        {
            return false;
        }
        self.last = Some(now);
        true
    }
}

/// Wait out the host's load, then sweep. The production entry point for both
/// startup schedules.
pub(crate) async fn run(db: &LocalDb, mut shutdown: watch::Receiver<bool>) {
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    if let Err(error) = sweep_when_host_allows(
        db,
        &mut shutdown,
        crate::pressure::platform::host_load_average,
        cores,
        SWEEP_LOAD_CHECK,
    )
    .await
    {
        log::warn!("integrity sweep failed: {error}");
    }
}

/// The sweep's admission loop, over any load source and check interval.
///
/// Each check reads the cadence first, so a sweep that stopped being due
/// while it waited (another process swept the same database) ends the wait.
pub(crate) async fn sweep_when_host_allows(
    db: &LocalDb,
    shutdown: &mut watch::Receiver<bool>,
    mut load: impl FnMut() -> Option<[f64; 3]>,
    cores: usize,
    check_every: Duration,
) -> DbResult<SweepRun> {
    let mut line = PostponedLine::default();
    let mut postponed_checks = 0usize;
    loop {
        if *shutdown.borrow() {
            return Ok(SweepRun::ShutDown);
        }
        if !integrity_sweep_due(db).await? {
            return Ok(SweepRun::NotDue);
        }
        match sweep_admission(load(), cores) {
            SweepAdmission::Admit { one, five, limit } => {
                log::info!(
                    "integrity sweep started: load 1m={one:.1} 5m={five:.1} under {limit} cores \
                     after {postponed_checks} postponed checks"
                );
                break;
            }
            SweepAdmission::AdmitNoLoadSignal => {
                log::info!("integrity sweep started: host load unavailable");
                break;
            }
            SweepAdmission::Postpone { one, five, limit } => {
                postponed_checks += 1;
                if line.due(Instant::now()) {
                    log_postponed(db, postponed_checks == 1, one, five, limit, check_every).await;
                }
                tokio::select! {
                    _ = shutdown.changed() => return Ok(SweepRun::ShutDown),
                    _ = tokio::time::sleep(check_every) => {}
                }
            }
        }
    }
    let outcome = run_integrity_sweep(db).await?;
    Ok(SweepRun::Swept {
        outcome,
        postponed_checks,
    })
}

async fn log_postponed(
    db: &LocalDb,
    first: bool,
    one: f64,
    five: f64,
    limit: usize,
    check_every: Duration,
) {
    let load = format!("load 1m={one:.1} 5m={five:.1} not under {limit} cores");
    let overdue = integrity_sweep_since_last(db)
        .await
        .ok()
        .flatten()
        .filter(|since| *since > OVERDUE_AFTER);
    match overdue {
        Some(since) => log::warn!(
            "integrity sweep {} days since its last run and still postponed: {load}; checking \
             every {}s",
            since.as_secs() / (24 * 60 * 60),
            check_every.as_secs(),
        ),
        None if first => log::info!(
            "integrity sweep due; postponed: {load}; checking every {}s",
            check_every.as_secs(),
        ),
        None => log::info!(
            "integrity sweep still postponed: {load}; checking every {}s",
            check_every.as_secs(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const HIGH: Option<[f64; 3]> = Some([20.0, 18.0, 12.0]);
    const LOW: Option<[f64; 3]> = Some([3.0, 4.0, 6.0]);

    #[test]
    fn the_sweep_is_admitted_only_with_both_averages_under_the_core_count() {
        assert_eq!(
            sweep_admission(Some([15.9, 15.9, 40.0]), 16),
            SweepAdmission::Admit {
                one: 15.9,
                five: 15.9,
                limit: 16
            },
            "the 15-minute average does not count"
        );
        for load in [[16.0, 2.0, 2.0], [2.0, 16.0, 2.0], [30.0, 30.0, 30.0]] {
            assert!(
                matches!(
                    sweep_admission(Some(load), 16),
                    SweepAdmission::Postpone { limit: 16, .. }
                ),
                "{load:?}"
            );
        }
        assert_eq!(sweep_admission(None, 16), SweepAdmission::AdmitNoLoadSignal);
        assert!(
            matches!(
                sweep_admission(Some([0.5, 0.5, 0.5]), 0),
                SweepAdmission::Admit { limit: 1, .. }
            ),
            "a core count of zero is treated as one"
        );
    }

    #[test]
    fn a_postponement_is_logged_once_then_hourly() {
        let start = Instant::now();
        let mut line = PostponedLine::default();
        let checks: Vec<bool> = (0..=13)
            .map(|check| line.due(start + SWEEP_LOAD_CHECK * check))
            .collect();
        let logged: Vec<usize> = checks
            .iter()
            .enumerate()
            .filter_map(|(check, due)| due.then_some(check))
            .collect();
        assert_eq!(logged, vec![0, 12], "five-minute checks, one line an hour");
    }

    async fn due_db() -> LocalDb {
        let db =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("sweep.db"))
                .await
                .unwrap();
        db.execute(
            "UPDATE integrity_sweep_state SET last_swept_at = NULL WHERE id = 1",
            (),
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn a_loaded_host_postpones_the_sweep_until_it_cools() {
        let db = due_db().await;
        let (_tx, mut shutdown) = watch::channel(false);
        let mut script = VecDeque::from([HIGH, HIGH, LOW]);
        let run = sweep_when_host_allows(
            &db,
            &mut shutdown,
            || script.pop_front().expect("no load check after admission"),
            16,
            Duration::from_millis(5),
        )
        .await
        .unwrap();
        assert_eq!(
            run,
            SweepRun::Swept {
                outcome: IntegritySweepOutcome::Clean,
                postponed_checks: 2
            }
        );
        assert!(script.is_empty());
        assert!(
            !integrity_sweep_due(&db).await.unwrap(),
            "the sweep is stamped"
        );
    }

    #[tokio::test]
    async fn a_sweep_that_is_not_due_reads_no_load() {
        let db = due_db().await;
        db.execute(
            "UPDATE integrity_sweep_state SET last_swept_at = unixepoch() WHERE id = 1",
            (),
        )
        .await
        .unwrap();
        let (_tx, mut shutdown) = watch::channel(false);
        let reads = AtomicUsize::new(0);
        let run = sweep_when_host_allows(
            &db,
            &mut shutdown,
            || {
                reads.fetch_add(1, Ordering::Relaxed);
                LOW
            },
            16,
            Duration::from_millis(5),
        )
        .await
        .unwrap();
        assert_eq!(run, SweepRun::NotDue);
        assert_eq!(reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn shutdown_ends_a_postponement_promptly_and_records_nothing() {
        let db = due_db().await;
        let (tx, mut shutdown) = watch::channel(false);
        let wait =
            sweep_when_host_allows(&db, &mut shutdown, || HIGH, 16, Duration::from_secs(3600));
        let mut wait = std::pin::pin!(wait);
        tokio::select! {
            _ = &mut wait => panic!("a loaded host must not admit the sweep"),
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        tx.send(true).unwrap();
        let run = tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("shutdown must end the wait")
            .unwrap();
        assert_eq!(run, SweepRun::ShutDown);
        assert_eq!(
            integrity_sweep_since_last(&db).await.unwrap(),
            None,
            "a postponement never stamps the sweep"
        );
        assert!(integrity_sweep_due(&db).await.unwrap());
    }
}
