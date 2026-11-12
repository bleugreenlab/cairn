//! What every archival maintenance pass shares: the operator's off switch, the
//! archival permit, and folding the logical log by the shared rule.

use std::time::{Duration, Instant};

use super::rewrite::ARCHIVAL_GATE;
use crate::storage::{CheckpointReport, DbResult, DrainBudget, LocalDb};

/// Whether a pass with an environment switch runs, given the switch's value:
/// unless explicitly turned off (`off`, `0`, `false` or `no`, any case).
pub fn switch_enabled(setting: Option<&str>) -> bool {
    !matches!(
        setting
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("off" | "0" | "false" | "no")
    )
}

/// Take the process-wide archival permit, adding the wait to `waited`.
pub(crate) async fn acquire_gate(
    waited: &mut Duration,
) -> Result<tokio::sync::SemaphorePermit<'static>, String> {
    let started = Instant::now();
    let permit = ARCHIVAL_GATE
        .acquire()
        .await
        .map_err(|error| format!("archival gate closed: {error}"))?;
    *waited += started.elapsed();
    Ok(permit)
}

/// The folds a pass made, and the largest logical log it saw.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FoldTally {
    pub folds: usize,
    /// Folds that were due while an earlier attempt was still in the engine
    /// or a declared maintenance read was open.
    pub skipped: usize,
    pub failed: usize,
    pub max: Duration,
    pub log_peak_bytes: u64,
}

impl FoldTally {
    /// Note the log's size without folding.
    pub fn observe(&mut self, db: &LocalDb) -> u64 {
        let log_bytes = db.logical_log_bytes();
        self.log_peak_bytes = self.log_peak_bytes.max(log_bytes);
        log_bytes
    }
}

/// Fold the logical log if it is past `fold_at_log_bytes`, no earlier attempt
/// is still in the engine, and no declared maintenance read (the integrity
/// sweep) is open. While either is, the fold is skipped and counted rather
/// than waited on: the pass keeps moving and a later check folds.
pub(crate) async fn fold_if_due(
    db: &LocalDb,
    fold_at_log_bytes: u64,
    tally: &mut FoldTally,
    pass: &str,
) {
    if tally.observe(db) < fold_at_log_bytes {
        return;
    }
    if db.checkpoint_in_engine() || db.maintenance_read_open().is_some() {
        tally.skipped += 1;
        return;
    }
    let started = Instant::now();
    let outcome = db
        .checkpoint(3, Duration::from_millis(50), DrainBudget::PRODUCTION)
        .await;
    record_fold(tally, outcome, started.elapsed(), pass);
}

/// Count one checkpoint call. A report naming a maintenance read is a skip,
/// not a fold: the read registered after the pre-check above, or while this
/// call waited for the checkpoint lock, and the checkpoint did not drain.
fn record_fold(
    tally: &mut FoldTally,
    outcome: DbResult<CheckpointReport>,
    took: Duration,
    pass: &str,
) {
    if outcome
        .as_ref()
        .is_ok_and(|checkpoint| checkpoint.maintenance_read.is_some())
    {
        tally.skipped += 1;
        return;
    }
    tally.folds += 1;
    tally.max = tally.max.max(took);
    match outcome {
        Ok(checkpoint) if checkpoint.succeeded() => {}
        Ok(checkpoint) => {
            tally.failed += 1;
            log::debug!("{pass}: fold failed: {checkpoint}");
        }
        Err(error) => {
            tally.failed += 1;
            log::debug!("{pass}: fold failed: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_switch_turns_off_only_on_an_explicit_value() {
        for off in ["off", "OFF", " 0 ", "false", "No"] {
            assert!(!switch_enabled(Some(off)), "{off}");
        }
        for on in [
            None,
            Some(""),
            Some("on"),
            Some("1"),
            Some("yes"),
            Some("offline"),
        ] {
            assert!(switch_enabled(on), "{on:?}");
        }
    }

    fn report(
        error: Option<&str>,
        maintenance_read: Option<crate::storage::MaintenanceRead>,
    ) -> CheckpointReport {
        CheckpointReport {
            attempts: usize::from(maintenance_read.is_none()),
            error: error.map(str::to_owned),
            log_bytes_before: 0,
            log_bytes_after: 0,
            duration: Duration::ZERO,
            drain: crate::storage::DrainReport {
                drained: maintenance_read.is_none(),
                open_at_close: 0,
                budget: Duration::ZERO,
                still_open: 0,
                oldest: None,
                waited: Duration::ZERO,
            },
            slow: false,
            in_engine: false,
            maintenance_read,
        }
    }

    /// A checkpoint that returns naming the integrity sweep, because the sweep
    /// registered after the pre-check, is counted as a skip rather than a
    /// failed fold.
    #[test]
    fn a_checkpoint_turned_away_by_a_maintenance_read_counts_as_a_skip() {
        let mut tally = FoldTally::default();
        record_fold(
            &mut tally,
            Ok(report(
                Some("integrity sweep open"),
                Some(crate::storage::MaintenanceRead::IntegritySweep),
            )),
            Duration::from_secs(1),
            "test",
        );
        assert_eq!(
            tally,
            FoldTally {
                skipped: 1,
                ..FoldTally::default()
            }
        );

        record_fold(
            &mut tally,
            Ok(report(None, None)),
            Duration::from_millis(5),
            "test",
        );
        record_fold(
            &mut tally,
            Ok(report(Some("busy"), None)),
            Duration::from_millis(7),
            "test",
        );
        assert_eq!((tally.folds, tally.failed, tally.skipped), (2, 1, 1));
        assert_eq!(tally.max, Duration::from_millis(7));
    }
}
