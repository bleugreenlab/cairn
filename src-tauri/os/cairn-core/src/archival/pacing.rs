//! What every archival maintenance pass shares: the operator's off switch, the
//! archival permit, and folding the logical log by the shared rule.

use std::time::{Duration, Instant};

use super::rewrite::ARCHIVAL_GATE;
use crate::storage::{DrainBudget, LocalDb};

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
    /// Folds that were due while an earlier attempt was still in the engine.
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

/// Fold the logical log if it is past `fold_at_log_bytes` and no earlier
/// attempt is still in the engine. While one is, the fold is skipped and
/// counted rather than waited on: the pass keeps moving and a later check
/// folds.
pub(crate) async fn fold_if_due(
    db: &LocalDb,
    fold_at_log_bytes: u64,
    tally: &mut FoldTally,
    pass: &str,
) {
    if tally.observe(db) < fold_at_log_bytes {
        return;
    }
    if db.checkpoint_in_engine() {
        tally.skipped += 1;
        return;
    }
    let started = Instant::now();
    let outcome = db
        .checkpoint(3, Duration::from_millis(50), DrainBudget::PRODUCTION)
        .await;
    tally.folds += 1;
    tally.max = tally.max.max(started.elapsed());
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
}
