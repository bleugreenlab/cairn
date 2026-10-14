//! Stable tokio runtime metrics.
//!
//! Only metrics that compile WITHOUT the `tokio_unstable` cfg (which a shipped
//! build cannot set) are collected. On the workspace's pinned tokio (1.49) that
//! is exactly: `num_workers`, `num_alive_tasks`, `global_queue_depth`, and the
//! per-worker `worker_total_busy_duration` (gated on 64-bit atomics via
//! `cfg_64bit_metrics!`, NOT on `tokio_unstable`). Verified against the pinned
//! source: `spawned_tasks_count`, `worker_mean_poll_time`, and
//! `io_driver_ready_count` are all behind `feature! { #![all(tokio_unstable,
//! ...)] }` in 1.49, so they are deliberately omitted here.

use tokio::runtime::RuntimeMetrics;

/// A point-in-time read of the stable tokio runtime metrics.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeSnapshot {
    pub(crate) num_workers: usize,
    pub num_alive_tasks: usize,
    pub global_queue_depth: usize,
    /// Sum of every worker's cumulative busy duration, in nanoseconds. `None`
    /// on targets without 64-bit atomics, where the underlying metric does not
    /// exist (every Cairn target has them, so this is `Some` in practice).
    pub(crate) total_worker_busy_nanos: Option<u128>,
}

impl RuntimeSnapshot {
    /// Read the stable metrics from a runtime's [`RuntimeMetrics`] handle.
    pub fn read(metrics: &RuntimeMetrics) -> Self {
        let num_workers = metrics.num_workers();
        Self {
            num_workers,
            num_alive_tasks: metrics.num_alive_tasks(),
            global_queue_depth: metrics.global_queue_depth(),
            total_worker_busy_nanos: total_worker_busy_nanos(metrics, num_workers),
        }
    }

    /// Mean busy ratio across workers between two snapshots: busy-time delta /
    /// (wall elapsed x workers). ~1.0 means every worker was fully busy the
    /// whole interval. `None` when either snapshot lacks the busy metric or the
    /// denominator is zero.
    pub fn busy_ratio_since(&self, prev: &RuntimeSnapshot, elapsed_nanos: u128) -> Option<f64> {
        let now = self.total_worker_busy_nanos?;
        let then = prev.total_worker_busy_nanos?;
        let workers = self.num_workers.max(1) as u128;
        let denom = elapsed_nanos.checked_mul(workers)?;
        if denom == 0 {
            return None;
        }
        Some(now.saturating_sub(then) as f64 / denom as f64)
    }
}

#[cfg(target_has_atomic = "64")]
fn total_worker_busy_nanos(metrics: &RuntimeMetrics, num_workers: usize) -> Option<u128> {
    let mut total = 0u128;
    for worker in 0..num_workers {
        total += metrics.worker_total_busy_duration(worker).as_nanos();
    }
    Some(total)
}

#[cfg(not(target_has_atomic = "64"))]
fn total_worker_busy_nanos(_metrics: &RuntimeMetrics, _num_workers: usize) -> Option<u128> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_stable_runtime_metrics() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let snap = RuntimeSnapshot::read(&rt.metrics());
        assert!(snap.num_workers >= 1);
        // Every supported target has 64-bit atomics, so the busy metric is live.
        #[cfg(target_has_atomic = "64")]
        assert!(snap.total_worker_busy_nanos.is_some());
    }

    #[test]
    fn busy_ratio_is_delta_over_wall_times_workers() {
        let base = RuntimeSnapshot {
            num_workers: 4,
            num_alive_tasks: 0,
            global_queue_depth: 0,
            total_worker_busy_nanos: Some(1_000),
        };
        let later = RuntimeSnapshot {
            total_worker_busy_nanos: Some(3_000),
            ..base
        };
        // 2000ns busy across 1000ns wall x 4 workers = 0.5.
        assert_eq!(later.busy_ratio_since(&base, 1_000), Some(0.5));
        // Missing baseline metric => no ratio.
        let no_metric = RuntimeSnapshot {
            total_worker_busy_nanos: None,
            ..base
        };
        assert_eq!(later.busy_ratio_since(&no_metric, 1_000), None);
    }
}
