//! What placing queued work costs this runner, counted where the work is done.
//!
//! Placement's cost is a rate: how often a waiting request re-judges the
//! fleet, and how many machines each judgment reads. The counters make that
//! rate observable without a profiler, on `cairn://executors` and in a log
//! line once a minute, so a change to how waiting is paced can be compared
//! before and after on the same terms.
//!
//! Every count is monotonic for the life of the process and costs one relaxed
//! atomic add where it is taken. Nothing here decides anything.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::unix_time_ms;

/// How often the counters are logged, and the window the page's rate reads.
const MINUTE: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(crate) struct FleetCounters {
    placement_passes: AtomicU64,
    fit_evaluations: AtomicU64,
    offers: AtomicU64,
    refusals: AtomicU64,
    reservations: AtomicU64,
    needs_worked_out: AtomicU64,
    announcements: AtomicU64,
    last_minute: Mutex<Option<CountedWindow>>,
}

/// The counts at one instant, or their growth over a window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlacementCounts {
    /// Matches the dispatcher ran: a walk of one machine's announced room,
    /// or one entry matched against the rooms.
    pub(crate) placement_passes: u64,
    /// Fits evaluated, summed over those matches.
    pub(crate) fit_evaluations: u64,
    /// Offers that reached a machine.
    pub(crate) offers: u64,
    /// Offers a machine refused.
    pub(crate) refusals: u64,
    /// Offers that carried a headroom authorization for aged work.
    pub(crate) reservations: u64,
    /// Times a queued request's needs on one machine were read from the
    /// profile store, rather than reused.
    pub(crate) needs_worked_out: u64,
    /// Room frames received from the fleet (cairn/7303).
    pub(crate) announcements: u64,
}

impl PlacementCounts {
    pub(crate) fn since(self, earlier: Self) -> Self {
        Self {
            placement_passes: self
                .placement_passes
                .saturating_sub(earlier.placement_passes),
            fit_evaluations: self.fit_evaluations.saturating_sub(earlier.fit_evaluations),
            offers: self.offers.saturating_sub(earlier.offers),
            refusals: self.refusals.saturating_sub(earlier.refusals),
            reservations: self.reservations.saturating_sub(earlier.reservations),
            needs_worked_out: self
                .needs_worked_out
                .saturating_sub(earlier.needs_worked_out),
            announcements: self.announcements.saturating_sub(earlier.announcements),
        }
    }
}

/// What the counters grew by over one window ending at a known instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CountedWindow {
    pub(crate) ended_at_unix_ms: u64,
    pub(crate) window_ms: u64,
    pub(crate) counts: PlacementCounts,
}

impl CountedWindow {
    /// `count` per second over this window.
    pub(crate) fn per_second(&self, count: u64) -> f64 {
        if self.window_ms == 0 {
            return 0.0;
        }
        count as f64 * 1_000.0 / self.window_ms as f64
    }

    pub(crate) fn sentence(&self) -> String {
        let counts = self.counts;
        format!(
            "{} placement passes ({:.1}/s), {} fit evaluations ({:.1}/s), {} offers, {} refusals, {} reservations, {} needs worked out, {} room announcements ({:.1}/s)",
            counts.placement_passes,
            self.per_second(counts.placement_passes),
            counts.fit_evaluations,
            self.per_second(counts.fit_evaluations),
            counts.offers,
            counts.refusals,
            counts.reservations,
            counts.needs_worked_out,
            counts.announcements,
            self.per_second(counts.announcements),
        )
    }
}

impl FleetCounters {
    /// What the dispatcher did: `passes` matches (a walk of one machine's
    /// room, or one entry placed), `fit_evaluations` fits evaluated in them.
    pub(crate) fn placement_work(&self, passes: u64, fit_evaluations: u64) {
        if passes > 0 {
            self.placement_passes.fetch_add(passes, Ordering::Relaxed);
        }
        if fit_evaluations > 0 {
            self.fit_evaluations
                .fetch_add(fit_evaluations, Ordering::Relaxed);
        }
    }

    /// An offer reached a machine; `reserving` when it authorized headroom.
    pub(crate) fn offer(&self, reserving: bool) {
        self.offers.fetch_add(1, Ordering::Relaxed);
        if reserving {
            self.reservations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A machine refused an offer.
    pub(crate) fn refusal(&self) {
        self.refusals.fetch_add(1, Ordering::Relaxed);
    }

    /// A machine announced its room.
    pub(crate) fn announcement(&self) {
        self.announcements.fetch_add(1, Ordering::Relaxed);
    }

    /// `count` needs were read from the profile store for a waiting request.
    pub(crate) fn needs_worked_out(&self, count: u64) {
        if count > 0 {
            self.needs_worked_out.fetch_add(count, Ordering::Relaxed);
        }
    }

    pub(crate) fn totals(&self) -> PlacementCounts {
        PlacementCounts {
            needs_worked_out: self.needs_worked_out.load(Ordering::Relaxed),
            announcements: self.announcements.load(Ordering::Relaxed),
            placement_passes: self.placement_passes.load(Ordering::Relaxed),
            fit_evaluations: self.fit_evaluations.load(Ordering::Relaxed),
            offers: self.offers.load(Ordering::Relaxed),
            refusals: self.refusals.load(Ordering::Relaxed),
            reservations: self.reservations.load(Ordering::Relaxed),
        }
    }

    /// The latest whole minute the log closed, if one has.
    pub(crate) fn last_minute(&self) -> Option<CountedWindow> {
        *self.last_minute.lock().unwrap()
    }

    fn close_window(&self, window: CountedWindow) {
        *self.last_minute.lock().unwrap() = Some(window);
    }
}

/// Log the counters' growth once a minute, and keep that minute for the page.
///
/// Holds the counters weakly, so the task ends with the fleet that owns them.
pub(crate) fn spawn_minute_log(counters: &Arc<FleetCounters>) {
    let counters = Arc::downgrade(counters);
    crate::orchestrator::spawn_onto(
        tokio::runtime::Handle::try_current().ok().as_ref(),
        async move {
            let mut previous = PlacementCounts::default();
            let mut started = Instant::now();
            loop {
                tokio::time::sleep(MINUTE).await;
                let Some(counters) = counters.upgrade() else {
                    return;
                };
                let totals = counters.totals();
                let window = CountedWindow {
                    ended_at_unix_ms: unix_time_ms(),
                    window_ms: started.elapsed().as_millis() as u64,
                    counts: totals.since(previous),
                };
                started = Instant::now();
                previous = totals;
                log::info!("placement work, last minute: {}", window.sentence());
                counters.close_window(window);
            }
        },
        || log::warn!("the placement counter log could not start; the counters still count"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_reports_growth_and_its_rate() {
        let counters = FleetCounters::default();
        counters.placement_work(1, 10);
        let before = counters.totals();
        counters.placement_work(1, 10);
        counters.placement_work(1, 4);
        counters.offer(true);
        counters.offer(false);
        counters.refusal();
        counters.needs_worked_out(3);
        counters.announcement();
        let window = CountedWindow {
            ended_at_unix_ms: 0,
            window_ms: 2_000,
            counts: counters.totals().since(before),
        };
        assert_eq!(
            window.counts,
            PlacementCounts {
                placement_passes: 2,
                fit_evaluations: 14,
                offers: 2,
                refusals: 1,
                reservations: 1,
                needs_worked_out: 3,
                announcements: 1,
            }
        );
        assert_eq!(
            window.sentence(),
            "2 placement passes (1.0/s), 14 fit evaluations (7.0/s), 2 offers, 1 refusals, 1 reservations, 3 needs worked out, 1 room announcements (0.5/s)"
        );
    }
}
