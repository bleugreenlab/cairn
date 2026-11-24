//! The served window-drain fit (`cairn_models_types::drain`), held so its
//! readers (the `cairn://models/usage?capacity` read, and cairn/6031's supply
//! ranking through `DrainFit::dollars_per_token`) take the fitted rates without
//! refitting. The orchestrator refits off-thread after a usage refresh records
//! readings, at most once per [`REFIT_INTERVAL_SECS`] and only when the
//! inputs moved past the held [`Watermark`]; a read that finds the book empty
//! fits once itself.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use cairn_models_types::drain::{DrainFit, Watermark};

/// The least time between two refits.
pub const REFIT_INTERVAL_SECS: i64 = 15 * 60;

#[derive(Debug, Clone)]
struct Held {
    fit: Arc<DrainFit>,
    watermark: Watermark,
    /// When the inputs were last compared, refitted or not.
    checked_at: i64,
}

/// The latest drain fit and the inputs it was fitted to.
#[derive(Debug, Default)]
pub struct DrainBook {
    held: RwLock<Option<Held>>,
    refitting: AtomicBool,
}

/// The single refit in flight; dropping it frees the slot.
pub struct RefitSlot(Arc<DrainBook>);

impl Drop for RefitSlot {
    fn drop(&mut self) {
        self.0.refitting.store(false, Ordering::Release);
    }
}

impl DrainBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// The held fit, if one has been made.
    pub fn fit(&self) -> Option<Arc<DrainFit>> {
        self.held.read().ok()?.as_ref().map(|held| held.fit.clone())
    }

    /// What the held fit was fitted to.
    pub fn watermark(&self) -> Option<Watermark> {
        self.held.read().ok()?.as_ref().map(|held| held.watermark)
    }

    /// Whether a refit is due at `now`: nothing is held, or the inputs were
    /// last compared at least [`REFIT_INTERVAL_SECS`] ago.
    pub fn due(&self, now: i64) -> bool {
        self.held
            .read()
            .ok()
            .and_then(|held| held.as_ref().map(|held| held.checked_at))
            .is_none_or(|checked_at| now - checked_at >= REFIT_INTERVAL_SECS)
    }

    /// Claim the one refit slot, or `None` while another refit runs.
    pub fn claim(self: &Arc<Self>) -> Option<RefitSlot> {
        self.refitting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| RefitSlot(self.clone()))
    }

    /// Hold `fit`, fitted at `at` to inputs reaching `watermark`.
    pub fn store(&self, fit: DrainFit, watermark: Watermark, at: i64) -> Arc<DrainFit> {
        let fit = Arc::new(fit);
        if let Ok(mut held) = self.held.write() {
            *held = Some(Held {
                fit: fit.clone(),
                watermark,
                checked_at: at,
            });
        }
        fit
    }

    /// The inputs were compared at `at` and had not moved: keep the fit and
    /// restart the cadence.
    pub fn unchanged(&self, at: i64) {
        if let Ok(mut held) = self.held.write() {
            if let Some(held) = held.as_mut() {
                held.checked_at = at;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refit_is_due_when_empty_then_once_per_interval_and_single_flight() {
        let book = Arc::new(DrainBook::new());
        assert!(book.due(0));
        assert!(book.fit().is_none());
        let slot = book.claim().unwrap();
        assert!(book.claim().is_none(), "one refit at a time");
        let watermark = Watermark {
            reading_id: 7,
            rollup_bucket: 3600,
            rollup_events: 9,
        };
        book.store(DrainFit::default(), watermark, 1_000);
        drop(slot);
        assert!(book.claim().is_some(), "the slot frees on drop");
        assert_eq!(book.watermark(), Some(watermark));
        assert!(!book.due(1_000 + REFIT_INTERVAL_SECS - 1));
        assert!(book.due(1_000 + REFIT_INTERVAL_SECS));
        book.unchanged(5_000);
        assert!(!book.due(5_000 + 1));
        assert!(book.fit().is_some());
    }
}
