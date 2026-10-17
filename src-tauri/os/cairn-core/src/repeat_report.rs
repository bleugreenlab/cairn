//! Report a repeating failure once per change, and summarize the repeats.
//!
//! A loop that keeps meeting the same failure (a channel that cannot attach, a
//! request a machine keeps refusing) owes the log its first occurrence and
//! any change, not every retry. Logging every retry is how one parked request
//! wrote 347,281 placement-rejection lines in a day. Between changes, the
//! repeats are counted and folded into the next periodic report, so the log
//! still shows that the condition persisted and how often it was met.

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// One repeating condition's reporting state.
#[derive(Debug)]
pub struct RepeatReporter {
    interval: Duration,
    last_signature: Option<String>,
    last_reported_at: Option<Instant>,
    last_seen_at: Option<Instant>,
    suppressed: u64,
}

impl RepeatReporter {
    /// `interval` is how long a persisting, unchanged condition goes between
    /// reports.
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_signature: None,
            last_reported_at: None,
            last_seen_at: None,
            suppressed: 0,
        }
    }

    /// Note one occurrence. Returns the line to log, or `None` when this one is
    /// a repeat that is folded into the next report.
    ///
    /// `signature` is what counts as "the same" (an error's text, a rejection's
    /// kind); `message` is what is logged when a report is due. A periodic
    /// report of an unchanged condition carries how many repeats it stands for.
    pub fn record(&mut self, now: Instant, signature: &str, message: &str) -> Option<String> {
        self.last_seen_at = Some(now);
        let changed = self.last_signature.as_deref() != Some(signature);
        let report_due = self
            .last_reported_at
            .is_none_or(|last| now.duration_since(last) >= self.interval);
        if !(changed || report_due) {
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }
        let suffix = if changed || self.suppressed == 0 {
            String::new()
        } else {
            format!(" ({} identical attempts suppressed)", self.suppressed)
        };
        self.last_signature = Some(signature.to_string());
        self.last_reported_at = Some(now);
        self.suppressed = 0;
        Some(format!("{message}{suffix}"))
    }
}

/// A [`RepeatReporter`] per key, for a loop that meets many conditions at once.
///
/// Keys that have not been seen for a few intervals are dropped, so a key that
/// stops recurring (a request that was placed or gave up) does not accumulate.
#[derive(Debug)]
pub struct KeyedRepeatReporter<K> {
    interval: Duration,
    reporters: HashMap<K, RepeatReporter>,
    last_pruned_at: Option<Instant>,
}

impl<K: Eq + Hash> KeyedRepeatReporter<K> {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            reporters: HashMap::new(),
            last_pruned_at: None,
        }
    }

    /// [`RepeatReporter::record`] for `key`.
    pub fn record(
        &mut self,
        now: Instant,
        key: K,
        signature: &str,
        message: &str,
    ) -> Option<String> {
        self.prune(now);
        let interval = self.interval;
        self.reporters
            .entry(key)
            .or_insert_with(|| RepeatReporter::new(interval))
            .record(now, signature, message)
    }

    /// Forget every key `matches` selects, because what it tracked is over.
    pub fn forget(&mut self, matches: impl Fn(&K) -> bool) {
        self.reporters.retain(|key, _| !matches(key));
    }

    fn prune(&mut self, now: Instant) {
        let horizon = self.interval.saturating_mul(3);
        if self
            .last_pruned_at
            .is_some_and(|last| now.duration_since(last) < self.interval)
        {
            return;
        }
        self.last_pruned_at = Some(now);
        self.reporters.retain(|_, reporter| {
            reporter
                .last_seen_at
                .is_some_and(|seen| now.duration_since(seen) < horizon)
        });
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.reporters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL: Duration = Duration::from_secs(60);

    #[test]
    fn a_repeat_is_suppressed_and_summarized_by_the_next_periodic_report() {
        let start = Instant::now();
        let mut reporter = RepeatReporter::new(INTERVAL);
        let record = |reporter: &mut RepeatReporter, at| {
            reporter.record(at, "executor disconnected", "executor disconnected")
        };
        assert_eq!(
            record(&mut reporter, start).as_deref(),
            Some("executor disconnected")
        );
        assert_eq!(record(&mut reporter, start + Duration::from_secs(2)), None);
        assert_eq!(record(&mut reporter, start + Duration::from_secs(4)), None);
        assert_eq!(
            record(&mut reporter, start + INTERVAL).as_deref(),
            Some("executor disconnected (2 identical attempts suppressed)")
        );
    }

    /// The signature decides sameness, not the message: a rejection whose
    /// figures move on every pass is still the same rejection.
    #[test]
    fn a_changed_signature_reports_at_once_and_a_changed_message_does_not() {
        let start = Instant::now();
        let mut reporter = RepeatReporter::new(INTERVAL);
        assert!(reporter.record(start, "stale", "silence 80s").is_some());
        assert_eq!(
            reporter.record(start + Duration::from_secs(1), "stale", "silence 81s"),
            None
        );
        assert_eq!(
            reporter
                .record(start + Duration::from_secs(2), "declined", "declined: cpu")
                .as_deref(),
            Some("declined: cpu")
        );
    }

    #[test]
    fn keys_report_independently_and_idle_keys_are_dropped() {
        let start = Instant::now();
        let mut reporters = KeyedRepeatReporter::new(INTERVAL);
        assert!(reporters.record(start, "a", "stale", "a stale").is_some());
        assert!(reporters.record(start, "b", "stale", "b stale").is_some());
        assert!(reporters.record(start, "a", "stale", "a stale").is_none());
        assert_eq!(reporters.len(), 2);
        reporters.forget(|key| *key == "a");
        assert_eq!(reporters.len(), 1);
        let later = start + INTERVAL * 4;
        assert!(reporters.record(later, "c", "stale", "c stale").is_some());
        assert_eq!(reporters.len(), 1, "b went idle and was dropped");
    }
}
