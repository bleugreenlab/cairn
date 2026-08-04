//! Content-safe, correlated timing events for the agent resume path.
use serde::Serialize;
use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

static PROCESS_ORIGIN: OnceLock<Instant> = OnceLock::new();
const FIRST_EVENT_CAPACITY: usize = 4096;

static FIRST_EVENTS: OnceLock<Mutex<FirstEvents>> = OnceLock::new();

struct FirstEvents {
    keys: HashSet<String>,
    insertion_order: VecDeque<String>,
    capacity: usize,
}

impl FirstEvents {
    fn new(capacity: usize) -> Self {
        Self {
            keys: HashSet::with_capacity(capacity),
            insertion_order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn mark(&mut self, key: String) -> bool {
        if self.keys.contains(&key) {
            return false;
        }
        if self.keys.len() == self.capacity {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.keys.remove(&oldest);
            }
        }
        self.insertion_order.push_back(key.clone());
        self.keys.insert(key)
    }
}

#[derive(Default, Serialize)]
pub(crate) struct ResumeTimingEvent<'a> {
    pub event: &'a str,
    pub monotonic_us: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_us: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued_message_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_attempts: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_failures: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<&'a str>,
}

pub(crate) fn mark_first(key: String) -> bool {
    FIRST_EVENTS
        .get_or_init(|| Mutex::new(FirstEvents::new(FIRST_EVENT_CAPACITY)))
        .lock()
        .map(|mut seen| seen.mark(key))
        .unwrap_or(false)
}

impl<'a> ResumeTimingEvent<'a> {
    pub(crate) fn new(event: &'a str) -> Self {
        Self {
            event,
            monotonic_us: monotonic_us(),
            ..Self::default()
        }
    }
    pub(crate) fn elapsed(mut self, started: Instant) -> Self {
        self.duration_us = Some(started.elapsed().as_micros());
        self
    }
    pub(crate) fn emit(self) {
        match serde_json::to_string(&self) {
            Ok(encoded) => log::info!(target: "cairn.resume_timing", "{encoded}"),
            Err(error) => {
                log::warn!(target: "cairn.resume_timing", "timing event serialization failed: {error}")
            }
        }
    }
}

pub(crate) fn monotonic_us() -> u128 {
    PROCESS_ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_micros()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn event_schema_cannot_carry_sensitive_content() {
        let mut event = ResumeTimingEvent::new("queue_claim_end");
        event.job_id = Some("job-1");
        event.count = Some(2);
        event.bytes = Some(17);
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["count"], 2);
        assert!(value.get("content").is_none());
        assert!(value.get("prompt").is_none());
    }
    #[test]
    fn monotonic_clock_never_moves_backwards() {
        assert!(monotonic_us() <= monotonic_us());
    }

    #[test]
    fn first_events_evict_the_oldest_key_at_capacity() {
        let mut events = FirstEvents::new(2);
        assert!(events.mark("oldest".into()));
        assert!(events.mark("newest".into()));
        assert!(!events.mark("oldest".into()));
        assert!(events.mark("third".into()));
        assert!(events.mark("oldest".into()));
        assert!(!events.mark("third".into()));
    }
}
