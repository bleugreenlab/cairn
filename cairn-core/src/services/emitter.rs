//! Event emitter service abstraction.
//!
//! Abstracts event emission to enable testing without a real Tauri runtime
//! and to support alternative backends (e.g., WebSocket broadcasting).

use serde_json::Value;

#[cfg(any(test, feature = "test-utils"))]
use std::sync::Mutex;

#[cfg(any(test, feature = "test-utils"))]
use mockall::automock;

/// Trait for emitting events to the frontend.
///
/// This abstraction allows tests to capture emitted events
/// without requiring a real Tauri window.
///
/// Note: Uses serde_json::Value instead of generics to be dyn-compatible.
#[cfg_attr(any(test, feature = "test-utils"), automock)]
pub trait EventEmitter: Send + Sync {
    /// Emit an event with a JSON payload.
    fn emit(&self, event: &str, payload: Value) -> Result<(), String>;

    /// Emit an event with no payload (null).
    fn emit_empty(&self, event: &str) -> Result<(), String>;
}

/// Test helper that captures emitted events.
///
/// Use this in tests to verify events were emitted correctly.
#[cfg(any(test, feature = "test-utils"))]
pub struct CapturingEmitter {
    events: Mutex<Vec<(String, Value)>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl Default for CapturingEmitter {
    fn default() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl CapturingEmitter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get all captured events as (name, payload) tuples.
    pub fn captured(&self) -> Vec<(String, Value)> {
        self.events.lock().unwrap().clone()
    }

    /// Check if an event with the given name was emitted.
    pub fn has_event(&self, name: &str) -> bool {
        self.events.lock().unwrap().iter().any(|(n, _)| n == name)
    }

    /// Get events with a specific name.
    pub fn events_named(&self, name: &str) -> Vec<Value> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
            .collect()
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl EventEmitter for CapturingEmitter {
    fn emit(&self, event: &str, payload: Value) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push((event.to_string(), payload));
        Ok(())
    }

    fn emit_empty(&self, event: &str) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push((event.to_string(), Value::Null));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn capturing_emitter_captures_events() {
        let emitter = CapturingEmitter::new();

        emitter.emit("test-event", json!("payload")).unwrap();
        emitter.emit_empty("empty-event").unwrap();

        let captured = emitter.captured();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].0, "test-event");
        assert_eq!(captured[0].1, json!("payload"));
        assert_eq!(captured[1].0, "empty-event");
        assert_eq!(captured[1].1, Value::Null);
    }

    #[test]
    fn capturing_emitter_has_event() {
        let emitter = CapturingEmitter::new();

        emitter.emit("my-event", json!(42)).unwrap();

        assert!(emitter.has_event("my-event"));
        assert!(!emitter.has_event("other-event"));
    }

    #[test]
    fn capturing_emitter_events_named() {
        let emitter = CapturingEmitter::new();

        emitter.emit("repeat", json!(1)).unwrap();
        emitter.emit("other", json!("x")).unwrap();
        emitter.emit("repeat", json!(2)).unwrap();

        let repeats = emitter.events_named("repeat");
        assert_eq!(repeats.len(), 2);
        assert_eq!(repeats[0], json!(1));
        assert_eq!(repeats[1], json!(2));
    }

    #[test]
    fn mock_emitter_works() {
        let mut mock = MockEventEmitter::new();
        mock.expect_emit().returning(|_, _| Ok(()));
        mock.expect_emit_empty().returning(|_| Ok(()));

        assert!(mock.emit("event", json!("data")).is_ok());
        assert!(mock.emit_empty("event").is_ok());
    }
}
