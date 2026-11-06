//! The host's own record that it is serving a tool call for a run.
//!
//! A provider can reach Cairn's gateway without announcing a tool item of its
//! own: a Codex agent's `functions.exec` cell calls `read`, `write` and `run`
//! directly, so the provider's event stream says nothing while the host works.
//! Whatever supervises that provider's silence still has to know the provider is
//! waiting on the host. A backend registers a [`HostCallWitness`] for its run;
//! the gateway opens a [`HostCallGuard`] for every call it serves, and the guard
//! reports the answer when it drops, which includes a call dropped unanswered.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, Weak};

/// Told when the host starts and finishes serving a tool call for its run.
pub trait HostCallWitness: Send + Sync {
    /// The host began serving `call_id`. Returns the scope the call was taken
    /// under (for Codex, the provider turn), or `None` when the witness has
    /// nothing live to attribute it to and will not hear of it again.
    fn call_started(&self, call_id: &str) -> Option<String>;

    /// The host answered or dropped `call_id`, which was taken under `scope`.
    /// A witness whose scope has since ended must ignore it.
    fn call_answered(&self, call_id: &str, scope: &str);
}

/// The witnesses registered per run, one per run at a time.
#[derive(Default)]
pub struct HostCallWitnesses {
    witnesses: Mutex<HashMap<String, Weak<dyn HostCallWitness>>>,
    sequence: AtomicU64,
}

impl HostCallWitnesses {
    /// Make `witness` the one told about `run_id`'s calls, replacing any other.
    pub fn register(&self, run_id: &str, witness: Weak<dyn HostCallWitness>) {
        if let Ok(mut witnesses) = self.witnesses.lock() {
            witnesses.insert(run_id.to_string(), witness);
        }
    }

    /// Withdraw `witness` from `run_id`, leaving a witness that replaced it.
    pub fn unregister(&self, run_id: &str, witness: &Weak<dyn HostCallWitness>) {
        if let Ok(mut witnesses) = self.witnesses.lock() {
            if witnesses
                .get(run_id)
                .is_some_and(|current| Weak::ptr_eq(current, witness))
            {
                witnesses.remove(run_id);
            }
        }
    }

    /// Begin serving a call for `run_id`. The returned guard reports the answer
    /// when it drops; `None` when nothing is watching the run.
    ///
    /// The call id is the provider's tool use id with a host sequence number,
    /// so two calls one exec cell makes concurrently stay distinct.
    pub fn serve(&self, run_id: &str, tool_use_id: Option<&str>) -> Option<HostCallGuard> {
        let witness = self.witnesses.lock().ok()?.get(run_id)?.clone();
        let live = witness.upgrade()?;
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let call_id = format!("{}#host-{sequence}", tool_use_id.unwrap_or("call"));
        let scope = live.call_started(&call_id)?;
        Some(HostCallGuard {
            witness,
            call_id,
            scope,
        })
    }
}

/// A call the host is serving. Dropping it reports the answer.
pub struct HostCallGuard {
    witness: Weak<dyn HostCallWitness>,
    call_id: String,
    scope: String,
}

impl HostCallGuard {
    pub fn call_id(&self) -> &str {
        &self.call_id
    }
}

impl Drop for HostCallGuard {
    fn drop(&mut self) {
        if let Some(witness) = self.witness.upgrade() {
            witness.call_answered(&self.call_id, &self.scope);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Default)]
    struct Recording {
        scope: Option<String>,
        events: Mutex<Vec<String>>,
    }

    impl HostCallWitness for Recording {
        fn call_started(&self, call_id: &str) -> Option<String> {
            self.events.lock().unwrap().push(format!("start {call_id}"));
            self.scope.clone()
        }
        fn call_answered(&self, call_id: &str, scope: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("answer {call_id} in {scope}"));
        }
    }

    fn witness(scope: Option<&str>) -> Arc<Recording> {
        Arc::new(Recording {
            scope: scope.map(str::to_string),
            ..Default::default()
        })
    }

    #[test]
    fn a_served_call_reports_its_answer_when_the_guard_drops() {
        let registry = HostCallWitnesses::default();
        let recording = witness(Some("turn-1"));
        let weak: Weak<dyn HostCallWitness> = Arc::downgrade(&recording) as _;
        registry.register("run-1", weak);

        let guard = registry.serve("run-1", Some("exec-7")).expect("watched");
        let call_id = guard.call_id().to_string();
        assert!(call_id.starts_with("exec-7#"), "{call_id}");
        assert_eq!(
            *recording.events.lock().unwrap(),
            [format!("start {call_id}")]
        );
        drop(guard);
        assert_eq!(
            *recording.events.lock().unwrap(),
            [
                format!("start {call_id}"),
                format!("answer {call_id} in turn-1")
            ]
        );
    }

    #[test]
    fn concurrent_calls_with_one_tool_use_id_stay_distinct() {
        let registry = HostCallWitnesses::default();
        let recording = witness(Some("turn-1"));
        registry.register("run-1", Arc::downgrade(&recording) as _);
        let first = registry.serve("run-1", Some("exec-7")).unwrap();
        let second = registry.serve("run-1", Some("exec-7")).unwrap();
        assert_ne!(first.call_id(), second.call_id());
    }

    #[test]
    fn an_unwatched_run_or_unscoped_witness_serves_without_a_guard() {
        let registry = HostCallWitnesses::default();
        assert!(registry.serve("run-1", None).is_none());

        let idle = witness(None);
        registry.register("run-1", Arc::downgrade(&idle) as _);
        assert!(registry.serve("run-1", None).is_none());

        let gone = witness(Some("turn-1"));
        registry.register("run-2", Arc::downgrade(&gone) as _);
        drop(gone);
        assert!(registry.serve("run-2", None).is_none());
    }

    #[test]
    fn unregistering_a_replaced_witness_leaves_its_successor() {
        let registry = HostCallWitnesses::default();
        let old = witness(Some("turn-1"));
        let new = witness(Some("turn-2"));
        let old_weak: Weak<dyn HostCallWitness> = Arc::downgrade(&old) as _;
        registry.register("run-1", old_weak.clone());
        registry.register("run-1", Arc::downgrade(&new) as _);

        registry.unregister("run-1", &old_weak);
        let guard = registry
            .serve("run-1", None)
            .expect("successor still watches");
        drop(guard);
        assert!(old.events.lock().unwrap().is_empty());
        assert_eq!(new.events.lock().unwrap().len(), 2);
    }
}
