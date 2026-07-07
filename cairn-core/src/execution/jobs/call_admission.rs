//! Generic bounded-admission bookkeeping for ephemeral (call) fan-out.
//!
//! [`CallAdmission`] is a pure, backend-agnostic slot ledger: it takes a ceiling
//! as a parameter and stores owned [`PreparedCallRun`]s, so the whole primitive
//! is unit-testable with no real backend process. The calls path (only) consults
//! it — see [`super::calls::start_call_run`] for the seam.
//!
//! A backend reporting `None` (Codex pools threads on one app-server;
//! OpenRouter runs calls in-process) makes [`CallAdmission::admit`] a pure
//! passthrough: it starts immediately, tracks nothing, and
//! [`CallAdmission::release`] is a no-op. Claude reports a bounded ceiling
//! derived from physical RAM (CAIRN-2557), so its calls admit up to the cap
//! and queue beyond it, promoted one per freed slot.

use super::PreparedCallRun;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// The admission decision for a single call.
pub enum Admission {
    /// Start the backend session now (caller invokes `start_call_run_now`).
    StartNow,
    /// Ceiling reached; the call is enqueued and its run stays `starting`.
    /// `release` will start it when a slot frees.
    Queued,
}

/// The outcome of releasing a finalized run's admission slot.
pub struct Released {
    /// Whether `run_id` was holding an in-flight slot (an admitted call, vs a
    /// queued/unknown run). The finalize hook uses this to reap the call's OS
    /// process at the moment its slot frees, so a real ceiling bounds real
    /// `claude` memory and not merely slot count (CAIRN-2548).
    pub held_slot: bool,
    /// The next queued call promoted into the freed slot, if any — the caller
    /// starts it.
    pub next: Option<PreparedCallRun>,
}

/// A backend-agnostic concurrency ledger for ephemeral calls, keyed by backend
/// name. Shared across `Orchestrator` clones behind an `Arc`; every method is
/// synchronous and non-blocking.
#[derive(Default)]
pub struct CallAdmission {
    inner: Mutex<AdmissionState>,
}

#[derive(Default)]
struct AdmissionState {
    /// Per-backend slot accounting, created lazily on first capped admit.
    backends: HashMap<String, BackendSlots>,
    /// `run_id -> backend` for runs currently holding a slot (O(1) release).
    admitted: HashMap<String, String>,
    /// `run_id -> backend` for runs sitting in a queue (no slot held).
    queued: HashMap<String, String>,
}

struct BackendSlots {
    /// The ceiling recorded from the descriptor on first admit for this backend.
    ceiling: usize,
    in_flight: usize,
    queue: VecDeque<PreparedCallRun>,
}

impl CallAdmission {
    /// Decide whether to start `prepared` now or queue it.
    ///
    /// - `ceiling == None` → [`Admission::StartNow`], **tracks nothing** (pure
    ///   passthrough — the Codex/OpenRouter production path).
    /// - `ceiling == Some(0)` → `Err` (fail-closed: a call that can never be
    ///   admitted must fail its run, never sit queued forever).
    /// - `ceiling == Some(n>=1)` → start now if a slot is free, else clone onto
    ///   the queue and return [`Admission::Queued`].
    ///
    /// Never blocks — the property that makes `restart_call` deadlock-free at
    /// ceiling 1.
    pub fn admit(
        &self,
        backend: &str,
        ceiling: Option<usize>,
        prepared: &PreparedCallRun,
    ) -> Result<Admission, String> {
        let Some(ceiling) = ceiling else {
            // Unbounded: pure passthrough, no bookkeeping.
            return Ok(Admission::StartNow);
        };
        if ceiling == 0 {
            return Err(format!(
                "call admission ceiling for backend {backend} is 0; call {} can never be admitted",
                prepared.run_id
            ));
        }

        let mut state = self.inner.lock().unwrap();
        let AdmissionState {
            backends,
            admitted,
            queued,
        } = &mut *state;
        let slots = backends.entry(backend.to_string()).or_insert(BackendSlots {
            ceiling,
            in_flight: 0,
            queue: VecDeque::new(),
        });

        if slots.in_flight < slots.ceiling {
            slots.in_flight += 1;
            admitted.insert(prepared.run_id.clone(), backend.to_string());
            Ok(Admission::StartNow)
        } else {
            slots.queue.push_back(prepared.clone());
            queued.insert(prepared.run_id.clone(), backend.to_string());
            Ok(Admission::Queued)
        }
    }

    /// Release the slot (if any) held by a finalized run, reporting whether it
    /// held one and promoting the next queued call when a slot frees.
    ///
    /// - `run_id` held a slot → decrement `in_flight`, report `held_slot: true`;
    ///   if a queued item exists, promote it (`next: Some`), else `next: None`.
    /// - `run_id` was queued (kill-while-queued) → dequeue it, `held_slot: false`.
    /// - `run_id` unknown (uncapped passthrough, or a non-call run) →
    ///   `held_slot: false, next: None`.
    ///
    /// `held_slot` is the finalize hook's signal to reap the call's OS process,
    /// tying slot release to process death so the ceiling bounds real memory
    /// (CAIRN-2548). Idempotent by construction (map removal): a second call for
    /// the same `run_id` reports `held_slot: false` and starts nothing, so
    /// hooking it from more than one terminal path never double-counts.
    pub fn release(&self, run_id: &str) -> Released {
        let mut state = self.inner.lock().unwrap();
        let AdmissionState {
            backends,
            admitted,
            queued,
        } = &mut *state;

        if let Some(backend) = admitted.remove(run_id) {
            let Some(slots) = backends.get_mut(&backend) else {
                return Released {
                    held_slot: true,
                    next: None,
                };
            };
            slots.in_flight = slots.in_flight.saturating_sub(1);
            if slots.in_flight < slots.ceiling {
                if let Some(next) = slots.queue.pop_front() {
                    slots.in_flight += 1;
                    queued.remove(&next.run_id);
                    admitted.insert(next.run_id.clone(), backend);
                    return Released {
                        held_slot: true,
                        next: Some(next),
                    };
                }
            }
            return Released {
                held_slot: true,
                next: None,
            };
        }

        if let Some(backend) = queued.remove(run_id) {
            // Kill-while-queued: no slot was ever held, just drop it from its queue.
            if let Some(slots) = backends.get_mut(&backend) {
                slots.queue.retain(|p| p.run_id != run_id);
            }
            return Released {
                held_slot: false,
                next: None,
            };
        }

        Released {
            held_slot: false,
            next: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::CallBatchCapability;
    use crate::models::{AgentConfig, ConfirmPolicy, Model, OutputSchema, OutputSchemaInfo};

    fn prepared(run_id: &str) -> PreparedCallRun {
        prepared_with_tool(run_id, None)
    }

    /// A minimal in-memory `PreparedCallRun`. `tool_name` lets a test tag the
    /// output schema so it can prove the schema survives the queue.
    fn prepared_with_tool(run_id: &str, tool_name: Option<&str>) -> PreparedCallRun {
        PreparedCallRun {
            job_id: format!("job-{run_id}"),
            run_id: run_id.to_string(),
            session_id: format!("sess-{run_id}"),
            owns_ephemeral_worktree: false,
            agent_config: AgentConfig {
                id: "Explore".into(),
                name: "Explore".into(),
                description: String::new(),
                prompt: String::new(),
                tools: Vec::new(),
                tier: None,
                workspace_id: None,
                project_id: None,
                created_at: 0,
                updated_at: 0,
                disallowed_tools: None,
                skills: None,
                fence: None,
                backend_preference: None,
                selection: None,
                extras: None,
            },
            selected_model: Some(Model::new("sonnet")),
            working_dir: "/tmp".into(),
            prompt: "do the thing".into(),
            output_schema: OutputSchemaInfo {
                schema: OutputSchema::Preset("return".into()),
                artifact_name: Some("return".into()),
                confirm_policy: ConfirmPolicy::default(),
                tool_name: tool_name.map(str::to_string),
                description: None,
            },
            execution_id: None,
            worktree_path: None,
        }
    }

    fn in_flight(adm: &CallAdmission, backend: &str) -> usize {
        adm.inner
            .lock()
            .unwrap()
            .backends
            .get(backend)
            .map(|s| s.in_flight)
            .unwrap_or(0)
    }

    fn queue_len(adm: &CallAdmission, backend: &str) -> usize {
        adm.inner
            .lock()
            .unwrap()
            .backends
            .get(backend)
            .map(|s| s.queue.len())
            .unwrap_or(0)
    }

    /// 1. Admits up to the ceiling, queues beyond.
    #[test]
    fn admits_up_to_ceiling_then_queues() {
        let adm = CallAdmission::default();
        assert!(matches!(
            adm.admit("claude", Some(2), &prepared("a")).unwrap(),
            Admission::StartNow
        ));
        assert!(matches!(
            adm.admit("claude", Some(2), &prepared("b")).unwrap(),
            Admission::StartNow
        ));
        assert!(matches!(
            adm.admit("claude", Some(2), &prepared("c")).unwrap(),
            Admission::Queued
        ));
        assert_eq!(in_flight(&adm, "claude"), 2);
        assert_eq!(queue_len(&adm, "claude"), 1);
    }

    /// 2. Completion releases a slot and starts the next queued call.
    #[test]
    fn completion_releases_and_starts_next() {
        let adm = CallAdmission::default();
        adm.admit("claude", Some(1), &prepared("a")).unwrap();
        adm.admit("claude", Some(1), &prepared("b")).unwrap();
        let next = adm.release("a").next.expect("queued b should be promoted");
        assert_eq!(next.run_id, "b");
        assert_eq!(queue_len(&adm, "claude"), 0);
        assert_eq!(in_flight(&adm, "claude"), 1);
    }

    /// 3. Kill-while-queued dequeues without releasing a slot, and never gets
    ///    spuriously admitted afterward.
    #[test]
    fn kill_while_queued_dequeues_without_release() {
        let adm = CallAdmission::default();
        adm.admit("claude", Some(1), &prepared("a")).unwrap();
        adm.admit("claude", Some(1), &prepared("b")).unwrap();
        assert_eq!(queue_len(&adm, "claude"), 1);

        // Kill the queued call: no slot held, nothing to start.
        assert!(adm.release("b").next.is_none());
        assert_eq!(queue_len(&adm, "claude"), 0);
        assert_eq!(in_flight(&adm, "claude"), 1);

        // Releasing the admitted call must NOT resurrect the killed one.
        assert!(adm.release("a").next.is_none());
        assert_eq!(in_flight(&adm, "claude"), 0);
    }

    /// Rollback contract for a StartNow start failure: `start_call_run` admits a
    /// call (StartNow, holding a slot) and, if `start_agent_session` then fails,
    /// finalizes the run — which drives `release` here. Model that failure by
    /// releasing the admitted run: the slot is freed and the next queued call is
    /// promoted, so a capped-backend start failure never leaks a slot. (The
    /// finalize wiring itself is integration-level — it needs a real backend to
    /// fail — but the leak-proofing lives in this release path.)
    #[test]
    fn admitted_start_failure_release_rolls_back_and_promotes() {
        let adm = CallAdmission::default();
        adm.admit("claude", Some(1), &prepared("a")).unwrap(); // StartNow, holds slot
        adm.admit("claude", Some(1), &prepared("b")).unwrap(); // Queued
                                                               // 'a' fails to start -> its finalize releases the slot and promotes 'b'.
        let next = adm
            .release("a")
            .next
            .expect("b promoted after a's start failure");
        assert_eq!(next.run_id, "b");
        assert_eq!(in_flight(&adm, "claude"), 1);
        // 'b' also fails -> released, no leak, a fresh admit starts immediately.
        assert!(adm.release("b").next.is_none());
        assert_eq!(in_flight(&adm, "claude"), 0);
        assert!(matches!(
            adm.admit("claude", Some(1), &prepared("c")).unwrap(),
            Admission::StartNow
        ));
    }

    /// 4. Kill-while-running releases the slot; a fresh admit then starts.
    #[test]
    fn kill_while_running_releases() {
        let adm = CallAdmission::default();
        adm.admit("claude", Some(1), &prepared("a")).unwrap();
        assert_eq!(in_flight(&adm, "claude"), 1);
        assert!(adm.release("a").next.is_none());
        assert_eq!(in_flight(&adm, "claude"), 0);
        assert!(matches!(
            adm.admit("claude", Some(1), &prepared("b")).unwrap(),
            Admission::StartNow
        ));
    }

    /// 5. Uncapped descriptor is pure passthrough: starts now, tracks nothing,
    ///    release is a no-op.
    #[test]
    fn uncapped_is_pure_passthrough() {
        let adm = CallAdmission::default();
        assert!(matches!(
            adm.admit("claude", None, &prepared("a")).unwrap(),
            Admission::StartNow
        ));
        let state = adm.inner.lock().unwrap();
        assert!(state.admitted.is_empty());
        assert!(state.queued.is_empty());
        assert!(state.backends.is_empty());
        drop(state);
        assert!(adm.release("a").next.is_none());
    }

    /// 6. Non-blocking at ceiling: with the single slot held, admit returns
    ///    Queued synchronously (the restart-deadlock guard at ceiling 1).
    #[test]
    fn non_blocking_at_ceiling_one() {
        let adm = CallAdmission::default();
        adm.admit("claude", Some(1), &prepared("a")).unwrap();
        assert!(matches!(
            adm.admit("claude", Some(1), &prepared("b")).unwrap(),
            Admission::Queued
        ));
    }

    /// 7. Fail-closed on a zero ceiling.
    #[test]
    fn zero_ceiling_is_err() {
        let adm = CallAdmission::default();
        assert!(adm.admit("claude", Some(0), &prepared("a")).is_err());
    }

    /// 8. The output schema survives the queue: a promoted call retains the
    ///    schema it was enqueued with (guards the native-output invariant).
    #[test]
    fn schema_survives_the_queue() {
        let adm = CallAdmission::default();
        adm.admit("claude", Some(1), &prepared("a")).unwrap();
        adm.admit(
            "claude",
            Some(1),
            &prepared_with_tool("b", Some("write_plan")),
        )
        .unwrap();
        let next = adm.release("a").next.expect("b promoted");
        assert_eq!(next.output_schema.tool_name.as_deref(), Some("write_plan"));
    }

    /// 9. Ceiling-1 completion chains: an admitted call advances the queue
    ///    exactly once.
    #[test]
    fn ceiling_one_completion_chains() {
        let adm = CallAdmission::default();
        assert!(matches!(
            adm.admit("claude", Some(1), &prepared("a")).unwrap(),
            Admission::StartNow
        ));
        assert!(matches!(
            adm.admit("claude", Some(1), &prepared("b")).unwrap(),
            Admission::Queued
        ));
        assert_eq!(
            adm.release("a").next.map(|p| p.run_id),
            Some("b".to_string())
        );
        assert!(adm.release("b").next.is_none());
    }

    /// Separate backends keep separate ceilings.
    #[test]
    fn ceilings_are_per_backend() {
        let adm = CallAdmission::default();
        adm.admit("claude", Some(1), &prepared("a")).unwrap();
        // A different backend at its own ceiling still starts now.
        assert!(matches!(
            adm.admit("codex", Some(1), &prepared("b")).unwrap(),
            Admission::StartNow
        ));
        assert_eq!(in_flight(&adm, "claude"), 1);
        assert_eq!(in_flight(&adm, "codex"), 1);
    }

    /// The descriptor's ceiling is the value threaded into `admit`; a sanity
    /// check that `CallBatchCapability.max_concurrency` is what callers pass.
    #[test]
    fn descriptor_ceiling_is_the_admit_argument() {
        let cap = CallBatchCapability {
            shape: crate::backends::CallBatchShape::DedicatedProcess,
            max_concurrency: Some(3),
        };
        let adm = CallAdmission::default();
        for id in ["a", "b", "c"] {
            assert!(matches!(
                adm.admit("claude", cap.max_concurrency, &prepared(id))
                    .unwrap(),
                Admission::StartNow
            ));
        }
        assert!(matches!(
            adm.admit("claude", cap.max_concurrency, &prepared("d"))
                .unwrap(),
            Admission::Queued
        ));
    }

    /// `release` reports whether the run held an in-flight slot — the signal the
    /// finalize hook keys on to reap the call's process the moment its slot
    /// frees (CAIRN-2548). A queued or unknown run held none.
    #[test]
    fn release_reports_slot_ownership() {
        let adm = CallAdmission::default();
        adm.admit("claude", Some(1), &prepared("a")).unwrap(); // admitted, holds the slot
        adm.admit("claude", Some(1), &prepared("b")).unwrap(); // queued, holds none
        assert!(!adm.release("b").held_slot, "a queued call holds no slot");
        assert!(adm.release("a").held_slot, "an admitted call holds a slot");
        assert!(
            !adm.release("zzz").held_slot,
            "an unknown run holds no slot"
        );
    }

    /// The live Claude descriptor caps concurrent ephemeral calls: prepared
    /// calls admit up to its `max_concurrency` and queue the rest, so a
    /// fan-out's `claude` process count is bounded regardless of width. The
    /// ceiling is now RAM-derived (CAIRN-2557), so this asserts the descriptor
    /// threads *whatever* bounded ceiling it reports into admission rather than
    /// a fixed number — only that it lies within the documented [4, 64] clamp.
    #[test]
    fn claude_descriptor_bounds_concurrent_calls() {
        use crate::backends::AgentBackend;
        let ceiling = crate::backends::claude::ClaudeBackend
            .call_batch_capability()
            .max_concurrency
            .expect("Claude reports a bounded ephemeral-call ceiling");
        assert!(
            (4..=64).contains(&ceiling),
            "Claude ceiling {ceiling} must lie within the documented clamp range"
        );
        let adm = CallAdmission::default();
        // Admit exactly `ceiling` calls, then a further batch that must queue.
        for i in 0..ceiling {
            assert!(matches!(
                adm.admit("Claude", Some(ceiling), &prepared(&format!("c{i}")))
                    .unwrap(),
                Admission::StartNow
            ));
        }
        for i in 0..3 {
            assert!(matches!(
                adm.admit("Claude", Some(ceiling), &prepared(&format!("q{i}")))
                    .unwrap(),
                Admission::Queued
            ));
        }
    }
}
