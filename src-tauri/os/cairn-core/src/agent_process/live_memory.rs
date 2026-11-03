//! Live resident memory of every runner-hosted agent process tree.
//!
//! One dedicated thread measures, at the executor's admission cadence, the
//! tree (leader plus descendants) of every registered agent process that has
//! an attached child, then publishes the result twice: as the latest value,
//! and as one `agent-memory-sample` event carrying the whole reading. It parks
//! while no agent process is registered and wakes when one registers.
//!
//! Nothing on a read path measures anything: [`AgentMemorySampler::latest`]
//! clones the last published reading. A process census run on demand, or one
//! per row a surface renders, would put an unbounded walk behind an invoke.
//!
//! A plain `std::thread`, not a Tokio worker: each tick blocks in kernel reads
//! for every member of every tree, which does not belong on a runtime worker
//! (the same reason `cairn-exec`'s `ProcessTreeSampler` runs on its own thread).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use cairn_common::executor_protocol::{
    Measurement, MeasurementGap, CPU_ADMISSION_SAMPLE_INTERVAL_MS,
};
use serde::{Deserialize, Serialize};

use crate::agent_process::memory::{MemoryProbe, TREE_READS_SUPPORTED};
use crate::agent_process::process::AgentProcessState;
use crate::services::EventEmitter;

/// The event each tick publishes, carrying an [`AgentMemoryReading`].
pub const AGENT_MEMORY_SAMPLE_EVENT: &str = "agent-memory-sample";

const SAMPLE_INTERVAL: Duration = Duration::from_millis(CPU_ADMISSION_SAMPLE_INTERVAL_MS);

/// One agent process tree's resident memory at the reading's time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentResident {
    pub run_id: String,
    /// Sum over the members that could be read; a floor when not `complete`.
    pub resident_bytes: u64,
    pub members: u32,
    /// Every member was read. An incomplete tree is shown as at least its
    /// total, never as its size.
    pub complete: bool,
}

/// Every measured tree, stamped with the tick that measured it. A run absent
/// from a measured reading had no readable tree at that tick.
pub type AgentMemoryReading = Measurement<Vec<AgentResident>>;

pub struct AgentMemorySampler {
    latest: Mutex<AgentMemoryReading>,
    started: AtomicBool,
    stopped: AtomicBool,
    interval: Duration,
    /// The registry whose signal a stop must raise to wake a parked thread.
    state: OnceLock<Weak<AgentProcessState>>,
}

impl Default for AgentMemorySampler {
    fn default() -> Self {
        Self::with_interval(SAMPLE_INTERVAL)
    }
}

impl AgentMemorySampler {
    pub(crate) fn with_interval(interval: Duration) -> Self {
        let latest = if TREE_READS_SUPPORTED {
            Measurement::default()
        } else {
            Measurement::unavailable(unix_ms(), MeasurementGap::UnsupportedPlatform)
        };
        Self {
            latest: Mutex::new(latest),
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            interval,
            state: OnceLock::new(),
        }
    }

    /// The last published reading. Never measures.
    pub fn latest(&self) -> AgentMemoryReading {
        self.latest
            .lock()
            .map(|latest| latest.clone())
            .unwrap_or_default()
    }

    /// Start the sampling thread, once. Where `probe` cannot read trees no
    /// thread starts, and the latest reading names the platform gap.
    pub fn start(
        self: &Arc<Self>,
        state: Arc<AgentProcessState>,
        probe: Arc<dyn MemoryProbe>,
        emitter: Arc<dyn EventEmitter>,
    ) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        if !probe.reads_trees() {
            self.publish(
                Measurement::unavailable(unix_ms(), MeasurementGap::UnsupportedPlatform),
                None,
            );
            return;
        }
        let _ = self.state.set(Arc::downgrade(&state));
        let sampler = self.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("agent-memory-sampler".into())
            .spawn(move || sampler.run(state, probe, emitter))
        {
            log::warn!("agent memory sampler could not start: {error}");
            self.publish(
                Measurement::unavailable_with(
                    unix_ms(),
                    MeasurementGap::SamplingFailed,
                    "sampler thread did not start",
                ),
                None,
            );
        }
    }

    /// Stop the thread at its next wake. Used by tests; a host keeps its
    /// sampler for the life of the process.
    #[cfg(test)]
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        if let Some(state) = self.state.get().and_then(Weak::upgrade) {
            state.live_tree_signal().notify();
        }
    }

    fn run(
        self: Arc<Self>,
        state: Arc<AgentProcessState>,
        probe: Arc<dyn MemoryProbe>,
        emitter: Arc<dyn EventEmitter>,
    ) {
        let signal = state.live_tree_signal();
        let mut empty_published = false;
        while !self.stopped.load(Ordering::SeqCst) {
            let seen = signal.generation();
            let trees = state.live_trees();
            if trees.is_empty() {
                // The last tree's exit is published once, so a surface stops
                // showing it; an idle registry then costs nothing.
                if !empty_published {
                    self.publish(
                        Measurement::measured(unix_ms(), Vec::new()),
                        Some(&*emitter),
                    );
                    empty_published = true;
                }
                signal.wait_past(seen, None);
                continue;
            }
            empty_published = false;
            let residents = trees
                .iter()
                .filter_map(|tree| {
                    let reading = probe.process_tree_resident(&tree.leader)?;
                    Some(AgentResident {
                        run_id: tree.run_id.clone(),
                        resident_bytes: reading.resident_bytes,
                        members: reading.members,
                        complete: !reading.incomplete(),
                    })
                })
                .collect();
            self.publish(Measurement::measured(unix_ms(), residents), Some(&*emitter));
            // A fixed cadence: a registration during the wait is measured at
            // the next tick rather than shortening this one.
            let deadline = Instant::now() + self.interval;
            while !self.stopped.load(Ordering::SeqCst) && Instant::now() < deadline {
                signal.wait_past(signal.generation(), Some(deadline));
            }
        }
    }

    fn publish(&self, reading: AgentMemoryReading, emitter: Option<&dyn EventEmitter>) {
        if let Some(emitter) = emitter {
            let _ = emitter.emit(
                AGENT_MEMORY_SAMPLE_EVENT,
                serde_json::to_value(&reading).unwrap_or(serde_json::Value::Null),
            );
        }
        if let Ok(mut latest) = self.latest.lock() {
            *latest = reading;
        }
    }
}

fn unix_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::memory::StubMemoryProbe;
    use crate::agent_process::process::RunHandle;
    use crate::services::testing::CapturingEmitter;
    use cairn_common::executor_protocol::MeasurementReading;
    use cairn_common::process_identity::ProcessAnchor;
    use std::sync::atomic::AtomicUsize;

    const TICK: Duration = Duration::from_millis(20);

    fn leader(pid: u32) -> ProcessAnchor {
        ProcessAnchor {
            pid,
            start_token: "s".into(),
        }
    }

    fn register(state: &AgentProcessState, run_id: &str, pid: u32) {
        state
            .register_process(
                run_id.into(),
                RunHandle::test_handle(Some(run_id), None).with_leader(leader(pid)),
            )
            .unwrap();
    }

    fn started(
        probe: StubMemoryProbe,
    ) -> (
        Arc<AgentMemorySampler>,
        Arc<AgentProcessState>,
        Arc<CapturingEmitter>,
        Arc<AtomicUsize>,
    ) {
        let reads = probe.tree_reads.clone();
        let sampler = Arc::new(AgentMemorySampler::with_interval(TICK));
        let state = Arc::new(AgentProcessState::default());
        let emitter = Arc::new(CapturingEmitter::new());
        sampler.start(state.clone(), Arc::new(probe), emitter.clone());
        (sampler, state, emitter, reads)
    }

    fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn residents(sampler: &AgentMemorySampler) -> Vec<AgentResident> {
        sampler.latest().value().cloned().unwrap_or_default()
    }

    #[test]
    fn an_empty_registry_parks_the_sampler_without_reading_anything() {
        let (sampler, _state, emitter, reads) = started(StubMemoryProbe::default());
        wait_for("the empty reading", || sampler.latest().value().is_some());
        std::thread::sleep(TICK * 10);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(
            emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len(),
            1,
            "an idle registry publishes its emptiness once"
        );
        sampler.stop();
    }

    #[test]
    fn a_registration_wakes_the_sampler_and_is_published_within_a_tick() {
        let probe = StubMemoryProbe::default()
            .with_tree(41, "s", vec![Some(100), Some(23)])
            .with_tree(42, "s", vec![Some(7), None]);
        let (sampler, state, emitter, _reads) = started(probe);
        wait_for("the empty reading", || sampler.latest().value().is_some());
        register(&state, "run-a", 41);
        register(&state, "run-b", 42);
        wait_for("both trees", || residents(&sampler).len() == 2);
        let mut published = residents(&sampler);
        published.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        assert_eq!(
            published,
            vec![
                AgentResident {
                    run_id: "run-a".into(),
                    resident_bytes: 123,
                    members: 2,
                    complete: true,
                },
                AgentResident {
                    run_id: "run-b".into(),
                    resident_bytes: 7,
                    members: 2,
                    complete: false,
                },
            ]
        );
        let event = emitter
            .events_named(AGENT_MEMORY_SAMPLE_EVENT)
            .last()
            .cloned()
            .unwrap();
        let event: AgentMemoryReading = serde_json::from_value(event).unwrap();
        assert!(matches!(event.reading, MeasurementReading::Measured { .. }));
        sampler.stop();
    }

    #[test]
    fn a_recycled_leader_is_left_out_rather_than_measured() {
        let probe = StubMemoryProbe::default().with_tree(41, "another-start", vec![Some(100)]);
        let (sampler, state, _emitter, reads) = started(probe);
        register(&state, "run-a", 41);
        wait_for("a read of the tree", || reads.load(Ordering::SeqCst) > 0);
        wait_for("the reading", || sampler.latest().value().is_some());
        assert!(residents(&sampler).is_empty());
        sampler.stop();
    }

    #[test]
    fn reading_the_latest_value_never_measures() {
        let probe = StubMemoryProbe::default().with_tree(41, "s", vec![Some(1)]);
        let reads = probe.tree_reads.clone();
        let sampler = AgentMemorySampler::with_interval(TICK);
        for _ in 0..1_000 {
            let _ = sampler.latest();
        }
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(sampler.latest().gap(), Some(MeasurementGap::NotSampled));
    }

    #[test]
    fn a_probe_without_tree_reads_starts_no_thread_and_names_the_gap() {
        let probe = StubMemoryProbe {
            no_tree_reads: true,
            ..StubMemoryProbe::default()
        };
        let (sampler, state, emitter, reads) = started(probe);
        register(&state, "run-a", 41);
        std::thread::sleep(TICK * 5);
        assert_eq!(
            sampler.latest().gap(),
            Some(MeasurementGap::UnsupportedPlatform)
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).is_empty());
    }
}
