//! Cached publication adapter for the runner's shared process-tree sampler.
//!
//! The session registry owns watch lifetimes; cairn-procmem owns the only census
//! thread. Reads clone published evidence and never perform kernel work.

use crate::agent_process::process::{AgentProcessState, RunnerMemoryObserver};
use crate::services::EventEmitter;
use cairn_common::executor_protocol::{Measurement, MeasurementGap, MACHINE_SAMPLE_INTERVAL_MS};
use cairn_common::process_identity::ProcessAnchor;
use cairn_procmem::{
    BatchObserver, ProcessTreeSampler, SamplingStatus, TickBatch, TickClocks, WatchId,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

pub const AGENT_MEMORY_SAMPLE_EVENT: &str = "agent-memory-sample";
const SAMPLE_INTERVAL: Duration = Duration::from_millis(MACHINE_SAMPLE_INTERVAL_MS);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentResident {
    pub run_id: String,
    /// Readable resident bytes (RSS): a floor when incomplete.
    pub resident_bytes: u64,
    /// Physical footprint over the same members as `resident_bytes`, absent
    /// when any of them has none (Windows, or Linux without a readable
    /// `smaps_rollup`). A reader shows this where present, since RSS counts
    /// shared pages a tree does not privately cost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_footprint_bytes: Option<u64>,
    pub members: u32,
    pub complete: bool,
}
pub type AgentMemoryReading = Measurement<Vec<AgentResident>>;

#[derive(Default)]
struct Published {
    reading: AgentMemoryReading,
    anchors: HashMap<String, (ProcessAnchor, WatchId)>,
}
#[derive(Default)]
pub struct AgentMemorySampler {
    latest: Mutex<Published>,
    state: OnceLock<Weak<AgentProcessState>>,
    emitter: OnceLock<Arc<dyn EventEmitter>>,
    engine: OnceLock<Arc<ProcessTreeSampler>>,
}
struct PublicationObserver(Weak<AgentMemorySampler>);
impl BatchObserver for PublicationObserver {
    fn tick(&self, batch: &TickBatch<'_>) {
        if let Some(publication) = self.0.upgrade() {
            publication.tick(batch);
        }
    }
    fn failed(&self, clocks: TickClocks, watches: &[(WatchId, SamplingStatus)]) {
        if watches
            .iter()
            .any(|(_, status)| matches!(status, SamplingStatus::Measured(_)))
        {
            return;
        }
        if let Some(publication) = self.0.upgrade() {
            publication.publish(
                Measurement::unavailable(clocks.unix_ms, MeasurementGap::SamplingFailed),
                HashMap::new(),
            );
        }
    }
    fn retired(&self, id: WatchId) {
        if let Some(publication) = self.0.upgrade() {
            if let Some(state) = publication.state.get().and_then(Weak::upgrade) {
                state.retire_memory_tick(id);
            }
            publication.retired();
        }
    }
    fn idle(&self, clocks: TickClocks) {
        if let Some(publication) = self.0.upgrade() {
            // Retirement already publishes the last empty transition. Idle is
            // a sampler lifecycle notification, not another publication.
            publication.publish_empty_once(clocks.unix_ms);
        }
    }
}
impl RunnerMemoryObserver for PublicationObserver {
    fn initially_empty(&self) {
        if let Some(publication) = self.0.upgrade() {
            publication.publish_empty_once(unix_ms());
        }
    }
    fn registration_failed(&self) {
        if let Some(publication) = self.0.upgrade() {
            publication.cache_gap(Measurement::unavailable_with(
                unix_ms(),
                MeasurementGap::SamplingFailed,
                "sampler thread did not start",
            ));
        }
    }
}
impl AgentMemorySampler {
    /// Cached only. Event delivery deliberately precedes replacement of this value.
    pub fn latest(&self) -> AgentMemoryReading {
        self.latest
            .lock()
            .map(|latest| latest.reading.clone())
            .unwrap_or_default()
    }
    pub fn start(self: &Arc<Self>, state: Arc<AgentProcessState>, emitter: Arc<dyn EventEmitter>) {
        let observer = Arc::new(PublicationObserver(Arc::downgrade(self)));
        let engine = ProcessTreeSampler::new(SAMPLE_INTERVAL, observer.clone());
        self.bind(state, emitter, engine, observer);
    }
    fn bind(
        self: &Arc<Self>,
        state: Arc<AgentProcessState>,
        emitter: Arc<dyn EventEmitter>,
        engine: Arc<ProcessTreeSampler>,
        observer: Arc<PublicationObserver>,
    ) {
        if self.state.set(Arc::downgrade(&state)).is_err() {
            return;
        }
        let _ = self.emitter.set(emitter);
        let _ = self.engine.set(engine.clone());
        if !cairn_procmem::platform::MEMORY_SUPPORTED {
            self.cache_gap(Measurement::unavailable(
                unix_ms(),
                MeasurementGap::UnsupportedPlatform,
            ));
            return;
        }
        state.start_memory_sampling(engine, observer);
    }
    fn tick(&self, batch: &TickBatch<'_>) {
        let Some(state) = self.state.get().and_then(Weak::upgrade) else {
            return;
        };
        state.cache_memory_tick(batch);
        if batch
            .watches
            .iter()
            .any(|w| matches!(w.status, SamplingStatus::FailedSampling))
        {
            // A failed census never re-stamps the last successful values.
            if self.latest().value().is_none() {
                self.publish(
                    Measurement::unavailable(batch.clocks.unix_ms, MeasurementGap::SamplingFailed),
                    HashMap::new(),
                );
            }
            return;
        }
        if batch
            .watches
            .iter()
            .any(|w| matches!(w.status, SamplingStatus::Unsupported))
        {
            self.publish(
                Measurement::unavailable(batch.clocks.unix_ms, MeasurementGap::UnsupportedPlatform),
                HashMap::new(),
            );
            return;
        }
        let mut anchors = HashMap::new();
        let mut rows = Vec::new();
        for (run_id, leader, id) in state.memory_bindings() {
            let Some(watch) = batch.watches.iter().find(|watch| watch.id == id) else {
                continue;
            };
            if let SamplingStatus::Measured(reading) = &watch.status {
                anchors.insert(run_id.clone(), (leader, id));
                rows.push(AgentResident {
                    run_id,
                    resident_bytes: reading.resident_bytes,
                    physical_footprint_bytes: reading.physical_footprint_bytes,
                    members: reading.members_total.min(u32::MAX as usize) as u32,
                    complete: !reading.incomplete,
                });
            }
        }
        rows.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        self.publish(Measurement::measured(batch.clocks.unix_ms, rows), anchors);
    }
    fn retired(&self) {
        let Some(state) = self.state.get().and_then(Weak::upgrade) else {
            return;
        };
        let current: HashMap<_, _> = state
            .memory_bindings()
            .into_iter()
            .map(|(run, anchor, id)| (run, (anchor, id)))
            .collect();
        let (mut reading, mut anchors) = {
            let latest = self.latest.lock().unwrap();
            (latest.reading.clone(), latest.anchors.clone())
        };
        let Some(rows) = reading.value().cloned() else {
            return;
        };
        let retained: Vec<_> = rows
            .iter()
            .filter(|row| {
                anchors
                    .get(&row.run_id)
                    .is_some_and(|anchor| current.get(&row.run_id) == Some(anchor))
            })
            .cloned()
            .collect();
        if retained.len() == rows.len() {
            return;
        }
        anchors.retain(|id, anchor| current.get(id) == Some(anchor));
        // Clearing stale rows is not a new successful census of surviving rows.
        reading = Measurement::measured(reading.measured_at_unix_ms, retained);
        if current.is_empty() {
            reading.measured_at_unix_ms = unix_ms();
        }
        self.publish(reading, anchors);
    }
    fn publish_empty_once(&self, time: u64) {
        if self.latest().value().is_some_and(|rows| rows.is_empty()) {
            return;
        }
        self.publish(Measurement::measured(time, Vec::new()), HashMap::new());
    }
    fn cache_gap(&self, reading: AgentMemoryReading) {
        if let Ok(mut latest) = self.latest.lock() {
            *latest = Published {
                reading,
                anchors: HashMap::new(),
            };
        }
    }
    fn publish(
        &self,
        reading: AgentMemoryReading,
        anchors: HashMap<String, (ProcessAnchor, WatchId)>,
    ) {
        if let Some(emitter) = self.emitter.get() {
            let _ = emitter.emit(
                AGENT_MEMORY_SAMPLE_EVENT,
                serde_json::to_value(&reading).unwrap_or(serde_json::Value::Null),
            );
        }
        if let Ok(mut latest) = self.latest.lock() {
            *latest = Published { reading, anchors };
        }
    }
}
fn unix_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::process::RunHandle;
    use crate::services::testing::CapturingEmitter;
    use cairn_procmem::{Census, CensusRow, KernelProcess, StartKey};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    const GOLDEN_TIME: u64 = 1_700_000_000_123;
    const EMPTY_GOLDEN: &str =
        r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"measured","value":[]}}"#;
    const PAIR_GOLDEN: &str = r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"measured","value":[{"runId":"run-a","residentBytes":123,"members":2,"complete":true}]}}"#;
    const LEADER_GOLDEN: &str = r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"measured","value":[{"runId":"run-a","residentBytes":100,"members":1,"complete":true}]}}"#;
    const SURVIVOR_GOLDEN: &str = r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"measured","value":[{"runId":"run-a","residentBytes":23,"members":1,"complete":true}]}}"#;

    fn anchor(pid: u32, start: u64) -> ProcessAnchor {
        ProcessAnchor {
            pid,
            start_token: StartKey(start, 0).token(),
        }
    }
    fn row(pid: u32, parent: u32, bytes: Option<u64>) -> CensusRow {
        CensusRow {
            process: KernelProcess {
                pid,
                parent_pid: parent,
                process_group_id: pid,
                session_id: None,
                start: StartKey(10, 0),
            },
            resident_bytes: bytes,
            physical_footprint_bytes: None,
            cpu_ms: Some(1),
            uninterruptible: false,
            storage_io_bytes: Some(1),
        }
    }
    fn register(state: &AgentProcessState, run: &str, pid: u32, start: u64) {
        state
            .register_process(
                run.into(),
                RunHandle::test_handle(Some(run), None).with_leader(anchor(pid, start)),
            )
            .unwrap();
    }
    struct Script {
        rows: Vec<CensusRow>,
        unreadable: Vec<u32>,
        incomplete: bool,
        success: bool,
        gate: Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>,
    }
    struct Rig {
        publication: Arc<AgentMemorySampler>,
        state: Arc<AgentProcessState>,
        engine: Arc<ProcessTreeSampler>,
        emitter: Arc<CapturingEmitter>,
        script: Arc<Mutex<Script>>,
        reads: Arc<AtomicUsize>,
        time: Arc<AtomicU64>,
    }
    impl Rig {
        fn new(rows: Vec<CensusRow>, preregister: bool) -> Self {
            Self::configured(rows, preregister, false, false)
        }
        fn configured(
            rows: Vec<CensusRow>,
            preregister: bool,
            automatic: bool,
            fail: bool,
        ) -> Self {
            let publication = Arc::new(AgentMemorySampler::default());
            let state = Arc::new(AgentProcessState::default());
            if preregister {
                register(&state, "run-a", 41, 10);
            }
            let observer = Arc::new(PublicationObserver(Arc::downgrade(&publication)));
            let script = Arc::new(Mutex::new(Script {
                rows,
                unreadable: Vec::new(),
                incomplete: false,
                success: true,
                gate: None,
            }));
            let source = script.clone();
            let reads = Arc::new(AtomicUsize::new(0));
            let counter = reads.clone();
            let time = Arc::new(AtomicU64::new(GOLDEN_TIME));
            let clock = time.clone();
            let engine = ProcessTreeSampler::with_source(
                Duration::from_secs(5),
                observer.clone(),
                Arc::new(move |out| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let script = source.lock().unwrap();
                    if let Some((entered, release)) = &script.gate {
                        entered.wait();
                        release.wait();
                    }
                    out.rows.extend(script.rows.iter().copied());
                    out.unreadable.extend(script.unreadable.iter().copied());
                    out.enumeration_incomplete = script.incomplete;
                    script.success
                }),
                Arc::new(move || TickClocks {
                    unix_ms: clock.load(Ordering::SeqCst),
                    monotonic_ms: clock.load(Ordering::SeqCst),
                }),
                automatic,
            );
            if fail {
                engine.fail_next_spawn();
            }
            let emitter = Arc::new(CapturingEmitter::new());
            publication.bind(state.clone(), emitter.clone(), engine.clone(), observer);
            Self {
                publication,
                state,
                engine,
                emitter,
                script,
                reads,
                time,
            }
        }
        fn tick(&self) {
            self.engine.sample_once(&mut Census::default());
        }
        fn golden(&self, golden: &str) {
            let reading = self.publication.latest();
            assert_eq!(serde_json::to_vec(&reading).unwrap(), golden.as_bytes());
            assert_eq!(
                self.emitter
                    .events_named(AGENT_MEMORY_SAMPLE_EVENT)
                    .last()
                    .unwrap(),
                &serde_json::from_str::<serde_json::Value>(golden).unwrap()
            );
        }
        fn rows(&self) -> Vec<AgentResident> {
            self.publication
                .latest()
                .value()
                .cloned()
                .unwrap_or_default()
        }
    }

    #[test]
    fn golden_runner_publication_from_shared_census() {
        for (rows, golden) in [
            (
                vec![row(41, 0, Some(100)), row(42, 41, Some(23))],
                PAIR_GOLDEN,
            ),
            (
                vec![row(41, 0, Some(100)), row(42, 41, None)],
                r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"measured","value":[{"runId":"run-a","residentBytes":100,"members":2,"complete":false}]}}"#,
            ),
            (
                vec![row(41, 0, Some(0))],
                r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"measured","value":[{"runId":"run-a","residentBytes":0,"members":1,"complete":true}]}}"#,
            ),
            (Vec::new(), EMPTY_GOLDEN),
            (
                {
                    let mut recycled = row(41, 0, Some(100));
                    recycled.process.start = StartKey(11, 0);
                    vec![recycled]
                },
                EMPTY_GOLDEN,
            ),
        ] {
            let rig = Rig::new(rows, true);
            assert_eq!(rig.reads.load(Ordering::SeqCst), 0);
            rig.tick();
            rig.golden(golden);
            let before = rig.reads.load(Ordering::SeqCst);
            for _ in 0..1000 {
                let _ = rig.publication.latest();
                let _ = rig.state.memory_diagnostic("run-a");
            }
            assert_eq!(rig.reads.load(Ordering::SeqCst), before);
        }
    }

    #[test]
    fn golden_footprint_rides_beside_rss_only_when_every_member_has_one() {
        let with = |mut row: CensusRow, footprint| {
            row.physical_footprint_bytes = footprint;
            row
        };
        let rig = Rig::new(
            vec![
                with(row(41, 0, Some(100)), Some(40)),
                with(row(42, 41, Some(23)), Some(5)),
            ],
            true,
        );
        rig.tick();
        rig.golden(
            r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"measured","value":[{"runId":"run-a","residentBytes":123,"physicalFootprintBytes":45,"members":2,"complete":true}]}}"#,
        );
        rig.script.lock().unwrap().rows[1].physical_footprint_bytes = None;
        rig.tick();
        rig.golden(PAIR_GOLDEN);
    }

    #[test]
    fn golden_runner_approved_membership_corrections_use_real_censuses() {
        // Old fresh-BFS outputs are wire fixtures, not another membership engine.
        for mechanism in ["reparent", "group", "session"] {
            let child_parent = if mechanism == "reparent" { 41 } else { 0 };
            let mut child = row(42, child_parent, Some(23));
            if mechanism == "group" {
                child.process.process_group_id = 41;
            }
            if mechanism == "session" {
                child.process.session_id = Some(41);
            }
            let mut leader = row(41, 0, Some(100));
            if mechanism == "session" {
                leader.process.session_id = Some(41);
            }
            let rig = Rig::new(vec![leader, child], true);
            rig.tick();
            rig.golden(PAIR_GOLDEN);
            rig.script.lock().unwrap().rows[1].process.parent_pid = 0;
            rig.tick();
            rig.golden(PAIR_GOLDEN);
            assert_ne!(PAIR_GOLDEN, LEADER_GOLDEN);
            rig.script.lock().unwrap().rows.remove(0);
            rig.tick();
            rig.golden(SURVIVOR_GOLDEN);
            assert_ne!(SURVIVOR_GOLDEN, EMPTY_GOLDEN);
            rig.script.lock().unwrap().rows[0].process.start = StartKey(11, 0);
            rig.tick();
            rig.golden(EMPTY_GOLDEN);
        }
    }

    #[test]
    fn incomplete_rss_identity_and_enumeration_recover_without_losing_members() {
        let rig = Rig::new(vec![row(41, 0, Some(100)), row(42, 41, Some(23))], true);
        rig.tick();
        for gap in ["rss", "identity", "enumeration", "cpu", "storage"] {
            {
                let mut script = rig.script.lock().unwrap();
                script.rows = vec![row(41, 0, Some(100)), row(42, 0, Some(23))];
                script.unreadable.clear();
                script.incomplete = false;
                match gap {
                    "rss" => script.rows[1].resident_bytes = None,
                    "identity" => {
                        script.rows.pop();
                        script.unreadable.push(42);
                    }
                    "enumeration" => {
                        script.rows.pop();
                        script.incomplete = true;
                    }
                    "cpu" => script.rows[1].cpu_ms = None,
                    "storage" => script.rows[1].storage_io_bytes = None,
                    _ => unreachable!(),
                }
            }
            rig.tick();
            let reading = rig.rows().pop().unwrap();
            assert_eq!(reading.members, 2, "{gap}");
            assert_eq!(reading.complete, matches!(gap, "cpu" | "storage"), "{gap}");
            let (_, evidence) = rig.state.memory_diagnostic("run-a").unwrap();
            let evidence = evidence.unwrap();
            assert_eq!(evidence.members.len(), 2);
            assert_eq!(evidence.incomplete, !reading.complete);
        }
        rig.script.lock().unwrap().rows = vec![row(41, 0, Some(100)), row(42, 0, Some(23))];
        rig.tick();
        rig.golden(PAIR_GOLDEN);
    }

    #[test]
    fn failed_census_ages_instead_of_restamping_and_then_recovers() {
        let rig = Rig::new(vec![row(41, 0, Some(100))], true);
        rig.tick();
        rig.golden(LEADER_GOLDEN);
        rig.time.store(GOLDEN_TIME + 100, Ordering::SeqCst);
        rig.script.lock().unwrap().success = false;
        let count = rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len();
        rig.tick();
        rig.golden(LEADER_GOLDEN);
        assert_eq!(
            rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len(),
            count
        );
        assert_eq!(
            rig.state
                .memory_diagnostic("run-a")
                .unwrap()
                .1
                .unwrap()
                .tick_unix_ms,
            GOLDEN_TIME
        );
        rig.script.lock().unwrap().success = true;
        rig.tick();
        assert_eq!(
            rig.publication.latest().measured_at_unix_ms,
            GOLDEN_TIME + 100
        );
    }

    #[test]
    fn registry_removal_replacement_displacement_and_warm_reuse_reconcile() {
        let rig = Rig::new(vec![row(41, 0, Some(100)), row(42, 41, Some(23))], true);
        rig.tick();
        rig.state.transition_to_warm("run-a");
        rig.script.lock().unwrap().rows[1].process.parent_pid = 0;
        rig.tick();
        rig.golden(PAIR_GOLDEN); // warm transition retained the sticky watch
        register(&rig.state, "run-a", 43, 10);
        let reads = rig.reads.load(Ordering::SeqCst);
        rig.engine.flush_retirements_once();
        assert_eq!(rig.reads.load(Ordering::SeqCst), reads);
        assert!(
            rig.rows().is_empty(),
            "retirement callback removed the old anchor"
        );
        assert!(rig.state.memory_diagnostic("run-a").unwrap().1.is_none());
        rig.script.lock().unwrap().rows = vec![row(43, 0, Some(7))];
        rig.tick();
        assert_eq!(rig.rows()[0].resident_bytes, 7);
        rig.state
            .register_process(
                "run-b".into(),
                RunHandle::test_handle(Some("run-a"), None).with_leader(anchor(44, 10)),
            )
            .unwrap();
        assert!(
            rig.state.memory_diagnostic("run-a").is_none(),
            "session displacement removed old run"
        );
        rig.engine.flush_retirements_once();
        assert_eq!(rig.reads.load(Ordering::SeqCst), reads + 1);
        assert!(rig.rows().is_empty());
        rig.script.lock().unwrap().rows = vec![row(44, 0, Some(8))];
        rig.tick();
        assert_eq!(rig.rows()[0].run_id, "run-b");
        rig.state.processes.lock().unwrap().remove("run-b");
        rig.engine.flush_retirements_once();
        assert_eq!(rig.reads.load(Ordering::SeqCst), reads + 2);
        assert!(
            rig.rows().is_empty(),
            "direct registry removal retired the watch"
        );
        let count = rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len();
        rig.tick();
        assert_eq!(
            rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len(),
            count
        );
    }

    #[test]
    fn overlapping_watches_share_one_census_and_idle_publishes_once() {
        let rig = Rig::new(vec![row(41, 0, Some(100)), row(42, 41, Some(23))], false);
        assert_eq!(rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len(), 1);
        assert_eq!(rig.reads.load(Ordering::SeqCst), 0);
        register(&rig.state, "run-a", 41, 10);
        register(&rig.state, "run-b", 42, 10);
        rig.tick();
        assert_eq!(rig.reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            rig.rows()
                .iter()
                .map(|r| r.resident_bytes)
                .collect::<Vec<_>>(),
            vec![123, 23]
        );
        rig.state.processes.lock().unwrap().remove("run-a");
        rig.engine.flush_retirements_once();
        assert_eq!(rig.reads.load(Ordering::SeqCst), 1);
        assert_eq!(rig.rows()[0].run_id, "run-b");
        rig.state.processes.lock().unwrap().remove("run-b");
        rig.engine.flush_retirements_once();
        assert_eq!(rig.reads.load(Ordering::SeqCst), 1);
        assert!(rig.rows().is_empty());
        let count = rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len();
        for _ in 0..10 {
            rig.tick();
        }
        assert_eq!(
            rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len(),
            count
        );
    }

    #[test]
    fn golden_gaps_and_injected_spawn_failure() {
        let publication = Arc::new(AgentMemorySampler::default());
        assert_eq!(
            serde_json::to_string(&publication.latest()).unwrap(),
            r#"{"measuredAtUnixMs":0,"reading":{"kind":"unavailable","reason":"notSampled"}}"#
        );
        let rig = Rig::configured(Vec::new(), true, true, true);
        assert!(rig
            .emitter
            .events_named(AGENT_MEMORY_SAMPLE_EVENT)
            .is_empty());
        let mut failure = rig.publication.latest();
        failure.measured_at_unix_ms = GOLDEN_TIME;
        assert_eq!(
            serde_json::to_string(&failure).unwrap(),
            r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"unavailable","reason":"samplingFailed","detail":"sampler thread did not start"}}"#
        );
        let unsupported: AgentMemoryReading =
            Measurement::unavailable(GOLDEN_TIME, MeasurementGap::UnsupportedPlatform);
        assert_eq!(
            serde_json::to_string(&unsupported).unwrap(),
            r#"{"measuredAtUnixMs":1700000000123,"reading":{"kind":"unavailable","reason":"unsupportedPlatform"}}"#
        );
    }

    #[test]
    fn adapters_share_the_authoritative_parity_census_sequence() {
        use cairn_procmem::test_support::parity_census_steps;
        let steps = parity_census_steps();
        let current = Arc::new(Mutex::new(steps[0].clone()));
        let publication = Arc::new(AgentMemorySampler::default());
        let state = Arc::new(AgentProcessState::default());
        register(&state, "run-a", 10, 1);
        register(&state, "run-b", 11, 2);
        let observer = Arc::new(PublicationObserver(Arc::downgrade(&publication)));
        let source = current.clone();
        let clock = current.clone();
        let engine = ProcessTreeSampler::with_source(
            Duration::from_secs(5),
            observer.clone(),
            Arc::new(move |census| {
                assert!(
                    census.storage_wanted.is_empty(),
                    "runner watches do not request storage"
                );
                source.lock().unwrap().census(census)
            }),
            Arc::new(move || clock.lock().unwrap().clocks),
            false,
        );
        let emitter = Arc::new(CapturingEmitter::new());
        publication.bind(state.clone(), emitter.clone(), engine.clone(), observer);
        let mut last_time = 0;
        for step in steps {
            *current.lock().unwrap() = step.clone();
            let before = emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len();
            engine.sample_once(&mut Census::default());
            let reading = publication.latest();
            let (_, cached) = state.memory_diagnostic("run-a").unwrap();
            match step.root_memory {
                Some((rss, read, total, incomplete)) => {
                    let cached = cached.expect(step.name);
                    let expected: Vec<_> = step
                        .root_members
                        .iter()
                        .map(|pid| {
                            anchor(
                                *pid,
                                match pid {
                                    10 => 1,
                                    11 => 2,
                                    12 => 3,
                                    13 => 4,
                                    _ => panic!("unexpected fixture identity"),
                                },
                            )
                        })
                        .collect();
                    assert_eq!(cached.members, expected, "{}", step.name);
                    assert_eq!(
                        (
                            cached.resident_bytes,
                            cached.members_read as usize,
                            cached.members_total as usize,
                            cached.incomplete
                        ),
                        (rss, read, total, incomplete),
                        "{}",
                        step.name
                    );
                    let root = reading
                        .value()
                        .unwrap()
                        .iter()
                        .find(|r| r.run_id == "run-a")
                        .unwrap();
                    assert_eq!(
                        (root.resident_bytes, root.members as usize, root.complete),
                        (rss, total, !incomplete),
                        "{}",
                        step.name
                    );
                }
                None => {
                    assert!(cached.is_none(), "{}", step.name);
                    assert!(reading.value().unwrap().iter().all(|r| r.run_id != "run-a"));
                }
            }
            if step.success {
                last_time = step.clocks.unix_ms;
            } else {
                assert_eq!(
                    emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len(),
                    before
                );
            }
            assert_eq!(reading.measured_at_unix_ms, last_time, "{}", step.name);
        }
    }

    #[test]
    fn initially_failed_or_unverified_census_is_an_honest_gap() {
        let rig = Rig::new(Vec::new(), true);
        rig.script.lock().unwrap().success = false;
        rig.tick();
        assert_eq!(
            rig.publication.latest().gap(),
            Some(MeasurementGap::SamplingFailed)
        );
        assert!(rig.state.memory_diagnostic("run-a").unwrap().1.is_none());
        rig.script.lock().unwrap().success = true;
        rig.script.lock().unwrap().unreadable.push(41);
        rig.tick();
        rig.golden(EMPTY_GOLDEN);
        rig.script.lock().unwrap().unreadable.clear();
        rig.script.lock().unwrap().rows.push(row(41, 0, Some(100)));
        rig.tick();
        rig.golden(LEADER_GOLDEN);
    }

    #[test]
    fn removal_or_replacement_fences_an_inflight_census_without_registry_deadlock() {
        for replacement in [None, Some(43), Some(41)] {
            let rig = Rig::new(vec![row(41, 0, Some(100))], true);
            rig.tick();
            let published = rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT).len();
            let entered = Arc::new(std::sync::Barrier::new(2));
            let release = Arc::new(std::sync::Barrier::new(2));
            rig.script.lock().unwrap().gate = Some((entered.clone(), release.clone()));
            let engine = rig.engine.clone();
            let tick = std::thread::spawn(move || engine.sample_once(&mut Census::default()));
            entered.wait();
            let state = rig.state.clone();
            let (mutated, observed) = std::sync::mpsc::channel();
            let retire = std::thread::spawn(move || {
                let mut registry = state.processes.lock().unwrap();
                registry.remove("run-a");
                if let Some(pid) = replacement {
                    registry.register(
                        "run-a".into(),
                        RunHandle::test_handle(Some("run-a"), None).with_leader(anchor(pid, 10)),
                    );
                }
                drop(registry);
                mutated.send(()).unwrap();
            });
            let mutation_completed = observed.recv_timeout(Duration::from_secs(5));
            release.wait();
            tick.join().unwrap();
            retire.join().unwrap();
            mutation_completed
                .expect("registry mutation must finish before the census is released");
            rig.engine.flush_retirements_once();
            assert_eq!(rig.reads.load(Ordering::SeqCst), 2);
            let events = rig.emitter.events_named(AGENT_MEMORY_SAMPLE_EVENT);
            assert!(events.len() > published);
            for event in &events[published..] {
                let reading: AgentMemoryReading = serde_json::from_value(event.clone()).unwrap();
                assert!(
                    reading.value().is_some_and(|rows| rows.is_empty()),
                    "in-flight delivery and retirement must never publish the old binding"
                );
            }
            assert!(
                rig.rows().is_empty(),
                "retired tick cannot republish the old binding"
            );
            if replacement.is_some() {
                assert!(rig.state.memory_diagnostic("run-a").unwrap().1.is_none());
            } else {
                assert!(rig.state.memory_diagnostic("run-a").is_none());
            }
        }
    }

    #[test]
    fn identical_anchors_keep_independent_sticky_histories_and_reinsert_generation() {
        let rig = Rig::new(vec![row(41, 0, Some(100)), row(42, 41, Some(23))], true);
        rig.tick();
        rig.script.lock().unwrap().rows[1].process.parent_pid = 0;
        register(&rig.state, "run-b", 41, 10);
        rig.tick();
        assert_eq!(
            rig.rows()
                .iter()
                .map(|r| r.resident_bytes)
                .collect::<Vec<_>>(),
            vec![123, 100]
        );
        assert_eq!(
            rig.state
                .memory_diagnostic("run-a")
                .unwrap()
                .1
                .unwrap()
                .members
                .len(),
            2
        );
        assert_eq!(
            rig.state
                .memory_diagnostic("run-b")
                .unwrap()
                .1
                .unwrap()
                .members
                .len(),
            1
        );
        register(&rig.state, "run-a", 41, 10);
        rig.engine.flush_retirements_once();
        assert_eq!(rig.reads.load(Ordering::SeqCst), 2);
        assert!(rig.state.memory_diagnostic("run-a").unwrap().1.is_none());
        assert!(rig.rows().iter().all(|r| r.run_id != "run-a"));
        rig.tick();
        assert_eq!(
            rig.rows()
                .iter()
                .map(|r| r.resident_bytes)
                .collect::<Vec<_>>(),
            vec![100, 100]
        );
    }

    #[test]
    fn empty_startup_reconciliation_publishes_once_without_locks_or_a_census() {
        struct StartupEmitter {
            state: Arc<AgentProcessState>,
            events: Mutex<usize>,
        }
        impl EventEmitter for StartupEmitter {
            fn emit(&self, _: &str, _: serde_json::Value) -> Result<(), String> {
                assert!(self.state.memory_diagnostic("run-a").is_none());
                assert!(self.state.memory_bindings().is_empty());
                *self.events.lock().unwrap() += 1;
                Ok(())
            }
            fn emit_empty(&self, event: &str) -> Result<(), String> {
                self.emit(event, serde_json::Value::Null)
            }
        }
        let publication = Arc::new(AgentMemorySampler::default());
        let state = Arc::new(AgentProcessState::default());
        register(&state, "run-a", 41, 10);
        state.processes.lock().unwrap().remove("run-a");
        let observer = Arc::new(PublicationObserver(Arc::downgrade(&publication)));
        let engine = ProcessTreeSampler::with_source(
            Duration::from_secs(5),
            observer.clone(),
            Arc::new(|_| panic!("empty reconciliation must not sample")),
            Arc::new(|| TickClocks {
                monotonic_ms: 0,
                unix_ms: GOLDEN_TIME,
            }),
            false,
        );
        let emitter = Arc::new(StartupEmitter {
            state: state.clone(),
            events: Mutex::new(0),
        });
        publication.bind(state.clone(), emitter.clone(), engine, observer);
        assert!(publication.latest().value().unwrap().is_empty());
        for _ in 0..10 {
            state.transition_to_warm("absent");
        }
        assert_eq!(*emitter.events.lock().unwrap(), 1);
    }

    #[test]
    fn event_is_delivered_before_cache_replacement() {
        struct OrderingEmitter {
            publication: Weak<AgentMemorySampler>,
            seen: Mutex<Vec<AgentMemoryReading>>,
        }
        impl EventEmitter for OrderingEmitter {
            fn emit(&self, _: &str, _: serde_json::Value) -> Result<(), String> {
                self.seen
                    .lock()
                    .unwrap()
                    .push(self.publication.upgrade().unwrap().latest());
                Ok(())
            }
            fn emit_empty(&self, event: &str) -> Result<(), String> {
                self.emit(event, serde_json::Value::Null)
            }
        }
        let publication = Arc::new(AgentMemorySampler::default());
        let emitter = Arc::new(OrderingEmitter {
            publication: Arc::downgrade(&publication),
            seen: Mutex::new(Vec::new()),
        });
        publication.emitter.set(emitter.clone()).ok().unwrap();
        publication.publish(
            Measurement::measured(GOLDEN_TIME, Vec::new()),
            HashMap::new(),
        );
        assert_eq!(
            emitter.seen.lock().unwrap()[0].gap(),
            Some(MeasurementGap::NotSampled)
        );
        assert_eq!(publication.latest().value(), Some(&Vec::new()));
    }
}
