//! The coarse fleet frame a live surface subscribes to instead of polling.
//!
//! `cairn://executors` answers "what is this machine doing" in full, and a UI
//! that wants a moving gauge used to get there by re-reading it on a timer. That
//! is a poll: it costs a full projection per tick whether or not anything moved,
//! and it still shows a machine that changed state a tick ago. This module is the
//! other direction — the runner already knows when fleet state moved, so it says
//! so, carrying just enough for a gauge.
//!
//! ## One derivation, two renderings
//!
//! A frame is built from [`ExecutorInspection`] values, the same projection the
//! resource renders, rather than from a second pass over the connection table.
//! The push and the read are then two renderings of one fact by construction: a
//! subscriber cannot be shown a machine as busy that the resource calls idle,
//! and a field added to one arrives at the other.
//!
//! ## Absent readings stay absent
//!
//! `cpu_percent` and `mem_percent` are `Option` and are omitted from the wire
//! when there is no reading. This is the same rule the resource states outright:
//! a gap is never printed as a zero, because "this platform cannot answer" and
//! "this machine is idle" are opposite facts and a gauge that renders the first
//! as the second is lying at a glance. A subscriber that wants to know *why* a
//! reading is missing reads the resource, which names the gap.

use std::sync::Mutex;

use cairn_common::executor_protocol::{ExecutorHealthStatus, ExecutorInspection};
use serde::{Deserialize, Serialize};

/// One machine, as a live gauge needs it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorTelemetryRow {
    /// The public address — exactly what a placement request accepts, so what a
    /// surface displays is what a user could target.
    pub name: String,
    /// Operating system as advertised (`macos`, `linux`, `windows`).
    pub platform: String,
    /// Link health, from heartbeat age alone. A machine whose telemetry has aged
    /// but whose beats arrive on time is online; conflating the two reports a
    /// healthy machine as a dead one.
    pub online: bool,
    /// Batches executing on this machine right now.
    pub running: usize,
    /// Batches admitted to its queue and not yet started.
    pub queued: usize,
    /// Non-idle share of processor time, 0-100. Absent when unmeasured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_percent: Option<f64>,
    /// Share of physical memory in use, 0-100. Absent when unmeasured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_percent: Option<f64>,
}

/// Every attached machine at one instant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FleetTelemetryFrame {
    /// The single instant every row was read at. One stamp for the frame, not
    /// one per row: these were taken under a single lock and describe the same
    /// moment.
    pub ts: u64,
    pub executors: Vec<ExecutorTelemetryRow>,
}

/// Round a `[0, 1]` share to a whole-tenth percentage.
///
/// A gauge cannot render more precision than this, and full float noise would
/// make every frame differ from the last for no visible reason.
fn percent(share: f64) -> f64 {
    (share.clamp(0.0, 1.0) * 1_000.0).round() / 10.0
}

impl FleetTelemetryFrame {
    /// Project inspections into a frame. Pure: the same inspections always yield
    /// the same frame.
    pub fn from_inspections(inspections: &[ExecutorInspection], ts: u64) -> Self {
        Self {
            ts,
            executors: inspections
                .iter()
                .map(|executor| {
                    let machine = &executor.health.machine;
                    ExecutorTelemetryRow {
                        name: executor.name.clone(),
                        platform: executor.health.advertisement.capabilities.os.clone(),
                        online: matches!(executor.health.status, ExecutorHealthStatus::Online),
                        running: executor.occupancy.executing_requests.len(),
                        queued: executor.occupancy.queued_requests.len(),
                        cpu_percent: machine.cpu.value().map(|cpu| percent(cpu.utilization)),
                        mem_percent: machine.memory.value().and_then(|memory| {
                            // A machine reporting zero total bytes has not
                            // measured memory, whatever the reading claims;
                            // dividing by it would print a gap as a number.
                            (memory.total_bytes > 0).then(|| {
                                percent(memory.used_bytes() as f64 / memory.total_bytes as f64)
                            })
                        }),
                    }
                })
                .collect(),
        }
    }
}

/// The minimum gap between two telemetry frames.
pub const TELEMETRY_INTERVAL_MS: u64 = 5_000;

/// What a caller should do with a fleet change that just landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryEmit {
    /// Build and send a frame now.
    Now,
    /// Too soon. Produce one after this delay so the change is not lost, via
    /// [`TelemetryCadence::capture_trailing`], which reopens the cadence before
    /// the frame is captured.
    After { delay_ms: u64 },
    /// Too soon, and a trailing frame is already scheduled to carry this change.
    Skip,
}

/// Rate-limits fleet frames to one per [`TELEMETRY_INTERVAL_MS`] without
/// dropping the last change of a burst.
///
/// Leading-edge-only throttling would be a bug here, and a known one: the
/// substrate-health lane above this had exactly that shape and left the UI stale
/// until the next 30-second heartbeat, because the final change of a burst is
/// precisely the one that describes the state the fleet came to rest in. So a
/// suppressed change schedules a trailing frame instead of vanishing, and the
/// frame a subscriber sees last is always the fleet's current state.
///
/// Unlike that lane, the bound belongs here rather than on the client: this
/// event carries its own payload, so an unthrottled burst would be bandwidth
/// spent on frames no one can perceive, not a refetch a client could coalesce.
pub struct TelemetryCadence {
    interval_ms: u64,
    state: Mutex<CadenceState>,
}

#[derive(Default)]
struct CadenceState {
    last_sent_unix_ms: Option<u64>,
    trailing_scheduled: bool,
}

impl Default for TelemetryCadence {
    fn default() -> Self {
        Self::new(TELEMETRY_INTERVAL_MS)
    }
}

impl TelemetryCadence {
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            state: Mutex::new(CadenceState::default()),
        }
    }

    /// Admit one fleet change and decide when its frame goes out.
    pub fn admit(&self, now_unix_ms: u64) -> TelemetryEmit {
        let mut state = self.state.lock().unwrap();
        if state.trailing_scheduled {
            return TelemetryEmit::Skip;
        }
        let elapsed = state
            .last_sent_unix_ms
            .map(|last| now_unix_ms.saturating_sub(last));
        match elapsed {
            Some(elapsed) if elapsed < self.interval_ms => {
                state.trailing_scheduled = true;
                TelemetryEmit::After {
                    delay_ms: self.interval_ms - elapsed,
                }
            }
            _ => {
                state.last_sent_unix_ms = Some(now_unix_ms);
                TelemetryEmit::Now
            }
        }
    }

    /// Produce a deferred trailing frame, reopening the cadence **before**
    /// `capture` reads the fleet.
    ///
    /// The ordering is the entire reason this is one function taking a closure
    /// rather than a "send, then mark sent" pair at the call site. A frame is a
    /// snapshot of fleet state at the instant it is captured, so a change
    /// landing after that instant is not in it. If the cadence were still
    /// closed at that moment, `admit` would answer [`TelemetryEmit::Skip`] on
    /// the grounds that a trailing frame was already scheduled to carry the
    /// change — but that frame had already been taken without it. Clearing the
    /// flag afterwards would then record the stale frame as current, and
    /// subscribers would stay wrong until some unrelated later change happened
    /// to move the fleet again.
    ///
    /// Reopening first inverts that: a change concurrent with the capture is
    /// admitted and schedules the next trailing frame. The cost is at worst one
    /// redundant frame carrying a change the previous one already had, which is
    /// the correct direction to be wrong in.
    ///
    /// This is the same invariant [`Self::admit`] already keeps on the immediate
    /// path, where `last_sent_unix_ms` advances before the caller captures:
    /// **the cadence advances before the snapshot is taken, never after.**
    pub fn capture_trailing<T>(&self, now_unix_ms: u64, capture: impl FnOnce() -> T) -> T {
        {
            let mut state = self.state.lock().unwrap();
            state.last_sent_unix_ms = Some(now_unix_ms);
            state.trailing_scheduled = false;
        }
        capture()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{
        CpuPressure, ExecutorAdvertisement, ExecutorCapabilities, ExecutorHealthSnapshot,
        ExecutorIdentity, ExecutorSubstrateReport, FleetSnapshot, MachineMemory, MachineTelemetry,
        Measurement, MeasurementGap,
    };

    fn inspection(name: &str, machine: MachineTelemetry) -> ExecutorInspection {
        let identity = ExecutorIdentity {
            executor_id: name.to_string(),
            device_id: format!("{name}-device"),
            display_name: name.to_string(),
        };
        let report = ExecutorSubstrateReport::default();
        ExecutorInspection {
            name: name.to_string(),
            recent_placements: Vec::new(),
            colocated: false,
            health: ExecutorHealthSnapshot {
                identity: identity.clone(),
                public_name: name.to_string(),
                colocated: false,
                status: ExecutorHealthStatus::Online,
                heartbeat_age_ms: 0,
                liveness_age_ms: None,
                telemetry_stale: false,
                advertisement: ExecutorAdvertisement {
                    identity,
                    capabilities: ExecutorCapabilities {
                        os: "macos".into(),
                        arch: "aarch64".into(),
                        logical_cores: 8,
                        concurrency_capacity: None,
                        toolchains: Vec::new(),
                        projects_served: Vec::new(),
                        disk_budget_bytes: None,
                        memory_budget_bytes: None,
                        sandbox: None,
                        toolchain_detection: None,
                    },
                    current_load: 0,
                    warm_roots: Vec::new(),
                    observed_at_unix_ms: 0,
                    liveness_observed_at_unix_ms: None,
                },
                admission: report.admission,
                queues: Vec::new(),
                host: report.host,
                disk: report.disk,
                machine,
                inventory: report.inventory,
                connection_generation: 1,
                applied_policy: report.applied_policy,
                drain_mode: false,
                resident_processes: Default::default(),
                command_processes: Default::default(),
                build_skew: None,
            },
            executor_build_id: None,
            occupancy: FleetSnapshot::default(),
            captured_at_unix_ms: 0,
            connection_timeline: Vec::new(),
        }
    }

    /// The rule the whole module exists to honor: a machine that has not
    /// answered has no number, and must never be rendered as a machine
    /// answering zero.
    #[test]
    fn unmeasured_readings_are_absent_rather_than_zero() {
        let machine = MachineTelemetry {
            cpu: Measurement::unavailable(0, MeasurementGap::UnsupportedPlatform),
            memory: Measurement::unavailable(0, MeasurementGap::NotSampled),
            ..Default::default()
        };
        let frame = FleetTelemetryFrame::from_inspections(&[inspection("quiet", machine)], 42);
        let row = &frame.executors[0];
        assert_eq!(row.cpu_percent, None);
        assert_eq!(row.mem_percent, None);

        let wire = serde_json::to_value(&frame).unwrap();
        let executor = &wire["executors"][0];
        assert!(executor.get("cpuPercent").is_none());
        assert!(executor.get("memPercent").is_none());
        assert_eq!(executor["name"], "quiet");
        assert_eq!(executor["platform"], "macos");
        assert_eq!(executor["online"], true);
        assert_eq!(wire["ts"], 42);
    }

    #[test]
    fn measured_readings_become_whole_tenth_percentages() {
        let machine = MachineTelemetry {
            cpu: Measurement::measured(
                10,
                CpuPressure {
                    utilization: 0.4567,
                    user: 0.3,
                    system: 0.1567,
                    logical_cores: 8,
                },
            ),
            memory: Measurement::measured(
                10,
                MachineMemory {
                    total_bytes: 1_000,
                    available_bytes: 250,
                },
            ),
            ..Default::default()
        };
        let frame = FleetTelemetryFrame::from_inspections(&[inspection("busy", machine)], 1);
        let row = &frame.executors[0];
        assert_eq!(row.cpu_percent, Some(45.7));
        assert_eq!(row.mem_percent, Some(75.0));
    }

    /// A machine claiming zero total memory has not measured memory. Dividing by
    /// it would print a gap as a number, which is the one thing this frame
    /// promises not to do.
    #[test]
    fn zero_total_memory_is_a_gap_rather_than_a_ratio() {
        let machine = MachineTelemetry {
            memory: Measurement::measured(
                10,
                MachineMemory {
                    total_bytes: 0,
                    available_bytes: 0,
                },
            ),
            ..Default::default()
        };
        let frame = FleetTelemetryFrame::from_inspections(&[inspection("odd", machine)], 1);
        assert_eq!(frame.executors[0].mem_percent, None);
    }

    #[test]
    fn first_change_emits_immediately() {
        let cadence = TelemetryCadence::new(5_000);
        assert_eq!(cadence.admit(1_000), TelemetryEmit::Now);
    }

    /// The property that makes this a throttle rather than a sampler: a change
    /// inside the quiet window is deferred, never dropped, so the fleet's
    /// resting state always reaches the subscriber.
    #[test]
    fn a_change_inside_the_window_is_deferred_not_dropped() {
        let cadence = TelemetryCadence::new(5_000);
        assert_eq!(cadence.admit(1_000), TelemetryEmit::Now);
        assert_eq!(
            cadence.admit(2_000),
            TelemetryEmit::After { delay_ms: 4_000 }
        );
        // Further changes ride the already-scheduled trailing frame rather than
        // scheduling their own; otherwise a burst of N changes would queue N
        // timers and defeat the bound entirely.
        assert_eq!(cadence.admit(2_100), TelemetryEmit::Skip);
        assert_eq!(cadence.admit(3_000), TelemetryEmit::Skip);

        cadence.capture_trailing(6_000, || ());
        assert_eq!(
            cadence.admit(6_100),
            TelemetryEmit::After { delay_ms: 4_900 }
        );
    }

    /// The interleaving the deferred-not-dropped guarantee actually has to
    /// survive, and the one a "send, then mark sent" call site loses.
    ///
    /// A frame is a snapshot taken at one instant. A change landing after that
    /// instant is not in it, so it needs a frame of its own. If the cadence were
    /// still closed while the frame was being captured, this change would be
    /// answered `Skip` — deferred to a frame that had already been taken without
    /// it — and subscribers would hold stale state indefinitely.
    #[test]
    fn a_change_arriving_while_the_trailing_frame_is_captured_schedules_another() {
        let cadence = TelemetryCadence::new(5_000);
        assert_eq!(cadence.admit(1_000), TelemetryEmit::Now);
        assert_eq!(
            cadence.admit(2_000),
            TelemetryEmit::After { delay_ms: 4_000 }
        );

        // The timer fires at 6_000 and a fleet change lands mid-capture.
        let admitted_during_capture = cadence.capture_trailing(6_000, || cadence.admit(6_000));

        assert_eq!(
            admitted_during_capture,
            TelemetryEmit::After { delay_ms: 5_000 },
            "a change concurrent with the capture must schedule its own frame, \
             not be deferred to the frame already being taken without it"
        );
    }

    /// The same invariant on the immediate path: `admit` advances the clock
    /// before returning `Now`, so a change concurrent with that capture is
    /// deferred to a real future frame rather than skipped.
    #[test]
    fn a_change_arriving_while_an_immediate_frame_is_captured_is_deferred() {
        let cadence = TelemetryCadence::new(5_000);
        assert_eq!(cadence.admit(1_000), TelemetryEmit::Now);
        assert_eq!(
            cadence.admit(1_000),
            TelemetryEmit::After { delay_ms: 5_000 }
        );
    }

    #[test]
    fn a_change_after_the_window_emits_immediately() {
        let cadence = TelemetryCadence::new(5_000);
        assert_eq!(cadence.admit(1_000), TelemetryEmit::Now);
        assert_eq!(cadence.admit(6_000), TelemetryEmit::Now);
        assert_eq!(cadence.admit(20_000), TelemetryEmit::Now);
    }
}
