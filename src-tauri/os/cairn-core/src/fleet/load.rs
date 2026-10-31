//! The fleet's recent load: each machine's readings on the sample cadence, and
//! what each running command and resident process is using now.
//!
//! Fed only by [`MachineSample`]s, which an executor publishes from its CPU
//! sampler without touching pool state (cairn/5600), so the history keeps
//! moving while a machine's refresh pass is stuck. The ring belongs to the
//! runner and is keyed by executor identity rather than by link, so a reconnect
//! does not restart a machine's graph.

use std::collections::{BTreeMap, HashMap, VecDeque};

use cairn_common::executor_protocol::{
    FleetSnapshot, LiveUsage, LiveUsageSample, LiveUsageSubject, MachineSample, Measurement,
    MeasurementGap, MeasurementReading,
};
use serde::Serialize;

/// Points kept per machine: ten minutes at the five-second sample.
pub const FLEET_LOAD_CAPACITY: usize = 120;

/// One machine reading, as fractions of the whole. `None` on an axis is a
/// reading the machine did not have at that instant, which a graph draws as a
/// break rather than a drop to zero.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetLoadPoint {
    /// When the readings were measured, not when the frame carrying them
    /// arrived.
    pub at_unix_ms: u64,
    pub cpu: Option<f64>,
    pub memory: Option<f64>,
    /// Linux PSI `full` over sixty seconds, as a fraction of wall time.
    pub memory_stall: Option<f64>,
    pub io_stall: Option<f64>,
}

fn measured<T>(measurement: &Measurement<T>) -> Option<&T> {
    match &measurement.reading {
        MeasurementReading::Measured { value } => Some(value),
        MeasurementReading::Unavailable { .. } => None,
    }
}

fn fraction(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.clamp(0.0, 1.0))
}

impl FleetLoadPoint {
    /// The point `sample` describes, or `None` for a sample that has measured
    /// neither CPU nor memory yet: a stamp of zero is the never-sampled
    /// default, not an instant.
    pub fn of_sample(sample: &MachineSample) -> Option<Self> {
        let at_unix_ms = sample
            .cpu
            .measured_at_unix_ms
            .max(sample.memory.measured_at_unix_ms);
        if at_unix_ms == 0 {
            return None;
        }
        Some(Self {
            at_unix_ms,
            cpu: measured(&sample.cpu).and_then(|cpu| fraction(cpu.utilization)),
            memory: measured(&sample.memory)
                .filter(|memory| memory.total_bytes > 0)
                .and_then(|memory| {
                    fraction(memory.used_bytes() as f64 / memory.total_bytes as f64)
                }),
            memory_stall: measured(&sample.memory_pressure)
                .and_then(|stall| fraction(f64::from(stall.full_avg60_millis) / 1_000.0)),
            io_stall: measured(&sample.io_pressure)
                .and_then(|stall| fraction(f64::from(stall.full_avg60_millis) / 1_000.0)),
        })
    }
}

/// One machine's recent points, oldest first, never more than
/// [`FLEET_LOAD_CAPACITY`].
#[derive(Debug, Clone, Default)]
pub struct LoadRing {
    points: VecDeque<FleetLoadPoint>,
}

impl LoadRing {
    /// Append `point` unless it is no newer than the last one, and say whether
    /// it was. A repeated stamp is the same reading carried again.
    pub fn record(&mut self, point: FleetLoadPoint) -> bool {
        if self
            .points
            .back()
            .is_some_and(|last| last.at_unix_ms >= point.at_unix_ms)
        {
            return false;
        }
        if self.points.len() == FLEET_LOAD_CAPACITY {
            self.points.pop_front();
        }
        self.points.push_back(point);
        true
    }

    pub fn series(&self) -> FleetLoadSeries {
        FleetLoadSeries {
            points: self.points.iter().copied().collect(),
            cadence: FleetLoadCadence::Sample,
        }
    }
}

/// Where a series' points come from. Every ring here is fed by the sample
/// frame; `heartbeat` is the desktop's own fallback for an executor that sends
/// none, and is never produced by the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FleetLoadCadence {
    Sample,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetLoadSeries {
    pub points: Vec<FleetLoadPoint>,
    pub cadence: FleetLoadCadence,
}

/// What `get_fleet_load` answers: every machine with a ring, by executor id.
pub type FleetLoad = BTreeMap<String, FleetLoadSeries>;

/// The `fleet-load-sample` event: one machine's newest sample, for a surface
/// to append without refetching the substrate snapshot.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetLoadSampleEvent {
    pub executor_id: String,
    /// Absent when the sample repeated the last point's stamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point: Option<FleetLoadPoint>,
    /// What each running command attempt and resident process is using, or
    /// the named reason the machine cannot say.
    pub live_usage: Measurement<Vec<LiveUsage>>,
}

/// The latest live usage a connection's samples brought, by subject.
#[derive(Debug, Clone, Default)]
pub struct LiveUsageIndex {
    measured_at_unix_ms: u64,
    gap: Option<MeasurementGap>,
    by_subject: HashMap<LiveUsageSubject, LiveUsageSample>,
}

impl LiveUsageIndex {
    /// Replace the index with `reading` if it was taken after the one held. A
    /// gap clears it: a machine that cannot say what its work uses has no
    /// current reading of it, however recent the last one was.
    pub fn adopt(&mut self, reading: &Measurement<Vec<LiveUsage>>) -> bool {
        if reading.measured_at_unix_ms <= self.measured_at_unix_ms {
            return false;
        }
        self.measured_at_unix_ms = reading.measured_at_unix_ms;
        self.gap = match &reading.reading {
            MeasurementReading::Measured { .. } => None,
            MeasurementReading::Unavailable { reason, .. } => Some(*reason),
        };
        self.by_subject = measured(reading)
            .into_iter()
            .flatten()
            .map(|usage| (usage.subject.clone(), usage.sample))
            .collect();
        true
    }

    /// Why there is no current reading, when there is none: the gap the last
    /// sample named, or [`MeasurementGap::NotSampled`] before any sample.
    pub fn gap(&self) -> Option<MeasurementGap> {
        if self.measured_at_unix_ms == 0 {
            return Some(MeasurementGap::NotSampled);
        }
        self.gap
    }

    pub fn get(&self, subject: &LiveUsageSubject) -> Option<LiveUsageSample> {
        self.by_subject.get(subject).copied()
    }

    /// Fill `live_usage` on every executing request and resident process in
    /// `snapshot` this index has a reading for.
    pub fn join_onto(&self, snapshot: &mut FleetSnapshot) {
        if self.by_subject.is_empty() {
            return;
        }
        for request in &mut snapshot.executing_requests {
            request.live_usage = self.get(&LiveUsageSubject::Command {
                request_id: request.request_id.clone(),
                attempt_id: request.attempt_id.clone(),
            });
        }
        for cell in &mut snapshot.cells {
            for (process_key, process) in &mut cell.occupancy.processes {
                process.live_usage = self.get(&LiveUsageSubject::Resident {
                    cell_id: cell.cell_id.clone(),
                    process_key: process_key.clone(),
                    generation: process.generation,
                });
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use cairn_common::executor_protocol::{CpuPressure, MachineMemory, MeasurementQuality};

    pub(in crate::fleet) fn sample_at(measured_at_unix_ms: u64, utilization: f64) -> MachineSample {
        MachineSample {
            cpu: Measurement::measured(
                measured_at_unix_ms,
                CpuPressure {
                    utilization,
                    user: utilization,
                    system: 0.0,
                    logical_cores: 8,
                },
            ),
            memory: Measurement::measured(
                measured_at_unix_ms,
                MachineMemory {
                    total_bytes: 16,
                    available_bytes: 4,
                },
            ),
            ..MachineSample::default()
        }
    }

    #[test]
    fn a_point_reads_fractions_and_names_what_was_not_measured() {
        let point = FleetLoadPoint::of_sample(&sample_at(5_000, 0.5)).unwrap();
        assert_eq!(point.at_unix_ms, 5_000);
        assert_eq!(point.cpu, Some(0.5));
        assert_eq!(point.memory, Some(0.75));
        assert_eq!(point.memory_stall, None, "not measured, not zero");
        assert_eq!(point.io_stall, None);
        assert_eq!(
            FleetLoadPoint::of_sample(&MachineSample::default()),
            None,
            "a never-sampled machine has no instant to plot"
        );
    }

    #[test]
    fn the_ring_holds_ten_minutes_and_drops_repeated_readings() {
        let mut ring = LoadRing::default();
        for tick in 1..=FLEET_LOAD_CAPACITY as u64 + 30 {
            assert!(ring.record(FleetLoadPoint::of_sample(&sample_at(tick * 5_000, 0.1)).unwrap()));
        }
        let last =
            FleetLoadPoint::of_sample(&sample_at((FLEET_LOAD_CAPACITY as u64 + 30) * 5_000, 0.9))
                .unwrap();
        assert!(!ring.record(last), "the same stamp carried again");
        let series = ring.series();
        assert_eq!(series.points.len(), FLEET_LOAD_CAPACITY);
        assert_eq!(
            series.points[0].at_unix_ms,
            31 * 5_000,
            "the oldest went first"
        );
        assert_eq!(series.cadence, FleetLoadCadence::Sample);
    }

    #[test]
    fn live_usage_joins_onto_the_rows_it_names_and_a_gap_clears_it() {
        let usage = LiveUsageSample {
            window_cpu_ms: 10_000,
            window_ms: 5_000,
            resident_bytes: Some(1 << 30),
            measured_at_unix_ms: 9,
            quality: MeasurementQuality::Sampled,
        };
        let mut index = LiveUsageIndex::default();
        assert!(index.adopt(&Measurement::measured(
            10,
            vec![LiveUsage {
                subject: LiveUsageSubject::Command {
                    request_id: "r".into(),
                    attempt_id: "a".into(),
                },
                sample: usage,
            }],
        )));
        assert!(
            !index.adopt(&Measurement::measured(10, Vec::new())),
            "a reading no newer than the held one is not adopted"
        );
        let command = LiveUsageSubject::Command {
            request_id: "r".into(),
            attempt_id: "a".into(),
        };
        assert_eq!(index.get(&command), Some(usage));
        assert_eq!(index.gap(), None);
        assert!(index.adopt(&Measurement::unavailable(
            11,
            MeasurementGap::UnsupportedPlatform,
        )));
        assert_eq!(index.get(&command), None);
        assert_eq!(index.gap(), Some(MeasurementGap::UnsupportedPlatform));
        assert_eq!(
            LiveUsageIndex::default().gap(),
            Some(MeasurementGap::NotSampled),
            "an executor that has sent no sample has no reading, not an idle one"
        );
    }
}
