//! Backend resource sampler — the daemon-side counterpart of the desktop's
//! webview-pressure sampler.
//!
//! Periodically emits a `backend-pressure` profiler event (memory footprint,
//! CPU utilization, allocation counters, stable tokio runtime metrics, and the
//! spawned agent-CLI process tree) into the same JSONL profiler pipeline
//! (`tracing::info!(target: "profiler", ...)`) that `scripts/profiler.ts`
//! already consumes. Started from `cairn-runner` and `cairn-server` startup.
//!
//! Cheap when disabled: the profiler log target ships `off` (see
//! `cairn_common::logging`), so each cycle first checks
//! `event_enabled!(target: "profiler", INFO)` and does NO sampling work — no
//! `ps`, no syscalls, no metric reads — while the target is filtered out. The
//! log level is fixed at process start, so this check is cheap and stable.
//!
//! The daemon binaries install [`alloc::CountingAllocator`] as their
//! `#[global_allocator]`; without it the allocation counters read zero.

pub mod alloc;
pub mod platform;
pub mod process_tree;
pub mod runtime_metrics;

pub use alloc::{AllocSnapshot, CountingAllocator};

use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::json;

use platform::ResourceReading;
use process_tree::ProcessRow;
use runtime_metrics::RuntimeSnapshot;

/// Sampling cadence. 10s balances resolution against the cost of shelling out
/// to `ps` each cycle: the desktop sampler runs at 5s for interactive webview
/// responsiveness, but backend pressure (memory growth, leak detection, the
/// agent process tree) is a slower-moving signal, and the sampler is a no-op
/// while the profiler target is disabled (the shipped default), so the interval
/// only bears cost when profiling is explicitly enabled.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// Top processes (by CPU) retained per event, matching the desktop sampler's
/// cap so event size stays bounded.
const TOP_PROCESS_LIMIT: usize = 8;

/// One process row in an emitted event.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfiledProcess {
    pid: u32,
    parent_pid: u32,
    cpu_percent: f64,
    rss_mb: f64,
    command: String,
}

/// Everything sampled in one cycle.
struct SampleData {
    resources: ResourceReading,
    alloc: AllocSnapshot,
    runtime: RuntimeSnapshot,
    ps_rows: Result<Vec<ProcessRow>, String>,
}

/// Derived inter-sample deltas. All plain numbers, so [`build_event`] stays a
/// pure, unit-testable function independent of `Instant`.
#[derive(Debug, Default, Clone, Copy)]
struct DerivedDeltas {
    interval_secs: Option<f64>,
    self_cpu_percent: Option<f64>,
    allocated_since_bytes: Option<u64>,
    worker_busy_ratio: Option<f64>,
}

/// The baseline carried between samples for delta computation.
#[derive(Clone, Copy)]
struct PrevSample {
    at: Instant,
    resources: ResourceReading,
    alloc: AllocSnapshot,
    runtime: RuntimeSnapshot,
}

/// Spawn the backend resource sampler on the current tokio runtime. `source_tag`
/// names the daemon (`"cairn-runner"` / `"cairn-server"`) and rides in each
/// event's meta so multiple daemons' streams stay distinguishable.
pub fn spawn_backend_sampler(source_tag: &'static str) {
    tokio::spawn(async move { run_sampler(source_tag).await });
}

async fn run_sampler(source_tag: &'static str) {
    let handle = tokio::runtime::Handle::current();
    let root_pid = std::process::id();
    let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut prev: Option<PrevSample> = None;

    loop {
        interval.tick().await;

        // Cheap gate: when the profiler target is filtered out (shipped
        // default) do no sampling work at all.
        if !tracing::event_enabled!(target: "profiler", tracing::Level::INFO) {
            // Drop the delta baseline so a later re-enable does not emit a delta
            // that spans the disabled gap.
            prev = None;
            continue;
        }

        let started = Instant::now();
        let resources = platform::read_process_resources();
        let alloc = AllocSnapshot::read();
        let runtime = RuntimeSnapshot::read(&handle.metrics());
        let ps_rows = process_tree::sample_ps_rows();

        let derived = prev
            .as_ref()
            .map(|p| {
                let elapsed = started.saturating_duration_since(p.at);
                let elapsed_nanos = elapsed.as_nanos();
                DerivedDeltas {
                    interval_secs: Some(elapsed.as_secs_f64()),
                    self_cpu_percent: resources.cpu_percent_since(&p.resources, elapsed_nanos),
                    allocated_since_bytes: Some(alloc.allocated_since(&p.alloc)),
                    worker_busy_ratio: runtime.busy_ratio_since(&p.runtime, elapsed_nanos),
                }
            })
            .unwrap_or_default();

        let data = SampleData {
            resources,
            alloc,
            runtime,
            ps_rows,
        };
        emit(build_event(
            source_tag,
            root_pid,
            &data,
            &derived,
            started.elapsed(),
        ));

        prev = Some(PrevSample {
            at: started,
            resources,
            alloc,
            runtime,
        });
    }
}

/// Build the `backend-pressure` event payload. Pure over its inputs.
fn build_event(
    source_tag: &str,
    root_pid: u32,
    data: &SampleData,
    derived: &DerivedDeltas,
    work_elapsed: Duration,
) -> serde_json::Value {
    let mut meta = serde_json::Map::new();
    meta.insert("daemon".into(), json!(source_tag));
    meta.insert("rootPid".into(), json!(root_pid));
    if let Some(secs) = derived.interval_secs {
        meta.insert("intervalSec".into(), json!(round_2(secs)));
    }

    // Memory.
    if let Some(footprint) = data.resources.phys_footprint_bytes {
        meta.insert("physFootprintMb".into(), json!(bytes_to_mb(footprint)));
    }
    meta.insert("rssMb".into(), json!(bytes_to_mb(data.resources.rss_bytes)));

    // CPU (self, from the precise cumulative-time delta).
    meta.insert(
        "cpuTimeSec".into(),
        json!(round_2(data.resources.cpu_time_nanos as f64 / 1e9)),
    );
    if let Some(percent) = derived.self_cpu_percent {
        meta.insert("selfCpuPercent".into(), json!(round_2(percent)));
    }
    if let Some(energy) = data.resources.energy_nanojoules {
        meta.insert(
            "energyMillijoules".into(),
            json!(round_2(energy as f64 / 1e6)),
        );
    }

    // Allocations.
    meta.insert(
        "allocTotalMb".into(),
        json!(bytes_to_mb(data.alloc.total_allocated_bytes)),
    );
    meta.insert(
        "allocLiveMb".into(),
        json!(bytes_to_mb(data.alloc.live_bytes())),
    );
    meta.insert("allocCount".into(), json!(data.alloc.alloc_count));
    if let Some(since) = derived.allocated_since_bytes {
        meta.insert("allocatedSinceMb".into(), json!(bytes_to_mb(since)));
    }

    // Tokio runtime (stable metrics only).
    meta.insert("tokioWorkers".into(), json!(data.runtime.num_workers));
    meta.insert(
        "tokioAliveTasks".into(),
        json!(data.runtime.num_alive_tasks),
    );
    meta.insert(
        "tokioGlobalQueueDepth".into(),
        json!(data.runtime.global_queue_depth),
    );
    if let Some(ratio) = derived.worker_busy_ratio {
        meta.insert("tokioWorkerBusyRatio".into(), json!(round_2(ratio)));
    }

    // Process tree (rooted at this daemon; captures spawned agent CLIs).
    match &data.ps_rows {
        Ok(rows) => {
            let tree = process_tree::select_process_tree(rows, root_pid);
            let tree_cpu_percent: f64 = tree.iter().map(|row| row.cpu_percent).sum();
            let tree_rss_mb: f64 = tree.iter().map(|row| row.rss_kb as f64 / 1024.0).sum();
            meta.insert("processCount".into(), json!(tree.len()));
            meta.insert("treeCpuPercent".into(), json!(round_2(tree_cpu_percent)));
            meta.insert("treeRssMb".into(), json!(round_2(tree_rss_mb)));

            let mut top = tree;
            top.sort_by(|a, b| {
                b.cpu_percent
                    .partial_cmp(&a.cpu_percent)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let top_processes: Vec<ProfiledProcess> = top
                .into_iter()
                .take(TOP_PROCESS_LIMIT)
                .map(|row| ProfiledProcess {
                    pid: row.pid,
                    parent_pid: row.ppid,
                    cpu_percent: round_2(row.cpu_percent),
                    rss_mb: round_2(row.rss_kb as f64 / 1024.0),
                    command: process_tree::shorten_command(&row.command),
                })
                .collect();
            meta.insert("topProcesses".into(), json!(top_processes));
        }
        Err(error) => {
            meta.insert("psError".into(), json!(error));
            meta.insert("topProcesses".into(), json!([] as [ProfiledProcess; 0]));
        }
    }

    json!({
        "v": 1,
        "ts": chrono::Utc::now().to_rfc3339(),
        "source": "backend",
        "kind": "backend-pressure",
        "name": "resource-sampler",
        "durationMs": round_2(work_elapsed.as_secs_f64() * 1000.0),
        "status": "ok",
        "meta": meta,
    })
}

fn emit(event: serde_json::Value) {
    tracing::info!(target: "profiler", "{}", event);
}

fn bytes_to_mb(bytes: u64) -> f64 {
    round_2(bytes as f64 / 1_048_576.0)
}

fn round_2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_with(ps_rows: Result<Vec<ProcessRow>, String>) -> SampleData {
        SampleData {
            resources: ResourceReading {
                phys_footprint_bytes: Some(200 * 1_048_576),
                rss_bytes: 150 * 1_048_576,
                cpu_time_nanos: 5_000_000_000,
                energy_nanojoules: None,
            },
            alloc: AllocSnapshot {
                total_allocated_bytes: 900 * 1_048_576,
                total_deallocated_bytes: 400 * 1_048_576,
                alloc_count: 12_345,
            },
            runtime: RuntimeSnapshot {
                num_workers: 8,
                num_alive_tasks: 42,
                global_queue_depth: 3,
                total_worker_busy_nanos: Some(10_000),
            },
            ps_rows,
        }
    }

    fn row(pid: u32, ppid: u32, cpu: f64, rss_kb: u64, command: &str) -> ProcessRow {
        ProcessRow {
            pid,
            ppid,
            cpu_percent: cpu,
            rss_kb,
            command: command.into(),
        }
    }

    #[test]
    fn build_event_has_stable_envelope_and_meta() {
        let data = sample_with(Ok(vec![
            row(100, 1, 3.0, 50_000, "/usr/bin/cairn-runner"),
            row(200, 100, 9.0, 80_000, "/usr/local/bin/claude"),
        ]));
        let derived = DerivedDeltas {
            interval_secs: Some(10.0),
            self_cpu_percent: Some(12.5),
            allocated_since_bytes: Some(20 * 1_048_576),
            worker_busy_ratio: Some(0.25),
        };
        let event = build_event(
            "cairn-runner",
            100,
            &data,
            &derived,
            Duration::from_millis(4),
        );

        assert_eq!(event["source"], "backend");
        assert_eq!(event["kind"], "backend-pressure");
        assert_eq!(event["v"], 1);
        let meta = &event["meta"];
        assert_eq!(meta["daemon"], "cairn-runner");
        assert_eq!(meta["rootPid"], 100);
        assert_eq!(meta["physFootprintMb"], 200.0);
        assert_eq!(meta["rssMb"], 150.0);
        assert_eq!(meta["selfCpuPercent"], 12.5);
        assert_eq!(meta["allocLiveMb"], 500.0);
        assert_eq!(meta["allocatedSinceMb"], 20.0);
        assert_eq!(meta["tokioWorkers"], 8);
        assert_eq!(meta["tokioAliveTasks"], 42);
        assert_eq!(meta["tokioWorkerBusyRatio"], 0.25);
        // Both processes are in the tree (200 is a child of root 100).
        assert_eq!(meta["processCount"], 2);
        let top = meta["topProcesses"].as_array().unwrap();
        assert!(top.len() <= TOP_PROCESS_LIMIT);
        // Sorted by CPU descending: claude (9.0) first, shortened to basename.
        assert_eq!(top[0]["command"], "claude");
        assert_eq!(top[0]["pid"], 200);
    }

    #[test]
    fn build_event_reports_ps_error_with_empty_tree() {
        let data = sample_with(Err("failed to run ps: not found".into()));
        let event = build_event(
            "cairn-server",
            1,
            &data,
            &DerivedDeltas::default(),
            Duration::ZERO,
        );
        let meta = &event["meta"];
        assert_eq!(meta["psError"], "failed to run ps: not found");
        assert_eq!(meta["topProcesses"].as_array().unwrap().len(), 0);
        assert!(meta.get("processCount").is_none());
        // First-sample deltas are absent.
        assert!(meta.get("selfCpuPercent").is_none());
        assert!(meta.get("intervalSec").is_none());
    }
}
