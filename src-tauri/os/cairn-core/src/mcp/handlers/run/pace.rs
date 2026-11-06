//! How long each item of a routed batch took and, when one ran far past what
//! its machine had learned to expect, the reading that says why.
//!
//! Durations are the executor's own per-item measurements. The expectation is
//! the fleet's, read before the run was learned from ([`BatchPace`]). The
//! explanation is built only from readings taken as the batch began and from
//! the item's own CPU, so it names a cause only where a reading shows one, and
//! says so when none does.

use super::types::ItemOutcome;
use crate::fleet::{compact_duration, BatchPace};
use cairn_common::executor_protocol::{ExecutionLoadContext, ProcessBatchItemOutcome};

/// An item is slow when it ran at least this many times its expectation...
const SLOW_FACTOR: u64 = 4;
/// ...and at least this much past it, so a two-second command that took nine
/// is not worth a clause.
const SLOW_MIN_EXCESS_MS: u64 = 60_000;
/// Host CPU utilization, in thousandths, read as saturated.
const SATURATED_CPU_MILLIS: u32 = 900;
/// A full-stall share, in thousandths of wall time, read as the host stalling.
const STALLING_FULL_MILLIS: u32 = 100;

/// The batch's items with each one's duration in its label, and the sentence
/// that goes beside the line naming the machine: the batch total, or for a
/// single item (which is shown without a label) that item's own duration and
/// any explanation it needs.
pub(super) fn time_batch(
    items: Vec<ProcessBatchItemOutcome>,
    pace: Option<&BatchPace>,
    batch_duration_ms: Option<u64>,
    load: Option<&ExecutionLoadContext>,
) -> (Vec<ItemOutcome>, String) {
    let lowered = pace.is_some_and(|pace| pace.lowered);
    let labels: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let expected_ms = pace.and_then(|pace| pace.expected_ms.get(index).copied().flatten());
            item_label(
                item.duration_ms,
                item.cpu.map(|cpu| cpu.cpu_ms()),
                expected_ms,
                load,
                lowered,
            )
        })
        .collect();
    let took = match labels.as_slice() {
        [only] => only.clone(),
        _ => elapsed(batch_duration_ms.unwrap_or_else(|| span_ms(&items))),
    };
    let labelled = items.len() > 1;
    let outcomes = items
        .into_iter()
        .zip(labels)
        .map(|(item, label)| {
            let mut outcome = ItemOutcome::from(item);
            // Inside the `=== … ===` line, as a read batch carries its
            // `[lines …]`, so every parser that splits on whole header lines
            // still finds this one.
            if labelled {
                outcome.header = format!("{} [{label}]", outcome.header);
            }
            outcome
        })
        .collect();
    (outcomes, format!("Took {took}."))
}

/// From the first item's start to the last item's end, for an executor that
/// did not state the batch's own duration.
fn span_ms(items: &[ProcessBatchItemOutcome]) -> u64 {
    let started = items.iter().map(|item| item.started_at_unix_ms).min();
    let finished = items.iter().map(|item| item.finished_at_unix_ms).max();
    match (started, finished) {
        (Some(started), Some(finished)) => finished.saturating_sub(started),
        _ => 0,
    }
}

fn item_label(
    duration_ms: u64,
    cpu_ms: Option<u64>,
    expected_ms: Option<u64>,
    load: Option<&ExecutionLoadContext>,
    lowered: bool,
) -> String {
    let took = elapsed(duration_ms);
    match expected_ms.filter(|&expected| ran_far_past(duration_ms, expected)) {
        Some(expected) => format!(
            "{took}, {}x the {} this machine had learned to expect: {}",
            duration_ms / expected,
            elapsed(expected),
            why_slow(duration_ms, cpu_ms, load, lowered)
        ),
        None => took,
    }
}

fn ran_far_past(duration_ms: u64, expected_ms: u64) -> bool {
    expected_ms > 0
        && duration_ms >= expected_ms.saturating_mul(SLOW_FACTOR)
        && duration_ms - expected_ms >= SLOW_MIN_EXCESS_MS
}

/// One clause naming what the host readings show. A stall is the strongest
/// reading and stands alone. Otherwise the clause names CPU saturation and the
/// lowered priority a colocated executor gives everything it runs, with the
/// item's own CPU beside them. The CPU figure is a process-tree census, which
/// undercounts short-lived processes, so it is reported as what was seen and
/// never by itself as starvation.
fn why_slow(
    duration_ms: u64,
    cpu_ms: Option<u64>,
    load: Option<&ExecutionLoadContext>,
    lowered: bool,
) -> String {
    let stalling = |share: Option<u32>| share.filter(|&share| share >= STALLING_FULL_MILLIS);
    if let Some(share) = load.and_then(|load| stalling(load.memory_pressure_full_millis)) {
        return format!(
            "memory was stalling the host ({}% of the time fully stalled as it began)",
            percent(share)
        );
    }
    if let Some(share) = load.and_then(|load| stalling(load.io_pressure_full_millis)) {
        return format!(
            "storage I/O was stalling the host ({}% of the time fully stalled as it began)",
            percent(share)
        );
    }
    let utilization = load.and_then(|load| load.cpu_utilization_millis);
    let beside = load.map_or(0, |load| {
        load.co_resident_compile_jobs
            .saturating_add(load.co_resident_light_jobs)
    });
    let host = [
        utilization.map(|share| format!("{}% busy as it began", percent(share))),
        (beside > 0).then(|| match beside {
            1 => "1 other command beside it".to_string(),
            count => format!("{count} other commands beside it"),
        }),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(", ");
    let seen = cpu_ms.map(|cpu| {
        format!(
            "its processes were seen using {} of CPU in {}",
            elapsed(cpu),
            elapsed(duration_ms)
        )
    });
    let saturated = utilization.is_some_and(|share| share >= SATURATED_CPU_MILLIS);
    let mut causes = Vec::new();
    if saturated {
        causes.push(format!("the host's CPU was saturated ({host})"));
    }
    if lowered {
        causes
            .push("it ran at lowered priority behind the host's default-priority work".to_string());
    }
    if causes.is_empty() {
        let shown: Vec<String> = [
            (!host.is_empty()).then(|| format!("the host was {host}")),
            seen,
        ]
        .into_iter()
        .flatten()
        .collect();
        return match shown.is_empty() {
            true => "no host reading from its start explains it".to_string(),
            false => format!(
                "no host reading from its start explains it ({})",
                shown.join("; ")
            ),
        };
    }
    causes.extend(seen);
    and_list(causes)
}

fn and_list(mut parts: Vec<String>) -> String {
    match parts.len() {
        0 => String::new(),
        1 => parts.remove(0),
        2 => format!("{} and {}", parts[0], parts[1]),
        _ => {
            let last = parts.pop().unwrap_or_default();
            format!("{}, and {last}", parts.join(", "))
        }
    }
}

fn percent(millis: u32) -> u32 {
    millis / 10
}

/// `350ms`, `41s`, `3m12s`, `2h22m`.
fn elapsed(ms: u64) -> String {
    match ms {
        0..=999 => format!("{ms}ms"),
        _ => compact_duration(ms),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::CpuTime;

    fn item(
        header: &str,
        started: u64,
        duration_ms: u64,
        cpu_ms: Option<u64>,
    ) -> ProcessBatchItemOutcome {
        ProcessBatchItemOutcome {
            header: header.into(),
            body: "out".into(),
            succeeded: true,
            suspended: false,
            images: Vec::new(),
            exit_code: Some(0),
            timed_out: false,
            started_at_unix_ms: started,
            finished_at_unix_ms: started + duration_ms,
            duration_ms,
            peak_rss_bytes: None,
            peak_process: None,
            cpu: cpu_ms.map(CpuTime::sampled),
            granted_units: None,
            build_jobs: None,
            disk_delta_bytes: None,
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            toolchain_fingerprint: None,
            verdict_environment_hash: None,
            stop: None,
        }
    }

    /// cairn/5775's afternoon on local, as the batch began.
    fn saturated_host() -> ExecutionLoadContext {
        ExecutionLoadContext {
            co_resident_compile_jobs: 3,
            co_resident_light_jobs: 14,
            cpu_utilization_millis: Some(1_000),
            ..ExecutionLoadContext::default()
        }
    }

    fn pace(lowered: bool, expected_ms: Vec<Option<u64>>) -> BatchPace {
        BatchPace {
            lowered,
            expected_ms,
        }
    }

    /// The specimen: a script that failed in 41 s beside a typecheck learned
    /// at 31 s that ran for 2h22m. Both durations show, and the long one says
    /// what the host readings show about why.
    #[test]
    fn a_batch_whose_second_item_runs_long_shows_both_durations_and_why() {
        let (outcomes, took) = time_batch(
            vec![
                item("bundle and ship", 0, 41_000, Some(3_000)),
                item("bun run typecheck", 0, 8_527_000, Some(41_000)),
            ],
            Some(&pace(true, vec![None, Some(31_000)])),
            Some(8_532_000),
            Some(&saturated_host()),
        );
        assert_eq!(outcomes[0].header, "bundle and ship [41s]");
        assert_eq!(
            outcomes[1].header,
            "bun run typecheck [2h22m, 275x the 31s this machine had learned to expect: \
             the host's CPU was saturated (100% busy as it began, 17 other commands beside it), \
             it ran at lowered priority behind the host's default-priority work, \
             and its processes were seen using 41s of CPU in 2h22m]"
        );
        assert_eq!(took, "Took 2h22m.");
    }

    #[test]
    fn a_batch_at_normal_speed_shows_durations_and_no_clause() {
        let (outcomes, took) = time_batch(
            vec![
                item("cargo check", 0, 95_000, Some(180_000)),
                item("bun run typecheck", 0, 33_000, Some(21_000)),
            ],
            Some(&pace(true, vec![Some(90_000), Some(31_000)])),
            Some(95_400),
            Some(&saturated_host()),
        );
        assert_eq!(outcomes[0].header, "cargo check [1m35s]");
        assert_eq!(outcomes[1].header, "bun run typecheck [33s]");
        assert_eq!(took, "Took 1m35s.");
    }

    /// A single item is shown without a label, so its duration and its
    /// explanation ride beside the machine line instead.
    #[test]
    fn a_single_slow_item_explains_itself_beside_the_machine_line() {
        let (outcomes, took) = time_batch(
            vec![item("bun run typecheck", 0, 600_000, None)],
            Some(&pace(false, vec![Some(30_000)])),
            Some(600_500),
            Some(&ExecutionLoadContext {
                io_pressure_full_millis: Some(430),
                ..saturated_host()
            }),
        );
        assert_eq!(outcomes[0].header, "bun run typecheck");
        assert_eq!(
            took,
            "Took 10m00s, 20x the 30s this machine had learned to expect: \
             storage I/O was stalling the host (43% of the time fully stalled as it began)."
        );
    }

    #[test]
    fn memory_stall_is_named_ahead_of_everything_else() {
        let label = item_label(
            600_000,
            Some(1_000),
            Some(30_000),
            Some(&ExecutionLoadContext {
                memory_pressure_full_millis: Some(250),
                io_pressure_full_millis: Some(400),
                ..saturated_host()
            }),
            true,
        );
        assert!(
            label.ends_with(
                "memory was stalling the host (25% of the time fully stalled as it began)"
            ),
            "{label}"
        );
    }

    /// Without a reading that shows a cause, the clause says so and shows what
    /// was read, rather than guessing.
    #[test]
    fn a_slow_item_no_reading_explains_says_so() {
        let label = item_label(
            600_000,
            Some(590_000),
            Some(30_000),
            Some(&ExecutionLoadContext {
                cpu_utilization_millis: Some(200),
                ..ExecutionLoadContext::default()
            }),
            false,
        );
        assert_eq!(
            label,
            "10m00s, 20x the 30s this machine had learned to expect: no host reading from its \
             start explains it (the host was 20% busy as it began; its processes were seen using \
             9m50s of CPU in 10m00s)"
        );
        let unread = item_label(600_000, None, Some(30_000), None, false);
        assert!(
            unread.ends_with(": no host reading from its start explains it"),
            "{unread}"
        );
    }

    /// A class prior is a starting point, not an expectation: nothing learned,
    /// no clause, however long it ran.
    #[test]
    fn nothing_learned_means_no_clause() {
        let (outcomes, _) = time_batch(
            vec![item("a", 0, 8_527_000, None), item("b", 0, 1, None)],
            Some(&pace(true, vec![None, None])),
            None,
            Some(&saturated_host()),
        );
        assert_eq!(outcomes[0].header, "a [2h22m]");
        assert_eq!(outcomes[1].header, "b [1ms]");
        let (unpaced, took) = time_batch(
            vec![item("a", 10, 2_000, None), item("b", 500, 4_000, None)],
            None,
            None,
            None,
        );
        assert_eq!(unpaced[1].header, "b [4s]");
        assert_eq!(took, "Took 4s.", "from the first start to the last end");
    }

    #[test]
    fn a_short_command_several_times_slower_is_not_worth_a_clause() {
        assert_eq!(item_label(9_000, None, Some(2_000), None, true), "9s");
        assert_eq!(item_label(350, None, Some(50), None, true), "350ms");
        assert!(
            !ran_far_past(100_000, 30_000),
            "past the excess, short of the factor"
        );
        assert!(ran_far_past(120_000, 30_000));
    }
}
