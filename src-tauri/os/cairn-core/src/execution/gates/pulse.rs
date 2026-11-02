//! The gate sweep's own loop, and how it shows it is alive.
//!
//! ## One sweeper
//!
//! [`run_gate_sweep_loop`] is the only production caller of
//! [`super::sweep::sweep`], so passes never overlap. It sweeps once at boot,
//! then sleeps until a nudge arrives ([`super::sweep::notify_gate_evidence_changed`])
//! or the next `clock` gate falls due, whichever is first, and at most
//! [`SweepLoopConfig::max_sleep`]. Nudges that arrive during a pass wait in
//! the channel and, after the debounce, become exactly one follow-up pass.
//! Nothing else waits on a pass: the recipe scheduler fires on its own clock.
//!
//! ## The pulse
//!
//! Each pass stamps [`SweepPulse`] and logs one INFO line ([`pass_line`]). A
//! dedicated OS thread ([`spawn_sweep_watchdog`]) reads the pulse's atomics and
//! the admission lock's holder slot, neither of which needs the runtime, so a
//! runtime too wedged to sweep can still be reported: an ERROR when no pass has
//! completed for [`STALL_AFTER_S`], repeated as long as it lasts, and an INFO
//! when passes resume. The queue view reads the same verdict as
//! `stalled_since`.
//!
//! The pulse also keeps when each held execution was last evaluated, in
//! process memory only: the operator's surfaces show it as `checked 2m ago`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use super::capacity::{AdmissionHolder, AdmissionLock};
use super::sweep::SweepReport;
use crate::models::{Execution, ExecutionGateCensus, Issue};
use crate::orchestrator::Orchestrator;
use crate::services::Clock;

/// How long the sweep may go without completing a pass before it is stalled.
pub const STALL_AFTER_S: i64 = 300;

/// How often a stall that goes on is reported again.
const STALL_REPEAT_S: i64 = 300;

/// How often the watchdog reads the pulse.
const WATCHDOG_TICK: Duration = Duration::from_secs(30);

/// What started a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Boot,
    Tick,
    Nudge,
}

impl Trigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Boot => "boot",
            Self::Tick => "tick",
            Self::Nudge => "nudge",
        }
    }
}

/// The loop's timings: injectable so a test can drive the loop quickly.
#[derive(Debug, Clone, Copy)]
pub struct SweepLoopConfig {
    /// Coalescing window after a nudge: a burst of satisfying writes (a merge
    /// resolves the issue, its PR and a check in quick succession) becomes one
    /// pass.
    pub debounce: Duration,
    /// The longest the loop sleeps with no nudge and no clock gate due.
    pub max_sleep: Duration,
}

impl Default for SweepLoopConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(250),
            max_sleep: Duration::from_secs(60),
        }
    }
}

/// The last completed pass, as the pulse's `watch` channel publishes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassSummary {
    pub passes_completed: u64,
    pub trigger: Option<Trigger>,
    /// The pass's liveness line, or its error.
    pub line: String,
    pub failed: bool,
}

/// The gate sweep's liveness, readable without the runtime.
pub struct SweepPulse {
    /// Set when the sweep loop starts: a host that runs no loop is never
    /// stalled.
    installed: AtomicBool,
    /// Unix seconds; 0 means none.
    boot_at: AtomicI64,
    last_started_at: AtomicI64,
    last_completed_at: AtomicI64,
    last_duration_ms: AtomicU64,
    passes_started: AtomicU64,
    passes_completed: AtomicU64,
    /// When each held execution was last evaluated, in unix seconds. Kept to
    /// the executions the last pass found held.
    evaluated: Mutex<HashMap<String, i64>>,
    passes: watch::Sender<PassSummary>,
    admission_lock: Arc<AdmissionLock>,
}

impl SweepPulse {
    pub fn new(admission_lock: Arc<AdmissionLock>) -> Self {
        Self {
            installed: AtomicBool::new(false),
            boot_at: AtomicI64::new(0),
            last_started_at: AtomicI64::new(0),
            last_completed_at: AtomicI64::new(0),
            last_duration_ms: AtomicU64::new(0),
            passes_started: AtomicU64::new(0),
            passes_completed: AtomicU64::new(0),
            evaluated: Mutex::new(HashMap::new()),
            passes: watch::channel(PassSummary::default()).0,
            admission_lock,
        }
    }

    pub(crate) fn mark_installed(&self, now: i64) {
        self.boot_at.store(now, Ordering::Relaxed);
        self.installed.store(true, Ordering::Release);
    }

    fn pass_started(&self, now: i64) {
        self.last_started_at.store(now, Ordering::Relaxed);
        self.passes_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn pass_completed(&self, now: i64, duration: Duration, summary: PassSummary) {
        self.last_completed_at.store(now, Ordering::Relaxed);
        self.last_duration_ms
            .store(duration_ms(duration), Ordering::Relaxed);
        let passes_completed = self.passes_completed.fetch_add(1, Ordering::AcqRel) + 1;
        self.passes.send_replace(PassSummary {
            passes_completed,
            ..summary
        });
    }

    pub fn passes_started(&self) -> u64 {
        self.passes_started.load(Ordering::Relaxed)
    }

    pub fn passes_completed(&self) -> u64 {
        self.passes_completed.load(Ordering::Acquire)
    }

    /// Every completed pass, as it completes.
    pub fn subscribe(&self) -> watch::Receiver<PassSummary> {
        self.passes.subscribe()
    }

    /// The last sign of progress: the last completed pass, or the loop's start
    /// when none has completed. `None` when no loop runs here.
    fn progress_at(&self) -> Option<i64> {
        if !self.installed.load(Ordering::Acquire) {
            return None;
        }
        match self.last_completed_at.load(Ordering::Relaxed) {
            0 => Some(self.boot_at.load(Ordering::Relaxed)),
            completed => Some(completed),
        }
    }

    /// Since when the sweep has made no progress, once that is longer than
    /// [`STALL_AFTER_S`].
    pub fn stalled_since(&self, now: i64) -> Option<i64> {
        stalled_since_at(now, self.progress_at())
    }

    fn readings(&self) -> StallReadings {
        StallReadings {
            progress_at: self.progress_at(),
            last_pass_ms: self.last_duration_ms.load(Ordering::Relaxed),
            holder: self.admission_lock.holder(),
        }
    }

    fn evaluated(&self) -> std::sync::MutexGuard<'_, HashMap<String, i64>> {
        self.evaluated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn note_evaluated(&self, execution_id: &str, now: i64) {
        self.evaluated().insert(execution_id.to_string(), now);
    }

    /// Forget every execution a pass no longer found held.
    pub(crate) fn retain_evaluated(&self, held: &HashSet<String>) {
        self.evaluated().retain(|id, _| held.contains(id));
    }

    pub fn evaluated_at(&self, execution_id: &str) -> Option<i64> {
        self.evaluated().get(execution_id).copied()
    }

    /// Stamp a held execution's census with when the sweep last evaluated it.
    pub fn stamp(&self, execution_id: &str, census: &mut ExecutionGateCensus) {
        if census.held {
            census.evaluated_at = self.evaluated_at(execution_id);
        }
    }

    /// [`Self::stamp`] every held execution an issue carries.
    pub fn stamp_issue(&self, issue: &mut Issue) {
        for held in &mut issue.held_executions {
            self.stamp(&held.execution_id, &mut held.census);
        }
    }

    /// [`Self::stamp`] every gated execution in `executions`.
    pub fn stamp_executions<'a>(&self, executions: impl IntoIterator<Item = &'a mut Execution>) {
        for execution in executions {
            if let Some(census) = execution.gate_census.as_mut() {
                self.stamp(&execution.id, census);
            }
        }
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// `progress_at`, once it is more than [`STALL_AFTER_S`] before `now`.
fn stalled_since_at(now: i64, progress_at: Option<i64>) -> Option<i64> {
    progress_at.filter(|at| now - at > STALL_AFTER_S)
}

// ---------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------

/// The single-flight gate sweep loop. Never returns: a closed nudge channel
/// leaves the timer, which is enough to release everything, only later.
pub(crate) async fn run_gate_sweep_loop(
    orch: Orchestrator,
    mut nudges: mpsc::UnboundedReceiver<()>,
    config: SweepLoopConfig,
) {
    orch.sweep_pulse.mark_installed(orch.services.clock.now());
    log::info!("execution gate sweep loop started");
    let mut trigger = Trigger::Boot;
    let mut open = true;
    loop {
        pass(&orch, trigger).await;
        let bound = sleep_bound(&orch, config.max_sleep).await;
        trigger = if open {
            tokio::select! {
                nudge = nudges.recv() => match nudge {
                    Some(()) => {
                        tokio::time::sleep(config.debounce).await;
                        while nudges.try_recv().is_ok() {}
                        Trigger::Nudge
                    }
                    None => {
                        open = false;
                        log::warn!(
                            "execution gate sweep lost its nudge channel; sweeping on the timer"
                        );
                        tokio::time::sleep(bound).await;
                        Trigger::Tick
                    }
                },
                _ = tokio::time::sleep(bound) => Trigger::Tick,
            }
        } else {
            tokio::time::sleep(bound).await;
            Trigger::Tick
        };
    }
}

/// `min(max_sleep, next clock gate due - now)`. A due instant already past is
/// excluded by [`super::sweep::next_gate_due_after`], so this never spins.
async fn sleep_bound(orch: &Orchestrator, max_sleep: Duration) -> Duration {
    let now = orch.services.clock.now();
    match super::sweep::next_gate_due_after(orch, now).await {
        Some(due) => Duration::from_secs(u64::try_from(due - now).unwrap_or(0)).min(max_sleep),
        None => max_sleep,
    }
}

/// One pass, stamped on the pulse and logged. A failed pass still completes:
/// the loop is alive, and the next pass retries.
async fn pass(orch: &Orchestrator, trigger: Trigger) {
    let pulse = &orch.sweep_pulse;
    pulse.pass_started(orch.services.clock.now());
    let started = Instant::now();
    let outcome = super::sweep::sweep(orch).await;
    let duration = started.elapsed();
    let summary = match outcome {
        Ok(report) => {
            let line = pass_line(trigger, &report, duration);
            log::info!("{line}");
            PassSummary {
                trigger: Some(trigger),
                line,
                ..PassSummary::default()
            }
        }
        Err(error) => {
            let line = format!(
                "execution gate sweep pass failed trigger={} duration_ms={} error={error}",
                trigger.as_str(),
                duration_ms(duration)
            );
            log::error!("{line}");
            PassSummary {
                trigger: Some(trigger),
                line,
                failed: true,
                ..PassSummary::default()
            }
        }
    };
    pulse.pass_completed(orch.services.clock.now(), duration, summary);
}

/// The one INFO line a pass logs.
pub fn pass_line(trigger: Trigger, report: &SweepReport, duration: Duration) -> String {
    let lock = |ms: Option<u64>| ms.map_or_else(|| "-".to_string(), |ms| ms.to_string());
    format!(
        "execution gate sweep pass trigger={} duration_ms={} evaluated={} candidates={} \
         released={} held_by_cap={} foreclosed={} lock_wait_ms={} lock_held_ms={} clauses={}",
        trigger.as_str(),
        duration_ms(duration),
        report.evaluated,
        report.candidates,
        report.released.len(),
        report.held_by_cap,
        report.foreclosed.len(),
        lock(report.lock_wait_ms),
        lock(report.lock_held_ms),
        clause_tally(&report.clauses),
    )
}

/// `issue_resolution:no_evidence=3,satisfied=1;pr_state:refuted=1`, or `-`.
fn clause_tally(clauses: &BTreeMap<(String, &'static str), usize>) -> String {
    if clauses.is_empty() {
        return "-".to_string();
    }
    let mut facts: Vec<(&str, Vec<String>)> = Vec::new();
    for ((fact, outcome), count) in clauses {
        let entry = format!("{outcome}={count}");
        match facts.last_mut() {
            Some((last, entries)) if *last == fact.as_str() => entries.push(entry),
            _ => facts.push((fact.as_str(), vec![entry])),
        }
    }
    facts
        .into_iter()
        .map(|(fact, entries)| format!("{fact}:{}", entries.join(",")))
        .collect::<Vec<_>>()
        .join(";")
}

// ---------------------------------------------------------------------------
// The watchdog
// ---------------------------------------------------------------------------

/// What the watchdog reads off the pulse, without the runtime.
#[derive(Debug, Clone)]
pub struct StallReadings {
    /// The last completed pass, or the loop's start; `None` with no loop.
    pub progress_at: Option<i64>,
    pub last_pass_ms: u64,
    pub holder: Option<AdmissionHolder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallEvent {
    Onset { since: i64 },
    Repeat { since: i64 },
    Recovered { lasted_s: i64 },
}

/// Decides when a stall is reported: once at onset, again every
/// [`STALL_REPEAT_S`] while it lasts, and once on recovery. Pure: it has no
/// clock and no thread of its own.
#[derive(Debug, Default)]
pub struct StallMonitor {
    /// The stall being reported: since when, and when it was last reported.
    stall: Option<(i64, i64)>,
}

impl StallMonitor {
    pub fn observe(&mut self, now: i64, readings: &StallReadings) -> Option<StallEvent> {
        match (stalled_since_at(now, readings.progress_at), self.stall) {
            (Some(since), None) => {
                self.stall = Some((since, now));
                Some(StallEvent::Onset { since })
            }
            (Some(since), Some((_, reported))) => {
                if now - reported >= STALL_REPEAT_S {
                    self.stall = Some((since, now));
                    Some(StallEvent::Repeat { since })
                } else {
                    None
                }
            }
            (None, Some((since, _))) => {
                self.stall = None;
                Some(StallEvent::Recovered {
                    lasted_s: now - since,
                })
            }
            (None, None) => None,
        }
    }
}

/// The log line for a stall event.
fn stall_line(event: StallEvent, readings: &StallReadings) -> String {
    match event {
        StallEvent::Onset { since } | StallEvent::Repeat { since } => {
            let (holder, subject, held_for) =
                readings.holder.as_ref().map_or(("none", "", 0), |holder| {
                    (
                        holder.site.as_str(),
                        holder.subject.as_str(),
                        duration_ms(holder.held),
                    )
                });
            format!(
                "execution gate sweep stalled since={} last_pass_ms={} lock_holder={holder} \
                 holder_subject={subject} held_for_ms={held_for}",
                crate::clock::stamp(since).unwrap_or_else(|| since.to_string()),
                readings.last_pass_ms,
            )
        }
        StallEvent::Recovered { lasted_s } => {
            format!("execution gate sweep recovered stalled_for_s={lasted_s}")
        }
    }
}

/// Watch the pulse from a thread of its own. It takes no tokio lock and awaits
/// nothing, so it reports a sweep the runtime can no longer run.
pub(crate) fn spawn_sweep_watchdog(pulse: Arc<SweepPulse>, clock: Arc<dyn Clock>) {
    let spawned = std::thread::Builder::new()
        .name("gate-sweep-watchdog".to_string())
        .spawn(move || {
            let mut monitor = StallMonitor::default();
            loop {
                std::thread::sleep(WATCHDOG_TICK);
                let readings = pulse.readings();
                let Some(event) = monitor.observe(clock.now(), &readings) else {
                    continue;
                };
                let line = stall_line(event, &readings);
                match event {
                    StallEvent::Recovered { .. } => log::info!("{line}"),
                    _ => log::error!("{line}"),
                }
            }
        });
    if let Err(error) = spawned {
        log::error!("could not start the gate sweep watchdog: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::gates::capacity::AdmissionSite;
    use crate::execution::gates::sweep::tests::{env, execution, file, now, Env, NOW};
    use serde_json::json;

    fn readings(progress_at: Option<i64>) -> StallReadings {
        StallReadings {
            progress_at,
            last_pass_ms: 7,
            holder: None,
        }
    }

    fn merged_after(number: i32) -> serde_json::Value {
        json!([{"fact": "issue_resolution", "issue": format!("cairn://p/prj/{number}"), "resolution": "merged"}])
    }

    /// Run the loop on a channel of the test's own.
    fn start_loop(env: &Env, config: SweepLoopConfig) -> mpsc::UnboundedSender<()> {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_gate_sweep_loop(env.orch.clone(), rx, config));
        tx
    }

    /// Wait, at most five seconds, for the pulse to report `passes` completed.
    async fn passes(env: &Env, passes: u64) -> PassSummary {
        let mut rx = env.orch.sweep_pulse.subscribe();
        let summary = tokio::time::timeout(
            Duration::from_secs(5),
            rx.wait_for(|summary| summary.passes_completed >= passes),
        )
        .await
        .unwrap_or_else(|_| panic!("no pass {passes} within 5s"))
        .unwrap()
        .clone();
        summary
    }

    #[test]
    fn the_monitor_reports_onset_repeat_and_recovery_once_each() {
        let mut monitor = StallMonitor::default();
        let progress = Some(1_000);
        assert_eq!(monitor.observe(1_000 + 299, &readings(progress)), None);
        assert_eq!(monitor.observe(1_000 + 300, &readings(progress)), None);
        assert_eq!(
            monitor.observe(1_000 + 360, &readings(progress)),
            Some(StallEvent::Onset { since: 1_000 })
        );
        assert_eq!(monitor.observe(1_000 + 361, &readings(progress)), None);
        assert_eq!(
            monitor.observe(1_000 + 360 + 299, &readings(progress)),
            None
        );
        assert_eq!(
            monitor.observe(1_000 + 360 + 300, &readings(progress)),
            Some(StallEvent::Repeat { since: 1_000 })
        );
        assert_eq!(
            monitor.observe(1_000 + 360 + 301, &readings(progress)),
            None
        );
        // A pass completes.
        assert_eq!(
            monitor.observe(1_000 + 700, &readings(Some(1_000 + 699))),
            Some(StallEvent::Recovered { lasted_s: 700 })
        );
        assert_eq!(
            monitor.observe(1_000 + 701, &readings(Some(1_000 + 699))),
            None
        );
        // No loop at all is never a stall.
        let mut idle = StallMonitor::default();
        assert_eq!(idle.observe(1_000_000, &readings(None)), None);
    }

    #[test]
    fn a_stall_line_names_the_lock_holder() {
        let holder = AdmissionHolder {
            site: AdmissionSite::SweepPass,
            subject: "3 candidates".to_string(),
            held: Duration::from_millis(1_234),
        };
        let line = stall_line(
            StallEvent::Onset { since: 1_000 },
            &StallReadings {
                progress_at: Some(1_000),
                last_pass_ms: 42,
                holder: Some(holder),
            },
        );
        assert!(
            line.starts_with("execution gate sweep stalled since="),
            "{line}"
        );
        assert!(
            line.ends_with(
                "last_pass_ms=42 lock_holder=sweep_pass holder_subject=3 candidates \
                 held_for_ms=1234"
            ),
            "{line}"
        );
        let free = stall_line(StallEvent::Repeat { since: 1_000 }, &readings(Some(1_000)));
        assert!(
            free.contains("lock_holder=none holder_subject= held_for_ms=0"),
            "{free}"
        );
        assert_eq!(
            stall_line(StallEvent::Recovered { lasted_s: 9 }, &readings(None)),
            "execution gate sweep recovered stalled_for_s=9"
        );
    }

    #[test]
    fn the_pass_line_carries_every_count_and_the_clause_tally() {
        let mut report = SweepReport {
            released: vec!["a".to_string()],
            foreclosed: vec!["g".to_string()],
            evaluated: 5,
            candidates: 2,
            held_by_cap: 1,
            lock_wait_ms: Some(0),
            lock_held_ms: Some(3),
            ..SweepReport::default()
        };
        report
            .clauses
            .insert(("issue_resolution".to_string(), "no_evidence"), 3);
        report
            .clauses
            .insert(("issue_resolution".to_string(), "satisfied"), 1);
        report
            .clauses
            .insert(("pr_state".to_string(), "foreclosed"), 1);
        assert_eq!(
            pass_line(Trigger::Nudge, &report, Duration::from_millis(12)),
            "execution gate sweep pass trigger=nudge duration_ms=12 evaluated=5 candidates=2 \
             released=1 held_by_cap=1 foreclosed=1 lock_wait_ms=0 lock_held_ms=3 \
             clauses=issue_resolution:no_evidence=3,satisfied=1;pr_state:foreclosed=1"
        );
        assert_eq!(
            pass_line(Trigger::Tick, &SweepReport::default(), Duration::ZERO),
            "execution gate sweep pass trigger=tick duration_ms=0 evaluated=0 candidates=0 \
             released=0 held_by_cap=0 foreclosed=0 lock_wait_ms=- lock_held_ms=- clauses=-"
        );
    }

    /// cairn/5844 V7: a merge while holds are pending releases both within one
    /// pass, and that pass's line names it.
    #[tokio::test]
    async fn a_merge_while_holds_are_pending_releases_them_within_one_pass() {
        let env = env("pulse-merge.db").await;
        file(&env, 2, merged_after(1)).await.unwrap();
        file(&env, 3, merged_after(1)).await.unwrap();
        assert_eq!(execution(&env, 2).await.1, "held");
        assert_eq!(execution(&env, 3).await.1, "held");

        let nudge = start_loop(&env, SweepLoopConfig::default());
        let boot = passes(&env, 1).await;
        assert_eq!(boot.trigger, Some(Trigger::Boot));
        assert!(
            boot.line.contains("issue_resolution:no_evidence=2"),
            "{}",
            boot.line
        );

        crate::issues::status::update_status(
            &env.orch,
            "i1",
            "merged",
            crate::issues::status::ResolutionActor::Agent,
            crate::issues::status::Confirmation::from_flag(true),
        )
        .await
        .unwrap();
        nudge.send(()).unwrap();
        let after = passes(&env, 2).await;
        assert_eq!(after.trigger, Some(Trigger::Nudge), "{}", after.line);
        assert!(
            after.line.contains("trigger=nudge")
                && after.line.contains("released=2")
                && after.line.contains("issue_resolution:satisfied=2"),
            "{}",
            after.line
        );
        assert_eq!(execution(&env, 2).await.1, "running");
        assert_eq!(execution(&env, 3).await.1, "running");
    }

    /// cairn/5844 V8, second half: ten nudges during one pass become exactly
    /// one follow-up pass.
    #[tokio::test]
    async fn nudges_during_a_pass_coalesce_into_one_follow_up_pass() {
        let env = env("pulse-coalesce.db").await;
        // A held execution whose gate is now met: a candidate, so the boot
        // pass takes the admission lock.
        file(&env, 3, merged_after(4)).await.unwrap();
        crate::execution::gates::sweep::tests::resolve_issue(&env, 4, "merged").await;
        env.orch
            .admission_lock
            .set_wait_bound(Duration::from_secs(5));
        let guard = env
            .orch
            .admission_lock
            .acquire(AdmissionSite::SweepPass, "the test")
            .await
            .unwrap();

        let nudge = start_loop(
            &env,
            SweepLoopConfig {
                debounce: Duration::from_millis(50),
                max_sleep: Duration::from_secs(600),
            },
        );
        // The boot pass is mid-pass, blocked on the lock the test holds.
        tokio::time::timeout(Duration::from_secs(5), async {
            while env.orch.sweep_pulse.passes_started() < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(env.orch.sweep_pulse.passes_completed(), 0);
        for _ in 0..10 {
            nudge.send(()).unwrap();
        }
        drop(guard);

        passes(&env, 2).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            env.orch.sweep_pulse.passes_completed(),
            2,
            "ten nudges during one pass are one follow-up pass"
        );
        assert_eq!(env.orch.sweep_pulse.passes_started(), 2);
    }

    /// cairn/5844 V9, the queue view's half: `stalled_since` follows the pulse.
    #[tokio::test]
    async fn the_queue_view_says_since_when_the_sweep_is_stalled() {
        let env = env("pulse-stalled.db").await;
        let view = super::super::capacity::queue_view(&env.orch).await.unwrap();
        assert_eq!(view.stalled_since, None, "no loop runs here");

        let pulse = &env.orch.sweep_pulse;
        pulse.mark_installed(NOW - 400);
        pulse.pass_completed(NOW - 360, Duration::ZERO, PassSummary::default());
        let view = super::super::capacity::queue_view(&env.orch).await.unwrap();
        assert_eq!(view.stalled_since, Some(NOW - 360));

        pulse.pass_completed(now(), Duration::ZERO, PassSummary::default());
        let view = super::super::capacity::queue_view(&env.orch).await.unwrap();
        assert_eq!(view.stalled_since, None, "a completed pass clears it");
    }

    /// cairn/5844 V10: a pass stamps each held execution it evaluates, the
    /// queue view carries the stamp, and a release prunes it on the next pass.
    #[tokio::test]
    async fn a_pass_stamps_what_it_evaluated_and_prunes_what_it_released() {
        use crate::execution::gates::capacity::tests::full_fleet;
        use crate::execution::gates::sweep::sweep_at;

        let env = env("pulse-evaluated.db").await;
        full_fleet(&env).await;
        file(&env, 1, json!([])).await.unwrap();
        let (held, status, ..) = execution(&env, 1).await;
        assert_eq!(status, "held");

        let unstamped = super::super::capacity::queue_view(&env.orch).await.unwrap();
        assert_eq!(
            unstamped.held[0].census.as_ref().unwrap().evaluated_at,
            None,
            "nothing has evaluated it yet"
        );

        sweep_at(&env.orch, now()).await.unwrap();
        let view = super::super::capacity::queue_view(&env.orch).await.unwrap();
        assert_eq!(view.held[0].execution_id, held);
        assert_eq!(
            view.held[0].census.as_ref().unwrap().evaluated_at,
            Some(NOW)
        );

        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();
        let report = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![held.clone()]);
        assert_eq!(env.orch.sweep_pulse.evaluated_at(&held), Some(NOW));
        sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(
            env.orch.sweep_pulse.evaluated_at(&held),
            None,
            "a released execution drops out on the next pass"
        );
    }

    /// A loop that has started but never completed a pass counts from its
    /// start.
    #[test]
    fn a_loop_with_no_completed_pass_counts_from_its_start() {
        let pulse = SweepPulse::new(Arc::new(AdmissionLock::default()));
        assert_eq!(pulse.stalled_since(10_000), None);
        pulse.mark_installed(1_000);
        assert_eq!(pulse.stalled_since(1_300), None);
        assert_eq!(pulse.stalled_since(1_301), Some(1_000));
    }
}
