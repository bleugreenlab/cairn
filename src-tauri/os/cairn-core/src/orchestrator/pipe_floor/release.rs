//! The bounded release pass (cairn/6440, cairn/7488): while the floor holds a
//! host whose fresh pipes are degraded, release idle, unowned, unprotected
//! sessions one at a time through the canonical Keep-policy retire until the
//! bar admission itself reopens on holds, and say why the pass stopped when
//! it does not.
//!
//! The bar is the admission latch's own code: a [`PipeHealth`] that starts
//! latched, fed the pass's readings, clears exactly when the monitor would on
//! the same readings. One reading above the floor right after a release is
//! not recovery: on a host at its kernel pipe budget the freed room is taken
//! by the next pipe an active session opens, often within one sample.
//!
//! Pure over an injected [`ReleaseWorld`], so every bound is tested without
//! processes. The live world is in the parent module.

use std::collections::{HashSet, VecDeque};

use cairn_common::pipe_health::{
    HoldPhase, PipeAdmission, PipeCapacity, PipeHealth, DEGRADED_BELOW_BYTES, RECOVERED_AT_BYTES,
    RECOVERY_SAMPLES, RECOVERY_SPACING_MS,
};
use cairn_common::wire_vocabulary::Measurement;
use cairn_procmem::ProcessIdentity;
use serde::{Deserialize, Serialize};

use super::census::{family_delta, Attribution, FamilyDelta};
use crate::storage::pipe_floor::Reclamation;

/// Releases one engagement may make toward the recovery bar. A host at its
/// pipe budget can need several: each release frees one session's pipes,
/// which active sessions start taking at once.
pub(crate) const MAX_RELEASES_PER_ENGAGEMENT: usize = 10;
/// Releases any window may hold, across engagements.
pub(crate) const MAX_RELEASES_PER_WINDOW: usize = 3;
pub(crate) const RELEASE_WINDOW_MS: u64 = 15_000;
/// Candidates a pass may find fenced before it stops: a refusal costs no
/// release budget, but the pass does not hunt forever.
pub(crate) const MAX_REFUSED_CANDIDATES: usize = 5;

/// What the floor says now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FloorNow {
    pub cleared: bool,
    pub phase: Option<HoldPhase>,
}

/// An idle session the collector would evict first, with its leader's
/// identity. Pooled Codex threads are never candidates: their teardown frees
/// no process of their own, so the pass could measure nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub run_id: String,
    pub job_id: Option<String>,
    pub session_id: Option<String>,
    pub family: String,
    pub leader: ProcessIdentity,
}

/// What the canonical retire did with a candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RetireAttempt {
    Released,
    /// A fence refused it (busy, no longer idle, owned, protected). It costs
    /// no release budget.
    NotReleasable(String),
    /// The teardown itself failed: the pass stops.
    Failed(String),
}

pub(crate) trait ReleaseWorld {
    fn floor(&mut self) -> FloorNow;
    /// A fresh-pipe reading taken now, never fed to the monitor, so the
    /// floor's own recovery spacing is untouched.
    fn probe(&mut self) -> Result<PipeCapacity, String>;
    fn census(&mut self) -> Result<Attribution, String>;
    /// Releasable candidates, least relevant first.
    fn candidates(&mut self) -> Vec<Candidate>;
    fn retire(&mut self, candidate: &Candidate) -> RetireAttempt;
    /// Whether `leader` is gone (exited, or its pid now names another
    /// process) within the pass's bound.
    fn await_exit(&mut self, leader: ProcessIdentity) -> bool;
    /// Sessions running a turn now, which the pass names when it runs out of
    /// idle ones to release.
    fn active_sessions(&mut self) -> usize;
    /// Let `ms` pass: the spacing between two recovery readings, or the rate
    /// bound's window freeing.
    fn wait(&mut self, ms: u64);
    fn now_ms(&mut self) -> u64;
    /// Persist one release as soon as it is measured.
    fn record(&mut self, release: &ReleaseRecord);
}

/// A process of the released tree still alive at the `after` reading.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SurvivingProcess {
    pub pid: u32,
    pub family: String,
    pub pipe_fds: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReleaseRecord {
    pub seq: usize,
    pub candidate: Candidate,
    pub released_at_ms: u64,
    /// The leader exited. Its descendants are signalled by nobody, so this
    /// proves the leader only; [`Self::surviving_descendants`] says the rest.
    pub leader_exit_confirmed: bool,
    /// The reading that decided this release.
    pub before: PipeCapacity,
    /// The reading right after the leader's exit: one point toward the bar,
    /// never the bar itself.
    pub after: Option<PipeCapacity>,
    pub family_delta: Vec<FamilyDelta>,
    pub surviving_descendants: Vec<SurvivingProcess>,
    pub outcome: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExperimentResult {
    pub verdict: Reclamation,
    pub detail: String,
    pub releases: Vec<ReleaseRecord>,
}

/// The admission latch's recovery rule over the pass's own readings.
struct RecoveryBar(PipeHealth);

impl RecoveryBar {
    /// Latched from the start: the pass runs only for a refusing floor, and a
    /// first reading above it is the start of a recovery streak, not one.
    fn latched() -> Self {
        Self(PipeHealth {
            degraded: true,
            ..PipeHealth::new(true)
        })
    }

    fn observe(&mut self, at: u64, reading: PipeCapacity) {
        self.0.observe(Measurement::measured(at, reading));
    }

    /// Whether admission would reopen on these readings now.
    fn holds(&self, now: u64) -> bool {
        !self.0.degraded && self.0.assess(now).admission == PipeAdmission::Healthy
    }
}

/// Why a pass ended.
enum Ending {
    /// Admission reopens: the monitor cleared on its own readings.
    Cleared,
    /// Admission reopens: the bar held on the pass's own readings.
    BarHeld,
    /// The hold is on expired evidence, where nothing is released.
    Expired,
    BudgetSpent,
    PoolExhausted {
        active: usize,
    },
    Stopped(String),
}

fn still_degraded(latest: Option<PipeCapacity>) -> String {
    match latest {
        Some(reading) => format!(
            "the host is still below the {RECOVERED_AT_BYTES} B recovery bar (latest fresh pipe {})",
            reading.describe()
        ),
        None => "the host is still degraded".into(),
    }
}

impl Ending {
    fn describe(&self, latest: Option<PipeCapacity>) -> String {
        match self {
            Self::Cleared => "the floor cleared on the monitor's own readings".into(),
            Self::BarHeld => format!(
                "{RECOVERY_SAMPLES} readings {} s apart reached {RECOVERED_AT_BYTES} B, the bar \
                 admission reopens on",
                RECOVERY_SPACING_MS / 1_000
            ),
            Self::Expired => "the hold is on expired evidence, not a degraded pipe".into(),
            Self::BudgetSpent => format!(
                "the budget of {MAX_RELEASES_PER_ENGAGEMENT} releases per engagement is spent and \
                 {}",
                still_degraded(latest)
            ),
            Self::PoolExhausted { active } => format!(
                "no idle, unowned, unprotected session is left to release and {}, with {active} \
                 active session{} holding processes",
                still_degraded(latest),
                if *active == 1 { "" } else { "s" }
            ),
            Self::Stopped(why) => why.clone(),
        }
    }
}

/// Run one engagement's release pass. `window` holds the release times of
/// recent passes, so the per-window bound spans engagements.
pub(crate) fn run_release_pass(
    world: &mut impl ReleaseWorld,
    initial: Attribution,
    window: &mut VecDeque<u64>,
) -> ExperimentResult {
    let mut releases: Vec<ReleaseRecord> = Vec::new();
    let mut tried: HashSet<String> = HashSet::new();
    let mut refused = 0usize;
    let mut census = initial;
    let mut bar = RecoveryBar::latched();
    let mut latest: Option<PipeCapacity> = None;
    // The last release's after reading, which is also the next decision's.
    let mut pending: Option<PipeCapacity> = None;
    let ending = loop {
        let floor = world.floor();
        if floor.cleared {
            break Ending::Cleared;
        }
        if matches!(floor.phase, Some(HoldPhase::Stale) | None) {
            break Ending::Expired;
        }
        let reading = match pending.take() {
            Some(reading) => reading,
            None => match world.probe() {
                Ok(reading) => {
                    let at = world.now_ms();
                    bar.observe(at, reading);
                    reading
                }
                Err(error) => break Ending::Stopped(format!("a reading failed: {error}")),
            },
        };
        latest = Some(reading);
        if bar.holds(world.now_ms()) {
            break Ending::BarHeld;
        }
        // At the recovery level: the next reading, spaced as admission spaces
        // its own, decides whether this one was recovery or a moment's room.
        if reading.accepted_bytes >= RECOVERED_AT_BYTES {
            world.wait(RECOVERY_SPACING_MS);
            continue;
        }
        if releases.len() >= MAX_RELEASES_PER_ENGAGEMENT {
            break Ending::BudgetSpent;
        }
        let now = world.now_ms();
        while window
            .front()
            .is_some_and(|at| now.saturating_sub(*at) >= RELEASE_WINDOW_MS)
        {
            window.pop_front();
        }
        if window.len() >= MAX_RELEASES_PER_WINDOW {
            let frees_at = window.front().copied().unwrap_or(now) + RELEASE_WINDOW_MS;
            world.wait(frees_at.saturating_sub(now).max(1));
            continue;
        }
        if refused >= MAX_REFUSED_CANDIDATES {
            break Ending::Stopped(format!(
                "{MAX_REFUSED_CANDIDATES} candidates were refused by their fences"
            ));
        }
        let Some(candidate) = world
            .candidates()
            .into_iter()
            .find(|candidate| !tried.contains(&candidate.run_id))
        else {
            break Ending::PoolExhausted {
                active: world.active_sessions(),
            };
        };
        tried.insert(candidate.run_id.clone());
        let descendants: Vec<ProcessIdentity> = census
            .trees
            .get(&candidate.run_id)
            .map(|tree| {
                tree.iter()
                    .copied()
                    .filter(|process| *process != candidate.leader)
                    .collect()
            })
            .unwrap_or_default();
        match world.retire(&candidate) {
            RetireAttempt::Released => {}
            RetireAttempt::NotReleasable(_) => {
                refused += 1;
                continue;
            }
            RetireAttempt::Failed(error) => {
                break Ending::Stopped(format!(
                    "releasing run {} failed: {error}",
                    candidate.run_id
                ))
            }
        }
        let released_at_ms = world.now_ms();
        window.push_back(released_at_ms);
        let leader_exit_confirmed = world.await_exit(candidate.leader);
        let after = world.probe();
        if let Ok(reading) = &after {
            let at = world.now_ms();
            bar.observe(at, *reading);
        }
        let after_census = world.census();
        let (family_delta, surviving_descendants) = match &after_census {
            Ok(after_census) => (
                family_delta(&census, after_census),
                descendants
                    .iter()
                    .filter_map(|process| {
                        after_census
                            .processes
                            .get(process)
                            .map(|counted| SurvivingProcess {
                                pid: process.pid,
                                family: counted.family.clone(),
                                pipe_fds: counted.pipe_fds,
                            })
                    })
                    .collect(),
            ),
            Err(_) => (Vec::new(), Vec::new()),
        };
        let mut record = ReleaseRecord {
            seq: releases.len() + 1,
            candidate,
            released_at_ms,
            leader_exit_confirmed,
            before: reading,
            after: after.as_ref().ok().copied(),
            family_delta,
            surviving_descendants,
            outcome: "released".into(),
        };
        let stop = match (&after, &after_census) {
            (Err(error), _) => Some(format!("the after reading failed: {error}")),
            (_, Err(error)) => Some(format!("the after census failed: {error}")),
            _ if !leader_exit_confirmed => {
                Some("the leader's exit was not confirmed in time".to_string())
            }
            _ => None,
        };
        if let Some(reason) = &stop {
            record.outcome = format!("released; stopped: {reason}");
        }
        world.record(&record);
        releases.push(record);
        if let Ok(after_census) = after_census {
            census = after_census;
        }
        if let Some(reason) = stop {
            break Ending::Stopped(reason);
        }
        pending = after.ok();
    };
    let verdict = verdict(&ending, &releases);
    let why = ending.describe(latest);
    let detail = if verdict == Reclamation::Skipped {
        match ending {
            Ending::BarHeld => {
                format!("the host reached the recovery bar without a release: {why}")
            }
            Ending::Cleared => "the floor had already cleared".into(),
            _ => why,
        }
    } else {
        format!("{}; stopped: {why}", describe_releases(&releases))
    };
    ExperimentResult {
        verdict,
        detail,
        releases,
    }
}

/// The pass's answer from why it ended and what it released.
///
/// - recovered: admission reopens after at least one release, on the bar's
///   spaced readings or the monitor's own clearance. One reading above the
///   floor between releases is not enough;
/// - not recovered: the whole budget was released with every leader's exit
///   confirmed, and no after reading left the degraded range;
/// - skipped: nothing was released because nothing was asked of the pass
///   (the floor cleared first, or the hold is on expired evidence);
/// - inconclusive: anything else, including a pass that ran out of idle
///   sessions or budget with the host still refusing.
fn verdict(ending: &Ending, releases: &[ReleaseRecord]) -> Reclamation {
    let reopened = matches!(ending, Ending::Cleared | Ending::BarHeld);
    if releases.is_empty() && (reopened || matches!(ending, Ending::Expired)) {
        return Reclamation::Skipped;
    }
    if reopened {
        return Reclamation::Recovered;
    }
    let flat = releases.len() == MAX_RELEASES_PER_ENGAGEMENT
        && releases.iter().all(|release| {
            release.leader_exit_confirmed
                && release
                    .after
                    .is_some_and(|after| after.accepted_bytes < DEGRADED_BELOW_BYTES)
        });
    if flat {
        Reclamation::NotRecovered
    } else {
        Reclamation::Inconclusive
    }
}

/// `3 releases, 512→512 B, 512→768 B, 512→512 B`.
pub(crate) fn describe_releases(releases: &[ReleaseRecord]) -> String {
    if releases.is_empty() {
        return "no releases".into();
    }
    let readings: Vec<String> = releases
        .iter()
        .map(|release| {
            format!(
                "{}→{}",
                release.before.accepted_bytes,
                release
                    .after
                    .map_or("?".to_string(), |after| after.describe())
            )
        })
        .collect();
    format!(
        "{} release{}, {}",
        releases.len(),
        if releases.len() == 1 { "" } else { "s" },
        readings.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_procmem::identity::StartKey;

    struct Script {
        floor: Vec<FloorNow>,
        readings: VecDeque<u64>,
        candidates: Vec<Candidate>,
        refuse: HashSet<String>,
        fail: HashSet<String>,
        unconfirmed: HashSet<String>,
        active: usize,
        now: u64,
        tick_ms: u64,
        retired: Vec<String>,
        recorded: usize,
    }

    const DEGRADED: FloorNow = FloorNow {
        cleared: false,
        phase: Some(HoldPhase::Degraded),
    };

    fn candidate(run: &str, pid: u32) -> Candidate {
        Candidate {
            run_id: run.into(),
            job_id: Some(format!("job-{run}")),
            session_id: None,
            family: "claude-cli".into(),
            leader: ProcessIdentity {
                pid,
                start_time: StartKey(pid as u64, 0),
            },
        }
    }

    impl Script {
        fn new(readings: &[u64], candidates: usize) -> Self {
            Self {
                floor: vec![DEGRADED],
                readings: readings.iter().copied().collect(),
                candidates: (0..candidates)
                    .map(|index| candidate(&format!("run-{index}"), 100 + index as u32))
                    .collect(),
                refuse: HashSet::new(),
                fail: HashSet::new(),
                unconfirmed: HashSet::new(),
                active: 0,
                now: 1_000_000,
                tick_ms: 1_000,
                retired: Vec::new(),
                recorded: 0,
            }
        }
    }

    impl ReleaseWorld for Script {
        fn floor(&mut self) -> FloorNow {
            if self.floor.len() > 1 {
                self.floor.remove(0)
            } else {
                self.floor[0]
            }
        }
        fn probe(&mut self) -> Result<PipeCapacity, String> {
            let bytes = self.readings.pop_front().ok_or("no reading scripted")?;
            Ok(PipeCapacity {
                accepted_bytes: bytes,
                at_least: false,
            })
        }
        fn census(&mut self) -> Result<Attribution, String> {
            Ok(Attribution::default())
        }
        fn candidates(&mut self) -> Vec<Candidate> {
            self.candidates
                .iter()
                .filter(|candidate| !self.retired.contains(&candidate.run_id))
                .cloned()
                .collect()
        }
        fn retire(&mut self, candidate: &Candidate) -> RetireAttempt {
            if self.refuse.contains(&candidate.run_id) {
                return RetireAttempt::NotReleasable("a launch holds its job".into());
            }
            if self.fail.contains(&candidate.run_id) {
                return RetireAttempt::Failed("teardown failed".into());
            }
            self.retired.push(candidate.run_id.clone());
            RetireAttempt::Released
        }
        fn await_exit(&mut self, leader: ProcessIdentity) -> bool {
            !self.unconfirmed.iter().any(|run| {
                self.candidates
                    .iter()
                    .any(|c| &c.run_id == run && c.leader == leader)
            })
        }
        fn active_sessions(&mut self) -> usize {
            self.active
        }
        fn wait(&mut self, ms: u64) {
            self.now += ms;
        }
        fn now_ms(&mut self) -> u64 {
            self.now += self.tick_ms;
            self.now
        }
        fn record(&mut self, _: &ReleaseRecord) {
            self.recorded += 1;
        }
    }

    fn pass(script: &mut Script, window: &mut VecDeque<u64>) -> ExperimentResult {
        run_release_pass(script, Attribution::default(), window)
    }

    /// No more than the window's releases in any window, whatever the passes.
    fn assert_rate_bound(times: &[u64]) {
        for pair in times.windows(MAX_RELEASES_PER_WINDOW + 1) {
            assert!(
                pair[MAX_RELEASES_PER_WINDOW] - pair[0] >= RELEASE_WINDOW_MS,
                "{} releases inside {} ms: {times:?}",
                MAX_RELEASES_PER_WINDOW + 1,
                RELEASE_WINDOW_MS
            );
        }
    }

    fn times(releases: &[ReleaseRecord]) -> Vec<u64> {
        releases
            .iter()
            .map(|release| release.released_at_ms)
            .collect()
    }

    #[test]
    fn a_host_that_needs_four_releases_gets_four_within_one_engagement() {
        // Three releases leave it below the recovery level (the third only
        // between the floor and the bar), the rate bound makes the fourth wait,
        // and two spaced readings at the bar end the pass.
        let mut script = Script::new(&[512, 512, 512, 8_192, 1_024, 65_536, 65_536], 8);
        let result = pass(&mut script, &mut VecDeque::new());
        assert_eq!(result.verdict, Reclamation::Recovered, "{}", result.detail);
        assert_eq!(result.releases.len(), 4);
        assert_eq!(script.retired.len(), 4);
        assert_eq!(script.recorded, 4);
        assert_rate_bound(&times(&result.releases));
        assert!(
            result
                .detail
                .contains("2 readings 4 s apart reached 16384 B"),
            "{}",
            result.detail
        );
        assert!(
            script.readings.is_empty(),
            "the confirming reading was taken"
        );
    }

    #[test]
    fn a_flapping_host_is_not_declared_recovered() {
        // The 2026-11-23 specimen: every release reads at least 64 KiB right
        // after its leader exits, and the next spaced reading is back at
        // 512 B. The pass keeps releasing to its budget and says the host is
        // still degraded.
        let mut readings = vec![512];
        for _ in 0..MAX_RELEASES_PER_ENGAGEMENT {
            readings.extend([65_536, 512]);
        }
        let mut script = Script::new(&readings, 40);
        let result = pass(&mut script, &mut VecDeque::new());
        assert_ne!(result.verdict, Reclamation::Recovered, "{}", result.detail);
        assert_eq!(result.verdict, Reclamation::Inconclusive);
        assert_eq!(result.releases.len(), MAX_RELEASES_PER_ENGAGEMENT);
        assert_rate_bound(&times(&result.releases));
        assert!(
            result.detail.contains(&format!(
                "stopped: the budget of {MAX_RELEASES_PER_ENGAGEMENT} releases per engagement is \
                 spent and the host is still below the 16384 B recovery bar (latest fresh pipe \
                 512 B)"
            )),
            "{}",
            result.detail
        );

        // A reading between the floor and the bar is not recovery either: it
        // releases again rather than ending the pass.
        let mut between = Script::new(&[512, 8_192, 8_192, 65_536, 65_536], 5);
        let result = pass(&mut between, &mut VecDeque::new());
        assert_eq!(result.releases.len(), 3);
        assert_eq!(result.verdict, Reclamation::Recovered);
    }

    #[test]
    fn an_exhausted_idle_pool_says_so_naming_the_active_sessions() {
        let mut script = Script::new(&[512, 512, 2_048], 2);
        script.active = 20;
        let result = pass(&mut script, &mut VecDeque::new());
        assert_eq!(result.releases.len(), 2);
        assert_eq!(result.verdict, Reclamation::Inconclusive);
        assert!(
            result.detail.ends_with(
                "stopped: no idle, unowned, unprotected session is left to release and the host \
                 is still below the 16384 B recovery bar (latest fresh pipe 2048 B), with 20 \
                 active sessions holding processes"
            ),
            "{}",
            result.detail
        );
        assert!(!result.detail.contains("recovered"), "{}", result.detail);
    }

    #[test]
    fn a_whole_flat_budget_is_not_recovered_and_one_more_is_never_made() {
        let mut script = Script::new(&[512; MAX_RELEASES_PER_ENGAGEMENT + 1], 20);
        script.tick_ms = 10_000;
        let result = pass(&mut script, &mut VecDeque::new());
        assert_eq!(result.verdict, Reclamation::NotRecovered);
        assert_eq!(result.releases.len(), MAX_RELEASES_PER_ENGAGEMENT);
        assert_eq!(script.retired.len(), MAX_RELEASES_PER_ENGAGEMENT);
        assert_eq!(script.recorded, MAX_RELEASES_PER_ENGAGEMENT);
        assert!(
            result.detail.contains("10 releases, 512→512 B"),
            "{}",
            result.detail
        );
        assert!(result.detail.contains("budget of 10"), "{}", result.detail);
    }

    #[test]
    fn no_more_than_three_releases_in_fifteen_seconds_across_engagements() {
        let mut window = VecDeque::new();
        let mut first = Script::new(&[512, 512, 512], 2);
        let first_result = pass(&mut first, &mut window);
        assert_eq!(first_result.releases.len(), 2);
        // The next engagement opens inside the same window: its first release
        // fits, its second waits for the window to free.
        let mut second = Script::new(&[512, 512, 512, 512, 512], 2);
        second.now = first.now;
        let second_result = pass(&mut second, &mut window);
        assert_eq!(second_result.releases.len(), 2);
        let mut all = times(&first_result.releases);
        all.extend(times(&second_result.releases));
        assert_rate_bound(&all);
    }

    #[test]
    fn a_recovered_reading_is_confirmed_before_the_pass_ends() {
        // 512 -> 64 KiB, then a spaced 64 KiB: one release, and recovered.
        let mut once = Script::new(&[512, 65_536, 65_536], 3);
        let result = pass(&mut once, &mut VecDeque::new());
        assert_eq!(result.verdict, Reclamation::Recovered);
        assert_eq!(once.retired.len(), 1);

        // A host already at the bar when the pass starts releases nothing.
        let mut healthy = Script::new(&[65_536, 65_536], 3);
        let result = pass(&mut healthy, &mut VecDeque::new());
        assert!(healthy.retired.is_empty(), "{:?}", healthy.retired);
        assert_eq!(result.verdict, Reclamation::Skipped);
        assert!(
            result.detail.contains("without a release"),
            "{}",
            result.detail
        );

        // The monitor clearing mid-pass ends it as recovered.
        let mut cleared = Script::new(&[512, 512, 512], 3);
        cleared.floor = vec![
            DEGRADED,
            FloorNow {
                cleared: true,
                phase: None,
            },
        ];
        let result = pass(&mut cleared, &mut VecDeque::new());
        assert_eq!(cleared.retired.len(), 1);
        assert_eq!(result.verdict, Reclamation::Recovered);
        assert!(
            result.detail.contains("monitor's own readings"),
            "{}",
            result.detail
        );
    }

    #[test]
    fn a_fenced_candidate_costs_no_budget_and_refusals_are_bounded() {
        let mut script = Script::new(&[512; 20], 7);
        script.tick_ms = 10_000;
        script.refuse.insert("run-0".into());
        script.refuse.insert("run-1".into());
        let result = pass(&mut script, &mut VecDeque::new());
        assert_eq!(result.releases.len(), 5, "two refusals took no budget");
        assert_eq!(
            result
                .releases
                .iter()
                .map(|release| release.candidate.run_id.as_str())
                .collect::<Vec<_>>(),
            vec!["run-2", "run-3", "run-4", "run-5", "run-6"]
        );

        let mut stubborn = Script::new(&[512; 20], 7);
        for index in 0..7 {
            stubborn.refuse.insert(format!("run-{index}"));
        }
        let result = pass(&mut stubborn, &mut VecDeque::new());
        assert!(result.releases.is_empty());
        assert!(
            result.detail.contains("5 candidates were refused"),
            "{}",
            result.detail
        );
        assert_eq!(result.verdict, Reclamation::Inconclusive);
    }

    #[test]
    fn an_unconfirmed_exit_or_a_failed_teardown_stops_the_pass() {
        let mut script = Script::new(&[512, 512, 512, 512], 3);
        script.unconfirmed.insert("run-0".into());
        let result = pass(&mut script, &mut VecDeque::new());
        assert_eq!(result.releases.len(), 1);
        assert!(!result.releases[0].leader_exit_confirmed);
        assert!(result.detail.contains("not confirmed"), "{}", result.detail);
        assert_eq!(result.verdict, Reclamation::Inconclusive);

        let mut failing = Script::new(&[512, 512], 3);
        failing.fail.insert("run-0".into());
        let result = pass(&mut failing, &mut VecDeque::new());
        assert!(result.releases.is_empty());
        assert!(result.detail.contains("failed"), "{}", result.detail);
    }

    #[test]
    fn a_cleared_or_expired_floor_or_no_candidate_releases_nothing() {
        let mut cleared = Script::new(&[512], 1);
        cleared.floor = vec![FloorNow {
            cleared: true,
            phase: None,
        }];
        let result = pass(&mut cleared, &mut VecDeque::new());
        assert_eq!(result.verdict, Reclamation::Skipped);
        assert_eq!(result.detail, "the floor had already cleared");
        let mut stale = Script::new(&[512], 1);
        stale.floor = vec![FloorNow {
            cleared: false,
            phase: Some(HoldPhase::Stale),
        }];
        assert_eq!(
            pass(&mut stale, &mut VecDeque::new()).verdict,
            Reclamation::Skipped
        );
        let mut empty = Script::new(&[512], 0);
        let result = pass(&mut empty, &mut VecDeque::new());
        assert_eq!(result.verdict, Reclamation::Inconclusive);
        assert!(result
            .detail
            .contains("no idle, unowned, unprotected session"));
    }

    #[test]
    fn descendants_alive_after_the_release_are_named() {
        use super::super::census::CountedProcess;
        let leader = candidate("run-0", 100).leader;
        let child = ProcessIdentity {
            pid: 101,
            start_time: StartKey(101, 0),
        };
        let mut initial = Attribution::default();
        initial.trees.insert("run-0".into(), vec![leader, child]);
        struct Orphaning(Script, ProcessIdentity);
        impl ReleaseWorld for Orphaning {
            fn floor(&mut self) -> FloorNow {
                self.0.floor()
            }
            fn probe(&mut self) -> Result<PipeCapacity, String> {
                self.0.probe()
            }
            fn census(&mut self) -> Result<Attribution, String> {
                let mut after = Attribution::default();
                after.processes.insert(
                    self.1,
                    CountedProcess {
                        family: "node".into(),
                        pipe_fds: 6,
                    },
                );
                Ok(after)
            }
            fn candidates(&mut self) -> Vec<Candidate> {
                self.0.candidates()
            }
            fn retire(&mut self, candidate: &Candidate) -> RetireAttempt {
                self.0.retire(candidate)
            }
            fn await_exit(&mut self, leader: ProcessIdentity) -> bool {
                self.0.await_exit(leader)
            }
            fn active_sessions(&mut self) -> usize {
                self.0.active_sessions()
            }
            fn wait(&mut self, ms: u64) {
                self.0.wait(ms)
            }
            fn now_ms(&mut self) -> u64 {
                self.0.now_ms()
            }
            fn record(&mut self, release: &ReleaseRecord) {
                self.0.record(release)
            }
        }
        let mut world = Orphaning(Script::new(&[512, 512], 1), child);
        let result = run_release_pass(&mut world, initial, &mut VecDeque::new());
        assert_eq!(
            result.releases[0].surviving_descendants,
            vec![SurvivingProcess {
                pid: 101,
                family: "node".into(),
                pipe_fds: 6
            }]
        );
    }
}
