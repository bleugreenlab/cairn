//! The bounded release experiment (cairn/6440): while the floor is engaged in
//! its degraded phase, release idle, unowned, unprotected sessions one at a
//! time through the canonical Keep-policy retire, and measure whether fresh
//! pipes recover.
//!
//! Pure over an injected [`ReleaseWorld`], so every bound is tested without
//! processes. The live world is in the parent module.

use std::collections::{HashSet, VecDeque};

use cairn_common::pipe_health::{
    HoldPhase, PipeCapacity, DEGRADED_BELOW_BYTES, RECOVERED_AT_BYTES,
};
use cairn_procmem::ProcessIdentity;
use serde::{Deserialize, Serialize};

use super::census::{family_delta, Attribution, FamilyDelta};
use crate::storage::pipe_floor::Reclamation;

/// Releases one engagement may make.
pub(crate) const MAX_RELEASES_PER_ENGAGEMENT: usize = 3;
/// Releases any window may hold, across engagements.
pub(crate) const MAX_RELEASES_PER_WINDOW: usize = 3;
pub(crate) const RELEASE_WINDOW_MS: u64 = 15_000;
/// Candidates one pass may try, released or not: a candidate a fence refuses
/// costs no release budget, but the pass does not hunt forever.
pub(crate) const MAX_CANDIDATE_ATTEMPTS: usize = 5;

/// What the floor says now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FloorNow {
    pub cleared: bool,
    pub phase: Option<HoldPhase>,
}

/// An idle session the collector would evict first, with its leader's
/// identity. Pooled Codex threads are never candidates: their teardown frees
/// no process of their own, so the experiment could measure nothing.
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
    /// process) within the experiment's bound.
    fn await_exit(&mut self, leader: ProcessIdentity) -> bool;
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
    pub before: PipeCapacity,
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

/// Why a pass stopped before any release counts as a skip, not a result.
fn skip_reason(floor: FloorNow) -> Option<&'static str> {
    if floor.cleared {
        return Some("the floor had already cleared");
    }
    match floor.phase {
        Some(HoldPhase::Degraded) => None,
        Some(HoldPhase::Recovering) => {
            Some("the floor is in recovery hysteresis, where nothing is released")
        }
        Some(HoldPhase::Stale) | None => {
            Some("the hold is on expired evidence, not a degraded pipe")
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
    let mut census = initial;
    let stop: String = loop {
        let floor = world.floor();
        if let Some(reason) = skip_reason(floor) {
            if releases.is_empty() {
                return ExperimentResult {
                    verdict: Reclamation::Skipped,
                    detail: reason.to_string(),
                    releases,
                };
            }
            break format!("stopped: {reason}");
        }
        if releases.len() >= MAX_RELEASES_PER_ENGAGEMENT {
            break format!(
                "stopped after {MAX_RELEASES_PER_ENGAGEMENT} releases, the per-engagement bound"
            );
        }
        let now = world.now_ms();
        while window
            .front()
            .is_some_and(|at| now.saturating_sub(*at) >= RELEASE_WINDOW_MS)
        {
            window.pop_front();
        }
        if window.len() >= MAX_RELEASES_PER_WINDOW {
            break format!(
                "stopped: {MAX_RELEASES_PER_WINDOW} releases in the last {} s",
                RELEASE_WINDOW_MS / 1_000
            );
        }
        if tried.len() >= MAX_CANDIDATE_ATTEMPTS {
            break format!("stopped: {MAX_CANDIDATE_ATTEMPTS} candidates tried");
        }
        let Some(candidate) = world
            .candidates()
            .into_iter()
            .find(|candidate| !tried.contains(&candidate.run_id))
        else {
            break "stopped: no idle, unowned, unprotected session left to release".into();
        };
        tried.insert(candidate.run_id.clone());
        let before = match world.probe() {
            Ok(reading) => reading,
            Err(error) => break format!("stopped: the before reading failed: {error}"),
        };
        // The monitor lags its own five-second cadence; the experiment's fresh
        // reading is the newer evidence. At or above the floor the host is
        // recovering, where nothing is released.
        if before.accepted_bytes >= DEGRADED_BELOW_BYTES {
            let reason = format!(
                "a fresh reading of {} is at or above the {DEGRADED_BELOW_BYTES} B floor, where \
                 nothing is released",
                before.describe()
            );
            if releases.is_empty() {
                return ExperimentResult {
                    verdict: Reclamation::Skipped,
                    detail: reason,
                    releases,
                };
            }
            break format!("stopped: {reason}");
        }
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
            RetireAttempt::NotReleasable(_) => continue,
            RetireAttempt::Failed(error) => {
                break format!(
                    "stopped: releasing run {} failed: {error}",
                    candidate.run_id
                )
            }
        }
        let released_at_ms = world.now_ms();
        window.push_back(released_at_ms);
        let leader_exit_confirmed = world.await_exit(candidate.leader);
        let after = world.probe();
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
            before,
            after: after.as_ref().ok().copied(),
            family_delta,
            surviving_descendants,
            outcome: "released".into(),
        };
        let stop = match (&after, &after_census) {
            (Err(error), _) => Some(format!("stopped: the after reading failed: {error}")),
            (_, Err(error)) => Some(format!("stopped: the after census failed: {error}")),
            _ if !leader_exit_confirmed => {
                Some("stopped: the leader's exit was not confirmed in time".to_string())
            }
            (Ok(reading), _) if reading.accepted_bytes >= DEGRADED_BELOW_BYTES => Some(format!(
                "stopped: an after reading of {} is at or above the {DEGRADED_BELOW_BYTES} B \
                 floor; recovery hysteresis from here",
                reading.describe()
            )),
            _ => None,
        };
        if let Some(reason) = &stop {
            record.outcome = format!("released; {reason}");
        }
        world.record(&record);
        releases.push(record);
        if let Ok(after_census) = after_census {
            census = after_census;
        }
        if let Some(reason) = stop {
            break reason;
        }
    };
    let verdict = verdict(&releases);
    ExperimentResult {
        detail: format!("{}; {stop}", describe_releases(&releases)),
        verdict,
        releases,
    }
}

/// The experiment's answer from its releases.
///
/// - recovered: some release took a reading below the floor to one at or
///   above the recovery level;
/// - not recovered: every allowed release completed with its leader's exit
///   confirmed, and no after reading left the degraded range;
/// - inconclusive: anything else. One flat release does not disprove a
///   shared quota, and a reading between the floor and the recovery level
///   proves nothing either way.
pub(crate) fn verdict(releases: &[ReleaseRecord]) -> Reclamation {
    let recovered = releases.iter().any(|release| {
        release.before.accepted_bytes < DEGRADED_BELOW_BYTES
            && release
                .after
                .is_some_and(|after| after.accepted_bytes >= RECOVERED_AT_BYTES)
    });
    if recovered {
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

    #[test]
    fn three_flat_releases_are_not_recovered_and_the_fourth_is_never_made() {
        let mut script = Script::new(&[512, 512, 512, 512, 512, 512], 5);
        script.tick_ms = 10_000;
        let result = pass(&mut script, &mut VecDeque::new());
        assert_eq!(result.verdict, Reclamation::NotRecovered);
        assert_eq!(result.releases.len(), 3);
        assert_eq!(script.retired.len(), 3);
        assert_eq!(script.recorded, 3);
        assert!(
            result.detail.contains("3 releases, 512→512 B"),
            "{}",
            result.detail
        );
        assert!(
            result.detail.contains("per-engagement bound"),
            "{}",
            result.detail
        );
    }

    #[test]
    fn no_more_than_three_releases_in_fifteen_seconds_across_engagements() {
        let mut window = VecDeque::new();
        let mut first = Script::new(&[512, 512, 512, 512], 5);
        first.tick_ms = 1_000;
        let result = pass(&mut first, &mut window);
        assert_eq!(result.releases.len(), 2);
        let mut second = Script::new(&[512, 512, 512, 512], 5);
        second.now = first.now;
        second.tick_ms = 1_000;
        let result = pass(&mut second, &mut window);
        assert_eq!(result.releases.len(), 1, "the window holds three");
        assert!(
            result.detail.contains("in the last 15 s"),
            "{}",
            result.detail
        );
        assert_eq!(result.verdict, Reclamation::Inconclusive);
    }

    #[test]
    fn nothing_is_released_while_recovering_or_after_a_recovered_reading() {
        let mut recovering = Script::new(&[65_536], 2);
        recovering.floor = vec![FloorNow {
            cleared: false,
            phase: Some(HoldPhase::Recovering),
        }];
        let result = pass(&mut recovering, &mut VecDeque::new());
        assert_eq!(result.verdict, Reclamation::Skipped);
        assert!(recovering.retired.is_empty());
        assert!(result.detail.contains("recovery hysteresis"));

        let mut recovered = Script::new(&[512, 65_536, 512, 512], 3);
        let result = pass(&mut recovered, &mut VecDeque::new());
        assert_eq!(result.verdict, Reclamation::Recovered);
        assert_eq!(recovered.retired.len(), 1, "no release after recovery");

        // The floor moving into recovery mid-pass also stops it.
        let mut moved = Script::new(&[512, 8_192, 512, 512], 3);
        moved.floor = vec![
            DEGRADED,
            FloorNow {
                cleared: false,
                phase: Some(HoldPhase::Recovering),
            },
        ];
        let result = pass(&mut moved, &mut VecDeque::new());
        assert_eq!(moved.retired.len(), 1);
        assert_eq!(
            result.verdict,
            Reclamation::Inconclusive,
            "8 KiB proves nothing"
        );
    }

    #[test]
    fn a_fresh_reading_above_the_floor_releases_nothing_whatever_the_monitor_says() {
        // The monitor still reads degraded, but the experiment's own before
        // reading is already healthy.
        let mut healthy = Script::new(&[65_536], 3);
        let result = pass(&mut healthy, &mut VecDeque::new());
        assert!(healthy.retired.is_empty(), "{:?}", healthy.retired);
        assert_eq!(result.verdict, Reclamation::Skipped);
        assert!(
            result.detail.contains("at or above the 4096 B floor"),
            "{}",
            result.detail
        );

        // 512 -> 8192 B: the host left the degraded range, so a second
        // candidate is not retired before the sampler catches up.
        let mut recovering = Script::new(&[512, 8_192, 8_192, 8_192], 3);
        let result = pass(&mut recovering, &mut VecDeque::new());
        assert_eq!(recovering.retired, vec!["run-0".to_string()]);
        assert_eq!(result.releases.len(), 1);
        assert_eq!(result.verdict, Reclamation::Inconclusive);
        assert!(
            result.detail.contains("recovery hysteresis"),
            "{}",
            result.detail
        );
    }

    #[test]
    fn a_fenced_candidate_costs_no_budget_and_attempts_are_bounded() {
        let mut script = Script::new(&[512; 12], 7);
        script.tick_ms = 10_000;
        script.refuse.insert("run-0".into());
        script.refuse.insert("run-1".into());
        let result = pass(&mut script, &mut VecDeque::new());
        assert_eq!(result.releases.len(), 3, "two refusals took no budget");
        assert_eq!(
            result
                .releases
                .iter()
                .map(|release| release.candidate.run_id.as_str())
                .collect::<Vec<_>>(),
            vec!["run-2", "run-3", "run-4"]
        );

        let mut stubborn = Script::new(&[512; 12], 7);
        for index in 0..7 {
            stubborn.refuse.insert(format!("run-{index}"));
        }
        let result = pass(&mut stubborn, &mut VecDeque::new());
        assert!(result.releases.is_empty());
        assert!(
            result.detail.contains("5 candidates tried"),
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
    fn a_cleared_floor_or_no_candidate_releases_nothing() {
        let mut cleared = Script::new(&[512], 1);
        cleared.floor = vec![FloorNow {
            cleared: true,
            phase: None,
        }];
        assert_eq!(
            pass(&mut cleared, &mut VecDeque::new()).verdict,
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
