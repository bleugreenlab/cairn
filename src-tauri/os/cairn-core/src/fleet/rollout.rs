//! Rolling executor self-update: which remote machine the runner empties and
//! directs to install the executor build it serves, one machine at a time.
//!
//! Two mismatches reach a resident executor and they are handled differently on
//! purpose. A protocol mismatch is refused at `Hello` and repaired by the
//! executor at once, because an incompatible executor can do no useful work. A
//! build mismatch is a machine that works but is not running the code the runner
//! would install: an executor fix merged without a protocol bump. That one is
//! scheduled here, so it costs the fleet nothing it was doing.
//!
//! The schedule has three properties, each against a specific failure:
//!
//! - **One machine at a time.** A bad artifact strands the one machine that
//!   took it, and the failure halts the rollout of that build before it
//!   reaches the next.
//! - **Empty before restart.** The machine whose turn it is is refused new
//!   placement, and the directive goes out only once the runner's queue binds
//!   nothing to it and two snapshots taken after the hold show nothing in
//!   flight, so a restart kills no work.
//! - **No machine holds the fleet hostage.** A machine still busy after the
//!   drain budget takes work again and its turn passes to the next; one that
//!   ignores a directive is excluded from that build without halting it.
//!
//! This module is the decision alone, over plain facts the fleet copies out of
//! its connection table, so every transition is testable without a link.

use std::collections::HashMap;

use cairn_common::executor_protocol::{
    SelfUpdatePhase, SelfUpdateStatus, UpdateReport, UpdateState,
};

/// How long a machine may take to empty before its turn passes to the next.
pub(crate) const DRAIN_BUDGET_MS: u64 = 15 * 60 * 1000;
/// How long a machine whose drain overran keeps its work before it is asked
/// again.
pub(crate) const DEFER_MS: u64 = 30 * 60 * 1000;
/// How long a directed machine may stay on the link it was directed on. The
/// executor ends that link as soon as it reads the directive, so a machine
/// still there after this did not act on it.
pub(crate) const ACT_BUDGET_MS: u64 = 2 * 60 * 1000;
/// How long a directed machine may stay away before the runner concludes the
/// update stranded it: the artifact download's own fifteen-minute bound plus
/// the replacement's rollback deadline, with margin.
pub(crate) const RETURN_BUDGET_MS: u64 = 25 * 60 * 1000;
/// Snapshots that must arrive after the hold before an empty machine is
/// believed empty. An executor holds at most one outbound snapshot, so the
/// second one received after a moment was built after it.
const FRESH_SNAPSHOTS: u64 = 2;
/// How many times a directed machine may come back on its old build without
/// reporting why before the build is halted. Once covers a link that dropped
/// for an unrelated reason before the directive was read.
const UNEXPLAINED_RETURNS: u8 = 1;

/// What the runner serves for one artifact target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArtifactBuild {
    /// The build identity of the artifact, hashed the way an executor hashes
    /// itself.
    Resolved(String),
    /// Why the runner has nothing to compare against.
    Unavailable(String),
}

/// One attached remote, as the rollout reads it.
#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub executor_id: String,
    pub name: String,
    pub generation: u64,
    /// The published artifact target this machine runs, when it is one.
    pub target: Option<&'static str>,
    pub running_build: Option<String>,
    pub last_update: Option<UpdateReport>,
    /// Work bound to or running on this machine that a restart would end.
    pub in_flight: u32,
    pub snapshots_received: u64,
    /// Whether the link is open and fresh enough to carry a directive.
    pub reachable: bool,
}

/// A directive to send now, on exactly this generation's link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Directive {
    pub executor_id: String,
    pub generation: u64,
    pub build_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransitionKind {
    /// Its turn began: new work is refused there.
    Draining,
    /// Empty; the directive was sent.
    Directed,
    /// It came back on the runner's build.
    Updated,
    /// An update to this build failed; no machine is directed to it.
    Halted,
    /// It did not empty in time and takes work again.
    Deferred,
    /// It did not act on the directive; other machines still update.
    Excluded,
    /// The turn ended without an outcome: the machine left during its drain,
    /// came back unexplained, or the runner now serves a different build.
    Abandoned,
}

impl TransitionKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Draining => "draining",
            Self::Directed => "directed",
            Self::Updated => "updated",
            Self::Halted => "halted",
            Self::Deferred => "deferred",
            Self::Excluded => "excluded",
            Self::Abandoned => "abandoned",
        }
    }
}

/// One change in the rollout, for the runner's event journal and log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Transition {
    pub executor_id: String,
    pub kind: TransitionKind,
    pub build_id: String,
    pub cause: String,
}

/// What one pass decided.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Step {
    /// The one machine to refuse new placement, if any.
    pub hold: Option<String>,
    pub direct: Option<Directive>,
    pub transitions: Vec<Transition>,
}

#[derive(Debug, Clone)]
struct Turn {
    executor_id: String,
    name: String,
    build_id: String,
    target: &'static str,
    unexplained_returns: u8,
    phase: TurnPhase,
}

#[derive(Debug, Clone)]
enum TurnPhase {
    Draining {
        since_unix_ms: u64,
        generation: u64,
        snapshots_at_hold: u64,
        in_flight: u32,
    },
    Directed {
        at_unix_ms: u64,
        generation: u64,
    },
}

/// The runner's rolling-update state. In memory: a runner restart begins a
/// fresh rollout, and what must survive one (a machine's failed update) is the
/// machine's own durable report, which it carries in every `Hello`.
#[derive(Debug, Default)]
pub(crate) struct Rollout {
    turn: Option<Turn>,
    /// Builds no machine is directed to, with why.
    halted: HashMap<String, String>,
    /// Machines whose drain overran, until when.
    deferred: HashMap<String, u64>,
    /// Machines that did not act on a directive, with the build and why.
    excluded: HashMap<String, (String, String)>,
}

impl Rollout {
    /// Whether this machine is the one being emptied or awaited.
    pub(crate) fn holds(&self, executor_id: &str) -> bool {
        self.turn
            .as_ref()
            .is_some_and(|turn| turn.executor_id == executor_id)
    }

    /// One pass over the attached remotes.
    pub(crate) fn advance(
        &mut self,
        candidates: &[Candidate],
        artifacts: &HashMap<String, ArtifactBuild>,
        now_unix_ms: u64,
    ) -> Step {
        let mut step = Step::default();
        self.absorb_reported_failures(candidates);
        self.deferred.retain(|_, until| *until > now_unix_ms);
        if let Some(turn) = self.turn.take() {
            self.turn = self.continue_turn(turn, candidates, artifacts, now_unix_ms, &mut step);
        }
        if self.turn.is_none() {
            if let Some((candidate, target, build_id)) = self.next(candidates, artifacts) {
                step.transitions.push(Transition {
                    executor_id: candidate.executor_id.clone(),
                    kind: TransitionKind::Draining,
                    build_id: build_id.clone(),
                    cause: format!(
                        "{} runs {} and the runner serves {build_id} for {target}; refusing it new work until it is empty",
                        candidate.name,
                        candidate.running_build.as_deref().unwrap_or("an unreported build"),
                    ),
                });
                self.turn = Some(Turn {
                    executor_id: candidate.executor_id.clone(),
                    name: candidate.name.clone(),
                    build_id,
                    target,
                    unexplained_returns: 0,
                    phase: TurnPhase::Draining {
                        since_unix_ms: now_unix_ms,
                        generation: candidate.generation,
                        snapshots_at_hold: candidate.snapshots_received,
                        in_flight: candidate.in_flight,
                    },
                });
            }
        }
        step.hold = self.turn.as_ref().map(|turn| turn.executor_id.clone());
        step
    }

    /// The directive the last pass produced never reached its link (the
    /// generation moved or the link closed first), so the turn goes back to
    /// draining on whatever link the machine has now rather than reading its
    /// return as an update that failed.
    pub(crate) fn directive_undelivered(&mut self, executor_id: &str, now_unix_ms: u64) {
        if let Some(turn) = self.turn.as_mut() {
            if turn.executor_id == executor_id {
                turn.phase = TurnPhase::Draining {
                    since_unix_ms: now_unix_ms,
                    generation: 0,
                    snapshots_at_hold: 0,
                    in_flight: 0,
                };
            }
        }
    }

    /// Where one machine stands, for `cairn://executors/<name>`.
    pub(crate) fn status(
        &self,
        candidate: &Candidate,
        artifacts: &HashMap<String, ArtifactBuild>,
    ) -> Option<SelfUpdateStatus> {
        let target = candidate.target?;
        let running = candidate.running_build.as_ref()?;
        let (runner_build_id, phase) = match artifacts.get(target)? {
            ArtifactBuild::Unavailable(reason) => (
                None,
                SelfUpdatePhase::ArtifactUnavailable {
                    reason: reason.clone(),
                },
            ),
            ArtifactBuild::Resolved(build_id) => (
                Some(build_id.clone()),
                self.phase_of(candidate, running, build_id),
            ),
        };
        Some(SelfUpdateStatus {
            target: target.to_string(),
            runner_build_id,
            phase,
        })
    }

    fn phase_of(&self, candidate: &Candidate, running: &str, build_id: &str) -> SelfUpdatePhase {
        if running == build_id {
            return SelfUpdatePhase::Current;
        }
        if let Some(turn) = self
            .turn
            .as_ref()
            .filter(|turn| turn.executor_id == candidate.executor_id && turn.build_id == build_id)
        {
            return match turn.phase {
                TurnPhase::Draining {
                    since_unix_ms,
                    in_flight,
                    ..
                } => SelfUpdatePhase::Draining {
                    in_flight,
                    since_unix_ms,
                },
                TurnPhase::Directed { at_unix_ms, .. } => SelfUpdatePhase::Directed { at_unix_ms },
            };
        }
        if let Some(cause) = self.halted.get(build_id) {
            return SelfUpdatePhase::Halted {
                cause: cause.clone(),
            };
        }
        if let Some((_, cause)) = self
            .excluded
            .get(&candidate.executor_id)
            .filter(|(excluded_build, _)| excluded_build == build_id)
        {
            return SelfUpdatePhase::Excluded {
                cause: cause.clone(),
            };
        }
        if let Some(until_unix_ms) = self.deferred.get(&candidate.executor_id) {
            return SelfUpdatePhase::Deferred {
                until_unix_ms: *until_unix_ms,
            };
        }
        SelfUpdatePhase::Waiting {
            behind: self.turn.as_ref().map(|turn| turn.name.clone()),
        }
    }

    /// A machine's own durable report of an update that did not take halts
    /// the build it was taking, whoever directed it. This is what survives a
    /// runner restart, and what stops a build that failed on one machine from
    /// being sent to the next.
    fn absorb_reported_failures(&mut self, candidates: &[Candidate]) {
        for candidate in candidates {
            let Some(report) = candidate.last_update.as_ref() else {
                continue;
            };
            let cause = match report.state {
                // A failure recorded against the running build names no
                // replacement: the protocol path records its pre-download
                // failures that way, and they say nothing about an artifact.
                UpdateState::Failed if report.to_build != report.from_build => format!(
                    "{}'s update to {} failed: {}",
                    candidate.name,
                    report.to_build,
                    report.cause.as_deref().unwrap_or("no cause recorded")
                ),
                // The swap reported success yet the machine runs something
                // else. Directing it again would loop on the same outcome.
                UpdateState::Succeeded
                    if candidate
                        .running_build
                        .as_ref()
                        .is_some_and(|running| *running != report.to_build) =>
                {
                    format!(
                        "{} reported installing {} but runs {}",
                        candidate.name,
                        report.to_build,
                        candidate.running_build.as_deref().unwrap_or_default()
                    )
                }
                _ => continue,
            };
            self.halted.entry(report.to_build.clone()).or_insert(cause);
        }
    }

    fn continue_turn(
        &mut self,
        mut turn: Turn,
        candidates: &[Candidate],
        artifacts: &HashMap<String, ArtifactBuild>,
        now_unix_ms: u64,
        step: &mut Step,
    ) -> Option<Turn> {
        let transition = |kind, cause: String| Transition {
            executor_id: turn.executor_id.clone(),
            kind,
            build_id: turn.build_id.clone(),
            cause,
        };
        let still_served = matches!(
            artifacts.get(turn.target),
            Some(ArtifactBuild::Resolved(build_id)) if *build_id == turn.build_id
        );
        if !still_served {
            step.transitions.push(transition(
                TransitionKind::Abandoned,
                format!(
                    "the runner no longer serves {} for {}",
                    turn.build_id, turn.target
                ),
            ));
            return None;
        }
        if let Some(cause) = self.halted.get(&turn.build_id) {
            step.transitions
                .push(transition(TransitionKind::Abandoned, cause.clone()));
            return None;
        }
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.executor_id == turn.executor_id);
        match turn.phase {
            TurnPhase::Draining {
                since_unix_ms,
                generation,
                snapshots_at_hold,
                ..
            } => {
                let Some(candidate) = candidate else {
                    step.transitions.push(transition(
                        TransitionKind::Abandoned,
                        format!(
                            "{} detached while draining; its turn comes back when it returns",
                            turn.name
                        ),
                    ));
                    return None;
                };
                if candidate.running_build.as_deref() == Some(turn.build_id.as_str()) {
                    step.transitions.push(transition(
                        TransitionKind::Updated,
                        format!("{} already runs {}", turn.name, turn.build_id),
                    ));
                    return None;
                }
                // A new link is a new account: only its own snapshots can
                // say the machine is empty.
                let (generation, snapshots_at_hold) = if candidate.generation == generation {
                    (generation, snapshots_at_hold)
                } else {
                    (candidate.generation, candidate.snapshots_received)
                };
                if candidate.reachable
                    && candidate.in_flight == 0
                    && candidate.snapshots_received >= snapshots_at_hold + FRESH_SNAPSHOTS
                {
                    step.direct = Some(Directive {
                        executor_id: turn.executor_id.clone(),
                        generation,
                        build_id: turn.build_id.clone(),
                    });
                    step.transitions.push(transition(
                        TransitionKind::Directed,
                        format!(
                            "{} is empty; directed to install {}",
                            turn.name, turn.build_id
                        ),
                    ));
                    turn.phase = TurnPhase::Directed {
                        at_unix_ms: now_unix_ms,
                        generation,
                    };
                    return Some(turn);
                }
                if now_unix_ms.saturating_sub(since_unix_ms) >= DRAIN_BUDGET_MS {
                    self.deferred
                        .insert(turn.executor_id.clone(), now_unix_ms + DEFER_MS);
                    step.transitions.push(transition(
                        TransitionKind::Deferred,
                        format!(
                            "{} still had {} in flight after {} minutes; it takes work again and is asked after {} minutes",
                            turn.name,
                            candidate.in_flight,
                            DRAIN_BUDGET_MS / 60_000,
                            DEFER_MS / 60_000,
                        ),
                    ));
                    return None;
                }
                turn.phase = TurnPhase::Draining {
                    since_unix_ms,
                    generation,
                    snapshots_at_hold,
                    in_flight: candidate.in_flight,
                };
                Some(turn)
            }
            TurnPhase::Directed {
                at_unix_ms,
                generation,
            } => {
                let elapsed = now_unix_ms.saturating_sub(at_unix_ms);
                match candidate {
                    Some(candidate) if candidate.generation != generation => {
                        if candidate.running_build.as_deref() == Some(turn.build_id.as_str()) {
                            step.transitions.push(transition(
                                TransitionKind::Updated,
                                format!("{} returned on {}", turn.name, turn.build_id),
                            ));
                            return None;
                        }
                        // Its own report of a failed update was absorbed above
                        // and has already halted this build. A return with no
                        // explanation is retried once before it is read as one.
                        if turn.unexplained_returns < UNEXPLAINED_RETURNS {
                            turn.unexplained_returns += 1;
                            turn.phase = TurnPhase::Draining {
                                since_unix_ms: now_unix_ms,
                                generation: candidate.generation,
                                snapshots_at_hold: candidate.snapshots_received,
                                in_flight: candidate.in_flight,
                            };
                            return Some(turn);
                        }
                        let cause = format!(
                            "{} came back on {} after being directed to {}, without reporting why",
                            turn.name,
                            candidate
                                .running_build
                                .as_deref()
                                .unwrap_or("an unreported build"),
                            turn.build_id
                        );
                        self.halted.insert(turn.build_id.clone(), cause.clone());
                        step.transitions
                            .push(transition(TransitionKind::Halted, cause));
                        None
                    }
                    Some(_) if elapsed >= ACT_BUDGET_MS => {
                        let cause = format!(
                            "{} kept the link it was directed on for {} minutes without updating",
                            turn.name,
                            ACT_BUDGET_MS / 60_000
                        );
                        self.excluded.insert(
                            turn.executor_id.clone(),
                            (turn.build_id.clone(), cause.clone()),
                        );
                        step.transitions
                            .push(transition(TransitionKind::Excluded, cause));
                        None
                    }
                    None if elapsed >= RETURN_BUDGET_MS => {
                        let cause = format!(
                            "{} did not return within {} minutes of being directed to {}",
                            turn.name,
                            RETURN_BUDGET_MS / 60_000,
                            turn.build_id
                        );
                        self.halted.insert(turn.build_id.clone(), cause.clone());
                        step.transitions
                            .push(transition(TransitionKind::Halted, cause));
                        None
                    }
                    _ => Some(turn),
                }
            }
        }
    }

    /// The machine whose turn is next: the least busy reachable remote whose
    /// build differs from what the runner serves for its target, among those
    /// not halted, deferred, or excluded for that build.
    fn next<'a>(
        &self,
        candidates: &'a [Candidate],
        artifacts: &HashMap<String, ArtifactBuild>,
    ) -> Option<(&'a Candidate, &'static str, String)> {
        candidates
            .iter()
            .filter(|candidate| candidate.reachable)
            .filter(|candidate| !self.deferred.contains_key(&candidate.executor_id))
            .filter_map(|candidate| {
                let target = candidate.target?;
                let running = candidate.running_build.as_ref()?;
                let ArtifactBuild::Resolved(build_id) = artifacts.get(target)? else {
                    return None;
                };
                let excluded = self
                    .excluded
                    .get(&candidate.executor_id)
                    .is_some_and(|(excluded_build, _)| excluded_build == build_id);
                (running != build_id && !excluded && !self.halted.contains_key(build_id))
                    .then(|| (candidate, target, build_id.clone()))
            })
            .min_by(|(left, ..), (right, ..)| {
                (left.in_flight, &left.name).cmp(&(right.in_flight, &right.name))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: &str = "x86_64-unknown-linux-gnu";
    const NOW: u64 = 1_800_000_000_000;

    fn remote(name: &str, build: &str) -> Candidate {
        Candidate {
            executor_id: format!("id-{name}"),
            name: name.into(),
            generation: 1,
            target: Some(TARGET),
            running_build: Some(build.into()),
            last_update: None,
            in_flight: 0,
            snapshots_received: 10,
            reachable: true,
        }
    }

    fn serving(build: &str) -> HashMap<String, ArtifactBuild> {
        HashMap::from([(TARGET.to_string(), ArtifactBuild::Resolved(build.into()))])
    }

    fn phase(
        rollout: &Rollout,
        candidate: &Candidate,
        artifacts: &HashMap<String, ArtifactBuild>,
    ) -> SelfUpdatePhase {
        rollout.status(candidate, artifacts).unwrap().phase
    }

    /// Two snapshots after the hold, so the machine's emptiness is believed.
    fn settle(candidate: &mut Candidate) {
        candidate.snapshots_received += FRESH_SNAPSHOTS;
    }

    /// The machine ends the link it was directed on, then returns on `build`.
    fn return_on(candidate: &mut Candidate, build: &str) {
        candidate.generation += 1;
        candidate.running_build = Some(build.into());
        candidate.snapshots_received = 0;
    }

    #[test]
    fn a_skewed_machine_is_held_and_directed_only_once_its_work_drains() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("bbs", "old");
        machine.in_flight = 2;

        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        assert_eq!(step.hold.as_deref(), Some("id-bbs"));
        assert_eq!(step.direct, None);
        assert_eq!(
            phase(&rollout, &machine, &artifacts),
            SelfUpdatePhase::Draining {
                in_flight: 2,
                since_unix_ms: NOW
            }
        );

        // Its work finishes, but nothing it has said since the hold shows it.
        machine.in_flight = 0;
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 1_000);
        assert_eq!(step.direct, None);

        settle(&mut machine);
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 2_000);
        assert_eq!(
            step.direct,
            Some(Directive {
                executor_id: "id-bbs".into(),
                generation: 1,
                build_id: "new".into(),
            })
        );
        assert_eq!(
            step.hold.as_deref(),
            Some("id-bbs"),
            "still held while directed"
        );
        assert_eq!(
            phase(&rollout, &machine, &artifacts),
            SelfUpdatePhase::Directed {
                at_unix_ms: NOW + 2_000
            }
        );

        // It ends the link, is away a while, and returns current.
        let step = rollout.advance(&[], &artifacts, NOW + 60_000);
        assert_eq!(step.hold.as_deref(), Some("id-bbs"));
        return_on(&mut machine, "new");
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 90_000);
        assert_eq!(step.hold, None);
        assert_eq!(step.transitions[0].kind, TransitionKind::Updated);
        assert_eq!(
            phase(&rollout, &machine, &artifacts),
            SelfUpdatePhase::Current
        );
    }

    #[test]
    fn a_fleet_of_three_updates_one_machine_at_a_time() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![
            remote("bbs", "old"),
            remote("fedora", "old"),
            remote("bgsrv-ub", "old"),
        ];
        // The least busy goes first; the rest wait behind it.
        fleet[0].in_flight = 3;
        fleet[1].in_flight = 1;
        let mut order = Vec::new();
        let mut now = NOW;
        for _ in 0..3 {
            let step = rollout.advance(&fleet, &artifacts, now);
            let held = step.hold.clone().expect("a turn begins");
            for other in fleet.iter().filter(|machine| machine.executor_id != held) {
                assert!(
                    matches!(
                        phase(&rollout, other, &artifacts),
                        SelfUpdatePhase::Waiting { .. } | SelfUpdatePhase::Current
                    ),
                    "only the machine whose turn it is is drained or directed"
                );
            }
            let index = fleet
                .iter()
                .position(|machine| machine.executor_id == held)
                .unwrap();
            order.push(fleet[index].name.clone());
            fleet[index].in_flight = 0;
            settle(&mut fleet[index]);
            now += 1_000;
            let step = rollout.advance(&fleet, &artifacts, now);
            assert_eq!(
                step.direct
                    .as_ref()
                    .map(|direct| direct.executor_id.as_str()),
                Some(held.as_str())
            );
            return_on(&mut fleet[index], "new");
            now += 1_000;
        }
        let step = rollout.advance(&fleet, &artifacts, now);
        assert_eq!(
            step.hold, None,
            "the rollout ends when every machine is current"
        );
        assert_eq!(order, ["bgsrv-ub", "fedora", "bbs"]);
        assert!(fleet
            .iter()
            .all(|machine| phase(&rollout, machine, &artifacts) == SelfUpdatePhase::Current));
    }

    #[test]
    fn a_reported_failure_halts_the_build_for_the_rest_of_the_fleet() {
        let artifacts = serving("bad");
        let mut rollout = Rollout::default();
        let mut fleet = vec![remote("a", "old"), remote("b", "old"), remote("c", "old")];
        rollout.advance(&fleet, &artifacts, NOW);
        settle(&mut fleet[0]);
        assert!(rollout
            .advance(&fleet, &artifacts, NOW + 1_000)
            .direct
            .is_some());

        // Rolled back by its service manager, it returns on the old build
        // carrying its own account of the failure.
        return_on(&mut fleet[0], "old");
        fleet[0].last_update = Some(UpdateReport {
            state: UpdateState::Failed,
            from_build: "old".into(),
            to_distribution_id: "ci-9".into(),
            to_build: "bad".into(),
            cause: Some("the replacement did not reach Ready before its rollback deadline".into()),
            at_unix_ms: NOW + 500,
        });
        let step = rollout.advance(&fleet, &artifacts, NOW + 120_000);
        assert_eq!(
            step.hold, None,
            "nobody else is drained for a build that failed"
        );
        for machine in &fleet {
            assert!(matches!(
                phase(&rollout, machine, &artifacts),
                SelfUpdatePhase::Halted { cause } if cause.contains("rollback deadline")
            ));
        }

        // A different artifact is a new rollout.
        let fixed = serving("fixed");
        let step = rollout.advance(&fleet, &fixed, NOW + 200_000);
        assert!(step.hold.is_some());
    }

    #[test]
    fn the_halt_survives_a_runner_restart_through_the_machine_report() {
        let artifacts = serving("bad");
        let mut failed = remote("a", "old");
        failed.last_update = Some(UpdateReport {
            state: UpdateState::Failed,
            from_build: "old".into(),
            to_distribution_id: "ci-9".into(),
            to_build: "bad".into(),
            cause: Some("staged executor identity mismatch".into()),
            at_unix_ms: NOW,
        });
        let mut fresh_runner = Rollout::default();
        let step = fresh_runner.advance(&[failed, remote("b", "old")], &artifacts, NOW);
        assert_eq!(step.hold, None);
    }

    #[test]
    fn a_machine_that_does_not_empty_passes_its_turn_and_is_deferred() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![remote("busy", "old"), remote("idle", "old")];
        fleet[0].in_flight = 0;
        fleet[1].in_flight = 1;
        // The resident batches pinned to `busy` keep arriving after its hold.
        assert_eq!(
            rollout.advance(&fleet, &artifacts, NOW).hold.as_deref(),
            Some("id-busy")
        );
        fleet[0].in_flight = 4;
        let step = rollout.advance(&fleet, &artifacts, NOW + DRAIN_BUDGET_MS);
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
        assert_eq!(
            step.hold.as_deref(),
            Some("id-idle"),
            "the next machine takes the turn"
        );
        assert_eq!(
            phase(&rollout, &fleet[0], &artifacts),
            SelfUpdatePhase::Deferred {
                until_unix_ms: NOW + DRAIN_BUDGET_MS + DEFER_MS
            }
        );
    }

    #[test]
    fn a_machine_that_never_returns_halts_the_build() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("bgsrv-mac", "old");
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        settle(&mut machine);
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 1_000);
        let others = [remote("fedora", "old")];
        let step = rollout.advance(&others, &artifacts, NOW + 1_000 + RETURN_BUDGET_MS);
        assert_eq!(step.transitions[0].kind, TransitionKind::Halted);
        assert_eq!(step.hold, None);
    }

    #[test]
    fn a_machine_that_ignores_the_directive_is_excluded_without_halting_the_build() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![remote("a", "old"), remote("b", "old")];
        rollout.advance(&fleet, &artifacts, NOW);
        settle(&mut fleet[0]);
        rollout.advance(&fleet, &artifacts, NOW + 1_000);
        let step = rollout.advance(&fleet, &artifacts, NOW + 1_000 + ACT_BUDGET_MS);
        assert_eq!(step.transitions[0].kind, TransitionKind::Excluded);
        assert_eq!(step.hold.as_deref(), Some("id-b"));
        assert!(matches!(
            phase(&rollout, &fleet[0], &artifacts),
            SelfUpdatePhase::Excluded { .. }
        ));
    }

    #[test]
    fn an_unexplained_return_is_retried_once_before_it_halts() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("a", "old");
        let mut now = NOW;
        for attempt in 0..=UNEXPLAINED_RETURNS {
            rollout.advance(std::slice::from_ref(&machine), &artifacts, now);
            settle(&mut machine);
            now += 1_000;
            assert!(rollout
                .advance(std::slice::from_ref(&machine), &artifacts, now)
                .direct
                .is_some());
            return_on(&mut machine, "old");
            now += 1_000;
            let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, now);
            if attempt < UNEXPLAINED_RETURNS {
                assert_eq!(
                    step.hold.as_deref(),
                    Some("id-a"),
                    "drained again on its new link"
                );
            } else {
                assert_eq!(step.transitions[0].kind, TransitionKind::Halted);
            }
        }
    }

    #[test]
    fn an_undelivered_directive_goes_back_to_draining() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("a", "old");
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        settle(&mut machine);
        assert!(rollout
            .advance(std::slice::from_ref(&machine), &artifacts, NOW + 1_000)
            .direct
            .is_some());
        rollout.directive_undelivered("id-a", NOW + 1_000);
        // The link it was meant for is gone; its next one has to settle anew.
        return_on(&mut machine, "old");
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 2_000);
        assert!(step.direct.is_none());
        assert!(step.transitions.is_empty(), "{:?}", step.transitions);
        settle(&mut machine);
        assert!(rollout
            .advance(std::slice::from_ref(&machine), &artifacts, NOW + 3_000)
            .direct
            .is_some());
    }

    #[test]
    fn a_new_artifact_abandons_the_turn_and_starts_over() {
        let mut rollout = Rollout::default();
        let machine = remote("a", "old");
        rollout.advance(std::slice::from_ref(&machine), &serving("new"), NOW);
        let step = rollout.advance(
            std::slice::from_ref(&machine),
            &serving("newer"),
            NOW + 1_000,
        );
        assert_eq!(step.transitions[0].kind, TransitionKind::Abandoned);
        assert_eq!(step.transitions[1].kind, TransitionKind::Draining);
        assert_eq!(step.transitions[1].build_id, "newer");
    }

    #[test]
    fn status_names_current_and_an_unavailable_artifact() {
        let rollout = Rollout::default();
        let machine = remote("a", "same");
        assert_eq!(
            phase(&rollout, &machine, &serving("same")),
            SelfUpdatePhase::Current
        );
        let unavailable = HashMap::from([(
            TARGET.to_string(),
            ArtifactBuild::Unavailable("no x86_64-unknown-linux-gnu executor artifact published for executor protocol v156".into()),
        )]);
        let status = rollout.status(&machine, &unavailable).unwrap();
        assert_eq!(status.runner_build_id, None);
        assert!(matches!(
            status.phase,
            SelfUpdatePhase::ArtifactUnavailable { .. }
        ));
        let mut unpublished = machine.clone();
        unpublished.target = None;
        assert_eq!(rollout.status(&unpublished, &serving("same")), None);
    }
}
