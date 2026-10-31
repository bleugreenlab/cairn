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
//! The schedule has four properties, each against a specific failure:
//!
//! - **One machine at a time.** A bad artifact strands the one machine that
//!   took it, and the failure halts the rollout of that build before it
//!   reaches the next.
//! - **Empty before restart, however long emptying takes.** The machine whose
//!   turn it is is refused new placement until it is empty, and the directive
//!   goes out only once the runner's queue binds nothing to it and two
//!   snapshots taken after the hold show nothing in flight, so a restart kills
//!   no work. A busy machine is doing its job, not holding the fleet hostage,
//!   and handing it work back after a fixed budget only guarantees that a
//!   machine with a steady stream of work never empties. So the turn is bounded
//!   per piece of work rather than as a whole: a command may hold it for its
//!   own learned duration plus margin, and work with no expected end for a
//!   fixed hold.
//! - **Long-lived services are cycled, not waited on.** A service instance
//!   never finishes, and any executor restart ends it whether the rollout waits
//!   or not; its owner already re-places it whenever its executor is lost. So
//!   service residents are named as what the restart will cycle rather than
//!   counted as work in flight. A service's one-shot command is work, and is
//!   waited on like any other.
//! - **No wedged machine holds the fleet hostage.** Work that outruns its bound
//!   gives the machine back and passes the turn to the next; a machine that
//!   ignores a directive is excluded from that build without halting it. A
//!   served build that moves during a drain retargets the drain rather than
//!   ending it, so a nearly empty machine is not sent to the back of the line.
//!
//! This module is the decision alone, over plain facts the fleet copies out of
//! its connection table, so every transition is testable without a link.

use std::collections::HashMap;

use cairn_common::executor_protocol::{
    DrainHold, SelfUpdatePhase, SelfUpdateStatus, UpdateReport, UpdateState,
};

/// The least slack a command gets past its own expected finish before the
/// rollout reads it as wedged rather than busy.
pub(crate) const COMMAND_MARGIN_MS: u64 = 10 * 60 * 1000;
/// How long work with no expected end may hold a turn: a command never
/// measured, an environment being acquired, or a terminal or REPL left
/// running, none of which says when it will finish.
pub(crate) const UNMEASURED_HOLD_MS: u64 = 60 * 60 * 1000;
/// How long a machine whose work outran its bound keeps that work before it
/// is asked again.
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
    Unavailable(ArtifactUnavailable),
}

/// Why the runner serves no executor for one target, and the newest protocol
/// it does hold one for there. The second half is what lets a stranded machine
/// be told how far behind the runner's cache is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUnavailable {
    pub reason: String,
    pub newest_held_protocol: Option<u32>,
}

/// One piece of work a restart of the machine would end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeldWork {
    /// Stable across passes, so a drain knows when it first saw this work.
    pub key: String,
    /// The work, named the way an operator reads it.
    pub label: String,
    pub started_at_unix_ms: Option<u64>,
    /// Its own learned upper duration, when it has been measured.
    pub expected_ms: Option<u64>,
}

impl HeldWork {
    /// When this work is expected to finish, and when the turn stops waiting
    /// for it, given when the drain first saw it.
    ///
    /// Measured from no earlier than first sight, so work that started long
    /// before the turn, or was already past its estimate when the turn began,
    /// still gets its margin rather than ending the turn on the first pass.
    fn bound(&self, first_seen_unix_ms: u64) -> DrainHold {
        let (expected_done_unix_ms, deadline_unix_ms) = match self.expected_ms {
            Some(expected) => {
                let margin = (expected / 2).max(COMMAND_MARGIN_MS);
                match self.started_at_unix_ms {
                    Some(started) => {
                        let done = started.saturating_add(expected);
                        (
                            Some(done),
                            done.max(first_seen_unix_ms).saturating_add(margin),
                        )
                    }
                    None => (
                        None,
                        first_seen_unix_ms
                            .saturating_add(expected)
                            .saturating_add(margin),
                    ),
                }
            }
            None => (None, first_seen_unix_ms.saturating_add(UNMEASURED_HOLD_MS)),
        };
        DrainHold {
            label: self.label.clone(),
            expected_done_unix_ms,
            deadline_unix_ms,
        }
    }
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
    /// Work bound to or running on this machine that a restart would end and
    /// that will finish on its own.
    pub work: Vec<HeldWork>,
    /// Long-lived service instances on this machine. A restart ends them too,
    /// but they never finish, so they are cycled rather than waited on.
    pub services: Vec<String>,
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
    /// Its turn began, or the build it drains for moved: new work is refused
    /// there.
    Draining,
    /// Empty; the directive was sent.
    Directed,
    /// It came back on the runner's build.
    Updated,
    /// An update to this build failed; no machine is directed to it.
    Halted,
    /// Work on it outran its bound, so it takes work again.
    Deferred,
    /// It did not act on the directive; other machines still update.
    Excluded,
    /// The turn ended without an outcome: the machine left during its drain,
    /// came back unexplained, or the runner serves nothing it could install.
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
        /// When this drain first saw each piece of work still in flight.
        first_seen: HashMap<String, u64>,
        /// What the last pass found holding the turn, latest bound first.
        holds: Vec<DrainHold>,
        /// Service instances the restart will cycle.
        cycles: Vec<String>,
    },
    Directed {
        at_unix_ms: u64,
        generation: u64,
        cycled: Vec<String>,
    },
}

impl TurnPhase {
    /// A drain's account of the work it found on one pass.
    fn draining(
        candidate: &Candidate,
        since_unix_ms: u64,
        generation: u64,
        snapshots_at_hold: u64,
        first_seen: HashMap<String, u64>,
        now_unix_ms: u64,
    ) -> Self {
        let (first_seen, holds) = account(&candidate.work, first_seen, now_unix_ms);
        Self::Draining {
            since_unix_ms,
            generation,
            snapshots_at_hold,
            first_seen,
            holds,
            cycles: candidate.services.clone(),
        }
    }
}

/// When each piece of work was first seen (work that finished is forgotten,
/// work new this pass is first seen now), and what bounds each, latest first.
fn account(
    work: &[HeldWork],
    mut first_seen: HashMap<String, u64>,
    now_unix_ms: u64,
) -> (HashMap<String, u64>, Vec<DrainHold>) {
    first_seen.retain(|key, _| work.iter().any(|item| item.key == *key));
    let mut holds: Vec<DrainHold> = work
        .iter()
        .map(|item| item.bound(*first_seen.entry(item.key.clone()).or_insert(now_unix_ms)))
        .collect();
    holds.sort_by(|a, b| {
        b.deadline_unix_ms
            .cmp(&a.deadline_unix_ms)
            .then_with(|| a.label.cmp(&b.label))
    });
    (first_seen, holds)
}

/// `a`, `a and b`, `a, b and c`.
fn listed(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// The runner's rolling-update state. In memory: a runner restart begins a
/// fresh rollout, and what must survive one (a machine's failed update) is the
/// machine's own durable report, which it carries in every `Hello`.
#[derive(Debug, Default)]
pub(crate) struct Rollout {
    turn: Option<Turn>,
    /// Builds no machine is directed to, with why.
    halted: HashMap<String, String>,
    /// Machines whose work outran its bound, until when.
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
                let waiting_on = match candidate.work.len() {
                    0 => String::new(),
                    count => format!(" and its {count} in flight have finished"),
                };
                step.transitions.push(Transition {
                    executor_id: candidate.executor_id.clone(),
                    kind: TransitionKind::Draining,
                    build_id: build_id.clone(),
                    cause: format!(
                        "{} runs {} and the runner serves {build_id} for {target}; it takes no new work until it is empty{waiting_on}",
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
                    phase: TurnPhase::draining(
                        candidate,
                        now_unix_ms,
                        candidate.generation,
                        candidate.snapshots_received,
                        HashMap::new(),
                        now_unix_ms,
                    ),
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
                    first_seen: HashMap::new(),
                    holds: Vec::new(),
                    cycles: Vec::new(),
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
            ArtifactBuild::Unavailable(unavailable) => (
                None,
                SelfUpdatePhase::ArtifactUnavailable {
                    reason: unavailable.reason.clone(),
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
        // Matched by machine alone: a drain retargets when the served build
        // moves, and a directed machine is awaited on the build it was sent.
        if let Some(turn) = self
            .turn
            .as_ref()
            .filter(|turn| turn.executor_id == candidate.executor_id)
        {
            return match &turn.phase {
                TurnPhase::Draining {
                    since_unix_ms,
                    holds,
                    cycles,
                    ..
                } => SelfUpdatePhase::Draining {
                    in_flight: u32::try_from(holds.len()).unwrap_or(u32::MAX),
                    since_unix_ms: *since_unix_ms,
                    holds: holds.clone(),
                    cycles: cycles.clone(),
                },
                TurnPhase::Directed {
                    at_unix_ms, cycled, ..
                } => SelfUpdatePhase::Directed {
                    at_unix_ms: *at_unix_ms,
                    cycled: cycled.clone(),
                },
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
        // A drain is for the machine, not for one build: when the served build
        // moves mid-drain the work already finished still counts, so the drain
        // is kept and its directive will name the new build. A directed
        // machine is installing what it was sent and is awaited on that.
        if matches!(turn.phase, TurnPhase::Draining { .. }) {
            match artifacts.get(turn.target) {
                Some(ArtifactBuild::Resolved(served)) if *served != turn.build_id => {
                    if !self.halted.contains_key(served) {
                        step.transitions.push(Transition {
                            executor_id: turn.executor_id.clone(),
                            kind: TransitionKind::Draining,
                            build_id: served.clone(),
                            cause: format!(
                                "the runner now serves {served} for {} in place of {}; {} keeps draining and will be directed to the new build",
                                turn.target, turn.build_id, turn.name
                            ),
                        });
                    }
                    turn.build_id = served.clone();
                }
                Some(ArtifactBuild::Resolved(_)) => {}
                _ => {
                    step.transitions.push(Transition {
                        executor_id: turn.executor_id.clone(),
                        kind: TransitionKind::Abandoned,
                        build_id: turn.build_id.clone(),
                        cause: format!(
                            "the runner no longer serves an executor for {}",
                            turn.target
                        ),
                    });
                    return None;
                }
            }
        }
        let transition = |kind, cause: String| Transition {
            executor_id: turn.executor_id.clone(),
            kind,
            build_id: turn.build_id.clone(),
            cause,
        };
        if let Some(cause) = self.halted.get(&turn.build_id) {
            step.transitions
                .push(transition(TransitionKind::Abandoned, cause.clone()));
            return None;
        }
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.executor_id == turn.executor_id);
        match std::mem::replace(
            &mut turn.phase,
            TurnPhase::Directed {
                at_unix_ms: 0,
                generation: 0,
                cycled: Vec::new(),
            },
        ) {
            TurnPhase::Draining {
                since_unix_ms,
                generation,
                snapshots_at_hold,
                first_seen,
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
                    && candidate.work.is_empty()
                    && candidate.snapshots_received >= snapshots_at_hold + FRESH_SNAPSHOTS
                {
                    step.direct = Some(Directive {
                        executor_id: turn.executor_id.clone(),
                        generation,
                        build_id: turn.build_id.clone(),
                    });
                    let cycling = match candidate.services.len() {
                        0 => String::new(),
                        1 => format!(
                            "; the restart cycles {}, which its owner places again on the new executor",
                            listed(&candidate.services)
                        ),
                        _ => format!(
                            "; the restart cycles {}, which their owners place again on the new executor",
                            listed(&candidate.services)
                        ),
                    };
                    step.transitions.push(transition(
                        TransitionKind::Directed,
                        format!(
                            "{} is empty; directed to install {}{cycling}",
                            turn.name, turn.build_id
                        ),
                    ));
                    turn.phase = TurnPhase::Directed {
                        at_unix_ms: now_unix_ms,
                        generation,
                        cycled: candidate.services.clone(),
                    };
                    return Some(turn);
                }
                turn.phase = TurnPhase::draining(
                    candidate,
                    since_unix_ms,
                    generation,
                    snapshots_at_hold,
                    first_seen,
                    now_unix_ms,
                );
                let TurnPhase::Draining { first_seen, .. } = &turn.phase else {
                    unreachable!("just set to draining");
                };
                // The most overdue piece of work that outran its bound, if
                // any: a machine whose work does not end is wedged, and a
                // wedged machine does not hold the fleet.
                let overrun = candidate
                    .work
                    .iter()
                    .map(|item| {
                        let seen = first_seen.get(&item.key).copied().unwrap_or(now_unix_ms);
                        (item, seen, item.bound(seen))
                    })
                    .filter(|(_, _, bound)| now_unix_ms >= bound.deadline_unix_ms)
                    .min_by_key(|(_, _, bound)| bound.deadline_unix_ms);
                if let Some((item, seen, bound)) = overrun {
                    let why = match bound.expected_done_unix_ms {
                        Some(done) => format!(
                            "was expected to finish {} minutes ago, from its own learned duration of {} minutes",
                            now_unix_ms.saturating_sub(done) / 60_000,
                            item.expected_ms.unwrap_or_default() / 60_000,
                        ),
                        None => format!(
                            "has held the turn {} minutes without finishing",
                            now_unix_ms.saturating_sub(seen) / 60_000
                        ),
                    };
                    self.deferred
                        .insert(turn.executor_id.clone(), now_unix_ms + DEFER_MS);
                    step.transitions.push(transition(
                        TransitionKind::Deferred,
                        format!(
                            "{}'s {} {why}, so it is read as wedged; {} takes work again and is asked after {} minutes",
                            turn.name,
                            item.label,
                            turn.name,
                            DEFER_MS / 60_000,
                        ),
                    ));
                    return None;
                }
                Some(turn)
            }
            TurnPhase::Directed {
                at_unix_ms,
                generation,
                cycled,
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
                            turn.phase = TurnPhase::draining(
                                candidate,
                                now_unix_ms,
                                candidate.generation,
                                candidate.snapshots_received,
                                HashMap::new(),
                                now_unix_ms,
                            );
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
                    _ => {
                        turn.phase = TurnPhase::Directed {
                            at_unix_ms,
                            generation,
                            cycled,
                        };
                        Some(turn)
                    }
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
                (left.work.len(), &left.name).cmp(&(right.work.len(), &right.name))
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
            work: Vec::new(),
            services: Vec::new(),
            snapshots_received: 10,
            reachable: true,
        }
    }

    const LANE_MS: u64 = 20 * 60_000;

    fn command(key: &str, started_at_unix_ms: u64, expected_ms: Option<u64>) -> HeldWork {
        HeldWork {
            key: key.into(),
            label: format!("`{key}`"),
            started_at_unix_ms: Some(started_at_unix_ms),
            expected_ms,
        }
    }

    /// `count` measured lanes that started at `NOW`.
    fn with_work(candidate: &mut Candidate, count: usize) {
        candidate.work = (0..count)
            .map(|index| command(&format!("lane-{index}"), NOW, Some(LANE_MS)))
            .collect();
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
        with_work(&mut machine, 2);

        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        assert_eq!(step.hold.as_deref(), Some("id-bbs"));
        assert_eq!(step.direct, None);
        assert!(matches!(
            phase(&rollout, &machine, &artifacts),
            SelfUpdatePhase::Draining {
                in_flight: 2,
                since_unix_ms: NOW,
                ..
            }
        ));

        // Its work finishes, but nothing it has said since the hold shows it.
        machine.work.clear();
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
                at_unix_ms: NOW + 2_000,
                cycled: Vec::new(),
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
        with_work(&mut fleet[0], 3);
        with_work(&mut fleet[1], 1);
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
            fleet[index].work.clear();
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

    /// The specimen: a machine the cadence keeps busy, whose lanes finish and
    /// whose pinned work keeps arriving, never once empty at a pass. The old
    /// whole-drain budget gave it its work back at fifteen minutes; now it
    /// keeps its turn for as long as every piece of work is within its own
    /// bound, and updates the moment it empties.
    #[test]
    fn a_machine_with_a_steady_arrival_of_work_keeps_its_turn_until_it_empties() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![remote("busy", "old"), remote("idle", "old")];
        fleet[1].work = vec![command("other", NOW, Some(LANE_MS))];
        fleet[1].work.push(command("other-2", NOW, Some(LANE_MS)));
        assert_eq!(
            rollout.advance(&fleet, &artifacts, NOW).hold.as_deref(),
            Some("id-busy")
        );

        // Every five minutes for eighty minutes, a lane finishes and the next
        // pinned one starts, so something is always in flight.
        let mut now = NOW;
        for arrival in 0..16 {
            now = NOW + arrival * 5 * 60_000;
            fleet[0].work = vec![command(&format!("pinned-{arrival}"), now, Some(LANE_MS))];
            let step = rollout.advance(&fleet, &artifacts, now);
            assert_eq!(step.hold.as_deref(), Some("id-busy"), "at {arrival}");
            assert!(step.transitions.is_empty(), "{:?}", step.transitions);
            assert!(matches!(
                phase(&rollout, &fleet[1], &artifacts),
                SelfUpdatePhase::Waiting { behind: Some(ref name) } if name == "busy"
            ));
        }
        assert!(
            now - NOW >= 75 * 60_000,
            "well past the old fifteen minutes"
        );

        fleet[0].work.clear();
        settle(&mut fleet[0]);
        let step = rollout.advance(&fleet, &artifacts, now + 1_000);
        assert_eq!(
            step.direct.map(|direct| direct.executor_id),
            Some("id-busy".to_string()),
            "it updates as soon as it is empty"
        );
    }

    /// What bounds a turn is each command's own learned duration plus margin:
    /// a lane past that is wedged, and a wedged machine gives its turn away.
    #[test]
    fn work_that_outruns_its_own_learned_duration_passes_the_turn() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![remote("busy", "old"), remote("idle", "old")];
        fleet[0].work = vec![
            command("short", NOW, Some(LANE_MS / 2)),
            command("rust-tests", NOW, Some(LANE_MS)),
        ];
        fleet[1].work = vec![command("a", NOW, None), command("b", NOW, None)];
        assert_eq!(
            rollout.advance(&fleet, &artifacts, NOW).hold.as_deref(),
            Some("id-busy")
        );
        let SelfUpdatePhase::Draining { holds, .. } = phase(&rollout, &fleet[0], &artifacts) else {
            panic!("draining");
        };
        assert_eq!(
            holds[0].label, "`rust-tests`",
            "the work that bounds the wait leads"
        );
        assert_eq!(holds[0].expected_done_unix_ms, Some(NOW + LANE_MS));
        assert_eq!(holds[0].deadline_unix_ms, NOW + LANE_MS + COMMAND_MARGIN_MS);

        // The short one finished; the long one is past its estimate but
        // within its margin.
        fleet[0].work.remove(0);
        let deadline = NOW + LANE_MS + COMMAND_MARGIN_MS;
        let step = rollout.advance(&fleet, &artifacts, deadline - 1);
        assert_eq!(step.hold.as_deref(), Some("id-busy"));

        let step = rollout.advance(&fleet, &artifacts, deadline);
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
        assert!(
            step.transitions[0]
                .cause
                .contains("`rust-tests` was expected to finish 10 minutes ago"),
            "{}",
            step.transitions[0].cause
        );
        assert_eq!(
            step.hold.as_deref(),
            Some("id-idle"),
            "the next machine takes the turn"
        );
        assert_eq!(
            phase(&rollout, &fleet[0], &artifacts),
            SelfUpdatePhase::Deferred {
                until_unix_ms: deadline + DEFER_MS
            }
        );
    }

    /// Work with no expected end (a command never measured, a terminal left
    /// running) holds the turn for the fixed hold from when the drain first
    /// saw it, however long before the drain it started.
    #[test]
    fn unmeasured_work_holds_the_turn_for_the_fixed_hold() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("a", "old");
        machine.work = vec![command("terminal `dev`", NOW - 6 * 3_600_000, None)];
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        let step = rollout.advance(
            std::slice::from_ref(&machine),
            &artifacts,
            NOW + UNMEASURED_HOLD_MS - 1,
        );
        assert_eq!(step.hold.as_deref(), Some("id-a"));
        let step = rollout.advance(
            std::slice::from_ref(&machine),
            &artifacts,
            NOW + UNMEASURED_HOLD_MS,
        );
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
        assert!(
            step.transitions[0]
                .cause
                .contains("has held the turn 60 minutes without finishing"),
            "{}",
            step.transitions[0].cause
        );
    }

    /// A service instance never finishes and any restart ends it, so it is
    /// named as what the restart cycles, never counted as work to wait for.
    #[test]
    fn a_service_resident_is_cycled_rather_than_waited_on() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("bgsrv-mac", "old");
        machine.services = vec![
            "service `iMessage channel`".into(),
            "service `desktop automation`".into(),
        ];
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        assert!(matches!(
            phase(&rollout, &machine, &artifacts),
            SelfUpdatePhase::Draining { in_flight: 0, ref cycles, .. } if cycles.len() == 2
        ));
        settle(&mut machine);
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 1_000);
        assert!(
            step.direct.is_some(),
            "its services do not keep it from updating"
        );
        assert!(
            step.transitions[0].cause.contains(
                "the restart cycles service `iMessage channel` and service `desktop automation`, which their owners place again"
            ),
            "{}",
            step.transitions[0].cause
        );
        assert!(matches!(
            phase(&rollout, &machine, &artifacts),
            SelfUpdatePhase::Directed { ref cycled, .. } if cycled.len() == 2
        ));
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

    /// The served build moving mid-drain retargets the drain: what already
    /// finished still counts, the machine stays held, and the directive names
    /// the new build.
    #[test]
    fn a_new_artifact_mid_drain_keeps_the_drain_and_retargets_it() {
        let mut rollout = Rollout::default();
        let mut machine = remote("a", "old");
        with_work(&mut machine, 1);
        rollout.advance(std::slice::from_ref(&machine), &serving("new"), NOW);
        let newer = serving("newer");
        let step = rollout.advance(std::slice::from_ref(&machine), &newer, NOW + 60_000);
        assert_eq!(step.hold.as_deref(), Some("id-a"));
        assert_eq!(step.transitions.len(), 1, "{:?}", step.transitions);
        assert_eq!(step.transitions[0].kind, TransitionKind::Draining);
        assert_eq!(step.transitions[0].build_id, "newer");
        assert!(matches!(
            phase(&rollout, &machine, &newer),
            SelfUpdatePhase::Draining {
                since_unix_ms: NOW,
                ..
            }
        ));

        machine.work.clear();
        settle(&mut machine);
        let step = rollout.advance(std::slice::from_ref(&machine), &newer, NOW + 61_000);
        assert_eq!(
            step.direct.map(|direct| direct.build_id),
            Some("newer".to_string())
        );
    }

    /// A directed machine is installing what it was sent. A newer served build
    /// does not abandon it mid-install; it is awaited on the build it took, and
    /// the newer one is a turn of its own.
    #[test]
    fn a_new_artifact_after_the_directive_awaits_the_directed_build() {
        let mut rollout = Rollout::default();
        let mut machine = remote("a", "old");
        rollout.advance(std::slice::from_ref(&machine), &serving("new"), NOW);
        settle(&mut machine);
        assert!(rollout
            .advance(std::slice::from_ref(&machine), &serving("new"), NOW + 1_000)
            .direct
            .is_some());
        let newer = serving("newer");
        let step = rollout.advance(&[], &newer, NOW + 30_000);
        assert_eq!(step.hold.as_deref(), Some("id-a"), "held while it installs");
        assert!(step.transitions.is_empty(), "{:?}", step.transitions);

        return_on(&mut machine, "new");
        let step = rollout.advance(std::slice::from_ref(&machine), &newer, NOW + 60_000);
        assert_eq!(step.transitions[0].kind, TransitionKind::Updated);
        assert_eq!(step.transitions[0].build_id, "new");
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
            ArtifactBuild::Unavailable(ArtifactUnavailable {
                reason: "no x86_64-unknown-linux-gnu executor artifact published for executor protocol v156".into(),
                newest_held_protocol: Some(155),
            }),
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
