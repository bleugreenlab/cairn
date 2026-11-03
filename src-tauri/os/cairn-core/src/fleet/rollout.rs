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
//! - **One machine at a time, the readiest first.** A bad artifact strands
//!   the one machine that took it, and the failure halts the rollout of that
//!   build before it reaches the next. The rule counts machines mid-update; it
//!   does not make one machine's work another machine's wait. The turn (the
//!   head) goes only to a machine with nothing in flight, which is confirmed
//!   empty, directed, and awaited on its return. A machine with work in flight
//!   drains beside it, refused new work but holding no one, so idle machines
//!   update ahead of it however long its cells run. Once it is empty it takes
//!   the head next, ahead of any idle machine, since it has refused work the
//!   longest. One machine drains at a time, so at most two machines refuse new
//!   work at once: the one updating and the one emptying.
//! - **Empty before restart, however long emptying takes.** A draining
//!   machine is refused new placement until it is empty, and the directive
//!   goes out only once the runner's queue binds nothing to it and two
//!   snapshots taken after the hold show nothing in flight, so a restart kills
//!   no work. A busy machine is doing its job, not holding the fleet hostage,
//!   and handing it work back after a fixed budget only guarantees that a
//!   machine with a steady stream of work never empties. So the drain is
//!   bounded per piece of work rather than as a whole: a command may hold it
//!   for its own learned duration plus margin, and work with no expected end
//!   for a fixed hold. A command past that bound keeps the drain for as long
//!   as its own stall stop still sees its tree progressing, because a slow
//!   command on a stalled disk is busy, not wedged. Since new placement is
//!   refused, a draining machine's work only shrinks, save the next command
//!   for something already resident there. A terminal or REPL that never ends
//!   is work with no expected end: it holds the drain for the fixed hold and
//!   then defers the machine like any wedged work, so that machine does not
//!   update past it, and it holds no other machine's update meanwhile.
//! - **Long-lived services are cycled, not waited on.** A service instance
//!   never finishes, and any executor restart ends it whether the rollout waits
//!   or not; its owner already re-places it whenever its executor is lost. So
//!   service residents are named as what the restart will cycle rather than
//!   counted as work in flight. A service's one-shot command is work, and is
//!   waited on like any other.
//! - **No wedged machine holds the fleet hostage.** Work that outruns its bound
//!   and has stopped progressing, or that outruns it with nothing able to say
//!   whether it progresses, gives the machine back and passes the drain to the
//!   next busy machine; a machine that
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
    /// What its own stall stop last said about its process tree, when that
    /// stop is judging it.
    pub progress: Option<Progress>,
}

/// A running command's stall-stop window, as its executor last sampled it:
/// how long its whole tree had gone without output, CPU or storage progress,
/// against the bound that stop ends it at (the cadence's silence interval).
///
/// One definition of progress serves both the stop and the drain, so a
/// command the executor would keep running is never read here as wedged, and
/// the instruments' exclusions (the executor's own output backpressure, an
/// incomplete reading) hold in both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Progress {
    pub quiet_ms: u64,
    pub silence_bound_ms: u64,
    pub measured_at_unix_ms: u64,
}

impl Progress {
    /// How long the tree has been quiet as of `now`. The reading ages with
    /// the sample that carried it, so one that stops being refreshed stops
    /// being evidence that the work still moves.
    fn quiet_at(&self, now_unix_ms: u64) -> u64 {
        self.quiet_ms
            .saturating_add(now_unix_ms.saturating_sub(self.measured_at_unix_ms))
    }

    /// Whether the tree has progressed within its bound, or `None` when no
    /// bound arms the judgement: an agent's batch carries zero.
    fn progressing(&self, now_unix_ms: u64) -> Option<bool> {
        (self.silence_bound_ms > 0).then(|| self.quiet_at(now_unix_ms) < self.silence_bound_ms)
    }
}

impl HeldWork {
    /// Whether this work is still progressing, by its own stall stop, or
    /// `None` when nothing can say: no stop is armed for it, its platform
    /// cannot read the tree, or it is not running.
    fn progressing(&self, now_unix_ms: u64) -> Option<bool> {
        self.progress
            .and_then(|progress| progress.progressing(now_unix_ms))
    }

    /// Whether this work holds the turn past its bound: it is past it and
    /// still progressing.
    fn held_past(hold: &DrainHold, now_unix_ms: u64) -> bool {
        now_unix_ms >= hold.deadline_unix_ms && hold.progressing == Some(true)
    }

    /// When this work is expected to finish, when the turn stops waiting for
    /// it unless it is still progressing, and whether it is, given when the
    /// drain first saw it.
    ///
    /// Measured from no earlier than first sight, so work that started long
    /// before the turn, or was already past its estimate when the turn began,
    /// still gets its margin rather than ending the turn on the first pass.
    fn hold(&self, first_seen_unix_ms: u64, now_unix_ms: u64) -> DrainHold {
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
            progressing: self.progressing(now_unix_ms),
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

/// A directed machine that stayed away past [`RETURN_BUDGET_MS`]: the update
/// stranded it. Reported once, on the pass that concludes it, because the
/// conclusion also halts the build, so no later pass directs it there again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Unreturned {
    pub executor_id: String,
    pub build_id: String,
    pub directed_at_unix_ms: u64,
}

/// What one pass decided.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Step {
    /// The machines to refuse new placement: the one mid-update and the one
    /// emptying for its turn.
    pub holds: Vec<String>,
    pub direct: Option<Directive>,
    pub transitions: Vec<Transition>,
    /// Machines this pass concluded did not return from their directive.
    pub unreturned: Vec<Unreturned>,
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
/// work new this pass is first seen now), and what bounds each: work held
/// past its bound by its progress first, since nothing bounds it but that,
/// then latest bound first.
fn account(
    work: &[HeldWork],
    mut first_seen: HashMap<String, u64>,
    now_unix_ms: u64,
) -> (HashMap<String, u64>, Vec<DrainHold>) {
    first_seen.retain(|key, _| work.iter().any(|item| item.key == *key));
    let mut holds: Vec<DrainHold> = work
        .iter()
        .map(|item| {
            let seen = *first_seen.entry(item.key.clone()).or_insert(now_unix_ms);
            item.hold(seen, now_unix_ms)
        })
        .collect();
    holds.sort_by(|a, b| {
        HeldWork::held_past(b, now_unix_ms)
            .cmp(&HeldWork::held_past(a, now_unix_ms))
            .then_with(|| b.deadline_unix_ms.cmp(&a.deadline_unix_ms))
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

/// Which of the rollout's two places a turn holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// The one machine being updated: confirmed empty, directed, or awaited
    /// on its return.
    Head,
    /// The one machine with work in flight, emptying beside the head without
    /// holding it.
    Drain,
}

/// The runner's rolling-update state. In memory: a runner restart begins a
/// fresh rollout, and what must survive one (a machine's failed update) is the
/// machine's own durable report, which it carries in every `Hello`.
#[derive(Debug, Default)]
pub(crate) struct Rollout {
    /// The machine mid-update. Only a machine with nothing in flight takes
    /// it, so no machine's work holds another machine's update.
    head: Option<Turn>,
    /// The machine emptying for its update: refused new work, holding no
    /// one, and next to take the head once it is empty.
    drain: Option<Turn>,
    /// Every other machine due an update, in the order the last pass would
    /// hand them the head, for a waiting machine's place in line.
    line: Vec<String>,
    /// Builds no machine is directed to, with why.
    halted: HashMap<String, String>,
    /// Machines whose work outran its bound, until when.
    deferred: HashMap<String, u64>,
    /// Machines that did not act on a directive, with the build and why.
    excluded: HashMap<String, (String, String)>,
}

impl Rollout {
    /// Whether this machine is being emptied or awaited, in either slot.
    pub(crate) fn holds(&self, executor_id: &str) -> bool {
        self.turns()
            .any(|(_, turn)| turn.executor_id == executor_id)
    }

    fn turns(&self) -> impl Iterator<Item = (Slot, &Turn)> {
        [(Slot::Head, &self.head), (Slot::Drain, &self.drain)]
            .into_iter()
            .filter_map(|(slot, turn)| turn.as_ref().map(|turn| (slot, turn)))
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
        if let Some(turn) = self.drain.take() {
            self.drain = self.continue_turn(
                turn,
                Slot::Drain,
                candidates,
                artifacts,
                now_unix_ms,
                &mut step,
            );
        }
        if let Some(turn) = self.head.take() {
            self.head = self.continue_turn(
                turn,
                Slot::Head,
                candidates,
                artifacts,
                now_unix_ms,
                &mut step,
            );
        }
        // A drained machine takes the head ahead of any idle one: it has
        // refused new work the longest.
        if self.head.is_none()
            && self
                .drain
                .as_ref()
                .is_some_and(|turn| ready(turn, candidates))
        {
            let turn = self.drain.take().expect("checked above");
            self.head = self.continue_turn(
                turn,
                Slot::Head,
                candidates,
                artifacts,
                now_unix_ms,
                &mut step,
            );
        }
        if self.head.is_none() {
            if let Some((candidate, target, build_id)) =
                self.next(Slot::Head, candidates, artifacts)
            {
                self.head = Some(begin(
                    candidate,
                    Slot::Head,
                    target,
                    build_id,
                    now_unix_ms,
                    &mut step,
                ));
            }
        }
        if self.drain.is_none() {
            if let Some((candidate, target, build_id)) =
                self.next(Slot::Drain, candidates, artifacts)
            {
                self.drain = Some(begin(
                    candidate,
                    Slot::Drain,
                    target,
                    build_id,
                    now_unix_ms,
                    &mut step,
                ));
            }
        }
        self.line = self.line_up(candidates, artifacts);
        step.holds = self
            .turns()
            .map(|(_, turn)| turn.executor_id.clone())
            .collect();
        step
    }

    /// The directive the last pass produced never reached its link (the
    /// generation moved or the link closed first), so the turn goes back to
    /// draining on whatever link the machine has now rather than reading its
    /// return as an update that failed.
    pub(crate) fn directive_undelivered(&mut self, executor_id: &str, now_unix_ms: u64) {
        if let Some(turn) = self.head.as_mut() {
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
        let updating = self.head.as_ref().map(|head| head.name.clone());
        // Matched by machine alone: a drain retargets when the served build
        // moves, and a directed machine is awaited on the build it was sent.
        if let Some((slot, turn)) = self
            .turns()
            .find(|(_, turn)| turn.executor_id == candidate.executor_id)
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
                    waits_for_turn: slot == Slot::Drain,
                    updating: updating.filter(|_| slot == Slot::Drain),
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
        // The machine updating now goes before everything in line.
        let place = self
            .line
            .iter()
            .position(|executor_id| *executor_id == candidate.executor_id)
            .unwrap_or(self.line.len());
        SelfUpdatePhase::Waiting {
            ahead: u32::try_from(place)
                .unwrap_or(u32::MAX)
                .saturating_add(u32::from(self.head.is_some())),
            updating,
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
        slot: Slot,
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
                if slot == Slot::Head && !candidate.work.is_empty() {
                    // Work arrived after all: bound before the hold, or the
                    // next command for something resident. The head is for a
                    // machine that can update now, so this one drains beside
                    // it, or takes work again while another machine drains.
                    let in_flight = candidate.work.len();
                    turn.phase = TurnPhase::draining(
                        candidate,
                        since_unix_ms,
                        generation,
                        snapshots_at_hold,
                        first_seen,
                        now_unix_ms,
                    );
                    if let Some(drain) = &self.drain {
                        step.transitions.push(transition(
                            TransitionKind::Abandoned,
                            format!(
                                "{} has {in_flight} in flight after all and {} is already draining, so it takes work again until it can drain",
                                turn.name, drain.name
                            ),
                        ));
                        return None;
                    }
                    step.transitions.push(transition(
                        TransitionKind::Draining,
                        format!(
                            "{} has {in_flight} in flight after all, so it drains without holding the turn; machines with nothing in flight update ahead of it",
                            turn.name
                        ),
                    ));
                    self.drain = Some(turn);
                    return None;
                }
                if slot == Slot::Head
                    && candidate.reachable
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
                // An empty head not yet confirmed empty, or not reachable to
                // direct: nothing it holds can be wedged.
                if slot == Slot::Head {
                    return Some(turn);
                }
                let TurnPhase::Draining { first_seen, .. } = &turn.phase else {
                    unreachable!("just set to draining");
                };
                // The most overdue piece of work that outran its bound and is
                // not still progressing, if any: a machine whose work neither
                // ends nor moves is wedged, and a wedged machine does not hold
                // the fleet. Work that outran its bound but whose tree is still
                // working is a slow machine doing its job, and keeps the turn.
                let overrun = candidate
                    .work
                    .iter()
                    .map(|item| {
                        let seen = first_seen.get(&item.key).copied().unwrap_or(now_unix_ms);
                        (item, seen, item.hold(seen, now_unix_ms))
                    })
                    .filter(|(_, _, hold)| {
                        now_unix_ms >= hold.deadline_unix_ms && hold.progressing != Some(true)
                    })
                    .min_by_key(|(_, _, hold)| hold.deadline_unix_ms);
                if let Some((item, seen, bound)) = overrun {
                    let silent = match item.progress {
                        Some(progress) if bound.progressing == Some(false) => format!(
                            ", and its tree has shown no progress for {} minutes against its own {}-minute stall bound",
                            progress.quiet_at(now_unix_ms) / 60_000,
                            progress.silence_bound_ms / 60_000,
                        ),
                        _ => String::new(),
                    };
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
                            "{}'s {} {why}{silent}, so it is read as wedged; {} takes work again and is asked after {} minutes",
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
                        step.unreturned.push(Unreturned {
                            executor_id: turn.executor_id.clone(),
                            build_id: turn.build_id.clone(),
                            directed_at_unix_ms: at_unix_ms,
                        });
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

    /// The target and served build this machine is due, when it is a
    /// reachable remote whose build differs from what the runner serves for
    /// its target and is not deferred, nor excluded or halted for that build.
    fn due(
        &self,
        candidate: &Candidate,
        artifacts: &HashMap<String, ArtifactBuild>,
    ) -> Option<(&'static str, String)> {
        if !candidate.reachable || self.deferred.contains_key(&candidate.executor_id) {
            return None;
        }
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
            .then(|| (target, build_id.clone()))
    }

    /// The machine to take a free slot: for the head, a due machine with
    /// nothing in flight; for the drain, the least busy due machine with work
    /// in flight. Neither takes a machine already in the other slot.
    fn next<'a>(
        &self,
        slot: Slot,
        candidates: &'a [Candidate],
        artifacts: &HashMap<String, ArtifactBuild>,
    ) -> Option<(&'a Candidate, &'static str, String)> {
        candidates
            .iter()
            .filter(|candidate| !self.holds(&candidate.executor_id))
            .filter(|candidate| candidate.work.is_empty() == (slot == Slot::Head))
            .filter_map(|candidate| {
                self.due(candidate, artifacts)
                    .map(|(target, build_id)| (candidate, target, build_id))
            })
            .min_by(|(left, ..), (right, ..)| {
                (left.work.len(), &left.name).cmp(&(right.work.len(), &right.name))
            })
    }

    /// Every due machine but the head, in the order they would take it: the
    /// drained machine if it is empty, then the idle, then the draining
    /// machine if it still has work, then the busy, least busy first.
    fn line_up(
        &self,
        candidates: &[Candidate],
        artifacts: &HashMap<String, ArtifactBuild>,
    ) -> Vec<String> {
        let head = self.head.as_ref().map(|turn| turn.executor_id.as_str());
        let drain = self.drain.as_ref().map(|turn| turn.executor_id.as_str());
        let mut line: Vec<(u8, &Candidate)> = candidates
            .iter()
            .filter(|candidate| Some(candidate.executor_id.as_str()) != head)
            .filter_map(|candidate| {
                let draining = Some(candidate.executor_id.as_str()) == drain;
                if !draining && self.due(candidate, artifacts).is_none() {
                    return None;
                }
                let idle = candidate.reachable && candidate.work.is_empty();
                let rank = match (draining, idle) {
                    (true, true) => 0,
                    (false, true) => 1,
                    (true, false) => 2,
                    (false, false) => 3,
                };
                Some((rank, candidate))
            })
            .collect();
        line.sort_by(|(left_rank, left), (right_rank, right)| {
            (*left_rank, left.work.len(), &left.name).cmp(&(
                *right_rank,
                right.work.len(),
                &right.name,
            ))
        });
        line.into_iter()
            .map(|(_, candidate)| candidate.executor_id.clone())
            .collect()
    }
}

/// Whether a draining machine can take the head: reachable, and nothing in
/// flight.
fn ready(turn: &Turn, candidates: &[Candidate]) -> bool {
    candidates.iter().any(|candidate| {
        candidate.executor_id == turn.executor_id
            && candidate.reachable
            && candidate.work.is_empty()
    })
}

/// A machine's turn begins in `slot`: it is refused new work from now.
fn begin(
    candidate: &Candidate,
    slot: Slot,
    target: &'static str,
    build_id: String,
    now_unix_ms: u64,
    step: &mut Step,
) -> Turn {
    let waiting_on = match slot {
        Slot::Head => {
            "; it has nothing in flight, so it takes the turn and takes no new work until it is directed"
                .to_string()
        }
        Slot::Drain => format!(
            "; it takes no new work until its {} in flight have finished, and holds no other machine's update meanwhile",
            candidate.work.len()
        ),
    };
    step.transitions.push(Transition {
        executor_id: candidate.executor_id.clone(),
        kind: TransitionKind::Draining,
        build_id: build_id.clone(),
        cause: format!(
            "{} runs {} and the runner serves {build_id} for {target}{waiting_on}",
            candidate.name,
            candidate
                .running_build
                .as_deref()
                .unwrap_or("an unreported build"),
        ),
    });
    Turn {
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
            progress: None,
        }
    }

    const HOUR: u64 = 60 * 60_000;
    /// A review check's silence bound, the interval its stall stop judges by.
    const REVIEW_BOUND_MS: u64 = 30 * 60_000;

    /// `item` as a sample taken at `at` reported it: its stall stop had seen
    /// no progress for `quiet_ms` against `silence_bound_ms`.
    fn sampled(mut item: HeldWork, quiet_ms: u64, silence_bound_ms: u64, at: u64) -> HeldWork {
        item.progress = Some(Progress {
            quiet_ms,
            silence_bound_ms,
            measured_at_unix_ms: at,
        });
        item
    }

    /// A machine running one whole-suite pass with a learned hour, beside a
    /// busier one, so it takes the first turn. Its bound falls at 90 minutes.
    fn suite_fleet() -> Vec<Candidate> {
        let mut fleet = vec![remote("fedora", "old"), remote("busy", "old")];
        fleet[0].work = vec![command("rust-tests", NOW, Some(HOUR))];
        fleet[1].work = vec![command("a", NOW, None), command("b", NOW, None)];
        fleet
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

    fn held(step: &Step) -> Vec<&str> {
        step.holds.iter().map(String::as_str).collect()
    }

    fn index_of(fleet: &[Candidate], executor_id: &str) -> usize {
        fleet
            .iter()
            .position(|machine| machine.executor_id == executor_id)
            .unwrap()
    }

    fn waiting(ahead: u32, updating: &str) -> SelfUpdatePhase {
        SelfUpdatePhase::Waiting {
            ahead,
            updating: Some(updating.into()),
        }
    }

    #[test]
    fn a_skewed_machine_is_held_and_directed_only_once_its_work_drains() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("bbs", "old");
        with_work(&mut machine, 2);

        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        assert_eq!(held(&step), ["id-bbs"]);
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
        assert_eq!(held(&step), ["id-bbs"], "still held while directed");
        assert_eq!(
            phase(&rollout, &machine, &artifacts),
            SelfUpdatePhase::Directed {
                at_unix_ms: NOW + 2_000,
                cycled: Vec::new(),
            }
        );

        // It ends the link, is away a while, and returns current.
        let step = rollout.advance(&[], &artifacts, NOW + 60_000);
        assert_eq!(held(&step), ["id-bbs"]);
        return_on(&mut machine, "new");
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 90_000);
        assert!(step.holds.is_empty(), "{:?}", step.holds);
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
        // The idle machine takes the turn, the least busy of the rest drains
        // beside it, and the busiest waits for the drain.
        with_work(&mut fleet[0], 3);
        with_work(&mut fleet[1], 1);
        let mut order = Vec::new();
        let mut now = NOW;
        for _ in 0..8 {
            let step = rollout.advance(&fleet, &artifacts, now);
            let mid_update = fleet
                .iter()
                .filter(|machine| {
                    matches!(
                        phase(&rollout, machine, &artifacts),
                        SelfUpdatePhase::Directed { .. }
                    )
                })
                .count();
            assert!(mid_update <= 1, "one machine mid-update at a time");
            assert!(step.holds.len() <= 2, "{:?}", step.holds);
            // Whatever is held finishes its work and says so twice.
            for executor_id in &step.holds {
                let index = index_of(&fleet, executor_id);
                fleet[index].work.clear();
                settle(&mut fleet[index]);
            }
            if let Some(direct) = step.direct {
                let index = index_of(&fleet, &direct.executor_id);
                order.push(fleet[index].name.clone());
                return_on(&mut fleet[index], "new");
            }
            now += 1_000;
        }
        let step = rollout.advance(&fleet, &artifacts, now);
        assert!(
            step.holds.is_empty(),
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
        assert!(
            step.holds.is_empty(),
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
        assert!(!step.holds.is_empty());
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
        assert!(step.holds.is_empty(), "{:?}", step.holds);
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
        let mut fleet = vec![remote("busy", "old"), remote("other", "old")];
        fleet[0].work = vec![command("pinned-0", NOW, Some(LANE_MS))];
        fleet[1].work = vec![command("other", NOW, Some(LANE_MS))];
        fleet[1].work.push(command("other-2", NOW, Some(LANE_MS)));
        assert_eq!(held(&rollout.advance(&fleet, &artifacts, NOW)), ["id-busy"]);

        // Every five minutes for eighty minutes, a lane finishes and the next
        // pinned one starts, so something is always in flight.
        let mut now = NOW;
        for arrival in 1..16 {
            now = NOW + arrival * 5 * 60_000;
            fleet[0].work = vec![command(&format!("pinned-{arrival}"), now, Some(LANE_MS))];
            let step = rollout.advance(&fleet, &artifacts, now);
            assert_eq!(held(&step), ["id-busy"], "at {arrival}");
            assert!(step.transitions.is_empty(), "{:?}", step.transitions);
            assert_eq!(
                phase(&rollout, &fleet[1], &artifacts),
                SelfUpdatePhase::Waiting {
                    ahead: 1,
                    updating: None,
                },
                "busy itself, it waits for the drain"
            );
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

    /// The bginf-sp2 specimen (cairn/5965): a Spark with a long cell in
    /// flight took the turn while bbs and fedora were busy, and once they went
    /// idle it held both their updates for up to an hour. Now the busy machine
    /// drains beside the turn and holds no one: the idle two update first, one
    /// at a time, and it updates last, once its cell ends.
    #[test]
    fn idle_machines_update_ahead_of_one_draining_a_long_cell() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![
            remote("bbs", "old"),
            remote("bginf-sp2", "old"),
            remote("fedora", "old"),
        ];
        with_work(&mut fleet[0], 4);
        fleet[1].work = vec![
            command("cell", NOW, None),
            command("b", NOW, Some(LANE_MS)),
            command("c", NOW, Some(LANE_MS)),
        ];
        with_work(&mut fleet[2], 5);
        let step = rollout.advance(&fleet, &artifacts, NOW);
        assert_eq!(
            held(&step),
            ["id-bginf-sp2"],
            "nobody can update now, so the least busy drains"
        );
        assert!(
            step.transitions[0]
                .cause
                .contains("holds no other machine's update meanwhile"),
            "{}",
            step.transitions[0].cause
        );

        // bbs and fedora finish; the Spark's long cell runs on.
        fleet[0].work.clear();
        fleet[2].work.clear();
        fleet[1].work.truncate(1);
        let mut now = NOW + 60_000;
        let step = rollout.advance(&fleet, &artifacts, now);
        assert_eq!(held(&step), ["id-bbs", "id-bginf-sp2"]);
        assert_eq!(phase(&rollout, &fleet[2], &artifacts), waiting(1, "bbs"));
        assert!(matches!(
            phase(&rollout, &fleet[1], &artifacts),
            SelfUpdatePhase::Draining {
                in_flight: 1,
                waits_for_turn: true,
                updating: Some(ref name),
                ..
            } if name == "bbs"
        ));

        let mut order = Vec::new();
        for _ in 0..6 {
            now += 1_000;
            fleet.iter_mut().for_each(settle);
            let step = rollout.advance(&fleet, &artifacts, now);
            if let Some(direct) = step.direct {
                let index = index_of(&fleet, &direct.executor_id);
                order.push(fleet[index].name.clone());
                return_on(&mut fleet[index], "new");
            }
            assert!(
                fleet
                    .iter()
                    .filter(|machine| matches!(
                        phase(&rollout, machine, &artifacts),
                        SelfUpdatePhase::Directed { .. }
                    ))
                    .count()
                    <= 1
            );
        }
        assert_eq!(order, ["bbs", "fedora"], "the idle two, one at a time");
        let step = rollout.advance(&fleet, &artifacts, now + 1_000);
        assert_eq!(held(&step), ["id-bginf-sp2"]);
        assert!(matches!(
            phase(&rollout, &fleet[1], &artifacts),
            SelfUpdatePhase::Draining {
                in_flight: 1,
                waits_for_turn: true,
                updating: None,
                ..
            }
        ));

        fleet[1].work.clear();
        let step = rollout.advance(&fleet, &artifacts, now + 2_000);
        assert_eq!(
            step.direct.map(|direct| direct.executor_id),
            Some("id-bginf-sp2".to_string()),
            "it updates the pass its cell ends"
        );
    }

    /// A draining machine whose cells end while another machine is mid-update
    /// waits for that one, then takes the turn ahead of an idle machine that
    /// has refused no work.
    #[test]
    fn a_drained_machine_takes_the_turn_next_once_the_machine_updating_returns() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![
            remote("bbs", "old"),
            remote("bginf-sp2", "old"),
            remote("fedora", "old"),
        ];
        fleet[1].work = vec![command("cell", NOW, None)];
        assert_eq!(
            held(&rollout.advance(&fleet, &artifacts, NOW)),
            ["id-bbs", "id-bginf-sp2"]
        );
        fleet.iter_mut().for_each(settle);
        let step = rollout.advance(&fleet, &artifacts, NOW + 1_000);
        assert_eq!(
            step.direct.map(|direct| direct.executor_id),
            Some("id-bbs".to_string())
        );

        // The Spark's cell ends while bbs is away installing.
        fleet[1].work.clear();
        let step = rollout.advance(&fleet, &artifacts, NOW + 2_000);
        assert_eq!(step.direct, None, "bbs is still mid-update");
        assert_eq!(held(&step), ["id-bbs", "id-bginf-sp2"]);
        assert_eq!(
            phase(&rollout, &fleet[1], &artifacts),
            SelfUpdatePhase::Draining {
                in_flight: 0,
                since_unix_ms: NOW,
                holds: Vec::new(),
                cycles: Vec::new(),
                waits_for_turn: true,
                updating: Some("bbs".into()),
            }
        );
        assert_eq!(
            phase(&rollout, &fleet[2], &artifacts),
            waiting(2, "bbs"),
            "bbs, then the drained Spark"
        );

        return_on(&mut fleet[0], "new");
        let step = rollout.advance(&fleet, &artifacts, NOW + 3_000);
        assert_eq!(step.transitions[0].kind, TransitionKind::Updated);
        assert_eq!(
            step.direct.map(|direct| direct.executor_id),
            Some("id-bginf-sp2".to_string()),
            "the drained machine goes before the idle one"
        );
        assert_eq!(
            phase(&rollout, &fleet[2], &artifacts),
            waiting(1, "bginf-sp2")
        );

        return_on(&mut fleet[1], "new");
        assert_eq!(
            held(&rollout.advance(&fleet, &artifacts, NOW + 4_000)),
            ["id-fedora"]
        );
        settle(&mut fleet[2]);
        let step = rollout.advance(&fleet, &artifacts, NOW + 5_000);
        assert_eq!(
            step.direct.map(|direct| direct.executor_id),
            Some("id-fedora".to_string())
        );
    }

    /// Work can reach a machine after it takes the turn: bound before the
    /// hold, or the next command for something resident there. The turn is
    /// for a machine that can update now, so it steps aside, draining beside
    /// the turn when no one else drains and taking work again when another
    /// machine does.
    #[test]
    fn a_machine_that_finds_work_after_taking_the_turn_steps_aside() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![remote("a", "old"), remote("b", "old"), remote("c", "old")];
        with_work(&mut fleet[2], 1);
        assert_eq!(
            held(&rollout.advance(&fleet, &artifacts, NOW)),
            ["id-a", "id-c"]
        );

        with_work(&mut fleet[0], 1);
        let step = rollout.advance(&fleet, &artifacts, NOW + 1_000);
        assert_eq!(held(&step), ["id-b", "id-c"], "a takes work again");
        assert!(step.transitions.iter().any(|transition| {
            transition.executor_id == "id-a"
                && transition.kind == TransitionKind::Abandoned
                && transition.cause.contains("c is already draining")
        }));
        assert_eq!(phase(&rollout, &fleet[0], &artifacts), waiting(2, "b"));

        let mut rollout = Rollout::default();
        let mut alone = remote("a", "old");
        rollout.advance(std::slice::from_ref(&alone), &artifacts, NOW);
        with_work(&mut alone, 1);
        let step = rollout.advance(std::slice::from_ref(&alone), &artifacts, NOW + 1_000);
        assert_eq!(held(&step), ["id-a"], "still refused new work");
        assert_eq!(step.transitions[0].kind, TransitionKind::Draining);
        assert!(matches!(
            phase(&rollout, &alone, &artifacts),
            SelfUpdatePhase::Draining {
                in_flight: 1,
                since_unix_ms: NOW,
                waits_for_turn: true,
                ..
            }
        ));
        alone.work.clear();
        settle(&mut alone);
        let step = rollout.advance(std::slice::from_ref(&alone), &artifacts, NOW + 2_000);
        assert!(step.direct.is_some(), "and updates once it is empty");
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
        assert_eq!(held(&rollout.advance(&fleet, &artifacts, NOW)), ["id-busy"]);
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
        assert_eq!(held(&step), ["id-busy"]);

        let step = rollout.advance(&fleet, &artifacts, deadline);
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
        assert!(
            step.transitions[0]
                .cause
                .contains("`rust-tests` was expected to finish 10 minutes ago"),
            "{}",
            step.transitions[0].cause
        );
        assert_eq!(held(&step), ["id-idle"], "the next machine takes the turn");
        assert_eq!(
            phase(&rollout, &fleet[0], &artifacts),
            SelfUpdatePhase::Deferred {
                until_unix_ms: deadline + DEFER_MS
            }
        );
    }

    /// The fedora specimen (cairn/5672): a whole-suite pass with a learned
    /// hour ran 170 minutes on a disk stalled 40-80%, emitting results the
    /// whole time. Past its bound it keeps the turn for as long as its own
    /// stall stop sees its tree progress, the drain says so, and the machine
    /// updates the moment the pass finishes.
    #[test]
    fn work_past_its_bound_that_is_still_progressing_keeps_the_turn() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = suite_fleet();
        let deadline = NOW + HOUR + HOUR / 2;
        for minute in [0, 90, 100, 130, 169] {
            let now = NOW + minute * 60_000;
            fleet[0].work = vec![sampled(
                command("rust-tests", NOW, Some(HOUR)),
                2 * 60_000,
                REVIEW_BOUND_MS,
                now,
            )];
            let step = rollout.advance(&fleet, &artifacts, now);
            assert_eq!(held(&step), ["id-fedora"], "at {minute}");
            assert!(
                step.transitions.is_empty() || minute == 0,
                "{:?}",
                step.transitions
            );
            let SelfUpdatePhase::Draining { holds, .. } = phase(&rollout, &fleet[0], &artifacts)
            else {
                panic!("draining at {minute}");
            };
            assert_eq!(holds[0].progressing, Some(true));
            assert_eq!(holds[0].deadline_unix_ms, deadline);
        }

        fleet[0].work.clear();
        settle(&mut fleet[0]);
        let step = rollout.advance(&fleet, &artifacts, NOW + 170 * 60_000);
        assert_eq!(
            step.direct.map(|direct| direct.executor_id),
            Some("id-fedora".to_string()),
            "it updates as soon as the pass finishes"
        );
    }

    /// Past its bound, work whose tree has gone quiet for its whole stall
    /// bound, by output, CPU and storage alike, is wedged, and the turn passes.
    #[test]
    fn work_past_its_bound_and_silent_on_its_instruments_passes_the_turn() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = suite_fleet();
        rollout.advance(&fleet, &artifacts, NOW);

        let now = NOW + 95 * 60_000;
        fleet[0].work = vec![sampled(
            command("rust-tests", NOW, Some(HOUR)),
            REVIEW_BOUND_MS - 1,
            REVIEW_BOUND_MS,
            now,
        )];
        let step = rollout.advance(&fleet, &artifacts, now);
        assert_eq!(
            held(&step),
            ["id-fedora"],
            "quiet, but not for its whole bound"
        );

        let now = now + 1;
        fleet[0].work = vec![sampled(
            command("rust-tests", NOW, Some(HOUR)),
            REVIEW_BOUND_MS,
            REVIEW_BOUND_MS,
            now,
        )];
        let step = rollout.advance(&fleet, &artifacts, now);
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
        assert!(
            step.transitions[0].cause.contains(
                "`rust-tests` was expected to finish 35 minutes ago, from its own learned duration of 60 minutes, and its tree has shown no progress for 30 minutes against its own 30-minute stall bound, so it is read as wedged"
            ),
            "{}",
            step.transitions[0].cause
        );
        assert_eq!(held(&step), ["id-busy"]);
    }

    /// A reading of progress is only as good as the sample that carried it:
    /// one that stops being refreshed ages, and stops holding the turn once
    /// its age makes up the bound.
    #[test]
    fn a_progress_reading_that_stops_being_refreshed_stops_holding_the_turn() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = suite_fleet();
        let last_sample = NOW + 100 * 60_000;
        fleet[0].work = vec![sampled(
            command("rust-tests", NOW, Some(HOUR)),
            0,
            REVIEW_BOUND_MS,
            last_sample,
        )];
        rollout.advance(&fleet, &artifacts, NOW);
        let step = rollout.advance(&fleet, &artifacts, last_sample + REVIEW_BOUND_MS - 1);
        assert_eq!(held(&step), ["id-fedora"]);
        let step = rollout.advance(&fleet, &artifacts, last_sample + REVIEW_BOUND_MS);
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
    }

    /// Where nothing can say whether the work progresses (a platform that
    /// cannot read the tree's CPU, or a stop with no complete reading), the
    /// progress test is off rather than half-run, and the bound alone decides,
    /// exactly as before.
    #[test]
    fn work_its_stall_stop_cannot_judge_is_bounded_by_its_duration_alone() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let fleet = suite_fleet();
        let deadline = NOW + HOUR + HOUR / 2;
        rollout.advance(&fleet, &artifacts, NOW);
        let SelfUpdatePhase::Draining { holds, .. } = phase(&rollout, &fleet[0], &artifacts) else {
            panic!("draining");
        };
        assert_eq!(holds[0].progressing, None);

        let step = rollout.advance(&fleet, &artifacts, deadline - 1);
        assert_eq!(held(&step), ["id-fedora"]);
        let step = rollout.advance(&fleet, &artifacts, deadline);
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
        assert!(
            step.transitions[0]
                .cause
                .contains("from its own learned duration of 60 minutes, so it is read as wedged"),
            "{}",
            step.transitions[0].cause
        );
    }

    /// An agent's batch carries a zero stall bound: no stop judges it, so its
    /// progress never keeps the turn, and it holds for the fixed hour like any
    /// work with no expected end.
    #[test]
    fn an_agent_batch_keeps_the_fixed_hold_and_is_never_judged_by_progress() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("a", "old");
        let batch = |now| sampled(command("`bun run dev`", NOW, None), 0, 0, now);
        machine.work = vec![batch(NOW)];
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);

        let now = NOW + UNMEASURED_HOLD_MS - 1;
        machine.work = vec![batch(now)];
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, now);
        assert_eq!(held(&step), ["id-a"]);
        let SelfUpdatePhase::Draining { holds, .. } = phase(&rollout, &machine, &artifacts) else {
            panic!("draining");
        };
        assert_eq!(holds[0].progressing, None, "a zero bound arms nothing");

        let now = NOW + UNMEASURED_HOLD_MS;
        machine.work = vec![batch(now)];
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, now);
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
        assert!(
            step.transitions[0].cause.contains(
                "has held the turn 60 minutes without finishing, so it is read as wedged"
            ),
            "{}",
            step.transitions[0].cause
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
        assert_eq!(held(&step), ["id-a"]);
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
            "service `axon/axon on bgsrv-mac`".into(),
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
                "the restart cycles service `iMessage channel` and service `axon/axon on bgsrv-mac`, which their owners place again"
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
        assert!(step.holds.is_empty(), "{:?}", step.holds);
    }

    /// The bound's conclusion is handed back as a fact about the machine, with
    /// when it was directed, exactly once: the build is halted, so no later
    /// pass directs it again or concludes it again.
    #[test]
    fn a_machine_that_never_returns_is_concluded_unreturned_once() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("bgsrv-win", "old");
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        settle(&mut machine);
        let directed = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 1_000);
        assert!(directed.direct.is_some());

        let early = rollout.advance(&[], &artifacts, NOW + RETURN_BUDGET_MS);
        assert!(
            early.unreturned.is_empty(),
            "inside the bound it is updating"
        );

        let step = rollout.advance(&[], &artifacts, NOW + 1_000 + RETURN_BUDGET_MS);
        assert_eq!(
            step.unreturned,
            [Unreturned {
                executor_id: "id-bgsrv-win".into(),
                build_id: "new".into(),
                directed_at_unix_ms: NOW + 1_000,
            }]
        );
        let later = rollout.advance(&[], &artifacts, NOW + 2 * RETURN_BUDGET_MS);
        assert!(later.unreturned.is_empty(), "{:?}", later.unreturned);
    }

    /// A machine that comes back inside the bound is an update, not a stranding.
    #[test]
    fn a_machine_that_returns_inside_the_bound_is_never_concluded_unreturned() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("bgsrv-win", "old");
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        settle(&mut machine);
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 1_000);
        rollout.advance(&[], &artifacts, NOW + RETURN_BUDGET_MS / 2);
        return_on(&mut machine, "new");
        let step = rollout.advance(
            std::slice::from_ref(&machine),
            &artifacts,
            NOW + RETURN_BUDGET_MS,
        );
        assert_eq!(step.transitions[0].kind, TransitionKind::Updated);
        assert!(step.unreturned.is_empty());
        let later = rollout.advance(
            std::slice::from_ref(&machine),
            &artifacts,
            NOW + 2 * RETURN_BUDGET_MS,
        );
        assert!(later.unreturned.is_empty());
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
        assert_eq!(held(&step), ["id-b"]);
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
                assert_eq!(held(&step), ["id-a"], "drained again on its new link");
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
        assert_eq!(held(&step), ["id-a"]);
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
        assert_eq!(held(&step), ["id-a"], "held while it installs");
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
