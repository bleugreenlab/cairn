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
//! An idle update head and one busy drain advance independently. Every restart
//! still requires empty work, reconciled runner bindings, fresh same-generation
//! snapshots, and the resident turn's fixed notice. Scheduling overruns never
//! prove command death: release the refusal once, then admit ordinary work while
//! the exact unresolved physical holders suppress another busy drain. Retargets
//! and reconnects do not make those holders new work. Boot-local episode history
//! records committed refusal intervals; a runner restart may relearn a holder at
//! the cost of one more drain. Sustained long work can postpone an update without
//! a deadline. Services cycle as before; terminals retain Announced/Ending notice.
//!
//! This module is the decision alone, over plain facts the fleet copies out of
//! its connection table, so every transition is testable without a link.

use std::collections::{HashMap, HashSet};

use cairn_common::executor_protocol::{
    describe_clock, CellOwnerRef, DrainHold, ResidencyHolder, ResidentHold, ResidentNotice,
    ResidentProcessKind, SelfUpdatePhase, SelfUpdateStatus, UpdateReport, UpdateState,
};

/// The least scheduling slack past a command's expected finish before an
/// unsuccessful drain releases admission. This is never a process lifetime.
pub(crate) const COMMAND_MARGIN_MS: u64 = 10 * 60 * 1000;
/// How long work with no expected end may hold a turn: a command never
/// measured, or an environment being acquired, neither of which says when it
/// will finish.
pub(crate) const UNMEASURED_HOLD_MS: u64 = 60 * 60 * 1000;
/// How long residents (terminals, REPLs, dev instances) may hold a turn,
/// from when it begins: long enough for their owners to read the notice and
/// save what they need, short enough that a resident costs the fleet minutes
/// rather than the machine.
pub(crate) const RESIDENT_NOTICE_MS: u64 = 5 * 60 * 1000;
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
/// The drain displays the executor's own progress evidence. It does not use
/// that sample to infer death or to override the executor's stall-stop policy.
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

    /// Whether the display should lead with work past its estimate that the
    /// executor still reports as progressing. This never authorizes a restart.
    fn held_past(hold: &DrainHold, now_unix_ms: u64) -> bool {
        now_unix_ms >= hold.deadline_unix_ms && hold.progressing == Some(true)
    }

    /// Expected finish, scheduling-cost bound and sampled progress, measured
    /// from when the drain first saw the work.
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
    /// Terminals, REPLs and dev instances on this machine. A restart ends
    /// them and they never finish on their own, so they hold a turn only for
    /// its notice period.
    pub residents: Vec<Resident>,
    pub snapshots_received: u64,
    /// Whether the link is open and fresh enough to carry a directive.
    pub reachable: bool,
}

/// One resident a restart of the machine would end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resident {
    /// Stable across passes, `process:{cell}/{process key}/{generation}`, so a
    /// turn tells each resident's owner once.
    pub key: String,
    /// The resident and whose it is, named the way an operator reads it.
    pub label: String,
    /// How the runner reaches its owner. The rollout carries it untouched.
    pub address: ResidentAddress,
}

/// Where a resident lives and who holds it, as its machine reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ResidentAddress {
    pub process_key: String,
    pub kind: Option<ResidentProcessKind>,
    pub holder: Option<ResidencyHolder>,
    /// Descriptive attribution only; a dev instance's never names a job,
    /// because that id keys lease adoption.
    pub owner: Option<CellOwnerRef>,
    /// The project the cell belongs to and the residency's selector (a dev
    /// instance's branch), which is how a dev instance's owning job is found.
    pub project_id: Option<String>,
    pub selector: Option<String>,
}

/// What a resident's owner is to be told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoticeKind {
    /// The drain first saw it: the restart comes no earlier than
    /// `restart_no_earlier_than_unix_ms`, and not before the machine's
    /// `commands_in_flight` finish.
    Announced {
        restart_no_earlier_than_unix_ms: u64,
        commands_in_flight: usize,
    },
    /// The restart that ends it was just directed.
    Ending,
}

/// A notice for one resident's owner. The rollout only decides that it is
/// due; the runner delivers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnerNotice {
    pub executor_id: String,
    pub name: String,
    pub build_id: String,
    /// The turn's operation, so one turn tells an owner each thing once.
    pub operation_id: String,
    pub resident: Resident,
    pub kind: NoticeKind,
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
    /// For a deferral, how many times this machine has now been deferred for
    /// this build.
    pub deferrals: Option<u32>,
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
    /// Residents' owners to tell what the update means for them.
    pub notices: Vec<OwnerNotice>,
}

/// Where a held machine's update stands, in the terms work waiting on it is
/// told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UpdateWait {
    /// Emptying: what it still waits on, latest bound first, the residents
    /// the restart will end, and when it is expected back.
    Draining {
        holds: Vec<DrainHold>,
        residents: Vec<String>,
        returns_by_unix_ms: Option<u64>,
    },
    /// Directed to install the build and awaited on its return.
    Directed {
        at_unix_ms: u64,
        returns_by_unix_ms: u64,
    },
}

#[derive(Debug, Clone)]
struct Turn {
    executor_id: String,
    name: String,
    build_id: String,
    target: &'static str,
    operation_id: String,
    unexplained_returns: u8,
    /// When residents stop holding this turn: one bound for the turn, set
    /// when it begins and never extended by residents that appear later.
    residents_release_at_unix_ms: u64,
    /// Residents whose owners this turn has told it is coming, or is telling
    /// now. A delivery that fails takes its resident back out, so the next
    /// pass tells the owner again, against the same deadline.
    announced: HashSet<String>,
    /// Residents whose owners this turn has told the restart is directed, or
    /// is telling now, retried the same way while the turn awaits the return.
    ending_sent: HashSet<String>,
    /// What delivering each resident's notice came to, as the runner
    /// reported it back.
    told: HashMap<String, ResidentNotice>,
    phase: TurnPhase,
}

impl Turn {
    /// Tell the owner of each resident this turn has not yet seen.
    fn announce(&mut self, candidate: &Candidate, step: &mut Step) {
        for resident in &candidate.residents {
            if self.announced.insert(resident.key.clone()) {
                step.notices.push(self.notice(
                    resident,
                    NoticeKind::Announced {
                        restart_no_earlier_than_unix_ms: self.residents_release_at_unix_ms,
                        commands_in_flight: candidate.work.len(),
                    },
                ));
            }
        }
    }

    /// Tell every resident's owner that the restart ending it is directed.
    fn end_residents(&mut self, residents: &[Resident], step: &mut Step) {
        for resident in residents {
            self.announced.insert(resident.key.clone());
            if self.ending_sent.insert(resident.key.clone()) {
                step.notices.push(self.notice(resident, NoticeKind::Ending));
            }
        }
    }

    fn notice(&self, resident: &Resident, kind: NoticeKind) -> OwnerNotice {
        OwnerNotice {
            executor_id: self.executor_id.clone(),
            name: self.name.clone(),
            build_id: self.build_id.clone(),
            operation_id: self.operation_id.clone(),
            resident: resident.clone(),
            kind,
        }
    }

    /// The residents named, for a surface, with their notice period and what
    /// their owners were told.
    fn resident_holds<'a>(
        &self,
        residents: impl Iterator<Item = (&'a String, &'a String)>,
    ) -> Vec<ResidentHold> {
        residents
            .map(|(key, label)| ResidentHold {
                label: label.clone(),
                ends_at_unix_ms: self.residents_release_at_unix_ms,
                notice: self.told.get(key).copied().unwrap_or_default(),
            })
            .collect()
    }
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
        /// Residents the restart will end, as `(key, label)`.
        residents: Vec<(String, String)>,
        /// A resident-only restart bound; unknown while any work remains.
        restart_expected_unix_ms: Option<u64>,
    },
    Directed {
        at_unix_ms: u64,
        generation: u64,
        cycled: Vec<String>,
        /// Residents the restart ended, kept whole so a failed Ending notice
        /// can be sent again while the machine is away.
        ended: Vec<Resident>,
    },
}

impl TurnPhase {
    /// A drain's account of the work it found on one pass.
    #[allow(clippy::too_many_arguments)]
    fn draining(
        candidate: &Candidate,
        since_unix_ms: u64,
        generation: u64,
        snapshots_at_hold: u64,
        first_seen: HashMap<String, u64>,
        residents_release_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> Self {
        let (first_seen, holds) = account(&candidate.work, first_seen, now_unix_ms);
        let restart_expected_unix_ms = restart_expected(
            &holds,
            !candidate.residents.is_empty(),
            residents_release_at_unix_ms,
            since_unix_ms,
        );
        Self::Draining {
            since_unix_ms,
            generation,
            snapshots_at_hold,
            first_seen,
            holds,
            cycles: candidate.services.clone(),
            residents: candidate
                .residents
                .iter()
                .map(|resident| (resident.key.clone(), resident.label.clone()))
                .collect(),
            restart_expected_unix_ms,
        }
    }
}

/// A scheduling budget is not a safe restart deadline. Only a machine with
/// no work in flight has a restart bound (its residents' fixed notice).
fn restart_expected(
    holds: &[DrainHold],
    has_residents: bool,
    residents_release_at_unix_ms: u64,
    since_unix_ms: u64,
) -> Option<u64> {
    holds.is_empty().then_some(if has_residents {
        residents_release_at_unix_ms
    } else {
        since_unix_ms
    })
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
pub(crate) fn listed(items: &[String]) -> String {
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
#[derive(Debug, Default, Clone)]
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
    /// An unsuccessful drain suppresses another until its exact holders settle.
    deferred: HashMap<String, Deferral>,
    /// Cost and holder history belong to the update episode, not its target build.
    episodes: HashMap<String, UpdateEpisode>,
    /// Machines that did not act on a directive, with the build and why.
    excluded: HashMap<String, (String, String)>,
}

/// Absence is considered only after a fresh same-generation account, joined
/// with the runner's bindings and unsettled check claims.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Deferral {
    blockers: HashMap<String, String>,
    generation: u64,
    snapshots_at_release: u64,
}

#[derive(Debug, Clone, Default)]
struct UpdateEpisode {
    since_unix_ms: u64,
    unsuccessful_drains: u32,
    release_counted: bool,
    refused_ms: u64,
    refusal_since_unix_ms: Option<u64>,
    operation_id: Option<String>,
    released_holders: HashSet<String>,
    repeated_holder: bool,
    completed: bool,
}

impl UpdateEpisode {
    fn close_refusal(&mut self, now_unix_ms: u64) {
        if let Some(since) = self.refusal_since_unix_ms.take() {
            self.refused_ms = self
                .refused_ms
                .saturating_add(now_unix_ms.saturating_sub(since));
            if !self.completed && !self.release_counted {
                self.unsuccessful_drains = self.unsuccessful_drains.saturating_add(1);
            }
            self.release_counted = false;
        }
    }

    fn event(&self, phase: &str, now_unix_ms: u64) -> serde_json::Value {
        serde_json::json!({
            "phase": phase,
            "operationId": self.operation_id,
            "pendingSinceUnixMs": self.since_unix_ms,
            "unsuccessfulDrains": self.unsuccessful_drains,
            "refusedMs": self.refusal_at(now_unix_ms),
            "repeatedHolder": self.repeated_holder,
            "holders": self.released_holders,
            "costMeaning": "time the self-update refusal stood this boot, not marginal capacity loss",
        })
    }

    fn refusal_at(&self, now_unix_ms: u64) -> u64 {
        self.refused_ms.saturating_add(
            self.refusal_since_unix_ms
                .map_or(0, |since| now_unix_ms.saturating_sub(since)),
        )
    }
}

/// The durable admission hold a proposed rollout needs before it can act.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SelfUpdateDrain {
    pub executor_id: String,
    pub operation_id: String,
    pub build_id: String,
}

impl Rollout {
    /// Called only after the exact owned drain mutation commits. Count the
    /// union of intervals during which our refusal stood, not a sum per holder.
    pub(crate) fn record_committed_drains(
        &mut self,
        now_unix_ms: u64,
    ) -> Vec<(String, serde_json::Value)> {
        let required: HashMap<_, _> = self
            .required_drains()
            .into_iter()
            .map(|hold| (hold.executor_id.clone(), hold))
            .collect();
        let mut events = Vec::new();
        for (executor_id, episode) in &mut self.episodes {
            let standing = required.get(executor_id);
            if episode.refusal_since_unix_ms.is_some()
                && standing
                    .is_some_and(|hold| episode.operation_id.as_ref() != Some(&hold.operation_id))
            {
                episode.close_refusal(now_unix_ms);
                events.push((
                    executor_id.clone(),
                    episode.event("refusalEnded", now_unix_ms),
                ));
                if episode.completed {
                    *episode = UpdateEpisode {
                        since_unix_ms: now_unix_ms,
                        ..UpdateEpisode::default()
                    };
                }
            }
            let phase = match (episode.refusal_since_unix_ms, standing) {
                (None, Some(hold)) => {
                    episode.refusal_since_unix_ms = Some(now_unix_ms);
                    episode.operation_id = Some(hold.operation_id.clone());
                    Some("refusalStarted")
                }
                (Some(_), None) => {
                    episode.close_refusal(now_unix_ms);
                    Some("refusalEnded")
                }
                _ => None,
            };
            if let Some(phase) = phase {
                events.push((executor_id.clone(), episode.event(phase, now_unix_ms)));
            }
        }
        events
    }

    pub(crate) fn required_drains(&self) -> Vec<SelfUpdateDrain> {
        self.turns()
            .map(|(_, turn)| SelfUpdateDrain {
                executor_id: turn.executor_id.clone(),
                operation_id: turn.operation_id.clone(),
                build_id: turn.build_id.clone(),
            })
            .collect()
    }

    /// Whether this machine is being emptied or awaited, in either slot.
    pub(crate) fn holds(&self, executor_id: &str) -> bool {
        self.turns()
            .any(|(_, turn)| turn.executor_id == executor_id)
    }

    /// Where this machine's update stands for work waiting on it, or `None`
    /// when no turn holds it.
    pub(crate) fn update_wait(&self, executor_id: &str) -> Option<UpdateWait> {
        self.turns()
            .find(|(_, turn)| turn.executor_id == executor_id)
            .map(|(_, turn)| match &turn.phase {
                TurnPhase::Draining {
                    holds,
                    residents,
                    restart_expected_unix_ms,
                    ..
                } => UpdateWait::Draining {
                    holds: holds.clone(),
                    residents: residents.iter().map(|(_, label)| label.clone()).collect(),
                    returns_by_unix_ms: restart_expected_unix_ms
                        .map(|at| at.saturating_add(RETURN_BUDGET_MS)),
                },
                TurnPhase::Directed { at_unix_ms, .. } => UpdateWait::Directed {
                    at_unix_ms: *at_unix_ms,
                    returns_by_unix_ms: at_unix_ms.saturating_add(RETURN_BUDGET_MS),
                },
            })
    }

    /// When this machine is expected back on the runner's build, for work
    /// waiting on it: its expected restart plus the return budget while it
    /// drains, the end of the return budget once directed. `None` when no
    /// turn holds it, or while any physical work or result debt remains.
    pub(crate) fn returns_by(&self, executor_id: &str) -> Option<u64> {
        let (_, turn) = self
            .turns()
            .find(|(_, turn)| turn.executor_id == executor_id)?;
        match &turn.phase {
            TurnPhase::Draining {
                restart_expected_unix_ms,
                ..
            } => restart_expected_unix_ms.map(|at| at.saturating_add(RETURN_BUDGET_MS)),
            TurnPhase::Directed { at_unix_ms, .. } => {
                Some(at_unix_ms.saturating_add(RETURN_BUDGET_MS))
            }
        }
    }

    /// A draining machine takes the head only once the head is free, so its
    /// restart comes no earlier than the head machine's return: the bound it
    /// publishes is the later of its own and the head's latest return, and
    /// open while the head's is.
    fn bound_drain_by_head(&mut self) {
        let Some(head) = self.head.as_ref() else {
            return;
        };
        let head_back = match &head.phase {
            TurnPhase::Directed { at_unix_ms, .. } => {
                Some(at_unix_ms.saturating_add(RETURN_BUDGET_MS))
            }
            TurnPhase::Draining {
                restart_expected_unix_ms,
                ..
            } => restart_expected_unix_ms.map(|at| at.saturating_add(RETURN_BUDGET_MS)),
        };
        if let Some(TurnPhase::Draining {
            restart_expected_unix_ms,
            ..
        }) = self.drain.as_mut().map(|turn| &mut turn.phase)
        {
            *restart_expected_unix_ms = match (*restart_expected_unix_ms, head_back) {
                (Some(own), Some(head)) => Some(own.max(head)),
                _ => None,
            };
        }
    }

    /// Record what delivering a resident's notice came to, for the page.
    /// A delivery that could not be made (`Pending`) makes that notice due
    /// again, so the next pass retries it under the same idempotency key and
    /// against the same deadline. Ignored when the turn it was for has ended.
    pub(crate) fn note_resident_notice(
        &mut self,
        executor_id: &str,
        operation_id: &str,
        resident_key: &str,
        kind: NoticeKind,
        notice: ResidentNotice,
    ) {
        for turn in [self.head.as_mut(), self.drain.as_mut()]
            .into_iter()
            .flatten()
        {
            if turn.executor_id == executor_id && turn.operation_id == operation_id {
                if notice == ResidentNotice::Pending {
                    match kind {
                        NoticeKind::Announced { .. } => turn.announced.remove(resident_key),
                        NoticeKind::Ending => turn.ending_sent.remove(resident_key),
                    };
                }
                // A told owner stays told; a later notice never unsays it.
                let entry = turn.told.entry(resident_key.to_string()).or_insert(notice);
                if !matches!(entry, ResidentNotice::Told { .. }) {
                    *entry = notice;
                }
            }
        }
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
        for candidate in candidates {
            let Some(ArtifactBuild::Resolved(build)) =
                candidate.target.and_then(|target| artifacts.get(target))
            else {
                continue;
            };
            if candidate.running_build.as_ref() == Some(build) {
                self.deferred.remove(&candidate.executor_id);
                if let Some(episode) = self.episodes.get_mut(&candidate.executor_id) {
                    episode.completed = true;
                }
            } else {
                let episode = self
                    .episodes
                    .entry(candidate.executor_id.clone())
                    .or_default();
                if episode.completed {
                    *episode = UpdateEpisode::default();
                }
                if episode.since_unix_ms == 0 {
                    episode.since_unix_ms = now_unix_ms;
                }
            }
        }
        self.deferred.retain(|executor_id, deferral| {
            let Some(candidate) = candidates.iter().find(|c| &c.executor_id == executor_id) else {
                return true;
            };
            if candidate.generation != deferral.generation {
                deferral.generation = candidate.generation;
                deferral.snapshots_at_release = candidate.snapshots_received;
                return true;
            }
            if candidate.reachable
                && candidate.snapshots_received
                    >= deferral
                        .snapshots_at_release
                        .saturating_add(FRESH_SNAPSHOTS)
            {
                deferral
                    .blockers
                    .retain(|key, _| candidate.work.iter().any(|w| &w.key == key));
            }
            !deferral.blockers.is_empty()
        });
        // A physical identity cannot become new work on reconnect or retarget.
        // If a reconciled holder reappears, surface an incident instead of paying
        // for another drain of the same work.
        for candidate in candidates {
            if self.deferred.contains_key(&candidate.executor_id)
                || self.holds(&candidate.executor_id)
            {
                continue;
            }
            let Some(episode) = self.episodes.get_mut(&candidate.executor_id) else {
                continue;
            };
            let blockers: HashMap<_, _> = candidate
                .work
                .iter()
                .filter(|w| episode.released_holders.contains(&w.key))
                .map(|w| (w.key.clone(), w.label.clone()))
                .collect();
            if blockers.is_empty() || episode.completed {
                continue;
            }
            if !episode.repeated_holder {
                episode.repeated_holder = true;
                step.transitions.push(Transition {
                    executor_id: candidate.executor_id.clone(),
                    kind: TransitionKind::Deferred,
                    build_id: candidate.target.and_then(|t| artifacts.get(t))
                        .and_then(|a| match a { ArtifactBuild::Resolved(b) => Some(b.clone()), _ => None })
                        .unwrap_or_default(),
                    cause: "incident: an unresolved physical holder reappeared after reconciliation; admission remains open and another drain is suppressed".into(),
                    deferrals: Some(episode.unsuccessful_drains),
                });
            }
            self.deferred.insert(
                candidate.executor_id.clone(),
                Deferral {
                    blockers,
                    generation: candidate.generation,
                    snapshots_at_release: candidate.snapshots_received,
                },
            );
        }
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
                .is_some_and(|turn| ready(turn, candidates, now_unix_ms))
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
        self.bound_drain_by_head();
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
                // The residents' notice period stands as it was: their
                // owners were told the restart was coming, and a link that
                // dropped under the directive does not extend it.
                turn.phase = TurnPhase::Draining {
                    since_unix_ms: now_unix_ms,
                    generation: 0,
                    snapshots_at_hold: 0,
                    first_seen: HashMap::new(),
                    holds: Vec::new(),
                    cycles: Vec::new(),
                    residents: Vec::new(),
                    restart_expected_unix_ms: Some(now_unix_ms),
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
                    residents,
                    restart_expected_unix_ms,
                    ..
                } => SelfUpdatePhase::Draining {
                    in_flight: u32::try_from(holds.len()).unwrap_or(u32::MAX),
                    since_unix_ms: *since_unix_ms,
                    holds: holds.clone(),
                    cycles: cycles.clone(),
                    waits_for_turn: slot == Slot::Drain,
                    updating: updating.filter(|_| slot == Slot::Drain),
                    residents: turn
                        .resident_holds(residents.iter().map(|(key, label)| (key, label))),
                    restart_expected_unix_ms: *restart_expected_unix_ms,
                },
                TurnPhase::Directed {
                    at_unix_ms,
                    cycled,
                    ended,
                    ..
                } => SelfUpdatePhase::Directed {
                    at_unix_ms: *at_unix_ms,
                    cycled: cycled.clone(),
                    ended: turn.resident_holds(
                        ended
                            .iter()
                            .map(|resident| (&resident.key, &resident.label)),
                    ),
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
        if let Some(deferral) = self.deferred.get(&candidate.executor_id) {
            let episode = &self.episodes[&candidate.executor_id];
            let mut blockers: Vec<_> = deferral.blockers.values().cloned().collect();
            blockers.sort();
            return SelfUpdatePhase::WaitingForSettlement {
                blockers,
                since_unix_ms: episode.since_unix_ms,
                unsuccessful_drains: episode.unsuccessful_drains,
                refused_ms: episode.refused_ms,
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
                            deferrals: None,
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
                        deferrals: None,
                    });
                    return None;
                }
            }
        }
        let (turn_executor, turn_build) = (turn.executor_id.clone(), turn.build_id.clone());
        let transition = move |kind, cause: String| Transition {
            executor_id: turn_executor.clone(),
            kind,
            build_id: turn_build.clone(),
            cause,
            deferrals: None,
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
                ended: Vec::new(),
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
                        turn.residents_release_at_unix_ms,
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
                    turn.announce(candidate, step);
                    self.drain = Some(turn);
                    return None;
                }
                // Residents hold the head too, but only to the turn's one
                // notice bound: a resident that appeared after the turn began
                // gets what is left of it, never a period of its own.
                let notice_served = candidate.residents.is_empty()
                    || now_unix_ms >= turn.residents_release_at_unix_ms;
                if slot == Slot::Head
                    && notice_served
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
                    let ending = if candidate.residents.is_empty() {
                        String::new()
                    } else {
                        let ended: Vec<String> = candidate
                            .residents
                            .iter()
                            .map(|resident| {
                                let told = match turn.told.get(&resident.key) {
                                    Some(ResidentNotice::Told { at_unix_ms }) => format!(
                                        " (its owner was told at {})",
                                        describe_clock(*at_unix_ms)
                                    ),
                                    Some(ResidentNotice::OwnerNotFound) => {
                                        " (no owner was found to tell)".to_string()
                                    }
                                    _ => String::new(),
                                };
                                format!("{}{told}", resident.label)
                            })
                            .collect();
                        format!(
                            "; the restart ends {}, whose notice period ended at {}",
                            listed(&ended),
                            describe_clock(turn.residents_release_at_unix_ms)
                        )
                    };
                    let state = if candidate.residents.is_empty() {
                        "is empty"
                    } else {
                        "has no work in flight"
                    };
                    step.transitions.push(transition(
                        TransitionKind::Directed,
                        format!(
                            "{} {state}; directed to install {}{cycling}{ending}",
                            turn.name, turn.build_id
                        ),
                    ));
                    turn.end_residents(&candidate.residents, step);
                    turn.phase = TurnPhase::Directed {
                        at_unix_ms: now_unix_ms,
                        generation,
                        cycled: candidate.services.clone(),
                        ended: candidate.residents.clone(),
                    };
                    return Some(turn);
                }
                turn.phase = TurnPhase::draining(
                    candidate,
                    since_unix_ms,
                    generation,
                    snapshots_at_hold,
                    first_seen,
                    turn.residents_release_at_unix_ms,
                    now_unix_ms,
                );
                turn.announce(candidate, step);
                // Only a busy drain has a scheduling overrun. An empty head
                // waits for confirmation; residents retain their fixed notice.
                if slot == Slot::Head {
                    return Some(turn);
                }
                let TurnPhase::Draining { first_seen, .. } = &turn.phase else {
                    unreachable!("just set to draining");
                };
                // This bounds the scheduling cost, never the command's life.
                // Neither an estimate nor a quiet sample proves process death.
                let overrun = candidate
                    .work
                    .iter()
                    .map(|item| {
                        let seen = first_seen.get(&item.key).copied().unwrap_or(now_unix_ms);
                        (item, seen, item.hold(seen, now_unix_ms))
                    })
                    .filter(|(_, _, hold)| now_unix_ms >= hold.deadline_unix_ms)
                    .min_by_key(|(_, _, hold)| hold.deadline_unix_ms);
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
                    let episode = self
                        .episodes
                        .entry(turn.executor_id.clone())
                        .or_insert_with(|| UpdateEpisode {
                            since_unix_ms,
                            ..UpdateEpisode::default()
                        });
                    episode.unsuccessful_drains = episode.unsuccessful_drains.saturating_add(1);
                    episode.release_counted = true;
                    let count = episode.unsuccessful_drains;
                    let blockers: HashMap<_, _> = candidate
                        .work
                        .iter()
                        .map(|w| (w.key.clone(), w.label.clone()))
                        .collect();
                    episode.released_holders.extend(blockers.keys().cloned());
                    self.deferred.insert(
                        turn.executor_id.clone(),
                        Deferral {
                            blockers,
                            generation: candidate.generation,
                            snapshots_at_release: candidate.snapshots_received,
                        },
                    );
                    let mut deferred = transition(
                        TransitionKind::Deferred,
                        format!(
                            "{}'s {} {why}; update not installed; scheduling bound exceeded, not a death verdict; admitting work, with no new drain until these physical attempts settle ({count} unsuccessful drains this boot)",
                            turn.name, item.label,
                        ),
                    );
                    deferred.deferrals = Some(count);
                    step.transitions.push(deferred);
                    return None;
                }
                Some(turn)
            }
            TurnPhase::Directed {
                at_unix_ms,
                generation,
                cycled,
                ended,
            } => {
                let elapsed = now_unix_ms.saturating_sub(at_unix_ms);
                match candidate {
                    Some(candidate) if candidate.generation != generation => {
                        if candidate.running_build.as_deref() == Some(turn.build_id.as_str()) {
                            if let Some(episode) = self.episodes.get_mut(&turn.executor_id) {
                                episode.completed = true;
                            }
                            self.deferred.remove(&turn.executor_id);
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
                            // A retry is a drain of its own: residents
                            // started since are new processes, owed a notice
                            // period of their own.
                            turn.unexplained_returns += 1;
                            turn.residents_release_at_unix_ms = now_unix_ms + RESIDENT_NOTICE_MS;
                            turn.phase = TurnPhase::draining(
                                candidate,
                                now_unix_ms,
                                candidate.generation,
                                candidate.snapshots_received,
                                HashMap::new(),
                                turn.residents_release_at_unix_ms,
                                now_unix_ms,
                            );
                            turn.announce(candidate, step);
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
                        // An Ending notice that could not be delivered is
                        // sent again while the machine is away.
                        turn.end_residents(&ended, step);
                        turn.phase = TurnPhase::Directed {
                            at_unix_ms,
                            generation,
                            cycled,
                            ended,
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
    /// nothing in flight and no resident; for the drain, the least busy due
    /// machine with work in flight or a resident to give notice to, so a
    /// notice period never holds the fleet's head. Neither takes a machine
    /// already in the other slot.
    fn next<'a>(
        &self,
        slot: Slot,
        candidates: &'a [Candidate],
        artifacts: &HashMap<String, ArtifactBuild>,
    ) -> Option<(&'a Candidate, &'static str, String)> {
        candidates
            .iter()
            .filter(|candidate| !self.holds(&candidate.executor_id))
            .filter(|candidate| candidate.is_empty() == (slot == Slot::Head))
            .filter_map(|candidate| {
                self.due(candidate, artifacts)
                    .map(|(target, build_id)| (candidate, target, build_id))
            })
            .min_by(|(left, ..), (right, ..)| {
                (left.work.len(), left.residents.len(), &left.name).cmp(&(
                    right.work.len(),
                    right.residents.len(),
                    &right.name,
                ))
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
                let idle = candidate.reachable && candidate.is_empty();
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

/// Whether a draining machine can take the head: reachable, nothing in
/// flight, and either no resident or the turn's notice period served.
fn ready(turn: &Turn, candidates: &[Candidate], now_unix_ms: u64) -> bool {
    candidates.iter().any(|candidate| {
        candidate.executor_id == turn.executor_id
            && candidate.reachable
            && candidate.work.is_empty()
            && (candidate.residents.is_empty() || now_unix_ms >= turn.residents_release_at_unix_ms)
    })
}

impl Candidate {
    /// Nothing a restart would end: no work in flight and no resident.
    fn is_empty(&self) -> bool {
        self.work.is_empty() && self.residents.is_empty()
    }
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
    let residents_release_at_unix_ms = now_unix_ms + RESIDENT_NOTICE_MS;
    let waiting_on = match slot {
        Slot::Head => {
            "; it has nothing in flight, so it takes the turn and takes no new work until it is directed"
                .to_string()
        }
        Slot::Drain => {
            let work = match candidate.work.len() {
                0 => String::new(),
                count => format!(" until its {count} in flight have finished"),
            };
            let residents = if candidate.residents.is_empty() {
                String::new()
            } else {
                let labels: Vec<String> = candidate
                    .residents
                    .iter()
                    .map(|resident| resident.label.clone())
                    .collect();
                format!(
                    "; the restart ends {}, whose owners are told now and which hold it no later than {}",
                    listed(&labels),
                    describe_clock(residents_release_at_unix_ms)
                )
            };
            format!(
                "; it takes no new work{work}, and holds no other machine's update meanwhile{residents}"
            )
        }
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
        deferrals: None,
    });
    let mut turn = Turn {
        executor_id: candidate.executor_id.clone(),
        name: candidate.name.clone(),
        build_id,
        target,
        operation_id: uuid::Uuid::new_v4().to_string(),
        unexplained_returns: 0,
        residents_release_at_unix_ms,
        announced: HashSet::new(),
        ending_sent: HashSet::new(),
        told: HashMap::new(),
        phase: TurnPhase::draining(
            candidate,
            now_unix_ms,
            candidate.generation,
            candidate.snapshots_received,
            HashMap::new(),
            residents_release_at_unix_ms,
            now_unix_ms,
        ),
    };
    turn.announce(candidate, step);
    turn
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
            residents: Vec::new(),
            snapshots_received: 10,
            reachable: true,
        }
    }

    /// A terminal of cairn/6613's, as the fleet labels one.
    fn terminal(slug: &str) -> Resident {
        Resident {
            key: format!("process:cell-1/{slug}/1"),
            label: format!("terminal `{slug}` of cairn/6613"),
            address: ResidentAddress::default(),
        }
    }

    fn notice_kinds(step: &Step) -> Vec<NoticeKind> {
        step.notices.iter().map(|notice| notice.kind).collect()
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
                ended: Vec::new(),
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
                residents: Vec::new(),
                // Nothing of its own holds it, but bbs holds the turn: it
                // restarts no earlier than bbs's latest return.
                restart_expected_unix_ms: Some(NOW + 1_000 + RETURN_BUDGET_MS),
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

    /// Learned duration plus margin bounds the admission cost, not the command.
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
        assert!(matches!(
            phase(&rollout, &fleet[0], &artifacts),
            SelfUpdatePhase::WaitingForSettlement {
                unsuccessful_drains: 1,
                ref blockers,
                ..
            } if blockers == &["`rust-tests`".to_string()]
        ));
    }

    #[test]
    fn scheduling_overrun_does_not_judge_process_progress_or_rearm_the_same_holder() {
        for quiet in [0, REVIEW_BOUND_MS] {
            let mut rollout = Rollout::default();
            let mut machine = remote("fedora", "old");
            machine.work = vec![sampled(
                command("suite/attempt-1", NOW, Some(HOUR)),
                quiet,
                REVIEW_BOUND_MS,
                NOW,
            )];
            let artifacts = serving("new");
            rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
            let deadline = NOW + HOUR + HOUR / 2;
            let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, deadline);
            assert!(step.direct.is_none());
            assert!(!rollout.holds(&machine.executor_id));
            assert!(matches!(
                phase(&rollout, &machine, &artifacts),
                SelfUpdatePhase::WaitingForSettlement { .. }
            ));
            let step = rollout.advance(
                std::slice::from_ref(&machine),
                &artifacts,
                deadline + 10 * HOUR,
            );
            assert!(!rollout.holds(&machine.executor_id));
            assert!(step.direct.is_none());
        }
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
            step.transitions[0]
                .cause
                .contains("scheduling bound exceeded, not a death verdict"),
            "{}",
            step.transitions[0].cause
        );
    }

    /// Work with no expected end (a command never measured, a request still
    /// preparing its cell) holds the turn for the fixed hold from when the
    /// drain first saw it, however long before the drain it started.
    #[test]
    fn unmeasured_work_holds_the_turn_for_the_fixed_hold() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("a", "old");
        machine.work = vec![command(
            "request r-1 (preparing its cell)",
            NOW - 6 * 3_600_000,
            None,
        )];
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

    /// The fedora specimen (cairn/6630): one check lane and one terminal in
    /// flight. The terminal's owner is told when the drain begins, the lane
    /// finishes, the terminal does not, and once the notice period has passed
    /// the machine is directed and the restart ends the terminal, its owner
    /// told again. Nothing is ever deferred.
    #[test]
    fn a_lane_and_a_terminal_drain_then_the_terminal_is_cycled() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("fedora", "old");
        with_work(&mut machine, 1);
        machine.residents = vec![terminal("lab")];
        let mut transitions = Vec::new();

        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        assert_eq!(held(&step), ["id-fedora"]);
        assert_eq!(
            notice_kinds(&step),
            [NoticeKind::Announced {
                restart_no_earlier_than_unix_ms: NOW + RESIDENT_NOTICE_MS,
                commands_in_flight: 1,
            }]
        );
        let notice = step.notices[0].clone();
        assert_eq!(notice.resident.label, "terminal `lab` of cairn/6613");
        assert_eq!(notice.build_id, "new");
        transitions.extend(step.transitions);
        let SelfUpdatePhase::Draining {
            in_flight,
            residents,
            restart_expected_unix_ms,
            waits_for_turn,
            ..
        } = phase(&rollout, &machine, &artifacts)
        else {
            panic!("draining");
        };
        assert_eq!(in_flight, 1, "the terminal is not counted as work");
        assert!(waits_for_turn, "a resident never takes the head");
        assert_eq!(
            residents,
            [ResidentHold {
                label: "terminal `lab` of cairn/6613".into(),
                ends_at_unix_ms: NOW + RESIDENT_NOTICE_MS,
                notice: ResidentNotice::Pending,
            }]
        );
        assert_eq!(restart_expected_unix_ms, None);
        assert_eq!(rollout.returns_by("id-fedora"), None);

        // The runner tells the owner; the page says so, and the owner is
        // never told the same thing twice.
        rollout.note_resident_notice(
            "id-fedora",
            &notice.operation_id,
            &notice.resident.key,
            notice.kind,
            ResidentNotice::Told { at_unix_ms: NOW },
        );
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 60_000);
        assert!(step.notices.is_empty(), "{:?}", step.notices);
        transitions.extend(step.transitions);

        // The lane finishes; the terminal stays.
        machine.work.clear();
        let now = NOW + LANE_MS;
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, now);
        assert_eq!(step.direct, None, "not yet confirmed empty");
        transitions.extend(step.transitions);
        settle(&mut machine);
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, now + 1_000);
        assert_eq!(
            step.direct
                .as_ref()
                .map(|direct| direct.executor_id.as_str()),
            Some("id-fedora")
        );
        assert_eq!(notice_kinds(&step), [NoticeKind::Ending]);
        let directed = step
            .transitions
            .iter()
            .find(|transition| transition.kind == TransitionKind::Directed)
            .expect("directed");
        assert!(
            directed
                .cause
                .contains("the restart ends terminal `lab` of cairn/6613 (its owner was told at"),
            "{}",
            directed.cause
        );
        transitions.extend(step.transitions);
        assert!(matches!(
            phase(&rollout, &machine, &artifacts),
            SelfUpdatePhase::Directed { ref ended, .. }
                if ended.len() == 1 && matches!(ended[0].notice, ResidentNotice::Told { .. })
        ));
        assert_eq!(
            rollout.returns_by("id-fedora"),
            Some(now + 1_000 + RETURN_BUDGET_MS)
        );
        assert!(
            transitions
                .iter()
                .all(|transition| transition.kind != TransitionKind::Deferred),
            "{transitions:?}"
        );
    }

    /// A machine whose only resident is a terminal drains beside the turn
    /// rather than taking it, and is held for the notice period, not the
    /// hour work with no expected end gets.
    #[test]
    fn a_terminal_alone_holds_only_the_notice_period() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![remote("fedora", "old"), remote("idle", "old")];
        fleet[0].residents = vec![terminal("lab")];
        let step = rollout.advance(&fleet, &artifacts, NOW);
        assert_eq!(held(&step), ["id-idle", "id-fedora"]);
        assert!(matches!(
            phase(&rollout, &fleet[0], &artifacts),
            SelfUpdatePhase::Draining {
                in_flight: 0,
                waits_for_turn: true,
                ..
            }
        ));
        assert!(matches!(
            phase(&rollout, &fleet[1], &artifacts),
            SelfUpdatePhase::Draining {
                waits_for_turn: false,
                ..
            }
        ));

        let mut rollout = Rollout::default();
        let mut machine = fleet.remove(0);
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        settle(&mut machine);
        let step = rollout.advance(
            std::slice::from_ref(&machine),
            &artifacts,
            NOW + RESIDENT_NOTICE_MS - 1,
        );
        assert_eq!(held(&step), ["id-fedora"]);
        assert_eq!(step.direct, None, "held through the notice period");
        let step = rollout.advance(
            std::slice::from_ref(&machine),
            &artifacts,
            NOW + RESIDENT_NOTICE_MS,
        );
        assert!(step.direct.is_some(), "directed once it has passed");
        assert_eq!(notice_kinds(&step), [NoticeKind::Ending]);
    }

    /// A terminal that outlives everything never reads as wedged: the
    /// machine's lanes keep it busy for two hours, and it is never deferred
    /// on the terminal's account.
    #[test]
    fn residents_never_defer_a_machine() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("fedora", "old");
        machine.residents = vec![terminal("lab")];
        machine.work = vec![command("lane-0", NOW, Some(LANE_MS))];
        let mut now = NOW;
        for arrival in 0..=24 {
            now = NOW + arrival * 5 * 60_000;
            machine.work = vec![command(&format!("lane-{arrival}"), now, Some(LANE_MS))];
            let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, now);
            assert_eq!(held(&step), ["id-fedora"], "at {arrival}");
            assert!(
                step.transitions
                    .iter()
                    .all(|transition| transition.kind != TransitionKind::Deferred),
                "{:?}",
                step.transitions
            );
        }
        assert!(now - NOW >= 2 * HOUR);
        machine.work.clear();
        settle(&mut machine);
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, now + 1_000);
        assert!(step.direct.is_some());
    }

    /// The notice period is one bound for the turn: a resident that appears
    /// later is told, but gets what is left of it, not a period of its own.
    #[test]
    fn the_notice_bound_is_not_extended_by_a_later_resident() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("fedora", "old");
        with_work(&mut machine, 1);
        machine.residents = vec![terminal("lab")];
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        machine.residents.push(terminal("watch"));
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 4 * 60_000);
        assert_eq!(step.notices.len(), 1, "only the new one is told");
        assert_eq!(
            step.notices[0].resident.label,
            "terminal `watch` of cairn/6613"
        );
        assert_eq!(
            step.notices[0].kind,
            NoticeKind::Announced {
                restart_no_earlier_than_unix_ms: NOW + RESIDENT_NOTICE_MS,
                commands_in_flight: 1,
            }
        );
        let SelfUpdatePhase::Draining { residents, .. } = phase(&rollout, &machine, &artifacts)
        else {
            panic!("draining");
        };
        assert!(residents
            .iter()
            .all(|resident| resident.ends_at_unix_ms == NOW + RESIDENT_NOTICE_MS));
    }

    /// A notice whose delivery failed (the owner lookup or the queue write
    /// errored) is due again on the next pass, against the same deadline, and
    /// stops once delivered. The same holds for the Ending notice while the
    /// machine is away.
    #[test]
    fn a_notice_whose_delivery_failed_is_retried_on_the_next_pass() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("fedora", "old");
        with_work(&mut machine, 1);
        machine.residents = vec![terminal("lab")];
        let announced = NoticeKind::Announced {
            restart_no_earlier_than_unix_ms: NOW + RESIDENT_NOTICE_MS,
            commands_in_flight: 1,
        };
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        assert_eq!(notice_kinds(&step), [announced]);
        let first = step.notices[0].clone();
        let report = |rollout: &mut Rollout, kind, outcome| {
            rollout.note_resident_notice(
                "id-fedora",
                &first.operation_id,
                &first.resident.key,
                kind,
                outcome,
            )
        };

        report(&mut rollout, announced, ResidentNotice::Pending);
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 30_000);
        assert_eq!(
            notice_kinds(&step),
            [announced],
            "same deadline, told again"
        );
        assert_eq!(step.notices[0].operation_id, first.operation_id);
        report(
            &mut rollout,
            announced,
            ResidentNotice::Told {
                at_unix_ms: NOW + 30_000,
            },
        );
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW + 60_000);
        assert!(step.notices.is_empty(), "{:?}", step.notices);

        machine.work.clear();
        settle(&mut machine);
        let step = rollout.advance(
            std::slice::from_ref(&machine),
            &artifacts,
            NOW + RESIDENT_NOTICE_MS,
        );
        assert!(step.direct.is_some());
        assert_eq!(notice_kinds(&step), [NoticeKind::Ending]);
        report(&mut rollout, NoticeKind::Ending, ResidentNotice::Pending);
        // The machine is away installing; the owner is still told.
        let step = rollout.advance(&[], &artifacts, NOW + RESIDENT_NOTICE_MS + 31_000);
        assert_eq!(notice_kinds(&step), [NoticeKind::Ending]);
        report(
            &mut rollout,
            NoticeKind::Ending,
            ResidentNotice::Told {
                at_unix_ms: NOW + RESIDENT_NOTICE_MS + 31_000,
            },
        );
        let step = rollout.advance(&[], &artifacts, NOW + RESIDENT_NOTICE_MS + 61_000);
        assert!(step.notices.is_empty(), "{:?}", step.notices);
    }

    /// A resident-only machine whose notice has passed still cannot restart
    /// while another machine holds the turn, so the bound it publishes is that
    /// machine's return, and its phase says which machine it waits for.
    #[test]
    fn a_drain_behind_a_directed_head_publishes_the_heads_return() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut fleet = vec![remote("a", "old"), remote("fedora", "old")];
        fleet[1].residents = vec![terminal("lab")];
        assert_eq!(
            held(&rollout.advance(&fleet, &artifacts, NOW)),
            ["id-a", "id-fedora"]
        );
        settle(&mut fleet[0]);
        settle(&mut fleet[1]);
        let step = rollout.advance(&fleet, &artifacts, NOW + 1_000);
        assert_eq!(
            step.direct.map(|direct| direct.executor_id),
            Some("id-a".to_string())
        );
        let head_back = NOW + 1_000 + RETURN_BUDGET_MS;

        // Six minutes on, a is still installing; fedora's notice has passed.
        let away = [fleet[1].clone()];
        let step = rollout.advance(&away, &artifacts, NOW + 6 * 60_000);
        assert_eq!(step.direct, None, "the turn is a's");
        let SelfUpdatePhase::Draining {
            restart_expected_unix_ms,
            waits_for_turn,
            updating,
            ..
        } = phase(&rollout, &fleet[1], &artifacts)
        else {
            panic!("draining");
        };
        assert_eq!(restart_expected_unix_ms, Some(head_back));
        assert!(waits_for_turn);
        assert_eq!(updating.as_deref(), Some("a"));
        assert_eq!(
            rollout.returns_by("id-fedora"),
            Some(head_back + RETURN_BUDGET_MS)
        );
    }

    #[test]
    fn refusal_cost_counts_committed_interval_unions_and_survives_retarget_and_release() {
        let mut rollout = Rollout::default();
        let mut machine = remote("fedora", "old");
        machine.work = vec![command("request/attempt-1", NOW, None)];
        rollout.advance(std::slice::from_ref(&machine), &serving("new"), NOW);
        let events = rollout.record_committed_drains(NOW + 5);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1["phase"], "refusalStarted");
        assert!(rollout.record_committed_drains(NOW + 100).is_empty());
        let episode = &rollout.episodes[&machine.executor_id];
        assert_eq!(episode.refusal_at(NOW + 100), 95);
        rollout.advance(std::slice::from_ref(&machine), &serving("newer"), NOW + 100);
        assert!(rollout.record_committed_drains(NOW + 100).is_empty());
        rollout.advance(
            std::slice::from_ref(&machine),
            &serving("newer"),
            NOW + HOUR,
        );
        let events = rollout.record_committed_drains(NOW + HOUR + 7);
        assert_eq!(events[0].1["phase"], "refusalEnded");
        assert_eq!(events[0].1["refusedMs"], HOUR + 2);
        assert_eq!(
            rollout.episodes[&machine.executor_id].unsuccessful_drains,
            1
        );
        assert!(rollout.record_committed_drains(NOW + 10 * HOUR).is_empty());
        assert_eq!(
            rollout.episodes[&machine.executor_id].refusal_at(NOW + 10 * HOUR),
            HOUR + 2
        );

        // A disappearing holder needs the post-release account barrier. A new
        // physical attempt under the same request does not inherit its wait.
        machine.work = vec![command("request/attempt-2", NOW + HOUR, None)];
        rollout.advance(
            std::slice::from_ref(&machine),
            &serving("newer"),
            NOW + HOUR + 10,
        );
        assert!(!rollout.holds(&machine.executor_id));
        settle(&mut machine);
        rollout.advance(
            std::slice::from_ref(&machine),
            &serving("newer"),
            NOW + HOUR + 20,
        );
        assert!(rollout.holds(&machine.executor_id));
        rollout.record_committed_drains(NOW + HOUR + 20);
        rollout.advance(&[], &serving("newer"), NOW + HOUR + 30);
        let events = rollout.record_committed_drains(NOW + HOUR + 30);
        assert_eq!(
            events[0].1["refusedMs"],
            HOUR + 12,
            "detach closes the second interval once"
        );
        assert_eq!(events[0].1["unsuccessfulDrains"], 2);
    }

    #[test]
    fn unresolved_attempts_never_repeat_a_busy_drain_across_time_reconnect_or_retarget() {
        let artifacts = serving("new");
        let mut rollout = Rollout::default();
        let mut machine = remote("fedora", "old");
        machine.work = vec![
            command("request-a/attempt-1", NOW, None),
            command("request-b/attempt-1", NOW, None),
        ];
        rollout.advance(std::slice::from_ref(&machine), &artifacts, NOW);
        let first = NOW + UNMEASURED_HOLD_MS;
        let step = rollout.advance(std::slice::from_ref(&machine), &artifacts, first);
        assert_eq!(step.transitions[0].kind, TransitionKind::Deferred);
        assert!(!rollout.holds(&machine.executor_id));
        assert!(step.direct.is_none());

        let newer = serving("newer");
        for hour in 2..8 {
            machine.generation += 1;
            machine.snapshots_received = 1;
            let step = rollout.advance(std::slice::from_ref(&machine), &newer, NOW + hour * HOUR);
            assert!(
                !rollout.holds(&machine.executor_id),
                "unchanged physical work must keep ordinary admission open"
            );
            assert!(step.direct.is_none());
            assert!(!step
                .transitions
                .iter()
                .any(|t| t.kind == TransitionKind::Deferred));
        }
        machine.work.remove(0);
        settle(&mut machine);
        rollout.advance(std::slice::from_ref(&machine), &newer, NOW + 8 * HOUR);
        assert!(
            !rollout.holds(&machine.executor_id),
            "the second original blocker remains"
        );

        machine.work.clear();
        settle(&mut machine);
        rollout.advance(std::slice::from_ref(&machine), &newer, NOW + 9 * HOUR);
        assert!(
            rollout.holds(&machine.executor_id),
            "settlement restores update eligibility"
        );
        settle(&mut machine);
        let step = rollout.advance(std::slice::from_ref(&machine), &newer, NOW + 9 * HOUR + 1);
        assert_eq!(step.direct.unwrap().build_id, "newer");
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
        rollout.record_committed_drains(NOW);
        settle(&mut machine);
        assert!(rollout
            .advance(std::slice::from_ref(&machine), &serving("new"), NOW + 1_000)
            .direct
            .is_some());
        rollout.record_committed_drains(NOW + 1_000);
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
        let events = rollout.record_committed_drains(NOW + 60_000);
        assert_eq!(
            events.len(),
            2,
            "the successful operation ends before the new target starts"
        );
        assert_eq!(events[0].1["refusedMs"], 60_000);
        assert_eq!(events[0].1["unsuccessfulDrains"], 0);
        assert_eq!(events[1].1["refusedMs"], 0);
        assert_eq!(
            rollout.episodes[&machine.executor_id].since_unix_ms,
            NOW + 60_000
        );
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
