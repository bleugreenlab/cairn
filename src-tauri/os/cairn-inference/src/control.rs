//! The act side: a deployment's phase as the orch holds it, and the
//! transitions an operator's request and an executor's report move it through.
//!
//! Pure, like the rest of the crate: the host persists the phase, sends the
//! commands and reads the telemetry. Every rule about what may happen next is
//! here so it can be tested without an executor.
//!
//! Only the deployment's head acts, as in bginf: the start and stop commands run
//! on the head, and a multi-machine profile's own scripts drive the other
//! machines (the dspark profile does that over ssh). Whether a start may be sent
//! is not decided here: placement holds the model as a resident on its gang's
//! machines, reserves its footprint there, and launches it once the gang fits.
//!
//! Health is the deployment's health URL answering, never its start script
//! ending. A start is healthy as soon as either the head or a probe sees the
//! URL answer; the script runs on to its own end, which may be never (a
//! foreground server). While starting or healthy, a deployment whose health
//! stops answering, or whose GPU memory falls from its footprint to nearly
//! nothing ([`AllocationWatch`]), has lost its engine: it fails with that
//! reason and frees its machines on its own.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::machine::MachineName;
use crate::profile::{InferenceProfile, MemoryPerMachine};

/// How long the head may take to start a deployment and see it answer its
/// health check, from the moment it receives the command: bginf's
/// `START_TIMEOUT` default.
pub const START_TIMEOUT_SECS: u64 = 1_800;
/// How long the stop command may run: bginf's bound.
pub const STOP_TIMEOUT_SECS: u64 = 600;
/// How long the health check may go on answering after the stop command has
/// finished before the stop is called failed. A compose stack that restarts
/// itself (`restart: unless-stopped`) is the case this catches.
pub const STOPPED_HEALTH_WAIT_SECS: u64 = 120;
/// How long a start waits, with its machines reserved, for every gang machine
/// to have the memory the deployment needs before it is called failed.
pub const FIT_WAIT_SECS: u64 = 900;
/// Slack on the executor's own bounds before the orch stops waiting for its
/// report and calls the command failed: the head may have died with it.
pub const REPORT_SLACK_SECS: u64 = 120;
/// How long the head may take to report on a start: the start's own bound,
/// plus the stop command a failed start runs before it reports, plus slack.
pub const START_REPORT_BOUND_SECS: u64 = START_TIMEOUT_SECS + STOP_TIMEOUT_SECS + REPORT_SLACK_SECS;
/// How long the head may take to report on a stop.
pub const STOP_REPORT_BOUND_SECS: u64 =
    STOP_TIMEOUT_SECS + STOPPED_HEALTH_WAIT_SECS + REPORT_SLACK_SECS;

/// A deployment's phase as the orch holds it.
///
/// Times are epoch milliseconds. A deployment Cairn has never acted on has no
/// phase at all. When placement starts it probes every deployment once, and
/// one that answers is adopted as healthy, so a model left running before
/// Cairn acted on it is held as a resident rather than ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "camelCase")]
pub enum Phase {
    Stopped {
        since_ms: i64,
    },
    /// Its machines are reserved and the start has not been sent: the head is
    /// not attached yet, or the gang does not fit the memory its machines have
    /// left while the work already on them finishes.
    Reserving {
        since_ms: i64,
        /// Why it has not been sent yet, as of the last check.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        waiting_on: Option<String>,
    },
    /// The head has the start command.
    Starting {
        since_ms: i64,
        /// When the start was requested, so the wait for the gang to fit is
        /// kept apart from the load itself.
        requested_ms: i64,
    },
    Healthy {
        since_ms: i64,
        /// How long the start took to bring it to health: from the start
        /// being sent to its health check first answering, whether or not
        /// the start script had ended. `None` when Cairn found it already
        /// answering (started before Cairn acted on it, or brought back by a
        /// power cycle).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        load_ms: Option<u64>,
    },
    Stopping {
        since_ms: i64,
    },
    Failed {
        at_ms: i64,
        reason: String,
    },
}

impl Phase {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Stopped { .. } => "stopped",
            Self::Reserving { .. } => "reserving",
            Self::Starting { .. } => "starting",
            Self::Healthy { .. } => "healthy",
            Self::Stopping { .. } => "stopping",
            Self::Failed { .. } => "failed",
        }
    }

    /// Whether this phase occupies the deployment's machines: a start is on
    /// its way, the model runs, or its stop has not finished. A failed
    /// deployment does not: a failed start has already run the stop command,
    /// and a deployment that stopped answering is gone.
    pub fn reserves(&self) -> bool {
        matches!(
            self,
            Self::Reserving { .. }
                | Self::Starting { .. }
                | Self::Healthy { .. }
                | Self::Stopping { .. }
        )
    }

    /// When the phase was entered.
    pub fn since_ms(&self) -> i64 {
        match self {
            Self::Stopped { since_ms }
            | Self::Reserving { since_ms, .. }
            | Self::Starting { since_ms, .. }
            | Self::Healthy { since_ms, .. }
            | Self::Stopping { since_ms } => *since_ms,
            Self::Failed { at_ms, .. } => *at_ms,
        }
    }

    /// Whether the phase waits on a start or stop the head is running, whose
    /// report decides what happens next unless health decides first.
    pub fn command_in_flight(&self) -> bool {
        matches!(self, Self::Starting { .. } | Self::Stopping { .. })
    }
}

/// What an operator or agent asks of one deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    Start,
    Stop,
}

impl Request {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
        }
    }
}

/// A request this phase does not allow, with the sentence saying why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal(pub String);

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refusal {}

/// The phase a request moves `current` to. `None` is a deployment Cairn has
/// never acted on, which is stopped as far as a request is concerned.
///
/// A stop is allowed from every phase but stopping, a stopped one included:
/// a stop must really stop, and a deployment Cairn believes stopped may have
/// been started behind its back. A stop from reserving sends nothing, because
/// nothing was started.
pub fn request(current: Option<&Phase>, request: Request, now_ms: i64) -> Result<Phase, Refusal> {
    match (request, current) {
        (Request::Start, None | Some(Phase::Stopped { .. } | Phase::Failed { .. })) => {
            Ok(Phase::Reserving {
                since_ms: now_ms,
                waiting_on: None,
            })
        }
        (Request::Start, Some(Phase::Reserving { .. } | Phase::Starting { .. })) => {
            Err(Refusal("it is already starting".into()))
        }
        (Request::Start, Some(Phase::Healthy { .. })) => {
            Err(Refusal("it is already healthy".into()))
        }
        (Request::Start, Some(Phase::Stopping { .. })) => Err(Refusal(
            "it is stopping; start it again once it has stopped".into(),
        )),
        (Request::Stop, Some(Phase::Stopping { .. })) => {
            Err(Refusal("it is already stopping".into()))
        }
        (Request::Stop, Some(Phase::Reserving { .. })) => Ok(Phase::Stopped { since_ms: now_ms }),
        (Request::Stop, _) => Ok(Phase::Stopping { since_ms: now_ms }),
    }
}

/// The head's account of the command it is running, as the executor reports
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Report {
    Starting,
    /// The health check answers, though the start script may still be
    /// running. `ran_start` is false when it already answered before the
    /// start command ran, so the command was not run at all.
    Healthy {
        ran_start: bool,
    },
    Stopping,
    Stopped,
    Failed {
        reason: String,
    },
}

/// Everything other than a request that moves a phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// The start command was sent to the head.
    StartSent,
    /// A command could not be sent, or a reserving start gave up waiting.
    NotSent { reason: String },
    /// The head reported on the command the orch holds as current.
    Report(Report),
    /// A probe of the health URL.
    Probed { healthy: bool },
    /// The engine is gone by a measure other than the health check: its GPU
    /// memory fell from the footprint it had reached ([`AllocationWatch`]).
    Lost { reason: String },
    /// Waiting while reserving, with what it is waiting on.
    Waiting { on: String },
    /// The command in flight outlived its bound with no report.
    Overdue { reason: String },
}

/// The phase an observation moves `current` to, or `None` when it changes
/// nothing (a report for a command no longer current, a probe that agrees).
pub fn observe(current: &Phase, observation: Observation, now_ms: i64) -> Option<Phase> {
    use Observation as O;
    match (current, observation) {
        (Phase::Reserving { since_ms, .. }, O::StartSent) => Some(Phase::Starting {
            since_ms: now_ms,
            requested_ms: *since_ms,
        }),
        (
            Phase::Reserving {
                since_ms,
                waiting_on,
            },
            O::Waiting { on },
        ) => {
            let changed = waiting_on.as_deref() != Some(on.as_str());
            changed.then_some(Phase::Reserving {
                since_ms: *since_ms,
                waiting_on: Some(on),
            })
        }
        (
            Phase::Reserving { .. } | Phase::Starting { .. } | Phase::Stopping { .. },
            O::NotSent { reason } | O::Overdue { reason },
        ) => Some(Phase::Failed {
            at_ms: now_ms,
            reason,
        }),
        // A reserving start whose model already answers is adopted as it
        // stands, rather than waiting for memory the model itself is using.
        (Phase::Reserving { .. }, O::Probed { healthy: true }) => Some(Phase::Healthy {
            since_ms: now_ms,
            load_ms: None,
        }),
        (Phase::Starting { since_ms, .. }, O::Report(Report::Healthy { ran_start })) => {
            Some(Phase::Healthy {
                since_ms: now_ms,
                load_ms: ran_start.then(|| now_ms.saturating_sub(*since_ms).max(0) as u64),
            })
        }
        // Health is the probe answering, not the start script ending: a
        // script that warms the model up, or serves in the foreground, is
        // still running when the model is ready.
        (Phase::Starting { since_ms, .. }, O::Probed { healthy: true }) => Some(Phase::Healthy {
            since_ms: now_ms,
            load_ms: Some(now_ms.saturating_sub(*since_ms).max(0) as u64),
        }),
        (Phase::Starting { .. } | Phase::Healthy { .. }, O::Lost { reason }) => {
            Some(Phase::Failed {
                at_ms: now_ms,
                reason,
            })
        }
        (Phase::Starting { .. } | Phase::Stopping { .. }, O::Report(Report::Failed { reason })) => {
            Some(Phase::Failed {
                at_ms: now_ms,
                reason,
            })
        }
        (Phase::Stopping { .. }, O::Report(Report::Stopped)) => {
            Some(Phase::Stopped { since_ms: now_ms })
        }
        // Back on its own: a power cycle restarts a compose stack that
        // restarts unless stopped. Its machines are reserved again at once.
        (Phase::Stopped { .. } | Phase::Failed { .. }, O::Probed { healthy: true }) => {
            Some(Phase::Healthy {
                since_ms: now_ms,
                load_ms: None,
            })
        }
        (Phase::Healthy { .. }, O::Probed { healthy: false }) => Some(Phase::Failed {
            at_ms: now_ms,
            reason: "it stopped answering its health check".into(),
        }),
        _ => None,
    }
}

/// Whether the start stays the row's command across this change: health
/// answered while the start script may still be running, so the script's own
/// end is still to be reported (see [`late_start_failure`]).
pub fn keeps_start_command(before: &Phase, after: &Phase) -> bool {
    matches!(
        (before, after),
        (Phase::Starting { .. }, Phase::Healthy { .. })
    )
}

/// A start script that ended badly after its deployment was already healthy.
/// It never moves the phase, since health decides that and a deployment
/// whose health also stops answering fails on the probe; the reason is kept
/// on the load as a note.
pub fn late_start_failure<'a>(current: &Phase, report: &'a Report) -> Option<&'a str> {
    match (current, report) {
        (Phase::Healthy { .. }, Report::Failed { reason }) => Some(reason),
        _ => None,
    }
}

/// How much of its footprint a machine's GPU memory must reach before a fall
/// counts, as a fraction `numerator / denominator`: a load that never got this
/// far has nothing to lose yet.
pub const LOADED_SHARE: (u64, u64) = (1, 2);
/// How little of its footprint a loaded machine's GPU memory must fall to for
/// its engine to count as gone.
pub const GONE_SHARE: (u64, u64) = (1, 10);

/// One gang machine's GPU memory against what the deployment settles at there.
/// `None` is unmeasured, which never counts either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Allocation {
    pub held_bytes: Option<u64>,
    pub footprint_bytes: Option<u64>,
}

/// Watches a starting or healthy deployment's GPU memory for its engine dying
/// under it. A machine counts once its GPU memory has climbed to
/// [`LOADED_SHARE`] of its footprint; a counted machine whose GPU memory then
/// falls below [`GONE_SHARE`] of it has lost its engine. The host keeps one
/// watch per deployment for as long as it is starting or healthy, and starts
/// a fresh one for the next start.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllocationWatch {
    /// The most each counted machine has held.
    peaks: BTreeMap<MachineName, u64>,
}

impl AllocationWatch {
    /// Take one reading of every gang machine. Returns why the engine is gone
    /// when a counted machine's GPU memory has collapsed.
    pub fn observe(&mut self, allocations: &BTreeMap<MachineName, Allocation>) -> Option<String> {
        let share = |bytes: u64, (num, den): (u64, u64)| bytes / den * num;
        for (machine, allocation) in allocations {
            let (Some(held), Some(footprint)) = (allocation.held_bytes, allocation.footprint_bytes)
            else {
                continue;
            };
            if footprint == 0 {
                continue;
            }
            if held >= share(footprint, LOADED_SHARE) {
                let peak = self.peaks.entry(machine.clone()).or_default();
                *peak = (*peak).max(held);
                continue;
            }
            if let Some(peak) = self.peaks.get(machine) {
                if held < share(footprint, GONE_SHARE) {
                    return Some(format!(
                        "its engine is gone: {machine}'s GPU memory fell to {} after reaching {} (its footprint is {})",
                        gib(held),
                        gib(*peak),
                        gib(footprint)
                    ));
                }
            }
        }
        None
    }
}

/// Why a command in flight has gone unreported for too long, or `None` while
/// it is within its bound. Reserving is bounded by [`FIT_WAIT_SECS`], so a
/// start that can never fit does not hold its machines forever.
pub fn overdue(phase: &Phase, now_ms: i64) -> Option<String> {
    let elapsed_s = |since: i64| (now_ms.saturating_sub(since).max(0) / 1_000) as u64;
    match phase {
        Phase::Reserving {
            since_ms,
            waiting_on,
        } if elapsed_s(*since_ms) > FIT_WAIT_SECS => Some(format!(
            "the start was not sent within {} minutes{}",
            FIT_WAIT_SECS / 60,
            waiting_on
                .as_deref()
                .map(|on| format!(": {on}"))
                .unwrap_or_default()
        )),
        Phase::Starting { since_ms, .. } if elapsed_s(*since_ms) > START_REPORT_BOUND_SECS => {
            Some(format!(
                "the head did not report on the start within {} minutes",
                START_REPORT_BOUND_SECS / 60
            ))
        }
        Phase::Stopping { since_ms } if elapsed_s(*since_ms) > STOP_REPORT_BOUND_SECS => {
            Some(format!(
                "the head did not report on the stop within {} minutes",
                STOP_REPORT_BOUND_SECS / 60
            ))
        }
        _ => None,
    }
}

/// One finished load: a start that reached health or failed. The duration is
/// what the idle windows of demand-driven loading are set from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadOutcome {
    Healthy { load_ms: u64 },
    Failed { reason: String },
}

/// The load a transition finished, if it finished one: starting to healthy
/// with the start actually run, or starting to failed. A deployment adopted
/// while already answering finished no load Cairn measured.
pub fn finished_load(before: &Phase, after: &Phase) -> Option<LoadOutcome> {
    match (before, after) {
        (
            Phase::Starting { .. },
            Phase::Healthy {
                load_ms: Some(load_ms),
                ..
            },
        ) => Some(LoadOutcome::Healthy { load_ms: *load_ms }),
        (Phase::Reserving { .. } | Phase::Starting { .. }, Phase::Failed { reason, .. }) => {
            Some(LoadOutcome::Failed {
                reason: reason.clone(),
            })
        }
        _ => None,
    }
}

pub fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64)
}

/// What a profile states it needs on each machine, in the kind of memory the
/// fit compares: system RAM on a unified machine, VRAM on a discrete card.
pub fn stated_need(profile: &InferenceProfile) -> Option<u64> {
    match profile.requirements.memory? {
        MemoryPerMachine::Unified { system } => Some(system.0),
        MemoryPerMachine::Discrete { vram, .. } => Some(vram.0),
    }
}

/// Whether a profile's footprint is the memory batch work uses: system RAM on
/// a unified-memory machine. A profile stating discrete VRAM takes nothing
/// batches use; one stating no memory is taken to be unified, the Sparks' and
/// halo's kind, so its footprint is never left out of the room batches see.
pub fn shares_memory_with_batches(profile: &InferenceProfile) -> bool {
    !matches!(
        profile.requirements.memory,
        Some(MemoryPerMachine::Discrete { .. })
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: i64 = 1_000_000;

    fn reserving() -> Phase {
        Phase::Reserving {
            since_ms: T0,
            waiting_on: None,
        }
    }

    fn starting() -> Phase {
        Phase::Starting {
            since_ms: T0 + 5_000,
            requested_ms: T0,
        }
    }

    #[test]
    fn a_start_reserves_then_sends_then_measures_the_load() {
        let phase = request(None, Request::Start, T0).unwrap();
        assert_eq!(phase, reserving());
        assert!(phase.reserves());
        let phase = observe(&phase, Observation::StartSent, T0 + 5_000).unwrap();
        assert_eq!(phase, starting());
        assert_eq!(
            observe(&phase, Observation::Report(Report::Starting), T0 + 6_000),
            None,
            "a report of the phase it is already in changes nothing"
        );
        let healthy = observe(
            &phase,
            Observation::Report(Report::Healthy { ran_start: true }),
            T0 + 65_000,
        )
        .unwrap();
        assert_eq!(
            healthy,
            Phase::Healthy {
                since_ms: T0 + 65_000,
                load_ms: Some(60_000)
            },
            "the load is timed from the command, not from the wait to fit"
        );
        assert!(healthy.reserves());
        assert_eq!(
            finished_load(&phase, &healthy),
            Some(LoadOutcome::Healthy { load_ms: 60_000 })
        );
    }

    #[test]
    fn a_model_already_answering_is_adopted_without_a_measured_load() {
        let healthy = observe(
            &starting(),
            Observation::Report(Report::Healthy { ran_start: false }),
            T0 + 6_000,
        )
        .unwrap();
        assert_eq!(
            healthy,
            Phase::Healthy {
                since_ms: T0 + 6_000,
                load_ms: None
            }
        );
        assert_eq!(finished_load(&starting(), &healthy), None);
        let adopted = observe(&reserving(), Observation::Probed { healthy: true }, T0 + 1).unwrap();
        assert!(matches!(adopted, Phase::Healthy { load_ms: None, .. }));
    }

    #[test]
    fn a_failed_start_reports_its_reason_and_frees_the_machines() {
        let failed = observe(
            &starting(),
            Observation::Report(Report::Failed {
                reason: "the start command exited 127: not found".into(),
            }),
            T0 + 7_000,
        )
        .unwrap();
        assert_eq!(failed.label(), "failed");
        assert!(!failed.reserves());
        assert_eq!(
            finished_load(&starting(), &failed),
            Some(LoadOutcome::Failed {
                reason: "the start command exited 127: not found".into()
            })
        );
        // A start that could not be sent fails the same way.
        let unsent = observe(
            &reserving(),
            Observation::NotSent {
                reason: "the head is not attached".into(),
            },
            T0 + 1,
        )
        .unwrap();
        assert!(!unsent.reserves());
        // And a failed deployment may be started again.
        assert_eq!(
            request(Some(&failed), Request::Start, T0 + 9)
                .unwrap()
                .label(),
            "reserving"
        );
    }

    #[test]
    fn a_stop_stops_and_a_stopped_deployment_frees_its_machines() {
        let healthy = Phase::Healthy {
            since_ms: T0,
            load_ms: Some(1),
        };
        let stopping = request(Some(&healthy), Request::Stop, T0 + 10).unwrap();
        assert_eq!(stopping, Phase::Stopping { since_ms: T0 + 10 });
        assert!(
            stopping.reserves(),
            "a stopping model still holds its memory"
        );
        assert_eq!(
            request(Some(&stopping), Request::Stop, T0 + 11),
            Err(Refusal("it is already stopping".into()))
        );
        assert_eq!(
            request(Some(&stopping), Request::Start, T0 + 11)
                .unwrap_err()
                .0,
            "it is stopping; start it again once it has stopped"
        );
        let stopped = observe(&stopping, Observation::Report(Report::Stopped), T0 + 20).unwrap();
        assert_eq!(stopped, Phase::Stopped { since_ms: T0 + 20 });
        assert!(!stopped.reserves());
    }

    #[test]
    fn a_stop_is_allowed_from_stopped_and_sends_nothing_from_reserving() {
        let stopped = Phase::Stopped { since_ms: T0 };
        assert_eq!(
            request(Some(&stopped), Request::Stop, T0 + 1)
                .unwrap()
                .label(),
            "stopping",
            "a stop must really stop, whatever Cairn believed"
        );
        assert_eq!(
            request(None, Request::Stop, T0 + 1).unwrap().label(),
            "stopping"
        );
        assert_eq!(
            request(Some(&reserving()), Request::Stop, T0 + 1).unwrap(),
            Phase::Stopped { since_ms: T0 + 1 }
        );
        assert!(request(Some(&starting()), Request::Start, T0).is_err());
        assert!(request(
            Some(&Phase::Healthy {
                since_ms: T0,
                load_ms: None
            }),
            Request::Start,
            T0
        )
        .is_err());
    }

    #[test]
    fn probes_catch_a_model_that_came_back_or_went_away() {
        let stopped = Phase::Stopped { since_ms: T0 };
        let back = observe(&stopped, Observation::Probed { healthy: true }, T0 + 1).unwrap();
        assert!(
            back.reserves(),
            "a model back after a power cycle holds its machines"
        );
        assert_eq!(
            observe(&stopped, Observation::Probed { healthy: false }, T0 + 1),
            None
        );
        let gone = observe(&back, Observation::Probed { healthy: false }, T0 + 2).unwrap();
        assert_eq!(
            gone,
            Phase::Failed {
                at_ms: T0 + 2,
                reason: "it stopped answering its health check".into()
            }
        );
        assert_eq!(
            observe(&back, Observation::Probed { healthy: true }, T0 + 3),
            None
        );
    }

    #[test]
    fn health_answering_before_the_start_script_ends_is_healthy() {
        let healthy = observe(
            &starting(),
            Observation::Probed { healthy: true },
            T0 + 5_000 + 401_000,
        )
        .unwrap();
        assert_eq!(
            healthy,
            Phase::Healthy {
                since_ms: T0 + 406_000,
                load_ms: Some(401_000)
            },
            "the load is timed to health answering, not to the script's warm-up ending"
        );
        assert_eq!(
            finished_load(&starting(), &healthy),
            Some(LoadOutcome::Healthy { load_ms: 401_000 })
        );
        assert!(keeps_start_command(&starting(), &healthy));
        assert_eq!(
            observe(
                &starting(),
                Observation::Probed { healthy: false },
                T0 + 6_000
            ),
            None,
            "a model still loading does not answer yet"
        );

        // The script's own end, once healthy, never moves the phase.
        let late = Report::Failed {
            reason: "the start command exited 1 after its health check had answered".into(),
        };
        assert_eq!(
            observe(&healthy, Observation::Report(late.clone()), T0 + 500_000),
            None
        );
        assert_eq!(
            late_start_failure(&healthy, &late),
            Some("the start command exited 1 after its health check had answered")
        );
        assert_eq!(late_start_failure(&starting(), &late), None);
        assert_eq!(
            late_start_failure(&healthy, &Report::Healthy { ran_start: true }),
            None
        );
    }

    #[test]
    fn an_engine_lost_while_starting_or_healthy_fails_and_frees_its_machines() {
        let lost = || Observation::Lost {
            reason: "its engine is gone".into(),
        };
        let failed = observe(&starting(), lost(), T0 + 9_000).unwrap();
        assert_eq!(
            failed,
            Phase::Failed {
                at_ms: T0 + 9_000,
                reason: "its engine is gone".into()
            }
        );
        assert!(!failed.reserves());
        assert_eq!(
            finished_load(&starting(), &failed),
            Some(LoadOutcome::Failed {
                reason: "its engine is gone".into()
            })
        );
        let healthy = Phase::Healthy {
            since_ms: T0,
            load_ms: Some(1),
        };
        let failed = observe(&healthy, lost(), T0 + 9_000).unwrap();
        assert!(!failed.reserves());
        assert_eq!(
            finished_load(&healthy, &failed),
            None,
            "its load already finished healthy"
        );
        for phase in [
            reserving(),
            Phase::Stopping { since_ms: T0 },
            Phase::Stopped { since_ms: T0 },
        ] {
            assert_eq!(observe(&phase, lost(), T0 + 1), None, "{phase:?}");
        }
    }

    fn allocations(
        readings: &[(&str, Option<u64>, Option<u64>)],
    ) -> BTreeMap<MachineName, Allocation> {
        readings
            .iter()
            .map(|(machine, held, footprint)| {
                (
                    MachineName::from(*machine),
                    Allocation {
                        held_bytes: held.map(|gib| gib << 30),
                        footprint_bytes: footprint.map(|gib| gib << 30),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn gpu_memory_that_collapses_after_loading_means_the_engine_is_gone() {
        let mut watch = AllocationWatch::default();
        let fp = Some(100);
        // Climbing: nothing to lose yet, even from zero.
        assert_eq!(
            watch.observe(&allocations(&[("sp1", Some(0), fp), ("sp2", Some(0), fp)])),
            None
        );
        assert_eq!(
            watch.observe(&allocations(&[("sp1", Some(30), fp), ("sp2", Some(5), fp)])),
            None
        );
        assert_eq!(
            watch.observe(&allocations(&[
                ("sp1", Some(82), fp),
                ("sp2", Some(82), fp)
            ])),
            None
        );
        // A dip that is not a collapse, and an unmeasured reading, count for
        // nothing.
        assert_eq!(
            watch.observe(&allocations(&[("sp1", Some(40), fp), ("sp2", None, fp)])),
            None
        );
        let reason = watch
            .observe(&allocations(&[("sp1", Some(0), fp), ("sp2", Some(82), fp)]))
            .expect("sp1 fell from 82 GiB to nothing");
        assert_eq!(
            reason,
            "its engine is gone: sp1's GPU memory fell to 0.0 GiB after reaching 82.0 GiB (its footprint is 100.0 GiB)"
        );
    }

    #[test]
    fn gpu_memory_with_no_footprint_or_no_load_says_nothing() {
        let mut watch = AllocationWatch::default();
        assert_eq!(
            watch.observe(&allocations(&[("sp1", Some(90), None)])),
            None
        );
        assert_eq!(
            watch.observe(&allocations(&[("sp1", Some(0), None)])),
            None,
            "nothing to compare against"
        );
        let mut watch = AllocationWatch::default();
        assert_eq!(
            watch.observe(&allocations(&[("sp1", Some(20), Some(100))])),
            None
        );
        assert_eq!(
            watch.observe(&allocations(&[("sp1", Some(0), Some(100))])),
            None,
            "it never got far enough to have lost anything"
        );
    }

    #[test]
    fn reports_for_no_command_in_flight_change_nothing() {
        let healthy = Phase::Healthy {
            since_ms: T0,
            load_ms: None,
        };
        assert_eq!(
            observe(&healthy, Observation::Report(Report::Stopped), T0 + 1),
            None
        );
        assert_eq!(
            observe(
                &Phase::Stopped { since_ms: T0 },
                Observation::Report(Report::Failed { reason: "x".into() }),
                T0 + 1
            ),
            None
        );
    }

    #[test]
    fn waiting_records_what_it_waits_on_once() {
        let waiting = observe(
            &reserving(),
            Observation::Waiting {
                on: "sp1 has 2.0 GiB free and needs 100.0 GiB".into(),
            },
            T0 + 1,
        )
        .unwrap();
        assert_eq!(
            observe(
                &waiting,
                Observation::Waiting {
                    on: "sp1 has 2.0 GiB free and needs 100.0 GiB".into()
                },
                T0 + 2
            ),
            None
        );
        assert_eq!(
            waiting.since_ms(),
            T0,
            "waiting keeps the reservation's start"
        );
    }

    #[test]
    fn every_command_in_flight_is_bounded() {
        let s = 1_000;
        assert_eq!(overdue(&reserving(), T0 + (FIT_WAIT_SECS as i64) * s), None);
        let fit_overdue = overdue(
            &Phase::Reserving {
                since_ms: T0,
                waiting_on: Some("sp2 has 1.0 GiB free and needs 9.0 GiB".into()),
            },
            T0 + (FIT_WAIT_SECS as i64 + 1) * s,
        )
        .unwrap();
        assert!(fit_overdue.ends_with(": sp2 has 1.0 GiB free and needs 9.0 GiB"));
        let starting_bound = START_REPORT_BOUND_SECS as i64 * s;
        assert_eq!(overdue(&starting(), T0 + 5_000 + starting_bound), None);
        assert!(overdue(&starting(), T0 + 5_000 + starting_bound + s).is_some());
        let stopping_bound = STOP_REPORT_BOUND_SECS as i64 * s;
        assert!(overdue(&Phase::Stopping { since_ms: T0 }, T0 + stopping_bound + s).is_some());
        assert_eq!(
            overdue(
                &Phase::Healthy {
                    since_ms: T0,
                    load_ms: None
                },
                i64::MAX
            ),
            None
        );
        let failed = observe(
            &starting(),
            Observation::Overdue {
                reason: "late".into(),
            },
            T0,
        )
        .unwrap();
        assert!(!failed.reserves());
    }

    #[test]
    fn the_phase_round_trips_through_its_stored_form() {
        for phase in [
            Phase::Stopped { since_ms: 1 },
            reserving(),
            starting(),
            Phase::Healthy {
                since_ms: 2,
                load_ms: Some(3),
            },
            Phase::Stopping { since_ms: 4 },
            Phase::Failed {
                at_ms: 5,
                reason: "r".into(),
            },
        ] {
            let json = serde_json::to_string(&phase).unwrap();
            assert_eq!(
                serde_json::from_str::<Phase>(&json).unwrap(),
                phase,
                "{json}"
            );
        }
    }
}
