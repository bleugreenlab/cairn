//! The act side: a deployment's phase as the orch holds it, the transitions an
//! operator's request and an executor's report move it through, the machines a
//! phase reserves, and whether a gang fits the memory its machines have left.
//!
//! Pure, like the rest of the crate: the host persists the phase, sends the
//! commands and reads the telemetry. Every rule about what may happen next is
//! here so it can be tested without an executor.
//!
//! Only the deployment's head acts, as in bginf: the start and stop commands run
//! on the head, and a multi-machine recipe's own scripts drive the other
//! machines (the dspark recipe does that over ssh). Cairn reserves every machine
//! of the gang while it is reserving, starting, healthy or stopping, so nothing
//! else is placed on a machine whose memory the model is about to take or holds.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::deployment::{Deployment, DeploymentId};
use crate::inventory::Inventory;
use crate::machine::MachineName;
use crate::recipe::{Engine, MemoryPerMachine, Recipe};

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
/// phase at all, and is neither probed nor reserved: bginf may be running it,
/// and Cairn does not take a machine away from builds for a model it did not
/// load.
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
        /// How long the start command took to bring it to health, from the
        /// head receiving it. `None` when Cairn found it already answering
        /// (started by bginf, or brought back by a power cycle).
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

    /// Whether this phase holds every machine of the deployment away from
    /// batch work. A failed deployment does not: a failed start has already
    /// run the stop command, and a deployment that stopped answering is gone.
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

    /// Whether a command the head is running decides what happens next, so a
    /// probe has nothing to add.
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
    /// The health check answers. `ran_start` is false when it already answered
    /// before the start command ran, so the command was not run at all.
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
    /// A probe of the health URL, sent while no command was in flight.
    Probed { healthy: bool },
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

/// A machine held by a deployment, and the phase that holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    pub deployment: DeploymentId,
    pub phase: &'static str,
}

/// Every machine a reserving phase holds, by machine. Deployments without a
/// phase, or whose record is gone, hold nothing.
pub fn reservations(
    inventory: &Inventory,
    phases: &BTreeMap<DeploymentId, Phase>,
) -> BTreeMap<MachineName, Reservation> {
    let mut held = BTreeMap::new();
    for (id, phase) in phases {
        if !phase.reserves() {
            continue;
        }
        let Some(deployment) = inventory.deployments.get(id) else {
            continue;
        };
        for machine in &deployment.machines {
            held.entry(machine.clone()).or_insert_with(|| Reservation {
                deployment: id.clone(),
                phase: phase.label(),
            });
        }
    }
    held
}

/// Why `deployment` cannot be started now because another deployment holds
/// one of its machines, or `None` when every machine is free of other
/// deployments.
pub fn conflict(
    inventory: &Inventory,
    phases: &BTreeMap<DeploymentId, Phase>,
    deployment: &Deployment,
) -> Option<Refusal> {
    let held = reservations(inventory, phases);
    deployment.machines.iter().find_map(|machine| {
        let holder = held.get(machine)?;
        (holder.deployment != deployment.id).then(|| {
            Refusal(format!(
                "{machine} is held by deployment {} ({}); stop it first",
                holder.deployment, holder.phase
            ))
        })
    })
}

/// The commands a recipe's engine runs on the head, or why it cannot be run
/// by Cairn yet.
pub fn commands(recipe: &Recipe) -> Result<(&str, Option<&str>), Refusal> {
    match &recipe.engine {
        Engine::Command { start, stop, .. } => Ok((start.as_str(), stop.as_deref())),
        other => Err(Refusal(format!(
            "recipe {} uses a {} engine, which Cairn cannot start yet; only command recipes are run",
            recipe.id,
            other.kind()
        ))),
    }
}

/// What one machine has left for a deployment and what the deployment needs
/// there, both in bytes of the same kind of memory (system RAM on a
/// unified-memory machine, VRAM on a discrete card). `None` is unmeasured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Room {
    pub free_bytes: Option<u64>,
    pub need_bytes: Option<u64>,
}

/// Whether a gang fits, from each machine's [`Room`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fit {
    Fits,
    /// The deployment has never been measured and its recipe states no
    /// memory, so nothing can be compared. The start proceeds and its load
    /// measures the footprint for next time.
    Unknown,
    /// These machines lack room, with what each has and needs.
    Short(Vec<(MachineName, Option<u64>, u64)>),
}

impl Fit {
    pub fn describe(&self) -> String {
        match self {
            Self::Fits => "every machine has the memory it needs".into(),
            Self::Unknown => "its footprint is unmeasured, so memory was not checked".into(),
            Self::Short(short) => short
                .iter()
                .map(|(machine, free, need)| match free {
                    Some(free) => {
                        format!("{machine} has {} free and needs {}", gib(*free), gib(*need))
                    }
                    None => format!(
                        "{machine}'s free memory is unmeasured; it needs {}",
                        gib(*need)
                    ),
                })
                .collect::<Vec<_>>()
                .join("; "),
        }
    }
}

pub fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64)
}

/// Whether every machine has the room the deployment needs on it. A machine
/// with a need and no measured free memory does not fit: an unmeasured
/// reading is never read as room.
pub fn fit(rooms: &BTreeMap<MachineName, Room>) -> Fit {
    if rooms.values().all(|room| room.need_bytes.is_none()) {
        return Fit::Unknown;
    }
    let short: Vec<_> = rooms
        .iter()
        .filter_map(|(machine, room)| {
            let need = room.need_bytes?;
            match room.free_bytes {
                Some(free) if free >= need => None,
                free => Some((machine.clone(), free, need)),
            }
        })
        .collect();
    if short.is_empty() {
        Fit::Fits
    } else {
        Fit::Short(short)
    }
}

/// What a recipe states it needs on each machine, in the kind of memory the
/// fit compares: system RAM on a unified machine, VRAM on a discrete card.
pub fn stated_need(recipe: &Recipe) -> Option<u64> {
    match recipe.requirements.memory? {
        MemoryPerMachine::Unified { system } => Some(system.0),
        MemoryPerMachine::Discrete { vram, .. } => Some(vram.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::bginf_test_inventory;

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
    fn a_gang_reserves_every_machine_and_blocks_an_overlapping_start() {
        let inventory = bginf_test_inventory();
        let mut phases = BTreeMap::new();
        phases.insert(DeploymentId::from("dsv4f-tp2"), starting());
        phases.insert(
            DeploymentId::from("qwen3-embed-4b"),
            Phase::Stopped { since_ms: T0 },
        );
        let held = reservations(&inventory, &phases);
        assert_eq!(
            held.keys().map(MachineName::as_str).collect::<Vec<_>>(),
            ["sp1", "sp2"],
            "both Sparks are held by the gang, and a stopped deployment holds nothing"
        );
        assert_eq!(held[&MachineName::from("sp2")].phase, "starting");

        let qwen = &inventory.deployments[&DeploymentId::from("qwen36-fast")];
        assert_eq!(
            conflict(&inventory, &phases, qwen).unwrap().0,
            "sp1 is held by deployment dsv4f-tp2 (starting); stop it first"
        );
        let dsv4f = &inventory.deployments[&DeploymentId::from("dsv4f-tp2")];
        assert_eq!(
            conflict(&inventory, &phases, dsv4f),
            None,
            "its own hold is no conflict"
        );

        phases.insert(
            DeploymentId::from("dsv4f-tp2"),
            Phase::Failed {
                at_ms: T0,
                reason: "x".into(),
            },
        );
        assert!(reservations(&inventory, &phases).is_empty());
        assert_eq!(conflict(&inventory, &phases, qwen), None);
    }

    #[test]
    fn a_deployment_whose_record_is_gone_holds_nothing() {
        let inventory = bginf_test_inventory();
        let mut phases = BTreeMap::new();
        phases.insert(DeploymentId::from("gone"), starting());
        assert!(reservations(&inventory, &phases).is_empty());
    }

    #[test]
    fn fit_compares_each_machine_and_never_reads_unmeasured_as_room() {
        let room = |free: Option<u64>, need: Option<u64>| Room {
            free_bytes: free,
            need_bytes: need,
        };
        let gib = 1u64 << 30;
        let mut rooms = BTreeMap::new();
        rooms.insert(
            MachineName::from("sp1"),
            room(Some(110 * gib), Some(100 * gib)),
        );
        rooms.insert(
            MachineName::from("sp2"),
            room(Some(110 * gib), Some(100 * gib)),
        );
        assert_eq!(fit(&rooms), Fit::Fits);
        rooms.insert(
            MachineName::from("sp2"),
            room(Some(20 * gib), Some(100 * gib)),
        );
        assert_eq!(
            fit(&rooms).describe(),
            "sp2 has 20.0 GiB free and needs 100.0 GiB"
        );
        rooms.insert(MachineName::from("sp2"), room(None, Some(100 * gib)));
        assert!(matches!(fit(&rooms), Fit::Short(_)));
        let unknown: BTreeMap<_, _> = [(MachineName::from("sp1"), room(Some(1), None))].into();
        assert_eq!(fit(&unknown), Fit::Unknown);
    }

    #[test]
    fn only_command_recipes_can_be_run() {
        let mut recipe = crate::fixture::command_recipe("r", 1);
        assert_eq!(
            commands(&recipe).unwrap(),
            ("./start.sh", Some("./stop.sh"))
        );
        recipe.engine = Engine::Container {
            image: "x@sha256:00".into(),
            args: Vec::new(),
        };
        assert!(commands(&recipe)
            .unwrap_err()
            .0
            .contains("container engine"));
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
