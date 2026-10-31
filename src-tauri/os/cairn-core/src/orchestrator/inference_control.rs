//! The act side of the cairn-inference facet: the orch starts, stops and
//! health-checks deployments through their head executors, holds every machine
//! of a gang away from batch work while it does, and records how long each load
//! took and how much GPU memory it settled at.
//!
//! The rules are the facet's (`cairn_inference::control`); this module is their
//! host. It keeps one row per deployment Cairn has acted on
//! (`inference_deployment_state`) and one per finished load
//! (`inference_deployment_loads`), and a single background task owns every
//! command after a request: it sends a reserving start once the gang fits,
//! applies the head's reports, probes deployments Cairn controls, fails a
//! command that outlives its bound, and restates the fleet's holds. A request
//! itself only moves the stored phase (and sends a stop, which needs no wait),
//! inside one database transaction, so a request and the task never act on
//! two different readings of a phase.
//!
//! Deployments Cairn has never acted on are left alone entirely: bginf may be
//! running them, and Cairn takes no machine away from builds for a model it did
//! not load or stop.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cairn_common::executor_protocol::{
    DeploymentOperation, DeploymentPhaseReport, DeploymentScript, ExecutorHealthSnapshot,
    GpuMemory, InferenceHold, MeasurementReading,
};
use cairn_db::turso::params;
use cairn_inference::control::{
    self, Drain, GangMachine, LoadOutcome, Observation, Phase, Report, Request, Room, RunningBatch,
    START_TIMEOUT_SECS, STOPPED_HEALTH_WAIT_SECS, STOP_TIMEOUT_SECS,
};
use cairn_inference::{Deployment, DeploymentId, Inventory};

use super::inference::load_inventory;
use super::Orchestrator;
use crate::fleet::inference::Preemption;
use crate::fleet::{DeploymentReportReceived, Fleet};
use crate::storage::{LocalDb, RowExt};

/// How often the task looks at every deployment it controls.
pub const TICK: Duration = Duration::from_secs(5);
/// How often a deployment Cairn controls is probed while no command runs.
pub const PROBE_INTERVAL_MS: i64 = 30_000;
/// How long a healthy deployment runs before its GPU memory is recorded as
/// its footprint, so the reading is the settled one and not the climb.
pub const SETTLE_MS: i64 = 60_000;
/// Loads kept per deployment.
const LOADS_KEPT: i64 = 50;
const PROBE_PREFIX: &str = "probe-";

fn now_ms() -> i64 {
    crate::fleet::unix_time_ms() as i64
}

fn new_command_id(kind: &str) -> String {
    format!("{kind}-{}", uuid::Uuid::new_v4())
}

/// One attached machine's readings, as the controller uses them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MachineReading {
    /// Memory the machine's GPUs hold: the GPU-attributed share on a
    /// unified-memory machine, VRAM used on discrete cards. `None` when no
    /// GPU memory was measured.
    pub gpu_allocated_bytes: Option<u64>,
    /// Memory a deployment could take: free VRAM on discrete cards, otherwise
    /// the host's available memory.
    pub free_bytes: Option<u64>,
    /// Whether `free_bytes` is the host's memory, the memory batch work uses
    /// (a unified-memory machine, or one with no discrete card), rather than
    /// VRAM no batch touches.
    pub free_is_host_memory: bool,
}

impl MachineReading {
    pub fn of(snapshot: &ExecutorHealthSnapshot) -> Self {
        let mut allocated: Option<u64> = None;
        let mut discrete_free: Option<u64> = None;
        if let MeasurementReading::Measured { value: devices } = &snapshot.host.gpus.reading {
            if devices.is_empty() {
                allocated = Some(0);
            }
            for device in devices {
                match &device.memory {
                    MeasurementReading::Measured {
                        value:
                            GpuMemory::Discrete {
                                used_bytes,
                                total_bytes,
                            },
                    } => {
                        *allocated.get_or_insert(0) += used_bytes;
                        *discrete_free.get_or_insert(0) += total_bytes.saturating_sub(*used_bytes);
                    }
                    MeasurementReading::Measured {
                        value:
                            GpuMemory::Unified {
                                gpu_allocated_bytes: MeasurementReading::Measured { value: bytes },
                                ..
                            },
                    } => {
                        *allocated.get_or_insert(0) += bytes;
                    }
                    _ => {}
                }
            }
        }
        let host_free = match &snapshot.machine.memory.reading {
            MeasurementReading::Measured { value } => Some(value.available_bytes),
            MeasurementReading::Unavailable { .. } => None,
        };
        Self {
            gpu_allocated_bytes: allocated,
            free_is_host_memory: discrete_free.is_none(),
            free_bytes: discrete_free.or(host_free),
        }
    }
}

/// What the controller needs of the fleet: sending to a head, reading the
/// attached machines, and applying the holds.
pub(crate) trait Heads: Send + Sync {
    fn send(
        &self,
        executor: &str,
        command_id: &str,
        deployment: &str,
        operation: DeploymentOperation,
    ) -> Result<(), String>;
    /// Every attached machine's readings, by public name.
    fn readings(&self) -> BTreeMap<String, MachineReading>;
    fn hold(&self, holds: HashMap<String, InferenceHold>);
    /// The batches running on these machines, by public name.
    fn running(&self, machines: &BTreeSet<String>) -> BTreeMap<String, Vec<RunningBatch>>;
    /// Stop one running batch to make room; false when it has already ended.
    fn preempt(&self, request_id: &str, attempt_id: &str, preemption: Preemption) -> bool;
}

impl Heads for Fleet {
    fn send(
        &self,
        executor: &str,
        command_id: &str,
        deployment: &str,
        operation: DeploymentOperation,
    ) -> Result<(), String> {
        self.send_deployment_command(executor, command_id, deployment, operation)
    }

    fn readings(&self) -> BTreeMap<String, MachineReading> {
        self.executor_health(crate::fleet::unix_time_ms())
            .iter()
            .map(|snapshot| (snapshot.public_name.clone(), MachineReading::of(snapshot)))
            .collect()
    }

    fn hold(&self, holds: HashMap<String, InferenceHold>) {
        self.set_inference_holds(holds);
    }

    fn running(&self, machines: &BTreeSet<String>) -> BTreeMap<String, Vec<RunningBatch>> {
        self.running_batches(machines)
    }

    fn preempt(&self, request_id: &str, attempt_id: &str, preemption: Preemption) -> bool {
        Fleet::preempt(
            self,
            (request_id.to_string(), attempt_id.to_string()),
            preemption,
        )
    }
}

/// One deployment's stored row.
#[derive(Debug, Clone, PartialEq)]
pub struct StateRow {
    pub phase: Phase,
    /// The start or stop its head is running, while one is.
    pub command_id: Option<String>,
    pub updated_at: i64,
}

/// One finished load, newest first in [`load_history`].
#[derive(Debug, Clone, PartialEq)]
pub struct LoadRow {
    pub recipe: String,
    pub requested_at: i64,
    pub sent_at: Option<i64>,
    pub finished_at: i64,
    pub healthy: bool,
    pub load_ms: Option<u64>,
    pub reason: Option<String>,
    /// GPU-allocated bytes per machine once the load had settled.
    pub footprint: Option<BTreeMap<String, u64>>,
}

pub async fn load_states(db: &LocalDb) -> Result<BTreeMap<DeploymentId, StateRow>, String> {
    let rows = db
        .query_all(
            "SELECT deployment_id, phase_json, command_id, updated_at
               FROM inference_deployment_state ORDER BY deployment_id",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?, row.i64(3)?)),
        )
        .await
        .map_err(|error| format!("read inference_deployment_state: {error}"))?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, json, command_id, updated_at)| {
            match serde_json::from_str::<Phase>(&json) {
                Ok(phase) => Some((
                    DeploymentId::new(id),
                    StateRow {
                        phase,
                        command_id,
                        updated_at,
                    },
                )),
                Err(error) => {
                    log::warn!(
                        "inference: the phase of {id} does not parse and is skipped: {error}"
                    );
                    None
                }
            }
        })
        .collect())
}

pub async fn load_history(db: &LocalDb) -> Result<BTreeMap<DeploymentId, Vec<LoadRow>>, String> {
    let rows = db
        .query_all(
            "SELECT deployment_id, recipe_id, requested_at, sent_at, finished_at, outcome, load_ms,
                    reason, footprint_json
               FROM inference_deployment_loads ORDER BY deployment_id, id DESC",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    LoadRow {
                        recipe: row.text(1)?,
                        requested_at: row.i64(2)?,
                        sent_at: row.opt_i64(3)?,
                        finished_at: row.i64(4)?,
                        healthy: row.text(5)? == "healthy",
                        load_ms: row.opt_i64(6)?.map(|ms| ms.max(0) as u64),
                        reason: row.opt_text(7)?,
                        footprint: row
                            .opt_text(8)?
                            .and_then(|json| serde_json::from_str(&json).ok()),
                    },
                ))
            },
        )
        .await
        .map_err(|error| format!("read inference_deployment_loads: {error}"))?;
    let mut history: BTreeMap<DeploymentId, Vec<LoadRow>> = BTreeMap::new();
    for (id, load) in rows {
        history.entry(DeploymentId::new(id)).or_default().push(load);
    }
    Ok(history)
}

/// The footprint the newest measured load recorded, per machine.
pub fn last_footprint(loads: Option<&Vec<LoadRow>>) -> BTreeMap<String, u64> {
    loads
        .into_iter()
        .flatten()
        .find_map(|load| load.footprint.clone())
        .unwrap_or_default()
}

type Decide = dyn Fn(Option<&StateRow>) -> Option<(Phase, Option<String>)> + Send + Sync;

/// Apply `decide` to the stored row in one transaction. Returns the phase
/// before and the row after when it changed anything.
async fn transition(
    db: &LocalDb,
    id: &DeploymentId,
    now_ms: i64,
    decide: Arc<Decide>,
) -> Result<Option<(Option<Phase>, StateRow)>, String> {
    let id = id.as_str().to_string();
    db.write(move |conn| {
        let id = id.clone();
        let decide = Arc::clone(&decide);
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT phase_json, command_id, updated_at
                       FROM inference_deployment_state WHERE deployment_id = ?1",
                    params![id.as_str()],
                )
                .await?;
            let current = match rows.next().await? {
                Some(row) => {
                    let json = row.text(0)?;
                    let command_id = row.opt_text(1)?;
                    let updated_at = row.i64(2)?;
                    serde_json::from_str::<Phase>(&json)
                        .ok()
                        .map(|phase| StateRow {
                            phase,
                            command_id,
                            updated_at,
                        })
                }
                None => None,
            };
            drop(rows);
            let Some((phase, command_id)) = decide(current.as_ref()) else {
                return Ok(None);
            };
            let json = serde_json::to_string(&phase).expect("phases serialize");
            conn.execute(
                "INSERT INTO inference_deployment_state
                   (deployment_id, phase_json, command_id, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(deployment_id) DO UPDATE SET
                   phase_json = excluded.phase_json,
                   command_id = excluded.command_id,
                   updated_at = excluded.updated_at",
                params![id.as_str(), json, command_id.clone(), now_ms],
            )
            .await?;
            Ok(Some((
                current.map(|row| row.phase),
                StateRow {
                    phase,
                    command_id,
                    updated_at: now_ms,
                },
            )))
        })
    })
    .await
    .map_err(|error| format!("write inference_deployment_state: {error}"))
}

async fn record_load(
    db: &LocalDb,
    deployment: &Deployment,
    before: &Phase,
    outcome: LoadOutcome,
    now_ms: i64,
) -> Result<(), String> {
    let (requested_at, sent_at) = match before {
        Phase::Starting {
            since_ms,
            requested_ms,
        } => (*requested_ms, Some(*since_ms)),
        other => (other.since_ms(), None),
    };
    let (outcome, load_ms, reason) = match outcome {
        LoadOutcome::Healthy { load_ms } => ("healthy", Some(load_ms as i64), None),
        LoadOutcome::Failed { reason } => ("failed", None, Some(reason)),
    };
    let id = deployment.id.as_str().to_string();
    let recipe = deployment.recipe.as_str().to_string();
    db.write(move |conn| {
        let (id, recipe, reason) = (id.clone(), recipe.clone(), reason.clone());
        Box::pin(async move {
            conn.execute(
                "INSERT INTO inference_deployment_loads
                   (deployment_id, recipe_id, requested_at, sent_at, finished_at, outcome, load_ms, reason)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![id.as_str(), recipe, requested_at, sent_at, now_ms, outcome, load_ms, reason],
            )
            .await?;
            conn.execute(
                "DELETE FROM inference_deployment_loads
                  WHERE deployment_id = ?1
                    AND id NOT IN (SELECT id FROM inference_deployment_loads
                                    WHERE deployment_id = ?1 ORDER BY id DESC LIMIT ?2)",
                params![id.as_str(), LOADS_KEPT],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("write inference_deployment_loads: {error}"))
}

/// Record the settled footprint on the load that made this healthy phase.
async fn record_footprint(
    db: &LocalDb,
    id: &DeploymentId,
    finished_at: i64,
    footprint: &BTreeMap<String, u64>,
) -> Result<(), String> {
    let json = serde_json::to_string(footprint).expect("footprints serialize");
    db.execute(
        "UPDATE inference_deployment_loads SET footprint_json = ?3
          WHERE deployment_id = ?1 AND finished_at = ?2 AND outcome = 'healthy'
            AND footprint_json IS NULL",
        params![id.as_str(), finished_at, json],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write inference_deployment_loads: {error}"))
}

fn phases_of(states: &BTreeMap<DeploymentId, StateRow>) -> BTreeMap<DeploymentId, Phase> {
    states
        .iter()
        .map(|(id, row)| (id.clone(), row.phase.clone()))
        .collect()
}

/// The machines every reserving phase holds, by executor public name.
pub fn holds_of(
    inventory: &Inventory,
    states: &BTreeMap<DeploymentId, StateRow>,
) -> HashMap<String, InferenceHold> {
    control::reservations(inventory, &phases_of(states))
        .into_iter()
        .map(|(machine, held)| {
            let waiting_on = match states.get(&held.deployment).map(|row| &row.phase) {
                Some(Phase::Reserving { waiting_on, .. }) => waiting_on.clone(),
                _ => None,
            };
            (
                machine.as_str().to_string(),
                InferenceHold {
                    deployment: held.deployment.to_string(),
                    phase: held.phase.to_string(),
                    waiting_on,
                },
            )
        })
        .collect()
}

/// Restate the fleet's holds from the stored phases.
pub(crate) async fn refresh_holds(db: &LocalDb, heads: &dyn Heads) -> Result<(), String> {
    let inventory = load_inventory(db).await?;
    let states = load_states(db).await?;
    heads.hold(holds_of(&inventory, &states));
    Ok(())
}

/// What the head runs for one deployment.
struct Plan {
    start: String,
    stop: Option<String>,
    health_url: String,
}

impl Plan {
    fn of(inventory: &Inventory, deployment: &Deployment) -> Result<Self, String> {
        let recipe = inventory
            .recipes
            .get(&deployment.recipe)
            .ok_or_else(|| format!("its recipe {} is missing", deployment.recipe))?;
        let (start, stop) = control::commands(recipe).map_err(|refusal| refusal.0)?;
        let address = inventory.address_of(&deployment.head).ok_or_else(|| {
            format!(
                "the head {}'s address is unknown, so its health URL cannot be formed",
                deployment.head
            )
        })?;
        Ok(Self {
            start: start.to_string(),
            stop: stop.map(str::to_string),
            health_url: deployment.health_url(address),
        })
    }

    fn start_operation(&self) -> DeploymentOperation {
        DeploymentOperation::Start {
            script: self.start.clone(),
            timeout_secs: START_TIMEOUT_SECS,
            health_url: self.health_url.clone(),
            cleanup: self.stop.clone().map(|script| DeploymentScript {
                script,
                timeout_secs: STOP_TIMEOUT_SECS,
            }),
        }
    }

    fn stop_operation(&self) -> Option<DeploymentOperation> {
        Some(DeploymentOperation::Stop {
            script: self.stop.clone()?,
            health_url: self.health_url.clone(),
            timeout_secs: STOP_TIMEOUT_SECS,
            stopped_health_wait_secs: STOPPED_HEALTH_WAIT_SECS,
        })
    }
}

/// Which stored command an observation must belong to.
#[derive(Clone)]
enum Matching {
    /// The task's own observation of the phase.
    Any,
    /// The head's report on this start or stop.
    Command(String),
    /// A probe, which says nothing while a command runs.
    Probe,
}

/// Apply an observation, record the load it finished, and restate the holds.
/// `command` is the command id the phase holds afterwards when it is one in
/// flight; `None` keeps the one it held.
#[allow(clippy::too_many_arguments)]
async fn apply(
    db: &LocalDb,
    heads: &dyn Heads,
    inventory: &Inventory,
    id: &DeploymentId,
    observation: Observation,
    matching: Matching,
    command: Option<String>,
    now_ms: i64,
) -> Result<Option<Phase>, String> {
    let changed = transition(
        db,
        id,
        now_ms,
        Arc::new(move |current| {
            let row = current?;
            match &matching {
                Matching::Any => {}
                Matching::Command(expected) => {
                    if row.command_id.as_deref() != Some(expected.as_str()) {
                        return None;
                    }
                }
                Matching::Probe => {
                    if row.phase.command_in_flight() {
                        return None;
                    }
                }
            }
            let next = control::observe(&row.phase, observation.clone(), now_ms)?;
            let command_id = next
                .command_in_flight()
                .then(|| command.clone().or_else(|| row.command_id.clone()))
                .flatten();
            Some((next, command_id))
        }),
    )
    .await?;
    let Some((before, after)) = changed else {
        return Ok(None);
    };
    settle(
        db,
        heads,
        inventory,
        id,
        before.as_ref(),
        &after.phase,
        now_ms,
    )
    .await?;
    Ok(Some(after.phase))
}

/// What follows any change of phase: the load it finished, the holds, a log.
async fn settle(
    db: &LocalDb,
    heads: &dyn Heads,
    inventory: &Inventory,
    id: &DeploymentId,
    before: Option<&Phase>,
    after: &Phase,
    now_ms: i64,
) -> Result<(), String> {
    log::info!(
        "inference: deployment {id} {} -> {}",
        before.map_or("(never acted on)", Phase::label),
        after.label()
    );
    if let (Some(before), Some(deployment)) = (before, inventory.deployments.get(id)) {
        if let Some(outcome) = control::finished_load(before, after) {
            record_load(db, deployment, before, outcome, now_ms).await?;
        }
    }
    refresh_holds(db, heads).await
}

fn machine_list(deployment: &Deployment) -> String {
    deployment
        .machines
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Start or stop one deployment, answering with what was done or why not.
pub(crate) async fn request(
    db: &LocalDb,
    heads: &dyn Heads,
    id: &str,
    request: Request,
    now_ms: i64,
) -> Result<String, String> {
    let inventory = load_inventory(db).await?;
    let id = DeploymentId::new(id);
    let deployment = inventory
        .deployments
        .get(&id)
        .ok_or_else(|| {
            format!("there is no deployment {id}; cairn://inference/deployments lists them")
        })?
        .clone();
    let plan = Plan::of(&inventory, &deployment)
        .map_err(|why| format!("Cairn cannot {} {id}: {why}", request.as_str()))?;
    let states = load_states(db).await?;
    match request {
        Request::Start => {
            if let Some(refusal) = control::conflict(&inventory, &phases_of(&states), &deployment) {
                return Err(format!("Cairn cannot start {id}: {refusal}"));
            }
        }
        Request::Stop => {
            if plan.stop.is_none() {
                return Err(format!(
                    "Cairn cannot stop {id}: its recipe {} has no stop command",
                    deployment.recipe
                ));
            }
        }
    }
    let stop_command = new_command_id("stop");
    let refused: Arc<Mutex<Option<String>>> = Arc::default();
    let changed = {
        let refused = Arc::clone(&refused);
        let stop_command = stop_command.clone();
        transition(
            db,
            &id,
            now_ms,
            Arc::new(move |current| {
                match control::request(current.map(|row| &row.phase), request, now_ms) {
                    Ok(next) => {
                        *refused.lock().unwrap() = None;
                        let command =
                            matches!(next, Phase::Stopping { .. }).then(|| stop_command.clone());
                        Some((next, command))
                    }
                    Err(refusal) => {
                        *refused.lock().unwrap() = Some(refusal.0);
                        None
                    }
                }
            }),
        )
        .await?
    };
    let Some((before, after)) = changed else {
        let why = refused
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| "nothing changed".to_string());
        return Err(format!("Cairn did not {} {id}: {why}", request.as_str()));
    };
    settle(
        db,
        heads,
        &inventory,
        &id,
        before.as_ref(),
        &after.phase,
        now_ms,
    )
    .await?;
    match &after.phase {
        Phase::Reserving { .. } => Ok(format!(
            "Deployment {id}: start requested. Its machines ({}) are reserved now and take no batch work; the start goes to its head {} once each machine has the memory it needs. Read cairn://inference/deployments/{id} for its phase.",
            machine_list(&deployment),
            deployment.head
        )),
        Phase::Stopping { .. } => {
            let operation = plan.stop_operation().expect("checked above");
            if let Err(why) = heads.send(deployment.head.as_str(), &stop_command, id.as_str(), operation) {
                let reason = format!("the stop could not be sent: {why}");
                apply(
                    db,
                    heads,
                    &inventory,
                    &id,
                    Observation::NotSent {
                        reason: reason.clone(),
                    },
                    Matching::Command(stop_command),
                    None,
                    now_ms,
                )
                .await?;
                return Err(format!("Cairn did not stop {id}: {reason}"));
            }
            Ok(format!(
                "Deployment {id}: stop sent to its head {}. Its machines ({}) stay reserved until its health check stops answering. Read cairn://inference/deployments/{id} for its phase.",
                deployment.head,
                machine_list(&deployment)
            ))
        }
        Phase::Stopped { .. } => Ok(format!(
            "Deployment {id}: its start had not been sent, so nothing ran; its machines are released."
        )),
        other => Ok(format!("Deployment {id}: {}", other.label())),
    }
}

/// Map the head's report onto the facet's vocabulary.
fn report_of(phase: DeploymentPhaseReport) -> Report {
    match phase {
        DeploymentPhaseReport::Starting => Report::Starting,
        DeploymentPhaseReport::Healthy { ran_start } => Report::Healthy { ran_start },
        DeploymentPhaseReport::Stopping => Report::Stopping,
        DeploymentPhaseReport::Stopped => Report::Stopped,
        DeploymentPhaseReport::Failed { reason } => Report::Failed { reason },
    }
}

/// Apply one report from a head.
pub(crate) async fn on_report(
    db: &LocalDb,
    heads: &dyn Heads,
    report: DeploymentReportReceived,
    now_ms: i64,
) -> Result<(), String> {
    let inventory = load_inventory(db).await?;
    let id = DeploymentId::new(report.deployment.as_str());
    let (observation, matching) = if report.command_id.starts_with(PROBE_PREFIX) {
        (
            Observation::Probed {
                healthy: matches!(report.phase, DeploymentPhaseReport::Healthy { .. }),
            },
            Matching::Probe,
        )
    } else {
        (
            Observation::Report(report_of(report.phase)),
            Matching::Command(report.command_id),
        )
    };
    apply(
        db,
        heads,
        &inventory,
        &id,
        observation,
        matching,
        None,
        now_ms,
    )
    .await?;
    Ok(())
}

/// The task's own state between ticks: when each deployment was last probed.
#[derive(Debug, Default)]
pub(crate) struct Probes {
    last: HashMap<DeploymentId, i64>,
}

impl Probes {
    fn due(&mut self, id: &DeploymentId, now_ms: i64) -> bool {
        match self.last.get(id) {
            Some(last) if now_ms - last < PROBE_INTERVAL_MS => false,
            _ => {
                self.last.insert(id.clone(), now_ms);
                true
            }
        }
    }
}

fn probe(
    heads: &dyn Heads,
    probes: &mut Probes,
    deployment: &Deployment,
    plan: &Plan,
    readings: &BTreeMap<String, MachineReading>,
    now_ms: i64,
) {
    if !readings.contains_key(deployment.head.as_str()) || !probes.due(&deployment.id, now_ms) {
        return;
    }
    let command = new_command_id(PROBE_PREFIX.trim_end_matches('-'));
    if let Err(why) = heads.send(
        deployment.head.as_str(),
        &command,
        deployment.id.as_str(),
        DeploymentOperation::Probe {
            health_url: plan.health_url.clone(),
        },
    ) {
        log::debug!("inference: could not probe {}: {why}", deployment.id);
    }
}

/// One pass over every deployment Cairn controls.
pub(crate) async fn tick(
    db: &LocalDb,
    heads: &dyn Heads,
    probes: &mut Probes,
    now_ms: i64,
) -> Result<(), String> {
    let inventory = load_inventory(db).await?;
    let states = load_states(db).await?;
    let history = load_history(db).await?;
    let readings = heads.readings();
    for (id, row) in &states {
        let Some(deployment) = inventory.deployments.get(id) else {
            continue;
        };
        if let Some(reason) = control::overdue(&row.phase, now_ms) {
            apply(
                db,
                heads,
                &inventory,
                id,
                Observation::Overdue { reason },
                Matching::Any,
                None,
                now_ms,
            )
            .await?;
            continue;
        }
        let plan = match Plan::of(&inventory, deployment) {
            Ok(plan) => plan,
            Err(why) => {
                if matches!(row.phase, Phase::Reserving { .. }) {
                    apply(
                        db,
                        heads,
                        &inventory,
                        id,
                        Observation::NotSent { reason: why },
                        Matching::Any,
                        None,
                        now_ms,
                    )
                    .await?;
                }
                continue;
            }
        };
        match &row.phase {
            Phase::Reserving { since_ms, .. } => {
                dispatch(
                    db, heads, &inventory, deployment, &plan, &readings, &history, probes,
                    *since_ms, now_ms,
                )
                .await?;
            }
            Phase::Healthy { since_ms, .. } => {
                probe(heads, probes, deployment, &plan, &readings, now_ms);
                let unrecorded =
                    history
                        .get(id)
                        .and_then(|loads| loads.first())
                        .is_some_and(|load| {
                            load.healthy
                                && load.finished_at == *since_ms
                                && load.footprint.is_none()
                        });
                if unrecorded && now_ms - since_ms >= SETTLE_MS {
                    let footprint: Option<BTreeMap<String, u64>> = deployment
                        .machines
                        .iter()
                        .map(|machine| {
                            let bytes = readings.get(machine.as_str())?.gpu_allocated_bytes?;
                            Some((machine.as_str().to_string(), bytes))
                        })
                        .collect();
                    if let Some(footprint) = footprint {
                        record_footprint(db, id, *since_ms, &footprint).await?;
                    }
                }
            }
            Phase::Stopped { .. } | Phase::Failed { .. } => {
                probe(heads, probes, deployment, &plan, &readings, now_ms);
            }
            Phase::Starting { .. } | Phase::Stopping { .. } => {}
        }
    }
    refresh_holds(db, heads).await
}

/// Send a reserving start once its head is attached and the gang fits,
/// draining its machines meanwhile: they take no new batch work (the hold),
/// the work already there may finish until the drain's bound, and past it the
/// checks whose measured memory is in the way are preempted
/// ([`control::drain`]).
#[allow(clippy::too_many_arguments)]
async fn dispatch(
    db: &LocalDb,
    heads: &dyn Heads,
    inventory: &Inventory,
    deployment: &Deployment,
    plan: &Plan,
    readings: &BTreeMap<String, MachineReading>,
    history: &BTreeMap<DeploymentId, Vec<LoadRow>>,
    probes: &mut Probes,
    reserving_since_ms: i64,
    now_ms: i64,
) -> Result<(), String> {
    let id = &deployment.id;
    let waiting = |on: String| Observation::Waiting { on };
    if !readings.contains_key(deployment.head.as_str()) {
        apply(
            db,
            heads,
            inventory,
            id,
            waiting(format!("its head {} is not attached", deployment.head)),
            Matching::Any,
            None,
            now_ms,
        )
        .await?;
        return Ok(());
    }
    let footprint = last_footprint(history.get(id));
    let recipe = inventory.recipes.get(&deployment.recipe);
    let stated = recipe.and_then(control::stated_need);
    let names: BTreeSet<String> = deployment
        .machines
        .iter()
        .map(|machine| machine.as_str().to_string())
        .collect();
    let mut running = heads.running(&names);
    let gang: BTreeMap<_, _> = deployment
        .machines
        .iter()
        .map(|machine| {
            let reading = readings.get(machine.as_str());
            (
                machine.clone(),
                GangMachine {
                    room: Room {
                        free_bytes: reading.and_then(|r| r.free_bytes),
                        need_bytes: footprint.get(machine.as_str()).copied().or(stated),
                    },
                    shared_with_batches: reading.is_some_and(|r| r.free_is_host_memory),
                    batches: running.remove(machine.as_str()).unwrap_or_default(),
                },
            )
        })
        .collect();
    let on = match control::drain(&gang, reserving_since_ms, now_ms) {
        Drain::Start => None,
        Drain::Wait(on) => Some(on),
        Drain::Preempt { batches, waiting } => {
            let model = recipe
                .map(|recipe| recipe.model.name.to_string())
                .unwrap_or_else(|| deployment.recipe.to_string());
            for batch in batches {
                heads.preempt(
                    &batch.request_id,
                    &batch.attempt_id,
                    Preemption {
                        deployment: id.to_string(),
                        model: model.clone(),
                        executor: batch.machine.as_str().to_string(),
                        need_bytes: batch.need_bytes,
                        free_bytes: batch.free_bytes,
                        resident_bytes: batch.resident_bytes,
                    },
                );
            }
            Some(waiting)
        }
    };
    if let Some(on) = on {
        apply(
            db,
            heads,
            inventory,
            id,
            waiting(on),
            Matching::Any,
            None,
            now_ms,
        )
        .await?;
        // It may already be running and using the very memory it waits for:
        // a probe that finds it answering adopts it.
        probe(heads, probes, deployment, plan, readings, now_ms);
        return Ok(());
    }
    let command = new_command_id("start");
    let sent = apply(
        db,
        heads,
        inventory,
        id,
        Observation::StartSent,
        Matching::Any,
        Some(command.clone()),
        now_ms,
    )
    .await?;
    if sent.is_none() {
        return Ok(());
    }
    log::info!("inference: starting {id} on {}", deployment.head);
    if let Err(why) = heads.send(
        deployment.head.as_str(),
        &command,
        id.as_str(),
        plan.start_operation(),
    ) {
        apply(
            db,
            heads,
            inventory,
            id,
            Observation::NotSent {
                reason: format!("the start could not be sent: {why}"),
            },
            Matching::Command(command),
            None,
            now_ms,
        )
        .await?;
    }
    Ok(())
}

/// Start or stop a deployment for an agent or the operator.
pub async fn request_deployment(
    orch: &Orchestrator,
    id: &str,
    action: Request,
) -> Result<String, String> {
    request(&orch.db.local, orch.fleet.as_ref(), id, action, now_ms()).await
}

/// Everything the deployment pages render about what Cairn has done.
#[derive(Debug, Clone, Default)]
pub struct ControlView {
    pub states: BTreeMap<DeploymentId, StateRow>,
    pub loads: BTreeMap<DeploymentId, Vec<LoadRow>>,
    /// Attached machines' readings, by public name.
    pub readings: BTreeMap<String, MachineReading>,
}

pub async fn load_control_view(orch: &Orchestrator) -> Result<ControlView, String> {
    Ok(ControlView {
        states: load_states(&orch.db.local).await?,
        loads: load_history(&orch.db.local).await?,
        readings: orch.fleet.readings(),
    })
}

/// Act on deployments for the life of the process: restate the holds from
/// the stored phases at once (a model loaded before a restart still holds its
/// machines), then apply every report and tick every [`TICK`].
pub fn spawn_deployment_control(orch: Orchestrator) {
    tokio::spawn(async move {
        let mut reports = orch.fleet.subscribe_deployment_reports();
        let mut probes = Probes::default();
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let heads: &dyn Heads = orch.fleet.as_ref();
        if let Err(error) = refresh_holds(&orch.db.local, heads).await {
            log::warn!("inference: could not restate deployment holds: {error}");
        }
        loop {
            tokio::select! {
                Some(report) = reports.recv() => {
                    if let Err(error) = on_report(&orch.db.local, heads, report, now_ms()).await {
                        log::warn!("inference: a deployment report was not applied: {error}");
                    }
                }
                _ = ticks.tick() => {
                    if let Err(error) = tick(&orch.db.local, heads, &mut probes, now_ms()).await {
                        log::warn!("inference: a deployment control pass failed: {error}");
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_inference::{
        DeploymentSet, Engine, ModelRef, Pool, PoolMachine, Recipe, RecordSource, Requirements,
    };

    #[derive(Default)]
    struct FakeHeads {
        sent: Mutex<Vec<(String, String, String, DeploymentOperation)>>,
        readings: Mutex<BTreeMap<String, MachineReading>>,
        holds: Mutex<HashMap<String, InferenceHold>>,
        refuse: Mutex<Option<String>>,
        running: Mutex<BTreeMap<String, Vec<RunningBatch>>>,
        preempted: Mutex<Vec<(String, Preemption)>>,
    }

    impl Heads for FakeHeads {
        fn send(
            &self,
            executor: &str,
            command_id: &str,
            deployment: &str,
            operation: DeploymentOperation,
        ) -> Result<(), String> {
            if let Some(why) = self.refuse.lock().unwrap().clone() {
                return Err(why);
            }
            self.sent.lock().unwrap().push((
                executor.into(),
                command_id.into(),
                deployment.into(),
                operation,
            ));
            Ok(())
        }
        fn readings(&self) -> BTreeMap<String, MachineReading> {
            self.readings.lock().unwrap().clone()
        }
        fn hold(&self, holds: HashMap<String, InferenceHold>) {
            *self.holds.lock().unwrap() = holds;
        }
        fn running(&self, machines: &BTreeSet<String>) -> BTreeMap<String, Vec<RunningBatch>> {
            self.running
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, _)| machines.contains(*name))
                .map(|(name, batches)| (name.clone(), batches.clone()))
                .collect()
        }
        fn preempt(&self, request_id: &str, _attempt_id: &str, preemption: Preemption) -> bool {
            for batches in self.running.lock().unwrap().values_mut() {
                for batch in batches.iter_mut().filter(|b| b.request_id == request_id) {
                    batch.preempted = true;
                }
            }
            self.preempted
                .lock()
                .unwrap()
                .push((request_id.to_string(), preemption));
            true
        }
    }

    impl FakeHeads {
        fn attach(&self, name: &str, free_gib: u64, allocated_gib: u64) {
            self.readings.lock().unwrap().insert(
                name.into(),
                MachineReading {
                    gpu_allocated_bytes: Some(allocated_gib << 30),
                    free_bytes: Some(free_gib << 30),
                    free_is_host_memory: true,
                },
            );
        }
        fn run(&self, name: &str, request_id: &str, restartable: bool, resident_gib: u64) {
            self.running
                .lock()
                .unwrap()
                .entry(name.into())
                .or_default()
                .push(RunningBatch {
                    request_id: request_id.into(),
                    attempt_id: format!("{request_id}-a"),
                    restartable,
                    resident_bytes: Some(resident_gib << 30),
                    preempted: false,
                });
        }
        fn last_sent(&self) -> (String, String, String, DeploymentOperation) {
            self.sent
                .lock()
                .unwrap()
                .last()
                .cloned()
                .expect("something was sent")
        }
        fn held(&self) -> Vec<String> {
            let mut names: Vec<_> = self.holds.lock().unwrap().keys().cloned().collect();
            names.sort();
            names
        }
    }

    const SP1: &str = "bginf-sp1";
    const SP2: &str = "bginf-sp2";
    const DSV4F: &str = "dsv4f-tp2";

    fn recipe(id: &str, machines: u32) -> Recipe {
        Recipe {
            id: id.into(),
            source: RecordSource::Bginf,
            model: ModelRef {
                name: id.into(),
                weights: Vec::new(),
            },
            served_name: id.into(),
            upstream_id: None,
            verbs: None,
            modalities: None,
            engine: Engine::Command {
                start: format!("./start-{id}.sh"),
                stop: Some(format!("./stop-{id}.sh")),
                description: None,
            },
            requirements: Requirements {
                machines,
                accelerator: None,
                interconnect: None,
                memory: None,
            },
            aliases: Vec::new(),
            notes: None,
        }
    }

    fn deployment(id: &str, machines: &[&str]) -> Deployment {
        Deployment {
            id: id.into(),
            source: RecordSource::Bginf,
            recipe: id.into(),
            machines: machines.iter().map(|m| (*m).into()).collect(),
            head: machines[0].into(),
            api_port: 8888,
            api_path: "/v1".into(),
            health_path: "/v1/models".into(),
        }
    }

    async fn sparks_db(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        let mut inventory = Inventory::default();
        inventory.insert_recipe(recipe(DSV4F, 2));
        inventory.insert_deployment(deployment(DSV4F, &[SP1, SP2]));
        inventory.insert_recipe(recipe("qwen36-fast", 1));
        inventory.insert_deployment(deployment("qwen36-fast", &[SP1]));
        inventory.insert_pool(Pool {
            id: "sparks".into(),
            source: RecordSource::Bginf,
            machines: vec![
                PoolMachine {
                    name: SP1.into(),
                    address: Some("10.20.20.40".into()),
                },
                PoolMachine {
                    name: SP2.into(),
                    address: Some("10.20.20.41".into()),
                },
            ],
            default: DeploymentSet::new(),
            pin: Some(DeploymentSet::new()),
        });
        inventory.validate().expect("the test inventory is valid");
        let rows: Vec<(&str, String, String)> = inventory
            .recipes
            .values()
            .map(|r| {
                (
                    "inference_recipes",
                    r.id.to_string(),
                    serde_json::to_string(r).unwrap(),
                )
            })
            .chain(inventory.deployments.values().map(|d| {
                (
                    "inference_deployments",
                    d.id.to_string(),
                    serde_json::to_string(d).unwrap(),
                )
            }))
            .chain(inventory.pools.values().map(|p| {
                (
                    "inference_pools",
                    p.id.to_string(),
                    serde_json::to_string(p).unwrap(),
                )
            }))
            .collect();
        for (table, id, body) in rows {
            db.execute(
                format!(
                    "INSERT INTO {table} (id, source, body_json, created_at, updated_at)
                     VALUES (?1, 'bginf', ?2, 0, 0)"
                ),
                params![id, body],
            )
            .await
            .unwrap();
        }
        db
    }

    async fn phase(db: &LocalDb, id: &str) -> Phase {
        load_states(db).await.unwrap()[&DeploymentId::from(id)]
            .phase
            .clone()
    }

    fn report(command_id: &str, phase: DeploymentPhaseReport) -> DeploymentReportReceived {
        DeploymentReportReceived {
            executor: SP1.into(),
            command_id: command_id.into(),
            deployment: DSV4F.into(),
            phase,
        }
    }

    #[tokio::test]
    async fn a_load_and_stop_runs_through_the_head_and_holds_the_gang_throughout() {
        let db = sparks_db("inference-control-cycle").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        heads.attach(SP2, 110, 0);
        let mut probes = Probes::default();

        let said = request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        assert!(said.contains("bginf-sp1, bginf-sp2"), "{said}");
        assert_eq!(phase(&db, DSV4F).await.label(), "reserving");
        assert_eq!(heads.held(), [SP1, SP2], "reserved before anything is sent");
        assert!(heads.sent.lock().unwrap().is_empty());

        tick(&db, &heads, &mut probes, 2_000).await.unwrap();
        let (executor, start_id, deployment, operation) = heads.last_sent();
        assert_eq!(
            (executor.as_str(), deployment.as_str()),
            (SP1, DSV4F),
            "only the head acts"
        );
        let DeploymentOperation::Start {
            script,
            health_url,
            cleanup,
            timeout_secs,
        } = operation
        else {
            panic!("a start was sent");
        };
        assert_eq!(script, "./start-dsv4f-tp2.sh");
        assert_eq!(health_url, "http://10.20.20.40:8888/v1/models");
        assert_eq!(timeout_secs, START_TIMEOUT_SECS);
        assert_eq!(cleanup.unwrap().script, "./stop-dsv4f-tp2.sh");
        assert_eq!(phase(&db, DSV4F).await.label(), "starting");

        // A report for some other command changes nothing.
        on_report(
            &db,
            &heads,
            report(
                "start-old",
                DeploymentPhaseReport::Healthy { ran_start: true },
            ),
            3_000,
        )
        .await
        .unwrap();
        assert_eq!(phase(&db, DSV4F).await.label(), "starting");

        on_report(
            &db,
            &heads,
            report(&start_id, DeploymentPhaseReport::Starting),
            3_000,
        )
        .await
        .unwrap();
        on_report(
            &db,
            &heads,
            report(
                &start_id,
                DeploymentPhaseReport::Healthy { ran_start: true },
            ),
            302_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Healthy {
                since_ms: 302_000,
                load_ms: Some(300_000)
            }
        );
        assert_eq!(heads.held(), [SP1, SP2]);
        let loads = &load_history(&db).await.unwrap()[&DeploymentId::from(DSV4F)];
        assert_eq!(loads.len(), 1);
        assert_eq!(
            (
                loads[0].healthy,
                loads[0].load_ms,
                loads[0].requested_at,
                loads[0].sent_at
            ),
            (true, Some(300_000), 1_000, Some(2_000))
        );

        // Settled: the GPU memory each Spark holds becomes the footprint.
        heads.attach(SP1, 10, 100);
        heads.attach(SP2, 10, 98);
        tick(&db, &heads, &mut probes, 302_000 + SETTLE_MS)
            .await
            .unwrap();
        let loads = &load_history(&db).await.unwrap()[&DeploymentId::from(DSV4F)];
        assert_eq!(
            loads[0].footprint,
            Some(BTreeMap::from([
                (SP1.to_string(), 100u64 << 30),
                (SP2.to_string(), 98u64 << 30)
            ]))
        );

        let said = request(&db, &heads, DSV4F, Request::Stop, 400_000)
            .await
            .unwrap();
        assert!(
            said.starts_with("Deployment dsv4f-tp2: stop sent to its head bginf-sp1"),
            "{said}"
        );
        let (executor, stop_id, _, operation) = heads.last_sent();
        assert_eq!(executor, SP1);
        assert!(
            matches!(operation, DeploymentOperation::Stop { ref script, .. } if script == "./stop-dsv4f-tp2.sh")
        );
        assert_eq!(
            heads.held(),
            [SP1, SP2],
            "a stopping model still holds its memory"
        );
        on_report(
            &db,
            &heads,
            report(&stop_id, DeploymentPhaseReport::Stopped),
            430_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Stopped { since_ms: 430_000 }
        );
        assert!(
            heads.held().is_empty(),
            "a stopped deployment releases both Sparks"
        );
    }

    #[tokio::test]
    async fn a_failed_start_reports_why_records_the_load_and_frees_the_machines() {
        let db = sparks_db("inference-control-failed").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        heads.attach(SP2, 110, 0);
        let mut probes = Probes::default();
        request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        tick(&db, &heads, &mut probes, 2_000).await.unwrap();
        let (_, start_id, _, _) = heads.last_sent();
        on_report(
            &db,
            &heads,
            report(
                &start_id,
                DeploymentPhaseReport::Failed {
                    reason: "the start command exited 127: not found".into(),
                },
            ),
            5_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Failed {
                at_ms: 5_000,
                reason: "the start command exited 127: not found".into()
            }
        );
        assert!(heads.held().is_empty());
        let loads = &load_history(&db).await.unwrap()[&DeploymentId::from(DSV4F)];
        assert_eq!(
            (loads[0].healthy, loads[0].reason.as_deref()),
            (false, Some("the start command exited 127: not found"))
        );
    }

    #[tokio::test]
    async fn a_start_waits_for_its_head_and_memory_and_gives_up_at_its_bound() {
        let db = sparks_db("inference-control-wait").await;
        let heads = FakeHeads::default();
        let mut probes = Probes::default();
        request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        tick(&db, &heads, &mut probes, 2_000).await.unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Reserving {
                since_ms: 1_000,
                waiting_on: Some("its head bginf-sp1 is not attached".into())
            }
        );
        assert!(heads.sent.lock().unwrap().is_empty());

        // A measured footprint from an earlier load makes memory the gate.
        db.execute(
            "INSERT INTO inference_deployment_loads
               (deployment_id, recipe_id, requested_at, finished_at, outcome, load_ms, footprint_json)
             VALUES (?1, ?1, 0, 1, 'healthy', 1, ?2)",
            params![DSV4F, format!("{{\"{SP1}\":{},\"{SP2}\":{}}}", 100u64 << 30, 100u64 << 30)],
        )
        .await
        .unwrap();
        heads.attach(SP1, 110, 0);
        heads.attach(SP2, 20, 0);
        tick(&db, &heads, &mut probes, 3_000).await.unwrap();
        let Phase::Reserving { waiting_on, .. } = phase(&db, DSV4F).await else {
            panic!("still reserving");
        };
        let waiting_on = waiting_on.unwrap_or_default();
        assert!(
            waiting_on.starts_with("memory: bginf-sp2 has 20.0 GiB free and needs 100.0 GiB; "),
            "{waiting_on}"
        );
        let (_, probe_id, _, operation) = heads.last_sent();
        assert!(
            matches!(operation, DeploymentOperation::Probe { .. }),
            "it may already be running"
        );
        assert!(probe_id.starts_with(PROBE_PREFIX));

        let bound = (control::FIT_WAIT_SECS as i64 + 1) * 1_000;
        tick(&db, &heads, &mut probes, 1_000 + bound).await.unwrap();
        let Phase::Failed { reason, .. } = phase(&db, DSV4F).await else {
            panic!("a start that never fits fails");
        };
        assert!(
            reason.contains("memory: bginf-sp2 has 20.0 GiB free and needs 100.0 GiB"),
            "{reason}"
        );
        assert!(heads.held().is_empty());
    }

    /// The load order on machines with batch work: hold (no new work), let
    /// the work finish until the drain's bound, then preempt only the checks
    /// whose measured memory is in the way, never the agent turn, naming the
    /// model and the memory; the start goes once the gang fits.
    #[tokio::test]
    async fn a_start_drains_its_machines_then_preempts_only_the_checks_in_its_way() {
        let db = sparks_db("inference-control-drain").await;
        let heads = FakeHeads::default();
        let mut probes = Probes::default();
        db.execute(
            "INSERT INTO inference_deployment_loads
               (deployment_id, recipe_id, requested_at, finished_at, outcome, load_ms, footprint_json)
             VALUES (?1, ?1, 0, 1, 'healthy', 1, ?2)",
            params![DSV4F, format!("{{\"{SP1}\":{},\"{SP2}\":{}}}", 100u64 << 30, 100u64 << 30)],
        )
        .await
        .unwrap();
        heads.attach(SP1, 60, 0);
        heads.attach(SP2, 110, 0);
        heads.run(SP1, "check-big", true, 45);
        heads.run(SP1, "check-small", true, 5);
        heads.run(SP1, "turn", false, 30);
        heads.run(SP2, "check-resident", true, 8);
        request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        assert_eq!(
            heads.held(),
            [SP1, SP2],
            "no new work from the moment it is asked for"
        );

        tick(&db, &heads, &mut probes, 2_000).await.unwrap();
        assert!(
            heads.preempted.lock().unwrap().is_empty(),
            "inside the bound"
        );
        let Phase::Reserving { waiting_on, .. } = phase(&db, DSV4F).await else {
            panic!("still reserving");
        };
        assert!(
            waiting_on
                .unwrap_or_default()
                .contains("may finish for 5 min more"),
            "the wait names the bound"
        );

        let past = 1_000 + (control::DRAIN_BOUND_SECS as i64) * 1_000;
        tick(&db, &heads, &mut probes, past).await.unwrap();
        let preempted = heads.preempted.lock().unwrap().clone();
        assert_eq!(
            preempted.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            ["check-big"],
            "40 GiB short on sp1: the one check that covers it; never the turn, never sp2's resident that fits"
        );
        let (_, why) = &preempted[0];
        assert_eq!(
            (
                why.deployment.as_str(),
                why.model.as_str(),
                why.executor.as_str()
            ),
            (DSV4F, DSV4F, SP1)
        );
        assert_eq!(
            (why.need_bytes, why.free_bytes, why.resident_bytes),
            (100 << 30, 60 << 30, 45 << 30)
        );
        assert!(
            heads
                .sent
                .lock()
                .unwrap()
                .iter()
                .all(|(_, _, _, op)| !matches!(op, DeploymentOperation::Start { .. })),
            "the start waits for the memory to come back"
        );

        // Its memory came back: the gang fits beside the turn and the
        // resident, and the start goes.
        heads
            .running
            .lock()
            .unwrap()
            .get_mut(SP1)
            .unwrap()
            .retain(|b| !b.preempted);
        heads.attach(SP1, 105, 0);
        tick(&db, &heads, &mut probes, past + 5_000).await.unwrap();
        assert!(matches!(
            heads.last_sent().3,
            DeploymentOperation::Start { .. }
        ));
        assert_eq!(
            heads.preempted.lock().unwrap().len(),
            1,
            "nothing else was stopped"
        );
    }

    #[tokio::test]
    async fn a_reserving_start_whose_model_already_answers_is_adopted() {
        let db = sparks_db("inference-control-adopt").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        on_report(
            &db,
            &heads,
            report(
                "probe-1",
                DeploymentPhaseReport::Healthy { ran_start: false },
            ),
            2_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Healthy {
                since_ms: 2_000,
                load_ms: None
            }
        );
    }

    #[tokio::test]
    async fn an_overlapping_start_and_an_unsendable_stop_are_refused_by_name() {
        let db = sparks_db("inference-control-refusals").await;
        let heads = FakeHeads::default();
        request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        let refusal = request(&db, &heads, "qwen36-fast", Request::Start, 1_001)
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            "Cairn cannot start qwen36-fast: bginf-sp1 is held by deployment dsv4f-tp2 (reserving); stop it first"
        );
        assert_eq!(
            request(&db, &heads, DSV4F, Request::Start, 1_002)
                .await
                .unwrap_err(),
            "Cairn did not start dsv4f-tp2: it is already starting"
        );
        assert!(request(&db, &heads, "nope", Request::Start, 1_003)
            .await
            .unwrap_err()
            .starts_with("there is no deployment nope"));

        // From reserving, a stop sends nothing.
        let said = request(&db, &heads, DSV4F, Request::Stop, 1_004)
            .await
            .unwrap();
        assert!(said.contains("nothing ran"), "{said}");
        assert!(heads.held().is_empty());

        *heads.refuse.lock().unwrap() = Some("executor bginf-sp1 is not attached".into());
        let refusal = request(&db, &heads, DSV4F, Request::Stop, 1_005)
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            "Cairn did not stop dsv4f-tp2: the stop could not be sent: executor bginf-sp1 is not attached"
        );
        assert_eq!(phase(&db, DSV4F).await.label(), "failed");
        assert!(heads.held().is_empty());
    }

    #[tokio::test]
    async fn a_stopped_model_back_after_a_power_cycle_is_held_again() {
        let db = sparks_db("inference-control-power-cycle").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        let mut probes = Probes::default();
        request(&db, &heads, DSV4F, Request::Stop, 1_000)
            .await
            .unwrap();
        let (_, stop_id, _, _) = heads.last_sent();
        on_report(
            &db,
            &heads,
            report(&stop_id, DeploymentPhaseReport::Stopped),
            2_000,
        )
        .await
        .unwrap();
        tick(&db, &heads, &mut probes, 3_000).await.unwrap();
        let (_, probe_id, _, operation) = heads.last_sent();
        assert!(matches!(operation, DeploymentOperation::Probe { .. }));
        on_report(
            &db,
            &heads,
            report(
                &probe_id,
                DeploymentPhaseReport::Healthy { ran_start: false },
            ),
            4_000,
        )
        .await
        .unwrap();
        assert_eq!(phase(&db, DSV4F).await.label(), "healthy");
        assert_eq!(heads.held(), [SP1, SP2]);
    }

    #[tokio::test]
    async fn deployments_cairn_never_touched_are_not_probed_or_held() {
        let db = sparks_db("inference-control-untouched").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        tick(&db, &heads, &mut Probes::default(), 1_000)
            .await
            .unwrap();
        assert!(heads.sent.lock().unwrap().is_empty());
        assert!(heads.held().is_empty());
    }
}
