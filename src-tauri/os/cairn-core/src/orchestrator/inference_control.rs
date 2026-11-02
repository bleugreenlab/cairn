//! The act side of the cairn-inference facet: the orch starts, stops and
//! health-checks deployments through their head executors, and records how
//! long each load took and how much GPU memory it settled at.
//!
//! The rules are the facet's (`cairn_inference::control`); this module is their
//! host. It keeps one row per deployment Cairn has acted on
//! (`inference_deployment_state`) and one per finished load
//! (`inference_deployment_loads`), and a single background task applies the
//! head's reports, probes deployments, watches a starting or healthy
//! deployment's GPU memory for its engine dying, records settled footprints
//! and fails a command that outlives its bound. A request itself only moves the stored
//! phase (and sends a stop, which needs no wait), inside one database
//! transaction, so a request and the task never act on two different readings
//! of a phase.
//!
//! What runs when is not decided here. Execution's placement
//! ([`crate::fleet::models`]) holds a running model as a resident on its
//! gang's machines: it asks for the start, drains the gang while the start is
//! reserving, and sends it ([`launch`]) once every machine has the room.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cairn_common::executor_protocol::{
    DeploymentOperation, DeploymentPhaseReport, DeploymentScript, ExecutorHealthSnapshot,
    MeasurementReading,
};
use cairn_db::turso::params;
use cairn_inference::control::{
    self, Allocation, AllocationWatch, LoadOutcome, Observation, Phase, Report, Request,
    START_TIMEOUT_SECS, STOPPED_HEALTH_WAIT_SECS, STOP_TIMEOUT_SECS,
};
use cairn_inference::render::{render, MachineAt};
use cairn_inference::{Deployment, DeploymentId, Inventory, MachineName};

use super::inference::load_inventory;
use super::Orchestrator;
use crate::fleet::inference::Preemption;
use crate::fleet::models::room::{gpu_allocated_bytes, RunningBatch};
use crate::fleet::pools::PoolMember;
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
        let mut discrete_free: Option<u64> = None;
        if let MeasurementReading::Measured { value: devices } = &snapshot.host.gpus.reading {
            for device in devices {
                if let MeasurementReading::Measured {
                    value:
                        cairn_common::executor_protocol::GpuMemory::Discrete {
                            used_bytes,
                            total_bytes,
                        },
                } = &device.memory
                {
                    *discrete_free.get_or_insert(0) += total_bytes.saturating_sub(*used_bytes);
                }
            }
        }
        let host_free = match &snapshot.machine.memory.reading {
            MeasurementReading::Measured { value } => Some(value.available_bytes),
            MeasurementReading::Unavailable { .. } => None,
        };
        Self {
            gpu_allocated_bytes: gpu_allocated_bytes(&snapshot.host),
            free_is_host_memory: discrete_free.is_none(),
            free_bytes: discrete_free.or(host_free),
        }
    }
}

/// What the act side and placement's model pass need of the fleet: sending to
/// a head, reading the attached machines and the batches on them, and stopping
/// one to make room.
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
    /// The batches running on these machines, by public name.
    fn running(&self, machines: &BTreeSet<String>) -> BTreeMap<String, Vec<RunningBatch>>;
    /// Stop one running batch to make room; false when it has already ended.
    fn preempt(&self, request_id: &str, attempt_id: &str, preemption: Preemption) -> bool;
    /// Each pooled machine's address on the fast link joining its pool, by
    /// public name, as placement detects the pools now.
    fn links(&self) -> BTreeMap<String, String>;
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

    fn links(&self) -> BTreeMap<String, String> {
        link_addresses(self.detected_pools().iter().flat_map(|pool| &pool.members))
    }
}

/// Each of `members`' addresses on the link joining its pool, by machine.
pub(crate) fn link_addresses<'a>(
    members: impl IntoIterator<Item = &'a PoolMember>,
) -> BTreeMap<String, String> {
    members
        .into_iter()
        .filter_map(|member| Some((member.machine.clone(), member.link_address?.to_string())))
        .collect()
}

/// `deployment`'s machines as the renderer reaches them: each one's address
/// from its machine record, and its fast-link address from `links`.
pub(crate) fn machines_at(
    inventory: &Inventory,
    deployment: &Deployment,
    links: &BTreeMap<String, String>,
) -> Vec<MachineAt> {
    deployment
        .machines
        .iter()
        .map(|name| MachineAt {
            name: name.clone(),
            address: inventory.address_of(name).map(str::to_string),
            link: links.get(name.as_str()).cloned(),
        })
        .collect()
}

/// One deployment's stored row.
#[derive(Debug, Clone, PartialEq)]
pub struct StateRow {
    pub phase: Phase,
    /// The start or stop its head is running, while one is. A deployment
    /// that became healthy while its start script was still running keeps the
    /// start's, so the script's own end can still be matched to it.
    pub command_id: Option<String>,
    pub updated_at: i64,
}

/// One finished load, newest first in [`load_history`].
#[derive(Debug, Clone, PartialEq)]
pub struct LoadRow {
    pub profile: String,
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
            "SELECT deployment_id, profile_id, requested_at, sent_at, finished_at, outcome, load_ms,
                    reason, footprint_json
               FROM inference_deployment_loads ORDER BY deployment_id, id DESC",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    LoadRow {
                        profile: row.text(1)?,
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
    let profile = deployment.profile.as_str().to_string();
    db.write(move |conn| {
        let (id, profile, reason) = (id.clone(), profile.clone(), reason.clone());
        Box::pin(async move {
            conn.execute(
                "INSERT INTO inference_deployment_loads
                   (deployment_id, profile_id, requested_at, sent_at, finished_at, outcome, load_ms, reason)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![id.as_str(), profile, requested_at, sent_at, now_ms, outcome, load_ms, reason],
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

/// Note on the load that made this healthy phase how its start script ended,
/// when it ended badly after health had answered.
async fn record_late_exit(
    db: &LocalDb,
    id: &DeploymentId,
    finished_at: i64,
    reason: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE inference_deployment_loads SET reason = ?3
          WHERE deployment_id = ?1 AND finished_at = ?2 AND outcome = 'healthy'",
        params![id.as_str(), finished_at, reason],
    )
    .await
    .map(|_| ())
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

/// Each deployment's phase as the act side holds it. A deployment with no row
/// is missing from the map: unknown, not stopped, since the act side has never
/// acted on it. A read that fails is an error, never an empty map, so a pass
/// never acts on phases it could not read.
pub(crate) async fn deployment_phases(
    db: &LocalDb,
) -> Result<BTreeMap<DeploymentId, Phase>, String> {
    Ok(load_states(db)
        .await?
        .into_iter()
        .map(|(id, row)| (id, row.phase))
        .collect())
}

/// What the head runs for one deployment.
struct Plan {
    start: String,
    stop: Option<String>,
    cleanup: Option<String>,
    health_url: String,
}

impl Plan {
    fn of(
        inventory: &Inventory,
        deployment: &Deployment,
        links: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let profile = inventory
            .profiles
            .get(&deployment.profile)
            .ok_or_else(|| format!("its profile {} is missing", deployment.profile))?;
        let machines = machines_at(inventory, deployment, links);
        let rendered = render(profile, deployment, &machines).map_err(|refusal| refusal.0)?;
        let address = inventory.address_of(&deployment.head).ok_or_else(|| {
            format!(
                "the head {}'s address is unknown, so its health URL cannot be formed",
                deployment.head
            )
        })?;
        Ok(Self {
            start: rendered.start,
            stop: rendered.stop,
            cleanup: rendered.cleanup,
            health_url: deployment.health_url(address),
        })
    }

    fn start_operation(&self) -> DeploymentOperation {
        DeploymentOperation::Start {
            script: self.start.clone(),
            timeout_secs: START_TIMEOUT_SECS,
            health_url: self.health_url.clone(),
            cleanup: self.cleanup.clone().map(|script| DeploymentScript {
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

/// Why Cairn could not render `deployment`'s start and stop for its head, or
/// `None` when it can: what a start would refuse, asked before anything is
/// stopped to make room for it.
pub(crate) fn render_problem(
    inventory: &Inventory,
    deployment: &Deployment,
    links: &BTreeMap<String, String>,
) -> Option<String> {
    Plan::of(inventory, deployment, links).err()
}

/// Which stored command an observation must belong to.
#[derive(Clone)]
enum Matching {
    /// The task's own observation of the phase.
    Any,
    /// The head's report on this start or stop.
    Command(String),
}

/// Apply an observation and record the load it finished. `command` is the
/// command id the phase holds afterwards when it is one in flight; `None`
/// keeps the one it held.
///
/// A probe that finds a deployment Cairn has never acted on answering gives it
/// a healthy row, so placement adopts it as a resident instead of placing work
/// on memory a model it did not know of holds.
async fn apply(
    db: &LocalDb,
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
            let Some(row) = current else {
                return (observation == Observation::Probed { healthy: true }).then_some((
                    Phase::Healthy {
                        since_ms: now_ms,
                        load_ms: None,
                    },
                    None,
                ));
            };
            match &matching {
                Matching::Any => {}
                Matching::Command(expected) => {
                    if row.command_id.as_deref() != Some(expected.as_str()) {
                        return None;
                    }
                }
            }
            let next = control::observe(&row.phase, observation.clone(), now_ms)?;
            let command_id = if next.command_in_flight() {
                command.clone().or_else(|| row.command_id.clone())
            } else if control::keeps_start_command(&row.phase, &next) {
                row.command_id.clone()
            } else {
                None
            };
            Some((next, command_id))
        }),
    )
    .await?;
    let Some((before, after)) = changed else {
        return Ok(None);
    };
    settle(db, inventory, id, before.as_ref(), &after.phase, now_ms).await?;
    Ok(Some(after.phase))
}

/// What follows any change of phase: the load it finished, and a log.
async fn settle(
    db: &LocalDb,
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
    Ok(())
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
    let plan = Plan::of(&inventory, &deployment, &heads.links())
        .map_err(|why| format!("Cairn cannot {} {id}: {why}", request.as_str()))?;
    if request == Request::Stop && plan.stop.is_none() {
        return Err(format!(
            "Cairn cannot stop {id}: its profile {} has no stop command",
            deployment.profile
        ));
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
    settle(db, &inventory, &id, before.as_ref(), &after.phase, now_ms).await?;
    match &after.phase {
        Phase::Reserving { .. } => Ok(format!(
            "Deployment {id}: start requested. Placement reserves its footprint on its machines ({}) now, and sends the start to its head {} once each machine has the memory it needs. Read cairn://inference/deployments/{id} for its phase.",
            machine_list(&deployment),
            deployment.head
        )),
        Phase::Stopping { .. } => {
            let operation = plan.stop_operation().expect("checked above");
            if let Err(why) = heads.send(deployment.head.as_str(), &stop_command, id.as_str(), operation) {
                let reason = format!("the stop could not be sent: {why}");
                apply(
                    db,
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
                "Deployment {id}: stop sent to its head {}; its memory on {} returns once its health check stops answering. Read cairn://inference/deployments/{id} for its phase.",
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
            Matching::Any,
        )
    } else {
        let phase = report_of(report.phase);
        if note_late_start_failure(db, &id, &report.command_id, &phase, now_ms).await? {
            return Ok(());
        }
        (
            Observation::Report(phase),
            Matching::Command(report.command_id),
        )
    };
    apply(db, &inventory, &id, observation, matching, None, now_ms).await?;
    Ok(())
}

/// A start script that ended badly after its deployment was already healthy:
/// noted on the load, and the row lets go of the start, while the phase stays
/// as health has it. Returns whether the report was that.
async fn note_late_start_failure(
    db: &LocalDb,
    id: &DeploymentId,
    command_id: &str,
    report: &Report,
    now_ms: i64,
) -> Result<bool, String> {
    let noted: Arc<Mutex<Option<(i64, String)>>> = Arc::default();
    {
        let noted = Arc::clone(&noted);
        let command_id = command_id.to_string();
        let report = report.clone();
        transition(
            db,
            id,
            now_ms,
            Arc::new(move |current| {
                let row = current?;
                if row.command_id.as_deref() != Some(command_id.as_str()) {
                    return None;
                }
                let reason = control::late_start_failure(&row.phase, &report)?;
                *noted.lock().unwrap() = Some((row.phase.since_ms(), reason.to_string()));
                Some((row.phase.clone(), None))
            }),
        )
        .await?;
    }
    let Some((healthy_since, reason)) = noted.lock().unwrap().take() else {
        return Ok(false);
    };
    log::warn!(
        "inference: deployment {id}'s start script ended badly after it was healthy; it stays healthy while its health check answers: {reason}"
    );
    record_late_exit(db, id, healthy_since, &reason).await?;
    Ok(true)
}

/// The task's own state between ticks: when each deployment was last probed,
/// how far each starting or healthy deployment's GPU memory has climbed, and
/// which deployments Cairn never acted on were already probed since their head
/// attached.
#[derive(Debug, Default)]
pub(crate) struct Probes {
    last: HashMap<DeploymentId, i64>,
    allocations: HashMap<DeploymentId, AllocationWatch>,
    unknown_probed: HashSet<DeploymentId>,
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
) -> bool {
    if !readings.contains_key(deployment.head.as_str()) || !probes.due(&deployment.id, now_ms) {
        return false;
    }
    let command = new_command_id(PROBE_PREFIX.trim_end_matches('-'));
    match heads.send(
        deployment.head.as_str(),
        &command,
        deployment.id.as_str(),
        DeploymentOperation::Probe {
            health_url: plan.health_url.clone(),
        },
    ) {
        Ok(()) => true,
        Err(why) => {
            log::debug!("inference: could not probe {}: {why}", deployment.id);
            false
        }
    }
}

/// Each gang machine's GPU memory against the deployment's footprint there:
/// the newest measured load's, or else what its profile states.
fn allocations_of(
    inventory: &Inventory,
    deployment: &Deployment,
    readings: &BTreeMap<String, MachineReading>,
    history: &BTreeMap<DeploymentId, Vec<LoadRow>>,
) -> BTreeMap<MachineName, Allocation> {
    let footprint = last_footprint(history.get(&deployment.id));
    let stated = inventory
        .profiles
        .get(&deployment.profile)
        .and_then(control::stated_need);
    deployment
        .machines
        .iter()
        .map(|machine| {
            (
                machine.clone(),
                Allocation {
                    held_bytes: readings
                        .get(machine.as_str())
                        .and_then(|reading| reading.gpu_allocated_bytes),
                    footprint_bytes: footprint.get(machine.as_str()).copied().or(stated),
                },
            )
        })
        .collect()
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
        let plan = match Plan::of(&inventory, deployment, &heads.links()) {
            Ok(plan) => plan,
            Err(why) => {
                if matches!(row.phase, Phase::Reserving { .. }) {
                    apply(
                        db,
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
        if !matches!(row.phase, Phase::Starting { .. } | Phase::Healthy { .. }) {
            probes.allocations.remove(id);
        }
        match &row.phase {
            // Placement drains the gang and launches the start. A model that
            // already answers while it waits is adopted as it stands, rather
            // than waiting for memory it is itself using.
            Phase::Reserving { .. } => {
                probe(heads, probes, deployment, &plan, &readings, now_ms);
            }
            Phase::Starting { .. } | Phase::Healthy { .. } => {
                // Probed while starting too: health answering is what makes
                // it healthy, whether or not the start script has ended.
                probe(heads, probes, deployment, &plan, &readings, now_ms);
                let allocations = allocations_of(&inventory, deployment, &readings, &history);
                let lost = probes
                    .allocations
                    .entry(id.clone())
                    .or_default()
                    .observe(&allocations);
                if let Some(reason) = lost {
                    probes.allocations.remove(id);
                    apply(
                        db,
                        &inventory,
                        id,
                        Observation::Lost { reason },
                        Matching::Any,
                        None,
                        now_ms,
                    )
                    .await?;
                    continue;
                }
                let Phase::Healthy { since_ms, .. } = &row.phase else {
                    continue;
                };
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
            Phase::Stopping { .. } => {}
        }
    }
    Ok(())
}

/// Probe every deployment Cairn has never acted on, once each time its head
/// attaches: one that answers gets a healthy row, which placement adopts as a
/// resident, so what is left running from before Cairn acted on it is held,
/// not ignored. One that does not answer is not asked again until its head is
/// seen detached and attached again (a power cycle, say), when a model may
/// have come up with it.
pub(crate) async fn probe_unknown(
    db: &LocalDb,
    heads: &dyn Heads,
    probes: &mut Probes,
    now_ms: i64,
) -> Result<(), String> {
    let inventory = load_inventory(db).await?;
    let states = load_states(db).await?;
    let readings = heads.readings();
    probes
        .unknown_probed
        .retain(|id| inventory.deployments.contains_key(id));
    for deployment in inventory.deployments.values() {
        let id = &deployment.id;
        if states.contains_key(id) || !readings.contains_key(deployment.head.as_str()) {
            probes.unknown_probed.remove(id);
            continue;
        }
        if probes.unknown_probed.contains(id) {
            continue;
        }
        if let Ok(plan) = Plan::of(&inventory, deployment, &heads.links()) {
            if probe(heads, probes, deployment, &plan, &readings, now_ms) {
                probes.unknown_probed.insert(id.clone());
            }
        }
    }
    Ok(())
}

/// Record what a reserving start waits on, in placement's words.
pub(crate) async fn note_waiting(
    db: &LocalDb,
    id: &DeploymentId,
    on: String,
    now_ms: i64,
) -> Result<(), String> {
    let inventory = load_inventory(db).await?;
    apply(
        db,
        &inventory,
        id,
        Observation::Waiting { on },
        Matching::Any,
        None,
        now_ms,
    )
    .await
    .map(|_| ())
}

/// Send a reserving start to its head: placement found every machine of its
/// gang with the room it needs. A start that cannot be sent fails, naming why.
pub(crate) async fn launch(
    db: &LocalDb,
    heads: &dyn Heads,
    id: &DeploymentId,
    now_ms: i64,
) -> Result<(), String> {
    let inventory = load_inventory(db).await?;
    let Some(deployment) = inventory.deployments.get(id) else {
        return Err(format!("there is no deployment {id}"));
    };
    let plan = match Plan::of(&inventory, deployment, &heads.links()) {
        Ok(plan) => plan,
        Err(why) => {
            apply(
                db,
                &inventory,
                id,
                Observation::NotSent { reason: why },
                Matching::Any,
                None,
                now_ms,
            )
            .await?;
            return Ok(());
        }
    };
    let command = new_command_id("start");
    let sent = apply(
        db,
        &inventory,
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
            &inventory,
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

/// Act on deployments for the life of the process: apply every report, tick
/// every [`TICK`], and wake placement's model pass whenever a phase may have
/// moved. Every deployment Cairn has never acted on is probed once each time
/// its head attaches, so a model left running is adopted.
pub fn spawn_deployment_control(orch: Orchestrator) {
    tokio::spawn(async move {
        let mut reports = orch.fleet.subscribe_deployment_reports();
        let mut probes = Probes::default();
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let heads: &dyn Heads = orch.fleet.as_ref();
        loop {
            tokio::select! {
                Some(report) = reports.recv() => {
                    if let Err(error) = on_report(&orch.db.local, report, now_ms()).await {
                        log::warn!("inference: a deployment report was not applied: {error}");
                    }
                    orch.fleet.wake_model_pass();
                }
                _ = ticks.tick() => {
                    let now = now_ms();
                    if let Err(error) = probe_unknown(&orch.db.local, heads, &mut probes, now).await {
                        log::debug!("inference: could not probe unknown deployments: {error}");
                    }
                    if let Err(error) = tick(&orch.db.local, heads, &mut probes, now).await {
                        log::warn!("inference: a deployment control pass failed: {error}");
                    }
                }
            }
        }
    });
}

/// The act side's test fixtures, shared with placement's model tests.
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use cairn_inference::{
        Engine, InferenceProfile, MachineRecord, ModelRef, RecordSource, Requirements,
    };

    #[derive(Default)]
    pub(crate) struct FakeHeads {
        pub(crate) sent: Mutex<Vec<(String, String, String, DeploymentOperation)>>,
        readings: Mutex<BTreeMap<String, MachineReading>>,
        pub(crate) refuse: Mutex<Option<String>>,
        pub(crate) running: Mutex<BTreeMap<String, Vec<RunningBatch>>>,
        pub(crate) preempted: Mutex<Vec<(String, Preemption)>>,
        pub(crate) links: Mutex<BTreeMap<String, String>>,
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
        fn links(&self) -> BTreeMap<String, String> {
            self.links.lock().unwrap().clone()
        }
    }

    impl FakeHeads {
        pub(crate) fn attach(&self, name: &str, free_gib: u64, allocated_gib: u64) {
            self.readings.lock().unwrap().insert(
                name.into(),
                MachineReading {
                    gpu_allocated_bytes: Some(allocated_gib << 30),
                    free_bytes: Some(free_gib << 30),
                    free_is_host_memory: true,
                },
            );
        }
        pub(crate) fn detach(&self, name: &str) {
            self.readings.lock().unwrap().remove(name);
        }
        pub(crate) fn run(
            &self,
            name: &str,
            request_id: &str,
            restartable: bool,
            resident_gib: u64,
        ) {
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
        pub(crate) fn last_sent(&self) -> (String, String, String, DeploymentOperation) {
            self.sent
                .lock()
                .unwrap()
                .last()
                .cloned()
                .expect("something was sent")
        }
        pub(crate) fn starts(&self) -> Vec<String> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, _, _, op)| matches!(op, DeploymentOperation::Start { .. }))
                .map(|(_, _, deployment, _)| deployment.clone())
                .collect()
        }
    }

    pub(crate) const SP1: &str = "bginf-sp1";
    pub(crate) const SP2: &str = "bginf-sp2";
    pub(crate) const DSV4F: &str = "dsv4f-tp2";
    pub(crate) const QWEN: &str = "qwen36-fast";

    fn profile(id: &str, machines: u32) -> InferenceProfile {
        InferenceProfile {
            id: id.into(),
            source: RecordSource::Cairn,
            model: ModelRef {
                name: id.into(),
                weights: Vec::new(),
            },
            served_name: id.into(),
            upstream_id: None,
            verbs: None,
            modalities: None,
            quantization: None,
            origin: Default::default(),
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
            notes: None,
        }
    }

    fn deployment(id: &str, machines: &[&str]) -> Deployment {
        Deployment {
            id: id.into(),
            source: RecordSource::Cairn,
            profile: id.into(),
            machines: machines.iter().map(|m| (*m).into()).collect(),
            head: machines[0].into(),
            api_port: 8888,
            api_path: "/v1".into(),
            health_path: "/v1/models".into(),
        }
    }

    /// sp1 and sp2 with their addresses: `dsv4f-tp2` spans both and
    /// `qwen36-fast` is on sp1. Each profile's model is named by its id.
    pub(crate) async fn sparks_db(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        let mut inventory = Inventory::default();
        inventory.insert_model(cairn_inference::Model::named(DSV4F));
        inventory.insert_model(cairn_inference::Model::named(QWEN));
        inventory.insert_profile(profile(DSV4F, 2));
        inventory.insert_deployment(deployment(DSV4F, &[SP1, SP2]));
        inventory.insert_profile(profile(QWEN, 1));
        inventory.insert_deployment(deployment(QWEN, &[SP1]));
        for (name, address) in [(SP1, "10.20.20.40"), (SP2, "10.20.20.41")] {
            inventory.insert_machine(MachineRecord {
                name: name.into(),
                source: RecordSource::Cairn,
                address: address.into(),
            });
        }
        inventory.validate().expect("the test inventory is valid");
        let rows: Vec<(&str, String, String)> = inventory
            .models
            .values()
            .map(|m| {
                (
                    "inference_models",
                    m.name.clone(),
                    serde_json::to_string(m).unwrap(),
                )
            })
            .chain(inventory.profiles.values().map(|r| {
                (
                    "inference_profiles",
                    r.id.to_string(),
                    serde_json::to_string(r).unwrap(),
                )
            }))
            .chain(inventory.deployments.values().map(|d| {
                (
                    "inference_deployments",
                    d.id.to_string(),
                    serde_json::to_string(d).unwrap(),
                )
            }))
            .chain(inventory.machines.values().map(|m| {
                (
                    "inference_machines",
                    m.name.to_string(),
                    serde_json::to_string(m).unwrap(),
                )
            }))
            .collect();
        for (table, id, body) in rows {
            db.execute(
                format!(
                    "INSERT INTO {table} (id, source, body_json, created_at, updated_at)
                     VALUES (?1, 'cairn', ?2, 0, 0)"
                ),
                params![id, body],
            )
            .await
            .unwrap();
        }
        db
    }

    pub(crate) async fn phase(db: &LocalDb, id: &str) -> Phase {
        load_states(db).await.unwrap()[&DeploymentId::from(id)]
            .phase
            .clone()
    }

    pub(crate) fn report(
        command_id: &str,
        phase: DeploymentPhaseReport,
    ) -> DeploymentReportReceived {
        DeploymentReportReceived {
            executor: SP1.into(),
            command_id: command_id.into(),
            deployment: DSV4F.into(),
            phase,
        }
    }

    #[tokio::test]
    async fn a_load_and_stop_runs_through_the_head() {
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
        assert!(
            heads.sent.lock().unwrap().is_empty(),
            "nothing is sent until placement launches it"
        );

        launch(&db, &heads, &DSV4F.into(), 2_000).await.unwrap();
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
            report(&start_id, DeploymentPhaseReport::Starting),
            3_000,
        )
        .await
        .unwrap();
        on_report(
            &db,
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
        assert!(
            phase(&db, DSV4F).await.reserves(),
            "a stopping model still occupies its machines"
        );
        on_report(
            &db,
            report(&stop_id, DeploymentPhaseReport::Stopped),
            430_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Stopped { since_ms: 430_000 }
        );
    }

    #[tokio::test]
    async fn a_failed_start_reports_why_and_records_the_load() {
        let db = sparks_db("inference-control-failed").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        launch(&db, &heads, &DSV4F.into(), 2_000).await.unwrap();
        let (_, start_id, _, _) = heads.last_sent();
        on_report(
            &db,
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
        let failed = phase(&db, DSV4F).await;
        assert_eq!(
            failed,
            Phase::Failed {
                at_ms: 5_000,
                reason: "the start command exited 127: not found".into()
            }
        );
        assert!(!failed.reserves(), "a failed start frees its machines");
        let loads = &load_history(&db).await.unwrap()[&DeploymentId::from(DSV4F)];
        assert_eq!(
            (loads[0].healthy, loads[0].reason.as_deref()),
            (false, Some("the start command exited 127: not found"))
        );
    }

    /// Start dsv4f-tp2 on two attached Sparks and return the start's id.
    async fn started(db: &LocalDb, heads: &FakeHeads) -> String {
        request(db, heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        launch(db, heads, &DSV4F.into(), 2_000).await.unwrap();
        let (_, start_id, _, operation) = heads.last_sent();
        assert!(matches!(operation, DeploymentOperation::Start { .. }));
        on_report(
            db,
            report(&start_id, DeploymentPhaseReport::Starting),
            3_000,
        )
        .await
        .unwrap();
        start_id
    }

    async fn insert_footprint(db: &LocalDb, gib: u64) {
        db.execute(
            "INSERT INTO inference_deployment_loads
               (deployment_id, profile_id, requested_at, finished_at, outcome, load_ms, footprint_json)
             VALUES (?1, ?1, 0, 1, 'healthy', 1, ?2)",
            params![DSV4F, format!("{{\"{SP1}\":{},\"{SP2}\":{}}}", gib << 30, gib << 30)],
        )
        .await
        .unwrap();
    }

    /// The dspark script warms the model up before it exits: the model is
    /// healthy when its probe answers, the load is timed to that, and the
    /// script's own late failure is noted without unloading a working model.
    #[tokio::test]
    async fn health_answering_before_the_start_script_exits_is_healthy() {
        let db = sparks_db("inference-control-health-first").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        heads.attach(SP2, 110, 0);
        let mut probes = Probes::default();
        let start_id = started(&db, &heads).await;

        tick(&db, &heads, &mut probes, 40_000).await.unwrap();
        let (_, probe_id, _, operation) = heads.last_sent();
        assert!(
            matches!(operation, DeploymentOperation::Probe { .. }),
            "a starting deployment is probed"
        );
        on_report(
            &db,
            report(
                &probe_id,
                DeploymentPhaseReport::Healthy { ran_start: false },
            ),
            403_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Healthy {
                since_ms: 403_000,
                load_ms: Some(401_000)
            }
        );
        let states = load_states(&db).await.unwrap();
        assert_eq!(
            states[&DeploymentId::from(DSV4F)].command_id.as_deref(),
            Some(start_id.as_str()),
            "the start script is still running"
        );

        // The head's own sighting of health, then the script's bad exit.
        on_report(
            &db,
            report(
                &start_id,
                DeploymentPhaseReport::Healthy { ran_start: true },
            ),
            404_000,
        )
        .await
        .unwrap();
        let late = "the start command exited 1 after its health check had answered: warm-up request timed out";
        on_report(
            &db,
            report(
                &start_id,
                DeploymentPhaseReport::Failed {
                    reason: late.into(),
                },
            ),
            450_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Healthy {
                since_ms: 403_000,
                load_ms: Some(401_000)
            },
            "health decides, not the script"
        );
        let loads = &load_history(&db).await.unwrap()[&DeploymentId::from(DSV4F)];
        assert_eq!(loads.len(), 1);
        assert_eq!(
            (
                loads[0].healthy,
                loads[0].load_ms,
                loads[0].reason.as_deref()
            ),
            (true, Some(401_000), Some(late))
        );
    }

    #[tokio::test]
    async fn a_start_script_that_never_exits_is_healthy_and_stays_so() {
        let db = sparks_db("inference-control-foreground").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        heads.attach(SP2, 110, 0);
        let mut probes = Probes::default();
        let start_id = started(&db, &heads).await;
        on_report(
            &db,
            report(
                &start_id,
                DeploymentPhaseReport::Healthy { ran_start: true },
            ),
            302_000,
        )
        .await
        .unwrap();
        let past_every_bound = 2_000 + (control::START_REPORT_BOUND_SECS as i64 + 60) * 1_000;
        tick(&db, &heads, &mut probes, past_every_bound)
            .await
            .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Healthy {
                since_ms: 302_000,
                load_ms: Some(300_000)
            }
        );
    }

    #[tokio::test]
    async fn an_engine_that_stops_answering_fails_and_frees_its_machines() {
        let db = sparks_db("inference-control-health-lost").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        heads.attach(SP2, 110, 0);
        let mut probes = Probes::default();
        let start_id = started(&db, &heads).await;
        on_report(
            &db,
            report(
                &start_id,
                DeploymentPhaseReport::Healthy { ran_start: true },
            ),
            400_000,
        )
        .await
        .unwrap();
        tick(&db, &heads, &mut probes, 440_000).await.unwrap();
        let (_, probe_id, _, operation) = heads.last_sent();
        assert!(matches!(operation, DeploymentOperation::Probe { .. }));
        on_report(
            &db,
            report(&probe_id, DeploymentPhaseReport::Stopped),
            441_000,
        )
        .await
        .unwrap();
        let failed = phase(&db, DSV4F).await;
        assert_eq!(
            failed,
            Phase::Failed {
                at_ms: 441_000,
                reason: "it stopped answering its health check".into()
            }
        );
        assert!(!failed.reserves(), "both Sparks are released");
    }

    /// bginf's agent killed the engine on 10-31 while Cairn read `starting`
    /// and held both Sparks: GPU memory that falls from the footprint to
    /// nothing now fails the start, and a healthy model, on its own.
    #[tokio::test]
    async fn gpu_memory_collapsing_under_a_start_or_a_healthy_model_fails_it() {
        let db = sparks_db("inference-control-collapse").await;
        let heads = FakeHeads::default();
        insert_footprint(&db, 100).await;
        heads.attach(SP1, 110, 0);
        heads.attach(SP2, 110, 0);
        let mut probes = Probes::default();
        started(&db, &heads).await;

        heads.attach(SP1, 28, 82);
        heads.attach(SP2, 28, 82);
        tick(&db, &heads, &mut probes, 10_000).await.unwrap();
        assert_eq!(phase(&db, DSV4F).await.label(), "starting");
        heads.attach(SP1, 110, 0);
        tick(&db, &heads, &mut probes, 15_000).await.unwrap();
        let Phase::Failed { at_ms, reason } = phase(&db, DSV4F).await else {
            panic!("the start failed");
        };
        assert_eq!(at_ms, 15_000);
        assert_eq!(
            reason,
            "its engine is gone: bginf-sp1's GPU memory fell to 0.0 GiB after reaching 82.0 GiB (its footprint is 100.0 GiB)"
        );
        let loads = &load_history(&db).await.unwrap()[&DeploymentId::from(DSV4F)];
        assert_eq!(
            (loads[0].healthy, loads[0].reason.as_deref()),
            (false, Some(reason.as_str()))
        );

        // Healthy, then the worker on sp2 dies.
        heads.attach(SP2, 110, 0);
        request(&db, &heads, DSV4F, Request::Start, 20_000)
            .await
            .unwrap();
        launch(&db, &heads, &DSV4F.into(), 21_000).await.unwrap();
        let (_, start_id, _, _) = heads.last_sent();
        on_report(
            &db,
            report(
                &start_id,
                DeploymentPhaseReport::Healthy { ran_start: true },
            ),
            100_000,
        )
        .await
        .unwrap();
        heads.attach(SP1, 10, 100);
        heads.attach(SP2, 10, 100);
        tick(&db, &heads, &mut probes, 101_000).await.unwrap();
        assert_eq!(phase(&db, DSV4F).await.label(), "healthy");
        heads.attach(SP2, 110, 1);
        tick(&db, &heads, &mut probes, 106_000).await.unwrap();
        let Phase::Failed { reason, .. } = phase(&db, DSV4F).await else {
            panic!("the healthy model failed");
        };
        assert!(
            reason.contains("bginf-sp2's GPU memory fell to 1.0 GiB"),
            "{reason}"
        );
    }

    #[tokio::test]
    async fn a_reserving_start_records_what_it_waits_on_and_gives_up_at_its_bound() {
        let db = sparks_db("inference-control-wait").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        let mut probes = Probes::default();
        request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
        note_waiting(
            &db,
            &DSV4F.into(),
            "memory: bginf-sp2 has 20.0 GiB free and needs 100.0 GiB".into(),
            2_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Reserving {
                since_ms: 1_000,
                waiting_on: Some("memory: bginf-sp2 has 20.0 GiB free and needs 100.0 GiB".into())
            }
        );
        tick(&db, &heads, &mut probes, 3_000).await.unwrap();
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
    async fn a_second_start_and_an_unsendable_stop_are_refused_by_name() {
        let db = sparks_db("inference-control-refusals").await;
        let heads = FakeHeads::default();
        request(&db, &heads, DSV4F, Request::Start, 1_000)
            .await
            .unwrap();
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

        *heads.refuse.lock().unwrap() = Some("executor bginf-sp1 is not attached".into());
        let refusal = request(&db, &heads, DSV4F, Request::Stop, 1_005)
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            "Cairn did not stop dsv4f-tp2: the stop could not be sent: executor bginf-sp1 is not attached"
        );
        assert_eq!(phase(&db, DSV4F).await.label(), "failed");
    }

    #[tokio::test]
    async fn a_stopped_model_back_after_a_power_cycle_is_healthy_again() {
        let db = sparks_db("inference-control-power-cycle").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        let mut probes = Probes::default();
        request(&db, &heads, DSV4F, Request::Stop, 1_000)
            .await
            .unwrap();
        let (_, stop_id, _, _) = heads.last_sent();
        on_report(&db, report(&stop_id, DeploymentPhaseReport::Stopped), 2_000)
            .await
            .unwrap();
        tick(&db, &heads, &mut probes, 3_000).await.unwrap();
        let (_, probe_id, _, operation) = heads.last_sent();
        assert!(matches!(operation, DeploymentOperation::Probe { .. }));
        on_report(
            &db,
            report(
                &probe_id,
                DeploymentPhaseReport::Healthy { ran_start: false },
            ),
            4_000,
        )
        .await
        .unwrap();
        assert_eq!(phase(&db, DSV4F).await.label(), "healthy");
    }

    /// A model left running before Cairn acted on it is found by one probe and
    /// given a healthy row, which placement adopts; one that does not answer
    /// is left alone until its head attaches again, and neither is ever sent a
    /// start or a stop for it.
    #[tokio::test]
    async fn a_deployment_cairn_never_touched_is_probed_once_and_adopted_if_it_answers() {
        let db = sparks_db("inference-control-untouched").await;
        let heads = FakeHeads::default();
        heads.attach(SP1, 110, 0);
        let mut probes = Probes::default();
        tick(&db, &heads, &mut probes, 1_000).await.unwrap();
        assert!(
            heads.sent.lock().unwrap().is_empty(),
            "the tick probes only rows"
        );

        probe_unknown(&db, &heads, &mut probes, 1_000)
            .await
            .unwrap();
        let probed: Vec<String> = heads
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, _, op)| matches!(op, DeploymentOperation::Probe { .. }))
            .map(|(_, _, deployment, _)| deployment.clone())
            .collect();
        assert_eq!(probed, [DSV4F, QWEN]);
        probe_unknown(&db, &heads, &mut probes, 2_000)
            .await
            .unwrap();
        assert_eq!(
            heads.sent.lock().unwrap().len(),
            2,
            "not again within the interval"
        );

        let (_, probe_id, _, _) = heads.sent.lock().unwrap()[0].clone();
        on_report(
            &db,
            report(
                &probe_id,
                DeploymentPhaseReport::Healthy { ran_start: false },
            ),
            3_000,
        )
        .await
        .unwrap();
        assert_eq!(
            phase(&db, DSV4F).await,
            Phase::Healthy {
                since_ms: 3_000,
                load_ms: None
            }
        );
        on_report(
            &db,
            DeploymentReportReceived {
                deployment: QWEN.into(),
                ..report("probe-2", DeploymentPhaseReport::Stopped)
            },
            3_000,
        )
        .await
        .unwrap();
        assert!(
            !load_states(&db)
                .await
                .unwrap()
                .contains_key(&DeploymentId::from(QWEN)),
            "one that does not answer stays unknown"
        );
        let probes_of = |heads: &FakeHeads, deployment: &str| {
            heads
                .sent
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, _, d, op)| {
                    d == deployment && matches!(op, DeploymentOperation::Probe { .. })
                })
                .count()
        };
        probe_unknown(&db, &heads, &mut probes, 1_000 + 10 * PROBE_INTERVAL_MS)
            .await
            .unwrap();
        assert_eq!(
            probes_of(&heads, QWEN),
            1,
            "a silent deployment is not asked again while its head stays attached"
        );
        heads.detach(SP1);
        probe_unknown(&db, &heads, &mut probes, 1_000 + 11 * PROBE_INTERVAL_MS)
            .await
            .unwrap();
        heads.attach(SP1, 110, 0);
        probe_unknown(&db, &heads, &mut probes, 1_000 + 12 * PROBE_INTERVAL_MS)
            .await
            .unwrap();
        assert_eq!(
            probes_of(&heads, QWEN),
            2,
            "its head attaching again asks once more"
        );
        assert!(heads.starts().is_empty());
    }
}
