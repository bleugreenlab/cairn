//! Placement runs local models (tenets 1336, 1337).
//!
//! A running model is a resident placement holds on a gang of machines inside
//! one detected pool ([`super::pools`]). A request for a model is waiting work
//! that leases its resident, acquiring it when it is absent. While a resident
//! loads, its machines keep its unmet footprint off-limits to batches; once it
//! is healthy, batches run beside it as measured room allows. When work is
//! refused for room a resident holds and nothing else takes it, placement
//! evicts the resident if it can give way: no lease, no pin, past its minimum
//! residency and its idle window. A pool with no batch work for its idle
//! window places its warm model. A pin is a standing lease.
//!
//! [`rules`] decides, [`room`] is the per-machine arithmetic placement checks
//! a batch against and the drain a loading model runs on its gang, and this
//! module hosts both: one pass, woken by a request, a pin, a deployment
//! report, a room refusal, or a short timer, reads the act side's phases, the
//! requests and the fleet, runs the rules, asks the act side to stop and start
//! what they decided, drains each reserving gang and launches it once it fits,
//! and publishes each machine's residents for placement to weigh. The act side
//! ([`crate::orchestrator::inference_control`]) only runs the commands.
//!
//! The residents are not stored: they are rebuilt from the act side's phases,
//! which are, so a restart adopts what runs and takes nothing away.

pub mod requests;
pub mod room;
pub mod rules;
pub mod settings;
pub mod store;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use cairn_common::executor_protocol::{MachineModelResident, MachineRole};
use cairn_inference::control::{self as act, Phase, Request as Act};
use cairn_inference::{DeploymentId, Inventory};
use cairn_models::types::catalog::VerbKind;
use serde::Serialize;

use self::requests::{choose_deployment, describe_waiting, Hold, HoldCause, Request, RequestId};
use self::requests::{RequestState, Wait};
use self::room::{drain, Drain, GangMachine, Room};
use self::rules::{
    Acquirable, Activity, Departure, Input, Left, Pin, Plan, Preference, State, Why,
};
use self::settings::{EffectiveSettings, MeasuredLoad, Ms};
use self::store::StoredPin;
use super::pools::{Pool, PoolKey, PoolMember};
use super::{executor_public_name, Fleet};
use crate::fleet::inference::Preemption;
use crate::orchestrator::inference::load_inventory;
use crate::orchestrator::inference_control::{
    self, deployment_phases, load_history, Heads, LoadRow, MachineReading, Need,
};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// How often the pass runs when nothing wakes it. It stands in for a timer
/// set to the next window's end, and keeps every window to within seconds.
pub const PASS_INTERVAL: Duration = Duration::from_secs(5);
/// Departures kept for the pages.
const DEPARTURES_KEPT: usize = 20;

fn now_ms() -> Ms {
    crate::fleet::unix_time_ms() as Ms
}

/// What the last pass knew, for the pages and the doors.
#[derive(Debug, Clone, Default)]
pub(crate) struct Book {
    state: State,
    waits: BTreeMap<RequestId, Wait>,
    holds: BTreeMap<DeploymentId, Option<Hold>>,
    /// Newest last.
    departures: Vec<Departure>,
    catalog: BTreeMap<DeploymentId, Acquirable>,
    pools: Vec<Pool>,
    settings: BTreeMap<PoolKey, EffectiveSettings>,
    footprints: BTreeMap<DeploymentId, BTreeMap<String, u64>>,
    passed_at: Ms,
}

/// Placement's live account of the model residents, inside [`Fleet`].
#[derive(Default)]
pub(crate) struct ModelPlacement {
    /// Residents by machine public name, as placement weighs them. Replaced
    /// whole by each pass; attach stamps a new connection from it.
    by_machine: Mutex<HashMap<String, Vec<MachineModelResident>>>,
    /// Machines that refused work for room a resident holds, where nothing
    /// else took the work, since the pass last read them.
    pub(in crate::fleet) refused: Mutex<BTreeSet<String>>,
    wake: tokio::sync::Notify,
    /// Serialises the passes and every door that changes what they read.
    book: tokio::sync::Mutex<Book>,
}

impl Fleet {
    /// Wake placement's model pass now.
    pub fn wake_model_pass(&self) {
        self.models.wake.notify_one();
    }

    /// Machines (by public name) that refused work only for room a model
    /// resident holds, where nothing else took the work. Placement evicts what
    /// may give way there on its next pass, which this wakes.
    pub(crate) fn note_room_refusal(&self, machines: impl IntoIterator<Item = String>) {
        let mut refused = self.models.refused.lock().unwrap();
        let before = refused.len();
        refused.extend(machines);
        if refused.len() != before {
            drop(refused);
            self.wake_model_pass();
        }
    }

    /// Every machine's model residents, by public name.
    pub fn model_residents(&self) -> HashMap<String, Vec<MachineModelResident>> {
        self.models.by_machine.lock().unwrap().clone()
    }

    /// The residents of the machine with this public name, read under the
    /// connection table's lock by attach.
    pub(super) fn model_residents_named(&self, public_name: &str) -> Vec<MachineModelResident> {
        self.models
            .by_machine
            .lock()
            .unwrap()
            .get(public_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Replace every machine's residents and restamp each live connection.
    /// Work waiting on a machine's room is woken when its residents change, so
    /// it is offered the room at once.
    pub(in crate::fleet) fn set_model_residents(
        &self,
        by_machine: HashMap<String, Vec<MachineModelResident>>,
    ) {
        let mut changed = false;
        {
            let mut connections = self.lock_connections();
            for entry in connections.values_mut() {
                let residents = by_machine
                    .get(&executor_public_name(entry))
                    .cloned()
                    .unwrap_or_default();
                changed |= entry.model_residents != residents;
                entry.model_residents = residents;
            }
            *self.models.by_machine.lock().unwrap() = by_machine;
        }
        if changed {
            self.connection_ready.notify_waiters();
        }
    }
}

/// What the pass needs of the fleet beyond the act side's [`Heads`].
pub(crate) trait Host: Send + Sync {
    fn heads(&self) -> &dyn Heads;
    fn models(&self) -> &ModelPlacement;
    /// The pools detected now.
    fn pools(&self) -> Vec<Pool>;
    /// Each attached machine's role, by public name.
    fn roles(&self) -> BTreeMap<String, MachineRole>;
    /// Every machine's residents, as placement weighs them.
    fn publish(&self, by_machine: HashMap<String, Vec<MachineModelResident>>);
}

impl Host for Fleet {
    fn heads(&self) -> &dyn Heads {
        self
    }

    fn models(&self) -> &ModelPlacement {
        &self.models
    }

    fn pools(&self) -> Vec<Pool> {
        self.detected_pools()
    }

    fn roles(&self) -> BTreeMap<String, MachineRole> {
        self.lock_connections()
            .values()
            .map(|entry| (executor_public_name(entry), entry.machine_role))
            .collect()
    }

    fn publish(&self, by_machine: HashMap<String, Vec<MachineModelResident>>) {
        self.set_model_residents(by_machine);
    }
}

/// Each pool's windows: the idle override, else derived from what a reload
/// costs now, the slowest of its deployments' reference loads
/// ([`inference_control::reference_load`]).
fn pool_settings(
    pools: &[Pool],
    catalog: &BTreeMap<DeploymentId, Acquirable>,
    history: &BTreeMap<DeploymentId, Vec<LoadRow>>,
    preferences: &BTreeMap<PoolKey, Preference>,
) -> BTreeMap<PoolKey, EffectiveSettings> {
    pools
        .iter()
        .map(|pool| {
            let measured = catalog
                .values()
                .filter(|a| a.pool.as_ref() == Some(&pool.key))
                .filter_map(|a| inference_control::reference_load(history.get(&a.deployment)?))
                .filter_map(|load| {
                    Some(MeasuredLoad {
                        secs: load.load_ms?.div_ceil(1_000),
                        cold: load.cold,
                    })
                })
                .max_by_key(|load| load.secs);
            let idle = preferences
                .get(&pool.key)
                .and_then(|p| p.idle_override_secs);
            (pool.key.clone(), EffectiveSettings::derive(idle, measured))
        })
        .collect()
}

/// A deployment's need on each of its machines: its last settled load, else
/// what its profile states, else what is predicted from what the profile runs,
/// a share of memory taken of what each machine reports
/// ([`inference_control::need_of`]). Refused when nothing predicts one.
fn need_of(
    inventory: &Inventory,
    id: &DeploymentId,
    history: &BTreeMap<DeploymentId, Vec<LoadRow>>,
    readings: &BTreeMap<String, MachineReading>,
) -> Result<Need, String> {
    let deployment = inventory
        .deployments
        .get(id)
        .ok_or_else(|| format!("there is no deployment {id}"))?;
    inference_control::need_of(inventory, deployment, history.get(id), |machine| {
        readings.get(machine)?.total_bytes
    })
}

/// Whether `id`'s footprint is in the memory batches use.
fn unified(inventory: &Inventory, id: &DeploymentId) -> bool {
    inventory
        .deployments
        .get(id)
        .and_then(|d| inventory.profiles.get(&d.profile))
        .is_none_or(act::shares_memory_with_batches)
}

/// Every machine's residents, as placement weighs them. Only what the pass
/// decided goes in, never the clock, so a pass that changed nothing publishes
/// the same residents and wakes no waiting work.
fn machine_residents(
    inventory: &Inventory,
    plan: &Plan,
    phases: &BTreeMap<DeploymentId, Phase>,
    footprints: &BTreeMap<DeploymentId, BTreeMap<String, u64>>,
    pins: &BTreeMap<DeploymentId, Pin>,
) -> HashMap<String, Vec<MachineModelResident>> {
    let (state, holds) = (&plan.state, &plan.holds);
    let mut by_machine: HashMap<String, Vec<MachineModelResident>> = HashMap::new();
    for resident in state.residents.values() {
        let id = &resident.deployment;
        let hold = holds.get(id);
        for machine in &resident.machines {
            by_machine
                .entry(machine.clone())
                .or_default()
                .push(MachineModelResident {
                    model: resident.model.clone(),
                    deployment: id.to_string(),
                    phase: phases.get(id).map_or("reserving", Phase::label).to_string(),
                    held_bytes: footprints.get(id).and_then(|f| f.get(machine)).copied(),
                    unified: unified(inventory, id),
                    pinned: pins.contains_key(id),
                    leased: matches!(
                        hold,
                        Some(Some(Hold {
                            because: HoldCause::Wanted { .. },
                            ..
                        }))
                    ),
                    evictable_at_unix_ms: plan
                        .evictable_at
                        .get(id)
                        .copied()
                        .flatten()
                        .map(|ms| u64::try_from(ms).unwrap_or(0)),
                });
        }
    }
    by_machine
}

/// Drain each reserving resident's gang, preempting what the drain chooses,
/// and launch the start once the gang fits. A start whose need nothing
/// predicts is refused, naming what is missing, rather than drained blind.
#[allow(clippy::too_many_arguments)]
async fn drive(
    db: &LocalDb,
    host: &dyn Host,
    inventory: &Inventory,
    state: &State,
    phases: &BTreeMap<DeploymentId, Phase>,
    needs: &BTreeMap<DeploymentId, Result<Need, String>>,
    readings: &BTreeMap<String, MachineReading>,
    now: Ms,
) {
    let heads = host.heads();
    for resident in state.residents.values() {
        let id = &resident.deployment;
        let Some(Phase::Reserving { since_ms, .. }) = phases.get(id) else {
            continue;
        };
        let Some(deployment) = inventory.deployments.get(id) else {
            continue;
        };
        let need = match needs.get(id) {
            Some(Ok(need)) => need,
            Some(Err(why)) => {
                let reason = format!("its memory need is unknown: {why}");
                if let Err(error) = inference_control::refuse(db, id, reason, now).await {
                    log::warn!("models: could not refuse {id}'s start: {error}");
                }
                continue;
            }
            None => continue,
        };
        let on = if !readings.contains_key(deployment.head.as_str()) {
            Some(format!("its head {} is not attached", deployment.head))
        } else {
            let mut running = heads.running(&resident.machines);
            let gang: BTreeMap<String, GangMachine> = resident
                .machines
                .iter()
                .map(|machine| {
                    let reading = readings.get(machine);
                    (
                        machine.clone(),
                        GangMachine {
                            room: Room {
                                free_bytes: reading.and_then(|r| r.free_bytes),
                                need_bytes: need.per_machine.get(machine).copied(),
                            },
                            shared_with_batches: reading.is_some_and(|r| r.free_is_host_memory),
                            batches: running.remove(machine).unwrap_or_default(),
                        },
                    )
                })
                .collect();
            match drain(&gang, need.predicted(), *since_ms, now) {
                Drain::Start => None,
                Drain::Wait(on) => Some(on),
                Drain::Preempt { batches, waiting } => {
                    for batch in batches {
                        heads.preempt(
                            &batch.request_id,
                            &batch.attempt_id,
                            Preemption {
                                deployment: id.to_string(),
                                model: resident.model.clone(),
                                executor: batch.machine.clone(),
                                need_bytes: batch.need_bytes,
                                free_bytes: batch.free_bytes,
                                resident_bytes: batch.resident_bytes,
                            },
                        );
                    }
                    Some(waiting)
                }
            }
        };
        let acted = match on {
            None => inference_control::launch(db, heads, id, now).await,
            Some(on) => inference_control::note_waiting(db, id, on, now).await,
        };
        if let Err(error) = acted {
            log::warn!("models: could not drive {id}'s start: {error}");
        }
    }
}

/// The book, after one pass if none has run yet, so a door used right after
/// startup sees what placement adopted.
async fn passed_book<'a>(
    db: &LocalDb,
    host: &'a dyn Host,
    now: Ms,
) -> Result<tokio::sync::MutexGuard<'a, Book>, String> {
    let mut book = host.models().book.lock().await;
    if book.passed_at == 0 {
        pass_locked(db, host, &mut book, now).await?;
    }
    Ok(book)
}

fn catalog_now(
    inventory: &Inventory,
    host: &dyn Host,
) -> (Vec<Pool>, BTreeMap<DeploymentId, Acquirable>) {
    let pools = host.pools();
    let catalog = rules::catalog(inventory, &pools, &host.roles());
    (pools, catalog)
}

/// One pass, holding the book.
async fn pass_locked(
    db: &LocalDb,
    host: &dyn Host,
    book: &mut Book,
    now: Ms,
) -> Result<(), String> {
    let heads = host.heads();
    let inventory = load_inventory(db).await?;
    let phases = deployment_phases(db).await?;
    let history = load_history(db).await?;
    let requests = store::load_requests(db).await?;
    let stored_pins = store::load_pins(db).await?;
    let preferences = store::load_preferences(db).await?;
    let (pools, catalog) = catalog_now(&inventory, host);
    let readings = heads.readings();
    let attached: BTreeSet<String> = readings.keys().cloned().collect();
    let activity = Activity {
        running: heads
            .running(&attached)
            .into_iter()
            .filter(|(_, batches)| !batches.is_empty())
            .map(|(machine, _)| machine)
            .collect(),
        refused: std::mem::take(&mut *host.models().refused.lock().unwrap()),
    };
    let settings = pool_settings(&pools, &catalog, &history, &preferences);
    let pins: BTreeMap<DeploymentId, Pin> = stored_pins
        .iter()
        .map(|(id, stored)| (id.clone(), stored.pin.clone()))
        .collect();
    let plan = rules::plan(
        &book.state,
        Input {
            now,
            pools: &pools,
            catalog: &catalog,
            phases: &phases,
            requests: &requests,
            pins: &pins,
            preferences: &preferences,
            settings: &settings,
            activity: &activity,
        },
    );
    store::settle(db, &plan.settled).await?;
    for departure in &plan.departures {
        log::info!("models: {}", departure.describe(now));
    }
    for (id, action) in plan
        .stops
        .iter()
        .map(|id| (id, Act::Stop))
        .chain(plan.starts.iter().map(|id| (id, Act::Start)))
    {
        match inference_control::request(db, heads, id.as_str(), action, now).await {
            Ok(said) => log::info!("models: {said}"),
            Err(why) => log::debug!("models: {why}; asked again on the next pass"),
        }
    }
    let phases = deployment_phases(db).await?;
    let needs: BTreeMap<DeploymentId, Result<Need, String>> = plan
        .state
        .residents
        .keys()
        .map(|id| (id.clone(), need_of(&inventory, id, &history, &readings)))
        .collect();
    let footprints: BTreeMap<DeploymentId, BTreeMap<String, u64>> = needs
        .iter()
        .map(|(id, need)| {
            let per_machine = need.as_ref().map(|need| need.per_machine.clone());
            (id.clone(), per_machine.unwrap_or_default())
        })
        .collect();
    drive(
        db,
        host,
        &inventory,
        &plan.state,
        &phases,
        &needs,
        &readings,
        now,
    )
    .await;
    let phases = deployment_phases(db).await?;
    host.publish(machine_residents(
        &inventory,
        &plan,
        &phases,
        &footprints,
        &pins,
    ));
    store::prune_answered(db, now).await?;

    book.departures.extend(plan.departures.iter().cloned());
    let excess = book.departures.len().saturating_sub(DEPARTURES_KEPT);
    book.departures.drain(..excess);
    book.state = plan.state;
    book.waits = plan.waits;
    book.holds = plan.holds;
    book.catalog = catalog;
    book.pools = pools;
    book.settings = settings;
    book.footprints = footprints;
    book.passed_at = now;
    Ok(())
}

/// One pass of placement's model rules.
pub(crate) async fn pass(db: &LocalDb, host: &dyn Host, now: Ms) -> Result<(), String> {
    let mut book = host.models().book.lock().await;
    pass_locked(db, host, &mut book, now).await
}

/// Run the pass for the life of the process: whenever something wakes it (a
/// request, a pin, a deployment report, a room refusal) and every
/// [`PASS_INTERVAL`] otherwise.
pub fn spawn(orch: Orchestrator) {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(PASS_INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = orch.fleet.models.wake.notified() => {}
                _ = ticks.tick() => {}
            }
            if let Err(error) = pass(&orch.db.local, orch.fleet.as_ref(), now_ms()).await {
                log::warn!("models: a placement pass failed: {error}");
            }
        }
    });
}

/// A caller's request for a model, as the store holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHandle {
    pub id: RequestId,
    pub pool: PoolKey,
    pub deployment: DeploymentId,
    pub model: String,
}

/// A request's state now, with what it waits on while it waits.
#[derive(Debug, Clone)]
pub struct RequestStatus {
    pub request: Request,
    pub wait: Option<Wait>,
    pub now: Ms,
}

impl RequestStatus {
    pub fn describe(&self) -> String {
        let request = &self.request;
        match (&request.state, &self.wait) {
            (RequestState::Waiting, Some(wait)) => describe_waiting(request, wait, self.now),
            (RequestState::Waiting, None) => format!(
                "{} (deployment {}) on pool {} is waiting for placement's next pass",
                request.model, request.deployment, request.pool
            ),
            (RequestState::Ready { .. }, _) => format!(
                "{} (deployment {}) on pool {} is healthy",
                request.model, request.deployment, request.pool
            ),
            (RequestState::Failed { reason, .. }, _) => format!("failed: {reason}"),
            (RequestState::Cancelled { .. }, _) => "cancelled".to_string(),
        }
    }
}

/// Ask for the model whose real name is `model`, on behalf of `requester`,
/// to serve `verb` when the caller is serving one (a deployment whose profile
/// does not serve it is never chosen), narrowed to `pool` when one is named.
/// Placement decides at once, so the returned handle's status already says
/// what the request waits on.
pub async fn request(
    orch: &Orchestrator,
    model: &str,
    verb: Option<VerbKind>,
    requester: &str,
    pool: Option<&PoolKey>,
) -> Result<RequestHandle, String> {
    request_with(
        &orch.db.local,
        orch.fleet.as_ref(),
        model,
        verb,
        requester,
        pool,
        now_ms(),
    )
    .await
}

pub(crate) async fn request_with(
    db: &LocalDb,
    host: &dyn Host,
    model: &str,
    verb: Option<VerbKind>,
    requester: &str,
    pool: Option<&PoolKey>,
    now: Ms,
) -> Result<RequestHandle, String> {
    let mut book = passed_book(db, host, now).await?;
    let inventory = load_inventory(db).await?;
    let (_, catalog) = catalog_now(&inventory, host);
    let deployment = choose_deployment(&catalog, &book.state.residents, model, verb, pool)?;
    let chosen = &catalog[&deployment];
    let pool = book
        .state
        .residents
        .get(&deployment)
        .map(|resident| resident.pool.clone())
        .or_else(|| chosen.pool.clone())
        .ok_or_else(|| format!("{deployment} is in no pool"))?;
    let model = chosen.model.clone();
    let id = store::insert_request(db, &pool, &deployment, &model, requester, now).await?;
    pass_locked(db, host, &mut book, now).await?;
    Ok(RequestHandle {
        id,
        pool,
        deployment,
        model,
    })
}

pub async fn request_status(orch: &Orchestrator, id: RequestId) -> Result<RequestStatus, String> {
    status_with(&orch.db.local, orch.fleet.as_ref(), id).await
}

pub(crate) async fn status_with(
    db: &LocalDb,
    host: &dyn Host,
    id: RequestId,
) -> Result<RequestStatus, String> {
    let request = store::load_requests(db)
        .await?
        .into_iter()
        .find(|r| r.id == id)
        .ok_or_else(|| format!("there is no request #{id}"))?;
    let book = host.models().book.lock().await;
    Ok(RequestStatus {
        wait: book.waits.get(&id).cloned(),
        request,
        now: book.passed_at.max(now_ms()),
    })
}

impl RequestHandle {
    pub async fn status(&self, orch: &Orchestrator) -> Result<RequestStatus, String> {
        request_status(orch, self.id).await
    }
}

/// Cancel a waiting request.
pub async fn cancel(orch: &Orchestrator, id: RequestId, by: &str) -> Result<String, String> {
    let said = store::cancel_request(&orch.db.local, id, by, now_ms()).await?;
    orch.fleet.wake_model_pass();
    Ok(said)
}

/// What a pin or a release names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinTarget {
    /// A model by its real name, on `pool` when one is named.
    Model { name: String, pool: Option<PoolKey> },
    /// One deployment: the form cairn/5730's verification pins with.
    Deployment(DeploymentId),
    /// Every pin on a pool; a release only.
    Pool(PoolKey),
}

/// What a release that names no pin is refused with.
pub const NOTHING_PINNED: &str = "nothing it names is pinned";

fn pinned_by(model: &str, holder: &str) -> String {
    format!("{model} is pinned by {holder}; only they can release it, or revoke it explicitly")
}

/// Pin a model for `holder`: a standing lease that acquires it if it is
/// absent, displacing unpinned residents on its machines, and keeps it
/// against eviction until released. A pin whose machines another holder's
/// pin holds is refused, naming them; the same holder pinning again moves its
/// pin.
pub async fn pin(orch: &Orchestrator, target: PinTarget, holder: &str) -> Result<String, String> {
    pin_with(
        &orch.db.local,
        orch.fleet.as_ref(),
        target,
        holder,
        now_ms(),
    )
    .await
}

/// Pin a model for `holder`, the operator, who outranks every other holder:
/// another holder's pin on its machines is revoked, naming them, rather than
/// refusing.
pub async fn pin_outranking(
    orch: &Orchestrator,
    target: PinTarget,
    holder: &str,
) -> Result<String, String> {
    pin_inner(
        &orch.db.local,
        orch.fleet.as_ref(),
        target,
        holder,
        true,
        now_ms(),
    )
    .await
}

pub(crate) async fn pin_with(
    db: &LocalDb,
    host: &dyn Host,
    target: PinTarget,
    holder: &str,
    now: Ms,
) -> Result<String, String> {
    pin_inner(db, host, target, holder, false, now).await
}

pub(crate) async fn pin_inner(
    db: &LocalDb,
    host: &dyn Host,
    target: PinTarget,
    holder: &str,
    outrank: bool,
    now: Ms,
) -> Result<String, String> {
    let mut book = passed_book(db, host, now).await?;
    let inventory = load_inventory(db).await?;
    let (_, catalog) = catalog_now(&inventory, host);
    let deployment = match &target {
        PinTarget::Model { name, pool } => {
            choose_deployment(&catalog, &book.state.residents, name, None, pool.as_ref())?
        }
        PinTarget::Deployment(id) => {
            let acquirable = catalog.get(id).ok_or_else(|| {
                format!("there is no deployment {id}; cairn://inference/deployments lists them")
            })?;
            if let Some(why) = &acquirable.refused {
                return Err(format!("{id} cannot run: {why}"));
            }
            id.clone()
        }
        PinTarget::Pool(pool) => {
            return Err(format!(
                "name the model to pin on pool {pool}; a pool is named only to release its pins"
            ))
        }
    };
    let chosen = catalog[&deployment].clone();
    let pins = store::load_pins(db).await?;
    for (other, stored) in &pins {
        let overlaps = other == &deployment
            || catalog
                .get(other)
                .is_some_and(|a| !a.machines.is_disjoint(&chosen.machines));
        if !overlaps {
            continue;
        }
        if stored.pin.holder != holder && !outrank {
            return Err(pinned_by(&stored.model, &stored.pin.holder));
        }
    }
    let mut revoked = Vec::new();
    for (other, stored) in &pins {
        let overlaps = other == &deployment
            || catalog
                .get(other)
                .is_some_and(|a| !a.machines.is_disjoint(&chosen.machines));
        if !overlaps {
            continue;
        }
        if stored.pin.holder != holder {
            store::delete_pin(db, other).await?;
            revoked.push(format!(
                "revoked {}'s pin on {} (deployment {other})",
                stored.pin.holder, stored.model
            ));
        } else if other != &deployment {
            store::delete_pin(db, other).await?;
        }
    }
    store::save_pin(
        db,
        &deployment,
        &StoredPin {
            model: chosen.model.clone(),
            pin: Pin {
                holder: holder.to_string(),
                at: now,
            },
        },
    )
    .await?;
    pass_locked(db, host, &mut book, now).await?;
    revoked.push(format!(
        "pinned {} (deployment {deployment}) on pool {} for {holder}; it stays resident and is never evicted for room until released",
        chosen.model,
        chosen.pool.map(|p| p.to_string()).unwrap_or_default()
    ));
    Ok(revoked.join("\n"))
}

/// Release `holder`'s pin on `target`, or with `revoke` anyone's. The model
/// stays resident and the rules carry on from it, its residency counted from
/// the pin.
pub async fn release(
    orch: &Orchestrator,
    target: PinTarget,
    holder: &str,
    revoke: bool,
) -> Result<String, String> {
    release_with(
        &orch.db.local,
        orch.fleet.as_ref(),
        target,
        holder,
        revoke,
        now_ms(),
    )
    .await
}

pub(crate) async fn release_with(
    db: &LocalDb,
    host: &dyn Host,
    target: PinTarget,
    holder: &str,
    revoke: bool,
    now: Ms,
) -> Result<String, String> {
    let mut book = host.models().book.lock().await;
    let inventory = load_inventory(db).await?;
    let (_, catalog) = catalog_now(&inventory, host);
    let pins = store::load_pins(db).await?;
    let matching: Vec<(&DeploymentId, &StoredPin)> = pins
        .iter()
        .filter(|(id, stored)| match &target {
            PinTarget::Deployment(wanted) => *id == wanted,
            PinTarget::Model { name, pool } => {
                stored.model.eq_ignore_ascii_case(name)
                    && pool.as_ref().is_none_or(|pool| {
                        catalog.get(*id).and_then(|a| a.pool.as_ref()) == Some(pool)
                    })
            }
            PinTarget::Pool(pool) => {
                catalog.get(*id).and_then(|a| a.pool.as_ref()) == Some(pool)
                    || book
                        .state
                        .residents
                        .get(*id)
                        .is_some_and(|r| &r.pool == pool)
            }
        })
        .collect();
    if matching.is_empty() {
        return Err(NOTHING_PINNED.to_string());
    }
    if !revoke {
        if let Some((_, stored)) = matching.iter().find(|(_, s)| s.pin.holder != holder) {
            return Err(pinned_by(&stored.model, &stored.pin.holder));
        }
    }
    let mut lines = Vec::new();
    for (id, stored) in matching {
        store::delete_pin(db, id).await?;
        if let Some(resident) = book.state.residents.get_mut(id) {
            if resident.healthy_since.is_some() {
                resident.healthy_since = Some(stored.pin.at);
            }
            resident.why = Why::PinReleased {
                holder: stored.pin.holder.clone(),
            };
        }
        lines.push(format!(
            "{} {}'s pin on {} (deployment {id}); it stays resident, its residency counted from the pin",
            if revoke { "revoked" } else { "released" },
            stored.pin.holder,
            stored.model
        ));
    }
    pass_locked(db, host, &mut book, now).await?;
    Ok(lines.join("\n"))
}

/// Set a pool's warm model (`Some(None)` clears it) and its idle window
/// override (`Some(None)` derives it again).
pub async fn set_preference(
    orch: &Orchestrator,
    pool: &PoolKey,
    warm: Option<Option<String>>,
    idle_secs: Option<Option<u64>>,
) -> Result<String, String> {
    preference_with(
        &orch.db.local,
        orch.fleet.as_ref(),
        pool,
        warm,
        idle_secs,
        now_ms(),
    )
    .await
}

pub(crate) async fn preference_with(
    db: &LocalDb,
    host: &dyn Host,
    pool: &PoolKey,
    warm: Option<Option<String>>,
    idle_secs: Option<Option<u64>>,
    now: Ms,
) -> Result<String, String> {
    let mut book = host.models().book.lock().await;
    let inventory = load_inventory(db).await?;
    let (pools, catalog) = catalog_now(&inventory, host);
    if !pools.iter().any(|p| &p.key == pool) {
        return Err(format!(
            "there is no pool {pool} now; cairn://inference/pools lists the pools placement detects"
        ));
    }
    let mut preferences = store::load_preferences(db).await?;
    let preference = preferences.entry(pool.clone()).or_default();
    let mut lines = Vec::new();
    if let Some(warm) = warm {
        if let Some(name) = &warm {
            let Some(found) = catalog
                .values()
                .find(|a| a.pool.as_ref() == Some(pool) && a.model.eq_ignore_ascii_case(name))
            else {
                let deployable: BTreeSet<&str> = catalog
                    .values()
                    .filter(|a| a.pool.as_ref() == Some(pool))
                    .map(|a| a.model.as_str())
                    .collect();
                return Err(format!(
                    "no deployment of '{name}' runs on pool {pool}; it runs {}",
                    if deployable.is_empty() {
                        "none".to_string()
                    } else {
                        deployable.into_iter().collect::<Vec<_>>().join(", ")
                    }
                ));
            };
            preference.warm_model = Some(found.model.clone());
            lines.push(format!(
                "pool {pool}'s warm model is {}: it is placed when the pool has had no batch work for its idle window",
                found.model
            ));
        } else {
            preference.warm_model = None;
            lines.push(format!("pool {pool} has no warm model"));
        }
    }
    if let Some(idle) = idle_secs {
        preference.idle_override_secs = idle;
        let settings = pool_settings(
            &pools,
            &catalog,
            &load_history(db).await?,
            &BTreeMap::from([(pool.clone(), preference.clone())]),
        );
        if let Some(settings) = settings.get(pool) {
            lines.push(format!(
                "pool {pool}: idle window {}; minimum residency {}; load bound {}",
                settings.idle, settings.min_residency, settings.load_bound
            ));
        }
    }
    if lines.is_empty() {
        lines.push(format!("pool {pool} unchanged"));
    }
    store::save_preference(db, pool, preference, now).await?;
    pass_locked(db, host, &mut book, now).await?;
    Ok(lines.join("\n"))
}

/// Unload `deployment` for `by` now: revoke its pin whoever holds it, cancel
/// the requests still waiting for it, and stop it. A pool's warm model comes
/// back after the pool's idle window, as the warm rule says.
pub async fn unload(
    orch: &Orchestrator,
    deployment: &DeploymentId,
    by: &str,
) -> Result<String, String> {
    unload_with(
        &orch.db.local,
        orch.fleet.as_ref(),
        deployment,
        by,
        now_ms(),
    )
    .await
}

pub(crate) async fn unload_with(
    db: &LocalDb,
    host: &dyn Host,
    deployment: &DeploymentId,
    by: &str,
    now: Ms,
) -> Result<String, String> {
    let mut lines = Vec::new();
    if let Some(stored) = store::load_pins(db).await?.get(deployment) {
        store::delete_pin(db, deployment).await?;
        lines.push(format!(
            "revoked {}'s pin on {} (deployment {deployment})",
            stored.pin.holder, stored.model
        ));
    }
    for request in store::load_requests(db).await? {
        if &request.deployment == deployment && request.state == RequestState::Waiting {
            lines.push(store::cancel_request(db, request.id, by, now).await?);
        }
    }
    match by_hand(db, host, deployment.as_str(), by, false, now).await {
        Ok(said) => lines.push(said),
        Err(why) if lines.is_empty() => return Err(why),
        Err(_) => lines.push(format!("{deployment} was not running")),
    }
    Ok(lines.join("\n"))
}

/// Start a deployment by hand: placement takes it as a resident, so its room
/// is reserved and the rules carry on from it. Refused while another resident
/// holds one of its machines.
pub async fn start_by_hand(orch: &Orchestrator, id: &str, by: &str) -> Result<String, String> {
    by_hand(&orch.db.local, orch.fleet.as_ref(), id, by, true, now_ms()).await
}

/// Stop a deployment by hand: placement lets it go and stops it. Refused
/// while it is pinned, naming the holder.
pub async fn stop_by_hand(orch: &Orchestrator, id: &str, by: &str) -> Result<String, String> {
    by_hand(&orch.db.local, orch.fleet.as_ref(), id, by, false, now_ms()).await
}

pub(crate) async fn by_hand(
    db: &LocalDb,
    host: &dyn Host,
    id: &str,
    by: &str,
    start: bool,
    now: Ms,
) -> Result<String, String> {
    let mut book = passed_book(db, host, now).await?;
    let inventory = load_inventory(db).await?;
    let (_, catalog) = catalog_now(&inventory, host);
    let id = DeploymentId::from(id);
    let acquirable = catalog
        .get(&id)
        .ok_or_else(|| {
            format!("there is no deployment {id}; cairn://inference/deployments lists them")
        })?
        .clone();
    if start {
        if book.state.residents.contains_key(&id) {
            return Ok(format!(
                "Deployment {id} is already resident; cairn://inference/deployments/{id} shows its phase."
            ));
        }
        let pool = match (&acquirable.pool, &acquirable.refused) {
            (Some(pool), _) => pool.clone(),
            (None, why) => {
                return Err(format!(
                    "Cairn cannot start {id}: {}",
                    why.as_deref().unwrap_or("it is in no detected pool")
                ))
            }
        };
        if let Some(other) = book
            .state
            .residents
            .values()
            .find(|r| !r.machines.is_disjoint(&acquirable.machines))
        {
            return Err(format!(
                "Cairn cannot start {id}: {} (deployment {}) is resident on its machines; stop it first",
                other.model, other.deployment
            ));
        }
        book.state.released.remove(&id);
        book.state.residents.insert(
            id.clone(),
            rules::Resident::chosen(&acquirable, pool, Why::Started { by: by.to_string() }, now),
        );
        pass_locked(db, host, &mut book, now).await?;
        return Ok(format!(
            "Deployment {id}: started by {by}. Placement holds it resident, reserves its footprint on {} and sends the start to its head once each machine has the room. Read cairn://inference/deployments/{id} for its phase.",
            acquirable.machines.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if let Some(stored) = store::load_pins(db).await?.get(&id) {
        return Err(format!(
            "Cairn did not stop {id}: {} is pinned by {}; release or revoke the pin first",
            stored.model, stored.pin.holder
        ));
    }
    let phases = deployment_phases(db).await?;
    let occupies = phases.get(&id).is_some_and(Phase::reserves);
    match book.state.residents.remove(&id) {
        Some(resident) => {
            book.departures.push(Departure::of(
                &resident,
                Left::Stopped { by: by.to_string() },
                now,
            ));
            book.state.released.insert(id.clone());
        }
        None if occupies => {
            book.state.released.insert(id.clone());
        }
        None => {
            return Err(format!(
                "Cairn did not stop {id}: it is not running; cairn://inference/deployments/{id} shows its phase"
            ))
        }
    }
    pass_locked(db, host, &mut book, now).await?;
    Ok(format!(
        "Deployment {id}: stopped by {by}. Placement let it go and asks its head to stop it; a request for it acquires it again. Read cairn://inference/deployments/{id} for its phase."
    ))
}

/// When `deployment`'s current load came healthy, as placement holds it.
pub async fn healthy_since(orch: &Orchestrator, deployment: &DeploymentId) -> Option<Ms> {
    orch.fleet
        .models
        .book
        .lock()
        .await
        .state
        .residents
        .get(deployment)
        .and_then(|resident| resident.healthy_since)
}

/// One resident, for the pages and the Inference page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResidentView {
    pub deployment: DeploymentId,
    pub model: String,
    pub pool: PoolKey,
    pub machines: BTreeSet<String>,
    /// The act side's phase; `None` before it has been asked.
    pub phase: Option<Phase>,
    pub why: Why,
    /// `why` in words.
    pub why_said: String,
    pub chosen_at: Ms,
    pub loading_since: Option<Ms>,
    pub healthy_since: Option<Ms>,
    /// Why it cannot give its machines up yet; `None` when it can.
    pub hold: Option<Hold>,
    pub pin: Option<Pin>,
    /// Its footprint per machine, as placement reserves it.
    pub footprint: BTreeMap<String, u64>,
    /// Whether that footprint is the memory batches use.
    pub unified: bool,
}

/// One detected pool, for the pages and the Inference page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolView {
    pub key: PoolKey,
    pub members: Vec<PoolMember>,
    pub rate_gbps: Option<u32>,
    pub settings: EffectiveSettings,
    pub warm_model: Option<String>,
    pub idle_override_secs: Option<u64>,
    /// The last time batch work ran on, or was refused room on, its machines.
    pub idle_since: Option<Ms>,
    pub residents: Vec<ResidentView>,
    /// The models deployable here, by real name.
    pub models: Vec<String>,
}

/// Where a deployment may run on the pools detected now, or why it may not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentPlacement {
    pub deployment: DeploymentId,
    pub model: String,
    pub pool: Option<PoolKey>,
    pub refused: Option<String>,
}

/// One request, with what it waits on in words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestView {
    pub request: Request,
    pub wait: Option<Wait>,
    pub said: String,
}

/// Everything placement knows about local models, as one serialisable
/// value: the pools with their residents, windows, warm models and pins,
/// where each deployment may run or why not, the requests and the recent
/// departures. The Inference page (cairn/5729) builds against it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelsView {
    pub now: Ms,
    /// When the pass last ran.
    pub passed_at: Ms,
    pub pools: Vec<PoolView>,
    /// Residents on machines that form no detected pool now (a member
    /// detached): held as they are until they stop.
    pub stranded: Vec<ResidentView>,
    pub deployments: Vec<DeploymentPlacement>,
    pub requests: Vec<RequestView>,
    /// Newest first.
    pub departures: Vec<Departure>,
}

impl ModelsView {
    pub fn resident(&self, deployment: &DeploymentId) -> Option<&ResidentView> {
        self.pools
            .iter()
            .flat_map(|pool| &pool.residents)
            .chain(&self.stranded)
            .find(|resident| &resident.deployment == deployment)
    }

    pub fn placement(&self, deployment: &DeploymentId) -> Option<&DeploymentPlacement> {
        self.deployments
            .iter()
            .find(|placement| &placement.deployment == deployment)
    }
}

pub async fn view(orch: &Orchestrator) -> Result<ModelsView, String> {
    view_with(&orch.db.local, orch.fleet.as_ref()).await
}

pub(crate) async fn view_with(db: &LocalDb, host: &dyn Host) -> Result<ModelsView, String> {
    let inventory = load_inventory(db).await?;
    let phases = deployment_phases(db).await?;
    let requests = store::load_requests(db).await?;
    let pins = store::load_pins(db).await?;
    let preferences = store::load_preferences(db).await?;
    let book = host.models().book.lock().await.clone();
    let now = now_ms().max(book.passed_at);
    let resident_view = |resident: &rules::Resident| ResidentView {
        deployment: resident.deployment.clone(),
        model: resident.model.clone(),
        pool: resident.pool.clone(),
        machines: resident.machines.clone(),
        phase: phases.get(&resident.deployment).cloned(),
        why: resident.why.clone(),
        why_said: resident.why.describe(resident.chosen_at, now),
        chosen_at: resident.chosen_at,
        loading_since: resident.loading_since,
        healthy_since: resident.healthy_since,
        hold: book.holds.get(&resident.deployment).cloned().flatten(),
        pin: pins
            .get(&resident.deployment)
            .map(|stored| stored.pin.clone()),
        footprint: book
            .footprints
            .get(&resident.deployment)
            .cloned()
            .unwrap_or_default(),
        unified: unified(&inventory, &resident.deployment),
    };
    let pools: Vec<PoolView> = book
        .pools
        .iter()
        .map(|pool| {
            let preference = preferences.get(&pool.key).cloned().unwrap_or_default();
            let models: BTreeSet<String> = book
                .catalog
                .values()
                .filter(|a| a.pool.as_ref() == Some(&pool.key))
                .map(|a| a.model.clone())
                .collect();
            PoolView {
                key: pool.key.clone(),
                members: pool.members.clone(),
                rate_gbps: pool.rate_gbps,
                settings: book.settings.get(&pool.key).copied().unwrap_or_default(),
                warm_model: preference.warm_model,
                idle_override_secs: preference.idle_override_secs,
                idle_since: book.state.clocks.get(&pool.key).map(|c| c.last_work_at),
                residents: book
                    .state
                    .residents
                    .values()
                    .filter(|r| r.pool == pool.key)
                    .map(resident_view)
                    .collect(),
                models: models.into_iter().collect(),
            }
        })
        .collect();
    let stranded = book
        .state
        .residents
        .values()
        .filter(|r| !book.pools.iter().any(|p| p.key == r.pool))
        .map(resident_view)
        .collect();
    let deployments = book
        .catalog
        .values()
        .map(|a| DeploymentPlacement {
            deployment: a.deployment.clone(),
            model: a.model.clone(),
            pool: a.pool.clone(),
            refused: a.refused.clone(),
        })
        .collect();
    let requests = requests
        .into_iter()
        .map(|request| {
            let wait = book.waits.get(&request.id).cloned();
            let said = RequestStatus {
                request: request.clone(),
                wait: wait.clone(),
                now,
            }
            .describe();
            RequestView {
                request,
                wait,
                said,
            }
        })
        .collect();
    Ok(ModelsView {
        now,
        passed_at: book.passed_at,
        pools,
        stranded,
        deployments,
        requests,
        departures: book.departures.iter().rev().cloned().collect(),
    })
}
