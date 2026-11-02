//! The rules placement runs local models by (tenets 1336, 1337): a pure
//! function of the model residents, the deployments placement may acquire on
//! the detected pools, the act side's phases, the requests, the pins, each
//! pool's warm preference and windows, and the batch activity placement saw on
//! the pools' machines since the last pass.
//!
//! A resident is a deployment placement holds on its gang's machines, which
//! reserve the model's measured footprint while it loads and run batches beside
//! it as room allows once it is healthy. A pass applies, in order:
//!
//! 1. A pin is a standing lease: a pinned deployment is acquired at once,
//!    displacing the unpinned residents on its machines, and is never idle.
//! 2. A request for a deployment that is not resident acquires it once every
//!    resident it overlaps can give its machines up. Requests are served first
//!    come, first served, and an earlier request's machines are reserved for it
//!    so a later one cannot overtake it onto part of a gang.
//! 3. A resident on a machine that refused work for room it holds is evicted
//!    when it can give its machines up: no lease, no pin, past its minimum
//!    residency and past its idle window since its last request.
//! 4. Otherwise a pool with no batch work and no waiting request for its idle
//!    window places its warm model, replacing residents that can give way.
//!
//! A resident gives its machines up only when nothing waits on it, it is not
//! pinned, not mid-load, its minimum residency has run and its idle window has
//! passed since its last request: so between two models the loaded one keeps
//! its machines until it goes idle, and the other queues. There is no
//! arbitration by queue depth or age.
//!
//! Phases are the act side's ([`Phase`]), keyed by deployment. A deployment
//! missing from the map is unknown: the act side has never acted on it, and
//! placement neither starts nor stops it on that account. A deployment the act
//! side reports reserving, starting or healthy that placement neither holds nor
//! released (one found answering when placement started, one started by hand,
//! one back after a power cycle) is adopted as a resident. Placement stops only
//! what it released.

use std::collections::{BTreeMap, BTreeSet};

use cairn_common::executor_protocol::MachineRole;
use cairn_inference::control::Phase;
use cairn_inference::{DeploymentId, Inventory};
use serde::{Deserialize, Serialize};

use super::requests::{Hold, HoldCause, Request, RequestId, RequestState, Wait};
use super::settings::{relative, EffectiveSettings, Ms};
use crate::fleet::pools::{Pool, PoolKey};

/// How long after a deployment placement released ended failed it is asked to
/// stop again, so a head that cannot be reached is not sent a stop on every
/// pass.
pub const RESTOP_AFTER_MS: Ms = 60_000;

/// A deployment, as placement may acquire it on the pools detected now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Acquirable {
    pub deployment: DeploymentId,
    /// The real name of the model its profile serves.
    pub model: String,
    pub machines: BTreeSet<String>,
    pub head: String,
    /// The detected pool every machine of the gang is in, when it may run.
    pub pool: Option<PoolKey>,
    /// Why it may not run now, when it may not.
    pub refused: Option<String>,
}

impl Acquirable {
    fn overlaps(&self, machines: &BTreeSet<String>) -> bool {
        !self.machines.is_disjoint(machines)
    }
}

/// What placement may acquire: every deployment of `inventory`, placed on
/// the detected `pools`. A gang must lie wholly inside one pool, number the
/// machines its profile states, and avoid machines whose role is builds only
/// (tenet 1176); one machine is the degenerate gang.
pub fn catalog(
    inventory: &Inventory,
    pools: &[Pool],
    roles: &BTreeMap<String, MachineRole>,
) -> BTreeMap<DeploymentId, Acquirable> {
    inventory
        .deployments
        .values()
        .map(|deployment| {
            let machines: BTreeSet<String> = deployment
                .machines
                .iter()
                .map(|machine| machine.as_str().to_string())
                .collect();
            let profile = inventory.profiles.get(&deployment.profile);
            let model = profile
                .map(|profile| profile.model.name.to_string())
                .unwrap_or_else(|| deployment.profile.to_string());
            let pool = pools
                .iter()
                .find(|pool| !machines.is_empty() && machines.iter().all(|m| pool.has(m)));
            let refused = match profile {
                None => Some(format!("its profile {} is missing", deployment.profile)),
                Some(profile) if profile.requirements.machines as usize != machines.len() => {
                    Some(format!(
                        "its profile runs on {} machines and the deployment names {}",
                        profile.requirements.machines,
                        machines.len()
                    ))
                }
                Some(_) => machines
                    .iter()
                    .find(|machine| roles.get(*machine) == Some(&MachineRole::BuildsOnly))
                    .map(|machine| {
                        format!("{machine} is builds only, so it runs no inference deployment")
                    })
                    .or_else(|| match pool {
                        Some(_) => None,
                        None if machines.len() > 1 => Some(format!(
                            "no fast link joins {}, so they are not one pool",
                            machines.iter().cloned().collect::<Vec<_>>().join(" and ")
                        )),
                        None => Some(format!(
                            "{} is in no detected pool: it is not attached or reports no GPU",
                            machines.iter().next().cloned().unwrap_or_default()
                        )),
                    }),
            };
            let acquirable = Acquirable {
                deployment: deployment.id.clone(),
                model,
                machines,
                head: deployment.head.as_str().to_string(),
                pool: refused
                    .is_none()
                    .then(|| pool.map(|p| p.key.clone()))
                    .flatten(),
                refused,
            };
            (deployment.id.clone(), acquirable)
        })
        .collect()
}

/// Why a resident is resident.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Why {
    /// Found reserving or answering when placement started, and kept.
    Adopted,
    /// A request acquired it.
    Requested {
        request: RequestId,
        requester: String,
    },
    /// A pin acquired it.
    Pinned { holder: String },
    /// Its pin was released or revoked, and it stayed.
    PinReleased { holder: String },
    /// Its pool was idle and it is the pool's warm model.
    Warm { idle_since: Ms },
    /// Started by hand on its deployment page.
    Started { by: String },
}

impl Why {
    pub fn describe(&self, since: Ms, now: Ms) -> String {
        let when = relative(since, now);
        match self {
            Self::Adopted => format!("adopted as it was running when placement started, {when}"),
            Self::Requested { request, requester } => {
                format!("{requester} requested it (request #{request}) {when}")
            }
            Self::Pinned { holder } => format!("pinned by {holder} {when}"),
            Self::PinReleased { holder } => {
                format!("kept when {holder}'s pin on it was released {when}")
            }
            Self::Warm { idle_since } => format!(
                "the pool's warm model, placed {when}: no batch work on the pool since {}",
                relative(*idle_since, now)
            ),
            Self::Started { by } => format!("started by {by} {when}"),
        }
    }
}

/// A model placement holds on its gang's machines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resident {
    pub deployment: DeploymentId,
    pub model: String,
    pub pool: PoolKey,
    pub machines: BTreeSet<String>,
    pub chosen_at: Ms,
    /// When the head was first seen starting it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loading_since: Option<Ms>,
    /// When it was first seen healthy: the start of its minimum residency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthy_since: Option<Ms>,
    pub why: Why,
}

impl Resident {
    pub fn chosen(acquirable: &Acquirable, pool: PoolKey, why: Why, now: Ms) -> Self {
        Self {
            deployment: acquirable.deployment.clone(),
            model: acquirable.model.clone(),
            pool,
            machines: acquirable.machines.clone(),
            chosen_at: now,
            loading_since: None,
            healthy_since: None,
            why,
        }
    }
}

/// Why a resident left.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Left {
    /// Work was refused for the room it held on `machines`, and it was idle.
    Evicted {
        machines: Vec<String>,
    },
    /// Another resident took its machines: a pin, a request, the warm model.
    Displaced {
        by: DeploymentId,
    },
    LoadFailed {
        reason: String,
    },
    /// Stopped by hand.
    Stopped {
        by: String,
    },
    /// Its deployment left the inventory.
    Removed,
}

/// A resident leaving, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Departure {
    pub deployment: DeploymentId,
    pub model: String,
    pub pool: PoolKey,
    pub at: Ms,
    pub why: Left,
}

impl Departure {
    pub fn of(resident: &Resident, why: Left, at: Ms) -> Self {
        Self {
            deployment: resident.deployment.clone(),
            model: resident.model.clone(),
            pool: resident.pool.clone(),
            at,
            why,
        }
    }

    pub fn describe(&self, now: Ms) -> String {
        let when = relative(self.at, now);
        let model = &self.model;
        match &self.why {
            Left::Evicted { machines } => format!(
                "{model} was evicted {when}: work was refused for the room it held on {} while it was idle",
                machines.join(" and ")
            ),
            Left::Displaced { by } => format!("{model} gave its machines to {by} {when}"),
            Left::LoadFailed { reason } => format!("{model} failed to load {when}: {reason}"),
            Left::Stopped { by } => format!("{model} was stopped by {by} {when}"),
            Left::Removed => format!("{model}'s deployment left the inventory {when}"),
        }
    }
}

/// A standing lease on one resident.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pin {
    pub holder: String,
    pub at: Ms,
}

/// A pool's preferences: the model it places when idle, and its idle window
/// when the operator overrides the derived one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Preference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warm_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_override_secs: Option<u64>,
}

/// A pool's idle clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolClock {
    /// The last time batch work ran on, or was refused room on, the pool's
    /// machines.
    pub last_work_at: Ms,
    /// When the warm model last failed to load, so a warm model that will not
    /// load is retried at most once an idle window rather than every pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warm_failed_at: Option<Ms>,
}

impl PoolClock {
    pub fn idle_since(&self) -> Ms {
        self.warm_failed_at
            .map_or(self.last_work_at, |failed| failed.max(self.last_work_at))
    }
}

/// What placement saw on the pools' machines since the last pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Activity {
    /// Machines running batch work now.
    pub running: BTreeSet<String>,
    /// Machines that refused work for room a model resident holds, where
    /// nothing else would take it.
    pub refused: BTreeSet<String>,
}

/// What a pass carries to the next.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct State {
    pub residents: BTreeMap<DeploymentId, Resident>,
    pub clocks: BTreeMap<PoolKey, PoolClock>,
    /// Deployments placement let go that may still occupy their machines:
    /// what it asks the act side to stop, until they are stopped.
    pub released: BTreeSet<DeploymentId>,
}

/// A deployment the act side reports occupying its machines, adopted as it
/// stands: residency counts from now, so adopting it takes nothing away.
fn adopted(acquirable: &Acquirable, phase: &Phase, now: Ms) -> Resident {
    let pool = acquirable
        .pool
        .clone()
        .unwrap_or_else(|| PoolKey::of(acquirable.machines.iter().map(String::as_str)));
    let mut resident = Resident::chosen(acquirable, pool, Why::Adopted, now);
    match phase {
        Phase::Healthy { .. } => resident.healthy_since = Some(now),
        Phase::Starting { since_ms, .. } => resident.loading_since = Some(*since_ms),
        _ => {}
    }
    resident
}

/// Everything one pass reads besides the state it carries.
#[derive(Debug, Clone, Copy)]
pub struct Input<'a> {
    pub now: Ms,
    pub pools: &'a [Pool],
    pub catalog: &'a BTreeMap<DeploymentId, Acquirable>,
    pub phases: &'a BTreeMap<DeploymentId, Phase>,
    /// Requests in any state: waiting ones are planned, answered ones count
    /// as demand.
    pub requests: &'a [Request],
    pub pins: &'a BTreeMap<DeploymentId, Pin>,
    pub preferences: &'a BTreeMap<PoolKey, Preference>,
    /// Each pool's windows; a pool missing here runs the built-in ones.
    pub settings: &'a BTreeMap<PoolKey, EffectiveSettings>,
    pub activity: &'a Activity,
}

impl Input<'_> {
    pub fn settings_of(&self, pool: &PoolKey) -> EffectiveSettings {
        self.settings.get(pool).copied().unwrap_or_default()
    }

    fn phase(&self, id: &DeploymentId) -> Option<&Phase> {
        self.phases.get(id)
    }
}

/// What one pass decided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub state: State,
    /// Requests that stopped waiting, and how.
    pub settled: Vec<(RequestId, RequestState)>,
    /// What each request still waiting waits on.
    pub waits: BTreeMap<RequestId, Wait>,
    /// Residents that left on this pass.
    pub departures: Vec<Departure>,
    /// What the act side is asked for: every stop, then every start.
    pub stops: Vec<DeploymentId>,
    pub starts: Vec<DeploymentId>,
    /// Why each resident cannot give its machines up yet, or `None` when it
    /// can: what placement shows beside the room it holds.
    pub holds: BTreeMap<DeploymentId, Option<Hold>>,
    /// When each resident may be evicted for room if nothing changes
    /// ([`Pass::evictable_at`]).
    pub evictable_at: BTreeMap<DeploymentId, Option<Ms>>,
}

/// One pass's view, shared by the rules.
struct Pass<'a> {
    input: Input<'a>,
    /// Waiting requests per deployment.
    wanted: BTreeMap<DeploymentId, u32>,
    /// The last request each deployment answered or received.
    demand: BTreeMap<DeploymentId, Ms>,
}

impl Pass<'_> {
    /// Why `resident` cannot give its machines up, or `None` when it can.
    fn hold(&self, resident: &Resident) -> Option<Hold> {
        let id = &resident.deployment;
        let hold = |because, until| {
            Some(Hold {
                deployment: id.clone(),
                model: resident.model.clone(),
                because,
                until,
            })
        };
        if let Some(pin) = self.input.pins.get(id) {
            return hold(
                HoldCause::Pinned {
                    holder: pin.holder.clone(),
                },
                None,
            );
        }
        if let Some(&requests) = self.wanted.get(id) {
            return hold(HoldCause::Wanted { requests }, None);
        }
        let settings = self.input.settings_of(&resident.pool);
        let Some(healthy_since) = resident.healthy_since else {
            return match resident.loading_since {
                Some(since) => hold(HoldCause::Loading, Some(since + settings.load_bound.ms())),
                // Chosen but never loaded: nothing to protect.
                None => None,
            };
        };
        let residency_end = healthy_since + settings.min_residency.ms();
        let idle_since = self
            .demand
            .get(id)
            .copied()
            .map_or(healthy_since, |d| d.max(healthy_since));
        let idle_end = idle_since + settings.idle.ms();
        let (because, until) = if residency_end >= idle_end {
            (HoldCause::Residency, residency_end)
        } else {
            (HoldCause::RecentRequest, idle_end)
        };
        (until > self.input.now).then(|| Hold {
            deployment: id.clone(),
            model: resident.model.clone(),
            because,
            until: Some(until),
        })
    }

    /// When `resident` may give its machines up if nothing changes: when its
    /// hold ends, or when the hold it had last ended. A resident idle for an
    /// hour reads the same instant on every pass, so publishing it changes
    /// nothing until something about the resident does. `None` while it is
    /// pinned, leased or loading.
    fn evictable_at(&self, resident: &Resident) -> Option<Ms> {
        let id = &resident.deployment;
        if self.input.pins.contains_key(id) || self.wanted.contains_key(id) {
            return None;
        }
        let settings = self.input.settings_of(&resident.pool);
        let Some(healthy_since) = resident.healthy_since else {
            return match resident.loading_since {
                Some(since) => {
                    let bound = since + settings.load_bound.ms();
                    (bound <= self.input.now).then_some(bound)
                }
                None => Some(resident.chosen_at),
            };
        };
        let residency_end = healthy_since + settings.min_residency.ms();
        let idle_since = self
            .demand
            .get(id)
            .copied()
            .map_or(healthy_since, |d| d.max(healthy_since));
        Some(residency_end.max(idle_since + settings.idle.ms()))
    }

    fn holds<'r>(&self, residents: impl IntoIterator<Item = &'r Resident>) -> Vec<Hold> {
        residents
            .into_iter()
            .filter_map(|resident| self.hold(resident))
            .collect()
    }
}

fn overlapping(
    residents: &BTreeMap<DeploymentId, Resident>,
    machines: &BTreeSet<String>,
) -> Vec<DeploymentId> {
    residents
        .values()
        .filter(|resident| !resident.machines.is_disjoint(machines))
        .map(|resident| resident.deployment.clone())
        .collect()
}

/// Take `ids` out of `residents`, recording each departure as `why`.
fn release(
    residents: &mut BTreeMap<DeploymentId, Resident>,
    ids: &[DeploymentId],
    why: impl Fn(&Resident) -> Left,
    departures: &mut Vec<Departure>,
    now: Ms,
) {
    for id in ids {
        if let Some(resident) = residents.remove(id) {
            departures.push(Departure::of(&resident, why(&resident), now));
        }
    }
}

/// One pass over every pool; see the module docs.
pub fn plan(state: &State, input: Input<'_>) -> Plan {
    let now = input.now;
    let phase = |id: &DeploymentId| input.phase(id);
    let mut residents = state.residents.clone();
    let mut clocks = state.clocks.clone();
    let mut released = state.released.clone();
    let mut departures: Vec<Departure> = Vec::new();
    let mut settled: Vec<(RequestId, RequestState)> = Vec::new();

    // What placement let go and the act side has since stopped is done with;
    // what the act side runs that placement neither holds nor let go is
    // adopted.
    released.retain(|id| {
        !residents.contains_key(id)
            && phase(id).is_some_and(|p| p.reserves() || matches!(p, Phase::Failed { .. }))
    });
    for (id, found) in input.phases {
        if !found.reserves()
            || matches!(found, Phase::Stopping { .. })
            || residents.contains_key(id)
            || released.contains(id)
        {
            continue;
        }
        if let Some(acquirable) = input.catalog.get(id) {
            residents.insert(id.clone(), adopted(acquirable, found, now));
        }
    }

    // Batch work, running or refused room, restarts a pool's idle clock. A
    // pool seen for the first time starts its clock now, so placing its warm
    // model waits a whole window.
    clocks.retain(|key, _| input.pools.iter().any(|pool| &pool.key == key));
    for pool in input.pools {
        let clock = clocks.entry(pool.key.clone()).or_insert(PoolClock {
            last_work_at: now,
            warm_failed_at: None,
        });
        if pool.machines().any(|machine| {
            input.activity.running.contains(machine) || input.activity.refused.contains(machine)
        }) {
            clock.last_work_at = now;
        }
    }

    let mut waiting: Vec<&Request> = input
        .requests
        .iter()
        .filter(|r| r.state == RequestState::Waiting)
        .collect();
    waiting.sort_by_key(|r| (r.at, r.id));
    waiting.retain(|r| {
        let reason = match input.catalog.get(&r.deployment) {
            // A resident serves requests whatever detection says now; only
            // acquiring a model anew needs a detected pool.
            Some(_) if residents.contains_key(&r.deployment) => return true,
            Some(acquirable) if acquirable.pool.is_some() => return true,
            Some(acquirable) => format!(
                "{} cannot run: {}",
                r.deployment,
                acquirable.refused.as_deref().unwrap_or("it is in no pool")
            ),
            None => format!("{} is no longer a deployment", r.deployment),
        };
        settled.push((r.id, RequestState::Failed { at: now, reason }));
        false
    });

    // What the act side reported about each resident since it was chosen.
    let mut failed: Vec<(DeploymentId, String)> = Vec::new();
    let mut gone: Vec<DeploymentId> = Vec::new();
    for (id, resident) in residents.iter_mut() {
        if !input.catalog.contains_key(id) {
            gone.push(id.clone());
            continue;
        }
        match phase(id) {
            // The load is timed from the head receiving the start, not from
            // the wait for the gang to fit.
            Some(Phase::Starting { since_ms, .. }) => {
                resident.loading_since.get_or_insert(*since_ms);
            }
            Some(Phase::Healthy { since_ms, .. }) => {
                resident.healthy_since.get_or_insert(*since_ms);
            }
            Some(Phase::Failed { at_ms, reason }) if *at_ms >= resident.chosen_at => {
                failed.push((id.clone(), reason.clone()));
                continue;
            }
            Some(Phase::Stopped { .. } | Phase::Stopping { .. } | Phase::Failed { .. }) => {
                // Seen healthy, now gone: whatever comes back is a new load.
                if resident.healthy_since.is_some() {
                    resident.healthy_since = None;
                    resident.loading_since = None;
                }
            }
            Some(Phase::Reserving { .. }) | None => {}
        }
        if let (Some(since), None) = (resident.loading_since, resident.healthy_since) {
            let bound = input.settings_of(&resident.pool).load_bound;
            if since + bound.ms() <= now {
                failed.push((
                    id.clone(),
                    format!("it did not come healthy within the load bound of {bound}"),
                ));
            }
        }
    }
    release(
        &mut residents,
        &gone,
        |_| Left::Removed,
        &mut departures,
        now,
    );
    // A pinned deployment's failure since the pin fails the requests waiting
    // on it too; the pin keeps it resident and it is not restarted.
    for (id, pin) in input.pins {
        if let Some(Phase::Failed { at_ms, reason }) = phase(id) {
            if *at_ms >= pin.at && !failed.iter().any(|(f, _)| f == id) {
                failed.push((id.clone(), reason.clone()));
            }
        }
    }
    for (id, reason) in failed {
        if !input.pins.contains_key(&id) {
            if let Some(resident) = residents.remove(&id) {
                if matches!(resident.why, Why::Warm { .. }) {
                    if let Some(clock) = clocks.get_mut(&resident.pool) {
                        clock.warm_failed_at = Some(now);
                    }
                }
                departures.push(Departure::of(
                    &resident,
                    Left::LoadFailed {
                        reason: reason.clone(),
                    },
                    now,
                ));
            }
        }
        waiting.retain(|r| {
            if r.deployment != id {
                return true;
            }
            settled.push((
                r.id,
                RequestState::Failed {
                    at: now,
                    reason: format!("{id} failed to load: {reason}"),
                },
            ));
            false
        });
    }

    let mut pass = Pass {
        input,
        wanted: BTreeMap::new(),
        demand: BTreeMap::new(),
    };
    for request in &waiting {
        *pass.wanted.entry(request.deployment.clone()).or_default() += 1;
    }
    for request in input.requests {
        if let RequestState::Ready { at } = request.state {
            let last = pass.demand.entry(request.deployment.clone()).or_default();
            *last = (*last).max(at.max(request.at));
        }
    }

    // Pins: a standing lease is acquired at once and displaces what is in its
    // way unless that is pinned too, which the pin door already refused.
    let mut pinned: Vec<(&DeploymentId, &Pin)> = input.pins.iter().collect();
    pinned.sort_by_key(|(id, pin)| (pin.at, (*id).clone()));
    for (id, pin) in pinned {
        if residents.contains_key(id) {
            continue;
        }
        let Some((acquirable, pool)) = input
            .catalog
            .get(id)
            .and_then(|a| Some((a, a.pool.clone()?)))
        else {
            continue;
        };
        let in_the_way = overlapping(&residents, &acquirable.machines);
        if in_the_way
            .iter()
            .any(|other| input.pins.contains_key(other))
        {
            continue;
        }
        release(
            &mut residents,
            &in_the_way,
            |_| Left::Displaced { by: id.clone() },
            &mut departures,
            now,
        );
        let mut resident = Resident::chosen(
            acquirable,
            pool,
            Why::Pinned {
                holder: pin.holder.clone(),
            },
            now,
        );
        resident.chosen_at = pin.at.min(now);
        residents.insert(id.clone(), resident);
    }

    // Requests, first come first served. Machines an earlier waiting request
    // is owed, and which request owes them.
    let mut reserved: BTreeMap<String, RequestId> = BTreeMap::new();
    let mut considered = BTreeSet::new();
    for request in &waiting {
        let id = &request.deployment;
        if residents.contains_key(id) || !considered.insert(id.clone()) {
            continue;
        }
        let Some((acquirable, pool)) = input
            .catalog
            .get(id)
            .and_then(|a| Some((a, a.pool.clone()?)))
        else {
            continue;
        };
        let needs = &acquirable.machines;
        if needs.iter().any(|m| reserved.contains_key(m)) {
            for machine in needs {
                reserved.entry(machine.clone()).or_insert(request.id);
            }
            continue;
        }
        let in_the_way = overlapping(&residents, needs);
        if pass
            .holds(in_the_way.iter().map(|other| &residents[other]))
            .is_empty()
        {
            release(
                &mut residents,
                &in_the_way,
                |_| Left::Displaced { by: id.clone() },
                &mut departures,
                now,
            );
            residents.insert(
                id.clone(),
                Resident::chosen(
                    acquirable,
                    pool,
                    Why::Requested {
                        request: request.id,
                        requester: request.requester.clone(),
                    },
                    now,
                ),
            );
        } else {
            for machine in needs {
                reserved.entry(machine.clone()).or_insert(request.id);
            }
        }
    }

    if !input.activity.refused.is_empty() {
        // Work was refused for room on these machines and went nowhere: the
        // residents there that can give way are evicted, and only those.
        let evicted: Vec<(DeploymentId, Vec<String>)> = residents
            .values()
            .filter(|resident| pass.hold(resident).is_none())
            .filter_map(|resident| {
                let machines: Vec<String> = resident
                    .machines
                    .intersection(&input.activity.refused)
                    .cloned()
                    .collect();
                (!machines.is_empty()).then(|| (resident.deployment.clone(), machines))
            })
            .collect();
        for (id, machines) in evicted {
            if let Some(resident) = residents.remove(&id) {
                departures.push(Departure::of(&resident, Left::Evicted { machines }, now));
            }
        }
    } else {
        // The warm preference: a pool with no batch work and no waiting
        // request for its idle window places its warm model.
        for pool in input.pools {
            let Some(warm) = input
                .preferences
                .get(&pool.key)
                .and_then(|p| p.warm_model.as_deref())
            else {
                continue;
            };
            let settings = input.settings_of(&pool.key);
            let clock = clocks[&pool.key];
            let wanted_here = waiting.iter().any(|r| {
                input
                    .catalog
                    .get(&r.deployment)
                    .is_some_and(|a| a.pool.as_ref() == Some(&pool.key))
            });
            if wanted_here || now - clock.idle_since() < settings.idle.ms() {
                continue;
            }
            if residents
                .values()
                .any(|r| r.pool == pool.key && r.model.eq_ignore_ascii_case(warm))
            {
                continue;
            }
            let Some(acquirable) = input
                .catalog
                .values()
                .filter(|a| a.pool.as_ref() == Some(&pool.key))
                .find(|a| a.model.eq_ignore_ascii_case(warm))
            else {
                continue;
            };
            let in_the_way = overlapping(&residents, &acquirable.machines);
            if !pass
                .holds(in_the_way.iter().map(|other| &residents[other]))
                .is_empty()
            {
                continue;
            }
            release(
                &mut residents,
                &in_the_way,
                |_| Left::Displaced {
                    by: acquirable.deployment.clone(),
                },
                &mut departures,
                now,
            );
            residents.insert(
                acquirable.deployment.clone(),
                Resident::chosen(
                    acquirable,
                    pool.key.clone(),
                    Why::Warm {
                        idle_since: clock.idle_since(),
                    },
                    now,
                ),
            );
        }
    }

    // Deployments placement released that still reserve their machines, which
    // a resident's start waits on.
    let releasing: Vec<(&DeploymentId, &Acquirable)> = input
        .phases
        .iter()
        .filter(|(id, phase)| phase.reserves() && !residents.contains_key(*id))
        .filter_map(|(id, _)| Some((id, input.catalog.get(id)?)))
        .collect();
    let in_the_way_of = |machines: &BTreeSet<String>| -> Vec<DeploymentId> {
        releasing
            .iter()
            .filter(|(_, other)| other.overlaps(machines))
            .map(|(id, _)| (*id).clone())
            .collect()
    };

    // What each request still waiting waits on.
    let mut waits = BTreeMap::new();
    for request in waiting {
        let id = &request.deployment;
        let needs = input
            .catalog
            .get(id)
            .map(|a| a.machines.clone())
            .unwrap_or_default();
        let wait = if let Some(resident) = residents.get(id) {
            match phase(id) {
                Some(Phase::Healthy { .. }) => {
                    settled.push((request.id, RequestState::Ready { at: now }));
                    continue;
                }
                Some(Phase::Starting { since_ms, .. }) => {
                    let since = resident.loading_since.unwrap_or(*since_ms);
                    Wait::Loading {
                        since,
                        bound_at: since + input.settings_of(&resident.pool).load_bound.ms(),
                    }
                }
                Some(Phase::Reserving {
                    since_ms,
                    waiting_on,
                }) => Wait::Fitting {
                    since: *since_ms,
                    waiting_on: waiting_on.clone(),
                },
                Some(Phase::Stopping { .. }) => Wait::Stopping,
                None | Some(Phase::Stopped { .. } | Phase::Failed { .. }) => {
                    let in_the_way = in_the_way_of(&needs);
                    if in_the_way.is_empty() {
                        Wait::Starting
                    } else {
                        Wait::Unloading {
                            deployments: in_the_way,
                        }
                    }
                }
            }
        } else {
            let by = pass.holds(
                overlapping(&residents, &needs)
                    .iter()
                    .map(|other| &residents[other]),
            );
            let behind = needs
                .iter()
                .filter_map(|m| reserved.get(m))
                .find(|owner| **owner != request.id);
            match (behind, by.is_empty()) {
                (Some(&behind), true) => Wait::Queued {
                    behind,
                    deployment: input
                        .requests
                        .iter()
                        .find(|r| r.id == behind)
                        .map(|r| r.deployment.clone())
                        .unwrap_or_else(|| id.clone()),
                },
                _ => Wait::Held { by },
            }
        };
        waits.insert(request.id, wait);
    }

    // Converge: stop what placement released, then start what it holds.
    for departure in &departures {
        released.insert(departure.deployment.clone());
    }
    released.retain(|id| !residents.contains_key(id));
    let mut stops = Vec::new();
    for id in &released {
        let stop = match phase(id) {
            Some(Phase::Reserving { .. } | Phase::Starting { .. } | Phase::Healthy { .. }) => true,
            Some(Phase::Failed { at_ms, .. }) => now - at_ms >= RESTOP_AFTER_MS,
            Some(Phase::Stopped { .. } | Phase::Stopping { .. }) | None => false,
        };
        if stop && input.catalog.contains_key(id) {
            stops.push(id.clone());
        }
    }
    let mut starts = Vec::new();
    for (id, resident) in &residents {
        if !input.catalog.get(id).is_some_and(|a| a.pool.is_some()) {
            continue;
        }
        let chosen_at = input.pins.get(id).map_or(resident.chosen_at, |pin| pin.at);
        let start = match phase(id) {
            None | Some(Phase::Stopped { .. }) => true,
            Some(Phase::Failed { at_ms, .. }) => *at_ms < chosen_at,
            Some(_) => false,
        };
        if start && in_the_way_of(&resident.machines).is_empty() {
            starts.push(id.clone());
        }
    }

    let holds = residents
        .values()
        .map(|resident| (resident.deployment.clone(), pass.hold(resident)))
        .collect();
    let evictable_at = residents
        .values()
        .map(|resident| (resident.deployment.clone(), pass.evictable_at(resident)))
        .collect();
    Plan {
        state: State {
            residents,
            clocks,
            released,
        },
        settled,
        waits,
        departures,
        stops,
        starts,
        holds,
        evictable_at,
    }
}
