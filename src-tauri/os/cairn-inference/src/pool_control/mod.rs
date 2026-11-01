//! The pool controller: which deployment set each controlled pool should run,
//! decided by the orch itself (tenet 1321) and written as desired state.
//!
//! A pool under Cairn's control carries a [`PoolControl`] beside its default:
//! Cairn's own pin, with a holder, and the controller's active set with why it
//! is what it is. [`plan`] is the whole policy, a pure function of that record,
//! the pool's requests, each deployment's [`Phase`] as the act side reports it,
//! and the [`BuildPressure`] on the pool's machines:
//!
//! 1. A request for a deployment that is not in the active set adds it, once
//!    every member it overlaps can give the pool up. The request waits
//!    visibly ([`Wait`]) until the deployment is healthy.
//! 2. With batches waiting for the pool, a member with no request for the
//!    model idle window, past its minimum residency, is unloaded.
//! 3. A pool with no batch work for the pool idle window loads its default.
//!
//! A member gives the pool up only when nothing waits on it, it is not
//! mid-load, its minimum residency has run, and its model idle window has
//! passed since its last request: so between two models the loaded one keeps
//! the pool until it goes idle, and the other queues. Waiting requests are
//! served first come, first served, and an earlier request's machines are
//! reserved for it so a later one cannot overtake it onto part of a gang.
//! There is no arbitration by queue depth or age, and a request names one
//! deployment and is never served by another.
//!
//! A pin overrides the controller: while it is held the desired set is the
//! pin's, the active set is left alone, and releasing it hands its set to the
//! controller so the rules carry on from what is loaded.
//!
//! Phases are the act side's ([`crate::control::Phase`]), keyed by deployment.
//! A deployment missing from the map is unknown: the act side has never acted
//! on it, so nothing says whether it runs, and it is never read as stopped.
//!
//! [`plan`] decides and [`converge`] says what the act side must be asked for
//! the pool to run what was decided: stops first, then starts. The host sends
//! those through the act side, whose refusals are the one authority over what
//! may be sent now, and asks again on its next pass. Placement reacts to the
//! same record, watching its `generation`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::control::{self as act, Phase};
use crate::deployment::{Deployment, DeploymentId};
use crate::inventory::Inventory;
use crate::machine::MachineName;
use crate::pool::{DeploymentSet, Pool, PoolId};

pub mod settings;

pub use settings::{duration_label, EffectiveSettings, PoolSettings, Setting, SettingOrigin};

#[cfg(test)]
mod tests;

/// Epoch milliseconds.
pub type Ms = i64;

/// A request's id, assigned by the host's store.
pub type RequestId = i64;

/// Cairn's manual override: it holds `set` against idle unload until released
/// by its holder or revoked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pin {
    pub set: DeploymentSet,
    pub holder: String,
    pub at: Ms,
}

/// One member of the controller's active set and what the controller has
/// seen of it since choosing it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Member {
    pub chosen_at: Ms,
    /// When the act side was first seen starting it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loading_since: Option<Ms>,
    /// When it was first seen healthy: the start of its minimum residency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthy_since: Option<Ms>,
}

impl Member {
    pub fn chosen(at: Ms) -> Self {
        Self {
            chosen_at: at,
            loading_since: None,
            healthy_since: None,
        }
    }
}

/// Why the controller's active set last changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Why {
    /// Control began with what was running then.
    Adopted,
    /// A request added `deployment`.
    Requested {
        deployment: DeploymentId,
        request: RequestId,
        requester: String,
    },
    /// Rule 2: `deployments` went idle while `waiting` batches wanted the pool.
    Unloaded {
        deployments: DeploymentSet,
        waiting: u32,
    },
    /// Rule 3: the pool had no batch work since `idle_since`.
    IdleDefault { idle_since: Ms },
    /// A pin was released or revoked, and its set carried over.
    PinReleased { holder: String },
    /// `deployment` did not load.
    LoadFailed {
        deployment: DeploymentId,
        reason: String,
    },
    /// `deployment` left the inventory or the pool.
    Removed { deployment: DeploymentId },
}

/// The controller's chosen set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Active {
    pub members: BTreeMap<DeploymentId, Member>,
    pub why: Why,
    /// When `why` happened.
    pub since: Ms,
}

impl Active {
    pub fn set(&self) -> DeploymentSet {
        self.members.keys().cloned().collect()
    }

    fn change(&mut self, why: Why, now: Ms) {
        self.why = why;
        self.since = now;
    }
}

/// A pool's control record, held beside its default and the mirrored pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolControl {
    pub pool: PoolId,
    /// Whether the controller drives this pool. Settings and a pin may be kept
    /// while it does not.
    pub controlled: bool,
    #[serde(default)]
    pub settings: PoolSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<Pin>,
    pub active: Active,
    /// The last time batch work ran on, or waited for, the pool's machines.
    pub last_batch_work_at: Ms,
    /// Advances each time [`desired`] changes and at no other time, so a
    /// consumer reacts to a change in what the pool wants instead of polling
    /// every pass's bookkeeping.
    #[serde(default)]
    pub generation: u64,
}

impl PoolControl {
    /// A record for `pool` taking control at `now`, adopting every deployment
    /// of the pool that is healthy so that taking control changes nothing that
    /// is running.
    pub fn begin(
        pool: &Pool,
        deployments: &BTreeMap<DeploymentId, Deployment>,
        phases: &BTreeMap<DeploymentId, Phase>,
        now: Ms,
    ) -> Self {
        let members = deployments
            .values()
            .filter(|d| in_pool(pool, d))
            .filter(|d| matches!(phases.get(&d.id), Some(Phase::Healthy { .. })))
            .map(|d| {
                (
                    d.id.clone(),
                    Member {
                        healthy_since: Some(now),
                        ..Member::chosen(now)
                    },
                )
            })
            .collect();
        Self {
            pool: pool.id.clone(),
            controlled: true,
            settings: PoolSettings::default(),
            pin: None,
            active: Active {
                members,
                why: Why::Adopted,
                since: now,
            },
            last_batch_work_at: now,
            generation: 0,
        }
    }

    /// Advance `generation` if [`desired`] differs from `before`. [`plan`] does
    /// this itself; a caller that changes the record any other way (taking or
    /// handing back control, a pin, a release) calls it once afterwards.
    pub fn advance_generation(&mut self, before: &Option<DeploymentSet>) {
        if desired(self) != *before {
            self.generation += 1;
        }
    }

    /// Pin `set` for `holder`. A pin held by someone else is refused, naming
    /// them; the same holder may re-pin.
    pub fn pin(&mut self, set: DeploymentSet, holder: &str, now: Ms) -> Result<(), PinRefused> {
        if let Some(pin) = &self.pin {
            if pin.holder != holder {
                return Err(PinRefused::HeldBy(pin.clone()));
            }
        }
        self.pin = Some(Pin {
            set,
            holder: holder.to_string(),
            at: now,
        });
        Ok(())
    }

    /// Release the pin, by its holder or, with `revoke`, by anyone. Its set
    /// becomes the active set, so the rules carry on from what it held: a
    /// pinned deployment that is healthy counts its residency from the pin.
    pub fn release(
        &mut self,
        holder: &str,
        revoke: bool,
        phases: &BTreeMap<DeploymentId, Phase>,
        now: Ms,
    ) -> Result<Pin, PinRefused> {
        let pin = match &self.pin {
            None => return Err(PinRefused::NotPinned),
            Some(pin) if pin.holder != holder && !revoke => {
                return Err(PinRefused::HeldBy(pin.clone()))
            }
            Some(pin) => pin.clone(),
        };
        self.pin = None;
        let previous = std::mem::take(&mut self.active.members);
        self.active.members = pin
            .set
            .iter()
            .map(|id| {
                let member = previous.get(id).cloned().unwrap_or_else(|| Member {
                    healthy_since: matches!(phases.get(id), Some(Phase::Healthy { .. }))
                        .then_some(pin.at),
                    ..Member::chosen(now)
                });
                (id.clone(), member)
            })
            .collect();
        self.active.change(
            Why::PinReleased {
                holder: pin.holder.clone(),
            },
            now,
        );
        Ok(pin)
    }
}

/// Why a pin or release was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinRefused {
    HeldBy(Pin),
    NotPinned,
}

impl fmt::Display for PinRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeldBy(pin) => write!(
                f,
                "the pool is pinned to {} by {}; only they can release it, or revoke it explicitly",
                set_label(&pin.set),
                pin.holder
            ),
            Self::NotPinned => f.write_str("the pool has no pin"),
        }
    }
}

impl std::error::Error for PinRefused {}

/// Batch work on, or waiting for, a pool's machines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildPressure {
    /// Agent turns running on the pool's machines. Never preempted.
    pub running_turns: u32,
    /// Every other batch running on the pool's machines: restartable checks.
    pub running_other: u32,
    /// Batches waiting that the pool's machines refused for a reason
    /// unloading a model would relieve.
    pub waiting: u32,
}

impl BuildPressure {
    pub fn running(self) -> u32 {
        self.running_turns + self.running_other
    }

    pub fn any(self) -> bool {
        self.running() > 0 || self.waiting > 0
    }
}

/// One caller's request for a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub id: RequestId,
    pub deployment: DeploymentId,
    /// The model as the caller named it.
    pub model: String,
    pub requester: String,
    pub at: Ms,
    pub state: RequestState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum RequestState {
    Waiting,
    /// The deployment was healthy at `at`.
    Ready {
        at: Ms,
    },
    Failed {
        at: Ms,
        reason: String,
    },
    Cancelled {
        at: Ms,
    },
}

impl RequestState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Ready { .. } => "ready",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
        }
    }
}

/// Why a member cannot give the pool up yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum HoldCause {
    /// Requests wait on it.
    Wanted { requests: u32 },
    /// It is loading.
    Loading,
    /// Its minimum residency has not run.
    Residency,
    /// It was requested within the model idle window.
    RecentRequest,
}

/// A member holding machines a waiting request needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Hold {
    pub deployment: DeploymentId,
    pub because: HoldCause,
    /// When the hold ends if nothing changes; `None` while requests wait on it.
    pub until: Option<Ms>,
}

/// What a waiting request waits on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Wait {
    /// Its deployment is desired; others still occupy its machines.
    Unloading { deployments: Vec<DeploymentId> },
    /// Its deployment is desired and nothing is in the way; the controller
    /// asks the act side to start it on this pass.
    Starting,
    /// Its deployment is desired and its machines are reserved, but the start
    /// has not been sent: the act side waits for every machine to have the
    /// memory the deployment needs (`waiting_on`, in the act side's words)
    /// while the batches on the pool's machines finish, and fails the start
    /// if it never fits.
    Fitting {
        since: Ms,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        waiting_on: Option<String>,
        turns: u32,
        other: u32,
    },
    /// The act side is loading it.
    Loading { since: Ms, bound_at: Ms },
    /// Its deployment is desired, and the act side is still stopping it.
    Stopping,
    /// A pin holds the pool.
    Pinned { holder: String, set: DeploymentSet },
    /// Loaded deployments hold machines it needs.
    Held { by: Vec<Hold> },
    /// An earlier request waits for machines it needs.
    Queued {
        behind: RequestId,
        deployment: DeploymentId,
    },
}

/// Which slot a controlled pool's desired set came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ControlSlot {
    Pin,
    Requested,
    Default,
    Nothing,
}

impl ControlSlot {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pin => "pin",
            Self::Requested => "requested",
            Self::Default => "default",
            Self::Nothing => "nothing",
        }
    }
}

/// The reason behind a controlled pool's desired set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ControlReason {
    Pin { pin: Pin },
    Controller { why: Why, since: Ms },
}

/// A controlled pool's desired set, the slot it came from, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlResolution {
    pub desired: DeploymentSet,
    pub slot: ControlSlot,
    pub reason: ControlReason,
}

/// What one pass of the controller decided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    /// The control record to write back.
    pub control: PoolControl,
    pub resolution: ControlResolution,
    /// Requests that stopped waiting, and how.
    pub settled: Vec<(RequestId, RequestState)>,
    /// What each request still waiting waits on.
    pub waits: BTreeMap<RequestId, Wait>,
}

/// What the controller observes on one pass.
#[derive(Debug, Clone, Copy)]
pub struct Observed<'a> {
    pub now: Ms,
    pub phases: &'a BTreeMap<DeploymentId, Phase>,
    pub builds: BuildPressure,
}

fn in_pool(pool: &Pool, deployment: &Deployment) -> bool {
    !deployment.machines.is_empty() && deployment.machines.iter().all(|m| pool.has_machine(m))
}

fn machines(deployment: &Deployment) -> BTreeSet<MachineName> {
    deployment.machines.iter().cloned().collect()
}

pub(crate) fn set_label(set: &DeploymentSet) -> String {
    if set.is_empty() {
        "nothing".to_string()
    } else {
        set.iter()
            .map(DeploymentId::as_str)
            .collect::<Vec<_>>()
            .join("+")
    }
}

/// The set the record chooses: the pin's, or the controller's.
fn chosen(control: &PoolControl) -> DeploymentSet {
    match &control.pin {
        Some(pin) => pin.set.clone(),
        None => control.active.set(),
    }
}

/// What a pool wants loaded: the pin's set or the controller's, or `None`
/// when the controller does not drive the pool. The one definition the act
/// side and placement read. `None` and the empty set mean different things:
/// no controller (the pool is not Cairn's to decide), and a controller that
/// wants nothing loaded (its machines are free for batch work).
pub fn desired(control: &PoolControl) -> Option<DeploymentSet> {
    control.controlled.then(|| chosen(control))
}

/// The slot and reason behind [`desired`].
pub fn resolution(pool: &Pool, control: &PoolControl) -> ControlResolution {
    if let Some(pin) = &control.pin {
        return ControlResolution {
            desired: pin.set.clone(),
            slot: ControlSlot::Pin,
            reason: ControlReason::Pin { pin: pin.clone() },
        };
    }
    let set = control.active.set();
    let slot = match &control.active.why {
        _ if set.is_empty() => ControlSlot::Nothing,
        Why::IdleDefault { .. } => ControlSlot::Default,
        Why::Requested { .. } => ControlSlot::Requested,
        _ if set == pool.default => ControlSlot::Default,
        _ => ControlSlot::Requested,
    };
    ControlResolution {
        desired: set,
        slot,
        reason: ControlReason::Controller {
            why: control.active.why.clone(),
            since: control.active.since,
        },
    }
}

/// Check that `set` can be a pool's desired set: deployments of the pool on
/// disjoint machines.
pub fn check_set(
    pool: &Pool,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    set: &DeploymentSet,
) -> Result<(), String> {
    let mut taken: BTreeMap<&MachineName, &DeploymentId> = BTreeMap::new();
    for id in set {
        let Some(deployment) = deployments.get(id) else {
            return Err(format!("there is no deployment {id}"));
        };
        if !in_pool(pool, deployment) {
            return Err(format!("{id} is not a deployment of pool {}", pool.id));
        }
        for machine in &deployment.machines {
            if let Some(other) = taken.insert(machine, id) {
                return Err(format!("{other} and {id} both need {machine}"));
            }
        }
    }
    Ok(())
}

/// The deployments a caller's model name can mean: a deployment id names
/// exactly that deployment; otherwise every deployment of a recipe whose id,
/// served name, model name or alias matches, case-insensitively.
pub fn deployments_serving(inventory: &Inventory, name: &str) -> Vec<DeploymentId> {
    let name = name.trim();
    if let Some(deployment) = inventory.deployments.get(&DeploymentId::from(name)) {
        return vec![deployment.id.clone()];
    }
    let matches = |candidate: &str| candidate.eq_ignore_ascii_case(name);
    inventory
        .deployments
        .values()
        .filter(|d| {
            inventory.recipes.get(&d.recipe).is_some_and(|r| {
                matches(r.id.as_str())
                    || matches(&r.served_name)
                    || matches(&r.model.name)
                    || r.aliases.iter().any(|a| matches(a))
            })
        })
        .map(|d| d.id.clone())
        .collect()
}

/// One pass's view of the pool, shared by the rules.
struct Pass<'a> {
    now: Ms,
    deployments: &'a BTreeMap<DeploymentId, Deployment>,
    settings: &'a EffectiveSettings,
    /// Waiting requests per deployment.
    wanted: BTreeMap<DeploymentId, u32>,
    /// The last request each deployment answered or received.
    demand: BTreeMap<DeploymentId, Ms>,
}

impl Pass<'_> {
    fn machines_of(&self, id: &DeploymentId) -> BTreeSet<MachineName> {
        self.deployments.get(id).map(machines).unwrap_or_default()
    }

    /// Why `member` cannot give the pool up, or `None` when it can.
    fn hold(&self, id: &DeploymentId, member: &Member) -> Option<Hold> {
        let hold = |because, until| {
            Some(Hold {
                deployment: id.clone(),
                because,
                until,
            })
        };
        if let Some(&requests) = self.wanted.get(id) {
            return hold(HoldCause::Wanted { requests }, None);
        }
        let Some(healthy_since) = member.healthy_since else {
            return match member.loading_since {
                Some(since) => hold(
                    HoldCause::Loading,
                    Some(since + self.settings.load_bound.ms()),
                ),
                // Chosen but never loaded: nothing to protect.
                None => None,
            };
        };
        let residency_end = healthy_since + self.settings.min_residency.ms();
        let idle_since = self
            .demand
            .get(id)
            .copied()
            .map_or(healthy_since, |d| d.max(healthy_since));
        let idle_end = idle_since + self.settings.model_idle.ms();
        let (because, until) = if residency_end >= idle_end {
            (HoldCause::Residency, residency_end)
        } else {
            (HoldCause::RecentRequest, idle_end)
        };
        (until > self.now).then(|| Hold {
            deployment: id.clone(),
            because,
            until: Some(until),
        })
    }

    /// Members of `active` whose machines meet `wanted`.
    fn overlapping(&self, active: &Active, wanted: &BTreeSet<MachineName>) -> Vec<DeploymentId> {
        active
            .members
            .keys()
            .filter(|id| !self.machines_of(id).is_disjoint(wanted))
            .cloned()
            .collect()
    }

    fn holds(&self, active: &Active, ids: &[DeploymentId]) -> Vec<Hold> {
        ids.iter()
            .filter_map(|id| self.hold(id, &active.members[id]))
            .collect()
    }
}

/// Where rule 3's pool idle window counts from: the last batch work, or a
/// failed load since, so a default that will not load is retried at most once
/// a window rather than on every pass.
fn pool_idle_since(control: &PoolControl) -> Ms {
    match control.active.why {
        Why::LoadFailed { .. } => control.last_batch_work_at.max(control.active.since),
        _ => control.last_batch_work_at,
    }
}

/// One pass of the controller over a controlled pool; see the module docs.
///
/// `requests` are the pool's requests in any state: waiting ones are planned,
/// answered ones count as demand.
pub fn plan(
    pool: &Pool,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    control: &PoolControl,
    settings: &EffectiveSettings,
    requests: &[Request],
    observed: Observed<'_>,
) -> Plan {
    let now = observed.now;
    let phase = |id: &DeploymentId| observed.phases.get(id);
    let before = desired(control);
    let mut control = control.clone();
    if observed.builds.any() {
        control.last_batch_work_at = now;
    }
    let mut settled: Vec<(RequestId, RequestState)> = Vec::new();

    let mut waiting: Vec<&Request> = requests
        .iter()
        .filter(|r| r.state == RequestState::Waiting)
        .collect();
    waiting.sort_by_key(|r| (r.at, r.id));
    waiting.retain(|r| match deployments.get(&r.deployment) {
        Some(d) if in_pool(pool, d) => true,
        _ => {
            settled.push((
                r.id,
                RequestState::Failed {
                    at: now,
                    reason: format!(
                        "{} is no longer a deployment of pool {}",
                        r.deployment, pool.id
                    ),
                },
            ));
            false
        }
    });

    // What the act side reported about each member since it was chosen.
    let mut failed: Vec<(DeploymentId, String)> = Vec::new();
    let ids: Vec<DeploymentId> = control.active.members.keys().cloned().collect();
    for id in ids {
        if !deployments.get(&id).is_some_and(|d| in_pool(pool, d)) {
            control.active.members.remove(&id);
            control.active.change(Why::Removed { deployment: id }, now);
            continue;
        }
        let member = control
            .active
            .members
            .get_mut(&id)
            .expect("listed from the members");
        match phase(&id) {
            // The load is timed from the head receiving the start, not from
            // the wait for the gang to fit.
            Some(Phase::Starting { since_ms, .. }) => {
                member.loading_since.get_or_insert(*since_ms);
            }
            Some(Phase::Healthy { since_ms, .. }) => {
                member.healthy_since.get_or_insert(*since_ms);
            }
            Some(Phase::Failed { at_ms, reason }) if *at_ms >= member.chosen_at => {
                failed.push((id.clone(), reason.clone()));
                continue;
            }
            Some(Phase::Stopped { .. } | Phase::Stopping { .. } | Phase::Failed { .. }) => {
                // Seen healthy, now gone: whatever comes back is a new load.
                if member.healthy_since.is_some() {
                    member.healthy_since = None;
                    member.loading_since = None;
                }
            }
            // Reserved, but nothing sent: no load to protect yet. Unknown: the
            // act side has not been asked yet.
            Some(Phase::Reserving { .. }) | None => {}
        }
        if let (Some(since), None) = (member.loading_since, member.healthy_since) {
            if since + settings.load_bound.ms() <= now {
                failed.push((
                    id.clone(),
                    format!(
                        "it did not come healthy within the load bound of {}",
                        settings.load_bound
                    ),
                ));
            }
        }
    }
    // A pinned deployment's failure fails the requests waiting on it too.
    if let Some(pin) = &control.pin {
        for id in &pin.set {
            if let Some(Phase::Failed { at_ms, reason }) = phase(id) {
                if *at_ms >= pin.at && !failed.iter().any(|(f, _)| f == id) {
                    failed.push((id.clone(), reason.clone()));
                }
            }
        }
    }
    for (id, reason) in failed {
        if control.active.members.remove(&id).is_some() {
            control.active.change(
                Why::LoadFailed {
                    deployment: id.clone(),
                    reason: reason.clone(),
                },
                now,
            );
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
        now,
        deployments,
        settings,
        wanted: BTreeMap::new(),
        demand: BTreeMap::new(),
    };
    for request in &waiting {
        *pass.wanted.entry(request.deployment.clone()).or_default() += 1;
    }
    for request in requests {
        if let RequestState::Ready { at } = request.state {
            let last = pass.demand.entry(request.deployment.clone()).or_default();
            *last = (*last).max(at.max(request.at));
        }
    }

    // Machines earlier waiting requests are owed, and which request owes them.
    let mut reserved: BTreeMap<MachineName, RequestId> = BTreeMap::new();
    if control.pin.is_none() {
        // Rule 1, first come first served.
        let mut considered = BTreeSet::new();
        for request in &waiting {
            let id = &request.deployment;
            if control.active.members.contains_key(id) || !considered.insert(id.clone()) {
                continue;
            }
            let needs = pass.machines_of(id);
            if needs.iter().any(|m| reserved.contains_key(m)) {
                for machine in &needs {
                    reserved.entry(machine.clone()).or_insert(request.id);
                }
                continue;
            }
            let overlapping = pass.overlapping(&control.active, &needs);
            if pass.holds(&control.active, &overlapping).is_empty() {
                for gone in &overlapping {
                    control.active.members.remove(gone);
                }
                control
                    .active
                    .members
                    .insert(id.clone(), Member::chosen(now));
                control.active.change(
                    Why::Requested {
                        deployment: id.clone(),
                        request: request.id,
                        requester: request.requester.clone(),
                    },
                    now,
                );
            } else {
                for machine in needs {
                    reserved.entry(machine).or_insert(request.id);
                }
            }
        }

        if observed.builds.waiting > 0 {
            // Rule 2.
            let idle: DeploymentSet = control
                .active
                .members
                .iter()
                .filter(|(id, member)| pass.hold(id, member).is_none())
                .map(|(id, _)| id.clone())
                .collect();
            if !idle.is_empty() {
                for id in &idle {
                    control.active.members.remove(id);
                }
                control.active.change(
                    Why::Unloaded {
                        deployments: idle,
                        waiting: observed.builds.waiting,
                    },
                    now,
                );
            }
        } else if waiting.is_empty()
            && !observed.builds.any()
            && now - pool_idle_since(&control) >= settings.pool_idle.ms()
        {
            // Rule 3.
            let missing: Vec<DeploymentId> = pool
                .default
                .iter()
                .filter(|id| !control.active.members.contains_key(*id))
                .filter(|id| deployments.get(*id).is_some_and(|d| in_pool(pool, d)))
                .cloned()
                .collect();
            if !missing.is_empty() {
                let needs: BTreeSet<MachineName> =
                    missing.iter().flat_map(|id| pass.machines_of(id)).collect();
                let overlapping = pass.overlapping(&control.active, &needs);
                if pass.holds(&control.active, &overlapping).is_empty() {
                    for gone in &overlapping {
                        control.active.members.remove(gone);
                    }
                    for id in missing {
                        control.active.members.insert(id, Member::chosen(now));
                    }
                    control.active.change(
                        Why::IdleDefault {
                            idle_since: pool_idle_since(&control),
                        },
                        now,
                    );
                }
            }
        }
    }

    // What each request still waiting waits on.
    let desired_set = chosen(&control);
    let mut waits = BTreeMap::new();
    for request in waiting {
        let id = &request.deployment;
        let needs = pass.machines_of(id);
        let wait = if desired_set.contains(id) {
            match phase(id) {
                Some(Phase::Healthy { .. }) => {
                    settled.push((request.id, RequestState::Ready { at: now }));
                    continue;
                }
                Some(Phase::Starting { since_ms, .. }) => {
                    let since = control
                        .active
                        .members
                        .get(id)
                        .and_then(|m| m.loading_since)
                        .unwrap_or(*since_ms);
                    Wait::Loading {
                        since,
                        bound_at: since + settings.load_bound.ms(),
                    }
                }
                Some(Phase::Reserving {
                    since_ms,
                    waiting_on,
                }) => Wait::Fitting {
                    since: *since_ms,
                    waiting_on: waiting_on.clone(),
                    turns: observed.builds.running_turns,
                    other: observed.builds.running_other,
                },
                Some(Phase::Stopping { .. }) => Wait::Stopping,
                None | Some(Phase::Stopped { .. } | Phase::Failed { .. }) => {
                    let in_the_way: Vec<DeploymentId> = deployments
                        .values()
                        .filter(|d| in_pool(pool, d) && !desired_set.contains(&d.id))
                        .filter(|d| !machines(d).is_disjoint(&needs))
                        .filter(|d| phase(&d.id).is_some_and(Phase::reserves))
                        .map(|d| d.id.clone())
                        .collect();
                    if in_the_way.is_empty() {
                        Wait::Starting
                    } else {
                        Wait::Unloading {
                            deployments: in_the_way,
                        }
                    }
                }
            }
        } else if let Some(pin) = &control.pin {
            Wait::Pinned {
                holder: pin.holder.clone(),
                set: pin.set.clone(),
            }
        } else {
            let by = pass.holds(&control.active, &pass.overlapping(&control.active, &needs));
            let behind = needs
                .iter()
                .filter_map(|m| reserved.get(m))
                .find(|owner| **owner != request.id);
            match (behind, by.is_empty()) {
                (Some(&behind), true) => Wait::Queued {
                    behind,
                    deployment: requests
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

    control.advance_generation(&before);
    let resolution = resolution(pool, &control);
    Plan {
        control,
        resolution,
        settled,
        waits,
    }
}

/// How long after a deployment the pool does not want ended failed the
/// controller asks for it to be stopped again, so a head that cannot be
/// reached is not sent a stop on every pass.
pub const RESTOP_AFTER_MS: Ms = 60_000;

/// What the act side is asked on one pass for `pool` to run what `control`
/// wants: every stop, then every start. Empty for a pool not under control.
///
/// A deployment of the pool that is not desired is stopped while it is
/// reserving, starting or healthy. One that ended failed is stopped again
/// [`RESTOP_AFTER_MS`] after it failed: a failure frees its machines though
/// the model may still be resident. One the act side has never acted on is
/// stopped too, which is what brings whatever was left running before Cairn
/// acted (bginf's last model, say) under the act side's probing, unless another deployment holds one of its machines,
/// since the two cannot both run there.
///
/// A desired deployment is started when it is unknown, stopped, or failed
/// before it was chosen. One that failed since is the plan's to give up; a
/// pinned one stays failed until the pin changes. A start the act side
/// refuses (another deployment still holds a machine) is asked again on the
/// next pass, and no fit bound is added here: the act side fails a start that
/// never fits.
pub fn converge(
    pool: &Pool,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    control: &PoolControl,
    phases: &BTreeMap<DeploymentId, Phase>,
    now: Ms,
) -> Vec<(DeploymentId, act::Request)> {
    let Some(wanted) = desired(control) else {
        return Vec::new();
    };
    let held: BTreeSet<&MachineName> = phases
        .iter()
        .filter(|(_, phase)| phase.reserves())
        .filter_map(|(id, _)| deployments.get(id))
        .flat_map(|d| &d.machines)
        .collect();
    let mut steps = Vec::new();
    for deployment in deployments
        .values()
        .filter(|d| in_pool(pool, d) && !wanted.contains(&d.id))
    {
        let stop = match phases.get(&deployment.id) {
            Some(Phase::Reserving { .. } | Phase::Starting { .. } | Phase::Healthy { .. }) => true,
            Some(Phase::Failed { at_ms, .. }) => now - at_ms >= RESTOP_AFTER_MS,
            Some(Phase::Stopped { .. } | Phase::Stopping { .. }) => false,
            None => deployment.machines.iter().all(|m| !held.contains(m)),
        };
        if stop {
            steps.push((deployment.id.clone(), act::Request::Stop));
        }
    }
    let chosen_at = |id: &DeploymentId| match &control.pin {
        Some(pin) => Some(pin.at),
        None => control.active.members.get(id).map(|m| m.chosen_at),
    };
    for id in &wanted {
        if !deployments.get(id).is_some_and(|d| in_pool(pool, d)) {
            continue;
        }
        let start = match phases.get(id) {
            None | Some(Phase::Stopped { .. }) => true,
            Some(Phase::Failed { at_ms, .. }) => chosen_at(id).is_some_and(|at| *at_ms < at),
            Some(_) => false,
        };
        if start {
            steps.push((id.clone(), act::Request::Start));
        }
    }
    steps
}

/// The slowest healthy load recorded for a deployment of `pool`, in whole
/// seconds rounded up: what the pool's unset settings derive from.
pub fn slowest_load_secs<'a>(
    pool: &Pool,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    healthy_loads_ms: impl IntoIterator<Item = (&'a DeploymentId, u64)>,
) -> Option<u64> {
    healthy_loads_ms
        .into_iter()
        .filter(|(id, _)| deployments.get(*id).is_some_and(|d| in_pool(pool, d)))
        .map(|(_, ms)| ms.div_ceil(1_000))
        .max()
}

/// How long ago `at` was, or how far off, relative to `now`.
fn relative(at: Ms, now: Ms) -> String {
    let secs = |ms: Ms| u64::try_from(ms / 1_000).unwrap_or(0);
    if at <= now {
        format!("{} ago", duration_label(secs(now - at)))
    } else {
        format!("in {}", duration_label(secs(at - now)))
    }
}

fn ids_label(ids: &[DeploymentId]) -> String {
    ids.iter()
        .map(DeploymentId::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

fn plural(count: u32, one: &str, many: &str) -> String {
    if count == 1 {
        format!("1 {one}")
    } else {
        format!("{count} {many}")
    }
}

impl Hold {
    pub fn describe(&self, now: Ms) -> String {
        let until = self
            .until
            .map(|at| format!(", until {}", relative(at, now)))
            .unwrap_or_default();
        match &self.because {
            HoldCause::Wanted { requests } => format!(
                "{} keeps the pool while {} wait on it",
                self.deployment,
                plural(*requests, "request", "requests")
            ),
            HoldCause::Loading => format!("{} is loading{until}", self.deployment),
            HoldCause::Residency => {
                format!("{} is inside its minimum residency{until}", self.deployment)
            }
            HoldCause::RecentRequest => format!(
                "{} was requested within its idle window and keeps the pool{until}",
                self.deployment
            ),
        }
    }
}

impl Wait {
    /// What the request waits on, in a sentence fragment.
    pub fn describe(&self, now: Ms) -> String {
        match self {
            Self::Unloading { deployments } => {
                format!("unloading {} from its machines", ids_label(deployments))
            }
            Self::Starting => "nothing in the way; the controller is starting it".to_string(),
            Self::Fitting {
                since,
                waiting_on,
                turns,
                other,
            } => {
                let mut said = format!(
                    "waiting to fit since {}, its machines reserved and taking no new batches",
                    relative(*since, now)
                );
                if let Some(on) = waiting_on {
                    said.push_str(&format!("; waiting on {on}"));
                }
                if turns + other > 0 {
                    said.push_str(&format!(
                        "; {} still running on the pool's machines",
                        plural(turns + other, "batch", "batches")
                    ));
                    match turns {
                        0 => {}
                        1 => said.push_str(", 1 of them an agent turn that is never preempted"),
                        n => said.push_str(&format!(
                            ", {n} of them agent turns that are never preempted"
                        )),
                    }
                }
                let gives_up = since + (act::FIT_WAIT_SECS as Ms) * 1_000;
                said.push_str(&format!(
                    "; the start fails {} if it never fits",
                    relative(gives_up, now)
                ));
                said
            }
            Self::Loading { since, bound_at } => format!(
                "loading since {}, given up {} if it is not healthy by then",
                relative(*since, now),
                relative(*bound_at, now)
            ),
            Self::Stopping => "the act side is still stopping its previous run".to_string(),
            Self::Pinned { holder, set } => {
                format!("the pool is pinned to {} by {holder}", set_label(set))
            }
            Self::Held { by } if by.is_empty() => "the pool's loaded models".to_string(),
            Self::Held { by } => by
                .iter()
                .map(|hold| hold.describe(now))
                .collect::<Vec<_>>()
                .join("; "),
            Self::Queued { behind, deployment } => format!(
                "queued behind request #{behind} for {deployment}, which asked first for the same machines"
            ),
        }
    }
}

/// A waiting request in one line, naming the model, the pool and what it
/// waits on.
pub fn describe_waiting(request: &Request, pool: &PoolId, wait: &Wait, now: Ms) -> String {
    let model = if request.model == request.deployment.as_str() {
        request.model.clone()
    } else {
        format!("{} (deployment {})", request.model, request.deployment)
    };
    format!(
        "{model} on pool {pool} for {}, waiting {}: {}",
        request.requester,
        relative(request.at, now).trim_end_matches(" ago"),
        wait.describe(now)
    )
}

impl ControlReason {
    pub fn describe(&self, now: Ms) -> String {
        match self {
            Self::Pin { pin } => format!(
                "pinned to {} by {} {}; it holds against idle unload until released or revoked",
                set_label(&pin.set),
                pin.holder,
                relative(pin.at, now)
            ),
            Self::Controller { why, since } => {
                let when = relative(*since, now);
                match why {
                    Why::Adopted => {
                        format!("adopted what was running when Cairn took control, {when}")
                    }
                    Why::Requested {
                        deployment,
                        request,
                        requester,
                    } => format!("{requester} requested {deployment} (request #{request}) {when}"),
                    Why::Unloaded {
                        deployments,
                        waiting,
                    } => format!(
                        "unloaded {} {when}: no request for its idle window while {} waited for the pool",
                        set_label(deployments),
                        plural(*waiting, "batch", "batches")
                    ),
                    Why::IdleDefault { idle_since } => format!(
                        "loaded the default {when}: no batch work on the pool since {}",
                        relative(*idle_since, now)
                    ),
                    Why::PinReleased { holder } => {
                        format!("kept what {holder}'s pin held when it was released {when}")
                    }
                    Why::LoadFailed { deployment, reason } => {
                        format!("{deployment} failed to load {when}: {reason}")
                    }
                    Why::Removed { deployment } => {
                        format!("{deployment} left the pool {when}")
                    }
                }
            }
        }
    }
}
