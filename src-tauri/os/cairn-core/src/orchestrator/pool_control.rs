//! The cairn-inference pool controller's host: the control records and the
//! request queue in the orch's database, the tick that plans every controlled
//! pool and writes what it decides, and the request API.
//!
//! Each tick reads, per controlled pool, the act side's phase for every
//! deployment, the build pressure on the pool's machines from the fleet
//! ([`crate::fleet::Fleet::machine_set_pressure`]), and the pool's requests,
//! then runs the facet's pure [`control::plan`] and writes back the control
//! record (the desired set the act side converges on and placement reacts to)
//! and the requests it answered. It starts, stops and claims nothing.
//!
//! Every read-modify-write of a control record (the tick, a request, a pool
//! patch) holds one lock, so a patch and a tick never overwrite each other.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use cairn_db::turso::params;
use cairn_inference::bginf::compare;
use cairn_inference::pool_control::{
    self as control, BuildPressure, EffectiveSettings, Ms, Observed, Phase, Plan, PoolControl,
    Request, RequestId, RequestState, Wait,
};
use cairn_inference::{DeploymentId, DeploymentSet, Inventory, Pool, PoolId};

use super::inference::{link_config, load_inventory, load_link, machine_map, LinkConfig};
use super::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// How often the controller plans every controlled pool.
pub const TICK_INTERVAL: Duration = Duration::from_secs(10);

/// How long an answered request is kept as demand history and on the page.
pub const ANSWERED_KEPT_MS: Ms = 7 * 24 * 3_600_000;

static CONTROL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn now_ms() -> Ms {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as Ms)
        .unwrap_or(0)
}

/// A request as stored, with the pool it was made against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRequest {
    pub pool: PoolId,
    pub request: Request,
}

pub async fn load_controls(db: &LocalDb) -> Result<BTreeMap<PoolId, PoolControl>, String> {
    let rows = db
        .query_all(
            "SELECT pool, body_json FROM inference_pool_control ORDER BY pool",
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("read inference_pool_control: {error}"))?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(pool, body)| match serde_json::from_str::<PoolControl>(&body) {
                Ok(control) => Some((control.pool.clone(), control)),
                Err(error) => {
                    log::warn!(
                        "inference: control row {pool} does not parse and is skipped: {error}"
                    );
                    None
                }
            },
        )
        .collect())
}

async fn save_control(db: &LocalDb, control: &PoolControl, at: Ms) -> Result<(), String> {
    let body = serde_json::to_string(control).expect("control records serialize");
    let generation = i64::try_from(control.generation).unwrap_or(i64::MAX);
    db.execute(
        "INSERT INTO inference_pool_control (pool, body_json, generation, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(pool) DO UPDATE SET body_json = excluded.body_json,
           generation = excluded.generation, updated_at = excluded.updated_at",
        params![control.pool.to_string(), body, generation, at],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write inference_pool_control: {error}"))
}

fn stored_state(state: &str, at: Option<Ms>, reason: Option<String>) -> RequestState {
    let at = at.unwrap_or(0);
    match state {
        "ready" => RequestState::Ready { at },
        "failed" => RequestState::Failed {
            at,
            reason: reason.unwrap_or_default(),
        },
        "cancelled" => RequestState::Cancelled { at },
        _ => RequestState::Waiting,
    }
}

fn state_columns(state: &RequestState) -> (&'static str, Option<Ms>, Option<String>) {
    match state {
        RequestState::Waiting => ("waiting", None, None),
        RequestState::Ready { at } => ("ready", Some(*at), None),
        RequestState::Failed { at, reason } => ("failed", Some(*at), Some(reason.clone())),
        RequestState::Cancelled { at } => ("cancelled", Some(*at), None),
    }
}

/// Every stored request: the waiting ones and the last week's answered ones.
pub async fn load_requests(db: &LocalDb) -> Result<Vec<StoredRequest>, String> {
    db.query_all(
        "SELECT id, pool, deployment, model, requester, created_at, state, state_at, reason
           FROM inference_requests ORDER BY id",
        (),
        |row| {
            Ok(StoredRequest {
                pool: PoolId::from(row.text(1)?),
                request: Request {
                    id: row.i64(0)?,
                    deployment: DeploymentId::from(row.text(2)?),
                    model: row.text(3)?,
                    requester: row.text(4)?,
                    at: row.i64(5)?,
                    state: stored_state(&row.text(6)?, row.opt_i64(7)?, row.opt_text(8)?),
                },
            })
        },
    )
    .await
    .map_err(|error| format!("read inference_requests: {error}"))
}

/// Record that `settled` requests stopped waiting. A request already answered
/// (cancelled meanwhile) keeps its answer.
async fn settle(db: &LocalDb, settled: &[(RequestId, RequestState)]) -> Result<(), String> {
    for (id, state) in settled {
        let (state, at, reason) = state_columns(state);
        db.execute(
            "UPDATE inference_requests SET state = ?2, state_at = ?3, reason = ?4
              WHERE id = ?1 AND state = 'waiting'",
            params![*id, state, at, reason],
        )
        .await
        .map_err(|error| format!("write inference_requests: {error}"))?;
    }
    Ok(())
}

async fn prune_answered(db: &LocalDb, now: Ms) -> Result<(), String> {
    db.execute(
        "DELETE FROM inference_requests WHERE state != 'waiting' AND state_at < ?1",
        params![now - ANSWERED_KEPT_MS],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("prune inference_requests: {error}"))
}

/// Each deployment's phase as the act side reports it.
///
/// The act side that starts and stops deployments itself (cairn/5660) reports
/// phases of its own, failures and stops in progress included; until it is on
/// the base branch, the phases are read from bginf's node reports, the one
/// place a deployment's state is observed today. This is the one function to
/// swap.
pub(crate) async fn deployment_phases(
    orch: &Orchestrator,
    inventory: &Inventory,
) -> BTreeMap<DeploymentId, Phase> {
    let LinkConfig::Configured(link) = link_config(orch) else {
        return BTreeMap::new();
    };
    let state = match load_link(&orch.db.local).await {
        Ok(Some(row)) => row.state(),
        Ok(None) => None,
        Err(error) => {
            log::warn!("inference: the controller could not read bginf's last state: {error}");
            None
        }
    };
    let Some(state) = state else {
        return BTreeMap::new();
    };
    control::phases_observed(
        &inventory.deployments,
        &compare::observations(&state, &machine_map(&link)),
    )
}

fn pool_pressure(orch: &Orchestrator, pool: &Pool) -> BuildPressure {
    let names: BTreeSet<String> = pool.machines.iter().map(|m| m.name.to_string()).collect();
    orch.fleet.machine_set_pressure(&names)
}

/// The settings a pool runs with. Every unset setting derives from measured
/// loads, which the act side records (cairn/5660); until it does, each is the
/// built-in value.
pub fn effective_settings(control: &PoolControl) -> EffectiveSettings {
    control.settings.effective(None)
}

/// One controlled pool planned against what is observed now.
#[derive(Debug, Clone)]
pub struct PoolPlan {
    pub settings: EffectiveSettings,
    pub builds: BuildPressure,
    pub plan: Plan,
}

fn plan_pool(
    inventory: &Inventory,
    pool: &Pool,
    control: &PoolControl,
    requests: &[StoredRequest],
    phases: &BTreeMap<DeploymentId, Phase>,
    builds: BuildPressure,
    now: Ms,
) -> PoolPlan {
    let settings = effective_settings(control);
    let mine: Vec<Request> = requests
        .iter()
        .filter(|r| r.pool == control.pool)
        .map(|r| r.request.clone())
        .collect();
    let plan = control::plan(
        pool,
        &inventory.deployments,
        control,
        &settings,
        &mine,
        Observed {
            now,
            phases,
            builds,
        },
    );
    PoolPlan {
        settings,
        builds,
        plan,
    }
}

/// Everything the controller's parts of the `cairn://inference` pages render:
/// the stored records, and for every controlled pool a plan computed now and
/// not written, so a read shows what the next tick will do.
#[derive(Debug, Clone, Default)]
pub struct ControlView {
    pub now: Ms,
    pub controls: BTreeMap<PoolId, PoolControl>,
    pub plans: BTreeMap<PoolId, PoolPlan>,
    pub phases: BTreeMap<DeploymentId, Phase>,
    pub requests: Vec<StoredRequest>,
}

impl ControlView {
    /// What a waiting request waits on now.
    pub fn wait_of(&self, request: &StoredRequest) -> Option<&Wait> {
        self.plans
            .get(&request.pool)?
            .plan
            .waits
            .get(&request.request.id)
    }
}

pub async fn load_control_view(
    orch: &Orchestrator,
    inventory: &Inventory,
) -> Result<ControlView, String> {
    let db = &orch.db.local;
    let now = now_ms();
    let controls = load_controls(db).await?;
    let requests = load_requests(db).await?;
    let phases = deployment_phases(orch, inventory).await;
    let plans = controls
        .values()
        .filter(|c| c.controlled)
        .filter_map(|c| {
            let pool = inventory.pools.get(&c.pool)?;
            let builds = pool_pressure(orch, pool);
            Some((
                c.pool.clone(),
                plan_pool(inventory, pool, c, &requests, &phases, builds, now),
            ))
        })
        .collect();
    Ok(ControlView {
        now,
        controls,
        plans,
        phases,
        requests,
    })
}

/// One pass over every controlled pool, writing what it decides.
pub async fn tick(orch: &Orchestrator) -> Result<(), String> {
    let _held = CONTROL.lock().await;
    let db = &orch.db.local;
    let now = now_ms();
    let controls = load_controls(db).await?;
    if controls.values().any(|c| c.controlled) {
        let inventory = load_inventory(db).await?;
        let phases = deployment_phases(orch, &inventory).await;
        let requests = load_requests(db).await?;
        for control in controls.values().filter(|c| c.controlled) {
            let Some(pool) = inventory.pools.get(&control.pool) else {
                continue;
            };
            let builds = pool_pressure(orch, pool);
            let planned = plan_pool(&inventory, pool, control, &requests, &phases, builds, now);
            let plan = planned.plan;
            if plan.control.generation != control.generation {
                log::info!(
                    "inference: pool {} now wants {} ({})",
                    control.pool,
                    set_label(&plan.resolution.desired),
                    plan.resolution.reason.describe(now)
                );
            }
            if plan.control != *control {
                save_control(db, &plan.control, now).await?;
            }
            settle(db, &plan.settled).await?;
        }
    }
    prune_answered(db, now).await
}

/// Plan every controlled pool now and every [`TICK_INTERVAL`] for the life of
/// the process.
pub fn spawn_pool_controller(orch: Orchestrator) {
    tokio::spawn(async move {
        loop {
            if let Err(error) = tick(&orch).await {
                log::warn!("inference: pool controller tick failed: {error}");
            }
            tokio::time::sleep(TICK_INTERVAL).await;
        }
    });
}

fn set_label(set: &DeploymentSet) -> String {
    if set.is_empty() {
        "nothing".to_string()
    } else {
        set.iter()
            .map(DeploymentId::as_str)
            .collect::<Vec<_>>()
            .join("+")
    }
}

/// A caller's request for one deployment, as the queue holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHandle {
    pub id: RequestId,
    pub pool: PoolId,
    pub deployment: DeploymentId,
}

/// A request's state now, with what it waits on while it waits.
#[derive(Debug, Clone)]
pub struct RequestStatus {
    pub request: StoredRequest,
    pub wait: Option<Wait>,
    pub now: Ms,
}

impl RequestStatus {
    pub fn describe(&self) -> String {
        let request = &self.request.request;
        match (&request.state, &self.wait) {
            (RequestState::Waiting, Some(wait)) => {
                control::describe_waiting(request, &self.request.pool, wait, self.now)
            }
            (RequestState::Waiting, None) => format!(
                "{} on pool {} is waiting for the controller's next pass",
                request.deployment, self.request.pool
            ),
            (RequestState::Ready { .. }, _) => format!(
                "{} on pool {} is healthy",
                request.deployment, self.request.pool
            ),
            (RequestState::Failed { reason, .. }, _) => format!("failed: {reason}"),
            (RequestState::Cancelled { .. }, _) => "cancelled".to_string(),
        }
    }
}

impl RequestHandle {
    pub async fn status(&self, orch: &Orchestrator) -> Result<RequestStatus, String> {
        request_status(orch, self.id).await
    }

    /// Wait until the request is answered or `timeout` passes, and return its
    /// status then: ready, failed, cancelled, or still waiting with what on.
    pub async fn answered(
        &self,
        orch: &Orchestrator,
        timeout: Duration,
    ) -> Result<RequestStatus, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let status = self.status(orch).await?;
            if status.request.request.state != RequestState::Waiting
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_secs(2).min(timeout)).await;
        }
    }
}

/// Ask for the model `model` on behalf of `requester`. The name must mean
/// exactly one deployment, in a pool under Cairn's control; the controller
/// decides at once, so the returned handle's status already says what the
/// request waits on.
pub async fn request_model(
    orch: &Orchestrator,
    model: &str,
    requester: &str,
) -> Result<RequestHandle, String> {
    let model = model.trim();
    let db = &orch.db.local;
    let inventory = load_inventory(db).await?;
    let (deployment, pool) = deployment_for(&inventory, model)?;
    let id = {
        let _held = CONTROL.lock().await;
        let controls = load_controls(db).await?;
        if !controls.get(&pool).is_some_and(|c| c.controlled) {
            return Err(uncontrolled_refusal(&pool, &deployment));
        }
        insert_request(db, &pool, &deployment, model, requester, now_ms()).await?
    };
    tick(orch).await?;
    Ok(RequestHandle {
        id,
        pool,
        deployment,
    })
}

/// The one deployment `model` names and the pool it is in, or why there is
/// none: the name must mean exactly one deployment, and that deployment must be
/// in a pool for anything to load it.
pub(crate) fn deployment_for(
    inventory: &Inventory,
    model: &str,
) -> Result<(DeploymentId, PoolId), String> {
    let model = model.trim();
    if model.is_empty() {
        return Err("name the model to load".to_string());
    }
    let deployment = match control::deployments_serving(inventory, model).as_slice() {
        [] => {
            return Err(format!(
                "no deployment serves '{model}'; cairn://inference/deployments lists them"
            ))
        }
        [one] => one.clone(),
        many => {
            return Err(format!(
                "'{model}' is served by {}; name the deployment you need",
                many.iter()
                    .map(DeploymentId::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    };
    let pool = inventory
        .pool_of_deployment(&inventory.deployments[&deployment])
        .map(|p| p.id.clone())
        .ok_or_else(|| format!("{deployment} is in no pool, so nothing can load it"))?;
    Ok((deployment, pool))
}

/// Why a request for `deployment` cannot load it: Cairn does not decide what
/// `pool` runs.
pub(crate) fn uncontrolled_refusal(pool: &PoolId, deployment: &DeploymentId) -> String {
    format!(
        "pool {pool} is not under Cairn's control, so nothing would load {deployment}; take control with a patch to cairn://inference/pools {{pool:\"{pool}\", controlled:true}}"
    )
}

async fn insert_request(
    db: &LocalDb,
    pool: &PoolId,
    deployment: &DeploymentId,
    model: &str,
    requester: &str,
    at: Ms,
) -> Result<RequestId, String> {
    let row = (
        pool.to_string(),
        deployment.to_string(),
        model.to_string(),
        requester.to_string(),
    );
    db.write(move |conn| {
        let (pool, deployment, model, requester) = row.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "INSERT INTO inference_requests (pool, deployment, model, requester, created_at, state)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'waiting') RETURNING id",
                    params![pool, deployment, model, requester, at],
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                crate::storage::DbError::Row("INSERT ... RETURNING id returned no row".into())
            })?;
            row.i64(0)
        })
    })
    .await
    .map_err(|error| format!("write inference_requests: {error}"))
}

pub async fn request_status(orch: &Orchestrator, id: RequestId) -> Result<RequestStatus, String> {
    let inventory = load_inventory(&orch.db.local).await?;
    let view = load_control_view(orch, &inventory).await?;
    let request = view
        .requests
        .iter()
        .find(|r| r.request.id == id)
        .cloned()
        .ok_or_else(|| format!("there is no request #{id}"))?;
    Ok(RequestStatus {
        wait: view.wait_of(&request).cloned(),
        request,
        now: view.now,
    })
}

/// Cancel a waiting request.
pub async fn cancel_request(
    orch: &Orchestrator,
    id: RequestId,
    by: &str,
) -> Result<String, String> {
    let db = &orch.db.local;
    let _held = CONTROL.lock().await;
    let changed = db
        .execute(
            "UPDATE inference_requests SET state = 'cancelled', state_at = ?2, reason = ?3
              WHERE id = ?1 AND state = 'waiting'",
            params![id, now_ms(), format!("cancelled by {by}")],
        )
        .await
        .map_err(|error| format!("write inference_requests: {error}"))?;
    if changed == 0 {
        let state = db
            .query_opt_text(
                "SELECT state FROM inference_requests WHERE id = ?1",
                params![id],
            )
            .await
            .map_err(|error| format!("read inference_requests: {error}"))?;
        return Err(match state {
            Some(state) => format!("request #{id} is {state}, not waiting"),
            None => format!("there is no request #{id}"),
        });
    }
    Ok(format!("Cancelled request #{id}"))
}

/// A patch to one pool's control: any of taking or handing back control, the
/// settings, a pin, a release or revocation, applied in that order.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolPatch {
    pub pool: PoolId,
    pub controlled: Option<bool>,
    /// Each key present sets a setting; `None` derives it again.
    pub settings: Vec<(SettingKey, Option<u64>)>,
    pub pin: Option<DeploymentSet>,
    pub release: bool,
    pub revoke: bool,
}

impl Default for PoolPatch {
    fn default() -> Self {
        Self {
            pool: PoolId::from(""),
            controlled: None,
            settings: Vec::new(),
            pin: None,
            release: false,
            revoke: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingKey {
    ModelIdle,
    PoolIdle,
    MinResidency,
    LoadBound,
}

impl SettingKey {
    pub const ALL: [(&'static str, SettingKey); 4] = [
        ("modelIdleSecs", SettingKey::ModelIdle),
        ("poolIdleSecs", SettingKey::PoolIdle),
        ("minResidencySecs", SettingKey::MinResidency),
        ("loadBoundSecs", SettingKey::LoadBound),
    ];

    fn slot(self, settings: &mut control::PoolSettings) -> &mut Option<u64> {
        match self {
            Self::ModelIdle => &mut settings.model_idle_secs,
            Self::PoolIdle => &mut settings.pool_idle_secs,
            Self::MinResidency => &mut settings.min_residency_secs,
            Self::LoadBound => &mut settings.load_bound_secs,
        }
    }
}

impl PoolPatch {
    /// Parse a `cairn://inference/pools` patch payload.
    pub fn parse(payload: &serde_json::Value) -> Result<Self, String> {
        let object = payload.as_object().ok_or("the payload must be an object")?;
        let mut patch = PoolPatch::default();
        for (key, value) in object {
            match key.as_str() {
                "pool" => {
                    patch.pool = value
                        .as_str()
                        .filter(|s| !s.trim().is_empty())
                        .ok_or("pool must name a pool")?
                        .trim()
                        .into();
                }
                "controlled" => {
                    patch.controlled = Some(value.as_bool().ok_or("controlled must be true or false")?)
                }
                "release" => patch.release = value.as_bool().ok_or("release must be true")?,
                "revoke" => patch.revoke = value.as_bool().ok_or("revoke must be true")?,
                "pin" => {
                    let ids = value
                        .as_array()
                        .ok_or("pin must be an array of deployment ids")?;
                    patch.pin = Some(
                        ids.iter()
                            .map(|id| {
                                id.as_str()
                                    .map(DeploymentId::from)
                                    .ok_or("pin must be an array of deployment ids")
                            })
                            .collect::<Result<_, _>>()?,
                    );
                }
                "settings" => {
                    let settings = value.as_object().ok_or("settings must be an object")?;
                    for (name, value) in settings {
                        let key = SettingKey::ALL
                            .iter()
                            .find(|(known, _)| known == name)
                            .map(|(_, key)| *key)
                            .ok_or_else(|| {
                                format!(
                                    "unknown setting '{name}'; the settings are {}",
                                    SettingKey::ALL.map(|(n, _)| n).join(", ")
                                )
                            })?;
                        let secs = match value {
                            serde_json::Value::Null => None,
                            value => Some(value.as_u64().filter(|s| *s > 0).ok_or_else(|| {
                                format!("{name} must be a positive number of seconds, or null to derive it")
                            })?),
                        };
                        patch.settings.push((key, secs));
                    }
                }
                other => {
                    return Err(format!(
                        "unknown key '{other}'; a pool patch takes pool, controlled, settings, pin, release and revoke"
                    ))
                }
            }
        }
        if patch.pool.as_str().is_empty() {
            return Err("pool must name a pool".to_string());
        }
        if patch.release && patch.revoke {
            return Err("release your own pin or revoke one, not both".to_string());
        }
        Ok(patch)
    }
}

/// Apply `patch` for `actor` and report what changed, one line each.
pub async fn patch_pool(
    orch: &Orchestrator,
    patch: &PoolPatch,
    actor: &str,
) -> Result<String, String> {
    let db = &orch.db.local;
    let lines = {
        let _held = CONTROL.lock().await;
        let now = now_ms();
        let inventory = load_inventory(db).await?;
        let pool = inventory.pools.get(&patch.pool).ok_or_else(|| {
            format!(
                "there is no pool {}; cairn://inference/pools lists them",
                patch.pool
            )
        })?;
        let phases = deployment_phases(orch, &inventory).await;
        let mut controls = load_controls(db).await?;
        let existing = controls.remove(&patch.pool);
        let (control, lines) = apply_patch(pool, &inventory, existing, patch, actor, &phases, now)?;
        save_control(db, &control, now).await?;
        if !control.controlled {
            db.execute(
                "UPDATE inference_requests SET state = 'cancelled', state_at = ?2,
                        reason = 'the pool left Cairn''s control'
                  WHERE pool = ?1 AND state = 'waiting'",
                params![control.pool.to_string(), now],
            )
            .await
            .map_err(|error| format!("write inference_requests: {error}"))?;
        }
        lines
    };
    tick(orch).await?;
    Ok(lines.join("\n"))
}

/// The pure half of [`patch_pool`]: the record after `patch`, and what changed.
fn apply_patch(
    pool: &Pool,
    inventory: &Inventory,
    existing: Option<PoolControl>,
    patch: &PoolPatch,
    actor: &str,
    phases: &BTreeMap<DeploymentId, Phase>,
    now: Ms,
) -> Result<(PoolControl, Vec<String>), String> {
    let mut lines = Vec::new();
    let before = existing.as_ref().and_then(control::desired);
    let mut control = match (existing, patch.controlled) {
        (Some(control), Some(true)) if control.controlled => control,
        (Some(previous), Some(true)) => {
            let mut control = PoolControl::begin(pool, &inventory.deployments, phases, now);
            control.settings = previous.settings;
            control.pin = previous.pin;
            control.generation = previous.generation;
            lines.push(took_control(&control));
            control
        }
        (None, Some(true)) => {
            let control = PoolControl::begin(pool, &inventory.deployments, phases, now);
            lines.push(took_control(&control));
            control
        }
        (Some(control), _) => control,
        (None, _) => {
            let mut control = PoolControl::begin(pool, &inventory.deployments, phases, now);
            control.controlled = false;
            control
        }
    };
    if patch.controlled == Some(true) && pool.pin.as_ref().is_none_or(|pin| !pin.is_empty()) {
        lines.push(format!(
            "warning: bginf still drives pool {} (its pin is {}, not off); pin bginf to nothing for this pool or stop it, or bginf and Cairn will fight over it",
            pool.id,
            pool.pin.as_ref().map_or("unset".to_string(), set_label)
        ));
    }
    if patch.controlled == Some(false) && control.controlled {
        control.controlled = false;
        lines.push(format!(
            "handed pool {} back: the controller no longer decides it, and its waiting requests are cancelled",
            pool.id
        ));
    }
    for (key, secs) in &patch.settings {
        *key.slot(&mut control.settings) = *secs;
    }
    if !patch.settings.is_empty() {
        let settings = effective_settings(&control);
        lines.push(format!(
            "settings for pool {}: model idle {}, pool idle {}, minimum residency {}, load bound {}",
            pool.id,
            settings.model_idle,
            settings.pool_idle,
            settings.min_residency,
            settings.load_bound
        ));
    }
    if let Some(set) = &patch.pin {
        if !control.controlled {
            return Err(format!(
                "pool {} is not under Cairn's control; take control first (controlled:true)",
                pool.id
            ));
        }
        control::check_set(pool, &inventory.deployments, set)?;
        control
            .pin(set.clone(), actor, now)
            .map_err(|refused| refused.to_string())?;
        lines.push(format!(
            "pinned pool {} to {} for {actor}",
            pool.id,
            set_label(set)
        ));
    }
    if patch.release || patch.revoke {
        let pin = control
            .release(actor, patch.revoke, phases, now)
            .map_err(|refused| refused.to_string())?;
        lines.push(format!(
            "{} {}'s pin on pool {} ({}); the controller carries on from it",
            if patch.revoke { "revoked" } else { "released" },
            pin.holder,
            pool.id,
            set_label(&pin.set)
        ));
    }
    if lines.is_empty() {
        lines.push(format!("pool {} unchanged", pool.id));
    }
    control.advance_generation(&before);
    Ok((control, lines))
}

fn took_control(control: &PoolControl) -> String {
    format!(
        "took control of pool {}: the controller now decides its desired set, starting from {} (what was healthy)",
        control.pool,
        set_label(&control.active.set())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inventory() -> Inventory {
        let yaml = r#"
recipes:
  - id: dsv4f-tp2
    source: bginf
    model: {name: DeepSeek V4 Flash}
    servedName: deepseek-v4-flash
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 2}
deployments:
  - id: dsv4f-tp2
    source: bginf
    recipe: dsv4f-tp2
    machines: [sp1, sp2]
    head: sp1
    apiPort: 8888
    apiPath: /v1
    healthPath: /v1/models
pools:
  - id: sparks
    source: bginf
    machines: [{name: sp1}, {name: sp2}]
    default: [dsv4f-tp2]
    pin: []
"#;
        Inventory::from_yaml(yaml).unwrap()
    }

    fn sparks(inventory: &Inventory) -> &Pool {
        &inventory.pools[&PoolId::from("sparks")]
    }

    fn parse(value: serde_json::Value) -> Result<PoolPatch, String> {
        PoolPatch::parse(&value)
    }

    #[test]
    fn a_pool_patch_parses_every_key_and_refuses_strangers() {
        let patch = parse(serde_json::json!({
            "pool": "sparks",
            "controlled": true,
            "settings": {"modelIdleSecs": 1800, "loadBoundSecs": null},
            "pin": ["dsv4f-tp2"]
        }))
        .unwrap();
        assert_eq!(patch.pool, PoolId::from("sparks"));
        assert_eq!(patch.controlled, Some(true));
        assert_eq!(
            patch.settings,
            vec![
                (SettingKey::LoadBound, None),
                (SettingKey::ModelIdle, Some(1800))
            ]
        );
        assert_eq!(patch.pin, Some(BTreeSet::from(["dsv4f-tp2".into()])));
        assert!(
            parse(serde_json::json!({"pool": "sparks", "settings": {"idle": 5}}))
                .unwrap_err()
                .starts_with("unknown setting 'idle'")
        );
        assert!(
            parse(serde_json::json!({"pool": "sparks", "settings": {"poolIdleSecs": 0}})).is_err()
        );
        assert!(parse(serde_json::json!({"pool": "sparks", "holder": "me"})).is_err());
        assert!(parse(serde_json::json!({"controlled": true})).is_err());
        assert!(
            parse(serde_json::json!({"pool": "sparks", "release": true, "revoke": true})).is_err()
        );
    }

    #[test]
    fn taking_control_adopts_what_is_healthy_and_keeps_settings_across_a_hand_back() {
        let inventory = inventory();
        let pool = sparks(&inventory);
        let healthy = BTreeMap::from([(DeploymentId::from("dsv4f-tp2"), Phase::Healthy)]);
        let take = PoolPatch {
            pool: "sparks".into(),
            controlled: Some(true),
            settings: vec![(SettingKey::MinResidency, Some(900))],
            ..PoolPatch::default()
        };
        let (control, lines) =
            apply_patch(pool, &inventory, None, &take, "operator", &healthy, 5).unwrap();
        assert!(control.controlled);
        assert_eq!(control.active.set(), BTreeSet::from(["dsv4f-tp2".into()]));
        assert_eq!(control.settings.min_residency_secs, Some(900));
        assert_eq!(control.generation, 1, "no controller before, one now");
        assert_eq!(
            lines[0],
            "took control of pool sparks: the controller now decides its desired set, starting from dsv4f-tp2 (what was healthy)"
        );
        assert!(
            !lines.iter().any(|l| l.starts_with("warning")),
            "bginf is pinned off for the pool: {lines:?}"
        );

        let back = PoolPatch {
            pool: "sparks".into(),
            controlled: Some(false),
            ..PoolPatch::default()
        };
        let (control, _) = apply_patch(
            pool,
            &inventory,
            Some(control),
            &back,
            "operator",
            &healthy,
            6,
        )
        .unwrap();
        assert!(!control.controlled);
        assert_eq!(control::desired(&control), None);
        assert_eq!(control.generation, 2);
        let (control, _) = apply_patch(
            pool,
            &inventory,
            Some(control),
            &take,
            "operator",
            &BTreeMap::new(),
            7,
        )
        .unwrap();
        assert_eq!(
            control.active.set(),
            BTreeSet::new(),
            "re-adopted, nothing healthy now"
        );
        assert_eq!(control.settings.min_residency_secs, Some(900));
        assert_eq!(control.generation, 3, "the generation never goes back");
    }

    #[test]
    fn taking_control_warns_while_bginf_still_drives_the_pool() {
        let mut inventory = inventory();
        inventory
            .pools
            .get_mut(&PoolId::from("sparks"))
            .unwrap()
            .pin = None;
        let take = PoolPatch {
            pool: "sparks".into(),
            controlled: Some(true),
            ..PoolPatch::default()
        };
        let (_, lines) = apply_patch(
            sparks(&inventory),
            &inventory,
            None,
            &take,
            "operator",
            &BTreeMap::new(),
            0,
        )
        .unwrap();
        assert_eq!(
            lines[1],
            "warning: bginf still drives pool sparks (its pin is unset, not off); pin bginf to nothing for this pool or stop it, or bginf and Cairn will fight over it"
        );
    }

    #[test]
    fn a_pin_needs_control_and_a_second_holder_is_refused() {
        let inventory = inventory();
        let pool = sparks(&inventory);
        let none = BTreeMap::new();
        let pin = |set: &[&str]| PoolPatch {
            pool: "sparks".into(),
            pin: Some(set.iter().map(|id| DeploymentId::from(*id)).collect()),
            ..PoolPatch::default()
        };
        assert!(apply_patch(
            pool,
            &inventory,
            None,
            &pin(&["dsv4f-tp2"]),
            "operator",
            &none,
            0
        )
        .unwrap_err()
        .contains("not under Cairn's control"));

        let control = PoolControl::begin(pool, &inventory.deployments, &none, 0);
        let (control, lines) = apply_patch(
            pool,
            &inventory,
            Some(control),
            &pin(&["dsv4f-tp2"]),
            "operator",
            &none,
            1,
        )
        .unwrap();
        assert_eq!(lines, vec!["pinned pool sparks to dsv4f-tp2 for operator"]);
        assert_eq!(
            apply_patch(pool, &inventory, Some(control.clone()), &pin(&[]), "cairn://p/cairn/9/1/builder", &none, 2)
                .unwrap_err(),
            "the pool is pinned to dsv4f-tp2 by operator; only they can release it, or revoke it explicitly"
        );
        assert_eq!(
            apply_patch(
                pool,
                &inventory,
                Some(control.clone()),
                &pin(&["nope"]),
                "operator",
                &none,
                2
            )
            .unwrap_err(),
            "there is no deployment nope"
        );
        let revoke = PoolPatch {
            pool: "sparks".into(),
            revoke: true,
            ..PoolPatch::default()
        };
        let (control, lines) = apply_patch(
            pool,
            &inventory,
            Some(control),
            &revoke,
            "cairn://p/cairn/9/1/builder",
            &none,
            3,
        )
        .unwrap();
        assert_eq!(control.pin, None);
        assert_eq!(
            lines,
            vec!["revoked operator's pin on pool sparks (dsv4f-tp2); the controller carries on from it"]
        );
    }

    #[test]
    fn stored_request_states_round_trip() {
        for state in [
            RequestState::Waiting,
            RequestState::Ready { at: 5 },
            RequestState::Failed {
                at: 6,
                reason: "exit 1".into(),
            },
            RequestState::Cancelled { at: 7 },
        ] {
            let (name, at, reason) = state_columns(&state);
            assert_eq!(stored_state(name, at, reason), state);
        }
    }

    #[tokio::test]
    async fn control_rows_and_requests_persist() {
        let db = crate::storage::migrated_test_db("inference-control").await;
        let inventory = inventory();
        let mut control = PoolControl::begin(
            sparks(&inventory),
            &inventory.deployments,
            &BTreeMap::new(),
            1,
        );
        control.settings.pool_idle_secs = Some(60);
        control.generation = 4;
        save_control(&db, &control, 1).await.unwrap();
        assert_eq!(
            load_controls(&db).await.unwrap(),
            BTreeMap::from([(PoolId::from("sparks"), control)])
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT generation FROM inference_pool_control WHERE pool = 'sparks'",
                ()
            )
            .await
            .unwrap(),
            Some(4),
            "the column placement watches mirrors the record"
        );

        let first = insert_request(
            &db,
            &"sparks".into(),
            &"dsv4f-tp2".into(),
            "deepseek",
            "me",
            10,
        )
        .await
        .unwrap();
        let second = insert_request(
            &db,
            &"sparks".into(),
            &"dsv4f-tp2".into(),
            "deepseek",
            "you",
            11,
        )
        .await
        .unwrap();
        assert!(second > first);
        settle(&db, &[(first, RequestState::Ready { at: 20 })])
            .await
            .unwrap();
        settle(&db, &[(second, RequestState::Cancelled { at: 21 })])
            .await
            .unwrap();
        // An answered request keeps its answer.
        settle(&db, &[(second, RequestState::Ready { at: 22 })])
            .await
            .unwrap();
        let requests = load_requests(&db).await.unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|r| r.request.state.clone())
                .collect::<Vec<_>>(),
            vec![
                RequestState::Ready { at: 20 },
                RequestState::Cancelled { at: 21 }
            ]
        );
        assert_eq!(requests[0].request.model, "deepseek");

        prune_answered(&db, 20 + ANSWERED_KEPT_MS + 1)
            .await
            .unwrap();
        let left = load_requests(&db).await.unwrap();
        assert_eq!(
            left.len(),
            1,
            "only the one answered at 21 is within the week"
        );
    }
}
