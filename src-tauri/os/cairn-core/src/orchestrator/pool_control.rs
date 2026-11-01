//! The cairn-inference pool controller's host: the control records and the
//! request queue in the orch's database, the tick that plans every controlled
//! pool and writes what it decides, and the request API.
//!
//! Each tick reads, per controlled pool, the act side's phase for every
//! deployment ([`super::inference_control::load_states`]), the build pressure
//! on the pool's machines from the fleet
//! ([`crate::fleet::Fleet::machine_set_pressure`]), the pool's requests and its
//! measured loads, then runs the facet's pure [`control::plan`] and writes back
//! the control record (the desired set placement reacts to) and the requests it
//! answered. It then converges the pool: [`control::converge`] says what to
//! stop and start, and each is asked of the act side
//! ([`super::inference_control::request`]), whose refusals stand until the next
//! pass asks again. It claims no machine itself: the act side's holds do.
//!
//! Every read-modify-write of a control record (the tick, a request, a pool
//! patch) holds one lock, so a patch and a tick never overwrite each other.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use cairn_db::turso::params;
use cairn_inference::control::Phase;
use cairn_inference::pool_control::{
    self as control, BuildPressure, EffectiveSettings, Ms, Observed, Plan, PoolControl, Request,
    RequestId, RequestState, Wait,
};
use cairn_inference::{DeploymentId, DeploymentSet, Inventory, Pool, PoolId};

use super::inference::load_inventory;
use super::inference_control::{self, Heads};
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

/// Each deployment's phase as the act side holds it. A deployment with no row
/// is missing from the map: unknown, not stopped, since the act side has never
/// acted on it. A read that fails is an error, never an empty map, so a pass
/// never acts on phases it could not read.
pub(crate) async fn deployment_phases(
    db: &LocalDb,
) -> Result<BTreeMap<DeploymentId, Phase>, String> {
    Ok(inference_control::load_states(db)
        .await?
        .into_iter()
        .map(|(id, row)| (id, row.phase))
        .collect())
}

/// The slowest healthy load the act side recorded for each deployment, in
/// milliseconds.
async fn slowest_loads(db: &LocalDb) -> Result<BTreeMap<DeploymentId, u64>, String> {
    Ok(inference_control::load_history(db)
        .await?
        .into_iter()
        .filter_map(|(id, loads)| {
            loads
                .iter()
                .filter(|load| load.healthy)
                .filter_map(|load| load.load_ms)
                .max()
                .map(|ms| (id, ms))
        })
        .collect())
}

fn pool_pressure(orch: &Orchestrator, pool: &Pool) -> BuildPressure {
    let names: BTreeSet<String> = pool.machines.iter().map(|m| m.name.to_string()).collect();
    orch.fleet.machine_set_pressure(&names)
}

/// The settings `pool` runs with: each the operator left unset derives from
/// the slowest healthy load recorded for a deployment of the pool.
pub fn effective_settings(
    pool: &Pool,
    inventory: &Inventory,
    control: &PoolControl,
    slowest: &BTreeMap<DeploymentId, u64>,
) -> EffectiveSettings {
    control.settings.effective(control::slowest_load_secs(
        pool,
        &inventory.deployments,
        slowest.iter().map(|(id, ms)| (id, *ms)),
    ))
}

/// One controlled pool planned against what is observed now.
#[derive(Debug, Clone)]
pub struct PoolPlan {
    pub settings: EffectiveSettings,
    pub builds: BuildPressure,
    pub plan: Plan,
}

#[allow(clippy::too_many_arguments)]
fn plan_pool(
    inventory: &Inventory,
    pool: &Pool,
    control: &PoolControl,
    requests: &[StoredRequest],
    phases: &BTreeMap<DeploymentId, Phase>,
    slowest: &BTreeMap<DeploymentId, u64>,
    builds: BuildPressure,
    now: Ms,
) -> PoolPlan {
    let settings = effective_settings(pool, inventory, control, slowest);
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
    /// The settings of every pool with a control record, handed back or not.
    pub settings: BTreeMap<PoolId, EffectiveSettings>,
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
    let phases = deployment_phases(db).await?;
    let slowest = slowest_loads(db).await?;
    let plans = controls
        .values()
        .filter(|c| c.controlled)
        .filter_map(|c| {
            let pool = inventory.pools.get(&c.pool)?;
            let builds = pool_pressure(orch, pool);
            Some((
                c.pool.clone(),
                plan_pool(
                    inventory, pool, c, &requests, &phases, &slowest, builds, now,
                ),
            ))
        })
        .collect();
    let settings = controls
        .values()
        .filter_map(|c| {
            let pool = inventory.pools.get(&c.pool)?;
            Some((
                c.pool.clone(),
                effective_settings(pool, inventory, c, &slowest),
            ))
        })
        .collect();
    Ok(ControlView {
        now,
        controls,
        plans,
        settings,
        phases,
        requests,
    })
}

/// One pass over every controlled pool: plan it, write what it decides, and
/// converge it.
pub async fn tick(orch: &Orchestrator) -> Result<(), String> {
    let _held = CONTROL.lock().await;
    pass(
        &orch.db.local,
        orch.fleet.as_ref(),
        &|pool: &Pool| pool_pressure(orch, pool),
        now_ms(),
    )
    .await
}

/// The body of [`tick`], with the fleet's two parts passed in: the heads the
/// act side commands, and the build pressure on a pool's machines.
async fn pass(
    db: &LocalDb,
    heads: &dyn Heads,
    pressure: &(dyn Fn(&Pool) -> BuildPressure + Sync),
    now: Ms,
) -> Result<(), String> {
    let controls = load_controls(db).await?;
    if controls.values().any(|c| c.controlled) {
        let inventory = load_inventory(db).await?;
        let phases = deployment_phases(db).await?;
        let slowest = slowest_loads(db).await?;
        let requests = load_requests(db).await?;
        for control in controls.values().filter(|c| c.controlled) {
            let Some(pool) = inventory.pools.get(&control.pool) else {
                continue;
            };
            let builds = pressure(pool);
            let planned = plan_pool(
                &inventory, pool, control, &requests, &phases, &slowest, builds, now,
            );
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
            converge(db, heads, &inventory, pool, &plan.control, &phases, now).await;
        }
    }
    prune_answered(db, now).await
}

/// Ask the act side for what `control` wants of `pool`: every stop, then
/// every start. The act side decides what may be sent now (a start is refused
/// while another deployment holds one of its machines, a stopping one
/// included), and whatever it refuses is asked again on the next pass.
async fn converge(
    db: &LocalDb,
    heads: &dyn Heads,
    inventory: &Inventory,
    pool: &Pool,
    control: &PoolControl,
    phases: &BTreeMap<DeploymentId, Phase>,
    now: Ms,
) {
    for (id, action) in control::converge(pool, &inventory.deployments, control, phases, now) {
        match inference_control::request(db, heads, id.as_str(), action, now).await {
            Ok(said) => log::info!("inference: pool {} converging: {said}", pool.id),
            Err(why) => log::debug!(
                "inference: pool {} converging: {why}; asked again on the next pass",
                pool.id
            ),
        }
    }
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

/// Ask for the model `model`, by its real name, on behalf of `requester`; a
/// `pool` narrows which of its deployments is meant. The name must then mean
/// exactly one deployment, in a pool under Cairn's control; the controller
/// decides at once, so the returned handle's status already says what the
/// request waits on.
pub async fn request_model(
    orch: &Orchestrator,
    model: &str,
    pool: Option<&PoolId>,
    requester: &str,
) -> Result<RequestHandle, String> {
    let model = model.trim();
    let db = &orch.db.local;
    let inventory = load_inventory(db).await?;
    let (deployment, pool) = deployment_for(&inventory, model, pool)?;
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

/// The one deployment of the model named `model` (its real name), in `pool`
/// when one is given, and the pool it is in; or why there is none. The name and
/// pool must mean exactly one deployment, and that deployment must be in a pool
/// for anything to load it.
pub(crate) fn deployment_for(
    inventory: &Inventory,
    model: &str,
    pool: Option<&PoolId>,
) -> Result<(DeploymentId, PoolId), String> {
    let model = model.trim();
    if model.is_empty() {
        return Err("name the model to load".to_string());
    }
    let placed: Vec<(DeploymentId, Option<PoolId>)> =
        control::deployments_serving(inventory, model)
            .into_iter()
            .map(|id| {
                let pool = inventory
                    .pool_of_deployment(&inventory.deployments[&id])
                    .map(|p| p.id.clone());
                (id, pool)
            })
            .collect();
    if placed.is_empty() {
        return Err(format!(
            "no deployment serves '{model}'; a model is named by its real name, and cairn://inference/recipes lists them"
        ));
    }
    let candidates: Vec<_> = match pool {
        Some(wanted) => placed
            .into_iter()
            .filter(|(_, pool)| pool.as_ref() == Some(wanted))
            .collect(),
        None => placed,
    };
    match candidates.as_slice() {
        [] => Err(format!(
            "no deployment of '{model}' is in pool {}",
            pool.map(PoolId::as_str).unwrap_or_default()
        )),
        [(deployment, Some(pool))] => Ok((deployment.clone(), pool.clone())),
        [(deployment, None)] => Err(format!(
            "{deployment} is in no pool, so nothing can load it"
        )),
        many => {
            let pools: BTreeSet<&str> = many
                .iter()
                .map(|(_, pool)| pool.as_ref().map_or("no pool", PoolId::as_str))
                .collect();
            Err(match pool {
                Some(pool) => format!(
                    "'{model}' has {} deployments in pool {pool}; remove all but one",
                    many.len()
                ),
                None => format!(
                    "'{model}' is deployed in {}; name the pool",
                    pools.into_iter().collect::<Vec<_>>().join(", ")
                ),
            })
        }
    }
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

/// A patch to one pool: its warm model, then its control (taking or handing
/// back control, the settings, a pin, a release or revocation), applied in
/// that order.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolPatch {
    pub pool: PoolId,
    /// The model, by real name, the pool loads when idle; `Some(None)` warms
    /// nothing. Held in the pool record's `default`.
    pub warm: Option<Option<String>>,
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
            warm: None,
            controlled: None,
            settings: Vec::new(),
            pin: None,
            release: false,
            revoke: false,
        }
    }
}

impl PoolPatch {
    /// Whether the patch touches the controller's record, not only the pool's
    /// warm model.
    fn changes_control(&self) -> bool {
        self.controlled.is_some()
            || !self.settings.is_empty()
            || self.pin.is_some()
            || self.release
            || self.revoke
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
                "warm" => {
                    patch.warm = Some(match value {
                        serde_json::Value::Null => None,
                        value => Some(
                            value
                                .as_str()
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .ok_or("warm must be a model's real name, or null for none")?
                                .to_string(),
                        ),
                    })
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
                        "unknown key '{other}'; a pool patch takes pool, warm, controlled, settings, pin, release and revoke"
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

/// Unload `deployment` for `actor`: take it out of what its pool wants (its
/// pin included, whoever holds it), cancel the requests still waiting for it,
/// and stop it. A pool's warm model comes back after the pool's idle window,
/// as the warm rule says.
pub async fn unload(
    orch: &Orchestrator,
    deployment: &DeploymentId,
    actor: &str,
) -> Result<String, String> {
    let db = &orch.db.local;
    let inventory = load_inventory(db).await?;
    let record = inventory
        .deployments
        .get(deployment)
        .ok_or_else(|| format!("there is no deployment {deployment}"))?;
    if let Some(pool) = inventory.pool_of_deployment(record).map(|p| p.id.clone()) {
        let _held = CONTROL.lock().await;
        let now = now_ms();
        let mut controls = load_controls(db).await?;
        if let Some(control) = controls.get_mut(&pool) {
            let before = control::desired(control);
            if control.dismiss(deployment, actor, now) {
                control.advance_generation(&before);
                save_control(db, control, now).await?;
            }
        }
        db.execute(
            "UPDATE inference_requests SET state = 'cancelled', state_at = ?2, reason = ?3
              WHERE deployment = ?1 AND state = 'waiting'",
            params![deployment.to_string(), now, format!("unloaded by {actor}")],
        )
        .await
        .map_err(|error| format!("write inference_requests: {error}"))?;
    }
    inference_control::request_deployment(
        orch,
        deployment.as_str(),
        cairn_inference::control::Request::Stop,
    )
    .await
}

/// One step [`set_pinned`] takes, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PinStep {
    /// Release the pin whoever holds it.
    Revoke,
    /// Release the actor's own pin.
    Release,
    Pin(DeploymentSet),
}

/// What pinning or unpinning `deployment` takes, for `actor`, the operator.
///
/// A pin is the pool's whole wanted set while it stands, so pinning starts
/// from what is pinned, else from what the pool wants now, and nothing running
/// is stopped by it. Unpinning releases the whole pin: its set carries over to
/// the controller, which unloads each member when idle, where taking one
/// member out of a standing pin would stop it at once. A pin another holder
/// placed is revoked first, since the operator outranks it. No steps means
/// nothing to change.
fn pin_steps(
    control: Option<&PoolControl>,
    deployment: &DeploymentId,
    pinned: bool,
    actor: &str,
) -> Vec<PinStep> {
    let pin = control.and_then(|control| control.pin.as_ref());
    let theirs = pin.is_some_and(|pin| pin.holder != actor);
    if pinned {
        let mut set = match pin {
            Some(pin) => pin.set.clone(),
            None => control.and_then(control::desired).unwrap_or_default(),
        };
        if !set.insert(deployment.clone()) && !theirs && pin.is_some() {
            return Vec::new();
        }
        let mut steps = Vec::new();
        if theirs {
            steps.push(PinStep::Revoke);
        }
        steps.push(PinStep::Pin(set));
        steps
    } else if pin.is_some_and(|pin| pin.set.contains(deployment)) {
        vec![if theirs {
            PinStep::Revoke
        } else {
            PinStep::Release
        }]
    } else {
        Vec::new()
    }
}

/// Pin `deployment` against idle unload, or unpin it, for `actor`, the
/// operator; see [`pin_steps`] for what that takes.
pub async fn set_pinned(
    orch: &Orchestrator,
    deployment: &DeploymentId,
    pinned: bool,
    actor: &str,
) -> Result<String, String> {
    let db = &orch.db.local;
    let inventory = load_inventory(db).await?;
    let record = inventory
        .deployments
        .get(deployment)
        .ok_or_else(|| format!("there is no deployment {deployment}"))?;
    let pool = inventory
        .pool_of_deployment(record)
        .map(|p| p.id.clone())
        .ok_or_else(|| format!("{deployment} is in no pool, so there is nothing to pin"))?;
    let control = load_controls(db).await?.remove(&pool);
    let steps = pin_steps(control.as_ref(), deployment, pinned, actor);
    if steps.is_empty() {
        return Ok(format!(
            "{deployment} is {}pinned",
            if pinned { "already " } else { "not " }
        ));
    }
    let mut lines = Vec::new();
    for step in steps {
        let patch = PoolPatch {
            pool: pool.clone(),
            release: step == PinStep::Release,
            revoke: step == PinStep::Revoke,
            pin: match step {
                PinStep::Pin(set) => Some(set),
                _ => None,
            },
            ..PoolPatch::default()
        };
        lines.push(patch_pool(orch, &patch, actor).await?);
    }
    Ok(lines.join("\n"))
}

/// Apply `patch` for `actor` and report what changed, one line each.
pub async fn patch_pool(
    orch: &Orchestrator,
    patch: &PoolPatch,
    actor: &str,
) -> Result<String, String> {
    let db = &orch.db.local;
    let mut lines = Vec::new();
    if let Some(warm) = &patch.warm {
        lines.push(super::inference::set_pool_warm(db, &patch.pool, warm.as_deref()).await?);
    }
    if patch.warm.is_none() || patch.changes_control() {
        let _held = CONTROL.lock().await;
        let now = now_ms();
        let inventory = load_inventory(db).await?;
        let pool = inventory.pools.get(&patch.pool).ok_or_else(|| {
            format!(
                "there is no pool {}; cairn://inference/pools lists them",
                patch.pool
            )
        })?;
        let phases = deployment_phases(db).await?;
        let slowest = control::slowest_load_secs(
            pool,
            &inventory.deployments,
            slowest_loads(db).await?.iter().map(|(id, ms)| (id, *ms)),
        );
        let mut controls = load_controls(db).await?;
        let existing = controls.remove(&patch.pool);
        let (control, control_lines) = apply_patch(
            pool, &inventory, existing, patch, actor, &phases, slowest, now,
        )?;
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
        lines.extend(control_lines);
    }
    tick(orch).await?;
    Ok(lines.join("\n"))
}

/// The pure half of [`patch_pool`]: the record after `patch`, and what changed.
/// `slowest_load_secs` is the pool's slowest measured load, which the unset
/// settings derive from.
#[allow(clippy::too_many_arguments)]
fn apply_patch(
    pool: &Pool,
    inventory: &Inventory,
    existing: Option<PoolControl>,
    patch: &PoolPatch,
    actor: &str,
    phases: &BTreeMap<DeploymentId, Phase>,
    slowest_load_secs: Option<u64>,
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
        let settings = control.settings.effective(slowest_load_secs);
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
        "took control of pool {}: the controller now decides its desired set, starting from {} (what the act side knows is healthy); a deployment of the pool Cairn has never acted on is stopped, which brings it under probing",
        control.pool,
        set_label(&control.active.set())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inventory() -> Inventory {
        let yaml = r#"
models:
  - {name: DeepSeek V4 Flash}
recipes:
  - id: dsv4f-tp2
    model: {name: DeepSeek V4 Flash}
    servedName: deepseek-v4-flash
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 2}
deployments:
  - id: dsv4f-tp2
    recipe: dsv4f-tp2
    machines: [sp1, sp2]
    head: sp1
    apiPort: 8888
    apiPath: /v1
    healthPath: /v1/models
pools:
  - id: sparks
    machines: [{name: sp1}, {name: sp2}]
    default: [dsv4f-tp2]
"#;
        Inventory::from_yaml(yaml).unwrap()
    }

    fn sparks(inventory: &Inventory) -> &Pool {
        &inventory.pools[&PoolId::from("sparks")]
    }

    #[test]
    fn the_operator_pins_from_what_runs_unpins_whole_and_outranks_another_holder() {
        use cairn_inference::pool_control::{Member, Pin};
        let inventory = inventory();
        let id = |s: &str| DeploymentId::from(s);
        let set = |ids: &[&str]| ids.iter().map(|s| id(s)).collect::<DeploymentSet>();
        let mut control = PoolControl::begin(
            sparks(&inventory),
            &inventory.deployments,
            &BTreeMap::new(),
            0,
        );
        control.active.members.insert(id("a"), Member::chosen(0));
        control.active.members.insert(id("b"), Member::chosen(0));

        // Pinning one of two running models pins both, so neither stops.
        assert_eq!(
            pin_steps(Some(&control), &id("a"), true, "operator"),
            [PinStep::Pin(set(&["a", "b"]))]
        );
        // Nothing pinned: nothing to unpin.
        assert!(pin_steps(Some(&control), &id("a"), false, "operator").is_empty());

        control.pin = Some(Pin {
            set: set(&["a", "b"]),
            holder: "operator".into(),
            at: 0,
        });
        // Unpinning one releases the whole pin; its set carries over, and
        // nothing is stopped at once.
        assert_eq!(
            pin_steps(Some(&control), &id("a"), false, "operator"),
            [PinStep::Release]
        );
        assert!(pin_steps(Some(&control), &id("a"), true, "operator").is_empty());

        // An agent's pin is revoked, never a refusal.
        control.pin.as_mut().unwrap().holder = "cairn://p/cairn/1/1/builder".into();
        assert_eq!(
            pin_steps(Some(&control), &id("a"), false, "operator"),
            [PinStep::Revoke]
        );
        assert_eq!(
            pin_steps(Some(&control), &id("c"), true, "operator"),
            [PinStep::Revoke, PinStep::Pin(set(&["a", "b", "c"]))]
        );
        assert_eq!(
            pin_steps(Some(&control), &id("a"), true, "operator"),
            [PinStep::Revoke, PinStep::Pin(set(&["a", "b"]))]
        );
    }

    #[test]
    fn a_request_names_a_model_by_its_real_name_and_a_pool_narrows_it() {
        let mut inventory = inventory();
        let found = |inventory: &Inventory, model: &str, pool: Option<&str>| {
            deployment_for(inventory, model, pool.map(PoolId::from).as_ref())
        };
        assert_eq!(
            found(&inventory, "deepseek v4 flash", None),
            Ok(("dsv4f-tp2".into(), "sparks".into()))
        );
        for internal in ["dsv4f-tp2", "deepseek-v4-flash"] {
            assert!(found(&inventory, internal, None)
                .unwrap_err()
                .starts_with("no deployment serves"));
        }
        // A second deployment of the model, in another pool.
        inventory.upsert(
            Inventory::parse_yaml(
                r#"
deployments:
  - {id: dsv4f-bbs, recipe: dsv4f-tp2, machines: [bbs], head: bbs, apiPort: 8000, apiPath: /v1, healthPath: /v1/models}
pools:
  - {id: bbs, machines: [{name: bbs}]}
"#,
            )
            .unwrap(),
        );
        assert_eq!(
            found(&inventory, "DeepSeek V4 Flash", None),
            Err("'DeepSeek V4 Flash' is deployed in bbs, sparks; name the pool".into())
        );
        assert_eq!(
            found(&inventory, "DeepSeek V4 Flash", Some("bbs")),
            Ok(("dsv4f-bbs".into(), "bbs".into()))
        );
        assert!(found(&inventory, "DeepSeek V4 Flash", Some("halo"))
            .unwrap_err()
            .starts_with("no deployment of"));
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
        let warm = parse(serde_json::json!({"pool": "sparks", "warm": " org/Model "})).unwrap();
        assert_eq!(warm.warm, Some(Some("org/Model".to_string())));
        assert!(!warm.changes_control());
        let cold = parse(serde_json::json!({"pool": "sparks", "warm": null})).unwrap();
        assert_eq!(cold.warm, Some(None));
        assert!(parse(serde_json::json!({"pool": "sparks", "warm": ""})).is_err());
        assert!(parse(serde_json::json!({"controlled": true})).is_err());
        assert!(
            parse(serde_json::json!({"pool": "sparks", "release": true, "revoke": true})).is_err()
        );
    }

    #[test]
    fn taking_control_adopts_what_is_healthy_and_keeps_settings_across_a_hand_back() {
        let inventory = inventory();
        let pool = sparks(&inventory);
        let healthy = BTreeMap::from([(
            DeploymentId::from("dsv4f-tp2"),
            Phase::Healthy {
                since_ms: 0,
                load_ms: None,
            },
        )]);
        let take = PoolPatch {
            pool: "sparks".into(),
            controlled: Some(true),
            settings: vec![(SettingKey::MinResidency, Some(900))],
            ..PoolPatch::default()
        };
        let (control, lines) =
            apply_patch(pool, &inventory, None, &take, "operator", &healthy, None, 5).unwrap();
        assert!(control.controlled);
        assert_eq!(control.active.set(), BTreeSet::from(["dsv4f-tp2".into()]));
        assert_eq!(control.settings.min_residency_secs, Some(900));
        assert_eq!(control.generation, 1, "no controller before, one now");
        assert_eq!(
            lines[0],
            "took control of pool sparks: the controller now decides its desired set, starting from dsv4f-tp2 (what the act side knows is healthy); a deployment of the pool Cairn has never acted on is stopped, which brings it under probing"
        );
        assert_eq!(lines.len(), 2, "took control, then the settings: {lines:?}");
        assert!(
            lines[1].starts_with("settings for pool sparks:"),
            "{lines:?}"
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
            None,
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
            None,
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
            None,
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
            None,
            1,
        )
        .unwrap();
        assert_eq!(lines, vec!["pinned pool sparks to dsv4f-tp2 for operator"]);
        assert_eq!(
            apply_patch(pool, &inventory, Some(control.clone()), &pin(&[]), "cairn://p/cairn/9/1/builder", &none, None, 2)
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
                None,
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
            None,
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

    mod convergence {
        use super::super::*;
        use crate::fleet::DeploymentReportReceived;
        use crate::orchestrator::inference_control::tests::{
            phase, sparks_db, FakeHeads, DSV4F, QWEN, SP1, SP2,
        };
        use crate::orchestrator::inference_control::{on_report, Probes};
        use cairn_common::executor_protocol::{DeploymentOperation, DeploymentPhaseReport};
        use cairn_inference::control::Request as Act;
        use cairn_inference::pool_control::Member;

        fn no_builds(_: &Pool) -> BuildPressure {
            BuildPressure::default()
        }

        async fn report(
            db: &LocalDb,
            heads: &FakeHeads,
            deployment: &str,
            command_id: &str,
            phase: DeploymentPhaseReport,
            now: Ms,
        ) {
            on_report(
                db,
                heads,
                DeploymentReportReceived {
                    executor: SP1.into(),
                    command_id: command_id.into(),
                    deployment: deployment.into(),
                    phase,
                },
                now,
            )
            .await
            .unwrap();
        }

        /// Load `id` through the act side, as a manual start would, taking
        /// `load_ms` from the head receiving the start to health.
        async fn load(db: &LocalDb, heads: &FakeHeads, id: &str, at: Ms, load_ms: Ms) {
            inference_control::request(db, heads, id, Act::Start, at)
                .await
                .unwrap();
            inference_control::tick(db, heads, &mut Probes::default(), at + 1)
                .await
                .unwrap();
            let (_, command, _, _) = heads.last_sent();
            report(
                db,
                heads,
                id,
                &command,
                DeploymentPhaseReport::Healthy { ran_start: true },
                at + 1 + load_ms,
            )
            .await;
            assert_eq!(phase(db, id).await.label(), "healthy");
        }

        /// Take control of `sparks` at `at`, the controller wanting `ids`.
        async fn take_control(db: &LocalDb, ids: &[&str], at: Ms) {
            let inventory = load_inventory(db).await.unwrap();
            let pool = &inventory.pools[&PoolId::from("sparks")];
            let phases = deployment_phases(db).await.unwrap();
            let mut control = PoolControl::begin(pool, &inventory.deployments, &phases, at);
            control.active.members = ids
                .iter()
                .map(|id| (DeploymentId::from(*id), Member::chosen(at)))
                .collect();
            save_control(db, &control, at).await.unwrap();
        }

        async fn known(db: &LocalDb, id: &str) -> bool {
            deployment_phases(db)
                .await
                .unwrap()
                .contains_key(&DeploymentId::from(id))
        }

        fn sent(heads: &FakeHeads) -> Vec<(String, &'static str)> {
            heads
                .sent
                .lock()
                .unwrap()
                .iter()
                .map(|(_, _, deployment, operation)| {
                    let kind = match operation {
                        DeploymentOperation::Start { .. } => "start",
                        DeploymentOperation::Stop { .. } => "stop",
                        _ => "other",
                    };
                    (deployment.clone(), kind)
                })
                .collect()
        }

        #[tokio::test]
        async fn a_pass_stops_what_the_pool_does_not_want_then_starts_what_it_does() {
            let db = sparks_db("pool-control-converge").await;
            let heads = FakeHeads::default();
            heads.attach(SP1, 110, 0);
            heads.attach(SP2, 110, 0);
            load(&db, &heads, QWEN, 1_000, 10_000).await;
            take_control(&db, &[DSV4F], 20_000).await;

            pass(&db, &heads, &no_builds, 30_000).await.unwrap();
            assert_eq!(phase(&db, QWEN).await.label(), "stopping");
            assert_eq!(sent(&heads).last(), Some(&(QWEN.to_string(), "stop")));
            assert!(
                !known(&db, DSV4F).await,
                "the start is refused while qwen36-fast still holds {SP1}"
            );

            pass(&db, &heads, &no_builds, 40_000).await.unwrap();
            assert!(!known(&db, DSV4F).await, "refused again, asked again");
            assert_eq!(
                sent(&heads)
                    .iter()
                    .filter(|(_, kind)| *kind == "stop")
                    .count(),
                1,
                "a stop in flight is not sent twice"
            );

            let (_, stop_id, _, _) = heads.last_sent();
            report(
                &db,
                &heads,
                QWEN,
                &stop_id,
                DeploymentPhaseReport::Stopped,
                45_000,
            )
            .await;
            pass(&db, &heads, &no_builds, 50_000).await.unwrap();
            assert_eq!(phase(&db, DSV4F).await.label(), "reserving");
            assert_eq!(heads.held(), [SP1, SP2]);
        }

        #[tokio::test]
        async fn a_stop_that_ended_failed_is_sent_again() {
            let db = sparks_db("pool-control-restop").await;
            let heads = FakeHeads::default();
            heads.attach(SP1, 110, 0);
            heads.attach(SP2, 110, 0);
            load(&db, &heads, QWEN, 1_000, 10_000).await;
            take_control(&db, &[], 20_000).await;

            *heads.refuse.lock().unwrap() = Some("the link is down".into());
            pass(&db, &heads, &no_builds, 30_000).await.unwrap();
            let Phase::Failed { reason, .. } = phase(&db, QWEN).await else {
                panic!("the unsent stop failed");
            };
            assert!(reason.contains("the link is down"), "{reason}");
            *heads.refuse.lock().unwrap() = None;

            pass(&db, &heads, &no_builds, 60_000).await.unwrap();
            assert_eq!(
                phase(&db, QWEN).await.label(),
                "failed",
                "not asked again within the minute"
            );
            assert_eq!(
                phase(&db, DSV4F).await.label(),
                "stopping",
                "never acted on, and no longer behind a held machine: stopped to bring it under probing"
            );

            pass(&db, &heads, &no_builds, 90_000).await.unwrap();
            assert_eq!(phase(&db, QWEN).await.label(), "stopping");
            assert_eq!(sent(&heads).last(), Some(&(QWEN.to_string(), "stop")));
        }

        const FEDORA: &str = "fedora";
        const EMBED: &str = "qwen3-embed-4b";
        const MIN: Ms = 60_000;

        /// The halo pool as adopted from bginf: its one machine is `fedora`,
        /// which is also a general build executor, and its default is the
        /// llama.cpp embedder.
        async fn halo_db(name: &str) -> LocalDb {
            use crate::orchestrator::inference::{write_inventory, Authoring};
            let db = crate::storage::migrated_test_db(name).await;
            let yaml = r#"
models:
  - {name: Qwen/Qwen3-Embedding-4B-GGUF (Q8_0)}
recipes:
  - {id: qwen3-embed-4b, model: {name: Qwen/Qwen3-Embedding-4B-GGUF (Q8_0)}, servedName: qwen3-embedding-4b, engine: {kind: command, start: ./start-embed.sh, stop: podman rm -f bginf-embed}, requirements: {machines: 1}}
deployments:
  - {id: qwen3-embed-4b, recipe: qwen3-embed-4b, machines: [fedora], head: fedora, apiPort: 8090, apiPath: /v1, healthPath: /health}
pools:
  - {id: halo, machines: [{name: fedora, address: 10.20.20.43}], default: [qwen3-embed-4b]}
"#;
            write_inventory(&db, &Authoring::Replace(yaml.into()))
                .await
                .unwrap();
            db
        }

        fn builds(running_other: u32, waiting: u32) -> impl Fn(&Pool) -> BuildPressure + Sync {
            move |_| BuildPressure {
                running_other,
                waiting,
                ..BuildPressure::default()
            }
        }

        async fn fedora_reports(
            db: &LocalDb,
            heads: &FakeHeads,
            phase: DeploymentPhaseReport,
            now: Ms,
        ) {
            let (_, command_id, _, _) = heads.last_sent();
            on_report(
                db,
                heads,
                DeploymentReportReceived {
                    executor: FEDORA.into(),
                    command_id,
                    deployment: EMBED.into(),
                    phase,
                },
                now,
            )
            .await
            .unwrap();
        }

        /// Tenets 1321 and 1322 on a pool whose one machine also runs builds,
        /// with nothing special about it: the embedder bginf left running is
        /// stopped once, which brings it under probing; the default loads only
        /// after a pool idle window with no batch work; and builds waiting for
        /// the machine unload it once it is idle past its minimum residency.
        #[tokio::test]
        async fn the_controller_takes_halo_and_fedora_builds_whenever_the_embedder_is_unloaded() {
            let db = halo_db("pool-control-halo").await;
            let heads = FakeHeads::default();
            heads.attach(FEDORA, 60, 0);
            let inventory = load_inventory(&db).await.unwrap();
            let halo = &inventory.pools[&PoolId::from("halo")];
            let take = PoolPatch {
                pool: "halo".into(),
                controlled: Some(true),
                ..PoolPatch::default()
            };
            let (control, lines) = apply_patch(
                halo,
                &inventory,
                None,
                &take,
                "operator",
                &BTreeMap::new(),
                None,
                0,
            )
            .unwrap();
            assert!(
                lines == [took_control(&control)]
                    && lines[0].starts_with("took control of pool halo"),
                "{lines:?}"
            );
            save_control(&db, &control, 0).await.unwrap();

            pass(&db, &heads, &builds(1, 2), MIN).await.unwrap();
            assert_eq!(sent(&heads), vec![(EMBED.to_string(), "stop")]);
            fedora_reports(&db, &heads, DeploymentPhaseReport::Stopped, MIN + 5_000).await;
            assert!(heads.held().is_empty(), "fedora is free for builds");

            pass(&db, &heads, &builds(1, 0), 10 * MIN).await.unwrap();
            pass(&db, &heads, &no_builds, 20 * MIN).await.unwrap();
            assert_eq!(sent(&heads).len(), 1, "builds ran there ten minutes ago");
            assert!(heads.held().is_empty());

            pass(&db, &heads, &no_builds, 26 * MIN).await.unwrap();
            assert_eq!(phase(&db, EMBED).await.label(), "reserving");
            assert_eq!(heads.held(), [FEDORA]);
            inference_control::tick(&db, &heads, &mut Probes::default(), 26 * MIN + 1)
                .await
                .unwrap();
            assert_eq!(sent(&heads).last(), Some(&(EMBED.to_string(), "start")));
            fedora_reports(
                &db,
                &heads,
                DeploymentPhaseReport::Healthy { ran_start: true },
                27 * MIN,
            )
            .await;
            assert_eq!(phase(&db, EMBED).await.label(), "healthy");

            pass(&db, &heads, &builds(0, 3), 28 * MIN).await.unwrap();
            assert_eq!(
                phase(&db, EMBED).await.label(),
                "healthy",
                "inside its minimum residency"
            );
            pass(&db, &heads, &builds(0, 3), 60 * MIN).await.unwrap();
            assert_eq!(phase(&db, EMBED).await.label(), "stopping");
            assert_eq!(sent(&heads).last(), Some(&(EMBED.to_string(), "stop")));
        }

        #[tokio::test]
        async fn the_settings_derive_from_the_slowest_healthy_load() {
            let db = sparks_db("pool-control-settings").await;
            let heads = FakeHeads::default();
            heads.attach(SP1, 110, 0);
            heads.attach(SP2, 110, 0);
            load(&db, &heads, QWEN, 1_000, 250_400).await;
            let inventory = load_inventory(&db).await.unwrap();
            let pool = &inventory.pools[&PoolId::from("sparks")];
            let control = PoolControl::begin(pool, &inventory.deployments, &BTreeMap::new(), 0);
            let settings = effective_settings(
                pool,
                &inventory,
                &control,
                &slowest_loads(&db).await.unwrap(),
            );
            assert_eq!(
                settings.load_bound.to_string(),
                "12m 33s (from a measured load of 4m 11s)"
            );
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
