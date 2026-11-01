//! Verifying a recipe on a pool: load it, check its health, benchmark it, and
//! record what it measured, its memory per machine, load time and speed, or
//! the stage it failed at and why.
//!
//! A verification runs as a background task and returns its id at once. It
//! loads the recipe as deployment `verify-<recipe>-<pool>` on the pool's first
//! machines through the same act side a deployment's own page uses, so
//! reservation, fit and drain are unchanged. A pool under the controller is
//! held by pinning it to that deployment as `verify:<id>`; one pinned by
//! anyone else is refused, naming them. A pool not under control has whatever
//! holds its machines stopped first and started again afterwards. Every path
//! ends with the deployment stopped, the pin released and the pool restored.
//!
//! A start that fails because the context does not fit the KV cache (vLLM
//! names the cache's size) is recorded, the recipe is revised in place with
//! its context clamped to that size, and the revision is verified next. A pass
//! whose measured cache is smaller than the recipe's context clamps it too.
//! Cairn never changes a host's drivers: a driver or runtime mismatch is the
//! recorded reason, not something fixed.
//!
//! A verification left running when the orch stops is failed at the next boot
//! with "orch restarted", and its deployment and pin are cleaned up.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use cairn_db::turso::params;
use cairn_inference::control::{Phase, Request, START_REPORT_BOUND_SECS, STOP_REPORT_BOUND_SECS};
use cairn_inference::pool_control;
use cairn_inference::verify::{self, ChatSample, EmbedSample, Speeds, Stage};
use cairn_inference::{
    AcceleratorFamily, Deployment, DeploymentId, Engine, Inventory, PoolId, Recipe, RecipeId,
    RecipeOrigin, RecordSource, Server,
};
use cairn_models::types::catalog::VerbKind;
use sha2::{Digest, Sha256};

use super::inference::{load_inventory, write_inventory, Authoring, Removals};
use super::inference_control::{load_history, load_states, SETTLE_MS};
use crate::storage::{LocalDb, RowExt};

pub type Pending<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Everything a verification does to the world.
pub trait Act: Send + Sync {
    fn start(&self, deployment: String) -> Pending<'_, Result<String, String>>;
    fn stop(&self, deployment: String) -> Pending<'_, Result<String, String>>;
    /// Pin a controlled pool to `set` for `holder`.
    fn pin(
        &self,
        pool: String,
        set: Vec<String>,
        holder: String,
    ) -> Pending<'_, Result<String, String>>;
    fn release(&self, pool: String, holder: String) -> Pending<'_, Result<String, String>>;
    /// A GET's body, `None` when it did not answer successfully.
    fn get(&self, url: String) -> Pending<'_, Option<String>>;
    fn chat(&self, api_base: String, model: String) -> Pending<'_, Result<ChatSample, String>>;
    fn embed(&self, api_base: String, model: String) -> Pending<'_, Result<EmbedSample, String>>;
    fn sleep(&self, ms: u64) -> Pending<'_, ()>;
    fn now_ms(&self) -> i64;
}

const KEPT: i64 = 20;
const POLL_MS: u64 = 5_000;

fn holder(id: &str) -> String {
    format!("verify:{id}")
}

fn deployment_id(recipe: &RecipeId, pool: &PoolId) -> DeploymentId {
    DeploymentId::new(format!("verify-{recipe}-{pool}"))
}

/// The sha256 of a recipe's canonical JSON.
pub fn digest(recipe: &Recipe) -> String {
    Sha256::digest(verify::canonical(recipe).as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// One stored verification.
#[derive(Debug, Clone, PartialEq)]
pub struct VerificationRow {
    pub id: String,
    pub recipe: String,
    pub recipe_digest: String,
    pub pool: String,
    pub deployment: String,
    pub machines: Vec<String>,
    pub requested_by: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub outcome: String,
    pub stage: String,
    pub reason: Option<String>,
    pub load_ms: Option<i64>,
    pub footprint: Option<BTreeMap<String, u64>>,
    pub context_tokens: Option<i64>,
    pub kv_tokens: Option<i64>,
    pub ttft_ms: Option<i64>,
    pub decode_tps: Option<f64>,
    pub prefill_tps: Option<f64>,
    pub embed_tps: Option<f64>,
    pub engine_version: Option<String>,
}

impl VerificationRow {
    /// Whether its recipe has changed since it ran.
    pub fn stale(&self, inventory: &Inventory) -> bool {
        inventory
            .recipes
            .get(&RecipeId::from(self.recipe.as_str()))
            .is_none_or(|recipe| digest(recipe) != self.recipe_digest)
    }
}

const COLUMNS: &str =
    "id, recipe_id, recipe_digest, pool_id, deployment_id, machines_json, requested_by,
    started_at, finished_at, outcome, stage, reason, load_ms, footprint_json, context_tokens,
    kv_tokens, ttft_ms, decode_tps, prefill_tps, embed_tps, engine_version";

fn row_of(row: &cairn_db::turso::Row) -> Result<VerificationRow, cairn_db::storage::DbError> {
    Ok(VerificationRow {
        id: row.text(0)?,
        recipe: row.text(1)?,
        recipe_digest: row.text(2)?,
        pool: row.text(3)?,
        deployment: row.text(4)?,
        machines: serde_json::from_str(&row.text(5)?).unwrap_or_default(),
        requested_by: row.text(6)?,
        started_at: row.i64(7)?,
        finished_at: row.opt_i64(8)?,
        outcome: row.text(9)?,
        stage: row.text(10)?,
        reason: row.opt_text(11)?,
        load_ms: row.opt_i64(12)?,
        footprint: row
            .opt_text(13)?
            .and_then(|json| serde_json::from_str(&json).ok()),
        context_tokens: row.opt_i64(14)?,
        kv_tokens: row.opt_i64(15)?,
        ttft_ms: row.opt_i64(16)?,
        decode_tps: row.opt_f64(17)?,
        prefill_tps: row.opt_f64(18)?,
        embed_tps: row.opt_f64(19)?,
        engine_version: row.opt_text(20)?,
    })
}

/// Every stored verification, newest first.
pub async fn load_verifications(db: &LocalDb) -> Result<Vec<VerificationRow>, String> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM inference_verifications ORDER BY started_at DESC, id DESC"),
        (),
        row_of,
    )
    .await
    .map_err(|error| format!("read inference_verifications: {error}"))
}

async fn set_stage(db: &LocalDb, id: &str, stage: Stage) -> Result<(), String> {
    db.execute(
        "UPDATE inference_verifications SET stage = ?2 WHERE id = ?1",
        params![id, stage.as_str()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write inference_verifications: {error}"))
}

async fn finish(
    db: &LocalDb,
    id: &str,
    passed: bool,
    reason: Option<String>,
    at: i64,
) -> Result<(), String> {
    db.execute(
        "UPDATE inference_verifications SET outcome = ?2, reason = ?3, finished_at = ?4,
                stage = CASE WHEN ?2 = 'passed' THEN 'done' ELSE stage END
          WHERE id = ?1",
        params![id, if passed { "passed" } else { "failed" }, reason, at],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write inference_verifications: {error}"))
}

/// What was measured, written as it is learned.
#[derive(Debug, Default)]
struct Measured {
    load_ms: Option<u64>,
    footprint: Option<BTreeMap<String, u64>>,
    context: Option<u64>,
    kv: Option<u64>,
    version: Option<String>,
    speeds: Speeds,
}

async fn record(db: &LocalDb, id: &str, m: &Measured) -> Result<(), String> {
    db.execute(
        "UPDATE inference_verifications SET load_ms = ?2, footprint_json = ?3, context_tokens = ?4,
                kv_tokens = ?5, ttft_ms = ?6, decode_tps = ?7, prefill_tps = ?8, embed_tps = ?9,
                engine_version = ?10
          WHERE id = ?1",
        params![
            id,
            m.load_ms.map(|v| v as i64),
            m.footprint
                .as_ref()
                .map(|f| serde_json::to_string(f).expect("footprints serialize")),
            m.context.map(|v| v as i64),
            m.kv.map(|v| v as i64),
            m.speeds.ttft_ms.map(|v| v.round() as i64),
            m.speeds.decode_tps,
            m.speeds.prefill_tps,
            m.speeds.embed_tps,
            m.version.clone()
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write inference_verifications: {error}"))
}

/// What one verification is of.
#[derive(Debug, Clone)]
pub struct Planned {
    pub id: String,
    /// When it began: a phase older than this belongs to an earlier run of the
    /// same deployment and says nothing about this one.
    pub started_at: i64,
    pub recipe: RecipeId,
    pub pool: PoolId,
    pub deployment: DeploymentId,
}

fn port_for(recipe: &Recipe) -> u16 {
    match &recipe.engine {
        Engine::Container(container) if container.server == Server::LlamaCpp => 8090,
        _ => 8000,
    }
}

/// Refuse a verification that cannot start, write its deployment and its row,
/// and return what to run.
pub async fn begin(
    db: &LocalDb,
    recipe_id: &str,
    pool_id: &str,
    requested_by: &str,
    now: i64,
) -> Result<Planned, String> {
    let inventory = load_inventory(db).await?;
    let recipe = inventory
        .recipes
        .get(&RecipeId::from(recipe_id))
        .ok_or_else(|| {
            format!("there is no recipe {recipe_id}; cairn://inference/recipes lists them")
        })?
        .clone();
    let pool = inventory
        .pools
        .get(&PoolId::from(pool_id))
        .ok_or_else(|| format!("there is no pool {pool_id}; cairn://inference/pools lists them"))?
        .clone();
    if !matches!(recipe.engine, Engine::Container(_)) {
        return Err(format!(
            "recipe {recipe_id} is a {} recipe; verification measures container recipes",
            recipe.engine.kind()
        ));
    }
    let k = recipe.requirements.machines as usize;
    if pool.machines.len() < k {
        return Err(format!(
            "recipe {recipe_id} runs on {k} machines and pool {pool_id} has {}",
            pool.machines.len()
        ));
    }
    let id = uuid::Uuid::new_v4().to_string();
    if let Some(control) = super::pool_control::load_controls(db).await?.get(&pool.id) {
        if let Some(pin) = control.pin.as_ref().filter(|_| control.controlled) {
            if !pin.holder.starts_with("verify:") {
                return Err(format!(
                    "pool {pool_id} is pinned by {}; verification needs the pool to itself, so it waits for that pin to be released",
                    pin.holder
                ));
            }
            return Err(format!(
                "pool {pool_id} is already being verified ({}); wait for it to finish",
                pin.holder
            ));
        }
    }
    let machines: Vec<_> = pool
        .machines
        .iter()
        .take(k)
        .map(|m| m.name.clone())
        .collect();
    let deployment = Deployment {
        id: deployment_id(&recipe.id, &pool.id),
        source: RecordSource::Cairn,
        recipe: recipe.id.clone(),
        head: machines[0].clone(),
        machines: machines.clone(),
        api_port: port_for(&recipe),
        api_path: "/v1".into(),
        health_path: "/v1/models".into(),
    };
    let mut records = Inventory::default();
    records.insert_deployment(deployment.clone());
    write_inventory(
        db,
        &Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove: Removals::default(),
        },
    )
    .await?;
    let machines_json =
        serde_json::to_string(&machines.iter().map(|m| m.to_string()).collect::<Vec<_>>())
            .expect("machine names serialize");
    let (recipe_key, pool_key) = (recipe.id.to_string(), pool.id.to_string());
    let (row_id, digest, requester, deployment_key) = (
        id.clone(),
        digest(&recipe),
        requested_by.to_string(),
        deployment.id.to_string(),
    );
    let busy: std::sync::Arc<std::sync::Mutex<Option<String>>> = Default::default();
    let busy_in = std::sync::Arc::clone(&busy);
    db.write(move |conn| {
        let busy = std::sync::Arc::clone(&busy_in);
        let (row_id, recipe_key, digest, pool_key, deployment_key, machines_json, requester) = (
            row_id.clone(),
            recipe_key.clone(),
            digest.clone(),
            pool_key.clone(),
            deployment_key.clone(),
            machines_json.clone(),
            requester.clone(),
        );
        Box::pin(async move {
            let mut running = conn
                .query(
                    "SELECT id FROM inference_verifications
                      WHERE pool_id = ?1 AND outcome = 'running' LIMIT 1",
                    params![pool_key.clone()],
                )
                .await?;
            if let Some(row) = running.next().await? {
                *busy.lock().unwrap() = Some(row.text(0)?);
                return Ok(());
            }
            drop(running);
            conn.execute(
                "INSERT INTO inference_verifications
                   (id, recipe_id, recipe_digest, pool_id, deployment_id, machines_json,
                    requested_by, started_at, outcome, stage)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'running', 'render')",
                params![
                    row_id,
                    recipe_key.clone(),
                    digest,
                    pool_key.clone(),
                    deployment_key,
                    machines_json,
                    requester,
                    now
                ],
            )
            .await?;
            conn.execute(
                "DELETE FROM inference_verifications
                  WHERE recipe_id = ?1 AND pool_id = ?2 AND outcome != 'running'
                    AND id NOT IN (SELECT id FROM inference_verifications
                                    WHERE recipe_id = ?1 AND pool_id = ?2
                                    ORDER BY started_at DESC LIMIT ?3)",
                params![recipe_key, pool_key, KEPT],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("write inference_verifications: {error}"))?;
    if let Some(running) = busy.lock().unwrap().take() {
        return Err(format!(
            "pool {pool_id} is already being verified (verification {running}); wait for it to finish"
        ));
    }
    if let Some(problem) = super::inference_control::render_problem(&inventory, &deployment) {
        let reason = format!("it cannot be rendered into scripts for its head: {problem}");
        finish(db, &id, false, Some(reason.clone()), now).await?;
        return Err(format!(
            "verification {id} failed at render, before anything was stopped: {reason}"
        ));
    }
    Ok(Planned {
        id,
        started_at: now,
        recipe: recipe.id,
        pool: pool.id,
        deployment: deployment.id,
    })
}

/// How one verification ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    Failed(String),
    /// It failed because the context did not fit; the recipe was clamped and
    /// should be verified again.
    Clamped {
        reason: String,
        context: u64,
    },
}

async fn phase(db: &LocalDb, deployment: &DeploymentId) -> Result<Option<Phase>, String> {
    Ok(load_states(db)
        .await?
        .get(deployment)
        .map(|row| row.phase.clone()))
}

async fn wait_for(
    db: &LocalDb,
    act: &dyn Act,
    deployment: &DeploymentId,
    bound_secs: u64,
    done: impl Fn(&Phase) -> bool,
) -> Result<Option<Phase>, String> {
    let deadline = act.now_ms() + bound_secs as i64 * 1_000;
    loop {
        let current = phase(db, deployment).await?;
        if current.as_ref().is_some_and(&done) {
            return Ok(current);
        }
        if act.now_ms() >= deadline {
            return Ok(current);
        }
        act.sleep(POLL_MS).await;
    }
}

/// What the pool ran before, to be put back.
enum Restore {
    Controlled {
        previous: BTreeSet<String>,
    },
    Uncontrolled {
        stopped: Vec<DeploymentId>,
        /// Whether the verification's own deployment was asked to start.
        started: bool,
    },
}

/// Hold the pool for `plan` and start its deployment. A failure says what
/// was already changed, so it is put back.
async fn hold(
    db: &LocalDb,
    act: &dyn Act,
    plan: &Planned,
) -> Result<Restore, (Option<Restore>, String)> {
    let controls = super::pool_control::load_controls(db)
        .await
        .map_err(|why| (None, why))?;
    if let Some(control) = controls.get(&plan.pool).filter(|c| c.controlled) {
        let previous = pool_control::desired(control)
            .unwrap_or_default()
            .iter()
            .map(ToString::to_string)
            .collect();
        act.pin(
            plan.pool.to_string(),
            vec![plan.deployment.to_string()],
            holder(&plan.id),
        )
        .await
        .map_err(|why| (None, why))?;
        return Ok(Restore::Controlled { previous });
    }
    let inventory = load_inventory(db).await.map_err(|why| (None, why))?;
    let ours = inventory
        .deployments
        .get(&plan.deployment)
        .ok_or((None, "the verification's deployment is gone".to_string()))?
        .clone();
    let states = load_states(db).await.map_err(|why| (None, why))?;
    let mut stopped = Vec::new();
    for (id, row) in &states {
        if *id == plan.deployment || !row.phase.reserves() {
            continue;
        }
        let Some(other) = inventory.deployments.get(id) else {
            continue;
        };
        if other.machines.iter().any(|m| ours.machines.contains(m)) {
            let stopping = act.stop(id.to_string()).await;
            if let Err(why) = stopping {
                let restore = Restore::Uncontrolled {
                    stopped,
                    started: false,
                };
                return Err((Some(restore), why));
            }
            stopped.push(id.clone());
            let _ = wait_for(db, act, id, STOP_REPORT_BOUND_SECS, |p| !p.reserves()).await;
        }
    }
    if let Err(why) = act.start(plan.deployment.to_string()).await {
        let restore = Restore::Uncontrolled {
            stopped,
            started: false,
        };
        return Err((Some(restore), why));
    }
    Ok(Restore::Uncontrolled {
        stopped,
        started: true,
    })
}

async fn restore(db: &LocalDb, act: &dyn Act, plan: &Planned, restore: Restore) -> Vec<String> {
    let mut problems = Vec::new();
    let mut note = |result: Result<String, String>| {
        if let Err(why) = result {
            problems.push(why);
        }
    };
    match restore {
        Restore::Controlled { previous } => {
            note(
                act.pin(
                    plan.pool.to_string(),
                    previous.into_iter().collect(),
                    holder(&plan.id),
                )
                .await,
            );
            note(act.release(plan.pool.to_string(), holder(&plan.id)).await);
        }
        Restore::Uncontrolled { stopped, started } => {
            if started {
                note(act.stop(plan.deployment.to_string()).await);
                let _ = wait_for(db, act, &plan.deployment, STOP_REPORT_BOUND_SECS, |p| {
                    !p.reserves()
                })
                .await;
            }
            for id in stopped {
                note(act.start(id.to_string()).await);
            }
        }
    }
    problems
}

/// Run one verification to its end.
pub async fn run(db: &LocalDb, act: &dyn Act, plan: &Planned) -> Result<Outcome, String> {
    set_stage(db, &plan.id, Stage::Start).await?;
    let held = match hold(db, act, plan).await {
        Ok(held) => held,
        Err((partial, why)) => {
            let problems = match partial {
                Some(partial) => restore(db, act, plan, partial).await,
                None => Vec::new(),
            };
            let reason = if problems.is_empty() {
                why.clone()
            } else {
                format!("{why}; afterwards: {}", problems.join("; "))
            };
            finish(db, &plan.id, false, Some(reason), act.now_ms()).await?;
            return Ok(Outcome::Failed(why));
        }
    };
    let measured = measure(db, act, plan).await;
    let problems = restore(db, act, plan, held).await;
    let (outcome, reason) = match measured {
        Ok(m) => {
            record(db, &plan.id, &m).await?;
            let clamp = m.kv.filter(|kv| m.context.is_some_and(|c| c > *kv));
            if let Some(kv) = clamp {
                // The pass measured the cache the clamp is cut to, so it is
                // the measured fit of the revised recipe, not a stale one.
                if let Some(revised) = clamp_recipe(db, &plan.recipe, kv).await? {
                    set_digest(db, &plan.id, &revised).await?;
                }
            }
            (Outcome::Passed, None)
        }
        Err(why) => match verify::kv_limit_from_failure(&why) {
            Some(kv) if clamp_recipe(db, &plan.recipe, kv).await?.is_some() => (
                Outcome::Clamped {
                    reason: why.clone(),
                    context: kv / 1024 * 1024,
                },
                Some(why),
            ),
            _ => (Outcome::Failed(why.clone()), Some(why)),
        },
    };
    let reason = match (reason, problems.is_empty()) {
        (reason, true) => reason,
        (Some(reason), false) => Some(format!("{reason}; afterwards: {}", problems.join("; "))),
        (None, false) => Some(format!("passed; afterwards: {}", problems.join("; "))),
    };
    finish(
        db,
        &plan.id,
        outcome == Outcome::Passed,
        reason,
        act.now_ms(),
    )
    .await?;
    Ok(outcome)
}

async fn set_digest(db: &LocalDb, id: &str, digest: &str) -> Result<(), String> {
    db.execute(
        "UPDATE inference_verifications SET recipe_digest = ?2 WHERE id = ?1",
        params![id, digest],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write inference_verifications: {error}"))
}

/// Clamp the recipe's context in place; the revised recipe's digest when it
/// changed.
async fn clamp_recipe(db: &LocalDb, recipe: &RecipeId, kv: u64) -> Result<Option<String>, String> {
    let inventory = load_inventory(db).await?;
    let Some(revised) = inventory
        .recipes
        .get(recipe)
        .and_then(|r| verify::clamp_context(r, kv))
    else {
        return Ok(None);
    };
    let revised_digest = digest(&revised);
    let mut records = Inventory::default();
    records.insert_recipe(revised);
    write_inventory(
        db,
        &Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove: Removals::default(),
        },
    )
    .await?;
    Ok(Some(revised_digest))
}

async fn measure(db: &LocalDb, act: &dyn Act, plan: &Planned) -> Result<Measured, String> {
    // Only a phase this run produced counts: the deployment id is reused
    // across runs, and on a controlled pool the controller starts it on its
    // own pass, so until then the stored phase is the previous run's.
    let started = plan.started_at;
    let reached = wait_for(
        db,
        act,
        &plan.deployment,
        START_REPORT_BOUND_SECS,
        |p| match p {
            Phase::Healthy { since_ms, .. } => *since_ms >= started,
            Phase::Failed { at_ms, .. } => *at_ms >= started,
            _ => false,
        },
    )
    .await?;
    let since = match reached {
        Some(Phase::Healthy { since_ms, .. }) if since_ms >= started => since_ms,
        Some(Phase::Failed { at_ms, reason }) if at_ms >= started => return Err(reason),
        other => {
            return Err(format!(
                "it was not healthy within {START_REPORT_BOUND_SECS}s; it was {}",
                other.as_ref().map_or("never acted on", Phase::label)
            ))
        }
    };
    set_stage(db, &plan.id, Stage::Health).await?;
    let inventory = load_inventory(db).await?;
    let recipe = inventory
        .recipes
        .get(&plan.recipe)
        .ok_or("the recipe is gone")?
        .clone();
    let deployment = inventory
        .deployments
        .get(&plan.deployment)
        .ok_or("the deployment is gone")?
        .clone();
    let address = inventory
        .address_of(&deployment.head)
        .ok_or_else(|| format!("the head {}'s address is unknown", deployment.head))?
        .to_string();
    let mut measured = Measured {
        context: verify::context_of(&recipe),
        ..Measured::default()
    };

    let footprint_by = since + SETTLE_MS + 60_000;
    loop {
        let load = load_history(db)
            .await?
            .get(&plan.deployment)
            .and_then(|loads| {
                loads
                    .iter()
                    .find(|l| l.healthy && l.finished_at == since)
                    .cloned()
            });
        if let Some(load) = &load {
            measured.load_ms = load.load_ms;
            measured.footprint = load.footprint.clone();
        }
        if measured.footprint.is_some() || act.now_ms() >= footprint_by {
            break;
        }
        act.sleep(POLL_MS).await;
    }

    let api_base = deployment.api_base(&address);
    if let Some(models) = act.get(format!("{api_base}/models")).await {
        measured.context = cairn_inference::served::max_model_len(&models, &recipe.served_name)
            .or(measured.context);
    }
    if let Some(metrics) = act.get(deployment.metrics_url(&address)).await {
        measured.kv = cairn_inference::metrics::kv_cache_tokens(&metrics);
    }
    let origin = api_base.trim_end_matches(&deployment.api_path).to_string();
    measured.version = act.get(format!("{origin}/version")).await.and_then(|body| {
        serde_json::from_str::<serde_json::Value>(&body)
            .ok()?
            .get("version")?
            .as_str()
            .map(str::to_string)
    });
    record(db, &plan.id, &measured).await?;

    set_stage(db, &plan.id, Stage::Benchmark).await?;
    let embeds = recipe
        .verbs
        .as_ref()
        .is_some_and(|verbs| verbs.contains(&VerbKind::Embed));
    measured.speeds = if embeds {
        let mut samples = Vec::new();
        for _ in 0..3 {
            samples.push(
                act.embed(api_base.clone(), recipe.served_name.clone())
                    .await?,
            );
        }
        verify::embed_speeds(&samples)?
    } else {
        act.chat(api_base.clone(), recipe.served_name.clone())
            .await?;
        let mut samples = Vec::new();
        for _ in 0..3 {
            samples.push(
                act.chat(api_base.clone(), recipe.served_name.clone())
                    .await?,
            );
        }
        verify::chat_speeds(&samples)?
    };
    Ok(measured)
}

/// Fail every verification a stopped orch left running, and undo what it
/// held.
pub async fn recover(db: &LocalDb, act: &dyn Act) -> Result<Vec<String>, String> {
    let running: Vec<VerificationRow> = load_verifications(db)
        .await?
        .into_iter()
        .filter(|row| row.outcome == "running")
        .collect();
    let mut recovered = Vec::new();
    for row in &running {
        finish(
            db,
            &row.id,
            false,
            Some("orch restarted".into()),
            act.now_ms(),
        )
        .await?;
        let _ = act.stop(row.deployment.clone()).await;
        recovered.push(row.id.clone());
    }
    for (pool, control) in super::pool_control::load_controls(db).await? {
        if let Some(pin) = control.pin.filter(|pin| pin.holder.starts_with("verify:")) {
            let _ = act.release(pool.to_string(), pin.holder).await;
        }
    }
    Ok(recovered)
}

/// The recipes of `model` to try on a pool of `family` hardware, imported
/// first, then generated, more precise before less and fewer machines first.
pub fn candidates(
    inventory: &Inventory,
    model: &str,
    pool: &PoolId,
    family: Option<AcceleratorFamily>,
) -> Vec<RecipeId> {
    let size = inventory.pools.get(pool).map_or(0, |p| p.machines.len());
    let mut recipes: Vec<&Recipe> = inventory
        .recipes
        .values()
        .filter(|r| r.model.name == model && matches!(r.engine, Engine::Container(_)))
        .filter(|r| r.requirements.machines as usize <= size)
        .filter(|r| match (family, r.requirements.accelerator) {
            (Some(family), Some(accelerator)) => accelerator.family == family,
            _ => true,
        })
        .collect();
    let rank = |r: &Recipe| match r.origin {
        RecipeOrigin::Imported { .. } => 0,
        RecipeOrigin::Authored => 1,
        RecipeOrigin::Generated { .. } => 2,
    };
    recipes.sort_by(|a, b| {
        rank(a)
            .cmp(&rank(b))
            .then_with(|| {
                let p = |r: &Recipe| {
                    cairn_inference::generate::precision(r.quantization.as_deref().unwrap_or(""))
                };
                p(b).cmp(&p(a))
            })
            .then(a.requirements.machines.cmp(&b.requirements.machines))
            .then(a.id.cmp(&b.id))
    });
    recipes.into_iter().map(|r| r.id.clone()).collect()
}

/// Verify `recipes` in order on `pool`, each once more after a clamp, until
/// one passes (or every one, with `all`). Each verification is its own row.
pub async fn verify_in_order(
    db: &LocalDb,
    act: &dyn Act,
    recipes: Vec<RecipeId>,
    pool: &PoolId,
    requested_by: &str,
    all: bool,
    first: Option<Planned>,
) -> Result<Vec<(String, Outcome)>, String> {
    let mut results = Vec::new();
    let mut first = first;
    for recipe in recipes {
        let mut clamps = 0;
        loop {
            let plan = match first.take() {
                Some(plan) if plan.recipe == recipe => plan,
                _ => match begin(
                    db,
                    recipe.as_str(),
                    pool.as_str(),
                    requested_by,
                    act.now_ms(),
                )
                .await
                {
                    Ok(plan) => plan,
                    Err(why) => {
                        results.push((String::new(), Outcome::Failed(why)));
                        break;
                    }
                },
            };
            let outcome = run(db, act, &plan).await?;
            results.push((plan.id.clone(), outcome.clone()));
            match outcome {
                Outcome::Clamped { .. } if clamps == 0 => clamps += 1,
                Outcome::Passed if !all => return Ok(results),
                _ => break,
            }
        }
    }
    Ok(results)
}

/// The live world: the act side, the pool controller and HTTP.
pub struct Live {
    pub orch: super::Orchestrator,
    pub client: reqwest::Client,
}

impl Live {
    pub fn new(orch: super::Orchestrator) -> Result<Self, String> {
        let client = cairn_tls::client_builder()
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|error| format!("could not build an HTTP client: {error}"))?;
        Ok(Self { orch, client })
    }
}

impl Act for Live {
    fn start(&self, deployment: String) -> Pending<'_, Result<String, String>> {
        Box::pin(async move {
            super::inference_control::request_deployment(&self.orch, &deployment, Request::Start)
                .await
        })
    }

    fn stop(&self, deployment: String) -> Pending<'_, Result<String, String>> {
        Box::pin(async move {
            super::inference_control::request_deployment(&self.orch, &deployment, Request::Stop)
                .await
        })
    }

    fn pin(
        &self,
        pool: String,
        set: Vec<String>,
        holder: String,
    ) -> Pending<'_, Result<String, String>> {
        Box::pin(async move {
            let patch = super::pool_control::PoolPatch {
                pool: pool.as_str().into(),
                pin: Some(set.into_iter().map(DeploymentId::new).collect()),
                ..super::pool_control::PoolPatch::default()
            };
            super::pool_control::patch_pool(&self.orch, &patch, &holder).await
        })
    }

    fn release(&self, pool: String, holder: String) -> Pending<'_, Result<String, String>> {
        Box::pin(async move {
            let patch = super::pool_control::PoolPatch {
                pool: pool.as_str().into(),
                release: true,
                ..super::pool_control::PoolPatch::default()
            };
            super::pool_control::patch_pool(&self.orch, &patch, &holder).await
        })
    }

    fn get(&self, url: String) -> Pending<'_, Option<String>> {
        Box::pin(async move {
            let response = self
                .client
                .get(&url)
                .timeout(Duration::from_secs(10))
                .send()
                .await
                .ok()?;
            if !response.status().is_success() {
                return None;
            }
            response.text().await.ok()
        })
    }

    fn chat(&self, api_base: String, model: String) -> Pending<'_, Result<ChatSample, String>> {
        Box::pin(async move {
            use futures_util::StreamExt;
            let body = serde_json::json!({
                "model": model,
                "messages": [{"role": "user", "content": verify::chat_prompt()}],
                "max_tokens": 256,
                "temperature": 0,
                "stream": true,
                "stream_options": {"include_usage": true},
            });
            let started = std::time::Instant::now();
            let response = self
                .client
                .post(format!("{api_base}/chat/completions"))
                .json(&body)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|error| format!("the chat benchmark request failed: {error}"))?;
            let mut stream = response.bytes_stream();
            let mut buffer = String::new();
            let mut first: Option<f64> = None;
            let mut usage: Option<(u64, u64)> = None;
            let mut chunks = 0u64;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes.map_err(|error| format!("the chat stream broke: {error}"))?;
                buffer.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(end) = buffer.find('\n') {
                    let line: String = buffer.drain(..=end).collect();
                    let Some(data) = line.trim().strip_prefix("data:") else {
                        continue;
                    };
                    let Ok(event) = serde_json::from_str::<serde_json::Value>(data.trim()) else {
                        continue;
                    };
                    let delta = &event["choices"][0]["delta"];
                    let produced = ["content", "reasoning_content", "reasoning"]
                        .iter()
                        .any(|key| delta[key].as_str().is_some_and(|s| !s.is_empty()));
                    if produced {
                        chunks += 1;
                        first.get_or_insert(started.elapsed().as_secs_f64() * 1000.0);
                    }
                    if let (Some(p), Some(c)) = (
                        event["usage"]["prompt_tokens"].as_u64(),
                        event["usage"]["completion_tokens"].as_u64(),
                    ) {
                        usage = Some((p, c));
                    }
                }
            }
            let total = started.elapsed().as_secs_f64() * 1000.0;
            let (prompt_tokens, completion_tokens) = usage.unwrap_or((0, chunks));
            Ok(ChatSample {
                ttft_ms: first.unwrap_or(total),
                total_ms: total,
                prompt_tokens,
                completion_tokens,
            })
        })
    }

    fn embed(&self, api_base: String, model: String) -> Pending<'_, Result<EmbedSample, String>> {
        Box::pin(async move {
            let started = std::time::Instant::now();
            let answer: serde_json::Value = self
                .client
                .post(format!("{api_base}/embeddings"))
                .json(&serde_json::json!({"model": model, "input": verify::embed_batch()}))
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|error| format!("the embeddings benchmark request failed: {error}"))?
                .json()
                .await
                .map_err(|error| format!("the embeddings answer is not JSON: {error}"))?;
            let total_ms = started.elapsed().as_secs_f64() * 1000.0;
            let vectors = answer["data"].as_array().cloned().unwrap_or_default();
            let dims: BTreeSet<usize> = vectors
                .iter()
                .map(|v| v["embedding"].as_array().map_or(0, Vec::len))
                .collect();
            if vectors.len() != verify::embed_batch().len() || dims.len() != 1 || dims.contains(&0)
            {
                return Err(format!(
                    "the embeddings answer held {} vectors of dimensions {dims:?} for {} inputs",
                    vectors.len(),
                    verify::embed_batch().len()
                ));
            }
            Ok(EmbedSample {
                total_ms,
                tokens: answer["usage"]["prompt_tokens"].as_u64().unwrap_or(0),
            })
        })
    }

    fn sleep(&self, ms: u64) -> Pending<'_, ()> {
        Box::pin(tokio::time::sleep(Duration::from_millis(ms)))
    }

    fn now_ms(&self) -> i64 {
        crate::fleet::unix_time_ms() as i64
    }
}

/// Fail what a previous orch left running, once at boot.
pub fn spawn_recovery(orch: super::Orchestrator) {
    tokio::spawn(async move {
        let Ok(live) = Live::new(orch.clone()) else {
            return;
        };
        match recover(&orch.db.local, &live).await {
            Ok(recovered) if !recovered.is_empty() => log::info!(
                "inference: failed {} verification(s) left running by the last orch",
                recovered.len()
            ),
            Ok(_) => {}
            Err(error) => log::warn!("inference: verification recovery failed: {error}"),
        }
    });
}

/// Start verifying `recipes` on `pool` in the background, the first planned
/// now so a refusal is answered at once; return the first verification's id.
pub async fn spawn(
    orch: &super::Orchestrator,
    recipes: Vec<RecipeId>,
    pool: &str,
    requested_by: &str,
    all: bool,
) -> Result<String, String> {
    let first = recipes
        .first()
        .ok_or_else(|| format!("no recipe to verify on pool {pool}"))?
        .clone();
    let plan = begin(
        &orch.db.local,
        first.as_str(),
        pool,
        requested_by,
        crate::fleet::unix_time_ms() as i64,
    )
    .await?;
    let id = plan.id.clone();
    let live = Live::new(orch.clone())?;
    let (pool, requested_by) = (PoolId::from(pool), requested_by.to_string());
    let db = orch.db.local.clone();
    tokio::spawn(async move {
        if let Err(error) =
            verify_in_order(&db, &live, recipes, &pool, &requested_by, all, Some(plan)).await
        {
            log::warn!("inference: a verification on pool {pool} stopped: {error}");
        }
    });
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const RECIPE: &str = r#"
models:
  - {name: chat/Model}
  - {name: Qwen/Qwen3-Embedding-4B}
recipes:
  - id: embed
    model:
      name: Qwen/Qwen3-Embedding-4B
      weights: [{repo: Qwen/Qwen3-Embedding-4B-GGUF, revision: f4602530db1d980e16da9d7d3a70294cf5c190be, path: Qwen3-Embedding-4B-Q8_0.gguf, sha256: b60ae5ce2dd6a0b77f82cadf21def1f310a3e10cde380ad0081b07a9d416949d}]
    servedName: Qwen/Qwen3-Embedding-4B
    verbs: [embed]
    quantization: q8_0
    engine:
      kind: container
      image: ghcr.io/ggml-org/llama.cpp@sha256:afdc5265d1cfc729ad3ebaac59fd60e5d15c3b7aef455d7dc16bc1dccbc4dc64
      server: llamaCpp
      args: [-m, "{weights}", --port, "{port}", -c, "40960"]
      mounts: [{from: {kind: hfCache}, to: /models, readOnly: true}]
    requirements: {machines: 1}
  - id: chat
    model:
      name: chat/Model
      weights: [{repo: chat/Model, revision: abc, path: m.safetensors, sha256: b60ae5ce2dd6a0b77f82cadf21def1f310a3e10cde380ad0081b07a9d416949d}]
    servedName: chat/Model
    verbs: [complete]
    engine:
      kind: container
      image: x@sha256:afdc5265d1cfc729ad3ebaac59fd60e5d15c3b7aef455d7dc16bc1dccbc4dc64
      server: vllm
      args: [vllm, serve, "{model}", --port, "{port}", --max-model-len, "262144"]
    requirements: {machines: 1}
  - id: old
    model: {name: chat/Model}
    servedName: old
    engine: {kind: command, start: ./old.sh, stop: ./stop-old.sh}
    requirements: {machines: 1}
deployments:
  - {id: old, recipe: old, machines: [box], head: box, apiPort: 9000, apiPath: /v1, healthPath: /v1/models}
pools:
  - {id: solo, machines: [{name: box, address: 10.0.0.9}]}
"#;

    /// A world where starts and stops take effect at once, through the same
    /// stored phases the act side writes.
    pub(crate) struct Fake<'a> {
        db: &'a LocalDb,
        pub(crate) calls: Mutex<Vec<String>>,
        /// The reasons the next starts fail with, in order.
        failures: Mutex<Vec<String>>,
        now: Mutex<i64>,
        /// The act side refuses a start with this.
        refuse_start: Mutex<Option<String>>,
        /// A controlled pool's verify deployment, which the controller starts
        /// on its next pass rather than at once.
        pending: Mutex<Option<String>>,
    }

    impl<'a> Fake<'a> {
        fn new(db: &'a LocalDb) -> Self {
            Self {
                db,
                calls: Mutex::default(),
                failures: Mutex::default(),
                now: Mutex::new(1_000_000),
                refuse_start: Mutex::default(),
                pending: Mutex::default(),
            }
        }

        async fn set(&self, deployment: &str, phase: Phase) {
            let json = serde_json::to_string(&phase).unwrap();
            self.db
                .execute(
                    "INSERT INTO inference_deployment_state (deployment_id, phase_json, command_id, updated_at)
                     VALUES (?1, ?2, NULL, 0)
                     ON CONFLICT(deployment_id) DO UPDATE SET phase_json = excluded.phase_json",
                    params![deployment, json],
                )
                .await
                .unwrap();
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        /// The deployment loads: healthy now, or failed with the next queued
        /// reason.
        async fn come_up(&self, deployment: String) {
            let now = self.now_ms();
            let failure = {
                let mut failures = self.failures.lock().unwrap();
                (!failures.is_empty()).then(|| failures.remove(0))
            };
            match failure {
                Some(reason) => {
                    self.set(&deployment, Phase::Failed { at_ms: now, reason })
                        .await
                }
                None => {
                    self.set(
                        &deployment,
                        Phase::Healthy {
                            since_ms: now,
                            load_ms: Some(42_000),
                        },
                    )
                    .await;
                    self.db
                                .execute(
                                    "INSERT INTO inference_deployment_loads
                                       (deployment_id, recipe_id, requested_at, finished_at, outcome, load_ms, footprint_json)
                                     VALUES (?1, 'r', ?2, ?2, 'healthy', 42000, '{\"box\":9663676416}')",
                                    params![deployment, now],
                                )
                                .await
                                .unwrap();
                }
            }
        }
    }

    impl Act for Fake<'_> {
        fn start(&self, deployment: String) -> Pending<'_, Result<String, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("start {deployment}"));
                if deployment.starts_with("verify-") {
                    if let Some(why) = self.refuse_start.lock().unwrap().clone() {
                        return Err(why);
                    }
                }
                self.come_up(deployment).await;
                Ok("started".into())
            })
        }

        fn stop(&self, deployment: String) -> Pending<'_, Result<String, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("stop {deployment}"));
                self.set(
                    &deployment,
                    Phase::Stopped {
                        since_ms: self.now_ms(),
                    },
                )
                .await;
                Ok("stopped".into())
            })
        }

        fn pin(
            &self,
            pool: String,
            set: Vec<String>,
            holder: String,
        ) -> Pending<'_, Result<String, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("pin {pool} {set:?} {holder}"));
                if let Some(ours) = set.iter().find(|d| d.starts_with("verify-")) {
                    *self.pending.lock().unwrap() = Some(ours.clone());
                }
                Ok("pinned".into())
            })
        }

        fn release(&self, pool: String, holder: String) -> Pending<'_, Result<String, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("release {pool} {holder}"));
                Ok("released".into())
            })
        }

        fn get(&self, url: String) -> Pending<'_, Option<String>> {
            Box::pin(async move {
                if url.ends_with("/v1/models") {
                    Some(r#"{"data":[{"id":"x","max_model_len":262144}]}"#.into())
                } else if url.ends_with("/metrics") {
                    Some("vllm:cache_config_info{kv_cache_size_tokens=\"100000\"} 1\n".into())
                } else if url.ends_with("/version") {
                    Some(r#"{"version":"0.32.0"}"#.into())
                } else {
                    None
                }
            })
        }

        fn chat(
            &self,
            _api_base: String,
            _model: String,
        ) -> Pending<'_, Result<ChatSample, String>> {
            Box::pin(async move {
                Ok(ChatSample {
                    ttft_ms: 200.0,
                    total_ms: 5_300.0,
                    prompt_tokens: 1_000,
                    completion_tokens: 256,
                })
            })
        }

        fn embed(
            &self,
            _api_base: String,
            _model: String,
        ) -> Pending<'_, Result<EmbedSample, String>> {
            Box::pin(async move {
                Ok(EmbedSample {
                    total_ms: 500.0,
                    tokens: 1_000,
                })
            })
        }

        fn sleep(&self, ms: u64) -> Pending<'_, ()> {
            *self.now.lock().unwrap() += ms as i64;
            let pending = self.pending.lock().unwrap().take();
            Box::pin(async move {
                if let Some(deployment) = pending {
                    self.come_up(deployment).await;
                }
            })
        }

        fn now_ms(&self) -> i64 {
            *self.now.lock().unwrap()
        }
    }

    async fn seeded(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        write_inventory(&db, &Authoring::Replace(RECIPE.into()))
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn a_pass_records_its_measurements_and_restores_the_pool() {
        let db = seeded("inference-verify-pass").await;
        let fake = Fake::new(&db);
        fake.set(
            "old",
            Phase::Healthy {
                since_ms: 0,
                load_ms: None,
            },
        )
        .await;
        let plan = begin(&db, "embed", "solo", "operator", 1).await.unwrap();
        assert_eq!(plan.deployment.as_str(), "verify-embed-solo");
        assert_eq!(run(&db, &fake, &plan).await.unwrap(), Outcome::Passed);
        assert_eq!(
            fake.calls(),
            vec![
                "stop old",
                "start verify-embed-solo",
                "stop verify-embed-solo",
                "start old"
            ]
        );
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(row.outcome, "passed");
        assert_eq!(row.stage, "done");
        assert_eq!(row.load_ms, Some(42_000));
        assert_eq!(row.footprint.as_ref().unwrap()["box"], 9 << 30);
        assert_eq!(row.embed_tps, Some(2_000.0));
        assert_eq!(row.engine_version.as_deref(), Some("0.32.0"));
        assert!(!row.stale(&load_inventory(&db).await.unwrap()));
    }

    #[tokio::test]
    async fn a_kv_failure_clamps_the_recipe_and_the_revision_passes() {
        let db = seeded("inference-verify-clamp").await;
        let fake = Fake::new(&db);
        fake.failures.lock().unwrap().push(
            "ValueError: The model's max seq len (262144) is larger than the maximum number of tokens that can be stored in KV cache (120000).".into(),
        );
        let results = verify_in_order(
            &db,
            &fake,
            vec!["chat".into()],
            &"solo".into(),
            "operator",
            false,
            None,
        )
        .await
        .unwrap();
        assert!(
            matches!(
                results[0].1,
                Outcome::Clamped {
                    context: 119_808,
                    ..
                }
            ),
            "{results:?}"
        );
        assert_eq!(results[1].1, Outcome::Passed);
        let inventory = load_inventory(&db).await.unwrap();
        let chat = &inventory.recipes[&RecipeId::from("chat")];
        assert_eq!(
            verify::context_of(chat),
            Some(99_328),
            "the pass measured 100000 tokens of KV cache and clamped again"
        );
        let rows = load_verifications(&db).await.unwrap();
        let failed = rows.iter().find(|r| r.outcome == "failed").unwrap();
        assert_eq!(failed.stage, "start");
        assert!(failed.stale(&inventory), "the recipe changed after it ran");
        let passed = rows.iter().find(|r| r.outcome == "passed").unwrap();
        assert!(
            !passed.stale(&inventory),
            "the pass measured the clamp it made"
        );
        assert_eq!(passed.decode_tps, Some(50.0));
        assert_eq!(passed.kv_tokens, Some(100_000));
    }

    #[tokio::test]
    async fn a_failure_is_recorded_and_the_pool_is_restored_on_every_path() {
        let db = seeded("inference-verify-fail").await;
        let fake = Fake::new(&db);
        fake.failures
            .lock()
            .unwrap()
            .push("CUDA driver version is insufficient for CUDA runtime version".into());
        let plan = begin(&db, "chat", "solo", "operator", 1).await.unwrap();
        let outcome = run(&db, &fake, &plan).await.unwrap();
        assert!(matches!(&outcome, Outcome::Failed(why) if why.contains("driver")));
        assert_eq!(fake.calls().last().unwrap(), "stop verify-chat-solo");
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(
            (row.outcome.as_str(), row.stage.as_str()),
            ("failed", "start")
        );
    }

    #[tokio::test]
    async fn a_pool_pinned_by_someone_else_is_refused_and_boot_recovers_a_running_one() {
        let db = seeded("inference-verify-pinned").await;
        let pool = load_inventory(&db).await.unwrap().pools[&PoolId::from("solo")].clone();
        let mut control =
            pool_control::PoolControl::begin(&pool, &BTreeMap::new(), &BTreeMap::new(), 0);
        control.pin(BTreeSet::new(), "agent-7", 0).unwrap();
        db.execute(
            "INSERT INTO inference_pool_control (pool, body_json, updated_at) VALUES ('solo', ?1, 0)",
            params![serde_json::to_string(&control).unwrap()],
        )
        .await
        .unwrap();
        let refusal = begin(&db, "chat", "solo", "operator", 1).await.unwrap_err();
        assert!(refusal.contains("pinned by agent-7"), "{refusal}");

        db.execute("DELETE FROM inference_pool_control", ())
            .await
            .unwrap();
        let plan = begin(&db, "chat", "solo", "operator", 1).await.unwrap();
        let fake = Fake::new(&db);
        assert_eq!(
            recover(fake.db, &fake).await.unwrap(),
            vec![plan.id.clone()]
        );
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(row.reason.as_deref(), Some("orch restarted"));
        assert_eq!(fake.calls(), vec!["stop verify-chat-solo"]);
    }

    #[tokio::test]
    async fn a_controlled_pool_waits_for_this_runs_load_not_the_last_ones_failure() {
        let db = seeded("inference-verify-controlled").await;
        let pool = load_inventory(&db).await.unwrap().pools[&PoolId::from("solo")].clone();
        let control =
            pool_control::PoolControl::begin(&pool, &BTreeMap::new(), &BTreeMap::new(), 0);
        assert!(control.controlled);
        db.execute(
            "INSERT INTO inference_pool_control (pool, body_json, updated_at) VALUES ('solo', ?1, 0)",
            params![serde_json::to_string(&control).unwrap()],
        )
        .await
        .unwrap();
        let fake = Fake::new(&db);
        fake.set(
            "verify-chat-solo",
            Phase::Failed {
                at_ms: 5,
                reason: "an earlier run's failure".into(),
            },
        )
        .await;
        let plan = begin(&db, "chat", "solo", "operator", fake.now_ms())
            .await
            .unwrap();
        assert_eq!(run(&db, &fake, &plan).await.unwrap(), Outcome::Passed);
        let calls = fake.calls();
        assert_eq!(
            calls[0],
            format!("pin solo [\"verify-chat-solo\"] verify:{}", plan.id)
        );
        assert!(calls[1].starts_with("pin solo []"), "{calls:?}");
        assert_eq!(calls[2], format!("release solo verify:{}", plan.id));
    }

    #[tokio::test]
    async fn a_refused_start_puts_back_what_it_stopped() {
        let db = seeded("inference-verify-refused").await;
        let fake = Fake::new(&db);
        fake.set(
            "old",
            Phase::Healthy {
                since_ms: 0,
                load_ms: None,
            },
        )
        .await;
        *fake.refuse_start.lock().unwrap() = Some("box is held by deployment other".into());
        let plan = begin(&db, "embed", "solo", "operator", 1).await.unwrap();
        let outcome = run(&db, &fake, &plan).await.unwrap();
        assert_eq!(
            outcome,
            Outcome::Failed("box is held by deployment other".into())
        );
        assert_eq!(
            fake.calls(),
            vec!["stop old", "start verify-embed-solo", "start old"],
            "the refused deployment is not stopped; the pool's model is restarted"
        );
    }

    #[tokio::test]
    async fn a_recipe_that_cannot_render_fails_before_anything_is_stopped() {
        let db = seeded("inference-verify-render").await;
        let mut inventory = load_inventory(&db).await.unwrap();
        let mut linked = inventory.recipes[&RecipeId::from("chat")].clone();
        if let Engine::Container(container) = &mut linked.engine {
            container.args.push("{self.link}".into());
        }
        inventory.insert_recipe(linked);
        write_inventory(&db, &Authoring::Replace(inventory.to_yaml()))
            .await
            .unwrap();
        let refusal = begin(&db, "chat", "solo", "operator", 1).await.unwrap_err();
        assert!(
            refusal.contains("failed at render, before anything was stopped"),
            "{refusal}"
        );
        assert!(refusal.contains("no link address"), "{refusal}");
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(
            (row.outcome.as_str(), row.stage.as_str()),
            ("failed", "render")
        );
    }

    #[tokio::test]
    async fn one_verification_holds_a_pool_at_a_time() {
        let db = seeded("inference-verify-one").await;
        let first = begin(&db, "chat", "solo", "operator", 1).await.unwrap();
        let refusal = begin(&db, "embed", "solo", "operator", 2)
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            format!(
                "pool solo is already being verified (verification {}); wait for it to finish",
                first.id
            )
        );
    }

    #[test]
    fn candidates_take_imported_then_generated_more_precise_first() {
        let mut inventory = Inventory::from_yaml(RECIPE).unwrap();
        let mut generated = inventory.recipes[&RecipeId::from("chat")].clone();
        generated.id = "chat-gen".into();
        generated.origin = RecipeOrigin::Generated {
            template: "vllm-openai".into(),
            model_revision: "abc".into(),
        };
        inventory.insert_recipe(generated);
        let mut imported = inventory.recipes[&RecipeId::from("chat")].clone();
        imported.id = "chat-imp".into();
        imported.origin = RecipeOrigin::Imported {
            url: "u".into(),
            commit: "c".into(),
            compose: "c".into(),
            service: "s".into(),
        };
        inventory.insert_recipe(imported);
        assert_eq!(
            candidates(
                &inventory,
                "chat/Model",
                &"solo".into(),
                Some(AcceleratorFamily::Nvidia)
            ),
            vec![RecipeId::from("chat-imp"), "chat".into(), "chat-gen".into()]
        );
    }
}
