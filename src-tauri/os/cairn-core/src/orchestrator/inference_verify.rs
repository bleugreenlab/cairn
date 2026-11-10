//! Verifying a profile on a pool: load it, check its health, benchmark it, and
//! record what it measured, its memory per machine, load time and speed, or
//! the stage it failed at and why.
//!
//! A verification runs as a background task and returns its id at once. It
//! writes the profile as deployment `verify-<profile>-<pool>` on the first
//! machines of a pool placement detects, and pins that deployment for
//! `verify:<id>`: placement acquires it as it acquires any pinned model,
//! displacing the unpinned residents on its machines, reserving its room and
//! draining batches as for any load. A pool another holder has pinned is
//! refused, naming them. A verification on its own ends with the deployment
//! unloaded (its pin revoked and it stopped); placement's own rules bring the
//! pool's models back (a request reacquires one, an idle pool places its warm
//! model).
//!
//! A load of a model with no deployment on the pool verifies first and keeps
//! what passes: the requester's request for the deployment is recorded while
//! the verification's pin still holds it, then the pin is released, so the
//! model stays resident and serving, its idle clock running as for any load,
//! and is never stopped and started again. A load whose verification fails
//! ends unloaded like any other.
//!
//! A start that fails because the context does not fit the KV cache (vLLM
//! names the cache's size) is recorded, the profile is revised in place with
//! its context clamped to that size, and the revision is verified next. A pass
//! whose measured cache is smaller than the profile's context clamps it too.
//! Cairn never changes a host's drivers: a driver or runtime mismatch is the
//! recorded reason, not something fixed.
//!
//! A verification left running when the orch stops is failed at the next boot
//! with "orch restarted", and its deployment is unloaded.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use cairn_db::turso::params;
use cairn_inference::control::{Phase, START_REPORT_BOUND_SECS, STOP_REPORT_BOUND_SECS};
use cairn_inference::verify::{self, ChatSample, EmbedSample, Speeds, Stage};
use cairn_inference::{
    unknown_address, AcceleratorFamily, Address, Addresses, Deployment, DeploymentId, Engine,
    InferenceProfile, InferenceProfileId, Inventory, MachineName, Network, ProfileOrigin,
    RecordSource, Server,
};
use cairn_models::types::catalog::VerbKind;
use sha2::{Digest, Sha256};

use super::inference::{
    load_inventory, prepare_inventory_write, save_rows_in, write_inventory, Authoring, Removals,
    RowChange, INVENTORY,
};
use super::inference_control::{load_history, load_states, rendered_head, SETTLE_MS};
use super::inference_generate::promotion::{self, Admission, Binding, Gate};
use crate::fleet::models::{self, store::load_pins, PinTarget};
use crate::fleet::pools::{Pool, PoolKey};
use crate::storage::{LocalDb, RowExt};

pub type Pending<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Everything a verification does to the world.
pub trait Act: Send + Sync {
    /// Pin `deployment` for `holder`: placement acquires it, displacing the
    /// unpinned residents on its machines.
    fn pin(&self, deployment: String, holder: String) -> Pending<'_, Result<String, String>>;
    /// Unload `deployment` for `holder`: revoke its pin and stop it.
    fn unload(&self, deployment: String, holder: String) -> Pending<'_, Result<String, String>>;
    /// Request `model` on `pool` for `requester`, as a load does; refused
    /// unless placement leases `deployment` for it.
    fn request(
        &self,
        deployment: String,
        model: String,
        pool: String,
        requester: String,
    ) -> Pending<'_, Result<String, String>>;
    /// Release `holder`'s pin on `deployment`, leaving it resident.
    fn unpin(&self, deployment: String, holder: String) -> Pending<'_, Result<String, String>>;
    /// A GET's body, `None` when it did not answer successfully.
    fn get(&self, url: String) -> Pending<'_, Option<String>>;
    fn chat(&self, api_base: String, model: String) -> Pending<'_, Result<ChatSample, String>>;
    fn embed(&self, api_base: String, model: String) -> Pending<'_, Result<EmbedSample, String>>;
    fn sleep(&self, ms: u64) -> Pending<'_, ()>;
    fn now_ms(&self) -> i64;
    /// How the attached machines are reached now.
    fn network(&self) -> Network;
}

const KEPT: i64 = 20;
const POLL_MS: u64 = 5_000;

fn holder(id: &str) -> String {
    format!("verify:{id}")
}

fn deployment_id(profile: &InferenceProfileId, pool: &PoolKey) -> DeploymentId {
    DeploymentId::new(format!("verify-{profile}-{pool}"))
}

/// The sha256 of a profile's canonical JSON.
pub fn digest(profile: &InferenceProfile) -> String {
    Sha256::digest(verify::canonical(profile).as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// One stored verification.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VerificationRow {
    pub id: String,
    pub profile: String,
    pub profile_digest: String,
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
    /// Whether its profile has changed since it ran.
    pub fn stale(&self, inventory: &Inventory) -> bool {
        inventory
            .profiles
            .get(&InferenceProfileId::from(self.profile.as_str()))
            .is_none_or(|profile| digest(profile) != self.profile_digest)
    }
}

const COLUMNS: &str =
    "id, profile_id, profile_digest, pool_id, deployment_id, machines_json, requested_by,
    started_at, finished_at, outcome, stage, reason, load_ms, footprint_json, context_tokens,
    kv_tokens, ttft_ms, decode_tps, prefill_tps, embed_tps, engine_version";

fn row_of(row: &cairn_db::turso::Row) -> Result<VerificationRow, cairn_db::storage::DbError> {
    Ok(VerificationRow {
        id: row.text(0)?,
        profile: row.text(1)?,
        profile_digest: row.text(2)?,
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
    pub profile: InferenceProfileId,
    pub pool: PoolKey,
    pub deployment: DeploymentId,
    /// Where its head is reached, as the plan was rendered.
    pub head: Address,
    /// Who the deployment stays loaded for once it passes, when a load of its
    /// model started it; `None` unloads it after measuring.
    pub then_keep: Option<String>,
    /// It is a generation draft revision's one claim, already running, that a
    /// repeated start returned rather than starting another.
    pub replayed: bool,
}

fn port_for(profile: &InferenceProfile) -> u16 {
    match &profile.engine {
        Engine::Container(container) if container.server == Server::LlamaCpp => 8090,
        _ => 8000,
    }
}

/// The running row a verification claims its pool with, the deployment rows
/// it writes, and the generation state it binds, all written together.
#[derive(Clone)]
struct Claim {
    id: String,
    profile: String,
    digest: String,
    pool: String,
    deployment: String,
    machines_json: String,
    requested_by: String,
    started_at: i64,
    changes: Vec<RowChange>,
    binding: Option<Binding>,
}

/// What claiming a pool came to.
enum Claimed {
    New,
    /// Another verification runs on the pool.
    Busy(String),
    /// The draft revision's one claim was taken first; this is it.
    Replay(String),
    /// The generation is no longer bound as it was checked.
    Unbound,
}

/// Insert `claim`'s running row and its deployment unless a verification is
/// already running on its pool, and trim the profile's history on the pool to
/// [`KEPT`] rows. A generation draft's claim also binds the generation's one
/// verification of this revision, conditionally on its being still exactly
/// as checked, in the same transaction; nothing is written when it is not.
/// Two claims of one idle pool each read it idle under their own snapshot,
/// so the read alone admits both; the partial unique index over running rows
/// per pool makes the later commit conflict and re-run this closure, which
/// then finds the earlier claim.
async fn claim_pool(db: &LocalDb, claim: Claim) -> Result<Claimed, String> {
    db.write(move |conn| {
        let claim = claim.clone();
        Box::pin(async move {
            let mut running = conn
                .query(
                    "SELECT id FROM inference_verifications
                      WHERE pool_id = ?1 AND outcome = 'running' LIMIT 1",
                    params![claim.pool.clone()],
                )
                .await?;
            if let Some(row) = running.next().await? {
                return Ok(Claimed::Busy(row.text(0)?));
            }
            drop(running);
            if let Some(binding) = &claim.binding {
                let bound = conn
                    .execute(
                        "UPDATE inference_generations SET verification_id = ?1
                          WHERE id = ?2 AND state = 'running' AND draft_profile = ?3
                            AND draft_revision = ?4 AND draft_digest = ?5
                            AND approval_revision = ?4 AND approved_surface = ?6
                            AND verification_id IS NULL",
                        params![
                            claim.id.clone(),
                            binding.generation.clone(),
                            binding.profile.clone(),
                            binding.revision,
                            binding.profile_digest.clone(),
                            binding.surface_digest.clone()
                        ],
                    )
                    .await?;
                if bound == 0 {
                    let mut current = conn
                        .query(
                            "SELECT verification_id FROM inference_generations WHERE id = ?1",
                            params![binding.generation.clone()],
                        )
                        .await?;
                    let existing = match current.next().await? {
                        Some(row) => row.opt_text(0)?,
                        None => None,
                    };
                    return Ok(existing.map_or(Claimed::Unbound, Claimed::Replay));
                }
            }
            save_rows_in(conn, claim.changes.clone(), claim.started_at, None).await?;
            conn.execute(
                "INSERT INTO inference_verifications
                   (id, profile_id, profile_digest, pool_id, deployment_id, machines_json,
                    requested_by, started_at, outcome, stage)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'running', 'render')",
                params![
                    claim.id,
                    claim.profile.clone(),
                    claim.digest,
                    claim.pool.clone(),
                    claim.deployment,
                    claim.machines_json,
                    claim.requested_by,
                    claim.started_at
                ],
            )
            .await?;
            conn.execute(
                "DELETE FROM inference_verifications
                  WHERE profile_id = ?1 AND pool_id = ?2 AND outcome != 'running'
                    AND id NOT IN (SELECT id FROM inference_verifications
                                    WHERE profile_id = ?1 AND pool_id = ?2
                                    ORDER BY started_at DESC LIMIT ?3)",
                params![claim.profile, claim.pool, KEPT],
            )
            .await?;
            Ok(Claimed::New)
        })
    })
    .await
    .map_err(|error| format!("write inference_verifications: {error}"))
}

/// The plan of a draft revision's claim that is already running, returned to
/// a repeated start.
fn replayed(
    inventory: &Inventory,
    claim: &VerificationRow,
    network: &Network,
) -> Result<Planned, String> {
    let deployment = inventory
        .deployments
        .get(&DeploymentId::from(claim.deployment.as_str()))
        .ok_or_else(|| {
            format!(
                "verification {} is running, and its deployment {} is gone",
                claim.id, claim.deployment
            )
        })?;
    let head = rendered_head(inventory, deployment, network)
        .map_err(|problem| format!("verification {} is running: {problem}", claim.id))?;
    Ok(Planned {
        id: claim.id.clone(),
        started_at: claim.started_at,
        profile: InferenceProfileId::from(claim.profile.as_str()),
        pool: PoolKey::from(claim.pool.as_str()),
        deployment: deployment.id.clone(),
        head,
        then_keep: None,
        replayed: true,
    })
}

/// Refuse a verification that cannot start, write its deployment and its row,
/// and return what to run. Every way a verification starts comes through
/// here, so this is where a generation's draft is held to the operator's
/// approval: the snapshot checked is the snapshot claimed and rendered, read
/// and claimed under the inventory lock so nothing replaces it in between,
/// and a refusal writes nothing. A machine it would use whose address is
/// unknown is refused before anything is written. A repeated start of a draft
/// revision whose one claim is running returns that claim.
pub async fn begin(
    db: &LocalDb,
    profile_id: &str,
    pool: &Pool,
    network: &Network,
    requested_by: &str,
    now: i64,
) -> Result<Planned, String> {
    let pool_id = &pool.key;
    let held = INVENTORY.lock().await;
    let inventory = load_inventory(db).await?;
    let profile = inventory
        .profiles
        .get(&InferenceProfileId::from(profile_id))
        .ok_or_else(|| {
            format!("there is no profile {profile_id}; cairn://inference/profiles lists them")
        })?
        .clone();
    if !matches!(profile.engine, Engine::Container(_)) {
        return Err(format!(
            "profile {profile_id} is a {} profile; verification measures container profiles",
            profile.engine.kind()
        ));
    }
    let k = profile.requirements.machines as usize;
    if pool.members.len() < k {
        return Err(format!(
            "profile {profile_id} runs on {k} machines and pool {pool_id} has {}",
            pool.members.len()
        ));
    }
    let binding = match Gate::load(db)
        .await?
        .verification_admission(&profile, pool_id.as_str())?
    {
        Admission::Ordinary => None,
        Admission::Draft(binding) => Some(binding),
        Admission::Replay(claim) => return replayed(&inventory, &claim, network),
    };
    let id = uuid::Uuid::new_v4().to_string();
    for (pinned, stored) in load_pins(db).await? {
        let on_pool = inventory
            .deployments
            .get(&pinned)
            .is_some_and(|d| d.machines.iter().any(|m| pool.has(m.as_str())));
        if !on_pool {
            continue;
        }
        let holder = &stored.pin.holder;
        if holder.starts_with("verify:") {
            return Err(format!(
                "pool {pool_id} is already being verified ({holder}); wait for it to finish"
            ));
        }
        return Err(format!(
            "pool {pool_id} is pinned by {holder} ({}); verification needs the pool to itself, so it waits for that pin to be released",
            stored.model
        ));
    }
    let machines: Vec<MachineName> = pool.machines().take(k).map(MachineName::from).collect();
    if let Some(problem) = Addresses::new(&inventory, network).unreachable(&machines[0], &machines)
    {
        return Err(format!(
            "profile {profile_id} cannot be verified on pool {pool_id}: {problem}"
        ));
    }
    let deployment = Deployment {
        id: deployment_id(&profile.id, pool_id),
        source: RecordSource::Cairn,
        profile: profile.id.clone(),
        head: machines[0].clone(),
        machines: machines.clone(),
        api_port: port_for(&profile),
        api_path: "/v1".into(),
        health_path: "/v1/models".into(),
    };
    let mut records = Inventory::default();
    records.insert_deployment(deployment.clone());
    let (changes, _) = prepare_inventory_write(
        db,
        &inventory,
        &Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove: Removals::default(),
        },
        &held,
    )
    .await?;
    let claim = Claim {
        id: id.clone(),
        profile: profile.id.to_string(),
        digest: digest(&profile),
        pool: pool_id.to_string(),
        deployment: deployment.id.to_string(),
        machines_json: serde_json::to_string(
            &machines.iter().map(|m| m.to_string()).collect::<Vec<_>>(),
        )
        .expect("machine names serialize"),
        requested_by: requested_by.to_string(),
        started_at: now,
        changes,
        binding,
    };
    match claim_pool(db, claim).await? {
        Claimed::New => {}
        Claimed::Busy(running) => {
            return Err(format!(
                "pool {pool_id} is already being verified (verification {running}); wait for it to finish"
            ))
        }
        Claimed::Replay(claimed) => {
            let claim = load_verifications(db)
                .await?
                .into_iter()
                .find(|row| row.id == claimed && row.outcome == "running")
                .ok_or_else(|| {
                    format!(
                        "profile {profile_id} cannot be verified: this revision already had its one verification, {claimed}"
                    )
                })?;
            return replayed(&load_inventory(db).await?, &claim, network);
        }
        Claimed::Unbound => {
            return Err(format!(
                "profile {profile_id} cannot be verified: its generation changed while the verification was being claimed; nothing was written"
            ))
        }
    }
    drop(held);
    let head = match rendered_head(&inventory, &deployment, network) {
        Ok(head) => head,
        Err(problem) => {
            let reason = format!("it cannot be rendered into scripts for its head: {problem}");
            finish(db, &id, false, Some(reason.clone()), now).await?;
            return Err(format!(
                "verification {id} failed at render, before anything was stopped: {reason}"
            ));
        }
    };
    Ok(Planned {
        id,
        started_at: now,
        profile: profile.id,
        pool: pool_id.clone(),
        deployment: deployment.id,
        head,
        then_keep: None,
        replayed: false,
    })
}

/// How one verification ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    Failed(String),
    /// It failed because the context did not fit; the profile was clamped and
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

/// Let the verification's deployment go: unload it and wait for it to stop
/// holding its machines. What went wrong, if anything.
async fn release(db: &LocalDb, act: &dyn Act, plan: &Planned) -> Vec<String> {
    let mut problems = Vec::new();
    if let Err(why) = act
        .unload(plan.deployment.to_string(), holder(&plan.id))
        .await
    {
        problems.push(why);
    }
    let _ = wait_for(db, act, &plan.deployment, STOP_REPORT_BOUND_SECS, |p| {
        !p.reserves()
    })
    .await;
    problems
}

/// Keep a passed verification's deployment loaded for `requester`: record
/// their request for it while the verification's pin still holds it, then
/// release the pin, so it is never stopped and started again. A request that
/// cannot be recorded leaves it to be unloaded as a verification on its own
/// is. What went wrong, if anything.
async fn keep(db: &LocalDb, act: &dyn Act, plan: &Planned, requester: &str) -> Vec<String> {
    let model = load_inventory(db).await.and_then(|inventory| {
        inventory
            .profiles
            .get(&plan.profile)
            .map(|profile| profile.model.name.clone())
            .ok_or_else(|| "the profile is gone".to_string())
    });
    let requested = match model {
        Ok(model) => {
            act.request(
                plan.deployment.to_string(),
                model,
                plan.pool.to_string(),
                requester.to_string(),
            )
            .await
        }
        Err(why) => Err(why),
    };
    if let Err(why) = requested {
        let mut problems = vec![format!(
            "it could not be kept loaded for {requester}: {why}"
        )];
        problems.extend(release(db, act, plan).await);
        return problems;
    }
    match act
        .unpin(plan.deployment.to_string(), holder(&plan.id))
        .await
    {
        Ok(_) => Vec::new(),
        Err(why) => vec![why],
    }
}

/// Run one verification to its end, then settle it: a pass of a generation
/// draft revision's approved claim promotes that revision.
///
/// A generation's draft is never revised by its own verification: clamping its
/// context would change the revision the operator approved and this claim is
/// for. A draft whose context does not fit its cache fails, naming the cache
/// it measured, so the generation can write a revision that fits.
pub async fn run(db: &LocalDb, act: &dyn Act, plan: &Planned) -> Result<Outcome, String> {
    let generated = promotion::load_owned(db)
        .await?
        .contains_key(plan.profile.as_str());
    set_stage(db, &plan.id, Stage::Start).await?;
    if let Err(why) = act.pin(plan.deployment.to_string(), holder(&plan.id)).await {
        finish(db, &plan.id, false, Some(why.clone()), act.now_ms()).await?;
        promotion::settled(db, &plan.id).await;
        return Ok(Outcome::Failed(why));
    }
    let mut measured = measure(db, act, plan).await;
    if generated {
        if let Ok(m) = &measured {
            if let Some(kv) = m.kv.filter(|kv| m.context.is_some_and(|c| c > *kv)) {
                record(db, &plan.id, m).await?;
                measured = Err(format!(
                    "it served, but its context of {} tokens is larger than the {kv}-token KV cache it measured; a generated draft is not revised by its verification, so it needs a revision whose context fits",
                    m.context.unwrap_or_default()
                ));
            }
        }
    }
    let problems = match (&measured, &plan.then_keep) {
        (Ok(_), Some(requester)) => keep(db, act, plan, requester).await,
        _ => release(db, act, plan).await,
    };
    let (outcome, reason) = match measured {
        Ok(m) => {
            record(db, &plan.id, &m).await?;
            let clamp = m.kv.filter(|kv| m.context.is_some_and(|c| c > *kv));
            if let Some(kv) = clamp {
                // The pass measured the cache the clamp is cut to, so it is
                // the measured fit of the revised profile, not a stale one.
                if let Some(revised) = clamp_profile(db, &plan.profile, kv).await? {
                    set_digest(db, &plan.id, &revised).await?;
                }
            }
            (Outcome::Passed, None)
        }
        Err(why) => match verify::kv_limit_from_failure(&why) {
            Some(kv) if !generated && clamp_profile(db, &plan.profile, kv).await?.is_some() => (
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
    promotion::settled(db, &plan.id).await;
    Ok(outcome)
}

async fn set_digest(db: &LocalDb, id: &str, digest: &str) -> Result<(), String> {
    db.execute(
        "UPDATE inference_verifications SET profile_digest = ?2 WHERE id = ?1",
        params![id, digest],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write inference_verifications: {error}"))
}

/// Clamp the profile's context in place; the revised profile's digest when it
/// changed.
async fn clamp_profile(
    db: &LocalDb,
    profile: &InferenceProfileId,
    kv: u64,
) -> Result<Option<String>, String> {
    let inventory = load_inventory(db).await?;
    let Some(revised) = inventory
        .profiles
        .get(profile)
        .and_then(|r| verify::clamp_context(r, kv))
    else {
        return Ok(None);
    };
    let revised_digest = digest(&revised);
    let mut records = Inventory::default();
    records.insert_profile(revised);
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
    // across runs, and placement starts it on its own pass once its machines
    // have room, so until then the stored phase is the previous run's.
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
    let profile = inventory
        .profiles
        .get(&plan.profile)
        .ok_or("the profile is gone")?
        .clone();
    let deployment = inventory
        .deployments
        .get(&plan.deployment)
        .ok_or("the deployment is gone")?
        .clone();
    let address = Addresses::new(&inventory, &act.network())
        .from_orch(&deployment.head)
        .ok_or_else(|| format!("the head {}", unknown_address(&deployment.head)))?
        .address;
    let mut measured = Measured {
        context: verify::context_of(&profile),
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
        measured.context = cairn_inference::served::max_model_len(&models, &profile.served_name)
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
    let embeds = profile
        .verbs
        .as_ref()
        .is_some_and(|verbs| verbs.contains(&VerbKind::Embed));
    measured.speeds = if embeds {
        let mut samples = Vec::new();
        for _ in 0..3 {
            samples.push(
                act.embed(api_base.clone(), profile.served_name.clone())
                    .await?,
            );
        }
        verify::embed_speeds(&samples)?
    } else {
        act.chat(api_base.clone(), profile.served_name.clone())
            .await?;
        let mut samples = Vec::new();
        for _ in 0..3 {
            samples.push(
                act.chat(api_base.clone(), profile.served_name.clone())
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
        let _ = act.unload(row.deployment.clone(), holder(&row.id)).await;
        promotion::settled(db, &row.id).await;
        recovered.push(row.id.clone());
    }
    for (deployment, stored) in load_pins(db).await? {
        if stored.pin.holder.starts_with("verify:") {
            let _ = act.unload(deployment.to_string(), stored.pin.holder).await;
        }
    }
    Ok(recovered)
}

/// The profiles of `model` (its real name, case-insensitive) to try on a pool
/// of `size` machines of `family` hardware, imported first, then generated,
/// more precise before less and fewer machines first.
pub fn candidates(
    inventory: &Inventory,
    model: &str,
    size: usize,
    family: Option<AcceleratorFamily>,
) -> Vec<InferenceProfileId> {
    let mut profiles: Vec<&InferenceProfile> = inventory
        .profiles
        .values()
        .filter(|r| r.model.name.trim().eq_ignore_ascii_case(model.trim()))
        .filter(|r| matches!(r.engine, Engine::Container(_)))
        .filter(|r| r.requirements.machines as usize <= size)
        .filter(|r| match (family, r.requirements.accelerator) {
            (Some(family), Some(accelerator)) => accelerator.family == family,
            _ => true,
        })
        .collect();
    let rank = |r: &InferenceProfile| match r.origin {
        ProfileOrigin::Imported { .. } => 0,
        ProfileOrigin::Authored => 1,
        ProfileOrigin::Generated { .. } => 2,
    };
    profiles.sort_by(|a, b| {
        rank(a)
            .cmp(&rank(b))
            .then_with(|| {
                let p = |r: &InferenceProfile| {
                    cairn_inference::fit::precision(r.quantization.as_deref().unwrap_or(""))
                };
                p(b).cmp(&p(a))
            })
            .then(a.requirements.machines.cmp(&b.requirements.machines))
            .then(a.id.cmp(&b.id))
    });
    profiles.into_iter().map(|r| r.id.clone()).collect()
}

/// Verify `profiles` in order on `pool`, each once more after a clamp, until
/// one passes (or every one, with `all`, unless what passes is kept loaded
/// for `then_keep`). Each verification is its own row.
#[allow(clippy::too_many_arguments)]
pub async fn verify_in_order(
    db: &LocalDb,
    act: &dyn Act,
    profiles: Vec<InferenceProfileId>,
    pool: &Pool,
    requested_by: &str,
    all: bool,
    then_keep: Option<&str>,
    first: Option<Planned>,
) -> Result<Vec<(String, Outcome)>, String> {
    let mut results = Vec::new();
    let mut first = first;
    for profile in profiles {
        let mut clamps = 0;
        loop {
            let mut plan = match first.take() {
                Some(plan) if plan.profile == profile => plan,
                _ => match begin(
                    db,
                    profile.as_str(),
                    pool,
                    &act.network(),
                    requested_by,
                    act.now_ms(),
                )
                .await
                {
                    Ok(plan) if plan.replayed => {
                        results.push((
                            plan.id.clone(),
                            Outcome::Failed(format!(
                                "verification {} of it is already running",
                                plan.id
                            )),
                        ));
                        break;
                    }
                    Ok(plan) => plan,
                    Err(why) => {
                        results.push((String::new(), Outcome::Failed(why)));
                        break;
                    }
                },
            };
            plan.then_keep = then_keep.map(str::to_string);
            let outcome = run(db, act, &plan).await?;
            results.push((plan.id.clone(), outcome.clone()));
            match outcome {
                Outcome::Clamped { .. } if clamps == 0 => clamps += 1,
                Outcome::Passed if !all || then_keep.is_some() => return Ok(results),
                _ => break,
            }
        }
    }
    Ok(results)
}

/// The live world: placement and HTTP.
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
    fn pin(&self, deployment: String, holder: String) -> Pending<'_, Result<String, String>> {
        Box::pin(async move {
            models::pin(
                &self.orch,
                PinTarget::Deployment(DeploymentId::new(deployment)),
                &holder,
            )
            .await
        })
    }

    fn unload(&self, deployment: String, holder: String) -> Pending<'_, Result<String, String>> {
        Box::pin(async move {
            models::unload(&self.orch, &DeploymentId::new(deployment), &holder).await
        })
    }

    fn request(
        &self,
        deployment: String,
        model: String,
        pool: String,
        requester: String,
    ) -> Pending<'_, Result<String, String>> {
        Box::pin(async move {
            let handle = models::request(
                &self.orch,
                &model,
                None,
                &requester,
                Some(&PoolKey::from(pool.as_str())),
            )
            .await?;
            if handle.deployment.as_str() != deployment {
                let _ = models::cancel(&self.orch, handle.id, &requester).await;
                return Err(format!(
                    "placement leased {} for {model}, not {deployment}",
                    handle.deployment
                ));
            }
            Ok(handle.status(&self.orch).await?.describe())
        })
    }

    fn unpin(&self, deployment: String, holder: String) -> Pending<'_, Result<String, String>> {
        Box::pin(async move {
            let target = PinTarget::Deployment(DeploymentId::new(deployment.clone()));
            match models::release(&self.orch, target, &holder, false).await {
                // Someone revoked it already: it is resident all the same.
                Err(why) if why == models::NOTHING_PINNED => {
                    Ok(format!("{deployment} is not pinned"))
                }
                said => said,
            }
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

    fn network(&self) -> Network {
        self.orch.fleet.network()
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

/// Start verifying `profiles` on `pool` in the background, the first planned
/// now so a refusal is answered at once; return the first verification's
/// plan. With `then_keep`, the first that passes stays loaded for them.
pub async fn spawn(
    orch: &super::Orchestrator,
    profiles: Vec<InferenceProfileId>,
    pool: Pool,
    requested_by: &str,
    all: bool,
    then_keep: Option<String>,
) -> Result<Planned, String> {
    let first = profiles
        .first()
        .ok_or_else(|| format!("no profile to verify on pool {}", pool.key))?
        .clone();
    let plan = begin(
        &orch.db.local,
        first.as_str(),
        &pool,
        &orch.fleet.network(),
        requested_by,
        crate::fleet::unix_time_ms() as i64,
    )
    .await?;
    if plan.replayed {
        return Ok(plan);
    }
    let planned = plan.clone();
    let live = Live::new(orch.clone())?;
    let requested_by = requested_by.to_string();
    let db = orch.db.local.clone();
    tokio::spawn(async move {
        if let Err(error) = verify_in_order(
            &db,
            &live,
            profiles,
            &pool,
            &requested_by,
            all,
            then_keep.as_deref(),
            Some(plan),
        )
        .await
        {
            log::warn!(
                "inference: a verification on pool {} stopped: {error}",
                pool.key
            );
        }
    });
    Ok(planned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::models::rules::Pin;
    use crate::fleet::models::store::{delete_pin, save_pin, StoredPin};
    use crate::orchestrator::inference::{save_profile, WriteError};
    use crate::orchestrator::inference_generate::{insert_generation, GenerationRow};
    use std::sync::Mutex;

    const PROFILE: &str = r#"
models:
  - {name: chat/Model}
  - {name: Qwen/Qwen3-Embedding-4B}
profiles:
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
  - {id: old, profile: old, machines: [box], head: box, apiPort: 9000, apiPath: /v1, healthPath: /v1/models}
machines:
  - {name: box, address: 10.0.0.9}
"#;

    /// The pool the profiles are verified on: `box`, on its own.
    fn solo() -> Pool {
        Pool::of_one("box")
    }

    /// A world where placement acts at once, through the same stored phases
    /// the act side writes: a pinned deployment comes up on placement's next
    /// pass (the next sleep), an unloaded one stops.
    pub(crate) struct Fake<'a> {
        db: &'a LocalDb,
        pub(crate) calls: Mutex<Vec<String>>,
        /// The reasons the next loads fail with, in order.
        failures: Mutex<Vec<String>>,
        now: Mutex<i64>,
        /// Placement refuses a pin with this.
        refuse_pin: Mutex<Option<String>>,
        /// The pinned deployment placement starts on its next pass.
        pending: Mutex<Option<String>>,
        /// Placement refuses a request with this.
        refuse_request: Mutex<Option<String>>,
        /// The KV cache the engine's metrics report, in tokens.
        kv_tokens: Mutex<u64>,
    }

    impl<'a> Fake<'a> {
        fn new(db: &'a LocalDb) -> Self {
            Self {
                db,
                calls: Mutex::default(),
                failures: Mutex::default(),
                now: Mutex::new(1_000_000),
                refuse_pin: Mutex::default(),
                pending: Mutex::default(),
                refuse_request: Mutex::default(),
                kv_tokens: Mutex::new(100_000),
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
                                       (deployment_id, profile_id, requested_at, finished_at, outcome, load_ms, footprint_json)
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
        fn pin(&self, deployment: String, holder: String) -> Pending<'_, Result<String, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("pin {deployment} {holder}"));
                if let Some(why) = self.refuse_pin.lock().unwrap().clone() {
                    return Err(why);
                }
                *self.pending.lock().unwrap() = Some(deployment);
                Ok("pinned".into())
            })
        }

        fn unload(
            &self,
            deployment: String,
            holder: String,
        ) -> Pending<'_, Result<String, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("unload {deployment} {holder}"));
                self.set(
                    &deployment,
                    Phase::Stopped {
                        since_ms: self.now_ms(),
                    },
                )
                .await;
                Ok("unloaded".into())
            })
        }

        fn request(
            &self,
            deployment: String,
            model: String,
            pool: String,
            requester: String,
        ) -> Pending<'_, Result<String, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("request {deployment} {model} {pool} {requester}"));
                match self.refuse_request.lock().unwrap().clone() {
                    Some(why) => Err(why),
                    None => Ok("requested".into()),
                }
            })
        }

        fn unpin(&self, deployment: String, holder: String) -> Pending<'_, Result<String, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("unpin {deployment} {holder}"));
                Ok("released".into())
            })
        }

        fn get(&self, url: String) -> Pending<'_, Option<String>> {
            let kv = *self.kv_tokens.lock().unwrap();
            Box::pin(async move {
                if url.ends_with("/v1/models") {
                    Some(r#"{"data":[{"id":"x","max_model_len":262144}]}"#.into())
                } else if url.ends_with("/metrics") {
                    Some(format!(
                        "vllm:cache_config_info{{kv_cache_size_tokens=\"{kv}\"}} 1\n"
                    ))
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

        fn network(&self) -> Network {
            Network::default()
        }
    }

    async fn seeded(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        write_inventory(&db, &Authoring::Replace(PROFILE.into()))
            .await
            .unwrap();
        db
    }

    /// Make `profile` revision 1 of an open generation's draft on `box`,
    /// approved by the operator or not. Its stored origin says nothing about
    /// a generation: the ownership row alone governs it.
    async fn own(db: &LocalDb, profile: &str, approved: bool) -> GenerationRow {
        use crate::orchestrator::inference_generate::promotion::tests::{generation, owned};
        let inventory = load_inventory(db).await.unwrap();
        let row = owned(&inventory.profiles[&InferenceProfileId::from(profile)], 1);
        db.execute(
            "INSERT INTO inference_generated_profiles
               (profile_id, generation_id, revision, profile_digest, surface_digest, active)
             VALUES (?1, ?2, ?3, ?4, ?5, 1)",
            params![
                row.profile.clone(),
                row.generation.clone(),
                row.revision,
                row.profile_digest.clone(),
                row.surface_digest.clone()
            ],
        )
        .await
        .unwrap();
        let mut generation = generation(&row, None);
        if !approved {
            generation.approval_revision = None;
            generation.approved_surface = None;
        }
        insert_generation(db, &generation).await.unwrap();
        generation
    }

    async fn edit(db: &LocalDb, profile: &str, from: &str, to: &str) -> Result<String, WriteError> {
        let yaml = crate::orchestrator::inference::profile_yaml(db, profile)
            .await
            .unwrap();
        assert!(yaml.contains(from), "{yaml}");
        save_profile(db, Some(profile), &yaml.replace(from, to)).await
    }

    #[tokio::test]
    async fn an_unapproved_draft_is_refused_by_every_start_of_its_verification_and_nothing_is_written(
    ) {
        let db = seeded("inference-verify-unapproved-draft").await;
        own(&db, "embed", false).await;
        let before = load_inventory(&db).await.unwrap();
        let why = begin(&db, "embed", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap_err();
        assert!(why.contains("has not approved"), "{why}");
        // A load verifies its candidates in order through the same start.
        let fake = Fake::new(&db);
        let results = verify_in_order(
            &db,
            &fake,
            vec!["embed".into()],
            &solo(),
            "operator",
            false,
            Some("operator"),
            None,
        )
        .await
        .unwrap();
        assert!(
            matches!(&results[..], [(id, Outcome::Failed(why))] if id.is_empty() && why.contains("has not approved")),
            "{results:?}"
        );
        assert_eq!(
            load_inventory(&db).await.unwrap().deployments,
            before.deployments,
            "no deployment was written"
        );
        assert!(load_verifications(&db).await.unwrap().is_empty());
        assert!(fake.calls().is_empty(), "nothing was pinned");
        // Nor on another pool than its generation's, approved or not.
        let db = seeded("inference-verify-other-pool").await;
        own(&db, "embed", true).await;
        let why = begin(
            &db,
            "embed",
            &Pool::of_one("else"),
            &Network::default(),
            "operator",
            1,
        )
        .await
        .unwrap_err();
        assert!(why.contains("not else"), "{why}");
        assert!(load_verifications(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_approved_draft_is_claimed_once_held_while_it_runs_and_promoted_only_by_its_pass() {
        let db = seeded("inference-verify-approved-draft").await;
        let generation = own(&db, "embed", true).await;
        let plan = begin(&db, "embed", &solo(), &Network::default(), "agent", 1)
            .await
            .unwrap();
        assert!(!plan.replayed);
        let bound = crate::orchestrator::inference_generate::get_generation(&db, &generation.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bound.verification_id.as_deref(), Some(plan.id.as_str()));
        // A repeated start returns the one claim.
        let again = begin(&db, "embed", &solo(), &Network::default(), "agent", 2)
            .await
            .unwrap();
        assert!(again.replayed);
        assert_eq!(again.id, plan.id);
        assert_eq!(load_verifications(&db).await.unwrap().len(), 1);
        // Neither the draft nor its claim's deployment changes while it runs.
        let refused = edit(&db, "embed", "40960", "32768").await.unwrap_err();
        assert!(String::from(refused).contains(&format!("verification {} is running", plan.id)));
        let removed = crate::orchestrator::inference::delete_profile(&db, "embed").await;
        assert!(removed.is_err());

        let fake = Fake::new(&db);
        *fake.kv_tokens.lock().unwrap() = 262_144;
        assert_eq!(run(&db, &fake, &plan).await.unwrap(), Outcome::Passed);
        let owned = &promotion::load_owned(&db).await.unwrap()["embed"];
        assert_eq!(owned.promoted.as_deref(), Some(plan.id.as_str()));
        let gate = Gate::load(&db).await.unwrap();
        assert!(!gate
            .unpromoted_profiles(&load_inventory(&db).await.unwrap())
            .contains("embed"));

        // An edit loses the promotion for good: undoing it brings back the
        // content, not the pass.
        edit(&db, "embed", "40960", "32768").await.unwrap();
        edit(&db, "embed", "32768", "40960").await.unwrap();
        assert_eq!(
            promotion::load_owned(&db).await.unwrap()["embed"].promoted,
            None
        );
        let inventory = load_inventory(&db).await.unwrap();
        assert!(Gate::load(&db)
            .await
            .unwrap()
            .unpromoted_profiles(&inventory)
            .contains("embed"));
    }

    #[tokio::test]
    async fn a_failed_verification_spends_its_revision() {
        let db = seeded("inference-verify-draft-spent").await;
        own(&db, "embed", true).await;
        let plan = begin(&db, "embed", &solo(), &Network::default(), "agent", 1)
            .await
            .unwrap();
        let fake = Fake::new(&db);
        *fake.refuse_pin.lock().unwrap() = Some("no room".into());
        assert!(matches!(
            run(&db, &fake, &plan).await.unwrap(),
            Outcome::Failed(_)
        ));
        assert_eq!(
            promotion::load_owned(&db).await.unwrap()["embed"].promoted,
            None
        );
        let why = begin(&db, "embed", &solo(), &Network::default(), "agent", 2)
            .await
            .unwrap_err();
        assert!(
            why.contains(&format!("already had its one verification, {}", plan.id)),
            "{why}"
        );
    }

    #[tokio::test]
    async fn a_generated_draft_that_does_not_fit_its_cache_fails_and_is_not_rewritten() {
        let db = seeded("inference-verify-draft-no-clamp").await;
        own(&db, "chat", true).await;
        let before =
            load_inventory(&db).await.unwrap().profiles[&InferenceProfileId::from("chat")].clone();
        let plan = begin(&db, "chat", &solo(), &Network::default(), "agent", 1)
            .await
            .unwrap();
        let fake = Fake::new(&db);
        let outcome = run(&db, &fake, &plan).await.unwrap();
        assert!(
            matches!(&outcome, Outcome::Failed(why) if why.contains("100000-token KV cache")),
            "{outcome:?}"
        );
        assert_eq!(
            load_inventory(&db).await.unwrap().profiles[&InferenceProfileId::from("chat")],
            before,
            "the approved revision is not clamped in place"
        );
        assert_eq!(
            promotion::load_owned(&db).await.unwrap()["chat"].promoted,
            None
        );
    }

    #[tokio::test]
    async fn the_final_start_refuses_an_unpromoted_draft_and_starts_only_its_approved_claim() {
        use crate::orchestrator::inference_control::tests::FakeHeads;
        use crate::orchestrator::inference_control::{launch, load_states, request};
        use cairn_inference::control::Request;

        let db = seeded("inference-verify-final-start").await;
        own(&db, "embed", true).await;
        // A deployment of the draft written by hand, as a stale one would be.
        write_inventory(
            &db,
            &Authoring::Patch {
                upsert: Some(
                    "deployments:\n  - {id: embed-box, profile: embed, machines: [box], head: box, apiPort: 8090, apiPath: /v1, healthPath: /v1/models}\n"
                        .into(),
                ),
                remove: Removals::default(),
            },
        )
        .await
        .unwrap();
        let heads = FakeHeads::default();
        heads.attach("box", 100, 0);
        request(&db, &heads, "embed-box", Request::Start, 1)
            .await
            .unwrap();
        launch(&db, &heads, &"embed-box".into(), 2).await.unwrap();
        assert!(heads.starts().is_empty(), "nothing was sent");
        let phase = &load_states(&db).await.unwrap()[&DeploymentId::from("embed-box")].phase;
        assert!(
            format!("{phase:?}").contains("no verification of it has passed"),
            "{phase:?}"
        );

        // The approved claim's own deployment starts.
        let plan = begin(&db, "embed", &solo(), &Network::default(), "agent", 3)
            .await
            .unwrap();
        request(&db, &heads, plan.deployment.as_str(), Request::Start, 4)
            .await
            .unwrap();
        launch(&db, &heads, &plan.deployment, 5).await.unwrap();
        assert_eq!(heads.starts(), vec![plan.deployment.to_string()]);
        // The stale one still does not, while the claim runs.
        request(&db, &heads, "embed-box", Request::Start, 6)
            .await
            .unwrap();
        launch(&db, &heads, &"embed-box".into(), 7).await.unwrap();
        assert_eq!(heads.starts(), vec![plan.deployment.to_string()]);
    }

    #[tokio::test]
    async fn a_pass_pins_records_its_measurements_and_unloads() {
        let db = seeded("inference-verify-pass").await;
        let fake = Fake::new(&db);
        let plan = begin(&db, "embed", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap();
        assert_eq!(plan.deployment.as_str(), "verify-embed-box");
        assert_eq!(run(&db, &fake, &plan).await.unwrap(), Outcome::Passed);
        let holder = format!("verify:{}", plan.id);
        assert_eq!(
            fake.calls(),
            vec![
                format!("pin verify-embed-box {holder}"),
                format!("unload verify-embed-box {holder}"),
            ]
        );
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(row.outcome, "passed");
        assert_eq!(row.stage, "done");
        assert_eq!(row.pool, "box");
        assert_eq!(row.load_ms, Some(42_000));
        assert_eq!(row.footprint.as_ref().unwrap()["box"], 9 << 30);
        assert_eq!(row.embed_tps, Some(2_000.0));
        assert_eq!(row.engine_version.as_deref(), Some("0.32.0"));
        assert!(!row.stale(&load_inventory(&db).await.unwrap()));
    }

    /// A verification a load started, planned on `box`.
    async fn load_plan(db: &LocalDb, profile: &str) -> Planned {
        Planned {
            then_keep: Some("operator".into()),
            ..begin(db, profile, &solo(), &Network::default(), "operator", 1)
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    async fn a_load_s_pass_hands_the_pin_to_a_request_and_never_stops_it() {
        let db = seeded("inference-verify-keep").await;
        let fake = Fake::new(&db);
        let plan = load_plan(&db, "embed").await;
        assert_eq!(run(&db, &fake, &plan).await.unwrap(), Outcome::Passed);
        let holder = format!("verify:{}", plan.id);
        assert_eq!(
            fake.calls(),
            vec![
                format!("pin verify-embed-box {holder}"),
                "request verify-embed-box Qwen/Qwen3-Embedding-4B box operator".to_string(),
                format!("unpin verify-embed-box {holder}"),
            ],
            "the request is recorded while the pin holds it, and nothing unloads it"
        );
        assert!(matches!(
            phase(&db, &plan.deployment).await.unwrap(),
            Some(Phase::Healthy { .. })
        ));
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(
            (row.outcome.as_str(), row.reason.as_deref()),
            ("passed", None)
        );
        assert_eq!(row.embed_tps, Some(2_000.0));
    }

    #[tokio::test]
    async fn a_load_s_failure_unloads_as_any_verification_does() {
        let db = seeded("inference-verify-keep-fail").await;
        let fake = Fake::new(&db);
        fake.failures
            .lock()
            .unwrap()
            .push("CUDA driver version is insufficient for CUDA runtime version".into());
        let plan = load_plan(&db, "chat").await;
        assert!(matches!(
            run(&db, &fake, &plan).await.unwrap(),
            Outcome::Failed(_)
        ));
        let holder = format!("verify:{}", plan.id);
        assert_eq!(
            fake.calls(),
            vec![
                format!("pin verify-chat-box {holder}"),
                format!("unload verify-chat-box {holder}"),
            ]
        );
        assert_eq!(load_verifications(&db).await.unwrap()[0].outcome, "failed");
    }

    #[tokio::test]
    async fn a_load_s_pass_placement_will_not_lease_is_unloaded_and_says_why() {
        let db = seeded("inference-verify-keep-refused").await;
        let fake = Fake::new(&db);
        *fake.refuse_request.lock().unwrap() = Some("verify-embed-box is in no pool".into());
        let plan = load_plan(&db, "embed").await;
        assert_eq!(run(&db, &fake, &plan).await.unwrap(), Outcome::Passed);
        assert_eq!(
            fake.calls().last().unwrap(),
            &format!("unload verify-embed-box verify:{}", plan.id)
        );
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(
            row.reason.as_deref(),
            Some("passed; afterwards: it could not be kept loaded for operator: verify-embed-box is in no pool")
        );
    }

    #[tokio::test]
    async fn a_load_stops_at_the_first_pass_even_when_asked_for_every_profile() {
        let db = seeded("inference-verify-keep-first").await;
        let fake = Fake::new(&db);
        let results = verify_in_order(
            &db,
            &fake,
            vec!["embed".into(), "chat".into()],
            &solo(),
            "operator",
            true,
            Some("operator"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 1, "{results:?}");
        assert_eq!(results[0].1, Outcome::Passed);
        assert!(fake.calls().iter().all(|call| !call.starts_with("unload")));
    }

    #[tokio::test]
    async fn a_pin_waits_for_this_runs_load_not_the_last_ones_failure() {
        let db = seeded("inference-verify-this-run").await;
        let fake = Fake::new(&db);
        fake.set(
            "verify-chat-box",
            Phase::Failed {
                at_ms: 5,
                reason: "an earlier run's failure".into(),
            },
        )
        .await;
        let plan = begin(
            &db,
            "chat",
            &solo(),
            &Network::default(),
            "operator",
            fake.now_ms(),
        )
        .await
        .unwrap();
        assert_eq!(run(&db, &fake, &plan).await.unwrap(), Outcome::Passed);
    }

    #[tokio::test]
    async fn a_kv_failure_clamps_the_profile_and_the_revision_passes() {
        let db = seeded("inference-verify-clamp").await;
        let fake = Fake::new(&db);
        fake.failures.lock().unwrap().push(
            "ValueError: The model's max seq len (262144) is larger than the maximum number of tokens that can be stored in KV cache (120000).".into(),
        );
        let results = verify_in_order(
            &db,
            &fake,
            vec!["chat".into()],
            &Pool::of_one("box"),
            "operator",
            false,
            None,
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
        let chat = &inventory.profiles[&InferenceProfileId::from("chat")];
        assert_eq!(
            verify::context_of(chat),
            Some(99_328),
            "the pass measured 100000 tokens of KV cache and clamped again"
        );
        let rows = load_verifications(&db).await.unwrap();
        let failed = rows.iter().find(|r| r.outcome == "failed").unwrap();
        assert_eq!(failed.stage, "start");
        assert!(failed.stale(&inventory), "the profile changed after it ran");
        let passed = rows.iter().find(|r| r.outcome == "passed").unwrap();
        assert!(
            !passed.stale(&inventory),
            "the pass measured the clamp it made"
        );
        assert_eq!(passed.decode_tps, Some(50.0));
        assert_eq!(passed.kv_tokens, Some(100_000));
    }

    #[tokio::test]
    async fn a_failure_is_recorded_and_the_deployment_unloaded() {
        let db = seeded("inference-verify-fail").await;
        let fake = Fake::new(&db);
        fake.failures
            .lock()
            .unwrap()
            .push("CUDA driver version is insufficient for CUDA runtime version".into());
        let plan = begin(&db, "chat", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap();
        let outcome = run(&db, &fake, &plan).await.unwrap();
        assert!(matches!(&outcome, Outcome::Failed(why) if why.contains("driver")));
        assert_eq!(
            fake.calls().last().unwrap(),
            &format!("unload verify-chat-box verify:{}", plan.id)
        );
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(
            (row.outcome.as_str(), row.stage.as_str()),
            ("failed", "start")
        );
    }

    #[tokio::test]
    async fn a_refused_pin_fails_with_placements_reason_and_changes_nothing() {
        let db = seeded("inference-verify-refused").await;
        let fake = Fake::new(&db);
        *fake.refuse_pin.lock().unwrap() =
            Some("verify-embed-box cannot run: box is in no detected pool".into());
        let plan = begin(&db, "embed", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap();
        let outcome = run(&db, &fake, &plan).await.unwrap();
        assert_eq!(
            outcome,
            Outcome::Failed("verify-embed-box cannot run: box is in no detected pool".into())
        );
        assert_eq!(
            fake.calls(),
            vec![format!("pin verify-embed-box verify:{}", plan.id)],
            "nothing was acquired, so nothing is unloaded"
        );
    }

    #[tokio::test]
    async fn a_pool_pinned_by_someone_else_is_refused_and_boot_recovers_a_running_one() {
        let db = seeded("inference-verify-pinned").await;
        let old = DeploymentId::from("old");
        save_pin(
            &db,
            &old,
            &StoredPin {
                model: "chat/Model".into(),
                pin: Pin {
                    holder: "agent-7".into(),
                    at: 0,
                },
            },
        )
        .await
        .unwrap();
        let refusal = begin(&db, "chat", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap_err();
        assert!(
            refusal.contains("pinned by agent-7 (chat/Model)"),
            "{refusal}"
        );

        delete_pin(&db, &old).await.unwrap();
        let plan = begin(&db, "chat", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap();
        let fake = Fake::new(&db);
        assert_eq!(
            recover(fake.db, &fake).await.unwrap(),
            vec![plan.id.clone()]
        );
        let row = &load_verifications(&db).await.unwrap()[0];
        assert_eq!(row.reason.as_deref(), Some("orch restarted"));
        assert_eq!(
            fake.calls(),
            vec![format!("unload verify-chat-box verify:{}", plan.id)]
        );
    }

    #[tokio::test]
    async fn a_profile_that_cannot_render_fails_before_anything_is_stopped() {
        let db = seeded("inference-verify-render").await;
        let mut inventory = load_inventory(&db).await.unwrap();
        let mut linked = inventory.profiles[&InferenceProfileId::from("chat")].clone();
        if let Engine::Container(container) = &mut linked.engine {
            container.args.push("{self.link}".into());
        }
        inventory.insert_profile(linked);
        write_inventory(&db, &Authoring::Replace(inventory.to_yaml()))
            .await
            .unwrap();
        let refusal = begin(&db, "chat", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap_err();
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

    /// An enrolled executor needs no machine record: its verification is
    /// planned at the address its connection came from. A machine neither
    /// recorded nor attached is refused before anything is written.
    #[tokio::test]
    async fn an_enrolled_executor_is_verified_where_it_connected_from() {
        let db = seeded("inference-verify-observed").await;
        let mut inventory = load_inventory(&db).await.unwrap();
        inventory.machines.clear();
        write_inventory(&db, &Authoring::Replace(inventory.to_yaml()))
            .await
            .unwrap();

        let refusal = begin(&db, "embed", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            "profile embed cannot be verified on pool box: box's address is unknown: no executor named box is attached and it has no machine record; enrol it as an executor or add a machine record"
        );
        assert!(load_verifications(&db).await.unwrap().is_empty());

        let observed = Network {
            connections: BTreeMap::from([("box".to_string(), "10.0.0.7".parse().unwrap())]),
            ..Network::default()
        };
        let plan = begin(&db, "embed", &solo(), &observed, "operator", 1)
            .await
            .unwrap();
        assert_eq!(
            plan.head.to_string(),
            "10.0.0.7 (observed from its connection)"
        );
    }

    #[tokio::test]
    async fn one_verification_holds_a_pool_at_a_time() {
        let db = seeded("inference-verify-one").await;
        let first = begin(&db, "chat", &solo(), &Network::default(), "operator", 1)
            .await
            .unwrap();
        let refusal = begin(&db, "embed", &solo(), &Network::default(), "operator", 2)
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            format!(
                "pool box is already being verified (verification {}); wait for it to finish",
                first.id
            )
        );
    }

    /// Two verifications of one pool claim it at once. Each reads the pool as
    /// idle and inserts its own row, so only a key over the running rows can
    /// make them conflict (cairn/6166). It races the claim itself: through
    /// `begin`, the inventory write before it staggers the two and hides the
    /// race. Looped, from two OS threads: two tasks on one test runtime did
    /// not interleave these transactions when measured (0 doubled claims in
    /// 200 rounds, against 199 in 200 from threads without the index).
    #[test]
    fn racing_claims_of_one_pool_admit_exactly_one() {
        use crate::storage::RowExt;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let db = runtime.block_on(seeded("inference-verify-race"));
        for round in 0..100 {
            let barrier = std::sync::Barrier::new(2);
            let claims = ["chat", "embed"].map(|profile| Claim {
                id: format!("{profile}-{round}"),
                profile: profile.into(),
                digest: "digest".into(),
                pool: "box".into(),
                deployment: format!("verify-{profile}-box"),
                machines_json: r#"["box"]"#.into(),
                requested_by: "operator".into(),
                started_at: round,
                changes: Vec::new(),
                binding: None,
            });
            let admitted = std::thread::scope(|scope| {
                let claimed: Vec<_> = claims
                    .into_iter()
                    .map(|claim| {
                        let (db, barrier, handle) = (&db, &barrier, runtime.handle().clone());
                        scope.spawn(move || {
                            barrier.wait();
                            matches!(
                                handle.block_on(claim_pool(db, claim)).unwrap(),
                                Claimed::New
                            )
                        })
                    })
                    .collect();
                claimed
                    .into_iter()
                    .map(|claim| claim.join().unwrap())
                    .filter(|admitted| *admitted)
                    .count()
            });
            let running = runtime
                .block_on(db.query_one(
                    "SELECT COUNT(*) FROM inference_verifications
                      WHERE pool_id = 'box' AND outcome = 'running'",
                    (),
                    |row| row.i64(0),
                ))
                .unwrap();
            assert_eq!(
                (admitted, running),
                (1, 1),
                "round {round}: one verification holds the pool"
            );
            runtime
                .block_on(db.execute("DELETE FROM inference_verifications", ()))
                .unwrap();
        }
    }

    #[test]
    fn candidates_take_imported_then_generated_more_precise_first() {
        let mut inventory = Inventory::from_yaml(PROFILE).unwrap();
        let mut generated = inventory.profiles[&InferenceProfileId::from("chat")].clone();
        generated.id = "chat-gen".into();
        generated.origin = ProfileOrigin::Generated {
            run: None,
            model_revision: "abc".into(),
            starting_template: Some("vllm-openai".into()),
            sources: vec![],
            answers: vec![],
        };
        inventory.insert_profile(generated);
        let mut imported = inventory.profiles[&InferenceProfileId::from("chat")].clone();
        imported.id = "chat-imp".into();
        imported.origin = ProfileOrigin::Imported {
            url: "u".into(),
            commit: "c".into(),
            compose: "c".into(),
            service: "s".into(),
        };
        inventory.insert_profile(imported);
        assert_eq!(
            candidates(&inventory, "chat/model", 1, Some(AcceleratorFamily::Nvidia)),
            vec![
                InferenceProfileId::from("chat-imp"),
                "chat".into(),
                "chat-gen".into()
            ]
        );
    }
}
