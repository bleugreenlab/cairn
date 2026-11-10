//! Generating a profile is an ordinary, messageable issue session (tenet 1364).
//!
//! The issue owns conversation and execution status. The private generation
//! ledger owns its exact launch binding, candidate budget, draft revision,
//! consent and cleanup. A candidate advances inventory and authority together.
//! Only server-minted operator consent admits the exact revision to verification;
//! only verification can promote it for ordinary loading.
//!
//! Scope survives terminal state and session rotation. Legacy workflows are
//! fenced and reconciled at upgrade, never converted into ordinary sessions.

use std::collections::BTreeMap;

use cairn_common::executor_protocol::{
    model_gpus, ExecutorHealthSnapshot, GpuDevice, GpuMemory, GpuVendor, MachineMemory,
    MeasurementReading,
};
use cairn_inference::fit::{self, Arch, HwMemory, MachineHw, PoolHw, Predicted};
use cairn_inference::hf::{self, RepoFiles};
use cairn_inference::{
    AcceleratorFamily, Addresses, CitedSource, Engine, InferenceProfile, InferenceProfileId,
    Inventory, MachineName, Model, Network, OperatorAnswer, ProfileOrigin, RecordSource,
};
use cairn_models::types::catalog::Modality;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::inference::{load_inventory, Authoring, Removals};
use super::inference_models::{weight_sources, Hub};
use super::Orchestrator;
use crate::fleet::pools::Pool;

pub(crate) mod scope;
use crate::storage::{LocalDb, RowExt};

mod approval;
pub(crate) use approval::{
    approval_answered, draft_replaced, request_approval, stamp_operator_answers,
};
pub use approval::{
    approval_holds, approval_question, approve_payload, current_draft, unapproved_draft,
    ApprovalOutcome, Draft, APPROVE, REJECT,
};

/// Revision-bound verification and promotion of generated profiles.
pub mod promotion;

/// Regenerating a generated profile in place.
pub(crate) mod replacement;

/// A generation still being worked on.
pub const RUNNING: &str = "running";

/// The latest health of every attached executor, by public name.
pub(crate) fn executor_snapshots(orch: &Orchestrator) -> BTreeMap<String, ExecutorHealthSnapshot> {
    orch.fleet
        .executor_health(crate::fleet::unix_time_ms())
        .into_iter()
        .map(|snapshot| (snapshot.public_name.clone(), snapshot))
        .collect()
}

/// The pool placement detects now under `key`.
pub(crate) fn detected_pool(orch: &Orchestrator, key: &str) -> Result<Pool, String> {
    orch.fleet
        .detected_pools()
        .into_iter()
        .find(|pool| pool.key.0 == key)
        .ok_or_else(|| {
            format!("there is no pool {key} now; cairn://inference/pools lists the pools placement detects")
        })
}

/// A pool's key from its machine names in any order (`bginf-sp2+bginf-sp1`).
pub fn pool_key(machines: &str) -> String {
    let mut names: Vec<&str> = machines
        .split('+')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect();
    names.sort_unstable();
    names.dedup();
    names.join("+")
}

/// The detected pool a generation names: a pool's key, or its machines
/// joined with `+` in any order.
pub(crate) fn resolve_pool(orch: &Orchestrator, pool: &str) -> Result<Pool, String> {
    detected_pool(orch, &pool_key(pool))
}

const GIB: u64 = 1 << 30;

/// A machine's hardware as an operator stated it on a generation recorded
/// before every machine's hardware came from its executor's report. New
/// generations state none; earlier ones are still read with what they stated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatedHardware {
    pub family: AcceleratorFamily,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<String>,
    pub memory_gib: f64,
    /// Whether the GPU's memory is the machine's (unified) or its own VRAM.
    pub unified: bool,
    pub arch: String,
}

impl StatedHardware {
    fn machine(&self, name: &MachineName) -> Result<MachineHw, String> {
        Ok(MachineHw {
            name: name.clone(),
            family: self.family,
            gpu: self.gpu.clone().unwrap_or_default(),
            memory: {
                let bytes = (self.memory_gib * GIB as f64) as u64;
                if self.unified {
                    HwMemory::Unified { total: bytes }
                } else {
                    HwMemory::Discrete { vram: bytes }
                }
            },
            arch: Arch::parse(&self.arch)
                .ok_or_else(|| format!("architecture {} is not aarch64 or x86_64", self.arch))?,
        })
    }
}

/// The hardware one executor reports, or why it reports too little.
pub fn reported_hardware(
    name: &MachineName,
    snapshot: &ExecutorHealthSnapshot,
) -> Result<MachineHw, String> {
    hardware_from(
        name,
        &snapshot.advertisement.capabilities.arch,
        &snapshot.host.gpus.reading,
        &snapshot.machine.memory.reading,
    )
}

/// A machine's hardware from its readings: the accelerator family is the
/// vendor of the GPUs a model runs on, so a machine with no such GPU read has
/// none, and is refused naming it rather than given one.
fn hardware_from(
    name: &MachineName,
    arch: &str,
    gpus: &MeasurementReading<Vec<GpuDevice>>,
    memory: &MeasurementReading<MachineMemory>,
) -> Result<MachineHw, String> {
    let arch = Arch::parse(arch).ok_or_else(|| {
        format!("{name} reports architecture {arch}, which is not aarch64 or x86_64")
    })?;
    let state = |why: String| {
        format!(
            "{name}'s accelerator is unknown: {why}; attach {name}'s executor where it can read its GPU (NVML for NVIDIA, amdgpu for AMD) so it reports one, then try again"
        )
    };
    let devices = match gpus {
        MeasurementReading::Measured { value } => value,
        MeasurementReading::Unavailable { detail, .. } => {
            return Err(state(match detail {
                Some(detail) => format!("it has not reported its GPUs ({detail})"),
                None => "it has not reported its GPUs".into(),
            }))
        }
    };
    let gpus = model_gpus(devices);
    let Some(device) = gpus.first() else {
        return Err(state("it reports no GPU it can read".into()));
    };
    let family = match device.vendor {
        GpuVendor::Nvidia => AcceleratorFamily::Nvidia,
        GpuVendor::Apple => AcceleratorFamily::Apple,
        GpuVendor::Amd => AcceleratorFamily::Amd,
    };
    let memory = match &device.memory {
        MeasurementReading::Measured {
            value: GpuMemory::Discrete { .. },
        } => HwMemory::Discrete {
            vram: gpus
                .iter()
                .filter_map(|gpu| match &gpu.memory {
                    MeasurementReading::Measured {
                        value: GpuMemory::Discrete { total_bytes, .. },
                    } => Some(*total_bytes),
                    _ => None,
                })
                .sum(),
        },
        MeasurementReading::Measured {
            value: GpuMemory::Unified { .. },
        } => match memory {
            MeasurementReading::Measured { value } => HwMemory::Unified {
                total: value.total_bytes,
            },
            MeasurementReading::Unavailable { .. } => {
                return Err(format!("{name} has not reported its memory"))
            }
        },
        MeasurementReading::Unavailable { .. } => {
            return Err(format!("{name} has not reported its GPU's memory"))
        }
    };
    Ok(MachineHw {
        name: name.clone(),
        family,
        gpu: device.model.clone(),
        memory,
        arch,
    })
}

/// A detected pool's hardware: each machine's as its executor reports it, or
/// as an earlier generation stated it, and whether every machine has a
/// fast-link address.
pub fn pool_hardware(
    pool: &Pool,
    snapshots: &BTreeMap<String, ExecutorHealthSnapshot>,
    stated: &BTreeMap<String, StatedHardware>,
) -> Result<PoolHw, String> {
    let mut machines = Vec::new();
    let mut problems = Vec::new();
    for member in &pool.members {
        let name = MachineName::from(member.machine.as_str());
        let machine = match stated.get(member.machine.as_str()) {
            Some(stated) => stated.machine(&name),
            None => match snapshots.get(member.machine.as_str()) {
                Some(snapshot) => reported_hardware(&name, snapshot),
                None => Err(format!(
                    "{} is not attached, so its hardware is unknown; attach its executor so it reports its GPU, then try again",
                    member.machine
                )),
            },
        };
        match machine {
            Ok(machine) => machines.push(machine),
            Err(why) => problems.push(why),
        }
    }
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    Ok(PoolHw {
        linked: pool.members.len() > 1 && pool.members.iter().all(|m| m.link_address.is_some()),
        machines,
    })
}

/// One machine as the generating agent reads it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineFacts {
    pub name: String,
    pub family: AcceleratorFamily,
    pub gpu: String,
    /// `unified` (the machine's memory is the GPU's) or `discrete`.
    pub memory: &'static str,
    pub memory_bytes: u64,
    /// What [`fit::fit`] lets an engine take here.
    pub usable_bytes: u64,
    pub arch: Arch,
    pub blackwell: bool,
    /// Whether this machine's hardware is what an earlier generation stated,
    /// not its executor's report.
    pub stated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_address: Option<String>,
}

/// A pool's hardware as the generating agent reads it: exactly the
/// [`PoolHw`] the fit arithmetic uses, and the link joining it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolFacts {
    pub pool: String,
    pub linked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_gbps: Option<u32>,
    pub machines: Vec<MachineFacts>,
}

pub fn pool_facts(
    pool: &Pool,
    hardware: &PoolHw,
    inventory: &Inventory,
    network: &Network,
    stated: &BTreeMap<String, StatedHardware>,
) -> PoolFacts {
    let addresses = Addresses::new(inventory, network);
    PoolFacts {
        pool: pool.key.0.clone(),
        linked: hardware.linked,
        link_gbps: pool.rate_gbps,
        machines: hardware
            .machines
            .iter()
            .map(|machine| {
                let member = pool
                    .members
                    .iter()
                    .find(|member| member.machine == machine.name.as_str());
                MachineFacts {
                    name: machine.name.to_string(),
                    family: machine.family,
                    gpu: machine.gpu.clone(),
                    memory: match machine.memory {
                        HwMemory::Unified { .. } => "unified",
                        HwMemory::Discrete { .. } => "discrete",
                    },
                    memory_bytes: machine.memory_bytes(),
                    usable_bytes: machine.usable_bytes(),
                    arch: machine.arch,
                    blackwell: machine.blackwell(),
                    stated: stated.contains_key(machine.name.as_str()),
                    address: addresses
                        .from_orch(&machine.name)
                        .map(|address| address.to_string()),
                    link_address: member
                        .and_then(|member| member.link_address)
                        .map(|address| address.to_string()),
                }
            })
            .collect(),
    }
}

/// The pool `pool` names, with its hardware, stated where `stated` says.
pub(crate) fn hardware_of(
    orch: &Orchestrator,
    pool: &str,
    stated: &BTreeMap<String, StatedHardware>,
) -> Result<(Pool, PoolHw), String> {
    let detected = resolve_pool(orch, pool)?;
    let hardware = pool_hardware(&detected, &executor_snapshots(orch), stated)?;
    Ok((detected, hardware))
}

/// A registered model's repositories' files, sorted by what they are to it,
/// and the derivatives that could not be read.
pub async fn model_files(hub: &dyn Hub, model: &Model) -> Result<(RepoFiles, Vec<String>), String> {
    let (repos, skipped) = weight_sources(hub, model).await?;
    Ok((hf::classify(&repos, model.facts.dtype.as_deref()), skipped))
}

/// What [`fit::fit`] says of one weight source on a pool.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FitAnswer {
    pub source: String,
    pub quantization: String,
    pub weight_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicted: Option<Predicted>,
    /// Why it does not fit, when it does not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
}

/// The fit of `source` (a source key), or of every source when `None`.
pub fn fit_sources(
    model: &Model,
    files: &RepoFiles,
    source: Option<&str>,
    hardware: &PoolHw,
) -> Result<Vec<FitAnswer>, String> {
    let sources: Vec<&hf::WeightSource> = match source {
        Some(key) => vec![files
            .source(key)
            .ok_or_else(|| unknown_source(files, key))?],
        None => files.sources.iter().collect(),
    };
    Ok(sources
        .into_iter()
        .map(|source| {
            let fits = fit::fit(model, source.bytes(), hardware);
            FitAnswer {
                source: source.key.clone(),
                quantization: source.quantization.clone(),
                weight_bytes: source.bytes(),
                why: fits.as_ref().err().cloned(),
                predicted: fits.ok(),
            }
        })
        .collect())
}

fn unknown_source(files: &RepoFiles, key: &str) -> String {
    format!(
        "the model has no weight source {key}; its sources are {}",
        files
            .sources
            .iter()
            .map(|source| source.key.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Whether a model takes image or audio input, which a llama.cpp profile can
/// serve only with a projector.
fn needs_projector(model: &Model) -> bool {
    model.facts.modalities.as_ref().is_some_and(|modalities| {
        modalities
            .input
            .iter()
            .any(|m| matches!(m, Modality::Image | Modality::Audio))
    })
}

/// A shipped template made into a whole profile for `source` on the pool, as
/// a starting point. A projector is the one named by its path, or for a model
/// that takes images, the one beside the weights at their precision.
pub fn starting_profile(
    template: &str,
    model: &Model,
    files: &RepoFiles,
    source: &str,
    projector: Option<&str>,
    hardware: &PoolHw,
) -> Result<InferenceProfile, String> {
    let templates = fit::templates();
    let template = templates.iter().find(|t| t.id == template).ok_or_else(|| {
        format!(
            "there is no template {template}; the templates are {}",
            templates
                .iter()
                .map(|t| t.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })?;
    template.serves(hardware).map_err(|why| {
        match fit::templates_for(hardware).map(|serving| {
            serving
                .iter()
                .map(|t| t.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        }) {
            Ok(serving) if !serving.is_empty() => {
                format!("{why}; the templates that serve this pool are {serving}")
            }
            Ok(_) => format!("{why}; no shipped template serves this pool"),
            Err(_) => why,
        }
    })?;
    let source = files
        .source(source)
        .ok_or_else(|| unknown_source(files, source))?;
    let predicted = fit::fit(model, source.bytes(), hardware)?;
    let projector = match projector {
        Some(path) => Some(
            files
                .projectors
                .iter()
                .find(|p| p.file.path == path)
                .ok_or_else(|| format!("the model has no projector {path}"))?,
        ),
        None if needs_projector(model) => files.projector_for(source),
        None => None,
    };
    Ok(fit::starting_profile(
        template, source, model, hardware, &predicted, projector,
    ))
}

// ---------------------------------------------------------------------------
// Generations
// ---------------------------------------------------------------------------

/// One generation, as `inference_generations` keeps it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationRow {
    pub id: String,
    pub project_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue_id: Option<String>,
    pub job_id: String,
    /// Historical workflows are fenced at upgrade; new launches use `agent`.
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    pub attempts: i64,
    pub draft_revision: i64,
    /// Canonical full-profile digest, not merely the runnable surface.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub draft_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_prompt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_revision: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_id: Option<String>,
    pub cleanup_pending: bool,
    /// The workflow node running it: the provenance `run` of what it writes.
    pub node_uri: String,
    pub model: String,
    pub pool: String,
    /// `running` until it ends: `verified`, `cannotRun`, `gaveUp`,
    /// `cancelled`.
    pub state: String,
    /// The draft it has written that is still in the inventory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub draft_profile: Option<String>,
    /// When the draft was last written; a verification counts only if it
    /// started after.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub draft_written_at: Option<i64>,
    /// The digest of the draft's runnable surface the operator approved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approved_surface: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub hardware: BTreeMap<String, StatedHardware>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Value>,
    pub started_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
}

impl GenerationRow {
    pub fn is_open(&self) -> bool {
        self.state == RUNNING
    }
}

const COLUMNS: &str = "id, project_id, issue_id, job_id, node_uri, model, pool, state, \
     draft_profile, hardware_json, outcome_json, started_at, finished_at, draft_written_at, \
     approved_surface, mode, execution_id, attempts, draft_revision, draft_digest, \
     approval_prompt_id, approval_run_id, approval_revision, verification_id, cleanup_pending";

fn generation_of(row: &cairn_db::turso::Row) -> crate::storage::DbResult<GenerationRow> {
    Ok(GenerationRow {
        id: row.text(0)?,
        project_id: row.text(1)?,
        issue_id: row.opt_text(2)?,
        job_id: row.text(3)?,
        node_uri: row.text(4)?,
        model: row.text(5)?,
        pool: row.text(6)?,
        state: row.text(7)?,
        draft_profile: row.opt_text(8)?,
        hardware: row
            .opt_text(9)?
            .and_then(|json| serde_json::from_str(&json).ok())
            .unwrap_or_default(),
        outcome: row
            .opt_text(10)?
            .and_then(|json| serde_json::from_str(&json).ok()),
        started_at: row.i64(11)?,
        finished_at: row.opt_i64(12)?,
        draft_written_at: row.opt_i64(13)?,
        approved_surface: row.opt_text(14)?,
        mode: row.text(15)?,
        execution_id: row.opt_text(16)?,
        attempts: row.i64(17)?,
        draft_revision: row.i64(18)?,
        draft_digest: row.opt_text(19)?,
        approval_prompt_id: row.opt_text(20)?,
        approval_run_id: row.opt_text(21)?,
        approval_revision: row.opt_i64(22)?,
        verification_id: row.opt_text(23)?,
        cleanup_pending: row.i64(24)? != 0,
    })
}

async fn select_generations(
    db: &LocalDb,
    clause: &'static str,
    arg: Option<String>,
) -> Result<Vec<GenerationRow>, String> {
    db.read(move |conn| {
        let arg = arg.clone();
        Box::pin(async move {
            let sql = format!("SELECT {COLUMNS} FROM inference_generations {clause}");
            let mut rows = match arg {
                Some(arg) => conn.query(&sql, (arg.as_str(),)).await?,
                None => conn.query(&sql, ()).await?,
            };
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(generation_of(&row)?);
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Every generation, newest first.
pub async fn list_generations(db: &LocalDb) -> Result<Vec<GenerationRow>, String> {
    select_generations(db, "ORDER BY started_at DESC, id DESC", None).await
}

pub async fn get_generation(db: &LocalDb, id: &str) -> Result<Option<GenerationRow>, String> {
    Ok(
        select_generations(db, "WHERE id = ?1", Some(id.to_string()))
            .await?
            .pop(),
    )
}

/// The open generation bound to a job, if it has one.
pub async fn open_generation_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<GenerationRow>, String> {
    Ok(select_generations(
        db,
        "WHERE job_id = ?1 AND state = 'running'",
        Some(job_id.to_string()),
    )
    .await?
    .pop())
}

pub async fn insert_generation(db: &LocalDb, row: &GenerationRow) -> Result<(), String> {
    let row = row.clone();
    db.write(move |conn| {
        let row = row.clone();
        Box::pin(async move { insert_generation_in(conn, &row).await })
    })
    .await
    .map_err(|error| error.to_string())
}

/// [`insert_generation`] inside the caller's transaction.
pub(crate) async fn insert_generation_in(
    conn: &crate::storage::TrackedConnection,
    row: &GenerationRow,
) -> crate::storage::DbResult<()> {
    let hardware = serde_json::to_string(&row.hardware)
        .map_err(|error| crate::storage::DbError::Row(error.to_string()))?;
    let outcome = row.outcome.as_ref().map(Value::to_string);
    conn.execute(
        &format!(
            "INSERT INTO inference_generations ({COLUMNS}) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)"
        ),
        cairn_db::turso::params![
            row.id.as_str(),
            row.project_id.as_str(),
            row.issue_id.as_deref(),
            row.job_id.as_str(),
            row.node_uri.as_str(),
            row.model.as_str(),
            row.pool.as_str(),
            row.state.as_str(),
            row.draft_profile.as_deref(),
            hardware.as_str(),
            outcome.as_deref(),
            row.started_at,
            row.finished_at,
            row.draft_written_at,
            row.approved_surface.as_deref(),
            row.mode.as_str(),
            row.execution_id.as_deref(),
            row.attempts,
            row.draft_revision,
            row.draft_digest.as_deref(),
            row.approval_prompt_id.as_deref(),
            row.approval_run_id.as_deref(),
            row.approval_revision,
            row.verification_id.as_deref(),
            i64::from(row.cleanup_pending)
        ],
    )
    .await?;
    Ok(())
}

/// What a container profile runs on its machines: everything that decides
/// what code executes there and what of the host it reaches. The operator
/// approves this before a generated draft is verified, since verifying runs
/// it on the fleet.
pub fn runnable_surface(profile: &InferenceProfile) -> Option<Value> {
    let Engine::Container(container) = &profile.engine else {
        return None;
    };
    Some(serde_json::json!({
        "schema": 2,
        "weights": profile.model.weights,
        "configFiles": profile.model.config_files,
        "server": container.server,
        "accelerator": profile.requirements.accelerator,
        "image": container.image,
        "entrypoint": container.entrypoint,
        "args": container.args,
        "env": container.env,
        "mounts": container.mounts,
        "host": container.host,
        "files": container.files,
        "ranks": container.ranks,
    }))
}

/// The sha256 of a runnable surface's JSON, which names what was approved.
pub fn surface_digest(surface: &Value) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(surface.to_string().as_bytes()))
}

/// Why a generation cannot start, before anything is launched: the model is
/// unregistered, the pool's hardware cannot be read, or Cairn cannot reach a
/// machine, so no profile could be verified there. The pool's first machine
/// heads a verification, as [`super::inference_verify::begin`] places it.
pub fn preflight(
    inventory: &Inventory,
    network: &Network,
    model: &str,
    pool: &Pool,
    hardware: Result<PoolHw, String>,
) -> Result<PoolHw, String> {
    if !inventory.models.contains_key(model) {
        return Err(format!(
            "model {model} is not registered; append it to cairn://inference/models first"
        ));
    }
    let hardware = hardware?;
    let machines: Vec<MachineName> = pool.machines().map(MachineName::from).collect();
    if let Some(problem) = machines
        .first()
        .and_then(|head| Addresses::new(inventory, network).unreachable(head, &machines))
    {
        return Err(format!(
            "no profile generated for pool {} could be verified: {problem}",
            pool.key
        ));
    }
    Ok(hardware)
}

mod launch;
pub(crate) use launch::admit_launch;

mod lifecycle;
pub use launch::{
    issue_uri, launch, reconcile_starting, LaunchedGeneration, LAUNCH_FAILED, RECIPE, STARTING,
};
pub(crate) use lifecycle::{
    after_job_ended, after_operator_stop, finalize_generation, issue_resolved,
    verification_settled, withdraw_questions, RETIRED_WORKFLOW,
};
pub use lifecycle::{
    cancel, reconcile_generations_on_startup, reconcile_on_startup, record_outcome, CANCELLED,
    EXHAUSTED, FAILED, VERIFIED,
};

/// What the bound agent appends to `cairn://inference/profiles` as `generated`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GeneratedWrite {
    /// The profile, as Cairn's record YAML or as JSON.
    profile: Value,
    #[serde(default)]
    sources: Vec<CitedSource>,
    #[serde(default)]
    answers: Vec<OperatorAnswer>,
    #[serde(default)]
    starting_template: Option<String>,
}

/// Write a generation's draft profile, for its bound ordinary agent
/// node (`caller_home`, running `caller_job`). The profile's origin is
/// stamped here, its `run` from the caller and never from the payload. Only a
/// container profile of the generation's model is accepted, with no host
/// devices and no source checkout (a profile that needs them is the
/// operator's to write), it may replace only this generation's earlier
/// draft, and the inventory validates it as a whole. Answers the summary and
/// `{profile, generation, surface, surfaceDigest}`: what the draft would run,
/// which the operator approves before it is verified.
pub async fn write_generated(
    db: &LocalDb,
    caller_home: &str,
    caller_job: &str,
    payload: &Value,
) -> Result<(String, Value), String> {
    let held = super::inference::INVENTORY.lock().await;
    let mut generation = open_generation_for_job(db, caller_job)
        .await?
        .ok_or("only the agent of an open profile generation writes a generated profile; start one from Settings > Inference")?;
    if generation.mode != "agent"
        || generation.node_uri != caller_home
        || generation.execution_id.is_none()
    {
        return Err("only the generation's exact bound node writes its draft".into());
    }
    if let Some(draft) = &generation.draft_profile {
        if super::inference_verify::load_verifications(db)
            .await?
            .iter()
            .any(|row| row.profile == *draft && row.outcome == "running")
        {
            return Err(
                "the draft is held by verification; wait for it to settle before replacing it"
                    .into(),
            );
        }
    }
    let id = generation.id.clone();
    let reserved = db
        .write(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "UPDATE inference_generations SET attempts = attempts + 1
                 WHERE id = ?1 AND state = 'running' AND attempts < 3 RETURNING attempts",
                        (id.as_str(),),
                    )
                    .await?;
                rows.next().await?.map(|row| row.i64(0)).transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let result = match reserved {
        Some(attempt) => {
            generation.attempts = attempt;
            write_generated_held(db, caller_home, &generation, payload, &held).await
        }
        None => Err(
            "this generation has used its three candidate submissions; start a new generation"
                .into(),
        ),
    };
    // The final accepted candidate still gets its approval and verification.
    // Only a failed submission (or a request beyond the budget) exhausts it.
    if result.is_err() && (generation.attempts == 3 || reserved.is_none()) {
        // The finalizer takes the inventory lock itself.
        drop(held);
        finalize_generation(db, &generation.id, EXHAUSTED, None).await?;
    }
    result
}

async fn write_generated_held(
    db: &LocalDb,
    caller_home: &str,
    generation: &GenerationRow,
    payload: &Value,
    held: &tokio::sync::MutexGuard<'_, ()>,
) -> Result<(String, Value), String> {
    let write: GeneratedWrite = serde_json::from_value(payload.clone()).map_err(|error| {
        format!("generated is {{profile, sources?, answers?, startingTemplate?}}: {error}")
    })?;
    let written: Value = match &write.profile {
        Value::String(yaml) => serde_yaml::from_str(yaml).map_err(|e| e.to_string()),
        other => Ok(other.clone()),
    }
    .map_err(|error| format!("generated.profile is not a profile: {error}"))?;
    if let Some(to) = writable_cache_mount(&written) {
        return Err(format!(
            "a generated profile mounts the Hugging Face cache read-only, and Cairn gives it only the pinned repositories' directories; drop readOnly: false from the mount at {to}"
        ));
    }
    let mut profile: InferenceProfile = serde_json::from_value(written)
        .map_err(|error| format!("generated.profile is not a profile: {error}"))?;
    if profile.model.name != generation.model {
        return Err(format!(
            "this generation is for {}; the profile serves {}",
            generation.model, profile.model.name
        ));
    }
    if profile.requirements.memory.is_none() {
        return Err("a generated profile states requirements.memory from the fit tool".into());
    }
    let Engine::Container(container) = &profile.engine else {
        return Err(format!(
            "a generated profile runs a digest-pinned container; a {} engine is the operator's to write at cairn://inference",
            profile.engine.kind()
        ));
    };
    if !container.host.devices.is_empty() {
        return Err(format!(
            "a generated profile passes no host devices beyond the GPUs ({}); one that needs them is the operator's to write in its editor",
            container.host.devices.join(", ")
        ));
    }
    if let Some(files) = &container.files {
        return Err(format!(
            "a generated profile checks out no source repository ({}); one that needs it is the operator's to write in its editor",
            files.url
        ));
    }
    if container.server == cairn_inference::Server::Vllm
        && container
            .mounts
            .iter()
            .any(|mount| mount.from == cairn_inference::MountSource::HfCache)
        && profile.model.config_files.is_empty()
    {
        return Err("a generated vLLM profile pins its repository's configuration, tokenizer and chat-template files (model.configFiles), as its starting profile does: its container reaches only the pinned files, read-only".into());
    }
    let inventory = load_inventory(db).await?;
    let model = inventory
        .models
        .get(&generation.model)
        .ok_or_else(|| format!("model {} is no longer registered", generation.model))?;
    let target = replacement::load(db, &generation.id).await?;
    if let Some(target) = &target {
        if profile.id.as_str() != target.profile_id {
            return Err(format!(
                "this generation replaces profile {}; every candidate must use that exact id",
                target.profile_id
            ));
        }
    }
    let previous = generation.draft_profile.as_deref();
    let replaces_target = previous.is_none() && target.is_some();
    if inventory.profiles.contains_key(&profile.id)
        && previous != Some(profile.id.as_str())
        && !replaces_target
    {
        return Err(format!(
            "profile {} exists already and is not this generation's draft; give the profile another id",
            profile.id
        ));
    }
    profile.source = RecordSource::Cairn;
    profile.origin = ProfileOrigin::Generated {
        run: Some(caller_home.to_string()),
        model_revision: model.revision.clone().unwrap_or_default(),
        starting_template: write.starting_template,
        sources: write.sources,
        answers: write.answers,
    };
    let mut remove = Removals::default();
    if let Some(previous) =
        previous.or_else(|| target.as_ref().map(|target| target.profile_id.as_str()))
    {
        let previous = InferenceProfileId::from(previous);
        if inventory.profiles.contains_key(&previous) {
            remove.deployments = inventory
                .deployments
                .values()
                .filter(|deployment| deployment.profile == previous)
                .map(|deployment| deployment.id.clone())
                .collect();
            if previous != profile.id {
                remove.profiles.push(previous);
            }
        }
    }
    let mut records = Inventory::default();
    records.insert_profile(profile.clone());
    let (changes, wrote) = super::inference::prepare_inventory_write_for_generation(
        db,
        &inventory,
        &Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove,
        },
        held,
        Some(&generation.id),
    )
    .await?;
    let surface = runnable_surface(&profile).unwrap_or(Value::Null);
    let digest = surface_digest(&surface);
    let full_digest = super::inference_verify::digest(&profile);
    let revision = generation.draft_revision + 1;
    let at = super::inference::now_ms();
    let (generation_id, profile_id, old_revision, attempts) = (
        generation.id.clone(),
        profile.id.to_string(),
        generation.draft_revision,
        generation.attempts,
    );
    let surface_hash = digest.clone();
    db.write(move |conn| {
        let (generation_id, profile_id, full_digest, surface_hash, changes) = (
            generation_id.clone(), profile_id.clone(), full_digest.clone(), surface_hash.clone(), changes.clone(),
        );
        Box::pin(async move {
            let replaces_target = replacement::validate_in(conn, &generation_id, &profile_id).await?;
            let changed = conn.execute(
                "UPDATE inference_generations SET draft_profile = ?2, draft_written_at = ?3,
                 draft_revision = ?4, draft_digest = ?5, approved_surface = NULL,
                 approval_revision = NULL, verification_id = NULL, cleanup_pending = 0
                 WHERE id = ?1 AND state = 'running' AND draft_revision = ?6 AND attempts = ?7",
                cairn_db::turso::params![
                    generation_id.as_str(), profile_id.as_str(), at, revision,
                    full_digest.as_str(), old_revision, attempts,
                ],
            ).await?;
            if changed != 1 {
                return Err(crate::storage::DbError::Row("generation changed while writing its draft".into()));
            }
            conn.execute(
                "UPDATE inference_generated_profiles SET active = 0
                 WHERE generation_id = ?1 AND profile_id != ?2",
                (generation_id.as_str(), profile_id.as_str()),
            ).await?;
            let mut owners = conn.query(
                "SELECT generation_id, active FROM inference_generated_profiles WHERE profile_id = ?1",
                (profile_id.as_str(),),
            ).await?;
            let owner = owners.next().await?.map(|row| {
                Ok::<_, crate::storage::DbError>((row.text(0)?, row.i64(1)? != 0))
            }).transpose()?;
            drop(owners);
            if let Some((old_generation, active)) = owner.filter(|(owner, _)| owner != &generation_id && !replaces_target) {
                let mut states = conn.query(
                    "SELECT state FROM inference_generations WHERE id = ?1",
                    (old_generation.as_str(),),
                ).await?;
                let terminal = states.next().await?.map(|row| row.text(0)).transpose()?
                    .is_some_and(|state| state != RUNNING && state != STARTING);
                drop(states);
                let mut profiles = conn.query(
                    "SELECT id FROM inference_profiles WHERE id = ?1",
                    (profile_id.as_str(),),
                ).await?;
                let present = profiles.next().await?.is_some();
                drop(profiles);
                let mut claims = conn.query(
                    "SELECT id FROM inference_verifications WHERE profile_id = ?1 AND outcome = 'running' LIMIT 1",
                    (profile_id.as_str(),),
                ).await?;
                let running = claims.next().await?.is_some();
                drop(claims);
                if active || !terminal || present || running {
                    return Err(crate::storage::DbError::Row("profile belongs to another generation".into()));
                }
            }
            let owned = conn.execute(
                "INSERT INTO inference_generated_profiles
                 (profile_id, generation_id, revision, profile_digest, surface_digest, promoted_verification_id, active)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, 1)
                 ON CONFLICT(profile_id) DO UPDATE SET generation_id = excluded.generation_id,
                 revision = excluded.revision, profile_digest = excluded.profile_digest,
                 surface_digest = excluded.surface_digest, promoted_verification_id = NULL, active = 1",
                cairn_db::turso::params![
                    profile_id, generation_id.as_str(), revision, full_digest, surface_hash,
                ],
            ).await?;
            if owned != 1 {
                return Err(crate::storage::DbError::Row("profile belongs to another generation".into()));
            }
            super::inference::save_rows_in(conn, changes, at, Some(&generation_id)).await
        })
    }).await.map_err(|error| error.to_string())?;
    Ok((
        format!(
            "Wrote profile {id} as generation {generation}'s draft. Once the operator approves what it runs (surface {digest}), verify it: patch cairn://inference/profiles/{id} {{verify:{{pool:\"{pool}\"}}}}.\n{wrote}",
            id = profile.id,
            generation = generation.id,
            pool = generation.pool,
        ),
        serde_json::json!({
            "profile": profile.id.as_str(),
            "generation": generation.id,
            "revision": revision,
            "attempt": generation.attempts,
            "surface": surface,
            "surfaceDigest": digest,
        }),
    ))
}

/// Where `profile`, as written, mounts the Hugging Face cache with
/// `readOnly: false`. A profile reads such a mount as read-only, so this asks
/// what was written, to refuse it rather than quietly correct it.
fn writable_cache_mount(profile: &Value) -> Option<String> {
    profile
        .pointer("/engine/mounts")?
        .as_array()?
        .iter()
        .find(|mount| {
            mount.pointer("/from/kind").and_then(Value::as_str) == Some("hfCache")
                && mount.get("readOnly").and_then(Value::as_bool) == Some(false)
        })
        .map(|mount| {
            mount
                .get("to")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        })
}

/// The domain record and issue link. Conversation, questions and execution
/// progress belong to the ordinary issue, not to a second Settings projection.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationView {
    #[serde(flatten)]
    pub row: GenerationRow,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surface: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surface_digest: Option<String>,
}

pub async fn generation_views(orch: &Orchestrator) -> Result<Vec<GenerationView>, String> {
    let mut views = Vec::new();
    for row in list_generations(&orch.db.local).await? {
        views.push(generation_view(orch, row).await);
    }
    Ok(views)
}

pub async fn generation_view(orch: &Orchestrator, row: GenerationRow) -> GenerationView {
    let issue_uri = launch::issue_uri(orch, &row).await;
    let surface = owned_draft_surface(&orch.db.local, &row)
        .await
        .ok()
        .flatten();
    GenerationView {
        status: row.state.clone(),
        row,
        issue_uri,
        surface_digest: surface.as_ref().map(surface_digest),
        surface,
    }
}

/// A historical generation must never display a later owner's runnable surface.
async fn owned_draft_surface(
    db: &LocalDb,
    generation: &GenerationRow,
) -> Result<Option<Value>, String> {
    let Some(draft) = generation.draft_profile.as_deref() else {
        return Ok(None);
    };
    let _held = super::inference::INVENTORY.lock().await;
    let owner = db
        .query_opt(
            "SELECT generation_id FROM inference_generated_profiles WHERE profile_id = ?1",
            (draft.to_string(),),
            |row| row.text(0),
        )
        .await
        .map_err(|error| error.to_string())?;
    if owner.as_deref() != Some(generation.id.as_str()) {
        return Ok(None);
    }
    Ok(load_inventory(db)
        .await?
        .profiles
        .get(&InferenceProfileId::from(draft))
        .and_then(runnable_surface))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::pools::PoolMember;
    use crate::orchestrator::inference_models::tests::FakeHub;
    use crate::orchestrator::inference_models::{register, HUGGING_FACE};

    fn bbs_hw() -> PoolHw {
        PoolHw {
            machines: vec![MachineHw {
                name: "bbs".into(),
                family: AcceleratorFamily::Nvidia,
                gpu: "NVIDIA GeForce RTX 4090".into(),
                memory: HwMemory::Discrete { vram: 24 * GIB },
                arch: Arch::X86_64,
            }],
            linked: false,
        }
    }

    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    async fn registered(db: &LocalDb) {
        let mut hub = FakeHub::fixtures();
        hub.bodies.insert(
            format!("{HUGGING_FACE}/api/models?filter=base_model:quantized:Qwen/Qwen3-Embedding-4B&limit=100"),
            "[]".into(),
        );
        register(db, &hub, "Qwen/Qwen3-Embedding-4B", None)
            .await
            .unwrap();
    }

    fn generation(id: &str, job: &str) -> GenerationRow {
        GenerationRow {
            id: id.into(),
            project_id: "p".into(),
            issue_id: None,
            job_id: job.into(),
            mode: "agent".into(),
            execution_id: Some(format!("exec-{id}")),
            attempts: 0,
            draft_revision: 0,
            draft_digest: None,
            approval_prompt_id: None,
            approval_run_id: None,
            approval_revision: None,
            verification_id: None,
            cleanup_pending: false,
            node_uri: "cairn://p/prj/9/1/wf".into(),
            model: "Qwen/Qwen3-Embedding-4B".into(),
            pool: "bbs".into(),
            state: RUNNING.into(),
            draft_profile: None,
            draft_written_at: None,
            approved_surface: None,
            hardware: BTreeMap::new(),
            outcome: None,
            started_at: 1,
            finished_at: None,
        }
    }

    fn profile_yaml(id: &str, engine: &str) -> String {
        format!(
            r#"
id: {id}
model:
  name: Qwen/Qwen3-Embedding-4B
  weights:
    - {{repo: Qwen/Qwen3-Embedding-4B-GGUF, revision: abc, path: Qwen3-Embedding-4B-Q8_0.gguf, sha256: {SHA}}}
servedName: Qwen/Qwen3-Embedding-4B
origin: {{kind: generated, run: cairn://p/forged/1/1/x, modelRevision: forged}}
engine: {engine}
requirements:
  machines: 1
  memory: {{kind: discrete, vram: 8000000000, host: 0}}
"#
        )
    }

    const CONTAINER: &str = "{kind: container, image: \"ghcr.io/ggml-org/llama.cpp@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\", server: llamaCpp, args: [-m, \"{weights}\"]}";

    fn write(id: &str) -> Value {
        serde_json::json!({
            "profile": profile_yaml(id, CONTAINER),
            "sources": [{"url": "https://github.com/ggml-org/llama.cpp", "title": "llama.cpp"}],
            "answers": [{"question": "Quantization", "answer": "q8_0"}],
            "startingTemplate": "llamacpp-cuda",
        })
    }

    /// bginf-halo's Radeon 8060S as the amdgpu reader reports it: a unified
    /// device, its pool the machine's memory.
    fn radeon_8060s() -> GpuDevice {
        use cairn_common::executor_protocol::{UnifiedAllocationSource, UnifiedPool};
        GpuDevice {
            index: 0,
            vendor: GpuVendor::Amd,
            model: "AMD Radeon 8060S Graphics".into(),
            uuid: None,
            cores: None,
            memory: MeasurementReading::Measured {
                value: GpuMemory::Unified {
                    gpu_allocated_bytes: MeasurementReading::Measured { value: 4 * GIB },
                    source: UnifiedAllocationSource::AmdgpuGtt,
                    pool: MeasurementReading::Measured {
                        value: UnifiedPool::HostMemory,
                    },
                    apertures: None,
                },
            },
            utilization_millis: MeasurementReading::Measured { value: 0 },
        }
    }

    #[test]
    fn a_machine_s_accelerator_is_its_reported_gpu_s_and_unknown_is_refused_by_name() {
        use cairn_common::executor_protocol::MeasurementGap;
        let fedora = MachineName::from("fedora");
        let memory = MeasurementReading::Measured {
            value: MachineMemory {
                total_bytes: 125 * GIB,
                available_bytes: 88 * GIB,
            },
        };

        let unread = MeasurementReading::Unavailable {
            reason: MeasurementGap::UnsupportedPlatform,
            detail: Some("NVML not loadable".into()),
        };
        let why = hardware_from(&fedora, "x86_64", &unread, &memory).unwrap_err();
        assert_eq!(
            why,
            "fedora's accelerator is unknown: it has not reported its GPUs (NVML not loadable); attach fedora's executor where it can read its GPU (NVML for NVIDIA, amdgpu for AMD) so it reports one, then try again"
        );
        let none = MeasurementReading::Measured { value: vec![] };
        let why = hardware_from(&fedora, "x86_64", &none, &memory).unwrap_err();
        assert!(
            why.starts_with("fedora's accelerator is unknown: it reports no GPU it can read;"),
            "{why}"
        );
        // Nothing Settings or the generating agent can send states hardware,
        // so the refusal asks only for what the operator can do.
        assert!(!why.contains("state"), "{why}");

        let unattached =
            pool_hardware(&Pool::of_one("fedora"), &BTreeMap::new(), &BTreeMap::new()).unwrap_err();
        assert_eq!(
            unattached,
            "fedora is not attached, so its hardware is unknown; attach its executor so it reports its GPU, then try again"
        );

        let read = MeasurementReading::Measured {
            value: vec![radeon_8060s()],
        };
        let halo = hardware_from(&fedora, "x86_64", &read, &memory).unwrap();
        assert_eq!(halo.family, AcceleratorFamily::Amd);
        assert_eq!(halo.memory, HwMemory::Unified { total: 125 * GIB });
        assert_eq!(
            fit::templates_for(&PoolHw {
                machines: vec![halo],
                linked: false,
            })
            .unwrap()
            .iter()
            .map(|t| t.id.as_str())
            .collect::<Vec<_>>(),
            vec!["llamacpp-vulkan", "llamacpp-rocm", "vllm-rocm"]
        );
    }

    #[test]
    fn a_starting_profile_from_another_accelerator_s_template_is_refused_naming_the_right_ones() {
        let stated = StatedHardware {
            family: AcceleratorFamily::Amd,
            gpu: Some("AMD Radeon 8060S".into()),
            memory_gib: 125.0,
            unified: true,
            arch: "x86_64".into(),
        };
        let halo = PoolHw {
            machines: vec![stated.machine(&"fedora".into()).unwrap()],
            linked: false,
        };
        let model = Model::named("Qwen/Qwen3-Embedding-4B");
        let files = RepoFiles::default();
        let why =
            starting_profile("llamacpp-cuda", &model, &files, "x@q8_0", None, &halo).unwrap_err();
        assert_eq!(
            why,
            "template llamacpp-cuda runs on nvidia (cuda), and the pool's accelerator is amd; the templates that serve this pool are llamacpp-vulkan, llamacpp-rocm, vllm-rocm"
        );
        let why =
            starting_profile("llamacpp-rocm", &model, &files, "x@q8_0", None, &halo).unwrap_err();
        assert!(
            why.starts_with("the model has no weight source x@q8_0"),
            "a serving template passes on to the source: {why}"
        );
        let why = starting_profile("llamacpp-rocm", &model, &files, "x@q8_0", None, &bbs_hw())
            .unwrap_err();
        assert_eq!(
            why,
            "template llamacpp-rocm runs on amd (rocm), and the pool's accelerator is nvidia; the templates that serve this pool are vllm-openai, llamacpp-cuda"
        );
    }

    #[test]
    fn a_pool_is_named_by_its_machines_in_any_order() {
        assert_eq!(pool_key("bginf-sp2+bginf-sp1"), "bginf-sp1+bginf-sp2");
        assert_eq!(pool_key(" bbs "), "bbs");
    }

    #[test]
    fn a_generation_is_refused_up_front_for_an_unregistered_model_unread_hardware_or_no_address() {
        let mut inventory = Inventory::default();
        let pool = Pool::of_one("bbs");
        let unreached = Network::default();
        let preflight = |inventory: &Inventory, network: &Network, hw| {
            preflight(inventory, network, "Qwen/Qwen3-Embedding-4B", &pool, hw)
        };
        let refusal = preflight(&inventory, &unreached, Ok(bbs_hw())).unwrap_err();
        assert!(refusal.contains("is not registered"), "{refusal}");

        inventory.models.insert(
            "Qwen/Qwen3-Embedding-4B".into(),
            Model {
                name: "Qwen/Qwen3-Embedding-4B".into(),
                source: Default::default(),
                revision: None,
                facts: Default::default(),
                read_at: None,
            },
        );
        let refusal =
            preflight(&inventory, &unreached, Err("bbs is not attached".into())).unwrap_err();
        assert_eq!(refusal, "bbs is not attached");

        let refusal = preflight(&inventory, &unreached, Ok(bbs_hw())).unwrap_err();
        assert_eq!(
            refusal,
            "no profile generated for pool bbs could be verified: bbs's address is unknown: no executor named bbs is attached and it has no machine record; enrol it as an executor or add a machine record"
        );

        let attached = Network {
            connections: BTreeMap::from([("bbs".to_string(), "10.10.10.10".parse().unwrap())]),
            ..Network::default()
        };
        assert!(preflight(&inventory, &attached, Ok(bbs_hw())).is_ok());
        inventory.insert_machine(cairn_inference::MachineRecord {
            name: "bbs".into(),
            source: RecordSource::Cairn,
            address: "10.10.10.10".into(),
        });
        assert!(preflight(&inventory, &unreached, Ok(bbs_hw())).is_ok());
    }

    #[test]
    fn the_agent_reads_the_hardware_fit_uses() {
        let mut pool = Pool::of_one("bbs");
        pool.members = vec![PoolMember {
            machine: "bbs".into(),
            link_address: None,
        }];
        let attached = Network {
            connections: BTreeMap::from([("bbs".to_string(), "10.10.10.10".parse().unwrap())]),
            ..Network::default()
        };
        let facts = pool_facts(
            &pool,
            &bbs_hw(),
            &Inventory::default(),
            &attached,
            &BTreeMap::new(),
        );
        assert_eq!(
            facts.machines[0].address.as_deref(),
            Some("10.10.10.10 (observed from its connection)")
        );
        assert_eq!(facts.machines[0].memory, "discrete");
        assert_eq!(
            facts.machines[0].usable_bytes,
            bbs_hw().machines[0].usable_bytes()
        );
        assert!(!facts.machines[0].blackwell);
    }

    const REPLACEMENT_HOME: &str = "cairn://p/prj/10/1/generator";

    async fn replacement_fixture(db: &LocalDb) -> replacement::Target {
        registered(db).await;
        insert_generation(db, &generation("old", "old-job"))
            .await
            .unwrap();
        write_generated(db, "cairn://p/prj/9/1/wf", "old-job", &write("a"))
            .await
            .unwrap();
        // Seed the terminal owner's state; its saved profile is unpromoted.
        db.execute(
            "UPDATE inference_generations SET state = 'verified', finished_at = 2 WHERE id = 'old'",
            (),
        )
        .await
        .unwrap();
        let mut edited =
            load_inventory(db).await.unwrap().profiles[&InferenceProfileId::from("a")].clone();
        let Engine::Container(container) = &mut edited.engine else {
            panic!("container")
        };
        container.args.extend(["--ctx-size".into(), "4096".into()]);
        super::super::inference::save_profile(
            db,
            Some("a"),
            &serde_yaml::to_string(&edited).unwrap(),
        )
        .await
        .unwrap();
        let target = replacement::capture(db, "Qwen/Qwen3-Embedding-4B", "a")
            .await
            .unwrap();
        let mut next = generation("new", "new-job");
        next.node_uri = REPLACEMENT_HOME.into();
        insert_generation(db, &next).await.unwrap();
        let captured = target.clone();
        db.write(move |conn| {
            let captured = captured.clone();
            Box::pin(async move { replacement::record_in(conn, "new", &captured).await })
        })
        .await
        .unwrap();
        target
    }

    async fn saved_body(db: &LocalDb) -> String {
        db.query_opt(
            "SELECT body_json FROM inference_profiles WHERE id = 'a'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn a_replacement_claims_only_its_exact_target_at_the_first_accepted_write() {
        let db = crate::storage::migrated_test_db("generation-replacement-door").await;
        let target = replacement_fixture(&db).await;
        assert_eq!(saved_body(&db).await, target.body);
        let refusal = write_generated(&db, REPLACEMENT_HOME, "new-job", &write("another"))
            .await
            .unwrap_err();
        assert!(refusal.contains("exact id"), "{refusal}");
        assert_eq!(saved_body(&db).await, target.body);
        assert!(get_generation(&db, "new")
            .await
            .unwrap()
            .unwrap()
            .draft_profile
            .is_none());

        let (_, data) = write_generated(&db, REPLACEMENT_HOME, "new-job", &write("a"))
            .await
            .unwrap();
        let row = get_generation(&db, "new").await.unwrap().unwrap();
        assert_eq!((row.attempts, row.draft_revision), (2, 1));
        assert_eq!(row.draft_profile.as_deref(), Some("a"));
        assert!(row.approved_surface.is_none());
        assert!(row.approval_revision.is_none());
        assert!(row.verification_id.is_none());
        assert_eq!(data["profile"], "a");
        let receipt = db
            .query_opt(
                "SELECT generation_id, revision, inventory_revision, promoted_verification_id
             FROM inference_generated_profiles WHERE profile_id = 'a'",
                (),
                |row| Ok((row.text(0)?, row.i64(1)?, row.i64(2)?, row.opt_text(3)?)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            receipt,
            ("new".into(), 1, target.inventory_revision + 1, None)
        );
        let inventory = load_inventory(&db).await.unwrap();
        assert!(promotion::Gate::load(&db)
            .await
            .unwrap()
            .unpromoted_profiles(&inventory)
            .contains("a"));
        assert!(
            matches!(&inventory.profiles[&InferenceProfileId::from("a")].origin,
            ProfileOrigin::Generated { run: Some(run), .. } if run == REPLACEMENT_HOME)
        );
        assert!(
            write_generated(&db, "cairn://p/prj/9/1/wf", "old-job", &write("a"))
                .await
                .is_err()
        );
        let old = get_generation(&db, "old").await.unwrap().unwrap();
        assert_eq!((old.state.as_str(), old.attempts), (VERIFIED, 1));
        assert!(owned_draft_surface(&db, &old).await.unwrap().is_none());
        assert!(owned_draft_surface(&db, &row).await.unwrap().is_some());

        finalize_generation(&db, "new", FAILED, None).await.unwrap();
        assert!(!load_inventory(&db)
            .await
            .unwrap()
            .profiles
            .contains_key(&InferenceProfileId::from("a")));
        assert_eq!(
            replacement::load(&db, "new").await.unwrap().unwrap().body,
            target.body
        );
    }

    #[tokio::test]
    async fn a_replacement_refuses_edit_and_undo_even_if_bytes_and_clock_match_its_capture() {
        let db = crate::storage::migrated_test_db("generation-replacement-stale").await;
        let target = replacement_fixture(&db).await;
        let original =
            load_inventory(&db).await.unwrap().profiles[&InferenceProfileId::from("a")].clone();
        let mut edited = original.clone();
        let Engine::Container(container) = &mut edited.engine else {
            panic!("container")
        };
        container.args.push("--verbose".into());
        for profile in [&edited, &original] {
            super::super::inference::save_profile(
                &db,
                Some("a"),
                &serde_yaml::to_string(profile).unwrap(),
            )
            .await
            .unwrap();
        }
        db.execute(
            "UPDATE inference_profiles SET updated_at = ?1 WHERE id = 'a'",
            (target.updated_at,),
        )
        .await
        .unwrap();
        assert_eq!(saved_body(&db).await, target.body);
        let refusal = write_generated(&db, REPLACEMENT_HOME, "new-job", &write("a"))
            .await
            .unwrap_err();
        assert!(refusal.contains("written again"), "{refusal}");
        let row = get_generation(&db, "new").await.unwrap().unwrap();
        assert_eq!((row.attempts, row.draft_revision), (1, 0));
        assert!(row.draft_profile.is_none());
        finalize_generation(&db, "new", CANCELLED, None)
            .await
            .unwrap();
        assert_eq!(saved_body(&db).await, target.body);
        let inventory = load_inventory(&db).await.unwrap();
        assert!(promotion::Gate::load(&db)
            .await
            .unwrap()
            .unpromoted_profiles(&inventory)
            .contains("a"));
    }

    #[tokio::test]
    async fn a_refused_inventory_write_rolls_back_replacement_ownership_and_can_retry_its_capture()
    {
        let db = crate::storage::migrated_test_db("generation-replacement-rollback").await;
        let target = replacement_fixture(&db).await;
        db.execute(
            "CREATE TRIGGER refuse_replacement BEFORE UPDATE ON inference_profiles
             WHEN NEW.id = 'a' BEGIN SELECT RAISE(ABORT, 'replacement inventory refused'); END",
            (),
        )
        .await
        .unwrap();
        let refusal = write_generated(&db, REPLACEMENT_HOME, "new-job", &write("a"))
            .await
            .unwrap_err();
        assert!(
            refusal.contains("replacement inventory refused"),
            "{refusal}"
        );
        assert_eq!(saved_body(&db).await, target.body);
        let owner = db.query_opt(
            "SELECT generation_id, inventory_revision FROM inference_generated_profiles WHERE profile_id = 'a'",
            (), |row| Ok((row.text(0)?, row.i64(1)?)),
        ).await.unwrap().unwrap();
        assert_eq!(owner, ("old".into(), target.inventory_revision));
        let row = get_generation(&db, "new").await.unwrap().unwrap();
        assert_eq!((row.attempts, row.draft_revision), (1, 0));
        assert!(row.draft_profile.is_none());
        db.execute("DROP TRIGGER refuse_replacement", ())
            .await
            .unwrap();
        write_generated(&db, REPLACEMENT_HOME, "new-job", &write("a"))
            .await
            .unwrap();
        let row = get_generation(&db, "new").await.unwrap().unwrap();
        assert_eq!((row.attempts, row.draft_revision), (2, 1));
    }

    #[tokio::test]
    async fn a_failed_draft_id_refuses_import_but_a_fresh_import_is_startable() {
        use crate::orchestrator::inference_import::{persist, Candidate};
        let db = crate::storage::migrated_test_db("generation-reserved-import").await;
        registered(&db).await;
        insert_generation(&db, &generation("old", "old-job"))
            .await
            .unwrap();
        write_generated(&db, "cairn://p/prj/9/1/wf", "old-job", &write("a"))
            .await
            .unwrap();
        finalize_generation(&db, "old", FAILED, None).await.unwrap();
        assert!(load_inventory(&db).await.unwrap().profiles.is_empty());
        let mut profile: InferenceProfile =
            serde_yaml::from_str(&profile_yaml("a", CONTAINER)).unwrap();
        profile.origin = ProfileOrigin::Imported {
            url: "https://github.com/example/server".into(),
            commit: "abc".into(),
            compose: "compose.yaml".into(),
            service: "server".into(),
        };
        let candidate = |profile| Candidate {
            spec: cairn_inference::import::ImportSpec::default(),
            profile,
            notes: vec![],
            references: vec![],
        };
        let refused = persist(&db, candidate(profile.clone()))
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains("reserved by generation old"), "{refused}");
        assert!(refused.contains("a-new"), "{refused}");
        assert!(load_inventory(&db).await.unwrap().profiles.is_empty());
        profile.id = "a-new".into();
        persist(&db, candidate(profile)).await.unwrap();
        let inventory = load_inventory(&db).await.unwrap();
        let deployment = serde_yaml::from_str(
            "{id: fresh, profile: a-new, machines: [bbs], head: bbs, apiPort: 8090, apiPath: /v1, healthPath: /v1/models}"
        ).unwrap();
        let gate = promotion::Gate::load(&db).await.unwrap();
        assert!(gate.start_refusal(&inventory, &deployment).is_none());
    }

    #[tokio::test]
    async fn a_deleted_generated_id_is_refused_inside_the_inventory_save_transaction() {
        use crate::orchestrator::inference::{delete_profile, diff_rows, save_rows};
        let db = crate::storage::migrated_test_db("generation-reserved-save").await;
        registered(&db).await;
        insert_generation(&db, &generation("old", "old-job"))
            .await
            .unwrap();
        write_generated(&db, "cairn://p/prj/9/1/wf", "old-job", &write("a"))
            .await
            .unwrap();
        delete_profile(&db, "a").await.unwrap();
        let before = load_inventory(&db).await.unwrap();
        let mut next = before.clone();
        let mut profile: InferenceProfile =
            serde_yaml::from_str(&profile_yaml("a", CONTAINER)).unwrap();
        profile.origin = ProfileOrigin::Authored;
        next.insert_profile(profile);
        // Bypass preparation to prove the transactional writer is the guard.
        let refused = save_rows(&db, diff_rows(&before, &next), 2)
            .await
            .unwrap_err();
        assert!(refused.contains("reserved by generation old"), "{refused}");
        assert_eq!(load_inventory(&db).await.unwrap(), before);
    }

    #[tokio::test]
    async fn a_removed_profile_id_is_reusable_only_after_its_generation_ends() {
        let db = crate::storage::migrated_test_db("generation-reuse-cleaned-id").await;
        registered(&db).await;
        let home = "cairn://p/prj/9/1/wf";
        insert_generation(&db, &generation("old", "old-job"))
            .await
            .unwrap();
        write_generated(&db, home, "old-job", &write("a"))
            .await
            .unwrap();
        super::super::inference::delete_profile(&db, "a")
            .await
            .unwrap();
        insert_generation(&db, &generation("new", "new-job"))
            .await
            .unwrap();
        let refusal = write_generated(&db, home, "new-job", &write("a"))
            .await
            .unwrap_err();
        assert!(
            refusal.contains("belongs to another generation"),
            "{refusal}"
        );
        finalize_generation(&db, "old", CANCELLED, None)
            .await
            .unwrap();
        write_generated(&db, home, "new-job", &write("a"))
            .await
            .unwrap();
        let receipt = db.query_opt(
            "SELECT generation_id, active, promoted_verification_id FROM inference_generated_profiles WHERE profile_id = 'a'",
            (), |row| Ok((row.text(0)?, row.i64(1)?, row.opt_text(2)?)),
        ).await.unwrap().unwrap();
        assert_eq!(receipt, ("new".into(), 1, None));
        let old = get_generation(&db, "old").await.unwrap().unwrap();
        assert_eq!((old.state.as_str(), old.attempts), (CANCELLED, 1));
        let new = get_generation(&db, "new").await.unwrap().unwrap();
        assert_eq!((new.attempts, new.draft_revision), (2, 1));
    }

    #[tokio::test]
    async fn editing_and_undoing_a_draft_does_not_restore_approval() {
        let db = crate::storage::migrated_test_db("generation-edit-undo-consent").await;
        registered(&db).await;
        insert_generation(&db, &generation("g", "job"))
            .await
            .unwrap();
        let (_, data) = write_generated(&db, "cairn://p/prj/9/1/wf", "job", &write("a"))
            .await
            .unwrap();
        db.execute(
            "UPDATE inference_generations SET approved_surface = ?1, approval_revision = 1,
             approval_prompt_id = 'old-prompt', approval_run_id = 'old-run' WHERE id = 'g'",
            (data["surfaceDigest"].as_str().unwrap(),),
        )
        .await
        .unwrap();
        let original =
            load_inventory(&db).await.unwrap().profiles[&InferenceProfileId::from("a")].clone();
        let mut edited = original.clone();
        let Engine::Container(container) = &mut edited.engine else {
            panic!("container")
        };
        container.args.extend(["--ctx-size".into(), "4096".into()]);
        for profile in [&edited, &original] {
            super::super::inference::save_profile(
                &db,
                Some("a"),
                &serde_yaml::to_string(profile).unwrap(),
            )
            .await
            .unwrap();
        }
        let row = get_generation(&db, "g").await.unwrap().unwrap();
        assert!(row.approved_surface.is_none());
        assert!(row.approval_revision.is_none());
        assert_eq!(row.approval_prompt_id.as_deref(), Some("old-prompt"));
        assert_eq!((row.attempts, row.draft_revision), (1, 1));
        let revision = db.query_opt(
            "SELECT inventory_revision FROM inference_generated_profiles WHERE profile_id = 'a'",
            (), |row| row.i64(0),
        ).await.unwrap().unwrap();
        assert_eq!(
            revision, 3,
            "writing, editing and undoing each advance the inventory revision"
        );
    }

    #[tokio::test]
    async fn legacy_workflows_cannot_submit_candidates_or_spend_attempts() {
        let db = crate::storage::migrated_test_db("generation-legacy-write-refused").await;
        let mut legacy = generation("legacy", "old-workflow");
        legacy.mode = "workflow".into();
        legacy.execution_id = None;
        insert_generation(&db, &legacy).await.unwrap();
        let refusal = write_generated(&db, &legacy.node_uri, &legacy.job_id, &write("a"))
            .await
            .unwrap_err();
        assert!(refusal.contains("bound node"), "{refusal}");
        assert_eq!(
            get_generation(&db, "legacy")
                .await
                .unwrap()
                .unwrap()
                .attempts,
            0
        );
        assert!(load_inventory(&db).await.unwrap().profiles.is_empty());
    }

    #[tokio::test]
    async fn invalid_candidates_spend_the_budget_even_when_they_write_nothing() {
        let db = crate::storage::migrated_test_db("generation-invalid-budget").await;
        insert_generation(&db, &generation("g", "job"))
            .await
            .unwrap();
        for attempt in 1..=3 {
            assert!(write_generated(
                &db,
                "cairn://p/prj/9/1/wf",
                "job",
                &serde_json::json!({"profile": {}})
            )
            .await
            .is_err());
            let row = get_generation(&db, "g").await.unwrap().unwrap();
            assert_eq!(row.attempts, attempt);
            assert_eq!(row.draft_revision, 0);
            assert_eq!(row.state, if attempt == 3 { "exhausted" } else { RUNNING });
            assert!(load_inventory(&db).await.unwrap().profiles.is_empty());
        }
        assert!(write_generated(
            &db,
            "cairn://p/prj/9/1/wf",
            "job",
            &serde_json::json!({"profile": {}})
        )
        .await
        .is_err());
        assert_eq!(get_generation(&db, "g").await.unwrap().unwrap().attempts, 3);
    }

    #[tokio::test]
    async fn concurrent_candidate_failures_cannot_overspend_three_attempts() {
        let db = crate::storage::migrated_test_db("generation-concurrent-budget").await;
        insert_generation(&db, &generation("g", "job"))
            .await
            .unwrap();
        let invalid = serde_json::json!({"profile": {}});
        let results = futures_util::future::join_all(
            (0..8).map(|_| write_generated(&db, "cairn://p/prj/9/1/wf", "job", &invalid)),
        )
        .await;
        assert!(results.iter().all(Result::is_err));
        let row = get_generation(&db, "g").await.unwrap().unwrap();
        assert_eq!(row.attempts, 3);
        assert_eq!(row.state, "exhausted");
    }

    #[tokio::test]
    async fn the_third_accepted_candidate_keeps_its_approval_opportunity() {
        let db = crate::storage::migrated_test_db("generation-third-candidate").await;
        registered(&db).await;
        let mut row = generation("g", "job");
        row.mode = "agent".into();
        row.execution_id = Some("execution".into());
        let home = row.node_uri.clone();
        insert_generation(&db, &row).await.unwrap();
        let refusal = write_generated(&db, "wrong-node", "job", &write("a"))
            .await
            .unwrap_err();
        assert!(refusal.contains("bound node"), "{refusal}");
        assert_eq!(get_generation(&db, "g").await.unwrap().unwrap().attempts, 0);
        for attempt in 1..=3 {
            let (_, data) = write_generated(&db, &home, "job", &write("a"))
                .await
                .unwrap();
            let row = get_generation(&db, "g").await.unwrap().unwrap();
            assert_eq!((row.attempts, row.draft_revision), (attempt, attempt));
            assert_eq!(row.state, RUNNING);
            assert_eq!(data["revision"], attempt);
            assert!(row.approved_surface.is_none());
            assert!(row.approval_revision.is_none());
            let inventory = load_inventory(&db).await.unwrap();
            let profile = &inventory.profiles[&InferenceProfileId::from("a")];
            assert_eq!(
                row.draft_digest.as_deref(),
                Some(super::super::inference_verify::digest(profile).as_str())
            );
            let receipt = db
                .query_opt(
                    "SELECT revision, profile_digest, active, promoted_verification_id
                 FROM inference_generated_profiles WHERE profile_id = 'a'",
                    (),
                    |row| Ok((row.i64(0)?, row.text(1)?, row.i64(2)?, row.opt_text(3)?)),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(receipt, (attempt, row.draft_digest.unwrap(), 1, None));
        }
        let refusal = write_generated(&db, &home, "job", &write("a"))
            .await
            .unwrap_err();
        assert!(refusal.contains("three candidate submissions"), "{refusal}");
        assert_eq!(
            get_generation(&db, "g").await.unwrap().unwrap().state,
            "exhausted"
        );
        assert!(load_inventory(&db).await.unwrap().profiles.is_empty());
    }

    #[tokio::test]
    async fn a_generated_write_is_stamped_from_its_caller_and_only_its_agent_writes_one() {
        let db = crate::storage::migrated_test_db("inference-generate-write").await;
        registered(&db).await;
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-none", &write("a"))
            .await
            .unwrap_err();
        assert!(
            refusal.starts_with("only the agent of an open profile generation"),
            "{refusal}"
        );

        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();
        let (said, data) = write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf", &write("a"))
            .await
            .unwrap();
        assert!(said.contains("added profile a"), "{said}");
        assert_eq!(
            (data["profile"].as_str(), data["generation"].as_str()),
            (Some("a"), Some("g1"))
        );
        assert_eq!(data["surfaceDigest"].as_str().map(str::len), Some(64));
        let inventory = load_inventory(&db).await.unwrap();
        let written = &inventory.profiles[&InferenceProfileId::from("a")];
        let ProfileOrigin::Generated {
            run,
            model_revision,
            starting_template,
            sources,
            answers,
        } = &written.origin
        else {
            panic!("{:?}", written.origin)
        };
        assert_eq!(
            run.as_deref(),
            Some("cairn://p/prj/9/1/wf"),
            "stamped, not forged"
        );
        assert_ne!(model_revision, "forged");
        assert_eq!(starting_template.as_deref(), Some("llamacpp-cuda"));
        assert_eq!(sources[0].url, "https://github.com/ggml-org/llama.cpp");
        assert_eq!(answers[0].answer, "q8_0");
        assert_eq!(
            get_generation(&db, "g1")
                .await
                .unwrap()
                .unwrap()
                .draft_profile
                .as_deref(),
            Some("a")
        );

        // The next attempt's draft replaces the first.
        write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf", &write("b"))
            .await
            .unwrap();
        let inventory = load_inventory(&db).await.unwrap();
        assert!(!inventory
            .profiles
            .contains_key(&InferenceProfileId::from("a")));
        assert!(inventory
            .profiles
            .contains_key(&InferenceProfileId::from("b")));
    }

    #[tokio::test]
    async fn a_generated_write_is_refused_with_its_problems_and_never_takes_another_profile() {
        let db = crate::storage::migrated_test_db("inference-generate-refused").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();

        let shell = serde_json::json!({
            "profile": profile_yaml("c", "{kind: command, start: ./run.sh}"),
        });
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf", &shell)
            .await
            .unwrap_err();
        assert!(
            refusal.contains("a command engine is the operator's"),
            "{refusal}"
        );

        let tagged = serde_json::json!({
            "profile": profile_yaml("c", "{kind: container, image: \"ghcr.io/x:latest\", server: llamaCpp}"),
        });
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf", &tagged)
            .await
            .unwrap_err();
        assert!(refusal.contains("is not pinned by digest"), "{refusal}");

        let mut other = generation("g0", "j-other");
        other.state = "verified".into();
        insert_generation(&db, &other).await.unwrap();
        write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf", &write("taken"))
            .await
            .unwrap();
        insert_generation(&db, &generation("g2", "j-wf2"))
            .await
            .unwrap();
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf2", &write("taken"))
            .await
            .unwrap_err();
        assert!(
            refusal.contains("exists already and is not this generation's draft"),
            "{refusal}"
        );
    }

    #[tokio::test]
    async fn a_generated_draft_reaches_no_host_device_and_checks_out_no_source() {
        let db = crate::storage::migrated_test_db("inference-generate-host").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();
        let devices = serde_json::json!({"profile": profile_yaml("d", "{kind: container, image: \"ghcr.io/x@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\", server: llamaCpp, host: {devices: [/dev/nvme0n1]}}")});
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf", &devices)
            .await
            .unwrap_err();
        assert!(
            refusal.contains("no host devices beyond the GPUs (/dev/nvme0n1)"),
            "{refusal}"
        );
        let files = serde_json::json!({"profile": profile_yaml("f", "{kind: container, image: \"ghcr.io/x@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\", server: llamaCpp, files: {url: \"https://github.com/o/r\", commit: 0123456789abcdef0123456789abcdef01234567}}")});
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf", &files)
            .await
            .unwrap_err();
        assert!(
            refusal.contains("checks out no source repository"),
            "{refusal}"
        );
        let writable = serde_json::json!({"profile": profile_yaml("w", "{kind: container, image: \"ghcr.io/x@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\", server: llamaCpp, mounts: [{from: {kind: hfCache}, to: /models, readOnly: false}]}")});
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-wf", &writable)
            .await
            .unwrap_err();
        assert!(
            refusal.contains("drop readOnly: false from the mount at /models"),
            "{refusal}"
        );
        let unconfigured = serde_json::json!({"profile": profile_yaml("v", "{kind: container, image: \"ghcr.io/x@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\", server: vllm, mounts: [{from: {kind: hfCache}, to: /root/.cache/huggingface}]}")});
        insert_generation(&db, &generation("g-config", "j-config"))
            .await
            .unwrap();
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-config", &unconfigured)
            .await
            .unwrap_err();
        assert!(refusal.contains("model.configFiles"), "{refusal}");
    }
}
