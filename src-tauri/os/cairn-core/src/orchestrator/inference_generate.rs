//! Generating a profile is an agent session (tenet 1364).
//!
//! The operator names a model and a pool in Settings > Inference, and
//! [`launch`] starts the shipped `inference-generate` workflow on them. Its
//! script gathers the facts this module and its reads give it (the model's
//! files, the pool's hardware, the fit arithmetic and the shipped templates as
//! starting points), has a read-only agent research how the model is best run
//! there, asks the operator the questions the agent found worth asking, has the
//! agent write a profile and has verification measure it, revising within a
//! bounded number of attempts. There is no other way Cairn generates a profile.
//!
//! Every write the session makes is the script's, through two narrow doors
//! only its own workflow node may use: [`write_generated`] writes its draft
//! profile, stamped with the run that wrote it, and [`record_outcome`] ends
//! the generation. Verification decides: a generation ends `verified` only
//! with a passed verification of its draft on its pool, and any other ending
//! removes the draft, so a generated profile in the inventory is one that
//! loaded and was measured. Each generation is a row in
//! `inference_generations`, which is what lets the operator come back to one.
//!
//! A machine's hardware comes from its executor's latest health: the GPU's
//! vendor (its accelerator family), name and memory, the machine's memory and
//! CPU architecture, read from the GPUs a model runs on (its discrete cards
//! when it has any). A machine that reports no GPU has no known accelerator
//! and is refused by name unless the generation states its hardware; nothing
//! assumes one. The accelerator chooses the templates that serve the pool.

use std::collections::BTreeMap;

use cairn_common::executor_protocol::{
    model_gpus, ExecutorHealthSnapshot, GpuDevice, GpuMemory, GpuVendor, MachineMemory,
    MeasurementReading,
};
use cairn_common::identity::AppearanceTransport;
use cairn_inference::fit::{self, Arch, HwMemory, MachineHw, PoolHw, Predicted};
use cairn_inference::hf::{self, RepoFiles};
use cairn_inference::{
    AcceleratorFamily, Addresses, CitedSource, DeploymentId, Engine, InferenceProfile,
    InferenceProfileId, Inventory, MachineName, Model, Network, OperatorAnswer, ProfileOrigin,
    RecordSource,
};
use cairn_models::types::catalog::Modality;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::inference::{load_inventory, write_inventory, Authoring, Removals};
use super::inference_models::{weight_sources, Hub};
use super::inference_verify::VerificationRow;
use super::Orchestrator;
use crate::fleet::pools::Pool;
use crate::storage::{LocalDb, RowExt};

/// The shipped workflow a generation runs.
pub const WORKFLOW: &str = "inference-generate";

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

/// A machine's hardware as the operator states it, for one whose executor
/// cannot report it.
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
            "{name}'s accelerator is unknown: {why}; state its hardware: hardware: {{\"{name}\": {{family: nvidia|amd, memoryGib, unified, arch}}}}"
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

/// A detected pool's hardware: each machine's stated or reported, and whether
/// every machine has a fast-link address.
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
                    "{} is not attached, so its hardware is unknown; attach it or state its hardware",
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
    /// Whether the operator stated this machine's hardware.
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
     approved_surface";

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

/// The open generation a workflow job runs, if it runs one.
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
    let hardware = serde_json::to_string(&row.hardware).map_err(|e| e.to_string())?;
    let outcome = row.outcome.as_ref().map(Value::to_string);
    db.write(move |conn| {
        let row = row.clone();
        let hardware = hardware.clone();
        let outcome = outcome.clone();
        Box::pin(async move {
            conn.execute(
                &format!(
                    "INSERT INTO inference_generations ({COLUMNS}) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
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
                    row.approved_surface.as_deref()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn update(
    db: &LocalDb,
    sql: &'static str,
    id: &str,
    value: Option<String>,
) -> Result<(), String> {
    let id = id.to_string();
    let now = super::inference::now_ms();
    db.write(move |conn| {
        let (id, value) = (id.clone(), value.clone());
        Box::pin(async move {
            conn.execute(
                sql,
                cairn_db::turso::params![id.as_str(), value.as_deref(), now],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Record a newly written draft, stamping when it was written.
async fn set_draft(db: &LocalDb, id: &str, profile: &str) -> Result<(), String> {
    update(
        db,
        "UPDATE inference_generations SET draft_profile = ?2, draft_written_at = ?3 WHERE id = ?1",
        id,
        Some(profile.to_string()),
    )
    .await
}

/// Forget a draft that has left the inventory.
async fn clear_draft(db: &LocalDb, id: &str) -> Result<(), String> {
    update(
        db,
        "UPDATE inference_generations SET draft_profile = ?2 WHERE id = ?1 AND ?3 IS NOT NULL",
        id,
        None,
    )
    .await
}

async fn set_approved(db: &LocalDb, id: &str, digest: &str) -> Result<(), String> {
    update(
        db,
        "UPDATE inference_generations SET approved_surface = ?2 WHERE id = ?1 AND ?3 IS NOT NULL",
        id,
        Some(digest.to_string()),
    )
    .await
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
        "weights": profile.model.weights,
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

fn draft_digest(inventory: &Inventory, draft: &str) -> Option<String> {
    inventory
        .profiles
        .get(&InferenceProfileId::from(draft))
        .and_then(runnable_surface)
        .map(|surface| surface_digest(&surface))
}

/// Why `profile` may not be verified yet: it is an open generation's draft
/// whose runnable surface the operator has not approved as it stands.
pub async fn unapproved_draft(db: &LocalDb, profile: &str) -> Result<Option<String>, String> {
    let open = select_generations(
        db,
        "WHERE draft_profile = ?1 AND state = 'running'",
        Some(profile.to_string()),
    )
    .await?;
    let Some(generation) = open.into_iter().next() else {
        return Ok(None);
    };
    let inventory = load_inventory(db).await?;
    let current = draft_digest(&inventory, profile);
    Ok((current.is_none() || current != generation.approved_surface).then(|| {
        format!(
            "profile {profile} is generation {}'s draft, and the operator has not approved what it runs as it stands; its workflow asks before verifying it",
            generation.id
        )
    }))
}

/// End an open generation. Answers whether this call ended it.
async fn finish(
    db: &LocalDb,
    id: &str,
    state: &str,
    outcome: Option<&Value>,
) -> Result<bool, String> {
    let (id, state) = (id.to_string(), state.to_string());
    let outcome = outcome.map(Value::to_string);
    let now = super::inference::now_ms();
    db.write(move |conn| {
        let (id, state, outcome) = (id.clone(), state.clone(), outcome.clone());
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE inference_generations SET state = ?2, outcome_json = ?3, finished_at = ?4 \
                     WHERE id = ?1 AND state = 'running'",
                    cairn_db::turso::params![id.as_str(), state.as_str(), outcome.as_deref(), now],
                )
                .await?;
            Ok(changed > 0)
        })
    })
    .await
    .map_err(|error| error.to_string())
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

/// Start generating a profile for `model` on `pool`: the shipped workflow,
/// launched standalone in `project_id` under an anchor issue, and its row.
pub async fn launch(
    orch: &Orchestrator,
    project_id: &str,
    model: &str,
    pool: &str,
    hardware: BTreeMap<String, StatedHardware>,
) -> Result<GenerationRow, String> {
    let inventory = load_inventory(&orch.db.local).await?;
    let detected = resolve_pool(orch, pool)?;
    let reported = pool_hardware(&detected, &executor_snapshots(orch), &hardware);
    preflight(
        &inventory,
        &orch.fleet.network(),
        model,
        &detected,
        reported,
    )?;
    let id = uuid::Uuid::new_v4().to_string();
    let mut args = serde_json::json!({
        "generation": id,
        "model": model,
        "pool": detected.key.0,
    });
    if !hardware.is_empty() {
        args["hardware"] = serde_json::to_value(&hardware).map_err(|e| e.to_string())?;
    }
    let launched = crate::execution::jobs::launch_standalone_workflow(
        orch,
        project_id,
        WORKFLOW,
        args,
        Some(format!("Generate profile: {model} on {}", detected.key)),
    )
    .await?;
    let row = GenerationRow {
        id,
        project_id: project_id.to_string(),
        issue_id: Some(launched.issue_id.clone()),
        job_id: launched.job_id.clone(),
        node_uri: cairn_common::uri::build_node_uri(
            &launched.project_key,
            launched.issue_number,
            launched.exec_seq,
            &launched.node_segment,
        ),
        model: model.to_string(),
        pool: detected.key.0.clone(),
        state: RUNNING.to_string(),
        draft_profile: None,
        draft_written_at: None,
        approved_surface: None,
        hardware,
        outcome: None,
        started_at: super::inference::now_ms(),
        finished_at: None,
    };
    insert_generation(&orch.db.local, &row).await?;
    orch.notifier.emit_change("inference_generations");
    Ok(row)
}

/// Take a draft profile out of the inventory with the verification
/// deployments that name it. Refused while one of them still holds its
/// machines.
pub async fn remove_draft(db: &LocalDb, profile: &str) -> Result<String, String> {
    let inventory = load_inventory(db).await?;
    let id = InferenceProfileId::from(profile);
    if !inventory.profiles.contains_key(&id) {
        return Ok(format!("profile {profile} is already gone"));
    }
    let deployments: Vec<DeploymentId> = inventory
        .deployments
        .values()
        .filter(|deployment| deployment.profile == id)
        .map(|deployment| deployment.id.clone())
        .collect();
    write_inventory(
        db,
        &Authoring::Patch {
            upsert: None,
            remove: Removals {
                profiles: vec![id],
                deployments,
                ..Removals::default()
            },
        },
    )
    .await
}

/// What the workflow appends to `cairn://inference/profiles` as `generated`.
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

/// Write a generation's draft profile, for the generation's own workflow
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
    let generation = open_generation_for_job(db, caller_job)
        .await?
        .ok_or("only the workflow of an open profile generation writes a generated profile; start one from Settings > Inference")?;
    let write: GeneratedWrite = serde_json::from_value(payload.clone()).map_err(|error| {
        format!("generated is {{profile, sources?, answers?, startingTemplate?}}: {error}")
    })?;
    let mut profile: InferenceProfile = match &write.profile {
        Value::String(yaml) => serde_yaml::from_str(yaml).map_err(|e| e.to_string()),
        other => serde_json::from_value(other.clone()).map_err(|e| e.to_string()),
    }
    .map_err(|error| format!("generated.profile is not a profile: {error}"))?;
    if profile.model.name != generation.model {
        return Err(format!(
            "this generation is for {}; the profile serves {}",
            generation.model, profile.model.name
        ));
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
    let inventory = load_inventory(db).await?;
    let model = inventory
        .models
        .get(&generation.model)
        .ok_or_else(|| format!("model {} is no longer registered", generation.model))?;
    let previous = generation.draft_profile.as_deref();
    if inventory.profiles.contains_key(&profile.id) && previous != Some(profile.id.as_str()) {
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
    if let Some(previous) = previous.filter(|previous| *previous != profile.id.as_str()) {
        let previous = InferenceProfileId::from(previous);
        if inventory.profiles.contains_key(&previous) {
            remove.deployments = inventory
                .deployments
                .values()
                .filter(|deployment| deployment.profile == previous)
                .map(|deployment| deployment.id.clone())
                .collect();
            remove.profiles.push(previous);
        }
    }
    let mut records = Inventory::default();
    records.insert_profile(profile.clone());
    let wrote = write_inventory(
        db,
        &Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove,
        },
    )
    .await?;
    set_draft(db, &generation.id, profile.id.as_str()).await?;
    let surface = runnable_surface(&profile).unwrap_or(Value::Null);
    let digest = surface_digest(&surface);
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
            "surface": surface,
            "surfaceDigest": digest,
        }),
    ))
}

/// The question a generation's workflow asked the operator, as stored.
pub struct AskedQuestion {
    pub questions_json: String,
    pub response: Option<String>,
    /// The surface the recorded answer came through, from the answer ledger
    /// every surface claims an answer in.
    pub answered_on: Option<AppearanceTransport>,
}

/// The question at `uri` (`.../questions/q-N`) a job asked, from the
/// database its rows live in.
pub async fn asked_question(
    orch: &Orchestrator,
    job_id: &str,
    uri: &str,
) -> Result<Option<AskedQuestion>, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    asked_question_in(&db, job_id, uri).await
}

async fn asked_question_in(
    db: &LocalDb,
    job_id: &str,
    uri: &str,
) -> Result<Option<AskedQuestion>, String> {
    let segment = uri
        .rsplit_once("/questions/")
        .map(|(_, segment)| segment.to_string())
        .ok_or_else(|| format!("{uri} is not a question's URI"))?;
    let job_id = job_id.to_string();
    let asked = db
        .read(move |conn| {
            let (job_id, segment) = (job_id.clone(), segment.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.id, p.questions, p.response FROM prompts p \
                         JOIN runs r ON p.run_id = r.id \
                         WHERE r.job_id = ?1 AND p.uri_segment = ?2",
                        (job_id.as_str(), segment.as_str()),
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => Some((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
                    None => None,
                })
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let Some((prompt_id, questions_json, response)) = asked else {
        return Ok(None);
    };
    let answered_on = match response {
        Some(_) => crate::channels::ledger::resolution_for_action(
            db,
            &cairn_common::uri::canonicalize_uri_identity(&prompt_id),
        )
        .await?
        .map(|winner| winner.resolution_provenance().map(|answer| answer.surface))
        .transpose()?,
        None => None,
    };
    Ok(Some(AskedQuestion {
        questions_json,
        response,
        answered_on,
    }))
}

/// Whether an answer through `surface` speaks for the operator: the same two
/// verified surfaces that may grant authority (`docs/authorization.md`), an
/// owner or admin signed in or the desktop app's own credential. An answer
/// written to the question's resource, which any agent can do, does not.
fn speaks_for_operator(surface: Option<AppearanceTransport>) -> bool {
    matches!(
        surface,
        Some(
            AppearanceTransport::AuthenticatedOperator | AppearanceTransport::AuthenticatedDesktop
        )
    )
}

/// The label an operator's approval of a draft's runnable surface chooses.
pub const APPROVE: &str = "Approve";

/// Record the operator's approval of a generation's draft, from its own
/// workflow (`caller_job`): `question` is the question the workflow asked
/// (`asked_question`), which must name the draft's current surface digest and
/// be answered `Approve` by the operator ([`speaks_for_operator`]). Answers the
/// approved digest.
pub async fn approve(
    db: &LocalDb,
    caller_job: &str,
    id: &str,
    question: Option<AskedQuestion>,
) -> Result<String, String> {
    let generation = get_generation(db, id)
        .await?
        .ok_or_else(|| format!("there is no generation {id}"))?;
    if generation.job_id != caller_job {
        return Err("only the generation's own workflow records the operator's approval".into());
    }
    if !generation.is_open() {
        return Err(format!(
            "generation {id} has already ended {}",
            generation.state
        ));
    }
    let draft = generation
        .draft_profile
        .as_deref()
        .ok_or("the generation has no draft to approve")?;
    let digest = draft_digest(&load_inventory(db).await?, draft)
        .ok_or_else(|| format!("the draft {draft} is no longer a container profile"))?;
    let question = question.ok_or("that question was not asked by this generation")?;
    if !question.questions_json.contains(&digest) {
        return Err(format!(
            "that question did not show the draft's runnable surface as it stands ({digest})"
        ));
    }
    let questions: Vec<crate::models::question::Question> =
        serde_json::from_str(&question.questions_json).unwrap_or_default();
    let surface = question.answered_on;
    let answer = question
        .response
        .map(|response| crate::models::question::PromptAnswers::decode_stored(&response))
        .and_then(|stored| stored.into_answers(questions.len()).ok())
        .and_then(|answers| {
            answers
                .answers()
                .first()
                .map(|answer| answer.answer.clone())
        });
    match answer.as_deref() {
        Some(APPROVE) => {}
        Some(other) => return Err(format!("the operator answered {other:?}, not {APPROVE}")),
        None => return Err("the operator has not answered that question".into()),
    }
    if !speaks_for_operator(surface) {
        return Err(match surface {
            Some(surface) => format!(
                "that question was answered through {surface:?}, which does not speak for the operator; only the operator's answer in the app approves a draft"
            ),
            None => "that answer has no record of who gave it; only the operator's answer in the app approves a draft".into(),
        });
    }
    set_approved(db, id, &digest).await?;
    Ok(digest)
}

/// The final outcomes a generation's workflow reports.
const OUTCOMES: &[&str] = &["verified", "cannotRun", "gaveUp"];

/// Whether `row` is a pass of `generation`'s current draft on its pool,
/// started after the draft was last written.
fn passes_draft(generation: &GenerationRow, row: &VerificationRow) -> bool {
    generation.draft_profile.as_deref() == Some(row.profile.as_str())
        && row.pool == generation.pool
        && row.outcome == "passed"
        && generation
            .draft_written_at
            .is_none_or(|written| row.started_at >= written)
}

/// Take a generation's draft out of the inventory and forget it, or say why
/// it stays (a verification of it still holds its machines).
async fn drop_draft(db: &LocalDb, generation: &GenerationRow) -> Option<String> {
    let draft = generation.draft_profile.as_deref()?;
    Some(match remove_draft(db, draft).await {
        Ok(_) => match clear_draft(db, &generation.id).await {
            Ok(()) => format!("Removed its draft profile {draft}."),
            Err(why) => format!("Removed its draft profile {draft}, but not from its row: {why}"),
        },
        Err(why) => format!("Its draft profile {draft} stays until its verification ends: {why}"),
    })
}

/// End a generation with the outcome its own workflow (`caller_job`)
/// reports. `verified` names its verification, which must be a pass of the
/// draft on the generation's pool that started after the draft was written;
/// any other outcome removes the draft. Answers the summary and the ended
/// row.
pub async fn record_outcome(
    db: &LocalDb,
    caller_job: &str,
    id: &str,
    outcome: &Value,
    verifications: &[VerificationRow],
) -> Result<(String, GenerationRow), String> {
    let generation = get_generation(db, id)
        .await?
        .ok_or_else(|| format!("there is no generation {id}"))?;
    if generation.job_id != caller_job {
        return Err("only the generation's own workflow reports its outcome".into());
    }
    if !generation.is_open() {
        return Err(format!(
            "generation {id} has already ended {}",
            generation.state
        ));
    }
    let state = outcome
        .get("outcome")
        .and_then(Value::as_str)
        .filter(|state| OUTCOMES.contains(state))
        .ok_or_else(|| format!("outcome.outcome is one of {}", OUTCOMES.join(", ")))?;
    let mut said = Vec::new();
    if state == "verified" {
        let draft = generation.draft_profile.as_deref().ok_or(
            "a generation is verified only once it has written a draft and it passed verification",
        )?;
        let named = outcome
            .get("verification")
            .and_then(Value::as_str)
            .ok_or("a verified outcome names its verification (outcome.verification)")?;
        let row = verifications
            .iter()
            .find(|row| row.id == named)
            .ok_or_else(|| format!("there is no verification {named}"))?;
        if !passes_draft(&generation, row) {
            return Err(format!(
                "verification {named} is not a pass of the current draft {draft} on pool {}; verification decides",
                generation.pool
            ));
        }
        said.push(format!("Generation {id} verified profile {draft}."));
    } else {
        said.push(format!("Generation {id} ended {state}."));
        said.extend(drop_draft(db, &generation).await);
    }
    if !finish(db, id, state, Some(outcome)).await? {
        return Err(format!("generation {id} ended while this was recorded"));
    }
    let ended = get_generation(db, id).await?.unwrap_or(generation);
    Ok((said.join(" "), ended))
}

/// Stop a running generation: its workflow stops, a question it was waiting
/// on is withdrawn, its draft is removed, and its anchor issue closes.
pub async fn cancel(orch: &Orchestrator, id: &str) -> Result<String, String> {
    let generation = get_generation(&orch.db.local, id)
        .await?
        .ok_or_else(|| format!("there is no generation {id}"))?;
    if !generation.is_open() {
        return Err(format!(
            "generation {id} has already ended {}",
            generation.state
        ));
    }
    crate::orchestrator::lifecycle::stop_workflow(orch, &generation.job_id).await?;
    withdraw_questions(orch, &generation.job_id).await;
    let mut said = vec![format!("Cancelled generation {id}.")];
    // A verification under way keeps its draft's deployment loaded, and the
    // inventory refuses to remove it until that ends; `reconcile` removes it
    // then.
    said.extend(drop_draft(&orch.db.local, &generation).await);
    finish(&orch.db.local, id, "cancelled", None).await?;
    orch.notifier.emit_change("inference_generations");
    close_anchor_when_done(orch, &generation);
    Ok(said.join(" "))
}

/// Bring generations in line with their workflows: one whose workflow ended
/// (`ended` holds each such job id) without reporting an outcome ends
/// `failed`, and an ended generation that is not `verified` loses any draft
/// still in the inventory, unless a pass of it on the pool was recorded, which
/// keeps it as verification decided. A draft a verification still holds stays
/// until the next pass. Answers the generations this call ended.
pub async fn reconcile(
    db: &LocalDb,
    ended: &std::collections::BTreeSet<String>,
    verifications: &[VerificationRow],
) -> Result<Vec<GenerationRow>, String> {
    let mut now_ended = Vec::new();
    for generation in list_generations(db).await? {
        let passed = verifications
            .iter()
            .any(|row| passes_draft(&generation, row));
        if generation.is_open() {
            if !ended.contains(&generation.job_id) {
                continue;
            }
            let why = if passed {
                "its workflow ended without reporting an outcome; its draft passed verification and stays"
            } else {
                "its workflow ended without reporting an outcome"
            };
            if !passed {
                drop_draft(db, &generation).await;
            }
            let outcome = serde_json::json!({ "outcome": "failed", "why": why });
            if finish(db, &generation.id, "failed", Some(&outcome)).await? {
                now_ended.push(generation);
            }
        } else if generation.state != "verified" && generation.draft_profile.is_some() && !passed {
            drop_draft(db, &generation).await;
        }
    }
    Ok(now_ended)
}

/// [`reconcile`] against the live jobs, closing the anchor of each generation
/// it ends.
pub async fn reconcile_generations(orch: &Orchestrator) -> Result<(), String> {
    let mut ended = std::collections::BTreeSet::new();
    for generation in list_generations(&orch.db.local).await? {
        let stale_draft = generation.state != "verified" && generation.draft_profile.is_some();
        if !generation.is_open() && !stale_draft {
            continue;
        }
        if matches!(
            job_status(orch, &generation.job_id).await.as_deref(),
            Some("complete" | "failed" | "cancelled")
        ) {
            ended.insert(generation.job_id);
        }
    }
    let verifications = super::inference_verify::load_verifications(&orch.db.local).await?;
    let now_ended = reconcile(&orch.db.local, &ended, &verifications).await?;
    if !now_ended.is_empty() {
        orch.notifier.emit_change("inference_generations");
    }
    for generation in &now_ended {
        close_anchor_when_done(orch, generation);
    }
    Ok(())
}

/// Withdraw any question a stopped generation's workflow still waits on: its
/// prompts carry no turn, so stopping the job does not withdraw them.
async fn withdraw_questions(orch: &Orchestrator, job_id: &str) {
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await else {
        return;
    };
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    let result = db
        .write(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.id FROM prompts p JOIN runs r ON p.run_id = r.id \
                         WHERE r.job_id = ?1 AND p.response IS NULL AND p.withdrawn_at IS NULL",
                        (job_id.as_str(),),
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                drop(rows);
                for id in &ids {
                    crate::mcp::handlers::planning::withdraw_prompt_conn(
                        conn,
                        id,
                        crate::mcp::handlers::permission::WithdrawalCause::OperatorStop,
                        now,
                    )
                    .await?;
                }
                Ok(ids)
            })
        })
        .await;
    match result {
        Ok(ids) => {
            for id in ids {
                let change = crate::notify::prompt_db_change_for_id(&db, &id, "update").await;
                let _ = orch.services.emitter.emit("db-change", change);
            }
        }
        Err(error) => {
            log::warn!("inference: withdrawing a cancelled generation's questions failed: {error}")
        }
    }
}

/// Close a generation's anchor issue once its workflow job has ended. Closing
/// an issue closes its sessions, so it waits for the workflow to finish
/// writing its output first.
pub(crate) fn close_anchor_when_done(orch: &Orchestrator, generation: &GenerationRow) {
    let Some(issue_id) = generation.issue_id.clone() else {
        return;
    };
    let job_id = generation.job_id.clone();
    let orch = orch.clone();
    tokio::spawn(async move {
        const POLL: std::time::Duration = std::time::Duration::from_secs(3);
        const BOUND: u32 = 200;
        for _ in 0..BOUND {
            if job_ended(&orch, &job_id).await {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
        let db = match crate::issues::crud::owning_db_for_issue(&orch.db, &issue_id).await {
            Ok(db) => db,
            Err(error) => {
                log::warn!("inference: a generation's anchor issue could not be found: {error}");
                return;
            }
        };
        match crate::issues::crud::resolve(
            &db,
            &*orch.services.clock,
            &issue_id,
            crate::transitions::Resolution::Closed,
            &orch.anon_device_manager.device_id(),
        )
        .await
        {
            Ok(_) => {
                if let Ok(Some(issue)) = crate::issues::crud::get(&db, &issue_id).await {
                    let _ = orch.services.emitter.emit(
                        "db-change",
                        crate::notify::issue_db_change(&issue, "update"),
                    );
                }
            }
            Err(error) => log::warn!("inference: closing a generation's anchor failed: {error}"),
        }
    });
}

/// A job's status, read from the database its rows live in.
async fn job_status(orch: &Orchestrator, job_id: &str) -> Option<String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .ok()?;
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT status FROM jobs WHERE id = ?1", (job_id.as_str(),))
                .await?;
            Ok(match rows.next().await? {
                Some(row) => row.opt_text(0)?,
                None => None,
            })
        })
    })
    .await
    .ok()
    .flatten()
}

async fn job_ended(orch: &Orchestrator, job_id: &str) -> bool {
    matches!(
        job_status(orch, job_id).await.as_deref(),
        Some("complete" | "failed" | "cancelled") | None
    )
}

/// A question a generation's workflow waits on the operator to answer.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingQuestion {
    pub prompt_id: String,
    pub run_id: String,
    pub uri: String,
    pub questions: Value,
}

/// A generation as Settings > Inference and `cairn://inference/generations`
/// show it: its row, the phase its workflow is in, the question it waits
/// on, and its state as it stands (a workflow that ended without reporting an
/// outcome reads `failed`).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationView {
    #[serde(flatten)]
    pub row: GenerationRow,
    /// The state as it stands, which [`GenerationRow::state`] is unless the
    /// workflow ended without an outcome.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The workflow's latest log line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_log: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_question: Option<PendingQuestion>,
    /// The current draft surface, so a workflow with an older digest can ask again.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surface: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surface_digest: Option<String>,
}

/// Every generation as it stands, newest first, reconciled with its workflow
/// first.
pub async fn generation_views(orch: &Orchestrator) -> Result<Vec<GenerationView>, String> {
    if let Err(error) = reconcile_generations(orch).await {
        log::warn!("inference: reconciling generations failed: {error}");
    }
    let mut views = Vec::new();
    for row in list_generations(&orch.db.local).await? {
        views.push(generation_view(orch, row).await);
    }
    Ok(views)
}

pub async fn generation_view(orch: &Orchestrator, row: GenerationRow) -> GenerationView {
    let mut view = GenerationView {
        status: row.state.clone(),
        row,
        phase: None,
        last_log: None,
        pending_question: None,
        surface: None,
        surface_digest: None,
    };
    if !view.row.is_open() {
        return view;
    }
    if let (Some(draft), Ok(inventory)) = (
        view.row.draft_profile.as_deref(),
        load_inventory(&orch.db.local).await,
    ) {
        view.surface = inventory
            .profiles
            .get(&InferenceProfileId::from(draft))
            .and_then(runnable_surface);
        view.surface_digest = view.surface.as_ref().map(surface_digest);
    }
    if job_ended(orch, &view.row.job_id).await {
        view.status = "failed".into();
    }
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, &view.row.job_id).await
    else {
        return view;
    };
    if let Ok(entries) = crate::workflow_progress::list_entries(&db, &view.row.job_id).await {
        view.phase = entries
            .iter()
            .rev()
            .find(|entry| entry.kind == "phase")
            .map(|entry| entry.text.clone());
        view.last_log = entries
            .iter()
            .rev()
            .find(|entry| entry.kind != "phase")
            .map(|entry| entry.text.clone());
    }
    let job_id = view.row.job_id.clone();
    let pending = db
        .read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.id, p.run_id, p.uri_segment, p.questions FROM prompts p \
                         JOIN runs r ON p.run_id = r.id \
                         WHERE r.job_id = ?1 AND p.response IS NULL AND p.withdrawn_at IS NULL \
                         ORDER BY p.created_at DESC LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => Some((row.text(0)?, row.text(1)?, row.opt_text(2)?, row.text(3)?)),
                    None => None,
                })
            })
        })
        .await
        .ok()
        .flatten();
    if let Some((prompt_id, run_id, segment, questions)) = pending {
        view.pending_question = Some(PendingQuestion {
            uri: format!(
                "{}/questions/{}",
                view.row.node_uri,
                segment.unwrap_or_default()
            ),
            prompt_id,
            run_id,
            questions: serde_json::from_str(&questions).unwrap_or(Value::Null),
        });
    }
    view
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
            node_uri: "cairn://p/prj/9/1/inference-generate".into(),
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
            "fedora's accelerator is unknown: it has not reported its GPUs (NVML not loadable); state its hardware: hardware: {\"fedora\": {family: nvidia|amd, memoryGib, unified, arch}}"
        );
        let none = MeasurementReading::Measured { value: vec![] };
        let why = hardware_from(&fedora, "x86_64", &none, &memory).unwrap_err();
        assert!(
            why.starts_with("fedora's accelerator is unknown: it reports no GPU it can read;"),
            "{why}"
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

    #[tokio::test]
    async fn a_generated_write_is_stamped_from_its_caller_and_only_its_workflow_writes_one() {
        let db = crate::storage::migrated_test_db("inference-generate-write").await;
        registered(&db).await;
        let refusal = write_generated(&db, "cairn://p/prj/9/1/wf", "j-none", &write("a"))
            .await
            .unwrap_err();
        assert!(
            refusal.starts_with("only the workflow of an open profile generation"),
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
        let refusal = write_generated(&db, "h", "j-wf", &shell).await.unwrap_err();
        assert!(
            refusal.contains("a command engine is the operator's"),
            "{refusal}"
        );

        let tagged = serde_json::json!({
            "profile": profile_yaml("c", "{kind: container, image: \"ghcr.io/x:latest\", server: llamaCpp}"),
        });
        let refusal = write_generated(&db, "h", "j-wf", &tagged)
            .await
            .unwrap_err();
        assert!(refusal.contains("is not pinned by digest"), "{refusal}");

        let mut other = generation("g0", "j-other");
        other.state = "verified".into();
        insert_generation(&db, &other).await.unwrap();
        write_generated(&db, "h", "j-wf", &write("taken"))
            .await
            .unwrap();
        insert_generation(&db, &generation("g2", "j-wf2"))
            .await
            .unwrap();
        let refusal = write_generated(&db, "h", "j-wf2", &write("taken"))
            .await
            .unwrap_err();
        assert!(
            refusal.contains("exists already and is not this generation's draft"),
            "{refusal}"
        );
    }

    /// A pass of `profile` on `pool`, started after any draft was written.
    fn passed(profile: &str, pool: &str) -> VerificationRow {
        passed_at("v", profile, pool, i64::MAX / 2)
    }

    fn passed_at(id: &str, profile: &str, pool: &str, started_at: i64) -> VerificationRow {
        VerificationRow {
            id: id.into(),
            profile: profile.into(),
            profile_digest: String::new(),
            pool: pool.into(),
            deployment: String::new(),
            machines: vec![],
            requested_by: String::new(),
            started_at,
            finished_at: Some(1),
            outcome: "passed".into(),
            stage: "done".into(),
            reason: None,
            load_ms: None,
            footprint: None,
            context_tokens: None,
            kv_tokens: None,
            ttft_ms: None,
            decode_tps: None,
            prefill_tps: None,
            embed_tps: Some(1000.0),
            engine_version: None,
        }
    }

    #[tokio::test]
    async fn verification_decides_the_outcome_and_any_other_ending_removes_the_draft() {
        let db = crate::storage::migrated_test_db("inference-generate-outcome").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();
        write_generated(&db, "h", "j-wf", &write("a"))
            .await
            .unwrap();
        let verified = serde_json::json!({"outcome": "verified", "verification": "v"});

        let refusal = record_outcome(&db, "j-intruder", "g1", &verified, &[])
            .await
            .unwrap_err();
        assert!(
            refusal.contains("only the generation's own workflow"),
            "{refusal}"
        );
        let unnamed = serde_json::json!({"outcome": "verified"});
        let refusal = record_outcome(&db, "j-wf", "g1", &unnamed, &[passed("a", "bbs")])
            .await
            .unwrap_err();
        assert!(refusal.contains("names its verification"), "{refusal}");
        let refusal = record_outcome(&db, "j-wf", "g1", &verified, &[])
            .await
            .unwrap_err();
        assert!(refusal.contains("there is no verification v"), "{refusal}");
        let refusal = record_outcome(&db, "j-wf", "g1", &verified, &[passed("a", "halo")])
            .await
            .unwrap_err();
        assert!(refusal.contains("verification decides"), "{refusal}");
        // A pass of an earlier profile with the same id, before this draft
        // was written, measured other content.
        let refusal = record_outcome(
            &db,
            "j-wf",
            "g1",
            &verified,
            &[passed_at("v", "a", "bbs", 0)],
        )
        .await
        .unwrap_err();
        assert!(
            refusal.contains("not a pass of the current draft"),
            "{refusal}"
        );
        let (_, ended) = record_outcome(&db, "j-wf", "g1", &verified, &[passed("a", "bbs")])
            .await
            .unwrap();
        assert_eq!(ended.state, "verified");
        assert!(load_inventory(&db)
            .await
            .unwrap()
            .profiles
            .contains_key(&InferenceProfileId::from("a")));
        let refusal = record_outcome(&db, "j-wf", "g1", &verified, &[passed("a", "bbs")])
            .await
            .unwrap_err();
        assert!(refusal.contains("already ended verified"), "{refusal}");

        insert_generation(&db, &generation("g2", "j-wf2"))
            .await
            .unwrap();
        write_generated(&db, "h", "j-wf2", &write("b"))
            .await
            .unwrap();
        let gave_up = serde_json::json!({"outcome": "gaveUp", "why": "three attempts failed"});
        let (said, ended) = record_outcome(&db, "j-wf2", "g2", &gave_up, &[])
            .await
            .unwrap();
        assert_eq!(ended.state, "gaveUp");
        assert!(said.contains("Removed its draft profile b"), "{said}");
        assert!(!load_inventory(&db)
            .await
            .unwrap()
            .profiles
            .contains_key(&InferenceProfileId::from("b")));
        assert_eq!(ended.outcome, Some(gave_up));
        assert_eq!(ended.draft_profile, None, "the removed draft is forgotten");
    }

    #[tokio::test]
    async fn a_generated_draft_reaches_no_host_device_and_checks_out_no_source() {
        let db = crate::storage::migrated_test_db("inference-generate-host").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();
        let devices = serde_json::json!({"profile": profile_yaml("d", "{kind: container, image: \"ghcr.io/x@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\", server: llamaCpp, host: {devices: [/dev/nvme0n1]}}")});
        let refusal = write_generated(&db, "h", "j-wf", &devices)
            .await
            .unwrap_err();
        assert!(
            refusal.contains("no host devices beyond the GPUs (/dev/nvme0n1)"),
            "{refusal}"
        );
        let files = serde_json::json!({"profile": profile_yaml("f", "{kind: container, image: \"ghcr.io/x@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\", server: llamaCpp, files: {url: \"https://github.com/o/r\", commit: 0123456789abcdef0123456789abcdef01234567}}")});
        let refusal = write_generated(&db, "h", "j-wf", &files).await.unwrap_err();
        assert!(
            refusal.contains("checks out no source repository"),
            "{refusal}"
        );
    }

    fn asked(digest: &str, answer: Option<&str>) -> AskedQuestion {
        AskedQuestion {
            questions_json: serde_json::json!([{
                "question": format!("Run this draft on bbs? (surface {digest})"),
                "header": "Approve",
                "options": [{"label": "Approve", "description": ""}, {"label": "Reject", "description": ""}]
            }])
            .to_string(),
            response: answer.map(|answer| serde_json::json!([{"index": 0, "answer": answer}]).to_string()),
            answered_on: answer.map(|_| AppearanceTransport::AuthenticatedDesktop),
        }
    }

    /// Seed `j-wf`'s question `q-N` showing `digest`, answered `Approve`
    /// through `surface` the way every answer surface records it: a claim in
    /// the answer ledger, then the stored response.
    async fn answered_through(db: &LocalDb, n: u32, digest: &str, surface: AppearanceTransport) {
        let asked = asked(digest, Some(APPROVE));
        if n == 1 {
            db.execute_batch(
                "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1);
                 INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                   VALUES ('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
                 INSERT INTO jobs (id, project_id, agent_config_id, status, uri_segment, created_at, updated_at)
                   VALUES ('j-wf', 'p', 'inference-generate', 'running', 'workflow', 1, 1);
                 INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                   VALUES ('r-wf', 'p', 'j-wf', 'running', 1, 1);",
            )
            .await
            .unwrap();
        }
        let prompt = format!("prompt-{n}");
        db.execute(
            "INSERT INTO prompts (id, run_id, job_id, questions, uri_segment, response, created_at) \
             VALUES (?1, 'r-wf', 'j-wf', ?2, ?3, ?4, 1)",
            (
                prompt.as_str(),
                asked.questions_json.as_str(),
                format!("q-{n}"),
                asked.response.clone().unwrap(),
            ),
        )
        .await
        .unwrap();
        let actor = (surface == AppearanceTransport::ResourcePatch)
            .then_some("cairn://p/cairn/1/1/builder");
        crate::channels::ledger::claim_ask_resolution(
            db,
            &prompt,
            asked.response.as_deref().unwrap(),
            surface,
            None,
            None,
            actor,
            "question",
            &prompt,
            1,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn only_the_operators_answer_in_the_app_approves_a_draft() {
        let db = crate::storage::migrated_test_db("inference-generate-approver").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();
        let (_, data) = write_generated(&db, "h", "j-wf", &write("a"))
            .await
            .unwrap();
        let digest = data["surfaceDigest"].as_str().unwrap().to_string();
        let uri = |n: u32| format!("cairn://p/cairn/1/1/workflow/questions/q-{n}");

        // A `questions/q-N` write is open to any agent, the workflow included,
        // and answers through the resource surface.
        for (n, surface) in [
            (1, AppearanceTransport::ResourcePatch),
            (2, AppearanceTransport::LocalInvoke),
            (3, AppearanceTransport::NonOperatorInvoke),
        ] {
            answered_through(&db, n, &digest, surface).await;
            let asked = asked_question_in(&db, "j-wf", &uri(n)).await.unwrap();
            assert_eq!(asked.as_ref().unwrap().answered_on, Some(surface));
            let refusal = approve(&db, "j-wf", "g1", asked).await.unwrap_err();
            assert!(
                refusal.contains("does not speak for the operator"),
                "{refusal}"
            );
            assert!(unapproved_draft(&db, "a").await.unwrap().is_some());
        }
        let unrecorded = AskedQuestion {
            answered_on: None,
            ..asked(&digest, Some(APPROVE))
        };
        let refusal = approve(&db, "j-wf", "g1", Some(unrecorded))
            .await
            .unwrap_err();
        assert!(refusal.contains("no record of who gave it"), "{refusal}");

        answered_through(&db, 4, &digest, AppearanceTransport::AuthenticatedDesktop).await;
        let asked = asked_question_in(&db, "j-wf", &uri(4)).await.unwrap();
        assert_eq!(approve(&db, "j-wf", "g1", asked).await.unwrap(), digest);
        assert_eq!(unapproved_draft(&db, "a").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_draft_is_verified_only_once_the_operator_approved_what_it_runs_as_it_stands() {
        let db = crate::storage::migrated_test_db("inference-generate-approve").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();
        let (_, data) = write_generated(&db, "h", "j-wf", &write("a"))
            .await
            .unwrap();
        let digest = data["surfaceDigest"].as_str().unwrap().to_string();
        assert_eq!(data["surface"]["image"], "ghcr.io/ggml-org/llama.cpp@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
        assert!(unapproved_draft(&db, "a").await.unwrap().is_some());
        assert_eq!(unapproved_draft(&db, "not-a-draft").await.unwrap(), None);

        let refusal = approve(&db, "j-intruder", "g1", Some(asked(&digest, Some(APPROVE))))
            .await
            .unwrap_err();
        assert!(refusal.contains("own workflow"), "{refusal}");
        let refusal = approve(&db, "j-wf", "g1", Some(asked(&digest, None)))
            .await
            .unwrap_err();
        assert!(refusal.contains("has not answered"), "{refusal}");
        let refusal = approve(&db, "j-wf", "g1", Some(asked(&digest, Some("Reject"))))
            .await
            .unwrap_err();
        assert!(refusal.contains("not Approve"), "{refusal}");
        let refusal = approve(&db, "j-wf", "g1", Some(asked("0000", Some(APPROVE))))
            .await
            .unwrap_err();
        assert!(
            refusal.contains("did not show the draft's runnable surface"),
            "{refusal}"
        );
        let refusal = approve(&db, "j-wf", "g1", None).await.unwrap_err();
        assert!(
            refusal.contains("not asked by this generation"),
            "{refusal}"
        );

        assert_eq!(
            approve(&db, "j-wf", "g1", Some(asked(&digest, Some(APPROVE))))
                .await
                .unwrap(),
            digest
        );
        assert_eq!(unapproved_draft(&db, "a").await.unwrap(), None);

        // A revision that runs something else needs approving again.
        let changed = serde_json::json!({"profile": profile_yaml("a", "{kind: container, image: \"ghcr.io/ggml-org/llama.cpp@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\", server: llamaCpp, args: [-m, \"{weights}\", --flash-attn]}")});
        let (_, data) = write_generated(&db, "h", "j-wf", &changed).await.unwrap();
        assert_ne!(data["surfaceDigest"].as_str().unwrap(), digest);
        assert!(unapproved_draft(&db, "a").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn weights_server_and_accelerator_changes_each_require_fresh_approval() {
        let db = crate::storage::migrated_test_db("inference-generate-surface-fields").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();
        let baseline: InferenceProfile =
            serde_yaml::from_str(&profile_yaml("a", CONTAINER)).unwrap();
        let baseline = serde_json::to_value(baseline).unwrap();
        let mut changes = Vec::new();
        for field in ["repo", "revision", "path", "sha256"] {
            let mut changed = baseline.clone();
            changed["model"]["weights"][0][field] = match field {
                "repo" => "other/weights".into(),
                "revision" => "def".into(),
                "path" => "other.gguf".into(),
                _ => "f".repeat(64).into(),
            };
            changes.push((field, changed));
        }
        let mut server = baseline.clone();
        server["engine"]["server"] = "vllm".into();
        changes.push(("server", server));
        let mut accelerator = baseline.clone();
        accelerator["requirements"]["accelerator"] =
            serde_json::json!({"family": "amd", "runtime": "rocm"});
        changes.push(("accelerator", accelerator));

        for (field, changed) in changes {
            let (_, original) =
                write_generated(&db, "h", "j-wf", &serde_json::json!({"profile": baseline}))
                    .await
                    .unwrap();
            let old = original["surfaceDigest"].as_str().unwrap();
            approve(&db, "j-wf", "g1", Some(asked(old, Some(APPROVE))))
                .await
                .unwrap();
            assert_eq!(unapproved_draft(&db, "a").await.unwrap(), None);
            let (_, revised) =
                write_generated(&db, "h", "j-wf", &serde_json::json!({"profile": changed}))
                    .await
                    .unwrap();
            let fresh = revised["surfaceDigest"].as_str().unwrap();
            assert_ne!(old, fresh, "{field}");
            assert!(
                unapproved_draft(&db, "a").await.unwrap().is_some(),
                "{field}"
            );
            assert!(
                approve(&db, "j-wf", "g1", Some(asked(old, Some(APPROVE))))
                    .await
                    .is_err(),
                "{field}"
            );
            approve(&db, "j-wf", "g1", Some(asked(fresh, Some(APPROVE))))
                .await
                .unwrap();
            assert_eq!(unapproved_draft(&db, "a").await.unwrap(), None, "{field}");
        }
    }

    #[tokio::test]
    async fn an_open_draft_with_a_legacy_digest_requires_the_expanded_surface_approval() {
        let db = crate::storage::migrated_test_db("inference-generate-old-surface").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-wf"))
            .await
            .unwrap();
        let (_, written) = write_generated(&db, "h", "j-wf", &write("a"))
            .await
            .unwrap();
        let mut legacy = written["surface"].clone();
        for field in ["weights", "server", "accelerator"] {
            legacy.as_object_mut().unwrap().remove(field);
        }
        let legacy = surface_digest(&legacy);
        set_approved(&db, "g1", &legacy).await.unwrap();
        assert!(unapproved_draft(&db, "a").await.unwrap().is_some());
        assert!(
            approve(&db, "j-wf", "g1", Some(asked(&legacy, Some(APPROVE))))
                .await
                .is_err()
        );
        let current = written["surfaceDigest"].as_str().unwrap();
        assert_ne!(legacy, current);
        approve(&db, "j-wf", "g1", Some(asked(current, Some(APPROVE))))
            .await
            .unwrap();
        assert_eq!(unapproved_draft(&db, "a").await.unwrap(), None);
        assert!(get_generation(&db, "g1").await.unwrap().unwrap().is_open());
    }

    #[tokio::test]
    async fn a_workflow_that_ends_without_an_outcome_fails_and_loses_its_unverified_draft() {
        let db = crate::storage::migrated_test_db("inference-generate-reconcile").await;
        registered(&db).await;
        for (id, job, draft) in [("g1", "j-1", "a"), ("g2", "j-2", "b"), ("g3", "j-3", "c")] {
            insert_generation(&db, &generation(id, job)).await.unwrap();
            write_generated(&db, "h", job, &write(draft)).await.unwrap();
        }
        let ended: std::collections::BTreeSet<String> =
            ["j-1".to_string(), "j-2".to_string()].into();
        // g2's draft passed before its workflow died; g3's workflow still runs.
        let verifications = [passed("b", "bbs")];
        let now_ended = reconcile(&db, &ended, &verifications).await.unwrap();
        let mut ended_ids: Vec<&str> = now_ended.iter().map(|g| g.id.as_str()).collect();
        ended_ids.sort_unstable();
        assert_eq!(ended_ids, vec!["g1", "g2"]);
        let inventory = load_inventory(&db).await.unwrap();
        assert!(!inventory
            .profiles
            .contains_key(&InferenceProfileId::from("a")));
        assert!(
            inventory
                .profiles
                .contains_key(&InferenceProfileId::from("b")),
            "verification decided"
        );
        assert!(inventory
            .profiles
            .contains_key(&InferenceProfileId::from("c")));
        let g1 = get_generation(&db, "g1").await.unwrap().unwrap();
        assert_eq!((g1.state.as_str(), g1.draft_profile), ("failed", None));
        assert!(get_generation(&db, "g3").await.unwrap().unwrap().is_open());
        assert!(reconcile(&db, &ended, &verifications)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn a_draft_left_behind_by_a_cancel_is_removed_once_it_can_be() {
        let db = crate::storage::migrated_test_db("inference-generate-leftover").await;
        registered(&db).await;
        insert_generation(&db, &generation("g1", "j-1"))
            .await
            .unwrap();
        write_generated(&db, "h", "j-1", &write("a")).await.unwrap();
        // A cancel during a verification ends the generation with its draft
        // still held; the next reconcile removes it.
        finish(&db, "g1", "cancelled", None).await.unwrap();
        reconcile(&db, &Default::default(), &[]).await.unwrap();
        assert!(!load_inventory(&db)
            .await
            .unwrap()
            .profiles
            .contains_key(&InferenceProfileId::from("a")));
        assert_eq!(
            get_generation(&db, "g1")
                .await
                .unwrap()
                .unwrap()
                .draft_profile,
            None
        );
    }
}
