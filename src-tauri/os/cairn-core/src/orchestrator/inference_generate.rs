//! Generating profiles for a registered model on a pool: the hardware each
//! machine reports, the model's weight sources read from Hugging Face, and the
//! facet's generator (`cairn_inference::generate`) between them. Every
//! candidate is written as a profile; what was left out is said with why.
//!
//! A machine's hardware comes from its executor's latest health: the GPU's
//! vendor, name and memory, the machine's memory and CPU architecture. A
//! machine that reports no GPU (an AMD card, which executors do not read yet)
//! is refused by name unless the request states its hardware.

use std::collections::BTreeMap;

use cairn_common::executor_protocol::{
    ExecutorHealthSnapshot, GpuMemory, GpuVendor, MeasurementReading,
};
use cairn_inference::generate::{self, Arch, HwMemory, MachineHw, PoolHw};
use cairn_inference::{AcceleratorFamily, Addresses, Inventory, MachineName, Network};
use serde::{Deserialize, Serialize};

use super::inference::{load_inventory, write_inventory, Authoring, Removals};
use super::inference_models::{weight_sources, Hub};
use super::Orchestrator;
use crate::fleet::pools::Pool;
use crate::storage::LocalDb;

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

const GIB: u64 = 1 << 30;

/// A machine's hardware as a request states it, for one whose executor
/// cannot report it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatedHardware {
    pub family: AcceleratorFamily,
    #[serde(default)]
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
    let arch = Arch::parse(&snapshot.advertisement.capabilities.arch).ok_or_else(|| {
        format!(
            "{name} reports architecture {}, which no template serves",
            snapshot.advertisement.capabilities.arch
        )
    })?;
    let MeasurementReading::Measured { value: devices } = &snapshot.host.gpus.reading else {
        return Err(format!("{name} has not reported its GPUs"));
    };
    let Some(device) = devices.first() else {
        return Err(format!(
            "{name} reports no GPU it can read (an AMD card reads as none); state its hardware in the request: hardware: {{\"{name}\": {{family, memoryGib, unified, arch}}}}"
        ));
    };
    let family = match device.vendor {
        GpuVendor::Nvidia => AcceleratorFamily::Nvidia,
        GpuVendor::Apple => AcceleratorFamily::Apple,
    };
    let memory = match &device.memory {
        MeasurementReading::Measured {
            value: GpuMemory::Discrete { total_bytes, .. },
        } => HwMemory::Discrete {
            vram: total_bytes * devices.len() as u64,
        },
        MeasurementReading::Measured {
            value: GpuMemory::Unified { .. },
        } => match &snapshot.machine.memory.reading {
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

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

/// One candidate written, with what the generator predicts of it on the pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedCandidate {
    pub profile: String,
    pub machines: u32,
    pub weight_bytes: u64,
    pub per_machine_bytes: u64,
    pub context: u64,
    pub kv_tokens: Option<u64>,
}

/// What a generation wrote, best first, and the whole account of it in words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Generation {
    pub candidates: Vec<GeneratedCandidate>,
    pub said: String,
}

/// Generate profiles for `model` on `pool` and write them. A pool with a
/// machine Cairn cannot reach is refused before anything is generated, since
/// no candidate for it could be verified: its first machine heads a
/// verification, as [`super::inference_verify::begin`] places it.
pub async fn generate(
    db: &LocalDb,
    hub: &dyn Hub,
    model: &str,
    pool: &Pool,
    snapshots: &BTreeMap<String, ExecutorHealthSnapshot>,
    network: &Network,
    stated: &BTreeMap<String, StatedHardware>,
) -> Result<Generation, String> {
    let inventory = load_inventory(db).await?;
    let model = inventory.models.get(model).cloned().ok_or_else(|| {
        format!("model {model} is not registered; append it to cairn://inference/models first")
    })?;
    let hardware = pool_hardware(pool, snapshots, stated)?;
    let machines: Vec<MachineName> = pool.machines().map(MachineName::from).collect();
    if let Some(problem) = machines
        .first()
        .and_then(|head| Addresses::new(&inventory, network).unreachable(head, &machines))
    {
        return Err(format!(
            "no profile generated for pool {} could be verified: {problem}",
            pool.key
        ));
    }
    let pool = &pool.key;
    let (repos, skipped) = weight_sources(hub, &model).await?;
    let generated = generate::generate(&model, &repos, &hardware);
    let mut said = String::new();
    if generated.candidates.is_empty() {
        said.push_str(&format!(
            "No profile for {} fits pool {pool}.\n",
            model.name
        ));
    } else {
        let mut records = Inventory::default();
        for candidate in &generated.candidates {
            records.insert_profile(candidate.profile.clone());
        }
        let wrote = write_inventory(
            db,
            &Authoring::Patch {
                upsert: Some(records.to_yaml()),
                remove: Removals::default(),
            },
        )
        .await?;
        said.push_str(&format!(
            "Candidates for {} on pool {pool}, best first (predicted; verification decides):\n",
            model.name
        ));
        for candidate in &generated.candidates {
            let p = &candidate.predicted;
            said.push_str(&format!(
                "- {}: x{} machine(s), {} of weights, {} a machine, context {}{}\n",
                candidate.profile.id,
                p.machines,
                gib(p.weight_bytes),
                gib(p.per_machine_bytes),
                p.context,
                match p.kv_tokens {
                    Some(tokens) => format!(" (KV cache predicted {tokens} tokens)"),
                    None => " (KV geometry unknown: verification clamps it)".into(),
                }
            ));
        }
        said.push_str(&wrote);
        said.push('\n');
    }
    if !generated.rejected.is_empty() {
        said.push_str("Not offered:\n");
        for rejected in &generated.rejected {
            said.push_str(&format!(
                "- {} through {}: {}\n",
                rejected.weights, rejected.template, rejected.why
            ));
        }
    }
    if !skipped.is_empty() {
        said.push_str(&format!(
            "Derivatives that could not be read: {}\n",
            skipped.join("; ")
        ));
    }
    if let Some(best) = generated.candidates.first() {
        said.push_str(&format!(
            "Verify on the pool: patch cairn://inference/models/{} {{verify:{{pool:\"{pool}\"}}}}, or one profile: patch cairn://inference/profiles/{} {{verify:{{pool:\"{pool}\"}}}}",
            model.name, best.profile.id
        ));
    }
    Ok(Generation {
        candidates: generated
            .candidates
            .iter()
            .map(|candidate| GeneratedCandidate {
                profile: candidate.profile.id.to_string(),
                machines: candidate.predicted.machines,
                weight_bytes: candidate.predicted.weight_bytes,
                per_machine_bytes: candidate.predicted.per_machine_bytes,
                context: candidate.predicted.context,
                kv_tokens: candidate.predicted.kv_tokens,
            })
            .collect(),
        said,
    })
}

#[cfg(test)]
mod tests {
    use super::super::inference_models::tests::FakeHub;
    use super::super::inference_models::{register, HUGGING_FACE};
    use super::*;

    fn stated(
        family: AcceleratorFamily,
        gib: f64,
        unified: bool,
        arch: &str,
        gpu: &str,
    ) -> StatedHardware {
        StatedHardware {
            family,
            gpu: Some(gpu.into()),
            memory_gib: gib,
            unified,
            arch: arch.into(),
        }
    }

    #[tokio::test]
    async fn the_embedder_generates_for_a_4090_and_writes_its_candidates() {
        let db = crate::storage::migrated_test_db("inference-generate-bbs").await;
        let mut hub = FakeHub::fixtures();
        hub.bodies.insert(
            format!("{HUGGING_FACE}/api/models?filter=base_model:quantized:Qwen/Qwen3-Embedding-4B&limit=100"),
            "[]".into(),
        );
        register(&db, &hub, "Qwen/Qwen3-Embedding-4B", None)
            .await
            .unwrap();
        let hardware = BTreeMap::from([(
            "bbs".to_string(),
            stated(
                AcceleratorFamily::Nvidia,
                24.0,
                false,
                "x86_64",
                "NVIDIA GeForce RTX 4090",
            ),
        )]);
        let refusal = generate(
            &db,
            &hub,
            "Qwen/Qwen3-Embedding-4B",
            &Pool::of_one("bbs"),
            &BTreeMap::new(),
            &Network::default(),
            &hardware,
        )
        .await
        .unwrap_err();
        assert_eq!(
            refusal,
            "no profile generated for pool bbs could be verified: bbs's address is unknown: no executor named bbs is attached and it has no machine record; enrol it as an executor or add a machine record"
        );
        let attached = Network {
            connections: BTreeMap::from([("bbs".to_string(), "10.10.10.10".parse().unwrap())]),
            ..Network::default()
        };
        let said = generate(
            &db,
            &hub,
            "Qwen/Qwen3-Embedding-4B",
            &Pool::of_one("bbs"),
            &BTreeMap::new(),
            &attached,
            &hardware,
        )
        .await
        .unwrap();
        assert_eq!(
            said.candidates
                .first()
                .map(|c| (c.profile.as_str(), c.machines)),
            Some(("qwen3-embedding-4b-bf16-vllm-x1", 1))
        );
        let said = said.said;
        assert!(
            said.starts_with("Candidates for Qwen/Qwen3-Embedding-4B on pool bbs, best first"),
            "{said}"
        );
        assert!(
            said.contains("- qwen3-embedding-4b-bf16-vllm-x1: x1"),
            "{said}"
        );
        assert!(
            said.contains("added profile qwen3-embedding-4b-q8_0-llamacpp-x1"),
            "{said}"
        );
        let inventory = load_inventory(&db).await.unwrap();
        assert!(inventory
            .profiles
            .contains_key(&"qwen3-embedding-4b-bf16-vllm-x1".into()));
    }

    #[tokio::test]
    async fn an_unreported_machine_and_an_unregistered_model_are_refused_by_name() {
        let db = crate::storage::migrated_test_db("inference-generate-refusals").await;
        let fedora = Pool::of_one("fedora");
        let hub = FakeHub::fixtures();
        let refusal = generate(
            &db,
            &hub,
            "Qwen/Qwen3-Embedding-4B",
            &fedora,
            &BTreeMap::new(),
            &Network::default(),
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(refusal.contains("is not registered"), "{refusal}");
        register(&db, &hub, "Qwen/Qwen3-Embedding-4B", None)
            .await
            .unwrap();
        let refusal = generate(
            &db,
            &hub,
            "Qwen/Qwen3-Embedding-4B",
            &fedora,
            &BTreeMap::new(),
            &Network::default(),
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(refusal.contains("fedora is not attached"), "{refusal}");
    }
}
