//! Generating recipes for a registered model on a pool: the hardware each
//! machine reports, the model's weight sources read from Hugging Face, and the
//! facet's generator (`cairn_inference::generate`) between them. Every
//! candidate is written as a recipe; what was left out is said with why.
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
use cairn_inference::{AcceleratorFamily, Inventory, MachineName, PoolId};
use serde::Deserialize;

use super::inference::{load_inventory, write_inventory, Authoring, Removals};
use super::inference_models::{weight_sources, Hub};
use crate::storage::LocalDb;

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

/// A pool's hardware: each machine's stated or reported, and whether every
/// machine has a fast-link address.
pub fn pool_hardware(
    inventory: &Inventory,
    pool: &PoolId,
    snapshots: &BTreeMap<String, ExecutorHealthSnapshot>,
    stated: &BTreeMap<String, StatedHardware>,
) -> Result<PoolHw, String> {
    let pool = inventory
        .pools
        .get(pool)
        .ok_or_else(|| format!("there is no pool {pool}; cairn://inference/pools lists them"))?;
    let mut machines = Vec::new();
    let mut problems = Vec::new();
    for member in &pool.machines {
        let machine = match stated.get(member.name.as_str()) {
            Some(stated) => stated.machine(&member.name),
            None => match snapshots.get(member.name.as_str()) {
                Some(snapshot) => reported_hardware(&member.name, snapshot),
                None => Err(format!(
                    "{} is not attached, so its hardware is unknown; attach it or state its hardware",
                    member.name
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
        linked: pool.machines.len() > 1 && pool.machines.iter().all(|m| m.link.is_some()),
        machines,
    })
}

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

/// Generate recipes for `model` on `pool` and write them.
pub async fn generate(
    db: &LocalDb,
    hub: &dyn Hub,
    model: &str,
    pool: &str,
    snapshots: &BTreeMap<String, ExecutorHealthSnapshot>,
    stated: &BTreeMap<String, StatedHardware>,
) -> Result<String, String> {
    let inventory = load_inventory(db).await?;
    let model = inventory.models.get(model).cloned().ok_or_else(|| {
        format!("model {model} is not registered; append it to cairn://inference/models first")
    })?;
    let hardware = pool_hardware(&inventory, &PoolId::from(pool), snapshots, stated)?;
    let (repos, skipped) = weight_sources(hub, &model).await?;
    let generated = generate::generate(&model, &repos, &hardware);
    let mut said = String::new();
    if generated.candidates.is_empty() {
        said.push_str(&format!("No recipe for {} fits pool {pool}.\n", model.name));
    } else {
        let mut records = Inventory::default();
        for candidate in &generated.candidates {
            records.insert_recipe(candidate.recipe.clone());
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
                candidate.recipe.id,
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
            "Verify on the pool: patch cairn://inference/models/{} {{verify:{{pool:\"{pool}\"}}}}, or one recipe: patch cairn://inference/recipes/{} {{verify:{{pool:\"{pool}\"}}}}",
            model.name, best.recipe.id
        ));
    }
    Ok(said)
}

#[cfg(test)]
mod tests {
    use super::super::inference_models::tests::FakeHub;
    use super::super::inference_models::{register, HUGGING_FACE};
    use super::*;

    async fn db_with_pool(name: &str, yaml: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        write_inventory(
            &db,
            &Authoring::Patch {
                upsert: Some(yaml.into()),
                remove: Removals::default(),
            },
        )
        .await
        .unwrap();
        db
    }

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
        let db = db_with_pool(
            "inference-generate-bbs",
            "pools:\n  - {id: bbs, machines: [{name: bbs, address: 10.20.20.30}]}\n",
        )
        .await;
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
        let said = generate(
            &db,
            &hub,
            "Qwen/Qwen3-Embedding-4B",
            "bbs",
            &BTreeMap::new(),
            &hardware,
        )
        .await
        .unwrap();
        assert!(
            said.starts_with("Candidates for Qwen/Qwen3-Embedding-4B on pool bbs, best first"),
            "{said}"
        );
        assert!(
            said.contains("- qwen3-embedding-4b-bf16-vllm-x1: x1"),
            "{said}"
        );
        assert!(
            said.contains("added recipe qwen3-embedding-4b-q8_0-llamacpp-x1"),
            "{said}"
        );
        let inventory = load_inventory(&db).await.unwrap();
        assert!(inventory
            .recipes
            .contains_key(&"qwen3-embedding-4b-bf16-vllm-x1".into()));
    }

    #[tokio::test]
    async fn an_unreported_machine_and_an_unregistered_model_are_refused_by_name() {
        let db = db_with_pool(
            "inference-generate-refusals",
            "pools:\n  - {id: halo, machines: [{name: fedora}]}\n",
        )
        .await;
        let hub = FakeHub::fixtures();
        let refusal = generate(
            &db,
            &hub,
            "Qwen/Qwen3-Embedding-4B",
            "halo",
            &BTreeMap::new(),
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
            "halo",
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(refusal.contains("fedora is not attached"), "{refusal}");
        let refusal = generate(
            &db,
            &hub,
            "Qwen/Qwen3-Embedding-4B",
            "nowhere",
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(refusal.contains("there is no pool nowhere"), "{refusal}");
    }
}
