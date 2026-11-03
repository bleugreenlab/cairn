//! The arithmetic and starting points a profile-generating agent works with
//! (tenet 1364).
//!
//! There is one way Cairn generates a profile: an agent session (the
//! `inference-generate` workflow) gathers the model's facts and the machines'
//! hardware, researches how the model is best run there, asks the operator,
//! writes a profile and has verification measure it. Nothing here chooses a
//! profile. What it offers the agent, through `cairn://inference/fit` and
//! `cairn://inference/templates`, is:
//!
//! - [`fit`]: whether weights of a size fit a pool, on how many machines, and
//!   with how much context. A prediction; verification decides. Each of `k`
//!   machines carries `weights / k × 1.05` plus a reserve for activations, in
//!   the memory an engine may use there: 0.90 of a discrete card's VRAM or
//!   0.85 of a unified machine's memory. More than one machine needs a linked
//!   pool, and `k` must divide the attention heads. The context is the
//!   model's, clamped to what the memory left over holds in KV cache when the
//!   model's KV geometry is known, and floored to a multiple of 1024; below
//!   8192 nothing fits. With the geometry unknown nothing is clamped here, and
//!   verification clamps to the cache the engine measures.
//! - [`templates`] (`templates.yaml`): engines known to work, each an image
//!   pinned by digest with the server it runs, the hardware it serves and the
//!   weight formats it reads, and [`starting_profile`], one of them made into
//!   a whole profile for a weight source on a pool, which the agent may take
//!   as it is or change. A template serves a pool only on the pool's
//!   accelerator ([`PoolHw::accelerator`], read from its machines' hardware
//!   and never assumed) and its machines' CPU ([`Template::serves`]).

use std::collections::BTreeMap;
use std::sync::OnceLock;

use cairn_models_types::catalog::Modality;
use serde::{Deserialize, Serialize};

use crate::hf::{AuxiliaryFile, WeightSource};
use crate::machine::MachineName;
use crate::model::Model;
use crate::profile::{
    Accelerator, AcceleratorFamily, AcceleratorRuntime, Container, Engine, HostOptions,
    InferenceProfile, Interconnect, MemoryPerMachine, ModelRef, Mount, MountSource, ProfileOrigin,
    Requirements, Server, SystemRamBytes, VramBytes, WeightFile,
};

const GIB: u64 = 1 << 30;
/// What an engine needs beyond its weights and KV cache: activations, CUDA
/// graphs, the runtime itself.
pub const RESERVE_BYTES: u64 = 2 * GIB;
pub const DISCRETE_USABLE: f64 = 0.90;
pub const UNIFIED_USABLE: f64 = 0.85;
pub const WEIGHT_OVERHEAD: f64 = 1.05;
pub const MIN_CONTEXT: u64 = 8_192;

/// One engine known to work: a starting point, not a rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Template {
    pub id: String,
    pub server: Server,
    pub family: AcceleratorFamily,
    pub runtime: AcceleratorRuntime,
    /// `None` serves either CPU architecture.
    #[serde(default)]
    pub arch: Option<Arch>,
    pub image: String,
    pub formats: Vec<Format>,
    pub command: Vec<String>,
    /// What the research behind the template found worth knowing when running
    /// it: the GPUs its image is built for, known issues and their workarounds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

impl Template {
    /// Whether this template's image runs on every machine of `pool`, or why
    /// not: the pool's accelerator must be the template's family, and every
    /// machine's CPU the template's architecture when it names one.
    pub fn serves(&self, pool: &PoolHw) -> Result<(), String> {
        let family = pool.accelerator()?;
        if self.family != family {
            return Err(format!(
                "template {} runs on {} ({}), and the pool's accelerator is {}",
                self.id,
                self.family.as_str(),
                self.runtime.as_str(),
                family.as_str()
            ));
        }
        if let Some(arch) = self.arch {
            if let Some(machine) = pool.machines.iter().find(|m| m.arch != arch) {
                return Err(format!(
                    "template {}'s image is built for {} only, and {} is {}",
                    self.id,
                    arch.as_str(),
                    machine.name,
                    machine.arch.as_str()
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Safetensors,
    Gguf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Arch {
    Aarch64,
    X86_64,
}

impl Arch {
    /// An executor's `arch` (`aarch64`, `arm64`, `x86_64`, `amd64`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "aarch64" | "arm64" => Some(Self::Aarch64),
            "x86_64" | "amd64" => Some(Self::X86_64),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Aarch64 => "aarch64",
            Self::X86_64 => "x86_64",
        }
    }
}

/// The templates shipped with Cairn.
pub fn templates() -> &'static [Template] {
    static TEMPLATES: OnceLock<Vec<Template>> = OnceLock::new();
    TEMPLATES.get_or_init(|| {
        serde_yaml::from_str(include_str!("templates.yaml")).expect("templates.yaml parses")
    })
}

/// The shipped templates that serve `pool`, or why the pool's accelerator
/// cannot choose any.
pub fn templates_for(pool: &PoolHw) -> Result<Vec<&'static Template>, String> {
    pool.accelerator()?;
    Ok(templates()
        .iter()
        .filter(|template| template.serves(pool).is_ok())
        .collect())
}

/// One machine's hardware.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineHw {
    pub name: MachineName,
    pub family: AcceleratorFamily,
    /// The GPU's marketing name (`NVIDIA GB10`, `NVIDIA GeForce RTX 4090`).
    pub gpu: String,
    pub memory: HwMemory,
    pub arch: Arch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HwMemory {
    /// The machine's memory is the GPU's.
    Unified {
        total: u64,
    },
    Discrete {
        vram: u64,
    },
}

impl MachineHw {
    /// What an engine may take on this machine.
    pub fn usable_bytes(&self) -> u64 {
        (self.memory_bytes() as f64 * self.usable_fraction()) as u64
    }

    /// The GPU's memory: the machine's on a unified machine, the card's VRAM
    /// on a discrete one.
    pub fn memory_bytes(&self) -> u64 {
        match self.memory {
            HwMemory::Unified { total } => total,
            HwMemory::Discrete { vram } => vram,
        }
    }

    /// The share of [`Self::memory_bytes`] an engine may take.
    pub fn usable_fraction(&self) -> f64 {
        match self.memory {
            HwMemory::Unified { .. } => UNIFIED_USABLE,
            HwMemory::Discrete { .. } => DISCRETE_USABLE,
        }
    }

    /// NVIDIA Blackwell, which FP4 kernels need.
    pub fn blackwell(&self) -> bool {
        let gpu = self.gpu.to_ascii_uppercase();
        [
            "GB10",
            "GB20",
            "B200",
            "B100",
            "RTX 50",
            "BLACKWELL",
            "THOR",
        ]
        .iter()
        .any(|mark| gpu.contains(mark))
    }
}

/// A pool's machines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolHw {
    pub machines: Vec<MachineHw>,
    /// Whether its machines are joined by a link fast enough to split a model
    /// across them.
    pub linked: bool,
}

impl PoolHw {
    /// The accelerator family every machine of the pool has, which decides the
    /// engine images that can serve it. Each machine's family comes from its
    /// hardware read or the operator's statement of it; a machine whose
    /// accelerator is unknown never becomes a [`MachineHw`]. Refused, naming
    /// each machine's, when they differ.
    pub fn accelerator(&self) -> Result<AcceleratorFamily, String> {
        let Some(first) = self.machines.first() else {
            return Err("the pool has no machines, so its accelerator is unknown".into());
        };
        if self.machines.iter().all(|m| m.family == first.family) {
            return Ok(first.family);
        }
        Err(format!(
            "the pool's machines have different accelerators ({}), and one engine image serves one",
            self.machines
                .iter()
                .map(|m| format!("{}: {}", m.name, m.family.as_str()))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

/// What [`fit`] predicts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Predicted {
    pub machines: u32,
    pub weight_bytes: u64,
    /// Each machine's share: weights, overhead and reserve.
    pub per_machine_bytes: u64,
    pub context: u64,
    /// The KV cache the memory left over holds, when the geometry is known.
    pub kv_tokens: Option<u64>,
}

/// A profile id from what distinguishes it: `<model>-<quant>-<server>-x<k>`.
pub fn profile_id(
    model: &str,
    quantization: Option<&str>,
    server: Server,
    machines: u32,
) -> String {
    let name = model.rsplit('/').next().unwrap_or(model);
    let mut id = name.to_ascii_lowercase();
    if let Some(quant) = quantization {
        id.push('-');
        id.push_str(&quant.to_ascii_lowercase());
    }
    let server = match server {
        Server::Vllm => "vllm",
        Server::LlamaCpp => "llamacpp",
    };
    id.push_str(&format!("-{server}-x{machines}"));
    id
}

/// A checkpoint dtype as a quantization label (`bfloat16` is `bf16`).
pub fn dtype_label(dtype: &str) -> String {
    match dtype {
        "bfloat16" => "bf16".into(),
        "float16" => "fp16".into(),
        "float32" => "fp32".into(),
        other => other.to_string(),
    }
}

/// Bits per weight a quantization label stands for, roughly; higher is more
/// precise.
pub fn precision(label: &str) -> u32 {
    let label = label.to_ascii_lowercase();
    if label.starts_with("f32") || label == "fp32" {
        32
    } else if ["bf16", "fp16", "f16"].contains(&label.as_str()) {
        16
    } else if label == "fp8" || label.starts_with("q8") {
        8
    } else if label.starts_with("q6") {
        6
    } else if label.starts_with("q5") {
        5
    } else if label == "nvfp4"
        || label == "awq"
        || label == "gptq"
        || label.starts_with("q4")
        || label.starts_with("mxfp4")
    {
        4
    } else {
        3
    }
}

/// Bytes of KV cache one token takes on one machine of `k`.
fn kv_bytes_per_token(model: &Model, k: u32) -> Option<u64> {
    let elements = model.facts.kv_elements_per_token()?;
    // Latent attention keeps one vector per layer that every rank holds;
    // grouped-query attention shards its KV heads across tensor-parallel
    // ranks, as far as there are heads to shard.
    let shards = if model.facts.kv_lora_rank.is_some() {
        1
    } else {
        u64::from(k.min(model.facts.kv_heads.unwrap_or(1).max(1)))
    };
    Some((elements * 2).div_ceil(shards))
}

fn floor_1024(tokens: u64) -> u64 {
    tokens / 1024 * 1024
}

fn gib(bytes: u64) -> f64 {
    bytes as f64 / GIB as f64
}

/// The machine count, per-machine share and context for `weight_bytes` of
/// `model`'s weights on `pool`, or why they do not fit.
pub fn fit(model: &Model, weight_bytes: u64, pool: &PoolHw) -> Result<Predicted, String> {
    pool.accelerator()?;
    let usable = pool
        .machines
        .iter()
        .map(MachineHw::usable_bytes)
        .min()
        .unwrap_or(0);
    let max_k = if pool.linked {
        pool.machines.len().max(1) as u32
    } else {
        1
    };
    let heads = model.facts.attention_heads;
    let share =
        |k: u32| (weight_bytes as f64 / f64::from(k) * WEIGHT_OVERHEAD) as u64 + RESERVE_BYTES;
    let k = (1..=max_k)
        .filter(|k| heads.is_none_or(|heads| heads % k == 0))
        .find(|k| share(*k) <= usable)
        .ok_or_else(|| {
            format!(
                "its weights ({:.1} GiB) need {:.1} GiB a machine even across {max_k}, and each machine has {:.1} GiB to give",
                gib(weight_bytes),
                gib(share(max_k)),
                gib(usable)
            )
        })?;
    let per_machine = share(k);
    let left = usable.saturating_sub(per_machine);
    let kv_tokens = kv_bytes_per_token(model, k).map(|per_token| left / per_token.max(1));
    let trained = model.facts.context.unwrap_or(MIN_CONTEXT);
    let context = floor_1024(match kv_tokens {
        Some(tokens) => trained.min(tokens),
        None => trained,
    });
    if context < MIN_CONTEXT {
        return Err(format!(
            "across {k} machine(s) only {context} tokens of KV cache fit beside the weights; {MIN_CONTEXT} is the least worth serving"
        ));
    }
    Ok(Predicted {
        machines: k,
        weight_bytes,
        per_machine_bytes: per_machine,
        context,
        kv_tokens,
    })
}

fn embeds(model: &Model) -> bool {
    model
        .facts
        .verbs
        .as_ref()
        .is_some_and(|verbs| verbs.contains(&cairn_models_types::catalog::VerbKind::Embed))
}

/// `template` made into a whole profile for `source` on `pool` as `fit`
/// predicted it. On llama.cpp a `projector` is pinned beside the weights and
/// passed as `--mmproj`; without one the profile takes text alone, so it
/// never claims an image input it cannot serve. Every shard of a split source
/// is pinned and `-m` names the first, which llama.cpp loads the rest beside.
pub fn starting_profile(
    template: &Template,
    source: &WeightSource,
    model: &Model,
    pool: &PoolHw,
    predicted: &Predicted,
    projector: Option<&AuxiliaryFile>,
) -> InferenceProfile {
    let k = predicted.machines;
    let machine = &pool.machines[0];
    let mut command = template.command.clone();
    let embed = embeds(model);
    let mount_to = match template.server {
        Server::Vllm => "/root/.cache/huggingface",
        Server::LlamaCpp => "/models",
    };
    let mut weights: Vec<WeightFile> = source
        .files
        .iter()
        .map(|file| WeightFile {
            repo: source.repo.clone(),
            revision: source.revision.clone(),
            path: file.path.clone(),
            sha256: file.sha256.clone(),
            size: Some(file.size),
        })
        .collect();
    let projector = projector.filter(|_| template.server == Server::LlamaCpp);
    match template.server {
        Server::Vllm => {
            command.extend([
                "--max-model-len".into(),
                predicted.context.to_string(),
                "--gpu-memory-utilization".into(),
                format!("{:.2}", machine.usable_fraction()),
            ]);
            if embed {
                command.extend(["--runner".into(), "pooling".into()]);
            }
            if k > 1 {
                command.extend(["--tensor-parallel-size".into(), k.to_string()]);
            }
        }
        Server::LlamaCpp => {
            command.extend(["-c".into(), predicted.context.to_string()]);
            if embed {
                command.extend(["--embeddings".into(), "--pooling".into(), "last".into()]);
            }
            if let Some(projector) = projector {
                let file = WeightFile {
                    repo: projector.repo.clone(),
                    revision: projector.revision.clone(),
                    path: projector.file.path.clone(),
                    sha256: projector.file.sha256.clone(),
                    size: Some(projector.file.size),
                };
                command.extend([
                    "--mmproj".into(),
                    crate::render::cached_weight_path(mount_to, &file),
                ]);
                weights.push(file);
            }
        }
    }
    let (entrypoint, args, ranks) = if k > 1 {
        let quoted: Vec<String> = command
            .iter()
            .map(|word| format!("'{}'", word.replace('\'', r"'\''")))
            .collect();
        let script = format!(
            "exec {} --nnodes {{machines}} --node-rank \"$NODE_RANK\" --master-addr {{head.link}} --distributed-executor-backend mp ${{HEADLESS:+--headless}}",
            quoted.join(" ")
        );
        let ranks = (0..k)
            .map(|rank| {
                let mut env = BTreeMap::from([
                    ("NODE_RANK".to_string(), rank.to_string()),
                    ("VLLM_HOST_IP".to_string(), "{self.link}".to_string()),
                ]);
                if rank > 0 {
                    env.insert("HEADLESS".into(), "1".into());
                }
                env
            })
            .collect();
        (
            Some(vec!["bash".to_string(), "-c".to_string()]),
            vec![script],
            ranks,
        )
    } else {
        (Some(Vec::new()), command, Vec::new())
    };
    let need = match template.server {
        // vLLM takes its whole memory share up front.
        Server::Vllm => machine.usable_bytes(),
        Server::LlamaCpp => {
            predicted.per_machine_bytes
                + projector.map_or(0, |p| p.file.size)
                + kv_bytes_per_token(model, k).map_or(0, |per_token| per_token * predicted.context)
        }
    };
    let memory = match machine.memory {
        HwMemory::Unified { .. } => MemoryPerMachine::Unified {
            system: SystemRamBytes(need),
        },
        HwMemory::Discrete { .. } => MemoryPerMachine::Discrete {
            vram: VramBytes(need),
            host: SystemRamBytes(0),
        },
    };
    let mut modalities = model.facts.modalities.clone();
    if template.server == Server::LlamaCpp && projector.is_none() {
        if let Some(modalities) = modalities.as_mut() {
            modalities.input.retain(|m| *m == Modality::Text);
        }
    }
    InferenceProfile {
        id: profile_id(&model.name, Some(&source.quantization), template.server, k).into(),
        source: Default::default(),
        model: ModelRef {
            name: model.name.clone(),
            weights,
        },
        served_name: model.name.clone(),
        upstream_id: None,
        verbs: model.facts.verbs.clone(),
        modalities,
        quantization: Some(source.quantization.clone()),
        origin: ProfileOrigin::Generated {
            run: None,
            model_revision: model.revision.clone().unwrap_or_default(),
            starting_template: Some(template.id.clone()),
            sources: Vec::new(),
            answers: Vec::new(),
        },
        engine: Engine::Container(Container {
            image: template.image.clone(),
            server: template.server,
            entrypoint,
            args,
            env: BTreeMap::new(),
            mounts: vec![Mount {
                from: MountSource::HfCache,
                to: mount_to.into(),
                read_only: template.server == Server::LlamaCpp,
            }],
            host: HostOptions {
                ipc_host: true,
                network_host: true,
                shm_size: None,
                gpus: true,
                devices: Vec::new(),
                ulimits: BTreeMap::new(),
            },
            files: None,
            ranks,
        }),
        requirements: Requirements {
            machines: k,
            accelerator: Some(Accelerator {
                family: template.family,
                runtime: template.runtime,
            }),
            interconnect: (k > 1).then_some(Interconnect::Rdma),
            memory: Some(memory),
        },
        notes: template.notes.clone(),
    }
}

/// The share of a device's memory vLLM pre-allocates when the profile sets no
/// `--gpu-memory-utilization`.
pub const VLLM_DEFAULT_SHARE: f64 = 0.90;

/// A memory need predicted, per machine, for a profile that states none, from
/// what it runs. Placement reserves a stated need or a measured load in
/// preference to this; it exists so no profile reaches placement without one.
#[derive(Debug, Clone, PartialEq)]
pub enum PredictedNeed {
    /// The engine pre-allocates this share of each machine's memory whatever
    /// it loads (vLLM's `--gpu-memory-utilization`, SGLang's
    /// `--mem-fraction-static`).
    Share {
        engine: &'static str,
        flag: &'static str,
        fraction: f64,
        /// The engine's default, the profile setting no share of its own.
        defaulted: bool,
    },
    /// Each machine's split of the weights times [`WEIGHT_OVERHEAD`], the KV
    /// cache its context takes where the model's geometry is known, and
    /// [`RESERVE_BYTES`].
    Sized {
        bytes: u64,
        weight_bytes: u64,
        /// Where the weights' size came from.
        weights_from: String,
        machines: u32,
        /// The context's tokens and the KV cache they take on one machine.
        kv: Option<(u64, u64)>,
    },
}

impl PredictedNeed {
    /// The need on one machine with `total` bytes of the memory the engine
    /// lives in (system memory on a unified machine, VRAM on a discrete card).
    /// A share of a machine that has not reported its memory is unknown.
    pub fn bytes_on(&self, total: Option<u64>) -> Option<u64> {
        match self {
            Self::Share { fraction, .. } => total.map(|total| (total as f64 * fraction) as u64),
            Self::Sized { bytes, .. } => Some(*bytes),
        }
    }

    /// How it was predicted, in a clause.
    pub fn describe(&self) -> String {
        match self {
            Self::Share {
                engine,
                flag,
                fraction,
                defaulted: true,
            } => format!(
                "{engine} pre-allocates {fraction:.2} of each machine's memory, its default (the profile sets no `{flag}`)"
            ),
            Self::Share {
                engine,
                flag,
                fraction,
                defaulted: false,
            } => format!(
                "{engine} pre-allocates {fraction:.2} of each machine's memory (`{flag} {fraction}`)"
            ),
            Self::Sized {
                weight_bytes,
                weights_from,
                machines,
                kv,
                ..
            } => {
                let across = if *machines > 1 {
                    format!(" split across {machines} machines")
                } else {
                    String::new()
                };
                let kv = match kv {
                    Some((tokens, bytes)) => {
                        format!(
                            ", {} of KV cache for {tokens} tokens",
                            crate::control::gib(*bytes)
                        )
                    }
                    None => String::new(),
                };
                format!(
                    "{} of weights ({weights_from}){across} × {WEIGHT_OVERHEAD}{kv}, and a {} reserve",
                    crate::control::gib(*weight_bytes),
                    crate::control::gib(RESERVE_BYTES)
                )
            }
        }
    }
}

/// The engine a profile runs, as far as the profile shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Runs {
    Vllm,
    LlamaCpp,
    Sglang,
    Unknown,
}

/// The engine a run of words names, if any does.
fn named_engine<'a>(words: impl IntoIterator<Item = &'a str>) -> Option<Runs> {
    words.into_iter().find_map(|word| {
        let word = word.to_ascii_lowercase();
        if word.contains("sglang") {
            Some(Runs::Sglang)
        } else if word.contains("vllm") {
            Some(Runs::Vllm)
        } else if ["llama-server", "llama.cpp", "llama-cpp"]
            .iter()
            .any(|name| word.contains(name))
        {
            Some(Runs::LlamaCpp)
        } else {
            None
        }
    })
}

/// Shell-ish words: whitespace-split with surrounding quotes and separators
/// dropped. Enough to find an engine's flags in a start line or an argument.
fn words_of(text: &str) -> impl Iterator<Item = &str> {
    text.split_whitespace()
        .map(|word| word.trim_matches(|c| matches!(c, '\'' | '"' | ';' | ',' | '(' | ')')))
        .filter(|word| !word.is_empty())
}

/// The last value of any of `names` that parses, as `--flag value` or
/// `--flag=value`: the last one given is the one an engine takes, and a value
/// that does not parse (`bash -c '...'` is no context size) is not one.
fn flag<T: std::str::FromStr>(words: &[&str], names: &[&str]) -> Option<T> {
    let mut found = None;
    for (i, word) in words.iter().enumerate() {
        for name in names {
            let value = if word == name {
                words.get(i + 1).copied()
            } else {
                word.strip_prefix(name)
                    .and_then(|rest| rest.strip_prefix('='))
            };
            if let Some(parsed) = value.and_then(|value| value.parse().ok()) {
                found = Some(parsed);
            }
        }
    }
    found
}

/// The words of what a profile runs (a container's entrypoint, arguments and
/// environment, a native artifact's arguments, a command's start line) and the
/// free text describing it, if any.
fn engine_words(profile: &InferenceProfile) -> (Vec<&str>, Option<&str>) {
    match &profile.engine {
        Engine::Container(container) => (
            container
                .entrypoint
                .iter()
                .flatten()
                .chain(&container.args)
                .chain(container.env.values())
                .flat_map(|text| words_of(text))
                .collect(),
            None,
        ),
        Engine::Native { artifact, args, .. } => (
            std::iter::once(artifact)
                .chain(args)
                .flat_map(|text| words_of(text))
                .collect(),
            None,
        ),
        Engine::Command {
            start, description, ..
        } => (words_of(start).collect(), description.as_deref()),
    }
}

/// The size of a profile's weights and where it came from: its listed weight
/// files when every one states its size, else its model's parameter count at
/// the profile's quantization (or the model's, or its checkpoint dtype). A
/// quantization label this crate does not know is read as 16 bits, so an
/// unknown never shrinks the need.
fn weight_size(profile: &InferenceProfile, model: Option<&Model>) -> Option<(u64, String)> {
    let listed = &profile.model.weights;
    if !listed.is_empty() && listed.iter().all(|file| file.size.is_some()) {
        let bytes = listed.iter().filter_map(|file| file.size).sum();
        return Some((bytes, "its listed weight files".into()));
    }
    let facts = &model?.facts;
    let parameters = facts.parameters?;
    let label = profile
        .quantization
        .clone()
        .or_else(|| facts.quantization.clone())
        .or_else(|| facts.dtype.as_deref().map(dtype_label))?;
    let lower = label.to_ascii_lowercase();
    let bits = match precision(&lower) {
        3 if !(lower.starts_with('q') || lower.starts_with("iq")) => 16,
        bits => bits,
    };
    Some((
        parameters * u64::from(bits) / 8,
        format!("{:.1}B parameters at {label}", parameters as f64 / 1e9),
    ))
}

/// Predict what `profile` needs on each of its machines from what it runs, for
/// a profile that states no memory. `model` is its model's record, whose facts
/// size the weights when the profile lists no sizes and give the KV geometry.
///
/// vLLM pre-allocates `--gpu-memory-utilization` (0.90 by default) of each
/// machine's memory whatever the weights, and SGLang its
/// `--mem-fraction-static` when set. Otherwise, llama.cpp and anything
/// unrecognised take their weights split across the machines times
/// [`WEIGHT_OVERHEAD`], plus [`RESERVE_BYTES`]; llama.cpp adds the KV cache for
/// its `-c` context (the total its `--parallel` slots share), or the model's
/// trained context when it sets none. The engine is a container's server, else
/// one its start line or arguments name, else one its description names.
///
/// Refused, naming what is missing, when the profile shows neither a share
/// nor a size for its weights.
pub fn predict_need(
    profile: &InferenceProfile,
    model: Option<&Model>,
) -> Result<PredictedNeed, String> {
    let (words, description) = engine_words(profile);
    let runs = match &profile.engine {
        Engine::Container(container) => match container.server {
            Server::Vllm => Runs::Vllm,
            Server::LlamaCpp => Runs::LlamaCpp,
        },
        _ => named_engine(words.iter().copied())
            .or_else(|| description.and_then(|text| named_engine(words_of(text))))
            .unwrap_or(Runs::Unknown),
    };
    let share = |names: &[&str]| flag::<f64>(&words, names).filter(|f| *f > 0.0 && *f <= 1.0);
    match runs {
        Runs::Vllm => {
            let stated = share(&["--gpu-memory-utilization"]);
            return Ok(PredictedNeed::Share {
                engine: "vLLM",
                flag: "--gpu-memory-utilization",
                fraction: stated.unwrap_or(VLLM_DEFAULT_SHARE),
                defaulted: stated.is_none(),
            });
        }
        Runs::Sglang => {
            if let Some(fraction) = share(&["--mem-fraction-static"]) {
                return Ok(PredictedNeed::Share {
                    engine: "SGLang",
                    flag: "--mem-fraction-static",
                    fraction,
                    defaulted: false,
                });
            }
        }
        Runs::LlamaCpp | Runs::Unknown => {}
    }
    let Some((weight_bytes, weights_from)) = weight_size(profile, model) else {
        return Err(
            "its profile states no memory, its engine takes no share of memory the profile shows, and no size for its weights is known (it lists no weight sizes and its model has no parameter count): add `requirements.memory` or verify it on an idle pool"
                .into(),
        );
    };
    let machines = profile.requirements.machines.max(1);
    let kv = match (runs, model) {
        (Runs::LlamaCpp, Some(model)) => flag::<u64>(&words, &["-c", "--ctx-size"])
            .filter(|tokens| *tokens > 0)
            .or(model.facts.context)
            .zip(kv_bytes_per_token(model, machines))
            .map(|(tokens, per_token)| (tokens, tokens * per_token)),
        _ => None,
    };
    let bytes = (weight_bytes as f64 / f64::from(machines) * WEIGHT_OVERHEAD) as u64
        + kv.map_or(0, |(_, bytes)| bytes)
        + RESERVE_BYTES;
    Ok(PredictedNeed::Sized {
        bytes,
        weight_bytes,
        weights_from,
        machines,
        kv,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hf::{self, classify, HfRepo, RepoFiles};

    macro_rules! repo {
        ($slug:literal) => {
            hf::parse(
                include_str!(concat!("../tests/fixtures/hf/", $slug, ".api.json")),
                Some(include_str!(concat!(
                    "../tests/fixtures/hf/",
                    $slug,
                    ".config.json"
                ))),
            )
            .unwrap()
        };
        ($slug:literal, no_config) => {
            hf::parse(
                include_str!(concat!("../tests/fixtures/hf/", $slug, ".api.json")),
                None,
            )
            .unwrap()
        };
    }

    fn model_of(repo: &HfRepo) -> Model {
        Model {
            name: repo.repo.clone(),
            source: Default::default(),
            revision: Some(repo.revision.clone()),
            facts: repo.facts.clone(),
            read_at: None,
        }
    }

    fn files(model: &Model, repos: &[HfRepo]) -> RepoFiles {
        classify(repos, model.facts.dtype.as_deref())
    }

    fn template(id: &str) -> &'static Template {
        templates().iter().find(|t| t.id == id).unwrap()
    }

    fn spark(name: &str) -> MachineHw {
        MachineHw {
            name: name.into(),
            family: AcceleratorFamily::Nvidia,
            gpu: "NVIDIA GB10".into(),
            memory: HwMemory::Unified { total: 128 * GIB },
            arch: Arch::Aarch64,
        }
    }

    fn sparks() -> PoolHw {
        PoolHw {
            machines: vec![spark("bginf-sp1"), spark("bginf-sp2")],
            linked: true,
        }
    }

    fn bbs() -> PoolHw {
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

    fn halo() -> PoolHw {
        PoolHw {
            machines: vec![MachineHw {
                name: "fedora".into(),
                family: AcceleratorFamily::Amd,
                gpu: "AMD Radeon 8060S".into(),
                memory: HwMemory::Unified { total: 128 * GIB },
                arch: Arch::X86_64,
            }],
            linked: false,
        }
    }

    fn args(profile: &InferenceProfile) -> &[String] {
        let Engine::Container(container) = &profile.engine else {
            unreachable!()
        };
        &container.args
    }

    #[test]
    fn the_templates_parse_and_pin_their_images() {
        assert_eq!(templates().len(), 6);
        assert!(templates()
            .iter()
            .all(|t| crate::profile::is_pinned_image(&t.image)));
    }

    fn ids(pool: &PoolHw) -> Vec<&'static str> {
        templates_for(pool)
            .unwrap()
            .iter()
            .map(|t| t.id.as_str())
            .collect()
    }

    #[test]
    fn a_pool_is_served_only_by_templates_for_its_accelerator_and_cpu() {
        assert_eq!(
            ids(&halo()),
            vec!["llamacpp-vulkan", "llamacpp-rocm", "vllm-rocm"]
        );
        assert_eq!(ids(&bbs()), vec!["vllm-openai", "llamacpp-cuda"]);
        assert_eq!(
            ids(&sparks()),
            vec!["vllm-ngc", "llamacpp-cuda"],
            "the x86_64 images are not offered to aarch64 machines"
        );

        let why = template("llamacpp-cuda").serves(&halo()).unwrap_err();
        assert_eq!(
            why,
            "template llamacpp-cuda runs on nvidia (cuda), and the pool's accelerator is amd"
        );
        let why = template("vllm-openai").serves(&sparks()).unwrap_err();
        assert_eq!(
            why,
            "template vllm-openai's image is built for x86_64 only, and bginf-sp1 is aarch64"
        );
    }

    #[test]
    fn a_pool_without_one_known_accelerator_is_refused_naming_why() {
        let mixed = PoolHw {
            machines: vec![spark("bginf-sp1"), halo().machines[0].clone()],
            linked: true,
        };
        let why = mixed.accelerator().unwrap_err();
        assert_eq!(
            why,
            "the pool's machines have different accelerators (bginf-sp1: nvidia, fedora: amd), and one engine image serves one"
        );
        assert_eq!(templates_for(&mixed).unwrap_err(), why);
        assert_eq!(template("llamacpp-rocm").serves(&mixed).unwrap_err(), why);
        assert_eq!(fit(&Model::named("m"), GIB, &mixed).unwrap_err(), why);

        let empty = PoolHw {
            machines: vec![],
            linked: false,
        };
        assert!(empty
            .accelerator()
            .unwrap_err()
            .contains("its accelerator is unknown"));
    }

    /// The live case: the embedder bginf-halo serves today by hand, generated
    /// from the ROCm template and rendered as its head would run it.
    #[test]
    fn the_embedder_on_halo_starts_with_rocm_s_devices_and_no_nvidia_flags() {
        let own = repo!("Qwen--Qwen3-Embedding-4B");
        let gguf = repo!("Qwen--Qwen3-Embedding-4B-GGUF", no_config);
        let model = model_of(&own);
        let files = files(&model, &[own, gguf]);
        let q8 = files.source("Qwen/Qwen3-Embedding-4B-GGUF@q8_0").unwrap();
        let predicted = fit(&model, q8.bytes(), &halo()).unwrap();
        let rocm = template("llamacpp-rocm");
        rocm.serves(&halo()).unwrap();
        let profile = starting_profile(rocm, q8, &model, &halo(), &predicted, None);
        assert_eq!(profile.problems(), vec![]);
        assert_eq!(
            profile.requirements.accelerator,
            Some(Accelerator {
                family: AcceleratorFamily::Amd,
                runtime: AcceleratorRuntime::Rocm,
            })
        );
        assert!(
            matches!(
                profile.requirements.memory,
                Some(MemoryPerMachine::Unified { system }) if system.0 > predicted.weight_bytes
            ),
            "the same arithmetic as CUDA, typed unified: {:?}",
            profile.requirements.memory
        );
        assert!(profile
            .notes
            .as_deref()
            .is_some_and(|notes| notes.contains("gfx1151")));

        let deployment = crate::deployment::Deployment {
            id: "embed".into(),
            source: Default::default(),
            profile: profile.id.clone(),
            machines: vec!["fedora".into()],
            head: "fedora".into(),
            api_port: 8090,
            api_path: "/v1".into(),
            health_path: "/health".into(),
        };
        let start = crate::render::render(
            &profile,
            &deployment,
            &[crate::render::MachineAt {
                name: "fedora".into(),
                address: None,
                link: None,
            }],
        )
        .unwrap()
        .start;
        assert!(
            start.contains("--device /dev/dri --device /dev/kfd --security-opt seccomp=unconfined"),
            "{start}"
        );
        assert!(start
            .contains("if [ \"$rt\" = docker ]; then gpu=(--group-add video); else gpu=(); fi"));
        assert!(start.contains("--group-add keep-groups"), "podman's own");
        assert!(!start.contains("--gpus") && !start.contains("nvidia.com"));
        assert!(start.contains(&format!("'{}'", rocm.image)));
    }

    #[test]
    fn deepseek_spans_both_sparks_and_does_not_fit_a_4090() {
        let repo = repo!("deepseek-ai--DeepSeek-V4-Flash-Vision-Exp");
        let model = model_of(&repo);
        let files = files(&model, &[repo]);
        let source = &files.sources[0];
        assert_eq!(source.quantization, "fp8");

        let predicted = fit(&model, source.bytes(), &sparks()).unwrap();
        assert_eq!(predicted.machines, 2);
        assert_eq!(predicted.kv_tokens, None, "compressed attention");
        assert_eq!(predicted.context, 1_048_576, "verification clamps it");

        let profile = starting_profile(
            template("vllm-ngc"),
            source,
            &model,
            &sparks(),
            &predicted,
            None,
        );
        assert_eq!(
            profile.id.as_str(),
            "deepseek-v4-flash-vision-exp-fp8-vllm-x2"
        );
        assert_eq!(profile.problems(), vec![]);
        let Engine::Container(container) = &profile.engine else {
            unreachable!()
        };
        assert_eq!(container.ranks.len(), 2);
        assert_eq!(container.ranks[1]["HEADLESS"], "1");
        assert!(container.args[0].contains("--node-rank \"$NODE_RANK\" --master-addr {head.link}"));
        assert!(container.args[0].contains("'--tensor-parallel-size' '2'"));
        assert_eq!(profile.requirements.interconnect, Some(Interconnect::Rdma));
        assert!(matches!(
            profile.requirements.memory,
            Some(MemoryPerMachine::Unified { .. })
        ));

        let why = fit(&model, source.bytes(), &bbs()).unwrap_err();
        assert!(why.contains("need"), "{why}");
    }

    #[test]
    fn the_embedder_fits_bbs_halo_and_a_spark() {
        let own = repo!("Qwen--Qwen3-Embedding-4B");
        let gguf = repo!("Qwen--Qwen3-Embedding-4B-GGUF", no_config);
        let model = model_of(&own);
        let files = files(&model, &[own, gguf]);

        let bf16 = files.source("Qwen/Qwen3-Embedding-4B@bf16").unwrap();
        let predicted = fit(&model, bf16.bytes(), &bbs()).unwrap();
        let vllm = starting_profile(
            template("vllm-openai"),
            bf16,
            &model,
            &bbs(),
            &predicted,
            None,
        );
        assert_eq!(vllm.id.as_str(), "qwen3-embedding-4b-bf16-vllm-x1");
        assert!(args(&vllm).windows(2).any(|w| w == ["--runner", "pooling"]));

        let q8 = files.source("Qwen/Qwen3-Embedding-4B-GGUF@q8_0").unwrap();
        let predicted = fit(&model, q8.bytes(), &halo()).unwrap();
        assert_eq!(predicted.context, 40_960);
        let llama = starting_profile(
            template("llamacpp-vulkan"),
            q8,
            &model,
            &halo(),
            &predicted,
            None,
        );
        assert_eq!(llama.problems(), vec![]);
        assert_eq!(
            llama.origin,
            ProfileOrigin::Generated {
                run: None,
                model_revision: model.revision.clone().unwrap(),
                starting_template: Some("llamacpp-vulkan".into()),
                sources: vec![],
                answers: vec![],
            }
        );
        assert!(args(&llama).windows(2).any(|w| w == ["--pooling", "last"]));

        let one_spark = PoolHw {
            machines: vec![spark("bginf-sp1")],
            linked: false,
        };
        assert_eq!(fit(&model, bf16.bytes(), &one_spark).unwrap().machines, 1);
    }

    #[test]
    fn the_qwen36_gguf_is_sized_by_its_real_weights_and_wires_its_projector() {
        let base = repo!("Qwen--Qwen3.6-35B-A3B");
        let gguf = repo!("ggml-org--Qwen3.6-35B-A3B-GGUF", no_config);
        let model = model_of(&base);
        let files = files(&model, &[gguf]);
        let bf16 = files.source("ggml-org/Qwen3.6-35B-A3B-GGUF@bf16").unwrap();
        let q4 = files
            .source("ggml-org/Qwen3.6-35B-A3B-GGUF@q4_k_m")
            .unwrap();

        for source in [bf16, q4] {
            let why = fit(&model, source.bytes(), &bbs()).unwrap_err();
            assert!(why.contains("need"), "{}: {why}", source.key);
        }

        let one_spark = PoolHw {
            machines: vec![spark("bginf-sp1")],
            linked: false,
        };
        let predicted = fit(&model, bf16.bytes(), &one_spark).unwrap();
        assert_eq!(predicted.weight_bytes, 69_376_637_248);
        let projector = files
            .projectors
            .iter()
            .find(|p| p.quantization.as_deref() == Some("bf16"));
        let with = starting_profile(
            template("llamacpp-cuda"),
            bf16,
            &model,
            &one_spark,
            &predicted,
            projector,
        );
        assert_eq!(with.problems(), vec![]);
        assert_eq!(with.model.weights.len(), 2);
        assert_eq!(with.model.weights[0].path, "Qwen3.6-35B-A3B-BF16.gguf");
        let mmproj = args(&with)
            .windows(2)
            .find(|w| w[0] == "--mmproj")
            .map(|w| w[1].clone())
            .unwrap();
        assert!(
            mmproj.starts_with("/models/hub/models--ggml-org--Qwen3.6-35B-A3B-GGUF/snapshots/")
                && mmproj.ends_with("/mmproj-Qwen3.6-35B-A3B-BF16.gguf"),
            "{mmproj}"
        );
        assert_eq!(with.modalities, model.facts.modalities);

        let without = starting_profile(
            template("llamacpp-cuda"),
            bf16,
            &model,
            &one_spark,
            &predicted,
            None,
        );
        assert_eq!(
            without.modalities.unwrap().input,
            vec![Modality::Text],
            "no projector, no image input"
        );
    }

    #[test]
    fn the_context_is_clamped_to_the_kv_cache_left_over() {
        let own = repo!("Qwen--Qwen3-Embedding-4B");
        let mut model = model_of(&own);
        model.facts.context = Some(10_000_000);
        let bytes = files(&model, &[own]).sources[0].bytes();
        let card = |gib_of_vram: u64| PoolHw {
            machines: vec![MachineHw {
                memory: HwMemory::Discrete {
                    vram: gib_of_vram * GIB,
                },
                ..bbs().machines[0].clone()
            }],
            linked: false,
        };
        let predicted = fit(&model, bytes, &card(16)).unwrap();
        let kv = predicted.kv_tokens.unwrap();
        assert!(
            predicted.context <= kv && kv - predicted.context < 1024,
            "{predicted:?}"
        );
        assert_eq!(predicted.context % 1024, 0);

        let why = fit(&model, bytes, &card(11)).unwrap_err();
        assert!(why.contains("tokens of KV cache fit"), "{why}");
    }

    /// A generated profile with its stated memory taken away.
    fn unstated(mut profile: InferenceProfile) -> InferenceProfile {
        profile.requirements.memory = None;
        profile
    }

    fn command(start: &str, description: Option<&str>) -> InferenceProfile {
        let mut profile = crate::fixture::command_profile("hand-written", 1);
        profile.engine = Engine::Command {
            start: start.into(),
            stop: None,
            description: description.map(str::to_string),
        };
        profile
    }

    #[test]
    fn vllm_needs_its_memory_share_of_each_machine_as_generation_states_it() {
        let repo = repo!("deepseek-ai--DeepSeek-V4-Flash-Vision-Exp");
        let model = model_of(&repo);
        let files = files(&model, &[repo]);
        let source = &files.sources[0];
        let predicted = fit(&model, source.bytes(), &sparks()).unwrap();
        let profile = starting_profile(
            template("vllm-ngc"),
            source,
            &model,
            &sparks(),
            &predicted,
            None,
        );
        let stated = match profile.requirements.memory {
            Some(MemoryPerMachine::Unified { system }) => system.0,
            ref other => panic!("{other:?}"),
        };
        let need = predict_need(&unstated(profile), Some(&model)).unwrap();
        let PredictedNeed::Share {
            engine,
            flag,
            fraction,
            defaulted,
        } = &need
        else {
            panic!("{need:?}");
        };
        assert_eq!(
            (*engine, *flag, *defaulted),
            ("vLLM", "--gpu-memory-utilization", false)
        );
        assert_eq!(
            format!("{fraction:.2}"),
            format!("{:.2}", sparks().machines[0].usable_fraction())
        );
        assert_eq!(
            need.bytes_on(Some(128 * GIB)),
            Some(stated),
            "the same share generation states"
        );
        assert_eq!(need.bytes_on(None), None, "a share of nothing reported");
        assert!(need.describe().starts_with("vLLM pre-allocates"));
    }

    #[test]
    fn llama_cpp_needs_its_weights_with_overhead_its_kv_and_the_reserve() {
        let own = repo!("Qwen--Qwen3-Embedding-4B");
        let gguf = repo!("Qwen--Qwen3-Embedding-4B-GGUF", no_config);
        let model = model_of(&own);
        let files = files(&model, &[own, gguf]);
        let q8 = files.source("Qwen/Qwen3-Embedding-4B-GGUF@q8_0").unwrap();
        let predicted = fit(&model, q8.bytes(), &bbs()).unwrap();
        let profile = starting_profile(
            template("llamacpp-cuda"),
            q8,
            &model,
            &bbs(),
            &predicted,
            None,
        );
        let stated = profile.requirements.memory;
        let PredictedNeed::Sized {
            bytes,
            weight_bytes,
            kv: Some((tokens, kv_bytes)),
            ..
        } = predict_need(&unstated(profile), Some(&model)).unwrap()
        else {
            panic!("a llama.cpp profile is sized");
        };
        assert_eq!(weight_bytes, predicted.weight_bytes);
        assert_eq!(tokens, predicted.context, "its -c");
        assert_eq!(
            bytes,
            (weight_bytes as f64 * WEIGHT_OVERHEAD) as u64 + kv_bytes + RESERVE_BYTES
        );
        assert_eq!(
            stated,
            Some(MemoryPerMachine::Discrete {
                vram: VramBytes(bytes),
                host: SystemRamBytes(0)
            }),
            "the same arithmetic generation states"
        );
    }

    /// `qwen3-embed-4b` as adopted from bginf: a raw start line running the
    /// llama.cpp image, no weight sizes, its model registered.
    #[test]
    fn a_command_is_read_for_its_engine_and_sized_from_its_model_s_parameters() {
        let own = repo!("Qwen--Qwen3-Embedding-4B");
        let model = model_of(&own);
        let mut profile = command(
            "M=/home/dev/models/qwen3-embedding-4b; podman run -d --name bginf-embed -v $M:/models:ro ghcr.io/ggml-org/llama.cpp@sha256:afdc -m /models/Qwen3-Embedding-4B-Q8_0.gguf --embedding --pooling last -c 32768 -b 8192 --parallel 4 -ngl 99",
            Some("llama.cpp server-vulkan, 4 slots x 8k ctx"),
        );
        profile.quantization = Some("q8_0".into());
        let PredictedNeed::Sized {
            bytes,
            weight_bytes,
            weights_from,
            kv: Some((tokens, kv_bytes)),
            ..
        } = predict_need(&profile, Some(&model)).unwrap()
        else {
            panic!("sized from its parameters");
        };
        let parameters = model.facts.parameters.unwrap();
        assert_eq!(weight_bytes, parameters, "8 bits a parameter");
        assert_eq!(weights_from, "4.0B parameters at q8_0");
        assert_eq!(tokens, 32_768, "the context its four slots share");
        assert_eq!(kv_bytes, 32_768 * kv_bytes_per_token(&model, 1).unwrap());
        assert_eq!(
            bytes,
            (weight_bytes as f64 * WEIGHT_OVERHEAD) as u64 + kv_bytes + RESERVE_BYTES
        );

        // `bash -c '...'` is no context size: the model's trained one is.
        let wrapped = command("bash -c 'llama-server -m /models/x.gguf'", None);
        let PredictedNeed::Sized { kv, .. } = predict_need(
            &InferenceProfile {
                quantization: Some("q8_0".into()),
                ..wrapped
            },
            Some(&model),
        )
        .unwrap() else {
            panic!("sized");
        };
        assert_eq!(kv.map(|(tokens, _)| tokens), model.facts.context);
    }

    /// `qwen36-fast` and `dsv4f-tp2` as adopted: a script whose engine only
    /// the description names, so vLLM's default share is the need.
    #[test]
    fn an_opaque_script_described_as_vllm_needs_vllm_s_default_share() {
        let profile = command(
            "cd /home/dev && ./serve-qwen36.sh",
            Some("vllm (nvcr.io/nvidia/vllm:26.05.post1-py3, marlin moe, 128k ctx, util 0.45)"),
        );
        let predicted = predict_need(&profile, None).unwrap();
        assert_eq!(
            predicted,
            PredictedNeed::Share {
                engine: "vLLM",
                flag: "--gpu-memory-utilization",
                fraction: VLLM_DEFAULT_SHARE,
                defaulted: true,
            }
        );
        assert!(predicted.describe().contains("its default"));

        let sglang = command(
            "python -m sglang.launch_server --mem-fraction-static=0.7",
            None,
        );
        assert_eq!(
            predict_need(&sglang, None)
                .unwrap()
                .bytes_on(Some(100 * GIB)),
            Some(70 * GIB)
        );
    }

    #[test]
    fn a_profile_showing_neither_a_share_nor_a_weight_size_is_refused_naming_why() {
        let why = predict_need(&command("./start.sh", None), None).unwrap_err();
        assert!(why.contains("lists no weight sizes"), "{why}");
        assert!(why.contains("add `requirements.memory`"), "{why}");
        // A model with no parameter count sizes nothing either.
        assert!(predict_need(&command("./start.sh", None), Some(&Model::named("m"))).is_err());
    }
}
