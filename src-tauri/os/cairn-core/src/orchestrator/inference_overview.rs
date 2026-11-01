//! The Inference page's one seam: everything it shows, as [`InferenceOverview`]
//! and [`RunningDetail`], and every operation it offers, as the named actions
//! below. The desktop commands call these and nothing else, and each action
//! goes through the same function the `cairn://inference` writes do, so the
//! page and an agent change a pool the same way.
//!
//! Today the overview is read from the inventory, the pool controller's
//! records and request queue, the act side's phases and load history, the
//! fleet's executor health, and the engines' live metrics. When placement runs
//! local models (cairn/5728) this module is rewired to it; the wire types and
//! the page do not change.
//!
//! A machine's memory is its discrete GPUs' VRAM when it has any, otherwise
//! its system memory (unified memory: a GB10 Spark, Strix Halo). A machine
//! with no reading contributes nothing, and a pool with no reading has no
//! memory at all rather than a zero.

use std::collections::{BTreeMap, BTreeSet};

use cairn_common::executor_protocol::{ExecutorHealthSnapshot, GpuMemory, MeasurementReading};
use cairn_inference::control::Phase;
use cairn_inference::metrics::EngineSummary;
use cairn_inference::pool_control::{self as control, PoolControl, RequestState};
use cairn_inference::served::{self, EngineMeasure};
use cairn_inference::{DeploymentId, Engine, Inventory, MemoryPerMachine, Pool, PoolId, Recipe};
use cairn_models::types::catalog::Modality;
use serde::Serialize;

use super::inference_control::{self, LoadRow};
use super::pool_control::{self, PoolPatch, SettingKey, StoredRequest};
use super::Orchestrator;

/// Who the page acts as.
pub const OPERATOR: &str = "operator";

/// How long a failed load stays on its pool's line.
const FAILED_SHOWN_MS: i64 = 30 * 60_000;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceOverview {
    pub pools: Vec<InferencePool>,
    pub models: Vec<InferenceModel>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferencePool {
    pub id: String,
    pub machines: Vec<InferenceMachine>,
    /// The interconnect joining the pool's machines, once detected
    /// (cairn/5728).
    pub link: Option<InferenceLink>,
    pub memory: Option<PoolMemory>,
    pub running: Vec<RunningModel>,
    /// The model (real name) the pool loads when idle.
    pub warm: Option<String>,
    pub loadable: Vec<LoadableModel>,
    pub unload_idle: UnloadIdle,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceMachine {
    pub name: String,
    pub gpu: Option<String>,
    pub memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceLink {
    pub machines: [String; 2],
    pub bytes_per_sec: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMemory {
    pub total_bytes: u64,
    pub model_bytes: u64,
    pub other_bytes: u64,
    pub free_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RunningState {
    Ready,
    Loading,
    Stopping,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunningModel {
    pub deployment: String,
    pub model: String,
    pub recipe: String,
    pub state: RunningState,
    pub pinned: bool,
    pub loading: Option<Loading>,
    pub failed: Option<String>,
    pub rates: Option<Rates>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Loading {
    pub elapsed_ms: u64,
    /// The slowest healthy load this deployment has had.
    pub expected_ms: Option<u64>,
    /// Requests waiting for it.
    pub waiting: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Rates {
    pub requests_per_min: Option<f64>,
    pub tokens_per_sec: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadableModel {
    pub model: String,
    pub recipe: String,
    pub size_bytes: Option<u64>,
    pub fits: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnloadIdle {
    pub secs: u64,
    /// Derived from measured loads, not set by the operator.
    pub auto: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceModel {
    /// The real name, the checkpoint's Hugging Face repository id.
    pub name: String,
    /// Who published it: the part of the name before the slash.
    pub maker: Option<String>,
    pub facts: ModelFacts,
    pub recipes: Vec<ModelRecipe>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelFacts {
    pub inputs: Vec<Modality>,
    pub outputs: Vec<Modality>,
    pub parameters: Option<u64>,
    pub context_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRecipe {
    pub id: String,
    pub quantization: Option<String>,
    pub server: Option<String>,
    pub machines: u32,
    pub origin: Option<String>,
    pub in_use: bool,
    pub fit: Vec<RecipeFit>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeFit {
    pub pool: String,
    pub fits: bool,
    pub memory_bytes: Option<u64>,
    pub load_ms: Option<u64>,
    pub decode_tokens_per_sec: Option<f64>,
    pub failed: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunningDetail {
    pub deployment: String,
    pub model: String,
    pub metrics: Option<EngineSummary>,
    pub config: RunningConfig,
}

/// How a running model is run, never its start or stop shell.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunningConfig {
    pub recipe: String,
    pub engine: String,
    /// The container image, native artifact, or the description of a command
    /// recipe.
    pub runs: Option<String>,
    pub context_tokens: Option<u64>,
    pub kv_cache_tokens: Option<u64>,
    pub machines: Vec<String>,
    pub port: u16,
}

/// A short answer from a running model.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TryAnswer {
    pub text: String,
    pub tokens_per_sec: Option<f64>,
}

/// One machine as the overview reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MachineFacts {
    pub gpu: Option<String>,
    /// Total and free bytes of the memory a model lives in.
    pub memory: Option<(u64, u64)>,
    /// What the machine's GPUs hold now.
    pub gpu_allocated_bytes: Option<u64>,
}

impl MachineFacts {
    pub(crate) fn of(snapshot: &ExecutorHealthSnapshot) -> Self {
        let devices = match &snapshot.host.gpus.reading {
            MeasurementReading::Measured { value } => value.as_slice(),
            MeasurementReading::Unavailable { .. } => &[],
        };
        let gpu = devices.first().map(|first| match devices.len() {
            1 => first.model.clone(),
            n => format!("{n}× {}", first.model),
        });
        let mut discrete: Option<(u64, u64)> = None;
        let mut allocated: Option<u64> = None;
        for device in devices {
            match &device.memory {
                MeasurementReading::Measured {
                    value:
                        GpuMemory::Discrete {
                            used_bytes,
                            total_bytes,
                        },
                } => {
                    let (used, total) = discrete.get_or_insert((0, 0));
                    *used += used_bytes;
                    *total += total_bytes;
                    *allocated.get_or_insert(0) += used_bytes;
                }
                MeasurementReading::Measured {
                    value:
                        GpuMemory::Unified {
                            gpu_allocated_bytes: MeasurementReading::Measured { value },
                            ..
                        },
                } => *allocated.get_or_insert(0) += value,
                _ => {}
            }
        }
        let memory = match discrete {
            Some((used, total)) => Some((total, total.saturating_sub(used))),
            None => match &snapshot.machine.memory.reading {
                MeasurementReading::Measured { value } => {
                    Some((value.total_bytes, value.available_bytes))
                }
                MeasurementReading::Unavailable { .. } => None,
            },
        };
        Self {
            gpu,
            memory,
            gpu_allocated_bytes: allocated,
        }
    }
}

/// Everything the overview is assembled from.
pub(crate) struct Sources<'a> {
    /// The inventory as served: settings declarations laid over the recipes.
    pub inventory: &'a Inventory,
    pub phases: &'a BTreeMap<DeploymentId, Phase>,
    pub loads: &'a BTreeMap<DeploymentId, Vec<LoadRow>>,
    pub controls: &'a BTreeMap<PoolId, PoolControl>,
    pub requests: &'a [StoredRequest],
    pub machines: &'a BTreeMap<String, MachineFacts>,
    pub engines: &'a BTreeMap<DeploymentId, EngineSummary>,
    pub measures: &'a BTreeMap<DeploymentId, EngineMeasure>,
    pub now: i64,
}

fn since(now: i64, at: i64) -> u64 {
    u64::try_from(now.saturating_sub(at)).unwrap_or(0)
}

impl Sources<'_> {
    fn model_of(&self, deployment: &DeploymentId) -> Option<&Recipe> {
        let record = self.inventory.deployments.get(deployment)?;
        self.inventory.recipes.get(&record.recipe)
    }

    fn waiting(&self, deployment: &DeploymentId) -> Vec<&StoredRequest> {
        self.requests
            .iter()
            .filter(|r| {
                &r.request.deployment == deployment && r.request.state == RequestState::Waiting
            })
            .collect()
    }

    fn slowest_load_ms(&self, deployment: &DeploymentId) -> Option<u64> {
        self.loads
            .get(deployment)?
            .iter()
            .filter(|load| load.healthy)
            .filter_map(|load| load.load_ms)
            .max()
    }

    /// The newest measured footprint, per machine.
    fn footprint(&self, deployment: &DeploymentId) -> BTreeMap<String, u64> {
        inference_control::last_footprint(self.loads.get(deployment))
    }

    fn running(&self, pool: &Pool, deployment: &DeploymentId) -> Option<RunningModel> {
        let recipe = self.model_of(deployment)?;
        let waiting = self.waiting(deployment);
        let expected_ms = self.slowest_load_ms(deployment);
        let loading = |at: i64| Loading {
            elapsed_ms: since(self.now, at),
            expected_ms,
            waiting: waiting.len() as u32,
        };
        let (state, loading, failed) = match self.phases.get(deployment) {
            Some(Phase::Healthy { .. }) => (RunningState::Ready, None, None),
            Some(Phase::Starting { requested_ms, .. }) => {
                (RunningState::Loading, Some(loading(*requested_ms)), None)
            }
            Some(Phase::Reserving { since_ms, .. }) => {
                (RunningState::Loading, Some(loading(*since_ms)), None)
            }
            Some(Phase::Stopping { .. }) => (RunningState::Stopping, None, None),
            Some(Phase::Failed { at_ms, reason }) if self.now - at_ms < FAILED_SHOWN_MS => {
                (RunningState::Failed, None, Some(reason.clone()))
            }
            _ => {
                // Asked for and not started yet: loading from the first ask.
                let first = waiting.iter().map(|r| r.request.at).min()?;
                (RunningState::Loading, Some(loading(first)), None)
            }
        };
        let pinned = self
            .controls
            .get(&pool.id)
            .and_then(|control| control.pin.as_ref())
            .is_some_and(|pin| pin.set.contains(deployment));
        let rates = self
            .engines
            .get(deployment)
            .map(|engine| Rates {
                requests_per_min: engine.requests_per_min,
                tokens_per_sec: engine.generated_tokens_per_sec,
            })
            .filter(|rates| rates.requests_per_min.is_some() || rates.tokens_per_sec.is_some());
        Some(RunningModel {
            deployment: deployment.to_string(),
            model: recipe.model.name.clone(),
            recipe: recipe.id.to_string(),
            state,
            pinned,
            loading,
            failed,
            rates,
        })
    }

    /// The smallest memory a machine of `pool` has, of those that report.
    fn smallest_machine(&self, pool: &Pool) -> Option<u64> {
        pool.machines
            .iter()
            .filter_map(|m| self.machines.get(m.name.as_str())?.memory)
            .map(|(total, _)| total)
            .min()
    }

    /// What `deployment` takes: its whole size and the most it needs of one
    /// machine, from its measured footprint, else its recipe's declaration.
    fn size(&self, deployment: &DeploymentId, recipe: &Recipe) -> (Option<u64>, Option<u64>) {
        let footprint = self.footprint(deployment);
        if !footprint.is_empty() {
            return (
                Some(footprint.values().sum()),
                footprint.values().max().copied(),
            );
        }
        let per_machine = recipe.requirements.memory.map(|memory| match memory {
            MemoryPerMachine::Unified { system } => system.0,
            MemoryPerMachine::Discrete { vram, .. } => vram.0,
        });
        (
            per_machine.map(|bytes| bytes * u64::from(recipe.requirements.machines)),
            per_machine,
        )
    }

    fn fits(&self, pool: &Pool, recipe: &Recipe, per_machine: Option<u64>) -> bool {
        if recipe.requirements.machines as usize > pool.machines.len() {
            return false;
        }
        match (per_machine, self.smallest_machine(pool)) {
            (Some(needs), Some(has)) => needs <= has,
            _ => true,
        }
    }

    fn pool_memory(&self, pool: &Pool, running: &[RunningModel]) -> Option<PoolMemory> {
        let busy: Vec<&DeploymentId> = running
            .iter()
            .filter(|r| matches!(r.state, RunningState::Ready | RunningState::Loading))
            .filter_map(|r| {
                self.inventory
                    .deployments
                    .get_key_value(&DeploymentId::from(r.deployment.as_str()))
                    .map(|(id, _)| id)
            })
            .collect();
        let mut memory: Option<PoolMemory> = None;
        for machine in &pool.machines {
            let name = machine.name.as_str();
            let Some(facts) = self.machines.get(name) else {
                continue;
            };
            let Some((total, free)) = facts.memory else {
                continue;
            };
            let used = total.saturating_sub(free);
            let holders: Vec<&&DeploymentId> = busy
                .iter()
                .filter(|id| {
                    self.inventory.deployments[**id]
                        .machines
                        .iter()
                        .any(|m| m.as_str() == name)
                })
                .collect();
            let model = if holders.is_empty() {
                0
            } else {
                facts.gpu_allocated_bytes.unwrap_or_else(|| {
                    holders
                        .iter()
                        .filter_map(|id| self.footprint(id).get(name).copied())
                        .sum()
                })
            }
            .min(used);
            let sum = memory.get_or_insert(PoolMemory {
                total_bytes: 0,
                model_bytes: 0,
                other_bytes: 0,
                free_bytes: 0,
            });
            sum.total_bytes += total;
            sum.model_bytes += model;
            sum.other_bytes += used - model;
            sum.free_bytes += free;
        }
        memory
    }

    fn pool(&self, pool: &Pool) -> InferencePool {
        let deployments: Vec<&DeploymentId> = self
            .inventory
            .deployments
            .values()
            .filter(|d| {
                self.inventory
                    .pool_of_deployment(d)
                    .is_some_and(|p| p.id == pool.id)
            })
            .map(|d| &d.id)
            .collect();
        let running: Vec<RunningModel> = deployments
            .iter()
            .filter_map(|id| self.running(pool, id))
            .collect();
        // A model on its way down is not offered for loading until it is down.
        let busy: BTreeSet<&str> = running
            .iter()
            .filter(|r| r.state != RunningState::Failed)
            .map(|r| r.deployment.as_str())
            .collect();
        let loadable = deployments
            .iter()
            .filter(|id| !busy.contains(id.as_str()))
            .filter_map(|id| {
                let recipe = self.model_of(id)?;
                let (size, per_machine) = self.size(id, recipe);
                Some(LoadableModel {
                    model: recipe.model.name.clone(),
                    recipe: recipe.id.to_string(),
                    size_bytes: size,
                    fits: self.fits(pool, recipe, per_machine),
                })
            })
            .collect();
        let warm = pool
            .default
            .iter()
            .find_map(|id| self.model_of(id))
            .map(|recipe| recipe.model.name.clone());
        let slowest: BTreeMap<&DeploymentId, u64> = deployments
            .iter()
            .filter_map(|id| Some((*id, self.slowest_load_ms(id)?)))
            .collect();
        let settings = self
            .controls
            .get(&pool.id)
            .map(|control| control.settings)
            .unwrap_or_default()
            .effective(control::slowest_load_secs(
                pool,
                &self.inventory.deployments,
                slowest,
            ));
        InferencePool {
            id: pool.id.to_string(),
            machines: pool
                .machines
                .iter()
                .map(|machine| {
                    let facts = self.machines.get(machine.name.as_str());
                    InferenceMachine {
                        name: machine.name.to_string(),
                        gpu: facts.and_then(|f| f.gpu.clone()),
                        memory_bytes: facts.and_then(|f| f.memory).map(|(total, _)| total),
                    }
                })
                .collect(),
            link: None,
            memory: self.pool_memory(pool, &running),
            running,
            warm,
            loadable,
            unload_idle: UnloadIdle {
                secs: settings.model_idle.secs,
                auto: !matches!(settings.model_idle.origin, control::SettingOrigin::Operator),
            },
        }
    }

    /// The models, from the recipes grouped by real name, with each recipe's
    /// fit per pool from the act side's load history.
    ///
    /// The fallback until model records and verifications exist (cairn/5730),
    /// which replace this function: then facts come from the model record and
    /// fit from the verification rows, quantization, server and origin from the
    /// recipe, and nothing here survives.
    fn models_from_recipes(&self, running: &[RunningModel]) -> Vec<InferenceModel> {
        let in_use: BTreeSet<&str> = running
            .iter()
            .filter(|r| matches!(r.state, RunningState::Ready | RunningState::Loading))
            .map(|r| r.recipe.as_str())
            .collect();
        let mut by_name: BTreeMap<String, Vec<&Recipe>> = BTreeMap::new();
        for recipe in self.inventory.recipes.values() {
            by_name
                .entry(recipe.model.name.trim().to_ascii_lowercase())
                .or_default()
                .push(recipe);
        }
        by_name
            .into_values()
            .filter_map(|recipes| {
                let first = *recipes.first()?;
                let name = first.model.name.trim().to_string();
                let modalities = served::modalities(first);
                let deployments_of = |recipe: &Recipe| -> Vec<&DeploymentId> {
                    self.inventory
                        .deployments
                        .values()
                        .filter(|d| d.recipe == recipe.id)
                        .map(|d| &d.id)
                        .collect()
                };
                let context_tokens = recipes
                    .iter()
                    .flat_map(|recipe| deployments_of(recipe))
                    .filter_map(|id| self.measures.get(id)?.context_length())
                    .max();
                let model_recipes = recipes
                    .iter()
                    .map(|recipe| ModelRecipe {
                        id: recipe.id.to_string(),
                        quantization: None,
                        server: None,
                        machines: recipe.requirements.machines,
                        origin: None,
                        in_use: in_use.contains(recipe.id.as_str()),
                        fit: deployments_of(recipe)
                            .into_iter()
                            .filter_map(|id| {
                                let pool = self
                                    .inventory
                                    .pool_of_deployment(&self.inventory.deployments[id])?;
                                let (size, per_machine) = self.size(id, recipe);
                                let latest = self.loads.get(id).and_then(|loads| loads.first());
                                Some(RecipeFit {
                                    pool: pool.id.to_string(),
                                    fits: self.fits(pool, recipe, per_machine),
                                    memory_bytes: size,
                                    load_ms: self.slowest_load_ms(id),
                                    decode_tokens_per_sec: None,
                                    failed: latest
                                        .filter(|load| !load.healthy)
                                        .and_then(|load| load.reason.clone()),
                                })
                            })
                            .collect(),
                    })
                    .collect();
                Some(InferenceModel {
                    maker: name.split_once('/').map(|(maker, _)| maker.to_string()),
                    name,
                    facts: ModelFacts {
                        inputs: modalities.input,
                        outputs: modalities.output,
                        parameters: None,
                        context_tokens,
                    },
                    recipes: model_recipes,
                })
            })
            .collect()
    }

    pub(crate) fn overview(&self) -> InferenceOverview {
        let pools: Vec<InferencePool> = self
            .inventory
            .pools
            .values()
            .map(|pool| self.pool(pool))
            .collect();
        let running: Vec<RunningModel> = pools
            .iter()
            .flat_map(|pool| pool.running.iter().cloned())
            .collect();
        InferenceOverview {
            models: self.models_from_recipes(&running),
            pools,
        }
    }

    pub(crate) fn running_detail(&self, deployment: &DeploymentId) -> Option<RunningDetail> {
        let record = self.inventory.deployments.get(deployment)?;
        let recipe = self.inventory.recipes.get(&record.recipe)?;
        let measure = self.measures.get(deployment);
        let runs = match &recipe.engine {
            Engine::Container(container) => Some(container.image.clone()),
            Engine::Native {
                artifact, version, ..
            } => Some(format!("{artifact} {version}")),
            Engine::Command { description, .. } => description.clone(),
        };
        Some(RunningDetail {
            deployment: deployment.to_string(),
            model: recipe.model.name.clone(),
            metrics: self.engines.get(deployment).copied(),
            config: RunningConfig {
                recipe: recipe.id.to_string(),
                engine: recipe.engine.kind().to_string(),
                runs,
                context_tokens: measure.and_then(EngineMeasure::context_length),
                kv_cache_tokens: measure.and_then(|m| m.kv_cache_tokens),
                machines: record.machines.iter().map(ToString::to_string).collect(),
                port: record.api_port,
            },
        })
    }
}

/// Read every source once and call `with` on them.
async fn with_sources<T>(
    orch: &Orchestrator,
    with: impl FnOnce(&Sources<'_>) -> T,
) -> Result<T, String> {
    let db = &orch.db.local;
    let inventory = super::inference_route::served_inventory(orch).await?;
    let phases = pool_control::deployment_phases(db).await?;
    let loads = inference_control::load_history(db).await?;
    let controls = pool_control::load_controls(db).await?;
    let requests = pool_control::load_requests(db).await?;
    let machines: BTreeMap<String, MachineFacts> = orch
        .fleet
        .executor_health(crate::fleet::unix_time_ms())
        .iter()
        .map(|snapshot| (snapshot.public_name.clone(), MachineFacts::of(snapshot)))
        .collect();
    let engines = super::inference_metrics::summaries();
    let measures = super::inference_route::measures();
    Ok(with(&Sources {
        inventory: &inventory,
        phases: &phases,
        loads: &loads,
        controls: &controls,
        requests: &requests,
        machines: &machines,
        engines: &engines,
        measures: &measures,
        now: crate::fleet::unix_time_ms() as i64,
    }))
}

pub async fn overview(orch: &Orchestrator) -> Result<InferenceOverview, String> {
    with_sources(orch, |sources| sources.overview()).await
}

pub async fn running_detail(
    orch: &Orchestrator,
    deployment: &str,
) -> Result<RunningDetail, String> {
    let id = DeploymentId::from(deployment);
    with_sources(orch, |sources| sources.running_detail(&id))
        .await?
        .ok_or_else(|| format!("there is no deployment {deployment}"))
}

/// Load `model` (its real name) on `pool`: a request, so the model loads and
/// its idle clock starts.
pub async fn load(orch: &Orchestrator, pool: &str, model: &str) -> Result<String, String> {
    let handle =
        pool_control::request_model(orch, model, Some(&PoolId::from(pool)), OPERATOR).await?;
    let status = handle.status(orch).await?;
    Ok(format!("Request #{}: {}", handle.id, status.describe()))
}

pub async fn unload(orch: &Orchestrator, deployment: &str) -> Result<String, String> {
    pool_control::unload(orch, &DeploymentId::from(deployment), OPERATOR).await
}

pub async fn set_pinned(
    orch: &Orchestrator,
    deployment: &str,
    pinned: bool,
) -> Result<String, String> {
    pool_control::set_pinned(orch, &DeploymentId::from(deployment), pinned, OPERATOR).await
}

pub async fn set_warm(
    orch: &Orchestrator,
    pool: &str,
    model: Option<&str>,
) -> Result<String, String> {
    let patch = PoolPatch {
        pool: PoolId::from(pool),
        warm: Some(model.map(str::to_string)),
        ..PoolPatch::default()
    };
    pool_control::patch_pool(orch, &patch, OPERATOR).await
}

/// Override the pool's unload-idle window in minutes, or derive it again.
pub async fn set_unload_idle(
    orch: &Orchestrator,
    pool: &str,
    minutes: Option<u64>,
) -> Result<String, String> {
    if minutes == Some(0) {
        return Err("unload idle is a positive number of minutes, or auto".into());
    }
    let patch = PoolPatch {
        pool: PoolId::from(pool),
        settings: vec![(SettingKey::ModelIdle, minutes.map(|m| m * 60))],
        ..PoolPatch::default()
    };
    pool_control::patch_pool(orch, &patch, OPERATOR).await
}

/// Ask `model` (its real name) one short question through the models facet.
pub async fn try_prompt(
    orch: &Orchestrator,
    model: &str,
    prompt: &str,
) -> Result<TryAnswer, String> {
    let outcome = orch
        .complete_named(
            None,
            crate::responses::ResponseCaller::Internal {
                label: "the Inference page's try".into(),
            },
            serde_json::json!({
                "model": format!("{}/{model}", cairn_models::types::catalog::INFERENCE_BACKEND_KEY),
                "prompt": prompt,
            }),
            &crate::mcp::handlers::run::response_item_bound(Some(120_000)),
            None,
        )
        .await?;
    let tokens_per_sec = outcome
        .usage
        .output_tokens
        .filter(|_| outcome.latency_ms > 0)
        .map(|tokens| tokens as f64 * 1000.0 / outcome.latency_ms as f64);
    Ok(TryAnswer {
        text: outcome.text,
        tokens_per_sec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_inference::pool_control::{Active, Pin, PoolSettings, Request, Why};

    const GIB: u64 = 1 << 30;
    const DSV4: &str = "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp";
    const QWEN: &str = "Qwen/Qwen3.6-35B-A3B";

    fn inventory() -> Inventory {
        Inventory::from_yaml(
            r#"
models:
  - {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
  - {name: Qwen/Qwen3.6-35B-A3B}
  - {name: Qwen/Qwen3-Embedding-4B}
recipes:
  - id: dsv4f-tp2
    model: {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
    servedName: deepseek-v4-flash-vision-exp
    modalities: {input: [text, image], output: [text]}
    engine: {kind: command, start: ./start.sh, description: vLLM dspark image}
    requirements: {machines: 2}
  - id: qwen36-fast
    model: {name: Qwen/Qwen3.6-35B-A3B}
    servedName: qwen36-fast
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1, memory: {kind: unified, system: 200000000000}}
  - id: qwen3-embed-4b
    model: {name: Qwen/Qwen3-Embedding-4B}
    servedName: qwen3-embedding-4b
    verbs: [embed]
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1}
deployments:
  - {id: dsv4f-tp2, recipe: dsv4f-tp2, machines: [sp1, sp2], head: sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen36-fast, recipe: qwen36-fast, machines: [sp1], head: sp1, apiPort: 8000, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, recipe: qwen3-embed-4b, machines: [fedora], head: fedora, apiPort: 8090, apiPath: /v1, healthPath: /health}
pools:
  - {id: sparks, machines: [{name: sp1}, {name: sp2}], default: [dsv4f-tp2]}
  - {id: halo, machines: [{name: fedora}], default: [qwen3-embed-4b]}
"#,
        )
        .unwrap()
    }

    fn spark(allocated: Option<u64>) -> MachineFacts {
        MachineFacts {
            gpu: Some("NVIDIA GB10".into()),
            memory: Some((122 * GIB, 20 * GIB)),
            gpu_allocated_bytes: allocated,
        }
    }

    fn healthy_load(ms: u64, footprint: &[(&str, u64)]) -> LoadRow {
        LoadRow {
            recipe: String::new(),
            requested_at: 0,
            sent_at: Some(0),
            finished_at: 0,
            healthy: true,
            load_ms: Some(ms),
            reason: None,
            footprint: Some(footprint.iter().map(|(m, b)| (m.to_string(), *b)).collect()),
        }
    }

    fn waiting(id: i64, deployment: &str, at: i64) -> StoredRequest {
        StoredRequest {
            pool: "sparks".into(),
            request: Request {
                id,
                deployment: deployment.into(),
                model: DSV4.into(),
                requester: "operator".into(),
                at,
                state: RequestState::Waiting,
            },
        }
    }

    struct World {
        inventory: Inventory,
        phases: BTreeMap<DeploymentId, Phase>,
        loads: BTreeMap<DeploymentId, Vec<LoadRow>>,
        controls: BTreeMap<PoolId, PoolControl>,
        requests: Vec<StoredRequest>,
        machines: BTreeMap<String, MachineFacts>,
        engines: BTreeMap<DeploymentId, EngineSummary>,
        measures: BTreeMap<DeploymentId, EngineMeasure>,
    }

    impl World {
        fn new() -> Self {
            Self {
                inventory: inventory(),
                phases: BTreeMap::new(),
                loads: BTreeMap::new(),
                controls: BTreeMap::new(),
                requests: Vec::new(),
                machines: BTreeMap::new(),
                engines: BTreeMap::new(),
                measures: BTreeMap::new(),
            }
        }

        fn overview(&self) -> InferenceOverview {
            self.sources().overview()
        }

        fn sources(&self) -> Sources<'_> {
            Sources {
                inventory: &self.inventory,
                phases: &self.phases,
                loads: &self.loads,
                controls: &self.controls,
                requests: &self.requests,
                machines: &self.machines,
                engines: &self.engines,
                measures: &self.measures,
                now: 600_000,
            }
        }
    }

    fn pool<'a>(overview: &'a InferenceOverview, id: &str) -> &'a InferencePool {
        overview.pools.iter().find(|p| p.id == id).unwrap()
    }

    #[test]
    fn a_ready_model_shows_its_rates_and_holds_its_share_of_unified_memory() {
        let mut world = World::new();
        world.phases.insert(
            "dsv4f-tp2".into(),
            Phase::Healthy {
                since_ms: 0,
                load_ms: Some(400_000),
            },
        );
        world.machines.insert("sp1".into(), spark(Some(90 * GIB)));
        world.machines.insert("sp2".into(), spark(Some(90 * GIB)));
        world.engines.insert(
            "dsv4f-tp2".into(),
            EngineSummary {
                requests_per_min: Some(3.1),
                generated_tokens_per_sec: Some(46.0),
                ..EngineSummary::default()
            },
        );
        let overview = world.overview();
        let sparks = pool(&overview, "sparks");
        assert_eq!(sparks.running.len(), 1);
        let running = &sparks.running[0];
        assert_eq!(running.model, DSV4);
        assert_eq!(running.state, RunningState::Ready);
        assert!(!running.pinned);
        assert_eq!(
            running.rates,
            Some(Rates {
                requests_per_min: Some(3.1),
                tokens_per_sec: Some(46.0)
            })
        );
        assert_eq!(
            sparks.memory,
            Some(PoolMemory {
                total_bytes: 244 * GIB,
                model_bytes: 180 * GIB,
                other_bytes: 24 * GIB,
                free_bytes: 40 * GIB,
            })
        );
        assert_eq!(sparks.warm.as_deref(), Some(DSV4));
        assert_eq!(sparks.machines[0].gpu.as_deref(), Some("NVIDIA GB10"));
        // The running model is not offered for loading; the other recipe is.
        assert_eq!(
            sparks
                .loadable
                .iter()
                .map(|l| l.model.as_str())
                .collect::<Vec<_>>(),
            [QWEN]
        );
    }

    #[test]
    fn discrete_vram_is_the_pool_memory_and_no_reading_draws_nothing() {
        let mut world = World::new();
        world.machines.insert(
            "fedora".into(),
            MachineFacts {
                gpu: Some("RTX 4090".into()),
                memory: Some((24 * GIB, 20 * GIB)),
                gpu_allocated_bytes: Some(4 * GIB),
            },
        );
        let overview = world.overview();
        // Nothing runs on halo, so the 4 GiB in use is not a model's.
        assert_eq!(
            pool(&overview, "halo").memory,
            Some(PoolMemory {
                total_bytes: 24 * GIB,
                model_bytes: 0,
                other_bytes: 4 * GIB,
                free_bytes: 20 * GIB,
            })
        );
        assert_eq!(pool(&overview, "sparks").memory, None);
        assert_eq!(pool(&overview, "sparks").machines[0].memory_bytes, None);
    }

    #[test]
    fn a_loading_model_says_how_long_against_its_slowest_load_and_who_waits() {
        let mut world = World::new();
        world.phases.insert(
            "dsv4f-tp2".into(),
            Phase::Starting {
                since_ms: 400_000,
                requested_ms: 420_000,
            },
        );
        world.loads.insert(
            "dsv4f-tp2".into(),
            vec![
                healthy_load(300_000, &[("sp1", 100 * GIB)]),
                healthy_load(420_000, &[]),
            ],
        );
        world.requests = vec![
            waiting(1, "dsv4f-tp2", 410_000),
            waiting(2, "dsv4f-tp2", 415_000),
        ];
        let overview = world.overview();
        let running = &pool(&overview, "sparks").running[0];
        assert_eq!(running.state, RunningState::Loading);
        assert_eq!(
            running.loading,
            Some(Loading {
                elapsed_ms: 180_000,
                expected_ms: Some(420_000),
                waiting: 2,
            })
        );
    }

    #[test]
    fn a_request_not_yet_started_already_reads_as_loading() {
        let mut world = World::new();
        world.requests = vec![waiting(1, "qwen36-fast", 540_000)];
        let overview = world.overview();
        let running = &pool(&overview, "sparks").running[0];
        assert_eq!(running.model, QWEN);
        assert_eq!(running.loading.unwrap().elapsed_ms, 60_000);
        assert_eq!(running.loading.unwrap().waiting, 1);
    }

    #[test]
    fn a_pinned_model_says_so() {
        let mut world = World::new();
        world.phases.insert(
            "dsv4f-tp2".into(),
            Phase::Healthy {
                since_ms: 0,
                load_ms: None,
            },
        );
        world.controls.insert(
            "sparks".into(),
            PoolControl {
                pool: "sparks".into(),
                controlled: true,
                settings: PoolSettings::default(),
                pin: Some(Pin {
                    set: ["dsv4f-tp2".into()].into(),
                    holder: OPERATOR.into(),
                    at: 0,
                }),
                active: Active {
                    members: BTreeMap::new(),
                    why: Why::Adopted,
                    since: 0,
                },
                last_batch_work_at: 0,
                generation: 0,
            },
        );
        assert!(pool(&world.overview(), "sparks").running[0].pinned);
    }

    #[test]
    fn a_recipe_bigger_than_a_machine_will_not_fit() {
        let mut world = World::new();
        world.machines.insert("sp1".into(), spark(None));
        world.machines.insert("sp2".into(), spark(None));
        let overview = world.overview();
        let sparks = pool(&overview, "sparks");
        let qwen = sparks.loadable.iter().find(|l| l.model == QWEN).unwrap();
        // 200 GB declared against a 122 GiB machine.
        assert!(!qwen.fits);
        assert_eq!(qwen.size_bytes, Some(200_000_000_000));
        let deepseek = sparks.loadable.iter().find(|l| l.model == DSV4).unwrap();
        assert!(deepseek.fits);
        assert_eq!(deepseek.size_bytes, None);
        let model = overview.models.iter().find(|m| m.name == QWEN).unwrap();
        assert!(!model.recipes[0].fit[0].fits);
        assert_eq!(model.recipes[0].fit[0].pool, "sparks");
    }

    #[test]
    fn unload_idle_derives_from_the_slowest_load_until_the_operator_sets_it() {
        let mut world = World::new();
        assert_eq!(
            pool(&world.overview(), "sparks").unload_idle,
            UnloadIdle {
                secs: 30 * 60,
                auto: true
            }
        );
        world
            .loads
            .insert("dsv4f-tp2".into(), vec![healthy_load(120_000, &[])]);
        // 6 × 2 min is under the 15-minute floor.
        assert_eq!(
            pool(&world.overview(), "sparks").unload_idle,
            UnloadIdle {
                secs: 15 * 60,
                auto: true
            }
        );
        let mut control = PoolControl::begin(
            &world.inventory.pools[&PoolId::from("sparks")],
            &world.inventory.deployments,
            &BTreeMap::new(),
            0,
        );
        control.settings.model_idle_secs = Some(600);
        world.controls.insert("sparks".into(), control);
        assert_eq!(
            pool(&world.overview(), "sparks").unload_idle,
            UnloadIdle {
                secs: 600,
                auto: false
            }
        );
    }

    #[test]
    fn models_group_recipes_by_real_name_with_facts_and_measured_fit() {
        let mut world = World::new();
        world.loads.insert(
            "dsv4f-tp2".into(),
            vec![healthy_load(
                420_000,
                &[("sp1", 101 * GIB), ("sp2", 99 * GIB)],
            )],
        );
        world.measures.insert(
            "dsv4f-tp2".into(),
            EngineMeasure {
                max_model_len: Some(1_048_576),
                kv_cache_tokens: None,
            },
        );
        let overview = world.overview();
        let names: Vec<&str> = overview.models.iter().map(|m| m.name.as_str()).collect();
        // By real name, case-insensitively.
        assert_eq!(names, [DSV4, "Qwen/Qwen3-Embedding-4B", QWEN]);
        let deepseek = &overview.models[0];
        assert_eq!(deepseek.maker.as_deref(), Some("deepseek-ai"));
        assert_eq!(deepseek.facts.inputs, [Modality::Text, Modality::Image]);
        assert_eq!(deepseek.facts.context_tokens, Some(1_048_576));
        let recipe = &deepseek.recipes[0];
        assert_eq!(recipe.machines, 2);
        assert!(!recipe.in_use);
        assert_eq!(
            recipe.fit,
            [RecipeFit {
                pool: "sparks".into(),
                fits: true,
                memory_bytes: Some(200 * GIB),
                load_ms: Some(420_000),
                decode_tokens_per_sec: None,
                failed: None,
            }]
        );
        let embedder = &overview.models[1];
        assert_eq!(embedder.facts.outputs, [Modality::Embeddings]);
    }

    #[test]
    fn the_drill_down_shows_how_it_runs_but_never_its_shell() {
        let world = World::new();
        let detail = world.sources().running_detail(&"dsv4f-tp2".into()).unwrap();
        assert_eq!(detail.model, DSV4);
        assert_eq!(detail.config.engine, "command");
        assert_eq!(detail.config.runs.as_deref(), Some("vLLM dspark image"));
        assert_eq!(detail.config.machines, ["sp1", "sp2"]);
        assert_eq!(detail.config.port, 8888);
        let json = serde_json::to_string(&detail).unwrap();
        assert!(!json.contains("start.sh"), "{json}");
    }
}
