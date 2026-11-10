//! The Inference page's one seam: everything it shows, as [`InferenceOverview`]
//! and [`RunningDetail`], and every operation it offers, as the named actions
//! below. The desktop commands call these and nothing else, and each action
//! goes through the same function the `cairn://inference` writes do, so the
//! page and an agent change a pool the same way.
//!
//! The overview is read from placement's view of local models
//! ([`crate::fleet::models::view`]: the detected pools with their link, warm
//! model and windows, the residents and their pins, where each deployment may
//! run, and the requests), the inventory, the act side's phases and load
//! history, the fleet's executor health, and the engines' live metrics. Its
//! actions are placement's doors.
//!
//! A machine's memory is its discrete GPUs' VRAM when it has any, otherwise
//! its system memory (unified memory: a GB10 Spark, Strix Halo). A machine
//! with no reading contributes nothing, and a pool with no reading has no
//! memory at all rather than a zero.

use std::collections::{BTreeMap, BTreeSet};

use cairn_common::executor_protocol::{
    model_gpus, ExecutorHealthSnapshot, GpuMemory, GpuVendor, MeasurementReading,
};
use cairn_inference::control::Phase;
use cairn_inference::metrics::EngineSummary;
use cairn_inference::served::{self, EngineMeasure};
use cairn_inference::{
    AcceleratorFamily, DeploymentId, Engine, InferenceProfile, InferenceProfileId, Inventory,
    MemoryPerMachine, Model, Server, ValidationError, WeightFile,
};
use cairn_models::types::catalog::Modality;
use serde::Serialize;

use super::inference::{Refusal, WriteError};
use super::inference_control::{self, LoadRow};
use super::inference_verify::VerificationRow;
use super::Orchestrator;
use crate::fleet::models::requests::{Request, RequestState};
use crate::fleet::models::settings::SettingOrigin;
use crate::fleet::models::{self, ModelsView, PinTarget, PoolView};
use crate::fleet::pools::PoolKey;

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
    /// The interconnect joining the pool's machines, as detection measured it.
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
    pub profile: String,
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
    /// What a reload of this deployment costs now, as its pool's windows
    /// weigh it ([`inference_control::reference_load`]).
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
    /// The profile a load runs: its deployment's on the pool, or the one it
    /// verifies first.
    pub profile: Option<String>,
    pub size_bytes: Option<u64>,
    /// Why it cannot load on this pool, in a few words; `None` when it can.
    pub blocked: Option<String>,
    /// No deployment of it is on the pool yet, but a container profile of it
    /// fits: a load verifies `profile` there first and keeps it loaded if it
    /// passes.
    pub verifies: bool,
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
    pub profiles: Vec<ModelProfile>,
    /// Why none of its deployments can run now, in a few words.
    pub blocked: Option<String>,
    /// Its profile generations, newest first.
    pub generations: Vec<super::inference_generate::GenerationView>,
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
pub struct ModelProfile {
    pub id: String,
    pub quantization: Option<String>,
    /// What runs it: the server of a container profile, else the engine kind.
    pub engine: String,
    pub machines: u32,
    /// authored, imported or generated.
    pub origin: String,
    /// How a generated profile was made.
    pub provenance: Option<ProfileProvenance>,
    pub in_use: bool,
    /// A container profile, which verification can measure on a pool.
    pub verifiable: bool,
    /// Verified before, but only as it was before an edit: those
    /// verifications no longer count, and a load verifies it first.
    pub edited: bool,
    /// A profile a generation owns, not promoted as it stands, that nothing
    /// works on now: a fresh generation may regenerate it in place.
    pub regenerable: bool,
    /// Its measured fit per pool; empty when nothing is measured yet.
    pub fit: Vec<ProfileFit>,
}

/// How a generated profile was made: the run that wrote it, the template it
/// started from, what it cited and what the operator answered.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileProvenance {
    pub run: Option<String>,
    pub starting_template: Option<String>,
    pub sources: Vec<cairn_inference::CitedSource>,
    pub answers: Vec<cairn_inference::OperatorAnswer>,
}

fn provenance(origin: &cairn_inference::ProfileOrigin) -> Option<ProfileProvenance> {
    match origin {
        cairn_inference::ProfileOrigin::Generated {
            run,
            starting_template,
            sources,
            answers,
            ..
        } => Some(ProfileProvenance {
            run: run.clone(),
            starting_template: starting_template.clone(),
            sources: sources.clone(),
            answers: answers.clone(),
        }),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileFit {
    pub pool: String,
    pub fits: bool,
    pub memory_bytes: Option<u64>,
    pub load_ms: Option<u64>,
    pub decode_tokens_per_sec: Option<f64>,
    pub failed: Option<String>,
    /// A verification on the pool is under way.
    pub verifying: bool,
    /// `memory_bytes` is predicted from what the profile runs: nothing
    /// measured it and the profile states none.
    pub predicted: bool,
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
    pub profile: String,
    pub engine: String,
    /// The container image, native artifact, or the description of a command
    /// profile.
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
    /// The GPUs a model runs on ([`model_gpus`]), named by the first.
    pub gpu: Option<String>,
    /// Their maker, which decides the profiles that suit it.
    pub family: Option<AcceleratorFamily>,
    /// Total and free bytes of the memory a model lives in.
    pub memory: Option<(u64, u64)>,
    /// What the machine's GPUs hold now.
    pub gpu_allocated_bytes: Option<u64>,
}

impl MachineFacts {
    pub(crate) fn of(snapshot: &ExecutorHealthSnapshot) -> Self {
        let devices = model_gpus(match &snapshot.host.gpus.reading {
            MeasurementReading::Measured { value } => value.as_slice(),
            MeasurementReading::Unavailable { .. } => &[],
        });
        let gpu = devices.first().map(|first| match devices.len() {
            1 => first.model.clone(),
            n => format!("{n}× {}", first.model),
        });
        let family = devices.first().map(|first| match first.vendor {
            GpuVendor::Nvidia => AcceleratorFamily::Nvidia,
            GpuVendor::Apple => AcceleratorFamily::Apple,
            GpuVendor::Amd => AcceleratorFamily::Amd,
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
            family,
            memory,
            gpu_allocated_bytes: allocated,
        }
    }
}

/// Everything the overview is assembled from.
pub(crate) struct Sources<'a> {
    /// The inventory as served: settings declarations laid over the profiles.
    pub inventory: &'a Inventory,
    pub phases: &'a BTreeMap<DeploymentId, Phase>,
    pub loads: &'a BTreeMap<DeploymentId, Vec<LoadRow>>,
    /// What placement holds and decides about local models.
    pub models: &'a ModelsView,
    pub machines: &'a BTreeMap<String, MachineFacts>,
    pub engines: &'a BTreeMap<DeploymentId, EngineSummary>,
    pub measures: &'a BTreeMap<DeploymentId, EngineMeasure>,
    /// The verifications, newest first, without those of a profile changed
    /// since they ran.
    pub verifications: &'a [VerificationRow],
    /// The profiles edited since they were verified: every verification of
    /// one ran against content it no longer has.
    pub edited: &'a BTreeSet<String>,
    /// Generated profiles no verification has promoted as they stand: never
    /// offered or loaded, whether or not a deployment of one is here.
    pub unpromoted: &'a BTreeSet<String>,
    /// Generated profiles a fresh generation may regenerate in place.
    pub regenerable: &'a BTreeSet<String>,
    pub now: i64,
}

fn since(now: i64, at: i64) -> u64 {
    u64::try_from(now.saturating_sub(at)).unwrap_or(0)
}

/// Whether two real names name one model: the join between a model record and
/// the profiles that serve it.
fn same_model(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

fn engine_label(engine: &Engine) -> String {
    match engine {
        Engine::Container(container) => match container.server {
            Server::Vllm => "vLLM",
            Server::LlamaCpp => "llama.cpp",
        }
        .to_string(),
        other => other.kind().to_string(),
    }
}

/// A model and the profiles that serve it, joined by real name. A profile
/// whose model has no record still makes a model of its own name.
struct ModelGroup<'a> {
    name: String,
    record: Option<&'a Model>,
    profiles: Vec<&'a InferenceProfile>,
}

impl Sources<'_> {
    fn model_of(&self, deployment: &DeploymentId) -> Option<&InferenceProfile> {
        let record = self.inventory.deployments.get(deployment)?;
        self.inventory.profiles.get(&record.profile)
    }

    fn waiting(&self, deployment: &DeploymentId) -> Vec<&Request> {
        self.models
            .requests
            .iter()
            .map(|view| &view.request)
            .filter(|r| &r.deployment == deployment && r.state == RequestState::Waiting)
            .collect()
    }

    fn reference_load_ms(&self, deployment: &DeploymentId) -> Option<u64> {
        inference_control::reference_load(self.loads.get(deployment)?)?.load_ms
    }

    /// The newest measured footprint, per machine.
    fn footprint(&self, deployment: &DeploymentId) -> BTreeMap<String, u64> {
        inference_control::last_footprint(self.loads.get(deployment))
    }

    /// The deployments placement may place on `pool`, and those resident there.
    fn deployments_of(&self, pool: &PoolView) -> Vec<DeploymentId> {
        let mut ids: BTreeSet<DeploymentId> = self
            .models
            .deployments
            .iter()
            .filter(|placement| placement.pool.as_ref() == Some(&pool.key))
            .map(|placement| placement.deployment.clone())
            .collect();
        ids.extend(pool.residents.iter().map(|r| r.deployment.clone()));
        ids.into_iter()
            .filter(|id| self.inventory.deployments.contains_key(id))
            .collect()
    }

    fn pool_of(&self, deployment: &DeploymentId) -> Option<&PoolView> {
        let key = self.models.placement(deployment)?.pool.as_ref()?;
        self.models.pools.iter().find(|pool| &pool.key == key)
    }

    fn running(&self, deployment: &DeploymentId) -> Option<RunningModel> {
        let profile = self.model_of(deployment)?;
        let resident = self.models.resident(deployment);
        let waiting = self.waiting(deployment);
        let expected_ms = self.reference_load_ms(deployment);
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
            Some(Phase::Failed { at_ms, reason })
                if resident.is_none() && self.now - at_ms < FAILED_SHOWN_MS =>
            {
                (RunningState::Failed, None, Some(reason.clone()))
            }
            _ => {
                // Placed and not started yet, or asked for and not placed
                // yet: loading from the earlier of the two.
                let first = resident
                    .map(|r| r.chosen_at)
                    .into_iter()
                    .chain(waiting.iter().map(|r| r.at))
                    .min()?;
                (RunningState::Loading, Some(loading(first)), None)
            }
        };
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
            model: profile.model.name.clone(),
            profile: profile.id.to_string(),
            state,
            pinned: resident.is_some_and(|r| r.pin.is_some()),
            loading,
            failed,
            rates,
        })
    }

    /// The smallest memory a machine of `pool` has, of those that report.
    fn smallest_machine(&self, pool: &PoolView) -> Option<u64> {
        pool.members
            .iter()
            .filter_map(|m| self.machines.get(m.machine.as_str())?.memory)
            .map(|(total, _)| total)
            .min()
    }

    /// What `deployment` takes: its whole size, the most it needs of one
    /// machine, and whether that is predicted rather than measured or stated
    /// ([`inference_control::need_of`]). The whole size is unknown while a
    /// machine whose share it is has not reported its memory.
    fn size(&self, deployment: &DeploymentId) -> (Option<u64>, Option<u64>, bool) {
        let Some(record) = self.inventory.deployments.get(deployment) else {
            return (None, None, false);
        };
        let need = inference_control::need_of(
            self.inventory,
            record,
            self.loads.get(deployment),
            |machine| Some(self.machines.get(machine)?.memory?.0),
        );
        let Ok(need) = need else {
            return (None, None, false);
        };
        let whole = (need.per_machine.len() == record.machines.len())
            .then(|| need.per_machine.values().sum());
        (
            whole,
            need.per_machine.values().max().copied(),
            need.predicted().is_some(),
        )
    }

    /// What `profile` declares it takes: its whole size and its size on one
    /// machine.
    fn declared_size(profile: &InferenceProfile) -> (Option<u64>, Option<u64>) {
        let per_machine = profile.requirements.memory.map(|memory| match memory {
            MemoryPerMachine::Unified { system } => system.0,
            MemoryPerMachine::Discrete { vram, .. } => vram.0,
        });
        (
            per_machine.map(|bytes| bytes * u64::from(profile.requirements.machines)),
            per_machine,
        )
    }

    fn fits(&self, pool: &PoolView, profile: &InferenceProfile, per_machine: Option<u64>) -> bool {
        if profile.requirements.machines as usize > pool.members.len() {
            return false;
        }
        match (per_machine, self.smallest_machine(pool)) {
            (Some(needs), Some(has)) => needs <= has,
            _ => true,
        }
    }

    fn pool_memory(&self, pool: &PoolView, running: &[RunningModel]) -> Option<PoolMemory> {
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
        for member in &pool.members {
            let name = member.machine.as_str();
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

    /// The newest verification that ran as `deployment`, when it is one's.
    fn verification_of(&self, deployment: &DeploymentId) -> Option<&VerificationRow> {
        self.verifications
            .iter()
            .find(|row| row.deployment == deployment.as_str())
    }

    fn model_groups(&self) -> Vec<ModelGroup<'_>> {
        let mut groups: BTreeMap<String, ModelGroup<'_>> = BTreeMap::new();
        for model in self.inventory.models.values() {
            let entry = groups
                .entry(model.name.trim().to_ascii_lowercase())
                .or_insert_with(|| ModelGroup {
                    name: model.name.trim().to_string(),
                    record: None,
                    profiles: Vec::new(),
                });
            entry.name = model.name.trim().to_string();
            entry.record = Some(model);
        }
        for profile in self.inventory.profiles.values() {
            groups
                .entry(profile.model.name.trim().to_ascii_lowercase())
                .or_insert_with(|| ModelGroup {
                    name: profile.model.name.trim().to_string(),
                    record: None,
                    profiles: Vec::new(),
                })
                .profiles
                .push(profile);
        }
        groups.into_values().collect()
    }

    /// Every model with a profile, as `pool`'s load menu offers it: those a
    /// deployment here can run first, then those that cannot, each with why.
    /// A pool busy with batch work still offers every model: a request stops
    /// new batches there, so loading is never withheld for it.
    fn loadable(
        &self,
        pool: &PoolView,
        deployments: &[DeploymentId],
        running: &[RunningModel],
        groups: &[ModelGroup<'_>],
    ) -> Vec<LoadableModel> {
        let mut offered: Vec<LoadableModel> = groups
            .iter()
            .filter(|group| !group.profiles.is_empty())
            // A model here, or on its way down, is not offered until it is down.
            .filter(|group| {
                !running
                    .iter()
                    .any(|r| r.state != RunningState::Failed && same_model(&r.model, &group.name))
            })
            .map(|group| self.offer(pool, deployments, group))
            .collect();
        offered.sort_by_key(|offer| (offer.blocked.is_some(), offer.verifies));
        offered
    }

    /// The hardware family of `pool`'s machines, from the first that reports.
    fn family(&self, pool: &PoolView) -> Option<AcceleratorFamily> {
        pool.members
            .iter()
            .find_map(|m| self.machines.get(m.machine.as_str())?.family)
    }

    /// The container profiles of `group` that suit `pool`'s machines, in the
    /// order verification tries them, each with whether its declared size
    /// fits the pool.
    fn verifiable_here<'s>(
        &'s self,
        pool: &PoolView,
        group: &ModelGroup<'_>,
    ) -> Vec<(&'s InferenceProfile, bool)> {
        super::inference_verify::candidates(
            self.inventory,
            &group.name,
            pool.members.len(),
            self.family(pool),
        )
        .iter()
        .filter(|id| !self.unpromoted.contains(id.as_str()))
        .filter_map(|id| self.inventory.profiles.get(id))
        .map(|profile| {
            let (_, per_machine) = Self::declared_size(profile);
            (profile, self.fits(pool, profile, per_machine))
        })
        .collect()
    }

    /// The profiles a load of `model` on `pool` verifies before it keeps the
    /// first that passes, in order; `None` when the load is a plain request
    /// (a deployment of it is here) or cannot load at all. The menu offers it
    /// as verifying first by the same rule.
    pub(crate) fn load_verifies(
        &self,
        pool: &PoolKey,
        model: &str,
    ) -> Option<Vec<InferenceProfileId>> {
        let pool = self.models.pools.iter().find(|p| &p.key == pool)?;
        let groups = self.model_groups();
        let group = groups.iter().find(|g| same_model(&g.name, model))?;
        if !self.offer(pool, &self.deployments_of(pool), group).verifies {
            return None;
        }
        Some(
            self.verifiable_here(pool, group)
                .into_iter()
                .filter(|(_, fits)| *fits)
                .map(|(profile, _)| profile.id.clone())
                .collect(),
        )
    }

    fn offer(
        &self,
        pool: &PoolView,
        deployments: &[DeploymentId],
        group: &ModelGroup<'_>,
    ) -> LoadableModel {
        let mut here: Vec<(&InferenceProfile, Option<u64>, Option<String>)> = deployments
            .iter()
            .filter_map(|id| Some((id, self.model_of(id)?)))
            .filter(|(_, profile)| same_model(&profile.model.name, &group.name))
            .filter(|(_, profile)| !self.unpromoted.contains(profile.id.as_str()))
            // A deployment of a profile edited since it was verified runs
            // content nothing has vouched for: a load verifies it first.
            .filter(|(_, profile)| !self.edited.contains(profile.id.as_str()))
            .map(|(id, profile)| {
                let (size, per_machine, _) = self.size(id);
                let blocked = if !self.fits(pool, profile, per_machine) {
                    Some("won't fit".to_string())
                } else {
                    match self.verification_of(id).map(|row| row.outcome.as_str()) {
                        Some("running") => Some("verifying".to_string()),
                        Some("passed") | None => None,
                        Some(_) => Some("failed verify".to_string()),
                    }
                };
                (profile, size, blocked)
            })
            .collect();
        here.sort_by_key(|(_, _, blocked)| blocked.is_some());
        match here.into_iter().next() {
            Some((profile, size, blocked)) => LoadableModel {
                model: group.name.clone(),
                profile: Some(profile.id.to_string()),
                size_bytes: size,
                blocked,
                verifies: false,
            },
            None => self.unverified(pool, group),
        }
    }

    /// How `pool` offers `group`, which has no deployment there: loadable,
    /// verifying first, when a container profile of it fits, else greyed with
    /// why.
    fn unverified(&self, pool: &PoolView, group: &ModelGroup<'_>) -> LoadableModel {
        let verifiable = self.verifiable_here(pool, group);
        let (profile, verifies) = match verifiable.iter().find(|(_, fits)| *fits) {
            Some((profile, _)) => (Some(*profile), true),
            None => (verifiable.first().map(|(profile, _)| *profile), false),
        };
        LoadableModel {
            model: group.name.clone(),
            profile: profile.map(|p| p.id.to_string()),
            size_bytes: profile.and_then(|p| Self::declared_size(p).0),
            blocked: match (verifies, profile) {
                (true, _) => None,
                (false, Some(_)) => Some("won't fit".to_string()),
                (false, None) => Some(self.not_here(pool, group)),
            },
            verifies,
        }
    }

    /// Why `group`, with no deployment on `pool` and no container profile that
    /// suits it, cannot load there, in a few words.
    fn not_here(&self, pool: &PoolView, group: &ModelGroup<'_>) -> String {
        let members: BTreeSet<&str> = pool.members.iter().map(|m| m.machine.as_str()).collect();
        // A gang reaching past this pool's machines: they are not linked.
        for placement in self
            .models
            .deployments
            .iter()
            .filter(|placement| same_model(&placement.model, &group.name))
        {
            let Some(record) = self.inventory.deployments.get(&placement.deployment) else {
                continue;
            };
            let machines: Vec<&str> = record.machines.iter().map(|m| m.as_str()).collect();
            if machines.len() > 1
                && machines.iter().any(|m| members.contains(m))
                && !machines.iter().all(|m| members.contains(m))
            {
                return format!("needs {} link", machines.join("+"));
            }
        }
        let size = pool.members.len() as u32;
        if let Some(fewest) = group
            .profiles
            .iter()
            .map(|profile| profile.requirements.machines)
            .min()
            .filter(|fewest| *fewest > size)
        {
            return format!("needs ×{fewest}");
        }
        "no deployment here".to_string()
    }

    /// Why none of `group`'s deployments can run now, in a few words; `None`
    /// when one can, one is resident, or it has none.
    fn model_blocked(&self, group: &ModelGroup<'_>) -> Option<String> {
        let placements: Vec<_> = self
            .models
            .deployments
            .iter()
            .filter(|placement| same_model(&placement.model, &group.name))
            .collect();
        if placements.iter().any(|placement| {
            placement.pool.is_some() || self.models.resident(&placement.deployment).is_some()
        }) {
            return None;
        }
        let placement = placements.first()?;
        let pooled = |machine: &str| {
            self.models
                .pools
                .iter()
                .any(|pool| pool.members.iter().any(|m| m.machine == machine))
        };
        let machines: Vec<&str> = self
            .inventory
            .deployments
            .get(&placement.deployment)
            .map(|record| record.machines.iter().map(|m| m.as_str()).collect())
            .unwrap_or_default();
        Some(match machines.iter().find(|machine| !pooled(machine)) {
            Some(machine) => format!("{machine} is in no pool"),
            None if machines.len() > 1 => format!("no link between {}", machines.join(" and ")),
            None => placement
                .refused
                .clone()
                .unwrap_or_else(|| "cannot run now".to_string()),
        })
    }

    /// A profile's fit per pool: the newest verification there, else what its
    /// deployment's loads measured, else nothing (not measured).
    fn profile_fit(&self, profile: &InferenceProfile) -> Vec<ProfileFit> {
        let mut fits: BTreeMap<String, ProfileFit> = BTreeMap::new();
        for row in self
            .verifications
            .iter()
            .filter(|row| row.profile == profile.id.as_str())
        {
            if fits.contains_key(&row.pool) {
                continue;
            }
            let passed = row.outcome == "passed";
            fits.insert(
                row.pool.clone(),
                ProfileFit {
                    pool: row.pool.clone(),
                    fits: true,
                    memory_bytes: row
                        .footprint
                        .as_ref()
                        .filter(|_| passed)
                        .map(|footprint| footprint.values().sum()),
                    load_ms: row
                        .load_ms
                        .filter(|_| passed)
                        .and_then(|ms| u64::try_from(ms).ok()),
                    decode_tokens_per_sec: row.decode_tps.or(row.embed_tps).filter(|_| passed),
                    failed: (row.outcome == "failed")
                        .then(|| row.reason.clone().unwrap_or_else(|| "failed".to_string())),
                    verifying: row.outcome == "running",
                    predicted: false,
                },
            );
        }
        for id in self
            .inventory
            .deployments
            .values()
            .filter(|d| d.profile == profile.id)
            .map(|d| &d.id)
        {
            let Some(pool) = self.pool_of(id) else {
                continue;
            };
            let key = pool.key.to_string();
            if fits.contains_key(&key) {
                continue;
            }
            let (size, per_machine, predicted) = self.size(id);
            let fits_here = self.fits(pool, profile, per_machine);
            let loads = self.loads.get(id).filter(|loads| !loads.is_empty());
            if loads.is_none() && fits_here && !predicted {
                continue;
            }
            fits.insert(
                key.clone(),
                ProfileFit {
                    pool: key,
                    fits: fits_here,
                    memory_bytes: size,
                    load_ms: self.reference_load_ms(id),
                    decode_tokens_per_sec: None,
                    failed: loads
                        .and_then(|loads| loads.first())
                        .filter(|load| !load.healthy)
                        .and_then(|load| load.reason.clone()),
                    verifying: false,
                    predicted,
                },
            );
        }
        fits.into_values().collect()
    }

    fn pool(&self, pool: &PoolView, groups: &[ModelGroup<'_>]) -> InferencePool {
        let deployments = self.deployments_of(pool);
        let running: Vec<RunningModel> = deployments
            .iter()
            .filter_map(|id| self.running(id))
            .collect();
        let loadable = self.loadable(pool, &deployments, &running, groups);
        let link = match (pool.members.as_slice(), pool.rate_gbps) {
            ([a, b], Some(gbps)) => Some(InferenceLink {
                machines: [a.machine.clone(), b.machine.clone()],
                bytes_per_sec: u64::from(gbps) * 1_000_000_000 / 8,
            }),
            _ => None,
        };
        InferencePool {
            id: pool.key.to_string(),
            machines: pool
                .members
                .iter()
                .map(|member| {
                    let facts = self.machines.get(member.machine.as_str());
                    InferenceMachine {
                        name: member.machine.clone(),
                        gpu: facts.and_then(|f| f.gpu.clone()),
                        memory_bytes: facts.and_then(|f| f.memory).map(|(total, _)| total),
                    }
                })
                .collect(),
            link,
            memory: self.pool_memory(pool, &running),
            running,
            warm: pool.warm_model.clone(),
            loadable,
            unload_idle: UnloadIdle {
                secs: pool.settings.idle.secs,
                auto: !matches!(pool.settings.idle.origin, SettingOrigin::Operator),
            },
        }
    }

    /// The models: each model record with the profiles that serve it, its
    /// facts read from the model (falling back to its first profile's), and
    /// each profile's fit per pool.
    fn model_list(
        &self,
        groups: &[ModelGroup<'_>],
        running: &[RunningModel],
    ) -> Vec<InferenceModel> {
        let in_use: BTreeSet<&str> = running
            .iter()
            .filter(|r| matches!(r.state, RunningState::Ready | RunningState::Loading))
            .map(|r| r.profile.as_str())
            .collect();
        groups
            .iter()
            .map(|group| {
                let facts = group.record.map(|model| &model.facts);
                let (inputs, outputs) = match facts.and_then(|f| f.modalities.as_ref()) {
                    Some(modalities) => (modalities.input.clone(), modalities.output.clone()),
                    None => match group.profiles.first() {
                        Some(first) => {
                            let modalities = served::modalities(first);
                            (modalities.input, modalities.output)
                        }
                        None => (vec![Modality::Text], vec![Modality::Text]),
                    },
                };
                let measured_context = group
                    .profiles
                    .iter()
                    .flat_map(|profile| {
                        self.inventory
                            .deployments
                            .values()
                            .filter(move |d| d.profile == profile.id)
                    })
                    .filter_map(|d| self.measures.get(&d.id)?.context_length())
                    .max();
                InferenceModel {
                    maker: group
                        .name
                        .split_once('/')
                        .map(|(maker, _)| maker.to_string()),
                    name: group.name.clone(),
                    facts: ModelFacts {
                        inputs,
                        outputs,
                        parameters: facts.and_then(|f| f.parameters),
                        context_tokens: facts.and_then(|f| f.context).or(measured_context),
                    },
                    profiles: group
                        .profiles
                        .iter()
                        .map(|profile| ModelProfile {
                            id: profile.id.to_string(),
                            quantization: profile.quantization.clone(),
                            engine: engine_label(&profile.engine),
                            machines: profile.requirements.machines,
                            origin: profile.origin.label().to_string(),
                            provenance: provenance(&profile.origin),
                            in_use: in_use.contains(profile.id.as_str()),
                            verifiable: matches!(profile.engine, Engine::Container(_)),
                            edited: self.edited.contains(profile.id.as_str()),
                            regenerable: self.regenerable.contains(profile.id.as_str()),
                            fit: self.profile_fit(profile),
                        })
                        .collect(),
                    blocked: self.model_blocked(group),
                    generations: Vec::new(),
                }
            })
            .collect()
    }

    pub(crate) fn overview(&self) -> InferenceOverview {
        let groups = self.model_groups();
        let pools: Vec<InferencePool> = self
            .models
            .pools
            .iter()
            .map(|pool| self.pool(pool, &groups))
            .collect();
        let running: Vec<RunningModel> = pools
            .iter()
            .flat_map(|pool| pool.running.iter().cloned())
            .collect();
        InferenceOverview {
            models: self.model_list(&groups, &running),
            pools,
        }
    }

    pub(crate) fn running_detail(&self, deployment: &DeploymentId) -> Option<RunningDetail> {
        let record = self.inventory.deployments.get(deployment)?;
        let profile = self.inventory.profiles.get(&record.profile)?;
        let measure = self.measures.get(deployment);
        let runs = match &profile.engine {
            Engine::Container(container) => Some(container.image.clone()),
            Engine::Native {
                artifact, version, ..
            } => Some(format!("{artifact} {version}")),
            Engine::Command { description, .. } => description.clone(),
        };
        Some(RunningDetail {
            deployment: deployment.to_string(),
            model: profile.model.name.clone(),
            metrics: self.engines.get(deployment).copied(),
            config: RunningConfig {
                profile: profile.id.to_string(),
                engine: profile.engine.kind().to_string(),
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
    let phases = inference_control::deployment_phases(db).await?;
    let loads = inference_control::load_history(db).await?;
    let models = models::view(orch).await?;
    let machines: BTreeMap<String, MachineFacts> = orch
        .fleet
        .executor_health(crate::fleet::unix_time_ms())
        .iter()
        .map(|snapshot| (snapshot.public_name.clone(), MachineFacts::of(snapshot)))
        .collect();
    let engines = super::inference_metrics::summaries();
    let measures = super::inference_route::measures();
    // Staleness is judged against the records as written: the served inventory
    // lays settings declarations over them, which would read as a change.
    let records = super::inference::load_inventory(db).await?;
    let (verifications, stale) = current_verifications(
        super::inference_verify::load_verifications(db).await?,
        &records,
    );
    let gate = super::inference_generate::promotion::Gate::load(db).await?;
    let unpromoted = gate.unpromoted_profiles(&records);
    let regenerable = gate.regenerable_profiles(&records);
    Ok(with(&Sources {
        inventory: &inventory,
        phases: &phases,
        loads: &loads,
        models: &models,
        machines: &machines,
        engines: &engines,
        measures: &measures,
        verifications: &verifications,
        edited: &stale,
        unpromoted: &unpromoted,
        regenerable: &regenerable,
        now: crate::fleet::unix_time_ms() as i64,
    }))
}

/// The verifications of each profile as its records are now, and the profiles
/// edited since they were verified: verified only as they were before.
fn current_verifications(
    rows: Vec<VerificationRow>,
    records: &Inventory,
) -> (Vec<VerificationRow>, BTreeSet<String>) {
    let (current, stale): (Vec<_>, Vec<_>) = rows.into_iter().partition(|row| !row.stale(records));
    let vouched: BTreeSet<&str> = current.iter().map(|row| row.profile.as_str()).collect();
    let edited = stale
        .into_iter()
        .map(|row| row.profile)
        .filter(|profile| {
            !vouched.contains(profile.as_str())
                && records
                    .profiles
                    .contains_key(&InferenceProfileId::from(profile.as_str()))
        })
        .collect();
    (current, edited)
}

pub async fn overview(orch: &Orchestrator) -> Result<InferenceOverview, String> {
    let mut overview = with_sources(orch, |sources| sources.overview()).await?;
    let generations = super::inference_generate::generation_views(orch).await?;
    for model in &mut overview.models {
        model.generations = generations
            .iter()
            .filter(|generation| generation.row.model == model.name)
            .cloned()
            .collect();
    }
    Ok(overview)
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
/// its idle clock starts. A model with no deployment on the pool whose
/// container profiles fit it is verified there first, in the background, and
/// the first profile that passes stays loaded for the operator as if the
/// request had loaded it.
pub async fn load(orch: &Orchestrator, pool: &str, model: &str) -> Result<String, String> {
    let key = PoolKey::from(pool);
    let verifies = with_sources(orch, |sources| sources.load_verifies(&key, model)).await?;
    if let Some(profiles) = verifies {
        let detected = super::inference_generate::detected_pool(orch, pool)?;
        let first = profiles
            .first()
            .map(ToString::to_string)
            .unwrap_or_default();
        let others = profiles.len().saturating_sub(1);
        let plan = super::inference_verify::spawn(
            orch,
            profiles,
            detected,
            OPERATOR,
            false,
            Some(OPERATOR.to_string()),
        )
        .await?;
        let (id, head) = (plan.id, plan.head);
        return Ok(format!(
            "{model} has no deployment on {pool} yet, so loading verifies profile {first} there first (verification {id}, reaching its head at {head}){}; the first that passes stays loaded",
            match others {
                0 => String::new(),
                1 => ", then 1 more profile if it fails".to_string(),
                n => format!(", then {n} more profiles in turn if it fails"),
            }
        ));
    }
    let handle = models::request(orch, model, None, OPERATOR, Some(&PoolKey::from(pool))).await?;
    let status = handle.status(orch).await?;
    Ok(format!("Request #{}: {}", handle.id, status.describe()))
}

/// Unload `deployment` now, whoever pinned it and whatever waits for it.
pub async fn unload(orch: &Orchestrator, deployment: &str) -> Result<String, String> {
    models::unload(orch, &DeploymentId::from(deployment), OPERATOR).await
}

/// Pin `deployment` against eviction, or unpin it. The operator outranks
/// another holder: a pin in its way is revoked rather than refusing.
pub async fn set_pinned(
    orch: &Orchestrator,
    deployment: &str,
    pinned: bool,
) -> Result<String, String> {
    let target = PinTarget::Deployment(DeploymentId::from(deployment));
    if pinned {
        return models::pin_outranking(orch, target, OPERATOR).await;
    }
    match models::release(orch, target, OPERATOR, true).await {
        Err(why) if why == models::NOTHING_PINNED => Ok(format!("{deployment} is not pinned")),
        said => said,
    }
}

/// Set the model `pool` places when idle, or none.
pub async fn set_warm(
    orch: &Orchestrator,
    pool: &str,
    model: Option<&str>,
) -> Result<String, String> {
    models::set_preference(
        orch,
        &PoolKey::from(pool),
        Some(model.map(str::to_string)),
        None,
    )
    .await
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
    models::set_preference(
        orch,
        &PoolKey::from(pool),
        None,
        Some(minutes.map(|m| m * 60)),
    )
    .await
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

/// Register a model by its Hugging Face repository id, reading its facts, as
/// `append cairn://inference/models {repo}` does.
pub async fn register_model(orch: &Orchestrator, repo: &str) -> Result<String, String> {
    let hub = super::inference_models::Web::new()?;
    super::inference_models::register(&orch.db.local, &hub, repo.trim(), None).await
}

/// A started generation as the page needs it: its record's id and the
/// ordinary issue it runs in, which the page opens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartedGeneration {
    pub generation_id: String,
    pub issue_uri: String,
}

/// Start generating a profile for `model` on `pool`: one agent on an ordinary
/// issue filed in `project_id`, the project Settings has open. Every machine's
/// hardware is its executor's report; a pool with a machine that reports none
/// is refused here, before any issue is filed, naming the machine to attach.
/// `replacement` names the existing profile the generation regenerates in
/// place; only an operator's invoke reaches this.
pub async fn start_generation(
    orch: &Orchestrator,
    project_id: &str,
    model: &str,
    pool: &str,
    replacement: Option<&str>,
) -> Result<StartedGeneration, String> {
    let launched =
        super::inference_generate::launch(orch, project_id, model, pool, replacement).await?;
    Ok(StartedGeneration {
        generation_id: launched.generation.id,
        issue_uri: launched.issue_uri,
    })
}

/// One profile's record YAML, for its editor.
pub async fn profile_yaml(orch: &Orchestrator, profile: &str) -> Result<String, String> {
    super::inference::profile_yaml(&orch.db.local, profile).await
}

/// One reason a profile's save was refused, at the editor field it is about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileProblem {
    /// `id`, `image`, `args`, `env`, `weights`, `weights.<n>`, or `yaml` for
    /// the rest of the profile; `None` when it is about another record.
    pub field: Option<String>,
    pub message: String,
}

/// How a profile's save went: written, or every problem at its field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedProfile {
    pub message: String,
    /// Empty once it is written.
    pub problems: Vec<ProfileProblem>,
}

/// The editor field one refusal of a profile's save is about.
fn problem_field(
    refusal: &Refusal,
    profile: Option<&InferenceProfile>,
    replacing: Option<&str>,
) -> Option<String> {
    use ValidationError as V;
    let problem = match refusal {
        Refusal::Unparsed(_) => return Some("yaml".into()),
        Refusal::Taken(_) => return Some("id".into()),
        Refusal::Said(_) => return None,
        Refusal::Invalid(problem) => problem,
    };
    let about = match problem {
        // A deployment left naming the profile's old id: the rename did it.
        V::UnknownProfile { profile: id, .. } => {
            return (replacing == Some(id.as_str())).then(|| "id".into());
        }
        V::ProfileZeroMachines { profile }
        | V::UnpinnedWeights { profile }
        | V::BadWeightFile { profile, .. }
        | V::BadImageDigest { profile, .. }
        | V::BadArtifactSha { profile }
        | V::EmptyStart { profile }
        | V::UnknownPlaceholder { profile, .. }
        | V::RanksMismatch { profile, .. }
        | V::UnpinnedSourceMount { profile, .. }
        | V::BadSourceCommit { profile, .. }
        | V::ProfileModelUnregistered { profile, .. }
        | V::MachineCount { profile, .. } => profile,
        _ => return None,
    };
    let profile = profile.filter(|profile| &profile.id == about)?;
    Some(match problem {
        V::BadImageDigest { .. } => "image".into(),
        V::UnpinnedWeights { .. } => "weights".into(),
        V::BadWeightFile { index, .. } => format!("weights.{index}"),
        V::UnknownPlaceholder { placeholder, .. } => {
            let braced = format!("{{{placeholder}}}");
            match &profile.engine {
                Engine::Container(container)
                    if container.args.iter().any(|arg| arg.contains(&braced)) =>
                {
                    "args".into()
                }
                Engine::Container(container)
                    if container.env.values().any(|value| value.contains(&braced)) =>
                {
                    "env".into()
                }
                _ => "yaml".into(),
            }
        }
        _ => "yaml".into(),
    })
}

/// Every refusal of a profile's save, each at its field. A problem placed at
/// a field drops the `profile <id> ` its sentence opens with, which the
/// field already says.
fn profile_problems(
    refusals: &[Refusal],
    yaml: &str,
    replacing: Option<&str>,
) -> Vec<ProfileProblem> {
    let profile: Option<InferenceProfile> = serde_yaml::from_str(yaml).ok();
    refusals
        .iter()
        .map(|refusal| {
            let field = problem_field(refusal, profile.as_ref(), replacing);
            let mut message = refusal.to_string();
            if let (Some(_), Some(profile)) = (&field, &profile) {
                if let Some(rest) = message.strip_prefix(&format!("profile {} ", profile.id)) {
                    message = rest.to_string();
                }
            }
            ProfileProblem { field, message }
        })
        .collect()
}

/// Save a profile from its editor: its record YAML in place of `replacing`
/// (its id before the edit), or as a new profile when that is `None`. It is
/// validated with the whole inventory as every `cairn://inference` write is,
/// and a refusal is an answer naming each problem at its field, not an error.
pub async fn save_profile(
    orch: &Orchestrator,
    replacing: Option<&str>,
    yaml: &str,
) -> Result<SavedProfile, String> {
    match super::inference::save_profile(&orch.db.local, replacing, yaml).await {
        Ok(message) => Ok(SavedProfile {
            message,
            problems: Vec::new(),
        }),
        Err(WriteError::Refused(refusals)) => Ok(SavedProfile {
            message: "Refused; nothing was written.".into(),
            problems: profile_problems(&refusals, yaml, replacing),
        }),
        Err(WriteError::Failed(error)) => Err(error),
    }
}

/// Remove a profile and its deployments, refused while one of them runs.
pub async fn delete_profile(orch: &Orchestrator, profile: &str) -> Result<String, String> {
    Ok(super::inference::delete_profile(&orch.db.local, profile).await?)
}

/// A weight file as Hugging Face lists it, pinned: its revision's sha, its
/// sha256 and its size.
pub async fn weight_file(repo: &str, revision: &str, path: &str) -> Result<WeightFile, String> {
    let hub = super::inference_models::Web::new()?;
    super::inference_models::weight_file(&hub, repo.trim(), revision.trim(), path.trim()).await
}

/// Verify `profile` on `pool` in the background, as
/// `patch cairn://inference/profiles/<id> {verify}` does.
pub async fn verify_profile(
    orch: &Orchestrator,
    profile: &str,
    pool: &str,
) -> Result<String, String> {
    let detected = super::inference_generate::detected_pool(orch, pool)?;
    let plan = super::inference_verify::spawn(
        orch,
        vec![InferenceProfileId::from(profile)],
        detected,
        OPERATOR,
        false,
        None,
    )
    .await?;
    Ok(format!(
        "Verifying {profile} on {pool} (verification {}), reaching its head at {}",
        plan.id, plan.head
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::models::rules::{Pin, Why};
    use crate::fleet::models::settings::{EffectiveSettings, MeasuredLoad};
    use crate::fleet::models::{DeploymentPlacement, RequestView, ResidentView};
    use crate::fleet::pools::PoolMember;

    const GIB: u64 = 1 << 30;
    const DSV4: &str = "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp";
    const QWEN: &str = "Qwen/Qwen3.6-35B-A3B";
    const EMBED: &str = "Qwen/Qwen3-Embedding-4B";
    const SPARKS: &str = "sp1+sp2";

    fn inventory() -> Inventory {
        Inventory::from_yaml(
            r#"
models:
  - {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
  - {name: Qwen/Qwen3.6-35B-A3B}
  - {name: Qwen/Qwen3-Embedding-4B}
profiles:
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
  - {id: dsv4f-tp2, profile: dsv4f-tp2, machines: [sp1, sp2], head: sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen36-fast, profile: qwen36-fast, machines: [sp1], head: sp1, apiPort: 8000, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, profile: qwen3-embed-4b, machines: [fedora], head: fedora, apiPort: 8090, apiPath: /v1, healthPath: /health}
"#,
        )
        .unwrap()
    }

    fn pool_view(machines: &[&str], warm: &str) -> PoolView {
        PoolView {
            key: PoolKey::of(machines.iter().copied()),
            members: machines
                .iter()
                .map(|machine| PoolMember {
                    machine: machine.to_string(),
                    link_address: None,
                })
                .collect(),
            rate_gbps: None,
            settings: EffectiveSettings::derive(None, None),
            warm_model: Some(warm.to_string()),
            idle_override_secs: None,
            idle_since: None,
            residents: Vec::new(),
            models: Vec::new(),
        }
    }

    fn placed(deployment: &str, model: &str, pool: &str) -> DeploymentPlacement {
        DeploymentPlacement {
            deployment: deployment.into(),
            model: model.into(),
            pool: Some(PoolKey::from(pool)),
            refused: None,
        }
    }

    /// The two pools placement detects for the inventory: the Sparks joined by
    /// their link, and the Fedora box alone.
    fn placement() -> ModelsView {
        ModelsView {
            pools: vec![
                pool_view(&["sp1", "sp2"], DSV4),
                pool_view(&["fedora"], EMBED),
            ],
            deployments: vec![
                placed("dsv4f-tp2", DSV4, SPARKS),
                placed("qwen36-fast", QWEN, SPARKS),
                placed("qwen3-embed-4b", EMBED, "fedora"),
            ],
            ..ModelsView::default()
        }
    }

    fn resident(deployment: &str, model: &str, chosen_at: i64) -> ResidentView {
        ResidentView {
            deployment: deployment.into(),
            model: model.into(),
            pool: SPARKS.into(),
            machines: ["sp1".to_string(), "sp2".to_string()].into(),
            phase: None,
            why: Why::Adopted,
            why_said: String::new(),
            chosen_at,
            loading_since: None,
            healthy_since: None,
            hold: None,
            pin: None,
            footprint: BTreeMap::new(),
            unified: true,
        }
    }

    fn spark(allocated: Option<u64>) -> MachineFacts {
        MachineFacts {
            gpu: Some("NVIDIA GB10".into()),
            family: Some(AcceleratorFamily::Nvidia),
            memory: Some((122 * GIB, 20 * GIB)),
            gpu_allocated_bytes: allocated,
        }
    }

    fn healthy_load(ms: u64, footprint: &[(&str, u64)]) -> LoadRow {
        LoadRow {
            profile: String::new(),
            requested_at: 0,
            sent_at: Some(0),
            finished_at: 0,
            healthy: true,
            load_ms: Some(ms),
            reason: None,
            footprint: Some(footprint.iter().map(|(m, b)| (m.to_string(), *b)).collect()),
            cold: false,
        }
    }

    fn waiting(id: i64, deployment: &str, at: i64) -> RequestView {
        RequestView {
            request: Request {
                id,
                pool: SPARKS.into(),
                deployment: deployment.into(),
                model: DSV4.into(),
                requester: "operator".into(),
                at,
                state: RequestState::Waiting,
            },
            wait: None,
            said: String::new(),
        }
    }

    struct World {
        inventory: Inventory,
        phases: BTreeMap<DeploymentId, Phase>,
        loads: BTreeMap<DeploymentId, Vec<LoadRow>>,
        models: ModelsView,
        machines: BTreeMap<String, MachineFacts>,
        engines: BTreeMap<DeploymentId, EngineSummary>,
        measures: BTreeMap<DeploymentId, EngineMeasure>,
        verifications: Vec<VerificationRow>,
        edited: BTreeSet<String>,
        unpromoted: BTreeSet<String>,
        regenerable: BTreeSet<String>,
    }

    impl World {
        fn new() -> Self {
            Self {
                inventory: inventory(),
                phases: BTreeMap::new(),
                loads: BTreeMap::new(),
                models: placement(),
                machines: BTreeMap::new(),
                engines: BTreeMap::new(),
                measures: BTreeMap::new(),
                verifications: Vec::new(),
                edited: BTreeSet::new(),
                unpromoted: BTreeSet::new(),
                regenerable: BTreeSet::new(),
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
                models: &self.models,
                machines: &self.machines,
                engines: &self.engines,
                measures: &self.measures,
                verifications: &self.verifications,
                edited: &self.edited,
                unpromoted: &self.unpromoted,
                regenerable: &self.regenerable,
                now: 600_000,
            }
        }

        fn sparks(&mut self) -> &mut PoolView {
            &mut self.models.pools[0]
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
        let sparks = pool(&overview, SPARKS);
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
        // The running model is not offered for loading. The other one deployed
        // here declares 200 GB against 122 GiB machines, and the embedder has
        // no deployment here: both are listed, each with why.
        assert_eq!(
            sparks
                .loadable
                .iter()
                .map(|l| (l.model.as_str(), l.blocked.as_deref()))
                .collect::<Vec<_>>(),
            [
                (EMBED, Some("no deployment here")),
                (QWEN, Some("won't fit"))
            ]
        );
    }

    #[test]
    fn a_pool_shows_the_link_detection_measured_between_its_machines() {
        let mut world = World::new();
        assert_eq!(pool(&world.overview(), SPARKS).link, None);
        world.sparks().rate_gbps = Some(200);
        assert_eq!(
            pool(&world.overview(), SPARKS).link,
            Some(InferenceLink {
                machines: ["sp1".into(), "sp2".into()],
                bytes_per_sec: 25_000_000_000,
            })
        );
        assert_eq!(pool(&world.overview(), "fedora").link, None);
    }

    #[test]
    fn discrete_vram_is_the_pool_memory_and_no_reading_draws_nothing() {
        let mut world = World::new();
        world.machines.insert(
            "fedora".into(),
            MachineFacts {
                gpu: Some("RTX 4090".into()),
                family: Some(AcceleratorFamily::Nvidia),
                memory: Some((24 * GIB, 20 * GIB)),
                gpu_allocated_bytes: Some(4 * GIB),
            },
        );
        let overview = world.overview();
        // Nothing runs on the Fedora box, so the 4 GiB in use is not a model's.
        assert_eq!(
            pool(&overview, "fedora").memory,
            Some(PoolMemory {
                total_bytes: 24 * GIB,
                model_bytes: 0,
                other_bytes: 4 * GIB,
                free_bytes: 20 * GIB,
            })
        );
        assert_eq!(pool(&overview, SPARKS).memory, None);
        assert_eq!(pool(&overview, SPARKS).machines[0].memory_bytes, None);
    }

    #[test]
    fn a_loading_model_says_how_long_against_its_latest_load_and_who_waits() {
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
        world.models.requests = vec![
            waiting(1, "dsv4f-tp2", 410_000),
            waiting(2, "dsv4f-tp2", 415_000),
        ];
        let overview = world.overview();
        let running = &pool(&overview, SPARKS).running[0];
        assert_eq!(running.state, RunningState::Loading);
        assert_eq!(
            running.loading,
            Some(Loading {
                elapsed_ms: 180_000,
                expected_ms: Some(300_000),
                waiting: 2,
            }),
            "the newer load, not the slower"
        );
    }

    #[test]
    fn a_request_not_yet_started_already_reads_as_loading() {
        let mut world = World::new();
        world.models.requests = vec![waiting(1, "qwen36-fast", 540_000)];
        let overview = world.overview();
        let running = &pool(&overview, SPARKS).running[0];
        assert_eq!(running.model, QWEN);
        assert_eq!(running.loading.unwrap().elapsed_ms, 60_000);
        assert_eq!(running.loading.unwrap().waiting, 1);
    }

    #[test]
    fn a_resident_placed_and_not_yet_started_reads_as_loading_since_it_was_chosen() {
        let mut world = World::new();
        world.sparks().residents = vec![ResidentView {
            why: Why::Warm { idle_since: 0 },
            ..resident("dsv4f-tp2", DSV4, 500_000)
        }];
        let overview = world.overview();
        let running = &pool(&overview, SPARKS).running[0];
        assert_eq!(running.model, DSV4);
        assert_eq!(running.state, RunningState::Loading);
        assert_eq!(running.loading.unwrap().elapsed_ms, 100_000);
        assert_eq!(running.loading.unwrap().waiting, 0);
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
        world.sparks().residents = vec![ResidentView {
            pin: Some(Pin {
                holder: OPERATOR.into(),
                at: 0,
            }),
            ..resident("dsv4f-tp2", DSV4, 0)
        }];
        assert!(pool(&world.overview(), SPARKS).running[0].pinned);
    }

    #[test]
    fn a_profile_bigger_than_a_machine_will_not_fit() {
        let mut world = World::new();
        world.machines.insert("sp1".into(), spark(None));
        world.machines.insert("sp2".into(), spark(None));
        let overview = world.overview();
        let sparks = pool(&overview, SPARKS);
        let qwen = sparks.loadable.iter().find(|l| l.model == QWEN).unwrap();
        // 200 GB declared against a 122 GiB machine.
        assert_eq!(qwen.blocked.as_deref(), Some("won't fit"));
        assert_eq!(qwen.size_bytes, Some(200_000_000_000));
        let deepseek = sparks.loadable.iter().find(|l| l.model == DSV4).unwrap();
        assert_eq!(deepseek.blocked, None);
        assert_eq!(deepseek.profile.as_deref(), Some("dsv4f-tp2"));
        // Stating no memory, it is predicted at vLLM's default share of each
        // Spark (its description names vLLM).
        assert_eq!(
            deepseek.size_bytes,
            Some(2 * ((122 * GIB) as f64 * 0.9) as u64)
        );
        let model = overview.models.iter().find(|m| m.name == QWEN).unwrap();
        assert!(!model.profiles[0].fit[0].fits);
        assert_eq!(model.profiles[0].fit[0].pool, SPARKS);
    }

    #[test]
    fn unload_idle_is_placement_s_window_and_says_whether_the_operator_set_it() {
        let mut world = World::new();
        assert_eq!(
            pool(&world.overview(), SPARKS).unload_idle,
            UnloadIdle {
                secs: 30 * 60,
                auto: true
            }
        );
        // 6 × 2 min is under the 15-minute floor.
        let two_minutes = Some(MeasuredLoad {
            secs: 120,
            cold: false,
        });
        world.sparks().settings = EffectiveSettings::derive(None, two_minutes);
        assert_eq!(
            pool(&world.overview(), SPARKS).unload_idle,
            UnloadIdle {
                secs: 15 * 60,
                auto: true
            }
        );
        world.sparks().settings = EffectiveSettings::derive(Some(600), two_minutes);
        assert_eq!(
            pool(&world.overview(), SPARKS).unload_idle,
            UnloadIdle {
                secs: 600,
                auto: false
            }
        );
    }

    #[test]
    fn models_group_profiles_by_real_name_with_facts_and_measured_fit() {
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
        assert_eq!(names, [DSV4, EMBED, QWEN]);
        let deepseek = &overview.models[0];
        assert_eq!(deepseek.maker.as_deref(), Some("deepseek-ai"));
        assert_eq!(deepseek.facts.inputs, [Modality::Text, Modality::Image]);
        assert_eq!(deepseek.facts.context_tokens, Some(1_048_576));
        let profile = &deepseek.profiles[0];
        assert_eq!(profile.machines, 2);
        assert_eq!(profile.engine, "command");
        assert_eq!(profile.origin, "authored");
        assert!(!profile.verifiable);
        assert!(!profile.in_use);
        assert_eq!(
            profile.fit,
            [ProfileFit {
                pool: SPARKS.into(),
                fits: true,
                memory_bytes: Some(200 * GIB),
                load_ms: Some(420_000),
                decode_tokens_per_sec: None,
                failed: None,
                verifying: false,
                predicted: false,
            }]
        );
        let embedder = &overview.models[1];
        assert_eq!(embedder.facts.outputs, [Modality::Embeddings]);
        // Nothing measured on its pool: no fit, which the page says as such.
        assert_eq!(embedder.profiles[0].fit, []);
    }

    fn verification(profile: &str, pool: &str, outcome: &str) -> VerificationRow {
        VerificationRow {
            id: format!("{profile}-{pool}-{outcome}"),
            profile: profile.into(),
            profile_digest: String::new(),
            pool: pool.into(),
            deployment: format!("verify-{profile}-{pool}"),
            machines: Vec::new(),
            requested_by: OPERATOR.into(),
            started_at: 0,
            finished_at: None,
            outcome: outcome.into(),
            stage: String::new(),
            reason: (outcome == "failed").then(|| "out of memory".to_string()),
            load_ms: Some(300_000),
            footprint: Some(BTreeMap::from([("fedora".to_string(), 9 * GIB)])),
            context_tokens: None,
            kv_tokens: None,
            ttft_ms: None,
            decode_tps: None,
            prefill_tps: None,
            embed_tps: Some(2_400.0),
            engine_version: None,
        }
    }

    #[test]
    fn a_registered_model_shows_with_no_profile_and_its_facts_from_the_record() {
        let mut world = World::new();
        let mut fresh = Model::named("org/Fresh-7B");
        fresh.facts.parameters = Some(7_000_000_000);
        fresh.facts.context = Some(32_768);
        world.inventory.insert_model(fresh);
        let overview = world.overview();
        let fresh = overview
            .models
            .iter()
            .find(|m| m.name == "org/Fresh-7B")
            .unwrap();
        assert!(fresh.profiles.is_empty());
        assert_eq!(fresh.facts.parameters, Some(7_000_000_000));
        assert_eq!(fresh.facts.context_tokens, Some(32_768));
        assert_eq!(fresh.blocked, None);
        // No profile, so no pool offers it for loading.
        assert!(overview
            .pools
            .iter()
            .all(|pool| pool.loadable.iter().all(|l| l.model != "org/Fresh-7B")));
    }

    #[test]
    fn a_profile_s_fit_is_its_newest_verification_per_pool() {
        let mut world = World::new();
        world.verifications = vec![
            verification("qwen3-embed-4b", "fedora", "passed"),
            verification("qwen3-embed-4b", "fedora", "failed"),
            verification("qwen3-embed-4b", SPARKS, "running"),
        ];
        let overview = world.overview();
        let embedder = overview.models.iter().find(|m| m.name == EMBED).unwrap();
        assert_eq!(
            embedder.profiles[0].fit,
            [
                ProfileFit {
                    pool: "fedora".into(),
                    fits: true,
                    memory_bytes: Some(9 * GIB),
                    load_ms: Some(300_000),
                    decode_tokens_per_sec: Some(2_400.0),
                    failed: None,
                    verifying: false,
                    predicted: false,
                },
                ProfileFit {
                    pool: SPARKS.into(),
                    fits: true,
                    memory_bytes: None,
                    load_ms: None,
                    decode_tokens_per_sec: None,
                    failed: None,
                    verifying: true,
                    predicted: false,
                },
            ]
        );
    }

    #[test]
    fn every_pool_offers_every_model_and_greys_only_what_cannot_run_there() {
        let mut world = World::new();
        // The Sparks without their link: each is a pool of its own, and the
        // two-machine deployment has nowhere to run.
        world.models.pools = vec![
            pool_view(&["sp1"], QWEN),
            pool_view(&["sp2"], QWEN),
            pool_view(&["fedora"], EMBED),
        ];
        world.models.deployments = vec![
            DeploymentPlacement {
                refused: Some("no fast link joins sp1 and sp2, so they are not one pool".into()),
                pool: None,
                ..placed("dsv4f-tp2", DSV4, SPARKS)
            },
            placed("qwen36-fast", QWEN, "sp1"),
            placed("qwen3-embed-4b", EMBED, "fedora"),
        ];
        // Batch work on sp1 does not withhold loading there.
        world.models.pools[0].idle_since = Some(599_000);
        let overview = world.overview();
        let offered = |id: &str| {
            pool(&overview, id)
                .loadable
                .iter()
                .map(|l| (l.model.clone(), l.blocked.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            offered("sp1"),
            [
                (QWEN.to_string(), None),
                (DSV4.to_string(), Some("needs sp1+sp2 link".to_string())),
                (EMBED.to_string(), Some("no deployment here".to_string())),
            ]
        );
        assert_eq!(
            offered("fedora"),
            [
                (EMBED.to_string(), None),
                (DSV4.to_string(), Some("needs ×2".to_string())),
                (QWEN.to_string(), Some("no deployment here".to_string())),
            ]
        );
        let deepseek = overview.models.iter().find(|m| m.name == DSV4).unwrap();
        assert_eq!(
            deepseek.blocked.as_deref(),
            Some("no link between sp1 and sp2")
        );
        let qwen = overview.models.iter().find(|m| m.name == QWEN).unwrap();
        assert_eq!(qwen.blocked, None);
    }

    #[test]
    fn a_verification_s_deployment_loads_only_once_it_passed() {
        let mut world = World::new();
        world
            .inventory
            .insert_deployment(cairn_inference::Deployment {
                id: "verify-qwen3-embed-4b-sp1+sp2".into(),
                source: cairn_inference::RecordSource::Cairn,
                profile: "qwen3-embed-4b".into(),
                head: "sp1".into(),
                machines: vec!["sp1".into()],
                api_port: 8090,
                api_path: "/v1".into(),
                health_path: "/v1/models".into(),
            });
        world
            .models
            .deployments
            .push(placed("verify-qwen3-embed-4b-sp1+sp2", EMBED, SPARKS));
        world.verifications = vec![verification("qwen3-embed-4b", SPARKS, "failed")];
        let blocked = |world: &World| {
            pool(&world.overview(), SPARKS)
                .loadable
                .iter()
                .find(|l| l.model == EMBED)
                .unwrap()
                .blocked
                .clone()
        };
        assert_eq!(blocked(&world).as_deref(), Some("failed verify"));
        world.verifications = vec![verification("qwen3-embed-4b", SPARKS, "running")];
        assert_eq!(blocked(&world).as_deref(), Some("verifying"));
        world.verifications = vec![verification("qwen3-embed-4b", SPARKS, "passed")];
        assert_eq!(blocked(&world), None);
    }

    /// A llama.cpp profile of the embedder declaring `bytes` of unified memory
    /// on its one machine.
    fn embed_container(bytes: u64) -> InferenceProfile {
        Inventory::from_yaml(&format!(
            r#"
models:
  - {{name: Qwen/Qwen3-Embedding-4B}}
profiles:
  - id: qwen3-embed-4b-gguf
    model:
      name: Qwen/Qwen3-Embedding-4B
      weights: [{{repo: Qwen/Qwen3-Embedding-4B-GGUF, revision: f4602530db1d980e16da9d7d3a70294cf5c190be, path: Qwen3-Embedding-4B-Q8_0.gguf, sha256: b60ae5ce2dd6a0b77f82cadf21def1f310a3e10cde380ad0081b07a9d416949d}}]
    servedName: Qwen/Qwen3-Embedding-4B
    verbs: [embed]
    quantization: q8_0
    engine:
      kind: container
      image: ghcr.io/ggml-org/llama.cpp@sha256:afdc5265d1cfc729ad3ebaac59fd60e5d15c3b7aef455d7dc16bc1dccbc4dc64
      server: llamaCpp
      args: [-m, "{{weights}}", --port, "{{port}}"]
    requirements: {{machines: 1, memory: {{kind: unified, system: {bytes}}}}}
"#
        ))
        .unwrap()
        .profiles
        .into_values()
        .next()
        .unwrap()
    }

    /// The Sparks reporting their memory, with the embedder's container
    /// profile declaring `bytes`.
    fn sparks_with_embed_container(bytes: u64) -> World {
        let mut world = World::new();
        world.machines.insert("sp1".into(), spark(None));
        world.machines.insert("sp2".into(), spark(None));
        world.inventory.insert_profile(embed_container(bytes));
        world
    }

    #[test]
    fn a_fitting_container_profile_with_no_deployment_here_loads_by_verifying_first() {
        let world = sparks_with_embed_container(10 * GIB);
        let overview = world.overview();
        let sparks = pool(&overview, SPARKS);
        assert_eq!(
            sparks
                .loadable
                .iter()
                .map(|l| (l.model.as_str(), l.blocked.as_deref(), l.verifies))
                .collect::<Vec<_>>(),
            [
                (DSV4, None, false),
                (EMBED, None, true),
                (QWEN, Some("won't fit"), false)
            ],
            "what loads at once first, then what verifies first, then what cannot"
        );
        assert_eq!(
            sparks.loadable[1],
            LoadableModel {
                model: EMBED.into(),
                profile: Some("qwen3-embed-4b-gguf".into()),
                size_bytes: Some(10 * GIB),
                blocked: None,
                verifies: true,
            }
        );
        let sources = world.sources();
        assert_eq!(
            sources.load_verifies(&PoolKey::from(SPARKS), EMBED),
            Some(vec![InferenceProfileId::from("qwen3-embed-4b-gguf")])
        );
        // Deployed here already: a load is a plain request.
        assert_eq!(sources.load_verifies(&PoolKey::from(SPARKS), DSV4), None);
        let fedora = pool(&overview, "fedora");
        let embed = fedora.loadable.iter().find(|l| l.model == EMBED).unwrap();
        assert_eq!(
            (embed.profile.as_deref(), embed.verifies),
            (Some("qwen3-embed-4b"), false)
        );
        assert_eq!(sources.load_verifies(&PoolKey::from("fedora"), EMBED), None);
        // A model with only command profiles has nothing to verify.
        let qwen = fedora.loadable.iter().find(|l| l.model == QWEN).unwrap();
        assert_eq!(
            (qwen.blocked.as_deref(), qwen.verifies),
            (Some("no deployment here"), false)
        );
        assert_eq!(sources.load_verifies(&PoolKey::from("fedora"), QWEN), None);
    }

    #[test]
    fn an_unpromoted_generated_profile_is_neither_offered_nor_loaded() {
        let mut world = sparks_with_embed_container(10 * GIB);
        world.unpromoted.insert("qwen3-embed-4b-gguf".into());
        world.regenerable.insert("qwen3-embed-4b-gguf".into());
        let overview = world.overview();
        let regenerable: Vec<(&str, bool)> = overview
            .models
            .iter()
            .flat_map(|model| &model.profiles)
            .map(|profile| (profile.id.as_str(), profile.regenerable))
            .collect();
        assert!(regenerable.contains(&("qwen3-embed-4b-gguf", true)));
        assert!(regenerable
            .iter()
            .all(|(id, regenerable)| *regenerable == (*id == "qwen3-embed-4b-gguf")));
        let embed = pool(&overview, SPARKS)
            .loadable
            .iter()
            .find(|l| l.model == EMBED)
            .unwrap();
        assert_ne!(embed.profile.as_deref(), Some("qwen3-embed-4b-gguf"));
        assert!(!embed.verifies);
        assert_eq!(
            world.sources().load_verifies(&PoolKey::from(SPARKS), EMBED),
            None
        );
    }

    #[test]
    fn a_container_profile_too_big_for_the_pool_stays_greyed() {
        let world = sparks_with_embed_container(200 * GIB);
        let overview = world.overview();
        let embed = pool(&overview, SPARKS)
            .loadable
            .iter()
            .find(|l| l.model == EMBED)
            .unwrap();
        assert_eq!(
            (embed.blocked.as_deref(), embed.verifies, embed.size_bytes),
            (Some("won't fit"), false, Some(200 * GIB))
        );
        assert_eq!(
            world.sources().load_verifies(&PoolKey::from(SPARKS), EMBED),
            None
        );
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

    #[test]
    fn a_profile_edited_since_it_was_verified_says_so_and_loads_by_verifying_first() {
        const GGUF: &str = "qwen3-embed-4b-gguf";
        const VERIFY: &str = "verify-qwen3-embed-4b-gguf-sp1+sp2";
        let mut world = sparks_with_embed_container(10 * GIB);
        world
            .inventory
            .insert_deployment(cairn_inference::Deployment {
                id: VERIFY.into(),
                source: cairn_inference::RecordSource::Cairn,
                profile: GGUF.into(),
                head: "sp1".into(),
                machines: vec!["sp1".into()],
                api_port: 8090,
                api_path: "/v1".into(),
                health_path: "/v1/models".into(),
            });
        world.models.deployments.push(placed(VERIFY, EMBED, SPARKS));
        let mut passed = verification(GGUF, SPARKS, "passed");
        passed.profile_digest = super::super::inference_verify::digest(
            &world.inventory.profiles[&InferenceProfileId::from(GGUF)],
        );
        let observe = |world: &mut World, rows: Vec<VerificationRow>| {
            (world.verifications, world.edited) = current_verifications(rows, &world.inventory);
            let overview = world.overview();
            let profile = overview
                .models
                .iter()
                .find(|m| m.name == EMBED)
                .unwrap()
                .profiles
                .iter()
                .find(|p| p.id == GGUF)
                .unwrap()
                .clone();
            let offer = pool(&overview, SPARKS)
                .loadable
                .iter()
                .find(|l| l.model == EMBED)
                .unwrap()
                .clone();
            (profile, offer)
        };

        let (profile, offer) = observe(&mut world, vec![passed.clone()]);
        assert!(!profile.edited);
        assert_eq!(profile.fit.len(), 1, "the pass is its fit");
        assert_eq!(
            (offer.blocked, offer.verifies),
            (None, false),
            "a plain load"
        );

        let Engine::Container(container) = &mut world
            .inventory
            .profiles
            .get_mut(&InferenceProfileId::from(GGUF))
            .unwrap()
            .engine
        else {
            unreachable!()
        };
        container.args.extend(["--parallel".into(), "4".into()]);
        let (profile, offer) = observe(&mut world, vec![passed]);
        assert!(profile.edited);
        assert!(
            profile.fit.is_empty(),
            "the pass measured what it no longer is"
        );
        assert_eq!(
            (offer.profile.as_deref(), offer.blocked, offer.verifies),
            (Some(GGUF), None, true),
            "loading it verifies the edit first"
        );
        assert_eq!(
            world.sources().load_verifies(&PoolKey::from(SPARKS), EMBED),
            Some(vec![InferenceProfileId::from(GGUF)])
        );
    }

    #[test]
    fn each_refusal_of_a_save_lands_at_the_field_it_is_about() {
        use ValidationError as V;
        let profile = embed_container(10 * GIB);
        let yaml = serde_yaml::to_string(&profile).unwrap();
        let id = profile.id.clone();
        let refusals = [
            Refusal::Invalid(V::BadImageDigest {
                profile: id.clone(),
                image: "llama.cpp:latest".into(),
            }),
            Refusal::Invalid(V::BadWeightFile {
                profile: id.clone(),
                index: 1,
                problem: "no path",
            }),
            Refusal::Invalid(V::UnknownPlaceholder {
                profile: id.clone(),
                placeholder: "port".into(),
            }),
            Refusal::Invalid(V::RanksMismatch {
                profile: id.clone(),
                ranks: 2,
                machines: 1,
            }),
            Refusal::Invalid(V::UnknownProfile {
                deployment: "embed".into(),
                profile: "embed-before".into(),
            }),
            Refusal::Invalid(V::EmptyStart {
                profile: "another".into(),
            }),
            Refusal::Taken(id.clone()),
            Refusal::Said("deployment embed is healthy; stop it first".into()),
        ];
        let problems = profile_problems(&refusals, &yaml, Some("embed-before"));
        assert_eq!(
            problems
                .iter()
                .map(|p| p.field.as_deref())
                .collect::<Vec<_>>(),
            [
                Some("image"),
                Some("weights.1"),
                Some("args"),
                Some("yaml"),
                Some("id"),
                None,
                Some("id"),
                None
            ]
        );
        assert_eq!(
            problems[0].message,
            "image llama.cpp:latest is not pinned by digest (@sha256:<64 hex>)",
            "the field already names the profile"
        );
        assert_eq!(problems[1].message, "weight file 1: no path");
        assert_eq!(
            problems[5].message,
            "profile another has an empty start command"
        );
        assert_eq!(
            profile_problems(
                &[Refusal::Unparsed("bad indentation".into())],
                "id: [",
                None
            ),
            [ProfileProblem {
                field: Some("yaml".into()),
                message: "the document does not parse: bad indentation".into(),
            }]
        );
    }
}
