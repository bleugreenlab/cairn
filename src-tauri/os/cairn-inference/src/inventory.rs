//! The inventory: every recipe, deployment and pool, validated as a whole.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::deployment::{Deployment, DeploymentId};
use crate::machine::MachineName;
use crate::pool::{Pool, PoolId, RecordSource};
use crate::recipe::{Recipe, RecipeId};
use crate::resolve::{self, Resolution, Slot};

/// Every inference record Cairn knows, keyed by id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Inventory {
    pub recipes: BTreeMap<RecipeId, Recipe>,
    pub deployments: BTreeMap<DeploymentId, Deployment>,
    pub pools: BTreeMap<PoolId, Pool>,
}

/// Cairn's own record format: lists, so each id is written once.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InventoryFile {
    #[serde(default)]
    recipes: Vec<Recipe>,
    #[serde(default)]
    deployments: Vec<Deployment>,
    #[serde(default)]
    pools: Vec<Pool>,
}

/// One reason the inventory is not coherent, naming the records involved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    RecipeZeroMachines {
        recipe: RecipeId,
    },
    /// A container or native recipe that pins no weight file.
    UnpinnedWeights {
        recipe: RecipeId,
    },
    BadWeightFile {
        recipe: RecipeId,
        index: usize,
        problem: &'static str,
    },
    BadImageDigest {
        recipe: RecipeId,
        image: String,
    },
    BadArtifactSha {
        recipe: RecipeId,
    },
    EmptyStart {
        recipe: RecipeId,
    },
    UnknownRecipe {
        deployment: DeploymentId,
        recipe: RecipeId,
    },
    HeadNotInDeployment {
        deployment: DeploymentId,
        head: MachineName,
    },
    DuplicateDeploymentMachine {
        deployment: DeploymentId,
        machine: MachineName,
    },
    MachineCount {
        deployment: DeploymentId,
        recipe: RecipeId,
        required: u32,
        placed: usize,
    },
    BadPath {
        deployment: DeploymentId,
        which: &'static str,
        path: String,
    },
    /// A deployment whose machines lie in more than one pool, or partly in
    /// none (bginf's `straddle`). `None` stands for "no pool".
    DeploymentSpansPools {
        deployment: DeploymentId,
        pools: Vec<Option<PoolId>>,
    },
    DuplicatePoolMachine {
        pool: PoolId,
        machine: MachineName,
    },
    MachineInTwoPools {
        machine: MachineName,
        first: PoolId,
        second: PoolId,
    },
    UnknownSetMember {
        pool: PoolId,
        slot: Slot,
        deployment: DeploymentId,
    },
    SetMemberOutsidePool {
        pool: PoolId,
        slot: Slot,
        deployment: DeploymentId,
        machine: MachineName,
    },
    OverlappingSet {
        pool: PoolId,
        slot: Slot,
        first: DeploymentId,
        second: DeploymentId,
        machine: MachineName,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecipeZeroMachines { recipe } => {
                write!(f, "recipe {recipe} requires zero machines")
            }
            Self::UnpinnedWeights { recipe } => write!(
                f,
                "recipe {recipe} runs a container or native engine but pins no weight file"
            ),
            Self::BadWeightFile {
                recipe,
                index,
                problem,
            } => write!(f, "recipe {recipe} weight file {index}: {problem}"),
            Self::BadImageDigest { recipe, image } => write!(
                f,
                "recipe {recipe} image {image} is not pinned by digest (@sha256:<64 hex>)"
            ),
            Self::BadArtifactSha { recipe } => write!(
                f,
                "recipe {recipe} native artifact sha256 is not 64 lowercase hex digits"
            ),
            Self::EmptyStart { recipe } => write!(f, "recipe {recipe} has an empty start command"),
            Self::UnknownRecipe { deployment, recipe } => {
                write!(f, "deployment {deployment} names unknown recipe {recipe}")
            }
            Self::HeadNotInDeployment { deployment, head } => write!(
                f,
                "deployment {deployment} has head {head}, which is not one of its machines"
            ),
            Self::DuplicateDeploymentMachine {
                deployment,
                machine,
            } => write!(f, "deployment {deployment} lists machine {machine} twice"),
            Self::MachineCount {
                deployment,
                recipe,
                required,
                placed,
            } => write!(
                f,
                "deployment {deployment} places recipe {recipe} on {placed} machines, but it requires {required}"
            ),
            Self::BadPath {
                deployment,
                which,
                path,
            } => write!(
                f,
                "deployment {deployment} {which} path {path:?} does not start with /"
            ),
            Self::DeploymentSpansPools { deployment, pools } => {
                let pools: Vec<String> = pools
                    .iter()
                    .map(|p| p.as_ref().map_or("no pool".to_string(), |p| p.to_string()))
                    .collect();
                write!(
                    f,
                    "deployment {deployment} spans pools {}",
                    pools.join(", ")
                )
            }
            Self::DuplicatePoolMachine { pool, machine } => {
                write!(f, "pool {pool} lists machine {machine} twice")
            }
            Self::MachineInTwoPools {
                machine,
                first,
                second,
            } => write!(f, "machine {machine} is in pools {first} and {second}"),
            Self::UnknownSetMember {
                pool,
                slot,
                deployment,
            } => write!(
                f,
                "pool {pool} {} names unknown deployment {deployment}",
                slot.as_str()
            ),
            Self::SetMemberOutsidePool {
                pool,
                slot,
                deployment,
                machine,
            } => write!(
                f,
                "pool {pool} {} names deployment {deployment}, which uses machine {machine} outside the pool",
                slot.as_str()
            ),
            Self::OverlappingSet {
                pool,
                slot,
                first,
                second,
                machine,
            } => write!(
                f,
                "pool {pool} {} holds deployments {first} and {second}, which both use machine {machine}",
                slot.as_str()
            ),
        }
    }
}

impl std::error::Error for ValidationError {}

/// What pruning removes to cure one validation error.
enum Culprit {
    Recipe(RecipeId),
    Deployment(DeploymentId),
    /// Drop `machine` from `pool`; with `keep_first`, only its repeats.
    PoolMachine {
        pool: PoolId,
        machine: MachineName,
        keep_first: bool,
    },
    SetMember {
        pool: PoolId,
        slot: Slot,
        deployment: DeploymentId,
    },
}

impl ValidationError {
    fn culprit(&self) -> Culprit {
        match self {
            Self::RecipeZeroMachines { recipe }
            | Self::UnpinnedWeights { recipe }
            | Self::BadWeightFile { recipe, .. }
            | Self::BadImageDigest { recipe, .. }
            | Self::BadArtifactSha { recipe }
            | Self::EmptyStart { recipe } => Culprit::Recipe(recipe.clone()),
            Self::UnknownRecipe { deployment, .. }
            | Self::HeadNotInDeployment { deployment, .. }
            | Self::DuplicateDeploymentMachine { deployment, .. }
            | Self::MachineCount { deployment, .. }
            | Self::BadPath { deployment, .. }
            | Self::DeploymentSpansPools { deployment, .. } => {
                Culprit::Deployment(deployment.clone())
            }
            Self::DuplicatePoolMachine { pool, machine } => Culprit::PoolMachine {
                pool: pool.clone(),
                machine: machine.clone(),
                keep_first: true,
            },
            Self::MachineInTwoPools {
                machine, second, ..
            } => Culprit::PoolMachine {
                pool: second.clone(),
                machine: machine.clone(),
                keep_first: false,
            },
            Self::UnknownSetMember {
                pool,
                slot,
                deployment,
            }
            | Self::SetMemberOutsidePool {
                pool,
                slot,
                deployment,
                ..
            }
            | Self::OverlappingSet {
                pool,
                slot,
                second: deployment,
                ..
            } => Culprit::SetMember {
                pool: pool.clone(),
                slot: *slot,
                deployment: deployment.clone(),
            },
        }
    }
}

/// A YAML import that could not become an inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum YamlError {
    Parse(String),
    DuplicateId { kind: &'static str, id: String },
    Invalid(Vec<ValidationError>),
}

impl fmt::Display for YamlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(err) => write!(f, "inventory YAML does not parse: {err}"),
            Self::DuplicateId { kind, id } => write!(f, "{kind} {id} appears twice"),
            Self::Invalid(problems) => {
                let problems: Vec<String> = problems.iter().map(ToString::to_string).collect();
                write!(f, "inventory is invalid: {}", problems.join("; "))
            }
        }
    }
}

impl std::error::Error for YamlError {}

/// A pool id the inventory does not hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownPool(pub PoolId);

impl fmt::Display for UnknownPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown pool {}", self.0)
    }
}

impl std::error::Error for UnknownPool {}

/// What one import changed, per record kind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportReport {
    #[serde(default)]
    pub recipes: KindReport,
    #[serde(default)]
    pub deployments: KindReport,
    #[serde(default)]
    pub pools: KindReport,
    /// Imported records whose id another source already owns; skipped.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<String>,
    /// What the import refused or could not map, in words.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KindReport {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub updated: Vec<UpdatedRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdatedRecord {
    pub id: String,
    /// The record's top-level fields that changed, in serde naming.
    pub fields: Vec<String>,
}

impl KindReport {
    fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.updated.is_empty()
    }
}

impl ImportReport {
    /// Whether any record was added, removed or updated.
    pub fn has_changes(&self) -> bool {
        !(self.recipes.is_empty() && self.deployments.is_empty() && self.pools.is_empty())
    }

    /// One line per change, conflict and warning.
    pub fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for (kind, report) in [
            ("recipe", &self.recipes),
            ("deployment", &self.deployments),
            ("pool", &self.pools),
        ] {
            lines.extend(report.added.iter().map(|id| format!("added {kind} {id}")));
            lines.extend(
                report
                    .removed
                    .iter()
                    .map(|id| format!("removed {kind} {id}")),
            );
            lines.extend(
                report
                    .updated
                    .iter()
                    .map(|u| format!("updated {kind} {}: {}", u.id, u.fields.join(", "))),
            );
        }
        lines.extend(self.conflicts.iter().map(|c| format!("conflict: {c}")));
        lines.extend(self.warnings.iter().map(|w| format!("warning: {w}")));
        lines
    }
}

trait Sourced: Clone + PartialEq + Serialize {
    fn source(&self) -> RecordSource;
}

impl Sourced for Recipe {
    fn source(&self) -> RecordSource {
        self.source
    }
}

impl Sourced for Deployment {
    fn source(&self) -> RecordSource {
        self.source
    }
}

impl Sourced for Pool {
    fn source(&self) -> RecordSource {
        self.source
    }
}

fn changed_fields<T: Serialize>(before: &T, after: &T) -> Vec<String> {
    let (Ok(serde_json::Value::Object(before)), Ok(serde_json::Value::Object(after))) =
        (serde_json::to_value(before), serde_json::to_value(after))
    else {
        return vec!["record".to_string()];
    };
    let keys: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
    keys.into_iter()
        .filter(|key| before.get(*key) != after.get(*key))
        .cloned()
        .collect()
}

fn reconcile_kind<K: Ord + Clone + fmt::Display, T: Sourced>(
    kind: &str,
    current: &BTreeMap<K, T>,
    imported: &BTreeMap<K, T>,
    source: RecordSource,
    report: &mut KindReport,
    conflicts: &mut Vec<String>,
) -> BTreeMap<K, T> {
    let mut merged: BTreeMap<K, T> = current
        .iter()
        .filter(|(_, record)| record.source() != source)
        .map(|(id, record)| (id.clone(), record.clone()))
        .collect();
    for (id, record) in current {
        if record.source() == source && !imported.contains_key(id) {
            report.removed.push(id.to_string());
        }
    }
    for (id, record) in imported {
        if record.source() != source {
            conflicts.push(format!(
                "{kind} {id} is a {} record in a {} import; skipped",
                record.source().as_str(),
                source.as_str()
            ));
            continue;
        }
        match current.get(id) {
            Some(existing) if existing.source() != source => conflicts.push(format!(
                "{kind} {id} is already a {} record; the {} import of it was skipped",
                existing.source().as_str(),
                source.as_str()
            )),
            Some(existing) => {
                if existing != record {
                    report.updated.push(UpdatedRecord {
                        id: id.to_string(),
                        fields: changed_fields(existing, record),
                    });
                }
                merged.insert(id.clone(), record.clone());
            }
            None => {
                report.added.push(id.to_string());
                merged.insert(id.clone(), record.clone());
            }
        }
    }
    merged
}

fn path_ok(path: &str) -> bool {
    path.is_empty() || path.starts_with('/')
}

impl Inventory {
    pub fn insert_recipe(&mut self, recipe: Recipe) {
        self.recipes.insert(recipe.id.clone(), recipe);
    }

    pub fn insert_deployment(&mut self, deployment: Deployment) {
        self.deployments.insert(deployment.id.clone(), deployment);
    }

    pub fn insert_pool(&mut self, pool: Pool) {
        self.pools.insert(pool.id.clone(), pool);
    }

    /// Every validation error, in a stable order. Empty when coherent.
    pub fn problems(&self) -> Vec<ValidationError> {
        let mut problems = Vec::new();
        for recipe in self.recipes.values() {
            problems.extend(recipe.problems());
        }

        let mut membership: BTreeMap<&MachineName, &PoolId> = BTreeMap::new();
        for pool in self.pools.values() {
            let mut seen = BTreeSet::new();
            for machine in &pool.machines {
                if !seen.insert(&machine.name) {
                    problems.push(ValidationError::DuplicatePoolMachine {
                        pool: pool.id.clone(),
                        machine: machine.name.clone(),
                    });
                    continue;
                }
                match membership.get(&machine.name) {
                    Some(first) => problems.push(ValidationError::MachineInTwoPools {
                        machine: machine.name.clone(),
                        first: (*first).clone(),
                        second: pool.id.clone(),
                    }),
                    None => {
                        membership.insert(&machine.name, &pool.id);
                    }
                }
            }
        }

        for deployment in self.deployments.values() {
            let id = || deployment.id.clone();
            match self.recipes.get(&deployment.recipe) {
                None => problems.push(ValidationError::UnknownRecipe {
                    deployment: id(),
                    recipe: deployment.recipe.clone(),
                }),
                Some(recipe)
                    if recipe.requirements.machines as usize != deployment.machines.len() =>
                {
                    problems.push(ValidationError::MachineCount {
                        deployment: id(),
                        recipe: recipe.id.clone(),
                        required: recipe.requirements.machines,
                        placed: deployment.machines.len(),
                    })
                }
                Some(_) => {}
            }
            if !deployment.machines.contains(&deployment.head) {
                problems.push(ValidationError::HeadNotInDeployment {
                    deployment: id(),
                    head: deployment.head.clone(),
                });
            }
            let mut seen = BTreeSet::new();
            for machine in &deployment.machines {
                if !seen.insert(machine) {
                    problems.push(ValidationError::DuplicateDeploymentMachine {
                        deployment: id(),
                        machine: machine.clone(),
                    });
                }
            }
            for (which, path) in [
                ("api", &deployment.api_path),
                ("health", &deployment.health_path),
            ] {
                if !path_ok(path) {
                    problems.push(ValidationError::BadPath {
                        deployment: id(),
                        which,
                        path: path.clone(),
                    });
                }
            }
            let pools: BTreeSet<Option<&PoolId>> = deployment
                .machines
                .iter()
                .map(|m| membership.get(m).copied())
                .collect();
            if pools.len() > 1 {
                problems.push(ValidationError::DeploymentSpansPools {
                    deployment: id(),
                    pools: pools.into_iter().map(|p| p.cloned()).collect(),
                });
            }
        }

        for pool in self.pools.values() {
            for slot in resolve::SLOTS {
                let Some(set) = resolve::slot_set(pool, slot) else {
                    continue;
                };
                let mut occupied: BTreeMap<&MachineName, &DeploymentId> = BTreeMap::new();
                for member in set {
                    let Some(deployment) = self.deployments.get(member) else {
                        problems.push(ValidationError::UnknownSetMember {
                            pool: pool.id.clone(),
                            slot,
                            deployment: member.clone(),
                        });
                        continue;
                    };
                    if let Some(outside) = deployment.machines.iter().find(|m| !pool.has_machine(m))
                    {
                        problems.push(ValidationError::SetMemberOutsidePool {
                            pool: pool.id.clone(),
                            slot,
                            deployment: member.clone(),
                            machine: outside.clone(),
                        });
                        continue;
                    }
                    if let Some((machine, first)) = deployment
                        .machines
                        .iter()
                        .find_map(|m| occupied.get(m).map(|first| (m, *first)))
                    {
                        problems.push(ValidationError::OverlappingSet {
                            pool: pool.id.clone(),
                            slot,
                            first: first.clone(),
                            second: member.clone(),
                            machine: machine.clone(),
                        });
                        continue;
                    }
                    for machine in &deployment.machines {
                        occupied.insert(machine, member);
                    }
                }
            }
        }
        problems
    }

    /// `Ok` when coherent; otherwise every validation error.
    pub fn validate(&self) -> Result<(), Vec<ValidationError>> {
        let problems = self.problems();
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems)
        }
    }

    /// Remove whatever makes the inventory invalid, returning what was wrong.
    /// A bad recipe goes, then any deployment of it; a bad deployment goes;
    /// a bad set member leaves its set; a machine in two pools leaves the
    /// later one.
    pub fn prune_invalid(&mut self) -> Vec<ValidationError> {
        let mut found = Vec::new();
        loop {
            let problems = self.problems();
            if problems.is_empty() {
                break;
            }
            let before = self.clone();
            for problem in &problems {
                self.remove(problem.culprit());
            }
            found.extend(problems);
            if *self == before {
                break;
            }
        }
        found
    }

    fn remove(&mut self, culprit: Culprit) {
        match culprit {
            Culprit::Recipe(id) => {
                self.recipes.remove(&id);
            }
            Culprit::Deployment(id) => {
                self.deployments.remove(&id);
            }
            Culprit::PoolMachine {
                pool,
                machine,
                keep_first,
            } => {
                if let Some(pool) = self.pools.get_mut(&pool) {
                    let mut kept = false;
                    pool.machines.retain(|m| {
                        if m.name != machine {
                            return true;
                        }
                        let keep = keep_first && !kept;
                        kept = true;
                        keep
                    });
                }
            }
            Culprit::SetMember {
                pool,
                slot,
                deployment,
            } => {
                if let Some(pool) = self.pools.get_mut(&pool) {
                    let set = match slot {
                        Slot::Pin => pool.pin.as_mut(),
                        Slot::Default => Some(&mut pool.default),
                    };
                    if let Some(set) = set {
                        set.remove(&deployment);
                    }
                }
            }
        }
    }

    /// The pool holding `machine`, if any.
    pub fn pool_of_machine(&self, machine: &MachineName) -> Option<&Pool> {
        self.pools.values().find(|pool| pool.has_machine(machine))
    }

    /// The pool every machine of `deployment` lies in, if there is one.
    pub fn pool_of_deployment(&self, deployment: &Deployment) -> Option<&Pool> {
        let pool = self.pool_of_machine(deployment.machines.first()?)?;
        deployment
            .machines
            .iter()
            .all(|m| pool.has_machine(m))
            .then_some(pool)
    }

    /// The deployments placed entirely on `pool`'s machines.
    pub fn pool_deployments<'a>(&'a self, pool: &'a Pool) -> impl Iterator<Item = &'a Deployment> {
        self.deployments.values().filter(move |d| {
            !d.machines.is_empty() && d.machines.iter().all(|m| pool.has_machine(m))
        })
    }

    /// The network address of `machine`, as its pool records it.
    pub fn address_of(&self, machine: &MachineName) -> Option<&str> {
        self.pool_of_machine(machine)?.address_of(machine)
    }

    /// Resolve one pool against the powered machines.
    pub fn resolve_pool(
        &self,
        pool: &PoolId,
        powered: &BTreeSet<MachineName>,
    ) -> Result<Resolution, UnknownPool> {
        let pool = self
            .pools
            .get(pool)
            .ok_or_else(|| UnknownPool(pool.clone()))?;
        Ok(resolve::resolve(pool, &self.deployments, powered))
    }

    /// Replace the records of `source` with `imported`'s, leaving every other
    /// source's records alone, and report what changed. Re-importing the same
    /// input reports nothing.
    pub fn reconcile(
        current: &Inventory,
        imported: &Inventory,
        source: RecordSource,
    ) -> (Inventory, ImportReport) {
        let mut report = ImportReport::default();
        let mut conflicts = Vec::new();
        let recipes = reconcile_kind(
            "recipe",
            &current.recipes,
            &imported.recipes,
            source,
            &mut report.recipes,
            &mut conflicts,
        );
        let deployments = reconcile_kind(
            "deployment",
            &current.deployments,
            &imported.deployments,
            source,
            &mut report.deployments,
            &mut conflicts,
        );
        let pools = reconcile_kind(
            "pool",
            &current.pools,
            &imported.pools,
            source,
            &mut report.pools,
            &mut conflicts,
        );
        report.conflicts = conflicts;
        (
            Inventory {
                recipes,
                deployments,
                pools,
            },
            report,
        )
    }

    /// Cairn's own record format, as YAML.
    pub fn to_yaml(&self) -> String {
        let file = InventoryFile {
            recipes: self.recipes.values().cloned().collect(),
            deployments: self.deployments.values().cloned().collect(),
            pools: self.pools.values().cloned().collect(),
        };
        serde_yaml::to_string(&file).expect("inventory records serialize to YAML")
    }

    /// Parse Cairn's own record format and validate it.
    pub fn from_yaml(text: &str) -> Result<Inventory, YamlError> {
        let file: InventoryFile =
            serde_yaml::from_str(text).map_err(|err| YamlError::Parse(err.to_string()))?;
        let mut inventory = Inventory::default();
        for recipe in file.recipes {
            if inventory.recipes.contains_key(&recipe.id) {
                return Err(YamlError::DuplicateId {
                    kind: "recipe",
                    id: recipe.id.to_string(),
                });
            }
            inventory.insert_recipe(recipe);
        }
        for deployment in file.deployments {
            if inventory.deployments.contains_key(&deployment.id) {
                return Err(YamlError::DuplicateId {
                    kind: "deployment",
                    id: deployment.id.to_string(),
                });
            }
            inventory.insert_deployment(deployment);
        }
        for pool in file.pools {
            if inventory.pools.contains_key(&pool.id) {
                return Err(YamlError::DuplicateId {
                    kind: "pool",
                    id: pool.id.to_string(),
                });
            }
            inventory.insert_pool(pool);
        }
        inventory.validate().map_err(YamlError::Invalid)?;
        Ok(inventory)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{bginf_test_inventory, command_recipe, deployment, set};
    use crate::pool::PoolMachine;
    use crate::recipe::Engine;

    fn problems_of(inventory: &Inventory) -> Vec<ValidationError> {
        inventory.problems()
    }

    #[test]
    fn the_bginf_fixture_is_valid() {
        assert_eq!(bginf_test_inventory().validate(), Ok(()));
    }

    /// bginf `test_pools_resolve_independently`: the `straddle` profile spans
    /// two pools and is refused.
    #[test]
    fn a_deployment_spanning_pools_is_refused() {
        let mut inventory = bginf_test_inventory();
        inventory.insert_recipe(command_recipe("straddle", 2));
        inventory.insert_deployment(deployment("straddle", &["sp1", "halo"]));
        assert_eq!(
            problems_of(&inventory),
            vec![ValidationError::DeploymentSpansPools {
                deployment: "straddle".into(),
                pools: vec![Some("halo".into()), Some("sparks".into())],
            }]
        );
        let pruned = inventory.prune_invalid();
        assert_eq!(pruned.len(), 1);
        assert!(!inventory
            .deployments
            .contains_key(&DeploymentId::from("straddle")));
        assert_eq!(inventory.validate(), Ok(()));
    }

    /// bginf `test_pin_targets_the_profiles_pool_and_off_every_pool`: a pin
    /// of a Spark profile in pool halo is refused.
    #[test]
    fn a_set_member_outside_the_pool_is_refused() {
        let mut inventory = bginf_test_inventory();
        inventory.pools.get_mut(&PoolId::from("halo")).unwrap().pin = Some(set(&["qwen36-fast"]));
        assert_eq!(
            problems_of(&inventory),
            vec![ValidationError::SetMemberOutsidePool {
                pool: "halo".into(),
                slot: Slot::Pin,
                deployment: "qwen36-fast".into(),
                machine: "sp1".into(),
            }]
        );
    }

    #[test]
    fn overlapping_deployments_in_one_set_are_refused() {
        let mut inventory = bginf_test_inventory();
        inventory
            .pools
            .get_mut(&PoolId::from("sparks"))
            .unwrap()
            .default = set(&["dsv4f-tp2", "qwen36-fast"]);
        assert_eq!(
            problems_of(&inventory),
            vec![ValidationError::OverlappingSet {
                pool: "sparks".into(),
                slot: Slot::Default,
                first: "dsv4f-tp2".into(),
                second: "qwen36-fast".into(),
                machine: "sp1".into(),
            }]
        );
    }

    #[test]
    fn an_unknown_set_member_is_refused() {
        let mut inventory = bginf_test_inventory();
        inventory
            .pools
            .get_mut(&PoolId::from("sparks"))
            .unwrap()
            .default = set(&["nope"]);
        assert_eq!(
            problems_of(&inventory),
            vec![ValidationError::UnknownSetMember {
                pool: "sparks".into(),
                slot: Slot::Default,
                deployment: "nope".into(),
            }]
        );
    }

    #[test]
    fn a_machine_in_two_pools_is_refused() {
        let mut inventory = bginf_test_inventory();
        inventory
            .pools
            .get_mut(&PoolId::from("halo"))
            .unwrap()
            .machines
            .push(PoolMachine {
                name: "sp2".into(),
                address: None,
            });
        let problems = problems_of(&inventory);
        assert!(
            problems.contains(&ValidationError::MachineInTwoPools {
                machine: "sp2".into(),
                first: "halo".into(),
                second: "sparks".into(),
            }),
            "{problems:?}"
        );
    }

    #[test]
    fn a_head_outside_the_deployment_is_refused() {
        let mut inventory = bginf_test_inventory();
        inventory
            .deployments
            .get_mut(&DeploymentId::from("qwen36-fast"))
            .unwrap()
            .head = "sp2".into();
        assert_eq!(
            problems_of(&inventory),
            vec![ValidationError::HeadNotInDeployment {
                deployment: "qwen36-fast".into(),
                head: "sp2".into(),
            }]
        );
    }

    #[test]
    fn a_machine_count_unlike_the_recipe_is_refused() {
        let mut inventory = bginf_test_inventory();
        inventory
            .deployments
            .get_mut(&DeploymentId::from("qwen36-fast"))
            .unwrap()
            .machines = vec!["sp1".into(), "sp2".into()];
        assert_eq!(
            problems_of(&inventory),
            vec![ValidationError::MachineCount {
                deployment: "qwen36-fast".into(),
                recipe: "qwen36-fast".into(),
                required: 1,
                placed: 2,
            }]
        );
    }

    #[test]
    fn duplicate_machines_and_unknown_recipes_are_refused() {
        let mut inventory = bginf_test_inventory();
        inventory.insert_deployment(deployment("twice", &["sp1", "sp1"]));
        inventory
            .deployments
            .get_mut(&DeploymentId::from("twice"))
            .unwrap()
            .recipe = "dsv4f-tp2".into();
        inventory.insert_deployment(deployment("orphan", &["sp1"]));
        let problems = problems_of(&inventory);
        assert!(
            problems.contains(&ValidationError::DuplicateDeploymentMachine {
                deployment: "twice".into(),
                machine: "sp1".into(),
            })
        );
        assert!(problems.contains(&ValidationError::UnknownRecipe {
            deployment: "orphan".into(),
            recipe: "orphan".into(),
        }));
    }

    #[test]
    fn reimporting_identical_input_reports_nothing() {
        let current = bginf_test_inventory();
        let (merged, report) = Inventory::reconcile(&current, &current, RecordSource::Bginf);
        assert_eq!(merged, current);
        assert!(!report.has_changes());
        assert_eq!(report.lines(), Vec::<String>::new());
    }

    #[test]
    fn a_changed_start_command_updates_the_recipe_in_place() {
        let current = bginf_test_inventory();
        let mut imported = current.clone();
        imported
            .recipes
            .get_mut(&RecipeId::from("qwen36-fast"))
            .unwrap()
            .engine = Engine::Command {
            start: "./start.sh --port 8002".into(),
            stop: Some("./stop.sh".into()),
            description: None,
        };
        let (merged, report) = Inventory::reconcile(&current, &imported, RecordSource::Bginf);
        assert_eq!(merged, imported);
        assert_eq!(report.lines(), vec!["updated recipe qwen36-fast: engine"]);
    }

    #[test]
    fn a_removed_profile_removes_its_records_and_cairn_records_stay() {
        let mut current = bginf_test_inventory();
        let mut own = command_recipe("own", 1);
        own.source = RecordSource::Cairn;
        current.insert_recipe(own.clone());

        let mut imported = bginf_test_inventory();
        imported.recipes.remove(&RecipeId::from("qwen36-fast"));
        imported
            .deployments
            .remove(&DeploymentId::from("qwen36-fast"));
        let (merged, report) = Inventory::reconcile(&current, &imported, RecordSource::Bginf);
        assert_eq!(
            report.lines(),
            vec![
                "removed recipe qwen36-fast",
                "removed deployment qwen36-fast"
            ]
        );
        assert_eq!(merged.recipes.get(&RecipeId::from("own")), Some(&own));
        assert!(!merged.recipes.contains_key(&RecipeId::from("qwen36-fast")));
    }

    #[test]
    fn an_import_never_overwrites_another_sources_record() {
        let mut current = bginf_test_inventory();
        let mut own = command_recipe("qwen36-fast", 1);
        own.source = RecordSource::Cairn;
        current.insert_recipe(own.clone());
        let (merged, report) =
            Inventory::reconcile(&current, &bginf_test_inventory(), RecordSource::Bginf);
        assert_eq!(
            merged.recipes.get(&RecipeId::from("qwen36-fast")),
            Some(&own)
        );
        assert_eq!(report.conflicts.len(), 1, "{report:?}");
    }

    #[test]
    fn the_yaml_format_round_trips_and_validates_on_import() {
        let mut inventory = bginf_test_inventory();
        inventory.pools.get_mut(&PoolId::from("halo")).unwrap().pin = Some(set(&[]));
        let yaml = inventory.to_yaml();
        assert_eq!(Inventory::from_yaml(&yaml), Ok(inventory.clone()));

        let broken = yaml.replacen("head: halo", "head: sp1", 1);
        assert!(matches!(
            Inventory::from_yaml(&broken),
            Err(YamlError::Invalid(problems))
                if problems.iter().any(|p| matches!(p, ValidationError::HeadNotInDeployment { .. }))
        ));
    }

    #[test]
    fn the_empty_pin_survives_yaml_as_pinned_to_nothing() {
        let mut inventory = bginf_test_inventory();
        inventory.pools.get_mut(&PoolId::from("halo")).unwrap().pin = Some(set(&[]));
        let parsed = Inventory::from_yaml(&inventory.to_yaml()).unwrap();
        assert_eq!(parsed.pools[&PoolId::from("halo")].pin, Some(set(&[])));
        assert_eq!(parsed.pools[&PoolId::from("sparks")].pin, None);
    }
}
