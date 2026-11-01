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
    /// An id written twice in one inventory document.
    DuplicateId {
        kind: &'static str,
        id: String,
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
            Self::DuplicateId { kind, id } => write!(f, "{kind} {id} appears twice"),
        }
    }
}

impl std::error::Error for ValidationError {}

/// An inventory document that could not become an inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum YamlError {
    Parse(String),
    /// Every reason the document is not coherent.
    Invalid(Vec<ValidationError>),
}

impl fmt::Display for YamlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(err) => write!(f, "inventory YAML does not parse: {err}"),
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

/// What one change to the inventory added, removed and updated, per record
/// kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeReport {
    pub recipes: KindReport,
    pub deployments: KindReport,
    pub pools: KindReport,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KindReport {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub updated: Vec<UpdatedRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl ChangeReport {
    /// Whether any record was added, removed or updated.
    pub fn has_changes(&self) -> bool {
        !(self.recipes.is_empty() && self.deployments.is_empty() && self.pools.is_empty())
    }

    /// One line per change.
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
        lines
    }
}

/// What the inventory needs of every record kind: who owns it.
trait Sourced: PartialEq + Serialize {
    fn source(&self) -> RecordSource;
    fn set_source(&mut self, source: RecordSource);
}

macro_rules! sourced {
    ($($record:ty),*) => {$(
        impl Sourced for $record {
            fn source(&self) -> RecordSource {
                self.source
            }

            fn set_source(&mut self, source: RecordSource) {
                self.source = source;
            }
        }
    )*};
}

sourced!(Recipe, Deployment, Pool);

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

fn diff_kind<K: Ord + fmt::Display, T: Sourced>(
    current: &BTreeMap<K, T>,
    next: &BTreeMap<K, T>,
) -> KindReport {
    let mut report = KindReport::default();
    for (id, record) in next {
        match current.get(id) {
            None => report.added.push(id.to_string()),
            Some(before) if before != record => report.updated.push(UpdatedRecord {
                id: id.to_string(),
                fields: changed_fields(before, record),
            }),
            Some(_) => {}
        }
    }
    report.removed = current
        .keys()
        .filter(|id| !next.contains_key(*id))
        .map(ToString::to_string)
        .collect();
    report
}

fn foreign_of<'a, K: fmt::Display, T: Sourced>(
    kind: &'static str,
    records: &'a BTreeMap<K, T>,
) -> impl Iterator<Item = String> + 'a {
    records
        .iter()
        .filter(|(_, record)| record.source() != RecordSource::Cairn)
        .map(move |(id, record)| format!("{kind} {id} ({})", record.source().as_str()))
}

fn adopt_kind<K: fmt::Display, T: Sourced>(
    kind: &str,
    records: &mut BTreeMap<K, T>,
    from: RecordSource,
    adopted: &mut Vec<String>,
) {
    for (id, record) in records.iter_mut() {
        if record.source() == from && from != RecordSource::Cairn {
            record.set_source(RecordSource::Cairn);
            adopted.push(format!("{kind} {id}"));
        }
    }
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

    /// Every record that is not Cairn's own, as `kind id (source)`, recipes
    /// first, then deployments, then pools, each in id order.
    pub fn foreign_records(&self) -> Vec<String> {
        foreign_of("recipe", &self.recipes)
            .chain(foreign_of("deployment", &self.deployments))
            .chain(foreign_of("pool", &self.pools))
            .collect()
    }

    /// Make every record of `from` Cairn's own, keeping its id and contents,
    /// and name each one adopted. Adopting again adopts nothing.
    pub fn adopt(&mut self, from: RecordSource) -> Vec<String> {
        let mut adopted = Vec::new();
        adopt_kind("recipe", &mut self.recipes, from, &mut adopted);
        adopt_kind("deployment", &mut self.deployments, from, &mut adopted);
        adopt_kind("pool", &mut self.pools, from, &mut adopted);
        adopted
    }

    /// Add each of `records`' records, replacing the one with its id.
    pub fn upsert(&mut self, records: Inventory) {
        self.recipes.extend(records.recipes);
        self.deployments.extend(records.deployments);
        self.pools.extend(records.pools);
    }

    /// What turning `current` into `next` adds, removes and updates.
    pub fn changes(current: &Inventory, next: &Inventory) -> ChangeReport {
        ChangeReport {
            recipes: diff_kind(&current.recipes, &next.recipes),
            deployments: diff_kind(&current.deployments, &next.deployments),
            pools: diff_kind(&current.pools, &next.pools),
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

    /// Cairn's own record format, as YAML.
    pub fn to_yaml(&self) -> String {
        let file = InventoryFile {
            recipes: self.recipes.values().cloned().collect(),
            deployments: self.deployments.values().cloned().collect(),
            pools: self.pools.values().cloned().collect(),
        };
        serde_yaml::to_string(&file).expect("inventory records serialize to YAML")
    }

    /// The records a document in Cairn's own record format holds, keeping the
    /// first of any id written twice, and each id written twice.
    fn parse_records(text: &str) -> Result<(Inventory, Vec<ValidationError>), YamlError> {
        let file: InventoryFile =
            serde_yaml::from_str(text).map_err(|err| YamlError::Parse(err.to_string()))?;
        let mut inventory = Inventory::default();
        let mut duplicates = Vec::new();
        let mut duplicate = |kind: &'static str, id: &dyn fmt::Display| {
            duplicates.push(ValidationError::DuplicateId {
                kind,
                id: id.to_string(),
            })
        };
        for recipe in file.recipes {
            match inventory.recipes.contains_key(&recipe.id) {
                true => duplicate("recipe", &recipe.id),
                false => inventory.insert_recipe(recipe),
            }
        }
        for deployment in file.deployments {
            match inventory.deployments.contains_key(&deployment.id) {
                true => duplicate("deployment", &deployment.id),
                false => inventory.insert_deployment(deployment),
            }
        }
        for pool in file.pools {
            match inventory.pools.contains_key(&pool.id) {
                true => duplicate("pool", &pool.id),
                false => inventory.insert_pool(pool),
            }
        }
        Ok((inventory, duplicates))
    }

    /// Parse records in Cairn's own record format without checking that they
    /// cohere, as a document of some records to add to an inventory is. An id
    /// written twice is refused.
    pub fn parse_yaml(text: &str) -> Result<Inventory, YamlError> {
        let (inventory, duplicates) = Self::parse_records(text)?;
        if duplicates.is_empty() {
            Ok(inventory)
        } else {
            Err(YamlError::Invalid(duplicates))
        }
    }

    /// Parse a whole inventory in Cairn's own record format and validate it,
    /// refusing it with every problem it has.
    pub fn from_yaml(text: &str) -> Result<Inventory, YamlError> {
        let (inventory, mut problems) = Self::parse_records(text)?;
        problems.extend(inventory.problems());
        if problems.is_empty() {
            Ok(inventory)
        } else {
            Err(YamlError::Invalid(problems))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{command_recipe, deployment, set, test_inventory};
    use crate::pool::PoolMachine;
    use crate::recipe::Engine;

    fn problems_of(inventory: &Inventory) -> Vec<ValidationError> {
        inventory.problems()
    }

    #[test]
    fn the_test_inventory_is_valid() {
        assert_eq!(test_inventory().validate(), Ok(()));
    }

    /// bginf `test_pools_resolve_independently`: the `straddle` profile spans
    /// two pools and is refused.
    #[test]
    fn a_deployment_spanning_pools_is_refused() {
        let mut inventory = test_inventory();
        inventory.insert_recipe(command_recipe("straddle", 2));
        inventory.insert_deployment(deployment("straddle", &["sp1", "halo"]));
        assert_eq!(
            problems_of(&inventory),
            vec![ValidationError::DeploymentSpansPools {
                deployment: "straddle".into(),
                pools: vec![Some("halo".into()), Some("sparks".into())],
            }]
        );
    }

    /// bginf `test_pin_targets_the_profiles_pool_and_off_every_pool`: a pin
    /// of a Spark profile in pool halo is refused.
    #[test]
    fn a_set_member_outside_the_pool_is_refused() {
        let mut inventory = test_inventory();
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
        let mut inventory = test_inventory();
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
        let mut inventory = test_inventory();
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
        let mut inventory = test_inventory();
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
        let mut inventory = test_inventory();
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
        let mut inventory = test_inventory();
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
        let mut inventory = test_inventory();
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

    /// Every record of the fixture, as bginf mirrored it before it retired.
    fn mirrored() -> Inventory {
        let mut inventory = test_inventory();
        for recipe in inventory.recipes.values_mut() {
            recipe.source = RecordSource::Bginf;
        }
        for deployment in inventory.deployments.values_mut() {
            deployment.source = RecordSource::Bginf;
        }
        for pool in inventory.pools.values_mut() {
            pool.source = RecordSource::Bginf;
        }
        inventory
    }

    #[test]
    fn adopting_makes_every_mirrored_record_cairns_and_is_idempotent() {
        let mut inventory = mirrored();
        let mut own = command_recipe("own", 1);
        own.source = RecordSource::Cairn;
        inventory.insert_recipe(own);
        assert_eq!(inventory.foreign_records().len(), 8);
        assert_eq!(inventory.foreign_records()[0], "recipe dsv4f-tp2 (bginf)");

        let before = inventory.clone();
        let adopted = inventory.adopt(RecordSource::Bginf);
        assert_eq!(
            adopted,
            vec![
                "recipe dsv4f-tp2",
                "recipe qwen3-embed-4b",
                "recipe qwen36-fast",
                "deployment dsv4f-tp2",
                "deployment qwen3-embed-4b",
                "deployment qwen36-fast",
                "pool halo",
                "pool sparks",
            ]
        );
        assert!(inventory.foreign_records().is_empty());
        assert_eq!(
            inventory,
            test_inventory_with_own(),
            "ids and contents kept"
        );
        let report = Inventory::changes(&before, &inventory);
        assert_eq!(report.lines().len(), 8);
        assert!(report
            .lines()
            .iter()
            .all(|line| line.starts_with("updated ") && line.ends_with(": source")));

        assert!(inventory.adopt(RecordSource::Bginf).is_empty());
        assert!(inventory.adopt(RecordSource::Cairn).is_empty());
    }

    fn test_inventory_with_own() -> Inventory {
        let mut inventory = test_inventory();
        let mut own = command_recipe("own", 1);
        own.source = RecordSource::Cairn;
        inventory.insert_recipe(own);
        inventory
    }

    #[test]
    fn changes_name_what_was_added_removed_and_updated() {
        let current = test_inventory();
        assert!(!Inventory::changes(&current, &current).has_changes());

        let mut next = current.clone();
        next.recipes
            .get_mut(&RecipeId::from("qwen36-fast"))
            .unwrap()
            .engine = Engine::Command {
            start: "./start.sh --port 8002".into(),
            stop: Some("./stop.sh".into()),
            description: None,
        };
        next.insert_recipe(command_recipe("new", 1));
        next.pools.remove(&PoolId::from("halo"));
        assert_eq!(
            Inventory::changes(&current, &next).lines(),
            vec![
                "added recipe new",
                "updated recipe qwen36-fast: engine",
                "removed pool halo",
            ]
        );
    }

    #[test]
    fn an_upsert_adds_and_replaces_records_by_id() {
        let mut inventory = test_inventory();
        let mut records = Inventory::default();
        let mut changed = command_recipe("qwen36-fast", 1);
        changed.served_name = "qwen3.6-35b".into();
        records.insert_recipe(changed.clone());
        records.insert_recipe(command_recipe("new", 1));
        inventory.upsert(records);
        assert_eq!(inventory.recipes[&RecipeId::from("qwen36-fast")], changed);
        assert!(inventory.recipes.contains_key(&RecipeId::from("new")));
        assert_eq!(inventory.deployments.len(), 3);
    }

    #[test]
    fn the_yaml_format_round_trips_and_validates_on_import() {
        let mut inventory = test_inventory();
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
    fn a_refused_document_names_every_problem_at_once() {
        let yaml = r#"
recipes:
  - {id: r, model: {name: r}, servedName: r, engine: {kind: command, start: ./start.sh}, requirements: {machines: 1}}
  - {id: r, model: {name: r}, servedName: r, engine: {kind: command, start: ''}, requirements: {machines: 1}}
deployments:
  - {id: d, recipe: missing, machines: [a], head: b, apiPort: 8000, apiPath: /v1, healthPath: /health}
pools:
  - {id: p, machines: [{name: a}], default: [nope]}
"#;
        let Err(YamlError::Invalid(problems)) = Inventory::from_yaml(yaml) else {
            panic!("the document is refused");
        };
        let problems: Vec<String> = problems.iter().map(ToString::to_string).collect();
        assert_eq!(
            problems,
            vec![
                "recipe r appears twice",
                "deployment d names unknown recipe missing",
                "deployment d has head b, which is not one of its machines",
                "pool p default names unknown deployment nope",
            ]
        );
        assert!(Inventory::parse_yaml(yaml).is_err(), "an id written twice");
        assert!(matches!(
            Inventory::from_yaml("recipes: {"),
            Err(YamlError::Parse(_))
        ));
    }

    #[test]
    fn a_record_without_a_source_is_cairns() {
        let parsed = Inventory::parse_yaml(
            "recipes:\n  - {id: r, model: {name: r}, servedName: r, engine: {kind: command, start: ./s}, requirements: {machines: 1}}\n",
        )
        .unwrap();
        assert_eq!(
            parsed.recipes[&RecipeId::from("r")].source,
            RecordSource::Cairn
        );
    }

    #[test]
    fn the_empty_pin_survives_yaml_as_pinned_to_nothing() {
        let mut inventory = test_inventory();
        inventory.pools.get_mut(&PoolId::from("halo")).unwrap().pin = Some(set(&[]));
        let parsed = Inventory::from_yaml(&inventory.to_yaml()).unwrap();
        assert_eq!(parsed.pools[&PoolId::from("halo")].pin, Some(set(&[])));
        assert_eq!(parsed.pools[&PoolId::from("sparks")].pin, None);
    }
}
