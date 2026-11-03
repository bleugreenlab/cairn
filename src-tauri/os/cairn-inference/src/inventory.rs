//! The inventory: every profile, deployment and machine address, validated as
//! a whole.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::deployment::{Deployment, DeploymentId};
use crate::machine::{MachineName, MachineRecord};
use crate::model::Model;
use crate::profile::{InferenceProfile, InferenceProfileId};
use crate::record::RecordSource;

/// Every inference record Cairn knows, keyed by id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Inventory {
    /// Models by name, their Hugging Face repository id.
    pub models: BTreeMap<String, Model>,
    pub profiles: BTreeMap<InferenceProfileId, InferenceProfile>,
    pub deployments: BTreeMap<DeploymentId, Deployment>,
    pub machines: BTreeMap<MachineName, MachineRecord>,
}

/// Cairn's own record format: lists, so each id is written once. A document
/// still carrying the retired `pools:` section is refused naming it.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InventoryFile {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    models: Vec<Model>,
    #[serde(default)]
    profiles: Vec<InferenceProfile>,
    #[serde(default)]
    deployments: Vec<Deployment>,
    #[serde(default)]
    machines: Vec<MachineRecord>,
    /// The kind's name before profiles were renamed, read only to refuse it:
    /// a document still naming it would otherwise parse as holding no
    /// profiles, which a whole-document replace takes as removing them all.
    #[serde(default, rename = "recipes", skip_serializing)]
    retired_recipes: Option<serde::de::IgnoredAny>,
}

/// One reason the inventory is not coherent, naming the records involved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    ProfileZeroMachines {
        profile: InferenceProfileId,
    },
    /// A container or native profile that pins no weight file.
    UnpinnedWeights {
        profile: InferenceProfileId,
    },
    BadWeightFile {
        profile: InferenceProfileId,
        index: usize,
        problem: &'static str,
    },
    BadImageDigest {
        profile: InferenceProfileId,
        image: String,
    },
    BadArtifactSha {
        profile: InferenceProfileId,
    },
    EmptyStart {
        profile: InferenceProfileId,
    },
    /// A container's args or env name a placeholder the renderer does not
    /// fill.
    UnknownPlaceholder {
        profile: InferenceProfileId,
        placeholder: String,
    },
    /// A container spanning machines without exactly one rank env per
    /// machine.
    RanksMismatch {
        profile: InferenceProfileId,
        ranks: usize,
        machines: u32,
    },
    /// A container mounting from its source files without pinning them.
    UnpinnedSourceMount {
        profile: InferenceProfileId,
        path: String,
    },
    BadSourceCommit {
        profile: InferenceProfileId,
        commit: String,
    },
    /// A generated profile with no memory need, which placement and a
    /// verification load's drain both size by.
    GeneratedWithoutMemory {
        profile: InferenceProfileId,
    },
    /// A profile serving a model the inventory does not register.
    ProfileModelUnregistered {
        profile: InferenceProfileId,
        model: String,
    },
    UnknownProfile {
        deployment: DeploymentId,
        profile: InferenceProfileId,
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
        profile: InferenceProfileId,
        required: u32,
        placed: usize,
    },
    BadPath {
        deployment: DeploymentId,
        which: &'static str,
        path: String,
    },
    EmptyAddress {
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
            Self::ProfileZeroMachines { profile } => {
                write!(f, "profile {profile} requires zero machines")
            }
            Self::UnpinnedWeights { profile } => write!(
                f,
                "profile {profile} runs a container or native engine but pins no weight file"
            ),
            Self::BadWeightFile {
                profile,
                index,
                problem,
            } => write!(f, "profile {profile} weight file {index}: {problem}"),
            Self::BadImageDigest { profile, image } => write!(
                f,
                "profile {profile} image {image} is not pinned by digest (@sha256:<64 hex>)"
            ),
            Self::BadArtifactSha { profile } => write!(
                f,
                "profile {profile} native artifact sha256 is not 64 lowercase hex digits"
            ),
            Self::EmptyStart { profile } => write!(f, "profile {profile} has an empty start command"),
            Self::UnknownPlaceholder {
                profile,
                placeholder,
            } => write!(f, "profile {profile} uses unknown placeholder {{{placeholder}}}"),
            Self::RanksMismatch {
                profile,
                ranks,
                machines,
            } => write!(
                f,
                "profile {profile} runs on {machines} {} but gives env for {ranks} {}; give one entry per machine",
                if *machines == 1 { "machine" } else { "machines" },
                if *ranks == 1 { "rank" } else { "ranks" }
            ),
            Self::UnpinnedSourceMount { profile, path } => write!(
                f,
                "profile {profile} mounts {path} from its source files but pins no source (files: {{url, commit}})"
            ),
            Self::BadSourceCommit { profile, commit } => write!(
                f,
                "profile {profile} pins source commit {commit:?}, which is not a full 40-hex sha"
            ),
            Self::GeneratedWithoutMemory { profile } => write!(
                f,
                "profile {profile} is generated but states no memory need (requirements.memory)"
            ),
            Self::ProfileModelUnregistered { profile, model } => write!(
                f,
                "profile {profile} serves model {model}, which is not registered; register it at cairn://inference/models first"
            ),
            Self::UnknownProfile { deployment, profile } => {
                write!(f, "deployment {deployment} names unknown profile {profile}")
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
                profile,
                required,
                placed,
            } => write!(
                f,
                "deployment {deployment} places profile {profile} on {placed} machines, but it requires {required}"
            ),
            Self::BadPath {
                deployment,
                which,
                path,
            } => write!(
                f,
                "deployment {deployment} {which} path {path:?} does not start with /"
            ),
            Self::EmptyAddress { machine } => {
                write!(f, "machine {machine} has an empty address")
            }
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

/// What one change to the inventory added, removed and updated, per record
/// kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeReport {
    pub models: KindReport,
    pub profiles: KindReport,
    pub deployments: KindReport,
    pub machines: KindReport,
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
        !(self.models.is_empty()
            && self.profiles.is_empty()
            && self.deployments.is_empty()
            && self.machines.is_empty())
    }

    /// One line per change.
    pub fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for (kind, report) in [
            ("model", &self.models),
            ("profile", &self.profiles),
            ("deployment", &self.deployments),
            ("machine", &self.machines),
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

sourced!(Model, InferenceProfile, Deployment, MachineRecord);

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
    pub fn insert_model(&mut self, model: Model) {
        self.models.insert(model.name.clone(), model);
    }

    pub fn insert_profile(&mut self, profile: InferenceProfile) {
        self.profiles.insert(profile.id.clone(), profile);
    }

    pub fn insert_deployment(&mut self, deployment: Deployment) {
        self.deployments.insert(deployment.id.clone(), deployment);
    }

    pub fn insert_machine(&mut self, machine: MachineRecord) {
        self.machines.insert(machine.name.clone(), machine);
    }

    /// Every validation error, in a stable order. Empty when coherent.
    ///
    /// Which machines a deployment may run on together is not checked here:
    /// placement checks a gang against the pools it detects when it acquires
    /// the deployment, and says why when it cannot.
    pub fn problems(&self) -> Vec<ValidationError> {
        let mut problems = Vec::new();
        for profile in self.profiles.values() {
            problems.extend(profile.problems());
            if !self.models.contains_key(&profile.model.name) {
                problems.push(ValidationError::ProfileModelUnregistered {
                    profile: profile.id.clone(),
                    model: profile.model.name.clone(),
                });
            }
        }
        for machine in self.machines.values() {
            if machine.address.trim().is_empty() {
                problems.push(ValidationError::EmptyAddress {
                    machine: machine.name.clone(),
                });
            }
        }

        for deployment in self.deployments.values() {
            let id = || deployment.id.clone();
            match self.profiles.get(&deployment.profile) {
                None => problems.push(ValidationError::UnknownProfile {
                    deployment: id(),
                    profile: deployment.profile.clone(),
                }),
                Some(profile)
                    if profile.requirements.machines as usize != deployment.machines.len() =>
                {
                    problems.push(ValidationError::MachineCount {
                        deployment: id(),
                        profile: profile.id.clone(),
                        required: profile.requirements.machines,
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

    /// Every record that is not Cairn's own, as `kind id (source)`, profiles
    /// first, then deployments, then machines, each in id order.
    pub fn foreign_records(&self) -> Vec<String> {
        foreign_of("model", &self.models)
            .chain(foreign_of("profile", &self.profiles))
            .chain(foreign_of("deployment", &self.deployments))
            .chain(foreign_of("machine", &self.machines))
            .collect()
    }

    /// Make every record of `from` Cairn's own, keeping its id and contents,
    /// and name each one adopted. Adopting again adopts nothing.
    pub fn adopt(&mut self, from: RecordSource) -> Vec<String> {
        let mut adopted = Vec::new();
        adopt_kind("model", &mut self.models, from, &mut adopted);
        adopt_kind("profile", &mut self.profiles, from, &mut adopted);
        adopt_kind("deployment", &mut self.deployments, from, &mut adopted);
        adopt_kind("machine", &mut self.machines, from, &mut adopted);
        adopted
    }

    /// Add each of `records`' records, replacing the one with its id.
    pub fn upsert(&mut self, records: Inventory) {
        self.models.extend(records.models);
        self.profiles.extend(records.profiles);
        self.deployments.extend(records.deployments);
        self.machines.extend(records.machines);
    }

    /// What turning `current` into `next` adds, removes and updates.
    pub fn changes(current: &Inventory, next: &Inventory) -> ChangeReport {
        ChangeReport {
            models: diff_kind(&current.models, &next.models),
            profiles: diff_kind(&current.profiles, &next.profiles),
            deployments: diff_kind(&current.deployments, &next.deployments),
            machines: diff_kind(&current.machines, &next.machines),
        }
    }

    /// Cairn's own record format, as YAML.
    pub fn to_yaml(&self) -> String {
        let file = InventoryFile {
            models: self.models.values().cloned().collect(),
            profiles: self.profiles.values().cloned().collect(),
            deployments: self.deployments.values().cloned().collect(),
            machines: self.machines.values().cloned().collect(),
            retired_recipes: None,
        };
        serde_yaml::to_string(&file).expect("inventory records serialize to YAML")
    }

    /// The records a document in Cairn's own record format holds, keeping the
    /// first of any id written twice, and each id written twice.
    fn parse_records(text: &str) -> Result<(Inventory, Vec<ValidationError>), YamlError> {
        let file: InventoryFile =
            serde_yaml::from_str(text).map_err(|err| YamlError::Parse(err.to_string()))?;
        if file.retired_recipes.is_some() {
            return Err(YamlError::Parse(
                "`recipes:` is now `profiles:` (inference recipes were renamed profiles, and a deployment names its `profile:`)".into(),
            ));
        }
        let mut inventory = Inventory::default();
        let mut duplicates = Vec::new();
        let mut duplicate = |kind: &'static str, id: &dyn fmt::Display| {
            duplicates.push(ValidationError::DuplicateId {
                kind,
                id: id.to_string(),
            })
        };
        for model in file.models {
            match inventory.models.contains_key(&model.name) {
                true => duplicate("model", &model.name),
                false => inventory.insert_model(model),
            }
        }
        for profile in file.profiles {
            match inventory.profiles.contains_key(&profile.id) {
                true => duplicate("profile", &profile.id),
                false => inventory.insert_profile(profile),
            }
        }
        for deployment in file.deployments {
            match inventory.deployments.contains_key(&deployment.id) {
                true => duplicate("deployment", &deployment.id),
                false => inventory.insert_deployment(deployment),
            }
        }
        for machine in file.machines {
            match inventory.machines.contains_key(&machine.name) {
                true => duplicate("machine", &machine.name),
                false => inventory.insert_machine(machine),
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
    use crate::fixture::{
        command_profile, deployment, insert_command_profile, machine, test_inventory,
    };
    use crate::profile::Engine;

    fn problems_of(inventory: &Inventory) -> Vec<ValidationError> {
        inventory.problems()
    }

    #[test]
    fn the_test_inventory_is_valid() {
        assert_eq!(test_inventory().validate(), Ok(()));
    }

    /// Which machines a gang may span is placement's to judge against the
    /// pools it detects, not the inventory's.
    #[test]
    fn a_deployment_may_name_any_machines_its_profile_counts() {
        let mut inventory = test_inventory();
        insert_command_profile(&mut inventory, "straddle", 2);
        inventory.insert_deployment(deployment("straddle", &["sp1", "halo"]));
        assert_eq!(problems_of(&inventory), Vec::new());
    }

    #[test]
    fn a_machine_record_s_address_may_not_be_blank() {
        let mut blank = test_inventory();
        blank.insert_machine(machine("sp2", " "));
        assert_eq!(
            problems_of(&blank),
            vec![ValidationError::EmptyAddress {
                machine: "sp2".into()
            }]
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
    fn a_machine_count_unlike_the_profile_is_refused() {
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
                profile: "qwen36-fast".into(),
                required: 1,
                placed: 2,
            }]
        );
    }

    #[test]
    fn duplicate_machines_and_unknown_profiles_are_refused() {
        let mut inventory = test_inventory();
        inventory.insert_deployment(deployment("twice", &["sp1", "sp1"]));
        inventory
            .deployments
            .get_mut(&DeploymentId::from("twice"))
            .unwrap()
            .profile = "dsv4f-tp2".into();
        inventory.insert_deployment(deployment("orphan", &["sp1"]));
        let problems = problems_of(&inventory);
        assert!(
            problems.contains(&ValidationError::DuplicateDeploymentMachine {
                deployment: "twice".into(),
                machine: "sp1".into(),
            })
        );
        assert!(problems.contains(&ValidationError::UnknownProfile {
            deployment: "orphan".into(),
            profile: "orphan".into(),
        }));
    }

    /// Every record of the fixture, as bginf mirrored it before it retired.
    fn mirrored() -> Inventory {
        let mut inventory = test_inventory();
        for profile in inventory.profiles.values_mut() {
            profile.source = RecordSource::Bginf;
        }
        for deployment in inventory.deployments.values_mut() {
            deployment.source = RecordSource::Bginf;
        }
        for machine in inventory.machines.values_mut() {
            machine.source = RecordSource::Bginf;
        }
        inventory
    }

    #[test]
    fn adopting_makes_every_mirrored_record_cairns_and_is_idempotent() {
        let mut inventory = mirrored();
        insert_command_profile(&mut inventory, "own", 1);
        assert_eq!(inventory.foreign_records().len(), 8);
        assert_eq!(inventory.foreign_records()[0], "profile dsv4f-tp2 (bginf)");
        let before = inventory.clone();
        let adopted = inventory.adopt(RecordSource::Bginf);
        assert_eq!(
            adopted,
            vec![
                "profile dsv4f-tp2",
                "profile qwen3-embed-4b",
                "profile qwen36-fast",
                "deployment dsv4f-tp2",
                "deployment qwen3-embed-4b",
                "deployment qwen36-fast",
                "machine halo",
                "machine sp1",
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
        insert_command_profile(&mut inventory, "own", 1);
        inventory
    }

    #[test]
    fn changes_name_what_was_added_removed_and_updated() {
        let current = test_inventory();
        assert!(!Inventory::changes(&current, &current).has_changes());

        let mut next = current.clone();
        next.profiles
            .get_mut(&InferenceProfileId::from("qwen36-fast"))
            .unwrap()
            .engine = Engine::Command {
            start: "./start.sh --port 8002".into(),
            stop: Some("./stop.sh".into()),
            description: None,
        };
        next.insert_profile(command_profile("new", 1));
        next.machines.remove(&MachineName::from("halo"));
        assert_eq!(
            Inventory::changes(&current, &next).lines(),
            vec![
                "added profile new",
                "updated profile qwen36-fast: engine",
                "removed machine halo",
            ]
        );
    }

    #[test]
    fn an_upsert_adds_and_replaces_records_by_id() {
        let mut inventory = test_inventory();
        let mut records = Inventory::default();
        let mut changed = command_profile("qwen36-fast", 1);
        changed.served_name = "qwen3.6-35b".into();
        records.insert_profile(changed.clone());
        records.insert_profile(command_profile("new", 1));
        inventory.upsert(records);
        assert_eq!(
            inventory.profiles[&InferenceProfileId::from("qwen36-fast")],
            changed
        );
        assert!(inventory
            .profiles
            .contains_key(&InferenceProfileId::from("new")));
        assert_eq!(inventory.deployments.len(), 3);
    }

    #[test]
    fn the_yaml_format_round_trips_and_validates_on_import() {
        let inventory = test_inventory();
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
    fn a_document_with_the_retired_pools_section_is_refused_by_name() {
        let Err(YamlError::Parse(error)) =
            Inventory::from_yaml("pools:\n  - {id: sparks, machines: [{name: sp1}]}\n")
        else {
            panic!("pools are detected now, not written");
        };
        assert!(error.contains("unknown field `pools`"), "{error}");
        assert!(error.contains("machines"), "{error}");
    }

    #[test]
    fn a_refused_document_names_every_problem_at_once() {
        let yaml = r#"
profiles:
  - {id: r, model: {name: r}, servedName: r, engine: {kind: command, start: ./start.sh}, requirements: {machines: 1}}
  - {id: r, model: {name: r}, servedName: r, engine: {kind: command, start: ''}, requirements: {machines: 1}}
deployments:
  - {id: d, profile: missing, machines: [a], head: b, apiPort: 8000, apiPath: /v1, healthPath: /health}
machines:
  - {name: a, address: ''}
"#;
        let Err(YamlError::Invalid(problems)) = Inventory::from_yaml(yaml) else {
            panic!("the document is refused");
        };
        let problems: Vec<String> = problems.iter().map(ToString::to_string).collect();
        assert_eq!(
            problems,
            vec![
                "profile r appears twice",
                "profile r serves model r, which is not registered; register it at cairn://inference/models first",
                "machine a has an empty address",
                "deployment d names unknown profile missing",
                "deployment d has head b, which is not one of its machines",
            ]
        );
        assert!(Inventory::parse_yaml(yaml).is_err(), "an id written twice");
        assert!(matches!(
            Inventory::from_yaml("profiles: {"),
            Err(YamlError::Parse(_))
        ));
    }

    #[test]
    fn a_document_still_naming_recipes_is_refused_not_read_as_empty() {
        let refused = Inventory::parse_yaml(
            "recipes:\n  - {id: r, model: {name: r}, servedName: r, engine: {kind: command, start: ./s}, requirements: {machines: 1}}\n",
        )
        .unwrap_err()
        .to_string();
        assert!(
            refused.contains("`recipes:` is now `profiles:`"),
            "{refused}"
        );
        assert!(!Inventory::default().to_yaml().contains("recipes"));
    }

    #[test]
    fn a_record_without_a_source_is_cairns() {
        let parsed = Inventory::parse_yaml(
            "profiles:\n  - {id: r, model: {name: r}, servedName: r, engine: {kind: command, start: ./s}, requirements: {machines: 1}}\nmachines:\n  - {name: sp1, address: 10.20.20.40}\n",
        )
        .unwrap();
        assert_eq!(
            parsed.profiles[&InferenceProfileId::from("r")].source,
            RecordSource::Cairn
        );
        assert_eq!(
            parsed.machines[&MachineName::from("sp1")].source,
            RecordSource::Cairn
        );
    }

    #[test]
    fn models_round_trip_and_a_profile_needs_its_model_registered() {
        let mut inventory = test_inventory();
        let yaml = inventory.to_yaml();
        assert!(yaml.starts_with("models:\n- name: dsv4f-tp2\n"), "{yaml}");
        assert_eq!(Inventory::from_yaml(&yaml), Ok(inventory.clone()));

        inventory.models.remove("qwen36-fast");
        assert_eq!(
            inventory.problems(),
            vec![ValidationError::ProfileModelUnregistered {
                profile: "qwen36-fast".into(),
                model: "qwen36-fast".into(),
            }]
        );
        let mut next = inventory.clone();
        next.insert_model(crate::model::Model::named("qwen36-fast"));
        assert_eq!(
            Inventory::changes(&inventory, &next).lines(),
            vec!["added model qwen36-fast"]
        );
    }
}
