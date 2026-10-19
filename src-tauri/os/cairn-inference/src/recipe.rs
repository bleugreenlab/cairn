//! Recipes: how to run one model on a class of hardware.

use std::collections::BTreeSet;

use cairn_models_types::catalog::VerbKind;
use serde::{Deserialize, Serialize};

use crate::inventory::ValidationError;
use crate::pool::RecordSource;

crate::string_id!(
    /// A recipe's id, unique within the inventory.
    RecipeId
);

/// How to run one model: its weights, the engine that serves it, and what
/// the hardware must provide.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Recipe {
    pub id: RecipeId,
    pub source: RecordSource,
    pub model: ModelRef,
    /// The model name the engine serves it under.
    pub served_name: String,
    /// The id catalog matching uses (bginf's `catalog_id`, an OpenRouter slug).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<String>,
    /// The verbs this recipe serves. `None` is unknown: bginf profiles never
    /// say, so imported recipes leave it unset rather than guessing from the
    /// engine's flags.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verbs: Option<BTreeSet<VerbKind>>,
    pub engine: Engine,
    pub requirements: Requirements,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// The model a recipe serves, with its weight files pinned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRef {
    pub name: String,
    /// Container and native recipes pin at least one weight file; command
    /// recipes may pin none, because their shell fetches whatever it fetches.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub weights: Vec<WeightFile>,
}

/// One weight file, pinned by Hugging Face repository, revision, path and
/// content hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WeightFile {
    pub repo: String,
    pub revision: String,
    pub path: String,
    /// Lowercase hex SHA-256 of the file.
    pub sha256: String,
}

/// The engine that serves the model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Engine {
    /// A container image pinned by digest (`...@sha256:<64 hex>`).
    Container {
        image: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<String>,
    },
    /// A pinned native artifact.
    Native {
        artifact: String,
        version: String,
        sha256: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<String>,
    },
    /// An operator's raw start and stop shell, as bginf runs today. Every
    /// imported recipe is one; `description` carries bginf's free-text
    /// `engine`.
    Command {
        start: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
}

impl Engine {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Container { .. } => "container",
            Self::Native { .. } => "native",
            Self::Command { .. } => "command",
        }
    }
}

/// What the hardware must provide. Each optional field is `None` when
/// unknown, which imported recipes mostly are; renderings say "unknown".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Requirements {
    /// How many machines one deployment of this recipe occupies (a gang).
    pub machines: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accelerator: Option<Accelerator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interconnect: Option<Interconnect>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryPerMachine>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Accelerator {
    pub family: AcceleratorFamily,
    pub runtime: AcceleratorRuntime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AcceleratorFamily {
    Nvidia,
    Amd,
    Apple,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AcceleratorRuntime {
    Cuda,
    Rocm,
    Vulkan,
    Metal,
}

/// How a multi-machine deployment's machines talk to each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Interconnect {
    /// The ordinary network between the machines.
    Lan,
    /// A direct RDMA-capable link (the Sparks' ConnectX-7 cable).
    Rdma,
}

/// A recipe's memory footprint on each machine, typed by memory kind.
///
/// On a unified-memory machine (a GB10 Spark, a Radeon 8060S, Apple silicon)
/// the footprint is system RAM only; on a discrete card it is VRAM plus host
/// RAM. The two byte types are distinct, so a requirement can never be
/// compared against the wrong kind of memory.
///
/// This is a requirement, not a reading. The measured counterpart is the
/// executor protocol's `GpuMemory::{Unified, Discrete}`, which is what the
/// placement matcher compares against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum MemoryPerMachine {
    Unified {
        system: SystemRamBytes,
    },
    Discrete {
        vram: VramBytes,
        host: SystemRamBytes,
    },
}

/// Bytes of system RAM. Ordered only against itself:
///
/// ```compile_fail
/// use cairn_inference::{SystemRamBytes, VramBytes};
/// let fits = VramBytes(8 << 30) < SystemRamBytes(16 << 30);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SystemRamBytes(pub u64);

/// Bytes of discrete-card VRAM. Ordered only against itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VramBytes(pub u64);

pub(crate) fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Recipe {
    /// This recipe's own problems, independent of the rest of the inventory.
    pub fn problems(&self) -> Vec<ValidationError> {
        let mut problems = Vec::new();
        let recipe = || self.id.clone();
        if self.requirements.machines == 0 {
            problems.push(ValidationError::RecipeZeroMachines { recipe: recipe() });
        }
        for (index, weight) in self.model.weights.iter().enumerate() {
            let problem = if weight.repo.is_empty() {
                Some("no repository")
            } else if weight.revision.is_empty() {
                Some("no revision")
            } else if weight.path.is_empty() {
                Some("no path")
            } else if !is_sha256_hex(&weight.sha256) {
                Some("sha256 is not 64 lowercase hex digits")
            } else {
                None
            };
            if let Some(problem) = problem {
                problems.push(ValidationError::BadWeightFile {
                    recipe: recipe(),
                    index,
                    problem,
                });
            }
        }
        match &self.engine {
            Engine::Container { image, .. } => {
                let pinned = image
                    .rsplit_once("@sha256:")
                    .is_some_and(|(name, digest)| !name.is_empty() && is_sha256_hex(digest));
                if !pinned {
                    problems.push(ValidationError::BadImageDigest {
                        recipe: recipe(),
                        image: image.clone(),
                    });
                }
            }
            Engine::Native { sha256, .. } => {
                if !is_sha256_hex(sha256) {
                    problems.push(ValidationError::BadArtifactSha { recipe: recipe() });
                }
            }
            Engine::Command { start, .. } => {
                if start.trim().is_empty() {
                    problems.push(ValidationError::EmptyStart { recipe: recipe() });
                }
            }
        }
        if !matches!(self.engine, Engine::Command { .. }) && self.model.weights.is_empty() {
            problems.push(ValidationError::UnpinnedWeights { recipe: recipe() });
        }
        problems
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recipe(engine: Engine, weights: Vec<WeightFile>) -> Recipe {
        Recipe {
            id: "r".into(),
            source: RecordSource::Cairn,
            model: ModelRef {
                name: "m".into(),
                weights,
            },
            served_name: "m".into(),
            upstream_id: None,
            verbs: None,
            engine,
            requirements: Requirements {
                machines: 1,
                accelerator: None,
                interconnect: None,
                memory: None,
            },
            aliases: Vec::new(),
            notes: None,
        }
    }

    fn weight(sha256: &str) -> WeightFile {
        WeightFile {
            repo: "org/model".into(),
            revision: "abc123".into(),
            path: "model.gguf".into(),
            sha256: sha256.into(),
        }
    }

    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn a_container_must_be_pinned_by_digest_and_pin_its_weights() {
        let tagged = recipe(
            Engine::Container {
                image: "ghcr.io/org/server:latest".into(),
                args: vec![],
            },
            vec![weight(SHA)],
        );
        assert!(matches!(
            tagged.problems().as_slice(),
            [ValidationError::BadImageDigest { .. }]
        ));

        let unweighted = recipe(
            Engine::Container {
                image: format!("ghcr.io/org/server@sha256:{SHA}"),
                args: vec![],
            },
            vec![],
        );
        assert!(matches!(
            unweighted.problems().as_slice(),
            [ValidationError::UnpinnedWeights { .. }]
        ));

        let pinned = recipe(
            Engine::Container {
                image: format!("ghcr.io/org/server@sha256:{SHA}"),
                args: vec![],
            },
            vec![weight(SHA)],
        );
        assert_eq!(pinned.problems(), vec![]);
    }

    #[test]
    fn malformed_shas_are_refused() {
        let native = recipe(
            Engine::Native {
                artifact: "llama-server".into(),
                version: "b1".into(),
                sha256: SHA.to_uppercase(),
                args: vec![],
            },
            vec![weight("short")],
        );
        let problems = native.problems();
        assert!(problems
            .iter()
            .any(|p| matches!(p, ValidationError::BadArtifactSha { .. })));
        assert!(problems
            .iter()
            .any(|p| matches!(p, ValidationError::BadWeightFile { index: 0, .. })));
    }

    #[test]
    fn a_command_recipe_may_pin_no_weights() {
        let command = recipe(
            Engine::Command {
                start: "./start.sh".into(),
                stop: None,
                description: None,
            },
            vec![],
        );
        assert_eq!(command.problems(), vec![]);
    }

    #[test]
    fn memory_kinds_compare_only_within_their_kind() {
        assert!(SystemRamBytes(1) < SystemRamBytes(2));
        assert!(VramBytes(1) < VramBytes(2));
    }
}
