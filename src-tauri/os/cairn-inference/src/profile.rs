//! Profiles: how to run one model on a class of hardware.

use std::collections::{BTreeMap, BTreeSet};

use cairn_models_types::catalog::{Modalities, VerbKind};
use serde::{Deserialize, Serialize};

use crate::inventory::ValidationError;
use crate::record::RecordSource;

crate::string_id!(
    /// A profile's id, unique within the inventory.
    InferenceProfileId
);

/// How to run one model: its weights, the engine that serves it, and what
/// the hardware must provide.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProfile {
    pub id: InferenceProfileId,
    #[serde(default)]
    pub source: RecordSource,
    pub model: ModelRef,
    /// The model name the engine serves it under. A profile Cairn writes
    /// serves under the model's real name.
    pub served_name: String,
    /// The id catalog matching uses (an OpenRouter slug).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<String>,
    /// The verbs this profile serves. `None` is unknown, which the records
    /// adopted from bginf are, since its profiles never said; nothing guesses
    /// them from the engine's flags.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verbs: Option<BTreeSet<VerbKind>>,
    /// What the model takes in and gives out. `None` is unknown, for the same
    /// reason as `verbs`; what is served for an unknown profile is
    /// [`crate::served::modalities`]'s to say, never a guess from its name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Modalities>,
    /// The weights' quantization as a label (`nvfp4`, `fp8`, `bf16`, `q8_0`).
    /// `None` is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantization: Option<String>,
    /// Where the profile came from.
    #[serde(default, skip_serializing_if = "ProfileOrigin::is_authored")]
    pub origin: ProfileOrigin,
    pub engine: Engine,
    pub requirements: Requirements,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// Where a profile came from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ProfileOrigin {
    /// Written by hand.
    #[default]
    Authored,
    /// Compiled from a published compose file at a pinned commit.
    #[serde(rename_all = "camelCase")]
    Imported {
        url: String,
        commit: String,
        /// The compose file's path in the repository.
        compose: String,
        /// The compose service it was compiled from.
        service: String,
    },
    /// Written by a profile-generating agent session (tenet 1364) for the
    /// model at a revision. Profiles the retired template matcher wrote load
    /// as generated with no run, their `template` read as the starting
    /// template.
    #[serde(rename_all = "camelCase")]
    Generated {
        /// The generation run's node URI, stamped from the writer, never
        /// taken from what it wrote.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run: Option<String>,
        model_revision: String,
        /// The shipped template the agent started from, if any.
        #[serde(default, alias = "template", skip_serializing_if = "Option::is_none")]
        starting_template: Option<String>,
        /// What the agent cited for how it runs the model.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        sources: Vec<CitedSource>,
        /// The operator's answers to the agent's questions.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        answers: Vec<OperatorAnswer>,
    },
}

/// A source a generated profile cites.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CitedSource {
    pub url: String,
    #[serde(default)]
    pub title: String,
}

/// One question a generation asked the operator, and the answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorAnswer {
    pub question: String,
    pub answer: String,
}

impl ProfileOrigin {
    pub fn is_authored(&self) -> bool {
        matches!(self, Self::Authored)
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Authored => "authored",
            Self::Imported { .. } => "imported",
            Self::Generated { .. } => "generated",
        }
    }
}

/// The model a profile serves, with its weight files pinned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRef {
    /// The model's real name, the Hugging Face repository id of its checkpoint
    /// (`deepseek-ai/DeepSeek-V4-Flash-Vision-Exp`): the one name every caller
    /// asks for it by (tenet 1336). A quantized repository the weights come
    /// from is the profile's, not the model's name.
    pub name: String,
    /// Container and native profiles pin at least one weight file; command
    /// profiles may pin none, because their shell fetches whatever it fetches.
    /// The files may come from a quantized derivative's repository rather
    /// than the model's own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub weights: Vec<WeightFile>,
    /// The other files of the weights' repository a server reads beside
    /// them: vLLM's configuration, tokenizer and chat template. A GGUF file
    /// holds all of that itself, so a llama.cpp profile pins none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config_files: Vec<ConfigFile>,
}

impl ModelRef {
    /// Every repository a pinned file comes from, once each: the cache
    /// directories a container's Hugging Face cache mount reaches.
    pub fn repos(&self) -> BTreeSet<&str> {
        self.weights
            .iter()
            .map(|weight| weight.repo.as_str())
            .chain(self.config_files.iter().map(|file| file.repo.as_str()))
            .collect()
    }

    /// The repository and revision the weights come from: the first weight
    /// file's, or the model's own name with no revision when none is pinned.
    pub fn weights_source(&self) -> (&str, Option<&str>) {
        match self.weights.first() {
            Some(weight) => (weight.repo.as_str(), Some(weight.revision.as_str())),
            None => (self.name.as_str(), None),
        }
    }
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
    /// The file's size in bytes, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

impl WeightFile {
    /// Validate the components used both in the Hub URL and the local cache.
    /// This is lexical: the cache itself is trusted, including its symlinks.
    pub(crate) fn problem(&self) -> Option<&'static str> {
        if let Some(problem) = location_problem(&self.repo, &self.revision, &self.path) {
            return Some(problem);
        }
        if !is_sha256_hex(&self.sha256) {
            return Some("sha256 is not 64 lowercase hex digits");
        }
        None
    }
}

/// One file of a weight repository a server reads beside the weights, pinned
/// by repository, revision, path and hash. A file Hugging Face keeps in LFS
/// (a large `tokenizer.json`) is pinned by its content's SHA-256; any other
/// by its git blob id, the SHA-1 of `blob <size>\0` and its content, which is
/// the only hash the Hub states for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigFile {
    pub repo: String,
    pub revision: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_blob: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

impl ConfigFile {
    pub(crate) fn problem(&self) -> Option<&'static str> {
        if let Some(problem) = location_problem(&self.repo, &self.revision, &self.path) {
            return Some(problem);
        }
        match (&self.sha256, &self.git_blob) {
            (Some(sha256), None) if is_sha256_hex(sha256) => None,
            (None, Some(blob)) if is_lower_hex(blob, 40) => None,
            (Some(_), None) => Some("sha256 is not 64 lowercase hex digits"),
            (None, Some(_)) => Some("gitBlob is not 40 lowercase hex digits"),
            _ => Some("pin exactly one of sha256 (an LFS file) and gitBlob (any other)"),
        }
    }
}

/// Why a pinned file's repository, revision or path is unsafe to put in a
/// Hub URL or a cache path.
fn location_problem(repo: &str, revision: &str, path: &str) -> Option<&'static str> {
    {
        let repo_component = |part: &str| {
            !part.is_empty()
                && part.len() <= 96
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
                && !part.starts_with(['.', '-'])
                && !part.ends_with(['.', '-'])
                && !part.contains("..")
                && !part.contains("--")
        };
        let repo_ok = repo.split_once('/').is_some_and(|(owner, name)| {
            repo_component(owner) && repo_component(name) && !name.ends_with(".git")
        });
        if !repo_ok {
            return Some("repo must be a Hugging Face owner/name using letters, digits, _, - and . (no .. or --)");
        }
    }
    if revision.is_empty()
        || !revision
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        || revision.starts_with(['.', '-'])
        || revision.ends_with('.')
        || revision.contains("..")
        || revision.ends_with(".lock")
    {
        return Some(
            "revision must be a plain ref or sha using letters, digits, _, - and . (no / or ..)",
        );
    }
    if path.is_empty()
        || path.starts_with('/')
        || path
            .chars()
            .any(|c| c.is_control() || matches!(c, '\\' | ':' | '?' | '#' | '%'))
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Some("path must be relative with no empty, . or .. segment, backslash, control character or URL/drive delimiter (: ? # %)");
    }
    None
}

/// The engine that serves the model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Engine {
    /// A container image pinned by digest, and everything needed to run it.
    Container(Container),
    /// A pinned native artifact.
    Native {
        artifact: String,
        version: String,
        sha256: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<String>,
    },
    /// An operator's raw start and stop shell, run on the head as its
    /// executor's account. Every profile adopted from bginf is one;
    /// `description` is free text saying what the shell runs.
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
            Self::Container(_) => "container",
            Self::Native { .. } => "native",
            Self::Command { .. } => "command",
        }
    }
}

/// A container profile's complete runnable form: what
/// [`crate::render`] turns into the start and stop scripts its head runs.
///
/// `args`, `env` and each rank's env may hold the placeholders in
/// [`PLACEHOLDERS`], filled in per deployment and per rank when rendered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Container {
    /// `name@sha256:<64 hex>`.
    pub image: String,
    pub server: Server,
    /// Replaces the image's entrypoint; `Some` of an empty list clears it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    #[serde(default)]
    pub host: HostOptions,
    /// A repository checked out on every machine at a pinned commit, which
    /// [`MountSource::Source`] mounts read from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<SourcePin>,
    /// Extra env per rank, rank 0 (the head) first. On a profile of more than
    /// one machine there is exactly one entry per machine.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranks: Vec<BTreeMap<String, String>>,
}

/// The serving program inside the image, which says how it is health-checked
/// and benchmarked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Server {
    Vllm,
    LlamaCpp,
}

impl Server {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vllm => "vllm",
            Self::LlamaCpp => "llamaCpp",
        }
    }
}

/// One bind mount into the container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "MountRecord")]
pub struct Mount {
    pub from: MountSource,
    /// The path inside the container.
    pub to: String,
    /// Always true of a [`MountSource::HfCache`] mount: one written writable
    /// reads as read-only.
    #[serde(default)]
    pub read_only: bool,
}

/// A mount as written, before a Hugging Face cache mount is made read-only.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MountRecord {
    from: MountSource,
    to: String,
    #[serde(default)]
    read_only: bool,
}

impl From<MountRecord> for Mount {
    fn from(record: MountRecord) -> Self {
        let read_only = record.read_only || record.from == MountSource::HfCache;
        Self {
            from: record.from,
            to: record.to,
            read_only,
        }
    }
}

/// Where a mount's host side is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum MountSource {
    /// A path within the profile's pinned [`Container::files`] checkout.
    Source { path: String },
    /// The pinned repositories' directories in the machine's Hugging Face
    /// cache (`$HF_HOME`, or `~/.cache/huggingface`), read-only: for each
    /// repository [`ModelRef::repos`] names, `hub/models--<owner>--<name>`,
    /// at the same place under the mount's `to`. Nothing else of the cache is
    /// mounted, so its token and every other model's weights stay out of the
    /// container, and no engine can change weights another profile loads.
    /// Cairn fetches every pinned file before the container starts.
    HfCache,
}

/// How the container shares the host.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostOptions {
    #[serde(default)]
    pub ipc_host: bool,
    /// Host networking; without it the deployment's API port is published.
    #[serde(default)]
    pub network_host: bool,
    /// `/dev/shm` size as the runtime takes it (`64g`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shm_size: Option<String>,
    /// Give the container the machine's GPUs, in whatever form its
    /// accelerator family takes.
    #[serde(default)]
    pub gpus: bool,
    /// Further host devices, as paths (`/dev/infiniband`, or
    /// `host:container`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<String>,
    /// Resource limits by name, as `soft:hard` (`memlock: "-1:-1"`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ulimits: BTreeMap<String, String>,
}

/// A git repository pinned at a commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourcePin {
    /// A clone URL (`https://github.com/<owner>/<repo>`).
    pub url: String,
    /// The full 40-hex commit sha.
    pub commit: String,
}

/// The placeholders a container's args and env may hold, filled in when it
/// is rendered: the deployment's API port, the profile's served name, the
/// weights' repository and revision, the rank and machine count, the fast-link
/// address of the head and of the machine itself, and the in-container path of
/// the first weight file.
pub const PLACEHOLDERS: &[&str] = &[
    "port",
    "servedName",
    "model",
    "revision",
    "rank",
    "machines",
    "head.link",
    "self.link",
    "weights",
];

/// Every `{name}` placeholder in `text`: a brace, a letter, letters, digits
/// and dots, a brace. JSON in an argument (`{"method":...}`) is never one,
/// and neither is a shell expansion (`${HEADLESS}`), which the container's
/// own shell expands.
pub fn placeholders(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut index = 0;
    while let Some(offset) = text[index..].find('{') {
        let open = index + offset;
        index = open + 1;
        if open > 0 && bytes[open - 1] == b'$' {
            continue;
        }
        let rest = &text[open + 1..];
        let Some(close) = rest.find(['{', '}']) else {
            break;
        };
        if rest.as_bytes()[close] == b'{' {
            continue;
        }
        let name = &rest[..close];
        let well_formed = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '.');
        if well_formed {
            found.push(name);
            index = open + 1 + close + 1;
        }
    }
    found
}

impl Container {
    /// Every string the renderer fills placeholders in.
    fn templated(&self) -> impl Iterator<Item = &str> {
        self.args
            .iter()
            .map(String::as_str)
            .chain(self.env.values().map(String::as_str))
            .chain(
                self.ranks
                    .iter()
                    .flat_map(|env| env.values().map(String::as_str)),
            )
    }
}

/// What the hardware must provide. Each optional field is `None` when
/// unknown, which imported profiles mostly are; renderings say "unknown".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Requirements {
    /// How many machines one deployment of this profile occupies (a gang).
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AcceleratorFamily {
    Nvidia,
    Amd,
    Apple,
}

impl AcceleratorFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nvidia => "nvidia",
            Self::Amd => "amd",
            Self::Apple => "apple",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AcceleratorRuntime {
    Cuda,
    Rocm,
    Vulkan,
    Metal,
}

impl AcceleratorRuntime {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cuda => "cuda",
            Self::Rocm => "rocm",
            Self::Vulkan => "vulkan",
            Self::Metal => "metal",
        }
    }
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

/// A profile's memory footprint on each machine, typed by memory kind.
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
    is_lower_hex(value, 64)
}

/// Whether `value` is a full git commit sha: 40 lowercase hex digits.
pub fn is_commit_sha(value: &str) -> bool {
    is_lower_hex(value, 40)
}

fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Whether `image` is pinned by digest: `name@sha256:<64 hex>`.
pub fn is_pinned_image(image: &str) -> bool {
    image
        .rsplit_once("@sha256:")
        .is_some_and(|(name, digest)| !name.is_empty() && is_sha256_hex(digest))
}

impl InferenceProfile {
    /// This profile's own problems, independent of the rest of the inventory.
    pub fn problems(&self) -> Vec<ValidationError> {
        let mut problems = Vec::new();
        let profile = || self.id.clone();
        if self.requirements.machines == 0 {
            problems.push(ValidationError::ProfileZeroMachines { profile: profile() });
        }
        for (index, weight) in self.model.weights.iter().enumerate() {
            if let Some(problem) = weight.problem() {
                problems.push(ValidationError::BadWeightFile {
                    profile: profile(),
                    index,
                    problem,
                });
            }
        }
        for (index, file) in self.model.config_files.iter().enumerate() {
            if let Some(problem) = file.problem() {
                problems.push(ValidationError::BadConfigFile {
                    profile: profile(),
                    index,
                    problem,
                });
            }
        }
        match &self.engine {
            Engine::Container(container) => {
                problems.extend(self.container_problems(container));
            }
            Engine::Native { sha256, .. } => {
                if !is_sha256_hex(sha256) {
                    problems.push(ValidationError::BadArtifactSha { profile: profile() });
                }
            }
            Engine::Command { start, .. } => {
                if start.trim().is_empty() {
                    problems.push(ValidationError::EmptyStart { profile: profile() });
                }
            }
        }
        if !matches!(self.engine, Engine::Command { .. }) && self.model.weights.is_empty() {
            problems.push(ValidationError::UnpinnedWeights { profile: profile() });
        }
        if matches!(self.origin, ProfileOrigin::Generated { .. })
            && self.requirements.memory.is_none()
        {
            problems.push(ValidationError::GeneratedWithoutMemory { profile: profile() });
        }
        problems
    }

    fn container_problems(&self, container: &Container) -> Vec<ValidationError> {
        let mut problems = Vec::new();
        let profile = || self.id.clone();
        if !is_pinned_image(&container.image) {
            problems.push(ValidationError::BadImageDigest {
                profile: profile(),
                image: container.image.clone(),
            });
        }
        let mut unknown = BTreeSet::new();
        for text in container.templated() {
            for name in placeholders(text) {
                if !PLACEHOLDERS.contains(&name) {
                    unknown.insert(name.to_string());
                }
            }
        }
        for placeholder in unknown {
            problems.push(ValidationError::UnknownPlaceholder {
                profile: profile(),
                placeholder,
            });
        }
        let machines = self.requirements.machines as usize;
        let ranks_fit = if machines > 1 {
            container.ranks.len() == machines
        } else {
            container.ranks.len() <= 1
        };
        if !ranks_fit {
            problems.push(ValidationError::RanksMismatch {
                profile: profile(),
                ranks: container.ranks.len(),
                machines: self.requirements.machines,
            });
        }
        match &container.files {
            Some(pin) if !is_commit_sha(&pin.commit) => {
                problems.push(ValidationError::BadSourceCommit {
                    profile: profile(),
                    commit: pin.commit.clone(),
                });
            }
            Some(_) => {}
            None => {
                for mount in &container.mounts {
                    if let MountSource::Source { path } = &mount.from {
                        problems.push(ValidationError::UnpinnedSourceMount {
                            profile: profile(),
                            path: path.clone(),
                        });
                    }
                }
            }
        }
        problems
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(engine: Engine, weights: Vec<WeightFile>) -> InferenceProfile {
        InferenceProfile {
            id: "r".into(),
            source: RecordSource::Cairn,
            model: ModelRef {
                name: "m".into(),
                weights,
                config_files: vec![],
            },
            served_name: "m".into(),
            upstream_id: None,
            verbs: None,
            modalities: None,
            quantization: None,
            origin: ProfileOrigin::Authored,
            engine,
            requirements: Requirements {
                machines: 1,
                accelerator: None,
                interconnect: None,
                memory: None,
            },
            notes: None,
        }
    }

    fn weight(sha256: &str) -> WeightFile {
        WeightFile {
            repo: "org/model".into(),
            revision: "abc123".into(),
            path: "model.gguf".into(),
            sha256: sha256.into(),
            size: None,
        }
    }

    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    fn container(image: String) -> Container {
        Container {
            image,
            server: Server::Vllm,
            entrypoint: None,
            args: vec![],
            env: BTreeMap::new(),
            mounts: vec![],
            host: HostOptions::default(),
            files: None,
            ranks: vec![],
        }
    }

    #[test]
    fn a_container_must_be_pinned_by_digest_and_pin_its_weights() {
        let tagged = profile(
            Engine::Container(container("ghcr.io/org/server:latest".into())),
            vec![weight(SHA)],
        );
        assert!(matches!(
            tagged.problems().as_slice(),
            [ValidationError::BadImageDigest { .. }]
        ));

        let unweighted = profile(
            Engine::Container(container(format!("ghcr.io/org/server@sha256:{SHA}"))),
            vec![],
        );
        assert!(matches!(
            unweighted.problems().as_slice(),
            [ValidationError::UnpinnedWeights { .. }]
        ));

        let pinned = profile(
            Engine::Container(container(format!("ghcr.io/org/server@sha256:{SHA}"))),
            vec![weight(SHA)],
        );
        assert_eq!(pinned.problems(), vec![]);
    }

    #[test]
    fn placeholders_are_braced_names_and_json_is_not_one() {
        assert_eq!(
            placeholders("--port {port} --x {head.link}:{rank}"),
            vec!["port", "head.link", "rank"]
        );
        assert!(placeholders(r#"{"method":"dspark","n":5}"#).is_empty());
        assert_eq!(placeholders("{{port}}"), vec!["port"]);
        assert!(placeholders("--node-rank ${NODE_RANK} ${HEADLESS:+--headless} ${A}").is_empty());
        assert_eq!(placeholders("${A}{port}"), vec!["port"]);
    }

    #[test]
    fn a_container_is_refused_for_unknown_placeholders_ranks_and_unpinned_sources() {
        let mut engine = container(format!("ghcr.io/org/server@sha256:{SHA}"));
        engine.args = vec!["--port".into(), "{port}".into(), "{gpuCount}".into()];
        engine.env.insert("A".into(), "{nope}".into());
        engine.mounts.push(Mount {
            from: MountSource::Source {
                path: "patches".into(),
            },
            to: "/opt/patches".into(),
            read_only: true,
        });
        engine.ranks = vec![BTreeMap::new()];
        let mut gang = profile(Engine::Container(engine.clone()), vec![weight(SHA)]);
        gang.requirements.machines = 2;
        let problems: Vec<String> = gang.problems().iter().map(ToString::to_string).collect();
        assert_eq!(
            problems,
            vec![
                "profile r uses unknown placeholder {gpuCount}",
                "profile r uses unknown placeholder {nope}",
                "profile r runs on 2 machines but gives env for 1 rank; give one entry per machine",
                "profile r mounts patches from its source files but pins no source (files: {url, commit})",
            ]
        );

        engine.files = Some(SourcePin {
            url: "https://github.com/o/r".into(),
            commit: "f3d7645".into(),
        });
        engine.args = vec!["{port}".into()];
        engine.env.clear();
        engine.ranks = vec![BTreeMap::new(), BTreeMap::new()];
        gang.engine = Engine::Container(engine.clone());
        assert!(matches!(
            gang.problems().as_slice(),
            [ValidationError::BadSourceCommit { .. }]
        ));
        engine.files.as_mut().unwrap().commit = COMMIT.into();
        gang.engine = Engine::Container(engine);
        assert_eq!(gang.problems(), vec![]);
    }

    #[test]
    fn malformed_shas_are_refused() {
        let native = profile(
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
    fn a_command_profile_may_pin_no_weights() {
        let command = profile(
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
    fn the_container_form_round_trips_through_yaml() {
        let mut engine = container(format!("ghcr.io/org/server@sha256:{SHA}"));
        engine.server = Server::LlamaCpp;
        engine.entrypoint = Some(vec![]);
        engine.mounts.push(Mount {
            from: MountSource::HfCache,
            to: "/root/.cache/huggingface".into(),
            read_only: true,
        });
        engine.host.gpus = true;
        let mut generated = profile(Engine::Container(engine), vec![weight(SHA)]);
        generated.origin = ProfileOrigin::Generated {
            run: Some("cairn://p/cairn/9/1/workflow".into()),
            model_revision: "abc".into(),
            starting_template: Some("llamacpp-vulkan".into()),
            sources: vec![CitedSource {
                url: "https://github.com/ggml-org/llama.cpp".into(),
                title: "llama.cpp".into(),
            }],
            answers: vec![OperatorAnswer {
                question: "Quantization".into(),
                answer: "q8_0".into(),
            }],
        };
        generated.quantization = Some("q8_0".into());
        let yaml = serde_yaml::to_string(&generated).unwrap();
        assert!(yaml.contains("kind: container"), "{yaml}");
        assert!(yaml.contains("server: llamaCpp"), "{yaml}");
        assert!(yaml.contains("kind: hfCache"), "{yaml}");
        assert!(yaml.contains("modelRevision: abc"), "{yaml}");
        assert!(yaml.contains("startingTemplate: llamacpp-vulkan"), "{yaml}");
        assert_eq!(
            serde_yaml::from_str::<InferenceProfile>(&yaml).unwrap(),
            generated
        );
    }

    #[test]
    fn a_profile_the_template_matcher_wrote_still_loads_as_generated_with_no_run() {
        let origin: ProfileOrigin =
            serde_yaml::from_str("kind: generated\ntemplate: llamacpp-cuda\nmodelRevision: abc\n")
                .unwrap();
        assert_eq!(
            origin,
            ProfileOrigin::Generated {
                run: None,
                model_revision: "abc".into(),
                starting_template: Some("llamacpp-cuda".into()),
                sources: vec![],
                answers: vec![],
            }
        );
    }

    #[test]
    fn a_generated_profile_must_state_its_memory() {
        let mut generated = profile(
            Engine::Container(container(format!("ghcr.io/org/server@sha256:{SHA}"))),
            vec![weight(SHA)],
        );
        generated.origin = ProfileOrigin::Generated {
            run: None,
            model_revision: "abc".into(),
            starting_template: None,
            sources: vec![],
            answers: vec![],
        };
        let problems: Vec<String> = generated
            .problems()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            problems,
            vec!["profile r is generated but states no memory need (requirements.memory)"]
        );
        generated.requirements.memory = Some(MemoryPerMachine::Discrete {
            vram: VramBytes(1),
            host: SystemRamBytes(0),
        });
        assert_eq!(generated.problems(), vec![]);
    }

    #[test]
    fn memory_kinds_compare_only_within_their_kind() {
        assert!(SystemRamBytes(1) < SystemRamBytes(2));
        assert!(VramBytes(1) < VramBytes(2));
    }
}
