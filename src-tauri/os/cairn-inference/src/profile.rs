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
    /// them: vLLM's and MLX's configuration, tokenizer and chat template. A
    /// GGUF file holds all of that itself, so a llama.cpp profile pins none.
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
    /// A pinned artifact run as a process on the machine itself: the only
    /// way to Apple silicon's GPU, which no container reaches.
    Native(Native),
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
            Self::Native(_) => "native",
            Self::Command { .. } => "command",
        }
    }

    /// The serving program, for the engines whose record names it.
    pub fn server(&self) -> Option<Server> {
        match self {
            Self::Container(container) => Some(container.server),
            Self::Native(native) => Some(native.server),
            Self::Command { .. } => None,
        }
    }

    /// The arguments the serving program is given; none for a command.
    pub fn args(&self) -> &[String] {
        match self {
            Self::Container(container) => &container.args,
            Self::Native(native) => &native.args,
            Self::Command { .. } => &[],
        }
    }

    pub fn args_mut(&mut self) -> Option<&mut Vec<String>> {
        match self {
            Self::Container(container) => Some(&mut container.args),
            Self::Native(native) => Some(&mut native.args),
            Self::Command { .. } => None,
        }
    }

    /// Whether Cairn renders it and verification can measure it: a
    /// container or native engine, never an operator's opaque command.
    pub fn is_runnable_form(&self) -> bool {
        !matches!(self, Self::Command { .. })
    }
}

/// A native profile's complete runnable form: what [`crate::render`] turns
/// into the start and stop scripts its machine runs. Native profiles run on
/// one machine.
///
/// `args` and `env` may hold the placeholders in [`PLACEHOLDERS`]. Every
/// pinned file is read from the machine's own Hugging Face cache, which
/// `{cache}`, `{snapshot}` and `{weights}` name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Native {
    pub server: Server,
    pub artifact: Artifact,
    /// The serving program: a path inside an unpacked archive
    /// (`llama-b12283/llama-server`), or a console script of an installed
    /// wheel (`mlx_lm.server`).
    pub program: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// A downloadable build of a serving program, pinned by its content hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    /// An `https` URL whose last path segment is the file's name.
    pub url: String,
    /// The upstream release it is (`b12283`, `0.32.2`), as its publisher
    /// names it.
    pub version: String,
    /// Lowercase hex SHA-256 of the file.
    pub sha256: String,
    pub packaging: Packaging,
}

/// What the artifact file is, which says how it becomes runnable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Packaging {
    /// A `.tar.gz` unpacked whole; [`Native::program`] is a path inside it.
    Archive,
    /// A Python wheel installed, with its dependencies, into an environment
    /// of its own by the `uv` Cairn provisions on every executor. The wheel
    /// is pinned by its hash; its dependencies resolve only to releases
    /// published before `resolvedBefore`, so the same profile installs the
    /// same environment wherever it runs.
    #[serde(rename_all = "camelCase")]
    Wheel {
        /// The Python version the environment is built on (`3.12`).
        python: String,
        /// A date (`2026-10-17`) or UTC instant (`2026-10-17T00:00:00Z`).
        resolved_before: String,
    },
}

impl Artifact {
    /// The file's name: the URL's last path segment.
    pub fn file_name(&self) -> &str {
        let path = self.url.split(['?', '#']).next().unwrap_or(&self.url);
        path.rsplit('/').next().unwrap_or(path)
    }

    /// What is lexically wrong with the pin, so a script never fetches from
    /// or writes to somewhere it should not.
    pub(crate) fn problem(&self) -> Option<&'static str> {
        if !is_sha256_hex(&self.sha256) {
            return Some("sha256 is not 64 lowercase hex digits");
        }
        if !self.url.starts_with("https://")
            || self
                .url
                .chars()
                .any(|c| c.is_control() || c.is_whitespace() || matches!(c, '\'' | '"' | '\\'))
        {
            return Some(
                "url must be https, with no whitespace, quote, backslash or control character",
            );
        }
        let name = self.file_name();
        if name.is_empty()
            || name.starts_with('.')
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'+'))
        {
            return Some(
                "the url's last segment must be a file name of letters, digits, ., _, - and +",
            );
        }
        if self.version.trim().is_empty() || self.version.chars().any(char::is_control) {
            return Some("version must be the upstream release's name");
        }
        match &self.packaging {
            Packaging::Archive if !(name.ends_with(".tar.gz") || name.ends_with(".tgz")) => {
                Some("an archive artifact is a .tar.gz")
            }
            Packaging::Wheel { .. } if !name.ends_with(".whl") => {
                Some("a wheel artifact's file name ends in .whl, which uv needs to install it")
            }
            Packaging::Wheel { python, .. }
                if !python.split_once('.').is_some_and(|(major, minor)| {
                    [major, minor].iter().all(|part| {
                        !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())
                    })
                }) =>
            {
                Some("a wheel's python is a version like 3.12")
            }
            Packaging::Wheel {
                resolved_before, ..
            } if !is_cutoff(resolved_before) => Some(
                "a wheel's resolvedBefore is a date (2026-10-17) or a UTC instant (2026-10-17T00:00:00Z)",
            ),
            _ => None,
        }
    }
}

/// `YYYY-MM-DD`, or `YYYY-MM-DDTHH:MM:SSZ`: what `uv --exclude-newer` takes.
fn is_cutoff(value: &str) -> bool {
    let shape = |text: &str, pattern: &str| {
        text.len() == pattern.len()
            && text.bytes().zip(pattern.bytes()).all(|(c, p)| match p {
                b'9' => c.is_ascii_digit(),
                _ => c == p,
            })
    };
    shape(value, "9999-99-99") || shape(value, "9999-99-99T99:99:99Z")
}

impl Native {
    /// Every string the renderer fills placeholders in.
    fn templated(&self) -> impl Iterator<Item = &str> {
        self.args
            .iter()
            .chain(self.env.values())
            .map(String::as_str)
    }

    /// What is wrong with the program the artifact runs.
    pub(crate) fn program_problem(&self) -> Option<&'static str> {
        let segment_ok = |part: &str| {
            !matches!(part, "" | "." | "..")
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'+'))
        };
        if !self.program.split('/').all(segment_ok) {
            return Some("program must be a relative path of letters, digits, ., _, - and +, with no empty, . or .. segment");
        }
        match (&self.artifact.packaging, self.server) {
            (Packaging::Wheel { .. }, _) if self.program.contains('/') => {
                Some("a wheel's program is one of its console scripts, a name with no /")
            }
            (Packaging::Wheel { .. }, Server::MlxLm) if self.program != MLX_PROGRAM => {
                Some("an mlxLm server runs mlx_lm.server")
            }
            (Packaging::Archive, Server::MlxLm) => {
                Some("an mlxLm server comes as the mlx-lm wheel")
            }
            _ => None,
        }
    }
}

/// The console script an MLX server runs, which the renderer starts through
/// a launcher of its own ([`crate::render`]).
pub const MLX_PROGRAM: &str = "mlx_lm.server";

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

/// The serving program an engine runs, which says how it is started,
/// health-checked and benchmarked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Server {
    Vllm,
    LlamaCpp,
    /// `mlx_lm.server`, Apple's MLX: native only, on Apple silicon.
    MlxLm,
}

impl Server {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vllm => "vllm",
            Self::LlamaCpp => "llamaCpp",
            Self::MlxLm => "mlxLm",
        }
    }

    /// Its name as people write it.
    pub fn label(self) -> &'static str {
        match self {
            Self::Vllm => "vLLM",
            Self::LlamaCpp => "llama.cpp",
            Self::MlxLm => "MLX",
        }
    }

    /// The port a deployment of it serves on unless placed otherwise.
    pub fn default_port(self) -> u16 {
        match self {
            Self::Vllm => 8000,
            Self::LlamaCpp => 8090,
            Self::MlxLm => 8080,
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

/// The placeholders a container's or native engine's args and env may hold,
/// filled in when it is rendered: the deployment's API port, the profile's
/// served name, the weights' repository and revision, the rank and machine
/// count, the fast-link address of the head and of the machine itself, and
/// where the server reads the pinned files: the Hugging Face cache
/// (`{cache}`, the cache mount's path in a container, the machine's own
/// cache natively), the weights' snapshot directory in it (`{snapshot}`,
/// which a server loading a whole repository takes), and the first weight
/// file (`{weights}`).
pub const PLACEHOLDERS: &[&str] = &[
    "port",
    "servedName",
    "model",
    "revision",
    "rank",
    "machines",
    "head.link",
    "self.link",
    "cache",
    "snapshot",
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

/// Whether `name` names a secret: any word (split at `_`) of `SECRET`,
/// `PASSWORD`, `PASSWD`, `PASSPHRASE`, `CREDENTIAL(S)`, `APIKEY(S)` or `PAT`,
/// or a last word of `TOKEN`, `KEY` or `KEYS` (`HF_TOKEN`, `VLLM_API_KEY`).
/// `MAX_NUM_BATCHED_TOKENS` and `LONG_PREFILL_TOKEN_THRESHOLD` are counts, so
/// `TOKENS` and a `TOKEN` before another word are not.
pub fn is_secret_name(name: &str) -> bool {
    const ANYWHERE: &[&str] = &[
        "SECRET",
        "SECRETS",
        "PASSWORD",
        "PASSWORDS",
        "PASSWD",
        "PASSPHRASE",
        "CREDENTIAL",
        "CREDENTIALS",
        "APIKEY",
        "APIKEYS",
        "PAT",
    ];
    let upper = name.to_ascii_uppercase();
    let words: Vec<&str> = upper.split('_').filter(|w| !w.is_empty()).collect();
    words.iter().any(|word| ANYWHERE.contains(word))
        || words
            .last()
            .is_some_and(|last| matches!(*last, "TOKEN" | "KEY" | "KEYS"))
}

/// Every secret-named variable `value` reads in Compose's grammar (`$NAME`,
/// `${NAME}`, `${NAME:-word}` and the other operators, nested words
/// included), skipping the `$$` escape.
///
/// The renderer passes an engine's env as written and nothing at start
/// expands it, so such a reference in an env value is the reference's own
/// text standing where a credential should be.
/// Only env is read: a container's args are shell text its own shell may
/// expand from its env, so `${HF_TOKEN}` there is deliberate.
pub fn secret_references(value: &str) -> Vec<&str> {
    let mut found = Vec::new();
    let mut rest = value;
    while let Some(at) = rest.find('$') {
        let after = &rest[at + 1..];
        if let Some(tail) = after.strip_prefix('$') {
            rest = tail;
            continue;
        }
        let body = after.strip_prefix('{').unwrap_or(after);
        let len = body
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(body.len());
        let name = &body[..len];
        if name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') && is_secret_name(name) {
            found.push(name);
        }
        rest = after;
    }
    found
}

impl Container {
    /// Every env value with its key and, for a rank's own env, its rank.
    pub fn env_entries(&self) -> impl Iterator<Item = (Option<usize>, &str, &str)> {
        self.env
            .iter()
            .map(|(key, value)| (None, key.as_str(), value.as_str()))
            .chain(self.ranks.iter().enumerate().flat_map(|(rank, env)| {
                env.iter()
                    .map(move |(key, value)| (Some(rank), key.as_str(), value.as_str()))
            }))
    }

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
            Engine::Native(native) => problems.extend(self.native_problems(native)),
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

    fn unknown_placeholders<'a>(
        &self,
        texts: impl Iterator<Item = &'a str>,
    ) -> impl Iterator<Item = ValidationError> + '_ {
        let mut unknown = BTreeSet::new();
        for text in texts {
            for name in placeholders(text) {
                if !PLACEHOLDERS.contains(&name) {
                    unknown.insert(name.to_string());
                }
            }
        }
        unknown
            .into_iter()
            .map(|placeholder| ValidationError::UnknownPlaceholder {
                profile: self.id.clone(),
                placeholder,
            })
    }

    fn native_problems(&self, native: &Native) -> Vec<ValidationError> {
        let profile = || self.id.clone();
        let mut problems: Vec<ValidationError> =
            [native.artifact.problem(), native.program_problem()]
                .into_iter()
                .flatten()
                .map(|problem| ValidationError::BadArtifact {
                    profile: profile(),
                    problem,
                })
                .collect();
        problems.extend(self.unknown_placeholders(native.templated()));
        for (key, value) in &native.env {
            for reference in secret_references(value) {
                problems.push(ValidationError::UnresolvedSecret {
                    profile: profile(),
                    key: key.clone(),
                    rank: None,
                    reference: reference.to_string(),
                });
            }
        }
        if self.requirements.machines > 1 {
            problems.push(ValidationError::NativeSpansMachines {
                profile: profile(),
                machines: self.requirements.machines,
            });
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
        problems.extend(self.unknown_placeholders(container.templated()));
        for (rank, key, value) in container.env_entries() {
            for reference in secret_references(value) {
                problems.push(ValidationError::UnresolvedSecret {
                    profile: profile(),
                    key: key.to_string(),
                    rank,
                    reference: reference.to_string(),
                });
            }
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
    fn secret_names_are_tokens_keys_and_passwords_not_counts() {
        for name in [
            "HF_TOKEN",
            "HUGGING_FACE_HUB_TOKEN",
            "GITHUB_TOKEN",
            "VLLM_API_KEY",
            "DSPARK_API_KEYS",
            "AWS_SECRET_ACCESS_KEY",
            "DB_PASSWORD",
            "GITHUB_PAT",
            "hf_token",
        ] {
            assert!(is_secret_name(name), "{name}");
        }
        for name in [
            "MAX_NUM_BATCHED_TOKENS",
            "LONG_PREFILL_TOKEN_THRESHOLD",
            "TP3_PATCH_DIR",
            "VLLM_PORT",
            "HF_HOME",
            "DSPARK_REVISION",
        ] {
            assert!(!is_secret_name(name), "{name}");
        }
    }

    #[test]
    fn secret_references_follow_compose_and_skip_escapes_and_other_names() {
        assert_eq!(secret_references("${HF_TOKEN}"), vec!["HF_TOKEN"]);
        assert_eq!(secret_references("$HF_TOKEN"), vec!["HF_TOKEN"]);
        assert_eq!(
            secret_references("Bearer ${VLLM_API_KEY:-${FALLBACK_TOKEN}}"),
            vec!["VLLM_API_KEY", "FALLBACK_TOKEN"]
        );
        assert!(secret_references("$${HF_TOKEN} and $$HF_TOKEN").is_empty());
        assert!(secret_references("${HF_HOME}/x $HOME cost $5 $").is_empty());
        assert!(secret_references("${MAX_NUM_BATCHED_TOKENS}").is_empty());
        assert!(secret_references("hf_literalvalue").is_empty());
    }

    #[test]
    fn a_container_env_reading_a_secret_nothing_fills_is_refused_by_name_only() {
        let mut engine = container(format!("ghcr.io/org/server@sha256:{SHA}"));
        // Args are the container's own shell text, expanded from its env.
        engine.args = vec![
            "bash".into(),
            "-c".into(),
            "serve --key ${VLLM_API_KEY}".into(),
        ];
        engine.env.insert("HF_TOKEN".into(), "${HF_TOKEN}".into());
        engine.env.insert("HF_HOME".into(), "$${HF_HOME}".into());
        engine.ranks = vec![
            BTreeMap::new(),
            BTreeMap::from([("AUTH".to_string(), "Bearer $PEER_TOKEN".to_string())]),
        ];
        let mut gang = profile(Engine::Container(engine.clone()), vec![weight(SHA)]);
        gang.requirements.machines = 2;
        let problems = gang.problems();
        assert_eq!(
            problems,
            vec![
                ValidationError::UnresolvedSecret {
                    profile: "r".into(),
                    key: "HF_TOKEN".into(),
                    rank: None,
                    reference: "HF_TOKEN".into(),
                },
                ValidationError::UnresolvedSecret {
                    profile: "r".into(),
                    key: "AUTH".into(),
                    rank: Some(1),
                    reference: "PEER_TOKEN".into(),
                },
            ]
        );
        let said: Vec<String> = problems.iter().map(ToString::to_string).collect();
        assert!(
            said[0].contains("env HF_TOKEN reads ${HF_TOKEN}"),
            "{}",
            said[0]
        );
        assert!(
            said[0].contains("on the host with that machine's own token"),
            "{}",
            said[0]
        );
        assert!(
            said[1].contains("rank 1 env AUTH reads ${PEER_TOKEN}"),
            "{}",
            said[1]
        );
        assert!(!said[1].contains("Hugging Face"), "{}", said[1]);
        assert!(
            !said[1].contains("Bearer"),
            "the value is never said: {}",
            said[1]
        );
        assert!(said.iter().all(|s| s.contains("no supported store")));

        engine.env.remove("HF_TOKEN");
        engine.ranks[1].insert("AUTH".into(), "none".into());
        gang.engine = Engine::Container(engine);
        assert_eq!(gang.problems(), vec![]);
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

    fn llama_metal() -> Native {
        Native {
            server: Server::LlamaCpp,
            artifact: Artifact {
                url: "https://github.com/ggml-org/llama.cpp/releases/download/b12283/llama-b12283-bin-macos-arm64.tar.gz".into(),
                version: "b12283".into(),
                sha256: SHA.into(),
                packaging: Packaging::Archive,
            },
            program: "llama-b12283/llama-server".into(),
            args: vec!["-m".into(), "{weights}".into(), "--port".into(), "{port}".into()],
            env: BTreeMap::new(),
        }
    }

    fn mlx() -> Native {
        Native {
            server: Server::MlxLm,
            artifact: Artifact {
                url: "https://files.pythonhosted.org/packages/d1/4f/000eefdeb41388782b47e7f39904cf2d9e7e49a60df5e172097c358dba8e/mlx_lm-0.32.2-py3-none-any.whl".into(),
                version: "0.32.2".into(),
                sha256: SHA.into(),
                packaging: Packaging::Wheel {
                    python: "3.12".into(),
                    resolved_before: "2026-10-17T00:00:00Z".into(),
                },
            },
            program: MLX_PROGRAM.into(),
            args: vec!["--model".into(), "{snapshot}".into()],
            env: BTreeMap::new(),
        }
    }

    #[test]
    fn malformed_shas_are_refused() {
        let mut engine = llama_metal();
        engine.artifact.sha256 = SHA.to_uppercase();
        let native = profile(Engine::Native(engine), vec![weight("short")]);
        let problems = native.problems();
        assert!(problems.iter().any(|p| matches!(
            p,
            ValidationError::BadArtifact {
                problem: "sha256 is not 64 lowercase hex digits",
                ..
            }
        )));
        assert!(problems
            .iter()
            .any(|p| matches!(p, ValidationError::BadWeightFile { index: 0, .. })));
    }

    #[test]
    fn a_native_engine_pins_a_fetchable_artifact_and_runs_on_one_machine() {
        assert_eq!(
            profile(Engine::Native(llama_metal()), vec![weight(SHA)]).problems(),
            vec![]
        );
        assert_eq!(
            profile(Engine::Native(mlx()), vec![weight(SHA)]).problems(),
            vec![]
        );
        let said = |native: Native| -> Vec<String> {
            profile(Engine::Native(native), vec![weight(SHA)])
                .problems()
                .iter()
                .map(ToString::to_string)
                .collect()
        };
        let mut plain = llama_metal();
        plain.artifact.url = "http://example.com/llama.tar.gz".into();
        assert_eq!(
            said(plain),
            ["profile r native artifact: url must be https, with no whitespace, quote, backslash or control character"]
        );
        let mut zipped = llama_metal();
        zipped.artifact.url = "https://example.com/llama.zip".into();
        assert_eq!(
            said(zipped),
            ["profile r native artifact: an archive artifact is a .tar.gz"]
        );
        let mut escaping = llama_metal();
        escaping.program = "../../bin/sh".into();
        assert_eq!(
            said(escaping),
            ["profile r native artifact: program must be a relative path of letters, digits, ., _, - and +, with no empty, . or .. segment"]
        );
        let mut unpinned = mlx();
        unpinned.artifact.packaging = Packaging::Wheel {
            python: "3.12".into(),
            resolved_before: "latest".into(),
        };
        assert_eq!(
            said(unpinned),
            ["profile r native artifact: a wheel's resolvedBefore is a date (2026-10-17) or a UTC instant (2026-10-17T00:00:00Z)"]
        );
        let mut elsewhere = mlx();
        elsewhere.program = "python".into();
        assert_eq!(
            said(elsewhere),
            ["profile r native artifact: an mlxLm server runs mlx_lm.server"]
        );
        let mut odd = llama_metal();
        odd.args.push("{gpuCount}".into());
        odd.env.insert("HF_TOKEN".into(), "${HF_TOKEN}".into());
        let problems = said(odd);
        assert_eq!(problems[0], "profile r uses unknown placeholder {gpuCount}");
        assert!(problems[1].starts_with("profile r env HF_TOKEN reads ${HF_TOKEN}"));

        let mut gang = profile(Engine::Native(llama_metal()), vec![weight(SHA)]);
        gang.requirements.machines = 2;
        assert_eq!(
            gang.problems()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["profile r runs a native engine on 2 machines; a native engine runs on one machine"]
        );
    }

    #[test]
    fn the_native_form_round_trips_through_yaml() {
        let profile = profile(Engine::Native(mlx()), vec![weight(SHA)]);
        let yaml = serde_yaml::to_string(&profile).unwrap();
        assert!(yaml.contains("kind: native"), "{yaml}");
        assert!(yaml.contains("server: mlxLm"), "{yaml}");
        assert!(yaml.contains("kind: wheel"), "{yaml}");
        assert!(yaml.contains("resolvedBefore:"), "{yaml}");
        assert_eq!(
            serde_yaml::from_str::<InferenceProfile>(&yaml).unwrap(),
            profile
        );
        assert_eq!(mlx().artifact.file_name(), "mlx_lm-0.32.2-py3-none-any.whl");
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
