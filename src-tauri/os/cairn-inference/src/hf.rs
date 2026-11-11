//! Reading a Hugging Face repository: its model API answer
//! (`api/models/<repo>/revision/<sha>?blobs=true`) and its `config.json`,
//! parsed into a model's facts and its pinned weight files. Pure: the host
//! fetches, this reads.
//!
//! A quantized repository (`nvidia/Qwen3.6-35B-A3B-NVFP4`, a `-GGUF` repo) is
//! not a model of its own but weights for one of its base model's profiles.
//! [`HfRepo::base_candidates`] names which model it quantizes, best guess
//! first: the repository's own name without its quantization suffix (the GGUF
//! repo of `Qwen/Qwen3-Embedding-4B` declares `Qwen/Qwen3-4B-Base` as its
//! base, which is the embedder's own base, not the embedder), then whatever it
//! tags as `base_model:quantized:`. The host takes the first that exists.
//!
//! [`classify`] sorts the files of a model's repositories into what can run
//! it: weight sources (every safetensors file of a repository, which is MLX
//! weights when the repository says it is an MLX one; each GGUF
//! quantization, its split shards `-NNNNN-of-MMMMM` grouped into one source),
//! vision projectors (`mmproj-*`, which llama.cpp loads with `--mmproj`), and
//! speculative-decoding draft heads (`mtp-*`, `dflash-*`, `*draft*`). A
//! projector or draft head is never a source on its own: `mtp-*.gguf` is a few
//! gigabytes of draft head beside a model ten times its size. Draft heads are
//! listed and deliberately left unused.

use std::collections::{BTreeMap, BTreeSet};

use cairn_models_types::catalog::{Modalities, Modality, VerbKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::ModelFacts;
use crate::profile::{is_commit_sha, is_sha256_hex, ConfigFile};

/// What a weight source holds, which decides the servers that read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WeightFormat {
    Safetensors,
    Gguf,
    /// Safetensors files in MLX's own layout and quantization (an
    /// `mlx-community` repository), which only MLX reads.
    Mlx,
}

impl WeightFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Safetensors => "safetensors",
            Self::Gguf => "gguf",
            Self::Mlx => "mlx",
        }
    }
}

/// One file Hugging Face stores in LFS, with its content hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HfFile {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

impl HfFile {
    /// The file's format by its name; an MLX repository's safetensors files
    /// are MLX weights, which only [`HfRepo::mlx`] says.
    pub fn format(&self) -> Option<WeightFormat> {
        if self.path.ends_with(".safetensors") {
            Some(WeightFormat::Safetensors)
        } else if self.path.ends_with(".gguf") {
            Some(WeightFormat::Gguf)
        } else {
            None
        }
    }

    /// A GGUF file's quantization, from its name (`…-Q8_0.gguf` and
    /// `…-BF16-00001-of-00002.gguf` are `q8_0` and `bf16`).
    pub fn gguf_quantization(&self) -> Option<String> {
        let stem = self.name().strip_suffix(".gguf")?;
        let (stem, _) = split_shard(stem);
        quant_label(stem)
    }

    /// The file's name without its directory.
    pub fn name(&self) -> &str {
        self.path.rsplit('/').next().unwrap_or(&self.path)
    }

    /// What the file is to a model, from its name.
    pub fn role(&self) -> FileRole {
        let name = self.name().to_ascii_lowercase();
        if name.starts_with("mmproj") {
            FileRole::Projector
        } else if name.starts_with("mtp-")
            || name.starts_with("dflash-")
            || name.starts_with("eagle")
            || name.contains("draft")
        {
            FileRole::DraftHead
        } else if name.contains("imatrix") {
            FileRole::Other
        } else {
            FileRole::Weights
        }
    }
}

/// What a file in a model's repository is to the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FileRole {
    /// The model's weights, or one shard of them.
    Weights,
    /// A vision (or audio) projector a multimodal GGUF model loads beside its
    /// weights.
    Projector,
    /// A speculative-decoding draft head (multi-token prediction, DFlash,
    /// EAGLE). Never the model.
    DraftHead,
    /// Anything else, such as an importance matrix.
    Other,
}

/// `stem` without a split-shard suffix, and the shard's index and count when
/// it has one (`…-BF16-00001-of-00002` is `…-BF16`, 1 of 2).
fn split_shard(stem: &str) -> (&str, Option<(u32, u32)>) {
    let digits = |s: &str| s.len() == 5 && s.bytes().all(|b| b.is_ascii_digit());
    let Some((rest, count)) = stem.rsplit_once("-of-") else {
        return (stem, None);
    };
    let Some((base, index)) = rest.rsplit_once('-') else {
        return (stem, None);
    };
    if !digits(count) || !digits(index) {
        return (stem, None);
    }
    match (index.parse(), count.parse()) {
        (Ok(index), Ok(count)) => (base, Some((index, count))),
        _ => (stem, None),
    }
}

/// A quantization label from a GGUF file's stem: its last `-` or `.`
/// segment, when it reads as one.
fn quant_label(stem: &str) -> Option<String> {
    let tail = stem.rsplit(['-', '.']).next()?;
    let lower = tail.to_ascii_lowercase();
    let known = lower.starts_with('q')
        || lower.starts_with("iq")
        || lower.starts_with("tq")
        || lower.starts_with("mxfp4")
        || matches!(lower.as_str(), "f16" | "bf16" | "f32");
    known.then_some(lower)
}

/// One way to get a model's weights: the files of one format and quantization
/// in one repository, shards in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WeightSource {
    /// `<repo>@<quantization>`, unique among a model's sources; a
    /// quantization a repository holds twice is keyed by its file stem
    /// instead.
    pub key: String,
    pub repo: String,
    pub revision: String,
    pub format: WeightFormat,
    pub quantization: String,
    pub files: Vec<HfFile>,
    /// The repository's configuration, tokenizer and chat-template files,
    /// which a safetensors or MLX server reads beside the weights. None for
    /// GGUF.
    pub config_files: Vec<ConfigFile>,
}

impl WeightSource {
    /// The weights' size: every file's, summed.
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }
}

/// A projector, draft head or other file beside a model's weights.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuxiliaryFile {
    pub repo: String,
    pub revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization: Option<String>,
    pub file: HfFile,
}

/// A model's repositories' files, sorted by what they are to it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepoFiles {
    pub sources: Vec<WeightSource>,
    pub projectors: Vec<AuxiliaryFile>,
    /// Listed so they are seen to exist, and deliberately unused.
    pub draft_heads: Vec<AuxiliaryFile>,
    /// Incomplete shard sets, importance matrices and the like.
    pub other: Vec<AuxiliaryFile>,
}

impl RepoFiles {
    pub fn source(&self, key: &str) -> Option<&WeightSource> {
        self.sources.iter().find(|source| source.key == key)
    }

    /// The projector to load beside `source`: one from the same repository
    /// at the weights' precision, else at bf16, f16 or f32, else any.
    pub fn projector_for(&self, source: &WeightSource) -> Option<&AuxiliaryFile> {
        let beside: Vec<&AuxiliaryFile> = self
            .projectors
            .iter()
            .filter(|p| p.repo == source.repo && p.revision == source.revision)
            .collect();
        let at = |quant: &str| {
            beside
                .iter()
                .copied()
                .find(|p| p.quantization.as_deref() == Some(quant))
        };
        at(&source.quantization)
            .or_else(|| at("bf16"))
            .or_else(|| at("f16"))
            .or_else(|| at("f32"))
            .or_else(|| beside.first().copied())
    }
}

/// Sort `repos` (a model's own repository and its quantized derivatives) into
/// weight sources and the files beside them. A safetensors source's
/// quantization is its repository's quantization config, else its dtype, else
/// `model_dtype`; it is an MLX source when its repository is an MLX one.
pub fn classify(repos: &[HfRepo], model_dtype: Option<&str>) -> RepoFiles {
    let mut files = RepoFiles::default();
    for repo in repos {
        let aux = |file: &HfFile| AuxiliaryFile {
            repo: repo.repo.clone(),
            revision: repo.revision.clone(),
            quantization: file.gguf_quantization(),
            file: file.clone(),
        };
        let mut sources: Vec<WeightSource> = Vec::new();
        let safetensors: Vec<HfFile> = repo
            .weights_of(WeightFormat::Safetensors)
            .filter(|file| file.role() == FileRole::Weights)
            .cloned()
            .collect();
        if !safetensors.is_empty() {
            let quantization = repo
                .facts
                .quantization
                .clone()
                .or_else(|| repo.facts.dtype.as_deref().map(crate::fit::dtype_label))
                .or_else(|| model_dtype.map(crate::fit::dtype_label))
                .unwrap_or_else(|| "unknown".into());
            sources.push(WeightSource {
                key: String::new(),
                repo: repo.repo.clone(),
                revision: repo.revision.clone(),
                format: if repo.mlx {
                    WeightFormat::Mlx
                } else {
                    WeightFormat::Safetensors
                },
                quantization,
                files: safetensors,
                config_files: repo.config_files.clone(),
            });
        }
        // GGUF weights, grouped by directory and stem without the shard
        // suffix.
        let mut groups: BTreeMap<String, Vec<(u32, u32, &HfFile)>> = BTreeMap::new();
        for file in repo.weights_of(WeightFormat::Gguf) {
            match file.role() {
                FileRole::Projector => files.projectors.push(aux(file)),
                FileRole::DraftHead => files.draft_heads.push(aux(file)),
                FileRole::Other => files.other.push(aux(file)),
                FileRole::Weights => {
                    let path = file.path.strip_suffix(".gguf").unwrap_or(&file.path);
                    let (base, shard) = split_shard(path);
                    let (index, count) = shard.unwrap_or((1, 1));
                    groups
                        .entry(base.to_string())
                        .or_default()
                        .push((index, count, file));
                }
            }
        }
        for (base, mut shards) in groups {
            shards.sort_by_key(|(index, _, _)| *index);
            let count = shards[0].1;
            let complete = shards.len() as u32 == count
                && shards
                    .iter()
                    .enumerate()
                    .all(|(i, (index, total, _))| *index == i as u32 + 1 && *total == count);
            if !complete {
                files
                    .other
                    .extend(shards.iter().map(|(_, _, file)| aux(file)));
                continue;
            }
            let stem = base.rsplit('/').next().unwrap_or(&base);
            sources.push(WeightSource {
                key: String::new(),
                repo: repo.repo.clone(),
                revision: repo.revision.clone(),
                format: WeightFormat::Gguf,
                quantization: quant_label(stem).unwrap_or_else(|| stem.to_ascii_lowercase()),
                files: shards
                    .into_iter()
                    .map(|(_, _, file)| file.clone())
                    .collect(),
                config_files: Vec::new(),
            });
        }
        let mut seen: BTreeMap<String, usize> = BTreeMap::new();
        for source in &sources {
            *seen.entry(source.quantization.clone()).or_default() += 1;
        }
        for mut source in sources {
            let label = if seen[&source.quantization] > 1 {
                let first = source.files[0]
                    .path
                    .strip_suffix(".gguf")
                    .unwrap_or(&source.files[0].path);
                split_shard(first.rsplit('/').next().unwrap_or(first))
                    .0
                    .to_ascii_lowercase()
            } else {
                source.quantization.clone()
            };
            source.key = format!("{}@{label}", source.repo);
            files.sources.push(source);
        }
    }
    files
}

/// One repository at one revision, read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HfRepo {
    pub repo: String,
    /// The full revision sha the answer is for.
    pub revision: String,
    pub facts: ModelFacts,
    /// Whether downloading needs an accepted licence (Hugging Face's gate).
    pub gated: bool,
    /// The checkpoint's weight files, safetensors and GGUF, with their hashes.
    pub weights: Vec<HfFile>,
    /// The files a server reads beside the weights, by [`is_config_file`].
    pub config_files: Vec<ConfigFile>,
    /// Set when this repository is a quantized derivative of another model:
    /// the models it may quantize, best guess first.
    pub base_candidates: Vec<String>,
    /// Whether the repository says it holds MLX weights (its library is
    /// `mlx`, or it is tagged so), whose safetensors only MLX reads.
    pub mlx: bool,
}

impl HfRepo {
    pub fn is_derivative(&self) -> bool {
        !self.base_candidates.is_empty()
    }

    /// The weight files of one format.
    pub fn weights_of(&self, format: WeightFormat) -> impl Iterator<Item = &HfFile> {
        self.weights
            .iter()
            .filter(move |file| file.format() == Some(format))
    }
}

/// Whether a server reads `path` beside a repository's weights: its JSON
/// configuration (`config.json`, `tokenizer_config.json`, the safetensors
/// index, a sentence-transformers `1_Pooling/config.json`), its tokenizer
/// (`merges.txt`, a SentencePiece `.model`, a `.tiktoken`) and its chat
/// template (`.jinja`). Never code: a repository's Python is not run.
pub fn is_config_file(path: &str) -> bool {
    [".json", ".txt", ".model", ".tiktoken", ".jinja"]
        .iter()
        .any(|extension| path.ends_with(extension))
}

const QUANT_SUFFIXES: &[&str] = &[
    "-GGUF",
    "-NVFP4",
    "-FP8",
    "-FP8-Dynamic",
    "-AWQ",
    "-GPTQ",
    "-GPTQ-Int4",
    "-GPTQ-Int8",
    "-MLX",
    "-4bit",
    "-8bit",
];

/// Parse the model API answer and, when the repository has one, its
/// `config.json`.
pub fn parse(api: &str, config: Option<&str>) -> Result<HfRepo, String> {
    let api: Value = serde_json::from_str(api)
        .map_err(|error| format!("the Hugging Face answer is not JSON: {error}"))?;
    let config: Option<Value> = match config {
        None => None,
        Some(text) => Some(
            serde_json::from_str(text)
                .map_err(|error| format!("config.json is not JSON: {error}"))?,
        ),
    };
    let repo = api
        .get("id")
        .and_then(Value::as_str)
        .ok_or("the Hugging Face answer names no repository id")?
        .to_string();
    let revision = api
        .get("sha")
        .and_then(Value::as_str)
        .filter(|sha| is_commit_sha(sha))
        .ok_or_else(|| format!("the Hugging Face answer for {repo} names no revision sha"))?
        .to_string();
    let gated = match api.get("gated") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(_) => true,
    };
    let weights: Vec<HfFile> = api
        .get("siblings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|sibling| {
            let path = sibling.get("rfilename")?.as_str()?.to_string();
            let lfs = sibling.get("lfs")?;
            let sha256 = lfs.get("sha256")?.as_str()?.to_string();
            let size = lfs.get("size")?.as_u64()?;
            let file = HfFile { path, sha256, size };
            (file.format().is_some() && is_sha256_hex(&file.sha256)).then_some(file)
        })
        .collect();
    let config_files: Vec<ConfigFile> = api
        .get("siblings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|sibling| {
            let path = sibling.get("rfilename")?.as_str()?;
            if !is_config_file(path) {
                return None;
            }
            let lfs = sibling.get("lfs");
            let sha256 = lfs
                .and_then(|lfs| lfs.get("sha256")?.as_str())
                .map(str::to_string);
            let file = ConfigFile {
                repo: repo.clone(),
                revision: revision.clone(),
                path: path.to_string(),
                git_blob: match sha256 {
                    Some(_) => None,
                    None => Some(sibling.get("blobId")?.as_str()?.to_string()),
                },
                sha256,
                size: sibling.get("size").and_then(Value::as_u64),
            };
            file.problem().is_none().then_some(file)
        })
        .collect();

    let tags: Vec<&str> = api
        .get("tags")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let quantized_tags: Vec<String> = tags
        .iter()
        .filter_map(|tag| tag.strip_prefix("base_model:quantized:"))
        .map(str::to_string)
        .collect();
    let is_gguf = api.get("gguf").is_some_and(|g| !g.is_null())
        || weights
            .iter()
            .any(|w| w.format() == Some(WeightFormat::Gguf));
    let mlx =
        api.get("library_name").and_then(Value::as_str) == Some("mlx") || tags.contains(&"mlx");
    let config_quantization = config.as_ref().and_then(|config| {
        if mlx {
            mlx_quantization_label(config.get("quantization")?)
        } else {
            config
                .get("quantization_config")
                .and_then(quantization_label)
        }
    });

    let mut base_candidates = Vec::new();
    let quantized = is_gguf || !quantized_tags.is_empty();
    if quantized {
        if let Some(stripped) = QUANT_SUFFIXES
            .iter()
            .find_map(|suffix| repo.strip_suffix(suffix))
        {
            base_candidates.push(stripped.to_string());
        }
        for base in &quantized_tags {
            if !base_candidates.contains(base) {
                base_candidates.push(base.clone());
            }
        }
    }

    let mut facts = match &config {
        Some(config) => facts_of_config(config),
        None => ModelFacts::default(),
    };
    facts.quantization = config_quantization;
    if let Some(gguf) = api.get("gguf").filter(|g| !g.is_null()) {
        facts.model_type = facts
            .model_type
            .or_else(|| gguf.get("architecture")?.as_str().map(str::to_string));
        facts.context = facts
            .context
            .or_else(|| gguf.get("context_length")?.as_u64());
        facts.parameters = facts.parameters.or_else(|| gguf.get("total")?.as_u64());
    }
    facts.parameters = api
        .get("safetensors")
        .and_then(|st| st.get("total"))
        .and_then(Value::as_u64)
        .or(facts.parameters);
    let pipeline = api
        .get("pipeline_tag")
        .and_then(Value::as_str)
        .or_else(|| api.get("cardData")?.get("pipeline_tag")?.as_str());
    let has_vision = config.as_ref().is_some_and(|config| {
        config.get("vision_config").is_some() || config.get("vision_dim").is_some()
    });
    if let Some((verbs, modalities)) = pipeline.and_then(|tag| serves(tag, has_vision)) {
        facts.verbs = Some(verbs);
        facts.modalities = Some(modalities);
    }

    Ok(HfRepo {
        repo,
        revision,
        facts,
        gated,
        weights,
        config_files,
        base_candidates,
        mlx,
    })
}

/// An MLX config's `quantization` as a label: `4bit` for MLX's affine
/// quantization at 4 bits, else its mode (`mxfp4`, `nvfp4`, `mxfp8`).
fn mlx_quantization_label(quantization: &Value) -> Option<String> {
    match quantization.get("mode").and_then(Value::as_str) {
        None | Some("affine") => {
            let bits = quantization.get("bits")?.as_u64()?;
            Some(format!("{bits}bit"))
        }
        Some(mode) => Some(mode.to_ascii_lowercase()),
    }
}

/// The verbs and modalities a pipeline tag says a model serves.
fn serves(pipeline: &str, has_vision: bool) -> Option<(BTreeSet<VerbKind>, Modalities)> {
    match pipeline {
        "text-generation" | "image-text-to-text" => {
            let mut input = vec![Modality::Text];
            if pipeline == "image-text-to-text" || has_vision {
                input.push(Modality::Image);
            }
            Some((
                BTreeSet::from([VerbKind::Complete, VerbKind::Session]),
                Modalities {
                    input,
                    output: vec![Modality::Text],
                },
            ))
        }
        "feature-extraction" | "sentence-similarity" => Some((
            BTreeSet::from([VerbKind::Embed]),
            Modalities {
                input: vec![Modality::Text],
                output: vec![Modality::Embeddings],
            },
        )),
        _ => None,
    }
}

fn uint(config: &Value, key: &str) -> Option<u32> {
    config
        .get(key)?
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
}

/// The facts a `config.json` states. A multimodal config keeps the language
/// model's in `text_config`.
fn facts_of_config(config: &Value) -> ModelFacts {
    let text = config.get("text_config").unwrap_or(config);
    let architecture = config
        .get("architectures")
        .and_then(Value::as_array)
        .and_then(|list| list.first())
        .and_then(Value::as_str)
        .map(str::to_string);
    let model_type = config
        .get("model_type")
        .and_then(Value::as_str)
        .map(str::to_string);
    let dtype = [text, config]
        .iter()
        .find_map(|c| {
            c.get("torch_dtype")
                .or_else(|| c.get("dtype"))
                .and_then(Value::as_str)
        })
        .map(str::to_string);
    let layers = uint(text, "num_hidden_layers");
    let attention_heads = uint(text, "num_attention_heads");
    let mut facts = ModelFacts {
        architecture,
        model_type,
        dtype,
        context: text.get("max_position_embeddings").and_then(Value::as_u64),
        layers,
        attention_heads,
        ..ModelFacts::default()
    };
    // Compressed or sparse attention (DeepSeek V4's compress ratios and
    // indexer) caches neither a key and value per head nor one latent per
    // layer, so no formula here predicts its cache: it stays unknown and the
    // engine's measured cache decides the context.
    let compressed = text.get("compress_ratios").is_some() || text.get("index_topk").is_some();
    if compressed {
        return facts;
    }
    if let (Some(rank), Some(rope)) = (uint(text, "kv_lora_rank"), uint(text, "qk_rope_head_dim")) {
        facts.kv_lora_rank = Some(rank);
        facts.rope_head_dim = Some(rope);
        return facts;
    }
    facts.kv_heads = uint(text, "num_key_value_heads").or(attention_heads);
    facts.head_dim = uint(text, "head_dim").or_else(|| {
        let hidden = uint(text, "hidden_size")?;
        Some(hidden / attention_heads.filter(|h| *h > 0)?)
    });
    facts.kv_layers = match text.get("layer_types").and_then(Value::as_array) {
        Some(types) => Some(
            types
                .iter()
                .filter(|t| t.as_str() == Some("full_attention"))
                .count() as u32,
        ),
        None => uint(text, "full_attention_interval")
            .filter(|interval| *interval > 0)
            .zip(layers)
            .map(|(interval, layers)| layers / interval),
    };
    facts
}

/// A quantization config as a label: `nvfp4`, `fp8`, `awq`, `gptq`, or the
/// method's own name.
fn quantization_label(config: &Value) -> Option<String> {
    let method = config.get("quant_method").and_then(Value::as_str)?;
    let text = config.to_string().to_ascii_uppercase();
    let label = match method {
        "modelopt" | "compressed-tensors" if text.contains("NVFP4") => "nvfp4".to_string(),
        "modelopt" if text.contains("FP8") => "fp8".to_string(),
        "fp8" => "fp8".to_string(),
        other => other.to_ascii_lowercase(),
    };
    Some(label)
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! fixture {
        ($slug:literal) => {
            include_str!(concat!("../tests/fixtures/hf/", $slug, ".api.json"))
        };
        ($slug:literal, config) => {
            include_str!(concat!("../tests/fixtures/hf/", $slug, ".config.json"))
        };
    }

    fn read(api: &str, config: Option<&str>) -> HfRepo {
        parse(api, config).unwrap()
    }

    #[test]
    fn deepseek_v4_reads_as_fp8_with_compressed_attention_left_unknown() {
        let repo = read(
            fixture!("deepseek-ai--DeepSeek-V4-Flash-Vision-Exp"),
            Some(fixture!(
                "deepseek-ai--DeepSeek-V4-Flash-Vision-Exp",
                config
            )),
        );
        assert_eq!(repo.repo, "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp");
        assert_eq!(repo.revision, "6821d6ad3681a4b137b066b76094fa82ebd0a380");
        assert!(!repo.is_derivative());
        let facts = &repo.facts;
        assert_eq!(facts.architecture.as_deref(), Some("DeepseekV4ForCausalLM"));
        assert_eq!(facts.quantization.as_deref(), Some("fp8"));
        assert_eq!(facts.context, Some(1_048_576));
        assert_eq!(facts.parameters, Some(304_646_824_126));
        assert_eq!(facts.layers, Some(43));
        assert_eq!(facts.attention_heads, Some(64));
        assert_eq!(facts.kv_elements_per_token(), None, "compressed attention");
        assert_eq!(
            facts.verbs,
            Some(BTreeSet::from([VerbKind::Complete, VerbKind::Session]))
        );
        assert_eq!(
            facts.modalities.as_ref().unwrap().input,
            vec![Modality::Text, Modality::Image]
        );
        let safetensors = repo.weights_of(WeightFormat::Safetensors).count();
        assert!(safetensors > 40, "{safetensors}");
        assert!(repo.weights.iter().all(|w| w.size > 0));
    }

    #[test]
    fn the_text_only_deepseek_reads_text_in_text_out() {
        let repo = read(
            fixture!("deepseek-ai--DeepSeek-V4-Flash"),
            Some(fixture!("deepseek-ai--DeepSeek-V4-Flash", config)),
        );
        assert_eq!(repo.facts.modalities.unwrap().input, vec![Modality::Text]);
        assert_eq!(repo.facts.parameters, Some(290_944_616_402));
    }

    #[test]
    fn qwen36_reads_its_text_config_and_its_hybrid_kv_layers() {
        let repo = read(
            fixture!("Qwen--Qwen3.6-35B-A3B"),
            Some(fixture!("Qwen--Qwen3.6-35B-A3B", config)),
        );
        let facts = &repo.facts;
        assert_eq!(facts.dtype.as_deref(), Some("bfloat16"));
        assert_eq!(facts.quantization, None);
        assert_eq!(facts.context, Some(262_144));
        assert_eq!(facts.layers, Some(40));
        assert_eq!(facts.kv_layers, Some(10));
        assert_eq!(facts.kv_heads, Some(2));
        assert_eq!(facts.head_dim, Some(256));
        assert_eq!(facts.kv_elements_per_token(), Some(10 * 2 * 256 * 2));
        assert!(!repo.is_derivative());
    }

    #[test]
    fn the_nvfp4_build_is_a_derivative_of_qwen36() {
        let repo = read(
            fixture!("nvidia--Qwen3.6-35B-A3B-NVFP4"),
            Some(fixture!("nvidia--Qwen3.6-35B-A3B-NVFP4", config)),
        );
        assert_eq!(
            repo.base_candidates,
            vec!["nvidia/Qwen3.6-35B-A3B", "Qwen/Qwen3.6-35B-A3B"]
        );
        assert_eq!(repo.facts.quantization.as_deref(), Some("nvfp4"));
        let bytes: u64 = repo
            .weights_of(WeightFormat::Safetensors)
            .map(|w| w.size)
            .sum();
        assert!(bytes > 18 << 30 && bytes < 26 << 30, "{bytes}");
    }

    #[test]
    fn the_embedder_serves_embed_and_is_not_a_derivative_of_its_base() {
        let repo = read(
            fixture!("Qwen--Qwen3-Embedding-4B"),
            Some(fixture!("Qwen--Qwen3-Embedding-4B", config)),
        );
        assert!(!repo.is_derivative(), "a finetune is a model of its own");
        assert_eq!(repo.facts.verbs, Some(BTreeSet::from([VerbKind::Embed])));
        assert_eq!(
            repo.facts.modalities.as_ref().unwrap().output,
            vec![Modality::Embeddings]
        );
        assert_eq!(repo.facts.context, Some(40_960));
        assert_eq!(repo.facts.kv_elements_per_token(), Some(36 * 8 * 128 * 2));
    }

    #[test]
    fn the_gguf_repo_names_the_embedder_first_and_its_files_carry_their_quant() {
        let repo = read(fixture!("Qwen--Qwen3-Embedding-4B-GGUF"), None);
        assert_eq!(repo.base_candidates[0], "Qwen/Qwen3-Embedding-4B");
        let q8 = repo
            .weights_of(WeightFormat::Gguf)
            .find(|w| w.gguf_quantization().as_deref() == Some("q8_0"))
            .unwrap();
        assert_eq!(q8.path, "Qwen3-Embedding-4B-Q8_0.gguf");
        assert_eq!(
            q8.sha256,
            "b60ae5ce2dd6a0b77f82cadf21def1f310a3e10cde380ad0081b07a9d416949d"
        );
        assert_eq!(q8.size, 4_279_660_224);
        assert_eq!(repo.facts.context, Some(40_960));
        let quants: Vec<String> = repo
            .weights
            .iter()
            .filter_map(HfFile::gguf_quantization)
            .collect();
        assert_eq!(
            quants,
            vec!["q4_k_m", "q5_0", "q5_k_m", "q6_k", "q8_0", "f16"]
        );
    }

    #[test]
    fn split_shards_are_one_source_and_auxiliary_files_are_never_a_source() {
        let repo = read(fixture!("bartowski--Qwen_Qwen3.6-35B-A3B-GGUF"), None);
        let files = classify(std::slice::from_ref(&repo), None);
        let bf16 = files
            .source("bartowski/Qwen_Qwen3.6-35B-A3B-GGUF@bf16")
            .expect("the bf16 shards are one source");
        assert_eq!(bf16.files.len(), 2);
        assert!(bf16.files[0].path.ends_with("-bf16-00001-of-00002.gguf"));
        assert!(bf16.files[1].path.ends_with("-bf16-00002-of-00002.gguf"));
        assert_eq!(bf16.bytes(), 39_753_375_712 + 31_312_565_824);
        assert_eq!(
            bf16.files[0].gguf_quantization().as_deref(),
            Some("bf16"),
            "a shard's quantization is read past its shard suffix"
        );
        assert!(files.sources.iter().all(|source| {
            source
                .files
                .iter()
                .all(|file| file.role() == FileRole::Weights)
        }));
        let q4 = files
            .source("bartowski/Qwen_Qwen3.6-35B-A3B-GGUF@q4_k_m")
            .unwrap();
        assert_eq!(q4.files.len(), 1);
        assert_eq!(files.projectors.len(), 2, "{:?}", files.projectors);
        assert_eq!(files.draft_heads.len(), 2);
        assert!(files.other.iter().any(|f| f.file.path.contains("imatrix")));
    }

    #[test]
    fn the_live_case_draft_heads_and_projectors_are_not_the_model() {
        let repo = read(fixture!("ggml-org--Qwen3.6-35B-A3B-GGUF"), None);
        assert!(repo
            .base_candidates
            .contains(&"Qwen/Qwen3.6-35B-A3B".to_string()));
        let files = classify(std::slice::from_ref(&repo), None);
        let keys: Vec<&str> = files.sources.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "ggml-org/Qwen3.6-35B-A3B-GGUF@bf16",
                "ggml-org/Qwen3.6-35B-A3B-GGUF@q4_k_m",
                "ggml-org/Qwen3.6-35B-A3B-GGUF@q8_0",
            ]
        );
        assert_eq!(
            files
                .source("ggml-org/Qwen3.6-35B-A3B-GGUF@bf16")
                .unwrap()
                .bytes(),
            69_376_637_248
        );
        let drafts: Vec<&str> = files
            .draft_heads
            .iter()
            .map(|f| f.file.path.as_str())
            .collect();
        assert_eq!(drafts.len(), 5, "{drafts:?}");
        assert!(drafts.contains(&"mtp-Qwen3.6-35B-A3B-BF16.gguf"));
        assert!(drafts.contains(&"dflash-Qwen3.6-35B-A3B-Q8_0.gguf"));
        let projectors: Vec<(&str, Option<&str>)> = files
            .projectors
            .iter()
            .map(|f| (f.file.path.as_str(), f.quantization.as_deref()))
            .collect();
        assert_eq!(
            projectors,
            vec![
                ("mmproj-Qwen3.6-35B-A3B-BF16.gguf", Some("bf16")),
                ("mmproj-Qwen3.6-35B-A3B-Q8_0.gguf", Some("q8_0")),
            ]
        );
        let projector_of = |key: &str| {
            files
                .projector_for(files.source(key).unwrap())
                .map(|p| p.file.path.as_str())
        };
        assert_eq!(
            projector_of("ggml-org/Qwen3.6-35B-A3B-GGUF@q8_0"),
            Some("mmproj-Qwen3.6-35B-A3B-Q8_0.gguf"),
            "the projector at the weights' precision"
        );
        assert_eq!(
            projector_of("ggml-org/Qwen3.6-35B-A3B-GGUF@q4_k_m"),
            Some("mmproj-Qwen3.6-35B-A3B-BF16.gguf"),
            "else bf16"
        );
    }

    #[test]
    fn single_file_ggufs_and_safetensors_classify_as_before() {
        let own = read(
            fixture!("Qwen--Qwen3-Embedding-4B"),
            Some(fixture!("Qwen--Qwen3-Embedding-4B", config)),
        );
        let gguf = read(fixture!("Qwen--Qwen3-Embedding-4B-GGUF"), None);
        let files = classify(&[own, gguf], None);
        let mut keys: Vec<&str> = files.sources.iter().map(|s| s.key.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "Qwen/Qwen3-Embedding-4B-GGUF@f16",
                "Qwen/Qwen3-Embedding-4B-GGUF@q4_k_m",
                "Qwen/Qwen3-Embedding-4B-GGUF@q5_0",
                "Qwen/Qwen3-Embedding-4B-GGUF@q5_k_m",
                "Qwen/Qwen3-Embedding-4B-GGUF@q6_k",
                "Qwen/Qwen3-Embedding-4B-GGUF@q8_0",
                "Qwen/Qwen3-Embedding-4B@bf16",
            ]
        );
        let q8 = files.source("Qwen/Qwen3-Embedding-4B-GGUF@q8_0").unwrap();
        assert_eq!(q8.files.len(), 1);
        assert_eq!(q8.files[0].path, "Qwen3-Embedding-4B-Q8_0.gguf");
        assert_eq!(q8.bytes(), 4_279_660_224);
        assert!(files.projectors.is_empty() && files.draft_heads.is_empty());
    }

    #[test]
    fn a_safetensors_source_pins_its_config_files_by_lfs_hash_or_git_blob() {
        let repo = read(
            include_str!("../tests/fixtures/hf/Qwen--Qwen3-Embedding-4B.api.json"),
            Some(include_str!(
                "../tests/fixtures/hf/Qwen--Qwen3-Embedding-4B.config.json"
            )),
        );
        let paths: Vec<&str> = repo.config_files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "1_Pooling/config.json",
                "config.json",
                "config_sentence_transformers.json",
                "generation_config.json",
                "merges.txt",
                "model.safetensors.index.json",
                "modules.json",
                "tokenizer.json",
                "tokenizer_config.json",
                "vocab.json",
            ],
            "no README, .gitattributes or weights"
        );
        let by_path = |path: &str| repo.config_files.iter().find(|f| f.path == path).unwrap();
        let config = by_path("config.json");
        assert_eq!(
            config.git_blob.as_deref(),
            Some("8b4b87fc69023e7a224eb6563753aaf3223d8b98")
        );
        assert_eq!(config.sha256, None);
        assert_eq!(config.revision, repo.revision);
        let tokenizer = by_path("tokenizer.json");
        assert_eq!(
            tokenizer.sha256.as_deref(),
            Some("83cdf8c3a34f68862319cb1810ee7b1e2c0a44e0864ae930194ddb76bb7feb8d")
        );
        assert_eq!(tokenizer.git_blob, None, "an LFS file is pinned by content");

        let files = classify(&[repo], None);
        let source = files.source("Qwen/Qwen3-Embedding-4B@bf16").unwrap();
        assert_eq!(source.config_files.len(), 10);
    }

    #[test]
    fn an_incomplete_shard_set_is_not_a_source() {
        let file = |path: &str| HfFile {
            path: path.into(),
            sha256: "0".repeat(64),
            size: 1,
        };
        let repo = HfRepo {
            repo: "a/b-GGUF".into(),
            revision: "0".repeat(40),
            facts: ModelFacts::default(),
            gated: false,
            weights: vec![
                file("m-Q8_0-00001-of-00003.gguf"),
                file("m-Q8_0-00002-of-00003.gguf"),
            ],
            config_files: vec![],
            base_candidates: vec![],
            mlx: false,
        };
        let files = classify(&[repo], None);
        assert!(files.sources.is_empty());
        assert_eq!(files.other.len(), 2);
    }

    #[test]
    fn an_mlx_community_repo_is_mlx_weights_labelled_by_its_quantization() {
        let repo = read(
            fixture!("mlx-community--Qwen3.6-35B-A3B-4bit"),
            Some(fixture!("mlx-community--Qwen3.6-35B-A3B-4bit", config)),
        );
        assert!(repo.mlx);
        assert!(repo
            .base_candidates
            .contains(&"Qwen/Qwen3.6-35B-A3B".to_string()));
        assert_eq!(
            repo.facts.quantization.as_deref(),
            Some("4bit"),
            "its quantization, not its checkpoint's bf16"
        );
        let files = classify(std::slice::from_ref(&repo), Some("bfloat16"));
        let source = files
            .source("mlx-community/Qwen3.6-35B-A3B-4bit@4bit")
            .unwrap();
        assert_eq!(source.format, WeightFormat::Mlx);
        assert_eq!(source.files.len(), 4);
        assert_eq!(
            source.bytes(),
            5_288_196_018 + 5_368_472_749 + 5_368_324_139 + 4_377_211_365
        );
        assert!(
            source
                .config_files
                .iter()
                .any(|file| file.path == "tokenizer.json" && file.sha256.is_some()),
            "MLX reads its tokenizer beside the weights"
        );

        let embedder = read(
            fixture!("mlx-community--Qwen3-Embedding-4B-4bit-DWQ"),
            Some(fixture!(
                "mlx-community--Qwen3-Embedding-4B-4bit-DWQ",
                config
            )),
        );
        assert!(embedder.mlx);
        assert_eq!(embedder.facts.quantization.as_deref(), Some("4bit"));
        assert!(embedder
            .base_candidates
            .contains(&"Qwen/Qwen3-Embedding-4B".to_string()));

        let mode = |text: &str| mlx_quantization_label(&serde_json::from_str(text).unwrap());
        assert_eq!(
            mode(r#"{"group_size":32,"bits":4,"mode":"mxfp4"}"#).as_deref(),
            Some("mxfp4")
        );
        assert_eq!(
            mode(r#"{"group_size":64,"bits":8,"mode":"affine"}"#).as_deref(),
            Some("8bit")
        );
    }

    #[test]
    fn a_gated_repo_and_a_broken_answer_say_so() {
        let gated = r#"{"id":"meta-llama/Llama-4","sha":"0123456789abcdef0123456789abcdef01234567","gated":"manual","siblings":[]}"#;
        assert!(read(gated, None).gated);
        assert!(parse("{", None).unwrap_err().contains("not JSON"));
        assert!(parse(r#"{"id":"a/b","sha":"main"}"#, None)
            .unwrap_err()
            .contains("no revision sha"));
    }
}
