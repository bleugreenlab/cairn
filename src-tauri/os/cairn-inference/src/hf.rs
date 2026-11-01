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

use std::collections::BTreeSet;

use cairn_models_types::catalog::{Modalities, Modality, VerbKind};
use serde_json::Value;

use crate::model::ModelFacts;
use crate::profile::{is_commit_sha, is_sha256_hex};

/// What a weight file holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WeightFormat {
    Safetensors,
    Gguf,
}

/// One file Hugging Face stores in LFS, with its content hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HfFile {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

impl HfFile {
    pub fn format(&self) -> Option<WeightFormat> {
        if self.path.ends_with(".safetensors") {
            Some(WeightFormat::Safetensors)
        } else if self.path.ends_with(".gguf") {
            Some(WeightFormat::Gguf)
        } else {
            None
        }
    }

    /// A GGUF file's quantization, from its name (`…-Q8_0.gguf` is `q8_0`).
    pub fn gguf_quantization(&self) -> Option<String> {
        let stem = self.path.rsplit('/').next()?.strip_suffix(".gguf")?;
        let tail = stem.rsplit(['-', '.']).next()?;
        let lower = tail.to_ascii_lowercase();
        let known = lower.starts_with('q')
            || lower.starts_with("iq")
            || matches!(lower.as_str(), "f16" | "bf16" | "f32");
        known.then_some(lower)
    }
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
    /// Set when this repository is a quantized derivative of another model:
    /// the models it may quantize, best guess first.
    pub base_candidates: Vec<String>,
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
    let config_quantization = config
        .as_ref()
        .and_then(|config| config.get("quantization_config"))
        .and_then(quantization_label);

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
        base_candidates,
    })
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
    fn a_gated_repo_and_a_broken_answer_say_so() {
        let gated = r#"{"id":"meta-llama/Llama-4","sha":"0123456789abcdef0123456789abcdef01234567","gated":"manual","siblings":[]}"#;
        assert!(read(gated, None).gated);
        assert!(parse("{", None).unwrap_err().contains("not JSON"));
        assert!(parse(r#"{"id":"a/b","sha":"main"}"#, None)
            .unwrap_err()
            .contains("no revision sha"));
    }
}
