//! Models: a checkpoint registered under its real Hugging Face name, with the
//! facts its config states.
//!
//! A model is the base checkpoint (`Qwen/Qwen3.6-35B-A3B`), never a quantized
//! derivative of it: `nvidia/Qwen3.6-35B-A3B-NVFP4` is the weights of one of
//! that model's recipes. Every fact is optional, because a fact the config does
//! not state is unknown and is rendered as unknown, never guessed.

use std::collections::BTreeSet;

use cairn_models_types::catalog::{Modalities, VerbKind};
use serde::{Deserialize, Serialize};

use crate::pool::RecordSource;

/// A model, keyed by its Hugging Face repository id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Model {
    /// The Hugging Face repository id, such as `deepseek-ai/DeepSeek-V4-Flash`.
    pub name: String,
    #[serde(default)]
    pub source: RecordSource,
    /// The revision sha the facts were read at. `None` for a model registered
    /// without reading its config (the ones backfilled from existing recipes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default)]
    pub facts: ModelFacts,
    /// When the facts were read, in Unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_at: Option<i64>,
}

impl Model {
    /// A model known only by name, every fact unknown.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: RecordSource::Cairn,
            revision: None,
            facts: ModelFacts::default(),
            read_at: None,
        }
    }
}

/// What a model's config and Hugging Face metadata say about it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelFacts {
    /// The config's first `architectures` entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub architecture: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_type: Option<String>,
    /// Total parameters, from the safetensors index Hugging Face reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<u64>,
    /// The checkpoint's weight dtype (`bfloat16`), from `torch_dtype`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtype: Option<String>,
    /// The checkpoint's own quantization (`fp8`), from its
    /// `quantization_config`; `None` for an unquantized checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantization: Option<String>,
    /// The longest context the model was trained for, in tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Modalities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verbs: Option<BTreeSet<VerbKind>>,
    /// Hidden layers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layers: Option<u32>,
    /// The layers that keep a KV cache, where some do not (a hybrid model's
    /// linear-attention layers keep none). `None` means every layer does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_layers: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_heads: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_heads: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_dim: Option<u32>,
    /// Multi-head latent attention's compressed KV width (DeepSeek's
    /// `kv_lora_rank`). With it the cache holds this plus
    /// [`Self::rope_head_dim`] per token per layer, not per head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_lora_rank: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rope_head_dim: Option<u32>,
}

impl ModelFacts {
    /// Elements of KV cache one token takes across every layer, or `None` when
    /// the geometry is unknown. Multi-head latent attention caches one
    /// compressed vector per layer; grouped-query attention caches a key and a
    /// value per KV head.
    pub fn kv_elements_per_token(&self) -> Option<u64> {
        let layers = u64::from(self.kv_layers.or(self.layers)?);
        if let (Some(rank), Some(rope)) = (self.kv_lora_rank, self.rope_head_dim) {
            return Some(layers * u64::from(rank + rope));
        }
        let kv_heads = u64::from(self.kv_heads.or(self.attention_heads)?);
        Some(layers * kv_heads * u64::from(self.head_dim?) * 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_geometry_is_latent_or_grouped_or_unknown() {
        let mla = ModelFacts {
            layers: Some(43),
            kv_lora_rank: Some(512),
            rope_head_dim: Some(64),
            ..ModelFacts::default()
        };
        assert_eq!(mla.kv_elements_per_token(), Some(43 * 576));

        let gqa = ModelFacts {
            layers: Some(36),
            attention_heads: Some(32),
            kv_heads: Some(8),
            head_dim: Some(128),
            ..ModelFacts::default()
        };
        assert_eq!(gqa.kv_elements_per_token(), Some(36 * 8 * 128 * 2));

        let hybrid = ModelFacts {
            kv_layers: Some(10),
            ..gqa.clone()
        };
        assert_eq!(hybrid.kv_elements_per_token(), Some(10 * 8 * 128 * 2));

        assert_eq!(ModelFacts::default().kv_elements_per_token(), None);
    }
}
