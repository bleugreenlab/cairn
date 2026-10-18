//! What an embedding offer can do: the capability payload of
//! [`crate::catalog::Verb::Embed`].
//!
//! A vector is only comparable with vectors from the same model at the same
//! width, so the facts that decide which widths an offer can return, and how
//! it tells a document from a query, are declared here and checked on every
//! request and reply. Nothing in this module is a float: capabilities are
//! catalog values and compare exactly.

use serde::{Deserialize, Serialize};

/// The id the Cairn gateway's one embedding model is offered and stored under.
pub const CAIRN_GATEWAY_EMBED_MODEL: &str = "cohere-embed-v4";

/// The provider key the Cairn gateway is offered under.
pub const CAIRN_GATEWAY_KEY: &str = "cairn";

/// Which widths an offer can return. Every width is in dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EmbeddingWidths {
    /// Only the native width; any other request is refused.
    #[default]
    NativeOnly,
    /// These widths and the native one (Cohere v4's 256, 512, 1024, 1536).
    Choices(Vec<u32>),
    /// Matryoshka: any width from `min` up to the native width, sent as
    /// `dimensions`.
    UpTo { min: u32 },
}

/// How the model wants a document told apart from a query. The facet applies
/// it, so a consumer only ever states the purpose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PurposeHandling {
    /// A request field carries it (the gateway's `input_type`).
    Native,
    /// An instruction prefix on each text; either side may be empty.
    Prefix { document: String, query: String },
    /// The model makes no distinction.
    Ignored,
}

/// How much one request may carry. The facet splits a larger request into
/// consecutive requests within these limits rather than refusing it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_texts: Option<u32>,
    /// Characters summed over one request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<u32>,
    /// Tokens per text. Shown, not enforced: the facet does not tokenize, so
    /// the provider's own refusal is what a too-long text meets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_tokens: Option<u32>,
}

/// Everything an embedding offer declares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingCapabilities {
    /// `None`: the provider did not say. Then only a native-width request is
    /// admitted, and the result reports the width measured on the reply.
    #[serde(default)]
    pub native_dims: Option<u32>,
    #[serde(default)]
    pub widths: EmbeddingWidths,
    pub purpose: PurposeHandling,
    #[serde(default)]
    pub limits: BatchLimits,
}

impl EmbeddingCapabilities {
    /// The Cairn gateway's `/embed` (Cohere Embed v4 on Bedrock).
    ///
    /// Fixed at 1536 and native-only: the gateway accepts a `dims` field but
    /// never reads it, and always asks Bedrock for 1536 (`api/src/routes/embed.ts`,
    /// `api/src/lib/bedrock.ts`). Honouring other widths needs a new request
    /// field on the server, because installed clients already send `dims` and
    /// store whatever width comes back. At most 96 texts per request, Cohere's
    /// cap, which the gateway's own batcher flushes at.
    pub fn cairn_gateway() -> Self {
        Self {
            native_dims: Some(1536),
            widths: EmbeddingWidths::NativeOnly,
            purpose: PurposeHandling::Native,
            limits: BatchLimits {
                max_texts: Some(96),
                max_chars: None,
                max_input_tokens: None,
            },
        }
    }

    /// Whether these capabilities are self-consistent: an offer that does not
    /// know its native width can only be native-only, a Matryoshka floor lies
    /// at or under the native width, and a choice list names something.
    pub fn validate(&self) -> Result<(), String> {
        if self.native_dims == Some(0) {
            return Err("an embedding width must be greater than zero".to_string());
        }
        match (&self.widths, self.native_dims) {
            (EmbeddingWidths::NativeOnly, _) => Ok(()),
            (_, None) => Err(
                "an embedding model whose native width is unknown can only be native-only"
                    .to_string(),
            ),
            (EmbeddingWidths::UpTo { min }, Some(native)) if *min == 0 || *min > native => {
                Err(format!(
                    "a Matryoshka floor of {min} must be between 1 and the native width {native}"
                ))
            }
            (EmbeddingWidths::Choices(widths), Some(_)) if widths.is_empty() => {
                Err("a width choice list must name at least one width".to_string())
            }
            (EmbeddingWidths::Choices(widths), Some(_)) if widths.contains(&0) => {
                Err("an embedding width must be greater than zero".to_string())
            }
            _ => Ok(()),
        }
    }

    /// One line a catalog shows: `2560 · native width only · query prefix ·
    /// ≤32 texts`.
    pub fn summary(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        parts.push(match self.native_dims {
            Some(native) => native.to_string(),
            None => "width unknown".to_string(),
        });
        parts.push(match &self.widths {
            EmbeddingWidths::NativeOnly => "native width only".to_string(),
            EmbeddingWidths::Choices(widths) => format!(
                "widths {}",
                widths
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join("/")
            ),
            EmbeddingWidths::UpTo { min } => format!("any width {min}–native"),
        });
        parts.push(
            match &self.purpose {
                PurposeHandling::Native => "native purpose",
                PurposeHandling::Prefix { document, query } => {
                    match (document.is_empty(), query.is_empty()) {
                        (true, false) => "query prefix",
                        (false, true) => "document prefix",
                        _ => "purpose prefixes",
                    }
                }
                PurposeHandling::Ignored => "no purpose",
            }
            .to_string(),
        );
        if let Some(texts) = self.limits.max_texts {
            parts.push(format!("≤{texts} texts"));
        }
        if let Some(chars) = self.limits.max_chars {
            parts.push(format!("≤{chars} chars"));
        }
        parts.join(" · ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(capabilities: &EmbeddingCapabilities) -> serde_json::Value {
        let value = serde_json::to_value(capabilities).unwrap();
        assert_eq!(
            &serde_json::from_value::<EmbeddingCapabilities>(value.clone()).unwrap(),
            capabilities
        );
        value
    }

    #[test]
    fn every_width_and_purpose_shape_round_trips() {
        assert_eq!(
            round_trip(&EmbeddingCapabilities::cairn_gateway()),
            json!({
                "nativeDims": 1536,
                "widths": "nativeOnly",
                "purpose": "native",
                "limits": {"maxTexts": 96}
            })
        );
        let matryoshka = EmbeddingCapabilities {
            native_dims: Some(768),
            widths: EmbeddingWidths::UpTo { min: 64 },
            purpose: PurposeHandling::Prefix {
                document: "search_document: ".into(),
                query: "search_query: ".into(),
            },
            limits: BatchLimits {
                max_texts: Some(32),
                max_chars: Some(32_000),
                max_input_tokens: Some(2048),
            },
        };
        assert_eq!(
            round_trip(&matryoshka),
            json!({
                "nativeDims": 768,
                "widths": {"upTo": {"min": 64}},
                "purpose": {"prefix": {"document": "search_document: ", "query": "search_query: "}},
                "limits": {"maxTexts": 32, "maxChars": 32000, "maxInputTokens": 2048}
            })
        );
        let choices = EmbeddingCapabilities {
            native_dims: Some(1536),
            widths: EmbeddingWidths::Choices(vec![256, 512, 1024]),
            purpose: PurposeHandling::Ignored,
            limits: BatchLimits::default(),
        };
        assert_eq!(
            round_trip(&choices),
            json!({
                "nativeDims": 1536,
                "widths": {"choices": [256, 512, 1024]},
                "purpose": "ignored",
                "limits": {}
            })
        );
    }

    #[test]
    fn an_unknown_native_width_admits_only_native_only() {
        let mut capabilities = EmbeddingCapabilities {
            native_dims: None,
            widths: EmbeddingWidths::NativeOnly,
            purpose: PurposeHandling::Ignored,
            limits: BatchLimits::default(),
        };
        assert_eq!(capabilities.validate(), Ok(()));
        capabilities.widths = EmbeddingWidths::UpTo { min: 32 };
        assert!(capabilities.validate().is_err());
        capabilities.native_dims = Some(16);
        assert!(capabilities.validate().unwrap_err().contains("floor of 32"));
        capabilities.native_dims = Some(2560);
        assert_eq!(capabilities.validate(), Ok(()));
        capabilities.widths = EmbeddingWidths::Choices(vec![]);
        assert!(capabilities.validate().is_err());
        assert_eq!(EmbeddingCapabilities::cairn_gateway().validate(), Ok(()));
    }

    #[test]
    fn a_summary_names_width_purpose_and_limits() {
        assert_eq!(
            EmbeddingCapabilities::cairn_gateway().summary(),
            "1536 · native width only · native purpose · ≤96 texts"
        );
        let qwen = EmbeddingCapabilities {
            native_dims: Some(2560),
            widths: EmbeddingWidths::NativeOnly,
            purpose: PurposeHandling::Prefix {
                document: String::new(),
                query: "Instruct: x\nQuery: ".into(),
            },
            limits: BatchLimits {
                max_texts: Some(32),
                ..BatchLimits::default()
            },
        };
        assert_eq!(
            qwen.summary(),
            "2560 · native width only · query prefix · ≤32 texts"
        );
    }
}
