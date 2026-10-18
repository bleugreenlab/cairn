//! Which tokenizer an offer uses, as pinned knowledge Cairn ships.
//!
//! An exact token meter needs the model's own tokenizer, and the bytes it
//! parses must be ones Cairn chose: toks, the parser behind the exact meters,
//! is young and unfuzzed. So the files are pinned here by repository, commit,
//! size and sha256, and nothing else is ever parsed. A provider can only
//! *select* a pin, by the upstream Hugging Face identity it reports for an
//! offer (OpenRouter's `hugging_face_id`, vLLM's `root`, an Actual registry
//! id) or, for a provider that reports none, by an exact `(provider, model)`
//! pair listed on the pin. Selection is exact equality, never a name pattern.
//!
//! A pin's identity is its digest: a single file's sha256, or for a tokenizer
//! that is several files (Kimi's `tiktoken.model` beside its wrapper) the
//! sha256 of its manifest, the sorted lines `path\0sha256hex\n`. A
//! [`TokenizerRef`] carries that digest, so a pin published some other way
//! later (by a serving node, say) names a tokenizer in the same shape.
//!
//! Every row below was checked by `cairn-toks`' fleet test: fetched at the
//! pinned revision, verified, loaded by toks, and compared with the reference
//! tokenizer's counts. A family whose tokenizer toks refuses gets no row.

use std::borrow::Cow;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The tokenizer an offer declares: a family label for people, and the
/// digest that is its identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenizerRef {
    pub family: Cow<'static, str>,
    #[serde(serialize_with = "serialize_hex", deserialize_with = "deserialize_hex")]
    pub sha256: [u8; 32],
}

/// One file of a pinned tokenizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinnedFile {
    /// Path within the repository, and within the cache directory.
    pub path: &'static str,
    /// Lowercase hex sha256.
    pub sha256: &'static str,
    pub size: u64,
}

/// A tokenizer Cairn trusts, where its bytes come from, and which offers use it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenizerPin {
    pub family: &'static str,
    /// The Hugging Face repository the files are fetched from.
    pub repo: &'static str,
    /// A full commit sha, never a branch, so the bytes cannot move.
    pub revision: &'static str,
    /// One `tokenizer.json`, or every file toks reads for a tiktoken model.
    pub files: &'static [PinnedFile],
    /// Lowercase hex: the single file's sha256, or the manifest digest.
    pub digest: &'static str,
    /// Hugging Face repository ids whose tokenizer is exactly these bytes,
    /// compared ignoring ASCII case as Hugging Face compares them.
    pub upstreams: &'static [&'static str],
    /// `(provider, model)` pairs, for providers that report no upstream id.
    pub offers: &'static [(&'static str, &'static str)],
}

impl TokenizerPin {
    /// A tokenizer toks loads from a directory rather than from one buffer.
    pub fn is_directory(&self) -> bool {
        self.files.len() > 1
    }

    pub fn digest_bytes(&self) -> [u8; 32] {
        parse_hex(self.digest).expect("pinned digests are 64 hex digits")
    }

    pub fn reference(&self) -> TokenizerRef {
        TokenizerRef {
            family: Cow::Borrowed(self.family),
            sha256: self.digest_bytes(),
        }
    }
}

const OPENCODE_GO: &str = crate::catalog::OPENCODE_BACKEND_KEY;

const fn tokenizer_json(sha256: &'static str, size: u64) -> PinnedFile {
    PinnedFile {
        path: "tokenizer.json",
        sha256,
        size,
    }
}

/// Kimi K2.5 and K3 share one `tiktoken.model`; their wrappers differ, and
/// toks reads the wrapper to identify the encoding, so each is its own pin.
const KIMI_TIKTOKEN: PinnedFile = PinnedFile {
    path: "tiktoken.model",
    sha256: "b6c497a7469b33ced9c38afb1ad6e47f03f5e5dc05f15930799210ec050c5103",
    size: 2_795_286,
};

const GLM_5: &[PinnedFile] = &[tokenizer_json(
    "19e773648cb4e65de8660ea6365e10acca112d42a854923df93db4a6f333a82d",
    20_217_442,
)];
const KIMI_K3: &[PinnedFile] = &[
    PinnedFile {
        path: "encoding_k3.py",
        sha256: "49ff03305fdc4be26867972788d36150b67f8a9e852e62bb7959d87482223676",
        size: 26_304,
    },
    KIMI_TIKTOKEN,
    PinnedFile {
        path: "tokenization_kimi.py",
        sha256: "f28ea66e2d862a2a5814970b2ce40c2f7d8296ff09aed90a7e7def689b906944",
        size: 16_145,
    },
    PinnedFile {
        path: "tokenizer_config.json",
        sha256: "5d0803c94db9cd78763499e0956c95fd5a225c14a727e5a6cf5db3f96f010a6e",
        size: 3_478,
    },
];
const KIMI_K2_5: &[PinnedFile] = &[
    KIMI_TIKTOKEN,
    PinnedFile {
        path: "tokenization_kimi.py",
        sha256: "56aae6f32aa6d8604a119ed6097fd07968c3bc2fbbe225f151acbed9141add6f",
        size: 13_353,
    },
    PinnedFile {
        path: "tokenizer_config.json",
        sha256: "12fcab43d2b6068f46769f5ff373960bf7c17a94d7abbc50e2491306b2f6cf58",
        size: 4_862,
    },
];
const DEEPSEEK_V4_1: &[PinnedFile] = &[tokenizer_json(
    "c90dfa01249db1be4245780a052ede752e1361c612ac6d08e2bdada7d599476b",
    6_367_257,
)];
const DEEPSEEK_V4: &[PinnedFile] = &[tokenizer_json(
    "8f9f37ca37fdc4f5fd36d5cf4d3b0e8392edb4e894fd10cc0d70b4957c8633cf",
    6_367_146,
)];
const DEEPSEEK_V3_2: &[PinnedFile] = &[tokenizer_json(
    "cd050be35cae877f8f0aa847f45aa87e23835a56ca32b29b28545597852784e5",
    7_847_502,
)];
const QWEN_3_8: &[PinnedFile] = &[tokenizer_json(
    "0997f410c57a1f4e53b09e4be8f4a172d90edd9564368fb0847030937229b9f3",
    12_809_320,
)];
const QWEN_3_8_UNSLOTH_NVFP4: &[PinnedFile] = &[tokenizer_json(
    "06b9509352d2af50381ab2247e083b80d32d5c0aba91c272ca9ff729b6a0e523",
    19_989_325,
)];
const GEMMA_4: &[PinnedFile] = &[tokenizer_json(
    "cc8d3a0ce36466ccc1278bf987df5f71db1719b9ca6b4118264f45cb627bfe0f",
    32_169_626,
)];
const MINIMAX_M3: &[PinnedFile] = &[tokenizer_json(
    "bb1f1626cf01448f1e3b6036d0a061ffc66c91d9046aada14ea23a5441b5ad6e",
    9_731_500,
)];
const MIMO_V2_5: &[PinnedFile] = &[tokenizer_json(
    "633518aad78f9f61bae2ae420d621215754a4424c918b052cd8c22a3b59e99d2",
    7_033_572,
)];

const PINS: &[TokenizerPin] = &[
    TokenizerPin {
        family: "glm-5",
        repo: "zai-org/GLM-5.3",
        revision: "ba32f8c19c8738e2c57c71798c601d477a7bf844",
        files: GLM_5,
        digest: "19e773648cb4e65de8660ea6365e10acca112d42a854923df93db4a6f333a82d",
        upstreams: &[
            "zai-org/GLM-5",
            "zai-org/GLM-5.1",
            "zai-org/GLM-5.2",
            "zai-org/GLM-5.3",
            "zai-org/GLM-5.3-Flash",
        ],
        offers: &[
            (OPENCODE_GO, "glm-5.1"),
            (OPENCODE_GO, "glm-5.2"),
            (OPENCODE_GO, "glm-5.3"),
            (OPENCODE_GO, "glm-5.3-flash"),
        ],
    },
    TokenizerPin {
        family: "kimi-k3",
        repo: "moonshotai/Kimi-K3",
        revision: "f831ab66814297da540d832a5235f8e904f29d06",
        files: KIMI_K3,
        digest: "9daf23e519d7119b3cda6ba28922449503b6d4a0b3749f8c97d173a111490748",
        upstreams: &["moonshotai/Kimi-K3"],
        offers: &[(OPENCODE_GO, "kimi-k3")],
    },
    TokenizerPin {
        family: "kimi-k2.5",
        repo: "moonshotai/Kimi-K2.5",
        revision: "4d01dfe0332d63057c186e0b262165819efb6611",
        files: KIMI_K2_5,
        digest: "c1dde261820d8bb00602430e4f28d154b71cb74308abadaa4f10fd8bea532221",
        upstreams: &["moonshotai/Kimi-K2.5"],
        offers: &[],
    },
    TokenizerPin {
        family: "deepseek-v4.1",
        repo: "deepseek-ai/DeepSeek-V4.1-Flash",
        revision: "314113f8a7dadb388215396ba5fbee2c610a04d6",
        files: DEEPSEEK_V4_1,
        digest: "c90dfa01249db1be4245780a052ede752e1361c612ac6d08e2bdada7d599476b",
        upstreams: &[
            "deepseek-ai/DeepSeek-V4.1-Flash",
            "deepseek-ai/DeepSeek-V4.1-Flash-1017",
        ],
        offers: &[(OPENCODE_GO, "deepseek-v4.1-flash")],
    },
    TokenizerPin {
        family: "deepseek-v4",
        repo: "deepseek-ai/DeepSeek-V4-Flash",
        revision: "60d8d70770c6776ff598c94bb586a859a38244f1",
        files: DEEPSEEK_V4,
        digest: "8f9f37ca37fdc4f5fd36d5cf4d3b0e8392edb4e894fd10cc0d70b4957c8633cf",
        upstreams: &[
            "deepseek-ai/DeepSeek-V4-Flash",
            "deepseek-ai/DeepSeek-V4-Pro",
        ],
        offers: &[(OPENCODE_GO, "deepseek-v4-pro")],
    },
    TokenizerPin {
        family: "deepseek-v3.2",
        repo: "deepseek-ai/DeepSeek-V3.2",
        revision: "a7e62ac04ecb2c0a54d736dc46601c5606cf10a6",
        files: DEEPSEEK_V3_2,
        digest: "cd050be35cae877f8f0aa847f45aa87e23835a56ca32b29b28545597852784e5",
        upstreams: &["deepseek-ai/DeepSeek-V3.2"],
        offers: &[],
    },
    TokenizerPin {
        family: "qwen3.8",
        repo: "Qwen/Qwen3.8-27B",
        revision: "1d4bf0f2ff6012fd82039f2fa52739d0dd7c60c0",
        files: QWEN_3_8,
        digest: "0997f410c57a1f4e53b09e4be8f4a172d90edd9564368fb0847030937229b9f3",
        upstreams: &["Qwen/Qwen3.8-27B", "Qwen/Qwen3.8-Flash-Next"],
        offers: &[],
    },
    TokenizerPin {
        // A quantized repack ships its own serialization of the file, so it is
        // pinned as served rather than assumed equal to the original.
        family: "qwen3.8",
        repo: "unsloth/Qwen3.8-27B-NVFP4",
        revision: "f0b7c9e722f5565102fff8481c99e4d86ae099c7",
        files: QWEN_3_8_UNSLOTH_NVFP4,
        digest: "06b9509352d2af50381ab2247e083b80d32d5c0aba91c272ca9ff729b6a0e523",
        upstreams: &["unsloth/Qwen3.8-27B-NVFP4"],
        offers: &[],
    },
    TokenizerPin {
        family: "gemma-4",
        repo: "google/gemma-4-12B-it",
        revision: "707f0a3b8a3c7ad586ed01e27eafbad8a27dd0f7",
        files: GEMMA_4,
        digest: "cc8d3a0ce36466ccc1278bf987df5f71db1719b9ca6b4118264f45cb627bfe0f",
        upstreams: &[
            "google/gemma-4-12B-it",
            "google/gemma-4-e2b-it",
            "google/gemma-4-26B-A4B-it",
            "google/gemma-4-31B-it",
        ],
        offers: &[],
    },
    TokenizerPin {
        family: "minimax-m3",
        repo: "MiniMaxAI/MiniMax-M3",
        revision: "f0e1c1e04d40177e4673a22097036854f536e9c0",
        files: MINIMAX_M3,
        digest: "bb1f1626cf01448f1e3b6036d0a061ffc66c91d9046aada14ea23a5441b5ad6e",
        upstreams: &["MiniMaxAI/MiniMax-M3"],
        offers: &[(OPENCODE_GO, "minimax-m3")],
    },
    TokenizerPin {
        family: "mimo-v2.5",
        repo: "XiaomiMiMo/MiMo-V2.5",
        revision: "63651580ca774f8504f676040460aed3e1244ac1",
        files: MIMO_V2_5,
        digest: "633518aad78f9f61bae2ae420d621215754a4424c918b052cd8c22a3b59e99d2",
        upstreams: &["XiaomiMiMo/MiMo-V2.5"],
        offers: &[(OPENCODE_GO, "mimo-v2.5")],
    },
];

/// Every shipped pin.
pub fn pins() -> &'static [TokenizerPin] {
    PINS
}

/// The pin a digest names, if Cairn ships one.
pub fn pin_for_digest(digest: &[u8; 32]) -> Option<&'static TokenizerPin> {
    PINS.iter().find(|pin| pin.digest_bytes() == *digest)
}

/// The tokenizer an offer uses: by its upstream Hugging Face id when the
/// provider reports one, else by an exact `(provider, model)` pair.
pub fn tokenizer_for(provider: &str, model: &str, upstream: Option<&str>) -> Option<TokenizerRef> {
    let upstream = upstream.map(str::trim).filter(|id| !id.is_empty());
    let by_upstream = upstream.and_then(|upstream| {
        PINS.iter().find(|pin| {
            pin.upstreams
                .iter()
                .any(|known| known.eq_ignore_ascii_case(upstream))
        })
    });
    by_upstream
        .or_else(|| {
            PINS.iter()
                .find(|pin| pin.offers.iter().any(|&(p, m)| p == provider && m == model))
        })
        .map(TokenizerPin::reference)
}

fn parse_hex(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn serialize_hex<S: Serializer>(digest: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error> {
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    serializer.serialize_str(&hex)
}

fn deserialize_hex<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 32], D::Error> {
    let hex = String::deserialize(deserializer)?;
    parse_hex(&hex.to_ascii_lowercase())
        .ok_or_else(|| serde::de::Error::custom("a tokenizer digest is 64 hex digits"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn every_pin_is_well_formed() {
        for pin in PINS {
            assert!(!pin.files.is_empty(), "{}", pin.family);
            assert_eq!(pin.revision.len(), 40, "{}: a full commit sha", pin.family);
            assert!(pin.revision.bytes().all(|b| b.is_ascii_hexdigit()));
            for file in pin.files {
                assert!(
                    parse_hex(file.sha256).is_some(),
                    "{} {}",
                    pin.family,
                    file.path
                );
                assert!(
                    file.size > 0 && file.size <= 256 << 20,
                    "toks' source limit"
                );
                assert!(!file.path.contains("..") && !file.path.starts_with('/'));
            }
            assert!(
                !pin.upstreams.is_empty() || !pin.offers.is_empty(),
                "{} is selectable",
                pin.family
            );
        }
    }

    /// A single file's digest is the file's; a directory's is its manifest's.
    #[test]
    fn digests_follow_the_manifest_rule() {
        for pin in PINS {
            let expected = if pin.is_directory() {
                let mut lines: Vec<String> = pin
                    .files
                    .iter()
                    .map(|file| format!("{}\0{}\n", file.path, file.sha256))
                    .collect();
                lines.sort();
                hex(&Sha256::digest(lines.concat().as_bytes()))
            } else {
                pin.files[0].sha256.to_string()
            };
            assert_eq!(pin.digest, expected, "{}", pin.family);
        }
    }

    #[test]
    fn digests_are_unique_and_resolve_back_to_their_pin() {
        for pin in PINS {
            let found = pin_for_digest(&pin.digest_bytes()).unwrap();
            assert_eq!(found.repo, pin.repo);
        }
    }

    #[test]
    fn upstream_identity_selects_exactly() {
        let glm = tokenizer_for("openrouter", "z-ai/glm-5.3", Some("zai-org/GLM-5.3")).unwrap();
        assert_eq!(glm.family, "glm-5");
        // Hugging Face ids are case-insensitive; OpenRouter reports this one
        // as `MiniMaxAI/Minimax-M3`.
        let minimax = tokenizer_for(
            "openrouter",
            "minimax/minimax-m3",
            Some("MiniMaxAI/Minimax-M3"),
        )
        .unwrap();
        assert_eq!(minimax.family, "minimax-m3");
        // Near misses select nothing: no prefixes, no patterns.
        assert_eq!(
            tokenizer_for("openrouter", "x", Some("zai-org/GLM-5.3-Turbo")),
            None
        );
        assert_eq!(tokenizer_for("openrouter", "x", Some("zai-org/GLM")), None);
        assert_eq!(tokenizer_for("openrouter", "x", Some("")), None);
    }

    #[test]
    fn offers_select_for_providers_without_upstream_ids() {
        let kimi = tokenizer_for(OPENCODE_GO, "kimi-k3", None).unwrap();
        assert_eq!(kimi.family, "kimi-k3");
        assert_eq!(
            tokenizer_for(OPENCODE_GO, "glm-5.2", None).unwrap().family,
            "glm-5"
        );
        // The pair is exact: the same model name on another provider, or a
        // tier name, selects nothing.
        assert_eq!(tokenizer_for("ollama", "kimi-k3", None), None);
        assert_eq!(tokenizer_for(OPENCODE_GO, "lg", None), None);
        assert_eq!(tokenizer_for(OPENCODE_GO, "KIMI-K3", None), None);
    }

    #[test]
    fn references_serialize_as_hex() {
        let reference = tokenizer_for(OPENCODE_GO, "glm-5.2", None).unwrap();
        let json = serde_json::to_value(&reference).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "family": "glm-5",
                "sha256": "19e773648cb4e65de8660ea6365e10acca112d42a854923df93db4a6f333a82d",
            })
        );
        let back: TokenizerRef = serde_json::from_value(json).unwrap();
        assert_eq!(back, reference);
        assert!(serde_json::from_value::<TokenizerRef>(
            serde_json::json!({"family": "x", "sha256": "abc"})
        )
        .is_err());
    }
}
