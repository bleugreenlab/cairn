//! Common types used across the application.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Model identifier for any agent backend.
///
/// A thin wrapper around a string that works uniformly across backends.
/// Well-known aliases (sonnet, opus, haiku) are provided as constants,
/// but any string is accepted — the backend interprets it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct Model(String);

impl Model {
    // Well-known Claude aliases
    pub const SONNET: &str = "sonnet";
    pub const OPUS: &str = "opus";
    pub const HAIKU: &str = "haiku";
    pub const FABLE: &str = "fable";
    pub const GPT_5_4_MINI: &str = "gpt-5.4-mini";

    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// The raw model identifier string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Model {
    fn default() -> Self {
        Self(Self::SONNET.to_owned())
    }
}

impl std::fmt::Display for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for Model {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_owned()))
    }
}

impl From<&str> for Model {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for Model {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// One option of the single model dropdown: a concrete model bound to the
/// backend that serves it.
///
/// Model and backend are not independent axes — every option a user picks fully
/// determines its backend, so a resolved choice travels as one atomic value
/// everywhere it appears. `RuntimeExtras` stays separate, being genuinely
/// orthogonal. No code path holds a free-floating model next to a separate
/// backend; the pair is always this struct.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelection {
    pub backend: String,
    pub model: Model,
}

impl ModelSelection {
    pub fn new(backend: impl Into<String>, model: Model) -> Self {
        Self {
            backend: backend.into(),
            model,
        }
    }
}

/// Merge type for pull requests
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MergeType {
    Merge,
    #[default]
    Squash,
    Rebase,
}

impl std::fmt::Display for MergeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeType::Merge => write!(f, "merge"),
            MergeType::Squash => write!(f, "squash"),
            MergeType::Rebase => write!(f, "rebase"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_and_as_str() {
        let m = Model::new("codex-mini");
        assert_eq!(m.as_str(), "codex-mini");
    }

    #[test]
    fn default_is_sonnet() {
        let m = Model::default();
        assert_eq!(m.as_str(), "sonnet");
    }

    #[test]
    fn display_formats_inner_string() {
        let m = Model::new("gpt-5-codex");
        assert_eq!(format!("{}", m), "gpt-5-codex");
    }

    #[test]
    fn from_str_accepts_any_string() {
        let m: Model = "custom-model-v2".parse().unwrap();
        assert_eq!(m.as_str(), "custom-model-v2");
    }

    #[test]
    fn from_str_ref() {
        let m: Model = Model::from("opus");
        assert_eq!(m.as_str(), "opus");
    }

    #[test]
    fn from_string() {
        let m: Model = Model::from("haiku".to_string());
        assert_eq!(m.as_str(), "haiku");
    }

    #[test]
    fn equality() {
        assert_eq!(Model::new("sonnet"), Model::new("sonnet"));
        assert_ne!(Model::new("sonnet"), Model::new("opus"));
    }

    #[test]
    fn serde_roundtrip_bare_string() {
        // Model serializes as a plain JSON string, not {"Sonnet": ...}
        let m = Model::new("opus");
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(json, r#""opus""#);

        let deserialized: Model = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, m);
    }

    #[test]
    fn serde_deserializes_arbitrary_model_string() {
        // Arbitrary Codex model names deserialize correctly
        let m: Model = serde_json::from_str(r#""gpt-5-codex""#).unwrap();
        assert_eq!(m.as_str(), "gpt-5-codex");
    }

    #[test]
    fn constants_match_expected_values() {
        assert_eq!(Model::SONNET, "sonnet");
        assert_eq!(Model::OPUS, "opus");
        assert_eq!(Model::HAIKU, "haiku");
        assert_eq!(Model::FABLE, "fable");
    }

    #[test]
    fn runtime_extras_default_is_empty() {
        let extras = RuntimeExtras::default();
        assert_eq!(extras.max_thinking_tokens, None);
        assert_eq!(extras.reasoning_effort, None);
    }

    #[test]
    fn runtime_extras_serde_roundtrip() {
        let extras = RuntimeExtras {
            max_thinking_tokens: Some(32768),
            reasoning_effort: Some("high".to_string()),
        };
        let json = serde_json::to_string(&extras).unwrap();
        assert!(json.contains("maxThinkingTokens"));
        assert!(json.contains("reasoningEffort"));
        let restored: RuntimeExtras = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, extras);
    }

    #[test]
    fn runtime_extras_skips_none_fields() {
        let extras = RuntimeExtras {
            max_thinking_tokens: Some(16384),
            reasoning_effort: None,
        };
        let json = serde_json::to_string(&extras).unwrap();
        assert!(json.contains("maxThinkingTokens"));
        assert!(!json.contains("reasoningEffort"));
    }

    #[test]
    fn runtime_extras_empty_deserializes_from_empty_object() {
        let extras: RuntimeExtras = serde_json::from_str("{}").unwrap();
        assert_eq!(extras, RuntimeExtras::default());
    }

    #[test]
    fn tool_detail_level_default_is_blurb() {
        assert_eq!(ToolDetailLevel::default(), ToolDetailLevel::Blurb);
    }

    #[test]
    fn tool_detail_level_serde_roundtrip() {
        for (variant, expected_str) in [
            (ToolDetailLevel::Compact, r#""compact""#),
            (ToolDetailLevel::Blurb, r#""blurb""#),
            (ToolDetailLevel::Full, r#""full""#),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected_str, "serialize {:?}", variant);
            let deserialized: ToolDetailLevel = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, variant, "deserialize {}", expected_str);
        }
    }

    #[test]
    fn tool_detail_level_display() {
        assert_eq!(format!("{}", ToolDetailLevel::Compact), "compact");
        assert_eq!(format!("{}", ToolDetailLevel::Blurb), "blurb");
        assert_eq!(format!("{}", ToolDetailLevel::Full), "full");
    }

    #[test]
    fn thinking_display_mode_default_is_collapsed() {
        assert_eq!(
            ThinkingDisplayMode::default(),
            ThinkingDisplayMode::Collapsed
        );
    }

    #[test]
    fn thinking_display_mode_serde_roundtrip() {
        for (variant, expected_str) in [
            (ThinkingDisplayMode::Collapsed, r#""collapsed""#),
            (ThinkingDisplayMode::Full, r#""full""#),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected_str, "serialize {:?}", variant);
            let deserialized: ThinkingDisplayMode = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, variant, "deserialize {}", expected_str);
        }
    }

    #[test]
    fn thinking_display_mode_display() {
        assert_eq!(format!("{}", ThinkingDisplayMode::Collapsed), "collapsed");
        assert_eq!(format!("{}", ThinkingDisplayMode::Full), "full");
    }

    #[test]
    fn preset_to_extras_maps_all_fields() {
        let preset = Preset {
            model: Model::new("sonnet"),
            options: HashMap::from([(
                "reasoningEffort".to_string(),
                PresetOptionValue::Str("high".to_string()),
            )]),
        };
        let extras = preset.to_extras();
        assert_eq!(extras.max_thinking_tokens, None);
        assert_eq!(extras.reasoning_effort, Some("high".to_string()));
    }

    #[test]
    fn preset_to_extras_none_fields() {
        let preset = Preset {
            model: Model::new("haiku"),
            options: HashMap::new(),
        };
        let extras = preset.to_extras();
        assert_eq!(extras.max_thinking_tokens, None);
        assert_eq!(extras.reasoning_effort, None);
    }

    #[test]
    fn preset_serde_roundtrip() {
        let preset = Preset {
            model: Model::new("sonnet"),
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&preset).unwrap();
        let parsed: Preset = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, preset);
        // options should be absent from JSON when empty
        assert!(!json.contains("options"));
    }

    #[test]
    fn preset_legacy_fields_migrate_to_options_and_drop_budget() {
        let json = r#"{
            "model": "sonnet",
            "reasoningEffort": "high",
            "maxThinkingTokens": 31999
        }"#;

        let preset: Preset = serde_json::from_str(json).unwrap();
        assert_eq!(
            preset
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str),
            Some("high")
        );
        assert_eq!(preset.to_extras().max_thinking_tokens, None);

        let serialized = serde_json::to_string(&preset).unwrap();
        assert!(!serialized.contains("maxThinkingTokens"));
        assert!(serialized.contains("options"));
    }
}

/// A single preset: concrete model + backend-specific options.
///
/// Presets are configured per-backend per-tier in workspace settings.
/// Example: `codex/md` → `{ model: "gpt-5.3-codex", options: { reasoningEffort: "medium" } }`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", from = "PresetFile")]
pub struct Preset {
    pub model: Model,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub options: HashMap<String, PresetOptionValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PresetOptionValue {
    Str(String),
    Bool(bool),
    Int(i64),
}

impl PresetOptionValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PresetOptionValue::Str(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PresetFile {
    model: Model,
    #[serde(default)]
    options: Option<HashMap<String, PresetOptionValue>>,
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default)]
    max_thinking_tokens: Option<i32>,
}

impl From<PresetFile> for Preset {
    fn from(file: PresetFile) -> Self {
        let mut options = file.options.unwrap_or_default();
        if let Some(effort) = file.reasoning_effort {
            options
                .entry("reasoningEffort".to_string())
                .or_insert(PresetOptionValue::Str(effort));
        }
        let _ = file.max_thinking_tokens; // Intentionally discarded.
        Preset {
            model: file.model,
            options,
        }
    }
}

impl Preset {
    /// Convert this preset's options into backend runtime extras.
    pub fn to_extras(&self) -> RuntimeExtras {
        RuntimeExtras {
            max_thinking_tokens: None,
            reasoning_effort: self
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
        }
    }
}

/// Backend-specific runtime parameters.
///
/// These can be set at the agent level and override workspace defaults.
/// Both Claude and Codex now select reasoning via `reasoning_effort`
/// (Claude's CLI replaced `--max-thinking-tokens` with `--effort`).
/// `max_thinking_tokens` is retained only for backward-compatible
/// deserialization of older Claude presets; it is no longer sent to the CLI.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeExtras {
    /// Legacy Claude budget — deserialized for compat, mapped to effort "high".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_thinking_tokens: Option<i32>,
    /// Reasoning effort ("low", "medium", "high", "xhigh", "max"; None = backend default)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
}

/// Detail level for tool call display in chat transcripts
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ToolDetailLevel {
    Compact,
    #[default]
    Blurb,
    Full,
}

impl std::fmt::Display for ToolDetailLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolDetailLevel::Compact => write!(f, "compact"),
            ToolDetailLevel::Blurb => write!(f, "blurb"),
            ToolDetailLevel::Full => write!(f, "full"),
        }
    }
}

/// Display mode for thinking blocks in chat transcripts
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ThinkingDisplayMode {
    #[default]
    Collapsed,
    Full,
}

impl std::fmt::Display for ThinkingDisplayMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ThinkingDisplayMode::Collapsed => write!(f, "collapsed"),
            ThinkingDisplayMode::Full => write!(f, "full"),
        }
    }
}

impl std::str::FromStr for MergeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "merge" => Ok(MergeType::Merge),
            "squash" => Ok(MergeType::Squash),
            "rebase" => Ok(MergeType::Rebase),
            _ => Err(format!("Unknown merge type: {}", s)),
        }
    }
}
