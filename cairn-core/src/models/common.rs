//! Common types used across the application.

use serde::{Deserialize, Serialize};

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
            max_thinking_tokens: Some(31999),
            reasoning_effort: Some("high".to_string()),
        };
        let extras = preset.to_extras();
        assert_eq!(extras.max_thinking_tokens, Some(31999));
        assert_eq!(extras.reasoning_effort, Some("high".to_string()));
    }

    #[test]
    fn preset_to_extras_none_fields() {
        let preset = Preset {
            model: Model::new("haiku"),
            max_thinking_tokens: None,
            reasoning_effort: None,
        };
        let extras = preset.to_extras();
        assert_eq!(extras.max_thinking_tokens, None);
        assert_eq!(extras.reasoning_effort, None);
    }

    #[test]
    fn preset_serde_roundtrip() {
        let preset = Preset {
            model: Model::new("sonnet"),
            max_thinking_tokens: Some(31999),
            reasoning_effort: None,
        };
        let json = serde_json::to_string(&preset).unwrap();
        let parsed: Preset = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, preset);
        // reasoning_effort should be absent from JSON when None
        assert!(!json.contains("reasoningEffort"));
    }
}

/// A single preset: concrete model + backend-specific runtime parameters.
///
/// Presets are configured per-backend per-tier in workspace settings.
/// Example: `claude/md` → `{ model: "sonnet", max_thinking_tokens: Some(31999) }`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Preset {
    pub model: Model,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_thinking_tokens: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
}

impl Preset {
    /// Convert this preset's extras into a RuntimeExtras.
    pub fn to_extras(&self) -> RuntimeExtras {
        RuntimeExtras {
            max_thinking_tokens: self.max_thinking_tokens,
            reasoning_effort: self.reasoning_effort.clone(),
        }
    }
}

/// Backend-specific runtime parameters.
///
/// These can be set at the agent level and override workspace defaults.
/// Each backend uses the fields relevant to it (e.g. Claude uses
/// `max_thinking_tokens`, Codex uses `reasoning_effort`).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeExtras {
    /// Claude: max thinking tokens (None = inherit from workspace)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_thinking_tokens: Option<i32>,
    /// Codex: reasoning effort ("low", "medium", "high"; None = inherit/default)
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
