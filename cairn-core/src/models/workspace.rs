//! Workspace and settings types.

use serde::{Deserialize, Deserializer, Serialize};

use std::collections::HashMap;

use super::common::{MergeType, Model, Preset, ThinkingDisplayMode, ToolDetailLevel};

/// Custom deserializer for nullable optional fields in UpdateSettings.
/// Distinguishes between:
/// - Field missing from JSON → None (no update)
/// - Field present with null → Some(None) (clear/disable)
/// - Field present with value → Some(Some(v)) (set to v)
fn deserialize_optional_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    // This is called only if the field is present in the JSON
    // If the field is missing, serde uses the default (None)
    let value: Option<T> = Option::deserialize(deserializer)?;
    Ok(Some(value))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub system_prompt: String,
    pub branch_prefix: String,
    pub default_model: Model,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Settings DTO for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    // === Preset fields ===
    pub active_backend: String,
    pub default_tier: String,
    pub tiers: Vec<String>,
    pub backends: HashMap<String, HashMap<String, Preset>>,

    pub branch_prefix: String,
    pub system_prompt: String,
    pub max_thinking_tokens: Option<i32>,
    pub merge_type: MergeType,
    pub pull_on_merge: bool,
    /// Always true — setting removed, kept for serialization compat.
    pub auto_start_jobs: bool,
    pub timezone: String,
    pub orphan_cleanup_days: i32,
    /// Selected app bundle path for "Open In -> Terminal" (None = Terminal.app fallback)
    pub default_terminal: Option<String>,
    /// Selected app bundle path for "Open In -> Default App" (None = system default)
    pub default_app: Option<String>,
    /// Selected audio input device name for voice input (None = system default)
    pub audio_device: Option<String>,
    /// Selected whisper model name for voice input (None = no model selected)
    pub whisper_model: Option<String>,
    /// Whether agent bug reports are enabled (default: true)
    pub bug_reports: bool,
    /// API key for web search (Jina Search)
    pub web_search_api_key: Option<String>,
    /// Lookup tool display detail level (Read, Grep, Glob, non-committing Bash, etc.)
    pub lookup_detail_level: ToolDetailLevel,
    /// Change tool display detail level (Edit, Write, committing Bash)
    pub change_detail_level: ToolDetailLevel,
    /// Thinking block display mode in chat transcripts
    pub thinking_display_mode: ThinkingDisplayMode,
}

/// DTO for updating settings
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSettings {
    // === Preset fields ===
    pub active_backend: Option<String>,
    pub default_tier: Option<String>,
    pub tiers: Option<Vec<String>>,
    pub backends: Option<HashMap<String, HashMap<String, Preset>>>,

    pub branch_prefix: Option<String>,
    /// Deprecated - system_prompt is no longer used
    #[allow(dead_code)]
    pub system_prompt: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub max_thinking_tokens: Option<Option<i32>>,
    pub merge_type: Option<MergeType>,
    pub pull_on_merge: Option<bool>,
    /// Deprecated — auto_start_jobs is always true. Kept for deserialization compat.
    #[allow(dead_code)]
    pub auto_start_jobs: Option<bool>,
    pub timezone: Option<String>,
    pub orphan_cleanup_days: Option<i32>,
    /// Selected app bundle path for "Open In -> Terminal" (None = no change, Some(None) = clear)
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub default_terminal: Option<Option<String>>,
    /// Selected app bundle path for "Open In -> Default App" (None = no change, Some(None) = clear)
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub default_app: Option<Option<String>>,
    /// Selected audio input device name for voice input (None = no change, Some(None) = use default)
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub audio_device: Option<Option<String>>,
    /// Selected whisper model name (None = no change, Some(None) = clear selection)
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub whisper_model: Option<Option<String>>,
    /// Whether agent bug reports are enabled
    pub bug_reports: Option<bool>,
    /// API key for web search (Jina Search)
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub web_search_api_key: Option<Option<String>>,
    /// Lookup tool display detail level
    pub lookup_detail_level: Option<ToolDetailLevel>,
    /// Change tool display detail level
    pub change_detail_level: Option<ToolDetailLevel>,
    /// Thinking block display mode in chat transcripts
    pub thinking_display_mode: Option<ThinkingDisplayMode>,
}
