//! Workspace and settings types.

use serde::{Deserialize, Deserializer, Serialize};

use super::common::{MergeType, Model};

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
    pub default_model: Model,
    pub branch_prefix: String,
    pub system_prompt: String,
    pub max_thinking_tokens: Option<i32>,
    pub merge_type: MergeType,
    pub pull_on_merge: bool,
    pub auto_start_jobs: bool,
    pub timezone: String,
    pub orphan_cleanup_days: i32,
    /// Selected audio input device name for voice input (None = system default)
    pub audio_device: Option<String>,
    /// Selected whisper model name for voice input (None = no model selected)
    pub whisper_model: Option<String>,
    /// Whether agent bug reports are enabled (default: true)
    pub bug_reports: bool,
}

/// DTO for updating settings
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSettings {
    pub default_model: Option<Model>,
    pub branch_prefix: Option<String>,
    /// Deprecated - system_prompt is no longer used
    #[allow(dead_code)]
    pub system_prompt: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub max_thinking_tokens: Option<Option<i32>>,
    pub merge_type: Option<MergeType>,
    pub pull_on_merge: Option<bool>,
    pub auto_start_jobs: Option<bool>,
    pub timezone: Option<String>,
    pub orphan_cleanup_days: Option<i32>,
    /// Selected audio input device name for voice input (None = no change, Some(None) = use default)
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub audio_device: Option<Option<String>>,
    /// Selected whisper model name (None = no change, Some(None) = clear selection)
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub whisper_model: Option<Option<String>>,
    /// Whether agent bug reports are enabled
    pub bug_reports: Option<bool>,
}
