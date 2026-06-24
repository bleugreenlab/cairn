//! Workspace and settings types.

use serde::{Deserialize, Deserializer, Serialize};

use std::collections::HashMap;

use cairn_common::logging::LogLevel;

use super::common::{MergeType, Model, Preset, ThinkingDisplayMode};

/// How agent replies to the special `to: "external"` target are handled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExternalReplyMode {
    /// Persist the reply to the issue message stream and wake `cairn watch` callers.
    Watchers,
    /// Accept the target but do not persist or wake.
    Disabled,
}

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

/// OpenRouter provider-routing controls. OpenRouter is the only backend with a
/// routing concept, so this is a single typed object rather than a backend-keyed
/// map. Defaults leave OpenRouter's normal routing untouched.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenRouterRouting {
    /// Restrict routing to zero-data-retention provider endpoints (`provider.zdr`).
    /// Strictly opt-in: when false, no `zdr` field is sent.
    #[serde(default)]
    pub zero_data_retention: bool,
    /// Routing sort preference → `provider.sort`. None = OpenRouter's default
    /// (price-weighted load balancing); field is omitted when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<OpenRouterSort>,
}

/// Routing sort preference for `provider.sort`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenRouterSort {
    Price,
    Throughput,
    Latency,
}

/// Settings DTO for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    // === Preset fields ===
    pub active_backend: String,
    pub tiers: Vec<String>,
    pub backends: HashMap<String, HashMap<String, Preset>>,

    pub branch_prefix: String,
    pub system_prompt: String,
    pub max_thinking_tokens: Option<i32>,
    pub merge_type: MergeType,
    pub pull_on_merge: bool,
    /// Always true — setting removed, kept for serialization compat.
    pub auto_start_jobs: bool,
    pub orphan_cleanup_days: i32,
    /// Whether agent bug reports are enabled (default: true)
    pub bug_reports: bool,
    /// Thinking block display mode in chat transcripts
    pub thinking_display_mode: ThinkingDisplayMode,
    /// Number of exact-scope pending memories that triggers a memory-triage issue.
    pub pending_memory_threshold: i32,
    /// Behavior for replies to the documented `to: "external"` target.
    pub external_replies: ExternalReplyMode,
    /// File-log verbosity level (default `standard`; `verbose` opts into full
    /// debug + profiler logging). Takes effect on the next app start.
    pub log_level: LogLevel,
    /// Flat monthly subscription fee per backend, in USD. Empty = every backend
    /// is metered (no subscription normalization). Drives effective-cost
    /// analytics; OpenRouter is always metered regardless of this map.
    #[serde(default)]
    pub subscription_fees: HashMap<String, f64>,
    /// OpenRouter provider-routing controls (ZDR + sort). Default = OpenRouter's
    /// normal routing.
    #[serde(default)]
    pub openrouter_routing: OpenRouterRouting,
}

/// DTO for updating settings
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSettings {
    // === Preset fields ===
    pub active_backend: Option<String>,
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
    pub orphan_cleanup_days: Option<i32>,
    /// Whether agent bug reports are enabled
    pub bug_reports: Option<bool>,
    /// Thinking block display mode in chat transcripts
    pub thinking_display_mode: Option<ThinkingDisplayMode>,
    /// Number of exact-scope pending memories that triggers a memory-triage issue.
    pub pending_memory_threshold: Option<i32>,
    /// Behavior for replies to the documented `to: "external"` target.
    pub external_replies: Option<ExternalReplyMode>,
    /// File-log verbosity level.
    pub log_level: Option<LogLevel>,
    /// Flat monthly subscription fee per backend, in USD (replaces the whole map).
    pub subscription_fees: Option<HashMap<String, f64>>,
    /// OpenRouter provider-routing controls (replaces the whole object).
    pub openrouter_routing: Option<OpenRouterRouting>,
}
