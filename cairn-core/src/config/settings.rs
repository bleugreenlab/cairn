//! File-based workspace settings.
//!
//! Settings are stored in `~/.cairn/settings.yaml` and are the source of truth.
//! The database is no longer used for workspace settings.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

use crate::config::presets::{default_presets_config, PresetsConfig};
use crate::models::{MergeType, Model, Preset, Settings, ThinkingDisplayMode, ToolDetailLevel};

/// Custom deserializer for max_thinking_tokens to distinguish between:
/// - Field missing → None (should default to Some(31999))
/// - Field set to null → Some(None) (explicitly disabled)
/// - Field set to number → Some(Some(n))
fn deserialize_max_thinking_tokens<'de, D>(deserializer: D) -> Result<Option<Option<i32>>, D::Error>
where
    D: Deserializer<'de>,
{
    // This is called only if the field is present
    // If the field is missing, serde uses the default (None)
    let value: Option<i32> = Option::deserialize(deserializer)?;
    Ok(Some(value))
}

/// Settings as stored in YAML file.
/// All fields are optional - missing fields use defaults.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SettingsFile {
    // === Preset fields (new) ===
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tiers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<HashMap<String, HashMap<String, Preset>>>,

    // === Legacy model fields (kept for deserialization, skipped on serialize) ===
    #[serde(default, skip_serializing)]
    pub default_model: Option<Model>,
    #[serde(default, skip_serializing)]
    pub preferred_models: Option<Vec<Model>>,

    #[serde(default)]
    pub branch_prefix: Option<String>,
    /// Double Option to distinguish:
    /// - None = field missing → default to Some(31999)
    /// - Some(None) = field set to null → disabled
    /// - Some(Some(n)) = field set to number → enabled with n tokens
    #[serde(default, deserialize_with = "deserialize_max_thinking_tokens")]
    pub max_thinking_tokens: Option<Option<i32>>,
    #[serde(default)]
    pub merge_type: Option<MergeType>,
    #[serde(default)]
    pub pull_on_merge: Option<bool>,
    /// Deprecated — always true. Kept for deserialization compat (silently ignored).
    #[serde(default, skip_serializing)]
    #[allow(dead_code)]
    auto_start_jobs: Option<bool>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub orphan_cleanup_days: Option<i32>,
    #[serde(default)]
    pub default_terminal: Option<String>,
    #[serde(default)]
    pub default_app: Option<String>,
    /// Selected audio input device name for voice input (None = system default)
    #[serde(default)]
    pub audio_device: Option<String>,
    /// Selected whisper model name for voice input
    #[serde(default)]
    pub whisper_model: Option<String>,
    /// Whether agent bug reports are enabled (default: true)
    #[serde(default)]
    pub bug_reports: Option<bool>,
    /// API key for web search (Jina Search)
    #[serde(default)]
    pub web_search_api_key: Option<String>,
    /// Legacy single tool detail level — migrated to lookup/change on load.
    #[serde(default, skip_serializing)]
    pub tool_detail_level: Option<ToolDetailLevel>,
    /// Lookup tool display detail level
    #[serde(default)]
    pub lookup_detail_level: Option<ToolDetailLevel>,
    /// Change tool display detail level
    #[serde(default)]
    pub change_detail_level: Option<ToolDetailLevel>,
    /// Thinking block display mode in chat transcripts
    #[serde(default)]
    pub thinking_display_mode: Option<ThinkingDisplayMode>,
}

/// Map legacy preferredModels to tier presets.
///
/// For each legacy model, find the tier whose default model matches it and
/// patch that tier. Remaining models are assigned to remaining tiers by position.
/// This preserves the user's actual model choices across migration.
fn migrate_legacy_models_to_presets(
    legacy_models: &[Model],
    claude_presets: &mut HashMap<String, Preset>,
    tiers: &[String],
) {
    // Build a map: default_model_str → tier_name for matching
    let default_model_to_tier: HashMap<String, String> = {
        let defaults = crate::config::presets::default_claude_presets(Some(31999));
        defaults
            .into_iter()
            .map(|(tier, preset)| (preset.model.as_str().to_string(), tier))
            .collect()
    };

    let mut assigned_tiers: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut unassigned_models: Vec<Model> = Vec::new();

    // First pass: match legacy models to tiers by their default model
    for legacy_model in legacy_models {
        if let Some(tier) = default_model_to_tier.get(legacy_model.as_str()) {
            if !assigned_tiers.contains(tier) {
                if let Some(preset) = claude_presets.get_mut(tier) {
                    preset.model = legacy_model.clone();
                    assigned_tiers.insert(tier.clone());
                    continue;
                }
            }
        }
        unassigned_models.push(legacy_model.clone());
    }

    // Second pass: assign remaining models to remaining tiers by position
    let remaining_tiers: Vec<&String> = tiers
        .iter()
        .filter(|t| !assigned_tiers.contains(t.as_str()))
        .collect();

    for (model, tier) in unassigned_models.iter().zip(remaining_tiers.iter()) {
        if let Some(preset) = claude_presets.get_mut(tier.as_str()) {
            preset.model = model.clone();
        }
    }
}

impl SettingsFile {
    /// Convert to Settings DTO with defaults applied.
    ///
    /// Migration: if `backends` is absent but legacy model fields are present,
    /// builds default presets using the legacy values.
    pub fn to_settings(&self) -> Settings {
        // Resolve max_thinking_tokens (used for both legacy compat and preset defaults)
        let max_thinking_tokens = match self.max_thinking_tokens {
            None => Some(31999),  // Missing field → default enabled
            Some(inner) => inner, // Explicit value (None or Some(n))
        };

        // Build presets config (migrate from legacy if needed)
        let presets: PresetsConfig = if let Some(ref backends) = self.backends {
            PresetsConfig {
                active_backend: self
                    .active_backend
                    .clone()
                    .unwrap_or_else(|| "claude".to_string()),
                default_tier: self
                    .default_tier
                    .clone()
                    .unwrap_or_else(|| "md".to_string()),
                tiers: self.tiers.clone().unwrap_or_else(|| {
                    crate::config::presets::DEFAULT_TIERS
                        .iter()
                        .map(|s| s.to_string())
                        .collect()
                }),
                backends: backends.clone(),
            }
        } else {
            // Legacy migration: build default presets, then overlay legacy model fields.
            let mut config = default_presets_config(max_thinking_tokens);

            // Honor legacy defaultModel / preferredModels by patching Claude presets.
            // Strategy: match each legacy model to the tier whose default it matches,
            // then assign remaining models to remaining tiers by position.
            let legacy_models = match (&self.preferred_models, &self.default_model) {
                (Some(models), _) if !models.is_empty() => models.clone(),
                (_, Some(model)) => vec![model.clone()],
                _ => vec![],
            };

            if !legacy_models.is_empty() {
                if let Some(claude_presets) = config.backends.get_mut("claude") {
                    migrate_legacy_models_to_presets(&legacy_models, claude_presets, &config.tiers);

                    // If the first (default) model maps to a tier, set that as default_tier
                    let default_model = &legacy_models[0];
                    for (tier, preset) in claude_presets.iter() {
                        if preset.model == *default_model {
                            config.default_tier = tier.clone();
                            break;
                        }
                    }
                }
            }

            config
        };

        Settings {
            active_backend: presets.active_backend,
            default_tier: presets.default_tier,
            tiers: presets.tiers,
            backends: presets.backends,
            branch_prefix: self
                .branch_prefix
                .clone()
                .unwrap_or_else(|| "agent".to_string()),
            system_prompt: String::new(), // Deprecated, always empty
            max_thinking_tokens,
            merge_type: self.merge_type.clone().unwrap_or(MergeType::Squash),
            pull_on_merge: self.pull_on_merge.unwrap_or(true),
            auto_start_jobs: true, // Always true — setting removed
            timezone: self
                .timezone
                .clone()
                .unwrap_or_else(|| "system".to_string()),
            orphan_cleanup_days: self.orphan_cleanup_days.unwrap_or(3),
            default_terminal: self.default_terminal.clone(),
            default_app: self.default_app.clone(),
            audio_device: self.audio_device.clone(),
            whisper_model: self.whisper_model.clone(),
            bug_reports: self.bug_reports.unwrap_or(true),
            web_search_api_key: self.web_search_api_key.clone(),
            lookup_detail_level: self
                .lookup_detail_level
                .clone()
                .or_else(|| self.tool_detail_level.clone())
                .unwrap_or(ToolDetailLevel::Blurb),
            change_detail_level: self
                .change_detail_level
                .clone()
                .or_else(|| self.tool_detail_level.clone())
                .unwrap_or(ToolDetailLevel::Blurb),
            thinking_display_mode: self
                .thinking_display_mode
                .clone()
                .unwrap_or(ThinkingDisplayMode::Collapsed),
        }
    }

    /// Create from Settings DTO
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            active_backend: Some(settings.active_backend.clone()),
            default_tier: Some(settings.default_tier.clone()),
            tiers: Some(settings.tiers.clone()),
            backends: Some(settings.backends.clone()),
            default_model: None,
            preferred_models: None,
            branch_prefix: Some(settings.branch_prefix.clone()),
            max_thinking_tokens: Some(settings.max_thinking_tokens),
            merge_type: Some(settings.merge_type.clone()),
            pull_on_merge: Some(settings.pull_on_merge),
            auto_start_jobs: None, // No longer serialized
            timezone: Some(settings.timezone.clone()),
            orphan_cleanup_days: Some(settings.orphan_cleanup_days),
            default_terminal: settings.default_terminal.clone(),
            default_app: settings.default_app.clone(),
            audio_device: settings.audio_device.clone(),
            whisper_model: settings.whisper_model.clone(),
            bug_reports: Some(settings.bug_reports),
            web_search_api_key: settings.web_search_api_key.clone(),
            tool_detail_level: None, // No longer serialized
            lookup_detail_level: Some(settings.lookup_detail_level.clone()),
            change_detail_level: Some(settings.change_detail_level.clone()),
            thinking_display_mode: Some(settings.thinking_display_mode.clone()),
        }
    }
}

/// Get the path to the settings file
pub fn get_settings_path(config_dir: &std::path::Path) -> PathBuf {
    config_dir.join("settings.yaml")
}

/// Load settings from file. Returns defaults if file doesn't exist or is invalid.
pub fn load_settings(config_dir: &std::path::Path) -> Settings {
    match load_settings_file(config_dir) {
        Ok(file) => file.to_settings(),
        Err(e) => {
            log::info!("Using default settings: {}", e);
            SettingsFile::default().to_settings()
        }
    }
}

/// Load the raw settings file
fn load_settings_file(config_dir: &std::path::Path) -> Result<SettingsFile, String> {
    let path = get_settings_path(config_dir);

    if !path.exists() {
        return Ok(SettingsFile::default());
    }

    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read settings file: {}", e))?;

    serde_yaml::from_str(&content).map_err(|e| format!("Failed to parse settings file: {}", e))
}

/// Save settings to file
pub fn save_settings(config_dir: &std::path::Path, settings: &Settings) -> Result<(), String> {
    let path = get_settings_path(config_dir);

    // Ensure directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config directory: {}", e))?;
    }

    let file = SettingsFile::from_settings(settings);

    // Add header comment
    let yaml =
        serde_yaml::to_string(&file).map_err(|e| format!("Failed to serialize settings: {}", e))?;
    let content = format!("# Cairn Workspace Settings\n{}", yaml);

    std::fs::write(&path, content).map_err(|e| format!("Failed to write settings file: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Build a default Settings for test use (avoids repeating all fields).
    fn test_settings() -> Settings {
        SettingsFile::default().to_settings()
    }

    fn with_temp_home<F>(f: F)
    where
        F: FnOnce(&TempDir),
    {
        let temp = TempDir::new().unwrap();
        f(&temp);
    }

    #[test]
    fn test_settings_file_defaults() {
        let file = SettingsFile::default();
        let settings = file.to_settings();

        // Preset defaults
        assert_eq!(settings.active_backend, "claude");
        assert_eq!(settings.default_tier, "md");
        assert_eq!(settings.tiers, vec!["sm", "md", "lg"]);
        assert!(settings.backends.contains_key("claude"));
        assert!(settings.backends.contains_key("codex"));

        assert_eq!(
            settings.backends["claude"]["sm"].model,
            Model::new(Model::HAIKU)
        );
        assert_eq!(
            settings.backends["claude"]["md"].model,
            Model::new(Model::SONNET)
        );
        assert_eq!(
            settings.backends["claude"]["lg"].model,
            Model::new(Model::OPUS)
        );
        assert_eq!(settings.branch_prefix, "agent");
        assert_eq!(settings.max_thinking_tokens, Some(31999));
        assert_eq!(settings.merge_type, MergeType::Squash);
        assert!(settings.pull_on_merge);
        assert!(settings.auto_start_jobs); // Always true
        assert_eq!(settings.timezone, "system");
        assert_eq!(settings.lookup_detail_level, ToolDetailLevel::Blurb);
        assert_eq!(settings.change_detail_level, ToolDetailLevel::Blurb);
        assert_eq!(
            settings.thinking_display_mode,
            ThinkingDisplayMode::Collapsed
        );
    }

    #[test]
    fn test_settings_roundtrip() {
        let settings = Settings {
            branch_prefix: "feature".to_string(),
            max_thinking_tokens: Some(16000),
            merge_type: MergeType::Rebase,
            pull_on_merge: false,
            timezone: "America/New_York".to_string(),
            change_detail_level: ToolDetailLevel::Full,
            ..test_settings()
        };

        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();

        assert_eq!(restored.active_backend, settings.active_backend);
        assert_eq!(restored.default_tier, settings.default_tier);
        assert_eq!(restored.backends, settings.backends);
        assert_eq!(restored.branch_prefix, settings.branch_prefix);
        assert_eq!(restored.max_thinking_tokens, settings.max_thinking_tokens);
        assert_eq!(restored.merge_type, settings.merge_type);
        assert_eq!(restored.pull_on_merge, settings.pull_on_merge);
        assert!(restored.auto_start_jobs); // Always true
        assert_eq!(restored.timezone, settings.timezone);
        assert_eq!(restored.lookup_detail_level, settings.lookup_detail_level);
        assert_eq!(restored.change_detail_level, settings.change_detail_level);
        assert_eq!(
            restored.thinking_display_mode,
            settings.thinking_display_mode
        );
    }

    #[test]
    fn test_settings_roundtrip_disabled_thinking() {
        let settings = Settings {
            max_thinking_tokens: None,
            merge_type: MergeType::Rebase,
            pull_on_merge: false,
            ..test_settings()
        };

        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();

        assert_eq!(restored.max_thinking_tokens, None);
    }

    #[test]
    fn test_yaml_serialization() {
        let file = SettingsFile {
            branch_prefix: Some("test".to_string()),
            max_thinking_tokens: Some(Some(16000)),
            merge_type: Some(MergeType::Merge),
            pull_on_merge: Some(true),
            timezone: Some("UTC".to_string()),
            orphan_cleanup_days: Some(3),
            bug_reports: Some(true),
            ..Default::default()
        };

        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.preferred_models, file.preferred_models);
        assert_eq!(parsed.branch_prefix, file.branch_prefix);
        assert_eq!(parsed.max_thinking_tokens, file.max_thinking_tokens);
    }

    #[test]
    fn test_yaml_serialization_disabled_thinking() {
        let file = SettingsFile {
            max_thinking_tokens: Some(None),
            ..Default::default()
        };

        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.max_thinking_tokens, Some(None));

        let settings = parsed.to_settings();
        assert_eq!(settings.max_thinking_tokens, None);
    }

    #[test]
    fn test_yaml_deserialization_partial() {
        // Legacy format without backends → defaults are generated
        let yaml = r#"
branchPrefix: custom
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(settings.branch_prefix, "custom");
        assert_eq!(settings.max_thinking_tokens, Some(31999));
        assert_eq!(settings.merge_type, MergeType::Squash);
        assert_eq!(
            settings.backends["claude"]["md"].model,
            Model::new(Model::SONNET)
        );
        assert_eq!(settings.active_backend, "claude");
        assert_eq!(settings.default_tier, "md");
    }

    #[test]
    fn test_legacy_default_model_honored_in_migration() {
        // Old format with defaultModel: opus → default_tier set to lg (opus's natural tier)
        let yaml = r#"
defaultModel: opus
branchPrefix: agent
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(
            settings.backends["claude"]["lg"].model,
            Model::new(Model::OPUS)
        );
        assert_eq!(settings.active_backend, "claude");
        // opus maps to lg tier, so default_tier should be lg
        assert_eq!(settings.default_tier, "lg");
        assert_eq!(
            settings.backends["claude"]["lg"].model,
            Model::new(Model::OPUS)
        );
    }

    #[test]
    fn test_legacy_preferred_models_honored_in_migration() {
        // Old format with preferredModels: each model maps to its natural tier
        let yaml = r#"
preferredModels:
  - opus
  - sonnet
  - haiku
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        // Each model should be placed in its matching tier
        let claude = &settings.backends["claude"];
        assert_eq!(claude["sm"].model, Model::new(Model::HAIKU));
        assert_eq!(claude["md"].model, Model::new(Model::SONNET));
        assert_eq!(claude["lg"].model, Model::new(Model::OPUS));

        // First preferred model (opus) → default_tier becomes lg
        assert_eq!(settings.default_tier, "lg");
        assert_eq!(
            settings.backends["claude"]["lg"].model,
            Model::new(Model::OPUS)
        );
    }

    #[test]
    fn test_legacy_custom_models_assigned_by_position() {
        // A user with a custom/unknown model in their preferred list
        let yaml = r#"
preferredModels:
  - custom-model-v2
  - sonnet
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        let claude = &settings.backends["claude"];
        // sonnet matches md tier naturally
        assert_eq!(claude["md"].model, Model::new(Model::SONNET));
        // custom-model-v2 doesn't match any default → assigned to first remaining tier (sm)
        assert_eq!(claude["sm"].model, Model::new("custom-model-v2"));
        // lg stays at its default (opus)
        assert_eq!(claude["lg"].model, Model::new(Model::OPUS));
    }

    #[test]
    fn test_legacy_fields_ignored_when_backends_present() {
        // When backends is present, legacy model fields are ignored
        let yaml = r#"
defaultModel: haiku
activeBackend: claude
defaultTier: lg
tiers:
  - sm
  - md
  - lg
backends:
  claude:
    sm:
      model: haiku
    md:
      model: sonnet
      maxThinkingTokens: 31999
    lg:
      model: opus
      maxThinkingTokens: 31999
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(
            settings.backends["claude"]["lg"].model,
            Model::new(Model::OPUS)
        );
        assert_eq!(settings.default_tier, "lg");
    }

    #[test]
    fn test_presets_roundtrip() {
        // New format with backends roundtrips correctly
        let yaml = r#"
activeBackend: codex
defaultTier: md
tiers:
  - sm
  - md
  - lg
backends:
  codex:
    sm:
      model: gpt-5.4-mini
      reasoningEffort: low
    md:
      model: gpt-5.3-codex
      reasoningEffort: medium
    lg:
      model: gpt-5.4
      reasoningEffort: high
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(settings.active_backend, "codex");
        assert_eq!(settings.default_tier, "md");
        assert_eq!(
            settings.backends["codex"]["md"].model.as_str(),
            "gpt-5.3-codex"
        );

        // Roundtrip
        let file2 = SettingsFile::from_settings(&settings);
        let restored = file2.to_settings();
        assert_eq!(restored.active_backend, "codex");
        assert_eq!(restored.default_tier, "md");
        assert_eq!(restored.backends.get("codex").unwrap().len(), 3);
    }

    #[test]
    fn test_auto_start_jobs_always_true() {
        // Even if YAML has autoStartJobs: false, Settings.auto_start_jobs is true
        let yaml = r#"
autoStartJobs: false
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        assert!(settings.auto_start_jobs);
    }

    #[test]
    fn test_auto_start_jobs_not_serialized() {
        let settings = test_settings();
        let file = SettingsFile::from_settings(&settings);
        let yaml = serde_yaml::to_string(&file).unwrap();
        assert!(
            !yaml.contains("autoStartJobs"),
            "auto_start_jobs should not be serialized"
        );
    }

    #[test]
    fn test_file_save_and_load() {
        with_temp_home(|temp| {
            let path = temp.path().join("settings.yaml");

            let settings = Settings {
                branch_prefix: "dev".to_string(),
                max_thinking_tokens: None,
                ..test_settings()
            };

            let file = SettingsFile::from_settings(&settings);
            let yaml = serde_yaml::to_string(&file).unwrap();
            let content = format!("# Cairn Workspace Settings\n{}", yaml);
            std::fs::write(&path, content).unwrap();

            let loaded_content = std::fs::read_to_string(&path).unwrap();
            let loaded: SettingsFile = serde_yaml::from_str(&loaded_content).unwrap();
            let loaded_settings = loaded.to_settings();

            assert_eq!(loaded_settings.branch_prefix, "dev");
            assert!(loaded_settings.auto_start_jobs); // Always true
            assert_eq!(loaded_settings.max_thinking_tokens, None);
            // Default presets should be present
            assert_eq!(loaded_settings.active_backend, "claude");
            assert_eq!(loaded_settings.default_tier, "md");
        });
    }

    #[test]
    fn test_yaml_deserialization_missing_field() {
        let yaml = r#"
defaultModel: opus
branchPrefix: custom
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(file.max_thinking_tokens, None);

        let settings = file.to_settings();
        assert_eq!(settings.max_thinking_tokens, Some(31999));
    }

    #[test]
    fn test_yaml_deserialization_null_field() {
        let yaml = r#"
defaultModel: opus
branchPrefix: custom
maxThinkingTokens: null
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(file.max_thinking_tokens, Some(None));

        let settings = file.to_settings();
        assert_eq!(settings.max_thinking_tokens, None);
    }

    #[test]
    fn test_yaml_explicit_detail_levels_and_thinking_mode() {
        let yaml = r#"
lookupDetailLevel: compact
changeDetailLevel: full
thinkingDisplayMode: full
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(file.lookup_detail_level, Some(ToolDetailLevel::Compact));
        assert_eq!(file.change_detail_level, Some(ToolDetailLevel::Full));
        assert_eq!(file.thinking_display_mode, Some(ThinkingDisplayMode::Full));

        let settings = file.to_settings();
        assert_eq!(settings.lookup_detail_level, ToolDetailLevel::Compact);
        assert_eq!(settings.change_detail_level, ToolDetailLevel::Full);
        assert_eq!(settings.thinking_display_mode, ThinkingDisplayMode::Full);
    }

    #[test]
    fn test_legacy_tool_detail_level_migration() {
        // Old format with single toolDetailLevel — both new fields inherit its value
        let yaml = r#"
toolDetailLevel: compact
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        assert_eq!(settings.lookup_detail_level, ToolDetailLevel::Compact);
        assert_eq!(settings.change_detail_level, ToolDetailLevel::Compact);
    }

    #[test]
    fn test_legacy_tool_detail_full_migration() {
        let yaml = r#"
toolDetailLevel: full
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        assert_eq!(settings.lookup_detail_level, ToolDetailLevel::Full);
        assert_eq!(settings.change_detail_level, ToolDetailLevel::Full);
        // thinking_display_mode should default to collapsed when omitted
        assert_eq!(
            settings.thinking_display_mode,
            ThinkingDisplayMode::Collapsed
        );
    }

    #[test]
    fn test_new_fields_take_precedence_over_legacy() {
        // Both legacy + new fields → new fields win
        let yaml = r#"
toolDetailLevel: compact
lookupDetailLevel: full
changeDetailLevel: blurb
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        assert_eq!(settings.lookup_detail_level, ToolDetailLevel::Full);
        assert_eq!(settings.change_detail_level, ToolDetailLevel::Blurb);
    }

    #[test]
    fn test_legacy_tool_detail_not_serialized() {
        let settings = Settings {
            lookup_detail_level: ToolDetailLevel::Compact,
            change_detail_level: ToolDetailLevel::Full,
            ..test_settings()
        };
        let file = SettingsFile::from_settings(&settings);
        let yaml = serde_yaml::to_string(&file).unwrap();
        assert!(
            !yaml.contains("toolDetailLevel"),
            "legacy tool_detail_level should not be serialized"
        );
        assert!(yaml.contains("lookupDetailLevel"));
        assert!(yaml.contains("changeDetailLevel"));
    }

    #[test]
    fn test_legacy_model_fields_not_serialized() {
        let settings = test_settings();
        let file = SettingsFile::from_settings(&settings);
        let yaml = serde_yaml::to_string(&file).unwrap();

        // Legacy fields should not appear in output
        assert!(
            !yaml.contains("defaultModel"),
            "legacy defaultModel should not be serialized"
        );
        assert!(
            !yaml.contains("preferredModels"),
            "legacy preferredModels should not be serialized"
        );

        // Preset fields should appear
        assert!(
            yaml.contains("activeBackend"),
            "activeBackend should be serialized"
        );
        assert!(
            yaml.contains("defaultTier"),
            "defaultTier should be serialized"
        );
        assert!(yaml.contains("backends"), "backends should be serialized");
        assert!(yaml.contains("tiers"), "tiers should be serialized");
    }

    #[test]
    fn test_settings_preset_roundtrip_preserves_backends() {
        // Build settings with custom backends and verify they survive roundtrip
        let mut settings = test_settings();
        settings.active_backend = "codex".to_string();
        settings.default_tier = "lg".to_string();

        let file = SettingsFile::from_settings(&settings);
        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();
        let restored = parsed.to_settings();

        assert_eq!(restored.active_backend, "codex");
        assert_eq!(restored.default_tier, "lg");
        assert!(restored.backends.contains_key("claude"));
        assert!(restored.backends.contains_key("codex"));
    }
}
