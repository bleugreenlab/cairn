//! File-based workspace settings.
//!
//! Settings are stored in `~/.cairn/settings.yaml` and are the source of truth.
//! The database is no longer used for workspace settings.

use serde::{Deserialize, Deserializer, Serialize};
use std::path::PathBuf;

use crate::models::{MergeType, Model, Settings};

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
    #[serde(default)]
    pub default_model: Option<Model>,
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
    #[serde(default)]
    pub auto_start_jobs: Option<bool>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub orphan_cleanup_days: Option<i32>,
    /// Selected audio input device name for voice input (None = system default)
    #[serde(default)]
    pub audio_device: Option<String>,
    /// Selected whisper model name for voice input
    #[serde(default)]
    pub whisper_model: Option<String>,
    /// Whether agent bug reports are enabled (default: true)
    #[serde(default)]
    pub bug_reports: Option<bool>,
}

impl SettingsFile {
    /// Convert to Settings DTO with defaults applied
    pub fn to_settings(&self) -> Settings {
        Settings {
            default_model: self.default_model.clone().unwrap_or(Model::Sonnet),
            branch_prefix: self
                .branch_prefix
                .clone()
                .unwrap_or_else(|| "agent".to_string()),
            system_prompt: String::new(), // Deprecated, always empty
            max_thinking_tokens: match self.max_thinking_tokens {
                None => Some(31999),  // Missing field → default enabled
                Some(inner) => inner, // Explicit value (None or Some(n))
            },
            merge_type: self.merge_type.clone().unwrap_or(MergeType::Squash),
            pull_on_merge: self.pull_on_merge.unwrap_or(true),
            auto_start_jobs: self.auto_start_jobs.unwrap_or(false),
            timezone: self
                .timezone
                .clone()
                .unwrap_or_else(|| "system".to_string()),
            orphan_cleanup_days: self.orphan_cleanup_days.unwrap_or(3), // Default 3 days
            audio_device: self.audio_device.clone(),
            whisper_model: self.whisper_model.clone(),
            bug_reports: self.bug_reports.unwrap_or(true),
        }
    }

    /// Create from Settings DTO
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            default_model: Some(settings.default_model.clone()),
            branch_prefix: Some(settings.branch_prefix.clone()),
            max_thinking_tokens: Some(settings.max_thinking_tokens),
            merge_type: Some(settings.merge_type.clone()),
            pull_on_merge: Some(settings.pull_on_merge),
            auto_start_jobs: Some(settings.auto_start_jobs),
            timezone: Some(settings.timezone.clone()),
            orphan_cleanup_days: Some(settings.orphan_cleanup_days),
            audio_device: settings.audio_device.clone(),
            whisper_model: settings.whisper_model.clone(),
            bug_reports: Some(settings.bug_reports),
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

    fn with_temp_home<F>(f: F)
    where
        F: FnOnce(&TempDir),
    {
        let temp = TempDir::new().unwrap();
        // Note: This test doesn't actually change the home directory,
        // it just tests the serialization/deserialization logic
        f(&temp);
    }

    #[test]
    fn test_settings_file_defaults() {
        let file = SettingsFile::default();
        let settings = file.to_settings();

        assert_eq!(settings.default_model, Model::Sonnet);
        assert_eq!(settings.branch_prefix, "agent");
        assert_eq!(settings.max_thinking_tokens, Some(31999));
        assert_eq!(settings.merge_type, MergeType::Squash);
        assert!(settings.pull_on_merge);
        assert!(!settings.auto_start_jobs);
        assert_eq!(settings.timezone, "system");
    }

    #[test]
    fn test_settings_roundtrip() {
        let settings = Settings {
            default_model: Model::Opus,
            branch_prefix: "feature".to_string(),
            system_prompt: String::new(),
            max_thinking_tokens: Some(16000),
            merge_type: MergeType::Rebase,
            pull_on_merge: false,
            auto_start_jobs: true,
            timezone: "America/New_York".to_string(),
            orphan_cleanup_days: 3,
            audio_device: None,
            whisper_model: None,
            bug_reports: true,
        };

        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();

        assert_eq!(restored.default_model, settings.default_model);
        assert_eq!(restored.branch_prefix, settings.branch_prefix);
        assert_eq!(restored.max_thinking_tokens, settings.max_thinking_tokens);
        assert_eq!(restored.merge_type, settings.merge_type);
        assert_eq!(restored.pull_on_merge, settings.pull_on_merge);
        assert_eq!(restored.auto_start_jobs, settings.auto_start_jobs);
        assert_eq!(restored.timezone, settings.timezone);
    }

    #[test]
    fn test_settings_roundtrip_disabled_thinking() {
        let settings = Settings {
            default_model: Model::Opus,
            branch_prefix: "feature".to_string(),
            system_prompt: String::new(),
            max_thinking_tokens: None,
            merge_type: MergeType::Rebase,
            pull_on_merge: false,
            auto_start_jobs: true,
            timezone: "America/New_York".to_string(),
            orphan_cleanup_days: 3,
            audio_device: None,
            whisper_model: None,
            bug_reports: true,
        };

        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();

        assert_eq!(restored.max_thinking_tokens, None);
    }

    #[test]
    fn test_yaml_serialization() {
        let file = SettingsFile {
            default_model: Some(Model::Opus),
            branch_prefix: Some("test".to_string()),
            max_thinking_tokens: Some(Some(16000)),
            merge_type: Some(MergeType::Merge),
            pull_on_merge: Some(true),
            auto_start_jobs: Some(false),
            timezone: Some("UTC".to_string()),
            orphan_cleanup_days: Some(3),
            audio_device: None,
            whisper_model: None,
            bug_reports: Some(true),
        };

        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.default_model, file.default_model);
        assert_eq!(parsed.branch_prefix, file.branch_prefix);
        assert_eq!(parsed.max_thinking_tokens, file.max_thinking_tokens);
    }

    #[test]
    fn test_yaml_serialization_disabled_thinking() {
        // Test that explicitly disabled thinking tokens (Some(None)) serializes correctly
        let file = SettingsFile {
            default_model: Some(Model::Opus),
            branch_prefix: Some("test".to_string()),
            max_thinking_tokens: Some(None),
            merge_type: Some(MergeType::Merge),
            pull_on_merge: Some(true),
            auto_start_jobs: Some(false),
            timezone: Some("UTC".to_string()),
            orphan_cleanup_days: Some(3),
            audio_device: None,
            whisper_model: None,
            bug_reports: Some(true),
        };

        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.max_thinking_tokens, Some(None));

        // Verify it converts to Settings correctly
        let settings = parsed.to_settings();
        assert_eq!(settings.max_thinking_tokens, None);
    }

    #[test]
    fn test_yaml_deserialization_partial() {
        // Test that partial YAML still works (missing fields use None)
        let yaml = r#"
defaultModel: opus
branchPrefix: custom
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(settings.default_model, Model::Opus);
        assert_eq!(settings.branch_prefix, "custom");
        // Defaults for missing fields
        assert_eq!(settings.max_thinking_tokens, Some(31999));
        assert_eq!(settings.merge_type, MergeType::Squash);
    }

    #[test]
    fn test_file_save_and_load() {
        with_temp_home(|temp| {
            let path = temp.path().join("settings.yaml");

            let settings = Settings {
                default_model: Model::Haiku,
                branch_prefix: "dev".to_string(),
                system_prompt: String::new(),
                max_thinking_tokens: None,
                merge_type: MergeType::Squash,
                pull_on_merge: true,
                auto_start_jobs: true,
                timezone: "system".to_string(),
                orphan_cleanup_days: 3,
                audio_device: None,
                whisper_model: None,
                bug_reports: true,
            };

            let file = SettingsFile::from_settings(&settings);
            let yaml = serde_yaml::to_string(&file).unwrap();
            let content = format!("# Cairn Workspace Settings\n{}", yaml);
            std::fs::write(&path, content).unwrap();

            let loaded_content = std::fs::read_to_string(&path).unwrap();
            let loaded: SettingsFile = serde_yaml::from_str(&loaded_content).unwrap();
            let loaded_settings = loaded.to_settings();

            assert_eq!(loaded_settings.default_model, Model::Haiku);
            assert_eq!(loaded_settings.branch_prefix, "dev");
            assert!(loaded_settings.auto_start_jobs);
            assert_eq!(loaded_settings.max_thinking_tokens, None);
        });
    }

    #[test]
    fn test_yaml_deserialization_missing_field() {
        // Test that missing maxThinkingTokens field defaults to enabled (31999)
        let yaml = r#"
defaultModel: opus
branchPrefix: custom
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(file.max_thinking_tokens, None); // None = field missing

        let settings = file.to_settings();
        assert_eq!(settings.max_thinking_tokens, Some(31999)); // Defaults to enabled
    }

    #[test]
    fn test_yaml_deserialization_null_field() {
        // Test that explicit null value stays as disabled
        let yaml = r#"
defaultModel: opus
branchPrefix: custom
maxThinkingTokens: null
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(file.max_thinking_tokens, Some(None)); // Some(None) = explicitly disabled

        let settings = file.to_settings();
        assert_eq!(settings.max_thinking_tokens, None); // Stays disabled
    }
}
