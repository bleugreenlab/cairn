//! File-based project settings.
//!
//! Project settings are stored in `[project]/.cairn/config.yaml` and are the source of truth.
//! These files can be version-controlled with the project.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::models::{Preset, TerminalCommand};

/// External reference resource (git repo or local directory).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// Worktree behavior settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeSettings {
    /// When true, seed gitignored repo content into new worktrees.
    #[serde(default = "default_true")]
    pub seed_ignored: bool,
}

impl Default for WorktreeSettings {
    fn default() -> Self {
        Self { seed_ignored: true }
    }
}

/// Project settings as stored in YAML file.
/// All fields are optional - missing fields use defaults.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectSettingsFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_commands: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_commands: Option<Vec<TerminalCommand>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<Resource>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<WorktreeSettings>,
    /// Project-level override for active backend
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_backend: Option<String>,
    /// Project-level override for default tier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_tier: Option<String>,
    /// Project-level preset overrides (deep-merged with workspace)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<HashMap<String, HashMap<String, Preset>>>,
}

impl ProjectSettingsFile {
    pub fn should_seed_worktree_ignored(&self) -> bool {
        self.worktree
            .as_ref()
            .map(|worktree| worktree.seed_ignored)
            .unwrap_or(true)
    }
}

fn default_true() -> bool {
    true
}

/// Intermediate struct for loading legacy config files.
/// Used to detect removed fields (ciCommands, persistent) for migration.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyProjectSettingsFile {
    #[serde(default)]
    ci_commands: Option<Vec<String>>,
    #[serde(default)]
    setup_commands: Option<Vec<String>>,
    #[serde(default)]
    copy_files: Option<Vec<String>>,
    #[serde(default)]
    terminal_commands: Option<Vec<LegacyTerminalCommand>>,
    #[serde(default)]
    default_branch: Option<String>,
    #[serde(default)]
    resources: Option<Vec<Resource>>,
    #[serde(default)]
    worktree: Option<WorktreeSettings>,
    // Preset fields — must be present so they survive the legacy parse path
    #[serde(default)]
    active_backend: Option<String>,
    #[serde(default)]
    default_tier: Option<String>,
    #[serde(default)]
    backends: Option<HashMap<String, HashMap<String, Preset>>>,
}

/// Legacy terminal command with persistent field
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyTerminalCommand {
    name: String,
    command: String,
    #[serde(default)]
    persistent: bool,
}

/// Get the path to the project config file ([project]/.cairn/config.yaml)
pub fn get_project_config_path(project_path: &Path) -> PathBuf {
    project_path.join(".cairn").join("config.yaml")
}

/// Load project settings from file. Returns defaults if file doesn't exist.
/// Automatically migrates legacy config files (removes ciCommands and persistent field).
pub fn load_project_settings(project_path: &Path) -> ProjectSettingsFile {
    match load_project_settings_file(project_path) {
        Ok((file, needs_migration)) => {
            if needs_migration {
                log::info!(
                    "Migrating project config at {:?} (removing deprecated fields)",
                    project_path
                );
                if let Err(e) = save_project_settings(project_path, &file) {
                    log::warn!("Failed to migrate project settings: {}", e);
                }
            }
            file
        }
        Err(e) => {
            log::debug!(
                "Using default project settings for {:?}: {}",
                project_path,
                e
            );
            ProjectSettingsFile::default()
        }
    }
}

/// Load the raw project settings file.
/// Returns (settings, needs_migration) where needs_migration is true if legacy fields were found.
fn load_project_settings_file(project_path: &Path) -> Result<(ProjectSettingsFile, bool), String> {
    let path = get_project_config_path(project_path);

    if !path.exists() {
        return Ok((ProjectSettingsFile::default(), false));
    }

    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read project config file: {}", e))?;

    // First try to parse as legacy format to detect deprecated fields
    let legacy: LegacyProjectSettingsFile = serde_yaml::from_str(&content)
        .map_err(|e| format!("Failed to parse project config file: {}", e))?;

    // Check if migration is needed
    let has_ci_commands = legacy.ci_commands.is_some();
    let has_copy_files = legacy.copy_files.is_some();
    if let Some(ref files) = legacy.copy_files {
        log::warn!(
            "Removing deprecated copyFiles from project config: {:?}. \
             Gitignored content is now auto-seeded into worktrees via symlinks.",
            files
        );
    }
    let has_persistent = legacy
        .terminal_commands
        .as_ref()
        .map(|cmds| cmds.iter().any(|c| c.persistent))
        .unwrap_or(false);
    let needs_migration = has_ci_commands || has_copy_files || has_persistent;

    // Convert to current format (dropping deprecated fields)
    let settings = ProjectSettingsFile {
        setup_commands: legacy.setup_commands,
        terminal_commands: legacy.terminal_commands.map(|cmds| {
            cmds.into_iter()
                .map(|c| TerminalCommand {
                    name: c.name,
                    command: c.command,
                })
                .collect()
        }),
        default_branch: legacy.default_branch,
        resources: legacy.resources,
        worktree: legacy.worktree,
        active_backend: legacy.active_backend,
        default_tier: legacy.default_tier,
        backends: legacy.backends,
    };

    Ok((settings, needs_migration))
}

/// Save project settings to file
pub fn save_project_settings(
    project_path: &Path,
    settings: &ProjectSettingsFile,
) -> Result<(), String> {
    let path = get_project_config_path(project_path);

    // Ensure .cairn directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create .cairn directory: {}", e))?;
    }

    // Add header comment
    let yaml = serde_yaml::to_string(settings)
        .map_err(|e| format!("Failed to serialize project settings: {}", e))?;
    let content = format!("# Cairn Project Configuration\n{}", yaml);

    std::fs::write(&path, content)
        .map_err(|e| format!("Failed to write project config file: {}", e))
}

/// Create a default project config file with commented template
pub fn create_default_project_config(project_path: &Path) -> Result<(), String> {
    let path = get_project_config_path(project_path);

    // Don't overwrite existing config
    if path.exists() {
        return Ok(());
    }

    // Ensure .cairn directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create .cairn directory: {}", e))?;
    }

    let template = r#"# Cairn Project Configuration
#
# This file configures how Cairn works with this project.
# It can be committed to version control to share settings with your team.
#
# Worktrees can be seeded with gitignored content from the main repo
# (node_modules, target/, .env files, etc.).
# Directories are symlinked (instant, shared), files are copied.
#
# Toggle seeding on or off per project (default: true)
# worktree:
#   seedIgnored: true

# Commands to run when setting up a new worktree
# setupCommands:
#   - npm install

# Quick-access terminal commands
# terminalCommands:
#   - name: Dev Server
#     command: npm run dev
#   - name: Tests (watch)
#     command: npm test -- --watch

# Default branch for the project (defaults to 'main')
# defaultBranch: main

# External reference repositories and directories
# resources:
#   - name: docs
#     git: https://github.com/org/docs.git
#     description: Project documentation
#   - name: specs
#     path: ~/Documents/specs
#     description: Hardware specifications
"#;

    std::fs::write(&path, template)
        .map_err(|e| format!("Failed to write project config template: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_project_settings_defaults() {
        let settings = ProjectSettingsFile::default();
        assert!(settings.setup_commands.is_none());
        assert!(settings.terminal_commands.is_none());
        assert!(settings.default_branch.is_none());
        assert!(settings.should_seed_worktree_ignored());
    }

    #[test]
    fn test_project_settings_roundtrip() {
        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["npm install".to_string()]),
            terminal_commands: Some(vec![TerminalCommand {
                name: "Dev Server".to_string(),
                command: "npm run dev".to_string(),
            }]),
            default_branch: Some("develop".to_string()),
            ..Default::default()
        };

        let yaml = serde_yaml::to_string(&settings).unwrap();
        let parsed: ProjectSettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.setup_commands, settings.setup_commands);
        assert_eq!(parsed.default_branch, settings.default_branch);
        assert_eq!(parsed.terminal_commands.as_ref().map(|v| v.len()), Some(1));
    }

    #[test]
    fn test_yaml_deserialization_partial() {
        let yaml = r#"
setupCommands:
  - npm install
defaultBranch: main
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            settings.setup_commands,
            Some(vec!["npm install".to_string()])
        );
        assert_eq!(settings.default_branch, Some("main".to_string()));
        assert!(settings.terminal_commands.is_none());
        assert!(settings.should_seed_worktree_ignored());
    }

    #[test]
    fn test_worktree_seed_ignored_can_be_disabled() {
        let yaml = r#"
worktree:
  seedIgnored: false
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            settings.worktree.as_ref().map(|w| w.seed_ignored),
            Some(false)
        );
        assert!(!settings.should_seed_worktree_ignored());
    }

    #[test]
    fn test_legacy_copy_files_triggers_migration() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // Write a legacy config with copyFiles
        let legacy_content = r#"setupCommands:
  - npm install
copyFiles:
  - .env
  - config/secrets.yaml
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, legacy_content).unwrap();

        // Load should trigger migration and strip copyFiles
        let loaded = load_project_settings(project_path);
        assert_eq!(loaded.setup_commands, Some(vec!["npm install".to_string()]));

        // Verify file was migrated (no copyFiles)
        let migrated_content = std::fs::read_to_string(&config_path).unwrap();
        assert!(!migrated_content.contains("copyFiles"));
    }

    #[test]
    fn test_file_save_and_load() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["cargo build".to_string()]),
            default_branch: Some("main".to_string()),
            ..Default::default()
        };

        save_project_settings(project_path, &settings).unwrap();

        let loaded = load_project_settings(project_path);
        assert_eq!(loaded.setup_commands, settings.setup_commands);
        assert_eq!(loaded.default_branch, settings.default_branch);
    }

    #[test]
    fn test_create_default_config() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        create_default_project_config(project_path).unwrap();

        let config_path = get_project_config_path(project_path);
        assert!(config_path.exists());

        let content = std::fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("# Cairn Project Configuration"));
        assert!(content.contains("setupCommands"));
    }

    #[test]
    fn test_create_default_config_no_overwrite() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // Create a custom config first
        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["custom".to_string()]),
            ..Default::default()
        };
        save_project_settings(project_path, &settings).unwrap();

        // Try to create default - should not overwrite
        create_default_project_config(project_path).unwrap();

        let loaded = load_project_settings(project_path);
        assert_eq!(loaded.setup_commands, Some(vec!["custom".to_string()]));
    }

    #[test]
    fn test_get_project_config_path() {
        let path = get_project_config_path(Path::new("/home/user/project"));
        assert_eq!(path, PathBuf::from("/home/user/project/.cairn/config.yaml"));
    }

    #[test]
    fn test_resource_serde_git() {
        let yaml = r#"
resources:
  - name: openpnp
    git: https://github.com/openpnp/openpnp.git
    description: OpenPnP source code
    branch: develop
  - name: local-specs
    path: ~/Documents/specs
    description: Hardware specifications
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let resources = settings.resources.unwrap();
        assert_eq!(resources.len(), 2);

        assert_eq!(resources[0].name, "openpnp");
        assert_eq!(
            resources[0].git.as_deref(),
            Some("https://github.com/openpnp/openpnp.git")
        );
        assert!(resources[0].path.is_none());
        assert_eq!(resources[0].branch.as_deref(), Some("develop"));

        assert_eq!(resources[1].name, "local-specs");
        assert!(resources[1].git.is_none());
        assert_eq!(resources[1].path.as_deref(), Some("~/Documents/specs"));
        assert!(resources[1].branch.is_none());
    }

    #[test]
    fn test_resource_roundtrip() {
        let settings = ProjectSettingsFile {
            resources: Some(vec![Resource {
                name: "docs".to_string(),
                git: Some("https://github.com/org/docs.git".to_string()),
                path: None,
                description: Some("Project docs".to_string()),
                branch: None,
            }]),
            ..Default::default()
        };

        let yaml = serde_yaml::to_string(&settings).unwrap();
        let parsed: ProjectSettingsFile = serde_yaml::from_str(&yaml).unwrap();
        let resources = parsed.resources.unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name, "docs");
        assert_eq!(
            resources[0].git.as_deref(),
            Some("https://github.com/org/docs.git")
        );
    }

    #[test]
    fn test_settings_without_resources() {
        let yaml = r#"
setupCommands:
  - npm install
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert!(settings.resources.is_none());
    }

    #[test]
    fn test_legacy_config_migration() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // Write a legacy config with ciCommands and persistent field
        let legacy_content = r#"# Legacy config
ciCommands:
  - npm test
  - npm run build
setupCommands:
  - npm install
terminalCommands:
  - name: Dev Server
    command: npm run dev
    persistent: true
defaultBranch: main
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, legacy_content).unwrap();

        // Load should trigger migration
        let loaded = load_project_settings(project_path);

        // Verify deprecated fields are gone
        assert_eq!(loaded.setup_commands, Some(vec!["npm install".to_string()]));
        assert_eq!(loaded.default_branch, Some("main".to_string()));
        assert!(loaded.terminal_commands.is_some());
        let cmds = loaded.terminal_commands.unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].name, "Dev Server");
        assert_eq!(cmds[0].command, "npm run dev");

        // Verify file was migrated (no ciCommands or persistent)
        let migrated_content = std::fs::read_to_string(&config_path).unwrap();
        assert!(!migrated_content.contains("ciCommands"));
        assert!(!migrated_content.contains("persistent"));
    }

    #[test]
    fn test_preset_fields_survive_legacy_parse() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // Write a config with both legacy fields and preset overrides
        let content = r#"
setupCommands:
  - npm install
activeBackend: codex
defaultTier: lg
backends:
  codex:
    lg:
      model: gpt-5.4
      reasoningEffort: high
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        let loaded = load_project_settings(project_path);

        // Preset fields must survive the legacy parse path
        assert_eq!(loaded.active_backend.as_deref(), Some("codex"));
        assert_eq!(loaded.default_tier.as_deref(), Some("lg"));
        assert!(loaded.backends.is_some());
        let backends = loaded.backends.unwrap();
        assert!(backends.contains_key("codex"));
        assert_eq!(backends["codex"]["lg"].model.as_str(), "gpt-5.4");
    }

    #[test]
    fn test_preset_fields_survive_migration_rewrite() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // Legacy config with ciCommands (triggers migration) AND preset overrides
        let content = r#"
ciCommands:
  - npm test
setupCommands:
  - npm install
worktree:
  seedIgnored: false
activeBackend: codex
defaultTier: sm
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        // Load triggers migration (rewrites file without ciCommands)
        let loaded = load_project_settings(project_path);

        // Preset fields must survive the migration rewrite
        assert_eq!(loaded.active_backend.as_deref(), Some("codex"));
        assert_eq!(loaded.default_tier.as_deref(), Some("sm"));
        assert!(!loaded.should_seed_worktree_ignored());

        // Verify rewritten file still has preset fields
        let rewritten = std::fs::read_to_string(&config_path).unwrap();
        assert!(rewritten.contains("activeBackend"));
        assert!(rewritten.contains("codex"));
        assert!(rewritten.contains("seedIgnored: false"));
        assert!(!rewritten.contains("ciCommands"));
    }
}
