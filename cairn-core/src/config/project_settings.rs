//! File-based project settings.
//!
//! Project settings are stored in `[project]/.cairn/config.yaml` and are the source of truth.
//! These files can be version-controlled with the project.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::models::TerminalCommand;

/// Project settings as stored in YAML file.
/// All fields are optional - missing fields use defaults.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectSettingsFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_commands: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_files: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_commands: Option<Vec<TerminalCommand>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
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
    let has_persistent = legacy
        .terminal_commands
        .as_ref()
        .map(|cmds| cmds.iter().any(|c| c.persistent))
        .unwrap_or(false);
    let needs_migration = has_ci_commands || has_persistent;

    // Convert to current format (dropping deprecated fields)
    let settings = ProjectSettingsFile {
        setup_commands: legacy.setup_commands,
        copy_files: legacy.copy_files,
        terminal_commands: legacy.terminal_commands.map(|cmds| {
            cmds.into_iter()
                .map(|c| TerminalCommand {
                    name: c.name,
                    command: c.command,
                })
                .collect()
        }),
        default_branch: legacy.default_branch,
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

# Commands to run when setting up a new worktree
# setupCommands:
#   - npm install

# Files to copy from main repo to worktrees (e.g., .env files)
# copyFiles:
#   - .env
#   - config/local.yaml

# Quick-access terminal commands
# terminalCommands:
#   - name: Dev Server
#     command: npm run dev
#   - name: Tests (watch)
#     command: npm test -- --watch

# Default branch for the project (defaults to 'main')
# defaultBranch: main
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
        assert!(settings.copy_files.is_none());
        assert!(settings.terminal_commands.is_none());
        assert!(settings.default_branch.is_none());
    }

    #[test]
    fn test_project_settings_roundtrip() {
        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["npm install".to_string()]),
            copy_files: Some(vec![".env".to_string(), "config/local.yaml".to_string()]),
            terminal_commands: Some(vec![TerminalCommand {
                name: "Dev Server".to_string(),
                command: "npm run dev".to_string(),
            }]),
            default_branch: Some("develop".to_string()),
        };

        let yaml = serde_yaml::to_string(&settings).unwrap();
        let parsed: ProjectSettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.setup_commands, settings.setup_commands);
        assert_eq!(parsed.copy_files, settings.copy_files);
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
        assert!(settings.copy_files.is_none());
        assert!(settings.terminal_commands.is_none());
    }

    #[test]
    fn test_yaml_deserialization_with_copy_files() {
        let yaml = r#"
setupCommands:
  - npm install
copyFiles:
  - .env
  - config/secrets.yaml
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            settings.setup_commands,
            Some(vec!["npm install".to_string()])
        );
        assert_eq!(
            settings.copy_files,
            Some(vec![".env".to_string(), "config/secrets.yaml".to_string()])
        );
    }

    #[test]
    fn test_file_save_and_load() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["cargo build".to_string()]),
            copy_files: None,
            terminal_commands: None,
            default_branch: Some("main".to_string()),
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
            copy_files: None,
            terminal_commands: None,
            default_branch: None,
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
}
