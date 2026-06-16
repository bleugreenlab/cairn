//! File-based project settings.
//!
//! Project settings are stored in `[project]/.cairn/config.yaml` and are the source of truth.
//! These files can be version-controlled with the project.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::models::{Preset, TerminalCommand};
use crate::references::ProjectReference;

/// Worktree behavior settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeSettings {
    /// Legacy field — deserialized for migration but not re-serialized.
    #[serde(default, skip_serializing)]
    pub seed_ignored: Option<bool>,
    /// Rules for populating gitignored paths into new worktrees, grouped by strategy.
    #[serde(default, skip_serializing_if = "PopulateConfig::is_empty")]
    pub populate: PopulateConfig,
}

/// Configuration for how gitignored paths are populated into new worktrees.
///
/// Paths matching `copy` patterns are copied (isolated per worktree).
/// Paths matching `symlink` patterns are symlinked (shared with main repo).
/// Unmatched paths are skipped — new worktrees start clean by default.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PopulateConfig {
    /// Patterns whose matching paths are copied into the worktree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copy: Vec<String>,
    /// Patterns whose matching paths are symlinked to the main repo.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub symlink: Vec<String>,
}

impl PopulateConfig {
    pub fn is_empty(&self) -> bool {
        self.copy.is_empty() && self.symlink.is_empty()
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
    pub references: Option<Vec<ProjectReference>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<WorktreeSettings>,
    /// Project-level override for active backend
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_backend: Option<String>,
    /// Project-level preset overrides (deep-merged with workspace)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<HashMap<String, HashMap<String, Preset>>>,
    /// Project-level external MCP servers (overlay workspace set; project wins
    /// on key collision).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_servers: Option<HashMap<String, crate::config::mcp_servers::McpServerConfig>>,
}

impl ProjectSettingsFile {
    /// Get the populate config for worktrees.
    /// Returns the configured PopulateConfig, or empty (skip-all) by default.
    pub fn populate_config(&self) -> PopulateConfig {
        self.worktree
            .as_ref()
            .map(|w| w.populate.clone())
            .unwrap_or_default()
    }
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
    references: Option<Vec<ProjectReference>>,
    #[serde(default)]
    worktree: Option<WorktreeSettings>,
    // Preset fields — must be present so they survive the legacy parse path
    #[serde(default)]
    active_backend: Option<String>,
    #[serde(default)]
    backends: Option<HashMap<String, HashMap<String, Preset>>>,
    #[serde(default)]
    mcp_servers: Option<HashMap<String, crate::config::mcp_servers::McpServerConfig>>,
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

/// Get the path to the project config file (\[project\]/.cairn/config.yaml)
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
                match save_project_settings(project_path, &file) {
                    Ok(()) => {
                        // The rewrite lands in the project's canonical checkout;
                        // commit it so the migration does not float as dirt.
                        super::commit_project_config_change(
                            project_path,
                            "cairn: migrate project config",
                        );
                    }
                    Err(e) => log::warn!("Failed to migrate project settings: {}", e),
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

/// Resolve the effective default branch for a project.
///
/// Precedence: an explicit `defaultBranch` in the project's `.cairn/config.yaml`
/// wins, then the value stored on the project row, then the hard fallback
/// `"main"`. Both the UI projection and worktree creation resolve through this
/// helper so they always agree on which branch worktrees are based on.
pub fn resolve_default_branch(
    config: &ProjectSettingsFile,
    stored_default_branch: Option<&str>,
) -> String {
    config
        .default_branch
        .clone()
        .or_else(|| stored_default_branch.map(str::to_string))
        .unwrap_or_else(|| "main".to_string())
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
             Use worktree.populate.copy patterns instead.",
            files
        );
    }
    let has_persistent = legacy
        .terminal_commands
        .as_ref()
        .map(|cmds| cmds.iter().any(|c| c.persistent))
        .unwrap_or(false);
    // Legacy seedIgnored field triggers migration to clear it from the file
    let has_legacy_seed_ignored = legacy
        .worktree
        .as_ref()
        .and_then(|w| w.seed_ignored)
        .is_some();
    let needs_migration =
        has_ci_commands || has_copy_files || has_persistent || has_legacy_seed_ignored;

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
        references: legacy.references,
        worktree: legacy.worktree,
        active_backend: legacy.active_backend,
        backends: legacy.backends,
        mcp_servers: legacy.mcp_servers,
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
# Worktree population: control which gitignored content is pre-populated.
# Paths matching 'copy' patterns are copied (isolated per worktree).
# Paths matching 'symlink' patterns are symlinked (shared with main repo).
# Unmatched paths are skipped — setup commands handle the rest.
#
# worktree:
#   populate:
#     copy:
#       - ".env"
#       - ".env.*"
#     symlink:
#       - "target/"
#       - ".cache/"

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
# references:
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
    fn resolve_default_branch_precedence() {
        let mut config = ProjectSettingsFile::default();
        // Nothing configured: hard fallback.
        assert_eq!(resolve_default_branch(&config, None), "main");
        // Stored column used when there is no config override.
        assert_eq!(resolve_default_branch(&config, Some("staging")), "staging");
        // Config override wins over the stored column.
        config.default_branch = Some("develop".to_string());
        assert_eq!(resolve_default_branch(&config, Some("staging")), "develop");
    }

    #[test]
    fn test_project_settings_defaults() {
        let settings = ProjectSettingsFile::default();
        assert!(settings.setup_commands.is_none());
        assert!(settings.terminal_commands.is_none());
        assert!(settings.default_branch.is_none());
        assert!(settings.populate_config().is_empty());
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
        assert!(settings.populate_config().is_empty());
    }

    #[test]
    fn test_worktree_legacy_seed_ignored_parsed() {
        let yaml = r#"
worktree:
  seedIgnored: false
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();

        // Legacy field is deserialized as Option<bool>
        assert_eq!(
            settings.worktree.as_ref().and_then(|w| w.seed_ignored),
            Some(false)
        );
        // But populate_config is still empty (legacy field doesn't populate anything)
        assert!(settings.populate_config().is_empty());
    }

    #[test]
    fn test_worktree_populate_config() {
        let yaml = r#"
worktree:
  populate:
    copy:
      - ".env"
      - ".env.*"
    symlink:
      - "target/"
      - ".cache/"
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let config = settings.populate_config();

        assert!(!config.is_empty());
        assert_eq!(config.copy, vec![".env", ".env.*"]);
        assert_eq!(config.symlink, vec!["target/", ".cache/"]);
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
    fn test_populate_config_save_and_load_roundtrip() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        let settings = ProjectSettingsFile {
            worktree: Some(WorktreeSettings {
                seed_ignored: None,
                populate: PopulateConfig {
                    copy: vec![".env".to_string(), ".env.*".to_string()],
                    symlink: vec!["target/".to_string()],
                },
            }),
            ..Default::default()
        };

        save_project_settings(project_path, &settings).unwrap();

        let loaded = load_project_settings(project_path);
        let config = loaded.populate_config();
        assert_eq!(config.copy, vec![".env", ".env.*"]);
        assert_eq!(config.symlink, vec!["target/"]);

        // Verify the serialized YAML contains expected structure
        let config_path = get_project_config_path(project_path);
        let raw = std::fs::read_to_string(&config_path).unwrap();
        assert!(raw.contains("populate"));
        assert!(raw.contains(".env"));
        assert!(raw.contains("target/"));
        // seedIgnored should never appear in new files
        assert!(!raw.contains("seedIgnored"));
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
    fn test_reference_serde_git() {
        let yaml = r#"
references:
  - name: openpnp
    git: https://github.com/openpnp/openpnp.git
    description: OpenPnP source code
    branch: develop
  - name: local-specs
    path: ~/Documents/specs
    description: Hardware specifications
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let references = settings.references.unwrap();
        assert_eq!(references.len(), 2);

        assert_eq!(references[0].name, "openpnp");
        assert_eq!(
            references[0].git.as_deref(),
            Some("https://github.com/openpnp/openpnp.git")
        );
        assert!(references[0].path.is_none());
        assert_eq!(references[0].branch.as_deref(), Some("develop"));

        assert_eq!(references[1].name, "local-specs");
        assert!(references[1].git.is_none());
        assert_eq!(references[1].path.as_deref(), Some("~/Documents/specs"));
        assert!(references[1].branch.is_none());
    }

    #[test]
    fn test_reference_roundtrip() {
        let settings = ProjectSettingsFile {
            references: Some(vec![ProjectReference {
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
        let references = parsed.references.unwrap();
        assert_eq!(references.len(), 1);
        assert_eq!(references[0].name, "docs");
        assert_eq!(
            references[0].git.as_deref(),
            Some("https://github.com/org/docs.git")
        );
    }

    #[test]
    fn test_settings_without_references() {
        let yaml = r#"
setupCommands:
  - npm install
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert!(settings.references.is_none());
    }

    #[test]
    fn test_resources_key_is_not_accepted_as_references() {
        let yaml = r#"
resources:
  - name: docs
    git: https://github.com/org/docs.git
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert!(settings.references.is_none());
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
backends:
  codex:
    lg:
      model: gpt-5.5
      options:
        reasoningEffort: high
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        let loaded = load_project_settings(project_path);

        // Preset fields must survive the legacy parse path
        assert_eq!(loaded.active_backend.as_deref(), Some("codex"));
        assert!(loaded.backends.is_some());
        let backends = loaded.backends.unwrap();
        assert!(backends.contains_key("codex"));
        assert_eq!(backends["codex"]["lg"].model.as_str(), "gpt-5.5");
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
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        // Load triggers migration (rewrites file without ciCommands)
        let loaded = load_project_settings(project_path);

        // Preset fields must survive the migration rewrite
        assert_eq!(loaded.active_backend.as_deref(), Some("codex"));
        // Legacy seedIgnored is parsed but results in empty populate config
        assert!(loaded.populate_config().is_empty());

        // Verify rewritten file still has preset fields
        let rewritten = std::fs::read_to_string(&config_path).unwrap();
        assert!(rewritten.contains("activeBackend"));
        assert!(rewritten.contains("codex"));
        // seedIgnored is skip_serializing, so it's dropped on migration rewrite
        assert!(!rewritten.contains("seedIgnored"));
        assert!(!rewritten.contains("ciCommands"));
    }

    fn init_git_repo(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn commit_all(repo: &Path, msg: &str) {
        crate::env::git()
            .args(["add", "-A"])
            .current_dir(repo)
            .status()
            .unwrap();
        crate::env::git()
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@local.invalid",
                "commit",
                "-q",
                "-m",
                msg,
            ])
            .current_dir(repo)
            .status()
            .unwrap();
    }

    fn git_status(path: &Path) -> String {
        let out = crate::env::git()
            .args(["status", "--porcelain"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_head_subject(path: &Path) -> String {
        let out = crate::env::git()
            .args(["log", "-1", "--pretty=%s"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn legacy_config_migration_commits_in_canonical_repo() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        init_git_repo(project_path);

        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(
            &config_path,
            "ciCommands:\n  - npm test\nsetupCommands:\n  - npm install\n",
        )
        .unwrap();
        commit_all(project_path, "seed legacy config");

        // Loading migrates the file (drops ciCommands) and commits the rewrite.
        let loaded = load_project_settings(project_path);
        assert_eq!(loaded.setup_commands, Some(vec!["npm install".to_string()]));

        let migrated = std::fs::read_to_string(&config_path).unwrap();
        assert!(!migrated.contains("ciCommands"));
        assert!(
            git_status(project_path).is_empty(),
            "migration left the canonical repo dirty"
        );
        assert_eq!(
            git_head_subject(project_path),
            "cairn: migrate project config"
        );
    }
}
