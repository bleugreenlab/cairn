//! File-based project settings.
//!
//! Project settings are stored in `[project]/.cairn/config.yaml` and are the source of truth.
//! These files can be version-controlled with the project.

use serde::{Deserialize, Serialize};

pub use super::contextual_packages::{
    ContextualPackageKind, ContextualPackageRef, ContextualPackagesConfig,
};
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
    seed_ignored: Option<bool>,
    /// Rules for populating gitignored paths into new worktrees, grouped by strategy.
    #[serde(default, skip_serializing_if = "PopulateConfig::is_empty")]
    pub populate: PopulateConfig,
}

// The check/worktree-populate config value types now live in `models::project`
// (pure serde data, no upward dependency on config). Re-exported here so every
// `project_settings::CheckCommand`-style path keeps resolving.
pub use crate::models::{CheckCommand, CheckPolicy, CheckResourceClass, CheckWhen, PopulateConfig};

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
    pub checks: Option<HashMap<String, CheckCommand>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) default_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<ProjectReference>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<WorktreeSettings>,
    /// Project-level override for active backend
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) active_backend: Option<String>,
    /// Project-level preset overrides (deep-merged with workspace)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) backends: Option<HashMap<String, HashMap<String, Preset>>>,
    /// Project-level external MCP servers (overlay workspace set; project wins
    /// on key collision).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) mcp_servers: Option<HashMap<String, crate::config::mcp_servers::McpServerConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contextual_packages: Option<ContextualPackagesConfig>,
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
    checks: Option<HashMap<String, CheckCommand>>,
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
    #[serde(default)]
    contextual_packages: Option<ContextualPackagesConfig>,
}

/// Legacy terminal command with persistent field
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyTerminalCommand {
    name: String,
    command: String,
    #[serde(default)]
    persistent: bool,
    // Carried through the always-run legacy parse so a `write` carveout on a
    // terminal command is not dropped during migration. See `TerminalCommand`.
    #[serde(default)]
    write: Vec<String>,
}

/// Get the path to the project config file (\[project\]/.cairn/config.yaml)
pub(crate) fn get_project_config_path(project_path: &Path) -> PathBuf {
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
                // `save_project_settings` commits the rewrite (scoped to
                // `config.yaml`) so the migration does not float as dirt.
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
    let mut settings = ProjectSettingsFile {
        setup_commands: legacy.setup_commands,
        terminal_commands: legacy.terminal_commands.map(|cmds| {
            cmds.into_iter()
                .map(|c| TerminalCommand {
                    name: c.name,
                    command: c.command,
                    write: c.write,
                })
                .collect()
        }),
        checks: legacy.checks,
        default_branch: legacy.default_branch,
        references: legacy.references,
        worktree: legacy.worktree,
        active_backend: legacy.active_backend,
        backends: legacy.backends,
        mcp_servers: legacy.mcp_servers,
        contextual_packages: legacy.contextual_packages,
    };

    if let Some(policy) = &mut settings.contextual_packages {
        policy.normalize()?;
    }

    Ok((settings, needs_migration))
}

/// Load the project's terminal commands.
///
/// Reads `[project_path]/.cairn/config.yaml` directly without the migration
/// rewrite that `load_project_settings` performs, so it is safe on the hot
/// worktree-fence policy-build path (it reads each terminal command's `write`
/// carveout). Returns an empty list when the file is absent or invalid.
pub(crate) fn load_terminal_commands(project_path: &Path) -> Vec<crate::models::TerminalCommand> {
    load_project_settings_file(project_path)
        .map(|(file, _)| file.terminal_commands.unwrap_or_default())
        .unwrap_or_default()
}

/// Load the project's canonical worktree setup commands without rewriting config.
///
/// Build-slot submission uses this against the live primary checkout. Unlike
/// [`load_project_settings`], errors remain visible so an unreadable or invalid
/// setup policy fails as infrastructure before any command reaches an executor.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecutionProjectPolicy {
    pub(crate) setup_commands: Vec<String>,
    pub(crate) populate: PopulateConfig,
}

pub(crate) fn load_execution_project_policy(
    project_path: &Path,
) -> Result<ExecutionProjectPolicy, String> {
    load_project_settings_file(project_path).map(|(file, _)| ExecutionProjectPolicy {
        setup_commands: file.setup_commands.unwrap_or_default(),
        populate: file.worktree.unwrap_or_default().populate,
    })
}

pub fn load_setup_commands(project_path: &Path) -> Result<Vec<String>, String> {
    load_execution_project_policy(project_path).map(|policy| policy.setup_commands)
}

/// Load just the `checks` contract from a project's `.cairn/config.yaml`.
///
/// Reads the file directly without the migration rewrite (and its scoped git
/// commit) that `load_project_settings` performs, mirroring
/// [`load_terminal_commands`]. The synchronous `when:write` check runner calls
/// this against the project's LIVE main checkout on every sealed commit, so it
/// must stay side-effect free — a migration commit fired from inside an agent
/// run would be surprising. Returns `None` when the file is absent, invalid, or
/// declares no checks.
pub(crate) fn load_checks(project_path: &Path) -> Option<HashMap<String, CheckCommand>> {
    load_project_settings_file(project_path)
        .ok()
        .and_then(|(file, _)| file.checks)
}

/// Managed top-level config keys: the current schema fields plus the legacy keys
/// the migration removes. A top-level key on disk that is NOT in this set is a
/// user-authored key the comment-preserving merge leaves untouched.
const MANAGED_TOP_LEVEL_KEYS: &[&str] = &[
    "setupCommands",
    "terminalCommands",
    "checks",
    "defaultBranch",
    "references",
    "worktree",
    "activeBackend",
    "backends",
    "mcpServers",
    "contextualPackages",
    // Legacy keys the load-path migration strips.
    "ciCommands",
    "copyFiles",
];

/// Save project settings to file.
///
/// When `config.yaml` already exists and parses cleanly, the write is a
/// diff-scoped merge that rewrites only the subtrees whose value changed,
/// preserving every comment and unknown key (see [`super::yaml_edit`]). A fresh
/// file, or a document the merge declines, falls back to a full serialization
/// with the header comment. The commit is pushed best-effort: a project's config
/// lives on its shared branch, and a diverged local main breaks issue PR merges.
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

    let content = render_settings_yaml(&path, settings)?;

    std::fs::write(&path, content)
        .map_err(|e| format!("Failed to write project config file: {}", e))?;
    super::commit_and_maybe_push(
        std::slice::from_ref(&path),
        "cairn: update project config",
        Some(project_path),
    );
    Ok(())
}

/// The YAML text to write for `settings`.
///
/// Prefers a comment-preserving merge into the existing file; falls back to a
/// full serialization (with the header comment) for a fresh or unmergeable file.
/// A semantically no-op save returns the on-disk bytes verbatim, so git stages
/// nothing and no commit is produced.
fn render_settings_yaml(path: &Path, settings: &ProjectSettingsFile) -> Result<String, String> {
    let target_value = serde_yaml::to_value(settings)
        .map_err(|e| format!("Failed to serialize project settings: {}", e))?;
    let target_mapping = match &target_value {
        serde_yaml::Value::Mapping(m) => m,
        _ => return Err("project settings did not serialize to a mapping".to_string()),
    };

    if let Ok(original) = std::fs::read_to_string(path) {
        if !original.trim().is_empty() {
            match super::yaml_edit::merge_into_yaml(
                &original,
                target_mapping,
                MANAGED_TOP_LEVEL_KEYS,
            ) {
                Ok(merged) => return Ok(merged),
                Err(e) => {
                    log::debug!("Comment-preserving YAML merge fell back to full rewrite: {e}")
                }
            }
        }
    }

    let yaml = serde_yaml::to_string(settings)
        .map_err(|e| format!("Failed to serialize project settings: {}", e))?;
    Ok(format!("# Cairn Project Configuration\n{}", yaml))
}

/// Create a default project config file with commented template
pub(crate) fn create_default_project_config(project_path: &Path) -> Result<(), String> {
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
# Paths matching 'copy' patterns are copied from the main repo (isolated per worktree).
# Paths matching 'symlink' patterns are symlinked to the main repo.
# Unmatched paths are skipped — setup commands handle the rest.
#
# worktree:
#   populate:
#     copy:
#       - ".env"
#       - ".env.*"
#     symlink:
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
    fn removed_executor_keys_are_accepted_and_dropped_on_rewrite() {
        let yaml = r#"
run:
  executor: build-slot
checks:
  frontend:
    command: vitest run
    when: review
    executor: build-slot
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let frontend = settings.checks.as_ref().unwrap().get("frontend").unwrap();
        assert_eq!(frontend.command, "vitest run");
        assert_eq!(frontend.when, CheckWhen::Review);

        let serialized = serde_yaml::to_string(&settings).unwrap();
        assert!(!serialized.contains("run:"));
        assert!(!serialized.contains("executor:"));
    }

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
        assert!(settings.checks.is_none());
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
                write: vec![],
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
    fn test_checks_parse_and_roundtrip() {
        let yaml = r#"
checks:
  frontend:
    command: vitest related {changedFiles}
    impact:
      - src/**
      - packages/ui/**
    policy: gate
    when: idle
    executor: build-slot
  typecheck:
    command: tsc --noEmit
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let checks = settings.checks.as_ref().unwrap();
        let frontend = checks.get("frontend").unwrap();
        assert_eq!(frontend.command, "vitest related {changedFiles}");
        assert_eq!(frontend.policy, CheckPolicy::Gate);
        // `when: idle` is a legacy alias that deserializes to the merged Review
        // cadence (see `CheckWhen`).
        assert_eq!(frontend.when, CheckWhen::Review);
        assert_eq!(
            frontend.impact.as_deref(),
            Some(&["src/**".to_string(), "packages/ui/**".to_string()][..])
        );

        let typecheck = checks.get("typecheck").unwrap();
        assert_eq!(typecheck.command, "tsc --noEmit");
        assert_eq!(typecheck.policy, CheckPolicy::Advisory);
        assert_eq!(typecheck.when, CheckWhen::Write);

        let serialized = serde_yaml::to_string(&settings).unwrap();
        let reparsed: ProjectSettingsFile = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.checks, settings.checks);
    }

    #[test]
    fn test_check_timeout_parses_and_round_trips() {
        // A per-check `timeout` (seconds) parses, defaults to `None` when absent,
        // and survives serialization. `timeout` has no `skip_serializing_if`
        // default guard, so a plain non-zero value round-trips faithfully.
        let yaml = r#"
checks:
  rust-full:
    command: bun run test:rust
    when: review
    timeout: 2400
  frontend:
    command: vitest run
"#;
        let settings: ProjectSettingsFile = serde_yaml::from_str(yaml).unwrap();
        let checks = settings.checks.as_ref().unwrap();
        assert_eq!(checks.get("rust-full").unwrap().timeout, Some(2400));
        // A check that omits `timeout` deserializes to `None` (cadence default).
        assert_eq!(checks.get("frontend").unwrap().timeout, None);

        // Round-trip: the configured timeout survives a serialize/parse cycle.
        let serialized = serde_yaml::to_string(&settings).unwrap();
        let reparsed: ProjectSettingsFile = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.checks, settings.checks);
    }

    #[test]
    fn test_checks_reject_unknown_enums() {
        let bad_policy = r#"
checks:
  frontend:
    command: vitest run
    policy: blocking
"#;
        let err = serde_yaml::from_str::<ProjectSettingsFile>(bad_policy)
            .expect_err("unknown policy should fail");
        assert!(err.to_string().contains("blocking"));
        assert!(err.to_string().contains("advisory") || err.to_string().contains("Advisory"));

        let bad_when = r#"
checks:
  frontend:
    command: vitest run
    when: commit
"#;
        let err = serde_yaml::from_str::<ProjectSettingsFile>(bad_when)
            .expect_err("unknown cadence should fail");
        assert!(err.to_string().contains("commit"));
        assert!(err.to_string().contains("write") || err.to_string().contains("Write"));

        // `pr` was the old cadence name; it is now rejected in favor of `review`.
        let legacy_pr = r#"
checks:
  frontend:
    command: vitest run
    when: pr
"#;
        let err = serde_yaml::from_str::<ProjectSettingsFile>(legacy_pr)
            .expect_err("legacy `pr` cadence should fail");
        assert!(err.to_string().contains("pr"));

        // `review` is the current full-suite cadence and parses.
        let review = r#"
checks:
  frontend:
    command: vitest run
    when: review
"#;
        let settings = serde_yaml::from_str::<ProjectSettingsFile>(review)
            .expect("`review` cadence should parse");
        assert_eq!(
            settings.checks.unwrap().get("frontend").unwrap().when,
            CheckWhen::Review
        );
    }

    #[test]
    fn legacy_idle_cadence_aliases_to_review() {
        // The `idle` cadence was collapsed into `review`; a `#[serde(alias)]`
        // keeps un-migrated project configs parsing (rather than silently
        // disabling every check) by mapping the old name onto the merged cadence.
        let yaml = r#"
checks:
  frontend:
    command: vitest run
    when: idle
"#;
        let settings =
            serde_yaml::from_str::<ProjectSettingsFile>(yaml).expect("`idle` alias should parse");
        assert_eq!(
            settings.checks.unwrap().get("frontend").unwrap().when,
            CheckWhen::Review
        );
    }

    #[test]
    fn test_checks_survive_legacy_migration() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        let legacy_content = r#"ciCommands:
  - npm test
checks:
  frontend:
    command: vitest run
    policy: gate
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, legacy_content).unwrap();

        let loaded = load_project_settings(project_path);
        let frontend = loaded
            .checks
            .as_ref()
            .and_then(|checks| checks.get("frontend"))
            .expect("checks should survive migration-triggering load");
        assert_eq!(frontend.command, "vitest run");
        assert_eq!(frontend.policy, CheckPolicy::Gate);

        let migrated_content = std::fs::read_to_string(&config_path).unwrap();
        assert!(migrated_content.contains("checks:"));
        assert!(migrated_content.contains("frontend:"));
        assert!(!migrated_content.contains("ciCommands"));
    }

    #[test]
    fn load_checks_reads_without_migrating() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        // A legacy config (ciCommands) that `load_project_settings` would rewrite
        // and commit. `load_checks` must read the checks WITHOUT that side effect.
        let legacy =
            "ciCommands:\n  - npm test\nchecks:\n  frontend:\n    command: vitest run\n    when: write\n";
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, legacy).unwrap();

        let checks = load_checks(project_path).expect("checks present");
        assert!(checks.contains_key("frontend"));
        assert_eq!(
            std::fs::read_to_string(&config_path).unwrap(),
            legacy,
            "load_checks must not rewrite the file"
        );
    }

    #[test]
    fn load_checks_none_when_absent_or_no_checks() {
        let temp = TempDir::new().unwrap();
        assert!(load_checks(temp.path()).is_none(), "absent file ⇒ None");

        let config_path = get_project_config_path(temp.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, "setupCommands:\n  - npm install\n").unwrap();
        assert!(load_checks(temp.path()).is_none(), "no checks key ⇒ None");
    }

    #[test]
    fn test_project_settings_without_checks_is_load_save_stable() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();
        let content =
            "# Cairn Project Configuration\nsetupCommands:\n- npm install\ndefaultBranch: main\n";
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        let (_loaded, needs_migration) = load_project_settings_file(project_path).unwrap();
        assert!(!needs_migration);
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), content);
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

    fn git_init(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_bare(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "--bare", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_set_origin(repo: &Path, origin: &Path) {
        assert!(crate::env::git()
            .args(["remote", "add", "origin"])
            .arg(origin)
            .current_dir(repo)
            .status()
            .unwrap()
            .success());
    }

    fn git_commit_count(path: &Path) -> usize {
        let out = crate::env::git()
            .args(["rev-list", "--count", "HEAD"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .unwrap_or(0)
    }

    fn git_branch(path: &Path) -> String {
        let out = crate::env::git()
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn origin_has_branch(origin: &Path, branch: &str) -> bool {
        crate::env::git()
            .args(["rev-parse", "--verify", "--quiet"])
            .arg(format!("refs/heads/{branch}"))
            .current_dir(origin)
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[test]
    fn save_project_settings_commits_scoped_and_pushes() {
        let temp = TempDir::new().unwrap();
        let origin = temp.path().join("origin.git");
        std::fs::create_dir_all(&origin).unwrap();
        git_bare(&origin);
        let proj = temp.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();
        git_init(&proj);
        git_set_origin(&proj, &origin);
        std::fs::write(proj.join("unrelated.txt"), "dirty").unwrap();

        let settings = ProjectSettingsFile {
            setup_commands: Some(vec!["npm install".to_string()]),
            ..Default::default()
        };
        save_project_settings(&proj, &settings).unwrap();

        assert_eq!(git_head_subject(&proj), "cairn: update project config");
        // Exactly one scoped commit (no migration/double commit).
        assert_eq!(git_commit_count(&proj), 1);
        let status = git_status(&proj);
        assert!(
            status.contains("unrelated.txt"),
            "unrelated stays dirty: {status:?}"
        );
        assert!(
            !status.contains("config.yaml"),
            "config.yaml committed: {status:?}"
        );
        // The commit is pushed to origin best-effort on a detached thread, so a
        // diverged local main can never break issue PR merges. Poll until the
        // background push lands the branch.
        let branch = git_branch(&proj);
        let mut pushed = false;
        for _ in 0..40 {
            if origin_has_branch(&origin, &branch) {
                pushed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(pushed, "settings save should push its commit to origin");
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
    fn test_legacy_seed_populate_config_is_ignored() {
        // The `seed` worktree-populate mechanism was removed (CAIRN-2622): the
        // clone source was a live dev-instance target dir and could capture a
        // torn snapshot; sccache is the canonical cross-worktree compile cache.
        // A pre-existing config that still carries a `seed:` block must keep
        // deserializing — the key is now an ignored unknown field, and the
        // surviving copy/symlink rules are honored.
        let raw = r#"
worktree:
  populate:
    copy:
      - ".env"
    seed:
      - from: "~/.warm/target"
        to: "src-tauri/target"
        exclude: ["*/incremental"]
"#;

        let settings: ProjectSettingsFile = serde_yaml::from_str(raw).unwrap();
        let config = settings.populate_config();
        assert_eq!(config.copy, vec![".env"]);
        assert!(config.symlink.is_empty());
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
    fn terminal_command_write_parses_and_survives_migration() {
        let temp = TempDir::new().unwrap();
        let project_path = temp.path();

        // ciCommands forces the legacy migration rewrite; a terminal command's
        // `write` carveout must ride through it (the always-run legacy parse).
        let content = r#"
ciCommands:
  - npm test
terminalCommands:
  - name: Dev
    command: "bun run build:cmd && bun dev:instance"
    write:
      - "~/.cairn-dev-*"
"#;
        let config_path = get_project_config_path(project_path);
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, content).unwrap();

        let loaded = load_terminal_commands(project_path);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "Dev");
        assert_eq!(loaded[0].write, vec!["~/.cairn-dev-*"]);

        // After the migration rewrite the carveout is still present on disk.
        load_project_settings(project_path);
        let after = load_terminal_commands(project_path);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].write, vec!["~/.cairn-dev-*"]);
    }

    #[test]
    fn load_terminal_commands_empty_when_absent() {
        let temp = TempDir::new().unwrap();
        assert!(load_terminal_commands(temp.path()).is_empty());
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
    fn execution_policy_loads_setup_and_populate_without_rewriting() {
        let temp = TempDir::new().unwrap();
        let config_path = get_project_config_path(temp.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        let content = "ciCommands:\n  - old\nsetupCommands:\n  - bun install\nworktree:\n  populate:\n    copy: [.env]\n    symlink: [cache/]\n";
        std::fs::write(&config_path, content).unwrap();

        let policy = load_execution_project_policy(temp.path()).unwrap();
        assert_eq!(policy.setup_commands, vec!["bun install"]);
        assert_eq!(policy.populate.copy, vec![".env"]);
        assert_eq!(policy.populate.symlink, vec!["cache/"]);
        assert_eq!(std::fs::read_to_string(config_path).unwrap(), content);
    }

    #[test]
    fn setup_command_hot_path_is_fallible_and_side_effect_free() {
        let temp = TempDir::new().unwrap();
        let config_path = get_project_config_path(temp.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(&config_path, "setupCommands: [unterminated").unwrap();
        assert!(load_setup_commands(temp.path()).is_err());

        let legacy = "ciCommands:\n  - npm test\nsetupCommands:\n  - npm install\n";
        std::fs::write(&config_path, legacy).unwrap();
        assert_eq!(
            load_setup_commands(temp.path()).unwrap(),
            vec!["npm install".to_string()]
        );
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), legacy);
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

        // Loading migrates the file (drops ciCommands) and commits the rewrite
        // through `save_project_settings`, which is the scoped commit funnel.
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
            "cairn: update project config"
        );
    }
}
