//! File-based workspace settings.
//!
//! Settings are stored in `~/.cairn/settings.yaml` and are the source of truth.
//! The database is no longer used for workspace settings.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

use cairn_common::logging::LogLevel;

use crate::config::presets::{default_presets_config, PresetsConfig};
use crate::models::{
    ExternalReplyMode, MergeType, Model, OpenRouterRouting, Preset, Settings, ThinkingDisplayMode,
};

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
    pub tiers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<HashMap<String, HashMap<String, Preset>>>,

    /// External MCP servers reachable through the `cairn://mcp/...` gateway.
    /// Keyed by server name. Project config overlays this set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_servers: Option<HashMap<String, crate::config::mcp_servers::McpServerConfig>>,

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
    pub orphan_cleanup_days: Option<i32>,
    #[serde(default)]
    pub repo_target_sweep_days: Option<i32>,
    /// Whether agent bug reports are enabled (default: true)
    #[serde(default)]
    pub bug_reports: Option<bool>,
    /// Thinking block display mode in chat transcripts
    #[serde(default)]
    pub thinking_display_mode: Option<ThinkingDisplayMode>,
    /// File-log verbosity level. Absent = the light `Standard` default; `verbose`
    /// is the opt-in full-debug + profiler level (today's behavior).
    #[serde(default)]
    pub log_level: Option<LogLevel>,
    /// Whether memory review prompts and automatic memory-triage issue creation are enabled.
    #[serde(default)]
    pub memory_review_enabled: Option<bool>,
    /// Number of exact-scope pending memories that triggers a memory-triage issue.
    #[serde(default)]
    pub pending_memory_threshold: Option<i32>,
    /// How replies to the special `to: "external"` target are handled.
    #[serde(default)]
    pub external_replies: Option<ExternalReplyMode>,
    /// Sensitive paths the OS sandbox hard-denies reads of for worktree agents.
    /// `~` is expanded to the user's home. Absent = the conservative built-in
    /// default (cloud cred stores, ssh/gpg keys, `~/.cairn[-dev]`). See
    /// `docs/worktree-fence.md`.
    #[serde(default)]
    pub sandbox_deny_read: Option<Vec<String>>,
    /// Managed Build Services: Cairn-supervised shared daemons (e.g. an sccache
    /// server) that run under a service sandbox and inject client env into fenced
    /// agent spawns. Config-only (YAML, not in the Settings DTO). Absent = the
    /// built-in default (a disabled-unless-`sccache`-on-PATH sccache entry). See
    /// `docs/worktree-fence.md` — Managed Build Services.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_services: Option<HashMap<String, crate::config::build_services::BuildServiceConfig>>,
    /// Typed web-fetch provider options, keyed by provider id
    /// (`bmd`/`jina`/`firecrawl`) then option key. Config-only (YAML, not in the
    /// Settings DTO). Validated against the per-provider descriptor in
    /// `crate::config::web_fetch`. See `docs/settings.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_fetch: Option<HashMap<String, HashMap<String, serde_yaml::Value>>>,
    /// Which web-fetch provider backs fetch. Config-only (YAML, not in the
    /// Settings DTO). Absent / `regular` = the built-in plain-HTTP fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_web_fetch: Option<String>,
    /// Typed web-search provider options, keyed by provider id
    /// (`tavily`/`exa`/`brave`/`jina`) then option key. Config-only (YAML, not
    /// in the Settings DTO). Validated against the per-provider descriptor in
    /// `crate::config::web_search`. See `docs/settings.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_search: Option<HashMap<String, HashMap<String, serde_yaml::Value>>>,
    /// Which typed web-search provider backs `cairn://websearch`. Config-only
    /// (YAML, not in the Settings DTO). Absent = web search is unconfigured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_web_search: Option<String>,
    /// Typed PDF-extraction provider options, keyed by provider id
    /// (`local`/`bmd`) then option key. Config-only (YAML, not in the Settings
    /// DTO). Validated against `crate::config::pdf`. See `docs/settings.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pdf: Option<HashMap<String, HashMap<String, serde_yaml::Value>>>,
    /// Which PDF-extraction provider backs `.pdf` reads. Config-only (YAML, not
    /// in the Settings DTO). Absent / `local` = the built-in local extractor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_pdf: Option<String>,
    /// Flat monthly subscription fee per backend, in USD. A backend absent from
    /// this map is treated as metered (pay-as-you-go, no normalization). e.g.
    /// `{"claude": 200.0, "codex": 200.0}`. Drives effective-cost analytics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_fees: Option<HashMap<String, f64>>,
    /// OpenRouter provider-routing controls (ZDR + sort). Absent = OpenRouter's
    /// normal routing; omitted from YAML when all-default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub openrouter_routing: Option<OpenRouterRouting>,
    /// Per-project commands the user has accepted as worktree-fence crossers
    /// (`projectId -> [command, ...]`). A project terminal command's `write`
    /// carveout (or coarse fence crossing) is honored only when its command is
    /// listed here, so a cloned repo can declare a fence-crosser but cannot grant
    /// itself the crossing — acceptance is user-owned. Config-only (YAML, not in
    /// the Settings DTO); preserved across saves. See `crate::config::dev_commands`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_fence_commands: Option<HashMap<String, Vec<String>>>,
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
            // The set of known backends is defined in code, not in the settings
            // file. Start from the full default set so every available provider
            // (including ones added after this install was first configured)
            // always appears, then overlay the on-disk customizations per
            // backend so the user's tier choices still win. Without this, an
            // older settings.yaml that names only `claude`/`codex` would hide a
            // newer backend like `openrouter` entirely.
            let mut merged = default_presets_config(max_thinking_tokens).backends;
            for (name, presets) in backends.clone() {
                merged.insert(name, presets);
            }
            PresetsConfig {
                active_backend: self
                    .active_backend
                    .clone()
                    .unwrap_or_else(|| "claude".to_string()),
                tiers: self.tiers.clone().unwrap_or_else(|| {
                    crate::config::presets::DEFAULT_TIERS
                        .iter()
                        .map(|s| s.to_string())
                        .collect()
                }),
                backends: merged,
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
                }
            }

            config
        };

        Settings {
            active_backend: presets.active_backend,
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
            orphan_cleanup_days: self.orphan_cleanup_days.unwrap_or(3),
            repo_target_sweep_days: self.repo_target_sweep_days.unwrap_or(0).max(0),
            bug_reports: self.bug_reports.unwrap_or(true),
            thinking_display_mode: self
                .thinking_display_mode
                .clone()
                .unwrap_or(ThinkingDisplayMode::Full),
            memory_review_enabled: self.memory_review_enabled.unwrap_or(true),
            pending_memory_threshold: self.pending_memory_threshold.unwrap_or(5).max(1),
            external_replies: self
                .external_replies
                .clone()
                .unwrap_or(ExternalReplyMode::Watchers),
            log_level: self.log_level.unwrap_or(LogLevel::Standard),
            subscription_fees: self.subscription_fees.clone().unwrap_or_default(),
            openrouter_routing: self.openrouter_routing.clone().unwrap_or_default(),
        }
    }

    /// Create from Settings DTO
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            active_backend: Some(settings.active_backend.clone()),
            tiers: Some(settings.tiers.clone()),
            backends: Some(settings.backends.clone()),
            mcp_servers: None,
            default_model: None,
            preferred_models: None,
            branch_prefix: Some(settings.branch_prefix.clone()),
            max_thinking_tokens: Some(settings.max_thinking_tokens),
            merge_type: Some(settings.merge_type.clone()),
            pull_on_merge: Some(settings.pull_on_merge),
            auto_start_jobs: None, // No longer serialized
            orphan_cleanup_days: Some(settings.orphan_cleanup_days),
            repo_target_sweep_days: Some(settings.repo_target_sweep_days.max(0)),
            bug_reports: Some(settings.bug_reports),
            thinking_display_mode: Some(settings.thinking_display_mode.clone()),
            log_level: Some(settings.log_level),
            memory_review_enabled: Some(settings.memory_review_enabled),
            pending_memory_threshold: Some(settings.pending_memory_threshold.max(1)),
            external_replies: Some(settings.external_replies.clone()),
            subscription_fees: if settings.subscription_fees.is_empty() {
                None
            } else {
                Some(settings.subscription_fees.clone())
            },
            openrouter_routing: if settings.openrouter_routing == OpenRouterRouting::default() {
                None
            } else {
                Some(settings.openrouter_routing.clone())
            },
            // Config-only (YAML, not in the DTO); preserved across saves.
            sandbox_deny_read: None,
            build_services: None,
            accepted_fence_commands: None,
            web_fetch: None,
            active_web_fetch: None,
            web_search: None,
            active_web_search: None,
            pdf: None,
            active_pdf: None,
        }
    }
}

/// Get the path to the settings file
pub fn get_settings_path(config_dir: &std::path::Path) -> PathBuf {
    config_dir.join("settings.yaml")
}

/// The file-log verbosity level (default `Standard`). Read once at process
/// startup by each binary to seed `LogConfig.level`; logging initializes before
/// settings load, so changing it takes effect on the next run.
pub fn load_log_level(config_dir: &std::path::Path) -> LogLevel {
    load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.log_level)
        .unwrap_or_default()
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

/// Resolve the OS-sandbox read denylist for worktree agents: the configured
/// `sandboxDenyRead` paths (with `~` expanded) if present, otherwise the
/// conservative built-in default. An empty configured list disables the
/// denylist (writes are still confined).
pub fn load_sandbox_deny_read(config_dir: &std::path::Path) -> Vec<PathBuf> {
    let configured = load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.sandbox_deny_read);
    match configured {
        Some(paths) => paths.iter().map(|p| expand_home(p)).collect(),
        None => crate::services::sandbox::default_deny_read(),
    }
}

/// Load the configured Managed Build Services, or the built-in default set when
/// none are configured. The supervisor decides which to actually launch (e.g.
/// the default sccache entry only runs when `sccache` is on `PATH`).
pub fn load_build_services(
    config_dir: &std::path::Path,
) -> HashMap<String, crate::config::build_services::BuildServiceConfig> {
    let configured = load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.build_services);
    match configured {
        Some(map) => map,
        None => {
            let mut defaults = HashMap::new();
            defaults.insert(
                "sccache".to_string(),
                crate::config::build_services::default_sccache_service(),
            );
            defaults
        }
    }
}

/// Load the per-project map of user-accepted fence-crossing commands from
/// workspace settings (`projectId -> [command, ...]`). Empty when unset.
///
/// A terminal command's `write` carveout (or coarse fence crossing) only takes
/// effect once the user has accepted that command here — so a repo can declare a
/// fence-crosser but cannot grant itself the crossing. See `config::dev_commands`.
pub fn load_accepted_fence_commands(config_dir: &std::path::Path) -> HashMap<String, Vec<String>> {
    load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.accepted_fence_commands)
        .unwrap_or_default()
}

/// Accept or revoke one project command as a fence-crosser, persisting the change
/// to `acceptedFenceCommands` in `settings.yaml`. Surgical: only that key is
/// touched. An empty list for a project is dropped from the map.
pub fn set_accepted_fence_command(
    config_dir: &std::path::Path,
    project_id: &str,
    command: &str,
    accepted: bool,
) -> Result<(), String> {
    let mut map = load_accepted_fence_commands(config_dir);
    let command = command.trim().to_string();
    let entry = map.entry(project_id.to_string()).or_default();
    entry.retain(|c| c != &command);
    if accepted && !command.is_empty() {
        entry.push(command);
    }
    if entry.is_empty() {
        map.remove(project_id);
    }
    write_accepted_fence_commands_map(config_dir, &map)
}

/// Persist the `enabled` flag for one build service into the `buildServices`
/// mapping of `settings.yaml`, materializing the built-in defaults into the file
/// first if it has no `buildServices` block yet (so a toggle of the default
/// sccache entry persists). Surgical: only the `buildServices` key is touched,
/// every other setting and the header comment are preserved.
pub fn set_build_service_enabled(
    config_dir: &std::path::Path,
    name: &str,
    enabled: bool,
) -> Result<(), String> {
    // Start from the effective map (configured or built-in default).
    let mut map = load_build_services(config_dir);
    let cfg = map
        .get_mut(name)
        .ok_or_else(|| format!("unknown build service: {name}"))?;
    cfg.enabled = enabled;
    write_build_services_map(config_dir, &map)
}

/// Insert or replace one build service. Starts from the effective map
/// (configured or built-in default) so adding a sibling preserves the default
/// sccache entry; writing materializes the whole map into the file.
pub fn upsert_build_service(
    config_dir: &std::path::Path,
    name: &str,
    config: &crate::config::build_services::BuildServiceConfig,
) -> Result<(), String> {
    let mut map = load_build_services(config_dir);
    map.insert(name.to_string(), config.clone());
    write_build_services_map(config_dir, &map)
}

/// Remove one build service by name. Writes the remaining map verbatim — an
/// empty result persists as `buildServices: {}` (explicitly no services),
/// distinct from an absent block (which yields the built-in default).
pub fn delete_build_service(config_dir: &std::path::Path, name: &str) -> Result<(), String> {
    let mut map = load_build_services(config_dir);
    map.remove(name);
    write_build_services_map(config_dir, &map)
}

/// Surgically write the `buildServices` mapping into `settings.yaml`, leaving
/// every other key and the header comment intact.
fn write_build_services_map(
    config_dir: &std::path::Path,
    map: &HashMap<String, crate::config::build_services::BuildServiceConfig>,
) -> Result<(), String> {
    let value = serde_yaml::to_value(map)
        .map_err(|e| format!("Failed to serialize build services: {e}"))?;
    write_settings_key(config_dir, "buildServices", value)
}

/// Surgically write the `acceptedFenceCommands` mapping into `settings.yaml`.
fn write_accepted_fence_commands_map(
    config_dir: &std::path::Path,
    map: &HashMap<String, Vec<String>>,
) -> Result<(), String> {
    let value = serde_yaml::to_value(map)
        .map_err(|e| format!("Failed to serialize accepted fence commands: {e}"))?;
    write_settings_key(config_dir, "acceptedFenceCommands", value)
}

/// Surgically write one top-level `key` into `settings.yaml`, leaving every other
/// key and the header comment intact. The canonical UI write path for config-only
/// settings (build services, fence acceptance) that the worktree fence blocks
/// agents from editing directly.
fn write_settings_key(
    config_dir: &std::path::Path,
    key: &str,
    value: serde_yaml::Value,
) -> Result<(), String> {
    let path = get_settings_path(config_dir);
    let mut root = match std::fs::read_to_string(&path) {
        Ok(content) => match serde_yaml::from_str::<serde_yaml::Value>(&content)
            .map_err(|e| format!("Failed to parse settings file: {e}"))?
        {
            serde_yaml::Value::Mapping(m) => m,
            serde_yaml::Value::Null => serde_yaml::Mapping::new(),
            _ => return Err("settings file root is not a mapping".to_string()),
        },
        Err(_) => serde_yaml::Mapping::new(),
    };
    root.insert(serde_yaml::Value::String(key.to_string()), value);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config directory: {e}"))?;
    }
    let yaml =
        serde_yaml::to_string(&root).map_err(|e| format!("Failed to serialize settings: {e}"))?;
    std::fs::write(&path, format!("# Cairn Workspace Settings\n{yaml}"))
        .map_err(|e| format!("Failed to write settings file: {e}"))?;
    super::commit_and_maybe_push(
        std::slice::from_ref(&path),
        "cairn: update settings",
        Some(config_dir),
    );
    Ok(())
}

/// Resolve the template variables for build-service config expansion.
///
/// `{worktrees}` is always `~/.cairn/worktrees` (the canonical worktree root),
/// independent of the dev/prod `config_dir`, because worktrees live there in
/// both modes.
pub fn build_service_templates(
    config_dir: &std::path::Path,
    worktree: Option<std::path::PathBuf>,
) -> crate::config::build_services::Templates {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/"));
    let worktrees = home.join(".cairn").join("worktrees");
    crate::config::build_services::Templates {
        home,
        cairn_home: config_dir.to_path_buf(),
        worktrees,
        worktree,
    }
}

/// Expand a leading `~` / `~/` to the user's home directory.
fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    if path == "~" {
        if let Some(home) = dirs::home_dir() {
            return home;
        }
    }
    PathBuf::from(path)
}

/// Load the raw settings file
pub(crate) fn load_settings_file(config_dir: &std::path::Path) -> Result<SettingsFile, String> {
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

    let mut file = SettingsFile::from_settings(settings);

    // Preserve the external MCP server registry: it is config-only (edited in
    // YAML, not exposed through the Settings DTO), so carry the on-disk value
    // forward rather than dropping it on every settings save.
    file.mcp_servers = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.mcp_servers);

    // The sandbox read denylist is likewise config-only (YAML, not in the DTO):
    // carry the on-disk value forward rather than dropping it on every save.
    file.sandbox_deny_read = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.sandbox_deny_read);

    // Managed Build Services are config-only too: preserve the on-disk value.
    file.build_services = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.build_services);

    // Fence-command acceptance is config-only too: preserve the on-disk value.
    file.accepted_fence_commands = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.accepted_fence_commands);

    // The typed web-fetch registry and its active selector are config-only
    // too: carry the on-disk values forward rather than dropping them on save.
    file.web_fetch = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.web_fetch);
    file.active_web_fetch = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.active_web_fetch);

    // The typed web-search registry and its active selector are config-only too:
    // carry the on-disk values forward rather than dropping them on save.
    file.web_search = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.web_search);
    file.active_web_search = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.active_web_search);

    // The typed PDF registry and its active selector are config-only too:
    // carry the on-disk values forward rather than dropping them on save.
    file.pdf = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.pdf);
    file.active_pdf = load_settings_file(config_dir)
        .ok()
        .and_then(|existing| existing.active_pdf);

    // Add header comment
    let yaml =
        serde_yaml::to_string(&file).map_err(|e| format!("Failed to serialize settings: {}", e))?;
    let content = format!("# Cairn Workspace Settings\n{}", yaml);

    std::fs::write(&path, content).map_err(|e| format!("Failed to write settings file: {}", e))?;
    super::commit_and_maybe_push(
        std::slice::from_ref(&path),
        "cairn: update settings",
        Some(config_dir),
    );
    Ok(())
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

    fn git_init(path: &std::path::Path) {
        assert!(crate::env::git()
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_bare(path: &std::path::Path) {
        assert!(crate::env::git()
            .args(["init", "--bare", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_set_origin(repo: &std::path::Path, origin: &std::path::Path) {
        assert!(crate::env::git()
            .args(["remote", "add", "origin"])
            .arg(origin)
            .current_dir(repo)
            .status()
            .unwrap()
            .success());
    }

    fn git_status(path: &std::path::Path) -> String {
        let out = crate::env::git()
            .args(["status", "--porcelain"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_head_subject(path: &std::path::Path) -> String {
        let out = crate::env::git()
            .args(["log", "-1", "--pretty=%s"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_rev(path: &std::path::Path, rev: &str) -> String {
        let out = crate::env::git()
            .args(["rev-parse", rev])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_branch(path: &std::path::Path) -> String {
        let out = crate::env::git()
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn save_settings_commits_and_pushes_with_origin() {
        let temp = TempDir::new().unwrap();
        let origin = temp.path().join("origin.git");
        std::fs::create_dir_all(&origin).unwrap();
        git_bare(&origin);
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        git_init(&home);
        git_set_origin(&home, &origin);

        save_settings(&home, &test_settings()).unwrap();

        assert!(
            git_status(&home).is_empty(),
            "settings save left repo dirty"
        );
        assert_eq!(git_head_subject(&home), "cairn: update settings");

        // The writer's push is fire-and-forget; drive it synchronously to assert
        // the workspace remote advanced.
        crate::config::push_workspace_repo_best_effort(&home);
        let branch = git_branch(&home);
        assert_eq!(
            git_rev(&origin, &format!("refs/heads/{branch}")),
            git_rev(&home, "HEAD"),
            "workspace origin should advance to the settings commit"
        );
    }

    #[test]
    fn save_settings_commits_without_remote() {
        let temp = TempDir::new().unwrap();
        let home = temp.path();
        git_init(home);
        save_settings(home, &test_settings()).unwrap();
        assert!(
            git_status(home).is_empty(),
            "no-remote save left repo dirty"
        );
        assert_eq!(git_head_subject(home), "cairn: update settings");
    }

    #[test]
    fn write_settings_key_commits_scoped() {
        let temp = TempDir::new().unwrap();
        let home = temp.path();
        git_init(home);
        // Build-service / fence saves funnel through write_settings_key.
        std::fs::write(home.join("unrelated.txt"), "dirty").unwrap();

        write_settings_key(home, "buildServices", serde_yaml::Value::Null).unwrap();

        assert_eq!(git_head_subject(home), "cairn: update settings");
        let status = git_status(home);
        assert!(
            status.contains("unrelated.txt"),
            "unrelated stays dirty: {status:?}"
        );
        assert!(
            !status.contains("settings.yaml"),
            "settings.yaml committed: {status:?}"
        );
    }

    #[test]
    fn test_settings_file_defaults() {
        let file = SettingsFile::default();
        let settings = file.to_settings();

        // Preset defaults
        assert_eq!(settings.active_backend, "claude");
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
        assert_eq!(settings.orphan_cleanup_days, 3);
        assert_eq!(settings.repo_target_sweep_days, 0);
        assert!(settings.auto_start_jobs); // Always true
        assert_eq!(settings.thinking_display_mode, ThinkingDisplayMode::Full);
        assert_eq!(settings.external_replies, ExternalReplyMode::Watchers);
    }

    #[test]
    fn test_external_replies_yaml_roundtrips_disabled() {
        let yaml = r#"
externalReplies: disabled
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        assert_eq!(settings.external_replies, ExternalReplyMode::Disabled);

        let serialized = serde_yaml::to_string(&SettingsFile::from_settings(&settings)).unwrap();
        assert!(serialized.contains("externalReplies: disabled"));
    }

    #[test]
    fn test_settings_roundtrip() {
        let settings = Settings {
            branch_prefix: "feature".to_string(),
            max_thinking_tokens: Some(16000),
            merge_type: MergeType::Rebase,
            pull_on_merge: false,
            repo_target_sweep_days: 14,
            ..test_settings()
        };

        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();

        assert_eq!(restored.active_backend, settings.active_backend);
        assert_eq!(restored.backends, settings.backends);
        assert_eq!(restored.branch_prefix, settings.branch_prefix);
        assert_eq!(restored.max_thinking_tokens, settings.max_thinking_tokens);
        assert_eq!(restored.merge_type, settings.merge_type);
        assert_eq!(restored.pull_on_merge, settings.pull_on_merge);
        assert_eq!(restored.repo_target_sweep_days, 14);
        assert!(restored.auto_start_jobs); // Always true
        assert_eq!(
            restored.thinking_display_mode,
            settings.thinking_display_mode
        );
    }

    #[test]
    fn test_subscription_fees_roundtrip() {
        let mut fees = HashMap::new();
        fees.insert("claude".to_string(), 200.0);
        fees.insert("codex".to_string(), 200.0);
        let settings = Settings {
            subscription_fees: fees.clone(),
            ..test_settings()
        };

        // DTO -> file -> DTO survives.
        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();
        assert_eq!(restored.subscription_fees, fees);

        // YAML load/save survives, and an empty map serializes to absent.
        let yaml = serde_yaml::to_string(&file).unwrap();
        assert!(yaml.contains("subscriptionFees"));
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.to_settings().subscription_fees, fees);

        let empty = SettingsFile::from_settings(&test_settings());
        assert!(empty.subscription_fees.is_none());
        let empty_yaml = serde_yaml::to_string(&empty).unwrap();
        assert!(!empty_yaml.contains("subscriptionFees"));
    }

    #[test]
    fn test_openrouter_routing_roundtrip() {
        use crate::models::{OpenRouterRouting, OpenRouterSort};

        let routing = OpenRouterRouting {
            zero_data_retention: true,
            sort: Some(OpenRouterSort::Throughput),
        };
        let settings = Settings {
            openrouter_routing: routing.clone(),
            ..test_settings()
        };

        // DTO -> file -> DTO survives.
        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();
        assert_eq!(restored.openrouter_routing, routing);

        // YAML load/save survives, and an all-default routing serializes to absent.
        let yaml = serde_yaml::to_string(&file).unwrap();
        assert!(yaml.contains("openrouterRouting"));
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.to_settings().openrouter_routing, routing);

        let empty = SettingsFile::from_settings(&test_settings());
        assert!(empty.openrouter_routing.is_none());
        let empty_yaml = serde_yaml::to_string(&empty).unwrap();
        assert!(!empty_yaml.contains("openrouterRouting"));
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
            orphan_cleanup_days: Some(3),
            repo_target_sweep_days: Some(14),
            bug_reports: Some(true),
            ..Default::default()
        };

        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.preferred_models, file.preferred_models);
        assert_eq!(parsed.branch_prefix, file.branch_prefix);
        assert_eq!(parsed.max_thinking_tokens, file.max_thinking_tokens);
        assert_eq!(parsed.repo_target_sweep_days, Some(14));
    }

    #[test]
    fn repo_target_sweep_days_roundtrips_and_clamps_to_zero() {
        let yaml = "repoTargetSweepDays: 14\n";
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        assert_eq!(settings.repo_target_sweep_days, 14);

        let serialized = serde_yaml::to_string(&SettingsFile::from_settings(&settings)).unwrap();
        assert!(serialized.contains("repoTargetSweepDays: 14"));

        let negative: SettingsFile = serde_yaml::from_str("repoTargetSweepDays: -7\n").unwrap();
        assert_eq!(negative.to_settings().repo_target_sweep_days, 0);
        assert_eq!(
            SettingsFile::default().to_settings().repo_target_sweep_days,
            0
        );
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
    fn accepted_fence_commands_set_load_and_persist() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        std::fs::write(get_settings_path(dir), "branchPrefix: agent\n").unwrap();

        // Accept a command for a project.
        set_accepted_fence_command(dir, "proj-1", "bun run dev:instance", true).unwrap();
        assert_eq!(
            load_accepted_fence_commands(dir)
                .get("proj-1")
                .map(Vec::as_slice),
            Some(&["bun run dev:instance".to_string()][..])
        );

        // Accepting again is idempotent (no duplicate).
        set_accepted_fence_command(dir, "proj-1", "bun run dev:instance", true).unwrap();
        assert_eq!(load_accepted_fence_commands(dir)["proj-1"].len(), 1);

        // A normal settings save preserves the acceptance map.
        let settings = load_settings(dir);
        save_settings(dir, &settings).unwrap();
        assert!(load_accepted_fence_commands(dir).contains_key("proj-1"));

        // Revoke drops the now-empty project entry entirely.
        set_accepted_fence_command(dir, "proj-1", "bun run dev:instance", false).unwrap();
        assert!(load_accepted_fence_commands(dir).is_empty());
    }

    #[test]
    fn build_services_parse_and_persist_across_save() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        let yaml = r#"branchPrefix: agent
buildServices:
  sccache:
    enabled: true
    start: ["sccache", "--start-server"]
    ready:
      tcp: "127.0.0.1:9999"
    write:
      - "{worktrees}/**/build-cache/**"
"#;
        std::fs::write(get_settings_path(dir), yaml).unwrap();

        // The configured entry is returned (a glob distinct from the built-in
        // default proves the configured value — not the fallback — came through).
        let services = load_build_services(dir);
        assert!(services.contains_key("sccache"));
        assert!(services["sccache"].enabled);
        assert_eq!(
            services["sccache"].write,
            vec!["{worktrees}/**/build-cache/**"]
        );
        assert_eq!(
            services["sccache"]
                .ready
                .as_ref()
                .and_then(|r| r.tcp.as_deref()),
            Some("127.0.0.1:9999")
        );

        // Saving settings (which never touches buildServices) preserves it.
        let settings = load_settings(dir);
        save_settings(dir, &settings).unwrap();
        let after = load_build_services(dir);
        assert!(after.contains_key("sccache"));
        assert_eq!(
            after["sccache"].write,
            vec!["{worktrees}/**/build-cache/**"]
        );
    }

    #[test]
    fn set_build_service_enabled_materializes_default_and_preserves_other_settings() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        std::fs::write(get_settings_path(dir), "branchPrefix: custom\n").unwrap();

        // No buildServices block yet: toggling the default entry materializes it.
        set_build_service_enabled(dir, "sccache", false).unwrap();
        assert!(!load_build_services(dir)["sccache"].enabled);
        assert!(
            load_settings_file(dir).unwrap().build_services.is_some(),
            "toggle must write an explicit buildServices block"
        );
        // Unrelated settings survive the surgical write.
        assert_eq!(load_settings(dir).branch_prefix, "custom");

        // Toggle back on; unknown service errors.
        set_build_service_enabled(dir, "sccache", true).unwrap();
        assert!(load_build_services(dir)["sccache"].enabled);
        assert!(set_build_service_enabled(dir, "nope", true).is_err());
    }

    #[test]
    fn upsert_and_delete_build_service_round_trip() {
        use crate::config::build_services::{BuildServiceConfig, ReadyProbe};
        let temp = TempDir::new().unwrap();
        let dir = temp.path();

        let mut env = HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        let cfg = BuildServiceConfig {
            enabled: true,
            start: vec!["mycache".into(), "--serve".into()],
            ready: Some(ReadyProbe::tcp("127.0.0.1:5000")),
            state_dir: Some("{cairnHome}/mycache".into()),
            write: vec!["{worktrees}/**/out/**".into()],
            env,
        };
        upsert_build_service(dir, "mycache", &cfg).unwrap();

        let map = load_build_services(dir);
        // The new service is present AND the built-in default sccache survives.
        assert!(map.contains_key("mycache"));
        assert!(map.contains_key("sccache"));
        assert_eq!(map["mycache"].start, vec!["mycache", "--serve"]);
        assert_eq!(
            map["mycache"].env.get("FOO").map(String::as_str),
            Some("bar")
        );
        assert_eq!(
            map["mycache"].ready.as_ref().and_then(|r| r.tcp.as_deref()),
            Some("127.0.0.1:5000")
        );

        delete_build_service(dir, "mycache").unwrap();
        let after = load_build_services(dir);
        assert!(!after.contains_key("mycache"));
        assert!(after.contains_key("sccache"));
    }

    #[test]
    fn build_services_defaults_to_sccache_when_unset() {
        let temp = TempDir::new().unwrap();
        // No settings file: the built-in default sccache entry is synthesized.
        let services = load_build_services(temp.path());
        assert!(services.contains_key("sccache"));
        assert!(services["sccache"].enabled);
    }

    #[test]
    fn web_fetch_pdf_and_active_selectors_persist_across_save() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        let yaml = r#"branchPrefix: agent
webFetch:
  firecrawl:
    onlyMainContent: false
activeWebFetch: jina
activePdf: bmd
"#;
        std::fs::write(get_settings_path(dir), yaml).unwrap();

        // The config-only fields parse off the file.
        let file = load_settings_file(dir).unwrap();
        assert!(file.web_fetch.as_ref().unwrap().contains_key("firecrawl"));
        assert_eq!(file.active_web_fetch.as_deref(), Some("jina"));
        assert_eq!(file.active_pdf.as_deref(), Some("bmd"));

        // Saving settings (which never touches these) carries them forward.
        let settings = load_settings(dir);
        save_settings(dir, &settings).unwrap();
        let after = load_settings_file(dir).unwrap();
        assert!(after.web_fetch.as_ref().unwrap().contains_key("firecrawl"));
        assert_eq!(after.active_web_fetch.as_deref(), Some("jina"));
        assert_eq!(after.active_pdf.as_deref(), Some("bmd"));
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
    }

    #[test]
    fn test_legacy_default_model_honored_in_migration() {
        // Old format with defaultModel: opus patches opus into its natural tier.
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
      options:
        reasoningEffort: high
    lg:
      model: opus
      options:
        reasoningEffort: high
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(
            settings.backends["claude"]["lg"].model,
            Model::new(Model::OPUS)
        );
    }

    #[test]
    fn test_missing_default_backend_is_backfilled() {
        // An older settings.yaml that names only claude/codex must still surface
        // every known backend (e.g. openrouter) so the provider list is driven
        // by code, not by what the file happened to persist.
        let yaml = r#"
activeBackend: claude
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
    lg:
      model: opus
  codex:
    sm:
      model: gpt-5.4-mini
    md:
      model: gpt-5.3-codex
    lg:
      model: gpt-5.5
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        // The named backends keep their on-disk customizations.
        assert_eq!(
            settings.backends["claude"]["lg"].model,
            Model::new(Model::OPUS)
        );
        assert_eq!(
            settings.backends["codex"]["md"].model.as_str(),
            "gpt-5.3-codex"
        );
        // The unnamed-but-known backend is backfilled from defaults.
        assert!(
            settings.backends.contains_key("openrouter"),
            "openrouter must be present even though settings.yaml omits it"
        );
        assert_eq!(settings.backends["openrouter"].len(), 3);
    }

    #[test]
    fn test_presets_roundtrip() {
        // New format with backends roundtrips correctly
        let yaml = r#"
activeBackend: codex
tiers:
  - sm
  - md
  - lg
backends:
  codex:
    sm:
      model: gpt-5.4-mini
      options:
        reasoningEffort: low
    md:
      model: gpt-5.3-codex
      options:
        reasoningEffort: medium
    lg:
      model: gpt-5.5
      options:
        reasoningEffort: high
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(settings.active_backend, "codex");
        assert_eq!(
            settings.backends["codex"]["md"].model.as_str(),
            "gpt-5.3-codex"
        );

        // Roundtrip
        let file2 = SettingsFile::from_settings(&settings);
        let restored = file2.to_settings();
        assert_eq!(restored.active_backend, "codex");
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
    fn test_yaml_thinking_mode() {
        let yaml = r#"
thinkingDisplayMode: full
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(file.thinking_display_mode, Some(ThinkingDisplayMode::Full));

        let settings = file.to_settings();
        assert_eq!(settings.thinking_display_mode, ThinkingDisplayMode::Full);
    }

    #[test]
    fn test_log_level_defaults_to_standard_and_roundtrips() {
        // Absent in YAML → the light Standard default.
        let file: SettingsFile = serde_yaml::from_str("branchPrefix: custom\n").unwrap();
        assert_eq!(file.log_level, None);
        assert_eq!(file.to_settings().log_level, LogLevel::Standard);

        // Explicit value parses and survives the DTO round-trip.
        let file: SettingsFile = serde_yaml::from_str("logLevel: verbose\n").unwrap();
        assert_eq!(file.log_level, Some(LogLevel::Verbose));
        let settings = file.to_settings();
        assert_eq!(settings.log_level, LogLevel::Verbose);
        let restored = SettingsFile::from_settings(&settings).to_settings();
        assert_eq!(restored.log_level, LogLevel::Verbose);
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
            !yaml.contains("defaultTier"),
            "defaultTier should not be serialized"
        );
        assert!(yaml.contains("backends"), "backends should be serialized");
        assert!(yaml.contains("tiers"), "tiers should be serialized");
    }

    #[test]
    fn test_settings_preset_roundtrip_preserves_backends() {
        // Build settings with custom backends and verify they survive roundtrip
        let mut settings = test_settings();
        settings.active_backend = "codex".to_string();

        let file = SettingsFile::from_settings(&settings);
        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();
        let restored = parsed.to_settings();

        assert_eq!(restored.active_backend, "codex");
        assert!(restored.backends.contains_key("claude"));
        assert!(restored.backends.contains_key("codex"));
    }
}
