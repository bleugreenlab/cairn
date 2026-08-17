//! File-based workspace settings.
//!
//! Settings are stored in `~/.cairn/settings.yaml` and are the source of truth.
//! The database is no longer used for workspace settings.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use cairn_common::logging::LogLevel;

use crate::config::presets::{default_presets_config, PresetsConfig};
use crate::models::{
    ChannelsConfig, ExternalReplyMode, MergeType, Model, OpenRouterRouting, Preset, Settings,
    ThinkingDisplayMode, TranscriptDensity, TranscriptTextSize, UpdateSettings,
};

/// Where a warm thread rebuilds by default: four fifths of the model's context
/// window. High enough that an ordinary working session never reaches it, low
/// enough to leave room for the turn that discovers it — the seed, the reply,
/// and the tool results it pulls in all land after the reading that triggered.
pub(crate) const DEFAULT_THREAD_COMPACT_THRESHOLD: f64 = 0.8;

/// Keep the threshold inside the range where it means something.
///
/// At or below zero every warm turn would rebuild, which is the failure this
/// setting exists to prevent; above one it can never fire and the window fills
/// until the provider refuses the request. A NaN from hand-edited YAML reads as
/// "unset" rather than poisoning every comparison it touches.
pub(crate) fn clamp_compact_threshold(value: f64) -> f64 {
    if !value.is_finite() {
        return DEFAULT_THREAD_COMPACT_THRESHOLD;
    }

    value.clamp(0.1, 1.0)
}

/// Read the persisted voice opt-in. Missing settings are deliberately disabled.
pub fn load_voice_preferences(config_dir: &std::path::Path) -> crate::voice::VoicePreferences {
    load_settings_file(config_dir)
        .ok()
        .and_then(|file| file.voice)
        .unwrap_or_default()
}

/// Surgically persist voice intent without rewriting unrelated settings.
pub fn set_voice_preferences(
    config_dir: &std::path::Path,
    preferences: &crate::voice::VoicePreferences,
) -> Result<(), String> {
    mutate_workspace_settings(config_dir, "cairn: update voice settings", |root| {
        root.insert(
            serde_yaml::Value::String("voice".to_string()),
            serde_yaml::to_value(preferences)
                .map_err(|error| format!("Failed to serialize voice settings: {error}"))?,
        );
        Ok(())
    })
}

/// Number of days to retain JSONL logs. Invalid zero values heal to one day;
/// an absent setting uses the shared seven-day default.
pub fn load_log_retention_days(config_dir: &std::path::Path) -> u64 {
    load_settings_file(config_dir)
        .ok()
        .and_then(|file| file.log_retention_days)
        .unwrap_or(cairn_common::logging::DEFAULT_LOG_RETENTION_DAYS)
        .max(1)
}

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

/// Load the runner-owned fleet capability. An absent block is the disabled
/// default, including the canonical acquisition and execution timeouts.
pub fn load_fleet(config_dir: &std::path::Path) -> crate::fleet::FleetConfig {
    load_settings_file(config_dir)
        .ok()
        .and_then(|file| file.fleet)
        .unwrap_or_default()
        .healed()
}

/// Replace the runner-owned fleet capability in `settings.yaml` without
/// routing it through the general Settings DTO. Only `buildSlots` is touched.
pub fn set_fleet(
    config_dir: &std::path::Path,
    config: &crate::fleet::FleetConfig,
) -> Result<(), String> {
    config.validate()?;
    mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        root.insert(
            serde_yaml::Value::String("buildSlots".to_string()),
            serde_yaml::to_value(config)
                .map_err(|error| format!("Failed to serialize build slots: {error}"))?,
        );
        Ok(())
    })
}

/// Settings as stored in YAML file.
/// All fields are optional - missing fields use defaults.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SettingsFile {
    /// User intent only. Installed voice components are discovered and verified
    /// from the managed voice manifest rather than trusted from settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) voice: Option<crate::voice::VoicePreferences>,
    // === Preset fields (new) ===
    /// Tier name -> the backend that serves it by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tier_defaults: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tiers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backends: Option<HashMap<String, HashMap<String, Preset>>>,
    /// The providers this workspace has installed.
    ///
    /// Absent means the workspace predates installable providers, and is read
    /// as "everything Cairn ships" — exactly the behavior it had — until
    /// [`migrate_enabled_providers`] narrows it to what the workspace actually
    /// depends on and writes the result here. Once written, this is the durable
    /// answer: enablement is configuration the user owns, not something
    /// re-derived from whichever credentials happen to exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled_providers: Option<Vec<String>>,

    /// External MCP servers reachable through the `cairn://mcp/...` gateway.
    /// Keyed by server name. Project config overlays this set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) mcp_servers: Option<HashMap<String, crate::config::mcp_servers::McpServerConfig>>,

    /// Legacy global default provider. Read only, and dropped from the file on
    /// the next save: it meant "every tier resolves against this one backend",
    /// which `tier_defaults` now says per tier.
    #[serde(default, skip_serializing)]
    active_backend: Option<String>,

    // === Legacy model fields (kept for deserialization, skipped on serialize) ===
    #[serde(default, skip_serializing)]
    default_model: Option<Model>,
    #[serde(default, skip_serializing)]
    preferred_models: Option<Vec<Model>>,

    /// Double Option to distinguish:
    /// - None = field missing → default to Some(31999)
    /// - Some(None) = field set to null → disabled
    /// - Some(Some(n)) = field set to number → enabled with n tokens
    #[serde(default, deserialize_with = "deserialize_max_thinking_tokens")]
    max_thinking_tokens: Option<Option<i32>>,
    #[serde(default)]
    merge_type: Option<MergeType>,
    /// Deprecated — always true. Kept for deserialization compat (silently ignored).
    #[serde(default, skip_serializing)]
    #[allow(dead_code)]
    auto_start_jobs: Option<bool>,
    #[serde(default)]
    orphan_cleanup_days: Option<i32>,
    #[serde(default)]
    repo_target_sweep_days: Option<i32>,
    /// Whether agent bug reports are enabled (default: true)
    #[serde(default)]
    bug_reports: Option<bool>,
    /// Thinking block display mode in chat transcripts
    #[serde(default)]
    thinking_display_mode: Option<ThinkingDisplayMode>,
    /// Base text scale for transcript markdown.
    #[serde(default)]
    transcript_text_size: Option<TranscriptTextSize>,
    /// Vertical rhythm preset for transcript markdown.
    #[serde(default)]
    transcript_density: Option<TranscriptDensity>,
    /// File-log verbosity level. Absent = the light `Standard` default; `verbose`
    /// is the opt-in full-debug + profiler level (today's behavior).
    #[serde(default)]
    log_level: Option<LogLevel>,
    /// Age-based retention for shared JSONL logs.
    #[serde(default)]
    log_retention_days: Option<u64>,
    /// Whether end-of-job memory review prompts are enabled.
    #[serde(default)]
    memory_review_enabled: Option<bool>,
    /// Whether automatic memory-triage issue creation is enabled.
    #[serde(default)]
    memory_triage_enabled: Option<bool>,
    /// Maximum open memory-triage issues for an exact scope.
    #[serde(default)]
    max_open_triage_issues_per_scope: Option<i32>,
    /// Number of exact-scope pending memories that triggers a memory-triage issue.
    #[serde(default)]
    pending_memory_threshold: Option<i32>,
    /// Fraction of the model's context window at which a warm thread session rebuilds.
    #[serde(default)]
    thread_compact_threshold: Option<f64>,
    /// How replies to the special `to: "external"` target are handled.
    #[serde(default)]
    external_replies: Option<ExternalReplyMode>,
    /// Sensitive paths the OS sandbox hard-denies reads of from executor cells.
    /// `~` is expanded to the user's home. Absent = the conservative built-in
    /// default (cloud cred stores, ssh/gpg keys, `~/.cairn[-dev]`). See
    /// `docs/worktree-fence.md`.
    #[serde(default)]
    sandbox_deny_read: Option<Vec<String>>,
    /// Extra case-insensitive names redacted from browser network headers,
    /// query parameters, JSON fields, and form fields. The built-in sensitive
    /// names always apply; this config-only list can only add to them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    browser_network_sensitive_names: Option<Vec<String>>,
    /// Managed Build Services: optional user-configured shared daemons that run
    /// under a service sandbox and inject client env into eligible spawns.
    /// Config-only (YAML, not in the Settings DTO). Absent means no services. See
    /// `docs/worktree-fence.md` — Managed Build Services.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    build_services: Option<HashMap<String, crate::config::build_services::BuildServiceConfig>>,
    /// Runner-owned persistent fleet capability. Project configuration may
    /// request slot routing, but only this workspace-owned section grants it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "buildSlots")]
    pub(crate) fleet: Option<crate::fleet::FleetConfig>,
    /// Typed web-fetch provider options, keyed by provider id
    /// (`bmd`/`jina`/`firecrawl`) then option key. Config-only (YAML, not in the
    /// Settings DTO). Validated against the per-provider descriptor in
    /// `crate::config::web_fetch`. See `docs/settings.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) web_fetch: Option<HashMap<String, HashMap<String, serde_yaml::Value>>>,
    /// Which web-fetch provider backs fetch. Config-only (YAML, not in the
    /// Settings DTO). Absent / `regular` = the built-in plain-HTTP fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) active_web_fetch: Option<String>,
    /// Typed web-search provider options, keyed by provider id
    /// (`tavily`/`exa`/`brave`/`jina`) then option key. Config-only (YAML, not
    /// in the Settings DTO). Validated against the per-provider descriptor in
    /// `crate::config::web_search`. See `docs/settings.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) web_search: Option<HashMap<String, HashMap<String, serde_yaml::Value>>>,
    /// Which typed web-search provider backs `cairn://websearch`. Config-only
    /// (YAML, not in the Settings DTO). Absent = web search is unconfigured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) active_web_search: Option<String>,
    /// Typed PDF-extraction provider options, keyed by provider id
    /// (`local`/`bmd`) then option key. Config-only (YAML, not in the Settings
    /// DTO). Validated against `crate::config::pdf`. See `docs/settings.md`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) pdf: Option<HashMap<String, HashMap<String, serde_yaml::Value>>>,
    /// Which PDF-extraction provider backs `.pdf` reads. Config-only (YAML, not
    /// in the Settings DTO). Absent / `local` = the built-in local extractor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) active_pdf: Option<String>,
    /// Flat monthly subscription fee per backend, in USD. A backend absent from
    /// this map is treated as metered (pay-as-you-go, no normalization). e.g.
    /// `{"claude": 200.0, "codex": 200.0}`. Drives effective-cost analytics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    subscription_fees: Option<HashMap<String, f64>>,
    /// OpenRouter provider-routing controls (ZDR + sort). Absent = OpenRouter's
    /// normal routing; omitted from YAML when all-default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    openrouter_routing: Option<OpenRouterRouting>,
    /// Opt-in: route tier-based ephemeral calls through OpenRouter's in-process
    /// HTTP loop instead of the native backend. Absent/false = native routing;
    /// omitted from YAML when unset.
    #[serde(
        default,
        rename = "routeCallsViaOpenRouter",
        skip_serializing_if = "Option::is_none"
    )]
    route_calls_via_openrouter: Option<bool>,
    /// Workspace-owned external delivery policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    channels: Option<ChannelsConfig>,
    /// Exact per-project terminal commands the user has accepted to run outside
    /// the worktree fence (`projectId -> [command, ...]`). Acceptance is
    /// user-owned, so a cloned repository can declare a shortcut but cannot grant
    /// it host access. Config-only (YAML, not in the Settings DTO); preserved
    /// across saves. See `crate::config::dev_commands`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    accepted_fence_commands: Option<HashMap<String, Vec<String>>>,
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
    /// The per-tier default backends this file describes, narrowed to `tiers`.
    ///
    /// Precedence is explicit `tierDefaults`, then the legacy global
    /// `activeBackend` expanded across every tier (which is what it meant), then
    /// Claude for every tier. Entries naming a tier that no longer exists are
    /// dropped rather than persisted, so a deleted tier leaves nothing behind.
    fn resolved_tier_defaults(&self, tiers: &[String]) -> HashMap<String, String> {
        if let Some(configured) = &self.tier_defaults {
            return tiers
                .iter()
                .filter_map(|tier| {
                    configured
                        .get(tier)
                        .map(|backend| (tier.clone(), backend.clone()))
                })
                .collect();
        }
        let legacy = self.active_backend.as_deref().unwrap_or("claude");
        crate::config::presets::tier_defaults_from_single_backend(legacy, tiers)
    }

    /// The tier list this file describes, defaulted when it names none.
    fn resolved_tiers(&self) -> Vec<String> {
        self.tiers.clone().unwrap_or_else(|| {
            crate::config::presets::DEFAULT_TIERS
                .iter()
                .map(|s| s.to_string())
                .collect()
        })
    }

    /// The providers a workspace that predates installable providers
    /// demonstrably depends on, given the backends its stored accounts can
    /// serve.
    ///
    /// Every form of dependence counts, because each one is a place the user
    /// already told Cairn this provider matters: a tier that defaults to it, a
    /// configured account or host, call routing pointed through it, a
    /// subscription fee recorded against it. Credentials are one signal among
    /// these rather than the rule — this runs exactly once and its answer
    /// becomes durable configuration, so a credential that later expires or is
    /// deleted leaves the provider installed and visible.
    fn depended_on_providers(&self, credentialed: &[String]) -> Vec<String> {
        let mut evidence: Vec<String> = self
            .resolved_tier_defaults(&self.resolved_tiers())
            .into_values()
            .collect();
        evidence.extend(credentialed.iter().cloned());
        let routes_calls = self.route_calls_via_openrouter.unwrap_or(false)
            || self
                .openrouter_routing
                .as_ref()
                .is_some_and(|routing| *routing != OpenRouterRouting::default());
        if routes_calls {
            evidence.push("openrouter".to_string());
        }
        if let Some(fees) = &self.subscription_fees {
            evidence.extend(
                fees.iter()
                    .filter(|(_, fee)| **fee > 0.0)
                    .map(|(backend, _)| backend.clone()),
            );
        }
        evidence.retain(|backend| crate::backends::catalog::is_supported(backend));
        if evidence.is_empty() {
            evidence.push(crate::backends::CLAUDE_FAMILY_BACKEND.to_string());
        }
        crate::backends::catalog::in_catalog_order(evidence)
    }

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
            let tiers = self.resolved_tiers();
            PresetsConfig {
                tier_defaults: self.resolved_tier_defaults(&tiers),
                tiers,
                backends: merged,
                // Carried on the Settings DTO instead; `load_effective_presets`
                // is where enablement joins a resolution config.
                enabled_providers: None,
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

            config.tier_defaults = self.resolved_tier_defaults(&config.tiers);
            config
        };

        Settings {
            tier_defaults: presets.tier_defaults,
            tiers: presets.tiers,
            backends: presets.backends,
            enabled_providers: match &self.enabled_providers {
                Some(enabled) => crate::backends::catalog::in_catalog_order(enabled),
                // Un-migrated: every shipped provider, which is the surface this
                // workspace already had. `migrate_enabled_providers` narrows it.
                None => crate::backends::catalog::supported_keys()
                    .map(str::to_string)
                    .collect(),
            },
            system_prompt: String::new(), // Deprecated, always empty
            max_thinking_tokens,
            merge_type: self.merge_type.clone().unwrap_or(MergeType::Squash),
            auto_start_jobs: true, // Always true — setting removed
            orphan_cleanup_days: self.orphan_cleanup_days.unwrap_or(3),
            repo_target_sweep_days: self.repo_target_sweep_days.unwrap_or(0).max(0),
            bug_reports: self.bug_reports.unwrap_or(true),
            thinking_display_mode: self
                .thinking_display_mode
                .clone()
                .unwrap_or(ThinkingDisplayMode::Full),
            transcript_text_size: self.transcript_text_size.unwrap_or_default(),
            transcript_density: self.transcript_density.unwrap_or_default(),
            memory_review_enabled: self.memory_review_enabled.unwrap_or(true),
            memory_triage_enabled: self
                .memory_triage_enabled
                .unwrap_or(self.memory_review_enabled.unwrap_or(true)),
            max_open_triage_issues_per_scope: self
                .max_open_triage_issues_per_scope
                .unwrap_or(1)
                .max(1),
            pending_memory_threshold: self.pending_memory_threshold.unwrap_or(5).max(1),
            thread_compact_threshold: clamp_compact_threshold(
                self.thread_compact_threshold
                    .unwrap_or(DEFAULT_THREAD_COMPACT_THRESHOLD),
            ),
            external_replies: self
                .external_replies
                .clone()
                .unwrap_or(ExternalReplyMode::Watchers),
            log_level: self.log_level.unwrap_or(LogLevel::Standard),
            log_retention_days: self
                .log_retention_days
                .unwrap_or(cairn_common::logging::DEFAULT_LOG_RETENTION_DAYS)
                .max(1),
            subscription_fees: self.subscription_fees.clone().unwrap_or_default(),
            openrouter_routing: self.openrouter_routing.clone().unwrap_or_default(),
            route_calls_via_openrouter: self.route_calls_via_openrouter.unwrap_or(false),
            channels: self.channels.clone().unwrap_or_default(),
        }
    }

    /// Create from Settings DTO
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            voice: None,
            tier_defaults: Some(settings.tier_defaults.clone()),
            active_backend: None,
            tiers: Some(settings.tiers.clone()),
            backends: Some(settings.backends.clone()),
            enabled_providers: Some(settings.enabled_providers.clone()),
            mcp_servers: None,
            default_model: None,
            preferred_models: None,
            max_thinking_tokens: Some(settings.max_thinking_tokens),
            merge_type: Some(settings.merge_type.clone()),
            auto_start_jobs: None, // No longer serialized
            orphan_cleanup_days: Some(settings.orphan_cleanup_days),
            repo_target_sweep_days: Some(settings.repo_target_sweep_days.max(0)),
            bug_reports: Some(settings.bug_reports),
            thinking_display_mode: Some(settings.thinking_display_mode.clone()),
            transcript_text_size: Some(settings.transcript_text_size),
            transcript_density: Some(settings.transcript_density),
            log_level: Some(settings.log_level),
            log_retention_days: Some(settings.log_retention_days.max(1)),
            memory_review_enabled: Some(settings.memory_review_enabled),
            memory_triage_enabled: Some(settings.memory_triage_enabled),
            max_open_triage_issues_per_scope: Some(
                settings.max_open_triage_issues_per_scope.max(1),
            ),
            pending_memory_threshold: Some(settings.pending_memory_threshold.max(1)),
            thread_compact_threshold: Some(clamp_compact_threshold(
                settings.thread_compact_threshold,
            )),
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
            // Omit when false so an all-default settings file stays clean.
            route_calls_via_openrouter: settings.route_calls_via_openrouter.then_some(true),
            channels: (settings.channels != ChannelsConfig::default())
                .then(|| settings.channels.clone()),
            // Config-only (YAML, not in the DTO); preserved across saves.
            sandbox_deny_read: None,
            browser_network_sensitive_names: None,
            build_services: None,
            fleet: None,
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
pub(crate) fn get_settings_path(config_dir: &std::path::Path) -> PathBuf {
    config_dir.join("settings.yaml")
}

/// Modification stamp of the settings file — `(mtime, len)`, or `None` when it
/// is absent or unreadable.
///
/// Caches that derive a value from settings key on this rather than subscribing
/// to in-process saves: the config directory is git-backed
/// (`commit_and_maybe_push`) and is also edited by hand, so a save hook would
/// miss the changes that arrive from outside this process. One `stat` is cheap
/// next to reading and YAML-parsing the whole file.
pub fn settings_file_stamp(config_dir: &std::path::Path) -> Option<(std::time::SystemTime, u64)> {
    let metadata = std::fs::metadata(get_settings_path(config_dir)).ok()?;
    Some((metadata.modified().ok()?, metadata.len()))
}

const WORKSPACE_HEADER: &str = "# Cairn Workspace Settings";
static WORKSPACE_SETTINGS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Mutate the workspace settings mapping as one serialized, atomic transaction.
///
/// The runner is the sole process owner of the Cairn home, so one process-wide
/// lock is the canonical concurrency boundary for all `settings.yaml` writers.
/// The current document is read exactly once while holding that lock. Missing or
/// YAML-null documents start empty; every other read or parse failure is fatal.
pub(crate) fn mutate_workspace_settings<T>(
    config_dir: &std::path::Path,
    commit_message: &str,
    mutate: impl FnOnce(&mut serde_yaml::Mapping) -> Result<T, String>,
) -> Result<T, String> {
    let lock = WORKSPACE_SETTINGS_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let path = get_settings_path(config_dir);

    let content = match std::fs::read_to_string(&path) {
        Ok(content) => Some(content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("Failed to read settings file: {error}")),
    };
    let mut root = match content.as_deref() {
        None => serde_yaml::Mapping::new(),
        Some(content) => match serde_yaml::from_str::<serde_yaml::Value>(content)
            .map_err(|error| format!("Failed to parse settings file: {error}"))?
        {
            serde_yaml::Value::Mapping(mapping) => mapping,
            serde_yaml::Value::Null => serde_yaml::Mapping::new(),
            _ => return Err("settings file root is not a mapping".to_string()),
        },
    };
    let original = root.clone();
    let result = mutate(&mut root)?;

    if root == original {
        return Ok(result);
    }

    std::fs::create_dir_all(config_dir)
        .map_err(|error| format!("Failed to create config directory: {error}"))?;
    let yaml = serde_yaml::to_string(&root)
        .map_err(|error| format!("Failed to serialize settings: {error}"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(config_dir)
        .map_err(|error| format!("Failed to create temporary settings file: {error}"))?;
    write!(temporary, "{WORKSPACE_HEADER}\n{yaml}")
        .map_err(|error| format!("Failed to write temporary settings file: {error}"))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| format!("Failed to sync temporary settings file: {error}"))?;
    temporary
        .persist(&path)
        .map_err(|error| format!("Failed to replace settings file: {}", error.error))?;

    super::commit_and_maybe_push(
        std::slice::from_ref(&path),
        commit_message,
        Some(config_dir),
    );
    Ok(result)
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

/// Resolve the OS-sandbox read denylist for executor cells: the configured
/// `sandboxDenyRead` paths (with `~` expanded) if present, otherwise the
/// conservative built-in default. An empty configured list disables the
/// denylist (writes are still confined).
pub fn load_browser_network_sensitive_names(config_dir: &std::path::Path) -> Vec<String> {
    load_settings_file(config_dir)
        .ok()
        .and_then(|file| file.browser_network_sensitive_names)
        .unwrap_or_default()
}

pub(crate) fn load_sandbox_deny_read(config_dir: &std::path::Path) -> Vec<PathBuf> {
    let configured = load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.sandbox_deny_read);
    match configured {
        Some(paths) => paths.iter().map(|p| expand_home(p)).collect(),
        None => crate::services::sandbox::default_deny_read(),
    }
}

/// Load user-configured Managed Build Services. Cairn has no built-in service:
/// dependency reuse for executor cells is owned by the Cargo dependency catalog.
pub(crate) fn load_build_services(
    config_dir: &std::path::Path,
) -> HashMap<String, crate::config::build_services::BuildServiceConfig> {
    load_settings_file(config_dir)
        .ok()
        .and_then(|file| file.build_services)
        .unwrap_or_default()
}

/// Load the per-project map of user-accepted fence-crossing commands from
/// workspace settings (`projectId -> [command, ...]`). Empty when unset.
///
/// Acceptance is the sole unfencing decision for an exact command that is also
/// declared as a terminal shortcut by that project. A repository can declare the
/// shortcut but cannot grant itself host access. See `config::dev_commands`.
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
    let command = command.trim().to_string();
    mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let key = serde_yaml::Value::String("acceptedFenceCommands".to_string());
        let mut map: HashMap<String, Vec<String>> = match root.get(&key).cloned() {
            None | Some(serde_yaml::Value::Null) => HashMap::new(),
            Some(value) => serde_yaml::from_value(value)
                .map_err(|error| format!("Failed to parse accepted fence commands: {error}"))?,
        };
        let entry = map.entry(project_id.to_string()).or_default();
        entry.retain(|existing| existing != &command);
        if accepted && !command.is_empty() {
            entry.push(command.clone());
        }
        if entry.is_empty() {
            map.remove(project_id);
        }
        root.insert(
            key,
            serde_yaml::to_value(map)
                .map_err(|error| format!("Failed to serialize accepted fence commands: {error}"))?,
        );
        Ok(())
    })
}

/// Persist the `enabled` flag for one build service into the `buildServices`
/// mapping of `settings.yaml`. Every service is user-configured — there are no
/// built-in entries — so toggling a name this workspace has not configured is an
/// error rather than a materialized default. Surgical: only the `buildServices`
/// key is touched, every other setting and the header comment are preserved.
pub fn set_build_service_enabled(
    config_dir: &std::path::Path,
    name: &str,
    enabled: bool,
) -> Result<(), String> {
    mutate_build_services(config_dir, |map| {
        let config = map
            .get_mut(name)
            .ok_or_else(|| format!("unknown build service: {name}"))?;
        config.enabled = enabled;
        Ok(())
    })
}

/// Insert or replace one build service, preserving the workspace's other
/// configured services; writing materializes the whole map into the file.
pub fn upsert_build_service(
    config_dir: &std::path::Path,
    name: &str,
    config: &crate::config::build_services::BuildServiceConfig,
) -> Result<(), String> {
    mutate_build_services(config_dir, |map| {
        map.insert(name.to_string(), config.clone());
        Ok(())
    })
}

/// Remove one build service by name. Writes the remaining map verbatim; an
/// empty result persists as `buildServices: {}`, which reads back the same as
/// an absent block — no services.
pub fn delete_build_service(config_dir: &std::path::Path, name: &str) -> Result<(), String> {
    mutate_build_services(config_dir, |map| {
        map.remove(name);
        Ok(())
    })
}

fn mutate_build_services<T>(
    config_dir: &std::path::Path,
    mutate: impl FnOnce(
        &mut HashMap<String, crate::config::build_services::BuildServiceConfig>,
    ) -> Result<T, String>,
) -> Result<T, String> {
    mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let key = serde_yaml::Value::String("buildServices".to_string());
        let mut map = match root.get(&key).cloned() {
            Some(value) if !value.is_null() => serde_yaml::from_value(value)
                .map_err(|error| format!("Failed to parse build services: {error}"))?,
            None | Some(_) => HashMap::new(),
        };
        let result = mutate(&mut map)?;
        root.insert(
            key,
            serde_yaml::to_value(map)
                .map_err(|error| format!("Failed to serialize build services: {error}"))?,
        );
        Ok(result)
    })
}

/// Resolve the template variables for build-service config expansion.
///
/// `{worktrees}` is always `~/.cairn/worktrees` (the canonical worktree root),
/// independent of the dev/prod `config_dir`, because worktrees live there in
/// both modes.
pub(crate) fn build_service_templates(
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

const SETTINGS_DTO_KEYS: &[&str] = &[
    "tierDefaults",
    // Removed DTO key: the next save deletes the legacy global toggle once its
    // value has migrated into per-tier defaults.
    "activeBackend",
    "tiers",
    "backends",
    "enabledProviders",
    // Removed DTO keys remain here so the next save deletes stale YAML.
    "branchPrefix",
    "maxThinkingTokens",
    "mergeType",
    "pullOnMerge",
    "orphanCleanupDays",
    "repoTargetSweepDays",
    "bugReports",
    "thinkingDisplayMode",
    "logLevel",
    "memoryReviewEnabled",
    "memoryTriageEnabled",
    "maxOpenTriageIssuesPerScope",
    "pendingMemoryThreshold",
    "externalReplies",
    "subscriptionFees",
    "openrouterRouting",
    "routeCallsViaOpenRouter",
    "channels",
    // Legacy DTO-owned fields are removed once their values have migrated.
    "defaultModel",
    "preferredModels",
    "autoStartJobs",
];

fn merge_settings_keys(root: &mut serde_yaml::Mapping, settings: &Settings) -> Result<(), String> {
    let target = serde_yaml::to_value(SettingsFile::from_settings(settings))
        .map_err(|error| format!("Failed to serialize settings: {error}"))?;
    let target = target
        .as_mapping()
        .ok_or_else(|| "serialized settings root is not a mapping".to_string())?;
    for name in SETTINGS_DTO_KEYS {
        let key = serde_yaml::Value::String((*name).to_string());
        match target.get(&key) {
            Some(value) => {
                root.insert(key, value.clone());
            }
            None => {
                root.remove(&key);
            }
        }
    }
    Ok(())
}

/// Save only the top-level keys owned by the general Settings DTO.
pub fn save_settings(config_dir: &std::path::Path, settings: &Settings) -> Result<(), String> {
    mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        merge_settings_keys(root, settings)
    })
}

/// Apply a partial general-settings update to the latest on-disk document.
///
/// Loading the effective DTO, applying the patch, and merging its owned keys all
/// happen under the workspace settings lock. Concurrent partial requests can
/// therefore update disjoint fields without restoring stale values.
pub fn update_settings(
    config_dir: &std::path::Path,
    input: UpdateSettings,
) -> Result<Settings, String> {
    mutate_workspace_settings(config_dir, "cairn: update settings", |root| {
        let file: SettingsFile =
            serde_yaml::from_value(serde_yaml::Value::Mapping(root.clone()))
                .map_err(|error| format!("Failed to parse settings file: {error}"))?;
        let mut current = file.to_settings();
        let enablement_changed = input.enabled_providers.is_some();
        apply_settings_update(&mut current, input);
        validate_enabled_providers(&current, enablement_changed)?;
        merge_settings_keys(root, &current)?;
        Ok(current)
    })
}

/// Record which providers an existing workspace has installed, once.
///
/// `credentialed` is the set of backend keys the workspace's stored accounts
/// can serve; the caller supplies it because credentials live in the identity
/// store, not in `settings.yaml`. Returns the set it wrote, or `None` when the
/// workspace has already answered this question — running it again must never
/// re-derive enablement from current credentials, which is precisely the
/// coupling this setting exists to break.
pub fn migrate_enabled_providers(
    config_dir: &std::path::Path,
    credentialed: &[String],
) -> Result<Option<Vec<String>>, String> {
    mutate_workspace_settings(config_dir, "cairn: record installed providers", |root| {
        let key = serde_yaml::Value::String("enabledProviders".to_string());
        if root.contains_key(&key) {
            return Ok(None);
        }
        let file: SettingsFile =
            serde_yaml::from_value(serde_yaml::Value::Mapping(root.clone()))
                .map_err(|error| format!("Failed to parse settings file: {error}"))?;
        let enabled = file.depended_on_providers(credentialed);
        let value = serde_yaml::to_value(&enabled)
            .map_err(|error| format!("Failed to serialize installed providers: {error}"))?;
        root.insert(key, value);
        Ok(Some(enabled))
    })
}

/// Refuse an enablement change that would leave configuration pointing at a
/// provider the workspace no longer has.
///
/// Removal is prospective configuration, so it may not quietly rewrite the
/// user's other choices to make itself valid, and it may not leave a tier
/// resolving to nothing. Each refusal names the exact obstacle and the change
/// that would clear it, so the caller can offer a replacement in the same
/// update. Credentials are never consulted here: removing a provider from a
/// workspace does not touch its stored accounts.
fn validate_enabled_providers(settings: &Settings, enablement_changed: bool) -> Result<(), String> {
    use crate::backends::catalog;
    if settings.enabled_providers.is_empty() {
        return Err(
            "A workspace needs at least one provider. Add a provider before removing this one."
                .to_string(),
        );
    }
    if let Some(unknown) = settings
        .enabled_providers
        .iter()
        .find(|key| !catalog::is_supported(key))
    {
        return Err(format!(
            "Unknown provider '{unknown}'. This version of Cairn ships: {}.",
            catalog::supported_keys().collect::<Vec<_>>().join(", ")
        ));
    }

    let label = |key: &str| {
        catalog::descriptor(key)
            .map(|entry| entry.display_name.to_string())
            .unwrap_or_else(|| key.to_string())
    };
    let enabled = |key: &str| settings.enabled_providers.iter().any(|entry| entry == key);

    let mut stranded: Vec<(String, Vec<String>)> = Vec::new();
    for (tier, backend) in &settings.tier_defaults {
        if enabled(backend) {
            continue;
        }
        match stranded.iter_mut().find(|(name, _)| name == backend) {
            Some((_, tiers)) => tiers.push(tier.clone()),
            None => stranded.push((backend.clone(), vec![tier.clone()])),
        }
    }
    if let Some((backend, tiers)) = stranded.first_mut() {
        tiers.sort();
        return Err(format!(
            "{} still serves {} {}. Assign {} another provider in the same change, or keep {}.",
            label(backend),
            if tiers.len() == 1 { "tier" } else { "tiers" },
            tiers.join(", "),
            if tiers.len() == 1 {
                "that tier"
            } else {
                "those tiers"
            },
            label(backend),
        ));
    }

    // A tier default naming a provider with no preset for that tier resolves
    // through the tier's first defined provider instead — a redirect nobody
    // asked for. Tolerated where it already exists, refused where an enablement
    // change would be the thing that created it.
    if enablement_changed {
        for tier in &settings.tiers {
            let Some(backend) = settings.tier_defaults.get(tier) else {
                continue;
            };
            let serves = settings
                .backends
                .get(backend)
                .is_some_and(|presets| presets.contains_key(tier));
            if !serves {
                return Err(format!(
                    "{} has no model for tier {tier}, so it cannot serve it. Give it one in the tier matrix, or point that tier at a provider that already assigns it a model.",
                    label(backend)
                ));
            }
        }
    }

    if settings.route_calls_via_openrouter && !enabled("openrouter") {
        return Err(
            "This workspace routes tier-based calls through OpenRouter. Turn that off in the \
             same change, or keep OpenRouter."
                .to_string(),
        );
    }

    Ok(())
}

fn apply_settings_update(current: &mut Settings, input: UpdateSettings) {
    if let Some(value) = input.tier_defaults {
        current.tier_defaults = value;
    }
    if let Some(value) = input.tiers {
        // A tier that no longer exists keeps no default binding behind it.
        current.tier_defaults.retain(|tier, _| value.contains(tier));
        current.tiers = value;
    }
    if let Some(value) = input.backends {
        current.backends = value;
    }
    if let Some(value) = input.enabled_providers {
        current.enabled_providers = crate::backends::catalog::in_catalog_order(value);
    }
    if let Some(value) = input.max_thinking_tokens {
        current.max_thinking_tokens = value;
    }
    if let Some(value) = input.merge_type {
        current.merge_type = value;
    }
    if let Some(value) = input.orphan_cleanup_days {
        current.orphan_cleanup_days = value.clamp(1, 30);
    }
    if let Some(value) = input.repo_target_sweep_days {
        current.repo_target_sweep_days = value.max(0);
    }
    if let Some(value) = input.bug_reports {
        current.bug_reports = value;
    }
    if let Some(value) = input.thinking_display_mode {
        current.thinking_display_mode = value;
    }
    if let Some(value) = input.transcript_text_size {
        current.transcript_text_size = value;
    }
    if let Some(value) = input.transcript_density {
        current.transcript_density = value;
    }
    if let Some(value) = input.memory_review_enabled {
        current.memory_review_enabled = value;
    }
    if let Some(value) = input.memory_triage_enabled {
        current.memory_triage_enabled = value;
    }
    if let Some(value) = input.max_open_triage_issues_per_scope {
        current.max_open_triage_issues_per_scope = value.max(1);
    }
    if let Some(value) = input.pending_memory_threshold {
        current.pending_memory_threshold = value.max(1);
    }
    if let Some(value) = input.thread_compact_threshold {
        current.thread_compact_threshold = clamp_compact_threshold(value);
    }
    if let Some(value) = input.external_replies {
        current.external_replies = value;
    }
    if let Some(value) = input.log_level {
        current.log_level = value;
    }
    if let Some(value) = input.log_retention_days {
        current.log_retention_days = value.max(1);
    }
    if let Some(value) = input.openrouter_routing {
        current.openrouter_routing = value;
    }
    if let Some(value) = input.route_calls_via_openrouter {
        current.route_calls_via_openrouter = value;
    }
    if let Some(value) = input.channels {
        current.channels = value;
    }
    if let Some(value) = input.subscription_fees {
        current.subscription_fees = value
            .into_iter()
            .filter(|(_, fee)| fee.is_finite() && *fee > 0.0)
            .collect();
    }
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
    fn workspace_settings_transaction_commits_scoped() {
        let temp = TempDir::new().unwrap();
        let home = temp.path();
        git_init(home);
        std::fs::write(home.join("unrelated.txt"), "dirty").unwrap();

        mutate_workspace_settings(home, "cairn: update settings", |root| {
            root.insert(
                serde_yaml::Value::String("buildServices".to_string()),
                serde_yaml::Value::Null,
            );
            Ok(())
        })
        .unwrap();

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

        // Preset defaults: every tier starts bound to claude.
        assert_eq!(settings.tiers, vec!["sm", "md", "lg"]);
        assert_eq!(settings.tier_defaults["sm"], "claude");
        assert_eq!(settings.tier_defaults["md"], "claude");
        assert_eq!(settings.tier_defaults["lg"], "claude");
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
        assert_eq!(settings.max_thinking_tokens, Some(31999));
        assert_eq!(settings.merge_type, MergeType::Squash);
        assert_eq!(settings.orphan_cleanup_days, 3);
        assert_eq!(settings.repo_target_sweep_days, 0);
        assert!(settings.auto_start_jobs); // Always true
        assert_eq!(settings.thinking_display_mode, ThinkingDisplayMode::Full);
        assert_eq!(settings.transcript_text_size, TranscriptTextSize::Default);
        assert_eq!(settings.transcript_density, TranscriptDensity::Comfortable);
        assert_eq!(settings.external_replies, ExternalReplyMode::Watchers);
    }

    #[test]
    fn memory_triage_defaults_inherit_review_and_clamp_cap() {
        let disabled: SettingsFile =
            serde_yaml::from_str("memoryReviewEnabled: false\nmaxOpenTriageIssuesPerScope: 0\n")
                .unwrap();
        let settings = disabled.to_settings();
        assert!(!settings.memory_triage_enabled);
        assert_eq!(settings.max_open_triage_issues_per_scope, 1);

        let overridden: SettingsFile = serde_yaml::from_str(
            "memoryReviewEnabled: false\nmemoryTriageEnabled: true\nmaxOpenTriageIssuesPerScope: 3\n",
        )
        .unwrap();
        let settings = overridden.to_settings();
        assert!(settings.memory_triage_enabled);
        assert_eq!(settings.max_open_triage_issues_per_scope, 3);

        let serialized = serde_yaml::to_string(&SettingsFile::from_settings(&settings)).unwrap();
        assert!(serialized.contains("memoryTriageEnabled: true"));
        assert!(serialized.contains("maxOpenTriageIssuesPerScope: 3"));
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
    fn removed_git_settings_are_ignored_and_stripped_while_merge_type_round_trips() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        std::fs::write(
            get_settings_path(dir),
            "branchPrefix: legacy\npullOnMerge: false\nmergeType: rebase\n",
        )
        .unwrap();

        let settings = load_settings(dir);
        assert_eq!(settings.merge_type, MergeType::Rebase);

        save_settings(dir, &settings).unwrap();
        let yaml = std::fs::read_to_string(get_settings_path(dir)).unwrap();
        assert!(!yaml.contains("branchPrefix"), "{yaml}");
        assert!(!yaml.contains("pullOnMerge"), "{yaml}");
        assert!(yaml.contains("mergeType: rebase"), "{yaml}");
    }

    #[test]
    fn test_settings_roundtrip() {
        let settings = Settings {
            max_thinking_tokens: Some(16000),
            merge_type: MergeType::Rebase,
            repo_target_sweep_days: 14,
            transcript_text_size: TranscriptTextSize::Large,
            transcript_density: TranscriptDensity::Relaxed,
            ..test_settings()
        };

        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();

        assert_eq!(restored.tier_defaults, settings.tier_defaults);
        assert_eq!(restored.backends, settings.backends);
        assert_eq!(restored.max_thinking_tokens, settings.max_thinking_tokens);
        assert_eq!(restored.merge_type, settings.merge_type);
        assert_eq!(restored.repo_target_sweep_days, 14);
        assert!(restored.auto_start_jobs); // Always true
        assert_eq!(
            restored.thinking_display_mode,
            settings.thinking_display_mode
        );
        assert_eq!(restored.transcript_text_size, TranscriptTextSize::Large);
        assert_eq!(restored.transcript_density, TranscriptDensity::Relaxed);
    }

    #[test]
    fn channel_settings_yaml_roundtrip_and_defaults() {
        use crate::models::{
            ChannelsConfig, DiscordChannelConfig, IMessageChannelConfig, MessageClassPolicy,
            TelegramChannelConfig,
        };

        let defaults: SettingsFile = serde_yaml::from_str("channels:\n  imessage: {}\n").unwrap();
        let default_channel = defaults.to_settings().channels.imessage;
        assert!(!default_channel.enabled);
        assert_eq!(default_channel.route, MessageClassPolicy::default());
        assert_eq!(
            defaults.to_settings().channels.telegram,
            TelegramChannelConfig::default()
        );
        assert_eq!(
            defaults.to_settings().channels.discord,
            DiscordChannelConfig::default()
        );
        assert_eq!(
            defaults.to_settings().channels.default_thread,
            "cairn://p/CAIRN/3404"
        );

        let channels = ChannelsConfig {
            default_thread: "cairn://p/CAIRN/3404".to_string(),
            imessage: IMessageChannelConfig {
                enabled: true,
                executor: Some("bglab-mac".to_string()),
                to: "+15551234567".to_string(),
                allow_from: vec!["+15551234567".to_string()],
                inbound_capabilities: crate::models::ChannelInboundCapabilities {
                    permissions: false,
                    answers: true,
                    free_text: true,
                },
                route: MessageClassPolicy {
                    question: true,
                    permission: false,
                    notify: true,
                },
            },
            telegram: TelegramChannelConfig {
                enabled: true,
                chat_id: "-1001234567890".to_string(),
                allow_from: vec!["123456789".to_string()],
                inbound_capabilities: crate::models::ChannelInboundCapabilities::default(),
                route: MessageClassPolicy {
                    question: true,
                    permission: true,
                    notify: false,
                },
            },
            discord: DiscordChannelConfig {
                enabled: true,
                guild_id: "111111111111111111".to_string(),
                allow_from: vec!["987654321098765432".to_string()],
                inbound_capabilities: crate::models::ChannelInboundCapabilities {
                    permissions: false,
                    answers: false,
                    free_text: false,
                },
                route: MessageClassPolicy::default(),
            },
        };
        let settings = Settings {
            channels: channels.clone(),
            ..test_settings()
        };
        let file = SettingsFile::from_settings(&settings);
        let yaml = serde_yaml::to_string(&file).unwrap();
        assert!(yaml.contains("allowFrom"));
        assert!(yaml.contains("chatId"));
        assert!(yaml.contains("guildId"));
        assert!(yaml.contains("inboundCapabilities:"));
        assert!(yaml.contains("freeText: true"));
        assert!(!yaml.contains("botToken"));
        assert!(!yaml.contains("BOT_TOKEN"));
        assert_eq!(file.to_settings().channels, channels);
        assert_eq!(
            serde_yaml::from_str::<SettingsFile>(&yaml)
                .unwrap()
                .to_settings()
                .channels,
            channels
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
    fn test_route_calls_via_openrouter_roundtrip() {
        // Default is false, and a false value is omitted from YAML entirely.
        assert!(!test_settings().route_calls_via_openrouter);
        let empty = SettingsFile::from_settings(&test_settings());
        assert!(empty.route_calls_via_openrouter.is_none());
        assert!(!serde_yaml::to_string(&empty)
            .unwrap()
            .contains("routeCallsViaOpenRouter"));

        // Opt-in survives DTO -> file -> DTO and YAML load/save.
        let settings = Settings {
            route_calls_via_openrouter: true,
            ..test_settings()
        };
        let file = SettingsFile::from_settings(&settings);
        assert_eq!(file.route_calls_via_openrouter, Some(true));
        assert!(file.to_settings().route_calls_via_openrouter);
        let yaml = serde_yaml::to_string(&file).unwrap();
        assert!(yaml.contains("routeCallsViaOpenRouter"));
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();
        assert!(parsed.to_settings().route_calls_via_openrouter);
    }

    #[test]
    fn test_settings_roundtrip_disabled_thinking() {
        let settings = Settings {
            max_thinking_tokens: None,
            merge_type: MergeType::Rebase,
            ..test_settings()
        };

        let file = SettingsFile::from_settings(&settings);
        let restored = file.to_settings();

        assert_eq!(restored.max_thinking_tokens, None);
    }

    #[test]
    fn test_yaml_serialization() {
        let file = SettingsFile {
            max_thinking_tokens: Some(Some(16000)),
            merge_type: Some(MergeType::Merge),
            orphan_cleanup_days: Some(3),
            repo_target_sweep_days: Some(14),
            bug_reports: Some(true),
            ..Default::default()
        };

        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(parsed.preferred_models, file.preferred_models);
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
        std::fs::write(get_settings_path(dir), "logLevel: standard\n").unwrap();

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
        let yaml = r#"logLevel: standard
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
    fn toggling_a_build_service_is_surgical_and_preserves_other_settings() {
        use crate::config::build_services::BuildServiceConfig;
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        std::fs::write(get_settings_path(dir), "logLevel: verbose\n").unwrap();

        // Every build service is user-configured: #3493 replaced the managed
        // sccache default with Cargo dependency catalogs, so there is no
        // built-in entry for a toggle to find.
        assert!(
            set_build_service_enabled(dir, "nope", true).is_err(),
            "a service this workspace never configured cannot be toggled"
        );
        assert!(load_build_services(dir).is_empty());

        let config: BuildServiceConfig =
            serde_yaml::from_str("start: [\"my-daemon\", \"--serve\"]\n").unwrap();
        upsert_build_service(dir, "my-daemon", &config).unwrap();

        set_build_service_enabled(dir, "my-daemon", false).unwrap();
        assert!(!load_build_services(dir)["my-daemon"].enabled);
        assert!(
            load_settings_file(dir).unwrap().build_services.is_some(),
            "toggle must write an explicit buildServices block"
        );

        set_build_service_enabled(dir, "my-daemon", true).unwrap();
        assert!(load_build_services(dir)["my-daemon"].enabled);

        // The surgical write leaves every unrelated setting alone — which the
        // old comment claimed and nothing checked.
        assert_eq!(
            load_settings_file(dir).unwrap().log_level,
            Some(LogLevel::Verbose)
        );
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
            launch_env: HashMap::new(),
        };
        upsert_build_service(dir, "mycache", &cfg).unwrap();

        let map = load_build_services(dir);
        // Only the explicitly configured service is present.
        assert!(map.contains_key("mycache"));
        assert_eq!(map.len(), 1);
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
        assert!(after.is_empty());
    }

    #[test]
    fn build_services_defaults_to_empty_when_unset() {
        let temp = TempDir::new().unwrap();
        // No settings file means no managed daemon is launched.
        let services = load_build_services(temp.path());
        assert!(services.is_empty());
    }

    #[test]
    fn web_fetch_pdf_and_active_selectors_persist_across_save() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        let yaml = r#"logLevel: standard
webFetch:
  firecrawl:
    onlyMainContent: false
activeWebFetch: jina
webSearch:
  brave:
    count: 7
activeWebSearch: brave
pdf:
  bmd: {}
activePdf: bmd
futureUserKey:
  nested: preserved
"#;
        std::fs::write(get_settings_path(dir), yaml).unwrap();

        let settings = load_settings(dir);
        save_settings(dir, &settings).unwrap();
        let after = load_settings_file(dir).unwrap();
        assert!(after.web_fetch.as_ref().unwrap().contains_key("firecrawl"));
        assert_eq!(after.active_web_fetch.as_deref(), Some("jina"));
        assert!(after.web_search.as_ref().unwrap().contains_key("brave"));
        assert_eq!(after.active_web_search.as_deref(), Some("brave"));
        assert!(after.pdf.as_ref().unwrap().contains_key("bmd"));
        assert_eq!(after.active_pdf.as_deref(), Some("bmd"));

        let raw: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(get_settings_path(dir)).unwrap())
                .unwrap();
        assert_eq!(
            raw.get("futureUserKey")
                .and_then(|value| value.get("nested"))
                .and_then(serde_yaml::Value::as_str),
            Some("preserved")
        );
    }

    #[test]
    fn general_save_removes_legacy_owned_keys_but_preserves_unknown_keys() {
        let temp = TempDir::new().unwrap();
        let path = get_settings_path(temp.path());
        std::fs::write(
            &path,
            "defaultModel: opus\npreferredModels: [opus]\nautoStartJobs: false\nfutureKey: keep\n",
        )
        .unwrap();

        save_settings(temp.path(), &load_settings(temp.path())).unwrap();
        let root: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert!(root.get("defaultModel").is_none());
        assert!(root.get("preferredModels").is_none());
        assert!(root.get("autoStartJobs").is_none());
        assert_eq!(
            root.get("futureKey").and_then(serde_yaml::Value::as_str),
            Some("keep")
        );
    }

    #[test]
    fn malformed_settings_fail_closed_without_changing_bytes() {
        let temp = TempDir::new().unwrap();
        let path = get_settings_path(temp.path());
        let malformed = b"mergeType: [unterminated\n";
        std::fs::write(&path, malformed).unwrap();

        let error = save_settings(temp.path(), &test_settings()).unwrap_err();
        assert!(error.contains("Failed to parse settings file"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), malformed);
    }

    #[test]
    fn concurrent_partial_settings_updates_preserve_disjoint_fields() {
        use std::sync::{Arc, Barrier};

        let temp = TempDir::new().unwrap();
        save_settings(temp.path(), &test_settings()).unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(3));

        let prefix_home = Arc::clone(&home);
        let prefix_barrier = Arc::clone(&barrier);
        let prefix = std::thread::spawn(move || {
            prefix_barrier.wait();
            update_settings(
                &prefix_home,
                UpdateSettings {
                    ..UpdateSettings::default()
                },
            )
            .unwrap();
        });

        let reports_home = Arc::clone(&home);
        let reports_barrier = Arc::clone(&barrier);
        let reports = std::thread::spawn(move || {
            reports_barrier.wait();
            update_settings(
                &reports_home,
                UpdateSettings {
                    bug_reports: Some(false),
                    ..UpdateSettings::default()
                },
            )
            .unwrap();
        });

        barrier.wait();
        prefix.join().unwrap();
        reports.join().unwrap();

        let settings = load_settings(&home);
        assert!(!settings.bug_reports);
    }

    #[test]
    fn concurrent_workspace_mutations_preserve_both_keys() {
        use std::sync::{Arc, Barrier};

        let temp = TempDir::new().unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(3));
        let mut threads = Vec::new();
        for (key, value) in [("first", "one"), ("second", "two")] {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                mutate_workspace_settings(&home, "cairn: update settings", |root| {
                    root.insert(
                        serde_yaml::Value::String(key.to_string()),
                        serde_yaml::Value::String(value.to_string()),
                    );
                    Ok(())
                })
                .unwrap();
            }));
        }
        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }

        let root: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(get_settings_path(&home)).unwrap())
                .unwrap();
        assert_eq!(
            root.get("first").and_then(serde_yaml::Value::as_str),
            Some("one")
        );
        assert_eq!(
            root.get("second").and_then(serde_yaml::Value::as_str),
            Some("two")
        );
    }

    #[test]
    fn atomic_workspace_write_leaves_valid_yaml_without_temporary_artifacts() {
        let temp = TempDir::new().unwrap();
        save_settings(temp.path(), &test_settings()).unwrap();

        let raw = std::fs::read_to_string(get_settings_path(temp.path())).unwrap();
        serde_yaml::from_str::<serde_yaml::Value>(&raw).unwrap();
        let entries = std::fs::read_dir(temp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec![std::ffi::OsString::from("settings.yaml")]);
    }

    #[test]
    fn test_yaml_deserialization_partial() {
        // Legacy format without backends → defaults are generated
        let yaml = r#"
logLevel: verbose
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(settings.max_thinking_tokens, Some(31999));
        assert_eq!(settings.merge_type, MergeType::Squash);
        assert_eq!(
            settings.backends["claude"]["md"].model,
            Model::new(Model::SONNET)
        );
        assert_eq!(settings.tier_defaults["md"], "claude");
    }

    #[test]
    fn test_legacy_default_model_honored_in_migration() {
        // Old format with defaultModel: opus patches opus into its natural tier.
        let yaml = r#"
defaultModel: opus
logLevel: standard
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();

        assert_eq!(
            settings.backends["claude"]["lg"].model,
            Model::new(Model::OPUS)
        );
        assert_eq!(settings.tier_defaults["lg"], "claude");
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

        // A legacy global activeBackend loads as every tier defaulting to it.
        assert_eq!(settings.tier_defaults["sm"], "codex");
        assert_eq!(settings.tier_defaults["md"], "codex");
        assert_eq!(settings.tier_defaults["lg"], "codex");
        assert_eq!(
            settings.backends["codex"]["md"].model.as_str(),
            "gpt-5.3-codex"
        );

        // Roundtrip
        let file2 = SettingsFile::from_settings(&settings);
        let restored = file2.to_settings();
        assert_eq!(restored.tier_defaults, settings.tier_defaults);
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

            assert!(loaded_settings.auto_start_jobs); // Always true
            assert_eq!(loaded_settings.max_thinking_tokens, None);
            // Default presets should be present
            assert_eq!(loaded_settings.tier_defaults["md"], "claude");
        });
    }

    #[test]
    fn test_yaml_deserialization_missing_field() {
        let yaml = r#"
defaultModel: opus
logLevel: verbose
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
logLevel: verbose
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
        let file: SettingsFile = serde_yaml::from_str("{}\n").unwrap();
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
    fn test_log_retention_days_defaults_and_clamps_zero() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            load_log_retention_days(dir.path()),
            cairn_common::logging::DEFAULT_LOG_RETENTION_DAYS
        );
        std::fs::write(dir.path().join("settings.yaml"), "logRetentionDays: 0\n").unwrap();
        assert_eq!(load_log_retention_days(dir.path()), 1);
        std::fs::write(dir.path().join("settings.yaml"), "logRetentionDays: 3\n").unwrap();
        assert_eq!(load_log_retention_days(dir.path()), 3);
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
            yaml.contains("tierDefaults"),
            "tierDefaults should be serialized"
        );
        assert!(
            !yaml.contains("activeBackend"),
            "the legacy global toggle should not be serialized"
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
        settings
            .tier_defaults
            .insert("md".to_string(), "codex".to_string());

        let file = SettingsFile::from_settings(&settings);
        let yaml = serde_yaml::to_string(&file).unwrap();
        let parsed: SettingsFile = serde_yaml::from_str(&yaml).unwrap();
        let restored = parsed.to_settings();

        assert_eq!(restored.tier_defaults["md"], "codex");
        assert!(restored.backends.contains_key("claude"));
        assert!(restored.backends.contains_key("codex"));
    }

    #[test]
    fn tier_defaults_may_differ_per_tier_and_survive_a_roundtrip() {
        // The end state the per-tier model exists for: best model on top, cheap
        // faucet at the bottom, each written and read back independently.
        let yaml = r#"
tierDefaults:
  sm: openrouter
  md: codex
  lg: claude
tiers:
  - sm
  - md
  - lg
backends:
  claude:
    lg:
      model: opus
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        assert_eq!(settings.tier_defaults["sm"], "openrouter");
        assert_eq!(settings.tier_defaults["md"], "codex");
        assert_eq!(settings.tier_defaults["lg"], "claude");

        let restored = SettingsFile::from_settings(&settings).to_settings();
        assert_eq!(restored.tier_defaults, settings.tier_defaults);
    }

    #[test]
    fn an_explicit_tier_defaults_map_wins_over_a_legacy_active_backend() {
        // A file carrying both is not ambiguous: the per-tier map is the current
        // vocabulary and the global key is what it superseded.
        let yaml = r#"
activeBackend: codex
tierDefaults:
  lg: claude
tiers:
  - sm
  - md
  - lg
backends:
  claude:
    lg:
      model: opus
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        assert_eq!(settings.tier_defaults["lg"], "claude");
        // sm/md carry no binding, so they resolve to their first defined provider.
        assert!(!settings.tier_defaults.contains_key("sm"));
        assert!(!settings.tier_defaults.contains_key("md"));
    }

    #[test]
    fn the_legacy_global_key_is_dropped_from_the_file_once_it_has_migrated() {
        let yaml = r#"
activeBackend: codex
tiers:
  - sm
  - md
  - lg
backends:
  codex:
    md:
      model: gpt-5.3-codex
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let settings = file.to_settings();
        let rendered = serde_yaml::to_string(&SettingsFile::from_settings(&settings)).unwrap();
        assert!(
            !rendered.contains("activeBackend"),
            "legacy global toggle must not be written back: {rendered}"
        );
        assert!(rendered.contains("tierDefaults"), "{rendered}");
    }

    #[test]
    fn deleting_a_tier_removes_its_default_binding() {
        let mut settings = test_settings();
        settings
            .tier_defaults
            .insert("md".to_string(), "codex".to_string());
        apply_settings_update(
            &mut settings,
            UpdateSettings {
                tiers: Some(vec!["sm".to_string(), "lg".to_string()]),
                ..Default::default()
            },
        );
        assert!(
            !settings.tier_defaults.contains_key("md"),
            "a removed tier must not leave a default binding behind"
        );
        assert!(settings.tier_defaults.contains_key("lg"));
    }
}

#[cfg(test)]
mod fleet_settings_tests {
    use super::SettingsFile;

    #[test]
    fn fleet_config_is_config_only_and_round_trips() {
        let yaml = r#"
buildSlots:
  capacityWaitHorizonSeconds: 900
  defaultTimeoutSeconds: 1800
  cpuAdmission:
    entryUtilization: 0.8
    clearUtilization: 0.65
    entryWindowMs: 20000
    clearWindowMs: 10000
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let fleet = file.fleet.as_ref().unwrap();
        assert_eq!(fleet.capacity_wait_horizon_seconds, 900);
        assert_eq!(fleet.default_timeout_seconds, 1800);
        assert_eq!(fleet.cpu_admission.entry_utilization, 0.8);
        assert_eq!(
            fleet
                .resolve_executor_policy("executor-without-override")
                .cpu_admission,
            fleet.cpu_admission
        );
        let serialized = serde_yaml::to_string(&file).unwrap();
        let reparsed: SettingsFile = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.fleet, file.fleet);
        assert!(serialized.contains("buildSlots:"));
        assert!(fleet.remote_executors.is_empty());
        assert!(serialized.contains("cpuAdmission:"));
    }

    /// The retired `acquisitionDeadlineSeconds` is IGNORED rather than adopted,
    /// and a value below the floor is read as the leftover it is.
    ///
    /// This is the inverse of what this test once asserted, and the reversal is
    /// the fix. The old assertion was that carrying the number forward avoided
    /// "silently resetting a machine's tuning" — but the two keys never named
    /// the same quantity. `acquisitionDeadlineSeconds` was a per-attempt queue
    /// budget that a ten-minute executor-side pause compensated for; CAIRN-3268
    /// made the field a TOTAL wait with no such compensation. Carrying 20 across
    /// that rename did not preserve tuning, it cut the fleet's patience to 3% of
    /// its intended value — and then `set_fleet` re-serialized it under the new
    /// name, so nothing downstream could tell it from a number an operator had
    /// chosen. Every check on that fleet abandoned its machine after twenty
    /// seconds (CAIRN-3429). An alias migrates a spelling; it cannot migrate a
    /// semantics, and pretending otherwise is worse than resetting to a default
    /// the operator can see and change.
    #[test]
    fn the_retired_acquisition_deadline_does_not_become_a_wait_horizon() {
        let file: SettingsFile = serde_yaml::from_str(
            "buildSlots:\n  acquisitionDeadlineSeconds: 15\n  defaultTimeoutSeconds: 1800\n",
        )
        .unwrap();
        let fleet = file.fleet.as_ref().unwrap();
        assert_eq!(
            fleet.capacity_wait_horizon_seconds, 600,
            "the retired key names a quantity this field is not, so it supplies nothing"
        );

        // The already-laundered case: the number reached the new spelling before
        // the alias was removed, so only the floor can still catch it.
        let laundered: SettingsFile =
            serde_yaml::from_str("buildSlots:\n  capacityWaitHorizonSeconds: 20\n").unwrap();
        let laundered = laundered.fleet.unwrap();
        assert!(
            laundered.validate().is_err(),
            "no new horizon this short can be written"
        );
        assert_eq!(
            laundered.healed().capacity_wait_horizon_ms(),
            600_000,
            "a horizon too short to outlast one unit of work is not a policy anyone chose"
        );
    }

    /// The heal happens on load, so the number the scheduler spends and the
    /// number the operator sees in the Fleet editor are the same one. Reading a
    /// 20 in a form whose save button then refuses it is its own small lie.
    #[test]
    fn a_leftover_horizon_is_healed_where_the_config_is_read() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(
            super::get_settings_path(temp.path()),
            "buildSlots:\n  capacityWaitHorizonSeconds: 20\n  defaultTimeoutSeconds: 1800\n",
        )
        .unwrap();
        let loaded = super::load_fleet(temp.path());
        assert_eq!(loaded.capacity_wait_horizon_seconds, 600);
        assert!(
            loaded.validate().is_ok(),
            "what load hands out must be something save would accept"
        );
    }

    #[test]
    fn remote_executor_inventory_round_trips_without_secret_fields() {
        let yaml = r#"
buildSlots:
  remoteExecutors:
    linux-builder:
      host: bglab-ub.local
      sshUser: dev
      binaryPath: /opt/cairn/cairn-executor
      cairnHome: /home/dev/.cairn
      executorId: linux-builder
      deviceId: linux-builder-device
      displayName: Linux builder
      projectIds: [0f25d369-6e5b-4f5d-b590-b7652d895b4e]
      tunnelPort: 43849
      extraSshArgs: [-4]
"#;
        let file: SettingsFile = serde_yaml::from_str(yaml).unwrap();
        let fleet = file.fleet.as_ref().unwrap();
        fleet.validate().unwrap();
        let remote = &fleet.remote_executors["linux-builder"];
        assert_eq!(remote.ssh_user, "dev");
        assert_eq!(remote.extra_ssh_args, ["-4"]);

        let serialized = serde_yaml::to_string(&file).unwrap();
        let reparsed: SettingsFile = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.fleet, file.fleet);
        let serialized_lower = serialized.to_ascii_lowercase();
        for forbidden in ["token:", "credential:", "grant:", "secret:"] {
            assert!(
                !serialized_lower.contains(forbidden),
                "settings serialized forbidden secret field {forbidden}"
            );
        }
    }

    #[test]
    fn set_fleet_preserves_unrelated_settings_and_reopens() {
        use crate::fleet::FleetConfig;
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        std::fs::write(super::get_settings_path(dir), "logLevel: standard\n").unwrap();
        let config = FleetConfig {
            capacity_wait_horizon_seconds: 120,
            default_timeout_seconds: 900,
            cpu_admission: cairn_common::executor_protocol::CpuAdmissionPolicy {
                entry_utilization: 0.82,
                clear_utilization: 0.68,
                entry_window_ms: 20_000,
                clear_window_ms: 10_000,
            },
            ..FleetConfig::default()
        };

        super::set_fleet(dir, &config).unwrap();

        assert_eq!(super::load_fleet(dir), config);
        let yaml = std::fs::read_to_string(super::get_settings_path(dir)).unwrap();
        assert!(yaml.contains("logLevel: standard"));
        assert!(yaml.contains("cpuAdmission:"));
        assert!(!yaml.contains("projects:"));
    }

    #[test]
    fn legacy_fleet_config_loads_with_code_owned_default_profile() {
        use crate::fleet::{PlacementStance, PlacementWorkClass, DEFAULT_PLACEMENT_PROFILE};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(
            super::get_settings_path(temp.path()),
            "buildSlots:\n  capacityWaitHorizonSeconds: 900\n",
        )
        .unwrap();
        let fleet = super::load_fleet(temp.path());
        assert_eq!(fleet.active_placement_profile, DEFAULT_PLACEMENT_PROFILE);
        assert!(fleet.placement_profiles.is_empty());
        assert_eq!(
            fleet
                .active_placement_profile()
                .stance(PlacementWorkClass::ReviewChecks),
            PlacementStance::Any
        );
        assert_eq!(fleet.resolved_placement_profiles().len(), 3);
    }

    #[test]
    fn custom_profile_and_activation_survive_reopen() {
        use crate::fleet::{
            built_in_placement_profiles, FleetConfig, INTERACTIVE_PLACEMENT_PROFILE,
        };
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let mut fleet = FleetConfig::default();
        let mut profile = built_in_placement_profiles()[INTERACTIVE_PLACEMENT_PROFILE].clone();
        profile.machine_priority = vec!["local".into()];
        fleet
            .save_placement_profile("focused".into(), profile.clone())
            .unwrap();
        fleet.activate_placement_profile("focused").unwrap();
        super::set_fleet(temp.path(), &fleet).unwrap();

        let reopened = super::load_fleet(temp.path());
        assert_eq!(reopened.active_placement_profile, "focused");
        assert_eq!(reopened.active_placement_profile(), &profile);
    }

    #[test]
    fn invalid_active_profile_heals_on_load_but_is_rejected_on_save() {
        use crate::fleet::{FleetConfig, DEFAULT_PLACEMENT_PROFILE};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        std::fs::write(
            super::get_settings_path(temp.path()),
            "buildSlots:\n  activePlacementProfile: vanished\n",
        )
        .unwrap();
        assert_eq!(
            super::load_fleet(temp.path()).active_placement_profile,
            DEFAULT_PLACEMENT_PROFILE
        );

        let invalid = FleetConfig {
            active_placement_profile: "vanished".into(),
            ..FleetConfig::default()
        };
        assert!(super::set_fleet(temp.path(), &invalid).is_err());
    }

    #[test]
    fn built_in_profiles_cannot_be_shadowed_or_deleted() {
        use crate::fleet::{built_in_placement_profiles, FleetConfig, DEFAULT_PLACEMENT_PROFILE};

        let mut fleet = FleetConfig::default();
        let profile = built_in_placement_profiles()[DEFAULT_PLACEMENT_PROFILE].clone();
        assert!(fleet
            .save_placement_profile(DEFAULT_PLACEMENT_PROFILE.into(), profile)
            .is_err());
        assert!(fleet
            .delete_placement_profile(DEFAULT_PLACEMENT_PROFILE)
            .is_err());
    }

    #[test]
    fn fleet_validation_rejects_an_invalid_workspace_cpu_fallback() {
        let mut fleet = crate::fleet::FleetConfig::default();
        fleet.cpu_admission.clear_utilization = fleet.cpu_admission.entry_utilization;
        assert!(fleet.validate().is_err());
    }
}

#[cfg(test)]
mod enabled_provider_tests {
    use super::*;
    use tempfile::TempDir;

    fn write_settings(dir: &std::path::Path, yaml: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(get_settings_path(dir), yaml).unwrap();
    }

    fn recorded(dir: &std::path::Path) -> Option<Vec<String>> {
        load_settings_file(dir).unwrap().enabled_providers
    }

    fn installed(dir: &std::path::Path) -> Vec<String> {
        load_settings(dir).enabled_providers
    }

    #[test]
    fn an_unmigrated_workspace_still_sees_every_shipped_provider() {
        // Absent is not "nothing installed"; it is "never asked". Reading it as
        // the full catalog is what makes this change invisible until the
        // migration has actually looked at the workspace's evidence.
        let file: SettingsFile = serde_yaml::from_str("tiers: [sm, md, lg]\n").unwrap();
        let expected: Vec<String> = crate::backends::catalog::supported_keys()
            .map(str::to_string)
            .collect();
        assert_eq!(file.to_settings().enabled_providers, expected);
    }

    #[test]
    fn migration_keeps_every_provider_the_workspace_depends_on() {
        let temp = TempDir::new().unwrap();
        write_settings(
            temp.path(),
            r#"
tiers:
  - sm
  - md
  - lg
tierDefaults:
  sm: ollama
  md: claude
  lg: claude
routeCallsViaOpenRouter: true
"#,
        );

        // "native" is a credential target, not a shipped provider; it drops out.
        let enabled = migrate_enabled_providers(
            temp.path(),
            &["opencode-go".to_string(), "native".to_string()],
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            enabled,
            vec!["claude", "openrouter", "opencode-go", "ollama"]
        );
        assert_eq!(recorded(temp.path()).unwrap(), enabled);
        assert!(
            !enabled.contains(&"codex".to_string()),
            "a provider nothing depends on is not installed by the migration"
        );
    }

    #[test]
    fn a_recorded_subscription_fee_counts_as_dependence() {
        let temp = TempDir::new().unwrap();
        write_settings(
            temp.path(),
            "tiers: [sm, md, lg]\nsubscriptionFees:\n  codex: 200.0\n",
        );
        let enabled = migrate_enabled_providers(temp.path(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(enabled, vec!["claude", "codex"]);
    }

    #[test]
    fn a_legacy_global_backend_installs_the_provider_it_named() {
        let temp = TempDir::new().unwrap();
        write_settings(temp.path(), "activeBackend: codex\ntiers: [sm, md, lg]\n");
        let enabled = migrate_enabled_providers(temp.path(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(enabled, vec!["codex"]);
    }

    #[test]
    fn a_fresh_workspace_installs_the_provider_its_tiers_default_to() {
        let temp = TempDir::new().unwrap();
        let enabled = migrate_enabled_providers(temp.path(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(enabled, vec!["claude"]);
        assert_eq!(installed(temp.path()), vec!["claude"]);
    }

    #[test]
    fn enablement_is_recorded_once_and_never_re_derived_from_credentials() {
        let temp = TempDir::new().unwrap();
        write_settings(temp.path(), "tiers: [sm, md, lg]\n");
        let first = migrate_enabled_providers(temp.path(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(first, vec!["claude"]);

        // A credential added later does not install a provider, and a
        // credential removed later does not uninstall one.
        let second = migrate_enabled_providers(temp.path(), &["ollama".to_string()]).unwrap();
        assert!(second.is_none(), "the migration must not run twice");
        assert_eq!(recorded(temp.path()).unwrap(), first);
    }

    #[test]
    fn removing_a_provider_a_tier_still_defaults_to_is_refused_with_the_obstacle_named() {
        let temp = TempDir::new().unwrap();
        write_settings(
            temp.path(),
            r#"
enabledProviders:
  - claude
  - codex
tiers: [sm, md, lg]
tierDefaults:
  sm: codex
  md: claude
  lg: claude
"#,
        );

        let error = update_settings(
            temp.path(),
            UpdateSettings {
                enabled_providers: Some(vec!["claude".to_string()]),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert!(error.contains("OpenAI"), "{error}");
        assert!(error.contains("sm"), "{error}");
        assert_eq!(
            recorded(temp.path()).unwrap(),
            vec!["claude", "codex"],
            "a refused removal leaves the workspace exactly as it was"
        );
    }

    #[test]
    fn a_removal_that_reassigns_the_stranded_tier_succeeds_and_keeps_its_presets() {
        let temp = TempDir::new().unwrap();
        write_settings(
            temp.path(),
            r#"
enabledProviders:
  - claude
  - codex
tiers: [sm, md, lg]
tierDefaults:
  sm: codex
  md: claude
  lg: claude
backends:
  codex:
    sm:
      model: gpt-5.3-codex
"#,
        );

        let settings = update_settings(
            temp.path(),
            UpdateSettings {
                enabled_providers: Some(vec!["claude".to_string()]),
                tier_defaults: Some(HashMap::from([
                    ("sm".to_string(), "claude".to_string()),
                    ("md".to_string(), "claude".to_string()),
                    ("lg".to_string(), "claude".to_string()),
                ])),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(settings.enabled_providers, vec!["claude"]);
        // Disabling is prospective configuration, not deletion: the removed
        // provider's tier assignments survive so re-adding it restores them.
        assert_eq!(
            settings.backends["codex"]["sm"].model.as_str(),
            "gpt-5.3-codex"
        );
    }

    #[test]
    fn removing_openrouter_while_calls_route_through_it_is_refused() {
        let temp = TempDir::new().unwrap();
        write_settings(
            temp.path(),
            r#"
enabledProviders:
  - claude
  - openrouter
tiers: [sm, md, lg]
tierDefaults:
  sm: claude
  md: claude
  lg: claude
routeCallsViaOpenRouter: true
"#,
        );

        let error = update_settings(
            temp.path(),
            UpdateSettings {
                enabled_providers: Some(vec!["claude".to_string()]),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(error.contains("OpenRouter"), "{error}");

        // Turning the routing off in the same change clears the obstacle.
        let settings = update_settings(
            temp.path(),
            UpdateSettings {
                enabled_providers: Some(vec!["claude".to_string()]),
                route_calls_via_openrouter: Some(false),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(settings.enabled_providers, vec!["claude"]);
    }

    #[test]
    fn an_unknown_or_empty_provider_set_is_refused() {
        let temp = TempDir::new().unwrap();
        write_settings(
            temp.path(),
            "enabledProviders: [claude]\ntiers: [sm, md, lg]\n",
        );

        let unknown = update_settings(
            temp.path(),
            UpdateSettings {
                enabled_providers: Some(vec!["claude".to_string(), "gemini".to_string()]),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(unknown.contains("gemini"), "{unknown}");

        let empty = update_settings(
            temp.path(),
            UpdateSettings {
                enabled_providers: Some(Vec::new()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(empty.contains("at least one provider"), "{empty}");
    }

    #[test]
    fn adding_a_provider_normalizes_to_catalog_order_and_round_trips() {
        let temp = TempDir::new().unwrap();
        write_settings(
            temp.path(),
            "enabledProviders: [claude]\ntiers: [sm, md, lg]\n",
        );

        let settings = update_settings(
            temp.path(),
            UpdateSettings {
                enabled_providers: Some(vec![
                    "ollama".to_string(),
                    "claude".to_string(),
                    "openrouter".to_string(),
                ]),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(
            settings.enabled_providers,
            vec!["claude", "openrouter", "ollama"]
        );
        assert_eq!(installed(temp.path()), settings.enabled_providers);
    }
}
