//! Preset resolution — maps tier references to concrete runtime config.
//!
//! A **tier reference** is either:
//! - Unqualified: `"md"` → resolved against the active backend
//! - Qualified: `"codex/lg"` → resolved against the named backend
//!
//! The central function is [`resolve_agent_snapshot`], which all AgentSnapshot
//! construction sites must use instead of hand-rolling model resolution.

use std::collections::HashMap;
use std::path::Path;

use crate::config::agents::FileAgent;
use crate::config::project_settings::load_project_settings;
use crate::config::settings::load_settings;
use crate::models::{AgentSnapshot, Model, Preset, RuntimeExtras, SnapshotPresets};

/// Effective presets config (workspace + project merged).
#[derive(Debug, Clone)]
pub struct PresetsConfig {
    pub active_backend: String,
    pub default_tier: String,
    pub tiers: Vec<String>,
    pub backends: HashMap<String, HashMap<String, Preset>>,
}

/// Result of resolving a tier reference.
#[derive(Debug, Clone)]
pub struct ResolvedPreset {
    pub model: Model,
    pub extras: RuntimeExtras,
    pub backend: String,
}

/// Authored tier/backend pair stored on agents and snapshot agents.
#[derive(Debug, Clone)]
pub struct AuthoredSelection {
    pub tier: Model,
    pub backend: Option<String>,
}

impl From<&PresetsConfig> for SnapshotPresets {
    fn from(value: &PresetsConfig) -> Self {
        Self {
            active_backend: value.active_backend.clone(),
            default_tier: value.default_tier.clone(),
            tiers: value.tiers.clone(),
            backends: value.backends.clone(),
        }
    }
}

impl From<PresetsConfig> for SnapshotPresets {
    fn from(value: PresetsConfig) -> Self {
        SnapshotPresets::from(&value)
    }
}

impl From<&SnapshotPresets> for PresetsConfig {
    fn from(value: &SnapshotPresets) -> Self {
        Self {
            active_backend: value.active_backend.clone(),
            default_tier: value.default_tier.clone(),
            tiers: value.tiers.clone(),
            backends: value.backends.clone(),
        }
    }
}

impl From<SnapshotPresets> for PresetsConfig {
    fn from(value: SnapshotPresets) -> Self {
        PresetsConfig::from(&value)
    }
}

/// Default tier names.
pub const DEFAULT_TIERS: &[&str] = &["sm", "md", "lg"];

/// Build default Claude backend presets.
pub fn default_claude_presets(max_thinking: Option<i32>) -> HashMap<String, Preset> {
    let mut map = HashMap::new();
    map.insert(
        "sm".to_string(),
        Preset {
            model: Model::new(Model::HAIKU),
            max_thinking_tokens: None,
            reasoning_effort: None,
        },
    );
    map.insert(
        "md".to_string(),
        Preset {
            model: Model::new(Model::SONNET),
            max_thinking_tokens: max_thinking,
            reasoning_effort: None,
        },
    );
    map.insert(
        "lg".to_string(),
        Preset {
            model: Model::new(Model::OPUS),
            max_thinking_tokens: max_thinking,
            reasoning_effort: None,
        },
    );
    map
}

/// Build default Codex backend presets.
pub fn default_codex_presets() -> HashMap<String, Preset> {
    let mut map = HashMap::new();
    map.insert(
        "sm".to_string(),
        Preset {
            model: Model::new(Model::GPT_5_4_MINI),
            max_thinking_tokens: None,
            reasoning_effort: Some("low".to_string()),
        },
    );
    map.insert(
        "md".to_string(),
        Preset {
            model: Model::new("gpt-5.3-codex"),
            max_thinking_tokens: None,
            reasoning_effort: Some("medium".to_string()),
        },
    );
    map.insert(
        "lg".to_string(),
        Preset {
            model: Model::new("gpt-5.4"),
            max_thinking_tokens: None,
            reasoning_effort: Some("high".to_string()),
        },
    );
    map
}

/// Build a default PresetsConfig.
pub fn default_presets_config(max_thinking: Option<i32>) -> PresetsConfig {
    let mut backends = HashMap::new();
    backends.insert("claude".to_string(), default_claude_presets(max_thinking));
    backends.insert("codex".to_string(), default_codex_presets());

    PresetsConfig {
        active_backend: "claude".to_string(),
        default_tier: "md".to_string(),
        tiers: DEFAULT_TIERS.iter().map(|s| s.to_string()).collect(),
        backends,
    }
}

/// Parse a tier reference like `"md"` or `"codex/lg"`.
pub fn parse_tier_ref(tier_ref: &str) -> (Option<&str>, &str) {
    if let Some(idx) = tier_ref.find('/') {
        (Some(&tier_ref[..idx]), &tier_ref[idx + 1..])
    } else {
        (None, tier_ref)
    }
}

/// Check if a string looks like a tier reference (matches a known tier or contains `/`).
pub fn is_tier_ref(s: &str, config: &PresetsConfig) -> bool {
    if s.contains('/') {
        return true;
    }
    config.tiers.contains(&s.to_string())
}

/// Resolve a tier reference to a concrete preset.
///
/// - `"md"` → active backend's medium tier
/// - `"codex/lg"` → codex backend's large tier
pub fn resolve_preset(tier_ref: &str, config: &PresetsConfig) -> Result<ResolvedPreset, String> {
    let (explicit_backend, tier) = parse_tier_ref(tier_ref);
    let backend_name = explicit_backend.unwrap_or(&config.active_backend);

    let backend_presets = config
        .backends
        .get(backend_name)
        .ok_or_else(|| format!("Unknown backend: {}", backend_name))?;

    let preset = backend_presets
        .get(tier)
        .ok_or_else(|| format!("Unknown tier '{}' for backend '{}'", tier, backend_name))?;

    Ok(ResolvedPreset {
        model: preset.model.clone(),
        extras: preset.to_extras(),
        backend: backend_name.to_string(),
    })
}

/// Resolve a specific tier against an explicit backend.
pub fn resolve_preset_for_backend(
    backend: &str,
    tier: &str,
    config: &PresetsConfig,
) -> Result<ResolvedPreset, String> {
    resolve_preset(&format!("{}/{}", backend, tier), config)
}

/// Normalize a legacy concrete model selection to a tier ref when possible.
pub fn normalize_tier_selection(selection: &str, config: &PresetsConfig) -> String {
    let (backend, tier) = parse_tier_ref(selection);
    if backend.is_some() || config.tiers.contains(&tier.to_string()) {
        return selection.to_string();
    }

    let backend_names = std::iter::once(config.active_backend.as_str()).chain(
        config
            .backends
            .keys()
            .map(String::as_str)
            .filter(|name| *name != config.active_backend),
    );

    for backend_name in backend_names {
        for known_tier in &config.tiers {
            if config.backends[backend_name]
                .get(known_tier)
                .map(|preset| preset.model.as_str() == selection)
                .unwrap_or(false)
            {
                return if backend_name == config.active_backend {
                    known_tier.clone()
                } else {
                    format!("{}/{}", backend_name, known_tier)
                };
            }
        }
    }

    selection.to_string()
}

/// Normalize authored tier/backend inputs.
pub fn normalize_authored_selection(
    tier_selection: Option<&str>,
    backend: Option<&str>,
    config: &PresetsConfig,
) -> AuthoredSelection {
    let requested = tier_selection.unwrap_or(config.default_tier.as_str());
    let normalized = normalize_tier_selection(requested, config);
    let (explicit_backend, tier) = parse_tier_ref(&normalized);

    let authored_tier = if is_tier_ref(&normalized, config) {
        Model::new(tier)
    } else {
        Model::new(&normalized)
    };

    AuthoredSelection {
        tier: authored_tier,
        backend: backend.or(explicit_backend).map(str::to_string),
    }
}

/// Resolve authored tier/backend inputs to concrete runtime values.
pub fn resolve_runtime_selection(
    tier_selection: Option<&str>,
    backend: Option<&str>,
    config: &PresetsConfig,
) -> Result<(Model, String, RuntimeExtras), String> {
    let authored = normalize_authored_selection(tier_selection, backend, config);
    let tier = authored.tier.as_str();

    if is_tier_ref(tier, config) {
        let effective_backend = authored
            .backend
            .as_deref()
            .unwrap_or(config.active_backend.as_str());
        let resolved = resolve_preset_for_backend(effective_backend, tier, config)?;
        return Ok((resolved.model, resolved.backend, resolved.extras));
    }

    let backend = authored
        .backend
        .or_else(|| crate::backends::backend_for_model(tier).map(str::to_string))
        .unwrap_or_else(|| config.active_backend.clone());

    Ok((Model::new(tier), backend, RuntimeExtras::default()))
}

pub fn resolve_snapshot_agent_runtime(
    agent: &mut AgentSnapshot,
    config: &PresetsConfig,
) -> Result<(), String> {
    let (model, backend, extras) = resolve_runtime_selection(
        agent.tier.as_ref().map(Model::as_str),
        agent.backend_preference.as_deref(),
        config,
    )?;
    agent.model = Some(model);
    agent.resolved_backend = Some(backend);
    agent.extras = Some(extras);
    Ok(())
}

/// Load effective presets config (workspace + optional project overrides merged).
pub fn load_effective_presets(config_dir: &Path, project_path: Option<&Path>) -> PresetsConfig {
    let settings = load_settings(config_dir);

    let mut config = PresetsConfig {
        active_backend: settings.active_backend.clone(),
        default_tier: settings.default_tier.clone(),
        tiers: settings.tiers.clone(),
        backends: settings.backends.clone(),
    };

    // Merge project-level overrides
    if let Some(proj_path) = project_path {
        let proj_settings = load_project_settings(proj_path);
        if let Some(ab) = proj_settings.active_backend {
            config.active_backend = ab;
        }
        if let Some(dt) = proj_settings.default_tier {
            config.default_tier = dt;
        }
        if let Some(proj_backends) = proj_settings.backends {
            for (backend_name, tier_overrides) in proj_backends {
                let entry = config.backends.entry(backend_name).or_default();
                for (tier, preset) in tier_overrides {
                    entry.insert(tier, preset);
                }
            }
        }
    }

    config
}

/// Build a resolved AgentSnapshot from a FileAgent + optional tier override.
///
/// **Central function** — ALL AgentSnapshot construction must go through this.
///
/// Resolution order:
/// 1. tier from `tier_override`, then agent `tier`, then workspace `default_tier`
/// 2. backend seed (execution quickstart), then qualified tier backend, then agent `backend`
/// 3. concrete runtime model/extras are derived from the selected backend's preset matrix
pub fn resolve_agent_snapshot(
    file_agent: &FileAgent,
    tier_override: Option<&str>,
    config: &PresetsConfig,
) -> Result<AgentSnapshot, String> {
    resolve_agent_snapshot_with_seed_backend(file_agent, tier_override, None, config)
}

pub fn resolve_agent_snapshot_with_seed_backend(
    file_agent: &FileAgent,
    tier_override: Option<&str>,
    backend_seed: Option<&str>,
    config: &PresetsConfig,
) -> Result<AgentSnapshot, String> {
    let authored = normalize_authored_selection(
        tier_override.or_else(|| file_agent.tier.as_ref().map(Model::as_str)),
        backend_seed.or(file_agent.backend_preference.as_deref()),
        config,
    );
    let (resolved_model, resolved_backend, resolved_extras) = resolve_runtime_selection(
        Some(authored.tier.as_str()),
        authored.backend.as_deref(),
        config,
    )?;

    Ok(AgentSnapshot {
        id: file_agent.id.clone(),
        name: file_agent.name.clone(),
        description: file_agent.description.clone(),
        prompt: file_agent.prompt.clone(),
        tools: file_agent.tools.clone(),
        tier: Some(authored.tier),
        backend_preference: authored.backend,
        model: Some(resolved_model),
        disallowed_tools: file_agent.disallowed_tools.clone(),
        skills: file_agent.skills.clone(),
        approval_policy: file_agent.approval_policy,
        filesystem_scope: file_agent.filesystem_scope,
        resolved_backend: Some(resolved_backend),
        extras: Some(resolved_extras),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PresetsConfig {
        default_presets_config(Some(31999))
    }

    #[test]
    fn parse_tier_ref_unqualified() {
        assert_eq!(parse_tier_ref("md"), (None, "md"));
        assert_eq!(parse_tier_ref("lg"), (None, "lg"));
    }

    #[test]
    fn parse_tier_ref_qualified() {
        assert_eq!(parse_tier_ref("codex/lg"), (Some("codex"), "lg"));
        assert_eq!(parse_tier_ref("claude/sm"), (Some("claude"), "sm"));
    }

    #[test]
    fn resolve_unqualified_tier() {
        let config = test_config();
        let resolved = resolve_preset("md", &config).unwrap();
        assert_eq!(resolved.model.as_str(), "sonnet");
        assert_eq!(resolved.backend, "claude");
        assert_eq!(resolved.extras.max_thinking_tokens, Some(31999));
    }

    #[test]
    fn resolve_qualified_tier() {
        let config = test_config();
        let resolved = resolve_preset("codex/lg", &config).unwrap();
        assert_eq!(resolved.model.as_str(), "gpt-5.4");
        assert_eq!(resolved.backend, "codex");
        assert_eq!(resolved.extras.reasoning_effort, Some("high".to_string()));
    }

    #[test]
    fn resolve_sm_tier() {
        let config = test_config();
        let resolved = resolve_preset("sm", &config).unwrap();
        assert_eq!(resolved.model.as_str(), "haiku");
        assert_eq!(resolved.extras.max_thinking_tokens, None);
    }

    #[test]
    fn resolve_unknown_backend() {
        let config = test_config();
        let result = resolve_preset("unknown/md", &config);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_unknown_tier() {
        let config = test_config();
        let result = resolve_preset("xl", &config);
        assert!(result.is_err());
    }

    #[test]
    fn is_tier_ref_detects_tiers() {
        let config = test_config();
        assert!(is_tier_ref("sm", &config));
        assert!(is_tier_ref("md", &config));
        assert!(is_tier_ref("lg", &config));
        assert!(is_tier_ref("codex/lg", &config));
        assert!(!is_tier_ref("sonnet", &config));
        assert!(!is_tier_ref("opus", &config));
    }

    #[test]
    fn resolve_agent_snapshot_with_tier_override() {
        let config = test_config();
        let file_agent = make_test_agent(Some("md"));

        let snapshot = resolve_agent_snapshot(&file_agent, Some("lg"), &config).unwrap();
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "opus");
        assert_eq!(snapshot.resolved_backend.unwrap(), "claude");
    }

    #[test]
    fn resolve_agent_snapshot_with_agent_tier() {
        let config = test_config();
        let file_agent = make_test_agent(Some("sm"));

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "haiku");
    }

    #[test]
    fn resolve_agent_snapshot_falls_to_default_tier() {
        let config = test_config();
        let file_agent = make_test_agent(None);

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        // default_tier is "md" → sonnet
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "sonnet");
    }

    #[test]
    fn resolve_agent_snapshot_concrete_model_passthrough() {
        let config = test_config();
        let file_agent = make_test_agent(Some("sonnet"));

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        // Legacy concrete selections normalize to the matching tier on read.
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "sonnet");
    }

    #[test]
    fn resolve_agent_snapshot_qualified_override() {
        let config = test_config();
        let file_agent = make_test_agent(None);

        let snapshot = resolve_agent_snapshot(&file_agent, Some("codex/lg"), &config).unwrap();
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "gpt-5.4");
        assert_eq!(snapshot.resolved_backend.unwrap(), "codex");
    }

    #[test]
    fn resolve_agent_snapshot_with_backend_preference() {
        let config = test_config();
        let mut file_agent = make_test_agent(Some("md"));
        file_agent.backend_preference = Some("codex".to_string());

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "gpt-5.3-codex");
        assert_eq!(snapshot.resolved_backend.unwrap(), "codex");
    }

    #[test]
    fn resolve_agent_snapshot_concrete_tier_override_not_a_tier() {
        // Legacy concrete model selections normalize into tier/backend pairs.
        let config = test_config();
        let file_agent = make_test_agent(Some("md"));

        let snapshot = resolve_agent_snapshot(&file_agent, Some("gpt-5.4"), &config).unwrap();
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "gpt-5.4");
        let extras = snapshot.extras.unwrap();
        assert_eq!(extras.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(snapshot.backend_preference.as_deref(), Some("codex"));
    }

    #[test]
    fn resolve_agent_snapshot_agent_with_explicit_backend() {
        // FileAgent with a concrete model + explicit backend preference set
        let config = test_config();
        let mut file_agent = make_test_agent(Some("my-custom-model"));
        file_agent.backend_preference = Some("custom-backend".to_string());

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "my-custom-model");
        assert_eq!(snapshot.resolved_backend.unwrap(), "custom-backend");
    }

    #[test]
    fn resolve_agent_snapshot_populates_all_snapshot_fields() {
        // Verify that resolve_agent_snapshot carries through all FileAgent
        // fields, not just model/backend/extras.
        let config = test_config();
        let mut file_agent = make_test_agent(Some("md"));
        file_agent.skills = Some(vec!["testing".to_string()]);
        file_agent.disallowed_tools = Some(vec!["Bash".to_string()]);

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        assert_eq!(snapshot.id, "test");
        assert_eq!(snapshot.name, "Test");
        assert_eq!(snapshot.description, "Test agent");
        assert_eq!(snapshot.tools, vec!["Read".to_string()]);
        assert_eq!(snapshot.skills, Some(vec!["testing".to_string()]));
        assert_eq!(snapshot.disallowed_tools, Some(vec!["Bash".to_string()]));
    }

    #[test]
    fn resolve_agent_snapshot_with_seed_backend_prefers_seed_backend() {
        let config = test_config();
        let file_agent = make_test_agent(Some("md"));

        let snapshot =
            resolve_agent_snapshot_with_seed_backend(&file_agent, None, Some("codex"), &config)
                .unwrap();
        assert_eq!(snapshot.backend_preference.as_deref(), Some("codex"));
        assert_eq!(snapshot.model.as_ref().unwrap().as_str(), "gpt-5.3-codex");
        assert_eq!(snapshot.resolved_backend.as_deref(), Some("codex"));
    }

    #[test]
    fn resolve_runtime_selection_errors_for_unsupported_backend_tier() {
        let mut config = test_config();
        config
            .backends
            .get_mut("codex")
            .expect("codex presets")
            .remove("lg");

        let err = resolve_runtime_selection(Some("lg"), Some("codex"), &config).unwrap_err();
        assert!(err.contains("Unknown tier 'lg'"));
    }

    #[test]
    fn default_claude_presets_without_thinking() {
        let presets = default_claude_presets(None);
        assert_eq!(presets["sm"].model.as_str(), "haiku");
        assert_eq!(presets["sm"].max_thinking_tokens, None);
        assert_eq!(presets["md"].max_thinking_tokens, None);
        assert_eq!(presets["lg"].max_thinking_tokens, None);
    }

    #[test]
    fn default_codex_presets_have_reasoning_effort() {
        let presets = default_codex_presets();
        assert_eq!(presets["sm"].model.as_str(), Model::GPT_5_4_MINI);
        assert_eq!(presets["sm"].reasoning_effort, Some("low".to_string()));
        assert_eq!(presets["md"].reasoning_effort, Some("medium".to_string()));
        assert_eq!(presets["lg"].reasoning_effort, Some("high".to_string()));
        // Codex presets should not have max_thinking_tokens
        assert_eq!(presets["sm"].max_thinking_tokens, None);
    }

    fn make_test_agent(tier: Option<&str>) -> FileAgent {
        FileAgent {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: "Test agent".to_string(),
            prompt: "You are a test agent.".to_string(),
            tools: vec!["Read".to_string()],
            tier: tier.map(Model::new),
            approval_policy: None,
            filesystem_scope: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            is_project_scoped: false,
            file_path: std::path::PathBuf::new(),
        }
    }
}
