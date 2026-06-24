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

use serde::{Deserialize, Serialize};

use crate::config::agents::FileAgent;
use crate::config::project_settings::load_project_settings;
use crate::config::settings::load_settings;
use crate::models::{
    AgentSnapshot, Model, ModelSelection, Preset, PresetOptionValue, RuntimeExtras, SnapshotPresets,
};

/// Effective presets config (workspace + project merged).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PresetsConfig {
    pub active_backend: String,
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

/// Which level decided the resolved backend. Display/audit only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResolutionSource {
    /// A per-issue / per-execution backend override chose the backend.
    ExecutionOverride,
    /// The agent's authored backend preference chose the backend.
    AgentDefault,
    /// A single-provider tier pinned the backend.
    TierDefault,
    /// The workspace/project active backend chose among a multi-provider tier.
    ActiveBackend,
    /// A concrete (non-tier) model carried its own backend.
    ExplicitModel,
}

/// Resolution output: one atomic backend+model [`ModelSelection`], orthogonal
/// runtime [`RuntimeExtras`], and the provenance of the backend decision.
#[derive(Debug, Clone)]
pub struct ResolvedSelection {
    pub selection: ModelSelection,
    pub extras: RuntimeExtras,
    pub source: ResolutionSource,
}

/// A launch-time override for one agent node: a tier reference that resolves to
/// a selection, a backend-only override that keeps the agent's authored tier, or
/// a fully concrete atomic pin (composer output stored verbatim).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "value")]
pub enum LaunchSelectionOverride {
    /// Tier ref: "lg" or qualified "codex/lg".
    Tier(String),
    /// Override only the backend; keep the agent's authored tier.
    Backend(String),
    /// A fully concrete atomic backend+model pin.
    Concrete(ModelSelection),
}

// Migration-only conversion: a frozen `SnapshotPresets` (read off an old
// snapshot) is rehydrated into a `PresetsConfig` so `migrate_on_read` can
// recover `extras`. The write direction (freezing presets into a snapshot) is
// gone — nothing produces a fresh `SnapshotPresets` anymore.
impl From<&SnapshotPresets> for PresetsConfig {
    fn from(value: &SnapshotPresets) -> Self {
        Self {
            active_backend: value.active_backend.clone(),
            tiers: value.tiers.clone(),
            backends: value.backends.clone(),
        }
    }
}

/// Default tier names.
pub const DEFAULT_TIERS: &[&str] = &["sm", "md", "lg"];
pub const DEFAULT_TIER: &str = "md";

/// Build default Claude backend presets.
///
/// `legacy_thinking_enabled` reflects the deprecated workspace `max_thinking_tokens`
/// setting: when present (the historical default), reasoning models default to
/// "high" effort; otherwise effort is left to the CLI default.
fn reasoning_options(effort: Option<&str>) -> HashMap<String, PresetOptionValue> {
    effort
        .map(|value| {
            HashMap::from([(
                "reasoningEffort".to_string(),
                PresetOptionValue::Str(value.to_string()),
            )])
        })
        .unwrap_or_default()
}

/// Build default Claude backend presets.
///
/// `legacy_thinking_enabled` reflects the deprecated workspace `max_thinking_tokens`
/// setting: when present (the historical default), reasoning models default to
/// "high" effort; otherwise effort is left to the CLI default.
pub fn default_claude_presets(legacy_thinking_enabled: Option<i32>) -> HashMap<String, Preset> {
    let reasoning_default = legacy_thinking_enabled.map(|_| "high".to_string());
    let mut map = HashMap::new();
    map.insert(
        "sm".to_string(),
        Preset {
            model: Model::new(Model::HAIKU),
            options: HashMap::new(),
        },
    );
    map.insert(
        "md".to_string(),
        Preset {
            model: Model::new(Model::SONNET),
            options: reasoning_options(reasoning_default.as_deref()),
        },
    );
    map.insert(
        "lg".to_string(),
        Preset {
            model: Model::new(Model::OPUS),
            options: reasoning_options(reasoning_default.as_deref()),
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
            options: reasoning_options(Some("low")),
        },
    );
    map.insert(
        "md".to_string(),
        Preset {
            model: Model::new("gpt-5.3-codex"),
            options: reasoning_options(Some("medium")),
        },
    );
    map.insert(
        "lg".to_string(),
        Preset {
            model: Model::new("gpt-5.5"),
            options: reasoning_options(Some("high")),
        },
    );
    map
}

/// Build default OpenRouter backend presets.
pub fn default_openrouter_presets() -> HashMap<String, Preset> {
    let mut map = HashMap::new();
    map.insert(
        "sm".to_string(),
        Preset {
            model: Model::new("openrouter/auto"),
            options: HashMap::new(),
        },
    );
    map.insert(
        "md".to_string(),
        Preset {
            model: Model::new("~anthropic/claude-sonnet-latest"),
            options: reasoning_options(Some("medium")),
        },
    );
    map.insert(
        "lg".to_string(),
        Preset {
            model: Model::new("~openai/gpt-latest"),
            options: reasoning_options(Some("high")),
        },
    );
    map
}

/// Build a default PresetsConfig.
pub fn default_presets_config(max_thinking: Option<i32>) -> PresetsConfig {
    let mut backends = HashMap::new();
    backends.insert("claude".to_string(), default_claude_presets(max_thinking));
    backends.insert("codex".to_string(), default_codex_presets());
    backends.insert("openrouter".to_string(), default_openrouter_presets());

    PresetsConfig {
        active_backend: "claude".to_string(),
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

/// Ordered list of providers (backends) that define a preset for `tier`.
///
/// Ordering is active-backend-first then alphabetical, so the first element is a
/// deterministic "first defined provider" for the multi-provider fallbacks.
fn providers_for_tier(tier: &str, config: &PresetsConfig) -> Vec<String> {
    let mut names: Vec<String> = config
        .backends
        .iter()
        .filter(|(_, presets)| presets.contains_key(tier))
        .map(|(name, _)| name.clone())
        .collect();
    names.sort_by(|a, b| {
        if *a == config.active_backend {
            return if *b == config.active_backend {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Less
            };
        }
        if *b == config.active_backend {
            return std::cmp::Ordering::Greater;
        }
        a.cmp(b)
    });
    names
}

/// Choose the backend that serves `tier`, applying the tier-resolution semantics:
///
/// - **Single-provider tier** (defined on exactly one backend): always pins to that
///   backend; `override_backend`/`preferred_backend` are silent no-ops. Never errors.
/// - **Multi-provider tier**: `override` → `preferred` → active, restricted to the
///   providers the tier actually defines. An override/preference naming a backend the
///   tier does NOT define falls to the agent's preferred backend if the tier defines
///   it, else the tier's first defined provider.
///
/// Returns `None` only when the tier is defined on no backend (a genuinely undefined
/// tier name).
fn resolve_tier_backend(
    tier: &str,
    override_backend: Option<&str>,
    preferred_backend: Option<&str>,
    config: &PresetsConfig,
) -> Option<String> {
    let providers = providers_for_tier(tier, config);
    let first = providers.first()?.clone();

    // Single-provider tier: nothing to select; the override is a no-op.
    if providers.len() == 1 {
        return Some(first);
    }

    let defines = |backend: &str| providers.iter().any(|p| p == backend);

    if let Some(backend) = override_backend {
        if defines(backend) {
            return Some(backend.to_string());
        }
        // Override names a backend the tier doesn't define: prefer the agent's
        // preferred backend if the tier defines it, else the first defined provider.
        if let Some(preferred) = preferred_backend {
            if defines(preferred) {
                return Some(preferred.to_string());
            }
        }
        return Some(first);
    }

    if let Some(preferred) = preferred_backend {
        if defines(preferred) {
            return Some(preferred.to_string());
        }
        return Some(first);
    }

    if defines(&config.active_backend) {
        return Some(config.active_backend.clone());
    }
    Some(first)
}

/// Resolve a tier reference to a concrete preset.
///
/// - `"md"` → resolved against the tier's providers (active backend among them, or
///   its single provider when the tier is single-provider).
/// - `"codex/lg"` → the explicit backend acts as an override among the tier's providers.
///
/// A tier defined on >=1 backend always resolves; `'Unknown tier'` is reachable only
/// for a genuinely undefined tier name.
pub fn resolve_preset(tier_ref: &str, config: &PresetsConfig) -> Result<ResolvedPreset, String> {
    let (explicit_backend, tier) = parse_tier_ref(tier_ref);

    if let Some(backend_name) = resolve_tier_backend(tier, explicit_backend, None, config) {
        if let Some(preset) = config.backends.get(&backend_name).and_then(|m| m.get(tier)) {
            return Ok(ResolvedPreset {
                model: preset.model.clone(),
                extras: preset.to_extras(),
                backend: backend_name,
            });
        }
    }

    // Genuinely-undefined tier name: preserve the explicit-backend error semantics.
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
    let requested = tier_selection.unwrap_or(DEFAULT_TIER);
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

/// Resolve authored tier/backend inputs to a concrete atomic selection.
///
/// The single `backend` argument is treated as the backend **override** (per-issue /
/// per-execution selection, or a qualified tier ref). For callers that also carry a
/// distinct agent-preferred backend, or that need provenance, use
/// [`resolve_selection_with_provenance`].
///
/// Loud by design: an unresolvable tier or an unrecognized model returns a
/// descriptive `Err` instead of silently degrading to a bare model name.
pub fn resolve_runtime_selection(
    tier_selection: Option<&str>,
    backend: Option<&str>,
    config: &PresetsConfig,
) -> Result<(ModelSelection, RuntimeExtras), String> {
    let resolved = resolve_selection_with_provenance(tier_selection, backend, None, config)?;
    Ok((resolved.selection, resolved.extras))
}

/// Canonical resolution authority: maps authored tier/backend inputs to one
/// atomic [`ModelSelection`] plus orthogonal [`RuntimeExtras`], carrying the
/// provenance of which level decided the backend.
///
/// `override_backend` is the per-issue / per-execution override; it stays
/// distinct from `preferred_backend` (the agent's authored preference) so
/// single-provider auto-pin and the multi-provider fallbacks resolve per the
/// tier-resolution semantics.
///
/// Loud: a token that is neither a known tier ref nor a recognizable concrete
/// model (no `backend_for_model` match and no explicit backend) is an `Err`,
/// never a fabricated `Model::new(token)` against the active backend.
pub fn resolve_selection_with_provenance(
    tier_selection: Option<&str>,
    override_backend: Option<&str>,
    preferred_backend: Option<&str>,
    config: &PresetsConfig,
) -> Result<ResolvedSelection, String> {
    let authored = normalize_authored_selection(tier_selection, override_backend, config);
    let tier = authored.tier.as_str();

    if is_tier_ref(tier, config) {
        let backend =
            resolve_tier_backend(tier, authored.backend.as_deref(), preferred_backend, config)
                .ok_or_else(|| format!("Unknown tier '{}'", tier))?;
        let preset = config
            .backends
            .get(&backend)
            .and_then(|m| m.get(tier))
            .ok_or_else(|| format!("Unknown tier '{}' for backend '{}'", tier, backend))?;
        let source = if override_backend.is_some() {
            ResolutionSource::ExecutionOverride
        } else if preferred_backend.is_some() {
            ResolutionSource::AgentDefault
        } else if providers_for_tier(tier, config).len() == 1 {
            ResolutionSource::TierDefault
        } else {
            ResolutionSource::ActiveBackend
        };
        return Ok(ResolvedSelection {
            selection: ModelSelection {
                backend,
                model: preset.model.clone(),
            },
            extras: preset.to_extras(),
            source,
        });
    }

    // Not a tier ref: accept a concrete model only if it is recognizable — either
    // a backend resolves it (`backend_for_model`) or an explicit backend was
    // given (the legacy custom-model-with-backend case). Otherwise fail loudly.
    let explicit_backend = authored
        .backend
        .clone()
        .or_else(|| preferred_backend.map(str::to_string));
    let backend = match explicit_backend {
        Some(backend) => backend,
        None => crate::backends::backend_for_model(tier)
            .map(str::to_string)
            .ok_or_else(|| {
                format!(
                    "Unrecognized tier or model '{}' — not a configured tier and no backend resolves it",
                    tier
                )
            })?,
    };
    Ok(ResolvedSelection {
        selection: ModelSelection {
            backend,
            model: Model::new(tier),
        },
        extras: RuntimeExtras::default(),
        source: ResolutionSource::ExplicitModel,
    })
}

/// Load effective presets config (workspace + optional project overrides merged).
pub fn load_effective_presets(config_dir: &Path, project_path: Option<&Path>) -> PresetsConfig {
    let settings = load_settings(config_dir);

    let mut config = PresetsConfig {
        active_backend: settings.active_backend.clone(),
        tiers: settings.tiers.clone(),
        backends: settings.backends.clone(),
    };

    // Merge project-level overrides
    if let Some(proj_path) = project_path {
        let proj_settings = load_project_settings(proj_path);
        if let Some(ab) = proj_settings.active_backend {
            config.active_backend = ab;
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

/// Enumerate the atomic backend+model selections offered for a launch composer.
///
/// For each configured tier, in each backend that defines that tier (active-backend
/// first via [`providers_for_tier`]), yields one `ModelSelection { backend, model }`.
/// Deduplicated by `(backend, model)` so a model shared across tiers appears once.
/// This is the MVP option set: there is no canonical concrete-model registry beyond
/// tiers, so the launch composer offers exactly the tier-resolved selections (the
/// caller unions in a row's own concrete custom selection when needed).
pub fn available_selections(config: &PresetsConfig) -> Vec<ModelSelection> {
    let mut out: Vec<ModelSelection> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    for tier in &config.tiers {
        for backend in providers_for_tier(tier, config) {
            let Some(preset) = config.backends.get(&backend).and_then(|m| m.get(tier)) else {
                continue;
            };
            let model = preset.model.clone();
            if seen.insert((backend.clone(), model.as_str().to_string())) {
                out.push(ModelSelection { backend, model });
            }
        }
    }
    out
}

/// Build a resolved AgentSnapshot from a FileAgent + optional launch override.
///
/// **Central function** — ALL AgentSnapshot construction must go through this.
/// Resolution is loud: an unresolvable tier/backend or unrecognized model
/// returns `Err`. The resulting snapshot stores one atomic `selection`; the
/// authored `tier`/`backend_preference` are preserved as edit pre-fill only.
pub fn resolve_agent_snapshot(
    file_agent: &FileAgent,
    override_selection: Option<&LaunchSelectionOverride>,
    config: &PresetsConfig,
) -> Result<AgentSnapshot, String> {
    // Effective inputs that produced the resolution — also used to compute the
    // authored pre-fill so a Tier/Backend override stays sticky for later edits.
    let (eff_tier, eff_backend): (Option<&str>, Option<&str>) = match override_selection {
        Some(LaunchSelectionOverride::Tier(tier)) => (
            Some(tier.as_str()),
            file_agent.backend_preference.as_deref(),
        ),
        Some(LaunchSelectionOverride::Backend(backend)) => (
            file_agent.tier.as_ref().map(Model::as_str),
            Some(backend.as_str()),
        ),
        Some(LaunchSelectionOverride::Concrete(_)) | None => (
            file_agent.tier.as_ref().map(Model::as_str),
            file_agent.backend_preference.as_deref(),
        ),
    };

    let resolved = match override_selection {
        Some(LaunchSelectionOverride::Concrete(selection)) => ResolvedSelection {
            selection: selection.clone(),
            extras: RuntimeExtras::default(),
            source: ResolutionSource::ExecutionOverride,
        },
        Some(LaunchSelectionOverride::Tier(tier)) => resolve_selection_with_provenance(
            Some(tier),
            None,
            file_agent.backend_preference.as_deref(),
            config,
        )?,
        Some(LaunchSelectionOverride::Backend(backend)) => resolve_selection_with_provenance(
            file_agent.tier.as_ref().map(Model::as_str),
            Some(backend),
            file_agent.backend_preference.as_deref(),
            config,
        )?,
        None => resolve_selection_with_provenance(
            file_agent.tier.as_ref().map(Model::as_str),
            None,
            file_agent.backend_preference.as_deref(),
            config,
        )?,
    };

    let authored = normalize_authored_selection(eff_tier, eff_backend, config);

    Ok(AgentSnapshot {
        id: file_agent.id.clone(),
        name: file_agent.name.clone(),
        description: file_agent.description.clone(),
        prompt: file_agent.prompt.clone(),
        tools: file_agent.tools.clone(),
        tier: Some(authored.tier),
        backend_preference: authored.backend,
        selection: Some(resolved.selection),
        disallowed_tools: file_agent.disallowed_tools.clone(),
        skills: file_agent.skills.clone(),
        fence: file_agent.fence,
        sandbox: None,
        on_escape: None,
        extras: Some(resolved.extras),
        model: None,
        resolved_backend: None,
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
        assert_eq!(resolved.extras.reasoning_effort, Some("high".to_string()));
        assert_eq!(resolved.extras.max_thinking_tokens, None);
    }

    #[test]
    fn resolve_qualified_tier() {
        let config = test_config();
        let resolved = resolve_preset("codex/lg", &config).unwrap();
        assert_eq!(resolved.model.as_str(), "gpt-5.5");
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
    fn resolve_nonmatching_explicit_backend_falls_to_first_defined() {
        // 'md' is defined on >=1 backend, so a non-defining explicit backend no longer
        // errors — it resolves to the tier's first defined provider (active claude).
        let config = test_config();
        let resolved = resolve_preset("unknown/md", &config).unwrap();
        assert_eq!(resolved.backend, "claude");
        assert_eq!(resolved.model.as_str(), "sonnet");
    }

    #[test]
    fn resolve_unknown_tier() {
        let config = test_config();
        let result = resolve_preset("xl", &config);
        assert!(result.is_err());
    }

    #[test]
    fn available_selections_default_config() {
        let config = test_config();
        let avail = available_selections(&config);
        // Every default tier on both backends is represented.
        assert!(avail
            .iter()
            .any(|s| s.backend == "claude" && s.model.as_str() == "haiku"));
        assert!(avail
            .iter()
            .any(|s| s.backend == "claude" && s.model.as_str() == "sonnet"));
        assert!(avail
            .iter()
            .any(|s| s.backend == "claude" && s.model.as_str() == "opus"));
        assert!(avail
            .iter()
            .any(|s| s.backend == "codex" && s.model.as_str() == "gpt-5.3-codex"));
        assert!(avail
            .iter()
            .any(|s| s.backend == "codex" && s.model.as_str() == "gpt-5.5"));
    }

    #[test]
    fn available_selections_dedup_and_active_first() {
        let config = test_config();
        let avail = available_selections(&config);
        // No duplicate (backend, model) pairs.
        let mut keys: Vec<(String, String)> = avail
            .iter()
            .map(|s| (s.backend.clone(), s.model.as_str().to_string()))
            .collect();
        let len = keys.len();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), len, "available_selections must be deduped");
        // Active backend (claude) leads, since the first tier (sm) is multi-provider.
        assert_eq!(avail.first().unwrap().backend, "claude");
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

        let snapshot = resolve_agent_snapshot(
            &file_agent,
            Some(&LaunchSelectionOverride::Tier("lg".to_string())),
            &config,
        )
        .unwrap();
        let selection = snapshot.selection.as_ref().unwrap();
        assert_eq!(selection.model.as_str(), "opus");
        assert_eq!(selection.backend, "claude");
    }

    #[test]
    fn resolve_agent_snapshot_with_agent_tier() {
        let config = test_config();
        let file_agent = make_test_agent(Some("sm"));

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        assert_eq!(snapshot.selection.as_ref().unwrap().model.as_str(), "haiku");
    }

    #[test]
    fn resolve_agent_snapshot_falls_to_md() {
        let config = test_config();
        let file_agent = make_test_agent(None);

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        // DEFAULT_TIER is "md" → sonnet
        assert_eq!(
            snapshot.selection.as_ref().unwrap().model.as_str(),
            "sonnet"
        );
    }

    #[test]
    fn resolve_agent_snapshot_concrete_model_passthrough() {
        let config = test_config();
        let file_agent = make_test_agent(Some("sonnet"));

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        // Legacy concrete selections normalize to the matching tier on read.
        assert_eq!(
            snapshot.selection.as_ref().unwrap().model.as_str(),
            "sonnet"
        );
    }

    #[test]
    fn resolve_agent_snapshot_qualified_override() {
        let config = test_config();
        let file_agent = make_test_agent(None);

        let snapshot = resolve_agent_snapshot(
            &file_agent,
            Some(&LaunchSelectionOverride::Tier("codex/lg".to_string())),
            &config,
        )
        .unwrap();
        let selection = snapshot.selection.as_ref().unwrap();
        assert_eq!(selection.model.as_str(), "gpt-5.5");
        assert_eq!(selection.backend, "codex");
    }

    #[test]
    fn resolve_agent_snapshot_with_backend_preference() {
        let config = test_config();
        let mut file_agent = make_test_agent(Some("md"));
        file_agent.backend_preference = Some("codex".to_string());

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        let selection = snapshot.selection.as_ref().unwrap();
        assert_eq!(selection.model.as_str(), "gpt-5.3-codex");
        assert_eq!(selection.backend, "codex");
    }

    #[test]
    fn resolve_agent_snapshot_concrete_tier_override_not_a_tier() {
        // Legacy concrete model selections normalize into tier/backend pairs.
        let config = test_config();
        let file_agent = make_test_agent(Some("md"));

        let snapshot = resolve_agent_snapshot(
            &file_agent,
            Some(&LaunchSelectionOverride::Tier("gpt-5.5".to_string())),
            &config,
        )
        .unwrap();
        assert_eq!(
            snapshot.selection.as_ref().unwrap().model.as_str(),
            "gpt-5.5"
        );
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
        let selection = snapshot.selection.as_ref().unwrap();
        assert_eq!(selection.model.as_str(), "my-custom-model");
        assert_eq!(selection.backend, "custom-backend");
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

        let snapshot = resolve_agent_snapshot(
            &file_agent,
            Some(&LaunchSelectionOverride::Backend("codex".to_string())),
            &config,
        )
        .unwrap();
        assert_eq!(snapshot.backend_preference.as_deref(), Some("codex"));
        let selection = snapshot.selection.as_ref().unwrap();
        assert_eq!(selection.model.as_str(), "gpt-5.3-codex");
        assert_eq!(selection.backend, "codex");
    }

    #[test]
    fn resolve_runtime_selection_single_provider_tier_ignores_override() {
        // With codex's 'lg' removed, 'lg' is single-provider (claude only). An override
        // pointing at codex is a silent no-op — it auto-pins to claude/opus, never errors.
        let mut config = test_config();
        config
            .backends
            .get_mut("codex")
            .expect("codex presets")
            .remove("lg");

        let (selection, _) = resolve_runtime_selection(Some("lg"), Some("codex"), &config).unwrap();
        assert_eq!(selection.backend, "claude");
        assert_eq!(selection.model.as_str(), "opus");
    }

    #[test]
    fn default_claude_presets_without_thinking() {
        let presets = default_claude_presets(None);
        assert_eq!(presets["sm"].model.as_str(), "haiku");
        // No legacy budget → no effort default, no thinking tokens anywhere.
        assert_eq!(
            presets["sm"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            None
        );
        assert_eq!(
            presets["md"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            None
        );
        assert_eq!(
            presets["lg"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            None
        );
    }

    #[test]
    fn default_claude_presets_with_legacy_thinking_map_to_high_effort() {
        let presets = default_claude_presets(Some(31999));
        assert_eq!(
            presets["sm"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            None
        ); // haiku stays default
        assert_eq!(
            presets["md"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            Some("high".to_string())
        );
        assert_eq!(
            presets["lg"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            Some("high".to_string())
        );
        // The legacy budget is mapped to effort, never stored as a token count.
    }

    #[test]
    fn default_codex_presets_have_reasoning_effort() {
        let presets = default_codex_presets();
        assert_eq!(presets["sm"].model.as_str(), Model::GPT_5_4_MINI);
        assert_eq!(
            presets["sm"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            Some("low".to_string())
        );
        assert_eq!(
            presets["md"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            Some("medium".to_string())
        );
        assert_eq!(
            presets["lg"]
                .options
                .get("reasoningEffort")
                .and_then(PresetOptionValue::as_str)
                .map(str::to_string),
            Some("high".to_string())
        );
    }

    /// Config whose active backend is codex, with an extra single-provider tier
    /// `big` defined only on claude.
    fn single_provider_config() -> PresetsConfig {
        let mut config = default_presets_config(Some(31999));
        config.active_backend = "codex".to_string();
        config.tiers.push("big".to_string());
        config.backends.get_mut("claude").unwrap().insert(
            "big".to_string(),
            Preset {
                model: Model::new(Model::OPUS),
                options: HashMap::new(),
            },
        );
        config
    }

    #[test]
    fn single_provider_tier_pins_backend_ignoring_active() {
        // active backend is codex, but 'big' is defined only on claude.
        let config = single_provider_config();
        let resolved = resolve_preset("big", &config).unwrap();
        assert_eq!(resolved.backend, "claude");
        assert_eq!(resolved.model.as_str(), "opus");
    }

    #[test]
    fn single_provider_tier_pins_backend_ignoring_override() {
        let config = single_provider_config();
        // An override pointing at codex is a no-op for a single-provider tier.
        let (selection, _) =
            resolve_runtime_selection(Some("big"), Some("codex"), &config).unwrap();
        assert_eq!(selection.backend, "claude");
        assert_eq!(selection.model.as_str(), "opus");
    }

    #[test]
    fn single_provider_tier_pins_via_agent_snapshot_seed_override() {
        let config = single_provider_config();
        let mut file_agent = make_test_agent(Some("big"));
        file_agent.backend_preference = Some("codex".to_string());
        // Even an execution backend override pointing at codex is ignored.
        let snapshot = resolve_agent_snapshot(
            &file_agent,
            Some(&LaunchSelectionOverride::Backend("codex".to_string())),
            &config,
        )
        .unwrap();
        let selection = snapshot.selection.as_ref().unwrap();
        assert_eq!(selection.backend, "claude");
        assert_eq!(selection.model.as_str(), "opus");
    }

    #[test]
    fn multi_provider_tier_override_preferred_active_priority() {
        // active claude; sm/md/lg defined on both claude and codex.
        let config = test_config();
        // override wins among defined providers.
        assert_eq!(
            resolve_tier_backend("md", Some("codex"), Some("claude"), &config).unwrap(),
            "codex"
        );
        // no override: preferred wins.
        assert_eq!(
            resolve_tier_backend("md", None, Some("codex"), &config).unwrap(),
            "codex"
        );
        // neither: active backend.
        assert_eq!(
            resolve_tier_backend("md", None, None, &config).unwrap(),
            "claude"
        );
    }

    #[test]
    fn multi_provider_nonmatching_override_falls_to_preferred_then_first() {
        let config = test_config(); // active claude
                                    // override not defined; preferred codex is defined → codex.
        assert_eq!(
            resolve_tier_backend("md", Some("ghost"), Some("codex"), &config).unwrap(),
            "codex"
        );
        // override not defined; preferred not defined → first defined (active claude).
        assert_eq!(
            resolve_tier_backend("md", Some("ghost"), Some("phantom"), &config).unwrap(),
            "claude"
        );
        // override not defined; no preferred → first defined (active claude).
        assert_eq!(
            resolve_tier_backend("md", Some("ghost"), None, &config).unwrap(),
            "claude"
        );
    }

    #[test]
    fn multi_provider_first_defined_excludes_undefined_active() {
        // active claude no longer defines 'md'; md stays multi-provider via codex + gemini.
        let mut config = test_config();
        config.backends.get_mut("claude").unwrap().remove("md");
        let mut gem = HashMap::new();
        gem.insert(
            "md".to_string(),
            Preset {
                model: Model::new("gemini-pro"),
                options: HashMap::new(),
            },
        );
        config.backends.insert("gemini".to_string(), gem);

        // No override/preference, active not among providers → first defined (codex, alpha).
        assert_eq!(
            resolve_tier_backend("md", None, None, &config).unwrap(),
            "codex"
        );
        // Override names the (now non-defining) active backend → first defined.
        assert_eq!(
            resolve_tier_backend("md", Some("claude"), None, &config).unwrap(),
            "codex"
        );
    }

    #[test]
    fn existing_default_tiers_resolve_unchanged() {
        // sm/md/lg are all multi-provider today; resolution must be identical to before.
        let config = test_config();
        assert_eq!(
            resolve_preset("sm", &config).unwrap().model.as_str(),
            "haiku"
        );
        assert_eq!(
            resolve_preset("md", &config).unwrap().model.as_str(),
            "sonnet"
        );
        assert_eq!(
            resolve_preset("lg", &config).unwrap().model.as_str(),
            "opus"
        );
        assert_eq!(
            resolve_preset("codex/sm", &config).unwrap().model.as_str(),
            Model::GPT_5_4_MINI
        );
        assert_eq!(
            resolve_preset("codex/lg", &config).unwrap().model.as_str(),
            "gpt-5.5"
        );
    }

    #[test]
    fn no_unknown_tier_error_for_defined_tier() {
        let config = test_config();
        // Every defined tier resolves with any backend prefix — defined or not.
        for tier in ["sm", "md", "lg"] {
            assert!(resolve_preset(tier, &config).is_ok());
            assert!(resolve_preset(&format!("codex/{}", tier), &config).is_ok());
            assert!(resolve_preset(&format!("ghost/{}", tier), &config).is_ok());
            assert!(resolve_runtime_selection(Some(tier), Some("ghost"), &config).is_ok());
        }
        // A genuinely undefined tier name still errors.
        assert!(resolve_preset("xl", &config).is_err());
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TierResolutionCase {
        name: String,
        config: PresetsConfig,
        tier: String,
        #[serde(default, rename = "override")]
        override_backend: Option<String>,
        #[serde(default)]
        preferred: Option<String>,
        expected: TierResolutionExpected,
    }

    #[derive(Debug, Deserialize)]
    struct TierResolutionExpected {
        backend: String,
        model: String,
    }

    #[test]
    fn shared_tier_resolution_fixture() {
        let cases: Vec<TierResolutionCase> = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../test-fixtures/tier-resolution.json"
        )))
        .unwrap();

        for case in cases {
            let backend = resolve_tier_backend(
                &case.tier,
                case.override_backend.as_deref(),
                case.preferred.as_deref(),
                &case.config,
            )
            .unwrap_or_else(|| panic!("{}: expected backend", case.name));
            assert_eq!(backend, case.expected.backend, "{}", case.name);
            let model = case
                .config
                .backends
                .get(&backend)
                .and_then(|tiers| tiers.get(&case.tier))
                .map(|preset| preset.model.as_str());
            assert_eq!(model, Some(case.expected.model.as_str()), "{}", case.name);
        }
    }

    #[test]
    fn undefined_tier_is_loud_not_a_model_name_fallback() {
        // A custom 'xl' tier that is NOT defined in settings must error with a
        // descriptive message — never silently become Model::new("xl").
        let config = test_config();
        let err = resolve_runtime_selection(Some("xl"), None, &config).unwrap_err();
        assert!(
            err.contains("xl"),
            "error should name the unresolved token: {err}"
        );
        let file_agent = make_test_agent(Some("xl"));
        assert!(resolve_agent_snapshot(&file_agent, None, &config).is_err());
    }

    #[test]
    fn custom_tier_resolves_to_one_atomic_selection() {
        // A custom 'xl' tier defined in settings resolves to one selection whose
        // backend serves its model.
        let mut config = test_config();
        config.tiers.push("xl".to_string());
        config.backends.get_mut("claude").unwrap().insert(
            "xl".to_string(),
            Preset {
                model: Model::new("opus-xl"),
                options: HashMap::new(),
            },
        );
        let (selection, _) = resolve_runtime_selection(Some("xl"), None, &config).unwrap();
        assert_eq!(selection.backend, "claude");
        assert_eq!(selection.model.as_str(), "opus-xl");
        // The backend serves the model per the active config (atomicity).
        assert_eq!(
            config.backends[&selection.backend]["xl"].model.as_str(),
            selection.model.as_str()
        );
    }

    #[test]
    fn provenance_reports_each_decision_level() {
        let config = test_config();
        // Execution override (override_backend supplied).
        assert_eq!(
            resolve_selection_with_provenance(Some("md"), Some("codex"), None, &config)
                .unwrap()
                .source,
            ResolutionSource::ExecutionOverride
        );
        // Agent default (preferred_backend supplied, no override).
        assert_eq!(
            resolve_selection_with_provenance(Some("md"), None, Some("codex"), &config)
                .unwrap()
                .source,
            ResolutionSource::AgentDefault
        );
        // Active backend (multi-provider tier, neither override nor preference).
        assert_eq!(
            resolve_selection_with_provenance(Some("md"), None, None, &config)
                .unwrap()
                .source,
            ResolutionSource::ActiveBackend
        );
        // Tier default (single-provider tier pins the backend).
        let single = single_provider_config();
        assert_eq!(
            resolve_selection_with_provenance(Some("big"), None, None, &single)
                .unwrap()
                .source,
            ResolutionSource::TierDefault
        );
        // Explicit model (concrete model + explicit backend).
        assert_eq!(
            resolve_selection_with_provenance(Some("my-model"), Some("custom"), None, &config)
                .unwrap()
                .source,
            ResolutionSource::ExplicitModel
        );
    }

    fn make_test_agent(tier: Option<&str>) -> FileAgent {
        FileAgent {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: "Test agent".to_string(),
            prompt: "You are a test agent.".to_string(),
            tools: vec!["Read".to_string()],
            tier: tier.map(Model::new),
            fence: None,
            disallowed_tools: None,
            skills: None,
            hooks: None,
            backend_preference: None,
            is_project_scoped: false,
            file_path: std::path::PathBuf::new(),
        }
    }
}
