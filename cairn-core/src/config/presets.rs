//! Preset resolution — maps tier references to concrete runtime config.
//!
//! A **tier reference** is either:
//! - Unqualified: `"md"` → resolved against that tier's own default backend
//! - Qualified: `"codex/lg"` → resolved against the named backend
//!
//! The central function is [`resolve_agent_snapshot`], which all AgentSnapshot
//! construction sites must use instead of hand-rolling model resolution.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::agents::FileAgent;
use crate::config::project_settings::load_project_settings_read_only;
use crate::config::settings::load_settings;
use crate::models::{
    AgentSnapshot, Model, ModelSelection, Preset, PresetOptionValue, RuntimeExtras, SnapshotPresets,
};

/// Effective presets config (workspace + project merged).
///
/// `tier_defaults` binds each tier to the backend that serves it by default —
/// the whole point of tiers being interchangeable faucets is that `lg` can sit
/// on the best available model while `sm` sits on the cheapest adequate one, so
/// the default is per tier rather than one global provider toggle. A tier with
/// no entry (a freshly added custom tier) falls to its first defined provider.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PresetsConfig {
    pub(crate) tier_defaults: HashMap<String, String>,
    pub(crate) tiers: Vec<String>,
    pub(crate) backends: HashMap<String, HashMap<String, Preset>>,
}

impl PresetsConfig {
    /// The backend this tier defaults to, when one is configured.
    pub(crate) fn tier_default(&self, tier: &str) -> Option<&str> {
        self.tier_defaults.get(tier).map(String::as_str)
    }
}

/// Expand a single backend name into a per-tier default for every named tier.
///
/// This is what a legacy global `activeBackend` MEANT — every tier resolved
/// against that one provider — so it is also exactly how such a config migrates.
pub(crate) fn tier_defaults_from_single_backend(
    backend: &str,
    tiers: &[String],
) -> HashMap<String, String> {
    tiers
        .iter()
        .map(|tier| (tier.clone(), backend.to_string()))
        .collect()
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
    tier: Model,
    backend: Option<String>,
}

/// Which level decided the resolved backend. Display/audit only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResolutionSource {
    /// A per-issue / per-execution backend override chose the backend.
    ExecutionOverride,
    /// The agent's authored backend preference chose the backend.
    AgentDefault,
    /// The tier is defined on exactly one backend, which therefore pinned itself.
    SoleProvider,
    /// The tier's own configured default backend chose among its providers.
    TierDefaultBackend,
    /// Nothing that named a backend named one this tier actually defines — an
    /// override or preference pointing elsewhere, or a tier carrying no default
    /// binding at all — so the tier's first defined provider was used.
    FirstProvider,
    /// A concrete (non-tier) model carried its own backend.
    ExplicitModel,
}

/// The backend chosen to serve a tier, and the rung of the ladder that chose it.
///
/// The two travel together because provenance is only honest if it is read off
/// the branch that actually decided. Deriving it afterwards from which inputs
/// were merely PRESENT reports a cause that did not apply: an override is a
/// documented no-op on a sole-provider tier, and a preference naming a backend
/// the tier does not define is skipped — both would otherwise be reported as
/// having decided the selection they were ignored for.
#[derive(Debug, Clone)]
struct TierBackendChoice {
    backend: String,
    source: ResolutionSource,
}

/// Resolution output: one atomic backend+model [`ModelSelection`], orthogonal
/// runtime [`RuntimeExtras`], and the provenance of the backend decision.
#[derive(Debug, Clone)]
pub struct ResolvedSelection {
    pub(crate) selection: ModelSelection,
    pub(crate) extras: RuntimeExtras,
    pub(crate) source: ResolutionSource,
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
//
// The frozen shape predates per-tier defaults and carries one `activeBackend`.
// Every tier of that snapshot resolved against it, so expanding it across the
// frozen tier list reproduces the resolution the snapshot was written under —
// not a rename, a restatement of the same fact in the current vocabulary.
impl From<&SnapshotPresets> for PresetsConfig {
    fn from(value: &SnapshotPresets) -> Self {
        Self {
            tier_defaults: tier_defaults_from_single_backend(&value.active_backend, &value.tiers),
            tiers: value.tiers.clone(),
            backends: value.backends.clone(),
        }
    }
}

/// Default tier names.
pub(crate) const DEFAULT_TIERS: &[&str] = &["sm", "md", "lg"];
pub(crate) const DEFAULT_TIER: &str = "md";

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
pub(crate) fn default_claude_presets(
    legacy_thinking_enabled: Option<i32>,
) -> HashMap<String, Preset> {
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
fn default_codex_presets() -> HashMap<String, Preset> {
    let mut map = HashMap::new();
    map.insert(
        "sm".to_string(),
        Preset {
            model: Model::new(Model::GPT_5_6_LUNA),
            options: reasoning_options(Some("low")),
        },
    );
    map.insert(
        "md".to_string(),
        Preset {
            model: Model::new(Model::GPT_5_6_TERRA),
            options: reasoning_options(Some("medium")),
        },
    );
    map.insert(
        "lg".to_string(),
        Preset {
            model: Model::new(Model::GPT_5_6_SOL),
            options: reasoning_options(Some("ultra")),
        },
    );
    map
}

/// Build default OpenRouter backend presets.
fn default_openrouter_presets() -> HashMap<String, Preset> {
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

/// Build default OpenCode Go backend presets.
///
/// Go prices models against one shared dollar budget, so the tiers are chosen by
/// what each costs against it as much as by capability: `sm` is the cheap model
/// with effectively unlimited headroom, `md` the workhorse on the larger monthly
/// allowance, `lg` the strongest model with the smallest allowance. Efforts are
/// each model's own published vocabulary rather than a common ladder — Go's
/// models do not share one.
///
/// The two DeepSeek models are deliberately absent despite being the cheapest on
/// offer: they are served only from China-hosted infrastructure behind an
/// explicit per-workspace opt-in, so a default pointing at one fails for anyone
/// who has not opted in.
fn default_opencode_presets() -> HashMap<String, Preset> {
    let mut map = HashMap::new();
    map.insert(
        "sm".to_string(),
        Preset {
            model: Model::new("mimo-v2.5"),
            options: HashMap::new(),
        },
    );
    map.insert(
        "md".to_string(),
        Preset {
            model: Model::new("glm-5.2"),
            options: reasoning_options(Some("high")),
        },
    );
    map.insert(
        "lg".to_string(),
        Preset {
            model: Model::new("kimi-k3"),
            options: reasoning_options(Some("max")),
        },
    );
    map
}

/// Build a default PresetsConfig.
pub(crate) fn default_presets_config(max_thinking: Option<i32>) -> PresetsConfig {
    let mut backends = HashMap::new();
    backends.insert("claude".to_string(), default_claude_presets(max_thinking));
    backends.insert("codex".to_string(), default_codex_presets());
    backends.insert("openrouter".to_string(), default_openrouter_presets());
    backends.insert(
        crate::backends::opencode::OPENCODE_BACKEND_KEY.to_string(),
        default_opencode_presets(),
    );

    let tiers: Vec<String> = DEFAULT_TIERS.iter().map(|s| s.to_string()).collect();
    PresetsConfig {
        tier_defaults: tier_defaults_from_single_backend("claude", &tiers),
        tiers,
        backends,
    }
}

/// Parse a tier reference like `"md"` or `"codex/lg"`.
pub(crate) fn parse_tier_ref(tier_ref: &str) -> (Option<&str>, &str) {
    if let Some(idx) = tier_ref.find('/') {
        (Some(&tier_ref[..idx]), &tier_ref[idx + 1..])
    } else {
        (None, tier_ref)
    }
}

/// Check if a string looks like a tier reference (matches a known tier or contains `/`).
pub(crate) fn is_tier_ref(s: &str, config: &PresetsConfig) -> bool {
    if s.contains('/') {
        return true;
    }
    config.tiers.contains(&s.to_string())
}

/// Ordered list of providers (backends) that define a preset for `tier`.
///
/// Ordering puts THIS tier's default backend first, then the rest
/// alphabetically, so the first element is a deterministic "first defined
/// provider" for the multi-provider fallbacks.
fn providers_for_tier(tier: &str, config: &PresetsConfig) -> Vec<String> {
    let default_backend = config.tier_default(tier);
    let mut names: Vec<String> = config
        .backends
        .iter()
        .filter(|(_, presets)| presets.contains_key(tier))
        .map(|(name, _)| name.clone())
        .collect();
    names.sort_by(|a, b| {
        let a_default = Some(a.as_str()) == default_backend;
        let b_default = Some(b.as_str()) == default_backend;
        match (a_default, b_default) {
            (true, true) | (false, false) => a.cmp(b),
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
        }
    });
    names
}

/// Choose the backend that serves `tier`, and report which rung chose it.
///
/// - **Single-provider tier** (defined on exactly one backend): pins to that
///   backend; `override_backend`/`preferred_backend` are silent no-ops, and the
///   source says `SoleProvider` rather than crediting an input that was ignored.
/// - **Multi-provider tier**: one uniform ladder — `override` → `preferred` →
///   the tier's own default → the tier's first defined provider. Every rung is
///   skipped when it names a backend this tier does not define, so a stale
///   override does not knock resolution off the ladder; it simply does not
///   apply. Reaching the last rung is reported as `FirstProvider`, which is the
///   state an unbound custom tier lands in.
///
/// Returns `None` only when the tier is defined on no backend (a genuinely undefined
/// tier name).
fn resolve_tier_backend(
    tier: &str,
    override_backend: Option<&str>,
    preferred_backend: Option<&str>,
    config: &PresetsConfig,
) -> Option<TierBackendChoice> {
    let providers = providers_for_tier(tier, config);
    let first = providers.first()?.clone();

    // Single-provider tier: nothing to select; any override is a no-op.
    if providers.len() == 1 {
        return Some(TierBackendChoice {
            backend: first,
            source: ResolutionSource::SoleProvider,
        });
    }

    let defines = |backend: &str| providers.iter().any(|p| p == backend);
    let chose = |backend: &str, source: ResolutionSource| {
        Some(TierBackendChoice {
            backend: backend.to_string(),
            source,
        })
    };

    if let Some(backend) = override_backend {
        if defines(backend) {
            return chose(backend, ResolutionSource::ExecutionOverride);
        }
    }
    if let Some(preferred) = preferred_backend {
        if defines(preferred) {
            return chose(preferred, ResolutionSource::AgentDefault);
        }
    }
    if let Some(tier_default) = config.tier_default(tier) {
        if defines(tier_default) {
            return chose(tier_default, ResolutionSource::TierDefaultBackend);
        }
    }
    Some(TierBackendChoice {
        backend: first,
        source: ResolutionSource::FirstProvider,
    })
}

/// Resolve a tier reference to a concrete preset.
///
/// - `"md"` → resolved against the tier's providers (its own default backend among
///   them, or its single provider when the tier is single-provider).
/// - `"codex/lg"` → the explicit backend acts as an override among the tier's providers.
///
/// A tier defined on >=1 backend always resolves; `'Unknown tier'` is reachable only
/// for a genuinely undefined tier name.
pub fn resolve_preset(tier_ref: &str, config: &PresetsConfig) -> Result<ResolvedPreset, String> {
    let (explicit_backend, tier) = parse_tier_ref(tier_ref);

    if let Some(choice) = resolve_tier_backend(tier, explicit_backend, None, config) {
        if let Some(preset) = config
            .backends
            .get(&choice.backend)
            .and_then(|m| m.get(tier))
        {
            return Ok(ResolvedPreset {
                model: preset.model.clone(),
                extras: preset.to_extras(),
                backend: choice.backend,
            });
        }
    }

    // Genuinely-undefined tier name: preserve the explicit-backend error semantics.
    let backend_name = explicit_backend
        .or_else(|| config.tier_default(tier))
        .ok_or_else(|| format!("Unknown tier '{}'", tier))?;
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

/// Normalize a legacy concrete model selection to a tier ref when possible.
pub(crate) fn normalize_tier_selection(selection: &str, config: &PresetsConfig) -> String {
    let (backend, tier) = parse_tier_ref(selection);
    if backend.is_some() || config.tiers.contains(&tier.to_string()) {
        return selection.to_string();
    }

    // Search tier by tier, each in its own provider order (its default backend
    // first), so a legacy concrete model normalizes to the SHORTEST ref that
    // still resolves back to it: unqualified when the match sits on that tier's
    // default provider, qualified otherwise.
    for known_tier in &config.tiers {
        for backend_name in providers_for_tier(known_tier, config) {
            if config.backends[&backend_name]
                .get(known_tier)
                .map(|preset| preset.model.as_str() == selection)
                .unwrap_or(false)
            {
                return if Some(backend_name.as_str()) == config.tier_default(known_tier) {
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
fn normalize_authored_selection(
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
pub(crate) fn resolve_runtime_selection(
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
pub(crate) fn resolve_selection_with_provenance(
    tier_selection: Option<&str>,
    override_backend: Option<&str>,
    preferred_backend: Option<&str>,
    config: &PresetsConfig,
) -> Result<ResolvedSelection, String> {
    let authored = normalize_authored_selection(tier_selection, override_backend, config);
    let tier = authored.tier.as_str();

    if is_tier_ref(tier, config) {
        // Provenance comes from the choice itself, never from which inputs were
        // present: an override the tier ignores must not be reported as having
        // decided anything.
        let choice =
            resolve_tier_backend(tier, authored.backend.as_deref(), preferred_backend, config)
                .ok_or_else(|| format!("Unknown tier '{}'", tier))?;
        let preset = config
            .backends
            .get(&choice.backend)
            .and_then(|m| m.get(tier))
            .ok_or_else(|| format!("Unknown tier '{}' for backend '{}'", tier, choice.backend))?;
        return Ok(ResolvedSelection {
            selection: ModelSelection {
                backend: choice.backend,
                model: preset.model.clone(),
            },
            extras: preset.to_extras(),
            source: choice.source,
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

/// The backend a SPAWNED task authors, given the spawn payload's explicit
/// preference and the agent file's own.
///
/// Deliberately short: there is no third rung. A task does not inherit the
/// calling session's provider, because with per-tier defaults the tier IS the
/// routing decision — `sm` means "the cheapest adequate faucet", and which
/// provider that is belongs to the tier, not to whoever happened to call.
/// Inheritance would defeat tier defaults at exactly the site that consumes
/// unqualified tier refs most (fan-out tasks), pinning every child of an
/// opus parent to Claude no matter what `sm` is configured to use.
///
/// Both spawn paths — MCP-hosted delegation and the child-task job builder —
/// ask here, so the ladder cannot differ between them. It once did: one ranked
/// the inherited provider above the agent file's preference and the other below
/// it, so the same agent resolved differently depending on which door it came
/// through.
pub(crate) fn spawned_task_backend<'a>(
    explicit: Option<&'a str>,
    agent_file: Option<&'a str>,
) -> Option<&'a str> {
    explicit.or(agent_file)
}

/// Load effective presets config (workspace + optional project overrides merged).
pub fn load_effective_presets(config_dir: &Path, project_path: Option<&Path>) -> PresetsConfig {
    let settings = load_settings(config_dir);

    let mut config = PresetsConfig {
        tier_defaults: settings.tier_defaults.clone(),
        tiers: settings.tiers.clone(),
        backends: settings.backends.clone(),
    };

    // Merge project-level overrides
    if let Some(proj_path) = project_path {
        let proj_settings = load_project_settings_read_only(proj_path);
        // A project's legacy global `activeBackend` said "every tier here runs on
        // this provider"; it lands as exactly that, and an explicit per-tier
        // `tierDefaults` then overrides tier by tier.
        if let Some(backend) = proj_settings.legacy_active_backend() {
            config.tier_defaults = tier_defaults_from_single_backend(backend, &config.tiers);
        }
        if let Some(proj_tier_defaults) = proj_settings.tier_defaults.clone() {
            for (tier, backend) in proj_tier_defaults {
                config.tier_defaults.insert(tier, backend);
            }
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
/// For each configured tier, in each backend that defines that tier (that tier's
/// default backend first via [`providers_for_tier`]), yields one
/// `ModelSelection { backend, model }`.
/// Deduplicated by `(backend, model)` so a model shared across tiers appears once.
/// This is the MVP option set: there is no canonical concrete-model registry beyond
/// tiers, so the launch composer offers exactly the tier-resolved selections (the
/// caller unions in a row's own concrete custom selection when needed).
pub(crate) fn available_selections(config: &PresetsConfig) -> Vec<ModelSelection> {
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
        edited_at: None,
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
        assert_eq!(resolved.model.as_str(), "gpt-5.6-sol");
        assert_eq!(resolved.backend, "codex");
        assert_eq!(resolved.extras.reasoning_effort, Some("ultra".to_string()));
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
            .any(|s| s.backend == "codex" && s.model.as_str() == "gpt-5.6-terra"));
        assert!(avail
            .iter()
            .any(|s| s.backend == "codex" && s.model.as_str() == "gpt-5.6-sol"));
    }

    #[test]
    fn available_selections_dedup_and_tier_default_first() {
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
        // sm's default backend (claude) leads, since sm is multi-provider.
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
        assert_eq!(selection.model.as_str(), "gpt-5.6-sol");
        assert_eq!(selection.backend, "codex");
    }

    #[test]
    fn resolve_agent_snapshot_with_backend_preference() {
        let config = test_config();
        let mut file_agent = make_test_agent(Some("md"));
        file_agent.backend_preference = Some("codex".to_string());

        let snapshot = resolve_agent_snapshot(&file_agent, None, &config).unwrap();
        let selection = snapshot.selection.as_ref().unwrap();
        assert_eq!(selection.model.as_str(), "gpt-5.6-terra");
        assert_eq!(selection.backend, "codex");
    }

    #[test]
    fn resolve_agent_snapshot_concrete_tier_override_not_a_tier() {
        // Legacy concrete model selections normalize into tier/backend pairs.
        let config = test_config();
        let file_agent = make_test_agent(Some("md"));

        let snapshot = resolve_agent_snapshot(
            &file_agent,
            Some(&LaunchSelectionOverride::Tier("gpt-5.6-sol".to_string())),
            &config,
        )
        .unwrap();
        assert_eq!(
            snapshot.selection.as_ref().unwrap().model.as_str(),
            "gpt-5.6-sol"
        );
        let extras = snapshot.extras.unwrap();
        assert_eq!(extras.reasoning_effort.as_deref(), Some("ultra"));
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
        assert_eq!(selection.model.as_str(), "gpt-5.6-terra");
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
        assert_eq!(presets["sm"].model.as_str(), Model::GPT_5_6_LUNA);
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
            Some("ultra".to_string())
        );
    }

    /// Config whose tiers all default to codex, with an extra single-provider
    /// tier `big` defined only on claude.
    fn single_provider_config() -> PresetsConfig {
        let mut config = default_presets_config(Some(31999));
        config.tier_defaults = tier_defaults_from_single_backend("codex", &config.tiers);
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
    fn single_provider_tier_pins_backend_ignoring_other_tier_defaults() {
        // Every other tier defaults to codex, but 'big' is defined only on claude.
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

    /// Assert both halves of a choice: the backend AND the rung that chose it.
    fn assert_chose(
        tier: &str,
        override_backend: Option<&str>,
        preferred: Option<&str>,
        config: &PresetsConfig,
        expected_backend: &str,
        expected_source: ResolutionSource,
    ) {
        let choice = resolve_tier_backend(tier, override_backend, preferred, config)
            .unwrap_or_else(|| panic!("tier '{tier}' should resolve"));
        assert_eq!(choice.backend, expected_backend, "backend for '{tier}'");
        assert_eq!(choice.source, expected_source, "provenance for '{tier}'");
    }

    #[test]
    fn multi_provider_tier_override_preferred_tier_default_priority() {
        // every tier defaults to claude; sm/md/lg defined on both claude and codex.
        let config = test_config();
        // override wins among defined providers.
        assert_chose(
            "md",
            Some("codex"),
            Some("claude"),
            &config,
            "codex",
            ResolutionSource::ExecutionOverride,
        );
        // no override: preferred wins.
        assert_chose(
            "md",
            None,
            Some("codex"),
            &config,
            "codex",
            ResolutionSource::AgentDefault,
        );
        // neither: the tier's own default backend.
        assert_chose(
            "md",
            None,
            None,
            &config,
            "claude",
            ResolutionSource::TierDefaultBackend,
        );
    }

    #[test]
    fn each_tier_resolves_through_its_own_default_backend() {
        // The point of the model: one ladder per tier, not one provider for all.
        let mut config = test_config();
        config
            .tier_defaults
            .insert("lg".to_string(), "claude".to_string());
        config
            .tier_defaults
            .insert("md".to_string(), "codex".to_string());
        config
            .tier_defaults
            .insert("sm".to_string(), "openrouter".to_string());

        assert_eq!(resolve_preset("lg", &config).unwrap().backend, "claude");
        assert_eq!(
            resolve_preset("lg", &config).unwrap().model.as_str(),
            "opus"
        );
        assert_eq!(resolve_preset("md", &config).unwrap().backend, "codex");
        assert_eq!(
            resolve_preset("md", &config).unwrap().model.as_str(),
            Model::GPT_5_6_TERRA
        );
        assert_eq!(resolve_preset("sm", &config).unwrap().backend, "openrouter");
        assert_eq!(
            resolve_preset("sm", &config).unwrap().model.as_str(),
            "openrouter/auto"
        );
    }

    #[test]
    fn a_tier_default_never_leaks_into_a_sibling_tier() {
        // Pointing 'sm' at codex must leave 'md' and 'lg' exactly where they were.
        let mut config = test_config();
        config
            .tier_defaults
            .insert("sm".to_string(), "codex".to_string());

        assert_eq!(resolve_preset("sm", &config).unwrap().backend, "codex");
        assert_eq!(resolve_preset("md", &config).unwrap().backend, "claude");
        assert_eq!(resolve_preset("lg", &config).unwrap().backend, "claude");
    }

    #[test]
    fn qualified_refs_and_overrides_ignore_the_tier_default() {
        // A qualified ref and an execution override outrank the tier's default.
        let mut config = test_config();
        config
            .tier_defaults
            .insert("lg".to_string(), "codex".to_string());

        assert_eq!(
            resolve_preset("claude/lg", &config).unwrap().backend,
            "claude"
        );
        let (selection, _) =
            resolve_runtime_selection(Some("lg"), Some("claude"), &config).unwrap();
        assert_eq!(selection.backend, "claude");
        assert_eq!(selection.model.as_str(), "opus");
    }

    #[test]
    fn a_tier_without_a_default_falls_to_its_first_defined_provider() {
        // A freshly added custom tier carries no binding yet; it must still resolve.
        let mut config = test_config();
        config.tiers.push("xl".to_string());
        for backend in ["codex", "openrouter"] {
            config.backends.get_mut(backend).unwrap().insert(
                "xl".to_string(),
                Preset {
                    model: Model::new(format!("{backend}-xl")),
                    options: HashMap::new(),
                },
            );
        }
        assert!(config.tier_default("xl").is_none());
        // Alphabetically first among the tier's providers.
        assert_eq!(resolve_preset("xl", &config).unwrap().backend, "codex");
    }

    #[test]
    fn normalize_qualifies_a_model_that_is_not_on_its_tier_default() {
        // 'sonnet' is claude/md. With md defaulting to codex, the shortest ref
        // that still resolves back to sonnet is the qualified one.
        let mut config = test_config();
        assert_eq!(normalize_tier_selection("sonnet", &config), "md");
        config
            .tier_defaults
            .insert("md".to_string(), "codex".to_string());
        assert_eq!(normalize_tier_selection("sonnet", &config), "claude/md");
        assert_eq!(
            normalize_tier_selection(Model::GPT_5_6_TERRA, &config),
            "md"
        );
    }

    /// A rung naming a backend the tier does not define is SKIPPED, and the
    /// ladder continues below it — it does not short-circuit to the last rung.
    #[test]
    fn a_nonmatching_rung_is_skipped_and_the_ladder_continues() {
        let config = test_config(); // every tier defaults to claude

        // override not defined; preferred codex is defined → codex, credited to
        // the preference rather than to the override that was ignored.
        assert_chose(
            "md",
            Some("ghost"),
            Some("codex"),
            &config,
            "codex",
            ResolutionSource::AgentDefault,
        );
        // neither defined → the tier's own default still gets its turn.
        assert_chose(
            "md",
            Some("ghost"),
            Some("phantom"),
            &config,
            "claude",
            ResolutionSource::TierDefaultBackend,
        );
        assert_chose(
            "md",
            Some("ghost"),
            None,
            &config,
            "claude",
            ResolutionSource::TierDefaultBackend,
        );
    }

    #[test]
    fn multi_provider_first_defined_excludes_a_tier_default_that_does_not_define_it() {
        // md's default (claude) no longer defines 'md'; md stays multi-provider
        // via codex + gemini.
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

        // No override/preference, the tier default not among providers → first
        // defined (codex, alphabetically), and the source says so rather than
        // crediting a tier default that could not apply.
        assert_chose(
            "md",
            None,
            None,
            &config,
            "codex",
            ResolutionSource::FirstProvider,
        );
        // Override names the (now non-defining) default backend → first defined.
        assert_chose(
            "md",
            Some("claude"),
            None,
            &config,
            "codex",
            ResolutionSource::FirstProvider,
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
            Model::GPT_5_6_LUNA
        );
        assert_eq!(
            resolve_preset("codex/lg", &config).unwrap().model.as_str(),
            "gpt-5.6-sol"
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
            .unwrap_or_else(|| panic!("{}: expected backend", case.name))
            .backend;
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
    fn a_spawned_task_never_inherits_a_callers_provider() {
        // Explicit payload preference wins, then the agent file's own. There is
        // no third rung: with neither, the tier's default decides.
        assert_eq!(
            spawned_task_backend(Some("codex"), Some("claude")),
            Some("codex")
        );
        assert_eq!(spawned_task_backend(None, Some("claude")), Some("claude"));
        assert_eq!(spawned_task_backend(None, None), None);
    }

    #[test]
    fn a_claude_parent_spawning_sm_lands_on_sms_own_default() {
        // The acceptance case: lg defaults to claude, sm to another backend. A
        // claude-backed parent spawning an unqualified `sm` task with no
        // preference anywhere resolves through sm's default, not the caller's.
        let mut config = test_config();
        config
            .tier_defaults
            .insert("lg".to_string(), "claude".to_string());
        config
            .tier_defaults
            .insert("sm".to_string(), "codex".to_string());

        let resolved = resolve_selection_with_provenance(
            Some("sm"),
            None,
            spawned_task_backend(None, None),
            &config,
        )
        .unwrap();
        assert_eq!(resolved.selection.backend, "codex");
        assert_eq!(resolved.selection.model.as_str(), Model::GPT_5_6_LUNA);
        assert_eq!(resolved.source, ResolutionSource::TierDefaultBackend);

        // An explicit preference still wins, unchanged.
        let pinned = resolve_selection_with_provenance(
            Some("sm"),
            None,
            spawned_task_backend(Some("claude"), None),
            &config,
        )
        .unwrap();
        assert_eq!(pinned.selection.backend, "claude");
        assert_eq!(pinned.source, ResolutionSource::AgentDefault);
    }

    /// Provenance must name the rung that ACTUALLY chose, not one whose input
    /// merely arrived. Each case below is a state where an input was supplied
    /// and then ignored, so crediting it would tell the operator a false cause.
    #[test]
    fn provenance_never_credits_an_input_the_resolver_ignored() {
        // An override on a sole-provider tier is a documented no-op.
        let single = single_provider_config();
        let ignored_override =
            resolve_selection_with_provenance(Some("big"), Some("codex"), None, &single).unwrap();
        assert_eq!(ignored_override.selection.backend, "claude");
        assert_eq!(
            ignored_override.source,
            ResolutionSource::SoleProvider,
            "an override the tier ignores must not be reported as having decided"
        );

        // An agent preference naming a backend that does not define the tier is
        // skipped; the tier's own default decides instead.
        let config = test_config();
        let invalid_preference =
            resolve_selection_with_provenance(Some("md"), None, Some("ghost"), &config).unwrap();
        assert_eq!(invalid_preference.selection.backend, "claude");
        assert_eq!(
            invalid_preference.source,
            ResolutionSource::TierDefaultBackend,
            "a preference the tier cannot honor must not be reported as AgentDefault"
        );

        // An unbound multi-provider tier has no default to credit.
        let mut unbound = test_config();
        unbound.tier_defaults.remove("md");
        let fell_through =
            resolve_selection_with_provenance(Some("md"), None, None, &unbound).unwrap();
        assert_eq!(fell_through.selection.backend, "claude");
        assert_eq!(
            fell_through.source,
            ResolutionSource::FirstProvider,
            "a tier with no default binding must not claim one decided"
        );
    }

    /// A rung that cannot be honored is skipped, not treated as the end of the
    /// ladder: a stale override still lets the tier's own default decide.
    #[test]
    fn an_unhonorable_rung_is_skipped_rather_than_ending_the_ladder() {
        let mut config = test_config();
        config
            .tier_defaults
            .insert("md".to_string(), "codex".to_string());

        let resolved =
            resolve_selection_with_provenance(Some("md"), Some("ghost"), None, &config).unwrap();
        assert_eq!(resolved.selection.backend, "codex");
        assert_eq!(resolved.source, ResolutionSource::TierDefaultBackend);
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
        // The tier's own default backend (multi-provider tier, neither override
        // nor preference).
        assert_eq!(
            resolve_selection_with_provenance(Some("md"), None, None, &config)
                .unwrap()
                .source,
            ResolutionSource::TierDefaultBackend
        );
        // First provider: reached only when the tier ALSO has no usable default
        // (see `provenance_never_credits_an_input_the_resolver_ignored`), since
        // a skipped override still leaves the tier's own default its turn.
        let mut unbound = test_config();
        unbound.tier_defaults.remove("md");
        assert_eq!(
            resolve_selection_with_provenance(Some("md"), Some("ghost"), None, &unbound)
                .unwrap()
                .source,
            ResolutionSource::FirstProvider
        );
        // Sole provider (the tier is defined on exactly one backend).
        let single = single_provider_config();
        assert_eq!(
            resolve_selection_with_provenance(Some("big"), None, None, &single)
                .unwrap()
                .source,
            ResolutionSource::SoleProvider
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
            icon: None,
            bundles: Vec::new(),
            is_project_scoped: false,
            file_path: std::path::PathBuf::new(),
        }
    }
}
