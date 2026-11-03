//! The tier x provider grid Cairn shipped before pools and the ladder.
//!
//! A workspace that never edited a cell still resolved against these tables,
//! so the one-time conversion to `models` has to read them to see the grid the
//! workspace actually had: the effective grid is these defaults with the
//! user's file laid over them. They are frozen input to that conversion, not
//! configuration anyone edits, and they stay exactly as shipped so a
//! conversion run later converts the same grid one run earlier would have.

use std::collections::HashMap;

use crate::models::{Model, Preset, PresetOptionValue};

/// The tiers a workspace had when its file named none.
pub const LEGACY_TIERS: &[&str] = &["sm", "md", "lg"];

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

fn cell(model: &str, effort: Option<&str>) -> Preset {
    Preset {
        model: Model::new(model),
        options: reasoning_options(effort),
    }
}

fn tiers(cells: [(&str, Preset); 3]) -> HashMap<String, Preset> {
    cells
        .into_iter()
        .map(|(tier, preset)| (tier.to_string(), preset))
        .collect()
}

/// Claude's cells.
///
/// `legacy_thinking_enabled` is the deprecated workspace `maxThinkingTokens`:
/// when it is set (the historical default), Sonnet and Opus default to `high`
/// effort; otherwise effort is left to the CLI.
pub fn default_claude_presets(legacy_thinking_enabled: Option<i32>) -> HashMap<String, Preset> {
    let effort = legacy_thinking_enabled.map(|_| "high");
    tiers([
        ("sm", cell(Model::HAIKU, None)),
        ("md", cell(Model::SONNET, effort)),
        ("lg", cell(Model::OPUS, effort)),
    ])
}

fn default_codex_presets() -> HashMap<String, Preset> {
    tiers([
        ("sm", cell(Model::GPT_5_6_LUNA, Some("low"))),
        ("md", cell(Model::GPT_5_6_TERRA, Some("medium"))),
        ("lg", cell(Model::GPT_5_6_SOL, Some("ultra"))),
    ])
}

fn default_openrouter_presets() -> HashMap<String, Preset> {
    tiers([
        ("sm", cell("openrouter/auto", None)),
        (
            "md",
            cell("~anthropic/claude-sonnet-latest", Some("medium")),
        ),
        ("lg", cell("~openai/gpt-latest", Some("high"))),
    ])
}

/// OpenCode Go's cells.
///
/// Go prices models against one shared dollar budget, so the tiers were chosen
/// by what each costs against it as much as by capability: `sm` the cheap model
/// with effectively unlimited headroom, `md` the workhorse on the larger monthly
/// allowance, `lg` the strongest model with the smallest allowance. Efforts are
/// each model's own vocabulary; Go's models do not share one. The DeepSeek
/// models are absent because they are served only from China-hosted
/// infrastructure behind a per-workspace opt-in, so a default pointing at one
/// fails for anyone who has not opted in.
fn default_opencode_presets() -> HashMap<String, Preset> {
    tiers([
        ("sm", cell("mimo-v2.5", None)),
        ("md", cell("glm-5.2", Some("high"))),
        ("lg", cell("kimi-k3", Some("max"))),
    ])
}

/// Every shipped provider's cells, keyed by provider then tier.
pub fn default_grid(max_thinking: Option<i32>) -> HashMap<String, HashMap<String, Preset>> {
    HashMap::from([
        ("claude".to_string(), default_claude_presets(max_thinking)),
        ("codex".to_string(), default_codex_presets()),
        ("openrouter".to_string(), default_openrouter_presets()),
        (
            cairn_models_types::catalog::OPENCODE_BACKEND_KEY.to_string(),
            default_opencode_presets(),
        ),
    ])
}

/// One backend as the default of every named tier: what a legacy global
/// `activeBackend` meant, and so how such a config is read.
pub fn tier_defaults_from_single_backend(
    backend: &str,
    tiers: &[String],
) -> HashMap<String, String> {
    tiers
        .iter()
        .map(|tier| (tier.clone(), backend.to_string()))
        .collect()
}
