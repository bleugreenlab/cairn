//! Reasoning effort as a position on each model's own scale, and the lab that
//! trained a model.
//!
//! A requirement names effort as a number `x` in 0..1, never as a level name,
//! because the same word means different things on different models. Each
//! provider turns `x` into one of its own levels for one model
//! ([`CatalogProvider::effort`]), and [`CatalogProvider::position`] is the
//! inverse that puts a hand-chosen level back on the same 0..1 scale. Both
//! read one ordered list, [`CatalogProvider::effort_scale`], so they cannot
//! disagree.
//!
//! With `n` levels, `x` lands on level `min(n - 1, floor(x * n))`, and level
//! `i` sits at `(i + 0.5) / n`, the middle of its slice. So the round trip
//! `effort(position(level)) == level` holds for every level, and the whole of
//! 0..1 is covered with no gaps.

use cairn_domain::RuntimeExtras;
use cairn_models_types::catalog::{openrouter_maker, DiscoveredModel};
use serde::Serialize;

use super::CatalogProvider;

/// One model's reasoning levels on one provider, lowest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EffortScale {
    pub levels: Vec<String>,
}

impl EffortScale {
    /// The level `x` lands on. `x` outside 0..1 is clamped, and NaN reads as 0.
    pub fn at(&self, x: f64) -> &str {
        let n = self.levels.len();
        let x = if x.is_nan() { 0.0 } else { x.clamp(0.0, 1.0) };
        let index = ((x * n as f64).floor() as usize).min(n - 1);
        &self.levels[index]
    }

    /// Where `level` sits on 0..1, or `None` when it is not on this scale.
    pub fn position(&self, level: &str) -> Option<f64> {
        let index = self.levels.iter().position(|known| known == level)?;
        Some((index as f64 + 0.5) / self.levels.len() as f64)
    }
}

/// Effort words in the order every discovery source Cairn reads lists them
/// (Codex's model listing, OpenRouter's `supported_efforts`, models.dev's
/// `reasoning_options`). Used only to restore that order within one model's
/// own list when a source returns it shuffled; a list containing any other
/// word keeps the order discovery gave it.
const DISCOVERY_ORDER: &[&str] = &[
    "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
];

fn ordered(mut levels: Vec<String>) -> Vec<String> {
    let mut seen = Vec::new();
    levels.retain(|level| {
        let fresh = !seen.contains(level);
        seen.push(level.clone());
        fresh
    });
    let rank = |level: &String| DISCOVERY_ORDER.iter().position(|known| known == level);
    if levels.iter().all(|level| rank(level).is_some()) {
        levels.sort_by_key(|level| rank(level));
    }
    levels
}

impl CatalogProvider {
    /// `model`'s ordered effort levels here, or `None` when it takes no effort.
    ///
    /// A provider that lists levels per model ([`EffortVocabulary::per_model`])
    /// answers with a discovered model's own list, and with its static levels
    /// only for a model discovery has not reported. A mode the provider
    /// declares is never on the scale.
    ///
    /// [`EffortVocabulary::per_model`]: cairn_models_types::catalog::EffortVocabulary::per_model
    pub fn effort_scale(&self, model: &str) -> Option<EffortScale> {
        let vocabulary = self.descriptor.effort;
        let levels: Vec<String> = match self.discovered(model) {
            Some(discovered) if vocabulary.per_model => discovered
                .supported_reasoning_efforts
                .iter()
                .map(|effort| effort.reasoning_effort.clone())
                .collect(),
            _ => vocabulary.levels.iter().map(ToString::to_string).collect(),
        };
        let levels: Vec<String> = ordered(levels)
            .into_iter()
            .filter(|level| !vocabulary.modes.contains(&level.as_str()))
            .collect();
        (!levels.is_empty()).then_some(EffortScale { levels })
    }

    /// The launch options that run `model` at effort `x` (0..1), or `None` for
    /// a model that takes no effort. The bag is the same one a launch reads
    /// everywhere else.
    pub fn effort(&self, model: &str, x: f64) -> Option<RuntimeExtras> {
        let scale = self.effort_scale(model)?;
        Some(RuntimeExtras {
            reasoning_effort: Some(scale.at(x).to_string()),
            ..RuntimeExtras::default()
        })
    }

    /// Where `level` sits on `model`'s 0..1 scale here, or `None` when it is
    /// not one of that model's levels (a mode such as Claude's `max`, or a
    /// word another model uses).
    pub fn position(&self, model: &str, level: &str) -> Option<f64> {
        self.effort_scale(model)?.position(level)
    }

    /// What discovery says `model` costs here, when it priced it.
    pub fn pricing(&self, model: &str) -> Option<&cairn_models_types::catalog::ModelPricing> {
        self.discovered(model)?.pricing.as_ref()
    }

    /// The lab that trained `model`, which is not the route: Claude through
    /// OpenRouter is Anthropic's. Discovery's answer first, then the
    /// provider's single maker, then (on OpenRouter) the id's prefix. `None`
    /// when nothing can tell, which a constraint between makers treats as
    /// unproven rather than as different.
    pub fn maker(&self, model: &str) -> Option<String> {
        self.discovered(model)
            .and_then(|discovered: &DiscoveredModel| discovered.maker.clone())
            .or_else(|| self.descriptor.maker.as_deref().map(str::to_string))
            .or_else(|| (self.descriptor.key == "openrouter").then(|| openrouter_maker(model))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models_types::catalog::{
        claude_models, descriptor, CustomProvider, DiscoveredReasoningEffort, ProviderModelCatalog,
        OPENCODE_BACKEND_KEY,
    };

    fn discovered(id: &str, efforts: &[&str], maker: Option<&str>) -> DiscoveredModel {
        let mut model = claude_models().remove(0);
        model.id = id.to_string();
        model.model = id.to_string();
        model.maker = maker.map(str::to_string);
        model.supported_reasoning_efforts = efforts
            .iter()
            .map(|effort| DiscoveredReasoningEffort {
                reasoning_effort: effort.to_string(),
                description: None,
            })
            .collect();
        model
    }

    fn provider(key: &str, models: Vec<DiscoveredModel>) -> CatalogProvider {
        let descriptor = descriptor(key).cloned().unwrap_or_else(|| {
            cairn_models_types::catalog::custom_descriptor(&CustomProvider {
                account_id: key.to_string(),
                display_name: key.to_string(),
            })
        });
        CatalogProvider {
            descriptor,
            installed: true,
            models: Some(ProviderModelCatalog {
                backend: key.to_string(),
                models,
                options: Vec::new(),
                refreshed_at: Some(1),
                error: None,
            }),
            serves: Vec::new(),
            unable: Vec::new(),
        }
    }

    /// One fixture per backend kind, each with the models whose scales the
    /// property test walks.
    fn fixtures() -> Vec<(CatalogProvider, Vec<&'static str>)> {
        vec![
            (
                provider("claude", claude_models()),
                vec!["opus", "claude-sonnet-5-5"],
            ),
            (
                provider(
                    "codex",
                    vec![
                        discovered(
                            "gpt-5.6-sol",
                            &["low", "medium", "high", "xhigh", "ultra"],
                            None,
                        ),
                        discovered("gpt-5.6-luna", &["low", "medium", "high"], None),
                    ],
                ),
                vec!["gpt-5.6-sol", "gpt-5.6-luna", "gpt-undiscovered"],
            ),
            (
                provider(
                    "openrouter",
                    vec![
                        discovered(
                            "anthropic/claude-sonnet-4.5",
                            &["high", "low", "medium"],
                            None,
                        ),
                        discovered("moonshotai/kimi-k3", &["none", "low", "high", "max"], None),
                    ],
                ),
                vec!["anthropic/claude-sonnet-4.5", "moonshotai/kimi-k3"],
            ),
            (
                provider(
                    OPENCODE_BACKEND_KEY,
                    vec![
                        discovered(
                            "kimi-k3",
                            &["none", "low", "high", "max"],
                            Some("moonshotai"),
                        ),
                        discovered("glm-5.3", &["low", "high"], Some("z-ai")),
                    ],
                ),
                vec!["kimi-k3", "glm-5.3", "unpublished"],
            ),
        ]
    }

    /// The shared contract every provider's effort function meets.
    #[test]
    fn every_providers_effort_is_monotonic_total_mode_free_and_inverts() {
        for (provider, models) in fixtures() {
            let key = provider.descriptor.key.to_string();
            let modes = provider.descriptor.effort.modes;
            for model in models {
                let scale = provider
                    .effort_scale(model)
                    .unwrap_or_else(|| panic!("{key}/{model} has a scale"));
                let rank = |x: f64| {
                    let level = provider
                        .effort(model, x)
                        .and_then(|extras| extras.reasoning_effort)
                        .unwrap_or_else(|| panic!("{key}/{model} is defined at {x}"));
                    assert!(
                        !modes.contains(&level.as_str()),
                        "{key}/{model} at {x} returned mode {level}"
                    );
                    scale
                        .levels
                        .iter()
                        .position(|known| *known == level)
                        .expect("on its scale")
                };
                let mut previous = 0;
                let mut reached = vec![false; scale.levels.len()];
                for step in 0..=1000 {
                    let x = step as f64 / 1000.0;
                    let index = rank(x);
                    assert!(
                        index >= previous,
                        "{key}/{model} fell from {previous} to {index} at {x}"
                    );
                    previous = index;
                    reached[index] = true;
                }
                assert_eq!(rank(0.0), 0, "{key}/{model} starts at its lowest level");
                assert_eq!(
                    rank(1.0),
                    scale.levels.len() - 1,
                    "{key}/{model} ends at its highest"
                );
                assert!(
                    reached.iter().all(|hit| *hit),
                    "{key}/{model} reaches every level"
                );
                for level in &scale.levels {
                    let x = provider
                        .position(model, level)
                        .expect("a level has a position");
                    let back = provider
                        .effort(model, x)
                        .and_then(|extras| extras.reasoning_effort);
                    assert_eq!(
                        back.as_deref(),
                        Some(level.as_str()),
                        "{key}/{model} round trip"
                    );
                }
                for mode in modes {
                    assert_eq!(
                        provider.position(model, mode),
                        None,
                        "{key}/{model}: {mode} is a mode"
                    );
                }
            }
        }
    }

    #[test]
    fn claude_max_is_a_mode_and_xhigh_is_the_top() {
        let claude = provider("claude", claude_models());
        let scale = claude.effort_scale("opus").expect("claude takes effort");
        assert_eq!(scale.levels, ["low", "medium", "high", "xhigh"]);
        assert_eq!(claude.position("opus", "max"), None);
        assert_eq!(claude.position("opus", "xhigh"), Some(0.875));
    }

    #[test]
    fn a_discovered_scale_is_the_models_own_and_kimis_max_is_a_level() {
        let (codex, _) = fixtures().remove(1);
        assert_eq!(
            codex
                .effort_scale("gpt-5.6-sol")
                .unwrap()
                .levels
                .last()
                .unwrap(),
            "ultra"
        );
        assert_eq!(
            codex.effort_scale("gpt-5.6-luna").unwrap().levels,
            ["low", "medium", "high"]
        );
        // A model discovery has not listed takes the provider's static scale.
        assert_eq!(
            codex.effort_scale("gpt-undiscovered").unwrap().levels,
            ["low", "medium", "high", "xhigh"]
        );
        let (opencode, _) = fixtures().remove(3);
        assert_eq!(
            opencode
                .effort("kimi-k3", 1.0)
                .unwrap()
                .reasoning_effort
                .as_deref(),
            Some("max")
        );
        // A shuffled discovery list is restored to its own order.
        let (openrouter, _) = fixtures().remove(2);
        assert_eq!(
            openrouter
                .effort_scale("anthropic/claude-sonnet-4.5")
                .unwrap()
                .levels,
            ["low", "medium", "high"]
        );
    }

    #[test]
    fn a_model_without_effort_has_no_scale() {
        let openrouter = provider(
            "openrouter",
            vec![discovered("meta-llama/llama-4", &[], None)],
        );
        assert_eq!(openrouter.effort_scale("meta-llama/llama-4"), None);
        assert_eq!(openrouter.effort("meta-llama/llama-4", 0.5), None);
        // OpenRouter has no static scale for a model it has not listed.
        assert_eq!(openrouter.effort_scale("x-ai/grok-9"), None);
        let custom = provider("lab-box", vec![discovered("qwen3", &[], None)]);
        assert_eq!(custom.effort_scale("qwen3"), None);
    }

    #[test]
    fn maker_is_the_lab_not_the_route() {
        let claude = provider("claude", claude_models());
        assert_eq!(claude.maker("opus").as_deref(), Some("anthropic"));
        let codex = provider("codex", Vec::new());
        assert_eq!(codex.maker("gpt-5.6-sol").as_deref(), Some("openai"));
        let openrouter = provider("openrouter", vec![discovered("openrouter/auto", &[], None)]);
        assert_eq!(
            openrouter
                .maker("~anthropic/claude-sonnet-latest")
                .as_deref(),
            Some("anthropic")
        );
        assert_eq!(openrouter.maker("openrouter/auto"), None);
        let (opencode, _) = fixtures().remove(3);
        assert_eq!(opencode.maker("glm-5.3").as_deref(), Some("z-ai"));
        assert_eq!(opencode.maker("unpublished"), None);
        let custom = provider("lab-box", vec![discovered("qwen3", &[], None)]);
        assert_eq!(custom.maker("qwen3"), None);
    }
}
