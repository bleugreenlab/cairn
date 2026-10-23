//! Model token pricing, in USD per million tokens.
//!
//! Prices are public list prices as of [`PRICE_SOURCE_DATE`] and drift over
//! time, so treat this table as an estimate. Models are classified from the
//! stored `jobs.model` value by substring so both short aliases (`opus`,
//! `sonnet`, `fable`) and dated full names (`claude-sonnet-4-20250514`,
//! `gpt-5.6-sol`) resolve to a tier. Classification is version-aware where pricing
//! changed across versions: each Claude family reads its generation with
//! [`claude_generation`], so Opus 4/4.1, Opus 4.5 to 5.0 and Opus 5.5+ are three
//! prices, and Sonnet and Haiku each split at 5.5. A bare Claude alias is priced
//! as the generation the current CLI resolves it to (5.5); an executor on an
//! older CLI runs the alias on an older, dearer model, which this underprices.
//! Truly unrecognized models return `None` and are reported as unpriced.
//!
//! GPT tiers are OpenAI's Standard-processing list prices from
//! <https://developers.openai.com/api/docs/pricing>. A temporary promotion is
//! not a list price: that page shows GPT-5.6 Sol at a promotional rate (through
//! at least 2026-11-21), so its tier keeps the $5 / $30 launch price from
//! <https://openai.com/index/gpt-5-6/>. GPT-5.6 Terra and Luna take the page's
//! current rate, below their launch prices, because the page does not mark it
//! as promotional. Most GPT-5.4-and-later models bill a request over 272k input
//! tokens at a long-context tier (2x input, 1.5x output); as with Haiku 5.5's
//! tier below, request length is gone by the time rows are rolled up, so every
//! GPT request is priced at the short-context tier.
//!
//! For Claude agent sessions this table is the only cost source: they never
//! record `events.cost_usd`, because a subscription's per-turn cost figure is
//! notional. Only a one-shot completion on an API-key account records the CLI's
//! own `total_cost_usd`, and [`crate::cost`] prefers that recorded figure.
//!
//! Haiku 5.5 is priced by each request's prompt length (input + cache reads +
//! cache writes): $0.10 / $0.50 up to 100k tokens, five times that above. This
//! table prices rows already summed per (hour, run, model) in `token_rollup`,
//! where request length is gone, so Haiku 5.5 is priced at the up-to-100k tier
//! throughout. That underprices long sessions: on 2026-10-19, 37% of this
//! install's `haiku` agent requests since the 5.5 launch were over 100k, and
//! their exact tiered cost was about 2.9x this estimate. Pricing each request
//! at its own tier needs the rollup to keep long-prompt components apart
//! (cairn/5218).
//!
//! `cache_write` is Claude's 5-minute cache-write rate, and OpenAI's published
//! cache-write rate for GPT tiers. Codex usage is normalized into disjoint
//! uncached input and cache reads (`cairn-llm-wire`'s `codex_app_server::usage`)
//! but carries no cache writes, so a GPT tier's `cache_write` is unused for
//! Codex sessions: tokens OpenAI bills as cache writes (1.25x input from
//! GPT-5.6 on) are priced here at the plain input rate.

use cairn_models_types::catalog::claude_generation;

/// Date the price table was last reviewed against public list prices.
pub(crate) const PRICE_SOURCE_DATE: &str = "2026-10-20";

/// Per-million-token USD prices for one model tier.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelPrice {
    input: f64,
    cache_read: f64,
    cache_write: f64,
    output: f64,
}

impl ModelPrice {
    const fn new(input: f64, cache_read: f64, cache_write: f64, output: f64) -> Self {
        Self {
            input,
            cache_read,
            cache_write,
            output,
        }
    }
}

// Claude tiers.
const FABLE: ModelPrice = ModelPrice::new(10.0, 1.0, 12.50, 50.0); // Fable 5 / 5.1
const OPUS_5_5: ModelPrice = ModelPrice::new(4.0, 0.40, 5.0, 20.0); // Opus 5.5+
const OPUS_4_5: ModelPrice = ModelPrice::new(5.0, 0.50, 6.25, 25.0); // Opus 4.5 to 5
const OPUS_LEGACY: ModelPrice = ModelPrice::new(15.0, 1.50, 18.75, 75.0); // Opus 4 / 4.1
const SONNET_5_5: ModelPrice = ModelPrice::new(2.0, 0.20, 2.50, 10.0); // Sonnet 5.5+
const SONNET_4: ModelPrice = ModelPrice::new(3.0, 0.30, 3.75, 15.0); // Sonnet 4 to 5
/// Haiku 5.5 at its up-to-100k-token prompt tier; see the module doc.
const HAIKU_5_5: ModelPrice = ModelPrice::new(0.10, 0.01, 0.125, 0.50);
const HAIKU_4_5: ModelPrice = ModelPrice::new(1.0, 0.10, 1.25, 5.0); // Haiku 4.5

// GPT / Codex tiers.
const GPT_6_ASTRA: ModelPrice = ModelPrice::new(10.0, 1.0, 12.50, 50.0);
const GPT_6_1_SOL: ModelPrice = ModelPrice::new(2.0, 0.10, 2.50, 10.0);
const GPT_6_SOL: ModelPrice = ModelPrice::new(2.0, 0.20, 2.50, 10.0);
const GPT_6_LUNA: ModelPrice = ModelPrice::new(0.10, 0.01, 0.125, 0.50);
const GPT_5_6_SOL: ModelPrice = ModelPrice::new(5.0, 0.50, 6.25, 30.0);
const GPT_5_6_TERRA: ModelPrice = ModelPrice::new(2.0, 0.20, 2.50, 12.0);
const GPT_5_6_LUNA: ModelPrice = ModelPrice::new(0.20, 0.02, 0.25, 1.20);
const GPT_5_5: ModelPrice = ModelPrice::new(5.0, 0.50, 5.0, 30.0);
const GPT_5_4: ModelPrice = ModelPrice::new(2.50, 0.25, 2.50, 15.0);
const GPT_5_4_MINI: ModelPrice = ModelPrice::new(0.75, 0.075, 0.75, 4.50);

/// Resolve a `jobs.model` value to its price tier, or `None` when the model is
/// unrecognized.
fn price_for(model: Option<&str>) -> Option<ModelPrice> {
    let model = model?.to_ascii_lowercase();
    // Order matters: match the most specific aliases first.
    if model.contains("fable") || model.contains("mythos") {
        Some(FABLE)
    } else if model.contains("opus") {
        Some(if at_least(&model, "opus", (5, 5)) {
            OPUS_5_5
        } else if at_least(&model, "opus", (4, 5)) {
            OPUS_4_5
        } else {
            OPUS_LEGACY
        })
    } else if model.contains("sonnet") {
        Some(if at_least(&model, "sonnet", (5, 5)) {
            SONNET_5_5
        } else {
            SONNET_4
        })
    } else if model.contains("haiku") {
        Some(if at_least(&model, "haiku", (5, 5)) {
            HAIKU_5_5
        } else {
            HAIKU_4_5
        })
    } else if model.contains("gpt") || model.contains("codex") {
        Some(
            // `gpt-6.1-*` also contains `gpt-6`, so the point release goes first.
            if model.contains("gpt-6.1") || model.contains("gpt-6-1") {
                // Sol is the only 6.1 model.
                GPT_6_1_SOL
            } else if model.contains("gpt-6") {
                if model.contains("-astra") {
                    GPT_6_ASTRA
                } else if model.contains("-luna") {
                    GPT_6_LUNA
                } else {
                    GPT_6_SOL
                }
            } else if model.contains("5.6-luna") || model.contains("5-6-luna") {
                GPT_5_6_LUNA
            } else if model.contains("5.6-terra") || model.contains("5-6-terra") {
                GPT_5_6_TERRA
            } else if model.contains("5.6-sol")
                || model.contains("5-6-sol")
                || model.contains("5.6")
                || model.contains("5-6")
            {
                GPT_5_6_SOL
            } else if model.contains("mini") {
                GPT_5_4_MINI
            } else if model.contains("5.5") || model.contains("5-5") {
                GPT_5_5
            } else {
                // Bare `gpt-5` / `gpt-5.4` / codex default to the 5.4 tier.
                GPT_5_4
            },
        )
    } else {
        None
    }
}

/// Whether `model`, a lowercased name in `family`, is at least `generation`.
/// The bare alias (`opus`, `opus[1m]`) is the current generation; any other
/// name without a readable family-first version (`claude-3-5-haiku-…`) is an
/// old one.
fn at_least(model: &str, family: &str, generation: (u32, u32)) -> bool {
    match claude_generation(model) {
        Some(version) => version >= generation,
        None => model.trim_end_matches("[1m]") == family,
    }
}

/// Compute USD cost for a normalized token-component bundle. Persisted
/// components are disjoint, so every backend uses the same componentwise math.
pub(crate) fn cost_usd(
    _backend: &str,
    model: Option<&str>,
    input: i64,
    cache_read: i64,
    cache_create: i64,
    output: i64,
) -> f64 {
    let Some(price) = price_for(model) else {
        return 0.0;
    };
    let per = 1_000_000.0;
    (input as f64 * price.input
        + cache_read as f64 * price.cache_read
        + cache_create as f64 * price.cache_write
        + output as f64 * price.output)
        / per
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_aliases_price_as_the_generation_the_cli_resolves_them_to() {
        assert_eq!(price_for(Some("fable")), Some(FABLE));
        assert_eq!(price_for(Some("claude-fable-5-1")), Some(FABLE));
        assert_eq!(price_for(Some("opus")), Some(OPUS_5_5));
        assert_eq!(price_for(Some("opus[1m]")), Some(OPUS_5_5));
        assert_eq!(price_for(Some("sonnet")), Some(SONNET_5_5));
        assert_eq!(price_for(Some("haiku")), Some(HAIKU_5_5));
    }

    #[test]
    fn each_claude_family_is_priced_by_generation() {
        // 5.5: the published 2026-10 list prices.
        assert_eq!(price_for(Some("claude-opus-5-5")), Some(OPUS_5_5));
        assert_eq!(price_for(Some("claude-opus-5-5[1m]")), Some(OPUS_5_5));
        assert_eq!(price_for(Some("claude-sonnet-5-5")), Some(SONNET_5_5));
        assert_eq!(price_for(Some("claude-haiku-5-5")), Some(HAIKU_5_5));
        // 4.x and 5.0 keep their own prices.
        assert_eq!(price_for(Some("claude-opus-5")), Some(OPUS_4_5));
        assert_eq!(price_for(Some("claude-opus-4-5")), Some(OPUS_4_5));
        assert_eq!(price_for(Some("claude-sonnet-5")), Some(SONNET_4));
        assert_eq!(
            price_for(Some("claude-sonnet-4-5-20250929")),
            Some(SONNET_4)
        );
        assert_eq!(price_for(Some("claude-sonnet-4-20250514")), Some(SONNET_4));
        assert_eq!(
            price_for(Some("claude-haiku-4-5-20251001")),
            Some(HAIKU_4_5)
        );
        // A version-first name is an old model, not the current alias.
        assert_eq!(
            price_for(Some("claude-3-5-haiku-20241022")),
            Some(HAIKU_4_5)
        );
    }

    #[test]
    fn legacy_opus_is_priced_higher() {
        assert_eq!(
            price_for(Some("claude-opus-4-1-20250805")),
            Some(OPUS_LEGACY)
        );
        assert_eq!(price_for(Some("claude-opus-4-20250514")), Some(OPUS_LEGACY));
        assert_eq!(price_for(Some("claude-opus-4.1")), Some(OPUS_LEGACY));
    }

    /// The 5.5 rates agree with the CLI's own `modelUsage[..].costUSD`, observed
    /// from CLI 2.1.302 for a 1-input, 3-output-token turn.
    #[test]
    fn five_five_prices_match_the_clis_own_cost() {
        for (model, cli_cost) in [
            ("claude-haiku-5-5", 0.000_001_6),
            ("claude-sonnet-5-5", 0.000_032),
            ("claude-opus-5-5", 0.000_064),
            ("claude-fable-5-1", 0.000_16),
        ] {
            let cost = cost_usd("claude", Some(model), 1, 0, 0, 3);
            assert!((cost - cli_cost).abs() < 1e-12, "{model}: {cost}");
        }
    }

    #[test]
    fn gpt_versions_and_mini() {
        assert_eq!(price_for(Some("gpt-5.6-sol")), Some(GPT_5_6_SOL));
        assert_eq!(price_for(Some("gpt-5.6-terra")), Some(GPT_5_6_TERRA));
        assert_eq!(price_for(Some("gpt-5.6-luna")), Some(GPT_5_6_LUNA));
        assert_eq!(price_for(Some("gpt-5.5")), Some(GPT_5_5));
        assert_eq!(price_for(Some("gpt-5.4-mini")), Some(GPT_5_4_MINI));
        assert_eq!(price_for(Some("gpt-5")), Some(GPT_5_4));
        assert_eq!(price_for(Some("gpt-5-codex")), Some(GPT_5_4));
    }

    #[test]
    fn gpt_six_family_is_priced_by_size() {
        assert_eq!(price_for(Some("gpt-6.1-sol")), Some(GPT_6_1_SOL));
        assert_eq!(price_for(Some("gpt-6-astra")), Some(GPT_6_ASTRA));
        assert_eq!(price_for(Some("gpt-6-sol")), Some(GPT_6_SOL));
        assert_eq!(price_for(Some("gpt-6-luna")), Some(GPT_6_LUNA));
        // None of them falls through to the bare-`gpt` default.
        for model in ["gpt-6.1-sol", "gpt-6-astra", "gpt-6-sol", "gpt-6-luna"] {
            assert_ne!(price_for(Some(model)), Some(GPT_5_4), "{model}");
        }
    }

    /// `gpt-6.1-sol` contains `gpt-6`; it must not take GPT-6 Sol's dearer
    /// cached-input rate, which is the only field where the two differ.
    #[test]
    fn gpt_six_one_is_not_classified_as_gpt_six() {
        assert_ne!(GPT_6_1_SOL, GPT_6_SOL);
        assert_ne!(price_for(Some("gpt-6.1-sol")), Some(GPT_6_SOL));
        assert_eq!(price_for(Some("gpt-6-1-sol")), Some(GPT_6_1_SOL));
        assert_eq!(price_for(Some("gpt-6.1")), Some(GPT_6_1_SOL));
    }

    #[test]
    fn unknown_models_are_unpriced() {
        assert_eq!(price_for(Some("mystery-model")), None);
        assert_eq!(price_for(None), None);
        assert_eq!(cost_usd("claude", Some("mystery"), 1000, 0, 0, 1000), 0.0);
    }

    #[test]
    fn codex_cost_prices_cache_components() {
        // gpt-5.4: 1M input($2.50) + 0.5M cache read($0.125)
        // + 0.5M cache write($1.25) + 1M output($15).
        let cost = cost_usd(
            "codex",
            Some("gpt-5"),
            1_000_000,
            500_000,
            500_000,
            1_000_000,
        );
        assert!((cost - 18.875).abs() < 1e-9, "got {cost}");
    }

    #[test]
    fn claude_cost_prices_each_component() {
        // sonnet 5.5: 1M input(2) + 1M cache_read(0.20) + 1M cache_write(2.50) + 1M output(10)
        let cost = cost_usd(
            "claude",
            Some("sonnet"),
            1_000_000,
            1_000_000,
            1_000_000,
            1_000_000,
        );
        assert!((cost - 14.70).abs() < 1e-9, "got {cost}");
    }
}
