//! Model token pricing, in USD per million tokens.
//!
//! Prices are public list prices as of [`PRICE_SOURCE_DATE`] and drift over
//! time, so treat this table as an estimate. Models are classified from the
//! stored `jobs.model` value by substring so both short aliases (`opus`,
//! `sonnet`, `fable`) and dated full names (`claude-sonnet-4-20250514`,
//! `gpt-5.6-sol`) resolve to a tier. Classification is version-aware where pricing
//! changed across versions (legacy Opus 4/4.1 cost 3x the current Opus 4.5+).
//! Truly unrecognized models return `None` and are reported as unpriced.
//!
//! `cache_write` is the 5-minute cache-write rate. Codex cost ignores cache
//! entirely (its `input` already subsumes cache), so the cache fields are unused
//! for codex tiers.

/// Date the price table was last reviewed against public list prices.
pub(crate) const PRICE_SOURCE_DATE: &str = "2026-07-09";

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
const FABLE: ModelPrice = ModelPrice::new(10.0, 1.0, 12.50, 50.0);
const OPUS_CURRENT: ModelPrice = ModelPrice::new(5.0, 0.50, 6.25, 25.0); // Opus 4.5+
const OPUS_LEGACY: ModelPrice = ModelPrice::new(15.0, 1.50, 18.75, 75.0); // Opus 4 / 4.1
const SONNET: ModelPrice = ModelPrice::new(3.0, 0.30, 3.75, 15.0);
const HAIKU: ModelPrice = ModelPrice::new(1.0, 0.10, 1.25, 5.0); // Haiku 4.5

// GPT-5 / Codex tiers.
const GPT_5_6_SOL: ModelPrice = ModelPrice::new(5.0, 0.50, 6.25, 30.0);
const GPT_5_6_TERRA: ModelPrice = ModelPrice::new(2.50, 0.25, 3.125, 15.0);
const GPT_5_6_LUNA: ModelPrice = ModelPrice::new(1.0, 0.10, 1.25, 6.0);
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
        Some(if is_legacy_opus(&model) {
            OPUS_LEGACY
        } else {
            OPUS_CURRENT
        })
    } else if model.contains("sonnet") {
        Some(SONNET)
    } else if model.contains("haiku") {
        Some(HAIKU)
    } else if model.contains("gpt") || model.contains("codex") {
        Some(
            if model.contains("5.6-luna") || model.contains("5-6-luna") {
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

/// Legacy Opus 4 and 4.1 were priced 3x the current Opus 4.5+ line. Detect the
/// original dated build (`opus-4-2025…`) and the 4.1 line; everything else
/// (bare `opus`, `opus-4.5`+) uses the current price.
fn is_legacy_opus(model: &str) -> bool {
    model.contains("opus-4-1")
        || model.contains("opus-4.1")
        || model.contains("opus-4-2025")
        || model.contains("opus-4-0")
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
    fn classifies_short_and_dated_aliases() {
        assert_eq!(price_for(Some("fable")), Some(FABLE));
        assert_eq!(
            price_for(Some("claude-sonnet-4-20250514")),
            price_for(Some("sonnet"))
        );
        // Bare alias and current dated build share the current Opus price.
        assert_eq!(price_for(Some("opus")), Some(OPUS_CURRENT));
        assert_eq!(price_for(Some("claude-opus-4-5")), Some(OPUS_CURRENT));
    }

    #[test]
    fn legacy_opus_is_priced_higher() {
        assert_eq!(
            price_for(Some("claude-opus-4-1-20250805")),
            Some(OPUS_LEGACY)
        );
        assert_eq!(price_for(Some("claude-opus-4-20250514")), Some(OPUS_LEGACY));
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
        // sonnet: 1M input(3) + 1M cache_read(0.30) + 1M cache_write(3.75) + 1M output(15)
        let cost = cost_usd(
            "claude",
            Some("sonnet"),
            1_000_000,
            1_000_000,
            1_000_000,
            1_000_000,
        );
        assert!((cost - 22.05).abs() < 1e-9, "got {cost}");
    }
}
