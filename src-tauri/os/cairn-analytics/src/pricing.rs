//! Model token pricing, in USD per million tokens.
//!
//! Prices are public list prices as of [`PRICE_SOURCE_DATE`] and drift over
//! time, so treat this table as an estimate. Pricing is an exact table keyed
//! by canonical model id from `cairn_models_types::identity`. Stored model
//! names resolve through those identity tables, never by substring. A Claude
//! CLI alias is priced as the generation [`ALIAS_GENERATIONS`] names; an
//! executor on an older CLI can run it on an older, dearer model, which this
//! underprices. A model with no canonical id, or no price row, is unpriced.
//!
//! GPT tiers are OpenAI's Standard-processing list prices from
//! <https://developers.openai.com/api/docs/pricing>. A temporary promotion is
//! not a list price: that page shows GPT-5.6 Sol at a promotional rate (through
//! 2026-11-30), so its tier keeps the $5 / $30 launch price from
//! <https://openai.com/index/gpt-5-6/>. GPT-5.6 Terra and Luna take the page's
//! current rate, below their launch prices, because the page does not mark it
//! as promotional. Most GPT-5.4-and-later models bill a request over 272k input
//! tokens at a long-context tier (2x input, 1.5x output). That is a different
//! threshold from the one the rollup keeps (below), so every GPT request is
//! priced at the short-context tier.
//!
//! For Claude agent sessions this table is the only cost source: they never
//! record `events.cost_usd`, because a subscription's per-turn cost figure is
//! notional. Only a one-shot completion on an API-key account records the CLI's
//! own `total_cost_usd`, and [`crate::cost`] prefers that recorded figure.
//!
//! # Prompt-length tiers
//!
//! Some list prices depend on each request's prompt length (input + cache
//! reads + cache writes). Rows are priced after `token_rollup` has summed them
//! per (hour, run, session, model, backend, agent), where request length is
//! gone, so the fold keeps it in the one form pricing needs: alongside each
//! component sum it stores the part contributed by requests whose prompt
//! exceeded [`LONG_PROMPT_TOKENS`] (the `long_*_tokens` columns, read here as
//! [`TieredUsage::long_prompt`]). A model with a [`ModelPrice::above_long_prompt`]
//! tier prices that part at the long rates and the rest at its base rates;
//! every other model prices the whole sum at its base rates. Haiku 5.5 is the
//! one such model: $0.10 / $0.50 up to 100k prompt tokens, five times that
//! above.
//!
//! The threshold belongs to this table, not to the schema: the fold reads
//! [`LONG_PROMPT_TOKENS`] when it splits events. Moving it means re-folding
//! (clear `token_rollup_runs` and reopen the historical backfill, as the
//! migration that added the columns did). A second threshold, such as GPT's
//! 272k, means a second set of component columns split at it, a second field
//! on [`TieredUsage`], and a tier field on [`ModelPrice`] naming which split
//! its rates apply to. The two splits nest (a request over 272k is also over
//! 100k), so each tier prices the difference between adjacent splits.
//!
//! `cache_write` is Claude's 5-minute cache-write rate, and OpenAI's published
//! cache-write rate for GPT tiers. Codex usage is normalized into disjoint
//! uncached input and cache reads (`cairn-llm-wire`'s `codex_app_server::usage`)
//! but carries no cache writes, so a GPT tier's `cache_write` is unused for
//! Codex sessions: tokens OpenAI bills as cache writes (1.25x input from
//! GPT-5.6 on) are priced here at the plain input rate.

use cairn_models_types::identity;

/// Date the price table was last reviewed against public list prices.
pub(crate) const PRICE_SOURCE_DATE: &str = "2026-11-12";

/// Prompt length (input + cache reads + cache writes) above which a request
/// is billed at a model's [`ModelPrice::above_long_prompt`] tier. A prompt of
/// exactly this many tokens is still short.
pub(crate) const LONG_PROMPT_TOKENS: i64 = 100_000;

/// Disjoint token components summed over some set of requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Components {
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
}

impl Components {
    /// Prompt-side tokens: everything the request sent, cached or not.
    pub(crate) fn prompt(&self) -> i64 {
        self.input + self.cache_read + self.cache_create
    }

    fn minus(self, other: Self) -> Self {
        Self {
            input: self.input - other.input,
            cache_read: self.cache_read - other.cache_read,
            cache_create: self.cache_create - other.cache_create,
            output: self.output - other.output,
        }
    }

    fn without_output(self) -> Self {
        Self { output: 0, ..self }
    }

    fn output_only(self) -> Self {
        Self {
            output: self.output,
            ..Self::default()
        }
    }
}

/// What the price table prices: a group's summed components, and the part of
/// them that came from requests whose prompt exceeded [`LONG_PROMPT_TOKENS`].
/// `long_prompt` is a subset of `all`, never added to it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TieredUsage {
    pub all: Components,
    pub long_prompt: Components,
}

impl TieredUsage {
    /// The same usage with output tokens dropped, for the input side of a
    /// list-price split. `input_side` and [`Self::output_side`] price to the
    /// whole.
    pub(crate) fn input_side(&self) -> Self {
        Self {
            all: self.all.without_output(),
            long_prompt: self.long_prompt.without_output(),
        }
    }

    /// The same usage with only its output tokens.
    pub(crate) fn output_side(&self) -> Self {
        Self {
            all: self.all.output_only(),
            long_prompt: self.long_prompt.output_only(),
        }
    }
}

/// Per-million-token USD rates.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Rates {
    input: f64,
    cache_read: f64,
    cache_write: f64,
    output: f64,
}

impl Rates {
    fn cost(&self, tokens: Components) -> f64 {
        (tokens.input as f64 * self.input
            + tokens.cache_read as f64 * self.cache_read
            + tokens.cache_create as f64 * self.cache_write
            + tokens.output as f64 * self.output)
            / 1_000_000.0
    }
}

/// List prices for one model tier.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelPrice {
    base: Rates,
    /// Rates for a request whose prompt exceeds [`LONG_PROMPT_TOKENS`], when
    /// the model bills those differently.
    long_prompt: Option<Rates>,
}

impl ModelPrice {
    const fn new(input: f64, cache_read: f64, cache_write: f64, output: f64) -> Self {
        Self {
            base: Rates {
                input,
                cache_read,
                cache_write,
                output,
            },
            long_prompt: None,
        }
    }

    /// Bill requests whose prompt exceeds [`LONG_PROMPT_TOKENS`] at these
    /// rates instead of the base ones.
    const fn above_long_prompt(
        self,
        input: f64,
        cache_read: f64,
        cache_write: f64,
        output: f64,
    ) -> Self {
        Self {
            long_prompt: Some(Rates {
                input,
                cache_read,
                cache_write,
                output,
            }),
            ..self
        }
    }

    fn cost(&self, usage: &TieredUsage) -> f64 {
        match self.long_prompt {
            Some(long) => {
                self.base.cost(usage.all.minus(usage.long_prompt)) + long.cost(usage.long_prompt)
            }
            None => self.base.cost(usage.all),
        }
    }
}

// Claude list prices: https://platform.claude.com/docs/en/about-claude/pricing
const FABLE: ModelPrice = ModelPrice::new(10.0, 1.0, 12.50, 50.0); // Fable 5
const FABLE_5_1: ModelPrice = ModelPrice::new(10.0, 0.25, 12.50, 50.0);
const FABLE_5_2: ModelPrice = ModelPrice::new(8.0, 0.20, 10.0, 40.0);
const OPUS_5_5: ModelPrice = ModelPrice::new(4.0, 0.20, 5.0, 20.0);
const OPUS_4_5: ModelPrice = ModelPrice::new(5.0, 0.50, 6.25, 25.0); // Opus 4.5 to 5
const OPUS_LEGACY: ModelPrice = ModelPrice::new(15.0, 1.50, 18.75, 75.0); // Opus 4 / 4.1
const SONNET_5_5: ModelPrice = ModelPrice::new(2.0, 0.10, 2.50, 10.0);
const SONNET_5: ModelPrice = ModelPrice::new(2.0, 0.20, 2.50, 10.0);
const SONNET_4: ModelPrice = ModelPrice::new(3.0, 0.30, 3.75, 15.0); // Sonnet 4 to 4.6
/// Haiku 5.5: prompts over 100k tokens bill at five times the base rates.
const HAIKU_5_5: ModelPrice =
    ModelPrice::new(0.10, 0.01, 0.125, 0.50).above_long_prompt(0.50, 0.05, 0.625, 2.50);
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

/// The concrete Claude API id each bare alias is priced as, resolved through
/// the identity table so the id costs exactly what the alias does. The
/// `token_rollup` model key folds this generation onto its alias (`claude-opus-5-5` keys as `opus`) and leaves
/// every other generation under its own id, so keying never moves usage
/// between price tiers. When an alias moves to a new generation, update this
/// row with the tier boundary and re-fold.
pub(crate) const ALIAS_GENERATIONS: [(&str, &str); 4] = [
    ("haiku", "claude-haiku-5-5"),
    ("sonnet", "claude-sonnet-5-5"),
    ("opus", "claude-opus-5-5"),
    ("fable", "claude-fable-5-2"),
];

// Historical fallback corrections, reviewed 2026-11-07:
// https://www.anthropic.com/news/3-5-models-and-computer-use (2024-12-03 update),
// with 5-minute writes at 1.25x input and hits at 0.1x per
// https://platform.claude.com/docs/en/about-claude/pricing.
const HAIKU_3_5: ModelPrice = ModelPrice::new(0.80, 0.08, 1.0, 4.0);
// https://developers.openai.com/api/docs/models/gpt-5
// https://developers.openai.com/api/docs/models/gpt-5-codex
// Neither publishes a cache-write surcharge; use plain input for that component.
const GPT_5: ModelPrice = ModelPrice::new(1.25, 0.125, 1.25, 10.0);
// https://developers.openai.com/api/docs/models/codex-mini-latest
const CODEX_MINI: ModelPrice = ModelPrice::new(1.50, 0.375, 1.50, 6.0);
// https://developers.openai.com/api/docs/models/gpt-5.3-codex
const GPT_5_3_CODEX: ModelPrice = ModelPrice::new(1.75, 0.175, 1.75, 14.0);
// Standard processing: https://developers.openai.com/api/docs/pricing
const GPT_6_1_TERRA: ModelPrice = ModelPrice::new(1.0, 0.05, 1.25, 5.0);

const PRICES: &[(&str, ModelPrice)] = &[
    ("claude-fable-5", FABLE),
    ("claude-fable-5.1", FABLE_5_1),
    ("claude-fable-5.2", FABLE_5_2),
    ("claude-opus-5.5", OPUS_5_5),
    ("claude-opus-4.5", OPUS_4_5),
    ("claude-opus-4.6", OPUS_4_5),
    ("claude-opus-4.7", OPUS_4_5),
    ("claude-opus-4.8", OPUS_4_5),
    ("claude-opus-5", OPUS_4_5),
    ("claude-opus-4", OPUS_LEGACY),
    ("claude-opus-4.1", OPUS_LEGACY),
    ("claude-sonnet-5.5", SONNET_5_5),
    ("claude-sonnet-4", SONNET_4),
    ("claude-sonnet-4.5", SONNET_4),
    ("claude-sonnet-4.6", SONNET_4),
    ("claude-sonnet-5", SONNET_5),
    ("claude-haiku-5.5", HAIKU_5_5),
    ("claude-haiku-4.5", HAIKU_4_5),
    ("claude-haiku-3.5", HAIKU_3_5),
    ("gpt-6-astra", GPT_6_ASTRA),
    ("gpt-6.1-sol", GPT_6_1_SOL),
    ("gpt-6.1-terra", GPT_6_1_TERRA),
    ("gpt-6-sol", GPT_6_SOL),
    ("gpt-6-luna", GPT_6_LUNA),
    ("gpt-5.6-sol", GPT_5_6_SOL),
    ("gpt-5.6-terra", GPT_5_6_TERRA),
    ("gpt-5.6-luna", GPT_5_6_LUNA),
    ("gpt-5.5", GPT_5_5),
    ("gpt-5.4", GPT_5_4),
    ("gpt-5.4-mini", GPT_5_4_MINI),
    ("gpt-5.3-codex", GPT_5_3_CODEX),
    ("gpt-5", GPT_5),
    ("gpt-5-codex", GPT_5),
    ("codex-mini-latest", CODEX_MINI),
];

fn canonical_for(backend: &str, model: &str) -> Option<&'static str> {
    let model = model.trim().to_ascii_lowercase();
    let model = model.strip_suffix("[1m]").unwrap_or(&model);
    if let Some((_, concrete)) = ALIAS_GENERATIONS
        .iter()
        .find(|(alias, _)| alias.eq_ignore_ascii_case(model))
    {
        return identity::for_offer("claude", concrete, None);
    }
    identity::for_offer(backend, model, None).or_else(|| identity::for_offer("claude", model, None))
}

/// Resolve a stored model name through identity to its exact price row.
fn price_for(backend: &str, model: Option<&str>) -> Option<ModelPrice> {
    let canonical = canonical_for(backend, model?)?;
    PRICES
        .iter()
        .find_map(|(id, price)| (*id == canonical).then_some(*price))
}

/// List-price USD cost of `usage` on `model`, `0.0` when the model is
/// unpriced. Persisted components are disjoint for every backend, so the math
/// is componentwise, with each request's prompt-length tier already split out
/// in `usage`.
pub(crate) fn cost_usd(backend: &str, model: Option<&str>, usage: &TieredUsage) -> f64 {
    price_for(backend, model).map_or(0.0, |price| price.cost(usage))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One group of requests, none of them long.
    fn short(input: i64, cache_read: i64, cache_create: i64, output: i64) -> TieredUsage {
        TieredUsage {
            all: Components {
                input,
                cache_read,
                cache_create,
                output,
            },
            long_prompt: Components::default(),
        }
    }

    #[test]
    fn haiku_five_five_prices_each_request_at_its_prompt_tier() {
        // Request A: a 100k prompt exactly (the short tier's ceiling), 500 out.
        // Request B: a 100,001-token prompt, 500 out.
        let a = Components {
            input: 1_000,
            cache_read: 99_000,
            cache_create: 0,
            output: 500,
        };
        let b = Components {
            input: 1,
            cache_read: 90_000,
            cache_create: 10_000,
            output: 500,
        };
        let usage = TieredUsage {
            all: Components {
                input: a.input + b.input,
                cache_read: a.cache_read + b.cache_read,
                cache_create: a.cache_create + b.cache_create,
                output: a.output + b.output,
            },
            long_prompt: b,
        };
        // A at $0.10 / $0.01 / $0.125 / $0.50:  1000*0.10 + 99000*0.01 + 500*0.50
        let a_cost = (100.0 + 990.0 + 250.0) / 1e6;
        // B at $0.50 / $0.05 / $0.625 / $2.50: 1*0.5 + 90000*0.05 + 10000*0.625 + 500*2.5
        let b_cost = (0.5 + 4_500.0 + 6_250.0 + 1_250.0) / 1e6;
        for model in ["haiku", "claude-haiku-5-5"] {
            let cost = cost_usd("claude", Some(model), &usage);
            assert!((cost - (a_cost + b_cost)).abs() < 1e-12, "{model}: {cost}");
        }
        // The input and output sides still price to the whole.
        let sides = cost_usd("claude", Some("haiku"), &usage.input_side())
            + cost_usd("claude", Some("haiku"), &usage.output_side());
        assert!((sides - (a_cost + b_cost)).abs() < 1e-12, "sides {sides}");
    }

    /// A model without a long-prompt tier prices its long requests like any
    /// other, so the split never moves its cost.
    #[test]
    fn models_without_a_long_tier_ignore_the_split() {
        let all = Components {
            input: 1_000,
            cache_read: 200_000,
            cache_create: 0,
            output: 1_000,
        };
        let split = TieredUsage {
            all,
            long_prompt: all,
        };
        for model in ["sonnet", "opus", "claude-haiku-4-5", "gpt-5.6-sol"] {
            assert_eq!(
                cost_usd("claude", Some(model), &split),
                cost_usd("claude", Some(model), &short(1_000, 200_000, 0, 1_000)),
                "{model}"
            );
        }
    }

    #[test]
    fn bare_aliases_price_as_the_generation_the_cli_resolves_them_to() {
        assert_eq!(price_for("claude", Some("fable")), Some(FABLE_5_2));
        assert_eq!(
            price_for("claude", Some("claude-fable-5-2")),
            Some(FABLE_5_2)
        );
        // The previous generation keeps its own price when named explicitly.
        assert_eq!(
            price_for("claude", Some("claude-fable-5-1")),
            Some(FABLE_5_1)
        );
        assert_eq!(price_for("claude", Some("opus")), Some(OPUS_5_5));
        assert_eq!(price_for("claude", Some("opus[1m]")), Some(OPUS_5_5));
        assert_eq!(price_for("claude", Some("sonnet")), Some(SONNET_5_5));
        assert_eq!(price_for("claude", Some("haiku")), Some(HAIKU_5_5));
    }

    #[test]
    fn each_claude_family_is_priced_by_generation() {
        // 5.5: the published 2026-10 list prices.
        assert_eq!(price_for("claude", Some("claude-opus-5-5")), Some(OPUS_5_5));
        assert_eq!(
            price_for("claude", Some("claude-opus-5-5[1m]")),
            Some(OPUS_5_5)
        );
        assert_eq!(
            price_for("claude", Some("claude-sonnet-5-5")),
            Some(SONNET_5_5)
        );
        assert_eq!(
            price_for("claude", Some("claude-haiku-5-5")),
            Some(HAIKU_5_5)
        );
        // 4.x and 5.0 keep their own prices.
        assert_eq!(price_for("claude", Some("claude-opus-5")), Some(OPUS_4_5));
        assert_eq!(price_for("claude", Some("claude-opus-4-5")), Some(OPUS_4_5));
        assert_eq!(price_for("claude", Some("claude-sonnet-5")), Some(SONNET_5));
        assert_eq!(
            price_for("claude", Some("claude-sonnet-4-5-20250929")),
            Some(SONNET_4)
        );
        assert_eq!(
            price_for("claude", Some("claude-sonnet-4-20250514")),
            Some(SONNET_4)
        );
        assert_eq!(
            price_for("claude", Some("claude-haiku-4-5-20251001")),
            Some(HAIKU_4_5)
        );
        // A version-first name is an old model, not the current alias.
        assert_eq!(
            price_for("claude", Some("claude-3-5-haiku-20241022")),
            Some(HAIKU_3_5)
        );
    }

    #[test]
    fn legacy_opus_is_priced_higher() {
        assert_eq!(
            price_for("claude", Some("claude-opus-4-1-20250805")),
            Some(OPUS_LEGACY)
        );
        assert_eq!(
            price_for("claude", Some("claude-opus-4-20250514")),
            Some(OPUS_LEGACY)
        );
        assert_eq!(
            price_for("claude", Some("claude-opus-4.1")),
            Some(OPUS_LEGACY)
        );
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
            let cost = cost_usd("claude", Some(model), &short(1, 0, 0, 3));
            assert!((cost - cli_cost).abs() < 1e-12, "{model}: {cost}");
        }
    }

    #[test]
    fn gpt_versions_and_mini() {
        assert_eq!(price_for("claude", Some("gpt-5.6-sol")), Some(GPT_5_6_SOL));
        assert_eq!(
            price_for("claude", Some("gpt-5.6-terra")),
            Some(GPT_5_6_TERRA)
        );
        assert_eq!(
            price_for("claude", Some("gpt-5.6-luna")),
            Some(GPT_5_6_LUNA)
        );
        assert_eq!(price_for("claude", Some("gpt-5.5")), Some(GPT_5_5));
        assert_eq!(
            price_for("claude", Some("gpt-5.4-mini")),
            Some(GPT_5_4_MINI)
        );
        assert_eq!(price_for("claude", Some("gpt-5")), Some(GPT_5));
        assert_eq!(price_for("claude", Some("gpt-5-codex")), Some(GPT_5));
    }

    #[test]
    fn gpt_six_family_is_priced_by_size() {
        assert_eq!(price_for("claude", Some("gpt-6.1-sol")), Some(GPT_6_1_SOL));
        assert_eq!(price_for("claude", Some("gpt-6-astra")), Some(GPT_6_ASTRA));
        assert_eq!(price_for("claude", Some("gpt-6-sol")), Some(GPT_6_SOL));
        assert_eq!(price_for("claude", Some("gpt-6-luna")), Some(GPT_6_LUNA));
        // None of them falls through to the bare-`gpt` default.
        for model in ["gpt-6.1-sol", "gpt-6-astra", "gpt-6-sol", "gpt-6-luna"] {
            assert_ne!(price_for("claude", Some(model)), Some(GPT_5_4), "{model}");
        }
    }

    /// `gpt-6.1-sol` contains `gpt-6`; it must not take GPT-6 Sol's dearer
    /// cached-input rate, which is the only field where the two differ.
    #[test]
    fn gpt_six_one_is_not_classified_as_gpt_six() {
        assert_ne!(GPT_6_1_SOL, GPT_6_SOL);
        assert_ne!(price_for("claude", Some("gpt-6.1-sol")), Some(GPT_6_SOL));
        assert_eq!(price_for("claude", Some("gpt-6-1-sol")), None);
        assert_eq!(price_for("claude", Some("gpt-6.1")), None);
    }

    #[test]
    fn each_alias_generation_costs_what_its_alias_costs() {
        for (alias, concrete) in ALIAS_GENERATIONS {
            let price = price_for("claude", Some(alias));
            assert!(price.is_some(), "{alias} is priced");
            for spelling in [concrete.to_string(), format!("{concrete}[1m]")] {
                assert_eq!(
                    price_for("claude", Some(&spelling)),
                    price,
                    "{spelling} vs {alias}"
                );
            }
        }
    }

    #[test]
    fn alias_generations_cover_every_claude_alias() {
        let mut aliases: Vec<String> = cairn_models_types::catalog::claude_models()
            .into_iter()
            .map(|model| model.id)
            .collect();
        let mut covered: Vec<String> = ALIAS_GENERATIONS
            .iter()
            .map(|(alias, _)| alias.to_string())
            .collect();
        aliases.sort();
        covered.sort();
        assert_eq!(covered, aliases);
    }

    #[test]
    fn price_keys_are_unique_canonical_ids() {
        let mut seen = std::collections::BTreeSet::new();
        for (id, _) in PRICES {
            assert!(identity::canonical(id).is_some(), "{id}");
            assert!(seen.insert(id), "duplicate price for {id}");
        }
        for (_, concrete) in ALIAS_GENERATIONS {
            let id = identity::for_offer("claude", concrete, None).expect(concrete);
            assert!(PRICES.iter().any(|(key, _)| *key == id), "{concrete}");
        }
    }

    #[test]
    fn aliases_are_backend_independent_and_normalized() {
        for backend in ["claude", "codex", "openrouter"] {
            for model in ["opus", "Opus", "opus[1m]", "  OPUS[1M]  "] {
                assert_eq!(price_for(backend, Some(model)), Some(OPUS_5_5));
            }
            assert_eq!(
                price_for(backend, Some("claude-sonnet-4-5-20250929")),
                Some(SONNET_4)
            );
        }
        assert_eq!(
            price_for("openrouter", Some("anthropic/claude-opus-4.6")),
            Some(OPUS_4_5)
        );
    }

    #[test]
    fn sampled_live_models_have_exact_prices() {
        for (backend, model, expected) in [
            ("claude", "opus", Some(OPUS_5_5)),
            ("claude", "haiku", Some(HAIKU_5_5)),
            ("claude", "fable", Some(FABLE_5_2)),
            ("claude", "sonnet", Some(SONNET_5_5)),
            ("codex", "gpt-5.6-sol", Some(GPT_5_6_SOL)),
            ("codex", "gpt-6-astra", Some(GPT_6_ASTRA)),
            ("codex", "gpt-5.6-luna", Some(GPT_5_6_LUNA)),
            ("codex", "gpt-5.5", Some(GPT_5_5)),
            ("claude", "gpt-5.4", Some(GPT_5_4)),
            ("claude", "gpt-5.4-mini", Some(GPT_5_4_MINI)),
            ("codex", "gpt-5.3-codex", Some(GPT_5_3_CODEX)),
            ("openrouter", "deepseek/deepseek-v4-flash", None),
        ] {
            assert_eq!(
                price_for(backend, Some(model)),
                expected,
                "{backend}/{model}"
            );
            let expected_cost = expected.map_or(0.0, |p| p.cost(&short(17, 31, 11, 23)));
            assert_eq!(
                cost_usd(backend, Some(model), &short(17, 31, 11, 23)),
                expected_cost
            );
        }
    }

    #[test]
    fn fallback_corrections_use_each_models_own_list_price() {
        for (model, rates) in [
            ("gpt-5", (1.25, 0.125, 1.25, 10.0)),
            ("gpt-5-codex", (1.25, 0.125, 1.25, 10.0)),
            ("codex-mini-latest", (1.50, 0.375, 1.50, 6.0)),
            ("claude-3-5-haiku", (0.80, 0.08, 1.0, 4.0)),
            ("claude-3-5-haiku-20241022", (0.80, 0.08, 1.0, 4.0)),
            ("gpt-6.1-terra", (1.0, 0.05, 1.25, 5.0)),
            ("gpt-5.3-codex", (1.75, 0.175, 1.75, 14.0)),
            ("claude-fable-5-2", (8.0, 0.20, 10.0, 40.0)),
            ("claude-fable-5-1", (10.0, 0.25, 12.50, 50.0)),
            ("claude-fable-5", (10.0, 1.0, 12.50, 50.0)),
            ("claude-opus-5-5", (4.0, 0.20, 5.0, 20.0)),
            ("claude-sonnet-5-5", (2.0, 0.10, 2.50, 10.0)),
            ("claude-sonnet-5", (2.0, 0.20, 2.50, 10.0)),
        ] {
            assert_eq!(
                price_for("codex", Some(model)),
                Some(ModelPrice::new(rates.0, rates.1, rates.2, rates.3)),
                "{model}"
            );
        }
    }

    #[test]
    fn substrings_and_unknown_versions_never_inherit_prices() {
        for model in [
            "z-ai/glm",
            "my-opus-model",
            "not-gpt-5.4",
            "codex",
            "claude-opus-99-7",
            "claude-opus-4-6-20990101",
            "gpt-5.4-unlisted",
            "gpt-6.1",
            "gpt-6-1-sol",
            "mythos",
            "someone/claude-opus-4.6",
            "gpt-oss-20b",
        ] {
            assert_eq!(price_for("openrouter", Some(model)), None, "{model}");
        }
    }

    #[test]
    fn unknown_models_are_unpriced() {
        assert_eq!(price_for("claude", Some("mystery-model")), None);
        assert_eq!(price_for("claude", None), None);
        assert_eq!(
            cost_usd("claude", Some("mystery"), &short(1000, 0, 0, 1000)),
            0.0
        );
    }

    #[test]
    fn codex_cost_prices_cache_components() {
        // gpt-5.4: 1M input($2.50) + 0.5M cache read($0.125)
        // + 0.5M cache write($1.25) + 1M output($15).
        let cost = cost_usd(
            "claude",
            Some("gpt-5.4"),
            &short(1_000_000, 500_000, 500_000, 1_000_000),
        );
        assert!((cost - 18.875).abs() < 1e-9, "got {cost}");
    }

    #[test]
    fn claude_cost_prices_each_component() {
        // sonnet 5.5: 1M input(2) + 1M cache_read(0.10) + 1M cache_write(2.50) + 1M output(10)
        let cost = cost_usd(
            "claude",
            Some("sonnet"),
            &short(1_000_000, 1_000_000, 1_000_000, 1_000_000),
        );
        assert!((cost - 14.60).abs() < 1e-9, "got {cost}");
    }
}
