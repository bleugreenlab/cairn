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

use cairn_models_types::catalog::claude_generation;

/// Date the price table was last reviewed against public list prices.
pub(crate) const PRICE_SOURCE_DATE: &str = "2026-10-20";

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

// Claude tiers.
const FABLE: ModelPrice = ModelPrice::new(10.0, 1.0, 12.50, 50.0); // Fable 5 / 5.1
const OPUS_5_5: ModelPrice = ModelPrice::new(4.0, 0.40, 5.0, 20.0); // Opus 5.5+
const OPUS_4_5: ModelPrice = ModelPrice::new(5.0, 0.50, 6.25, 25.0); // Opus 4.5 to 5
const OPUS_LEGACY: ModelPrice = ModelPrice::new(15.0, 1.50, 18.75, 75.0); // Opus 4 / 4.1
const SONNET_5_5: ModelPrice = ModelPrice::new(2.0, 0.20, 2.50, 10.0); // Sonnet 5.5+
const SONNET_4: ModelPrice = ModelPrice::new(3.0, 0.30, 3.75, 15.0); // Sonnet 4 to 5
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

/// List-price USD cost of `usage` on `model`, `0.0` when the model is
/// unpriced. Persisted components are disjoint for every backend, so the math
/// is componentwise, with each request's prompt-length tier already split out
/// in `usage`.
pub(crate) fn cost_usd(model: Option<&str>, usage: &TieredUsage) -> f64 {
    price_for(model).map_or(0.0, |price| price.cost(usage))
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
            let cost = cost_usd(Some(model), &usage);
            assert!((cost - (a_cost + b_cost)).abs() < 1e-12, "{model}: {cost}");
        }
        // The input and output sides still price to the whole.
        let sides = cost_usd(Some("haiku"), &usage.input_side())
            + cost_usd(Some("haiku"), &usage.output_side());
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
                cost_usd(Some(model), &split),
                cost_usd(Some(model), &short(1_000, 200_000, 0, 1_000)),
                "{model}"
            );
        }
    }

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
            let cost = cost_usd(Some(model), &short(1, 0, 0, 3));
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
        assert_eq!(cost_usd(Some("mystery"), &short(1000, 0, 0, 1000)), 0.0);
    }

    #[test]
    fn codex_cost_prices_cache_components() {
        // gpt-5.4: 1M input($2.50) + 0.5M cache read($0.125)
        // + 0.5M cache write($1.25) + 1M output($15).
        let cost = cost_usd(
            Some("gpt-5"),
            &short(1_000_000, 500_000, 500_000, 1_000_000),
        );
        assert!((cost - 18.875).abs() < 1e-9, "got {cost}");
    }

    #[test]
    fn claude_cost_prices_each_component() {
        // sonnet 5.5: 1M input(2) + 1M cache_read(0.20) + 1M cache_write(2.50) + 1M output(10)
        let cost = cost_usd(
            Some("sonnet"),
            &short(1_000_000, 1_000_000, 1_000_000, 1_000_000),
        );
        assert!((cost - 14.70).abs() < 1e-9, "got {cost}");
    }
}
