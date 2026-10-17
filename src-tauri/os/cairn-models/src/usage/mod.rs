//! Provider usage as data: parsing what providers report.
pub mod book;
pub mod codex_rate_limits;
pub mod opencode;
pub mod openrouter;
pub mod pool;

use cairn_llm_wire::TurnUsage;
use cairn_models_types::usage::{Charge, Measure, Meter, MeteredUsage};
use serde_json::Value;

/// What one call consumed, read from the neutral usage every protocol family
/// reports through. This is the one interpretation of [`TurnUsage`]: transcript
/// event columns and the responses journal both read usage through it.
///
/// `prompt_tokens` is the whole prompt with `cached_tokens` inside it, so
/// Input is the uncached remainder and CacheRead the cached part. Reasoning is
/// the share of Output spent reasoning, counted inside Output the way providers
/// report it. A meter is measured only when its source field is present.
///
/// No CacheWrite is measured. The Anthropic mapping folds cache-creation tokens
/// into `prompt_tokens`, so they land in Input; separating them would change
/// what every stored HTTP turn has been accounted as.
pub fn metered(usage: &TurnUsage) -> MeteredUsage {
    let count = |value: i64| u64::try_from(value).unwrap_or(0);
    let detail = |details: &Option<Value>, key: &str| {
        details
            .as_ref()
            .and_then(|details| details.get(key))
            .and_then(Value::as_i64)
            .map(count)
    };
    let cache_read = detail(&usage.prompt_tokens_details, "cached_tokens");
    let input = usage
        .prompt_tokens
        .map(|prompt| count(prompt.into()).saturating_sub(cache_read.unwrap_or(0)));
    let output = usage.completion_tokens.map(|tokens| count(tokens.into()));
    let reasoning = usage
        .reasoning_tokens
        .map(|tokens| count(tokens.into()))
        .or_else(|| detail(&usage.completion_tokens_details, "reasoning_tokens"));
    let measures = [
        (Charge::Input, input),
        (Charge::CacheRead, cache_read),
        (Charge::Output, output),
        (Charge::Reasoning, reasoning),
    ]
    .into_iter()
    .filter_map(|(charge, quantity)| {
        quantity.map(|quantity| Measure {
            meter: Meter::tokens(charge),
            quantity,
        })
    })
    .collect();
    MeteredUsage {
        measures,
        cost_usd: usage.cost,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models_types::usage::BillingUnit;

    fn tokens(usage: &MeteredUsage, charge: Charge) -> Option<u64> {
        usage.quantity(charge, BillingUnit::Tokens)
    }

    #[test]
    fn chat_usage_splits_the_prompt_into_uncached_and_cached() {
        let usage: TurnUsage = serde_json::from_value(serde_json::json!({
            "prompt_tokens": 1200,
            "completion_tokens": 300,
            "prompt_tokens_details": {"cached_tokens": 1000},
            "completion_tokens_details": {"reasoning_tokens": 120},
            "cost": 0.0042
        }))
        .unwrap();
        let metered = metered(&usage);
        assert_eq!(tokens(&metered, Charge::Input), Some(200));
        assert_eq!(tokens(&metered, Charge::CacheRead), Some(1000));
        assert_eq!(tokens(&metered, Charge::Output), Some(300));
        assert_eq!(tokens(&metered, Charge::Reasoning), Some(120));
        assert_eq!(tokens(&metered, Charge::CacheWrite), None);
        assert_eq!(metered.prompt_tokens(), Some(1200));
        assert_eq!(metered.cost_usd, Some(0.0042));
    }

    #[test]
    fn an_unreported_field_is_not_measured() {
        let usage: TurnUsage =
            serde_json::from_value(serde_json::json!({"completion_tokens": 3})).unwrap();
        let metered = metered(&usage);
        assert_eq!(metered.measures.len(), 1);
        assert_eq!(tokens(&metered, Charge::Output), Some(3));
        assert_eq!(metered.prompt_tokens(), None);
        assert_eq!(metered.cost_usd, None);
    }

    #[test]
    fn anthropic_cache_creation_is_accounted_as_input() {
        // Pins today's accounting, not a preference: `from_anthropic` folds the
        // 64 cache-creation tokens into the prompt, so they read as Input and no
        // CacheWrite is measured. The whole prompt is still exact.
        let usage = TurnUsage::from_anthropic(Some(40), Some(25), Some(128), Some(64), None);
        let metered = metered(&usage);
        assert_eq!(tokens(&metered, Charge::Input), Some(104));
        assert_eq!(tokens(&metered, Charge::CacheRead), Some(128));
        assert_eq!(tokens(&metered, Charge::CacheWrite), None);
        assert_eq!(metered.prompt_tokens(), Some(232));
    }

    #[test]
    fn responses_reasoning_is_read_from_its_details() {
        let usage = TurnUsage::from_responses(
            Some(900),
            Some(450),
            None,
            Some(serde_json::json!({"cached_tokens": 300})),
            Some(serde_json::json!({"reasoning_tokens": 200})),
            Some(0.012),
        );
        let metered = metered(&usage);
        assert_eq!(tokens(&metered, Charge::Input), Some(600));
        assert_eq!(tokens(&metered, Charge::Reasoning), Some(200));
        assert_eq!(metered.prompt_tokens(), Some(900));
    }

    #[test]
    fn more_cached_than_prompted_never_reads_as_negative_input() {
        let usage: TurnUsage = serde_json::from_value(serde_json::json!({
            "prompt_tokens": 10,
            "prompt_tokens_details": {"cached_tokens": 20}
        }))
        .unwrap();
        assert_eq!(tokens(&metered(&usage), Charge::Input), Some(0));
    }
}
