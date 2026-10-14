//! OpenRouter provider usage snapshot producer.
//!
//! Sources the per-model breakdown from Cairn's own recorded real metered cost
//! (`events.cost_usd`, via the analytics layer), not the price-table estimate,
//! and best-effort augments it with the OpenRouter account credit balance.
//! Scope is workspace-wide and all-time: the provider card is a global account
//! view, not a project-scoped one.

use super::{openrouter_api_key, OPENROUTER_BACKEND_KEY};
use crate::orchestrator::Orchestrator;
use cairn_analytics::{self as analytics, types::Scope, types::TimeRange};
use cairn_llm::types::usage::{ProviderModelUsageRow, ProviderUsageSnapshot};
use cairn_llm::usage::openrouter::fetch_openrouter_credits;

/// Snapshot source tag for the OpenRouter per-model breakdown. The frontend
/// treats this as a canonical usage source, so a loaded breakdown is not
/// auto-refreshed.
const OPENROUTER_USAGE_SOURCE: &str = "openrouter_generation";

/// Build the OpenRouter usage snapshot: a per-model breakdown of the real
/// metered cost Cairn recorded for OpenRouter runs (workspace-wide, all-time),
/// plus the account credit balance when reachable.
///
/// An empty breakdown (`Some(vec![])`) is a real snapshot meaning "no recorded
/// OpenRouter usage yet", not an unsupported result. Credits are a best-effort
/// bonus: any failure leaves `credits: None` and still returns the breakdown.
pub async fn collect_openrouter_usage_snapshot(orch: &Orchestrator) -> ProviderUsageSnapshot {
    let Some(api_key) = openrouter_api_key(orch) else {
        return ProviderUsageSnapshot::unsupported(
            OPENROUTER_BACKEND_KEY,
            OPENROUTER_USAGE_SOURCE,
            "Connect an OpenRouter account to see usage.",
        );
    };

    let rows = match analytics::provider_model_costs(
        &orch.db.local,
        OPENROUTER_BACKEND_KEY,
        &Scope::new(None),
        &TimeRange::default(),
    )
    .await
    {
        Ok(rows) => rows,
        Err(err) => {
            return ProviderUsageSnapshot::error(
                OPENROUTER_BACKEND_KEY,
                OPENROUTER_USAGE_SOURCE,
                format!("Failed to load OpenRouter usage: {err}"),
                None,
            );
        }
    };

    let model_breakdown: Vec<ProviderModelUsageRow> = rows
        .into_iter()
        .map(|row| ProviderModelUsageRow {
            model: row.model,
            cost_usd: row.cost_usd,
            tokens: Some(row.billable_tokens),
            runs: Some(row.runs),
        })
        .collect();

    let (credits, raw) = fetch_openrouter_credits(&api_key).await;

    ProviderUsageSnapshot {
        backend: OPENROUTER_BACKEND_KEY.to_string(),
        source: OPENROUTER_USAGE_SOURCE.to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows: Vec::new(),
        credits,
        reset_credits: None,
        error: None,
        error_kind: None,
        unsupported_reason: None,
        raw,
        model_breakdown: Some(model_breakdown),
    }
}
