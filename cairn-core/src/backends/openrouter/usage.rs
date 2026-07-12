//! OpenRouter provider usage snapshot producer.
//!
//! Sources the per-model breakdown from Cairn's own recorded real metered cost
//! (`events.cost_usd`, via the analytics layer), not the price-table estimate,
//! and best-effort augments it with the OpenRouter account credit balance.
//! Scope is workspace-wide and all-time: the provider card is a global account
//! view, not a project-scoped one.

use std::time::Duration;

use serde_json::Value;

use super::{openrouter_api_key, OPENROUTER_BACKEND_KEY};
use crate::models::{ProviderCreditsSnapshot, ProviderModelUsageRow, ProviderUsageSnapshot};
use crate::orchestrator::Orchestrator;
use cairn_analytics::{self as analytics, types::Scope, types::TimeRange};

/// Snapshot source tag for the OpenRouter per-model breakdown. The frontend
/// treats this as a canonical usage source, so a loaded breakdown is not
/// auto-refreshed.
const OPENROUTER_USAGE_SOURCE: &str = "openrouter_generation";

/// OpenRouter account credits endpoint.
const OPENROUTER_CREDITS_URL: &str = "https://openrouter.ai/api/v1/credits";

/// Hard ceiling on the best-effort credits fetch; the breakdown never waits on
/// it longer than this.
const CREDITS_TIMEOUT: Duration = Duration::from_secs(8);

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

    let (credits, raw) = fetch_credits(&api_key).await;

    ProviderUsageSnapshot {
        backend: OPENROUTER_BACKEND_KEY.to_string(),
        source: OPENROUTER_USAGE_SOURCE.to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows: Vec::new(),
        credits,
        reset_credits: None,
        error: None,
        unsupported_reason: None,
        raw,
        model_breakdown: Some(model_breakdown),
    }
}

/// Best-effort fetch of the OpenRouter account credit balance. Any failure,
/// non-success status, or timeout yields `(None, None)`; a parsed body with no
/// recognizable credit fields yields `(None, Some(body))` so the raw response is
/// still available for inspection.
async fn fetch_credits(api_key: &str) -> (Option<ProviderCreditsSnapshot>, Option<Value>) {
    let client = match reqwest::Client::builder().timeout(CREDITS_TIMEOUT).build() {
        Ok(client) => client,
        Err(_) => return (None, None),
    };
    let response = match client
        .get(OPENROUTER_CREDITS_URL)
        .bearer_auth(api_key)
        .header("HTTP-Referer", "https://cairn.computer")
        .header("X-OpenRouter-Title", "Cairn")
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return (None, None),
    };
    if !response.status().is_success() {
        return (None, None);
    }
    let body: Value = match response.json().await {
        Ok(body) => body,
        Err(_) => return (None, None),
    };
    // OpenRouter returns `{ "data": { "total_credits", "total_usage" } }`.
    let data = body.get("data").unwrap_or(&body);
    let total_granted = data.get("total_credits").and_then(Value::as_f64);
    let total_used = data.get("total_usage").and_then(Value::as_f64);
    if total_granted.is_none() && total_used.is_none() {
        return (None, Some(body));
    }
    let balance = match (total_granted, total_used) {
        (Some(granted), Some(used)) => Some(granted - used),
        _ => None,
    };
    (
        Some(ProviderCreditsSnapshot {
            balance,
            total_granted,
            total_used,
            currency: Some("USD".to_string()),
        }),
        Some(body),
    )
}
