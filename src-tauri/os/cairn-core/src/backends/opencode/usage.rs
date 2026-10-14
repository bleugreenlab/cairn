//! OpenCode Go provider usage snapshot producer.
//!
//! Two independent measurements compose one snapshot, and keeping them
//! independent is the point:
//!
//! - **Live windows** come from Go's own `usage` endpoint, which is the only
//!   thing that knows the subscription's real headroom.
//! - **The model breakdown** comes from Cairn's own recorded metered cost
//!   (`events.cost_usd`, via the analytics layer), which is a real measurement of
//!   this workspace's Go usage no matter what the provider exposes.
//!
//! Neither is derived from the other. Cairn's recorded spend is not the
//! provider's headroom — other clients share the same subscription — so a failed
//! usage fetch leaves `windows` empty and lets the card fall back to Go's
//! published policy, rather than dressing up analytics as headroom. A wrong
//! number about someone's spending is not a small error.

use super::{opencode_api_key, OPENCODE_BACKEND_KEY};
use crate::orchestrator::Orchestrator;
use cairn_analytics::{self as analytics, types::Scope, types::TimeRange};
use cairn_llm::types::usage::{ProviderModelUsageRow, ProviderUsageSnapshot};
use cairn_llm::usage::opencode::fetch_usage_windows;

/// Snapshot source tag for the Go usage snapshot.
const OPENCODE_USAGE_SOURCE: &str = "opencode_go_usage";

/// Build the OpenCode Go usage snapshot: the subscription's live rolling windows
/// when the provider answers, plus the real metered cost Cairn recorded for Go
/// runs (workspace-wide, all-time).
///
/// An empty breakdown (`Some(vec![])`) is a real snapshot meaning "no recorded
/// Go usage yet", not an unsupported result. Empty `windows` means the provider
/// did not tell us its headroom this time; the card then states Go's published
/// policy as the policy it is, which is honest in a way a stale or inferred
/// number would not be.
pub async fn collect_opencode_usage_snapshot(orch: &Orchestrator) -> ProviderUsageSnapshot {
    let Some(api_key) = opencode_api_key(orch) else {
        return ProviderUsageSnapshot::unsupported(
            OPENCODE_BACKEND_KEY,
            OPENCODE_USAGE_SOURCE,
            "Connect an OpenCode account to see usage.",
        );
    };

    // A blocking HTTP client must not run on the async runtime's thread.
    let windows = match tokio::task::spawn_blocking(move || fetch_usage_windows(&api_key)).await {
        Ok(Ok(windows)) => windows,
        Ok(Err(error)) => {
            // Not a snapshot error: the model breakdown below is still a real
            // measurement, and surfacing this as a failure would hide it behind
            // an error banner. The card falls back to published policy.
            log::warn!("OpenCode Go live usage unavailable: {error}");
            Vec::new()
        }
        Err(error) => {
            log::warn!("OpenCode Go usage fetch panicked: {error}");
            Vec::new()
        }
    };

    let rows = match analytics::provider_model_costs(
        &orch.db.local,
        OPENCODE_BACKEND_KEY,
        &Scope::new(None),
        &TimeRange::default(),
    )
    .await
    {
        Ok(rows) => rows,
        Err(err) => {
            return ProviderUsageSnapshot {
                backend: OPENCODE_BACKEND_KEY.to_string(),
                source: OPENCODE_USAGE_SOURCE.to_string(),
                captured_at: chrono::Utc::now().timestamp(),
                // Live windows survive an analytics failure, for the same reason
                // the breakdown survives a usage-fetch failure: they measure
                // different things.
                windows,
                error: Some(format!("Failed to load OpenCode Go usage: {err}")),
                ..Default::default()
            };
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

    ProviderUsageSnapshot {
        backend: OPENCODE_BACKEND_KEY.to_string(),
        source: OPENCODE_USAGE_SOURCE.to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows,
        credits: None,
        reset_credits: None,
        error: None,
        error_kind: None,
        unsupported_reason: None,
        raw: None,
        model_breakdown: Some(model_breakdown),
    }
}
