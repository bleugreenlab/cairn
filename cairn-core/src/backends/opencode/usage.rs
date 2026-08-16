//! OpenCode Go provider usage snapshot producer.
//!
//! Go meters a subscription against dollar-denominated rolling windows ($12 per
//! 5 hours, $30 per week, $60 per month) and shows the running total in
//! OpenCode's own console. Cairn does not claim to know that headroom: the
//! provider's `usage` endpoint exists but requires an active subscription to
//! answer, so its response shape is unverified, and a snapshot invented from a
//! guessed shape would be worse than none — a wrong number about spending is not
//! a small error. The settings card states the published limits as the fixed
//! policy they are and links to the console for live headroom.
//!
//! What Cairn does know is what Cairn spent. The breakdown below is sourced from
//! its own recorded metered cost (`events.cost_usd`, via the analytics layer),
//! which is a real measurement of this workspace's Go usage regardless of what
//! the provider exposes. Scope is workspace-wide and all-time: the provider card
//! is a global account view, not a project-scoped one.

use super::{opencode_api_key, OPENCODE_BACKEND_KEY};
use crate::models::{ProviderModelUsageRow, ProviderUsageSnapshot};
use crate::orchestrator::Orchestrator;
use cairn_analytics::{self as analytics, types::Scope, types::TimeRange};

/// Snapshot source tag for the Go per-model breakdown. The frontend treats this
/// as a canonical usage source, so a loaded breakdown is not auto-refreshed.
const OPENCODE_USAGE_SOURCE: &str = "opencode_go_generation";

/// Build the OpenCode Go usage snapshot: a per-model breakdown of the real
/// metered cost Cairn recorded for Go runs (workspace-wide, all-time).
///
/// An empty breakdown (`Some(vec![])`) is a real snapshot meaning "no recorded
/// Go usage yet", not an unsupported result. `windows` stays empty on purpose:
/// a window in this model carries used/remaining percentages, and Cairn has no
/// measurement of the subscription's remaining headroom to put in one.
pub async fn collect_opencode_usage_snapshot(orch: &Orchestrator) -> ProviderUsageSnapshot {
    if opencode_api_key(orch).is_none() {
        return ProviderUsageSnapshot::unsupported(
            OPENCODE_BACKEND_KEY,
            OPENCODE_USAGE_SOURCE,
            "Connect an OpenCode account to see usage.",
        );
    }

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
            return ProviderUsageSnapshot::error(
                OPENCODE_BACKEND_KEY,
                OPENCODE_USAGE_SOURCE,
                format!("Failed to load OpenCode Go usage: {err}"),
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

    ProviderUsageSnapshot {
        backend: OPENCODE_BACKEND_KEY.to_string(),
        source: OPENCODE_USAGE_SOURCE.to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows: Vec::new(),
        credits: None,
        reset_credits: None,
        error: None,
        unsupported_reason: None,
        raw: None,
        model_breakdown: Some(model_breakdown),
    }
}
