//! Agent-economics analytics.
//!
//! Tier A surfaces token economics straight off event columns: average tokens
//! per session over time, token-to-line efficiency on PR-producing jobs, priced
//! cost trends, and per-model / per-role economics. The backend-shaped billable
//! rule lives in [`queries`] (the `token_events` CTE); pricing lives in
//! [`pricing`]; this module joins the two and normalizes roles.

mod backfill;
mod cost;
mod economics;
mod roles;
mod tokens;
mod tools;

pub mod pricing;
pub mod queries;
pub mod tool_extract;
pub mod types;

#[cfg(test)]
mod tests;

pub use backfill::{backfill_tool_invocations, run_historical_backfill};
pub use cost::{
    cost_by_backend_timeseries, cost_by_project, cost_timeseries, effective_cost,
    provider_model_costs, ProviderModelCost,
};
pub use economics::{
    issue_lead_times, merged_pr_cost_trend, merged_pr_economics, model_role_economics,
};
pub use tokens::{avg_tokens_per_session, token_composition_timeseries, tokens_per_loc};
pub use tools::{
    avg_tool_calls_per_session, command_durations, session_durations, target_breakdown,
    time_composition, tool_error_rate, tool_mix, tool_time_mix, usage_heatmap,
};
pub use types::{
    BackendCostPoint, Bucket, CommandDurationRow, CostPoint, EconomicsRow, EffectiveCostPoint,
    HeatmapCell, IssueLeadTimePoint, MergedPrEconomics, ModelRoleEconomics, PrCostTrendPoint,
    PrEconomicsRow, ProjectCost, Scope, SessionDurationPoint, TargetBreakdown, TargetShapeRow,
    TimeCompositionPoint, TimeRange, TokenCompositionPoint, TokensPerLocPoint,
    TokensPerSessionPoint, ToolBackfillSummary, ToolCallsPerSessionPoint, ToolErrorRatePoint,
    ToolMixPoint, ToolTimeMixPoint, TopTargetRow,
};
