//! Public analytics input and output types.
//!
//! Output types serialize camelCase for the frontend; input types (`Scope`,
//! `TimeRange`, `Bucket`) are constructed by the Tauri command layer from raw
//! optional arguments.

use serde::{Deserialize, Serialize};

/// Query scope: a single project, or the whole workspace when `project_id` is
/// `None`.
#[derive(Debug, Clone, Default)]
pub struct Scope {
    pub project_id: Option<String>,
}

impl Scope {
    pub fn new(project_id: Option<String>) -> Self {
        Self { project_id }
    }
}

/// Half-open time window in epoch seconds. A `None` bound is unbounded.
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeRange {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

impl TimeRange {
    pub fn new(start: Option<i64>, end: Option<i64>) -> Self {
        Self { start, end }
    }
}

/// Trend bucketing granularity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    /// Hourly buckets. Served directly from the hour-grain `token_rollup`, whose
    /// stored `bucket_start` is already the UTC-hour floor, so `bucket_expr(Hour)`
    /// on it is the identity floor; see `analytics::queries`.
    Hour,
    Day,
    Week,
    Month,
}

impl Bucket {
    /// A SQL expression that truncates the epoch-seconds column `ts_col` to the
    /// start of its bucket, returning an integer epoch. Day/Week use fixed-width
    /// flooring; calendar months can't be expressed that way, so Month uses
    /// SQLite's `strftime(... 'start of month')`. `ts_col` is always a controlled
    /// column name (never user text), so direct interpolation is safe.
    pub(crate) fn bucket_expr(self, ts_col: &str) -> String {
        match self {
            Bucket::Hour => format!("({ts_col} / 3600) * 3600"),
            Bucket::Day => format!("({ts_col} / 86400) * 86400"),
            Bucket::Week => format!("({ts_col} / 604800) * 604800"),
            Bucket::Month => {
                format!("CAST(strftime('%s', {ts_col}, 'unixepoch', 'start of month') AS INTEGER)")
            }
        }
    }

    /// Parse a raw frontend string: `hour`, `week`, `month`, else `day`.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw {
            Some(s) if s.eq_ignore_ascii_case("hour") => Bucket::Hour,
            Some(s) if s.eq_ignore_ascii_case("week") => Bucket::Week,
            Some(s) if s.eq_ignore_ascii_case("month") => Bucket::Month,
            _ => Bucket::Day,
        }
    }
}

/// One trend point of average billable tokens per session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TokensPerSessionPoint {
    /// Epoch seconds at the start of the bucket.
    pub bucket_start: i64,
    pub avg_tokens: f64,
    pub session_count: i64,
}

/// One PR-producing job's token-to-line efficiency.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TokensPerLocPoint {
    pub job_id: String,
    /// Epoch seconds the merge request opened.
    pub ts: i64,
    pub billable_tokens: i64,
    pub lines_changed: i64,
    pub tokens_per_line: f64,
    /// Real metered cost for this PR's job (exact `events.cost_usd` when
    /// reported, else the price-table estimate). Drives the cost-per-line chart.
    pub cost_usd: f64,
    pub model: Option<String>,
    pub role: String,
}

/// Priced cost for one (time bucket, backend) group; the stacked provider-split
/// cost chart. Per-bucket backend stack heights sum to the blended cost total.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BackendCostPoint {
    pub bucket_start: i64,
    pub backend: String,
    pub cost_usd: f64,
    pub billable_tokens: i64,
}

/// Token components (by type) for one time bucket; the stacked token-composition
/// chart. Input / cache-read / cache-create / output / thinking sum to the
/// bucket's total tokens.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TokenCompositionPoint {
    pub bucket_start: i64,
    pub input: i64,
    pub cache_read: i64,
    pub cache_create: i64,
    pub output: i64,
    pub thinking: i64,
}

/// Total real cost for one project across the range; the cost-by-project chart
/// (workspace scope only).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProjectCost {
    pub project_id: String,
    pub project_name: String,
    pub cost_usd: f64,
    pub billable_tokens: i64,
}

/// Tool-call error rate for one time bucket; the tool-error-rate trend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolErrorRatePoint {
    pub bucket_start: i64,
    pub total: i64,
    pub errors: i64,
    /// `errors / total`; 0 when no calls in the bucket.
    pub error_rate: f64,
}

/// Total billable tokens for one (day-of-week, hour) cell, in the user's local
/// time; the usage heatmap. `day_of_week` is SQLite `%w` (0 = Sunday).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeatmapCell {
    pub day_of_week: i64,
    pub hour: i64,
    pub tokens: i64,
}

/// One trend point of priced cost.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CostPoint {
    pub bucket_start: i64,
    pub cost_usd: f64,
    pub billable_tokens: i64,
}

/// One (fine-bucket, backend) effective-cost point. `effective_cost` allocates a
/// provider's flat subscription fee across its usage, or passes through real
/// metered cost. The flat fee is allocated per calendar month, but points are
/// emitted at the page's bucket granularity (`bucket_start`) so the trend can
/// follow the range selector; each fine bucket inherits the ratio of the month
/// that contains it. See `analytics::effective_cost`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveCostPoint {
    /// First-of-month epoch seconds (UTC) of the month this point belongs to;
    /// the ratio/fee allocation is keyed on this.
    pub month_start: i64,
    /// Start epoch seconds of the fine bucket (day/week/hour/month) this point
    /// covers. Equals `month_start` when the page bucket is Month.
    pub bucket_start: i64,
    pub backend: String,
    /// True only when this backend reports a real per-generation cost
    /// (genuinely pay-as-you-go, e.g. OpenRouter). Subscription backends
    /// (Claude/Codex) are never metered, even before a fee is configured.
    pub metered: bool,
    /// True when the month contains "now" (running estimate, not yet settled).
    pub provisional: bool,
    /// The flat monthly fee for a subscription backend; 0 for metered.
    pub subscription_fee: f64,
    /// Workspace-wide list-price cost for (month, backend); drives the ratio.
    pub list_cost: f64,
    /// List/real-price cost of the displayed (scoped) usage.
    pub scoped_cost: f64,
    /// `fee / workspace_list_cost` for subscriptions; `1.0` for metered;
    /// `None` when undefined (a fee is paid but there is no priced usage).
    pub ratio: Option<f64>,
    /// Effective cost of the scoped usage: `scoped_cost * ratio` for a
    /// subscription, or real metered cost. 0 when the fee is unrecovered.
    pub effective_cost: f64,
    /// Billable tokens in the scoped usage for this (bucket, backend).
    pub billable_tokens: i64,
    /// Input-side token denominator (`input + cache_read + cache_create`).
    pub input_tokens: i64,
    /// Output-side token denominator.
    pub output_tokens: i64,
    /// Effective cost attributed to input tokens; sums with
    /// `effective_output_cost` to `effective_cost`.
    pub effective_input_cost: f64,
    /// Effective cost attributed to output tokens.
    pub effective_output_cost: f64,
}

/// Economics for one grouping key (a model alias or a normalized role).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EconomicsRow {
    pub key: String,
    pub billable_tokens: i64,
    pub cost_usd: f64,
    pub runs: i64,
    pub input_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_create_tokens: i64,
    pub output_tokens: i64,
    pub thinking_tokens: i64,
    /// `cache_read / total_input` (Claude cache efficiency); 0 when no input.
    pub cache_hit_ratio: f64,
    /// `thinking / output`; 0 when no output.
    pub thinking_share: f64,
}

/// Model and role economics plus the price-table source date.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ModelRoleEconomics {
    pub by_model: Vec<EconomicsRow>,
    pub by_role: Vec<EconomicsRow>,
    pub price_source_date: String,
}

// --- Tier B: tool-invocation (target/error) analytics ---

/// Result of running the tool-invocation backfill.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ToolBackfillSummary {
    pub runs_processed: i64,
    pub rows_written: i64,
    pub total_indexed: i64,
}

/// Activity and error rate for one target shape (verb × scheme × kind).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TargetShapeRow {
    pub verb: String,
    pub scheme: String,
    pub kind: String,
    pub count: i64,
    pub error_count: i64,
    pub error_rate: f64,
}

/// One frequently-targeted resource or file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopTargetRow {
    pub target: String,
    pub scheme: String,
    pub count: i64,
    pub error_count: i64,
}

/// One trend point of average tool calls per session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallsPerSessionPoint {
    pub bucket_start: i64,
    pub avg_calls: f64,
    pub session_count: i64,
}

/// Tool/verb frequency for one time bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolMixPoint {
    pub bucket_start: i64,
    pub verb: String,
    pub count: i64,
}

/// Run wall-time composition for one time bucket. `model_overhead_s` is the
/// completed run wall time that is not accounted for by tool calls; it includes
/// model generation plus orchestration overhead and any in-run waiting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TimeCompositionPoint {
    pub bucket_start: i64,
    pub wall_s: i64,
    pub tool_s: i64,
    pub model_overhead_s: i64,
    pub run_count: i64,
}

/// Tool execution seconds by verb for one time bucket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolTimeMixPoint {
    pub bucket_start: i64,
    pub verb: String,
    pub seconds: i64,
    pub count: i64,
}

/// Aggregated duration for one target shape, ordered by total seconds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CommandDurationRow {
    pub verb: String,
    pub scheme: String,
    pub kind: String,
    pub target: Option<String>,
    pub count: i64,
    pub total_s: i64,
    pub avg_s: f64,
    pub max_s: i64,
    pub error_count: i64,
}

/// Average completed run wall time per session for one time bucket. A session's
/// duration is the sum of its completed runs' wall time rather than
/// first-to-last idle span.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SessionDurationPoint {
    pub bucket_start: i64,
    pub session_count: i64,
    pub avg_s: f64,
}

/// Target-shape breakdown plus the most-targeted resources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TargetBreakdown {
    pub shapes: Vec<TargetShapeRow>,
    pub top_targets: Vec<TopTargetRow>,
    pub total: i64,
}

// --- Delivery economics: normalized per merged PR shipped ---

/// Economics for one grouping key (a model alias or a normalized role),
/// normalized per merged PR that key's jobs shipped. Unlike [`EconomicsRow`]
/// (which scales with raw usage), the per-PR ratios expose the marginal cost of
/// shipping a unit of work, so a cheap-but-chatty model and an expensive-but-
/// terse one become directly comparable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PrEconomicsRow {
    pub key: String,
    /// Merged PRs attributed to this key's jobs.
    pub pr_count: i64,
    /// Total delivery cost (real metered where reported, else priced estimate).
    pub cost_usd: f64,
    pub billable_tokens: i64,
    pub lines_changed: i64,
    /// `cost_usd / pr_count`; 0 when no PRs.
    pub cost_per_pr: f64,
    /// `billable_tokens / pr_count`; 0 when no PRs.
    pub tokens_per_pr: f64,
    /// `billable_tokens / lines_changed`; 0 when no lines.
    pub tokens_per_line: f64,
}

/// Per-model and per-role delivery economics normalized per merged PR, plus the
/// price-table source date.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct MergedPrEconomics {
    pub by_model: Vec<PrEconomicsRow>,
    pub by_role: Vec<PrEconomicsRow>,
    pub price_source_date: String,
}

/// One time-bucket point of delivery efficiency: the cost and tokens the
/// workspace spent on the merged PRs that landed in the bucket. Bucketed on the
/// PR's `merged_at`, so the trend answers "is shipping getting cheaper?".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PrCostTrendPoint {
    pub bucket_start: i64,
    pub pr_count: i64,
    pub cost_usd: f64,
    pub billable_tokens: i64,
    pub lines_changed: i64,
}

/// One merged issue's lead time: the seconds from issue creation to its first
/// merged PR. The distribution of these values is the cycle-time histogram.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IssueLeadTimePoint {
    pub lead_seconds: i64,
    /// `merged_at` of the first merged PR, so the client can slice by time.
    pub merged_at: i64,
}
