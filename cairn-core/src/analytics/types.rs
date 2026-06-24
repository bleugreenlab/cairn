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
            Bucket::Day => format!("({ts_col} / 86400) * 86400"),
            Bucket::Week => format!("({ts_col} / 604800) * 604800"),
            Bucket::Month => {
                format!("CAST(strftime('%s', {ts_col}, 'unixepoch', 'start of month') AS INTEGER)")
            }
        }
    }

    /// Parse a raw frontend string: `week`, `month`, else `day`.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw {
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
    pub model: Option<String>,
    pub role: String,
}

/// One trend point of priced cost.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CostPoint {
    pub bucket_start: i64,
    pub cost_usd: f64,
    pub billable_tokens: i64,
}

/// One (month, backend) effective-cost point. `effective_cost` allocates a
/// provider's flat subscription fee across its usage, or passes through real
/// metered cost. See `analytics::effective_cost`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveCostPoint {
    /// First-of-month epoch seconds (UTC).
    pub month_start: i64,
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
    /// Billable tokens in the scoped usage for this (month, backend).
    pub billable_tokens: i64,
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

/// Target-shape breakdown plus the most-targeted resources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TargetBreakdown {
    pub shapes: Vec<TargetShapeRow>,
    pub top_targets: Vec<TopTargetRow>,
    pub total: i64,
}
