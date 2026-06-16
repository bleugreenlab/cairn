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
}

impl Bucket {
    /// Bucket width in seconds. The value is a fixed constant, never user text,
    /// so it is safe to interpolate directly into a SQL expression.
    pub(crate) fn seconds(self) -> i64 {
        match self {
            Bucket::Day => 86_400,
            Bucket::Week => 604_800,
        }
    }

    /// Parse a raw frontend string; anything but an explicit `week` is `Day`.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw {
            Some(s) if s.eq_ignore_ascii_case("week") => Bucket::Week,
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
