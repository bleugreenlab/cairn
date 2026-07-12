//! Per-model and per-role economics: token/cost accumulation, delivery
//! economics normalized per merged PR, the merged-PR cost trend, and issue lead
//! times.

use std::collections::HashMap;

use cairn_db::storage::{DbResult, LocalDb};

use super::cost::exact_or_priced;
use super::pricing;
use super::queries;
use super::roles::normalize_role;
use super::types::{
    Bucket, EconomicsRow, IssueLeadTimePoint, MergedPrEconomics, ModelRoleEconomics,
    PrCostTrendPoint, PrEconomicsRow, Scope, TimeRange,
};

/// Token/cost accumulator for one grouping key.
#[derive(Default)]
struct Acc {
    billable: i64,
    cost: f64,
    runs: i64,
    input: i64,
    cache_read: i64,
    cache_create: i64,
    output: i64,
    thinking: i64,
}

impl Acc {
    fn add(&mut self, row: &queries::GranularRow, cost: f64) {
        self.billable += row.billable;
        self.cost += cost;
        self.runs += row.runs;
        self.input += row.input;
        self.cache_read += row.cache_read;
        self.cache_create += row.cache_create;
        self.output += row.output;
        self.thinking += row.thinking;
    }
}

fn finalize(map: HashMap<String, Acc>) -> Vec<EconomicsRow> {
    let mut rows: Vec<EconomicsRow> = map
        .into_iter()
        .map(|(key, a)| {
            // Persisted prompt components are disjoint for every backend.
            let total_input = a.input + a.cache_read + a.cache_create;
            let cache_hit_ratio = if total_input > 0 {
                a.cache_read as f64 / total_input as f64
            } else {
                0.0
            };
            let thinking_share = if a.output > 0 {
                a.thinking as f64 / a.output as f64
            } else {
                0.0
            };
            EconomicsRow {
                key,
                billable_tokens: a.billable,
                cost_usd: a.cost,
                runs: a.runs,
                input_tokens: a.input,
                cache_read_tokens: a.cache_read,
                cache_create_tokens: a.cache_create,
                output_tokens: a.output,
                thinking_tokens: a.thinking,
                cache_hit_ratio,
                thinking_share,
            }
        })
        .collect();
    rows.sort_by(|a, b| b.billable_tokens.cmp(&a.billable_tokens));
    rows
}

/// Per-key accumulator for [`merged_pr_economics`]: sums delivery cost, tokens,
/// lines, and the PR count they were spent on.
#[derive(Default)]
struct PrAcc {
    pr_count: i64,
    cost: f64,
    billable: i64,
    lines: i64,
}

impl PrAcc {
    fn add(&mut self, row: &queries::MergedPrEconRow, cost: f64) {
        self.pr_count += row.pr_count;
        self.cost += cost;
        self.billable += row.billable;
        self.lines += row.lines;
    }
}

/// Resolve accumulated delivery economics into per-PR ratios, sorted by total
/// cost descending (the most expensive-to-ship key first).
fn finalize_pr(map: HashMap<String, PrAcc>) -> Vec<PrEconomicsRow> {
    let mut rows: Vec<PrEconomicsRow> = map
        .into_iter()
        .map(|(key, a)| {
            let cost_per_pr = if a.pr_count > 0 {
                a.cost / a.pr_count as f64
            } else {
                0.0
            };
            let tokens_per_pr = if a.pr_count > 0 {
                a.billable as f64 / a.pr_count as f64
            } else {
                0.0
            };
            let tokens_per_line = if a.lines > 0 {
                a.billable as f64 / a.lines as f64
            } else {
                0.0
            };
            PrEconomicsRow {
                key,
                pr_count: a.pr_count,
                cost_usd: a.cost,
                billable_tokens: a.billable,
                lines_changed: a.lines,
                cost_per_pr,
                tokens_per_pr,
                tokens_per_line,
            }
        })
        .collect();
    rows.sort_by(|a, b| {
        b.cost_usd
            .partial_cmp(&a.cost_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

/// Per-model and per-role economics (tokens, cost, runs, efficiency ratios).
pub async fn model_role_economics(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<ModelRoleEconomics> {
    let rows = queries::economics_granular(db, scope, range).await?;
    let mut by_model: HashMap<String, Acc> = HashMap::new();
    let mut by_role: HashMap<String, Acc> = HashMap::new();
    for row in &rows {
        // Prefer the real metered cost when any event in the group reported one
        // (matching `group_cost`), else fall back to the price-table estimate.
        // Without this the table reads ~$0 for metered providers (OpenRouter,
        // z-ai, deepseek) whose price table is empty.
        let cost = if row.exact_cost_count > 0 {
            row.exact_cost
        } else {
            pricing::cost_usd(
                &row.backend,
                row.model.as_deref(),
                row.input,
                row.cache_read,
                row.cache_create,
                row.output,
            )
        };
        let model_key = row
            .model
            .clone()
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        by_model.entry(model_key).or_default().add(row, cost);
        // Role groups on the job's agent identity (`agent_config_id`), not its
        // free-form `node_name`. Delegated tasks and child issues set node_name
        // to the task title, which would otherwise pollute the role breakdown
        // with one row per task; the agent config id is the actual agent role.
        by_role
            .entry(normalize_role(row.agent_config_id.as_deref()))
            .or_default()
            .add(row, cost);
    }
    Ok(ModelRoleEconomics {
        by_model: finalize(by_model),
        by_role: finalize(by_role),
        price_source_date: pricing::PRICE_SOURCE_DATE.to_string(),
    })
}

/// Delivery economics normalized per merged PR, by model and by agent role.
///
/// The comparative counterpart to [`model_role_economics`]: instead of totals
/// that scale with raw usage, this divides each key's cost and tokens by the
/// number of merged PRs its jobs shipped, so a terse model and a chatty one
/// become directly comparable on "what does it cost to ship a PR". Costs use the
/// same [`exact_or_priced`] rule as every other $-valued analytic, and role
/// groups on the job's agent identity (`agent_config_id`), matching
/// `model_role_economics` rather than the free-form `node_name`.
pub async fn merged_pr_economics(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<MergedPrEconomics> {
    let rows = queries::merged_pr_economics_rows(db, scope, range).await?;
    let mut by_model: HashMap<String, PrAcc> = HashMap::new();
    let mut by_role: HashMap<String, PrAcc> = HashMap::new();
    for row in &rows {
        let cost = exact_or_priced(
            &row.backend,
            row.model.as_deref(),
            row.input,
            row.cache_read,
            row.cache_create,
            row.output,
            row.exact_cost,
            row.exact_cost_count,
        );
        let model_key = row
            .model
            .clone()
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        by_model.entry(model_key).or_default().add(row, cost);
        by_role
            .entry(normalize_role(row.agent_config_id.as_deref()))
            .or_default()
            .add(row, cost);
    }
    Ok(MergedPrEconomics {
        by_model: finalize_pr(by_model),
        by_role: finalize_pr(by_role),
        price_source_date: pricing::PRICE_SOURCE_DATE.to_string(),
    })
}

/// Delivery cost efficiency over time: cost, tokens, and lines per merged PR
/// bucketed on each PR's `merged_at`. Each (model, backend) group is priced via
/// [`exact_or_priced`] before summing per bucket, so the trend answers "is
/// shipping getting cheaper?" rather than echoing raw spend.
pub async fn merged_pr_cost_trend(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<PrCostTrendPoint>> {
    let rows = queries::merged_pr_cost_trend_rows(db, scope, range, bucket).await?;
    // (pr_count, cost, billable, lines) accumulated per bucket.
    let mut buckets: HashMap<i64, (i64, f64, i64, i64)> = HashMap::new();
    for row in &rows {
        let cost = exact_or_priced(
            &row.backend,
            row.model.as_deref(),
            row.input,
            row.cache_read,
            row.cache_create,
            row.output,
            row.exact_cost,
            row.exact_cost_count,
        );
        let e = buckets.entry(row.bucket_start).or_insert((0, 0.0, 0, 0));
        e.0 += row.pr_count;
        e.1 += cost;
        e.2 += row.billable;
        e.3 += row.lines;
    }
    let mut points: Vec<PrCostTrendPoint> = buckets
        .into_iter()
        .map(
            |(bucket_start, (pr_count, cost_usd, billable_tokens, lines_changed))| {
                PrCostTrendPoint {
                    bucket_start,
                    pr_count,
                    cost_usd,
                    billable_tokens,
                    lines_changed,
                }
            },
        )
        .collect();
    points.sort_by_key(|p| p.bucket_start);
    Ok(points)
}

/// Lead time (issue creation -> first merged PR) for every merged issue in the
/// range. The raw per-issue seconds are the cycle-time distribution the
/// dashboard renders as a histogram with median / p90 markers.
pub async fn issue_lead_times(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<IssueLeadTimePoint>> {
    let rows = queries::issue_lead_times(db, scope, range).await?;
    Ok(rows
        .into_iter()
        .map(|r| IssueLeadTimePoint {
            lead_seconds: r.lead_seconds,
            merged_at: r.merged_at,
        })
        .collect())
}
