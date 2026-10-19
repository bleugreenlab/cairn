//! Priced-cost analytics: the canonical exact-or-priced cost rule plus the
//! cost-over-time, per-backend, per-project, per-provider, and
//! subscription-normalized (effective) cost queries.

use std::collections::HashMap;

use cairn_db::storage::{DbResult, LocalDb};

use super::pricing;
use super::queries;
use super::subscriptions;
use super::types::{
    BackendCostPoint, Bucket, CostPoint, EffectiveCostPoint, ProjectCost, Scope, TimeRange,
};

/// The canonical token-group cost rule: the real metered cost (`events.cost_usd`)
/// when any event in the group reported one, else the price-table estimate. The
/// price table returns ~$0 for many metered (OpenRouter) models, so a real cost
/// is always preferred when present. Every $-valued analytic routes through this.
#[allow(clippy::too_many_arguments)]
pub(super) fn exact_or_priced(
    backend: &str,
    model: Option<&str>,
    input: i64,
    cache_read: i64,
    cache_create: i64,
    output: i64,
    exact_cost: f64,
    exact_cost_count: i64,
) -> f64 {
    if exact_cost_count > 0 {
        exact_cost
    } else {
        pricing::cost_usd(backend, model, input, cache_read, cache_create, output)
    }
}

/// The cost of one (bucket, model, backend) group via [`exact_or_priced`].
fn group_cost(row: &queries::CostComponentRow) -> f64 {
    exact_or_priced(
        &row.backend,
        row.model.as_deref(),
        row.input,
        row.cache_read,
        row.cache_create,
        row.output,
        row.exact_cost,
        row.exact_cost_count,
    )
}

/// Real per-model cost for one backend: the metered dollar amount Cairn recorded
/// for that model, with billable tokens and run count.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderModelCost {
    pub model: String,
    pub cost_usd: f64,
    pub billable_tokens: i64,
    pub runs: i64,
}

/// Priced cost over time, summed per bucket across each (model, backend) group.
pub async fn cost_timeseries(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<CostPoint>> {
    let rows = queries::cost_components(db, scope, range, bucket).await?;
    let mut buckets: HashMap<i64, (f64, i64)> = HashMap::new();
    for row in &rows {
        let entry = buckets.entry(row.bucket_start).or_insert((0.0, 0));
        entry.0 += group_cost(row);
        entry.1 += row.billable;
    }
    let mut points: Vec<CostPoint> = buckets
        .into_iter()
        .map(|(bucket_start, (cost_usd, billable_tokens))| CostPoint {
            bucket_start,
            cost_usd,
            billable_tokens,
        })
        .collect();
    points.sort_by_key(|p| p.bucket_start);
    Ok(points)
}

/// Priced cost over time split per backend, summed per (bucket, backend) group.
/// Reuses [`queries::cost_components`] (no new SQL); the per-bucket backend stack
/// heights sum to the blended [`cost_timeseries`] total. Powers the stacked
/// provider-split cost chart.
pub async fn cost_by_backend_timeseries(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<BackendCostPoint>> {
    let rows = queries::cost_components(db, scope, range, bucket).await?;
    let mut buckets: HashMap<(i64, String), (f64, i64)> = HashMap::new();
    for row in &rows {
        let entry = buckets
            .entry((row.bucket_start, row.backend.clone()))
            .or_insert((0.0, 0));
        entry.0 += group_cost(row);
        entry.1 += row.billable;
    }
    let mut points: Vec<BackendCostPoint> = buckets
        .into_iter()
        .map(
            |((bucket_start, backend), (cost_usd, billable_tokens))| BackendCostPoint {
                bucket_start,
                backend,
                cost_usd,
                billable_tokens,
            },
        )
        .collect();
    points.sort_by(|a, b| {
        a.bucket_start
            .cmp(&b.bucket_start)
            .then_with(|| a.backend.cmp(&b.backend))
    });
    Ok(points)
}

/// Total real cost per project across the range, preferring metered cost over
/// the price table per (model, backend) group, sorted by cost descending. The
/// workspace-only cost-by-project chart.
pub async fn cost_by_project(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<ProjectCost>> {
    let rows = queries::cost_by_project(db, scope, range).await?;
    let mut by_project: HashMap<String, (String, f64, i64)> = HashMap::new();
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
        let entry = by_project
            .entry(row.project_id.clone())
            .or_insert_with(|| (row.project_name.clone(), 0.0, 0));
        entry.1 += cost;
        entry.2 += row.billable;
    }
    let mut out: Vec<ProjectCost> = by_project
        .into_iter()
        .map(
            |(project_id, (project_name, cost_usd, billable_tokens))| ProjectCost {
                project_id,
                project_name,
                cost_usd,
                billable_tokens,
            },
        )
        .collect();
    out.sort_by(|a, b| {
        b.cost_usd
            .partial_cmp(&a.cost_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(out)
}

/// Per-model cost for a single backend via [`exact_or_priced`], sorted by cost
/// descending.
///
/// This is the canonical answer for "what did we actually bill per model on this
/// backend" and powers the provider usage card (e.g. OpenRouter), where the
/// price table returns ~$0 for many metered models so the recorded cost carries
/// the whole answer. It differs from [`crate::model_role_economics`] in scope,
/// not in pricing: this breaks one backend down by model, while that is the
/// all-backend dashboard view grouped by model and by agent role. Both price
/// their groups by the same rule.
pub async fn provider_model_costs(
    db: &LocalDb,
    backend: &str,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<ProviderModelCost>> {
    let rows = queries::provider_model_components(db, backend, scope, range).await?;
    let mut costs: Vec<ProviderModelCost> = rows
        .into_iter()
        .map(|row| {
            let cost_usd = exact_or_priced(
                &row.backend,
                row.model.as_deref(),
                row.input,
                row.cache_read,
                row.cache_create,
                row.output,
                row.exact_cost,
                row.exact_cost_count,
            );
            let model = row
                .model
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| "unknown".to_string());
            ProviderModelCost {
                model,
                cost_usd,
                billable_tokens: row.billable,
                runs: row.runs,
            }
        })
        .collect();
    costs.sort_by(|a, b| {
        b.cost_usd
            .partial_cmp(&a.cost_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(costs)
}

/// First-of-month (UTC) epoch seconds for the given epoch. Mirrors the SQLite
/// `start of month` bucketing, so a fine (day/week/hour) bucket maps to the
/// calendar month whose flat fee it draws against.
fn month_floor(epoch: i64) -> i64 {
    use chrono::{Datelike, TimeZone, Utc};
    let Some(dt) = Utc.timestamp_opt(epoch, 0).single() else {
        return 0;
    };
    Utc.with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
        .single()
        .map(|d| d.timestamp())
        .unwrap_or(0)
}

/// First-of-month (UTC) epoch seconds for the current wall-clock time, so the
/// in-progress month is flagged provisional.
fn current_month_start() -> i64 {
    month_floor(chrono::Utc::now().timestamp())
}

/// Split an effective cost into input- and output-side components by the
/// list-price fraction, falling back to token share when the price table is
/// empty (e.g. an unknown metered model), else attributing nothing. The two
/// components always sum to `effective`.
fn split_effective(
    effective: f64,
    input_list: f64,
    output_list: f64,
    input_tokens: i64,
    output_tokens: i64,
) -> (f64, f64) {
    let list_denom = input_list + output_list;
    let frac_in = if list_denom > 0.0 {
        input_list / list_denom
    } else {
        let tok_denom = input_tokens + output_tokens;
        if tok_denom > 0 {
            input_tokens as f64 / tok_denom as f64
        } else {
            0.0
        }
    };
    (effective * frac_in, effective * (1.0 - frac_in))
}

/// The first instant of the month after the one starting at `month_start`.
fn next_month(month_start: i64) -> i64 {
    use chrono::{Datelike, TimeZone, Utc};
    let Some(dt) = Utc.timestamp_opt(month_start, 0).single() else {
        return month_start;
    };
    let (year, month) = if dt.month() == 12 {
        (dt.year() + 1, 1)
    } else {
        (dt.year(), dt.month() + 1)
    };
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .single()
        .map(|d| d.timestamp())
        .unwrap_or(month_start)
}

/// Effective (subscription-normalized) cost per (fine-bucket, backend).
///
/// A backend some subscription account served in a month is a subscription
/// that month: what it paid is the summed list price of the plans its
/// accounts held ([`subscriptions::month_subscriptions`], read from the
/// `subscription_periods` history), and the month's workspace-wide list-price
/// cost defines a ratio against it (`ratio = paid / list_cost`). The scoped
/// usage's list cost times that ratio is its share of what was paid, so the
/// effective costs across a provider-month sum to what was paid when scoped to
/// the whole workspace. A subscription month whose plans have no price shows
/// its list value as effective and no ratio, and reports the unpriced
/// accounts. A backend no subscription served is metered: it passes through
/// at its real cost (recorded per-generation cost, else list price) with
/// `ratio = 1`.
///
/// What was paid is allocated per calendar **month** (the ratio denominator),
/// but usage is emitted at the page's `bucket` granularity so the trend
/// follows the range selector: each fine bucket inherits the ratio of the
/// month that contains it (mapped via [`month_floor`]). Because the scoped
/// fine-bucket costs within a month sum to that month's scoped cost, the
/// per-bucket effective costs still sum to the paid share — exactly for
/// day/hour/month, and approximately for a week that straddles a month boundary
/// (it is attributed to the month of its `bucket_start`).
pub async fn effective_cost(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<EffectiveCostPoint>> {
    let periods = subscriptions::plan_periods(db).await?;
    // The ratio denominator is always workspace-wide and per calendar month,
    // even when display usage is scoped to a project or shown at a finer bucket:
    // a project shows its effective share of what the month paid.
    let workspace = Scope::new(None);
    let workspace_rows = queries::cost_components(db, &workspace, range, Bucket::Month).await?;
    // Scoped usage at the page granularity so the trend follows the range.
    let scoped_rows = queries::cost_components(db, scope, range, bucket).await?;

    let mut workspace_cost: HashMap<(i64, String), f64> = HashMap::new();
    for row in &workspace_rows {
        *workspace_cost
            .entry((row.bucket_start, row.backend.clone()))
            .or_default() += group_cost(row);
    }

    #[derive(Default)]
    struct FineAgg {
        cost: f64,
        billable: i64,
        input_tokens: i64,
        output_tokens: i64,
        /// Input/output split of the list price, summed from per-component
        /// pricing calls (linear, so they add back to the full list cost).
        input_list: f64,
        output_list: f64,
    }
    let mut fine: HashMap<(i64, String), FineAgg> = HashMap::new();
    for row in &scoped_rows {
        // Split list price into input/output sides by calling the linear pricing
        // function once per side: input_list + output_list == the full cost.
        let input_list = pricing::cost_usd(
            &row.backend,
            row.model.as_deref(),
            row.input,
            row.cache_read,
            row.cache_create,
            0,
        );
        let output_list =
            pricing::cost_usd(&row.backend, row.model.as_deref(), 0, 0, 0, row.output);
        let entry = fine
            .entry((row.bucket_start, row.backend.clone()))
            .or_default();
        entry.cost += group_cost(row);
        entry.billable += row.billable;
        entry.input_tokens += row.input + row.cache_read + row.cache_create;
        entry.output_tokens += row.output;
        entry.input_list += input_list;
        entry.output_list += output_list;
    }

    let current_month = current_month_start();
    let mut months: HashMap<(i64, String), subscriptions::MonthSubscriptions> = HashMap::new();
    let mut points: Vec<EffectiveCostPoint> = fine
        .into_iter()
        .map(|((bucket_start, backend), agg)| {
            let month_start = month_floor(bucket_start);
            let workspace_list = workspace_cost
                .get(&(month_start, backend.clone()))
                .copied()
                .unwrap_or(0.0);
            let month = months
                .entry((month_start, backend.clone()))
                .or_insert_with(|| {
                    subscriptions::month_subscriptions(
                        &periods,
                        &backend,
                        month_start,
                        next_month(month_start),
                    )
                })
                .clone();
            let provisional = month_start == current_month;
            let metered = !month.any();

            // Resolve the month's allocation rule into (ratio, effective).
            let (ratio, effective_cost) = match month.paid {
                // Nothing subscribed: paid as used, at the real cost.
                _ if metered => (Some(1.0), agg.cost),
                Some(paid) if workspace_list > 0.0 => {
                    let ratio = paid / workspace_list;
                    (Some(ratio), agg.cost * ratio)
                }
                // Paid, but no usage the price table can value: the ratio is
                // undefined and the paid amount is unrecovered.
                Some(_) => (None, 0.0),
                // Subscribed on plans with no price: the list value stands,
                // never a fabricated $0.
                None => (None, agg.cost),
            };
            let (effective_input_cost, effective_output_cost) = split_effective(
                effective_cost,
                agg.input_list,
                agg.output_list,
                agg.input_tokens,
                agg.output_tokens,
            );
            EffectiveCostPoint {
                month_start,
                bucket_start,
                backend,
                metered,
                provisional,
                subscription_cost: month.paid,
                plans: month.plans,
                unpriced_accounts: month.unpriced,
                list_cost: workspace_list,
                scoped_cost: agg.cost,
                ratio,
                effective_cost,
                billable_tokens: agg.billable,
                input_tokens: agg.input_tokens,
                output_tokens: agg.output_tokens,
                effective_input_cost,
                effective_output_cost,
            }
        })
        .collect();
    points.sort_by(|a, b| {
        a.bucket_start
            .cmp(&b.bucket_start)
            .then_with(|| a.backend.cmp(&b.backend))
    });
    Ok(points)
}
