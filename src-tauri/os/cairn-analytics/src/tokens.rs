//! Token-economics analytics: tokens per session, token-to-line efficiency,
//! token volume per model over time, and token-composition over time.

use std::collections::HashMap;

use cairn_db::storage::{DbResult, LocalDb};

use super::cost::exact_or_priced;
use super::queries;
use super::roles::normalize_role;
use super::types::{
    Bucket, ModelTokenPoint, Scope, TimeRange, TokenCompositionPoint, TokensPerLocPoint,
    TokensPerSessionPoint,
};

/// Average billable tokens per session, bucketed by the session's first event.
pub async fn avg_tokens_per_session(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<TokensPerSessionPoint>> {
    let rows = queries::avg_tokens_per_session(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| TokensPerSessionPoint {
            bucket_start: r.bucket_start,
            avg_tokens: r.avg_tokens,
            session_count: r.session_count,
        })
        .collect())
}

/// Token-to-line efficiency for every job that produced a merge request.
pub async fn tokens_per_loc(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<Vec<TokensPerLocPoint>> {
    let rows = queries::tokens_per_loc(db, scope, range).await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let tokens_per_line = if r.lines > 0 {
                r.billable as f64 / r.lines as f64
            } else {
                0.0
            };
            let cost_usd = exact_or_priced(
                &r.backend,
                r.model.as_deref(),
                r.input,
                r.cache_read,
                r.cache_create,
                r.output,
                r.exact_cost,
                r.exact_cost_count,
            );
            TokensPerLocPoint {
                job_id: r.job_id,
                ts: r.ts,
                billable_tokens: r.billable,
                lines_changed: r.lines,
                tokens_per_line,
                cost_usd,
                model: r.model,
                role: normalize_role(r.node_name.as_deref()),
            }
        })
        .collect())
}

/// Billable tokens over time split per model — the dashboard's main chart.
///
/// Reuses [`queries::cost_components`] (no new SQL), folding its
/// (bucket, model, backend) groups down to (bucket, model): the same alias
/// served through two providers is one model to the reader. Because the fold
/// only drops the backend key, each bucket's model heights sum to the same
/// billable total as [`super::cost_timeseries`].
///
/// Groups with no billable tokens are dropped. A metered settlement can land in
/// a (bucket, model) group that carries no token event — a zero-height bar whose
/// only content is cost, which this token-volume view has nothing to say about.
pub async fn tokens_by_model_timeseries(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ModelTokenPoint>> {
    let rows = queries::cost_components(db, scope, range, bucket).await?;
    let mut agg: HashMap<(i64, String), ModelTokenPoint> = HashMap::new();
    for row in &rows {
        let model = row
            .model
            .clone()
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        let entry = agg
            .entry((row.bucket_start, model.clone()))
            .or_insert_with(|| ModelTokenPoint {
                bucket_start: row.bucket_start,
                model,
                billable_tokens: 0,
                input_tokens: 0,
                output_tokens: 0,
                cost_usd: 0.0,
            });
        entry.billable_tokens += row.billable;
        entry.input_tokens += row.input + row.cache_read + row.cache_create;
        entry.output_tokens += row.output;
        entry.cost_usd += exact_or_priced(
            &row.backend,
            row.model.as_deref(),
            row.input,
            row.cache_read,
            row.cache_create,
            row.output,
            row.exact_cost,
            row.exact_cost_count,
        );
    }
    let mut points: Vec<ModelTokenPoint> = agg
        .into_values()
        .filter(|p| p.billable_tokens > 0)
        .collect();
    points.sort_by(|a, b| {
        a.bucket_start
            .cmp(&b.bucket_start)
            .then_with(|| a.model.cmp(&b.model))
    });
    Ok(points)
}

/// Token components (input / cache-read / cache-create / output / thinking) over
/// time, for the stacked token-composition chart.
pub async fn token_composition_timeseries(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<TokenCompositionPoint>> {
    let rows = queries::token_components(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| TokenCompositionPoint {
            bucket_start: r.bucket_start,
            input: r.input,
            cache_read: r.cache_read,
            cache_create: r.cache_create,
            output: r.output,
            thinking: r.thinking,
        })
        .collect())
}
