//! Token-economics analytics: tokens per session, token-to-line efficiency, and
//! token-composition over time.

use crate::storage::{DbResult, LocalDb};

use super::cost::exact_or_priced;
use super::queries;
use super::roles::normalize_role;
use super::types::{
    Bucket, Scope, TimeRange, TokenCompositionPoint, TokensPerLocPoint, TokensPerSessionPoint,
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
