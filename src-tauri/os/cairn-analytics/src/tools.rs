//! Tool-usage analytics: error rate, usage heatmap, target-shape breakdown,
//! tool-call and tool-time mixes, run wall-time composition, command durations,
//! and session durations.

use cairn_db::storage::{DbResult, LocalDb};

use super::queries;
use super::types::{
    Bucket, CommandDurationRow, HeatmapCell, Scope, SessionDurationPoint, TargetBreakdown,
    TargetShapeRow, TimeCompositionPoint, TimeRange, ToolCallsPerSessionPoint, ToolErrorRatePoint,
    ToolMixPoint, ToolTimeMixPoint,
};

/// Tool-call error rate over time (errors / total per bucket).
pub async fn tool_error_rate(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolErrorRatePoint>> {
    let rows = queries::tool_error_rate(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| ToolErrorRatePoint {
            bucket_start: r.bucket_start,
            total: r.total,
            errors: r.errors,
            error_rate: if r.total > 0 {
                r.errors as f64 / r.total as f64
            } else {
                0.0
            },
        })
        .collect())
}

/// Billable-token usage heatmap by local-time day-of-week and hour, summed from
/// the hour-grain `token_rollup`. `tz_offset_minutes` is the user's offset from
/// UTC (`-getTimezoneOffset()`), converted to seconds and applied inside
/// `strftime` so "when work happens" reads in local time.
pub async fn usage_heatmap(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    tz_offset_minutes: i64,
) -> DbResult<Vec<HeatmapCell>> {
    let rows = queries::usage_heatmap(db, scope, range, tz_offset_minutes * 60).await?;
    Ok(rows
        .into_iter()
        .map(|r| HeatmapCell {
            day_of_week: r.day_of_week,
            hour: r.hour,
            tokens: r.tokens,
        })
        .collect())
}

/// Target-shape activity, per-shape error rate, and the most-targeted resources.
pub async fn target_breakdown(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
) -> DbResult<TargetBreakdown> {
    let raw = queries::target_shapes(db, scope, range).await?;
    let total: i64 = raw.iter().map(|s| s.count).sum();
    let shapes = raw
        .into_iter()
        .map(|s| TargetShapeRow {
            error_rate: if s.count > 0 {
                s.error_count as f64 / s.count as f64
            } else {
                0.0
            },
            verb: s.verb,
            scheme: s.scheme,
            kind: s.kind,
            count: s.count,
            error_count: s.error_count,
        })
        .collect();
    let top_targets = queries::top_targets(db, scope, range).await?;
    Ok(TargetBreakdown {
        shapes,
        top_targets,
        total,
    })
}

/// Average tool calls per session over time. Powered by the tool-invocation
/// rollup (Tier B), so it returns empty until the index is built.
pub async fn avg_tool_calls_per_session(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolCallsPerSessionPoint>> {
    let rows = queries::tool_calls_per_session(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| ToolCallsPerSessionPoint {
            bucket_start: r.bucket_start,
            avg_calls: r.avg_calls,
            session_count: r.session_count,
        })
        .collect())
}

/// Tool-call frequency by verb over time.
pub async fn tool_mix(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolMixPoint>> {
    let rows = queries::tool_mix(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| ToolMixPoint {
            bucket_start: r.bucket_start,
            verb: r.verb,
            count: r.count,
        })
        .collect())
}

/// Completed run wall-time composition: tool execution vs model generation plus
/// orchestration overhead. The remainder deliberately includes permission waits
/// and other in-run overhead, so callers should label it "model + overhead".
pub async fn time_composition(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<TimeCompositionPoint>> {
    let rows = queries::time_composition(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| TimeCompositionPoint {
            bucket_start: r.bucket_start,
            wall_s: r.wall_s,
            tool_s: r.tool_s,
            model_overhead_s: r.model_overhead_s,
            run_count: r.run_count,
        })
        .collect())
}

/// Tool execution seconds by verb over time.
pub async fn tool_time_mix(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<ToolTimeMixPoint>> {
    let rows = queries::tool_time_mix(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| ToolTimeMixPoint {
            bucket_start: r.bucket_start,
            verb: r.verb,
            seconds: r.seconds,
            count: r.count,
        })
        .collect())
}

/// Slowest target shapes by total tool-execution time.
pub async fn command_durations(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    limit: i64,
) -> DbResult<Vec<CommandDurationRow>> {
    let rows = queries::command_durations(db, scope, range, limit).await?;
    Ok(rows
        .into_iter()
        .map(|r| CommandDurationRow {
            verb: r.verb,
            scheme: r.scheme,
            kind: r.kind,
            target: r.target,
            count: r.count,
            total_s: r.total_s,
            avg_s: r.avg_s,
            max_s: r.max_s,
            error_count: r.error_count,
        })
        .collect())
}

/// Average active work time per session over time. Session duration is the sum
/// of completed run wall time in the selected range.
pub async fn session_durations(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<Vec<SessionDurationPoint>> {
    let rows = queries::session_durations(db, scope, range, bucket).await?;
    Ok(rows
        .into_iter()
        .map(|r| SessionDurationPoint {
            bucket_start: r.bucket_start,
            session_count: r.session_count,
            avg_s: r.avg_s,
        })
        .collect())
}
