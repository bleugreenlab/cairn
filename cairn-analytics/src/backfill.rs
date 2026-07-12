//! Analytics rollup backfill: the on-demand tool-invocation backfill and the
//! startup historical repair passes with their completion markers.

use cairn_db::storage::{DbResult, LocalDb, RowExt};

use super::queries;
use super::types::ToolBackfillSummary;

/// Run the incremental tool-invocation backfill (Tier B). Off the hot path;
/// safe to invoke on demand from the dashboard.
pub async fn backfill_tool_invocations(db: &LocalDb) -> DbResult<ToolBackfillSummary> {
    queries::backfill_tool_invocations(db).await
}

/// Run startup analytics rollup repair passes. Spawn it on a background task at
/// startup.
///
/// Live event inserts now maintain both rollups incrementally
/// (`queries::maintain_rollups_on_insert`, hooked into the insert transaction),
/// but events that predate that seam were never folded. The original historical
/// pass populates both tiers — the Tier A token/cost fold and the Tier B
/// reconstruct-based tool-invocation backfill — then records completion so it
/// never re-scans the whole events table on later startups. A second marker
/// versions the narrower result-timestamp repair added after `result_ts`
/// existed: installs that already completed the original pass only revisit runs
/// that have NULL-result tool_invocations rows.
pub async fn run_historical_backfill(db: &LocalDb) -> DbResult<()> {
    let ran_full_backfill = if historical_backfill_complete(db).await? {
        false
    } else {
        queries::fold_token_rollup(db).await?;
        queries::backfill_tool_invocations(db).await?;
        mark_historical_backfill_complete(db).await?;
        true
    };

    if !tool_result_backfill_complete(db).await? {
        if !ran_full_backfill {
            queries::reconcile_tool_invocation_results(db).await?;
        }
        mark_tool_result_backfill_complete(db).await?;
    }

    Ok(())
}

async fn historical_backfill_complete(db: &LocalDb) -> DbResult<bool> {
    db.query_one(
        "SELECT backfilled_at FROM analytics_rollup_backfill_state WHERE id = 1",
        (),
        |row| Ok(row.opt_i64(0)?.is_some()),
    )
    .await
}

async fn mark_historical_backfill_complete(db: &LocalDb) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "UPDATE analytics_rollup_backfill_state SET backfilled_at = ?1 WHERE id = 1",
        (now,),
    )
    .await
    .map(|_| ())
}

async fn tool_result_backfill_complete(db: &LocalDb) -> DbResult<bool> {
    db.query_one(
        "SELECT tool_results_backfilled_at FROM analytics_rollup_backfill_state WHERE id = 1",
        (),
        |row| Ok(row.opt_i64(0)?.is_some()),
    )
    .await
}

async fn mark_tool_result_backfill_complete(db: &LocalDb) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "UPDATE analytics_rollup_backfill_state SET tool_results_backfilled_at = ?1 WHERE id = 1",
        (now,),
    )
    .await
    .map(|_| ())
}
