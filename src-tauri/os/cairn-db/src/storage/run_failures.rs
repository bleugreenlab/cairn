use super::{DbResult, TrackedConnection};
use crate::turso::params;

/// First-cause-wins writer shared by live finalization and recovery. Recovery
/// may annotate an already-ended run; it must never replace its original cause.
pub async fn record_run_failure_conn(
    conn: &TrackedConnection,
    run_id: &str,
    exit_reason: Option<&str>,
    error_message: &str,
    active_only: bool,
) -> DbResult<bool> {
    let changed = conn
        .execute(
            "UPDATE runs
         SET exit_reason = COALESCE(?1, exit_reason),
             error_message = COALESCE(error_message, ?2),
             updated_at = ?3
         WHERE id = ?4 AND (?5 = 0 OR status IN ('starting', 'live'))",
            params![
                exit_reason,
                error_message,
                chrono::Utc::now().timestamp(),
                run_id,
                i64::from(active_only)
            ],
        )
        .await?;
    Ok(changed > 0)
}
