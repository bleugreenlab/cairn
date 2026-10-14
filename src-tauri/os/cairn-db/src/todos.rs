use crate::storage::DbResult;
use crate::turso::params;

/// Complete authored in-progress work when its owning job authoritatively
/// reaches `complete`. Callers must own that exact job-status transition; run,
/// turn, and session boundaries are not completion signals.
pub async fn complete_in_progress_todos_conn(
    conn: &crate::turso::Connection,
    job_id: &str,
    now: i64,
) -> DbResult<()> {
    conn.execute(
        "UPDATE todos
         SET status = 'completed', updated_at = ?1
         WHERE job_id = ?2 AND status = 'in_progress'",
        params![now, job_id],
    )
    .await?;
    Ok(())
}
