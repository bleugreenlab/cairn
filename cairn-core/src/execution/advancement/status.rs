use super::*;

async fn execution_issue_id_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
) -> DbResult<Option<String>> {
    crate::storage::query_opt_text_conn(
        conn,
        "SELECT issue_id FROM executions WHERE id = ?1",
        (execution_id,),
    )
    .await
}

// `recompute_execution_and_issue` was removed in CAIRN-1156: job status is a
// derived projection, and its only caller (executor completion) now records the
// expansion fact and calls `recompute_execution_jobs_conn`.

/// Claim a ready pending job by transitioning it `pending → running`. This is the
/// single claim point: scanning only `pending` jobs and flipping them to Running
/// in one txn guarantees each job is handed off (and acted on) exactly once. There
/// is no `Ready` resting state — for an agent job the host then spawns the backend;
/// executor/checkpoint jobs are handled inline by `reduce_dag`.
pub(super) async fn transition_job_to_running_conn(
    conn: &cairn_db::turso::Connection,
    job: &DbJob,
) -> DbResult<()> {
    let from: JobStatus = job
        .status
        .parse()
        .map_err(|e| db_internal(format!("Invalid job status for {}: {e}", job.id)))?;
    if from != JobStatus::Pending {
        return Err(db_internal(format!(
            "Invalid job transition for {}: {from} -> running",
            job.id
        )));
    }

    let now = chrono::Utc::now().timestamp() as i32;
    conn.execute(
        "UPDATE jobs SET status = 'running', started_at = ?1, updated_at = ?1 WHERE id = ?2",
        params![now, job.id.as_str()],
    )
    .await?;

    if let Some(execution_id) = job.execution_id.as_deref() {
        crate::transitions::outcome::recompute_execution_status_conn(conn, execution_id).await?;
        let effective_issue_id =
            job.issue_id
                .clone()
                .or(execution_issue_id_conn(conn, execution_id).await?);
        if let Some(issue_id) = effective_issue_id {
            crate::transitions::outcome::recompute_issue_status_conn(conn, &issue_id).await?;
        }
    } else if let Some(issue_id) = job.issue_id.as_deref() {
        crate::transitions::outcome::recompute_issue_status_conn(conn, issue_id).await?;
    }

    Ok(())
}
