//! Execution and issue status recomputes.
//!
//! Execution status is recomputed from its jobs and action runs; issue status
//! (progress + attention) is recomputed from its executions, prompts, permission
//! requests, blocked jobs, and merge requests. Job status — the level beneath —
//! is itself a projection now (see [`crate::transitions::projection`] and
//! `execution::advancement::recompute`); these two functions are the upper
//! projections that slot above it.

use crate::models::{IssueAttention, IssueProgress};
use crate::storage::{DbError, DbResult, RowExt};
use turso::params;

pub async fn recompute_execution_status_conn(
    conn: &turso::Connection,
    execution_id: &str,
) -> DbResult<()> {
    let non_terminal_jobs = count(
        conn,
        "
        SELECT COUNT(*)
        FROM jobs
        WHERE execution_id = ?1 AND status NOT IN ('complete', 'failed', 'cancelled')
        ",
        execution_id,
    )
    .await?;

    if non_terminal_jobs > 0 {
        conn.execute(
            "UPDATE executions SET status = 'running' WHERE id = ?1",
            (execution_id,),
        )
        .await?;
        return Ok(());
    }

    let non_terminal_actions = count(
        conn,
        "
        SELECT COUNT(*)
        FROM action_runs
        WHERE execution_id = ?1 AND status NOT IN ('complete', 'failed')
        ",
        execution_id,
    )
    .await?;

    if non_terminal_actions > 0 {
        conn.execute(
            "UPDATE executions SET status = 'running' WHERE id = ?1",
            (execution_id,),
        )
        .await?;
        return Ok(());
    }

    let failed_jobs = count(
        conn,
        "SELECT COUNT(*) FROM jobs WHERE execution_id = ?1 AND status = 'failed'",
        execution_id,
    )
    .await?;
    let failed_actions = count(
        conn,
        "SELECT COUNT(*) FROM action_runs WHERE execution_id = ?1 AND status = 'failed'",
        execution_id,
    )
    .await?;

    let now = chrono::Utc::now().timestamp();
    if failed_jobs > 0 || failed_actions > 0 {
        conn.execute(
            "UPDATE executions SET status = 'failed', completed_at = ?1 WHERE id = ?2",
            params![now, execution_id],
        )
        .await?;
    } else {
        conn.execute(
            "UPDATE executions SET status = 'complete', completed_at = ?1 WHERE id = ?2",
            params![now, execution_id],
        )
        .await?;
    }
    Ok(())
}

pub async fn recompute_issue_status_conn(conn: &turso::Connection, issue_id: &str) -> DbResult<()> {
    let Some((progress, attention)) = issue_progress_attention(conn, issue_id).await? else {
        return Ok(());
    };
    let status = match progress {
        IssueProgress::Merged => "merged",
        IssueProgress::Closed => "closed",
        _ if attention.blocks_status_projection() => "waiting",
        IssueProgress::Backlog => "backlog",
        IssueProgress::Active => "active",
        IssueProgress::Complete => "complete",
        IssueProgress::Failed => "failed",
    };

    let now = chrono::Utc::now().timestamp();
    if matches!(progress, IssueProgress::Complete | IssueProgress::Failed)
        && issue_completed_at(conn, issue_id).await?.is_none()
    {
        conn.execute(
            "
            UPDATE issues
            SET status = ?1, progress = ?2, attention = ?3,
                completed_at = ?4, updated_at = ?5
            WHERE id = ?6
            ",
            params![
                status,
                progress.to_string(),
                attention.to_string(),
                now,
                now,
                issue_id
            ],
        )
        .await?;
    } else {
        conn.execute(
            "
            UPDATE issues
            SET status = ?1, progress = ?2, attention = ?3, updated_at = ?4
            WHERE id = ?5
            ",
            params![
                status,
                progress.to_string(),
                attention.to_string(),
                now,
                issue_id
            ],
        )
        .await?;
    }
    Ok(())
}

async fn issue_progress_attention(
    conn: &turso::Connection,
    issue_id: &str,
) -> DbResult<Option<(IssueProgress, IssueAttention)>> {
    let mut issue_rows = conn
        .query(
            "SELECT merged_at, closed_at FROM issues WHERE id = ?1",
            (issue_id,),
        )
        .await?;
    let Some(issue_row) = issue_rows.next().await? else {
        return Ok(None);
    };

    let progress = if issue_row.opt_i64(0)?.is_some() {
        IssueProgress::Merged
    } else if issue_row.opt_i64(1)?.is_some() {
        IssueProgress::Closed
    } else {
        let mut rows = conn
            .query(
                "SELECT status FROM executions WHERE issue_id = ?1",
                (issue_id,),
            )
            .await?;
        let mut statuses = Vec::new();
        while let Some(row) = rows.next().await? {
            statuses.push(row.text(0)?);
        }
        if statuses.is_empty() {
            IssueProgress::Backlog
        } else if statuses.iter().any(|status| status == "running") {
            IssueProgress::Active
        } else if statuses.iter().any(|status| status == "complete") {
            IssueProgress::Complete
        } else {
            IssueProgress::Failed
        }
    };

    let attention = if issue_count(
        conn,
        issue_id,
        "
        SELECT COUNT(*)
        FROM prompts p
        JOIN runs r ON p.run_id = r.id
        WHERE r.issue_id = ?1 AND p.response IS NULL
        ",
    )
    .await?
        > 0
    {
        IssueAttention::NeedsInput
    } else if issue_count(
        conn,
        issue_id,
        "
        SELECT COUNT(*)
        FROM permission_requests pr
        JOIN runs r ON pr.run_id = r.id
        WHERE r.issue_id = ?1 AND pr.status = 'pending'
        ",
    )
    .await?
        > 0
    {
        IssueAttention::NeedsAuthorization
    } else if issue_count(
        conn,
        issue_id,
        // A blocked job (embedded/standalone approval gate) OR a blocked
        // action_run (an open `pr` node holding the DAG, see CAIRN-1220) both
        // mean the issue awaits a human decision. One mechanism, counted together.
        "SELECT (SELECT COUNT(*) FROM jobs WHERE issue_id = ?1 AND status = 'blocked')
              + (SELECT COUNT(*) FROM action_runs WHERE issue_id = ?1 AND status = 'blocked')",
    )
    .await?
        > 0
    {
        IssueAttention::NeedsApproval
    } else {
        IssueAttention::None
    };

    Ok(Some((progress, attention)))
}

async fn issue_completed_at(conn: &turso::Connection, issue_id: &str) -> DbResult<Option<i64>> {
    crate::storage::query_opt_i64_conn(
        conn,
        "SELECT completed_at FROM issues WHERE id = ?1",
        (issue_id,),
    )
    .await
}

async fn count(conn: &turso::Connection, sql: &'static str, id: &str) -> DbResult<i64> {
    let mut rows = conn.query(sql, (id,)).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::internal("missing count row"))?;
    row.i64(0)
}

async fn issue_count(conn: &turso::Connection, issue_id: &str, sql: &'static str) -> DbResult<i64> {
    count(conn, sql, issue_id).await
}
