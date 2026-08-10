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
use cairn_db::turso::params;

pub(crate) async fn recompute_execution_status_conn(
    conn: &cairn_db::turso::Connection,
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

pub async fn recompute_job_owner_attention_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    issue_id: Option<&str>,
) -> DbResult<()> {
    let mut rows = conn
        .query("SELECT thread_id FROM jobs WHERE id = ?1", (job_id,))
        .await?;
    if let Some(thread_id) = rows
        .next()
        .await?
        .map(|row| row.opt_text(0))
        .transpose()?
        .flatten()
    {
        recompute_thread_attention_conn(conn, &thread_id).await
    } else if let Some(issue_id) = issue_id {
        recompute_issue_status_conn(conn, issue_id).await
    } else {
        Ok(())
    }
}

/// Project human attention for a first-class thread. Rest is silent; only a
/// pending question, permission request, or approval asks for attention.
pub async fn recompute_thread_attention_conn(
    conn: &cairn_db::turso::Connection,
    thread_id: &str,
) -> DbResult<()> {
    let attention = if count(
        conn,
        "SELECT COUNT(*) FROM prompts p JOIN runs r ON p.run_id = r.id JOIN jobs j ON r.job_id = j.id WHERE j.thread_id = ?1 AND p.response IS NULL",
        thread_id,
    )
    .await? > 0
    {
        IssueAttention::NeedsInput
    } else if count(
        conn,
        "SELECT COUNT(*) FROM permission_requests pr JOIN runs r ON pr.run_id = r.id JOIN jobs j ON r.job_id = j.id WHERE j.thread_id = ?1 AND pr.status = 'pending'",
        thread_id,
    )
    .await? > 0
    {
        IssueAttention::NeedsAuthorization
    } else if count(
        conn,
        "SELECT COUNT(*) FROM jobs WHERE thread_id = ?1 AND status = 'blocked'",
        thread_id,
    )
    .await? > 0
    {
        IssueAttention::NeedsApproval
    } else {
        IssueAttention::None
    };
    conn.execute(
        "UPDATE threads SET attention = ?1, updated_at = ?2 WHERE id = ?3",
        params![
            attention.to_string(),
            chrono::Utc::now().timestamp(),
            thread_id
        ],
    )
    .await?;
    Ok(())
}

pub async fn recompute_issue_status_conn(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<()> {
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
    conn: &cairn_db::turso::Connection,
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
        // A blocked job, a blocked action_run, or an open merge request all mean
        // the issue awaits a human decision. The merge_requests arm is
        // load-bearing: a coordinator can still need child wakes after its PR has
        // opened, even if the PR action_run is not the durable blocked row. Only
        // terminal PR states release the approval attention.
        "SELECT (SELECT COUNT(*) FROM jobs WHERE issue_id = ?1 AND status = 'blocked')
              + (SELECT COUNT(*) FROM action_runs WHERE issue_id = ?1 AND status = 'blocked')
              + (SELECT COUNT(*) FROM merge_requests WHERE issue_id = ?1 AND status NOT IN ('merged', 'closed'))",
    )
    .await?
        > 0
    {
        IssueAttention::NeedsApproval
    } else if issue_count(
            conn,
            issue_id,
            // A long-running agent node resting Idle keeps the issue in
            // `waiting` (awaiting a wake, not a human decision). Ranked below
            // the human-decision attentions so a real prompt/permission/approval
            // wins.
            //
            "SELECT COUNT(*) FROM jobs WHERE issue_id = ?1 AND status = 'idle'",
        )
        .await?
        > 0
    {
        IssueAttention::Idle
    } else {
        IssueAttention::None
    };

    Ok(Some((progress, attention)))
}

async fn issue_completed_at(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Option<i64>> {
    crate::storage::query_opt_i64_conn(
        conn,
        "SELECT completed_at FROM issues WHERE id = ?1",
        (issue_id,),
    )
    .await
}

async fn count(conn: &cairn_db::turso::Connection, sql: &'static str, id: &str) -> DbResult<i64> {
    let mut rows = conn.query(sql, (id,)).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::internal("missing count row"))?;
    row.i64(0)
}

async fn issue_count(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
    sql: &'static str,
) -> DbResult<i64> {
    count(conn, sql, issue_id).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("outcome-projection.db").await
    }

    async fn seed_completed_issue_with_pr(db: &LocalDb, pr_status: &str) {
        db.execute_script(&format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('i', 'p', 1, 'Issue', 'active', 'active', 'none', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e', 'recipe', 'i', 'p', 'complete', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
              VALUES('j', 'e', 'i', 'p', 'complete', 'builder', 'builder', 1, 1);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
              VALUES('mr', 'j', 'p', 'i', 'PR', 'feature', 'main', '{pr_status}', 1, 1);
            "
        ))
        .await
        .unwrap();
    }

    async fn issue_status_attention(db: &LocalDb) -> (String, String) {
        db.query_one(
            "SELECT status, attention FROM issues WHERE id = 'i'",
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn open_merge_request_keeps_completed_issue_waiting() {
        let db = migrated_db().await;
        seed_completed_issue_with_pr(&db, "open").await;

        db.write(|conn| Box::pin(async move { recompute_issue_status_conn(conn, "i").await }))
            .await
            .unwrap();

        assert_eq!(
            issue_status_attention(&db).await,
            ("waiting".to_string(), "needs_approval".to_string())
        );
    }

    /// Seed a project plus an issue carrying a running execution and one job.
    async fn seed_issue_with_job(db: &LocalDb, job_status: &str) {
        db.execute_script(&format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('i', 'p', 1, 'Issue', 'active', 'active', 'none', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e', 'recipe', 'i', 'p', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
              VALUES('j', 'e', 'i', 'p', '{job_status}', 'coordinator', 'coordinator', 1, 1);
            "
        ))
        .await
        .unwrap();
    }

    async fn recompute(db: &LocalDb) {
        db.write(|conn| Box::pin(async move { recompute_issue_status_conn(conn, "i").await }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn idle_job_holds_issue_waiting_with_idle_attention() {
        // A long-running coordinator resting Idle keeps its issue in `waiting`
        // with the dedicated `idle` attention — non-terminal, resumable.
        let db = migrated_db().await;
        seed_issue_with_job(&db, "idle").await;

        recompute(&db).await;

        assert_eq!(
            issue_status_attention(&db).await,
            ("waiting".to_string(), "idle".to_string())
        );
    }

    #[tokio::test]
    async fn a_thread_needing_a_human_decision_surfaces_like_any_issue() {
        // Only the resting presentation is gated. A blocked job is a pending
        // human decision, and it reaches the same attention on a thread as on an
        // issue — one channel, not a thread-specific one.
        {
            let db = migrated_db().await;
            seed_issue_with_job(&db, "blocked").await;

            recompute(&db).await;

            assert_eq!(
                issue_status_attention(&db).await,
                ("waiting".to_string(), "needs_approval".to_string()),
                "a blocked issue job must need approval"
            );
        }
    }

    #[tokio::test]
    async fn idle_thread_is_silent_and_questions_surface() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','PROJ','/tmp/p',1,1);
             INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) VALUES('th','p','topic','active','none',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,node_name,created_at,updated_at) VALUES('j','th','p','idle','thread','thread',1,1);
             INSERT INTO runs(id,job_id,project_id,status,created_at,updated_at) VALUES('r','j','p','live',1,1);"
        ).await.unwrap();
        db.write(|conn| Box::pin(async move { recompute_thread_attention_conn(conn, "th").await }))
            .await
            .unwrap();
        assert_eq!(
            db.query_opt_text("SELECT attention FROM threads WHERE id='th'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("none")
        );
        db.execute(
            "INSERT INTO prompts(id,run_id,questions,created_at) VALUES('q','r','[]',1)",
            (),
        )
        .await
        .unwrap();
        db.write(|conn| Box::pin(async move { recompute_thread_attention_conn(conn, "th").await }))
            .await
            .unwrap();
        assert_eq!(
            db.query_opt_text("SELECT attention FROM threads WHERE id='th'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("needs_input")
        );
    }

    #[tokio::test]
    async fn terminal_merge_request_releases_completed_issue() {
        for pr_status in ["merged", "closed"] {
            let db = migrated_db().await;
            seed_completed_issue_with_pr(&db, pr_status).await;

            db.write(|conn| Box::pin(async move { recompute_issue_status_conn(conn, "i").await }))
                .await
                .unwrap();

            assert_eq!(
                issue_status_attention(&db).await,
                ("complete".to_string(), "none".to_string()),
                "{pr_status} is terminal and must not hold approval attention"
            );
        }
    }
}
