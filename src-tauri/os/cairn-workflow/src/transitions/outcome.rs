//! Execution and issue status recomputes.
//!
//! Execution status is recomputed from its jobs and action runs; issue status
//! (progress + attention) is recomputed from its executions, prompts, permission
//! requests, blocked jobs, and merge requests. Job status — the level beneath —
//! is itself a projection now (see [`crate::transitions::projection`] and
//! `execution::advancement::recompute`); these two functions are the upper
//! projections that slot above it.

use crate::models::{IssueAttention, IssueProgress, IssueStatus};
use crate::storage::{DbError, DbResult, RowExt};
use cairn_db::turso::params;

/// What one [`recompute_issue_status_conn`] call actually did.
///
/// The projection itself is level-based: it derives an issue's status from the
/// facts standing right now, and running it twice on unchanged facts must reach
/// the same answer. Callers, though, need the *edge*. The resolved-attention
/// emit announces that an issue **became** terminal, which is a transition and
/// not a level — announcing the level re-announces every resolution that has
/// ever happened, every time some unrelated sweep touches the issue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatusWrite {
    /// A projected column (`status`, `progress`, or `attention`) actually moved.
    pub changed: bool,
    /// The issue crossed from non-terminal to terminal on THIS call.
    pub became_terminal: bool,
}

pub async fn recompute_execution_status_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
) -> DbResult<()> {
    // FIRST, before any job count: an execution filed with `after:` gates and
    // not yet released is HELD, whatever its (empty) job set says.
    //
    // This has to come first because a held execution has ZERO jobs. Reaching
    // the counts below with an empty job set falls straight through to the
    // terminal arms and derives `complete` — an execution that reports finished
    // successfully before it has started, on an issue the issue projection then
    // calls done. Ordering is the whole guard.
    //
    // `held` belongs here precisely BECAUSE status is a projection: it derives
    // from "has gates AND not released" exactly as `running` derives from "has
    // non-terminal jobs". Note that satisfying every gate is not release —
    // release is the guarded transition on `released_at`, which is also what
    // mints the jobs — so an execution stays held until that transition lands.
    //
    // Deliberately NOT narrowed to `cairn_db::awaiting_release_sql!`. A gated
    // execution that can never start (superseded, or its issue resolved) still
    // has zero jobs, and every other arm below would call that never-started
    // work `complete` or `failed` — and drag the issue roll-up with it. `held`
    // is the only value here that claims nothing ran. Whether it is WAITING is
    // the gate census's to say, through the shared predicate, and it renders
    // such an execution as `won't start`, never as held.
    let held_gates = count(
        conn,
        "
        SELECT COUNT(*)
        FROM execution_gates
        WHERE execution_id = ?1
          AND (SELECT released_at FROM executions WHERE id = ?1) IS NULL
        ",
        execution_id,
    )
    .await?;

    if held_gates > 0 {
        conn.execute(
            "UPDATE executions SET status = 'held' WHERE id = ?1",
            (execution_id,),
        )
        .await?;
        return Ok(());
    }

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

    // A condition still routed nowhere is work waiting on someone, exactly like
    // a blocked action run, so it keeps the execution running — but only while
    // the execution is live. A superseded execution, or one whose issue was
    // resolved, must still reach a terminal status; supersession and resolution
    // recompute after they land so it does.
    let blocked_conditions = count(
        conn,
        concat!(
            "SELECT COUNT(*)
             FROM condition_evaluations ce
             JOIN executions e ON e.id = ce.execution_id
             WHERE ce.execution_id = ?1 AND ",
            cairn_db::live_execution_sql!(),
            " AND ",
            cairn_db::still_blocked_condition_sql!()
        ),
        execution_id,
    )
    .await?;

    if blocked_conditions > 0 {
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
        recompute_issue_status_conn(conn, issue_id).await?;
        Ok(())
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
        "SELECT COUNT(*) FROM prompts p JOIN runs r ON p.run_id = r.id JOIN jobs j ON r.job_id = j.id WHERE j.thread_id = ?1 AND p.response IS NULL AND p.withdrawn_at IS NULL",
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

/// Project an issue's `status`/`progress`/`attention` from its current facts.
///
/// The write is idempotent: when all three projected columns already hold the
/// derived values, nothing is written and `updated_at` does not move. That is
/// load-bearing rather than an optimization. `issues.updated_at` is read as "when
/// did this issue last actually change" — by recency ordering, by the resource
/// hashes, and (until CAIRN-4756) by the resolved-push fingerprint. A recompute
/// that stamps `now` onto a long-settled issue makes that column report when the
/// projection last *ran*, which after a startup sweep is boot time for every
/// issue the sweep touched.
pub async fn recompute_issue_status_conn(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<StatusWrite> {
    let Some((progress, attention)) = issue_progress_attention(conn, issue_id).await? else {
        return Ok(StatusWrite::default());
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

    let progress_text = progress.to_string();
    let attention_text = attention.to_string();
    let stored = stored_projection(conn, issue_id).await?;
    let changed = match &stored {
        Some((stored_status, stored_progress, stored_attention)) => {
            stored_status != status
                || stored_progress != &progress_text
                || stored_attention != &attention_text
        }
        None => true,
    };
    let was_terminal = stored
        .as_ref()
        .and_then(|(stored_status, _, _)| stored_status.parse::<IssueStatus>().ok())
        .is_some_and(|stored_status| stored_status.is_terminal());
    let is_terminal = status
        .parse::<IssueStatus>()
        .is_ok_and(|projected| projected.is_terminal());

    let now = chrono::Utc::now().timestamp();
    // The `completed_at` backfill is a repair, not a projection write: it fires
    // whenever a terminated issue is still missing its completion instant, even
    // when the three projected columns are unchanged. It is self-limiting —
    // once stamped, the branch never fires for that issue again.
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
                progress_text.as_str(),
                attention_text.as_str(),
                now,
                now,
                issue_id
            ],
        )
        .await?;
    } else if changed {
        conn.execute(
            "
            UPDATE issues
            SET status = ?1, progress = ?2, attention = ?3, updated_at = ?4
            WHERE id = ?5
            ",
            params![
                status,
                progress_text.as_str(),
                attention_text.as_str(),
                now,
                issue_id
            ],
        )
        .await?;
    }
    Ok(StatusWrite {
        changed,
        became_terminal: is_terminal && !was_terminal,
    })
}

/// The three projected columns as they stand before this recompute writes.
///
/// Read as raw text rather than parsed: the comparison this feeds asks whether
/// the write would be a no-op, and that is a question about the stored bytes. A
/// value the model cannot parse must read as "different" so the projection
/// repairs it.
async fn stored_projection(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Option<(String, String, String)>> {
    let mut rows = conn
        .query(
            "SELECT status, progress, attention FROM issues WHERE id = ?1",
            (issue_id,),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    match (row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?) {
        (Some(status), Some(progress), Some(attention)) => Ok(Some((status, progress, attention))),
        _ => Ok(None),
    }
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
        } else if statuses.iter().all(|status| status == "held") {
            // Held executions are filed but un-started: no jobs, no admission
            // request, no slot. An issue whose executions are all held is
            // exactly as un-started as an issue carrying no execution at all,
            // so it projects the same way — never Active, and never the
            // Complete the terminal fall-through would otherwise hand it.
            // A held execution filed beside a real one does not reach here, so
            // it cannot drag a finished issue backwards.
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
        WHERE r.issue_id = ?1 AND p.response IS NULL AND p.withdrawn_at IS NULL
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
        // A blocked job, a blocked action_run, a blocked condition, or an open
        // merge request all mean the issue awaits a human decision. The
        // merge_requests arm is load-bearing: a coordinator can still need child
        // wakes after its PR has opened, even if the PR action_run is not the
        // durable blocked row. Only terminal PR states release the approval
        // attention.
        //
        // A condition has no job or action_run row, so it is counted from
        // `condition_evaluations`: a condition still routed nowhere, in an
        // execution that is still live. A condition left behind by a superseded
        // execution waits on no one.
        concat!(
            "SELECT (SELECT COUNT(*) FROM jobs WHERE issue_id = ?1 AND status = 'blocked')
              + (SELECT COUNT(*) FROM action_runs WHERE issue_id = ?1 AND status = 'blocked')
              + (SELECT COUNT(*)
                 FROM condition_evaluations ce
                 JOIN executions e ON e.id = ce.execution_id
                 WHERE e.issue_id = ?1 AND ",
            cairn_db::live_execution_sql!(),
            " AND ",
            cairn_db::still_blocked_condition_sql!(),
            ")
              + (SELECT COUNT(*) FROM merge_requests WHERE issue_id = ?1 AND status NOT IN ('merged', 'closed'))"
        ),
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
              VALUES('p', 'w', 'Project', 'proj', '/tmp/repo', 1, 1);
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
              VALUES('p', 'w', 'Project', 'proj', '/tmp/repo', 1, 1);
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

    async fn recompute(db: &LocalDb) -> StatusWrite {
        db.write(|conn| Box::pin(async move { recompute_issue_status_conn(conn, "i").await }))
            .await
            .unwrap()
    }

    async fn issue_updated_at(db: &LocalDb) -> i64 {
        db.query_one("SELECT updated_at FROM issues WHERE id = 'i'", (), |row| {
            row.i64(0)
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_no_op_recompute_leaves_updated_at_alone() {
        // `issues.updated_at` answers "when did this issue last change". A
        // recompute deriving exactly what is already stored has learned nothing
        // and must not stamp the column with the time it happened to run --
        // startup recovery sweeps every stale job's execution, which used to
        // rewrite `updated_at` across every issue it touched and, through the
        // fingerprint keyed on it, replay long-settled resolutions
        // (CAIRN-4756).
        let db = migrated_db().await;
        seed_issue_with_job(&db, "idle").await;

        let first = recompute(&db).await;
        assert!(
            first.changed,
            "the first recompute moves attention none -> idle"
        );

        // A sentinel rather than a re-read: both recomputes land in the same
        // second, so comparing timestamps alone would pass even unfixed.
        db.execute("UPDATE issues SET updated_at = 4242 WHERE id = 'i'", ())
            .await
            .unwrap();

        let second = recompute(&db).await;

        assert!(
            !second.changed,
            "a recompute over unchanged facts must report no change"
        );
        assert!(!second.became_terminal);
        assert_eq!(
            issue_updated_at(&db).await,
            4242,
            "a no-op recompute must not write, so updated_at must not move"
        );
    }

    #[tokio::test]
    async fn only_the_crossing_recompute_reports_the_terminal_edge() {
        // Terminality is a level anyone can read afterwards; the TRANSITION is
        // visible only to the call that made it. This is the distinction the
        // resolved-attention emit is keyed on.
        let db = migrated_db().await;
        seed_issue_with_job(&db, "idle").await;

        assert!(
            !recompute(&db).await.became_terminal,
            "a waiting issue crosses no terminal edge"
        );

        // What `issues::crud::merge` stamps; the projection follows it.
        db.execute("UPDATE issues SET merged_at = 100 WHERE id = 'i'", ())
            .await
            .unwrap();

        let crossing = recompute(&db).await;
        assert!(crossing.changed);
        assert!(
            crossing.became_terminal,
            "the recompute that projects merged owns the edge"
        );
        assert_eq!(issue_status_attention(&db).await.0, "merged");

        assert!(
            !recompute(&db).await.became_terminal,
            "an issue that was already merged crosses nothing"
        );
    }

    #[tokio::test]
    async fn idle_job_holds_issue_waiting_with_idle_attention() {
        // A long-running coordinator resting Idle keeps its issue in `waiting`
        // with the dedicated `idle` attention — non-terminal, resumable.
        let db = migrated_db().await;
        seed_issue_with_job(&db, "idle").await;

        let _ = recompute(&db).await;

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

            let _ = recompute(&db).await;

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
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','proj','/tmp/p',1,1);
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

    /// Seed an issue carrying one execution with ZERO jobs, optionally gated
    /// and optionally released. `status` starts at a deliberately wrong value so
    /// every assertion below is on what the projection derived, not on what the
    /// seed wrote.
    async fn seed_gated_execution(db: &LocalDb, gates: &[(&str, Option<i64>)], released: bool) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
              VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p', 'w', 'Project', 'proj', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('i', 'p', 1, 'Issue', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e', 'recipe', 'i', 'p', 'running', 1, 1);
            ",
        )
        .await
        .unwrap();
        for (seq, (id, satisfied_at)) in gates.iter().enumerate() {
            db.execute(
                "INSERT INTO execution_gates(id, execution_id, seq, predicate_json, satisfied_at, created_at)
                 VALUES(?1, 'e', ?2, '{\"fact\":\"issue_resolution\"}', ?3, 1)",
                params![*id, seq as i64, *satisfied_at],
            )
            .await
            .unwrap();
        }
        if released {
            db.execute("UPDATE executions SET released_at = 100 WHERE id = 'e'", ())
                .await
                .unwrap();
        }
    }

    async fn derived_execution_status(db: &LocalDb) -> String {
        db.query_one("SELECT status FROM executions WHERE id = 'e'", (), |row| {
            row.text(0)
        })
        .await
        .unwrap()
    }

    async fn recompute_execution_and_issue(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                recompute_execution_status_conn(conn, "e").await?;
                recompute_issue_status_conn(conn, "i").await
            })
        })
        .await
        .unwrap();
    }

    /// The projection trap. A held execution has zero jobs, and the job counts
    /// run first, so before the gate branch existed this execution fell straight
    /// through to the terminal arms and derived as `complete` — with
    /// `completed_at` stamped — and the issue projection then called the issue
    /// done. A gate that reports finished work that never started is worse than
    /// no gate at all, so the gate facts are read BEFORE the job counts.
    #[tokio::test]
    async fn a_gated_execution_with_no_jobs_is_held_not_complete() {
        let db = migrated_db().await;
        seed_gated_execution(&db, &[("g1", None)], false).await;

        recompute_execution_and_issue(&db).await;

        assert_eq!(
            derived_execution_status(&db).await,
            "held",
            "an execution with an unsatisfied gate and no jobs must derive as held"
        );
        assert_eq!(
            db.query_opt_i64("SELECT completed_at FROM executions WHERE id = 'e'", ())
                .await
                .unwrap(),
            None,
            "a held execution has not completed and must carry no completion stamp"
        );
        assert_eq!(
            issue_status_attention(&db).await,
            ("backlog".to_string(), "none".to_string()),
            "an issue whose only execution is held is un-started work: backlog, \
             never active and never complete"
        );
    }

    /// A gated execution fenced before release never ran, so it must not derive
    /// a terminal status: beside an unstarted successor, `complete` would make
    /// the whole issue read as done. It stays `held` in the projection; the
    /// gate census is what renders it as won't-start rather than waiting.
    #[tokio::test]
    async fn a_superseded_gated_execution_never_derives_terminal() {
        let db = migrated_db().await;
        seed_gated_execution(&db, &[("g1", None)], false).await;
        db.execute_script(
            "UPDATE executions SET lifecycle = 'superseded' WHERE id = 'e';
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
               VALUES('e2', 'recipe', 'i', 'p', 'held', 2, 2, 'pending');
             INSERT INTO execution_gates(id, execution_id, seq, predicate_json, created_at)
               VALUES('g2', 'e2', 0, '{\"fact\":\"issue_resolution\"}', 2);",
        )
        .await
        .unwrap();

        recompute_execution_and_issue(&db).await;

        assert_eq!(derived_execution_status(&db).await, "held");
        assert_eq!(
            db.query_opt_i64("SELECT completed_at FROM executions WHERE id = 'e'", ())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            issue_status_attention(&db).await.0,
            "backlog",
            "nothing on this issue has started"
        );
    }

    /// One satisfied gate out of two is still held: `after:` is an AND.
    #[tokio::test]
    async fn a_partly_satisfied_gate_set_stays_held() {
        let db = migrated_db().await;
        seed_gated_execution(&db, &[("g1", Some(50)), ("g2", None)], false).await;

        recompute_execution_and_issue(&db).await;

        assert_eq!(derived_execution_status(&db).await, "held");
    }

    /// Release is the whole of the transition. `released_at` is set, so the same
    /// row — gates and all, satisfied or not — derives from its jobs exactly as
    /// any other execution does. Held is not sticky.
    #[tokio::test]
    async fn a_released_execution_derives_from_its_jobs_again() {
        let db = migrated_db().await;
        seed_gated_execution(&db, &[("g1", Some(50))], true).await;

        recompute_execution_and_issue(&db).await;

        assert_eq!(
            derived_execution_status(&db).await,
            "complete",
            "a released execution is an ordinary execution"
        );
    }

    /// The new branch is a no-op for every execution that predates gates: no
    /// gate rows, so the job counts decide exactly as before. This is the whole
    /// backfill argument in executable form.
    #[tokio::test]
    async fn an_ungated_execution_is_untouched_by_the_held_branch() {
        let db = migrated_db().await;
        seed_gated_execution(&db, &[], false).await;

        recompute_execution_and_issue(&db).await;

        assert_eq!(derived_execution_status(&db).await, "complete");
        assert_eq!(issue_status_attention(&db).await.0, "complete");
    }

    /// A held execution filed onto an issue that already finished one does not
    /// drag that issue backwards. Only an issue whose executions are ALL held
    /// reads as backlog.
    #[tokio::test]
    async fn a_held_execution_beside_a_completed_one_leaves_the_issue_complete() {
        let db = migrated_db().await;
        seed_gated_execution(&db, &[("g1", None)], false).await;
        db.execute_script(
            // Superseded, because the held execution 'e' is the issue's current
            // attempt — 0230's partial unique index allows exactly one
            // canonical execution per issue.
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
               VALUES('e0', 'recipe', 'i', 'p', 'complete', 1, 0, 'superseded');",
        )
        .await
        .unwrap();

        recompute_execution_and_issue(&db).await;

        assert_eq!(derived_execution_status(&db).await, "held");
        assert_eq!(issue_status_attention(&db).await.0, "complete");
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
