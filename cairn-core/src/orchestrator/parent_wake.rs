use cairn_db::turso::params;

use crate::messages::{db as msg_db, queued::DeliveryUrgency};
use crate::models::ChannelType;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbResult, LocalDb, RowExt};

fn persist_system_direct(
    orch: &Orchestrator,
    recipient_run_id: &str,
    message: &str,
    urgency: DeliveryUrgency,
) -> Result<String, String> {
    let db = run_db_blocking({
        let dbs = orch.db.clone();
        let recipient_run_id = recipient_run_id.to_string();
        move || async move {
            crate::execution::routing::owning_db_for_run(&dbs, &recipient_run_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let msg = msg_db::insert_message_with_urgency(
        &db,
        &ChannelType::Direct,
        None,
        None,
        "system",
        Some(recipient_run_id),
        message,
        Some(urgency),
    )?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );
    Ok(msg.id)
}

pub(crate) fn load_parent_job(
    db: &LocalDb,
    child_issue_id: &str,
) -> Result<Option<String>, String> {
    let child_issue_id = child_issue_id.to_string();
    run_db_blocking(move || async move {
        db.read(|conn| {
            let child_issue_id = child_issue_id.clone();
            Box::pin(async move {
                let mut issue_rows = conn
                    .query(
                        "SELECT parent_job_id, parent_issue_id FROM issues WHERE id = ?1 LIMIT 1",
                        params![child_issue_id.as_str()],
                    )
                    .await?;
                let Some(issue_row) = issue_rows.next().await? else {
                    return Ok(None);
                };
                let spawning_job_id = issue_row.opt_text(0)?;
                let parent_issue_id = issue_row.opt_text(1)?;

                // Preferred: the exact job that spawned this child issue, as
                // long as it is still a resumable wake target.
                if let Some(job_id) = spawning_job_id {
                    if let Some(resolved) = resumable_job(conn, &job_id).await? {
                        return Ok(Some(resolved));
                    }
                }

                // Fallback for child issues with no recorded spawner (legacy
                // rows, human-linked sub-issues): the coordinator / recipe-root
                // job on the parent issue. `parent_job_id IS NULL` excludes
                // delegated sub-task jobs, which share the coordinator's
                // issue_id and would otherwise win the recency ordering.
                let Some(parent_issue_id) = parent_issue_id else {
                    return Ok(None);
                };
                let mut job_rows = conn
                    .query(
                        "
                        SELECT id
                        FROM jobs
                        WHERE issue_id = ?1
                          AND parent_job_id IS NULL
                          AND status != 'failed'
                          AND current_session_id IS NOT NULL
                        ORDER BY created_at DESC
                        LIMIT 1
                        ",
                        params![parent_issue_id.as_str()],
                    )
                    .await?;
                crate::storage::next_text(&mut job_rows, 0).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

/// Return `job_id` if it is a resumable wake target (exists, not failed, has a
/// session to resume); otherwise `None`, so the caller falls back to
/// issue-level resolution.
async fn resumable_job(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT id FROM jobs
             WHERE id = ?1 AND status != 'failed' AND current_session_id IS NOT NULL
             LIMIT 1",
            params![job_id],
        )
        .await?;
    crate::storage::next_text(&mut rows, 0).await
}

pub(crate) fn queue_or_resume_parent(
    orch: &Orchestrator,
    parent_job_id: &str,
    message: &str,
    urgency: DeliveryUrgency,
) {
    let owning = run_db_blocking({
        let dbs = orch.db.clone();
        let parent_job_id = parent_job_id.to_string();
        move || async move {
            crate::execution::routing::owning_db_for_job(&dbs, &parent_job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })
    .unwrap_or_else(|_| orch.db.local.clone());
    if let Some(recipient_run_id) =
        crate::messages::delivery::latest_run_for_job(&owning, parent_job_id)
    {
        let message_id = match persist_system_direct(orch, &recipient_run_id, message, urgency) {
            Ok(message_id) => {
                // Ride the attention push queue (CAIRN-1900): create a `direct:`
                // push so the nudge below has a drainable push and the raw
                // messages row is not orphaned by the retired delivered_at path.
                if let Err(error) = crate::messages::delivery::enqueue_direct_push(
                    orch,
                    parent_job_id,
                    &message_id,
                    urgency,
                ) {
                    log::warn!(
                        "failed to enqueue child attention direct push for parent job {}: {}",
                        &parent_job_id[..parent_job_id.len().min(8)],
                        error
                    );
                }
                Some(message_id)
            }
            Err(error) => {
                log::warn!(
                    "failed to persist child attention direct for parent job {}: {}",
                    &parent_job_id[..parent_job_id.len().min(8)],
                    error
                );
                None
            }
        };
        if let Err(error) =
            crate::messages::delivery::nudge_job_for_urgency(orch, parent_job_id, urgency)
        {
            log::warn!(
                "failed to nudge parent job {} for child attention: {}",
                &parent_job_id[..parent_job_id.len().min(8)],
                error
            );
        } else {
            log::info!(
                "queued child attention direct {:?} for parent job {} with {} urgency",
                message_id,
                &parent_job_id[..parent_job_id.len().min(8)],
                urgency.as_str()
            );
        }
        return;
    }

    log::warn!(
        "failed to persist child attention direct for parent job {}: no recipient run found; attempting legacy resume",
        &parent_job_id[..parent_job_id.len().min(8)]
    );
    match crate::execution::jobs::continue_job_impl(orch, parent_job_id, Some(message), None, None)
    {
        Ok(_) => log::info!(
            "resumed parent job {} for child attention",
            &parent_job_id[..parent_job_id.len().min(8)]
        ),
        Err(error) => log::warn!(
            "failed to resume parent job {} for child attention: {}",
            &parent_job_id[..parent_job_id.len().min(8)],
            error
        ),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::LocalDb;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("parent-wake.db").await
    }

    async fn seed_parent_child(db: &LocalDb, parent_status: &str) {
        let parent_status = parent_status.to_string();
        db.write(|conn| {
            let parent_status = parent_status.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
                     VALUES('parent', 'p', 1, 'Parent', 'backlog', 'backlog', 'none', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id)
                     VALUES('child', 'p', 2, 'Child', 'backlog', 'backlog', 'none', 2, 2, 'parent')",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
                     VALUES('parent-job', 'p', 'parent', ?1, 'session-parent', 3, 3)",
                    params![parent_status.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_parent_job_includes_completed_parent_jobs() {
        let db = migrated_db().await;
        seed_parent_child(&db, "complete").await;

        assert_eq!(
            load_parent_job(&db, "child").unwrap().as_deref(),
            Some("parent-job")
        );
    }

    #[tokio::test]
    async fn load_parent_job_ignores_failed_parent_jobs() {
        let db = migrated_db().await;
        seed_parent_child(&db, "failed").await;

        assert!(load_parent_job(&db, "child").unwrap().is_none());
    }

    /// Regression for CAIRN-1302: once the coordinator spawns a delegated
    /// sub-task, that sub-task job shares the coordinator's `issue_id` and is
    /// newer than the coordinator job. The wake target must remain the
    /// coordinator's own job (`parent_job_id IS NULL`), not the sub-task job.
    #[tokio::test]
    async fn load_parent_job_ignores_delegated_sub_task_jobs() {
        let db = migrated_db().await;
        seed_parent_child(&db, "complete").await;

        // Delegated sub-task job on the SAME issue as the coordinator, newer,
        // with its own session and `parent_job_id` pointing at the coordinator.
        db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, parent_job_id, created_at, updated_at)
             VALUES('subtask-job', 'p', 'parent', 'complete', 'session-subtask', 'parent-job', 9, 9)",
            (),
        )
        .await
        .unwrap();

        assert_eq!(
            load_parent_job(&db, "child").unwrap().as_deref(),
            Some("parent-job"),
        );
    }

    /// The recorded spawning job (`issues.parent_job_id`) is the wake target
    /// directly — even when a newer coordinator-shaped job also sits on the
    /// parent issue, which the recency-ordered fallback would otherwise pick.
    #[tokio::test]
    async fn load_parent_job_prefers_recorded_spawning_job() {
        let db = migrated_db().await;
        seed_parent_child(&db, "complete").await;
        db.execute_script(
            "
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
            VALUES('other-root', 'p', 'parent', 'running', 'session-other', 50, 50);
            UPDATE issues SET parent_job_id = 'parent-job' WHERE id = 'child';
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            load_parent_job(&db, "child").unwrap().as_deref(),
            Some("parent-job"),
        );
    }

    /// When the recorded spawner is no longer resumable (failed / no session),
    /// resolution falls back to the coordinator job on the parent issue.
    #[tokio::test]
    async fn load_parent_job_edge_falls_back_when_spawner_unresumable() {
        let db = migrated_db().await;
        seed_parent_child(&db, "complete").await;
        db.execute_script(
            "
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
            VALUES('dead-job', 'p', 'parent', 'failed', 'session-dead', 40, 40);
            UPDATE issues SET parent_job_id = 'dead-job' WHERE id = 'child';
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            load_parent_job(&db, "child").unwrap().as_deref(),
            Some("parent-job"),
        );
    }
}
