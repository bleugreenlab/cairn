use crate::messages::{db as msg_db, queued::DeliveryUrgency};
use crate::models::ChannelType;
use crate::orchestrator::{attention_push, Orchestrator};
use crate::storage::{run_db_blocking, LocalDb};

fn persist_system_direct(
    orch: &Orchestrator,
    recipient_run_id: &str,
    sender_name: &str,
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
        sender_name,
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

/// The job a child issue's attention belongs to: its validated spawning node,
/// whoever currently drives the parent issue, or its parent thread's live
/// session. One rule, defined once in
/// [`crate::orchestrator::wakes::coordinating_job_for_child`], so the direct
/// parent-push path and the subscription path can never disagree about who owns
/// a child.
pub(crate) fn load_parent_job(
    db: &LocalDb,
    child_issue_id: &str,
) -> Result<Option<String>, String> {
    let child_issue_id = child_issue_id.to_string();
    run_db_blocking(move || async move {
        db.write(|conn| {
            let child_issue_id = child_issue_id.clone();
            Box::pin(async move {
                crate::orchestrator::wakes::coordinating_job_for_child(conn, &child_issue_id).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

/// Queue a durable operator signal on the canonical spawning parent/coordinator
/// without resuming it. Returns `true` only when a new distinct state was queued.
pub(crate) async fn queue_passive_parent_push(
    db: &LocalDb,
    child_issue_id: &str,
    content_ref: &str,
    key: &str,
    fingerprint: &str,
) -> Result<bool, String> {
    let Some(parent_job_id) = load_parent_job(db, child_issue_id)? else {
        return Ok(false);
    };
    if crate::threads::is_dormant_thread_session(db, &parent_job_id).await {
        return Ok(false);
    }
    let latest = attention_push::latest_push_fingerprint(db, &parent_job_id, key)
        .await
        .map_err(|error| error.to_string())?;
    if latest.as_ref().and_then(|value| value.as_deref()) == Some(fingerprint) {
        return Ok(false);
    }
    attention_push::push_with_fingerprint(
        db,
        &parent_job_id,
        content_ref,
        attention_push::Wake::Passive,
        attention_push::Boundary::Event,
        key,
        Some(fingerprint),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(true)
}

pub(crate) fn queue_or_resume_parent(
    orch: &Orchestrator,
    parent_job_id: &str,
    sender_name: Option<&str>,
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
    // The direct parent-push path takes the same eligibility rule the
    // subscription path does. Checked before anything is persisted, so a closed
    // thread accrues no direct message and no push it would have to be dug out of
    // on reopen.
    if crate::threads::is_dormant_thread_session_sync(&owning, parent_job_id) {
        log::debug!(
            "skipped attention for job {}: its thread is closed",
            &parent_job_id[..parent_job_id.len().min(8)]
        );
        return;
    }
    if let Some(recipient_run_id) =
        crate::messages::delivery::latest_run_for_job(&owning, parent_job_id)
    {
        let message_id = match persist_system_direct(
            orch,
            &recipient_run_id,
            sender_name.unwrap_or("system"),
            message,
            urgency,
        ) {
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
    use crate::storage::{LocalDb, RowExt};
    use cairn_db::turso::params;

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
                     VALUES('p', 'w', 'Project', 'proj', '/tmp/repo', 1, 1)",
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
    async fn passive_parent_push_is_fingerprinted_and_never_rousing() {
        let db = migrated_db().await;
        seed_parent_child(&db, "complete").await;

        assert!(queue_passive_parent_push(
            &db,
            "child",
            "cairn://p/proj/2/1/builder/checks",
            "turn-checks-infrastructure:child",
            "state-a",
        )
        .await
        .unwrap());
        assert!(!queue_passive_parent_push(
            &db,
            "child",
            "cairn://p/proj/2/1/builder/checks",
            "turn-checks-infrastructure:child",
            "state-a",
        )
        .await
        .unwrap());
        assert!(queue_passive_parent_push(
            &db,
            "child",
            "cairn://p/proj/2/1/builder/checks",
            "turn-checks-infrastructure:child",
            "state-b",
        )
        .await
        .unwrap());

        let row = db
            .query_one(
                "SELECT wake, fingerprint, COUNT(*) OVER () FROM attention_pushes
                 WHERE recipient = 'parent-job' AND key = 'turn-checks-infrastructure:child'
                 ORDER BY created_at DESC LIMIT 1",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(row, ("passive".to_string(), "state-b".to_string(), 1));
    }

    /// The direct parent-push path takes the same recipient rule the
    /// subscription path does. Nothing is persisted for a closed thread, so
    /// reopening does not surface a backlog of pushes queued while it was
    /// dormant.
    #[tokio::test]
    async fn a_closed_parent_thread_takes_no_passive_push_and_reopening_restores_it() {
        let db = migrated_db().await;
        seed_parent_child(&db, "complete").await;
        db.execute_script(
            "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES('t','p','general','closed','none',1,1);
             INSERT INTO jobs(id, thread_id, project_id, status, node_name, uri_segment,
                              current_session_id, created_at, updated_at)
               VALUES('thread-job','t','p','idle','thread','thread','session-thread',3,3);
             UPDATE issues SET parent_issue_id = NULL, parent_thread_id = 't' WHERE id = 'child';",
        )
        .await
        .unwrap();

        let push = || {
            queue_passive_parent_push(
                &db,
                "child",
                "cairn://p/proj/2/1/builder/checks",
                "turn-checks-infrastructure:child",
                "state-a",
            )
        };
        assert!(!push().await.unwrap(), "a closed thread takes no push");
        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM attention_pushes", (), |row| row
                .i64(0))
                .await
                .unwrap(),
            0,
            "and nothing is persisted for it to find on reopen"
        );

        db.execute("UPDATE threads SET status='active' WHERE id='t'", ())
            .await
            .unwrap();
        assert!(push().await.unwrap(), "reopening restores the push path");
        assert_eq!(
            db.query_one(
                "SELECT recipient FROM attention_pushes LIMIT 1",
                (),
                |row| row.text(0)
            )
            .await
            .unwrap(),
            "thread-job"
        );
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

    /// A spawner that is still perfectly resumable but whose execution has been
    /// superseded is no longer the wake target: the node driving the parent's
    /// current execution is. Resumability alone — all this path used to check —
    /// keeps a retired coordinator receiving children it no longer owns.
    #[tokio::test]
    async fn load_parent_job_drops_a_spawner_whose_execution_was_superseded() {
        let db = migrated_db().await;
        seed_parent_child(&db, "idle").await;
        db.execute_script(
            "
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e1', 'r', 'parent', 'p', 'complete', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e2', 'r', 'parent', 'p', 'running', 10, 2);
            UPDATE jobs SET execution_id = 'e1' WHERE id = 'parent-job';
            INSERT INTO jobs(id, project_id, issue_id, execution_id, status, current_session_id, started_at, created_at, updated_at)
              VALUES('parent-job-2', 'p', 'parent', 'e2', 'running', 'session-2', 11, 10, 10);
            UPDATE issues SET parent_job_id = 'parent-job' WHERE id = 'child';
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            load_parent_job(&db, "child").unwrap().as_deref(),
            Some("parent-job-2"),
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
