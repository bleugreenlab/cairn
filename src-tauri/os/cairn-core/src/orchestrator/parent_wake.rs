use cairn_attention::runtime::{DeliveryOutcome, DeliveryRequest};

use crate::orchestrator::{attention_push, Orchestrator};
use crate::storage::{run_db_blocking, LocalDb};

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

/// Persist one direct delivery and run its recipient: the attention engine's
/// one way to hand a job a message.
///
/// One transaction in the recipient's owning database writes the message row
/// under the request's message id and its `direct:` push, each only if absent,
/// and settles the receipts the request carries when they live in that same
/// database. The nudge runs only after the commit. Receipts that live in the
/// private database while the recipient's job lives in a team database are
/// settled right after; they are already bound to this message id, so a retry
/// re-settles exactly this message and nothing is written twice.
///
/// A closed thread accrues nothing: dormancy is checked before anything is
/// persisted, so reopening does not surface a backlog. A job that has never
/// run takes the direct-message launch path, launched with the message as its
/// prompt; a launch that cannot happen yet fails, and the caller keeps the
/// receipts owed for its next attempt.
pub(crate) async fn deliver_direct(
    orch: &Orchestrator,
    request: DeliveryRequest<'_>,
) -> DeliveryOutcome {
    let job_id = request.job_id;
    let short = &job_id[..job_id.len().min(8)];
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    if crate::threads::is_dormant_thread_session(&owning, job_id).await {
        log::debug!("skipped attention for job {short}: its thread is closed");
        return DeliveryOutcome::Refused("its thread is closed".to_string());
    }
    let run = match crate::messages::delivery::latest_run_for_job_async(&owning, job_id).await {
        Ok(run) => run,
        Err(error) => return DeliveryOutcome::Failed(error),
    };
    let receipts = request.receipts.to_vec();
    let Some(run_id) = run else {
        if request.urgency == crate::messages::queued::DeliveryUrgency::Passive {
            return DeliveryOutcome::Refused(
                "it has never run, so no next run exists for a passive line to ride along with"
                    .to_string(),
            );
        }
        let resume =
            request
                .operator_message_id
                .map(|trigger_id| crate::execution::jobs::ResumeContext {
                    goal_reactivation: Some(
                        crate::execution::jobs::GoalReactivation::operator_message(trigger_id),
                    ),
                    ..Default::default()
                });
        return match crate::execution::jobs::continue_job_impl(
            orch,
            job_id,
            Some(request.message),
            None,
            resume,
        ) {
            Ok(_) => {
                log::info!("launched job {short} for an attention delivery");
                if !receipts.is_empty() {
                    if let Err(error) = cairn_attention::wakes::settle_receipts(
                        &orch.db.local,
                        &receipts,
                        job_id,
                        None,
                        None,
                    )
                    .await
                    {
                        log::warn!("settling receipts for launched job {short}: {error}");
                    }
                }
                DeliveryOutcome::Launched {
                    job_id: job_id.to_string(),
                }
            }
            Err(error) => DeliveryOutcome::Failed(format!("could not launch job {short}: {error}")),
        };
    };
    let message_id = request
        .message_id
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let settle_here = !receipts.is_empty() && std::sync::Arc::ptr_eq(&owning, &orch.db.local);
    let written = owning
        .write(|conn| {
            let (job, run, message_id, receipts) = (
                job_id.to_string(),
                run_id.clone(),
                message_id.clone(),
                receipts.clone(),
            );
            let sender = request.sender.unwrap_or("system").to_string();
            let content = request.message.to_string();
            let operator = request.operator_message_id.map(str::to_owned);
            let urgency = request.urgency;
            Box::pin(async move {
                let (push_id, _) = cairn_attention::direct::settle_direct_conn(
                    conn,
                    &job,
                    &run,
                    &sender,
                    &content,
                    urgency,
                    &message_id,
                    operator.as_deref(),
                )
                .await?;
                if settle_here {
                    cairn_attention::wakes::settle_receipts_conn(
                        conn,
                        &receipts,
                        &job,
                        Some(&message_id),
                        Some(&push_id),
                    )
                    .await?;
                }
                Ok(push_id)
            })
        })
        .await;
    let push_id = match written {
        Ok(push_id) => push_id,
        Err(error) => {
            return DeliveryOutcome::Failed(format!(
                "could not persist the direct for job {short}: {error}"
            ))
        }
    };
    if !settle_here && !receipts.is_empty() {
        if let Err(error) = cairn_attention::wakes::settle_receipts(
            &orch.db.local,
            &receipts,
            job_id,
            Some(&message_id),
            Some(&push_id),
        )
        .await
        {
            log::warn!("settling receipts of direct {message_id} for job {short}: {error}");
        }
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );
    orch.notifier.emit_change("attention_pushes");
    match crate::messages::delivery::nudge_job_for_urgency(orch, job_id, request.urgency) {
        Ok(_) => log::info!(
            "queued attention direct {message_id} for job {short} with {} urgency",
            request.urgency.as_str()
        ),
        Err(error) => log::warn!("failed to nudge job {short} for attention: {error}"),
    }
    DeliveryOutcome::Persisted {
        message_id,
        job_id: job_id.to_string(),
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
            -- CAIRN-4665: an issue has at most ONE canonical execution, enforced
            -- by a partial unique index. The retired attempt says so explicitly
            -- rather than being inferred from its lower `seq`, which is exactly
            -- the generalization this fixture's contract now rests on.
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
              VALUES('e1', 'r', 'parent', 'p', 'complete', 1, 1, 'superseded');
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
              VALUES('e2', 'r', 'parent', 'p', 'running', 10, 2, 'canonical');
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
