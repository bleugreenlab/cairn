use super::*;

#[derive(Clone)]
pub(super) struct TurnHead {
    id: String,
    state: TurnState,
}

pub(super) async fn next_turn_sequence_conn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
) -> DbResult<i32> {
    let mut rows = conn
        .query(
            "SELECT MAX(sequence) FROM turns WHERE session_id = ?1",
            (session_id,),
        )
        .await?;
    let max_seq = rows
        .next()
        .await?
        .map(|row| row.opt_i64(0))
        .transpose()?
        .flatten();
    Ok(max_seq.map(|seq| seq as i32).unwrap_or(0) + 1)
}

pub(super) async fn get_head_turn_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<TurnHead>> {
    let mut rows = conn
        .query(
            "SELECT t.id, t.state
             FROM jobs j
             LEFT JOIN turns current ON current.id = j.current_turn_id
             JOIN turns t ON t.id = COALESCE(
                 current.id,
                 (SELECT fallback.id
                    FROM turns fallback
                   WHERE fallback.job_id = j.id
                   ORDER BY fallback.created_at DESC, fallback.sequence DESC
                   LIMIT 1)
             )
             WHERE j.id = ?1
             LIMIT 1",
            (job_id,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            let state = row.text(1)?.parse().map_err(DbError::Row)?;
            Ok(TurnHead {
                id: row.text(0)?,
                state,
            })
        })
        .transpose()
}

pub(super) async fn create_turn_conn(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: Option<&str>,
    start_reason: TurnStartReason,
) -> DbResult<()> {
    let mut active_rows = conn
        .query(
            "SELECT COUNT(*) FROM turns
             WHERE job_id = ?1 AND (state = 'pending' OR state = 'running')",
            (job_id,),
        )
        .await?;
    let active_count = active_rows
        .next()
        .await?
        .map(|row| row.i64(0))
        .transpose()?
        .unwrap_or(0);
    if active_count > 0 {
        return Err(db_internal(format!(
            "Job {} already has an active turn (pending or running)",
            job_id
        )));
    }

    if let Some(pred_id) = predecessor_id {
        let mut successor_rows = conn
            .query(
                "SELECT COUNT(*) FROM turns WHERE predecessor_id = ?1",
                (pred_id,),
            )
            .await?;
        let successor_count = successor_rows
            .next()
            .await?
            .map(|row| row.i64(0))
            .transpose()?
            .unwrap_or(0);
        if successor_count > 0 {
            return Err(db_internal(format!(
                "Turn {} already has a successor",
                pred_id
            )));
        }
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let sequence = next_turn_sequence_conn(conn, session_id).await?;
    let state = TurnState::Pending.to_string();
    let reason = start_reason.to_string();
    conn.execute(
        "INSERT INTO turns(
            id, session_id, run_id, job_id, sequence, predecessor_id,
            state, yield_reason, start_reason, created_at, started_at, ended_at, updated_at
        )
        VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, NULL, ?7, ?8, NULL, NULL, ?8)",
        params![
            turn_id,
            session_id,
            Some(job_id),
            sequence,
            predecessor_id,
            state.as_str(),
            reason.as_str(),
            now,
        ],
    )
    .await?;
    conn.execute(
        "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
        params![Some(turn_id), job_id],
    )
    .await?;
    Ok(())
}

pub(super) async fn create_initial_turn_db(
    db: Arc<LocalDb>,
    turn_id: String,
    session_id: String,
    job_id: String,
) -> Result<(), String> {
    db.write(|conn| {
        let turn_id = turn_id.clone();
        let session_id = session_id.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            create_turn_conn(
                conn,
                &turn_id,
                &session_id,
                &job_id,
                None,
                TurnStartReason::Initial,
            )
            .await
        })
    })
    .await
    .map_err(|e| e.to_string())
}

pub(super) fn create_initial_turn(
    orch: &Orchestrator,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
) -> Result<(), String> {
    let db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    run_db(create_initial_turn_db(
        db,
        turn_id.to_string(),
        session_id.to_string(),
        job_id.to_string(),
    ))?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "turns",
            "action": "insert",
            "jobId": job_id,
            "sessionId": session_id,
        }),
    );
    Ok(())
}

/// The start reason for a follow-up resume. A resume is the post-completion
/// MemoryReview turn only when it is the system-initiated reflection delivery:
/// the job's memory review is `sent` AND the resume carries no user-authored
/// content. A user follow-up — an explicit resume message, a prompt/permission
/// answer (both fold into `user_initiated`), or a still-pending queued message
/// the resume sweep is about to claim — is always a normal FollowUp work turn,
/// even while a review is `sent`.
///
/// This distinction is load-bearing: the job-status projection excludes
/// `memory_review` turns from the job's terminal outcome (it reads the latest
/// *work* turn). Tagging a real work turn `memory_review` therefore hides the
/// completed work, so a confirm-gated node can never derive Complete even after
/// its artifact is confirmed — freezing DAG advancement with no error
/// (CAIRN-2014). Tagging the genuine reflection turn here is what lets the
/// projection treat the review as post-completion housekeeping rather than the
/// job's latest work.
async fn followup_start_reason_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    user_initiated: bool,
) -> DbResult<TurnStartReason> {
    // User steering is always a work turn, never a reflection.
    if user_initiated {
        return Ok(TurnStartReason::FollowUp);
    }
    let mut rows = conn
        .query(
            "SELECT memory_review_state FROM jobs WHERE id = ?1",
            (job_id,),
        )
        .await?;
    let state = rows
        .next()
        .await?
        .and_then(|row| row.opt_text(0).ok().flatten());
    if state.as_deref() != Some("sent") {
        return Ok(TurnStartReason::FollowUp);
    }
    // No explicit resume message, but a pending queued user follow-up (delivered
    // by the resume sweep that runs just after turn creation) still makes this a
    // work turn — the user is steering, not reflecting.
    let mut queued = conn
        .query(
            "SELECT 1 FROM queued_messages WHERE job_id = ?1 AND delivered_at IS NULL LIMIT 1",
            (job_id,),
        )
        .await?;
    if queued.next().await?.is_some() {
        return Ok(TurnStartReason::FollowUp);
    }
    Ok(TurnStartReason::MemoryReview)
}

pub(super) async fn create_successor_turn_conn(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: &str,
    start_reason: TurnStartReason,
) -> DbResult<()> {
    let mut rows = conn
        .query("SELECT state FROM turns WHERE id = ?1", (predecessor_id,))
        .await?;
    let pred_state = rows
        .next()
        .await?
        .map(|row| row.text(0))
        .transpose()?
        .ok_or_else(|| db_internal("Predecessor turn not found"))?;
    let pred_state: TurnState = pred_state
        .parse()
        .map_err(|e| db_internal(format!("Invalid predecessor state: {e}")))?;
    if !pred_state.is_terminal() {
        return Err(db_internal(format!(
            "Predecessor turn {} is in non-terminal state {:?}",
            predecessor_id, pred_state
        )));
    }

    create_turn_conn(
        conn,
        turn_id,
        session_id,
        job_id,
        Some(predecessor_id),
        start_reason,
    )
    .await
}

pub(super) fn create_followup_turn(
    orch: &Orchestrator,
    session_id: &str,
    job_id: &str,
    user_initiated: bool,
) -> Result<String, String> {
    let (turn_id, created) = run_db({
        let dbs = orch.db.clone();
        let session_id = session_id.to_string();
        let job_id = job_id.to_string();
        async move {
            let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())?;
            db.write(|conn| {
                let session_id = session_id.clone();
                let job_id = job_id.clone();
                Box::pin(async move {
                    let head_turn = get_head_turn_conn(conn, &job_id).await?;
                    let start_reason =
                        followup_start_reason_conn(conn, &job_id, user_initiated).await?;
                    let new_turn_id = ids::mint_child(&job_id);
                    if let Some(head) = head_turn {
                        if head.state == TurnState::Pending {
                            return Ok((head.id, false));
                        }
                        match create_successor_turn_conn(
                            conn,
                            &new_turn_id,
                            &session_id,
                            &job_id,
                            &head.id,
                            start_reason.clone(),
                        )
                        .await
                        {
                            Ok(_) => Ok((new_turn_id, true)),
                            Err(error) => {
                                log::warn!("Failed to create successor turn: {}", error);
                                let fallback_id = ids::mint_child(&job_id);
                                create_turn_conn(
                                    conn,
                                    &fallback_id,
                                    &session_id,
                                    &job_id,
                                    None,
                                    start_reason.clone(),
                                )
                                .await?;
                                Ok((fallback_id, true))
                            }
                        }
                    } else {
                        create_turn_conn(
                            conn,
                            &new_turn_id,
                            &session_id,
                            &job_id,
                            None,
                            start_reason,
                        )
                        .await?;
                        Ok((new_turn_id, true))
                    }
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    if created {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({
                "table": "turns",
                "action": "insert",
                "jobId": job_id,
                "sessionId": session_id,
            }),
        );
    }
    Ok(turn_id)
}

/// Flip a job's pending turn to `running` and wire it to `run_id`. Resolves the
/// run's owning database (fail-closed via `owning_db_for_run`) so a team job's
/// turn lands in its synced replica, then emits the scoped `turns` db-change.
///
/// This is the canonical turn-start the host job-start paths call. It replaced a
/// hand-rolled duplicate in the Tauri layer that wrote the `turns` UPDATE
/// unconditionally to the private DB, stranding a team job's turn (CAIRN-2206).
pub fn start_turn(orch: &Orchestrator, turn_id: &str, run_id: &str) -> Result<(), String> {
    let (session_id, job_id) = run_db({
        let dbs = orch.db.clone();
        let turn_id = turn_id.to_string();
        let run_id = run_id.to_string();
        async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            db.write(|conn| {
                let turn_id = turn_id.clone();
                let run_id = run_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT state, session_id, job_id FROM turns WHERE id = ?1",
                            (turn_id.as_str(),),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| db_internal("turn not found"))?;
                    let current = row.text(0)?;
                    let session_id = row.text(1)?;
                    let job_id = row.opt_text(2)?;
                    let from: TurnState = current.parse().map_err(|_| {
                        db_internal(format!("unparseable current state: {}", current))
                    })?;
                    if from != TurnState::Pending {
                        return Err(db_internal(format!(
                            "turn {} can only start from pending, got {}",
                            turn_id, from
                        )));
                    }
                    let now = chrono::Utc::now().timestamp() as i32;
                    conn.execute(
                        "UPDATE turns
                         SET state = 'running', run_id = ?1, started_at = ?2, updated_at = ?2
                         WHERE id = ?3",
                        params![run_id.as_str(), now, turn_id.as_str()],
                    )
                    .await?;
                    Ok((session_id, job_id))
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "turns",
            "action": "update",
            "jobId": job_id,
            "runId": run_id,
            "sessionId": session_id,
        }),
    );
    Ok(())
}

pub(super) fn valid_running_transition(from: &JobStatus) -> bool {
    matches!(
        from,
        JobStatus::Pending
            | JobStatus::Complete
            | JobStatus::Failed
            | JobStatus::Blocked
            | JobStatus::Idle
    )
}

pub(super) fn transition_job_to_running(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let (execution_id, issue_id) = run_db({
        let db = db.clone();
        let job_id = job_id.to_string();
        async move {
            db.write(|conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT status, execution_id, issue_id FROM jobs WHERE id = ?1",
                            (job_id.as_str(),),
                        )
                        .await?;
                    let Some(row) = rows.next().await? else {
                        return Err(db_internal(format!("job not found: {job_id}")));
                    };
                    let current_str = row.text(0)?;
                    let execution_id = row.opt_text(1)?;
                    let issue_id = row.opt_text(2)?;
                    let from: JobStatus = current_str.parse().map_err(|_| {
                        db_internal(format!("unparseable current status: {}", current_str))
                    })?;
                    if from == JobStatus::Running {
                        return Ok((execution_id, issue_id));
                    }
                    if !valid_running_transition(&from) {
                        return Err(db_internal(format!(
                            "transition not allowed for job {job_id}: {from} -> running"
                        )));
                    }
                    let now = chrono::Utc::now().timestamp() as i32;
                    if from == JobStatus::Pending {
                        conn.execute(
                            "UPDATE jobs
                             SET status = 'running', started_at = ?1, updated_at = ?1
                             WHERE id = ?2",
                            params![now, job_id.as_str()],
                        )
                        .await?;
                    } else {
                        conn.execute(
                            "UPDATE jobs SET status = 'running', updated_at = ?1 WHERE id = ?2",
                            params![now, job_id.as_str()],
                        )
                        .await?;
                    }

                    let effective_issue_id = if let Some(exec_id) = execution_id.as_deref() {
                        conn.execute(
                            "UPDATE executions SET status = 'running' WHERE id = ?1",
                            (exec_id,),
                        )
                        .await?;
                        issue_id
                            .clone()
                            .or(load_execution_issue_id_conn(conn, exec_id).await?)
                    } else {
                        issue_id.clone()
                    };
                    if let Some(issue_id) = effective_issue_id.as_deref() {
                        crate::transitions::outcome::recompute_issue_status_conn(conn, issue_id)
                            .await?;
                    }
                    Ok((execution_id, effective_issue_id))
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    // Reload the full job so the jobs db-change carries the complete scoping
    // set (issue/execution/parent/project ids), not just the partial query.
    let job = run_db({
        let db = db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::jobs::queries::get_job(&db, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let _ = orch
        .services
        .emitter
        .emit("db-change", crate::notify::job_db_change(&job, "update"));
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "executions",
            "action": "update",
            "issueId": issue_id.as_deref(),
            "executionId": execution_id.as_deref(),
        }),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "issues",
            "action": "update",
            "issueId": issue_id.as_deref(),
        }),
    );
    Ok(())
}

#[cfg(test)]
mod followup_start_reason_tests {
    use super::*;
    use crate::messages::queued::{enqueue_async, Delivery};
    use crate::storage::migrated_test_db;

    async fn insert_job(db: &LocalDb, job_id: &str, review_state: Option<&str>) {
        let job_id = job_id.to_string();
        let review_state = review_state.map(|s| s.to_string());
        db.write(|conn| {
            let job_id = job_id.clone();
            let review_state = review_state.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p','w','P','PRJ','/tmp/prj',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, project_id, status, memory_review_state, created_at, updated_at)
                     VALUES (?1, 'p', 'running', ?2, 1, 1)",
                    params![job_id.as_str(), review_state.as_deref()],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn start_reason(db: &LocalDb, job_id: &str, user_initiated: bool) -> TurnStartReason {
        let job_id = job_id.to_string();
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move { followup_start_reason_conn(conn, &job_id, user_initiated).await })
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn user_followup_while_review_sent_is_work_turn() {
        // CAIRN-2014: a user-initiated resume must be a FollowUp work turn even
        // while the memory review is `sent`. Tagging it `memory_review` would hide
        // the turn from the completion projection and strand a confirm-gated node.
        let db = migrated_test_db("turns-followup-user.db").await;
        insert_job(&db, "j-sent", Some("sent")).await;
        assert_eq!(
            start_reason(&db, "j-sent", true).await,
            TurnStartReason::FollowUp
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn system_resume_while_review_sent_is_memory_review() {
        // The genuine reflection delivery (no user content) is the MemoryReview turn.
        let db = migrated_test_db("turns-followup-system.db").await;
        insert_job(&db, "j-sent", Some("sent")).await;
        assert_eq!(
            start_reason(&db, "j-sent", false).await,
            TurnStartReason::MemoryReview
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_queued_message_makes_resume_a_work_turn() {
        // No explicit message, but a pending queued user follow-up (claimed by the
        // resume sweep) means the user is steering — a work turn, not a reflection.
        let db = migrated_test_db("turns-followup-queued.db").await;
        insert_job(&db, "j-sent", Some("sent")).await;
        enqueue_async(&db, "j-sent", "do more", Delivery::Queue)
            .await
            .unwrap();
        assert_eq!(
            start_reason(&db, "j-sent", false).await,
            TurnStartReason::FollowUp
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_review_pending_is_followup() {
        let db = migrated_test_db("turns-followup-none.db").await;
        insert_job(&db, "j-none", None).await;
        assert_eq!(
            start_reason(&db, "j-none", false).await,
            TurnStartReason::FollowUp
        );
    }
}
