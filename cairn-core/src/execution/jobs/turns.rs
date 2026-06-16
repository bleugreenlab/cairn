use super::*;

#[derive(Clone)]
pub(super) struct TurnHead {
    id: String,
    state: TurnState,
}

pub(super) async fn next_turn_sequence_conn(
    conn: &turso::Connection,
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
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Option<TurnHead>> {
    let mut rows = conn
        .query(
            "SELECT id, state FROM turns WHERE job_id = ?1 ORDER BY sequence DESC LIMIT 1",
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
    conn: &turso::Connection,
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
    run_db(create_initial_turn_db(
        orch.db.local.clone(),
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

/// The start reason for a follow-up resume. A resume that runs while the job's
/// memory review is `sent` (the review prompt was delivered, resuming the agent
/// into its reflection turn) is the MemoryReview turn; everything else is a
/// normal FollowUp. Tagging it here is what lets the job-status projection treat
/// the review as post-completion housekeeping rather than the job's latest work.
async fn followup_start_reason_conn(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<TurnStartReason> {
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
    Ok(if state.as_deref() == Some("sent") {
        TurnStartReason::MemoryReview
    } else {
        TurnStartReason::FollowUp
    })
}

pub(super) async fn create_successor_turn_conn(
    conn: &turso::Connection,
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
) -> Result<String, String> {
    let (turn_id, created) = run_db({
        let db = orch.db.local.clone();
        let session_id = session_id.to_string();
        let job_id = job_id.to_string();
        async move {
            db.write(|conn| {
                let session_id = session_id.clone();
                let job_id = job_id.clone();
                Box::pin(async move {
                    let head_turn = get_head_turn_conn(conn, &job_id).await?;
                    let start_reason = followup_start_reason_conn(conn, &job_id).await?;
                    let new_turn_id = uuid::Uuid::new_v4().to_string();
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
                                let fallback_id = uuid::Uuid::new_v4().to_string();
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

pub(super) fn start_turn(orch: &Orchestrator, turn_id: &str, run_id: &str) -> Result<(), String> {
    let (session_id, job_id) = run_db({
        let db = orch.db.local.clone();
        let turn_id = turn_id.to_string();
        let run_id = run_id.to_string();
        async move {
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
        JobStatus::Pending | JobStatus::Complete | JobStatus::Failed | JobStatus::Blocked
    )
}

pub(super) fn transition_job_to_running(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let (execution_id, issue_id) = run_db({
        let db = orch.db.local.clone();
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
        let db = orch.db.local.clone();
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
