use crate::models::{DelegatedStatus, ExecutionSnapshot};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use uuid::Uuid;

use super::common::{
    block_on, block_on_value, latest_run_for_job_arc, refresh_packet_state, select_optional_text,
    ParentRunContext,
};
use super::results::{latest_nonempty_artifact_content_arc, latest_nonempty_assistant_content_arc};

const DEFERRED_TASK_PARENT_SUSPEND_GRACE: std::time::Duration =
    std::time::Duration::from_millis(75);

pub(super) fn prepare_parent_for_delegated_wait(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    packet: &crate::models::DelegatedWorkPacket,
) -> Result<(), String> {
    if let Some(pred_turn_id) = packet.parent_turn_id.as_deref() {
        block_on(prepare_parent_for_delegated_wait_db(
            orch.db.local.clone(),
            pred_turn_id.to_string(),
            parent_ctx.job_id.clone(),
        ))?;
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "update"}),
        );
    }

    Ok(())
}

async fn prepare_parent_for_delegated_wait_db(
    db: std::sync::Arc<LocalDb>,
    pred_turn_id: String,
    parent_job_id: String,
) -> Result<(), String> {
    db.write(|conn| {
        let pred_turn_id = pred_turn_id.clone();
        let parent_job_id = parent_job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT state FROM turns WHERE id = ?1 LIMIT 1",
                    (pred_turn_id.as_str(),),
                )
                .await?;
            let turn_state = crate::storage::next_text(&mut rows, 0).await?;

            if matches!(turn_state.as_deref(), Some("running")) {
                let now = chrono::Utc::now().timestamp() as i32;
                conn.execute(
                    "
                    UPDATE turns
                    SET state = 'yielded',
                        yield_reason = 'dependency_wait',
                        ended_at = ?1,
                        updated_at = ?1
                    WHERE id = ?2
                    ",
                    (now, pred_turn_id.as_str()),
                )
                .await?;
            }

            let mut session_rows = conn
                .query(
                    "SELECT current_session_id FROM jobs WHERE id = ?1 LIMIT 1",
                    (parent_job_id.as_str(),),
                )
                .await?;
            let session_id = session_rows
                .next()
                .await?
                .map(|row| row.opt_text(0))
                .transpose()?
                .flatten();
            let Some(session_id) = session_id else {
                return Ok(());
            };

            let existing_successor = query_successor_turn(conn, &pred_turn_id).await?;
            if existing_successor.is_some() {
                return Ok(());
            }

            let sequence = next_turn_sequence(conn, &session_id).await?;
            let now = chrono::Utc::now().timestamp() as i32;
            let turn_id = Uuid::new_v4().to_string();
            conn.execute(
                "
                INSERT INTO turns (
                    id, session_id, run_id, job_id, sequence,
                    predecessor_id, state, yield_reason, start_reason,
                    created_at, started_at, ended_at, updated_at
                )
                VALUES (?1, ?2, NULL, ?3, ?4, ?5, 'pending', NULL,
                        'dependency_unblock', ?6, NULL, NULL, ?6)
                ",
                (
                    turn_id.as_str(),
                    session_id.as_str(),
                    parent_job_id.as_str(),
                    sequence,
                    pred_turn_id.as_str(),
                    now,
                ),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
                (turn_id.as_str(), parent_job_id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn next_turn_sequence(conn: &turso::Connection, session_id: &str) -> DbResult<i64> {
    let mut rows = conn
        .query(
            "SELECT MAX(sequence) FROM turns WHERE session_id = ?1",
            (session_id,),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| crate::storage::DbError::Row("missing turn sequence".to_string()))?;
    Ok(row.opt_i64(0)?.unwrap_or(0) + 1)
}

async fn query_successor_turn(
    conn: &turso::Connection,
    predecessor_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "
            SELECT id
            FROM turns
            WHERE predecessor_id = ?1
            ORDER BY sequence ASC
            LIMIT 1
            ",
            (predecessor_id,),
        )
        .await?;
    crate::storage::next_text(&mut rows, 0).await
}

async fn get_successor_turn(db: &LocalDb, predecessor_id: &str) -> Result<Option<String>, String> {
    let predecessor_id = predecessor_id.to_string();
    db.read(|conn| Box::pin(async move { query_successor_turn(conn, &predecessor_id).await }))
        .await
        .map_err(|e| format!("Failed to query successor turn: {}", e))
}

fn finish_deferred_parent_suspend_for_delegated_wait(
    orch: &Orchestrator,
    parent_run_id: &str,
    parent_job_id: &str,
    child_job_id: &str,
) -> Result<(), String> {
    crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
        orch,
        parent_run_id,
        "delegated_wait_suspended",
    )?;
    if let Err(error) = resume_suspended_parent_after_task_completion(orch, child_job_id) {
        log::warn!(
            "Post-suspend resume check failed for parent job {} via child {}: {}",
            parent_job_id,
            child_job_id,
            error
        );
    }
    Ok(())
}

pub(super) fn schedule_deferred_parent_suspend_for_delegated_wait(
    orch: Orchestrator,
    parent_run_id: String,
    parent_job_id: String,
    child_job_id: String,
) {
    tokio::spawn(async move {
        tokio::time::sleep(DEFERRED_TASK_PARENT_SUSPEND_GRACE).await;

        let worker_orch = orch.clone();
        let worker_parent_run_id = parent_run_id.clone();
        let worker_parent_job_id = parent_job_id.clone();
        let worker_child_job_id = child_job_id.clone();
        match tokio::task::spawn_blocking(move || {
            finish_deferred_parent_suspend_for_delegated_wait(
                &worker_orch,
                &worker_parent_run_id,
                &worker_parent_job_id,
                &worker_child_job_id,
            )
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log::warn!(
                    "Deferred suspend worker failed for parent job {} run {}: {}",
                    parent_job_id,
                    parent_run_id,
                    error
                );
            }
            Err(join_error) => {
                log::warn!(
                    "Deferred suspend worker panicked for parent job {} run {}: {}",
                    parent_job_id,
                    parent_run_id,
                    join_error
                );
            }
        }
    });
}

fn delegated_result_text(
    db: std::sync::Arc<LocalDb>,
    packet: &crate::models::DelegatedWorkPacket,
) -> Result<String, String> {
    let Some(job_id) = packet.result_artifact_job_id.as_deref() else {
        return Ok(format!(
            "Delegated task '{}' has no materialized result.",
            packet.title
        ));
    };

    if matches!(
        packet.status,
        DelegatedStatus::Failed | DelegatedStatus::Cancelled
    ) {
        return Ok(format!("Delegated task '{}' failed.", packet.title));
    }

    if let Some(content) = block_on_value(latest_nonempty_artifact_content_arc(
        db.clone(),
        job_id.to_string(),
        None,
    ))? {
        return Ok(content);
    }

    let child_run_id = block_on(latest_run_for_job_arc(db.clone(), job_id.to_string()))?;
    if let Some(child_run_id) = child_run_id.as_deref() {
        if let Some(content) = block_on_value(latest_nonempty_assistant_content_arc(
            db.clone(),
            child_run_id.to_string(),
        ))? {
            return Ok(content);
        }
    }

    Ok("Task completed.".to_string())
}

fn format_delegated_resume_message(
    packets: &[(crate::models::DelegatedWorkPacket, String)],
) -> String {
    if packets.len() == 1 {
        let (packet, result) = &packets[0];
        return format!("Delegated task '{}' result:\n\n{}", packet.title, result);
    }

    packets
        .iter()
        .enumerate()
        .map(|(i, (packet, result))| format!("## Task {}: {}\n\n{}", i + 1, packet.title, result))
        .collect::<Vec<_>>()
        .join("\n\n---\n\n")
}

fn format_delegated_tool_result(
    packets: &[(crate::models::DelegatedWorkPacket, String)],
) -> String {
    if packets.len() == 1 {
        return packets[0].1.clone();
    }
    format_delegated_resume_message(packets)
}

#[derive(Debug)]
struct TaskToolResultDelivery {
    run_id: String,
    session_id: String,
    tool_use_id: String,
    content: String,
    is_error: bool,
}

#[derive(Debug)]
struct TaskResumeDelivery {
    parent_job_id: String,
    parent_turn_id: String,
    message: String,
    tool_results: Vec<TaskToolResultDelivery>,
}

fn task_tool_result_deliveries(
    run_id: &str,
    session_id: &str,
    rendered_packets: &[(crate::models::DelegatedWorkPacket, String)],
) -> Option<Vec<TaskToolResultDelivery>> {
    let mut groups: Vec<(String, Vec<(crate::models::DelegatedWorkPacket, String)>)> = Vec::new();
    for (packet, result) in rendered_packets.iter().cloned() {
        let tool_use_id = packet.parent_tool_use_id.clone()?;
        if let Some((_, group)) = groups.iter_mut().find(|(id, _)| id == &tool_use_id) {
            group.push((packet, result));
        } else {
            groups.push((tool_use_id, vec![(packet, result)]));
        }
    }

    Some(
        groups
            .into_iter()
            .map(|(tool_use_id, group)| {
                let is_error = group.iter().any(|(packet, _)| {
                    matches!(
                        packet.status,
                        DelegatedStatus::Failed | DelegatedStatus::Cancelled
                    )
                });
                TaskToolResultDelivery {
                    run_id: run_id.to_string(),
                    session_id: session_id.to_string(),
                    tool_use_id,
                    content: format_delegated_tool_result(&group),
                    is_error,
                }
            })
            .collect(),
    )
}

/// The delegated packets that resume together with `trigger`: the same parent
/// job anchored on the same `parent_turn_id`. Concurrent task spawns coalesce
/// onto one wait (see `resolve_delegated_wait_anchor`), so every packet sharing
/// that anchor resumes as a unit — whether they came from one `write` call or
/// several back-to-back ones. The parent resume gate fires only once every
/// packet in this group is terminal, and they deliver their results together.
/// Falls back to `trigger` alone when it carries no anchor turn.
fn delegated_sibling_ids(
    packets: &[crate::models::DelegatedWorkPacket],
    trigger: &crate::models::DelegatedWorkPacket,
) -> Vec<String> {
    packets
        .iter()
        .filter(|candidate| {
            candidate.parent_job_id == trigger.parent_job_id
                && match trigger.parent_turn_id.as_deref() {
                    Some(anchor) => candidate.parent_turn_id.as_deref() == Some(anchor),
                    None => candidate.id == trigger.id,
                }
        })
        .map(|candidate| candidate.id.clone())
        .collect()
}

/// Atomically claim the pending resume-successor turn so concurrent sibling
/// finalizations resume the parent exactly once. Exactly one caller wins the
/// conditional flip to a terminal state; the rest observe 0 affected rows and
/// bail. Marking it terminal lets `create_followup_turn` open a fresh successor
/// for the resumed turn (the normal head-terminal continue path). The
/// `state = 'pending'` predicate is the durable guard — it survives the winner
/// resetting other pointers.
async fn claim_pending_successor(
    db: &LocalDb,
    parent_job_id: &str,
    successor_id: &str,
) -> Result<bool, String> {
    let parent_job_id = parent_job_id.to_string();
    let successor_id = successor_id.to_string();
    db.write(|conn| {
        let parent_job_id = parent_job_id.clone();
        let successor_id = successor_id.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp() as i32;
            let affected = conn
                .execute(
                    "UPDATE turns SET state = 'complete', ended_at = ?1, updated_at = ?1
                     WHERE id = ?2 AND state = 'pending'
                       AND ?2 = (SELECT current_turn_id FROM jobs WHERE id = ?3)",
                    (now, successor_id.as_str(), parent_job_id.as_str()),
                )
                .await?;
            Ok(affected == 1)
        })
    })
    .await
    .map_err(|e| format!("Failed to claim resume successor: {}", e))
}

pub fn resume_suspended_parent_after_task_completion(
    orch: &Orchestrator,
    child_job_id: &str,
) -> Result<(), String> {
    let db = orch.db.local.clone();
    let child_job_id = child_job_id.to_string();
    let resume = block_on({
        let db = db.clone();
        async move {
            let execution_id = select_optional_text(
                &db,
                "SELECT execution_id FROM jobs WHERE id = ?1",
                &child_job_id,
            )
            .await
            .map_err(|e| format!("Failed to load child execution: {}", e))?;
            let Some(execution_id) = execution_id else {
                return Ok(None);
            };

            let snapshot_json = select_optional_text(
                &db,
                "SELECT snapshot FROM executions WHERE id = ?1",
                &execution_id,
            )
            .await
            .map_err(|e| format!("Failed to load execution snapshot: {}", e))?;
            let Some(snapshot_json) = snapshot_json else {
                return Ok(None);
            };
            let snapshot: ExecutionSnapshot = serde_json::from_str(&snapshot_json)
                .map_err(|e| format!("Failed to parse execution snapshot: {}", e))?;
            let Some(packet) = snapshot
                .delegated_packets
                .iter()
                .find(|packet| {
                    packet.result_artifact_job_id.as_deref() == Some(child_job_id.as_str())
                })
                .cloned()
            else {
                return Ok(None);
            };

            let refreshed_packet = refresh_packet_state(&db, &execution_id, &packet.id)
                .await?
                .ok_or_else(|| format!("Delegated packet disappeared: {}", packet.id))?;
            if !matches!(
                refreshed_packet.status,
                DelegatedStatus::Completed | DelegatedStatus::Failed | DelegatedStatus::Cancelled
            ) {
                return Ok(None);
            }

            let sibling_ids = delegated_sibling_ids(&snapshot.delegated_packets, &refreshed_packet);

            let mut grouped_packets = Vec::with_capacity(sibling_ids.len());
            for packet_id in sibling_ids {
                let packet = refresh_packet_state(&db, &execution_id, &packet_id)
                    .await?
                    .ok_or_else(|| format!("Delegated packet disappeared: {}", packet_id))?;
                if !matches!(
                    packet.status,
                    DelegatedStatus::Completed
                        | DelegatedStatus::Failed
                        | DelegatedStatus::Cancelled
                ) {
                    log::debug!(
                        "Resume gate: parent job {} still waiting on sibling task {} (status {:?})",
                        refreshed_packet.parent_job_id,
                        packet.id,
                        packet.status
                    );
                    return Ok(None);
                }
                grouped_packets.push(packet);
            }

            let Some(parent_turn_id) = refreshed_packet.parent_turn_id.as_deref() else {
                log::warn!(
                    "Resume gate: delegated packet {} (parent job {}) has no anchor turn; \
                     parent cannot be resumed",
                    refreshed_packet.id,
                    refreshed_packet.parent_job_id
                );
                return Ok(None);
            };
            let Some(successor_id) = get_successor_turn(&db, parent_turn_id).await? else {
                log::warn!(
                    "Resume gate: parent job {} anchor turn {} has no pending successor; \
                     parent cannot be resumed",
                    refreshed_packet.parent_job_id,
                    parent_turn_id
                );
                return Ok(None);
            };
            // Atomically claim the pending successor. Concurrent sibling
            // finalizations all reach here once their batch is terminal, but
            // exactly one wins the conditional flip and resumes the parent; the
            // rest see 0 rows and bail.
            if !claim_pending_successor(&db, &refreshed_packet.parent_job_id, &successor_id).await?
            {
                return Ok(None);
            }

            grouped_packets.sort_by_key(|packet| (packet.task_index, packet.created_at));
            let mut rendered_packets = Vec::with_capacity(grouped_packets.len());
            for packet in grouped_packets {
                let result = delegated_result_text(db.clone(), &packet)?;
                rendered_packets.push((packet, result));
            }

            let full_combined = format_delegated_resume_message(&rendered_packets);
            let session_id = select_optional_text(
                &db,
                "SELECT current_session_id FROM jobs WHERE id = ?1",
                &refreshed_packet.parent_job_id,
            )
            .await
            .map_err(|e| format!("Failed to load parent session: {}", e))?;
            let origin_run_id = select_optional_text(
                &db,
                "SELECT run_id FROM turns WHERE id = ?1",
                parent_turn_id,
            )
            .await
            .map_err(|e| format!("Failed to load anchor turn run: {}", e))?;
            let origin_run_id = if origin_run_id.is_some() {
                origin_run_id
            } else if let Some(session_id) = session_id.as_deref() {
                select_optional_text(
                    &db,
                    "SELECT id FROM runs WHERE session_id = ?1 ORDER BY created_at DESC LIMIT 1",
                    session_id,
                )
                .await
                .map_err(|e| format!("Failed to load latest session run: {}", e))?
            } else {
                None
            };

            let tool_results = match (origin_run_id.as_deref(), session_id.as_deref()) {
                (Some(run_id), Some(session_id)) => {
                    task_tool_result_deliveries(run_id, session_id, &rendered_packets)
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            };

            Ok(Some(TaskResumeDelivery {
                parent_job_id: refreshed_packet.parent_job_id,
                parent_turn_id: parent_turn_id.to_string(),
                message: full_combined,
                tool_results,
            }))
        }
    })?;
    let Some(resume) = resume else {
        return Ok(());
    };
    let mut suppress_user_event = !resume.tool_results.is_empty();
    if suppress_user_event {
        let now = chrono::Utc::now().timestamp() as i32;
        for tool_result in &resume.tool_results {
            if let Err(e) = crate::execution::jobs::store_tool_result_event_with_turn(
                orch,
                &tool_result.run_id,
                &tool_result.session_id,
                &tool_result.tool_use_id,
                &tool_result.content,
                tool_result.is_error,
                now,
                Some(&resume.parent_turn_id),
            ) {
                log::warn!("Failed to store synthetic task tool_result: {}", e);
                suppress_user_event = false;
                break;
            }
        }
    }
    let resume_context = suppress_user_event.then_some(crate::execution::jobs::ResumeContext {
        suppress_user_event: true,
    });
    crate::execution::jobs::continue_job_impl(
        orch,
        &resume.parent_job_id,
        Some(&resume.message),
        None,
        resume_context,
    )
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(
        id: &str,
        parent_job: &str,
        anchor: Option<&str>,
    ) -> crate::models::DelegatedWorkPacket {
        packet_with_tool(id, parent_job, anchor, None)
    }

    fn packet_with_tool(
        id: &str,
        parent_job: &str,
        anchor: Option<&str>,
        parent_tool_use_id: Option<&str>,
    ) -> crate::models::DelegatedWorkPacket {
        let mut value = serde_json::json!({
            "id": id,
            "parentJobId": parent_job,
            "parentTurnId": anchor,
            "origin": "task_tool",
            "title": "Explore",
            "problemStatement": "x",
            "agentConfigId": "Explore",
            "ownership": { "cwd": "/tmp" },
            "outputContract": { "schemaType": "return" },
            "status": "completed",
            "createdAt": 0
        });
        if let Some(parent_tool_use_id) = parent_tool_use_id {
            value["parentToolUseId"] = serde_json::json!(parent_tool_use_id);
        }
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn task_tool_result_deliveries_group_by_originating_change() {
        let rendered = vec![
            (
                packet_with_tool("a", "builder", Some("T1"), Some("tool_a")),
                "one".to_string(),
            ),
            (
                packet_with_tool("b", "builder", Some("T1"), Some("tool_b")),
                "two".to_string(),
            ),
            (
                packet_with_tool("c", "builder", Some("T1"), Some("tool_a")),
                "three".to_string(),
            ),
        ];

        let deliveries = task_tool_result_deliveries("run", "sess", &rendered).unwrap();

        assert_eq!(deliveries.len(), 2);
        assert_eq!(deliveries[0].tool_use_id, "tool_a");
        assert!(deliveries[0].content.contains("## Task 1: Explore\n\none"));
        assert!(deliveries[0]
            .content
            .contains("## Task 2: Explore\n\nthree"));
        assert_eq!(deliveries[1].tool_use_id, "tool_b");
        assert_eq!(deliveries[1].content, "two");
    }

    #[test]
    fn single_task_tool_result_matches_fast_path_raw_result() {
        let rendered = vec![(
            packet_with_tool("a", "builder", Some("T1"), Some("tool_a")),
            "raw artifact".to_string(),
        )];

        let deliveries = task_tool_result_deliveries("run", "sess", &rendered).unwrap();

        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].content, "raw artifact");
    }

    #[test]
    fn task_tool_result_deliveries_falls_back_when_any_id_missing() {
        let rendered = vec![(packet("a", "builder", Some("T1")), "one".to_string())];
        assert!(task_tool_result_deliveries("run", "sess", &rendered).is_none());
    }

    #[test]
    fn siblings_share_parent_and_anchor_turn() {
        // Two back-to-back change(task) calls coalesce onto one anchor turn
        // (T1), so both resume as a unit even though they were separate calls.
        let trigger = packet("a", "builder", Some("T1"));
        let packets = vec![
            packet("a", "builder", Some("T1")),
            packet("b", "builder", Some("T1")), // coalesced sibling
            packet("c", "builder", Some("T2")), // later, separate wait under same parent
            packet("d", "other", Some("T1")),   // different parent
        ];
        let mut ids = delegated_sibling_ids(&packets, &trigger);
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn unanchored_trigger_resumes_alone() {
        let trigger = packet("solo", "builder", None);
        let packets = vec![
            packet("solo", "builder", None),
            packet("other", "builder", None), // no anchor turn — not a sibling of solo
        ];
        assert_eq!(
            delegated_sibling_ids(&packets, &trigger),
            vec!["solo".to_string()]
        );
    }

    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    /// Migrated DB with a project, a pending placeholder turn `succ`, and a job
    /// whose `current_turn_id` points at it.
    async fn migrated_db_with_pending_successor() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join("agents-claim-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                     VALUES ('proj', 'default', 'P', 'PROJ', '/tmp/p', 0, 0)",
                    (),
                )
                .await?;
                // Turn first: jobs.current_turn_id has an FK to turns(id).
                conn.execute(
                    "INSERT INTO turns(id, session_id, sequence, state, created_at, updated_at) \
                     VALUES ('succ', 'sess', 1, 'pending', 0, 0)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, status, project_id, created_at, updated_at, current_turn_id) \
                     VALUES ('job', 'pending', 'proj', 0, 0, 'succ')",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    async fn turn_state(db: &LocalDb, id: &str) -> String {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT state FROM turns WHERE id = ?1", (id.as_str(),))
                    .await?;
                rows.next().await?.unwrap().text(0)
            })
        })
        .await
        .unwrap()
    }

    /// Exactly one claim wins (flips the placeholder terminal); the rest lose.
    #[tokio::test]
    async fn claim_pending_successor_is_won_once() {
        let db = migrated_db_with_pending_successor().await;
        assert!(claim_pending_successor(&db, "job", "succ").await.unwrap());
        assert_eq!(turn_state(&db, "succ").await, "complete");
        // Second claim observes a non-pending turn and bails.
        assert!(!claim_pending_successor(&db, "job", "succ").await.unwrap());
    }

    /// A pending turn that is not the job's current_turn_id is not claimable.
    #[tokio::test]
    async fn claim_requires_current_turn_pointer() {
        let db = migrated_db_with_pending_successor().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO turns(id, session_id, sequence, state, created_at, updated_at) \
                     VALUES ('other', 'sess', 2, 'pending', 0, 0)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert!(!claim_pending_successor(&db, "job", "other").await.unwrap());
        assert_eq!(turn_state(&db, "other").await, "pending");
    }
}
