use crate::agent_process::process::{GoalPause, GoalPauseOwner, PauseIntent};
use crate::execution::jobs::GoalReactivation;
use crate::models::{DelegatedStatus, ExecutionSnapshot};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::ids;

use super::common::{
    block_on, block_on_value, latest_run_for_job_arc, refresh_packet_state, select_optional_text,
    ParentRunContext,
};
use super::results::{
    latest_nonempty_artifact_content_arc, latest_nonempty_assistant_content_arc, task_artifact_uri,
};
use crate::orchestrator::attention_push::{
    latest_push_fingerprint, push_with_fingerprint, Boundary, Wake,
};

pub(super) fn prepare_parent_for_delegated_wait(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
    packet: &crate::models::DelegatedWorkPacket,
) -> Result<(), String> {
    // Native goal continuation is evaluated as soon as Codex observes an idle
    // provider turn. Pause before publishing the yielded predecessor so provider
    // bookkeeping cannot race the canonical completion launcher. There is no
    // successor yet: the final child completion mints and claims it atomically,
    // before delivering the child result and releasing this turn's ownership.
    {
        let pred_turn_id = packet.parent_turn_id.as_deref().ok_or_else(|| {
            "Cannot park delegated wait without a durable parent turn".to_string()
        })?;
        crate::backends::stdin::pause_goal_for_durable_wait(
            &orch.process_state,
            &parent_ctx.run_id,
            PauseIntent::Stake(GoalPause::Owned(GoalPauseOwner::DependencyWait {
                parked_turn_id: pred_turn_id.to_string(),
            })),
        )?;
        let db = {
            let dbs = orch.db.clone();
            let job_id = parent_ctx.job_id.clone();
            block_on(async move {
                crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                    .await
                    .map_err(|e| e.to_string())
            })?
        };
        block_on(prepare_parent_for_delegated_wait_db(
            db.clone(),
            pred_turn_id.to_string(),
            parent_ctx.job_id.clone(),
        ))?;
        let change = block_on({
            let db = db.clone();
            let turn_id = pred_turn_id.to_string();
            async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await) }
        })?;
        let _ = orch.services.emitter.emit("db-change", change);
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
            let Some(_session_id) = session_id else {
                return Ok(());
            };

            // Keep the yielded predecessor as the job head for the entire wait.
            // Publishing a pending dependency_unblock here exposes an unowned
            // non-terminal turn while children are still running.
            conn.execute(
                "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
                (pred_turn_id.as_str(), parent_job_id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn next_turn_sequence(conn: &cairn_db::turso::Connection, session_id: &str) -> DbResult<i64> {
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
    conn: &cairn_db::turso::Connection,
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

/// Park the parent on the shared durable-wait primitive, then re-check whether
/// the child finished while the park was pending.
///
/// The park is deferred so the `tasks` call's own "suspended" result reaches the
/// agent ahead of the interrupt; the re-check is what keeps a child that
/// completed inside that window from leaving the parent parked forever.
pub(super) fn schedule_deferred_parent_suspend_for_delegated_wait(
    orch: Orchestrator,
    parent_run_id: String,
    parent_job_id: String,
    child_job_id: String,
    parked_turn_id: String,
) {
    let after_park_orch = orch.clone();
    crate::orchestrator::lifecycle::suspend_run_for_durable_wait_after_handoff_then(
        &orch,
        &parent_run_id,
        "delegated_wait_suspended",
        GoalPauseOwner::DependencyWait { parked_turn_id },
        move |parked| async move {
            if parked.is_err() {
                return;
            }
            let worker_orch = after_park_orch;
            let worker_parent_job_id = parent_job_id.clone();
            let worker_child_job_id = child_job_id.clone();
            let checked = tokio::task::spawn_blocking(move || {
                resume_suspended_parent_after_task_completion(&worker_orch, &worker_child_job_id)
            })
            .await;
            match checked {
                Ok(Ok(())) => {}
                Ok(Err(error)) => log::warn!(
                    "Post-suspend resume check failed for parent job {worker_parent_job_id} via child {child_job_id}: {error}"
                ),
                Err(join_error) => log::warn!(
                    "Post-suspend resume check panicked for parent job {worker_parent_job_id}: {join_error}"
                ),
            }
        },
    );
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

/// How a settled task reaches its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResumeMode {
    /// The task's own turn just ended: the ordinary completion path.
    Live,
    /// A boot repair settled a task whose completion a defective release never
    /// delivered. Only a parent still parked on the exact anchor is woken, and
    /// each result says plainly that it is late.
    Repair,
}

/// When a task's work ended: its latest work turn's end, the memory review
/// excluded. `None` when the task never ran a turn.
async fn task_finished_at(db: &LocalDb, job_id: &str) -> Result<Option<i64>, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT ended_at FROM turns
                     WHERE job_id = ?1 AND start_reason != 'memory_review'
                     ORDER BY created_at DESC, sequence DESC LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => row.opt_i64(0),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| format!("Failed to load task completion time: {e}"))
}

/// The notice a repaired delivery leads with, so the parent reads a long-held
/// result as history rather than as fresh news.
fn delayed_delivery_notice(finished_at: Option<i64>) -> String {
    let when = finished_at
        .and_then(|secs| chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0))
        .map(|at| format!("at {}", at.format("%Y-%m-%d %H:%M UTC")))
        .unwrap_or_else(|| "long before this delivery".to_string());
    format!(
        "Delayed delivery: this task finished {when}. A defect (fixed in cairn/5058) kept \
         this thread from being woken when it finished; read this result as history, not \
         as fresh news."
    )
}

/// Each packet's result text as its parent receives it. A repaired delivery
/// prefixes every result with its delayed-delivery notice, which reaches both
/// the combined resume message and the per-call Task `tool_result`s built from
/// these same strings.
async fn render_delegated_results(
    db: &std::sync::Arc<LocalDb>,
    packets: Vec<crate::models::DelegatedWorkPacket>,
    mode: ResumeMode,
) -> Result<Vec<(crate::models::DelegatedWorkPacket, String)>, String> {
    let mut rendered = Vec::with_capacity(packets.len());
    for packet in packets {
        let result = delegated_result_text(db.clone(), &packet)?;
        let result = match (mode, packet.result_artifact_job_id.as_deref()) {
            (ResumeMode::Repair, Some(job_id)) => format!(
                "{}\n\n{}",
                delayed_delivery_notice(task_finished_at(db, job_id).await?),
                result
            ),
            _ => result,
        };
        rendered.push((packet, result));
    }
    Ok(rendered)
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

/// What a settled delegated task should trigger on its spawning parent.
#[derive(Debug)]
enum TaskCompletionOutcome {
    /// Blocking spawn: deliver combined results after atomically claiming the
    /// suspended parent's dependency-unblock boundary.
    Resume(TaskResumeDelivery),
    /// Background spawn: a `tasks:` attention push was already written for the
    /// spawning node inside the closure. `wake_idle` says whether the effective
    /// wake level should nudge an idle spawner now (a passive in-flight push
    /// never nudges; the completion wake does).
    BackgroundNotify { recipient: String, wake_idle: bool },
}

/// Background-spawn completion handler: notify the spawning node that one of its
/// fire-and-forget tasks settled, passively while the batch is still in flight
/// and with a single `wake` when the last sibling settles. This is the
/// background sibling of the blocking successor-resume path; it never touches a
/// successor turn or `continue_job_impl`.
async fn notify_spawner_after_background_task_completion(
    db: &LocalDb,
    execution_id: &str,
    trigger: &crate::models::DelegatedWorkPacket,
    sibling_ids: &[String],
) -> Result<Option<TaskCompletionOutcome>, String> {
    let total = sibling_ids.len();
    let mut terminal_count = 0usize;
    for packet_id in sibling_ids {
        let packet = refresh_packet_state(db, execution_id, packet_id)
            .await?
            .ok_or_else(|| format!("Delegated packet disappeared: {}", packet_id))?;
        if matches!(
            packet.status,
            DelegatedStatus::Completed | DelegatedStatus::Failed | DelegatedStatus::Cancelled
        ) {
            terminal_count += 1;
        }
    }

    let recipient = trigger.parent_job_id.clone();
    // The anchor turn groups the batch (the same anchor `delegated_sibling_ids`
    // keys on); fall back to the packet id for an unanchored single task so the
    // supersession key is still stable.
    let anchor = trigger
        .parent_turn_id
        .as_deref()
        .unwrap_or(trigger.id.as_str());
    let key = format!("tasks:{anchor}");
    let settled = terminal_count >= total;

    // content_ref: a single-task batch links straight to the task artifact; a
    // multi-task batch links to the spawning node's tasks collection, which
    // lists each child's status and artifact URI. Fall back to the recipient job
    // id when neither resolves.
    let content_ref = if total <= 1 {
        match trigger.result_artifact_job_id.as_deref() {
            Some(job_id)
                if matches!(
                    trigger.status,
                    DelegatedStatus::Failed | DelegatedStatus::Cancelled
                ) =>
            {
                crate::messages::delivery::node_uri_for_job(db, job_id).await
            }
            Some(job_id) => task_artifact_uri(db, job_id).await,
            None => None,
        }
    } else {
        crate::messages::delivery::node_uri_for_job(db, &recipient)
            .await
            .map(|uri| format!("{uri}/tasks"))
    };
    let content_ref = content_ref.unwrap_or_else(|| recipient.clone());

    // Passive while the batch is in flight (the monotonic progress fingerprint
    // supersedes the prior undelivered row each completion); one Wake when the
    // last sibling settles. The fingerprint guard below makes a re-finalize
    // after delivery a no-op, so recovery never double-wakes.
    let (wake, fingerprint) = if settled {
        (Wake::Wake, format!("complete:{total}"))
    } else {
        (Wake::Passive, format!("progress:{terminal_count}/{total}"))
    };

    if let Some(Some(prev)) = latest_push_fingerprint(db, &recipient, &key)
        .await
        .map_err(|e| e.to_string())?
    {
        if prev == fingerprint {
            return Ok(None);
        }
    }

    let (_, effective) = push_with_fingerprint(
        db,
        &recipient,
        &content_ref,
        wake,
        Boundary::Event,
        &key,
        Some(&fingerprint),
    )
    .await
    .map_err(|e| e.to_string())?;

    Ok(Some(TaskCompletionOutcome::BackgroundNotify {
        recipient,
        wake_idle: effective.wakes_idle(),
    }))
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
/// job, the same `parent_turn_id`, AND the same `background` disposition.
/// Concurrent task spawns coalesce onto one wait (see
/// `resolve_delegated_wait_anchor`), so every packet sharing that anchor resumes
/// as a unit — whether they came from one `write` call or several back-to-back
/// ones. The parent resume gate fires only once every packet in this group is
/// terminal, and they deliver their results together. Falls back to `trigger`
/// alone when it carries no anchor turn.
///
/// `background` is part of the group identity, not just the anchor: a parent can
/// spawn a background batch and a blocking batch in the *same* turn, so both
/// share the anchor turn but have opposite resume semantics. Without this split
/// a blocking resume would wait for and inline a fire-and-forget sibling, and a
/// background completion wake would be delayed behind unrelated foreground work.
/// Keying the group on `background` too keeps the two paths from ever observing
/// each other's packets.
fn delegated_sibling_ids(
    packets: &[crate::models::DelegatedWorkPacket],
    trigger: &crate::models::DelegatedWorkPacket,
) -> Vec<String> {
    packets
        .iter()
        .filter(|candidate| {
            candidate.parent_job_id == trigger.parent_job_id
                && candidate.background == trigger.background
                // Origin partitions the group too (CAIRN-2481): a parent can spawn
                // a background task batch and a background call batch in the same
                // turn, sharing the anchor but with independent resume identities.
                && candidate.origin == trigger.origin
                && match trigger.parent_turn_id.as_deref() {
                    Some(anchor) => candidate.parent_turn_id.as_deref() == Some(anchor),
                    None => candidate.id == trigger.id,
                }
        })
        .map(|candidate| candidate.id.clone())
        .collect()
}

/// Atomically mint and claim the dependency-unblock marker after the final
/// blocking child is terminal. The yielded anchor remaining the job head is the
/// durable ownership predicate, so provider admission never sees an unowned
/// pending successor.
async fn claim_dependency_unblock(
    db: &LocalDb,
    parent_job_id: &str,
    predecessor_id: &str,
) -> Result<bool, String> {
    let parent_job_id = parent_job_id.to_string();
    let predecessor_id = predecessor_id.to_string();
    db.write(|conn| {
        let parent_job_id = parent_job_id.clone();
        let predecessor_id = predecessor_id.clone();
        Box::pin(async move {
            // Upgrade recovery: older builds published a pending marker while
            // parking. It is claimable only after this completion gate has
            // established terminal children, and only while it remains the job
            // head. New waits never enter this branch.
            if let Some(successor_id) = query_successor_turn(conn, &predecessor_id).await? {
                let now = chrono::Utc::now().timestamp() as i32;
                let affected = conn
                    .execute(
                        "UPDATE turns SET state = 'complete', ended_at = ?1, updated_at = ?1
                     WHERE id = ?2 AND state = 'pending'
                       AND ?2 = (SELECT current_turn_id FROM jobs WHERE id = ?3)",
                        (now, successor_id.as_str(), parent_job_id.as_str()),
                    )
                    .await?;
                return Ok(affected == 1);
            }
            let mut rows = conn
                .query(
                    "SELECT t.session_id, t.state, t.yield_reason
                     FROM turns t
                     JOIN jobs j ON j.id = ?1 AND j.current_turn_id = t.id
                     WHERE t.id = ?2 LIMIT 1",
                    (parent_job_id.as_str(), predecessor_id.as_str()),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(false);
            };
            let session_id = row.text(0)?;
            let state = row.text(1)?;
            let yield_reason = row.opt_text(2)?;
            if state != "yielded" || yield_reason.as_deref() != Some("dependency_wait") {
                return Ok(false);
            }

            let sequence = next_turn_sequence(conn, &session_id).await?;
            let now = chrono::Utc::now().timestamp() as i32;
            let successor_id = ids::mint_child(&parent_job_id);
            conn.execute(
                "INSERT INTO turns (
                    id, session_id, run_id, job_id, sequence, predecessor_id,
                    state, start_reason, created_at, started_at, ended_at, updated_at
                 ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, 'complete',
                           'dependency_unblock', ?6, NULL, ?6, ?6)",
                (
                    successor_id.as_str(),
                    session_id.as_str(),
                    parent_job_id.as_str(),
                    sequence,
                    predecessor_id.as_str(),
                    now,
                ),
            )
            .await?;
            let affected = conn
                .execute(
                    "UPDATE jobs SET current_turn_id = ?1
                 WHERE id = ?2 AND current_turn_id = ?3",
                    (
                        successor_id.as_str(),
                        parent_job_id.as_str(),
                        predecessor_id.as_str(),
                    ),
                )
                .await?;
            if affected != 1 {
                return Err(crate::storage::DbError::internal(
                    "dependency-unblock ownership changed during atomic claim".to_string(),
                ));
            }
            Ok(true)
        })
    })
    .await
    .map_err(|e| format!("Failed to claim resume successor: {}", e))
}

/// Whether `job_id` belongs to a one-shot ephemeral `CallTool` packet.
///
/// A call child is created directly with a pre-materialized `CallTool` packet
/// (CAIRN-2481) whose `result_artifact_job_id` is the call's own job id; it is
/// never resumed, so once its work completes it should be reaped rather than
/// left warm (CAIRN-2543). Reuses the same snapshot lookup as
/// `resume_suspended_parent_after_task_completion` — finds the packet by child
/// job id and inspects its origin. Returns false on any lookup failure (fail
/// safe: an unclassifiable job stays warm and is governed by the GC budget).
pub fn is_call_child(orch: &Orchestrator, job_id: &str) -> bool {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    block_on(async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        let Some(execution_id) =
            select_optional_text(&db, "SELECT execution_id FROM jobs WHERE id = ?1", &job_id)
                .await
                .map_err(|e| e.to_string())?
        else {
            return Ok(false);
        };
        let Some(snapshot_json) = select_optional_text(
            &db,
            "SELECT snapshot FROM executions WHERE id = ?1",
            &execution_id,
        )
        .await
        .map_err(|e| e.to_string())?
        else {
            return Ok(false);
        };
        let snapshot: ExecutionSnapshot =
            serde_json::from_str(&snapshot_json).map_err(|e| e.to_string())?;
        Ok(snapshot.delegated_packets.iter().any(|packet| {
            packet.result_artifact_job_id.as_deref() == Some(job_id.as_str())
                && packet.origin == crate::models::DelegationOrigin::CallTool
        }))
    })
    .unwrap_or(false)
}

pub fn resume_suspended_parent_after_task_completion(
    orch: &Orchestrator,
    child_job_id: &str,
) -> Result<(), String> {
    resume_parent_after_task_settled(orch, child_job_id, ResumeMode::Live)
}

/// Deliver a task that a boot repair just settled to the parent that spawned it,
/// if and only if that parent is still parked on the task's exact anchor turn.
///
/// This is the live completion path with two differences. A background packet
/// pushes nothing: the spawner was never parked on it, so a late wake would
/// interrupt whatever it is doing now with news it stopped waiting for. And each
/// delivered result leads with a delayed-delivery notice naming when the task
/// actually finished. A blocking packet still has to clear the same gates —
/// every sibling terminal, then the exact-anchor `dependency_unblock` claim — so
/// a parent whose head moved on is left alone, and a batch repaired together
/// wakes its parent exactly once.
pub fn resume_parked_parent_after_repair(
    orch: &Orchestrator,
    child_job_id: &str,
) -> Result<(), String> {
    resume_parent_after_task_settled(orch, child_job_id, ResumeMode::Repair)
}

fn resume_parent_after_task_settled(
    orch: &Orchestrator,
    child_job_id: &str,
    mode: ResumeMode,
) -> Result<(), String> {
    let db = {
        let dbs = orch.db.clone();
        let cjid = child_job_id.to_string();
        block_on(async move {
            crate::execution::routing::owning_db_for_job(&dbs, &cjid)
                .await
                .map_err(|e| e.to_string())
        })?
    };
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

            // Background spawns never prepared a successor turn, so the blocking
            // successor/resume path below does not apply: notify the spawner via
            // a push instead. A repair pushes nothing: nobody is parked on it.
            if refreshed_packet.background {
                if mode == ResumeMode::Repair {
                    return Ok(None);
                }
                return notify_spawner_after_background_task_completion(
                    &db,
                    &execution_id,
                    &refreshed_packet,
                    &sibling_ids,
                )
                .await;
            }

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
            // Publish an already-claimed marker only after every sibling is
            // terminal. Exactly one concurrent completion can replace the anchor.
            if !claim_dependency_unblock(&db, &refreshed_packet.parent_job_id, parent_turn_id)
                .await?
            {
                return Ok(None);
            }

            grouped_packets.sort_by_key(|packet| (packet.task_index, packet.created_at));
            let rendered_packets = render_delegated_results(&db, grouped_packets, mode).await?;

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

            Ok(Some(TaskCompletionOutcome::Resume(TaskResumeDelivery {
                parent_job_id: refreshed_packet.parent_job_id,
                parent_turn_id: parent_turn_id.to_string(),
                message: full_combined,
                tool_results,
            })))
        }
    })?;
    let resume = match resume {
        Some(TaskCompletionOutcome::Resume(resume)) => resume,
        Some(TaskCompletionOutcome::BackgroundNotify {
            recipient,
            wake_idle,
        }) => {
            orch.notifier.emit_change("attention_pushes");
            // A settled background batch upgrades to a `wake`; nudge an idle
            // spawner so it drains the push now. A passive in-flight push
            // reports `wake_idle == false` and rides along on the next run.
            if wake_idle {
                if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(
                    orch,
                    &recipient,
                    crate::messages::queued::DeliveryUrgency::Steer,
                ) {
                    log::warn!(
                        "background task completion wake for {} failed: {}",
                        recipient,
                        e
                    );
                }
            }
            return Ok(());
        }
        None => return Ok(()),
    };
    let mut suppress_user_event = !resume.tool_results.is_empty();
    if suppress_user_event {
        let now = chrono::Utc::now().timestamp() as i32;
        for tool_result in &resume.tool_results {
            if let Err(e) = crate::execution::jobs::store_blocking_write_result_with_resolution(
                orch,
                &tool_result.run_id,
                &tool_result.session_id,
                &tool_result.tool_use_id,
                &tool_result.content,
                tool_result.is_error,
                now,
                Some(&resume.parent_turn_id),
                None,
            ) {
                log::warn!("Failed to store synthetic task tool_result: {}", e);
                suppress_user_event = false;
                break;
            }
        }
    }
    let resume_context = Some(crate::execution::jobs::ResumeContext {
        suppress_user_event,
        goal_reactivation: Some(GoalReactivation::OwnedConditionResolved(
            GoalPauseOwner::DependencyWait {
                parked_turn_id: resume.parent_turn_id.clone(),
            },
        )),
        ..Default::default()
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

    #[test]
    fn delayed_delivery_notice_names_when_the_task_finished() {
        let notice = delayed_delivery_notice(Some(1_757_000_000));
        assert!(notice.starts_with("Delayed delivery:"), "{notice}");
        assert!(notice.contains("at 2025-09-04 15:33 UTC"), "{notice}");
        assert!(notice.contains("as history, not as fresh news"), "{notice}");
        assert!(delayed_delivery_notice(None).contains("long before this delivery"));
    }

    /// A repaired delivery prefixes every packet's result with its own notice and
    /// finish time; the live path renders the bare result. Both the combined
    /// resume message and the Task `tool_result`s are built from these strings.
    #[tokio::test]
    async fn repaired_results_lead_with_their_delayed_delivery_notice() {
        let db = LocalDb::open(tempfile::tempdir().unwrap().keep().join("render.db"))
            .await
            .unwrap();
        crate::storage::MigrationRunner::new(crate::storage::TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','w','P','prj','/tmp/prj',1,1);
             INSERT INTO jobs (id, project_id, status, created_at, updated_at)
               VALUES ('j-a','p','complete',1,1), ('j-b','p','complete',1,1);
             INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
               VALUES ('t-a','s-a','j-a',1,'complete','initial',1,1757000000,1),
                      ('t-b','s-b','j-b',1,'complete','initial',1,1757003600,1),
                      ('t-b-review','s-b','j-b',2,'complete','memory_review',2,1800000000,2);
             INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at)
               VALUES ('a-a','j-a','return',0,'{\"summary\":\"quic ok\"}',1,'return',1,1),
                      ('a-b','j-b','return',0,'{\"summary\":\"tar ok\"}',1,'return',1,1);",
        )
        .await
        .unwrap();
        let db = std::sync::Arc::new(db);
        let packets = || {
            ["j-a", "j-b"]
                .into_iter()
                .map(|job| {
                    let mut p = packet(job, "parent", Some("anchor"));
                    p.result_artifact_job_id = Some(job.to_string());
                    p
                })
                .collect::<Vec<_>>()
        };

        let repaired = render_delegated_results(&db, packets(), ResumeMode::Repair)
            .await
            .unwrap();
        assert!(repaired[0]
            .1
            .starts_with("Delayed delivery: this task finished at 2025-09-04 15:33 UTC"));
        assert!(repaired[0].1.contains("quic ok"));
        // The memory-review turn is not the task's finish.
        assert!(repaired[1]
            .1
            .starts_with("Delayed delivery: this task finished at 2025-09-04 16:33 UTC"));
        assert!(repaired[1].1.contains("tar ok"));
        let message = format_delegated_resume_message(&repaired);
        assert_eq!(message.matches("Delayed delivery:").count(), 2);

        let live = render_delegated_results(&db, packets(), ResumeMode::Live)
            .await
            .unwrap();
        assert!(live
            .iter()
            .all(|(_, text)| !text.contains("Delayed delivery")));
    }

    fn packet(
        id: &str,
        parent_job: &str,
        anchor: Option<&str>,
    ) -> crate::models::DelegatedWorkPacket {
        packet_with_tool(id, parent_job, anchor, None)
    }

    fn packet_origin(
        id: &str,
        parent_job: &str,
        anchor: Option<&str>,
        origin: &str,
    ) -> crate::models::DelegatedWorkPacket {
        let value = serde_json::json!({
            "id": id,
            "parentJobId": parent_job,
            "parentTurnId": anchor,
            "origin": origin,
            "title": "X",
            "problemStatement": "x",
            "agentConfigId": "A",
            "ownership": { "cwd": "/tmp" },
            "outputContract": { "schemaType": "return" },
            "status": "completed",
            "createdAt": 0
        });
        serde_json::from_value(value).unwrap()
    }

    /// A background task batch and a background call batch spawned in the same
    /// turn (shared anchor + background) never group together, so one never
    /// resumes on the other's completion (CAIRN-2481).
    #[test]
    fn sibling_ids_partition_by_origin() {
        let task = packet_origin("t", "job", Some("T1"), "task_tool");
        let call = packet_origin("c", "job", Some("T1"), "call_tool");
        let packets = vec![task.clone(), call.clone()];
        assert_eq!(
            delegated_sibling_ids(&packets, &task),
            vec!["t".to_string()]
        );
        assert_eq!(
            delegated_sibling_ids(&packets, &call),
            vec!["c".to_string()]
        );
    }

    fn packet_bg(
        id: &str,
        parent_job: &str,
        anchor: Option<&str>,
        background: bool,
    ) -> crate::models::DelegatedWorkPacket {
        let mut packet = packet(id, parent_job, anchor);
        packet.background = background;
        packet
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
    fn single_task_delivery_keeps_payload_raw_until_write_report_completion() {
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
    fn siblings_split_background_from_blocking_in_same_turn() {
        // A background batch and a blocking batch spawned in the same parent turn
        // share the anchor T1, but their resume semantics differ, so each path
        // must see only its own packets — never the other disposition's.
        let bg_trigger = packet_bg("a", "builder", Some("T1"), true);
        let fg_trigger = packet_bg("c", "builder", Some("T1"), false);
        let packets = vec![
            packet_bg("a", "builder", Some("T1"), true),
            packet_bg("b", "builder", Some("T1"), true),
            packet_bg("c", "builder", Some("T1"), false),
            packet_bg("d", "builder", Some("T1"), false),
        ];
        let mut bg = delegated_sibling_ids(&packets, &bg_trigger);
        bg.sort();
        assert_eq!(bg, vec!["a".to_string(), "b".to_string()]);
        let mut fg = delegated_sibling_ids(&packets, &fg_trigger);
        fg.sort();
        assert_eq!(fg, vec!["c".to_string(), "d".to_string()]);
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

    /// Migrated DB with a yielded dependency anchor as the current job head.
    async fn migrated_db_with_dependency_anchor() -> LocalDb {
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
                     VALUES ('proj', 'default', 'P', 'proj', '/tmp/p', 0, 0)",
                    (),
                )
                .await?;
                // Turn first: jobs.current_turn_id has an FK to turns(id).
                conn.execute(
                    "INSERT INTO turns(id, session_id, sequence, state, yield_reason, created_at, updated_at) \
                     VALUES ('anchor', 'sess', 1, 'yielded', 'dependency_wait', 0, 0)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, status, project_id, created_at, updated_at, current_turn_id) \
                     VALUES ('job', 'pending', 'proj', 0, 0, 'anchor')",
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

    /// Exactly one claim mints the completed marker; the rest lose.
    #[tokio::test]
    async fn dependency_unblock_claim_is_won_once() {
        let db = migrated_db_with_dependency_anchor().await;
        assert!(claim_dependency_unblock(&db, "job", "anchor")
            .await
            .unwrap());
        let successor_id = db
            .query_opt_text(
                "SELECT id FROM turns WHERE predecessor_id = ?1",
                ("anchor".to_string(),),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(turn_state(&db, &successor_id).await, "complete");
        assert!(!claim_dependency_unblock(&db, "job", "anchor")
            .await
            .unwrap());
    }

    /// Operator cancellation removes the yielded-anchor ownership predicate.
    #[tokio::test]
    async fn operator_stop_prevents_dependency_unblock_claim() {
        let db = migrated_db_with_dependency_anchor().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "UPDATE turns SET state = 'cancelled', end_reason = 'user_stop' WHERE id = 'anchor'",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert!(!claim_dependency_unblock(&db, "job", "anchor")
            .await
            .unwrap());
        assert!(db
            .query_opt_text(
                "SELECT id FROM turns WHERE predecessor_id = ?1",
                ("anchor".to_string(),),
            )
            .await
            .unwrap()
            .is_none());
    }
}

#[cfg(test)]
mod background_completion_tests {
    use super::{
        prepare_parent_for_delegated_wait, prepare_parent_for_delegated_wait_db,
        resume_suspended_parent_after_task_completion,
    };
    use crate::agent_process::process::{GoalPauseOwner, RunHandle};
    use crate::db::DbState;
    use crate::orchestrator::attention_push::{list_pending, stamp_delivered, Push, Wake};
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::{Arc, Mutex};

    const ANCHOR: &str = "T1";
    const TASKS_KEY: &str = "tasks:T1";

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        crate::identity::local::save_local_identity(
            &config_dir,
            &crate::identity::UserIdentity {
                user_id: "test-user".into(),
                email: "test@example.com".into(),
                name: "Test User".into(),
                claude_auth: None,
                codex_auth: Some(crate::credentials::CodexAuth::ApiKey("test-key".into())),
                github_token: None,
            },
        )
        .unwrap();
        let credential_store =
            crate::credentials::store::load_credential_store(&config_dir).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir)
            .credential_store(credential_store)
            .build()
    }

    /// Execution snapshot JSON with one delegated packet per child, all anchored
    /// on `T1`. Each tuple is `(packet_id, child_job_id, background)`.
    fn snapshot_json(children: &[(&str, &str, bool)]) -> String {
        let packets: Vec<serde_json::Value> = children
            .iter()
            .map(|(pid, cjob, bg)| {
                serde_json::json!({
                    "id": pid,
                    "parentJobId": "j-parent",
                    "parentTurnId": ANCHOR,
                    "origin": "task_tool",
                    "title": "Explore",
                    "problemStatement": "x",
                    "agentConfigId": "Explore",
                    "ownership": { "cwd": "/tmp" },
                    "outputContract": { "schemaType": "return" },
                    "status": "running",
                    "resultArtifactJobId": cjob,
                    "background": bg,
                    "createdAt": 0
                })
            })
            .collect();
        serde_json::json!({
            "recipe": {"id":"r","name":"R","description":null,"trigger":"manual","nodes":[],"edges":[]},
            "agents": {
                "Builder": {
                    "id": "Builder",
                    "name": "Builder",
                    "description": "",
                    "prompt": "Build the assigned change.",
                    "tools": [],
                    "disallowedTools": null,
                    "skills": null
                }
            },
            "skills": {},
            "triggerContext": {"issueId":"i","projectId":"p","triggerType":"manual"},
            "delegatedPackets": packets,
            "createdAt": 0
        })
        .to_string()
    }

    /// Seed a project/issue/execution, the spawning parent node `j-parent`, and a
    /// child job per packet. Each tuple is `(packet_id, child_job_id, background,
    /// child_status)`.
    async fn seed(db: &LocalDb, children: &[(&str, &str, bool, &str)]) {
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','prj','/tmp/repo',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',7,'I','active','active','none',1,1);",
        )
        .await
        .unwrap();

        let snap = snapshot_json(
            &children
                .iter()
                .map(|(pid, cjob, bg, _)| (*pid, *cjob, *bg))
                .collect::<Vec<_>>(),
        );
        db.write(move |conn| {
            let snap = snap.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,snapshot)
                     VALUES('e','r','i','p','running',1,1,?1)",
                    (snap.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id,execution_id,project_id,issue_id,status,uri_segment,node_name,created_at,updated_at)
                     VALUES('j-parent','e','p','i','running','builder','builder',1,1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        for (_, cjob, _, status) in children {
            let cjob = cjob.to_string();
            let status = status.to_string();
            db.write(move |conn| {
                let cjob = cjob.clone();
                let status = status.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO jobs(id,execution_id,project_id,issue_id,status,uri_segment,node_name,parent_job_id,created_at,updated_at)
                         VALUES(?1,'e','p','i',?2,?1,'Explore','j-parent',1,1)",
                        (cjob.as_str(), status.as_str()),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }
    }

    async fn set_job_status(db: &LocalDb, job_id: &str, status: &str) {
        let job_id = job_id.to_string();
        let status = status.to_string();
        db.write(move |conn| {
            let job_id = job_id.clone();
            let status = status.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE jobs SET status = ?1 WHERE id = ?2",
                    (status.as_str(), job_id.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn parent_pushes(orch: &Orchestrator) -> Vec<Push> {
        list_pending(&orch.db.local, "j-parent").await.unwrap()
    }

    async fn state_of_turn(db: &LocalDb, turn_id: &str) -> String {
        db.query_opt_text(
            "SELECT state FROM turns WHERE id = ?1",
            (turn_id.to_string(),),
        )
        .await
        .unwrap()
        .expect("turn exists")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blocking_task_wait_parks_then_unblocks_through_real_completion_launcher() {
        exercise_blocking_task_completion(None).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resumed_task_report_shaped_artifact_stays_opaque_and_preserves_write_metadata() {
        exercise_blocking_task_completion(Some(
            r#"{"applied":[],"blocking":{"text":"Delegated task 'inner' suspended; the parent run will resume from the real child result(s)."}}"#,
        )).await;
    }

    async fn exercise_blocking_task_completion(child_artifact: Option<&str>) {
        let db = crate::storage::migrated_test_db("delegated-wait-launcher.db").await;
        // The production specimen that escaped the prior regression: parking
        // happens while the blocking child is genuinely still running.
        seed(&db, &[("pkt-1", "j-c1", false, "running")]).await;
        db.execute_script(
            "INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('session-parent','j-parent','codex','native-thread','open',1,1,1);
             INSERT INTO runs(id,job_id,issue_id,project_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('run-parent','j-parent','i','p','session-parent','live','resume',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at)
               VALUES('T1','session-parent','run-parent','j-parent',1,'running','initial',1,1,1);
             UPDATE jobs
                SET current_session_id='session-parent', current_turn_id='T1',
                    agent_config_id='Builder', model='gpt-5.6-sol'
              WHERE id='j-parent';",
        )
        .await
        .unwrap();
        if let Some(child_artifact) = child_artifact {
            db.execute(
                "UPDATE executions SET snapshot=json_set(snapshot, '$.delegatedPackets[0].parentToolUseId', 'tool-write') WHERE id='e'",
                (),
            ).await.unwrap();
            db.execute(
                "INSERT INTO artifacts(id,job_id,artifact_type,data,version,created_at,updated_at) VALUES('child-result','j-c1','return',?1,1,1,1)",
                (serde_json::json!({"content": child_artifact}).to_string(),),
            ).await.unwrap();
            let report = serde_json::json!({
                "applied": [{"index": 0, "summary": "saved"}],
                "failures": [{"index": 1, "error": "refused"}],
                "partial_success": true, "commit": {"sha": "abc"},
                "blocking": {"text": "Delegated task 'Explore' suspended; the parent run will resume from the real child result(s)."}
            });
            db.execute(
                "INSERT INTO write_replay_ledger(run_id,tool_use_id,report,created_at) VALUES('run-parent','tool-write',?1,1)",
                (report.to_string(),),
            ).await.unwrap();
        }
        let orch = test_orchestrator(db);
        let (stdin, requests, provider_turn) =
            crate::backends::codex::scripted_delegated_wait_stdin_for_test(
                "native-thread",
                "provider-parent",
            );
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(stdin))),
            Some("session-parent".into()),
            Some("j-parent".into()),
        );
        handle.backend = Some("codex".into());
        handle.model = Some("gpt-5.6-sol".into());
        handle.begin_turn(ANCHOR);
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-parent".into(), handle);
        orch.process_state
            .set_current_turn_id("run-parent", Some(ANCHOR));

        let parent = super::super::common::ParentRunContext {
            run_id: "run-parent".into(),
            job_id: "j-parent".into(),
            execution_id: Some("e".into()),
            project_id: "p".into(),
            project_key: "prj".into(),
        };
        let waiting_packet = serde_json::from_value(serde_json::json!({
            "id": "pkt-1",
            "parentJobId": "j-parent",
            "parentTurnId": ANCHOR,
            "origin": "task_tool",
            "title": "Explore",
            "problemStatement": "x",
            "agentConfigId": "Explore",
            "ownership": { "cwd": "/tmp" },
            "outputContract": { "schemaType": "return" },
            "status": "running",
            "resultArtifactJobId": "j-c1",
            "createdAt": 0
        }))
        .unwrap();
        prepare_parent_for_delegated_wait(&orch, &parent, &waiting_packet)
            .unwrap_or_else(|error| panic!("{error}; requests={:?}", requests.lock().unwrap()));

        assert_eq!(state_of_turn(&orch.db.local, ANCHOR).await, "yielded");
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT yield_reason FROM turns WHERE id = ?1",
                    (ANCHOR.to_string(),),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("dependency_wait")
        );
        assert!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT id FROM turns WHERE predecessor_id = ?1",
                    (ANCHOR.to_string(),),
                )
                .await
                .unwrap()
                .is_none(),
            "parking a running child must not publish an unblock successor"
        );

        crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
            &orch,
            "run-parent",
            "delegated_wait_suspended",
            GoalPauseOwner::DependencyWait {
                parked_turn_id: ANCHOR.to_string(),
            },
        )
        .unwrap();
        *provider_turn.lock().unwrap() = None;
        resume_suspended_parent_after_task_completion(&orch, "j-c1")
            .expect("a running child is an idempotent resume no-op");
        assert!(requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request["method"] != "turn/start"));
        assert!(orch
            .db
            .local
            .query_opt_text(
                "SELECT id FROM turns WHERE predecessor_id = ?1",
                (ANCHOR.to_string(),),
            )
            .await
            .unwrap()
            .is_none());
        assert_eq!(state_of_turn(&orch.db.local, ANCHOR).await, "yielded");
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT end_reason FROM turns WHERE id = ?1",
                    (ANCHOR.to_string(),),
                )
                .await
                .unwrap(),
            None,
            "dependency suspension must not masquerade as user_stop"
        );

        set_job_status(&orch.db.local, "j-c1", "complete").await;
        resume_suspended_parent_after_task_completion(&orch, "j-c1")
            .expect("the terminal child should launch its waiting parent");
        resume_suspended_parent_after_task_completion(&orch, "j-c1")
            .expect("duplicate completion notification is a no-op");

        if let Some(child_artifact) = child_artifact {
            let data = orch.db.local.query_opt_text(
                "SELECT data FROM events WHERE run_id='run-parent' AND event_type='tool_result' AND json_extract(data,'$.toolUseId')='tool-write'",
                (),
            ).await.unwrap().expect("actual completion producer stores a tool result");
            let event: serde_json::Value = serde_json::from_str(&data).unwrap();
            let report: serde_json::Value = serde_json::from_str(
                event["toolResult"]
                    .as_str()
                    .expect("synthetic tool result text"),
            )
            .unwrap();
            assert_eq!(report["blocking"]["text"], child_artifact);
            assert_eq!(report["applied"][0]["summary"], "saved");
            assert_eq!(report["failures"][0]["error"], "refused");
            assert_eq!(report["commit"]["sha"], "abc");
            assert_eq!(report["partial_success"], true);
        }

        let successor_id = orch.db.local.query_opt_text(
            "SELECT id FROM turns WHERE predecessor_id = ?1 AND start_reason = 'dependency_unblock'",
            (ANCHOR.to_string(),),
        ).await.unwrap().expect("final child completion creates the claimed marker");
        let successor_count = orch.db.local.query_opt_i64(
            "SELECT COUNT(*) FROM turns WHERE predecessor_id = ?1 AND start_reason = 'dependency_unblock'",
            (ANCHOR.to_string(),),
        ).await.unwrap();
        assert_eq!(
            successor_count,
            Some(1),
            "duplicate completion must not mint twice"
        );

        {
            let sent = requests.lock().unwrap();
            assert_eq!(sent.len(), 8, "{sent:?}");
            assert_eq!(sent[0]["method"], "thread/goal/get");
            assert_eq!(sent[1]["method"], "thread/goal/set");
            assert_eq!(sent[1]["params"]["status"], "paused");
            assert_eq!(sent[2]["method"], "thread/goal/get");
            assert_eq!(sent[3]["method"], "thread/goal/set");
            assert_eq!(sent[3]["params"]["status"], "paused");
            assert_eq!(sent[4]["method"], "turn/interrupt");
            assert_eq!(sent[5]["method"], "thread/goal/get");
            assert_eq!(sent[5]["params"]["threadId"], "native-thread");
            assert_eq!(sent[6]["method"], "thread/goal/set");
            assert_eq!(sent[6]["params"]["status"], "active");
            assert_eq!(sent[7]["method"], "turn/start");
            assert_eq!(sent[7]["params"]["threadId"], "native-thread");
            let prompt = sent[7]["params"]["input"][0]["text"].as_str().unwrap();
            assert!(!prompt.contains("[Goal state: paused"));
            assert!(prompt.contains("Delegated task"));
            assert_eq!(
                sent.iter()
                    .filter(|request| request["method"] == "turn/start")
                    .count(),
                1,
                "duplicate completion must not start a second provider turn"
            );
        }
        assert_eq!(
            provider_turn.lock().unwrap().as_deref(),
            Some("provider-unblocked")
        );
        assert_eq!(
            state_of_turn(&orch.db.local, &successor_id).await,
            "complete"
        );
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT backend_id FROM sessions WHERE id = ?1",
                    ("session-parent".to_string(),),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("native-thread")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn operator_stop_with_running_child_never_publishes_dependency_unblock() {
        let db = crate::storage::migrated_test_db("delegated-wait-operator-stop.db").await;
        seed(&db, &[("pkt-1", "j-c1", false, "running")]).await;
        db.execute_script(
            "INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('session-parent','j-parent','codex','native-thread','open',1,1,1);
             INSERT INTO runs(id,job_id,issue_id,project_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('run-parent','j-parent','i','p','session-parent','live','resume',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at)
               VALUES('T1','session-parent','run-parent','j-parent',1,'running','initial',1,1,1);
             UPDATE jobs SET current_session_id='session-parent', current_turn_id='T1'
              WHERE id='j-parent';",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        prepare_parent_for_delegated_wait_db(
            orch.db.local.clone(),
            ANCHOR.to_string(),
            "j-parent".to_string(),
        )
        .await
        .unwrap();

        crate::orchestrator::lifecycle::stop_active_turn_for_run(&orch, "run-parent", true);
        assert_eq!(state_of_turn(&orch.db.local, ANCHOR).await, "cancelled");
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT end_reason FROM turns WHERE id = ?1",
                    (ANCHOR.to_string(),),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("user_stop")
        );

        set_job_status(&orch.db.local, "j-c1", "complete").await;
        resume_suspended_parent_after_task_completion(&orch, "j-c1").unwrap();
        assert!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT id FROM turns WHERE predecessor_id = ?1",
                    (ANCHOR.to_string(),),
                )
                .await
                .unwrap()
                .is_none(),
            "operator stop must remain intentional after child completion"
        );
    }

    /// One background task settling pushes a single `tasks:` wake to the spawner,
    /// linked to the task artifact.
    #[tokio::test]
    async fn single_background_task_wakes_spawner() {
        let db = crate::storage::migrated_test_db("bg-single.db").await;
        seed(&db, &[("pkt-1", "j-c1", true, "complete")]).await;
        let orch = test_orchestrator(db);

        resume_suspended_parent_after_task_completion(&orch, "j-c1").unwrap();

        let pushes = parent_pushes(&orch).await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].key, TASKS_KEY);
        assert_eq!(pushes[0].wake, Wake::Wake);
        assert!(
            pushes[0].content_ref.contains("/task/"),
            "content_ref = {}",
            pushes[0].content_ref
        );
    }

    /// A multi-task batch rides passively while siblings are still in flight and
    /// upgrades to exactly one wake when the last sibling settles.
    #[tokio::test]
    async fn batch_is_passive_until_last_sibling_settles() {
        let db = crate::storage::migrated_test_db("bg-batch.db").await;
        seed(
            &db,
            &[
                ("pkt-1", "j-c1", true, "running"),
                ("pkt-2", "j-c2", true, "running"),
                ("pkt-3", "j-c3", true, "running"),
            ],
        )
        .await;
        let orch = test_orchestrator(db);

        set_job_status(&orch.db.local, "j-c1", "complete").await;
        resume_suspended_parent_after_task_completion(&orch, "j-c1").unwrap();
        let pushes = parent_pushes(&orch).await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].key, TASKS_KEY);
        assert_eq!(pushes[0].wake, Wake::Passive);
        assert!(
            pushes[0].content_ref.ends_with("/tasks"),
            "content_ref = {}",
            pushes[0].content_ref
        );

        set_job_status(&orch.db.local, "j-c2", "complete").await;
        resume_suspended_parent_after_task_completion(&orch, "j-c2").unwrap();
        let pushes = parent_pushes(&orch).await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Passive);

        set_job_status(&orch.db.local, "j-c3", "complete").await;
        resume_suspended_parent_after_task_completion(&orch, "j-c3").unwrap();
        let pushes = parent_pushes(&orch).await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].key, TASKS_KEY);
        assert_eq!(pushes[0].wake, Wake::Wake);
    }

    /// Re-finalizing an already-settled, already-delivered batch creates no fresh
    /// push: the unchanged fingerprint guards against a double wake on recovery.
    #[tokio::test]
    async fn redelivery_after_settled_does_not_double_wake() {
        let db = crate::storage::migrated_test_db("bg-dedup.db").await;
        seed(&db, &[("pkt-1", "j-c1", true, "complete")]).await;
        let orch = test_orchestrator(db);

        resume_suspended_parent_after_task_completion(&orch, "j-c1").unwrap();
        let pushes = parent_pushes(&orch).await;
        assert_eq!(pushes.len(), 1);
        stamp_delivered(&orch.db.local, &[pushes[0].id.clone()], "ev-1")
            .await
            .unwrap();

        resume_suspended_parent_after_task_completion(&orch, "j-c1").unwrap();
        assert!(parent_pushes(&orch).await.is_empty());
    }

    /// A child that settles `failed` still counts terminal and fires the wake.
    #[tokio::test]
    async fn failed_child_still_fires_completion_wake() {
        let db = crate::storage::migrated_test_db("bg-failed.db").await;
        seed(&db, &[("pkt-1", "j-c1", true, "failed")]).await;
        let orch = test_orchestrator(db);

        resume_suspended_parent_after_task_completion(&orch, "j-c1").unwrap();
        let pushes = parent_pushes(&orch).await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Wake);
    }

    /// A child that settles `cancelled` likewise counts terminal and fires the
    /// wake — `refresh_packet_state` must map the cancelled job row to
    /// `DelegatedStatus::Cancelled`, or the batch never reaches terminal.
    #[tokio::test]
    async fn cancelled_child_still_fires_completion_wake() {
        let db = crate::storage::migrated_test_db("bg-cancelled.db").await;
        seed(&db, &[("pkt-1", "j-c1", true, "cancelled")]).await;
        let orch = test_orchestrator(db);

        resume_suspended_parent_after_task_completion(&orch, "j-c1").unwrap();
        let pushes = parent_pushes(&orch).await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Wake);
    }

    /// A non-background batch takes the blocking successor path and never creates
    /// a `tasks:` push.
    #[tokio::test]
    async fn blocking_batch_creates_no_tasks_push() {
        let db = crate::storage::migrated_test_db("bg-blocking.db").await;
        seed(&db, &[("pkt-1", "j-c1", false, "complete")]).await;
        let orch = test_orchestrator(db);

        resume_suspended_parent_after_task_completion(&orch, "j-c1").unwrap();
        assert!(parent_pushes(&orch).await.is_empty());
    }

    /// A background task and a blocking task spawned in the same parent turn
    /// share the anchor but form separate batches: finalizing the background one
    /// wakes immediately as a batch of one, not held behind the still-running
    /// foreground sibling.
    #[tokio::test]
    async fn mixed_same_turn_group_isolates_background_from_blocking() {
        let db = crate::storage::migrated_test_db("bg-mixed.db").await;
        seed(
            &db,
            &[
                ("pkt-bg", "j-bg", true, "complete"),
                ("pkt-fg", "j-fg", false, "running"),
            ],
        )
        .await;
        let orch = test_orchestrator(db);

        resume_suspended_parent_after_task_completion(&orch, "j-bg").unwrap();

        // The background batch is one task: it settles and wakes immediately,
        // ignoring the unrelated foreground sibling sharing the anchor.
        let pushes = parent_pushes(&orch).await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].key, TASKS_KEY);
        assert_eq!(pushes[0].wake, Wake::Wake);
    }

    /// Snapshot carrying a CallTool packet (`j-call`) and a TaskTool packet
    /// (`j-task`), so `is_call_child` can be checked against both origins.
    fn mixed_origin_snapshot() -> String {
        let packet = |pid: &str, cjob: &str, origin: &str| {
            serde_json::json!({
                "id": pid,
                "parentJobId": "j-parent",
                "parentTurnId": ANCHOR,
                "origin": origin,
                "title": "Explore",
                "problemStatement": "x",
                "agentConfigId": "Explore",
                "ownership": { "cwd": "/tmp" },
                "outputContract": { "schemaType": "return" },
                "status": "materialized",
                "resultArtifactJobId": cjob,
                "background": false,
                "createdAt": 0
            })
        };
        serde_json::json!({
            "recipe": {"id":"r","name":"R","description":null,"trigger":"manual","nodes":[],"edges":[]},
            "agents": {},
            "skills": {},
            "triggerContext": {"issueId":"i","projectId":"p","triggerType":"manual"},
            "delegatedPackets": [
                packet("pkt-call", "j-call", "call_tool"),
                packet("pkt-task", "j-task", "task_tool"),
            ],
            "createdAt": 0
        })
        .to_string()
    }

    /// `is_call_child` is true only for a job materialized from a CallTool
    /// packet; a TaskTool child and an unknown job are false (so the warm-path
    /// reaper never kills a resumable task or a job it cannot classify).
    #[tokio::test]
    async fn is_call_child_distinguishes_call_from_task() {
        let db = crate::storage::migrated_test_db("is-call-child.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','prj','/tmp/repo',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('i','p',7,'I','active','active','none',1,1);",
        )
        .await
        .unwrap();
        let snap = mixed_origin_snapshot();
        db.write(move |conn| {
            let snap = snap.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,snapshot)
                     VALUES('e','r','i','p','running',1,1,?1)",
                    (snap.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id,execution_id,project_id,issue_id,status,uri_segment,node_name,created_at,updated_at)
                     VALUES('j-parent','e','p','i','running','builder','builder',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id,execution_id,project_id,issue_id,status,uri_segment,node_name,parent_job_id,created_at,updated_at)
                     VALUES('j-call','e','p','i','complete','j-call','Explore','j-parent',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id,execution_id,project_id,issue_id,status,uri_segment,node_name,parent_job_id,created_at,updated_at)
                     VALUES('j-task','e','p','i','complete','j-task','Explore','j-parent',1,1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        assert!(super::is_call_child(&orch, "j-call"));
        assert!(!super::is_call_child(&orch, "j-task"));
        assert!(!super::is_call_child(&orch, "j-missing"));
    }
}
