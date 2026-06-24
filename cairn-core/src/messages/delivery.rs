//! Message delivery routing.
//!
//! Channel messages (project/issue) are stored in DB and pulled by the hook
//! handler using a per-process cursor — no push needed.
//!
//! Direct messages are pushed via stdin to auto-resume warm processes. Active
//! (mid-turn) processes see direct messages on the next prompt boundary via
//! the queued-direct injection paths (Claude `additionalContext` hook +
//! tool-result augmentation) — see CAIRN-1196.

use crate::messages::db as msg_db;
use crate::messages::queued::DeliveryUrgency;
use crate::models::{ChannelType, Message};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbError, LocalDb, RowExt};
use turso::params;

/// Look up the most recent run id for a job. Used by callers that have a job
/// id (PR conflict resolution, manager wake, etc.) and need to address the
/// recipient by run id for direct-message delivery.
pub fn latest_run_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { latest_run_for_job_async(db, &job_id).await })
        .ok()
        .flatten()
}

async fn latest_run_for_job_async(db: &LocalDb, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            crate::storage::next_text(&mut rows, 0).await
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Does this job currently have a pending or running turn? Shared between
/// the message handler's active-turn check and the queue-on-active fallback
/// path in conflict resolution (CAIRN-1196).
pub async fn head_turn_active(db: &LocalDb, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM turns
                     WHERE job_id = ?1 AND state IN ('pending', 'running')",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row("missing turn count".to_string()))?;
            Ok(row.i64(0)? > 0)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Sync wrapper for [`head_turn_active`] for use from non-async callers
/// (e.g. PR conflict resolution).
pub fn head_turn_active_sync(db: &LocalDb, job_id: &str) -> bool {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { head_turn_active(db, &job_id).await }).unwrap_or(false)
}

/// A yielded head turn carrying one of these reasons means the agent is blocked
/// on its OWN pending work and is therefore self-suspended — not idle, and not
/// wakeable by any external push. The set is exhaustive over `TurnYieldReason`
/// (`models::turn`): a sub-agent task/batch wait or a dependency wait yields
/// `dependency_wait`, the agent's own open question/prompt yields `user_input`,
/// and its own pending permission yields `permission`. Verify against the writers
/// (`mcp/handlers/planning.rs`, `mcp/handlers/permission.rs`,
/// `execution/delegation/runtime/resume.rs`) before extending.
fn is_self_suspend_reason(reason: &str) -> bool {
    matches!(reason, "user_input" | "permission" | "dependency_wait")
}

/// Is this job's agent **self-suspended** — its head turn `yielded` waiting on
/// its OWN pending work (a sub-agent task/batch or dependency wait, its own open
/// question/prompt, or its own pending permission)?
///
/// A self-suspended agent is not idle: it resumes only when its own pending work
/// resolves (the task returns, the question is answered, the permission is
/// decided, the dependency unblocks), draining any queued external push then. It
/// must NOT be woken by an external attention push, even a `wake`/`interrupt`
/// one (`docs/attention-redesign.md` — "Wakeable vs self-suspended"; CAIRN-1876).
///
/// A yielded head turn reliably means self-suspended: a normally-idle agent's
/// head turn is `complete` (the warm transition completes the turn — see
/// `orchestrator::lifecycle::transition_to_warm_state`), and a stopped agent's is
/// `interrupted`/`cancelled`; only the three durable-wait yields land a `yielded`
/// head turn. The reason check guards against any future non-self-suspend yield.
/// Head turn = the job's latest turn by sequence, mirroring the canonical
/// head-turn query in `jobs::queries::node_status_indicators`.
pub async fn head_turn_self_suspended(db: &LocalDb, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT state, yield_reason FROM turns
                     WHERE job_id = ?1
                     ORDER BY sequence DESC
                     LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(false);
            };
            if row.text(0)? != "yielded" {
                return Ok(false);
            }
            Ok(row
                .opt_text(1)?
                .as_deref()
                .map(is_self_suspend_reason)
                .unwrap_or(false))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Sync wrapper for [`head_turn_self_suspended`] for use from the non-async
/// flush-on-idle resume gate.
pub fn head_turn_self_suspended_sync(db: &LocalDb, job_id: &str) -> bool {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { head_turn_self_suspended(db, &job_id).await })
        .unwrap_or(false)
}

/// The recipient job's node URI, used as a `direct:` push's wake-card link. The
/// message body resolves from the messages row at drain, so this is only the UI
/// link; `None` when the job can't be resolved (the caller falls back).
async fn node_uri_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.key, i.number, COALESCE(e.seq, 1), j.uri_segment
                     FROM jobs j
                     JOIN issues i ON i.id = j.issue_id
                     JOIN projects p ON p.id = i.project_id
                     LEFT JOIN executions e ON e.id = j.execution_id
                     WHERE j.id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let key = row.text(0)?;
            let number = row.i64(1)? as i32;
            let seq = row.i64(2)? as i32;
            let segment = row.opt_text(3)?;
            Ok(segment.map(|seg| cairn_common::uri::build_node_uri(&key, number, seq, &seg)))
        })
    })
    .await
    .ok()
    .flatten()
}

/// Create a `direct:{message_id}` attention push so a system-originated direct
/// message rides the push queue (CAIRN-1900) rather than the retired
/// `delivered_at` pull path — without this the raw `messages` row is never
/// drained. `recipient_job_id` is the push recipient; the wake-card link is the
/// recipient's node URI when resolvable (the body resolves from the messages row
/// at drain). The wake level follows the urgency: `Interrupt` for interrupt,
/// `Passive` for a passive (ride-along) urgency, else `Wake`. Returns the
/// effective wake so the caller decides whether to nudge.
pub(crate) fn enqueue_direct_push(
    orch: &Orchestrator,
    recipient_job_id: &str,
    message_id: &str,
    urgency: DeliveryUrgency,
) -> Result<crate::orchestrator::attention_push::Wake, String> {
    use crate::orchestrator::attention_push::{Boundary, Wake};
    let requested = if urgency == DeliveryUrgency::Interrupt {
        Wake::Interrupt
    } else if urgency.wakes_idle() {
        Wake::Wake
    } else {
        Wake::Passive
    };
    let db = orch.db.local.clone();
    let recipient_job_id = recipient_job_id.to_string();
    let message_id = message_id.to_string();
    run_db_blocking(move || async move {
        let content_ref = node_uri_for_job(&db, &recipient_job_id)
            .await
            .unwrap_or_else(|| recipient_job_id.clone());
        let (_, effective) = crate::orchestrator::attention_push::push(
            &db,
            &recipient_job_id,
            &content_ref,
            requested,
            Boundary::Event,
            &format!("direct:{message_id}"),
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(effective)
    })
}

/// Persist a system-originated direct message addressed to `recipient_run_id` and
/// queue it on the attention push queue (CAIRN-1900). Used by orchestrator-internal
/// callers (base-branch advance notices, PR conflict resolution, etc.). Idle
/// recipients are nudged to resume; busy recipients drain the push at their next
/// event boundary.
pub fn queue_system_direct(
    orch: &Orchestrator,
    recipient_run_id: &str,
    content: &str,
) -> Result<(), String> {
    let msg = msg_db::insert_message(
        &orch.db.local,
        &ChannelType::Direct,
        None,
        None,
        "system",
        Some(recipient_run_id),
        content,
    )?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );

    let db = orch.db.local.clone();
    let run = recipient_run_id.to_string();
    let job_id = run_db_blocking(move || async move {
        Ok::<_, String>(crate::messages::side_channel::job_id_for_run(&db, &run).await)
    })
    .ok()
    .flatten();
    match job_id {
        Some(job_id) => match enqueue_direct_push(orch, &job_id, &msg.id, DeliveryUrgency::Steer) {
            Ok(effective) => {
                if effective.wakes_idle() {
                    if let Err(e) = nudge_job_for_urgency(orch, &job_id, DeliveryUrgency::Steer) {
                        log::warn!("queue_system_direct: nudge failed for job {job_id}: {e}");
                    }
                }
            }
            Err(e) => log::warn!("queue_system_direct: failed to enqueue direct push: {e}"),
        },
        None => log::warn!(
            "queue_system_direct: no job for run {recipient_run_id}; direct message {} undeliverable",
            msg.id
        ),
    }

    log::debug!("Queued system direct message {} for delivery", msg.id);
    Ok(())
}

/// Deliver a message after DB insertion.
///
/// For channel messages: no-op (hook pulls new messages via cursor).
/// For direct messages to warm processes: stdin push to auto-resume.
pub fn deliver(orch: &Orchestrator, message: &Message) {
    if message.channel_type == ChannelType::Direct {
        deliver_direct(orch, message);
    }
    // Channel messages: nothing to do — hook pulls from DB via cursor
}

/// Deliver a direct message via stdin if the recipient is a warm process.
/// This auto-resumes the process so it can react immediately.
fn deliver_direct(orch: &Orchestrator, message: &Message) {
    let recipient_run_id = match &message.recipient_run_id {
        Some(id) => id,
        None => return,
    };

    let is_warm = {
        let processes = match orch.process_state.processes.lock() {
            Ok(p) => p,
            Err(_) => return,
        };
        match processes.get(recipient_run_id.as_str()) {
            Some(process) => process.is_warm(),
            None => return,
        }
    };

    if is_warm {
        stdin_push(orch, recipient_run_id, message);
    }
    // Mid-turn (active) recipients: leave delivered_at = NULL so the next
    // hook fire / tool result picks the message up via
    // claim_pending_directs_for_run.
}

/// Send a message via stdin to a warm process to auto-resume it.
fn stdin_push(orch: &Orchestrator, run_id: &str, message: &Message) {
    // Get session_id and stdin handle together
    let session_id = {
        let processes = match orch.process_state.processes.lock() {
            Ok(p) => p,
            Err(_) => return,
        };
        match processes.get(run_id) {
            Some(p) => match &p.session_id {
                Some(sid) => sid.clone(),
                None => return,
            },
            None => return,
        }
    };

    let content = format!("[Message from {}] {}", message.sender_name, message.content);
    match crate::backends::stdin::send_user_message(
        &orch.process_state,
        run_id,
        &content,
        &session_id,
        None,
        None,
    ) {
        Ok(()) => {
            log::info!(
                "Stdin push: direct message to warm process {}",
                &run_id[..run_id.len().min(8)]
            );
            orch.process_state.transition_to_active(run_id);
        }
        Err(e) => {
            log::warn!("Failed to stdin push to {}: {}", run_id, e);
        }
    }
}

/// Everything still queued for an idle run that justifies a single resume.
///
/// Directs no longer appear here — they ride the attention push queue as
/// `direct:` pushes (CAIRN-1900), drained and stamped by `continue_job_impl` and
/// surfaced to this gate through the `waking_push` check. The flush carries no
/// prompt: `continue_job_impl` claims/injects notices, queued messages, and
/// pushes itself and supplies its own default lead-in.
struct PendingFlush {
    job_id: String,
    /// Count of child side-channel notices pending for the job, claimed and
    /// injected by `continue_job_impl`; kept only so a stranded notice still
    /// triggers a resume.
    notice_count: usize,
    /// Count of queued user messages (CAIRN-1309) pending for the job, claimed
    /// and injected by `continue_job_impl`; kept only so a stranded queued
    /// message still triggers a resume.
    queued_count: usize,
}

/// Peek — without stamping — the directs addressed to `run_id` and the count of
/// child side-channel notices queued for its job, and build the directs-only
/// resume prompt.
///
/// Returns `None` when nothing is pending. That `None` is the guard that keeps
/// flush-on-idle from spuriously resuming a healthy idle run: a resume only
/// happens when there is genuinely stuck content. Peeking (rather than the
/// stamping `claim_*` reads) lets the caller stamp the directs only after the
/// resume succeeds, so a failed resume leaves them pending for the next prompt
/// boundary.
///
/// Side-channel notices intentionally do **not** go into the prompt: every
/// `continue_job_impl` resume claims and injects pending notices for the job,
/// so the flush only needs their presence to decide whether to resume. Building
/// them in here as well would deliver each notice twice.
fn collect_pending_for_flush(db: &LocalDb, run_id: &str) -> Option<PendingFlush> {
    let run_id_owned = run_id.to_string();
    let job_id = run_db_blocking(move || async move {
        Ok(crate::messages::side_channel::job_id_for_run(db, &run_id_owned).await)
    })
    .ok()
    .flatten()?;

    // Passive side-channel notices no longer wake an idle job; they ride along
    // with the next agent-bound payload.
    let notice_count = 0;
    let queued_count =
        crate::messages::queued::peek_waking_pending_count_for_job(db, &job_id).unwrap_or(0);
    // CAIRN-1876: an agent self-suspended on its OWN pending work (a sub-agent
    // task/batch, its own open question/prompt, its own pending permission, or a
    // dependency wait) is NOT idle and must NOT be woken by an external attention
    // push — even a `wake`/`interrupt` one. The push stays queued (undelivered)
    // and rides along on the agent's own-work-resolution resume (the task returns
    // / the question is answered / the permission is decided / the dependency
    // unblocks), which drains the queue then. This guards both external
    // attention-push resume reasons below; directs and queued user messages are a
    // separate delivery mechanism (migrated in a later slice) and keep their
    // existing wake-on-idle behavior. (docs/attention-redesign.md — "Wakeable vs
    // self-suspended".)
    let self_suspended = head_turn_self_suspended_sync(db, &job_id);

    // CAIRN-1881/1900: a pending rousing (`wake`/`interrupt`) push for this job is
    // a reason to resume an *idle* agent — but never a self-suspended one.
    // `passive` pushes ride along on a resume that happens for another reason but
    // never wake an idle agent. Direct messages are now `direct:` pushes, so they
    // are covered here too. This site only decides whether to resume; the render +
    // atomic stamp happen in `continue_job_impl`.
    let waking_push = !self_suspended && {
        let job_id = job_id.clone();
        run_db_blocking(move || async move {
            Ok(
                crate::orchestrator::attention_push::has_pending_waking_live(db, &job_id)
                    .await
                    .unwrap_or(false),
            )
        })
        .unwrap_or(false)
    };

    if notice_count == 0 && queued_count == 0 && !waking_push {
        return None;
    }

    Some(PendingFlush {
        job_id,
        notice_count,
        queued_count,
    })
}

/// Resume a run that has just gone idle to deliver anything still queued for it:
/// child side-channel notices, queued user follow-ups, a deliverable attention
/// briefing, or a rousing attention push — direct messages are now `direct:`
/// pushes (CAIRN-1900). Called from the turn-end sites (warm transition and run
/// finalize), it closes the gap where a recipient's last turn goes idle with no
/// further tool boundary to drain queued content.
///
/// `collect_pending_for_flush` is the guard: it returns `None` for a healthy idle
/// run with nothing stuck and skips a self-suspended agent, so an external push
/// never resumes it. `continue_job_impl` claims, injects, renders, and atomically
/// stamps every pending item (notices, queued messages, pushes); this function
/// only decides whether to resume and carries no prompt of its own.
pub fn flush_pending_directs_on_idle(orch: &Orchestrator, run_id: &str) {
    let Some(pending) = collect_pending_for_flush(&orch.db.local, run_id) else {
        return;
    };

    let short = &pending.job_id[..pending.job_id.len().min(8)];
    match crate::execution::jobs::continue_job_impl(orch, &pending.job_id, None, None, None) {
        Ok(_) => log::info!(
            "flush-on-idle: resumed job {} ({} side-channel notice(s), {} queued user message(s); directs/pushes delivered by the resume)",
            short,
            pending.notice_count,
            pending.queued_count
        ),
        Err(e) => log::warn!("flush-on-idle: failed to resume job {}: {}", short, e),
    }
}

/// Nudge a job after a pending row has been durably persisted according to the
/// delivery urgency ladder. Passive never wakes. Queue/steer wake idle jobs.
/// Interrupt on an active recipient uses the same semantics as manually queueing
/// a message and pressing Stop: stop the active turn and let the normal idle
/// flush claim the already-persisted row and resume exactly once. Idle interrupt
/// recipients still resume immediately.
pub fn nudge_job_for_urgency(
    orch: &Orchestrator,
    job_id: &str,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    if !urgency.wakes_idle() {
        return Ok(());
    }

    let Some(run_id) = latest_run_for_job(&orch.db.local, job_id) else {
        return Ok(());
    };

    let active =
        orch.process_state.is_active(&run_id) || head_turn_active_sync(&orch.db.local, job_id);
    if active && urgency != DeliveryUrgency::Interrupt {
        return Ok(());
    }

    if urgency == DeliveryUrgency::Interrupt && active {
        // Match the validated manual flow: the message is already queued, so
        // Stop the active turn and let the existing turn-end/abort flush be the
        // single owner that resumes with the pending row. Calling
        // continue_job_impl here races that flush and can start a duplicate
        // successor with the generic "Continue where you left off." prompt.
        crate::orchestrator::lifecycle::stop_session(orch, &run_id)?;
        return Ok(());
    }

    flush_pending_directs_on_idle(orch, &run_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso::params;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("delivery-flush.db").await
    }

    /// Seed the minimal project/issue/execution/job/run graph so
    /// `job_id_for_run` resolves `run-1` -> `job-1`. FK constraints are enforced
    /// in the lib-test harness, so the parent rows are required.
    async fn seed_run(db: &LocalDb, run_id: &str, job_id: &str) {
        let run_id = run_id.to_string();
        let job_id = job_id.to_string();
        db.write(|conn| {
            let run_id = run_id.clone();
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Test Project', 'PROJ', '/tmp/test-repo', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                     VALUES ('issue-1', 'proj-1', 42, 'test issue', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
                     VALUES (?1, 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'running', 1, 1)",
                    params![job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                     VALUES (?1, 'proj-1', ?2, 'live', 1, 1)",
                    params![run_id.as_str(), job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    // Seed a pending message-bearing side-channel row directly (the production
    // user→child recorder moved to the attention ledger in CAIRN-1647; the
    // generic claim machinery this exercises is still used by the issue
    // comment / issue-message side channels).
    async fn seed_side_channel_notice(db: &LocalDb, job_id: &str, child_uri: &str, content: &str) {
        let rendered =
            format!("[Side-channel] the user messaged your child {child_uri}:\n{content}");
        let job_id = job_id.to_string();
        let child_uri = child_uri.to_string();
        db.write(move |conn| {
            let job_id = job_id.clone();
            let child_uri = child_uri.clone();
            let rendered = rendered.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO suppressed_wakes
                       (id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                        occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at)
                     VALUES (?1, NULL, ?2, 'issue', ?3, 'message', 1, ?3, ?4, 1, 1, NULL)",
                    params![
                        uuid::Uuid::new_v4().to_string(),
                        job_id.as_str(),
                        child_uri.as_str(),
                        rendered.as_str()
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Replace the job's head turn with one of the given `state` / `yield_reason`
    /// so `head_turn_self_suspended` reads a single, deterministic head turn.
    async fn set_head_turn(db: &LocalDb, job_id: &str, state: &str, reason: Option<&str>) {
        let job_id = job_id.to_string();
        let state = state.to_string();
        let reason = reason.map(|r| r.to_string());
        db.write(move |conn| {
            let job_id = job_id.clone();
            let state = state.clone();
            let reason = reason.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM turns WHERE job_id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at)
                     VALUES ('t-head', 'sess', 'run-1', ?1, 1, ?2, ?3, 'initial', 1, 1)",
                    params![job_id.as_str(), state.as_str(), reason.as_deref()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Seed a live rousing (`wake`) push addressed to `recipient` (the job id).
    /// The `resolved:` prefix is informational, so `lazy_resolve_live` keeps it
    /// live regardless of referent state — it is a pending rousing push for the
    /// gate to weigh.
    async fn seed_waking_push(db: &LocalDb, recipient: &str) {
        crate::orchestrator::attention_push::push(
            db,
            recipient,
            "cairn://p/PROJ/42",
            crate::orchestrator::attention_push::Wake::Wake,
            crate::orchestrator::attention_push::Boundary::Event,
            "resolved:cairn://p/PROJ/42",
        )
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn head_turn_self_suspended_classifies_yield_reasons() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;

        // No head turn at all -> not self-suspended (a fresh/idle job).
        assert!(!head_turn_self_suspended(&db, "job-1").await.unwrap());

        // All three yielded waits are self-suspends (own task/dependency,
        // own question, own permission).
        for reason in ["dependency_wait", "user_input", "permission"] {
            set_head_turn(&db, "job-1", "yielded", Some(reason)).await;
            assert!(
                head_turn_self_suspended(&db, "job-1").await.unwrap(),
                "yielded/{reason} must read as self-suspended"
            );
        }

        // A completed head turn is the normal idle case, not self-suspended.
        set_head_turn(&db, "job-1", "complete", None).await;
        assert!(!head_turn_self_suspended(&db, "job-1").await.unwrap());

        // A stopped (interrupted) head turn is idle/terminal, not self-suspended.
        set_head_turn(&db, "job-1", "interrupted", None).await;
        assert!(!head_turn_self_suspended(&db, "job-1").await.unwrap());

        // A running head turn is active, not self-suspended.
        set_head_turn(&db, "job-1", "running", None).await;
        assert!(!head_turn_self_suspended(&db, "job-1").await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_does_not_resume_self_suspended_agent_with_waking_push() {
        // The core CAIRN-1876 rule: an agent suspended waiting on its own
        // sub-agent task (yielded/dependency_wait) is NOT resumed by a pending
        // rousing push, and the push stays queued to ride along later.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_waking_push(&db, "job-1").await;
        set_head_turn(&db, "job-1", "yielded", Some("dependency_wait")).await;

        assert!(
            collect_pending_for_flush(&db, "run-1").is_none(),
            "a self-suspended agent must not be resumed by a pending wake push"
        );

        // Ride-along: the push remains undelivered, queued for the
        // own-work-resolution drain.
        let still_pending = crate::orchestrator::attention_push::list_pending(&db, "job-1")
            .await
            .unwrap();
        assert_eq!(
            still_pending.len(),
            1,
            "the gated wake push stays queued to ride along on the next resume"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_resumes_idle_agent_with_waking_push() {
        // The contrast: a genuinely idle agent (head turn completed, nothing of
        // its own pending) with the same pending wake push IS resumed.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_waking_push(&db, "job-1").await;
        set_head_turn(&db, "job-1", "complete", None).await;

        let pending = collect_pending_for_flush(&db, "run-1")
            .expect("an idle agent with a pending wake push must resume");
        assert_eq!(pending.job_id, "job-1");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_returns_none_when_nothing_queued() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        assert!(collect_pending_for_flush(&db, "run-1").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_returns_none_for_unknown_run() {
        let db = migrated_db().await;
        // No run row -> job can't be resolved -> nothing to flush, no panic.
        assert!(collect_pending_for_flush(&db, "ghost").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_does_not_resume_for_passive_notice_only() {
        // Passive side-channel notices do not wake idle jobs by themselves. They
        // remain pending until the next agent-bound payload/resume.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_side_channel_notice(
            &db,
            "job-1",
            "cairn://p/P/9/1/child",
            "user pinged the child",
        )
        .await;

        assert!(
            collect_pending_for_flush(&db, "run-1").is_none(),
            "passive side-channel notices must not trigger flush-on-idle resumes"
        );

        // Peek did not stamp -> the notice is still claimable when a later
        // agent-bound payload/resume happens.
        let still_pending =
            crate::messages::side_channel::claim_pending_side_channel_for_job_async(&db, "job-1")
                .await
                .unwrap();
        assert_eq!(still_pending.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_resumes_for_stranded_queued_message_but_keeps_prompt_empty() {
        // A queued user message (CAIRN-1309) with no directs still triggers a
        // resume (Some), but the prompt stays None: continue_job_impl claims and
        // injects queued messages itself, so the flush must not build them in.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        crate::messages::queued::enqueue_async(
            &db,
            "job-1",
            "please also add a test",
            crate::messages::queued::Delivery::Queue,
        )
        .await
        .unwrap();

        let pending = collect_pending_for_flush(&db, "run-1")
            .expect("a queued user message should trigger a resume");
        assert_eq!(pending.job_id, "job-1");
        assert_eq!(pending.queued_count, 1);

        // Peek did not stamp -> the message is still claimable by the resume.
        let still_pending = crate::messages::queued::claim_all_for_job_async(&db, "job-1")
            .await
            .unwrap();
        assert_eq!(still_pending.len(), 1);
    }
}
