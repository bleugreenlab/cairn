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

/// Persist a system-originated direct message addressed to `recipient_run_id`
/// with `delivered_at = NULL` so it's picked up at the recipient's next
/// prompt boundary (Claude hook additionalContext or Cairn tool-result
/// augmentation). Used by orchestrator-internal callers (PR conflict
/// resolution, etc.) that today silently drop the notification when the
/// recipient is mid-turn.
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
            // CAIRN-1196: defensive. Today's callers of `delivery::deliver`
            // only insert channel messages, so this branch is unreachable
            // for the `ChannelType::Direct` path — the active warm-stdin
            // push for direct messages goes through `continue_job_impl` and
            // the matching `mark_direct_delivered` lives in
            // `mcp/handlers/messages.rs::append_direct_message`. The stamp
            // here is correct in case future routing adds Direct through
            // this path.
            if message.channel_type == ChannelType::Direct {
                if let Err(e) = msg_db::mark_direct_delivered(&orch.db.local, &message.id) {
                    log::warn!(
                        "Failed to stamp delivered_at after warm stdin push for {}: {}",
                        message.id,
                        e
                    );
                }
            }
        }
        Err(e) => {
            log::warn!("Failed to stdin push to {}: {}", run_id, e);
        }
    }
}

/// Everything still queued for an idle run, packaged for a single resume.
struct PendingFlush {
    job_id: String,
    /// Directs addressed to the now-idle run. They are carried into the resume
    /// prompt and stamped delivered only after a successful resume.
    directs: Vec<Message>,
    /// Count of child side-channel notices pending for the job. These are *not*
    /// built into `prompt` and the flush never stamps them: `continue_job_impl`
    /// already claims and injects pending notices into every resume prompt
    /// itself, so building them in here too would double-deliver. The count is
    /// kept only so a stranded notice (with no directs) still triggers a resume.
    notice_count: usize,
    /// Count of queued user messages (CAIRN-1309) pending for the job. Like
    /// `notice_count`, these are claimed and injected by `continue_job_impl`
    /// itself on every resume, so the flush never builds or stamps them — the
    /// count only ensures a stranded queued message still triggers a resume.
    queued_count: usize,
    /// Resume prompt carrying the directs, or `None` when only side-channel
    /// notices / queued messages are pending — `continue_job_impl` then supplies
    /// its own default message plus the injected blocks.
    prompt: Option<String>,
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

    let directs = msg_db::peek_pending_directs_for_run(db, run_id).unwrap_or_default();
    // Passive side-channel notices no longer wake an idle job; they ride along
    // with the next agent-bound payload.
    let notice_count = 0;
    let queued_count =
        crate::messages::queued::peek_waking_pending_count_for_job(db, &job_id).unwrap_or(0);
    // CAIRN-1647: a pending attention evaluation whose deliverable set is
    // non-empty is a reason to resume, exactly like a stuck direct. An empty
    // set is not (the stale-wake drop). continue_job_impl renders the briefing.
    let eval_briefing =
        crate::orchestrator::attention_delivery::has_deliverable_briefing(db, &job_id);

    if directs.is_empty() && notice_count == 0 && queued_count == 0 && !eval_briefing {
        return None;
    }

    let prompt = if directs.is_empty() {
        None
    } else {
        let parts: Vec<String> = directs
            .iter()
            .map(crate::messages::render::render_direct_message)
            .collect();
        Some(parts.join("\n\n"))
    };

    Some(PendingFlush {
        job_id,
        directs,
        notice_count,
        queued_count,
        prompt,
    })
}

/// Deliver any directs / child side-channel notices still pending for a run that
/// has just gone idle.
///
/// The queue-on-active model (CAIRN-1196) leaves a mid-turn recipient's directs
/// `delivered_at = NULL` for the *next* prompt boundary to claim. When that
/// active turn is the run's last — it finishes its message and goes idle with no
/// further tool call — no boundary fires and the queued direct would sit
/// unclaimed until some unrelated future turn. Called from the turn-end sites
/// (warm transition and run finalize), this closes that gap: it peeks for
/// anything still pending and, if so, resumes the job to deliver it — the same
/// cold resume `queue_or_resume_parent` would have taken had the recipient
/// already been idle when the direct arrived.
///
/// Division of labour with `continue_job_impl`: the flush carries the directs in
/// the resume prompt and stamps them delivered after the resume succeeds.
/// Side-channel notices are claimed and injected by `continue_job_impl` itself
/// on every resume, so the flush never builds or stamps them — it only counts
/// normal side-channel notices so a stranded live notice still triggers a resume.
/// Suppressed wake digest rows are intentionally not counted here: they must ride
/// along only when some non-suppressed wake/resume already happens.
///
/// Race-free against the boundary claim: peek-then-resume-then-stamp means the
/// directs are stamped only after `continue_job_impl` succeeds. If a following
/// tool boundary claimed them first (the atomic SELECT+UPDATE in
/// [`msg_db::claim_pending_directs_for_run`]), this peek finds nothing and no
/// resume happens; if this flush stamps first, the boundary claim finds nothing.
/// Either way the run is woken at most once for the same content, and a healthy
/// mid-turn agent that makes another tool call is never spuriously resumed.
pub fn flush_pending_directs_on_idle(orch: &Orchestrator, run_id: &str) {
    let Some(pending) = collect_pending_for_flush(&orch.db.local, run_id) else {
        return;
    };

    let short = &pending.job_id[..pending.job_id.len().min(8)];
    match crate::execution::jobs::continue_job_impl(
        orch,
        &pending.job_id,
        pending.prompt.as_deref(),
        None,
        None,
    ) {
        Ok(_) => {
            // Directs are addressed to the now-idle run, not the run the resumed
            // turn uses, so no prompt boundary will ever claim them — stamp them
            // here. Side-channel notices were already claimed and injected by
            // continue_job_impl, so the flush must not touch them.
            for msg in &pending.directs {
                if let Err(e) = msg_db::mark_direct_delivered(&orch.db.local, &msg.id) {
                    log::warn!(
                        "flush-on-idle: failed to mark direct {} delivered: {}",
                        msg.id,
                        e
                    );
                }
            }
            log::info!(
                "flush-on-idle: resumed job {} to deliver {} queued direct(s) ({} side-channel notice(s), {} queued user message(s) delivered by the resume)",
                short,
                pending.directs.len(),
                pending.notice_count,
                pending.queued_count
            );
        }
        Err(e) => log::warn!(
            "flush-on-idle: failed to resume job {} to deliver queued directs: {}",
            short,
            e
        ),
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
    use crate::models::ChannelType;
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

    async fn seed_suppressed_wake(db: &LocalDb, job_id: &str, child_uri: &str, content: &str) {
        crate::orchestrator::wakes::record_suppressed_message(
            db,
            job_id,
            "issue",
            Some(child_uri),
            content,
        )
        .await
        .unwrap();
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
    async fn collect_pending_packages_directs_without_stamping() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        msg_db::insert_message_async(
            &db,
            &ChannelType::Direct,
            None,
            Some("sender-run"),
            "planner",
            Some("run-1"),
            "the child is blocked on a question",
            None,
        )
        .await
        .unwrap();

        let pending =
            collect_pending_for_flush(&db, "run-1").expect("a queued direct should be pending");
        assert_eq!(pending.job_id, "job-1");
        assert_eq!(pending.directs.len(), 1);
        assert_eq!(pending.notice_count, 0);
        let prompt = pending
            .prompt
            .as_deref()
            .expect("directs present -> prompt built");
        assert!(prompt.contains("[Direct message from planner]"));
        assert!(prompt.contains("the child is blocked on a question"));

        // collect peeks; it must not stamp -> the direct is still claimable.
        let still_pending = msg_db::claim_pending_directs_for_run_async(&db, "run-1")
            .await
            .unwrap();
        assert_eq!(
            still_pending.len(),
            1,
            "collect_pending_for_flush must not stamp delivered_at"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_ignores_suppressed_wakes_without_live_content() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_suppressed_wake(
            &db,
            "job-1",
            "cairn://p/P/9/1/child",
            "suppressed child churn",
        )
        .await;

        assert!(
            collect_pending_for_flush(&db, "run-1").is_none(),
            "suppressed digest rows must not trigger flush-on-idle resumes"
        );
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
        assert!(pending.directs.is_empty());
        assert_eq!(pending.queued_count, 1);
        assert!(
            pending.prompt.is_none(),
            "queued messages are delivered by continue_job_impl, not built into the flush prompt"
        );

        // Peek did not stamp -> the message is still claimable by the resume.
        let still_pending = crate::messages::queued::claim_all_for_job_async(&db, "job-1")
            .await
            .unwrap();
        assert_eq!(still_pending.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_excludes_side_channel_from_prompt_when_directs_present() {
        // With both a waking direct and a passive side-channel notice pending,
        // the prompt carries only the direct. The notice is not counted for
        // flush-on-idle and never appears in the prompt; it rides along through
        // continue_job_impl if the direct-driven resume succeeds.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        msg_db::insert_message_async(
            &db,
            &ChannelType::Direct,
            None,
            Some("sender-run"),
            "planner",
            Some("run-1"),
            "direct content",
            None,
        )
        .await
        .unwrap();
        seed_side_channel_notice(
            &db,
            "job-1",
            "cairn://p/P/9/1/child",
            "side channel content",
        )
        .await;

        let pending = collect_pending_for_flush(&db, "run-1").expect("both items are pending");
        assert_eq!(pending.directs.len(), 1);
        assert_eq!(pending.notice_count, 0);
        let prompt = pending
            .prompt
            .as_deref()
            .expect("directs present -> prompt built");
        assert!(prompt.contains("direct content"));
        assert!(
            !prompt.contains("[Side-channel]"),
            "side-channel content must not be double-built into the flush prompt"
        );
        assert!(!prompt.contains("side channel content"));
    }
}
