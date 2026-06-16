//! Message-related MCP handlers.
//!
//! Handles: message (send a message to a channel or direct to an agent)

use serde::Deserialize;

use crate::execution::jobs::continue_job_impl;
use crate::jobs::queries::{node_uri_segment_for_job, parent_uri_segment_for_job};
use crate::mcp::types::McpCallbackRequest;
use crate::messages::{db as msg_db, delivery};
use crate::models::{ChannelType, ExternalReplyMode, IssueAttention, IssueStatus};

use crate::orchestrator::attention::{AttentionEvent, AttentionFact, ExternalMessageReplyContent};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_common::uri::{build_job_base_uri, build_node_uri, parse_uri, CairnResource};
use turso::params;

/// Is the recipient run currently mid-turn?
///
/// Two independent signals matter (CAIRN-1196):
/// - **In-memory occupancy** on the live `RunHandle` (`is_active()`): the
///   process is `ServingTurn`, `AwaitingHost`, or `Busy`. Catches the case
///   the issue is named after (recipient is mid-tool-call right now).
/// - **Head turn state** in the DB: `pending` or `running`. Catches recipients
///   whose process handle isn't live yet (cold) but a turn is already in
///   flight — e.g. the very narrow window between turn creation and process
///   spawn, or a recipient whose runtime is in a different host.
///
/// Either signal returning true is enough to queue. Both being false means it
/// is safe to try the normal `continue_job_impl` resume.
async fn recipient_mid_turn(
    orch: &Orchestrator,
    recipient_run_id: &str,
    recipient_job_id: &str,
) -> bool {
    if orch.process_state.is_active(recipient_run_id) {
        return true;
    }
    delivery::head_turn_active(&orch.db.local, recipient_job_id)
        .await
        .unwrap_or(false)
}

async fn sender_name_for_run(db: &LocalDb, run_ctx: &super::RunContext) -> Result<String, String> {
    let node_name = run_ctx.job_name.as_deref().unwrap_or("unknown");
    if let Some(issue_number) = run_ctx.issue_number {
        let node_segment = node_uri_segment_for_job(db, &run_ctx.job_id)
            .await
            .unwrap_or_else(|| node_name.to_string());
        // Sub-task senders nest under their parent node as
        // `.../{seq}/{parent}/task/{segment}`. Without the parent join the
        // recorded sender_name was the broken top-level shape, which the
        // reply-to hint on a DM then echoed back — every reply to a sub-task
        // hit "No agent found" because the addressed URI was unreachable.
        let parent_segment = parent_uri_segment_for_job(db, &run_ctx.job_id).await;
        Ok(build_job_base_uri(
            &run_ctx.project_key,
            issue_number,
            run_ctx.exec_seq.unwrap_or(1),
            &node_segment,
            parent_segment.as_deref(),
        ))
    } else {
        Ok(node_name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_message_target_accepts_external_literal() {
        assert!(matches!(
            parse_message_target("external").unwrap(),
            MessageTarget::External
        ));
    }
}

async fn sender_context(
    db: &LocalDb,
    request: &McpCallbackRequest,
) -> Result<(Option<String>, String), String> {
    let Some(_) = request.run_id.as_ref() else {
        return Ok((None, "external".to_string()));
    };

    let run_ctx = super::run_context::lookup_run(db, request).await?;
    let sender_name = sender_name_for_run(db, &run_ctx).await?;

    Ok((Some(run_ctx.run_id), sender_name))
}

async fn resolve_channel_id(
    db: &LocalDb,
    project_key: &str,
    issue_number: Option<i32>,
) -> Result<String, String> {
    let requested_key = project_key.to_string();
    let lookup_key = requested_key.to_uppercase();
    db.read(|conn| {
        let lookup_key = lookup_key.clone();
        let requested_key = requested_key.clone();
        Box::pin(async move {
            let mut project_rows = conn
                .query(
                    "SELECT id, key FROM projects WHERE key = ?1 LIMIT 1",
                    params![lookup_key.as_str()],
                )
                .await?;

            let Some(project_row) = project_rows.next().await? else {
                return Err(DbError::Row(format!(
                    "No project found with key '{}'",
                    requested_key
                )));
            };
            let project_id = project_row.text(0)?;
            let canonical_key = project_row.text(1)?;

            if let Some(number) = issue_number {
                let mut issue_rows = conn
                    .query(
                        "SELECT id FROM issues WHERE project_id = ?1 AND number = ?2 LIMIT 1",
                        params![project_id.as_str(), number],
                    )
                    .await?;

                if issue_rows.next().await?.is_none() {
                    return Err(DbError::Row(format!(
                        "Issue {}-{} not found",
                        canonical_key, number
                    )));
                }
                Ok(format!("{}/{}", canonical_key, number))
            } else {
                Ok(canonical_key)
            }
        })
    })
    .await
    .map_err(|e| match e {
        DbError::Row(message) => message,
        other => other.to_string(),
    })
}

/// Resolve the recipient job + its latest run for a direct message.
///
/// `task_name = None` targets the top-level node job (`uri_segment = node_name`,
/// no parent). `task_name = Some(..)` targets the sub-agent task job nested under
/// that node (`uri_segment = task_name`, `parent_job_id` = the node job). A job is
/// a job — both node agents and task agents are addressable recipients.
async fn find_recipient_job(
    db: &LocalDb,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
) -> Result<Option<(String, String)>, String> {
    let lookup_key = project_key.to_uppercase();
    let node_name = node_name.to_string();
    let task_name = task_name.map(str::to_string);
    db.read(|conn| {
        let lookup_key = lookup_key.clone();
        let node_name = node_name.clone();
        let task_name = task_name.clone();
        Box::pin(async move {
            let mut issue_rows = conn
                .query(
                    "
                    SELECT i.id
                    FROM issues i
                    JOIN projects p ON i.project_id = p.id
                    WHERE p.key = ?1 AND i.number = ?2
                    LIMIT 1
                    ",
                    params![lookup_key.as_str(), issue_number],
                )
                .await?;

            let Some(issue_row) = issue_rows.next().await? else {
                return Ok(None);
            };
            let issue_id = issue_row.text(0)?;

            let mut execution_rows = conn
                .query(
                    "
                    SELECT id
                    FROM executions
                    WHERE issue_id = ?1 AND seq = ?2
                    LIMIT 1
                    ",
                    params![issue_id.as_str(), exec_seq],
                )
                .await?;

            let Some(execution_row) = execution_rows.next().await? else {
                return Ok(None);
            };
            let execution_id = execution_row.text(0)?;

            // A task agent nests under its node (parent scoping disambiguates a
            // task from a node that happens to share a segment); a node agent is
            // top-level (`parent_job_id IS NULL`).
            let mut candidates = match &task_name {
                Some(task) => {
                    conn.query(
                        "
                        SELECT j.id, r.id
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        JOIN jobs p ON j.parent_job_id = p.id
                        WHERE j.issue_id = ?1 AND j.execution_id = ?2
                          AND j.uri_segment = ?3 AND p.uri_segment = ?4
                        ORDER BY r.created_at DESC
                        LIMIT 1
                        ",
                        params![
                            issue_id.as_str(),
                            execution_id.as_str(),
                            task.as_str(),
                            node_name.as_str()
                        ],
                    )
                    .await?
                }
                None => {
                    conn.query(
                        "
                        SELECT j.id, r.id
                        FROM runs r
                        JOIN jobs j ON r.job_id = j.id
                        WHERE j.issue_id = ?1 AND j.execution_id = ?2
                          AND j.uri_segment = ?3 AND j.parent_job_id IS NULL
                        ORDER BY r.created_at DESC
                        LIMIT 1
                        ",
                        params![issue_id.as_str(), execution_id.as_str(), node_name.as_str()],
                    )
                    .await?
                }
            };

            candidates
                .next()
                .await?
                .map(|row| Ok((row.text(0)?, row.text(1)?)))
                .transpose()
        })
    })
    .await
    .map_err(|e| e.to_string())
}

pub async fn append_project_or_issue_message(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    issue_number: Option<i32>,
    content: &str,
) -> Result<String, String> {
    if content.is_empty() {
        return Err("Message content cannot be empty".to_string());
    }

    let channel_id = resolve_channel_id(&orch.db.local, project_key, issue_number).await?;
    let (sender_run_id, sender_name) = sender_context(&orch.db.local, request).await?;

    let (channel_type, success_message) = match issue_number {
        Some(number) => (
            ChannelType::Issue,
            format!(
                "Appended message to issue channel {}-{}",
                channel_id
                    .split('/')
                    .next()
                    .unwrap_or(project_key)
                    .to_uppercase(),
                number
            ),
        ),
        None => (
            ChannelType::Project,
            format!("Appended message to project channel {}", channel_id),
        ),
    };

    let msg = msg_db::insert_message(
        &orch.db.local,
        &channel_type,
        Some(channel_id.as_str()),
        sender_run_id.as_deref(),
        &sender_name,
        None,
        content,
    )
    .map_err(|e| format!("Failed to send message: {e}"))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );

    delivery::deliver(orch, &msg);

    if let Some(number) = issue_number {
        let exclude_job_id = super::run_context::lookup_run(&orch.db.local, request)
            .await
            .ok()
            .map(|ctx| ctx.job_id);
        let source = if sender_run_id.is_some() {
            "agent"
        } else {
            "user"
        };
        if let Err(error) =
            crate::messages::side_channel::record_issue_message_side_channel_by_issue_number(
                orch,
                project_key,
                number,
                source,
                content,
                exclude_job_id.as_deref(),
            )
            .await
        {
            log::warn!("failed to record issue message side-channel notices: {error}");
        }
    }

    Ok(success_message)
}

async fn touch_issue_for_external_reply(
    db: &LocalDb,
    project_key: &str,
    issue_number: i32,
) -> Result<(String, IssueAttention, IssueStatus, i64), String> {
    let lookup_key = project_key.to_uppercase();
    db.write(|conn| {
        let lookup_key = lookup_key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.id, i.attention, i.status, i.updated_at
                     FROM issues i
                     JOIN projects p ON i.project_id = p.id
                     WHERE p.key = ?1 AND i.number = ?2
                     LIMIT 1",
                    params![lookup_key.as_str(), issue_number],
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                DbError::Row(format!("Issue {lookup_key}-{issue_number} not found"))
            })?;
            let issue_id = row.text(0)?;
            let attention = row
                .text(1)?
                .parse::<IssueAttention>()
                .unwrap_or(IssueAttention::None);
            let status = row
                .text(2)?
                .parse::<IssueStatus>()
                .unwrap_or(IssueStatus::Active);
            let current_updated_at = row.opt_i64(3)?.unwrap_or(0);
            drop(rows);

            let now = chrono::Utc::now().timestamp();
            let updated_at = std::cmp::max(current_updated_at + 1, now);
            conn.execute(
                "UPDATE issues SET updated_at = ?1 WHERE id = ?2",
                params![updated_at, issue_id.as_str()],
            )
            .await?;

            Ok::<_, DbError>((issue_id, attention, status, updated_at))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn append_external_reply(
    orch: &Orchestrator,
    run_ctx: &super::RunContext,
    sender_name: &str,
    content: &str,
) -> Result<String, String> {
    let Some(issue_number) = run_ctx.issue_number else {
        return Err("External replies require an issue-associated run".to_string());
    };

    match orch.get_settings().external_replies {
        ExternalReplyMode::Disabled => {
            return Ok("External replies are disabled; message not delivered".to_string());
        }
        ExternalReplyMode::Watchers => {}
    }

    let issue_key =
        resolve_channel_id(&orch.db.local, &run_ctx.project_key, Some(issue_number)).await?;
    let msg = msg_db::insert_message(
        &orch.db.local,
        &ChannelType::Issue,
        Some(&issue_key),
        Some(&run_ctx.run_id),
        sender_name,
        None,
        content,
    )
    .map_err(|e| format!("Failed to send external reply: {e}"))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );

    delivery::deliver(orch, &msg);

    if let Err(error) =
        crate::messages::side_channel::record_issue_message_side_channel_by_issue_number(
            orch,
            &run_ctx.project_key,
            issue_number,
            "agent",
            content,
            Some(&run_ctx.job_id),
        )
        .await
    {
        log::warn!("failed to record external reply side-channel notices: {error}");
    }

    let (issue_id, attention, status, updated_at) =
        touch_issue_for_external_reply(&orch.db.local, &run_ctx.project_key, issue_number).await?;
    if let Err(error) =
        msg_db::stamp_message_delivered_at(&orch.db.local, &msg.id, updated_at).await
    {
        log::warn!(
            "failed to stamp external reply marker on message {}: {}",
            msg.id,
            error
        );
    }
    let issue_uri = format!(
        "cairn://p/{}/{}",
        run_ctx.project_key.to_uppercase(),
        issue_number
    );
    let detail_uri = format!("{issue_uri}/messages");
    orch.emit_attention_event(AttentionEvent {
        issue_id,
        issue_uri,
        fact: AttentionFact::ExternalMessageReply {
            escalate: false,
            detail_uri,
            message_id: msg.id,
            content: ExternalMessageReplyContent {
                sender: sender_name.to_string(),
                body: content.to_string(),
            },
        },
        attention,
        status,
        updated_at,
    });

    Ok(format!(
        "Sent external reply to watchers of {}-{}",
        run_ctx.project_key.to_uppercase(),
        issue_number
    ))
}

#[allow(clippy::too_many_arguments)]
pub async fn append_direct_message(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    content: &str,
    escalate: bool,
) -> Result<String, String> {
    append_direct_message_with_urgency(
        orch,
        request,
        project_key,
        issue_number,
        exec_seq,
        node_name,
        task_name,
        content,
        escalate,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn append_direct_message_with_urgency(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    content: &str,
    escalate: bool,
    payload_urgency: Option<crate::messages::queued::DeliveryUrgency>,
) -> Result<String, String> {
    if content.is_empty() {
        return Err("Message content cannot be empty".to_string());
    }

    // Human-readable recipient for success/error messages: "node" or "node/task".
    let recipient_label = match task_name {
        Some(task) => format!("{}/{}", node_name, task),
        None => node_name.to_string(),
    };

    // Echo the canonical URI the caller addressed in any not-found error so a
    // wrong-URI miss is debuggable without rebuilding the URI by hand. For
    // sub-task targets (task_name = Some), the addressed URI is
    // .../{exec}/{node}/task/{task}; for top-level nodes it's .../{exec}/{node}.
    let (addressed_uri, scope_hint) = match task_name {
        Some(task) => (
            build_job_base_uri(project_key, issue_number, exec_seq, task, Some(node_name)),
            format!(
                "no sub-task with uri_segment '{}' under parent '{}' in execution {}",
                task, node_name, exec_seq
            ),
        ),
        None => (
            build_node_uri(project_key, issue_number, exec_seq, node_name),
            format!(
                "no top-level node with uri_segment '{}' in execution {}",
                node_name, exec_seq
            ),
        ),
    };

    let (sender_run_id, sender_name) = sender_context(&orch.db.local, request).await?;
    let (job_id, recipient_run_id) = find_recipient_job(
        &orch.db.local,
        project_key,
        issue_number,
        exec_seq,
        node_name,
        task_name,
    )
    .await?
    .ok_or_else(|| format!("{} not found ({}).", addressed_uri, scope_hint))?;

    crate::orchestrator::wakes::seed_default_job_subscriptions(&orch.db.local, &job_id).await?;

    let urgency = if escalate {
        crate::messages::queued::DeliveryUrgency::Interrupt
    } else {
        payload_urgency.unwrap_or(crate::messages::queued::DeliveryUrgency::Steer)
    };
    let msg = msg_db::insert_message_with_urgency(
        &orch.db.local,
        &ChannelType::Direct,
        None,
        sender_run_id.as_deref(),
        &sender_name,
        Some(&recipient_run_id),
        content,
        Some(urgency),
    )
    .map_err(|e| format!("Failed to send message: {e}"))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );

    let mut formatted = crate::messages::render::render_direct_message(&msg);
    let wake_source = if request.run_id.is_none() {
        crate::orchestrator::wakes::WakeSource::User
    } else {
        crate::orchestrator::wakes::WakeSource::Peer {
            reference: Some(sender_name.clone()),
        }
    };
    match crate::orchestrator::wakes::route_wake(
        orch,
        crate::orchestrator::wakes::WakeEvent {
            source: wake_source.clone(),
            fact_kind: "message".to_string(),
            detail_uri: Some(addressed_uri.clone()),
            delivery: crate::orchestrator::wakes::WakeDelivery::MessageDigest {
                subscriber_job_id: job_id.clone(),
                content: formatted.clone(),
            },
            urgency,
        },
    )
    .await?
    {
        crate::orchestrator::wakes::WakeRouteAction::Delivered => {}
        crate::orchestrator::wakes::WakeRouteAction::Suppressed => {
            if let Err(e) = msg_db::mark_direct_delivered(&orch.db.local, &msg.id) {
                log::warn!(
                    "Failed to stamp suppressed direct message {} delivered: {}",
                    msg.id,
                    e
                );
            }
            return Ok(format!(
                "Suppressed direct message to {} under its wake subscriptions",
                recipient_label
            ));
        }
        crate::orchestrator::wakes::WakeRouteAction::Dropped => {
            if let Err(e) = msg_db::mark_direct_delivered(&orch.db.local, &msg.id) {
                log::warn!(
                    "Failed to stamp dropped direct message {} delivered: {}",
                    msg.id,
                    e
                );
            }
            return Ok(format!(
                "Stored direct message to {}, but no wake subscription matched",
                recipient_label
            ));
        }
    }

    if request.run_id.is_none() {
        if let Err(error) =
            crate::messages::side_channel::record_user_child_side_channel_by_issue_number(
                orch,
                project_key,
                issue_number,
                &addressed_uri,
                content,
            )
            .await
        {
            log::warn!(
                "failed to record user→child side-channel notice for {}: {}",
                addressed_uri,
                error
            );
        }
    }

    // CAIRN-1196: if the recipient is mid-turn, don't try to resume — leave the
    // message in the queue (delivered_at IS NULL) and let one of the two
    // injection paths pick it up at the recipient's next prompt boundary:
    //   - Claude path: handle_hook's additionalContext on PostToolUse /
    //     PostToolUseFailure / UserPromptSubmit
    //   - Codex/Claude shared path: tool-result augmentation on the
    //     synchronous Cairn MCP tool response
    // Both call msg_db::claim_pending_directs_for_run, which atomically stamps
    // delivered_at so neither double-delivers.
    if recipient_mid_turn(orch, &recipient_run_id, &job_id).await {
        if urgency == crate::messages::queued::DeliveryUrgency::Interrupt {
            // Match queued follow-up interrupt semantics: the direct row is
            // durable, so Stop the active turn and let the existing idle flush
            // claim and deliver it exactly once.
            if let Err(error) =
                crate::orchestrator::lifecycle::stop_session(orch, &recipient_run_id)
            {
                log::warn!(
                    "Failed to stop interrupted recipient {}: {}",
                    recipient_label,
                    error
                );
            }
            return Ok(format!(
                "Interrupted {}; queued direct message will deliver on resume",
                recipient_label
            ));
        }
        log::info!(
            "Direct message to {} queued for next prompt boundary (recipient mid-turn)",
            recipient_label
        );
        return Ok(format!(
            "Queued for delivery to {} \u{2014} will appear at their next prompt boundary",
            recipient_label
        ));
    }

    let digest_preview =
        crate::orchestrator::wakes::peek_claimable_suppressed_for_job_with_live_source(
            &orch.db.local,
            &job_id,
            Some(&wake_source),
        )
        .await?;
    if !digest_preview.is_empty() {
        formatted.push_str("\n\n");
        formatted.push_str(
            &crate::orchestrator::wakes::SuppressedWake::render_digest_with_context(
                &digest_preview,
                Some(&wake_source),
            ),
        );
    }

    match continue_job_impl(orch, &job_id, Some(&formatted), None, None) {
        Ok(_run) => {
            // The resume path delivered the message via stdin (warm) or the
            // resume prompt (cold). Stamp delivered_at so the queued-injection
            // paths don't re-deliver it on the next boundary, then claim/lift
            // any wake digest that was rendered into that successful resume.
            if let Err(e) = msg_db::mark_direct_delivered(&orch.db.local, &msg.id) {
                log::warn!(
                    "Failed to stamp delivered_at on resumed direct message {}: {}",
                    msg.id,
                    e
                );
            }
            if !digest_preview.is_empty() {
                if let Err(e) = crate::orchestrator::wakes::claim_suppressed_wake_preview(
                    &orch.db.local,
                    &job_id,
                    &digest_preview,
                )
                .await
                {
                    log::warn!(
                        "Failed to claim suppressed wake digest after successful direct resume to {}: {}",
                        recipient_label,
                        e
                    );
                }
            }
            Ok(format!("Sent direct message to {}", recipient_label))
        }
        Err(e) => {
            // The recipient's process state said it was idle but the turn
            // machinery rejected the resume — most likely a race where a turn
            // was created between our check and continue_job_impl. The message
            // stays queued and will be picked up by the injection paths.
            log::warn!(
                "Direct message to {} stored as queued after resume failure: {}",
                recipient_label,
                e
            );
            Ok(format!(
                "Queued for delivery to {} \u{2014} will appear at their next prompt boundary",
                recipient_label
            ))
        }
    }
}

// ============================================================================
// Payload Types
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagePayload {
    /// Message content
    pub content: String,
    /// Target cairn:// URI. Determines scope:
    /// cairn://PROJECT → project, cairn://PROJECT/NUMBER → issue,
    /// cairn://PROJECT/NUMBER/EXEC/NODE → direct.
    /// Omit for project channel.
    pub to: Option<String>,
    /// Force a direct message wake through a muted matching subscription.
    #[serde(default)]
    pub escalate: bool,
    /// Delivery urgency for direct messages. Defaults to steer.
    pub urgency: Option<crate::messages::queued::DeliveryUrgency>,
}

/// Parsed target from a cairn:// URI.
enum MessageTarget {
    /// Special reply target for external drivers watching the issue.
    External,
    /// Project channel: channel_type=project, channel_id=project_key
    Project { project_key: String },
    /// Issue channel: channel_type=issue, channel_id=KEY/NUMBER (e.g. "CRN/40")
    Issue {
        project_key: String,
        issue_number: i32,
    },
    /// Direct to a specific agent (identified by run_id lookup). `task_name`
    /// distinguishes a sub-agent task recipient from a top-level node agent.
    Direct {
        project_key: String,
        issue_number: i32,
        exec_seq: i32,
        node_name: String,
        task_name: Option<String>,
    },
}

/// Parse a cairn:// URI into a message target. Single classifier from a parsed
/// `CairnResource` to a `MessageTarget`; all URI parsing is delegated to the
/// canonical `parse_uri`.
///
/// - `cairn://p/PROJECT` (or `/messages`) -> project channel
/// - `cairn://p/PROJECT/NUMBER` (or `/messages`) -> issue channel
/// - `cairn://p/PROJECT/NUMBER/EXEC/NODE` (or `/messages`) -> direct message to a node agent
/// - `cairn://p/PROJECT/NUMBER/EXEC/NODE/task/NAME` (or `/messages`) -> direct to a sub-agent task
fn parse_message_target(uri: &str) -> Result<MessageTarget, String> {
    if uri == "external" {
        return Ok(MessageTarget::External);
    }

    match parse_uri(uri) {
        Some(CairnResource::Project { project })
        | Some(CairnResource::ProjectMessages { project }) => Ok(MessageTarget::Project {
            project_key: project,
        }),
        Some(CairnResource::Issue { project, number })
        | Some(CairnResource::IssueMessages { project, number }) => Ok(MessageTarget::Issue {
            project_key: project,
            issue_number: number,
        }),
        Some(CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        })
        | Some(CairnResource::NodeMessages {
            project,
            number,
            exec_seq,
            node_id,
        }) => Ok(MessageTarget::Direct {
            project_key: project,
            issue_number: number,
            exec_seq,
            node_name: node_id,
            task_name: None,
        }),
        Some(CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        })
        | Some(CairnResource::TaskMessages {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        }) => Ok(MessageTarget::Direct {
            project_key: project,
            issue_number: number,
            exec_seq,
            node_name: node_id,
            task_name: Some(task_name),
        }),
        Some(other) => Err(format!("Unsupported message target URI: {:?}", other)),
        None => Err(format!("Unrecognized URI format: {}", uri)),
    }
}

// ============================================================================
// Handler
// ============================================================================

/// Handle message tool call
pub async fn handle_message(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: MessagePayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    if payload.content.is_empty() {
        return "Message content cannot be empty".to_string();
    }

    // Get sender context
    let run_ctx = match super::run_context::lookup_run(&orch.db.local, request).await {
        Ok(ctx) => ctx,
        Err(e) => return format!("No active run: {}", e),
    };

    // Build sender URI: cairn://PROJECT/NUMBER/SEQ/NODE for issue agents,
    // or just the node name for project-level agents
    if run_ctx.job_name.is_none() {
        log::warn!(
            "No job_name for run_id={}, job_id={} — sender URI will use 'unknown'",
            run_ctx.run_id,
            run_ctx.job_id
        );
    }
    let sender_name = match sender_name_for_run(&orch.db.local, &run_ctx).await {
        Ok(name) => name,
        Err(e) => return format!("Failed to resolve sender: {}", e),
    };

    // Resolve target
    let target = match &payload.to {
        None => {
            // Default: project channel
            MessageTarget::Project {
                project_key: run_ctx.project_key.clone(),
            }
        }
        Some(uri) => match parse_message_target(uri) {
            Ok(t) => t,
            Err(e) => return format!("Invalid target URI: {}", e),
        },
    };

    // Route based on target
    match target {
        MessageTarget::External => {
            append_external_reply(orch, &run_ctx, &sender_name, &payload.content)
                .await
                .unwrap_or_else(|e| e)
        }
        MessageTarget::Project { project_key } => {
            let project_key = match resolve_channel_id(&orch.db.local, &project_key, None).await {
                Ok(channel_id) => channel_id,
                Err(e) => return e,
            };
            let msg = match msg_db::insert_message(
                &orch.db.local,
                &ChannelType::Project,
                Some(&project_key),
                Some(&run_ctx.run_id),
                &sender_name,
                None,
                &payload.content,
            ) {
                Ok(m) => m,
                Err(e) => return format!("Failed to send message: {}", e),
            };

            // Emit db-change event
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );

            // Deliver to inboxes
            delivery::deliver(orch, &msg);

            "Sent to project channel".to_string()
        }
        MessageTarget::Issue {
            project_key,
            issue_number,
        } => {
            let issue_key =
                match resolve_channel_id(&orch.db.local, &project_key, Some(issue_number)).await {
                    Ok(channel_id) => channel_id,
                    Err(e) => return e,
                };
            let msg = match msg_db::insert_message(
                &orch.db.local,
                &ChannelType::Issue,
                Some(&issue_key),
                Some(&run_ctx.run_id),
                &sender_name,
                None,
                &payload.content,
            ) {
                Ok(m) => m,
                Err(e) => return format!("Failed to send message: {}", e),
            };

            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );

            delivery::deliver(orch, &msg);

            if let Err(error) =
                crate::messages::side_channel::record_issue_message_side_channel_by_issue_number(
                    orch,
                    &project_key,
                    issue_number,
                    "agent",
                    &payload.content,
                    Some(&run_ctx.job_id),
                )
                .await
            {
                log::warn!("failed to record issue message side-channel notices: {error}");
            }

            "Sent to issue channel".to_string()
        }
        MessageTarget::Direct {
            project_key,
            issue_number,
            exec_seq,
            node_name,
            task_name,
        } => append_direct_message_with_urgency(
            orch,
            request,
            &project_key,
            issue_number,
            exec_seq,
            &node_name,
            task_name.as_deref(),
            &payload.content,
            payload.escalate,
            payload.urgency,
        )
        .await
        .unwrap_or_else(|e| e),
    }
}
