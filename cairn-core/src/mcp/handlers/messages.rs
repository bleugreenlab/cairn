//! Message-related MCP handlers.
//!
//! Handles: message (send a message to a channel or direct to an agent)

use serde::Deserialize;

use crate::jobs::queries::{node_uri_segment_for_job, parent_uri_segment_for_job};
use crate::mcp::types::McpCallbackRequest;
use crate::messages::{db as msg_db, delivery};
use crate::models::{ChannelType, ExternalReplyMode, IssueAttention, IssueStatus};

use crate::orchestrator::attention::{AttentionEvent, AttentionFact, ExternalMessageReplyContent};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_common::uri::{build_job_base_uri, build_node_uri, parse_uri, CairnResource};
use cairn_db::turso::params;

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

    // Channel resolution and the message row live in the database that owns the
    // project (CAIRN-2181): a team project's projects/issues/messages rows live
    // in its team replica, and reads are already routed there, so the append must
    // route too or posted messages disappear from the team-replica view.
    let owning_db = orch.db.for_project(project_key).await;
    let channel_id = resolve_channel_id(&owning_db, project_key, issue_number).await?;
    // content→execution boundary (CAIRN-2181): sender/run resolution is job-keyed
    // and stays private until CAIRN-2182.
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

    msg_db::insert_message(
        &owning_db,
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

    // Bump the issue's strictly-monotonic updated_at first and reuse it as the
    // reply message's created_at, so the row's own timestamp is the catch-up
    // cursor the live ExternalMessageReply event broadcasts (CAIRN-1906 retires
    // the messages.delivered_at marker the watch path used).
    let (issue_id, attention, status, updated_at) =
        touch_issue_for_external_reply(&orch.db.local, &run_ctx.project_key, issue_number).await?;

    let msg = msg_db::insert_external_reply(
        &orch.db.local,
        &issue_key,
        &run_ctx.run_id,
        sender_name,
        content,
        updated_at,
    )
    .await
    .map_err(|e| format!("Failed to send external reply: {e}"))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );

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
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn append_direct_message_for_remote_intent(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    content: &str,
    escalate: bool,
    intent_id: &str,
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
        Some(intent_id),
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
    mutation_key: Option<&str>,
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

    // Direct-message routing (CAIRN-2598): the recipient job, its wake
    // subscriptions, the message row, and the attention push all live in the
    // database that owns the target project — a team job lives in its team
    // replica, so a local-only path would miss it entirely. Sender/run
    // resolution stays job-keyed against the private DB, as on the project/issue
    // message path.
    let owning_db = orch.db.for_project(project_key).await;
    let (sender_run_id, sender_name) = sender_context(&orch.db.local, request).await?;
    let (job_id, recipient_run_id) = find_recipient_job(
        &owning_db,
        project_key,
        issue_number,
        exec_seq,
        node_name,
        task_name,
    )
    .await?
    .ok_or_else(|| format!("{} not found ({}).", addressed_uri, scope_hint))?;

    crate::orchestrator::wakes::seed_default_job_subscriptions(&owning_db, &job_id).await?;

    let urgency = if escalate {
        crate::messages::queued::DeliveryUrgency::Interrupt
    } else {
        payload_urgency.unwrap_or(crate::messages::queued::DeliveryUrgency::Steer)
    };
    let stable_message_id = mutation_key.map(|key| format!("remote-intent-message:{key}"));
    let msg = msg_db::insert_message_with_urgency_and_id(
        &owning_db,
        &ChannelType::Direct,
        None,
        sender_run_id.as_deref(),
        &sender_name,
        Some(&recipient_run_id),
        content,
        Some(urgency),
        stable_message_id.as_deref(),
    )
    .map_err(|e| format!("Failed to send message: {e}"))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );

    // Direct messages ride the attention push queue (CAIRN-1900). Create one
    // non-collapsing push per message keyed `direct:{message_id}` — the id is a
    // UUID, so supersede-by-key never merges two unread directs; each is its own
    // undelivered row. The push is ref'd to the addressed conversation surface;
    // "unread" is the row's `delivered_event_id IS NULL`, and the message text
    // resolves from the durable `messages` row at drain. Delivery, exactly-once
    // stamping, crashed-turn recovery, the busy-agent event-boundary drain, and
    // the self-suspended ride-along all come from the existing push machinery.
    let requested_wake = if urgency == crate::messages::queued::DeliveryUrgency::Interrupt {
        crate::orchestrator::attention_push::Wake::Interrupt
    } else {
        crate::orchestrator::attention_push::Wake::Wake
    };
    // Mute is consulted on the direct's source axis (its sender), which is not in
    // the push key, so the shared `mute_downgrade` rule is applied here rather
    // than in the central issue-mute path inside `push`. A user sender is the
    // `user` source; an agent sender is a `peer` keyed by sender name — matching
    // the legacy `WakeSource::User`/`Peer` axis.
    let (source_kind, source_ref) = if request.run_id.is_none() {
        ("user", None)
    } else {
        ("peer", Some(sender_name.as_str()))
    };
    let effective_wake = crate::orchestrator::wakes::mute_downgrade(
        &owning_db,
        &job_id,
        source_kind,
        source_ref,
        "message",
        requested_wake,
    )
    .await?;
    let push_key = format!("direct:{}", msg.id);
    let push_exists = if mutation_key.is_some() {
        crate::orchestrator::attention_push::has_push_identity(&owning_db, &job_id, &push_key)
            .await
            .map_err(|e| format!("Failed to inspect direct-message delivery identity: {e}"))?
    } else {
        false
    };
    if !push_exists {
        if let Err(e) = crate::orchestrator::attention_push::push(
            &owning_db,
            &job_id,
            &addressed_uri,
            effective_wake,
            crate::orchestrator::attention_push::Boundary::Event,
            &push_key,
        )
        .await
        {
            return Err(format!(
                "Failed to queue direct message to {}: {}",
                recipient_label, e
            ));
        }
    }

    // The user→child side-channel / catch-up notice for the watching parent is a
    // separate, out-of-scope mechanism (CAIRN-1894); keep emitting it on
    // user-origin sends.
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

    // Nudge only when the effective wake still wakes an idle recipient. A muted
    // source was downgraded to `Passive` and rides along on the recipient's next
    // run. `nudge_job_for_urgency` is the shared resume ladder: an idle recipient
    // resumes and drains the push; an `interrupt` on an active recipient stops the
    // turn so the turn-end flush delivers it; a non-interrupt active recipient is
    // left for the event-boundary push drain; a self-suspended recipient is not
    // resumed (the resume gate is `!self_suspended`-gated) and the direct rides
    // along when its own work resolves.
    if effective_wake.wakes_idle() {
        if let Err(e) = delivery::nudge_job_for_urgency(orch, &job_id, urgency) {
            log::warn!("direct message wake for {} failed: {}", recipient_label, e);
        }
    }

    Ok(format!("Sent direct message to {}", recipient_label))
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
            if let Err(e) = msg_db::insert_message(
                &orch.db.local,
                &ChannelType::Project,
                Some(&project_key),
                Some(&run_ctx.run_id),
                &sender_name,
                None,
                &payload.content,
            ) {
                return format!("Failed to send message: {}", e);
            }

            // Emit db-change event
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );

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
            if let Err(e) = msg_db::insert_message(
                &orch.db.local,
                &ChannelType::Issue,
                Some(&issue_key),
                Some(&run_ctx.run_id),
                &sender_name,
                None,
                &payload.content,
            ) {
                return format!("Failed to send message: {}", e);
            }

            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );

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
            None,
        )
        .await
        .unwrap_or_else(|e| e),
    }
}
