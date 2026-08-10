//! Message-related MCP handlers.
//!
//! Handles: message (send a message to a channel or direct to an agent)

use serde::Deserialize;

use crate::jobs::queries::{node_uri_segment_for_job, parent_uri_segment_for_job};
use crate::mcp::types::McpCallbackRequest;
use crate::messages::{db as msg_db, delivery};
use crate::models::ChannelType;
use crate::models::IssueStatus;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_common::uri::{build_job_base_uri, build_node_uri};
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

/// The recipient job behind a thread coordinate: the thread's own session, or a
/// task it spawned when one is named.
async fn find_thread_recipient_job(
    db: &LocalDb,
    project_key: &str,
    thread_name: &str,
    task_name: Option<&str>,
) -> Result<Option<(String, String)>, String> {
    let job_id = crate::resources::resolve_node_or_task_job_id(
        db,
        project_key,
        0,
        0,
        thread_name,
        task_name,
    )
    .await?;
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| row.text(0).map(|run_id| (job_id, run_id)))
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub enum MessageAuthor<'a> {
    Mcp(&'a McpCallbackRequest),
    Route(&'a str),
}

async fn ensure_issue_accepts_messages(
    db: &LocalDb,
    project_key: &str,
    issue_number: i32,
) -> Result<(), String> {
    let lookup_key = project_key.to_uppercase();
    let requested_key = project_key.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT i.status, p.key
                    FROM issues i
                    JOIN projects p ON i.project_id = p.id
                    WHERE p.key = ?1 AND i.number = ?2
                    LIMIT 1
                    ",
                    params![lookup_key.as_str(), issue_number],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Err(DbError::Row(format!(
                    "Issue {}-{} not found",
                    requested_key, issue_number
                )));
            };
            let status: IssueStatus = row.text(0)?.parse().map_err(DbError::Row)?;
            let canonical_key = row.text(1)?;
            if status.is_terminal() {
                return Err(DbError::Row(format!(
                    "Issue {canonical_key}-{issue_number} is terminal ({status}); messages cannot be sent to it or its agents"
                )));
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| match error {
        DbError::Row(message) => message,
        other => other.to_string(),
    })
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

pub async fn append_thread_message(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project_key: &str,
    thread_id: &str,
    content: &str,
) -> Result<String, String> {
    let owning_db = orch.db.for_project(project_key).await;
    let content =
        crate::durable_content::normalize_text(orch, request, project_key, content).await?;
    let (sender_run_id, sender_name) = sender_context(&orch.db.local, request).await?;
    crate::messages::delivery::append_thread_message(
        orch,
        &owning_db,
        thread_id,
        sender_run_id.as_deref(),
        &sender_name,
        &content,
    )
    .await
}

pub async fn append_project_or_issue_message(
    orch: &Orchestrator,
    author: MessageAuthor<'_>,
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
    if let Some(number) = issue_number {
        ensure_issue_accepts_messages(&owning_db, project_key, number).await?;
    }
    let (content, sender_run_id, sender_name, exclude_job_id) = match author {
        MessageAuthor::Mcp(request) => {
            let content =
                crate::durable_content::normalize_text(orch, request, project_key, content).await?;
            let (sender_run_id, sender_name) = sender_context(&orch.db.local, request).await?;
            let exclude_job_id = super::run_context::lookup_run(&orch.db.local, request)
                .await
                .ok()
                .map(|ctx| ctx.job_id);
            (content, sender_run_id, sender_name, exclude_job_id)
        }
        MessageAuthor::Route(route_id) => {
            (content.to_string(), None, format!("route:{route_id}"), None)
        }
    };
    let channel_id = resolve_channel_id(&owning_db, project_key, issue_number).await?;

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
        &content,
    )
    .map_err(|e| format!("Failed to send message: {e}"))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );

    if let Some(number) = issue_number {
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
                &content,
                exclude_job_id.as_deref(),
            )
            .await
        {
            log::warn!("failed to record issue message side-channel notices: {error}");
        }
    }

    Ok(success_message)
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
    // The reserved (0, 0) coordinate names a thread rather than an issue node.
    // Whether it names the thread's session or a task beneath it is the same
    // distinction `task_name` draws everywhere else, so both arms are "thread":
    // reading only the session out of this coordinate sent a message addressed to
    // a thread's task looking for issue 0 (CAIRN-3755).
    let is_thread = issue_number == 0 && exec_seq == 0;

    // Echo the canonical URI the caller addressed in any not-found error so a
    // wrong-URI miss is debuggable without rebuilding the URI by hand. For
    // sub-task targets (task_name = Some), the addressed URI is
    // .../{exec}/{node}/task/{task}; for top-level nodes it's .../{exec}/{node}.
    let (addressed_uri, scope_hint) = if is_thread {
        match task_name {
            Some(task) => (
                cairn_common::uri::build_thread_task_uri(project_key, node_name, task),
                format!("no task '{task}' under thread '{node_name}'"),
            ),
            None => (
                format!("cairn://p/{project_key}/{node_name}"),
                format!("no thread session for '{node_name}'"),
            ),
        }
    } else {
        match task_name {
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
        }
    };

    // Direct-message routing (CAIRN-2598): the recipient job, its wake
    // subscriptions, the message row, and the attention push all live in the
    // database that owns the target project — a team job lives in its team
    // replica, so a local-only path would miss it entirely. Sender/run
    // resolution stays job-keyed against the private DB, as on the project/issue
    // message path.
    let owning_db = orch.db.for_project(project_key).await;
    if !is_thread {
        ensure_issue_accepts_messages(&owning_db, project_key, issue_number).await?;
    }
    let content =
        crate::durable_content::normalize_text(orch, request, project_key, content).await?;
    let (sender_run_id, sender_name) = sender_context(&orch.db.local, request).await?;
    let recipient = if is_thread {
        find_thread_recipient_job(&owning_db, project_key, node_name, task_name).await?
    } else {
        find_recipient_job(
            &owning_db,
            project_key,
            issue_number,
            exec_seq,
            node_name,
            task_name,
        )
        .await?
    };
    let (job_id, recipient_run_id) =
        recipient.ok_or_else(|| format!("{} not found ({}).", addressed_uri, scope_hint))?;

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
        &content,
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

    // The watching coordinator's copy of a message addressed to a child node is a
    // passive catch-up push (CAIRN-1894), fanned out to every derived watcher of
    // that node's issue (CAIRN-3342). It never wakes them; it rides along on their
    // next run. The sender's own job is excluded so an agent messaging a child is
    // not told about its own message. Sender resolution routes to the database
    // that owns the sender's run (a team agent's run lives in its replica, not
    // the private DB), matching how the rest of this handler routes by owner.
    let sender_job_id = match sender_run_id.as_deref() {
        Some(run_id) => {
            let sender_db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
                .await
                .unwrap_or_else(|_| orch.db.local.clone());
            crate::messages::side_channel::job_id_for_run(&sender_db, run_id).await
        }
        None => None,
    };
    match crate::orchestrator::attention_delivery::create_catchup_pushes_for_watchers(
        &owning_db,
        &addressed_uri,
        sender_job_id.as_deref(),
    )
    .await
    {
        Ok(created) if created > 0 => orch.notifier.emit_change("attention_pushes"),
        Ok(_) => {}
        Err(error) => log::warn!("catch-up push creation for {addressed_uri} failed: {error}"),
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

// ============================================================================
// Handler
// ============================================================================
