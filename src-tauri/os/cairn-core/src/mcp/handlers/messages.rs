//! Message-related MCP handlers.
//!
//! Handles: message (send a message to a channel or direct to an agent)

use crate::mcp::types::McpCallbackRequest;
use crate::messages::{db as msg_db, delivery};
use crate::models::ChannelType;
use crate::models::IssueStatus;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_uri::{build_job_base_uri, build_node_uri, CairnResource};

async fn sender_name_for_run(db: &LocalDb, run_ctx: &super::RunContext) -> Result<String, String> {
    // The canonical resolver covers issue nodes, their tasks, thread sessions,
    // and thread-spawned tasks. Legacy jobs without a stored URI segment have no
    // canonical address, so preserve their historical bare-name identity.
    Ok(crate::jobs::queries::home_uri_for_job(db, &run_ctx.job_id)
        .await
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| run_ctx.job_name.as_deref().unwrap_or("unknown").to_string()))
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
    let lookup_key = cairn_uri::canonical_project(project_key);
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
                    "Issue {}/{} not found",
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
    // No run_id means the caller is an MCP/CLI session running outside any Cairn
    // run. It has no node URI, so it is recorded under the shared external-sender
    // name that `messages::render` renders a no-address reply hint for.
    let Some(_) = request.run_id.as_ref() else {
        let sender = if request.is_unverified_local() {
            crate::messages::render::UNVERIFIED_LOCAL_SENDER
        } else {
            crate::messages::render::EXTERNAL_SENDER
        };
        return Ok((None, sender.to_string()));
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
    let lookup_key = cairn_uri::canonical_project(&requested_key);
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
                        "Issue {}/{} not found",
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

enum RecipientJob {
    Started {
        job_id: String,
        run_id: String,
    },
    Pending {
        parent_segment: Option<String>,
        delegated: bool,
    },
}

/// Resolve the recipient job and its latest run for a direct message.
///
/// A pending job is still a resolved address even though it has no run yet. The
/// caller can then explain the dispatch condition instead of misreporting it as
/// missing.
async fn find_recipient_job(
    db: &LocalDb,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
) -> Result<Option<RecipientJob>, String> {
    let lookup_key = cairn_uri::canonical_project(project_key);
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

            // Resolve the job independently of its run. A never-started job has
            // no runs row yet, but is already an addressable node or task.
            let mut candidates = match &task_name {
                Some(task) => {
                    conn.query(
                        "
                        SELECT j.id, r.id, j.status, p.uri_segment,
                               j.parent_tool_use_id IS NOT NULL
                        FROM jobs j
                        JOIN jobs p ON j.parent_job_id = p.id
                        LEFT JOIN runs r ON r.job_id = j.id
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
                        SELECT j.id, r.id, j.status, NULL,
                               j.parent_tool_use_id IS NOT NULL
                        FROM jobs j
                        LEFT JOIN runs r ON r.job_id = j.id
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
                .map(|row| {
                    let job_id = row.text(0)?;
                    match row.opt_text(1)? {
                        Some(run_id) => Ok(RecipientJob::Started { job_id, run_id }),
                        None if row.text(2)? == "pending" => Ok(RecipientJob::Pending {
                            parent_segment: row.opt_text(3)?,
                            delegated: row.i64(4)? != 0,
                        }),
                        None => Err(DbError::Internal(format!(
                            "recipient job {job_id} has no run but is not pending"
                        ))),
                    }
                })
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
                cairn_uri::canonical_project(channel_id.split('/').next().unwrap_or(project_key),),
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
        } else if sender_name == crate::messages::render::UNVERIFIED_LOCAL_SENDER {
            crate::messages::render::UNVERIFIED_LOCAL_SENDER
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

/// Where a direct message can be sent: a node, a task beneath it, a thread's
/// session (the reserved `(0, 0)` coordinate carrying the thread's name), or a
/// task a thread spawned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectAddress {
    pub project: String,
    pub number: i32,
    pub exec_seq: i32,
    pub node: String,
    pub task: Option<String>,
}

impl DirectAddress {
    pub fn new(project: &str, number: i32, exec_seq: i32, node: &str, task: Option<&str>) -> Self {
        DirectAddress {
            project: project.to_string(),
            number,
            exec_seq,
            node: node.to_string(),
            task: task.map(str::to_string),
        }
    }

    /// The address a resource names, when a direct message can be sent to it:
    /// a node or task (bare or its `/messages`), or a bare thread, which names
    /// its session. A thread URI must already be delegated.
    pub fn from_resource(resource: &CairnResource) -> Option<Self> {
        match resource {
            CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            }
            | CairnResource::NodeMessages {
                project,
                number,
                exec_seq,
                node_id,
            } => Some(Self::new(project, *number, *exec_seq, node_id, None)),
            CairnResource::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            }
            | CairnResource::TaskMessages {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => Some(Self::new(
                project,
                *number,
                *exec_seq,
                node_id,
                Some(task_name),
            )),
            CairnResource::Thread {
                project,
                name,
                path,
            } if path.is_empty() => Some(Self::new(project, 0, 0, name, None)),
            _ => None,
        }
    }

    /// The reserved `(0, 0)` coordinate names a thread rather than an issue
    /// node. Whether it names the thread's session or a task beneath it is the
    /// same distinction `task` draws everywhere else (CAIRN-3755).
    fn is_thread(&self) -> bool {
        self.number == 0 && self.exec_seq == 0
    }

    /// The canonical URI of this address.
    pub fn uri(&self) -> String {
        self.addressed().0
    }

    /// The short name a single-recipient reply uses: `node` or `node/task`.
    fn short_label(&self) -> String {
        match &self.task {
            Some(task) => format!("{}/{}", self.node, task),
            None => self.node.clone(),
        }
    }

    /// The canonical URI the sender addressed, and what was searched when it
    /// names nothing, so a wrong-URI miss is debuggable without rebuilding the
    /// URI by hand.
    fn addressed(&self) -> (String, String) {
        let project = self.project.as_str();
        let node = self.node.as_str();
        if self.is_thread() {
            match self.task.as_deref() {
                Some(task) => (
                    cairn_uri::build_thread_task_uri(project, node, task),
                    format!("no task '{task}' under thread '{node}'"),
                ),
                None => (
                    format!("cairn://p/{project}/{node}"),
                    format!("no thread session for '{node}'"),
                ),
            }
        } else {
            match self.task.as_deref() {
                Some(task) => (
                    build_job_base_uri(project, self.number, self.exec_seq, task, Some(node)),
                    format!(
                        "no sub-task with uri_segment '{}' under parent '{}' in execution {}",
                        task, node, self.exec_seq
                    ),
                ),
                None => (
                    build_node_uri(project, self.number, self.exec_seq, node),
                    format!(
                        "no top-level node with uri_segment '{}' in execution {}",
                        node, self.exec_seq
                    ),
                ),
            }
        }
    }
}

/// One direct message as the sender asked for it.
pub struct DirectSend<'a> {
    /// The append target: the first recipient.
    pub target: DirectAddress,
    /// The other recipients, as the sender wrote them (canonical or deictic
    /// URIs), in order.
    pub cc: &'a [String],
    pub content: &'a str,
    pub urgency: crate::messages::queued::DeliveryUrgency,
    /// A remote intent's id: the message id becomes stable, and a re-driven
    /// intent writes nothing twice.
    pub mutation_key: Option<&'a str>,
}

/// A recipient that resolved: who it is, where it was addressed, and the
/// database that holds it.
struct ResolvedRecipient {
    /// What the sender wrote for this recipient.
    entry: String,
    address: DirectAddress,
    addressed_uri: String,
    job_id: String,
    run_id: String,
    db: std::sync::Arc<LocalDb>,
}

/// Resolve a `cc` entry the way the write path resolves its own target:
/// deictic expansion, then the URI grammar, then thread delegation.
async fn resolve_cc_entry(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    entry: &str,
) -> Result<DirectAddress, String> {
    let uri =
        crate::resources::resolve_home_relative_resource_uri(&orch.db, request, entry).await?;
    let resource =
        cairn_uri::parse_uri(&uri).ok_or_else(|| "is not a cairn:// address".to_string())?;
    let resource = cairn_uri::delegate_thread_descendant(resource)?;
    DirectAddress::from_resource(&resource).ok_or_else(|| {
        "is not an address a direct message can reach (a node, task, or thread, or its /messages)"
            .to_string()
    })
}

/// Resolve one recipient, writing nothing: its issue must accept messages,
/// and its job must exist and have started.
async fn resolve_recipient(
    orch: &Orchestrator,
    entry: String,
    address: DirectAddress,
) -> Result<ResolvedRecipient, String> {
    let (addressed_uri, scope_hint) = address.addressed();
    // The recipient job, its wake subscriptions, the message row, and the
    // attention push all live in the database that owns the target project
    // (CAIRN-2598): a team job lives in its team replica.
    let db = orch.db.for_project(&address.project).await;
    if !address.is_thread() {
        ensure_issue_accepts_messages(&db, &address.project, address.number).await?;
    }
    let (job_id, run_id) = if address.is_thread() {
        find_thread_recipient_job(
            &db,
            &address.project,
            &address.node,
            address.task.as_deref(),
        )
        .await?
        .ok_or_else(|| format!("{addressed_uri} not found ({scope_hint})."))?
    } else {
        match find_recipient_job(
            &db,
            &address.project,
            address.number,
            address.exec_seq,
            &address.node,
            address.task.as_deref(),
        )
        .await?
        .ok_or_else(|| format!("{addressed_uri} not found ({scope_hint})."))?
        {
            RecipientJob::Started { job_id, run_id } => (job_id, run_id),
            RecipientJob::Pending {
                parent_segment,
                delegated,
            } => {
                use crate::jobs::queries::PendingDispatch;
                let pending = match (delegated, parent_segment.as_deref()) {
                    (true, _) => PendingDispatch::Delegated,
                    (false, Some(parent)) => PendingDispatch::AfterParent(parent),
                    (false, None) => PendingDispatch::AfterRecipe,
                };
                let condition = crate::jobs::queries::pending_dispatch_description(pending);
                return Err(format!("{addressed_uri}: {condition}."));
            }
        }
    };
    Ok(ResolvedRecipient {
        entry,
        address,
        addressed_uri,
        job_id,
        run_id,
        db,
    })
}

/// Resolve the target and every cc entry together, writing nothing. Each
/// failure is collected, so one refusal names every bad entry. A cc entry
/// that resolves to a job already addressed, to the sender, or to another
/// database than the target's is refused rather than quietly dropped.
async fn resolve_recipients(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    send: &DirectSend<'_>,
) -> Result<Vec<ResolvedRecipient>, String> {
    let (target_uri, _) = send.target.addressed();
    let target = resolve_recipient(orch, target_uri.clone(), send.target.clone()).await;
    if send.cc.is_empty() {
        // A single recipient keeps the error text it has always had.
        return target.map(|recipient| vec![recipient]);
    }

    // A team agent's run lives in its replica, so the sender's job is read
    // from the database that owns the run.
    let sender_job = match request.run_id.as_deref() {
        Some(run_id) => {
            let sender_db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
                .await
                .unwrap_or_else(|_| orch.db.local.clone());
            crate::messages::side_channel::job_id_for_run(&sender_db, run_id).await
        }
        None => None,
    };
    let mut failures: Vec<(String, String)> = Vec::new();
    let mut resolved: Vec<ResolvedRecipient> = Vec::new();
    match target {
        Ok(recipient) => resolved.push(recipient),
        Err(error) => failures.push((target_uri.clone(), error)),
    }
    for entry in send.cc {
        let recipient = match resolve_cc_entry(orch, request, entry).await {
            Ok(address) => resolve_recipient(orch, entry.clone(), address).await,
            Err(error) => Err(error),
        };
        let recipient = match recipient {
            Ok(recipient) => recipient,
            Err(error) => {
                failures.push((entry.clone(), error));
                continue;
            }
        };
        let same_job = resolved
            .iter()
            .find(|other| other.job_id == recipient.job_id);
        let refusal = if let Some(other) = same_job {
            Some(format!(
                "names the same recipient as {}; each recipient is named once",
                other.entry
            ))
        } else if sender_job.as_deref() == Some(recipient.job_id.as_str()) {
            Some("is you, the sender".to_string())
        } else if resolved
            .first()
            .is_some_and(|target| !std::sync::Arc::ptr_eq(&target.db, &recipient.db))
        {
            Some(format!(
                "lives in a different database from {target_uri} (one of them is a team project), \
                 and one message is stored in one database; send it to {} separately",
                recipient.addressed_uri
            ))
        } else {
            None
        };
        match refusal {
            Some(reason) => failures.push((entry.clone(), reason)),
            None => resolved.push(recipient),
        }
    }
    if failures.is_empty() {
        return Ok(resolved);
    }
    let total = send.cc.len() + 1;
    let lines = failures
        .iter()
        .map(|(entry, reason)| format!("- {entry}: {}", reason.trim_end_matches('.')))
        .collect::<Vec<_>>()
        .join("\n");
    Err(format!(
        "{} of the {total} addresses cannot receive this message:\n{lines}\nNothing ran.",
        failures.len()
    ))
}

/// A recipient ready to be written: resolved, and its own mute applied.
struct PlannedRecipient {
    recipient: ResolvedRecipient,
    wake: crate::orchestrator::attention_push::Wake,
}

/// A direct message to one recipient.
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
    send_direct_message(
        orch,
        request,
        DirectSend {
            target: DirectAddress::new(project_key, issue_number, exec_seq, node_name, task_name),
            cc: &[],
            content,
            urgency: requested_urgency(escalate, None)?,
            mutation_key: None,
        },
    )
    .await
}

/// A direct message to one recipient on behalf of a team remote intent,
/// idempotent on the intent's id.
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
    send_direct_message(
        orch,
        request,
        DirectSend {
            target: DirectAddress::new(project_key, issue_number, exec_seq, node_name, task_name),
            cc: &[],
            content,
            urgency: requested_urgency(escalate, None)?,
            mutation_key: Some(intent_id),
        },
    )
    .await
}

/// The urgency a send asked for. `escalate` is `interrupt`, which forces a
/// wake through a mute; naming a different urgency beside it is refused.
/// Without either, a direct message steers.
pub fn requested_urgency(
    escalate: bool,
    urgency: Option<crate::messages::queued::DeliveryUrgency>,
) -> Result<crate::messages::queued::DeliveryUrgency, String> {
    use crate::messages::queued::DeliveryUrgency;
    match (escalate, urgency) {
        (true, Some(urgency)) if urgency != DeliveryUrgency::Interrupt => Err(format!(
            "escalate:true means urgency `interrupt`, but urgency `{}` was also given; give one",
            urgency.as_str()
        )),
        (true, _) => Ok(DeliveryUrgency::Interrupt),
        (false, urgency) => Ok(urgency.unwrap_or(DeliveryUrgency::Steer)),
    }
}

/// The canonical URI of a stored message, in the project whose database
/// holds it.
pub fn message_uri(project: &str, message_id: &str) -> String {
    format!(
        "cairn://p/{}/messages/{message_id}",
        cairn_uri::canonical_project(project)
    )
}

/// Send one direct message to its target and every cc recipient.
///
/// Everything is resolved before anything is written, and any bad address
/// refuses the whole send. Each recipient's own mute decides its wake; the
/// message row, one recipient row each, and one `direct:{id}` push each are
/// written in one transaction; recipients whose wake still wakes an idle agent
/// are nudged after commit.
pub async fn send_direct_message(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    send: DirectSend<'_>,
) -> Result<String, String> {
    use crate::orchestrator::attention_push::{self, Boundary, Wake};

    if send.content.is_empty() {
        return Err("Message content cannot be empty".to_string());
    }
    let recipients = resolve_recipients(orch, request, &send).await?;
    let project_key = send.target.project.as_str();
    let owning_db = recipients[0].db.clone();

    let content =
        crate::durable_content::normalize_text(orch, request, project_key, send.content).await?;
    let (sender_run_id, sender_name) = sender_context(&orch.db.local, request).await?;
    let urgency = send.urgency;
    let requested_wake = delivery::direct_wake_for_urgency(urgency);
    // Mute is consulted on the direct's source axis (its sender), which is not in
    // the push key, so the shared `mute_downgrade` rule is applied here rather
    // than in the central issue-mute path inside `push`. A user sender is the
    // `user` source; an agent sender is a `peer` keyed by sender name. An
    // unverified sender is no user.
    let (source_kind, source_ref) = if request.run_id.is_none() && !request.is_unverified_local() {
        ("user", None)
    } else {
        ("peer", Some(sender_name.as_str()))
    };
    let mut planned = Vec::with_capacity(recipients.len());
    for recipient in recipients {
        crate::orchestrator::wakes::seed_default_job_subscriptions(&owning_db, &recipient.job_id)
            .await?;
        let wake = crate::orchestrator::wakes::mute_downgrade(
            &owning_db,
            &recipient.job_id,
            source_kind,
            source_ref,
            "message",
            requested_wake,
        )
        .await?;
        planned.push(PlannedRecipient { recipient, wake });
    }

    let message_id = send
        .mutation_key
        .map(|key| format!("remote-intent-message:{key}"))
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let direct = cairn_attention::direct::NewDirect {
        id: message_id.clone(),
        sender_run_id: sender_run_id.clone(),
        sender_name: sender_name.clone(),
        content: content.clone(),
        urgency: Some(urgency),
        created_at: chrono::Utc::now().timestamp(),
        recipients: planned
            .iter()
            .map(|plan| cairn_attention::direct::NewRecipient {
                job_id: plan.recipient.job_id.clone(),
                run_id_at_send: Some(plan.recipient.run_id.clone()),
                wake: Some(plan.wake),
            })
            .collect(),
    };
    let pushes: Vec<(String, String, Wake)> = planned
        .iter()
        .map(|plan| {
            (
                plan.recipient.job_id.clone(),
                plan.recipient.addressed_uri.clone(),
                plan.wake,
            )
        })
        .collect();
    let idempotent = send.mutation_key.is_some();
    owning_db
        .write(|conn| {
            let direct = direct.clone();
            let pushes = pushes.clone();
            Box::pin(async move {
                cairn_attention::direct::insert_direct_message_conn(
                    conn,
                    &direct,
                    cairn_attention::direct::OnExisting::Verify,
                )
                .await?;
                // Direct messages ride the attention push queue (CAIRN-1900):
                // one non-collapsing push per recipient keyed
                // `direct:{message_id}`, ref'd to the addressed conversation
                // surface. The message text resolves from the durable row at
                // drain.
                let key = attention_push::direct_push_key(&direct.id, None);
                for (job_id, addressed_uri, wake) in &pushes {
                    if idempotent {
                        let mut rows = conn
                            .query(
                                "SELECT 1 FROM attention_pushes WHERE recipient=?1 AND key=?2 LIMIT 1",
                                cairn_db::turso::params![job_id.as_str(), key.as_str()],
                            )
                            .await?;
                        if rows.next().await?.is_some() {
                            continue;
                        }
                    }
                    attention_push::push_fingerprinted_conn(
                        conn,
                        job_id,
                        addressed_uri,
                        *wake,
                        Boundary::Event,
                        &key,
                        None,
                        false,
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|error| format!("Failed to send message: {error}"))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );
    orch.notifier.emit_change("attention_pushes");

    // Sender resolution routes to the database that owns the sender's run (a team
    // agent's run lives in its replica, not the private DB).
    let sender_uri = match sender_run_id.as_deref() {
        Some(run_id) => {
            let sender_db = crate::execution::routing::owning_db_for_run(&orch.db, run_id)
                .await
                .unwrap_or_else(|_| orch.db.local.clone());
            match crate::messages::side_channel::job_id_for_run(&sender_db, run_id).await {
                Some(job_id) => delivery::node_uri_for_job(&sender_db, &job_id).await,
                None => None,
            }
        }
        None => None,
    };

    // The operator's message to a child node leaves every derived watcher of that
    // node's issue a passive catch-up push (CAIRN-1894, CAIRN-3342): the watcher
    // is the one participant who would otherwise never see the operator intervene.
    // A sender with a run is an agent, and agent traffic inside a child is the
    // child's own business. A sender without a run is the operator (an external
    // CLI/MCP session or a remote intent; see `sender_context`).
    if sender_run_id.is_none() {
        for plan in &planned {
            let addressed_uri = &plan.recipient.addressed_uri;
            match crate::orchestrator::attention_delivery::create_catchup_pushes_for_watchers(
                &owning_db,
                addressed_uri,
                &message_id,
            )
            .await
            {
                Ok(created) if created > 0 => orch.notifier.emit_change("attention_pushes"),
                Ok(_) => {}
                Err(error) => {
                    log::warn!("catch-up push creation for {addressed_uri} failed: {error}")
                }
            }
        }
    }

    // Nudge only when the effective wake still wakes an idle recipient. A muted
    // source was downgraded to `Passive` and rides along on the recipient's next
    // run. `nudge_for_direct_message` is the shared resume ladder: an idle
    // recipient resumes and drains the push; an `interrupt` on an active
    // recipient stops the turn so the turn-end flush delivers it; a non-interrupt
    // active recipient is left for the event-boundary push drain; an `interrupt`
    // to a recipient parked only on `waitFor` ends that wait; any other
    // self-suspended recipient is held and the direct rides along when its own
    // work resolves. The message row and push are durable either way, so the
    // send succeeds and the reply says which of these happened.
    let from = sender_uri.unwrap_or_else(|| sender_name.clone());
    let outcomes: Vec<(String, Option<Result<delivery::NudgeOutcome, String>>)> = planned
        .iter()
        .map(|plan| {
            let wake = plan.wake.wakes_idle().then(|| {
                let message = delivery::DirectMessageRef {
                    from: from.clone(),
                    message_id: message_id.clone(),
                };
                let outcome = delivery::nudge_for_direct_message(
                    orch,
                    &plan.recipient.job_id,
                    urgency,
                    &message,
                );
                if let Err(e) = &outcome {
                    log::warn!(
                        "direct message wake for {} failed: {}",
                        plan.recipient.addressed_uri,
                        e
                    );
                }
                outcome
            });
            // A wake below the one asked for is a mute; a sender who asked
            // for a non-waking urgency was not muted by anyone.
            let wake = match wake {
                None if plan.wake >= requested_wake => Some(Ok(delivery::NudgeOutcome::NotWaking)),
                other => other,
            };
            (plan.recipient.addressed_uri.clone(), wake)
        })
        .collect();

    if planned.len() == 1 {
        let label = planned[0].recipient.address.short_label();
        let (_, wake) = outcomes.into_iter().next().expect("one recipient");
        return Ok(direct_message_reply(&label, wake));
    }
    let names = planned
        .iter()
        .map(|plan| plan.recipient.addressed_uri.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let mut reply = format!(
        "Sent direct message {} to {names}.",
        message_uri(project_key, &message_id)
    );
    for (addressed_uri, wake) in outcomes {
        reply.push_str(&format!("\n- {addressed_uri}: {}", recipient_outcome(wake)));
    }
    Ok(reply)
}

/// What the sender of a direct message is told: that it was sent, and what the
/// recipient does with it now. `wake` is `None` when the recipient muted the
/// sender, so nothing was woken.
fn direct_message_reply(
    recipient_label: &str,
    wake: Option<Result<delivery::NudgeOutcome, String>>,
) -> String {
    format!(
        "Sent direct message to {recipient_label}. {}",
        recipient_outcome(wake)
    )
}

/// One recipient's half of the reply: what it does with the message now.
fn recipient_outcome(wake: Option<Result<delivery::NudgeOutcome, String>>) -> String {
    use delivery::NudgeOutcome;
    match wake {
        None => "The recipient muted you; it will read this on its next turn.".to_string(),
        Some(Err(error)) => format!("The wake failed: {error}; the message stays queued."),
        Some(Ok(NudgeOutcome::NotWaking)) => {
            "It was sent passively and wakes nothing; the recipient will read it on its next turn."
                .to_string()
        }
        Some(Ok(NudgeOutcome::Refused(refusal))) => format!(
            "It cannot take a turn: {}; the message stays queued.",
            refusal.describe()
        ),
        Some(Ok(NudgeOutcome::QueuedForActiveTurn)) => {
            "Your message is queued. The recipient currently has an active turn; delivery is not \
             yet confirmed."
                .to_string()
        }
        Some(Ok(NudgeOutcome::Interrupted)) => {
            "Its turn was interrupted and it is resuming with your message.".to_string()
        }
        Some(Ok(NudgeOutcome::Resumed)) => {
            "A turn was dispatched for it, carrying your message.".to_string()
        }
        Some(Ok(NudgeOutcome::SupersededWait { labels })) => format!(
            "Its wait ({}) was ended early and it is resuming with your message.",
            labels.join(", ")
        ),
        Some(Ok(NudgeOutcome::Held(hold))) => {
            format!("It has not started a turn: {}.", hold.render())
        }
        Some(Ok(NudgeOutcome::Declined(reason))) => {
            format!("It was not woken: {reason}; the message stays queued.")
        }
    }
}

#[cfg(test)]
mod reply_tests {
    use super::direct_message_reply;
    use crate::messages::delivery::{DeliveryRefusal, Hold, NudgeOutcome};

    fn reply(wake: Option<Result<NudgeOutcome, String>>) -> String {
        direct_message_reply("builder", wake)
    }

    /// The sender always learns what the recipient does with the message, and
    /// the established prefix stays first so existing readers keep matching.
    #[test]
    fn every_outcome_tells_the_sender_what_happened() {
        let cases = [
            (
                Some(Ok(NudgeOutcome::Resumed)),
                "A turn was dispatched for it, carrying your message.",
            ),
            (
                Some(Ok(NudgeOutcome::QueuedForActiveTurn)),
                "delivery is not yet confirmed",
            ),
            (
                Some(Ok(NudgeOutcome::SupersededWait {
                    labels: vec!["waitFor: checks verdict rust-tests".into()],
                })),
                "Its wait (waitFor: checks verdict rust-tests) was ended early and it is resuming with your message.",
            ),
            (
                Some(Ok(NudgeOutcome::Held(Hold::Wait {
                    labels: vec!["waitFor: duration".into()],
                    supersedable: true,
                    notes: Vec::new(),
                }))),
                "It has not started a turn: it is parked on its own `waitFor: duration`",
            ),
            (
                Some(Ok(NudgeOutcome::Refused(DeliveryRefusal::Cancelled))),
                "It cannot take a turn: it is cancelled",
            ),
            (
                Some(Err("the active turn has not attached to a stoppable run yet".into())),
                "The wake failed: the active turn has not attached to a stoppable run yet; the message stays queued.",
            ),
            (None, "The recipient muted you"),
        ];
        for (wake, expected) in cases {
            let said = reply(wake);
            assert!(
                said.starts_with("Sent direct message to builder. "),
                "{said}"
            );
            assert!(said.contains(expected), "{said}");
        }
    }
}
