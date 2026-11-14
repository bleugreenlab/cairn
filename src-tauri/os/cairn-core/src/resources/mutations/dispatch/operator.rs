//! Writes to `cairn://operator`: an agent sending the operator a message, and
//! the operator answering one, or marking it seen, from the app.

use cairn_db::turso::{params, Value};
use cairn_uri::{parse_uri, CairnResource};

use super::super::{build_failure, ResourceMutationResult};
use super::append_payload;
use crate::channels::{agent_message, operator_message, provider_notify_fit};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::operator::messages::{self, HeadlineLink, NewDelivery, NewMessage, MAX_CHARS};
use crate::operator::{AnsweredVia, MessageFilter, SeenVia, Via};
use crate::orchestrator::Orchestrator;
use crate::resources::channels::{check_send_length, provider_summaries};
use crate::storage::RowExt;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let fail = |error: String| build_failure(index, item, error);
    match (resource, item.mode) {
        (CairnResource::Operator, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            send(orch, request, payload, dry_run)
                .await
                .map(Some)
                .map_err(fail)
        }
        (CairnResource::OperatorMessage { id }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            answer(orch, request, id, payload, dry_run)
                .await
                .map(Some)
                .map_err(fail)
        }
        (CairnResource::OperatorMessage { id }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            mark_seen(orch, request, id, payload, dry_run)
                .await
                .map(Some)
                .map_err(fail)
        }
        _ => Ok(None),
    }
}

/// A text field held to the message limit: refused by name and by how far
/// over, never truncated.
fn bounded_text<'a>(
    payload: &'a serde_json::Value,
    key: &str,
    required: bool,
) -> Result<Option<&'a str>, String> {
    let text = match payload.get(key) {
        None | Some(serde_json::Value::Null) if !required => return Ok(None),
        Some(serde_json::Value::String(text)) if !text.trim().is_empty() => text.as_str(),
        _ if required => {
            return Err(format!(
                "payload.{key} is required and must be a non-empty string"
            ))
        }
        _ => return Err(format!("payload.{key} must be a non-empty string")),
    };
    let count = text.chars().count();
    if count > MAX_CHARS {
        return Err(format!(
            "{key} is {count} characters; cairn://operator takes at most {MAX_CHARS} ({} over). Shorten the claim; it is never truncated.",
            count - MAX_CHARS
        ));
    }
    Ok(Some(text))
}

/// The channel `via` names, admitted only when the operator offers it: a
/// purpose line, and for a phone channel his own conversation configured.
async fn admit_via(orch: &Orchestrator, via: &str) -> Result<Via, String> {
    const VIAS: &str =
        "cairn://channels/app, cairn://channels/imessage, or cairn://channels/telegram";
    let provider = match parse_uri(via) {
        Some(CairnResource::ChannelsProvider { provider }) => provider,
        _ => {
            return Err(format!(
                "via must be a channel cairn://operator lists ({VIAS}); got '{via}'"
            ))
        }
    };
    let Some(channel) = Via::parse(&provider) else {
        return Err(format!(
            "{via} is not a channel to the operator: Discord has no single conversation with him, only per-project and per-thread channels. Use {VIAS}."
        ));
    };
    let summary = provider_summaries(orch)
        .await
        .into_iter()
        .find(|summary| summary.provider == channel.as_str())
        .ok_or_else(|| format!("{via} has no channel row"))?;
    if summary.purpose.is_none() {
        return Err(format!(
            "{via} has no purpose line, so it is not offered. The operator writes one per channel (the purpose patch on cairn://channels, from the desktop); until then send through a channel cairn://operator lists with one."
        ));
    }
    if !summary.enabled || !summary.configured {
        return Err(format!(
            "{via} has a purpose line but is not configured: it is {} and {}. Send through another channel cairn://operator offers.",
            if summary.enabled { "enabled" } else { "disabled" },
            if summary.configured {
                "has the operator's conversation"
            } else {
                "names no conversation for the operator"
            }
        ));
    }
    Ok(channel)
}

/// The sender's own thread, by project and name, with its ids.
struct SenderThread {
    thread_id: String,
    project_id: String,
    project: String,
    name: String,
}

async fn sender_thread(orch: &Orchestrator, home: &str) -> Result<Option<SenderThread>, String> {
    let Some(CairnResource::Thread { project, name, .. }) = parse_uri(home) else {
        return Ok(None);
    };
    let project = cairn_uri::canonical_project(&project);
    let db = orch.db.for_project(&project).await;
    let row = db
        .query_opt(
            "SELECT t.id, t.project_id FROM threads t JOIN projects p ON p.id = t.project_id
              WHERE LOWER(p.key) = ?1 AND t.name = ?2",
            params![project.clone(), name.clone()],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(row.map(|(thread_id, project_id)| SenderThread {
        thread_id,
        project_id,
        project,
        name,
    }))
}

/// What `headline` asks for, validated before anything is written.
enum HeadlineRequest {
    Existing(HeadlineLink),
    New {
        line: String,
        link: String,
        until: Option<String>,
    },
}

async fn headline_request(
    value: &serde_json::Value,
    home: &str,
    thread: Option<&SenderThread>,
) -> Result<HeadlineRequest, String> {
    let Some(thread) = thread else {
        return Err(format!(
            "payload.headline links a headline of the sender's own thread, and {home} is not a thread, so it has none"
        ));
    };
    match value {
        serde_json::Value::String(uri) => {
            let refusal = || {
                format!(
                    "payload.headline must be a row of your own headlines, cairn://p/{}/{}/db/headlines/ID; got '{uri}'",
                    thread.project, thread.name
                )
            };
            let Some(CairnResource::ThreadDbRow {
                project,
                name,
                table,
                pk,
            }) = parse_uri(uri)
            else {
                return Err(refusal());
            };
            if cairn_uri::canonical_project(&project) != thread.project
                || name != thread.name
                || table != "headlines"
            {
                return Err(refusal());
            }
            let row_id = pk.parse::<i64>().map_err(|_| refusal())?;
            let store = crate::threads::db_store::open_existing(&thread.thread_id)
                .await?
                .ok_or_else(|| format!("{uri} names no headline: your thread has written none"))?;
            let live = store
                .query_opt_i64(
                    "SELECT COUNT(*) FROM headlines
                      WHERE id = ?1 AND until > strftime('%Y-%m-%dT%H:%M:%SZ','now')",
                    params![row_id],
                )
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or(0);
            if live == 0 {
                return Err(format!(
                    "{uri} is not a live headline of yours: it does not exist or its until has passed"
                ));
            }
            Ok(HeadlineRequest::Existing(HeadlineLink {
                uri: cairn_uri::build_db_row_uri(
                    &format!("{}/db", cairn_uri::build_thread_uri(&thread.project, &thread.name)),
                    "headlines",
                    &row_id.to_string(),
                ),
                thread_id: thread.thread_id.clone(),
                row_id,
            }))
        }
        serde_json::Value::Object(fields) => {
            if let Some(key) = fields
                .keys()
                .find(|key| !matches!(key.as_str(), "line" | "ref" | "until"))
            {
                return Err(format!(
                    "payload.headline takes line, ref and until; '{key}' is not one of them"
                ));
            }
            let text = |key: &str| {
                fields
                    .get(key)
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string)
            };
            Ok(HeadlineRequest::New {
                line: text("line").ok_or("payload.headline.line is required")?,
                link: text("ref").ok_or("payload.headline.ref is required")?,
                until: text("until"),
            })
        }
        _ => Err(
            "payload.headline is a headline row URI of your thread, or {line, ref, until} to write one"
                .to_string(),
        ),
    }
}

/// Write a new headline through the thread store's one write path, so its
/// table triggers hold it to the cap, ref, and until rules and its change is
/// announced like any other headline's.
async fn write_headline(
    orch: &Orchestrator,
    thread: &SenderThread,
    line: &str,
    link: &str,
    until: Option<&str>,
) -> Result<HeadlineLink, String> {
    use crate::threads::db_store::{
        self, RowAction, RowKey, RowTarget, WriteScope, WriteStatement,
    };
    let (sql, params) = match until {
        Some(until) => (
            "INSERT INTO headlines (line, ref, until) VALUES (?1, ?2, ?3) RETURNING id",
            vec![
                Value::Text(line.to_string()),
                Value::Text(link.to_string()),
                Value::Text(until.to_string()),
            ],
        ),
        None => (
            "INSERT INTO headlines (line, ref) VALUES (?1, ?2) RETURNING id",
            vec![Value::Text(line.to_string()), Value::Text(link.to_string())],
        ),
    };
    let scope = WriteScope {
        thread_id: thread.thread_id.clone(),
        project_id: thread.project_id.clone(),
        project: thread.project.clone(),
        thread_name: thread.name.clone(),
    };
    let outcome = db_store::apply_write(
        &scope,
        vec![WriteStatement {
            sql: sql.to_string(),
            params,
            row: Some(RowTarget {
                table: "headlines".to_string(),
                action: RowAction::Insert,
                key: RowKey::Returned,
                position: None,
            }),
        }],
        orch.services.emitter.as_ref(),
    )
    .await?;
    let uri = outcome
        .rows
        .into_iter()
        .flatten()
        .next()
        .ok_or("the headline was written but returned no row")?;
    let row_id = uri
        .rsplit('/')
        .next()
        .and_then(|id| id.parse::<i64>().ok())
        .ok_or_else(|| format!("the headline row {uri} has no numeric id"))?;
    Ok(HeadlineLink {
        uri,
        thread_id: thread.thread_id.clone(),
        row_id,
    })
}

/// The operator's own conversation on a phone channel, exactly as that
/// provider's router is built with it.
fn operator_conversation(orch: &Orchestrator, via: Via) -> String {
    let channels = crate::config::settings::load_settings(&orch.config_dir).channels;
    match via {
        Via::IMessage => channels.imessage.to,
        Via::Telegram => channels.telegram.chat_id,
        Via::App => String::new(),
    }
}

/// The fit of the message as it will go on the wire. The running provider's
/// own renderer answers when it can; while it is not running, the declared
/// Telegram limit is checked against the header and body together, as
/// Telegram's renderer budgets them.
fn transport_fit(via: Via, header: &str, body: &str) -> Result<(), String> {
    let (overflow, limit) =
        provider_notify_fit(via.as_str(), header, body).unwrap_or_else(|| match via {
            Via::Telegram => {
                let limit = cairn_channels::telegram::TELEGRAM_TEXT_LIMIT;
                let length = header.chars().count() + 2 + body.chars().count();
                (length.saturating_sub(limit), Some(limit))
            }
            Via::IMessage | Via::App => (0, None),
        });
    check_send_length(via.as_str(), overflow, limit)
}

/// An agent sending the operator a message.
///
/// Every refusal comes before anything is written, in a fixed order: the
/// sender (a live agent, never the payload), the content and default, the
/// channel, the transport's limit, then the headline. A new headline is
/// written first and the message second, so a failure between them leaves a
/// valid headline and no message, never a message linking nothing.
async fn send(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    if request.is_desktop_operator()
        || request.run_id.as_deref().is_none_or(str::is_empty)
        || request.is_unverified_local()
    {
        return Err("Refused: a message to the operator is sent by a live thread or issue node, and this request speaks for no run.".to_string());
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            format!("Only a live agent node may send the operator a message: {error}")
        })?;
    let content = bounded_text(payload, "content", true)?.expect("required");
    let default = bounded_text(payload, "default", false)?;
    let via_uri = payload
        .get("via")
        .and_then(serde_json::Value::as_str)
        .ok_or("payload.via is required: the channel's URI, as cairn://operator lists it")?;
    let via = admit_via(orch, via_uri).await?;
    let header = agent_message::context_header(&agent.home);
    let body = operator_message::render(content, default);
    if via.is_phone() {
        transport_fit(via, &header, &body)?;
    }
    let thread = sender_thread(orch, &agent.home).await?;
    let headline = match payload.get("headline") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(headline_request(value, &agent.home, thread.as_ref()).await?),
    };
    let project_id = match &thread {
        Some(thread) => thread.project_id.clone(),
        None => {
            let project = parse_uri(&agent.home)
                .and_then(|home| home.project().map(cairn_uri::canonical_project))
                .ok_or_else(|| format!("{} names no project", agent.home))?;
            orch.db
                .local
                .query_opt_text(
                    "SELECT id FROM projects WHERE LOWER(key) = ?1",
                    params![project.clone()],
                )
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("no project {project} holds {}", agent.home))?
        }
    };
    if dry_run {
        return Ok(format!(
            "Would send a message to the operator via {} as {}.",
            via.display_name(),
            agent.home
        ));
    }

    let link = match headline {
        None => None,
        Some(HeadlineRequest::Existing(link)) => Some(link),
        Some(HeadlineRequest::New { line, link, until }) => Some(
            write_headline(
                orch,
                thread.as_ref().expect("a new headline needs a thread"),
                &line,
                &link,
                until.as_deref(),
            )
            .await?,
        ),
    };
    let now = chrono::Utc::now();
    let id = uuid::Uuid::new_v4().to_string();
    let message = NewMessage {
        id: id.clone(),
        project_id: project_id.clone(),
        sender_home: agent.home.clone(),
        sender_job_id: agent.job_id.clone(),
        via,
        content: content.to_string(),
        default_text: default.map(str::to_string),
        headline: link,
        created_at: now.timestamp(),
    };
    let delivery = via.is_phone().then(|| NewDelivery {
        channel: via.as_str(),
        conversation: operator_conversation(orch, via),
        rendered_text: body.clone(),
        created_at_ms: now.timestamp_millis(),
    });
    messages::insert(&orch.db.local, &message, delivery.as_ref()).await?;
    if via.is_phone() {
        operator_message::submit(via.as_str(), &id);
    }
    if default.is_some() {
        orch.operator_notifications_cache
            .invalidate(&project_id, "operator-message");
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "operator_messages", "action": "insert"}),
    );
    let stored = messages::get(&orch.db.local, &id)
        .await?
        .ok_or_else(|| format!("message {id} was written but cannot be read back"))?;
    let outbound = messages::delivery(&orch.db.local, &stored).await?;
    let state = messages::state(&stored, outbound.as_ref());
    Ok(format!(
        "Sent {} via {} ({}){}.",
        stored.uri(),
        via.display_name(),
        state.word(),
        stored
            .headline
            .as_deref()
            .map(|headline| format!(", linked to {headline}"))
            .unwrap_or_default()
    ))
}

/// The operator answering a message from the app: recorded as the answer,
/// then delivered to its sender as his own words (a thread through its
/// append, an issue node through its job), headed by the message it answers.
///
/// Recorded first so that only the write that closes the entry delivers: a
/// record that fails, or a concurrent answer that won, delivers nothing. A
/// delivery that fails after the record is this write's failure, naming that
/// the entry stays answered; the row is not reopened, since reopening could
/// race a second answer into a double delivery.
async fn answer(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    id: &str,
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    if !request.is_desktop_operator() {
        return Err("Refused: only the operator answers a message, from the app; an agent reads its answer when it arrives.".to_string());
    }
    let content = payload
        .get("content")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|content| !content.is_empty())
        .ok_or("payload.content is required and must be a non-empty string")?;
    let uri = messages::message_uri(id);
    let message = messages::get(&orch.db.local, id)
        .await?
        .ok_or_else(|| format!("{uri} names no message to the operator"))?;
    if message.answered_at.is_some() {
        return Err(format!("{uri} is already answered"));
    }
    if dry_run {
        return Ok(format!("Would answer {uri} to {}.", message.sender_home));
    }
    if crate::operator::record_answered(
        orch,
        id,
        AnsweredVia::App,
        None,
        chrono::Utc::now().timestamp(),
    )
    .await?
    .is_none()
    {
        return Err(format!("{uri} is already answered"));
    }
    let answer_ref = messages::deliver_answer(
        orch,
        &message,
        "operator",
        content,
        messages::AnswerHeader::Reply,
    )
    .await
    .map_err(|error| {
            format!(
                "{uri} is recorded as answered, but your reply did not reach {}: {error}. The entry stays answered and nothing was delivered; send the reply to {} another way.",
                message.sender_home, message.sender_home
            )
        })?;
    if let Some(answer_ref) = answer_ref {
        if let Err(error) = messages::attach_answer_ref(&orch.db.local, id, &answer_ref).await {
            log::warn!("answer to {uri} was delivered as {answer_ref}, but {error}");
        }
    }
    Ok(format!(
        "Answered {uri}; delivered to {}.",
        message.sender_home
    ))
}

/// The operator having seen a message in the app. The app writes this after
/// it has rendered the message, so the door's read stays a pure read; the
/// transition and its one passive notice to the sender are `record_seen`'s,
/// shared with the read receipt and the headline cursor.
async fn mark_seen(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    id: &str,
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    if !request.is_desktop_operator() {
        return Err("Refused: only the operator marks a message seen, by opening it in the app; an agent's read never does.".to_string());
    }
    if payload.get("seen") != Some(&serde_json::Value::Bool(true)) {
        return Err(
            "payload.seen must be true: a message is marked seen, never unseen".to_string(),
        );
    }
    let uri = messages::message_uri(id);
    if messages::get(&orch.db.local, id).await?.is_none() {
        return Err(format!("{uri} names no message to the operator"));
    }
    if dry_run {
        return Ok(format!("Would mark {uri} seen in the app."));
    }
    let moved = crate::operator::record_seen(
        orch,
        MessageFilter::Ids(vec![id.to_string()]),
        SeenVia::App,
        chrono::Utc::now().timestamp(),
    )
    .await?;
    Ok(if moved.is_empty() {
        format!("{uri} was already seen; nothing changed.")
    } else {
        format!("Marked {uri} seen in the app.")
    })
}

#[cfg(test)]
mod tests {
    use super::super::dispatch_resource_change;
    use crate::db::DbState;
    use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
    use crate::operator::{messages, SeenVia, State};
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    const SETTINGS: &str = "channels:\n  app:\n    purpose: everything else\n  imessage:\n    enabled: true\n    to: '+15551234567'\n    purpose: blocking me or costing by the hour\n  telegram:\n    enabled: true\n    chatId: '-100'\n";

    /// A project `prj` with threads `general` and `other`, each with a live
    /// run, their stores in a temporary directory, and `SETTINGS`.
    async fn orchestrator(name: &str) -> Orchestrator {
        orchestrator_capturing(name).await.0
    }

    /// [`orchestrator`], handing back the emitter it announces through.
    async fn orchestrator_capturing(name: &str) -> (Orchestrator, Arc<CapturingEmitter>) {
        let emitter = Arc::new(CapturingEmitter::new());
        let dir = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open_migrated_for_test(dir.join("t.db"))
            .await
            .unwrap();
        local
            .execute_script(
                "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
                 INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
                 VALUES ('p','w','P','prj','/tmp/p',1,1);
                 INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                 VALUES ('t-general', 'p', 'general', 'active', 1, 1),
                        ('t-other', 'p', 'other', 'active', 1, 1);",
            )
            .await
            .unwrap();
        for (thread, run) in [("t-general", "r-live"), ("t-other", "r-other")] {
            crate::threads::db_store::use_test_path(
                thread,
                dir.join(format!("{name}-{thread}.db")),
            );
            let session = crate::threads::ensure_thread_session(&local, thread)
                .await
                .unwrap();
            local
                .execute_script(format!(
                    "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                     VALUES ('{run}', 'p', '{session}', 'live', 1, 1);"
                ))
                .await
                .unwrap();
        }
        let config_dir = dir.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            crate::config::settings::get_settings_path(&config_dir),
            SETTINGS,
        )
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(dir.join("search")).unwrap());
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(
                TestServicesBuilder::new()
                    .with_emitter(emitter.clone())
                    .build(),
            ),
            config_dir,
        )
        .build();
        (orch, emitter)
    }

    fn request(run_id: Option<&str>) -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: run_id.map(str::to_string),
            tool: "change".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
            origin: None,
        }
    }

    async fn send(
        orch: &Orchestrator,
        run_id: Option<&str>,
        payload: serde_json::Value,
    ) -> Result<String, String> {
        let item = ChangeItem {
            target: "cairn://operator".into(),
            mode: ChangeMode::Append,
            payload: Some(payload),
        };
        dispatch_resource_change(orch, &request(run_id), 0, &item, false)
            .await
            .map(|applied| applied.summary)
            .map_err(|failure| failure.error)
    }

    /// The id a successful send reports.
    fn sent_id(summary: &str) -> String {
        summary
            .split("cairn://operator/messages/")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .unwrap()
            .to_string()
    }

    async fn state_of(orch: &Orchestrator, id: &str) -> State {
        let message = messages::get(&orch.db.local, id).await.unwrap().unwrap();
        let row = messages::delivery(&orch.db.local, &message).await.unwrap();
        messages::state(&message, row.as_ref())
    }

    async fn outbound_rows(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_opt_i64("SELECT COUNT(*) FROM channel_outbound", ())
            .await
            .unwrap()
            .unwrap_or_default()
    }

    async fn entries(orch: &Orchestrator) -> Vec<crate::notifications::OperatorNotification> {
        crate::notifications::derive_snapshot(
            &orch.db.local,
            &orch.db.local,
            &crate::notifications::HoldRegistry::default(),
            "p",
            chrono::Utc::now().timestamp(),
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .filter(|entry| entry.kind == crate::notifications::NotificationKind::OperatorMessage)
        .collect()
    }

    #[tokio::test]
    async fn a_thread_reads_its_channels_and_only_its_own_messages() {
        let orch = orchestrator("render").await;
        send(
            &orch,
            Some("r-live"),
            serde_json::json!({"content": "mine", "via": "cairn://channels/app"}),
        )
        .await
        .unwrap();
        send(
            &orch,
            Some("r-other"),
            serde_json::json!({"content": "theirs", "via": "cairn://channels/app"}),
        )
        .await
        .unwrap();
        let body =
            crate::resources::operator::render_operator(&orch, &request(Some("r-live")), &[]).await;
        for expected in [
            "- App (cairn://channels/app): everything else",
            "- iMessage (cairn://channels/imessage): blocking me or costing by the hour",
            "- Telegram (cairn://channels/telegram): not offered: no purpose line",
            "last read headlines: never",
            "last operator message: never",
            "## Your messages (cairn://p/prj/general)",
            "delivered, not yet seen",
            "content: mine",
        ] {
            assert!(body.contains(expected), "{expected}: {body}");
        }
        assert!(!body.contains("theirs"), "{body}");
        assert!(
            !body.contains("## Discord") && !body.contains("Discord ("),
            "{body}"
        );
    }

    #[tokio::test]
    async fn an_app_message_is_in_the_app_only_and_waits_only_with_a_default() {
        let orch = orchestrator("app").await;
        let plain = sent_id(
            &send(
                &orch,
                Some("r-live"),
                serde_json::json!({"content": "FYI", "via": "cairn://channels/app"}),
            )
            .await
            .unwrap(),
        );
        let body =
            crate::resources::operator::render_message(&orch, &request(Some("r-live")), &plain)
                .await;
        assert!(body.contains("- content: FYI"), "{body}");
        assert!(entries(&orch).await.is_empty());
        assert_eq!(outbound_rows(&orch).await, 0);

        let waiting = sent_id(
            &send(
                &orch,
                Some("r-live"),
                serde_json::json!({
                    "content": "Rename the crate?",
                    "via": "cairn://channels/app",
                    "default": "Assuming yes, I rename it Friday"
                }),
            )
            .await
            .unwrap(),
        );
        let open = entries(&orch).await;
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].key, format!("operator_message:{waiting}"));
        assert!(open[0].is_open());
        assert_eq!(
            open[0].action,
            Some(crate::notifications::NotificationAction::Reply {
                message: messages::message_uri(&waiting),
                default_text: "Assuming yes, I rename it Friday".to_string(),
            })
        );
        crate::operator::record_answered(
            &orch,
            &waiting,
            crate::operator::AnsweredVia::App,
            None,
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
        let resolved = entries(&orch).await;
        assert_eq!(
            resolved[0].state,
            crate::notifications::NotificationState::Resolved
        );
    }

    #[tokio::test]
    async fn refusals_name_what_would_be_accepted() {
        let orch = orchestrator("refusals").await;
        let app = "cairn://channels/app";
        for (run, payload, expected) in [
            (
                Some("r-live"),
                serde_json::json!({"content": "x".repeat(401), "via": app}),
                "400 (1 over)",
            ),
            (
                Some("r-live"),
                serde_json::json!({"content": "x", "via": app, "default": "y".repeat(402)}),
                "default is 402 characters",
            ),
            (
                Some("r-live"),
                serde_json::json!({"content": "x", "via": "cairn://channels/telegram"}),
                "has no purpose line, so it is not offered",
            ),
            (
                Some("r-live"),
                serde_json::json!({"content": "x", "via": "cairn://channels/discord"}),
                "Discord has no single conversation",
            ),
            (
                None,
                serde_json::json!({"content": "x", "via": app}),
                "speaks for no run",
            ),
            (
                Some("r-live"),
                serde_json::json!({
                    "content": "x",
                    "via": app,
                    "headline": "cairn://p/prj/other/db/headlines/1"
                }),
                "must be a row of your own headlines",
            ),
        ] {
            let error = send(&orch, run, payload).await.unwrap_err();
            assert!(error.contains(expected), "{expected}: {error}");
        }
        assert!(messages::all(&orch.db.local, 10).await.unwrap().is_empty());
        assert_eq!(outbound_rows(&orch).await, 0);
        assert!(error_names(&super::transport_fit(
            crate::operator::Via::Telegram,
            "[Cairn · prj/general]",
            &"x".repeat(4_100),
        )));
        fn error_names(result: &Result<(), String>) -> bool {
            result
                .as_ref()
                .is_err_and(|error| error.contains("telegram's 4096-character message limit"))
        }
    }

    /// `headline: {line, ref, until}` writes the row in the sender's store and
    /// links it; the cursor passing it marks the message seen once, with one
    /// passive notice to the sender.
    #[tokio::test]
    async fn a_new_headline_is_linked_and_the_cursor_passing_it_sees_the_message() {
        let orch = orchestrator("cursor").await;
        let id = sent_id(
            &send(
                &orch,
                Some("r-live"),
                serde_json::json!({
                    "content": "Merging retires the old home",
                    "via": "cairn://channels/imessage",
                    "default": "I merge at 5pm",
                    "headline": {"line": "Merging today", "ref": "cairn://p/prj/general", "until": "2099-01-01T00:00:00Z"}
                }),
            )
            .await
            .unwrap(),
        );
        let message = messages::get(&orch.db.local, &id).await.unwrap().unwrap();
        assert_eq!(
            message.headline.as_deref(),
            Some("cairn://p/prj/general/db/headlines/1")
        );
        let store = crate::threads::db_store::open_existing("t-general")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .query_opt_text("SELECT line FROM headlines WHERE id = 1", ())
                .await
                .unwrap()
                .as_deref(),
            Some("Merging today")
        );
        let row = messages::delivery(&orch.db.local, &message)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.kind, "operator");
        assert_eq!(row.conversation, "+15551234567");
        assert_eq!(row.status, "pending");

        let seen = crate::operator::messages::cursor_moved(&orch, "t-general", 1, 1_000)
            .await
            .unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            state_of(&orch, &id).await,
            State::Seen {
                via: Some(SeenVia::Cursor),
                since: 1_000
            }
        );
        let notice = format!(
            "reconcile:{}",
            crate::operator::messages::seen_notice_key(&id)
        );
        let content = orch
            .db
            .local
            .query_opt_text(
                "SELECT content FROM messages WHERE id = ?1",
                cairn_db::turso::params![notice],
            )
            .await
            .unwrap()
            .expect("one passive notice to the sender");
        assert!(
            content.contains(&messages::message_uri(&id)) && content.contains("I merge at 5pm"),
            "{content}"
        );
        assert!(
            crate::operator::messages::cursor_moved(&orch, "t-general", 1, 2_000)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// A read receipt fed to a sent row stamps `read_at` once and sees the
    /// message, without touching its delivery state.
    #[tokio::test]
    async fn a_read_receipt_sees_the_message() {
        let orch = orchestrator("receipt").await;
        let id = sent_id(
            &send(
                &orch,
                Some("r-live"),
                serde_json::json!({"content": "Blocked on the cert", "via": "cairn://channels/imessage"}),
            )
            .await
            .unwrap(),
        );
        const GUID: &str = "8F0D3C55-2E3B-4E7B-9C61-0F6A1B2C3D4E";
        orch.db
            .local
            .execute(
                "UPDATE channel_outbound SET status = 'sent', provider_guid = ?2, sent_at = ?3 WHERE id = ?1",
                cairn_db::turso::params![id.clone(), GUID, chrono::Utc::now().timestamp_millis()],
            )
            .await
            .unwrap();
        assert_eq!(state_of(&orch, &id).await, State::Delivered);
        let unread = crate::channels::ledger::unread_operator_sends(&orch.db.local, "imessage", 0)
            .await
            .unwrap();
        assert_eq!(unread.len(), 1);
        let seen = crate::channels::imessage_receipts::apply(
            &orch,
            "imessage",
            &unread,
            &[(GUID.to_string(), 1_762_307_200_000)],
        )
        .await
        .unwrap();
        assert_eq!(seen, vec![id.clone()]);
        assert_eq!(
            crate::channels::ledger::read_at(&orch.db.local, &id)
                .await
                .unwrap(),
            Some(1_762_307_200_000)
        );
        assert_eq!(
            state_of(&orch, &id).await,
            State::Seen {
                via: Some(SeenVia::Receipt),
                since: 1_762_307_200
            }
        );
        let row = crate::channels::ledger::get_by_id(&orch.db.local, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((row.status.as_str(), row.attempts), ("sent", 0));
    }

    /// Through the door the operator opens a message (seen in the app) and
    /// answers it; an agent's read sees nothing and its append is refused.
    #[tokio::test]
    async fn the_operator_opens_and_answers_through_the_door() {
        let orch = orchestrator("door").await;
        let id = sent_id(
            &send(
                &orch,
                Some("r-live"),
                serde_json::json!({"content": "Rename?", "via": "cairn://channels/app", "default": "I rename Friday"}),
            )
            .await
            .unwrap(),
        );
        let uri = messages::message_uri(&id);
        crate::resources::operator::render_message(&orch, &request(Some("r-live")), &id).await;
        assert_eq!(
            state_of(&orch, &id).await,
            State::Delivered,
            "an agent read sees nothing"
        );
        let agent_answer = dispatch_resource_change(
            &orch,
            &request(Some("r-live")),
            0,
            &ChangeItem {
                target: uri.clone(),
                mode: ChangeMode::Append,
                payload: Some(serde_json::json!({"content": "no"})),
            },
            false,
        )
        .await
        .unwrap_err()
        .error;
        assert!(
            agent_answer.contains("only the operator answers a message"),
            "{agent_answer}"
        );

        let opened = crate::channels::read_as_operator(&orch, &uri)
            .await
            .unwrap();
        assert!(opened.contains("- content: Rename?"), "{opened}");
        let answered = crate::channels::write_as_operator(
            &orch,
            &uri,
            "append",
            Some(serde_json::json!({"content": "yes, rename it"})),
        )
        .await
        .unwrap();
        assert!(answered.starts_with("Answered"), "{answered}");
        assert!(matches!(
            state_of(&orch, &id).await,
            State::Answered {
                via: Some(crate::operator::AnsweredVia::App),
                ..
            }
        ));
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT content FROM messages WHERE channel_type = 'thread' AND channel_id = 't-general' ORDER BY created_at DESC LIMIT 1",
                    (),
                )
                .await
                .unwrap(),
            Some(format!("[Reply to {uri}] yes, rename it"))
        );
        assert!(entries(&orch).await.iter().all(|entry| !entry.is_open()));
    }

    /// The operator's read through the door writes nothing: no `seen_at`, no
    /// notice to the sender, no announcement. The app's `patch {seen:true}`
    /// after it renders records seen in the app once, with one passive notice;
    /// a repeat moves nothing, and an agent may not write it.
    #[tokio::test]
    async fn the_operator_read_writes_nothing_and_the_seen_write_records_it_once() {
        let (orch, emitter) = orchestrator_capturing("pure-read").await;
        let id = sent_id(
            &send(
                &orch,
                Some("r-live"),
                serde_json::json!({"content": "Rename?", "via": "cairn://channels/app", "default": "I rename Friday"}),
            )
            .await
            .unwrap(),
        );
        let uri = messages::message_uri(&id);
        let notices = || async {
            orch.db
                .local
                .query_opt_i64(
                    "SELECT COUNT(*) FROM messages WHERE id = ?1",
                    cairn_db::turso::params![format!(
                        "reconcile:{}",
                        messages::seen_notice_key(&id)
                    )],
                )
                .await
                .unwrap()
                .unwrap_or_default()
        };
        let announced = || emitter.events_named("db-change").len();

        let before = announced();
        let opened = crate::channels::read_as_operator(&orch, &uri)
            .await
            .unwrap();
        assert!(opened.contains("delivered, not yet seen"), "{opened}");
        let unread = messages::get(&orch.db.local, &id).await.unwrap().unwrap();
        assert_eq!((unread.seen_at, unread.seen_via), (None, None));
        assert_eq!(notices().await, 0);
        assert_eq!(announced(), before, "a read announces nothing");

        let refused = dispatch_resource_change(
            &orch,
            &request(Some("r-live")),
            0,
            &ChangeItem {
                target: uri.clone(),
                mode: ChangeMode::Patch,
                payload: Some(serde_json::json!({"seen": true})),
            },
            false,
        )
        .await
        .unwrap_err()
        .error;
        assert!(
            refused.contains("only the operator marks a message seen"),
            "{refused}"
        );
        assert_eq!(state_of(&orch, &id).await, State::Delivered);

        let seen = || {
            crate::channels::write_as_operator(
                &orch,
                &uri,
                "patch",
                Some(serde_json::json!({"seen": true})),
            )
        };
        let marked = seen().await.unwrap();
        assert!(marked.starts_with("Marked"), "{marked}");
        assert!(matches!(
            state_of(&orch, &id).await,
            State::Seen {
                via: Some(SeenVia::App),
                ..
            }
        ));
        assert_eq!(notices().await, 1);
        assert!(emitter
            .events_named("db-change")
            .iter()
            .skip(before)
            .any(|event| event["table"] == "operator_messages"));

        let again = seen().await.unwrap();
        assert!(again.contains("already seen"), "{again}");
        assert_eq!(notices().await, 1, "one notice, however often it is opened");
    }

    /// The thread messages that carry an answer to `uri`.
    async fn replies(orch: &Orchestrator, uri: &str) -> i64 {
        orch.db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM messages WHERE channel_type = 'thread' AND content LIKE ?1",
                cairn_db::turso::params![format!("[Reply to {uri}]%")],
            )
            .await
            .unwrap()
            .unwrap_or_default()
    }

    /// A thread sender is addressed by name, so a thread whose session has no
    /// run when he sees the message still holds the passive notice for its
    /// next turn.
    #[tokio::test]
    async fn a_thread_with_no_run_holds_its_seen_notice_for_its_next_turn() {
        let orch = orchestrator("seen-runless").await;
        let local = &orch.db.local;
        local
            .execute_script(
                "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                 VALUES ('t-quiet', 'p', 'quiet', 'active', 1, 1);",
            )
            .await
            .unwrap();
        let session = crate::threads::ensure_thread_session(local, "t-quiet")
            .await
            .unwrap();
        messages::insert(
            local,
            &messages::NewMessage {
                id: "m-quiet".into(),
                project_id: "p".into(),
                sender_home: "cairn://p/prj/quiet".into(),
                sender_job_id: session.clone(),
                via: crate::operator::Via::App,
                content: "Rotating the key".into(),
                default_text: Some("I rotate at noon".into()),
                headline: None,
                created_at: 1,
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            crate::messages::delivery::latest_run_for_job_async(local, &session)
                .await
                .unwrap(),
            None
        );

        let seen = messages::record_seen(
            &orch,
            messages::MessageFilter::Ids(vec!["m-quiet".into()]),
            SeenVia::App,
            1_000,
        )
        .await
        .unwrap();
        assert_eq!(seen.len(), 1);
        let notice = crate::messages::delivery::system_direct_once_message_id(
            &messages::seen_notice_key("m-quiet"),
        );
        let (recipient, run) = local
            .query_opt(
                "SELECT recipient_job_id, recipient_run_id FROM message_recipients WHERE message_id = ?1",
                cairn_db::turso::params![notice.clone()],
                |row| {
                    use crate::storage::RowExt;
                    Ok((row.text(0)?, row.opt_text(1)?))
                },
            )
            .await
            .unwrap()
            .expect("the notice is addressed to the thread's session");
        assert_eq!((recipient.as_str(), run), (session.as_str(), None));
        let wake = local
            .query_opt_text(
                "SELECT wake FROM attention_pushes WHERE recipient = ?1 AND key = ?2",
                cairn_db::turso::params![
                    session.clone(),
                    crate::orchestrator::attention_push::direct_push_key(&notice, None)
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            wake.as_deref(),
            Some("passive"),
            "it rides the next turn, waking nothing"
        );
    }

    /// The answer is recorded before it is delivered: a record that fails
    /// delivers nothing, and a delivery that fails after the record is the
    /// write's failure and leaves the entry answered.
    #[tokio::test]
    async fn an_answer_is_recorded_before_it_is_delivered() {
        let orch = orchestrator("answer-order").await;
        let id = sent_id(
            &send(
                &orch,
                Some("r-live"),
                serde_json::json!({"content": "Rename?", "via": "cairn://channels/app", "default": "I rename Friday"}),
            )
            .await
            .unwrap(),
        );
        let uri = messages::message_uri(&id);
        let answer = || {
            crate::channels::write_as_operator(
                &orch,
                &uri,
                "append",
                Some(serde_json::json!({"content": "yes, rename it"})),
            )
        };

        orch.db
            .local
            .execute_script(
                "CREATE TRIGGER refuse_answer BEFORE UPDATE ON operator_messages
                 BEGIN SELECT RAISE(ABORT, 'answer store down'); END;",
            )
            .await
            .unwrap();
        let refused = answer().await.unwrap_err();
        assert!(refused.contains("answer store down"), "{refused}");
        assert_eq!(replies(&orch, &uri).await, 0, "no record, no delivery");
        assert_eq!(state_of(&orch, &id).await, State::Delivered);
        assert!(entries(&orch).await.iter().any(|entry| entry.is_open()));

        orch.db
            .local
            .execute_script(
                "DROP TRIGGER refuse_answer;
                 UPDATE threads SET status = 'closed' WHERE id = 't-general';",
            )
            .await
            .unwrap();
        let undelivered = answer().await.unwrap_err();
        assert!(
            undelivered.contains("recorded as answered")
                && undelivered.contains("cairn://p/prj/general")
                && undelivered.contains("closed"),
            "{undelivered}"
        );
        assert_eq!(replies(&orch, &uri).await, 0);
        assert!(matches!(state_of(&orch, &id).await, State::Answered { .. }));
        assert!(entries(&orch).await.iter().all(|entry| !entry.is_open()));

        orch.db
            .local
            .execute(
                "UPDATE threads SET status = 'active' WHERE id = 't-general'",
                (),
            )
            .await
            .unwrap();
        let again = answer().await.unwrap_err();
        assert!(again.contains("already answered"), "{again}");
        assert_eq!(replies(&orch, &uri).await, 0, "a retry delivers nothing");
    }

    #[tokio::test]
    async fn the_operator_sets_the_app_channels_purpose() {
        let orch = orchestrator("purpose").await;
        crate::channels::write_as_operator(
            &orch,
            "cairn://channels",
            "patch",
            Some(serde_json::json!({"provider": "app", "purpose": "anything not urgent"})),
        )
        .await
        .unwrap();
        assert_eq!(
            crate::config::settings::load_settings(&orch.config_dir)
                .channels
                .app
                .purpose
                .as_deref(),
            Some("anything not urgent")
        );
        let channels = crate::channels::read_as_operator(&orch, "cairn://channels")
            .await
            .unwrap();
        assert!(
            channels.contains("## App") && channels.contains("anything not urgent"),
            "{channels}"
        );
        let app = crate::channels::read_as_operator(&orch, "cairn://channels/app")
            .await
            .unwrap();
        assert!(app.contains("cairn://operator"), "{app}");
        let refused = crate::channels::read_as_operator(&orch, "cairn://channels/app/x")
            .await
            .unwrap();
        assert!(refused.contains("has no conversations"), "{refused}");
    }
}
