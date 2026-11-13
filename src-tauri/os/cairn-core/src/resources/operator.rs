//! `cairn://operator` and `cairn://operator/messages/{id}`: the operator as a
//! recipient, read.
//!
//! The channels it lists are the same per-provider summaries
//! `cairn://channels` renders ([`super::channels::provider_summaries`]), so the
//! two never describe a channel differently. An agent reads its own messages;
//! the desktop operator, through the channels door, reads all of them, and
//! opening one there is what marks it seen in the app.

use cairn_common::query::QueryParam;

use crate::mcp::types::McpCallbackRequest;
use crate::operator::messages::{self, Message};
use crate::operator::{SeenVia, Via};
use crate::orchestrator::Orchestrator;
use crate::resources::channels::{at_ms, bounded_limit, provider_summaries, PURPOSE_HOW};

const OPERATOR_URI: &str = "cairn://operator";

const RUN_LESS_REFUSAL: &str = "Refused: this request speaks for no run and carries no operator credential, so it has no messages to the operator. A live thread or issue node reads its own at cairn://operator; the operator reads them all from the desktop.";

/// Who is reading.
pub(crate) enum Reader {
    /// The verified desktop operator: every message, with its sender.
    Operator,
    /// A live agent: its own messages.
    Agent { home: String },
}

pub(crate) async fn reader(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<Reader, String> {
    if request.is_desktop_operator() {
        return Ok(Reader::Operator);
    }
    if request.run_id.as_deref().is_none_or(str::is_empty) || request.is_unverified_local() {
        return Err(RUN_LESS_REFUSAL.to_string());
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            format!("Only a live thread or issue node reads its messages to the operator, and this request's run could not be authenticated: {error}")
        })?;
    Ok(Reader::Agent { home: agent.home })
}

/// The one line a channel gets on `cairn://operator`.
fn channel_line(summary: &crate::resources::channels::ProviderSummary) -> String {
    let offer = match &summary.purpose {
        Some(purpose) if summary.enabled && summary.configured => purpose.clone(),
        Some(purpose) => format!("{purpose} (not configured, so a send is refused)"),
        None => format!("not offered: no purpose line ({PURPOSE_HOW})"),
    };
    format!(
        "- {} ({}): {offer}\n  presence: {} · {}\n",
        summary.display_name,
        summary.uri,
        summary.presence,
        summary.last_seen()
    )
}

fn message_line(message: &Message, state: &messages::State, now: i64, with_sender: bool) -> String {
    let mut out = format!(
        "- {} · via {} · {} · sent {}\n",
        message.uri(),
        message.via.display_name(),
        state.describe(now),
        at_ms(message.created_at * 1000),
    );
    if with_sender {
        out.push_str(&format!("  from: {}\n", message.sender_home));
    }
    out.push_str(&format!("  content: {}\n", message.content));
    if let Some(default) = &message.default_text {
        out.push_str(&format!("  default: {default}\n"));
    }
    out
}

/// `cairn://operator`.
pub async fn render_operator(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    params: &[QueryParam],
) -> String {
    match operator_view(orch, request, params).await {
        Ok(body) | Err(body) => body,
    }
}

async fn operator_view(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    params: &[QueryParam],
) -> Result<String, String> {
    if let Some(param) = params.iter().find(|param| param.key != "limit") {
        return Err(format!(
            "Unsupported query parameter '{}' for {OPERATOR_URI}",
            param.key
        ));
    }
    let limit = bounded_limit(params, OPERATOR_URI, 20, 200)?;
    let reader = reader(orch, request).await?;
    let now = chrono::Utc::now().timestamp();
    let mut out = String::from(
        "# Operator\n\nThe operator as a recipient. A message to him names its channel (`via`): pick the one whose purpose line it fits. A channel with no purpose line is not offered. Append {content, via} here to send one, with `default` when you will act without his reply.\n\n## Channels\n\n",
    );
    for summary in provider_summaries(orch)
        .await
        .iter()
        .filter(|summary| Via::parse(summary.provider).is_some())
    {
        out.push_str(&channel_line(summary));
    }
    let (heading, listed, with_sender) = match &reader {
        Reader::Operator => (
            "Messages".to_string(),
            messages::all(&orch.db.local, limit).await?,
            true,
        ),
        Reader::Agent { home, .. } => (
            format!("Your messages ({home})"),
            messages::for_sender(&orch.db.local, home, limit).await?,
            false,
        ),
    };
    out.push_str(&format!("\n## {heading}\n\n"));
    if listed.is_empty() {
        out.push_str("None.\n");
    }
    for message in &listed {
        let delivery = messages::delivery(&orch.db.local, message).await?;
        let state = messages::state(message, delivery.as_ref());
        out.push_str(&message_line(message, &state, now, with_sender));
    }
    if listed.len() as i64 == limit {
        out.push_str(&format!("\nNewest {limit} shown (raise ?limit=).\n"));
    }
    Ok(out)
}

/// `cairn://operator/messages/{id}`. The operator's read marks it seen in the
/// app; an agent's never does, and an agent reads only its own.
pub async fn render_message(orch: &Orchestrator, request: &McpCallbackRequest, id: &str) -> String {
    match message_view(orch, request, id).await {
        Ok(body) | Err(body) => body,
    }
}

async fn message_view(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    id: &str,
) -> Result<String, String> {
    let reader = reader(orch, request).await?;
    let uri = messages::message_uri(id);
    let Some(mut message) = messages::get(&orch.db.local, id).await? else {
        return Err(format!("{uri} names no message to the operator."));
    };
    match &reader {
        Reader::Agent { home, .. } if *home != message.sender_home => {
            return Err(format!(
                "{uri} is not one of your messages to the operator; read {OPERATOR_URI} for yours."
            ));
        }
        Reader::Agent { .. } => {}
        Reader::Operator => {
            let now = chrono::Utc::now().timestamp();
            if let Some(seen) = crate::operator::record_seen(
                orch,
                crate::operator::MessageFilter::Ids(vec![message.id.clone()]),
                SeenVia::App,
                now,
            )
            .await?
            .into_iter()
            .next()
            {
                message = seen;
            }
        }
    }
    let now = chrono::Utc::now().timestamp();
    let delivery = messages::delivery(&orch.db.local, &message).await?;
    let state = messages::state(&message, delivery.as_ref());
    let mut out = format!(
        "# Message to the operator\n\n- uri: {uri}\n- from: {}\n- via: {} ({})\n- sent: {}\n- state: {}\n- content: {}\n",
        message.sender_home,
        message.via.display_name(),
        message.via.provider_uri(),
        at_ms(message.created_at * 1000),
        state.describe(now),
        message.content,
    );
    if let Some(default) = &message.default_text {
        out.push_str(&format!("- default: {default}\n"));
    }
    if let Some(headline) = &message.headline {
        out.push_str(&format!("- headline: {headline}\n"));
    }
    if let Some(row) = &delivery {
        let read_at = crate::channels::ledger::read_at(&orch.db.local, &row.id).await?;
        out.push_str(&format!(
            "\n## Delivery\n\n- conversation: {}\n- status: {}\n- attempts: {}\n- last error: {}\n- sent at: {}\n- read at: {}\n",
            row.conversation,
            row.status,
            row.attempts,
            row.last_error.as_deref().unwrap_or("none"),
            row.sent_at.map(at_ms).unwrap_or_else(|| "not yet".to_string()),
            read_at.map(at_ms).unwrap_or_else(|| "no receipt".to_string()),
        ));
    }
    if let Some(answer) = &message.answer_ref {
        out.push_str(&format!("\n## Answer\n\n- reference: {answer}\n"));
    }
    Ok(out)
}
