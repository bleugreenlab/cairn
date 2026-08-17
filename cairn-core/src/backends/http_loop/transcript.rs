//! Cairn's persisted transcript, read back as replayable rows.
//!
//! Every HTTP protocol family rebuilds its outgoing conversation from the same
//! stored events, so WHICH rows replay — and the fact that a user turn's stable
//! image references must be resolved to bytes before anything can send them — is
//! one fact, kept here. What each row BECOMES on the wire is protocol-specific
//! and deliberately not decided here: a chat message, an Anthropic content
//! block, and a Responses input item are different shapes with different legal
//! orderings, and flattening them into a common enum would push each family's
//! exceptions into the others.

use crate::agent_process::stdin::MessageContent;
use crate::agent_process::stream::TranscriptEvent;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, RowExt};

/// Stands in for a tool result that was stored empty. A blank content block is
/// rejectable by every family here, and a dropped result would orphan its call,
/// so the empty result is stated rather than sent or removed.
pub(in crate::backends) const EMPTY_TOOL_RESULT: &str = "The tool returned no output.";

/// Stands in for a call whose result never landed, because the turn was
/// interrupted between dispatch and persistence.
pub(in crate::backends) const INTERRUPTED_TOOL_RESULT: &str =
    "Interrupted before the tool result was recorded.";

/// One prior transcript row, decoded.
///
/// A user turn arrives already resolved: turning `cairn://` image references
/// into bytes needs the database, so it happens inside the loader's runtime
/// block rather than being left for each protocol to redo. Every other row is
/// handed back as the persisted event, for the protocol to map.
pub(in crate::backends) enum ReplayRow {
    User(MessageContent),
    /// Boxed because a transcript event is several times the size of a user
    /// turn, and every row would otherwise pay for the largest variant.
    Event {
        event_type: String,
        event: Box<TranscriptEvent>,
    },
}

/// Load the rows a resumed session replays, oldest first.
///
/// `user:launch` carries the task the job was started on. It is bound as a
/// parameter rather than written into the `IN` list so the replay set cannot
/// drift from the constant (CAIRN-3408): dropping it here would rebuild a
/// conversation whose opening instruction is missing, and every turn after the
/// first would continue without ever having been told what to do.
///
/// Blank stored user content is dropped rather than replayed: an empty text
/// block is rejected by these providers, and replaying one turns a single bad
/// turn into a conversation that can never resume (CAIRN-3263).
pub(in crate::backends) fn load_prior_rows(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<ReplayRow>, String> {
    let session_id = session_id.to_string();
    let current_run_id = current_run_id.to_string();
    let project_id = project_id.to_string();
    let project_key = project_key.to_string();
    run_db_blocking(|| async move {
        let session_db = crate::projects::crud::owning_db(&orch.db, &project_id).await?;
        let rows = session_db
            .query_all(
                "SELECT event_type, data FROM events
                 WHERE session_id = ?1
                   AND run_id != ?2
                   AND (event_type IN ('user', 'assistant', 'tool_result')
                        OR event_type = ?3)
                 ORDER BY created_at ASC, rowid ASC",
                (
                    session_id.clone(),
                    current_run_id.clone(),
                    crate::transcripts::LAUNCH_EVENT_TYPE,
                ),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut out = Vec::new();
        for (event_type, data) in rows {
            let Ok(event) = serde_json::from_str::<TranscriptEvent>(&data) else {
                continue;
            };
            // A launch prompt reaches the provider in the same user role it
            // originally occupied. Namespacing it changed who Cairn says wrote
            // it, not what the model was told (CAIRN-3408).
            if event_type == "user" || event_type == crate::transcripts::LAUNCH_EVENT_TYPE {
                let Some(text) = event.content.filter(|text| !text.trim().is_empty()) else {
                    continue;
                };
                let content = crate::agent_process::stdin::resolve_stable_images(
                    &orch.db,
                    &project_id,
                    &project_key,
                    text,
                )
                .await?;
                out.push(ReplayRow::User(content));
            } else {
                out.push(ReplayRow::Event {
                    event_type,
                    event: Box::new(event),
                });
            }
        }
        Ok::<_, String>(out)
    })
}

/// The structured reasoning an assistant event stored, if it stored any.
///
/// Writers persist this under either casing, and store `null` or `[]` when the
/// turn did no reasoning; both read as absent so a non-reasoning turn does not
/// replay an empty reasoning field.
pub(in crate::backends) fn stored_reasoning_details(
    event: &TranscriptEvent,
) -> Option<serde_json::Value> {
    event
        .raw
        .as_ref()
        .and_then(|raw| {
            raw.get("reasoning_details")
                .or_else(|| raw.get("reasoningDetails"))
        })
        .filter(|value| {
            !value.is_null()
                && !matches!(value, serde_json::Value::Array(items) if items.is_empty())
        })
        .cloned()
}
