use super::wire::{ResponsesItem, ResponsesTurn};
use crate::backends::http_loop::transcript::{load_prior_rows, ReplayRow};
use crate::backends::{SessionConfig, SessionStart};
use crate::orchestrator::Orchestrator;
pub(crate) use cairn_models::wire::openai_responses::conversation::*;

/// Why a wholly empty turn is refused instead of assembled.
const EMPTY_USER_MESSAGE: &str = "Refusing to start a turn with an empty user message: an empty input item is rejected by the provider and would poison every later replay of this conversation.";

pub(crate) fn build_conversation_messages(
    orch: &Orchestrator,
    config: &SessionConfig,
    session_id: &str,
    system_prompt: &str,
) -> Result<Vec<ResponsesTurn>, String> {
    if config.message_content.is_blank() {
        return Err(EMPTY_USER_MESSAGE.to_string());
    }
    let mut items = vec![ResponsesItem::Instructions {
        text: system_prompt.to_string(),
    }];
    if !matches!(config.session_start, SessionStart::New { .. }) {
        items.extend(load_prior_items(
            orch,
            session_id,
            &config.run_id,
            &config.project_id,
            &config.project_key,
        )?);
    }
    items.push(user_item(&config.message_content));
    // Each replayed item is its own turn; only a fresh generation groups several
    // items (reasoning + message + calls) under one.
    Ok(items.into_iter().map(ResponsesTurn::one).collect())
}

fn load_prior_items(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<ResponsesItem>, String> {
    let rows = load_prior_rows(orch, session_id, current_run_id, project_id, project_key)?;
    let mut items = Vec::new();
    for row in rows {
        match row {
            ReplayRow::User(content) => items.push(user_item(&content)),
            ReplayRow::Event { event_type, event } => {
                items.extend(transcript_event_to_items(&event_type, *event))
            }
        }
    }
    Ok(normalize_call_pairs(items))
}
