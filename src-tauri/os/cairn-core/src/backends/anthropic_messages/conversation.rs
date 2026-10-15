use super::wire::MessagesMessage;
use crate::backends::http_loop::transcript::{load_prior_rows, ReplayRow};
use crate::backends::{SessionConfig, SessionStart};
use crate::orchestrator::Orchestrator;
pub(crate) use cairn_models::wire::anthropic_messages::conversation::*;

/// Why a wholly empty turn is refused instead of assembled.
const EMPTY_USER_MESSAGE: &str = "Refusing to start a turn with an empty user message: an empty content block is rejected by the provider and would poison every later replay of this conversation.";

pub(crate) fn build_conversation_messages(
    orch: &Orchestrator,
    config: &SessionConfig,
    session_id: &str,
    system_prompt: &str,
) -> Result<Vec<MessagesMessage>, String> {
    if config.message_content.is_blank() {
        return Err(EMPTY_USER_MESSAGE.to_string());
    }
    let mut messages = vec![MessagesMessage::system(system_prompt.to_string())];
    if !matches!(config.session_start, SessionStart::New { .. }) {
        messages.extend(load_prior_messages(
            orch,
            session_id,
            &config.run_id,
            &config.project_id,
            &config.project_key,
        )?);
    }
    messages.push(MessagesMessage::user_content(&config.message_content));
    Ok(coalesce_roles(messages))
}

pub(crate) fn load_prior_messages(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<MessagesMessage>, String> {
    let rows = load_prior_rows(orch, session_id, current_run_id, project_id, project_key)?;
    let messages = rows
        .into_iter()
        .filter_map(|row| match row {
            ReplayRow::User(content) => Some(MessagesMessage::user_content(&content)),
            ReplayRow::Event { event_type, event } => {
                transcript_event_to_message(&event_type, *event)
            }
        })
        .collect();
    Ok(normalize_tool_groups(messages))
}
