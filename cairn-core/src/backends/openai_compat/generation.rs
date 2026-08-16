//! Streamed chat-completions response -> neutral [`Generation`].
//!
//! Every provider riding the OpenAI-compatible path converges here, so tool-name
//! canonicalization happens exactly once, in one place, for all of them. The
//! normalization runs BEFORE anything is stored, pushed into the conversation,
//! or executed, so execution, the stored transcript, and any replay/resume all
//! reference the dispatched verb. Otherwise a successful call replays under an
//! invalid name like `Write_File` or `mcp__cairn__run` and reinforces it in the
//! model-facing history.

use crate::backends::http_loop::{repair, Generation, TurnToolCall};
use crate::backends::openai_compat::wire::{ChatContent, ChatMessage, ChatResponse};

pub(crate) fn into_generation(
    response: ChatResponse,
    provider_name: &str,
) -> Result<Generation<ChatMessage>, String> {
    let ChatResponse {
        id,
        model,
        choices,
        usage,
        streamed_text,
        finish_reason,
    } = response;
    let Some(choice) = choices.into_iter().next() else {
        return Err(format!("{provider_name} response did not include choices"));
    };
    let mut assistant_message = choice.message;
    if let Some(calls) = assistant_message.tool_calls.as_mut() {
        for call in calls.iter_mut() {
            if let Some(verb) = repair::normalize_tool_name(&call.function.name) {
                if verb != call.function.name {
                    log::warn!(
                        "{provider_name} normalized tool name {:?} -> {:?}",
                        call.function.name,
                        verb
                    );
                    call.function.name = verb.to_string();
                }
            }
        }
    }
    let assistant_text = assistant_message
        .content
        .as_ref()
        .and_then(ChatContent::as_text)
        .unwrap_or_default()
        .to_string();
    let tool_calls = assistant_message
        .tool_calls
        .as_ref()
        .map(|calls| {
            calls
                .iter()
                .map(|call| TurnToolCall {
                    id: call.id.clone(),
                    name: call.function.name.clone(),
                    arguments: call.function.arguments.clone(),
                })
                .collect()
        })
        .unwrap_or_default();
    let reasoning_details = assistant_message.reasoning_details.clone();
    Ok(Generation {
        assistant_message,
        assistant_text,
        tool_calls,
        reasoning_details,
        usage,
        finish_reason,
        generation_id: id,
        response_model: model,
        streamed_text,
    })
}

#[cfg(test)]
mod tests {
    use super::into_generation;
    use crate::backends::openai_compat::wire::ChatResponse;

    fn response(body: &str) -> ChatResponse {
        serde_json::from_str(body).expect("fixture parses as a chat response")
    }

    #[test]
    fn a_choiceless_response_names_the_provider_that_returned_it() {
        let error = into_generation(response(r#"{"choices":[]}"#), "OpenCode Go")
            .err()
            .expect("a response with no choices cannot produce a generation");
        assert_eq!(error, "OpenCode Go response did not include choices");
    }

    #[test]
    fn tool_names_are_canonicalized_before_the_call_is_stored_or_executed() {
        let generation = into_generation(
            response(
                r#"{"choices":[{"message":{"role":"assistant","tool_calls":[
                    {"id":"call_1","type":"function","function":{"name":"mcp__cairn__run","arguments":"{}"}}
                ]}}]}"#,
            ),
            "OpenCode Go",
        )
        .expect("a response with one tool call produces a generation");

        assert_eq!(generation.tool_calls[0].name, "run");
        // The message pushed back into the conversation carries the canonical
        // name too, so a resume does not reinforce the alias.
        assert_eq!(
            generation.assistant_message.tool_calls.as_ref().unwrap()[0]
                .function
                .name,
            "run"
        );
    }
}
