//! The standard [`HttpTurnHost`]: a protocol family plus a host's effects.
//!
//! [`FamilyTurn`] owns everything about a turn that is the same for every
//! application: the native conversation, fitting it to the context window,
//! building and posting each request through [`Complete`], reading the reply,
//! and appending assistant messages and tool results in their native shape.
//! What is the application's own (where streamed text goes, how a tool runs,
//! what gets persisted) is a [`TurnEffects`], so a host writes only that.
//! A host that needs to own the request itself still implements
//! [`HttpTurnHost`] directly.

use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use serde_json::Value;

use super::{
    run_turn, GenerationMeta, HttpTurnConfig, HttpTurnHost, ProtocolFamily, SuspendKind, TurnExit,
};
use crate::interface::complete::{Complete, CompletionCall, CompletionError};
use crate::wire::replay::SKIPPED_TOOL_RESULT;
use crate::wire::stream::StreamSink;
use crate::wire::{Generation, GenerationConfig, TurnToolCall, TurnUsage};

/// What running one tool call came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolDispatch {
    /// The result text the model reads.
    Result(String),
    /// The call is waiting on something outside the turn; its real result is
    /// supplied when the conversation resumes.
    Suspend(SuspendKind),
}

/// A host's side of a [`FamilyTurn`]: its stream, its tools, its storage.
///
/// Only the stream sink and tool execution are required. Every persistence
/// and event effect defaults to doing nothing, so a host overrides just the
/// ones it records.
pub trait TurnEffects {
    /// Where one generation's streamed text and thinking go. Asked for once
    /// per generation.
    fn stream_sink(&mut self) -> Box<dyn StreamSink + '_>;
    /// Run one tool call. `truncated` marks the last call of a generation cut
    /// off by its output cap, whose arguments may be incomplete.
    fn execute_tool(
        &mut self,
        call: &TurnToolCall,
        truncated: bool,
    ) -> Result<ToolDispatch, String>;
    /// The text a failed generation request reports. A host overrides this
    /// to say what a provider's bare failure means.
    fn explain_error(&self, error: CompletionError) -> String {
        error.into_stream_text()
    }
    /// The generation's usage, as soon as it is known.
    fn context_snapshot(&mut self, _usage: &TurnUsage) {}
    /// A final assistant message that was not already streamed.
    fn assistant_message(&mut self, _text: &str, _meta: GenerationMeta<'_>) -> Result<(), String> {
        Ok(())
    }
    /// An assistant message that carries tool calls.
    fn assistant_tool_calls(
        &mut self,
        _text: &str,
        _calls: &[TurnToolCall],
        _meta: GenerationMeta<'_>,
        _reasoning: Option<&Value>,
    ) -> Result<(), String> {
        Ok(())
    }
    /// The turn succeeded.
    fn success_result(&mut self, _meta: GenerationMeta<'_>) -> Result<(), String> {
        Ok(())
    }
    /// A call a suspension left unrun. The native conversation already
    /// records it as skipped; this is for the host's own record.
    fn skipped_tool_result(&mut self, _call_id: &str) -> Result<(), String> {
        Ok(())
    }
    /// Whether an executed tool ended the turn (a final-answer tool).
    fn terminal_tool_armed(&self) -> bool {
        false
    }
    /// The final answer of a turn that asked for an output schema.
    fn capture_final_output(&mut self, _text: &str) {}
}

/// One agent turn over protocol family `F`, with host effects `E`.
pub struct FamilyTurn<'a, F: ProtocolFamily, E: TurnEffects> {
    pub family: F,
    pub model: String,
    /// The generation knobs, including the tools every request advertises.
    pub config: GenerationConfig,
    /// The model's context window, or `None` to send the conversation
    /// untrimmed.
    pub context_window: Option<i64>,
    /// The native conversation, which this turn extends as it runs.
    pub conversation: Vec<F::Message>,
    pub effects: E,
    pub complete: &'a dyn Complete,
    pub cancel: Arc<AtomicBool>,
}

impl<F: ProtocolFamily, E: TurnEffects> FamilyTurn<'_, F, E> {
    /// Run the turn to its exit.
    pub fn run(&mut self) -> Result<TurnExit, String> {
        let cancel = self.cancel.clone();
        let provider_name = self.family.provider_name().to_string();
        let model = self.model.clone();
        let has_output_schema = self.config.output_schema.is_some();
        run_turn(
            self,
            HttpTurnConfig {
                backend_name: &provider_name,
                model: &model,
                has_output_schema,
            },
            &cancel,
        )
    }
}

impl<F: ProtocolFamily, E: TurnEffects> HttpTurnHost for FamilyTurn<'_, F, E> {
    fn post_generation(&mut self) -> Result<Generation<()>, String> {
        let outgoing = self.family.fit(&self.conversation, self.context_window);
        if matches!(outgoing, Cow::Owned(_)) {
            log::info!(
                "[cairn-context-trim] backend={} model={}",
                self.family.wire().backend_key(),
                self.model
            );
        }
        let body = self.family.body(&self.model, &outgoing, &self.config);
        drop(outgoing);
        let reply = {
            let mut sink = self.effects.stream_sink();
            self.complete.complete(CompletionCall::stream(
                self.family.wire(),
                body,
                &mut *sink,
                &self.cancel,
            ))
        };
        let reply = reply.map_err(|error| self.effects.explain_error(error))?;
        let Generation {
            assistant_message,
            assistant_text,
            tool_calls,
            reasoning_details,
            usage,
            finish_reason,
            generation_id,
            response_model,
            streamed_text,
        } = self.family.read(reply, &self.config.tools)?;
        self.conversation.push(assistant_message);
        Ok(Generation {
            assistant_message: (),
            assistant_text,
            tool_calls,
            reasoning_details,
            usage,
            finish_reason,
            generation_id,
            response_model,
            streamed_text,
        })
    }

    fn context_snapshot(&mut self, usage: &TurnUsage) {
        self.effects.context_snapshot(usage);
    }

    fn assistant_message(&mut self, text: &str, meta: GenerationMeta<'_>) -> Result<(), String> {
        self.effects.assistant_message(text, meta)
    }

    fn assistant_tool_calls(
        &mut self,
        text: &str,
        calls: &[TurnToolCall],
        meta: GenerationMeta<'_>,
        reasoning: Option<&Value>,
    ) -> Result<(), String> {
        self.effects
            .assistant_tool_calls(text, calls, meta, reasoning)
    }

    fn success_result(&mut self, meta: GenerationMeta<'_>) -> Result<(), String> {
        self.effects.success_result(meta)
    }

    fn execute_tool(
        &mut self,
        call: &TurnToolCall,
        truncated: bool,
    ) -> Result<Option<SuspendKind>, String> {
        match self.effects.execute_tool(call, truncated)? {
            ToolDispatch::Result(content) => {
                self.family
                    .push_tool_result(&mut self.conversation, &call.id, content);
                Ok(None)
            }
            ToolDispatch::Suspend(kind) => Ok(Some(kind)),
        }
    }

    fn skipped_tool_result(&mut self, call_id: &str) -> Result<(), String> {
        self.effects.skipped_tool_result(call_id)?;
        self.family.push_tool_result(
            &mut self.conversation,
            call_id,
            SKIPPED_TOOL_RESULT.to_string(),
        );
        Ok(())
    }

    fn terminal_tool_armed(&self) -> bool {
        self.effects.terminal_tool_armed()
    }

    fn capture_final_output(&mut self, text: &str) {
        self.effects.capture_final_output(text);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interface::complete::CompletionReply;
    use crate::turn::{ChatFamily, MessagesFamily, ResponsesFamily};
    use crate::wire::anthropic_messages::http::MessagesEndpoint;
    use crate::wire::anthropic_messages::wire::{ContentBlock, MessagesResponse};
    use crate::wire::openai_compat::http::{ChatDialect, Endpoint};
    use crate::wire::openai_compat::wire::ChatResponse;
    use crate::wire::openai_responses::http::ResponsesEndpoint;
    use crate::wire::openai_responses::wire::{ResponseEnvelope, ResponsesItem, ResponsesResponse};
    use crate::wire::stream::StreamSummary;
    use crate::wire::{MessageContent, ToolDefinition};
    use serde_json::{json, Map};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// Answers each request from a queue and records its body.
    #[derive(Default)]
    struct Scripted {
        replies: Mutex<VecDeque<Result<CompletionReply, CompletionError>>>,
        bodies: Mutex<Vec<Value>>,
    }

    impl Scripted {
        fn new(replies: Vec<Result<CompletionReply, CompletionError>>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
                ..Self::default()
            }
        }
    }

    impl Complete for Scripted {
        fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
            self.bodies.lock().unwrap().push(call.body);
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("no unscripted request")
        }
    }

    struct Quiet;

    impl StreamSink for Quiet {
        fn append(&mut self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn append_thinking(&mut self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn finalize(&mut self, _: StreamSummary) -> Result<(), String> {
            Ok(())
        }
    }

    /// Runs every call except `suspend_on`, which waits.
    #[derive(Default)]
    struct Effects {
        suspend_on: Option<&'static str>,
        skipped: Vec<String>,
    }

    impl TurnEffects for Effects {
        fn stream_sink(&mut self) -> Box<dyn StreamSink + '_> {
            Box::new(Quiet)
        }
        fn execute_tool(&mut self, call: &TurnToolCall, _: bool) -> Result<ToolDispatch, String> {
            Ok(if self.suspend_on == Some(call.id.as_str()) {
                ToolDispatch::Suspend(SuspendKind::Prompt)
            } else {
                ToolDispatch::Result(format!("ran {}", call.id))
            })
        }
        fn explain_error(&self, error: CompletionError) -> String {
            format!("explained: {}", error.into_stream_text())
        }
        fn skipped_tool_result(&mut self, call_id: &str) -> Result<(), String> {
            self.skipped.push(call_id.to_string());
            Ok(())
        }
    }

    fn tools() -> Vec<ToolDefinition> {
        vec![ToolDefinition {
            name: "read".to_string(),
            description: "Read.".to_string(),
            input_schema: json!({"type": "object"}),
        }]
    }

    fn turn<'a, F: ProtocolFamily>(
        family: F,
        complete: &'a Scripted,
        effects: Effects,
    ) -> FamilyTurn<'a, F, Effects> {
        let conversation =
            family.start_conversation("SYSTEM", &MessageContent::text("hi".to_string()));
        FamilyTurn {
            family,
            model: "m".to_string(),
            config: GenerationConfig {
                tools: tools(),
                ..GenerationConfig::default()
            },
            context_window: None,
            conversation,
            effects,
            complete,
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    fn ids(calls: &[&str]) -> Vec<String> {
        calls.iter().map(|id| id.to_string()).collect()
    }

    fn chat() -> ChatFamily {
        ChatFamily {
            endpoint: Endpoint {
                provider_name: "Chat".into(),
                backend_key: "chat".into(),
                chat_url: "http://127.0.0.1:1".into(),
                headers: vec![],
            },
            dialect: ChatDialect::default(),
            extras: Map::new(),
        }
    }

    fn chat_reply(calls: &[&str]) -> CompletionReply {
        let message = if calls.is_empty() {
            json!({"role": "assistant", "content": "done"})
        } else {
            let calls: Vec<Value> = ids(calls)
                .into_iter()
                .map(|id| json!({"id": id, "type": "function", "function": {"name": "read", "arguments": "{}"}}))
                .collect();
            json!({"role": "assistant", "content": null, "tool_calls": calls})
        };
        let response: ChatResponse =
            serde_json::from_value(json!({"choices": [{"message": message}]})).unwrap();
        CompletionReply::Chat(response)
    }

    fn messages() -> MessagesFamily {
        MessagesFamily {
            endpoint: MessagesEndpoint {
                provider_name: "Messages".into(),
                backend_key: "messages".into(),
                url: "http://127.0.0.1:1".into(),
                headers: vec![],
                max_output_tokens: 1024,
            },
            extras: Map::new(),
        }
    }

    fn messages_reply(calls: &[&str]) -> CompletionReply {
        let content: Vec<Value> = if calls.is_empty() {
            vec![json!({"type": "text", "text": "done"})]
        } else {
            ids(calls)
                .into_iter()
                .map(|id| json!({"type": "tool_use", "id": id, "name": "read", "input": {}}))
                .collect()
        };
        let response: MessagesResponse =
            serde_json::from_value(json!({"content": content})).unwrap();
        CompletionReply::Messages(response)
    }

    fn responses() -> ResponsesFamily {
        ResponsesFamily {
            endpoint: ResponsesEndpoint {
                provider_name: "Responses".into(),
                backend_key: "responses".into(),
                url: "http://127.0.0.1:1".into(),
                headers: vec![],
            },
            extras: Map::new(),
        }
    }

    fn responses_reply(calls: &[&str]) -> CompletionReply {
        let output: Vec<Value> = if calls.is_empty() {
            vec![
                json!({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "done"}]}),
            ]
        } else {
            ids(calls)
                .into_iter()
                .map(|id| json!({"type": "function_call", "call_id": id, "name": "read", "arguments": "{}"}))
                .collect()
        };
        let envelope: ResponseEnvelope =
            serde_json::from_value(json!({"status": "completed", "output": output})).unwrap();
        CompletionReply::Responses(ResponsesResponse::from_envelope(envelope))
    }

    /// Run a tool round then an answer; return the second request's body.
    fn second_request<F: ProtocolFamily>(
        family: F,
        reply: fn(&[&str]) -> CompletionReply,
    ) -> Value {
        let complete = Scripted::new(vec![Ok(reply(&["a", "b"])), Ok(reply(&[]))]);
        let exit = turn(family, &complete, Effects::default()).run();
        assert_eq!(exit, Ok(TurnExit::Completed));
        let bodies = complete.bodies.into_inner().unwrap();
        assert_eq!(bodies.len(), 2);
        bodies[1].clone()
    }

    #[test]
    fn chat_sends_tool_results_back_as_tool_messages() {
        let body = second_request(chat(), chat_reply);
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 5, "system, user, assistant, two results");
        for (message, id) in messages[3..].iter().zip(["a", "b"]) {
            assert_eq!(message["role"], "tool");
            assert_eq!(message["tool_call_id"], id);
            assert_eq!(message["content"], format!("ran {id}"));
        }
    }

    #[test]
    fn messages_sends_one_generations_results_in_one_user_message() {
        let body = second_request(messages(), messages_reply);
        let turns = body["messages"].as_array().unwrap();
        assert_eq!(
            turns.len(),
            3,
            "user, assistant, one user message of results"
        );
        assert_eq!(
            turns[2],
            json!({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "a", "content": "ran a"},
                {"type": "tool_result", "tool_use_id": "b", "content": "ran b"},
            ]})
        );
    }

    #[test]
    fn responses_sends_tool_results_back_as_function_call_outputs() {
        let body = second_request(responses(), responses_reply);
        let input = body["input"].as_array().unwrap();
        assert_eq!(input.len(), 5, "user, two calls, two outputs");
        for (item, id) in input[3..].iter().zip(["a", "b"]) {
            assert_eq!(
                *item,
                json!({"type": "function_call_output", "call_id": id, "output": format!("ran {id}")})
            );
        }
    }

    /// Suspend on the first of three calls; return the turn after it exits.
    fn suspended<'a, F: ProtocolFamily>(
        family: F,
        complete: &'a Scripted,
    ) -> FamilyTurn<'a, F, Effects> {
        let mut turn = turn(
            family,
            complete,
            Effects {
                suspend_on: Some("a"),
                ..Effects::default()
            },
        );
        assert_eq!(turn.run(), Ok(TurnExit::Suspended(SuspendKind::Prompt)));
        assert_eq!(turn.effects.skipped, ["b", "c"]);
        turn
    }

    #[test]
    fn a_suspension_records_the_unrun_calls_as_skipped_in_every_family() {
        let complete = Scripted::new(vec![Ok(chat_reply(&["a", "b", "c"]))]);
        let turn = suspended(chat(), &complete);
        let tail: Vec<_> = turn.conversation[turn.conversation.len() - 2..]
            .iter()
            .map(|message| serde_json::to_value(message).unwrap())
            .collect();
        for (message, id) in tail.iter().zip(["b", "c"]) {
            assert_eq!(message["tool_call_id"], id);
            assert_eq!(message["content"], SKIPPED_TOOL_RESULT);
        }

        let complete = Scripted::new(vec![Ok(messages_reply(&["a", "b", "c"]))]);
        let turn = suspended(messages(), &complete);
        let last = turn.conversation.last().unwrap();
        assert!(matches!(
            last.content.as_slice(),
            [
                ContentBlock::ToolResult { tool_use_id: b, content: skipped_b, .. },
                ContentBlock::ToolResult { tool_use_id: c, content: skipped_c, .. },
            ] if b == "b" && c == "c" && skipped_b == SKIPPED_TOOL_RESULT && skipped_c == SKIPPED_TOOL_RESULT
        ));

        let complete = Scripted::new(vec![Ok(responses_reply(&["a", "b", "c"]))]);
        let turn = suspended(responses(), &complete);
        let tail = &turn.conversation[turn.conversation.len() - 2..];
        for (turn, id) in tail.iter().zip(["b", "c"]) {
            assert!(matches!(
                turn.items.as_slice(),
                [ResponsesItem::FunctionCallOutput { call_id, output }]
                    if call_id == id && output == SKIPPED_TOOL_RESULT
            ));
        }
    }

    #[test]
    fn a_failed_request_reports_the_hosts_explanation() {
        fn failing<F: ProtocolFamily>(family: F) -> Result<TurnExit, String> {
            let complete = Scripted::new(vec![Err(CompletionError::Stream("HTTP 503".into()))]);
            turn(family, &complete, Effects::default()).run()
        }
        for exit in [failing(chat()), failing(messages()), failing(responses())] {
            assert_eq!(exit, Err("explained: HTTP 503".to_string()));
        }
    }
}
