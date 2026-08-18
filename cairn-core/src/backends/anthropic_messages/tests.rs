//! Anthropic Messages contract tests.
//!
//! The SSE fixtures under `testdata/` are recordings of real OpenCode Go
//! `/zen/go/v1/messages` traffic with identifiers rewritten, so these assert
//! against the dialect Cairn actually has to parse rather than against vendor
//! documentation. Nothing here touches the network or needs credentials.

use super::conversation::{
    coalesce_roles, normalize_tool_groups, transcript_event_to_message, trim_conversation_to_budget,
};
use super::generation::into_generation;
use super::http::{build_body, MessagesEndpoint, TERMINAL_EVENTS};
use super::wire::{
    ContentBlock, MessagesMessage, MessagesResponse, StreamEvent, StreamingMessage, ASSISTANT_ROLE,
    USER_ROLE,
};
use crate::agent_process::stdin::MessageContent;
use crate::agent_process::stream::{ToolUseInfo, TranscriptEvent};
use crate::backends::http_loop::require_terminal_event;
use crate::backends::{SessionConfig, SessionStart};
use serde_json::json;

const PROVIDER: &str = "OpenCode Go";

/// A user turn built the way production builds one, so these tests exercise the
/// real constructor rather than a test-only shortcut beside it.
fn user_message(text: impl Into<String>) -> MessagesMessage {
    MessagesMessage::user_content(&MessageContent::text(text.into()))
}

fn endpoint() -> MessagesEndpoint {
    MessagesEndpoint {
        provider_name: PROVIDER,
        backend_key: "opencode-go",
        url: "https://opencode.ai/zen/go/v1/messages".to_string(),
        headers: vec![
            ("x-api-key".to_string(), "test-key".to_string()),
            ("content-type".to_string(), "application/json".to_string()),
        ],
        max_output_tokens: 32_000,
    }
}

fn config(max_thinking_tokens: Option<i32>) -> SessionConfig {
    SessionConfig {
        run_id: "run-1".to_string(),
        working_dir: "/tmp".to_string(),
        project_id: "project".to_string(),
        project_key: "GO".to_string(),
        prompt: "hi".to_string(),
        message_content: MessageContent::text("hi"),
        system_prompt_content: None,
        system_prompt_dynamic_tail: None,
        model: None,
        session_start: SessionStart::New {
            session_id: "session-1".to_string(),
        },
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        mcp_config_json: "{}".to_string(),
        home_uri: "cairn://p/GO/1/1/builder".to_string(),
        max_thinking_tokens,
        reasoning_effort: None,
        service_tier: None,
        permissions: crate::backends::AgentPermissions::new(crate::models::Fence::Allow),
        bidirectional: false,
        identity: None,
        output_schema: None,
        ambient: false,
        is_ephemeral_call: false,
    }
}

/// Replay a recorded stream through the aggregator exactly as the transport
/// does, so these tests exercise the same parsing path a live turn uses.
fn replay(fixture: &str) -> MessagesResponse {
    let mut aggregate = StreamingMessage::default();
    let mut streamed_text = false;
    for line in fixture.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        let event: StreamEvent =
            serde_json::from_str(data).unwrap_or_else(|error| panic!("{data}: {error}"));
        if let StreamEvent::ContentBlockDelta {
            delta: super::wire::BlockDelta::TextDelta { .. },
            ..
        } = &event
        {
            streamed_text = true;
        }
        aggregate.apply(&event);
    }
    aggregate.into_response(streamed_text)
}

const TEXT_STREAM: &str = include_str!("testdata/text_stream.sse");
const TOOL_CALL_STREAM: &str = include_str!("testdata/tool_call_stream.sse");
const THINKING_STREAM: &str = include_str!("testdata/thinking_stream.sse");
const TRUNCATED_TOOL_STREAM: &str = include_str!("testdata/truncated_tool_stream.sse");
const IN_STREAM_ERROR: &str = include_str!("testdata/in_stream_error.sse");

// === Request construction ===

#[test]
fn the_system_prompt_is_lifted_out_of_the_message_array() {
    // Anthropic carries the system prompt as a top-level field. A system-role
    // message on this wire is rejected, so the lift is correctness.
    let messages = vec![
        MessagesMessage::system("You are Cairn.".to_string()),
        user_message("hi".to_string()),
    ];
    let body = build_body(&endpoint(), "minimax-m3", &messages, &config(None), true);

    assert_eq!(body["system"][0]["text"], json!("You are Cairn."));
    let turns = body["messages"].as_array().expect("messages is an array");
    assert_eq!(turns.len(), 1, "only the user turn stays in messages");
    assert_eq!(turns[0]["role"], json!("user"));
}

#[test]
fn every_request_states_the_output_cap_the_protocol_requires() {
    // Omitting max_tokens is an upstream 500 here, not a default, so it is never
    // left to chance.
    let body = build_body(
        &endpoint(),
        "minimax-m3",
        &[user_message("hi".to_string())],
        &config(None),
        true,
    );
    assert_eq!(body["max_tokens"], json!(32_000));
    assert_eq!(body["stream"], json!(true));
    assert_eq!(body["tool_choice"]["type"], json!("auto"));
}

#[test]
fn tools_are_advertised_in_this_protocols_own_wrapper() {
    // Same three verbs as every other family, named `input_schema` rather than
    // nested under a `function` object.
    let body = build_body(
        &endpoint(),
        "minimax-m3",
        &[user_message("hi".to_string())],
        &config(None),
        true,
    );
    let tools = body["tools"].as_array().expect("tools is an array");
    let names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(names, vec!["read", "write", "run"]);
    assert_eq!(tools[1]["input_schema"]["required"][0], json!("changes"));
    assert!(
        tools[0].get("function").is_none(),
        "chat/completions nesting must not leak into Messages"
    );
}

#[test]
fn a_thinking_budget_is_passed_through_and_kept_under_the_output_cap() {
    // The provider rejects a budget that meets or exceeds max_tokens, so an
    // over-large configured budget is clamped rather than sent and refused.
    let body = build_body(
        &endpoint(),
        "minimax-m3",
        &[user_message("hi".to_string())],
        &config(Some(4_096)),
        true,
    );
    assert_eq!(body["thinking"]["type"], json!("enabled"));
    assert_eq!(body["thinking"]["budget_tokens"], json!(4_096));

    let clamped = build_body(
        &endpoint(),
        "minimax-m3",
        &[user_message("hi".to_string())],
        &config(Some(999_999)),
        true,
    );
    assert_eq!(clamped["thinking"]["budget_tokens"], json!(31_999));
}

#[test]
fn no_thinking_budget_leaves_the_request_unconstrained() {
    // A reasoning EFFORT is deliberately not translated into a budget: inventing
    // that ladder would spend tokens according to a mapping nobody chose.
    let body = build_body(
        &endpoint(),
        "minimax-m3",
        &[user_message("hi".to_string())],
        &config(None),
        true,
    );
    assert!(body.get("thinking").is_none());
}

#[test]
fn a_message_left_empty_by_replay_repair_is_never_sent() {
    // An empty content array is rejected, so a message that lost all its blocks
    // is dropped instead of poisoning the request.
    let messages = vec![
        MessagesMessage::system("sys".to_string()),
        MessagesMessage {
            role: USER_ROLE.to_string(),
            content: Vec::new(),
        },
        user_message("hi".to_string()),
    ];
    let body = build_body(&endpoint(), "minimax-m3", &messages, &config(None), true);
    assert_eq!(body["messages"].as_array().expect("array").len(), 1);
}

// === Streaming ===

#[test]
fn a_text_only_stream_rebuilds_its_message_and_usage() {
    let generation = into_generation(replay(TEXT_STREAM), PROVIDER).expect("a text stream maps");

    assert_eq!(
        generation.assistant_text,
        "It looks like you started counting: four, five, six"
    );
    assert!(generation.tool_calls.is_empty());
    assert!(
        generation.streamed_text,
        "text that reached the frontend live is not re-stored as a second event"
    );
    assert_eq!(generation.generation_id.as_deref(), Some("msg_text_0001"));
    assert_eq!(generation.response_model.as_deref(), Some("minimax-m3"));
}

#[test]
fn an_output_cap_cutoff_reaches_the_loop_as_length() {
    // The turn loop refuses to dispatch a possibly-truncated side-effecting call
    // when it sees "length". Anthropic spells that condition "max_tokens", so
    // leaving it untranslated would silently disarm that guard.
    let generation = into_generation(replay(TEXT_STREAM), PROVIDER).expect("maps");
    assert_eq!(generation.finish_reason.as_deref(), Some("length"));

    // A normal ending is NOT rewritten into a chat-completions word it does not
    // mean.
    let ended = into_generation(replay(THINKING_STREAM), PROVIDER).expect("maps");
    assert_eq!(ended.finish_reason.as_deref(), Some("end_turn"));
}

#[test]
fn interleaved_tool_calls_keep_their_ids_arguments_and_order() {
    let generation =
        into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("a tool stream maps");

    assert_eq!(generation.assistant_text, "Reading both files.");
    assert_eq!(generation.tool_calls.len(), 2);
    assert_eq!(generation.tool_calls[0].id, "call_first");
    assert_eq!(generation.tool_calls[0].name, "read");
    // Arguments arrive as several `input_json_delta` fragments and must
    // reassemble into exactly what the model emitted.
    assert_eq!(
        generation.tool_calls[0].arguments,
        r#"{"path": "/etc/hosts"}"#
    );
    assert_eq!(
        generation.tool_calls[1].arguments,
        r#"{"commands":[{"command":"ls"}]}"#
    );
}

#[test]
fn tool_names_are_canonicalized_before_dispatch_and_before_replay() {
    let generation = into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("maps");

    assert_eq!(generation.tool_calls[1].name, "run");
    // The message pushed back into the conversation carries the canonical name
    // too, so a resume does not reinforce the alias in the model-facing history.
    let replayed = generation
        .assistant_message
        .content
        .iter()
        .find_map(|block| match block {
            ContentBlock::ToolUse { id, name, .. } if id == "call_second" => Some(name.clone()),
            _ => None,
        })
        .expect("the replayed message carries the second call");
    assert_eq!(replayed, "run");
}

#[test]
fn truncated_tool_arguments_reach_the_repair_path_as_written() {
    // A cut-off payload cannot parse into a JSON object. The raw text is what
    // the loop's repair path needs; an emptied object would look like a valid
    // call and could apply a partial write.
    let generation = into_generation(replay(TRUNCATED_TOOL_STREAM), PROVIDER).expect("maps");

    assert_eq!(generation.finish_reason.as_deref(), Some("length"));
    assert_eq!(generation.tool_calls.len(), 1);
    assert!(
        generation.tool_calls[0].arguments.ends_with("fn main"),
        "the raw truncated argument text survives: {}",
        generation.tool_calls[0].arguments
    );
    // The message that replays still has to be legal JSON on the wire.
    let ContentBlock::ToolUse { input, .. } = &generation.assistant_message.content[0] else {
        panic!("expected a tool use block");
    };
    assert!(input.is_object());
}

#[test]
fn thinking_blocks_survive_with_their_signature() {
    // Anthropic rejects a thinking block whose signature is missing or altered,
    // so reasoning is persisted as this protocol's own blocks and round-tripped
    // rather than translated through a shape that cannot carry one.
    let generation = into_generation(replay(THINKING_STREAM), PROVIDER).expect("maps");

    let details = generation
        .reasoning_details
        .as_ref()
        .expect("a thinking turn records reasoning");
    assert_eq!(details[0]["type"], json!("thinking"));
    assert_eq!(details[0]["thinking"], json!("17 * 23 = 340 + 51 = 391."));
    assert_eq!(details[0]["signature"], json!("703d9d7329b9"));
    // Thinking is not assistant text.
    assert_eq!(generation.assistant_text, "17 * 23 = **391**");
}

#[test]
fn an_in_stream_error_is_a_parseable_event_not_a_partial_success() {
    // The gateway delivers post-start failures in-band with HTTP still 200.
    let event = IN_STREAM_ERROR
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|data| !data.is_empty())
        .filter_map(|data| serde_json::from_str::<StreamEvent>(data).ok())
        .find(|event| matches!(event, StreamEvent::Error { .. }))
        .expect("the fixture carries an in-band error");
    let StreamEvent::Error { error } = event else {
        unreachable!()
    };
    assert_eq!(error.r#type.as_deref(), Some("OverloadedError"));
    assert_eq!(
        error.message.as_deref(),
        Some("Upstream capacity exceeded.")
    );
}

#[test]
fn an_unrecognized_event_does_not_fail_the_whole_stream() {
    // A gateway is free to add events. Failing the parse would crash a run over
    // something that changes nothing about the turn.
    let event: StreamEvent =
        serde_json::from_str(r#"{"type":"message_annotation","note":"x"}"#).expect("parses");
    assert!(matches!(event, StreamEvent::Unknown));
}

#[test]
fn a_stream_that_never_reported_usage_still_produces_a_generation() {
    let response = replay(
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"minimax-m3\"}}\n\
         data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\
         data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\
         data: {\"type\":\"message_stop\"}\n",
    );
    let generation = into_generation(response, PROVIDER).expect("maps");
    assert_eq!(generation.assistant_text, "hi");
    assert!(generation.usage.is_none());
    assert!(generation.finish_reason.is_none());
}

// === Usage mapping ===

#[test]
fn disjoint_anthropic_token_components_are_summed_into_the_neutral_prompt_total() {
    // Anthropic's `input_tokens` EXCLUDES cache reads and writes, while the
    // neutral shape (and the context dial) expects the whole prompt with the
    // cached subset named beside it. Passing them through unchanged would render
    // 40 - 128 = negative input and lose the cached prefix entirely.
    let generation = into_generation(replay(THINKING_STREAM), PROVIDER).expect("maps");
    let usage = generation.usage.expect("the stream reported usage");
    let serialized = serde_json::to_value(&usage).expect("usage serializes");

    assert_eq!(serialized["prompt_tokens"], json!(55 + 128 + 12));
    assert_eq!(serialized["completion_tokens"], json!(65));
    assert_eq!(serialized["total_tokens"], json!(55 + 128 + 12 + 65));
    assert_eq!(
        serialized["prompt_tokens_details"]["cached_tokens"],
        json!(128)
    );
    assert_eq!(
        serialized["prompt_tokens_details"]["cache_creation_input_tokens"],
        json!(12)
    );
}

#[test]
fn neutral_usage_keeps_its_eight_key_serialization() {
    // The frontend's token rollup and the radial context dial read these exact
    // keys out of stored transcript payloads, so the shape is a contract.
    let generation = into_generation(replay(TEXT_STREAM), PROVIDER).expect("maps");
    let serialized = serde_json::to_value(generation.usage.expect("usage")).expect("serializes");
    let mut keys: Vec<&str> = serialized
        .as_object()
        .expect("usage is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "completion_tokens",
            "completion_tokens_details",
            "cost",
            "cost_details",
            "prompt_tokens",
            "prompt_tokens_details",
            "reasoning_tokens",
            "total_tokens",
        ]
    );
}

#[test]
fn a_string_cost_is_read_as_a_number() {
    // The gateway reports cost as a JSON string where the rest of the wire uses
    // numbers; a turn's spend must not be lost to that.
    let generation = into_generation(replay(TEXT_STREAM), PROVIDER).expect("maps");
    assert_eq!(generation.usage.expect("usage").cost, Some(0.0));
}

// === Transcript replay ===

fn assistant_event(text: Option<&str>, tool_uses: Vec<ToolUseInfo>) -> TranscriptEvent {
    TranscriptEvent {
        event_type: "assistant".to_string(),
        session_id: Some("sess-1".to_string()),
        parent_tool_use_id: None,
        content: text.map(str::to_string),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: (!tool_uses.is_empty()).then_some(tool_uses),
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        queued_message_id: None,
        raw: None,
    }
}

fn tool_result_event(id: &str, result: &str) -> TranscriptEvent {
    TranscriptEvent {
        event_type: "tool_result".to_string(),
        session_id: Some("sess-1".to_string()),
        parent_tool_use_id: None,
        content: None,
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: Some(id.to_string()),
        tool_result: Some(result.to_string()),
        is_error: false,
        thinking_ms: None,
        queued_message_id: None,
        raw: None,
    }
}

fn tool_use(id: &str, name: &str) -> ToolUseInfo {
    ToolUseInfo {
        id: id.to_string(),
        name: name.to_string(),
        input: json!({"path": "/etc/hosts"}),
    }
}

#[test]
fn a_tool_result_replays_as_a_user_block_after_the_call_that_asked_for_it() {
    // There is no `tool` role on this protocol: a result is a block inside the
    // USER message following the assistant turn that called it.
    let messages = vec![
        transcript_event_to_message(
            "assistant",
            assistant_event(None, vec![tool_use("c1", "read")]),
        )
        .expect("a tool-calling turn replays"),
        transcript_event_to_message("tool_result", tool_result_event("c1", "127.0.0.1"))
            .expect("a result replays"),
    ];
    let normalized = normalize_tool_groups(messages);

    assert_eq!(normalized.len(), 2);
    assert_eq!(normalized[0].role, ASSISTANT_ROLE);
    assert_eq!(normalized[1].role, USER_ROLE);
    let ContentBlock::ToolResult {
        tool_use_id,
        content,
        ..
    } = &normalized[1].content[0]
    else {
        panic!("expected a tool_result block");
    };
    assert_eq!(tool_use_id, "c1");
    assert_eq!(content, "127.0.0.1");
}

#[test]
fn a_result_stored_out_of_order_is_still_paired_to_its_call() {
    // A foreground question can suspend a turn between dispatch and storage, so
    // results are associated by call id rather than adjacency.
    let messages = vec![
        transcript_event_to_message(
            "assistant",
            assistant_event(None, vec![tool_use("c1", "read"), tool_use("c2", "read")]),
        )
        .expect("replays"),
        transcript_event_to_message("tool_result", tool_result_event("c2", "second"))
            .expect("replays"),
        transcript_event_to_message("tool_result", tool_result_event("c1", "first"))
            .expect("replays"),
    ];
    let normalized = normalize_tool_groups(messages);

    // Both results land in one user message, in the order the calls were made.
    assert_eq!(normalized.len(), 2);
    let ids: Vec<&str> = normalized[1]
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolResult { tool_use_id, .. } => Some(tool_use_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec!["c1", "c2"]);
}

#[test]
fn a_call_whose_result_never_landed_gets_a_stated_placeholder() {
    // An unanswered tool_use is rejected outright, so an interrupted turn must
    // still produce a complete pairing rather than a request that cannot be sent.
    let messages = vec![transcript_event_to_message(
        "assistant",
        assistant_event(None, vec![tool_use("c1", "read")]),
    )
    .expect("replays")];
    let normalized = normalize_tool_groups(messages);

    assert_eq!(normalized.len(), 2);
    let ContentBlock::ToolResult { content, .. } = &normalized[1].content[0] else {
        panic!("expected a synthesized result");
    };
    assert!(content.contains("Interrupted"), "{content}");
}

#[test]
fn an_empty_tool_result_is_stated_rather_than_sent_blank() {
    let message = transcript_event_to_message("tool_result", tool_result_event("c1", "   "))
        .expect("replays");
    let ContentBlock::ToolResult { content, .. } = &message.content[0] else {
        panic!("expected a tool_result block");
    };
    assert_eq!(content, "The tool returned no output.");
}

#[test]
fn adjacent_same_role_messages_are_merged_so_roles_alternate() {
    // Two user messages in a row are rejected. This is what lets a batch of tool
    // results and the user's next prompt share one message.
    let merged = coalesce_roles(vec![
        MessagesMessage::tool_result("c1".to_string(), "out".to_string()),
        user_message("and now do this".to_string()),
    ]);

    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].role, USER_ROLE);
    assert_eq!(merged[0].content.len(), 2);
}

#[test]
fn stored_reasoning_replays_as_thinking_blocks_before_the_call_it_produced() {
    // Anthropic requires the thinking block that produced a tool call to precede
    // it, with its signature intact.
    let mut event = assistant_event(Some("reading"), vec![tool_use("c1", "read")]);
    event.raw = Some(json!({
        "reasoning_details": [
            {"type": "thinking", "thinking": "check the file", "signature": "sig-1"}
        ]
    }));
    let message = transcript_event_to_message("assistant", event).expect("replays");

    assert!(matches!(message.content[0], ContentBlock::Thinking { .. }));
    let ContentBlock::Thinking { signature, .. } = &message.content[0] else {
        unreachable!()
    };
    assert_eq!(signature.as_deref(), Some("sig-1"));
    assert!(matches!(message.content[1], ContentBlock::Text { .. }));
    assert!(matches!(message.content[2], ContentBlock::ToolUse { .. }));
}

#[test]
fn a_turn_that_only_thought_is_not_replayed_as_a_message() {
    // A message carrying nothing but reasoning says nothing the model can
    // continue from, and a thinking block that answers nothing is rejected.
    let mut event = assistant_event(None, Vec::new());
    event.raw = Some(json!({
        "reasoning_details": [{"type": "thinking", "thinking": "hmm", "signature": "s"}]
    }));
    assert!(transcript_event_to_message("assistant", event).is_none());
}

#[test]
fn a_user_turns_images_replay_as_image_blocks_without_an_empty_text_block() {
    // An empty text block beside an image is rejected, and a persisted one makes
    // every later replay fail the same way (CAIRN-3263).
    let content = MessageContent {
        text: "   ".to_string(),
        images: vec![crate::agent_process::stdin::MessageImage {
            mime_type: "image/png".to_string(),
            bytes: vec![1, 2, 3],
        }],
    };
    let message = MessagesMessage::user_content(&content);

    assert_eq!(message.content.len(), 1);
    let ContentBlock::Image { source } = &message.content[0] else {
        panic!("expected an image block");
    };
    assert_eq!(source.media_type, "image/png");
    assert_eq!(source.kind, "base64");
}

#[test]
fn trimming_collapses_aged_tool_output_and_leaves_the_decisions_intact() {
    // Only tool results are eligible: the system prompt, user turns, and the
    // assistant's tool-call decisions are what the model reasons from.
    let mut messages = vec![
        MessagesMessage::system("sys".to_string()),
        MessagesMessage::assistant(vec![ContentBlock::ToolUse {
            id: "c1".to_string(),
            name: "read".to_string(),
            input: json!({}),
        }]),
        MessagesMessage::tool_result("c1".to_string(), "x\n".repeat(4_000)),
    ];
    messages.extend((0..10).map(|i| user_message(format!("turn {i}"))));

    let trimmed = trim_conversation_to_budget(&messages, 100);

    let ContentBlock::ToolResult { content, .. } = &trimmed[2].content[0] else {
        panic!("expected the tool result");
    };
    assert!(content.contains("read output elided"), "{content}");
    assert_eq!(
        trimmed[0].content.len(),
        1,
        "the system prompt is untouched"
    );
    assert!(matches!(
        trimmed[1].content[0],
        ContentBlock::ToolUse { .. }
    ));
}

// === Premature end of stream ===

const EOF_BEFORE_STOP: &str = include_str!("testdata/eof_before_stop_stream.sse");

/// Replay a fixture and report whether the protocol said it was done.
fn replay_with_terminal(fixture: &str) -> (MessagesResponse, bool) {
    let mut aggregate = StreamingMessage::default();
    let mut streamed_text = false;
    for line in fixture.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        let event: StreamEvent =
            serde_json::from_str(data).unwrap_or_else(|error| panic!("{data}: {error}"));
        if let StreamEvent::ContentBlockDelta {
            delta: super::wire::BlockDelta::TextDelta { .. },
            ..
        } = &event
        {
            streamed_text = true;
        }
        aggregate.apply(&event);
    }
    let saw_terminal = aggregate.saw_terminal();
    (aggregate.into_response(streamed_text), saw_terminal)
}

#[test]
fn a_stream_cut_off_before_message_stop_is_not_a_complete_message() {
    // A proxy can close a chunked response without any read error, so reaching
    // end-of-stream says nothing about whether the model finished.
    let (_, saw_terminal) = replay_with_terminal(EOF_BEFORE_STOP);
    assert!(!saw_terminal);

    // A stream that did reach message_stop is complete even though the fixture
    // ends right there — the cost-carrying ping after it is optional.
    let (_, complete) = replay_with_terminal(THINKING_STREAM);
    assert!(complete);
}

#[test]
fn a_truncated_stream_is_refused_rather_than_stored_as_a_result() {
    let error = require_terminal_event(PROVIDER, TERMINAL_EVENTS, false, false)
        .expect_err("a stream that never finished is not a generation");
    // The failure has to name the signal this family was waiting for, not just
    // report that something ended.
    assert!(error.contains("message_stop"), "{error}");
    assert!(error.contains("not being recorded as a result"), "{error}");

    // A completed stream passes, and a cancelled turn is the one legitimate
    // early ending: the user asked for it and the run lands idle, not failed.
    assert!(require_terminal_event(PROVIDER, TERMINAL_EVENTS, true, false).is_ok());
    assert!(require_terminal_event(PROVIDER, TERMINAL_EVENTS, false, true).is_ok());
}

#[test]
fn a_side_effecting_call_from_a_truncated_stream_never_reaches_dispatch() {
    // This is what the refusal is actually protecting. The fixture's `run` call
    // is complete, parseable JSON — a destructive one — and the generation
    // carries NO stop reason, so the loop's truncation guard has nothing to
    // judge it by. Only refusing the stream keeps it from being dispatched.
    let (response, saw_terminal) = replay_with_terminal(EOF_BEFORE_STOP);
    let generation = into_generation(response, PROVIDER).expect("the partial message still maps");

    assert_eq!(generation.tool_calls.len(), 1);
    assert_eq!(generation.tool_calls[0].name, "run");
    assert!(serde_json::from_str::<serde_json::Value>(&generation.tool_calls[0].arguments).is_ok());
    assert!(generation.finish_reason.is_none());

    assert!(!saw_terminal);
    assert!(require_terminal_event(PROVIDER, TERMINAL_EVENTS, saw_terminal, false).is_err());
}
