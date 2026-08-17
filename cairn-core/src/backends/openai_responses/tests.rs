//! OpenAI Responses contract tests.
//!
//! The fixtures under `testdata/` are recordings of real OpenCode Go
//! `/zen/go/v1/responses` traffic with identifiers rewritten, so these assert
//! against the dialect Cairn actually has to parse. Nothing here touches the
//! network or needs credentials.

use super::conversation::{
    normalize_call_pairs, transcript_event_to_items, trim_conversation_to_budget, user_item,
};
use super::generation::into_generation;
use super::http::{apply_output_schema, build_body, require_terminal_event, tool_schemas};
use super::wire::{
    ContentPart, ResponseEnvelope, ResponseStreamEvent, ResponsesItem, ResponsesResponse,
    ResponsesTurn, StreamingResponse,
};
use crate::agent_process::stdin::{MessageContent, MessageImage};
use crate::agent_process::stream::{ToolUseInfo, TranscriptEvent};
use crate::backends::{SessionConfig, SessionStart};
use serde_json::json;

const PROVIDER: &str = "OpenCode Go";

/// A user input item built the way production builds one, so these tests
/// exercise the real constructor rather than a test-only shortcut beside it.
fn user_message(text: impl Into<String>) -> ResponsesItem {
    user_item(&MessageContent::text(text.into()))
}

const TOOL_CALL_STREAM: &str = include_str!("testdata/tool_call_stream.sse");
const REASONING_RESPONSE: &str = include_str!("testdata/reasoning_response.json");
const INCOMPLETE_STREAM: &str = include_str!("testdata/incomplete_stream.sse");
const FAILED_STREAM: &str = include_str!("testdata/failed_stream.sse");

fn config(
    reasoning_effort: Option<&str>,
    output_schema: Option<serde_json::Value>,
) -> SessionConfig {
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
        max_thinking_tokens: None,
        reasoning_effort: reasoning_effort.map(str::to_string),
        service_tier: None,
        permissions: crate::backends::AgentPermissions::new(crate::models::Fence::Allow),
        bidirectional: false,
        identity: None,
        output_schema,
        ambient: false,
        is_ephemeral_call: false,
    }
}

/// Replay a recorded stream through the aggregator exactly as the transport
/// does, so these tests exercise the same parsing path a live turn uses.
fn replay(fixture: &str) -> ResponsesResponse {
    let mut aggregate = StreamingResponse::default();
    let mut streamed_text = false;
    for line in fixture.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        let event: ResponseStreamEvent =
            serde_json::from_str(data).unwrap_or_else(|error| panic!("{data}: {error}"));
        if matches!(event, ResponseStreamEvent::OutputTextDelta { .. }) {
            streamed_text = true;
        }
        aggregate.apply(&event);
    }
    aggregate.into_response(streamed_text)
}

// === Request construction ===

#[test]
fn the_system_prompt_becomes_instructions_not_an_input_item() {
    let turns = vec![
        ResponsesTurn::one(ResponsesItem::Instructions {
            text: "You are Cairn.".to_string(),
        }),
        ResponsesTurn::one(user_message("hi".to_string())),
    ];
    let body = build_body("grok-4.5", &turns, &config(None, None), true);

    assert_eq!(body["instructions"], json!("You are Cairn."));
    let input = body["input"].as_array().expect("input is an array");
    assert_eq!(input.len(), 1);
    assert_eq!(input[0]["role"], json!("user"));
    assert_eq!(input[0]["content"][0]["type"], json!("input_text"));
}

#[test]
fn tools_are_advertised_in_this_protocols_own_wrapper() {
    // Same three verbs as every other family, flattened beside a `type`
    // discriminator rather than nested under a `function` object.
    let tools = tool_schemas();
    let names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(names, vec!["read", "write", "run"]);
    assert_eq!(tools[0]["type"], json!("function"));
    assert_eq!(tools[1]["parameters"]["required"][0], json!("changes"));
    assert!(tools[0].get("function").is_none());
}

#[test]
fn a_configured_effort_reaches_the_provider_as_written() {
    // Responses takes an effort word natively, so unlike the Messages family
    // there is nothing to invent here.
    let body = build_body(
        "gpt-5.6-luna",
        &[ResponsesTurn::one(user_message("hi".to_string()))],
        &config(Some("low"), None),
        true,
    );
    assert_eq!(body["reasoning"]["effort"], json!("low"));

    let unset = build_body(
        "gpt-5.6-luna",
        &[ResponsesTurn::one(user_message("hi".to_string()))],
        &config(None, None),
        true,
    );
    assert!(unset.get("reasoning").is_none());
}

#[test]
fn a_schema_constrained_run_uses_the_native_json_schema_format() {
    let schema = json!({"type": "object", "properties": {"answer": {"type": "string"}}});
    let mut body = json!({ "model": "grok-4.5" });
    apply_output_schema(&mut body, Some(&schema));

    assert_eq!(body["text"]["format"]["type"], json!("json_schema"));
    assert_eq!(body["text"]["format"]["name"], json!("cairn_output"));
    assert_eq!(body["text"]["format"]["strict"], json!(true));
    assert_eq!(body["text"]["format"]["schema"], schema);
}

#[test]
fn no_schema_leaves_the_response_unconstrained() {
    let mut body = json!({ "model": "grok-4.5" });
    apply_output_schema(&mut body, None);
    assert!(body.get("text").is_none());
}

#[test]
fn the_whole_transcript_is_replayed_rather_than_a_previous_response_id() {
    // Cairn resumes from its own persisted transcript, across restarts and
    // across gateways that need not retain anything, so a conversation that
    // only exists on the provider's side is one Cairn cannot continue.
    let turns = vec![
        ResponsesTurn::one(user_message("first".to_string())),
        ResponsesTurn::one(ResponsesItem::assistant_text("reply".to_string())),
        ResponsesTurn::one(user_message("second".to_string())),
    ];
    let body = build_body("grok-4.5", &turns, &config(None, None), true);

    assert_eq!(body["input"].as_array().expect("array").len(), 3);
    assert!(body.get("previous_response_id").is_none());
}

// === Streaming ===

#[test]
fn assistant_text_survives_a_terminal_envelope_that_drops_its_message() {
    // Recorded behaviour: `response.completed` carried ONLY the function-call
    // item, dropping the message that had streamed beside it. Trusting the
    // terminal envelope's `output` would silently lose the assistant's text.
    let generation = into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("maps");
    assert_eq!(generation.assistant_text, "I'll read it.");
}

#[test]
fn function_calls_keep_their_call_ids_names_and_arguments() {
    let generation = into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("maps");

    assert_eq!(generation.tool_calls.len(), 2);
    // `call_id`, not the item id: a function_call_output quoting the wrong one
    // orphans the result.
    assert_eq!(generation.tool_calls[0].id, "call-first");
    assert_eq!(generation.tool_calls[0].name, "read");
    assert_eq!(
        generation.tool_calls[0].arguments,
        r#"{"path":"/etc/hosts"}"#
    );
    assert_eq!(generation.tool_calls[1].id, "call-second");
    assert_eq!(generation.finish_reason.as_deref(), Some("tool_calls"));
}

#[test]
fn streamed_argument_deltas_survive_a_done_event_that_reports_them_empty() {
    // The fixture's `output_item.done` re-sends the call with `arguments: ""`
    // after the deltas carried the real payload. Taking the finished item
    // verbatim would erase what was just streamed.
    let generation = into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("maps");
    assert_eq!(
        generation.tool_calls[1].arguments,
        r#"{"commands":[{"command":"ls"}]}"#
    );
}

#[test]
fn tool_names_are_canonicalized_before_dispatch_and_before_replay() {
    let generation = into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("maps");

    assert_eq!(generation.tool_calls[1].name, "run");
    let replayed = generation
        .assistant_message
        .items
        .iter()
        .find_map(|item| match item {
            ResponsesItem::FunctionCall { call_id, name, .. } if call_id == "call-second" => {
                Some(name.clone())
            }
            _ => None,
        })
        .expect("the replayed turn carries the second call");
    assert_eq!(replayed, "run");
}

#[test]
fn an_output_cap_cutoff_reaches_the_loop_as_length() {
    // The loop refuses to dispatch a possibly-truncated side-effecting call when
    // it sees "length". Responses reports that as status `incomplete` with
    // reason `max_output_tokens`, so leaving it untranslated would disarm the
    // truncation guard.
    let generation = into_generation(replay(INCOMPLETE_STREAM), PROVIDER).expect("maps");

    assert_eq!(generation.finish_reason.as_deref(), Some("length"));
    // The partially-streamed arguments still reach the repair path as written.
    assert_eq!(generation.tool_calls.len(), 1);
    assert!(
        generation.tool_calls[0].arguments.ends_with("fn main"),
        "{}",
        generation.tool_calls[0].arguments
    );
}

#[test]
fn a_failed_response_is_recognized_as_a_failure_event() {
    let failure = FAILED_STREAM
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|data| !data.is_empty())
        .filter_map(|data| serde_json::from_str::<ResponseStreamEvent>(data).ok())
        .find(|event| matches!(event, ResponseStreamEvent::Failed { .. }))
        .expect("the fixture carries a failure");
    let ResponseStreamEvent::Failed { response } = failure else {
        unreachable!()
    };
    let error = response.error.expect("a failure names its error");
    assert_eq!(error.code.as_deref(), Some("server_error"));
    assert!(error
        .message
        .as_deref()
        .unwrap_or_default()
        .contains("Endpoint is unavailable"));
}

#[test]
fn an_unrecognized_event_does_not_fail_the_whole_stream() {
    let event: ResponseStreamEvent =
        serde_json::from_str(r#"{"type":"response.output_text.annotation.added"}"#)
            .expect("parses");
    assert!(matches!(event, ResponseStreamEvent::Unknown));
}

// === Reasoning and usage ===

#[test]
fn reasoning_items_round_trip_with_their_opaque_content() {
    // The provider validates `encrypted_content` on continuation; a summary
    // alone will not stand in for it.
    let envelope: ResponseEnvelope =
        serde_json::from_str(REASONING_RESPONSE).expect("the fixture parses");
    let generation =
        into_generation(ResponsesResponse::from_envelope(envelope), PROVIDER).expect("maps");

    let details = generation
        .reasoning_details
        .as_ref()
        .expect("a reasoning turn records reasoning");
    assert_eq!(details[0]["type"], json!("reasoning"));
    assert_eq!(
        details[0]["encrypted_content"],
        json!("gAAAAABOPAQUE-REASONING-BLOB")
    );
    assert!(details[0]["summary"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .contains("391"));
    // Reasoning is not assistant text.
    assert_eq!(generation.assistant_text, "17 × 23 = **391**");
    assert_eq!(generation.finish_reason.as_deref(), Some("stop"));
}

#[test]
fn responses_usage_passes_through_with_its_reasoning_tokens() {
    // Unlike Anthropic, this protocol already counts the way the neutral shape
    // does: `input_tokens` is the whole prompt with the cached subset named
    // beside it, so the components are NOT recombined.
    let generation = into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("maps");
    let serialized =
        serde_json::to_value(generation.usage.expect("usage")).expect("usage serializes");

    assert_eq!(serialized["prompt_tokens"], json!(322));
    assert_eq!(serialized["completion_tokens"], json!(40));
    assert_eq!(serialized["total_tokens"], json!(362));
    assert_eq!(
        serialized["prompt_tokens_details"]["cached_tokens"],
        json!(128)
    );
    assert_eq!(serialized["reasoning_tokens"], json!(17));
}

#[test]
fn neutral_usage_keeps_its_eight_key_serialization() {
    let generation = into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("maps");
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
    let generation = into_generation(replay(TOOL_CALL_STREAM), PROVIDER).expect("maps");
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
fn one_assistant_turn_replays_as_several_sibling_items() {
    // This protocol has no message that can carry reasoning, text, and calls at
    // once, so a single stored turn becomes an ordered run of items.
    let mut event = assistant_event(Some("reading"), vec![tool_use("c1", "read")]);
    event.raw = Some(json!({
        "reasoning_details": [
            {"type": "reasoning", "id": "rs_1", "encrypted_content": "blob",
             "summary": [{"type": "summary_text", "text": "check it"}]}
        ]
    }));
    let items = transcript_event_to_items("assistant", event);

    assert_eq!(items.len(), 3);
    assert!(matches!(items[0], ResponsesItem::Reasoning { .. }));
    assert!(matches!(items[1], ResponsesItem::Message { .. }));
    assert!(matches!(items[2], ResponsesItem::FunctionCall { .. }));
}

#[test]
fn a_replayed_reasoning_item_keeps_the_content_the_provider_validates() {
    let mut event = assistant_event(Some("ok"), Vec::new());
    event.raw = Some(json!({
        "reasoning_details": [
            {"type": "reasoning", "id": "rs_1", "encrypted_content": "opaque-blob", "summary": []}
        ]
    }));
    let items = transcript_event_to_items("assistant", event);

    let ResponsesItem::Reasoning {
        encrypted_content, ..
    } = &items[0]
    else {
        panic!("expected a reasoning item");
    };
    assert_eq!(encrypted_content.as_deref(), Some("opaque-blob"));
}

#[test]
fn a_call_output_replays_directly_after_the_call_that_asked_for_it() {
    let mut items = transcript_event_to_items(
        "assistant",
        assistant_event(None, vec![tool_use("c1", "read")]),
    );
    items.extend(transcript_event_to_items(
        "tool_result",
        tool_result_event("c1", "127.0.0.1"),
    ));
    let normalized = normalize_call_pairs(items);

    assert_eq!(normalized.len(), 2);
    let ResponsesItem::FunctionCallOutput { call_id, output } = &normalized[1] else {
        panic!("expected the paired output");
    };
    assert_eq!(call_id, "c1");
    assert_eq!(output, "127.0.0.1");
}

#[test]
fn an_output_stored_out_of_order_is_still_paired_to_its_call() {
    // A foreground question can suspend a turn between dispatch and storage, so
    // association is by call id rather than adjacency.
    let mut items = transcript_event_to_items(
        "assistant",
        assistant_event(None, vec![tool_use("c1", "read"), tool_use("c2", "read")]),
    );
    items.extend(transcript_event_to_items(
        "tool_result",
        tool_result_event("c2", "second"),
    ));
    items.extend(transcript_event_to_items(
        "tool_result",
        tool_result_event("c1", "first"),
    ));
    let normalized = normalize_call_pairs(items);

    let pairs: Vec<&str> = normalized
        .iter()
        .filter_map(|item| match item {
            ResponsesItem::FunctionCall { call_id, .. } => Some(call_id.as_str()),
            ResponsesItem::FunctionCallOutput { output, .. } => Some(output.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(pairs, vec!["c1", "first", "c2", "second"]);
}

#[test]
fn a_call_whose_output_never_landed_gets_a_stated_placeholder() {
    // An unanswered function_call is rejected, so an interrupted turn must still
    // produce a complete pairing rather than a request that cannot be sent.
    let items = normalize_call_pairs(transcript_event_to_items(
        "assistant",
        assistant_event(None, vec![tool_use("c1", "read")]),
    ));

    assert_eq!(items.len(), 2);
    let ResponsesItem::FunctionCallOutput { output, .. } = &items[1] else {
        panic!("expected a synthesized output");
    };
    assert!(output.contains("Interrupted"), "{output}");
}

#[test]
fn an_empty_call_output_is_stated_rather_than_sent_blank() {
    let items = transcript_event_to_items("tool_result", tool_result_event("c1", "  "));
    let ResponsesItem::FunctionCallOutput { output, .. } = &items[0] else {
        panic!("expected an output item");
    };
    assert_eq!(output, "The tool returned no output.");
}

#[test]
fn a_user_turns_images_replay_without_an_empty_text_part() {
    let content = MessageContent {
        text: "  ".to_string(),
        images: vec![MessageImage {
            mime_type: "image/png".to_string(),
            bytes: vec![1, 2, 3],
        }],
    };
    let ResponsesItem::Message { content, role, .. } = user_item(&content) else {
        panic!("expected a message item");
    };

    assert_eq!(role, "user");
    assert_eq!(content.len(), 1);
    let ContentPart::InputImage { image_url } = &content[0] else {
        panic!("expected an image part");
    };
    assert!(image_url.starts_with("data:image/png;base64,"));
}

#[test]
fn trimming_collapses_aged_call_output_and_leaves_the_decisions_intact() {
    let mut turns = vec![
        ResponsesTurn::one(ResponsesItem::Instructions {
            text: "sys".to_string(),
        }),
        ResponsesTurn::one(ResponsesItem::FunctionCall {
            id: None,
            call_id: "c1".to_string(),
            name: "read".to_string(),
            arguments: "{}".to_string(),
        }),
        ResponsesTurn::one(ResponsesItem::FunctionCallOutput {
            call_id: "c1".to_string(),
            output: "x\n".repeat(4_000),
        }),
    ];
    turns.extend((0..10).map(|i| ResponsesTurn::one(user_message(format!("turn {i}")))));

    let trimmed = trim_conversation_to_budget(&turns, 100);

    let ResponsesItem::FunctionCallOutput { output, .. } = &trimmed[2].items[0] else {
        panic!("expected the call output");
    };
    assert!(output.contains("read output elided"), "{output}");
    assert!(matches!(
        trimmed[1].items[0],
        ResponsesItem::FunctionCall { .. }
    ));
}

// === Premature end of stream ===

const EOF_BEFORE_TERMINAL: &str = include_str!("testdata/eof_before_terminal_stream.sse");

/// Replay a fixture and report whether the response announced an outcome.
fn replay_with_terminal(fixture: &str) -> (ResponsesResponse, bool) {
    let mut aggregate = StreamingResponse::default();
    let mut streamed_text = false;
    for line in fixture.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        let event: ResponseStreamEvent =
            serde_json::from_str(data).unwrap_or_else(|error| panic!("{data}: {error}"));
        if matches!(event, ResponseStreamEvent::OutputTextDelta { .. }) {
            streamed_text = true;
        }
        aggregate.apply(&event);
    }
    let saw_terminal = aggregate.saw_terminal();
    (aggregate.into_response(streamed_text), saw_terminal)
}

#[test]
fn a_stream_cut_off_before_a_terminal_event_is_not_a_complete_response() {
    // A proxy can close a chunked response without any read error, so reaching
    // end-of-stream says nothing about whether the model finished.
    let (_, saw_terminal) = replay_with_terminal(EOF_BEFORE_TERMINAL);
    assert!(!saw_terminal);

    // `completed` and `incomplete` are both real outcomes the provider reported.
    assert!(replay_with_terminal(TOOL_CALL_STREAM).1);
    assert!(replay_with_terminal(INCOMPLETE_STREAM).1);
}

#[test]
fn a_truncated_stream_is_refused_rather_than_stored_as_a_result() {
    let error = require_terminal_event(PROVIDER, false, false)
        .expect_err("a stream that never finished is not a generation");
    assert!(
        error.contains("ended before the response reported an outcome"),
        "{error}"
    );
    assert!(error.contains("not being recorded as a result"), "{error}");

    assert!(require_terminal_event(PROVIDER, true, false).is_ok());
    // A cancelled turn is the one legitimate early ending.
    assert!(require_terminal_event(PROVIDER, false, true).is_ok());
}

#[test]
fn a_side_effecting_call_from_a_truncated_stream_never_reaches_dispatch() {
    // This is what the refusal is actually protecting. The fixture's `run` call
    // is complete, parseable JSON — a destructive one — and the response never
    // reported a status, so nothing downstream can tell it was cut off. Only
    // refusing the stream keeps it from being dispatched.
    let (response, saw_terminal) = replay_with_terminal(EOF_BEFORE_TERMINAL);
    let generation = into_generation(response, PROVIDER).expect("the partial response still maps");

    assert_eq!(generation.tool_calls.len(), 1);
    assert_eq!(generation.tool_calls[0].name, "run");
    assert!(serde_json::from_str::<serde_json::Value>(&generation.tool_calls[0].arguments).is_ok());

    assert!(!saw_terminal);
    assert!(require_terminal_event(PROVIDER, saw_terminal, false).is_err());
}
