use super::context::{
    context_fit_budget, estimate_conversation_tokens, fit_conversation, trim_conversation_to_budget,
};
use super::conversation::{normalize_tool_call_groups, transcript_event_to_chat_message};
use super::http::tool_schemas;
use super::wire::{
    default_function_type, ChatContent, ChatMessage, ChatStreamChunk, ChatStreamDelta,
    PlainReasoning, StreamingAggregate, ToolCall, ToolFunction,
};
use crate::{MessageContent, MessageImage, ReplayEvent, ToolUseInfo};
use serde_json::json;

#[test]
fn normalize_tool_results_repairs_missing_and_separated_results() {
    let messages = vec![
        assistant_with_calls(&["missing", "late"]),
        ChatMessage::user("answer arrived separately".to_string()),
        ChatMessage::tool("late".to_string(), "recorded".to_string()),
    ];
    let normalized = normalize_tool_call_groups(messages);
    assert_eq!(normalized[1].tool_call_id.as_deref(), Some("missing"));
    assert_eq!(
        normalized[1]
            .content
            .as_ref()
            .and_then(ChatContent::as_text),
        Some("Interrupted before the tool result was recorded.")
    );
    assert_eq!(normalized[2].tool_call_id.as_deref(), Some("late"));
    assert_eq!(normalized[3].role, "user");
}

#[test]
fn normalize_tool_results_drops_orphans_and_duplicates() {
    let messages = vec![
        ChatMessage::tool("orphan".to_string(), "unused".to_string()),
        assistant_with_calls(&["call"]),
        ChatMessage::tool("call".to_string(), "first".to_string()),
        ChatMessage::tool("call".to_string(), "last".to_string()),
    ];
    let normalized = normalize_tool_call_groups(messages);
    assert_eq!(normalized.len(), 2);
    assert_eq!(normalized[1].tool_call_id.as_deref(), Some("call"));
    assert_eq!(
        normalized[1]
            .content
            .as_ref()
            .and_then(ChatContent::as_text),
        Some("last")
    );
}

#[test]
fn normalize_tool_results_handles_more_than_two_hundred_messages() {
    let mut messages = (0..250)
        .map(|index| ChatMessage::user(format!("message {index}")))
        .collect::<Vec<_>>();
    messages.push(assistant_with_calls(&["call"]));
    messages.push(ChatMessage::tool("call".to_string(), "result".to_string()));
    let normalized = normalize_tool_call_groups(messages);
    assert_eq!(normalized.len(), 252);
    assert_eq!(normalized[251].tool_call_id.as_deref(), Some("call"));
}

fn function_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        r#type: default_function_type(),
        function: ToolFunction {
            name: "write".to_string(),
            arguments: "{}".to_string(),
        },
    }
}

fn assistant_with_calls(ids: &[&str]) -> ChatMessage {
    ChatMessage {
        role: "assistant".to_string(),
        content: None,
        tool_call_id: None,
        tool_calls: Some(ids.iter().map(|id| function_call(id)).collect()),
        reasoning_details: None,
        reasoning: None,
    }
}

#[test]
fn normalize_tool_results_follows_assistant_tool_call_order() {
    // A suspended first tool call (`w`) persists the later call's placeholder
    // (`r`) at suspend time and its own real result only on resume, so the
    // raw event order is [assistant, tool(r), tool(w)] — the wrong order for
    // the assistant's tool_calls [w, r].
    let messages = vec![
        ChatMessage::user("hi".to_string()),
        assistant_with_calls(&["w", "r"]),
        ChatMessage::tool("r".to_string(), "placeholder".to_string()),
        ChatMessage::tool("w".to_string(), "real answer".to_string()),
    ];
    let reordered = normalize_tool_call_groups(messages);
    // Tool results now follow the assistant's tool_calls order: w then r.
    assert_eq!(reordered[2].tool_call_id.as_deref(), Some("w"));
    assert_eq!(
        reordered[2].content.as_ref().and_then(ChatContent::as_text),
        Some("real answer")
    );
    assert_eq!(reordered[3].tool_call_id.as_deref(), Some("r"));
    // Non-tool messages keep their position.
    assert_eq!(reordered[0].role, "user");
    assert_eq!(reordered[1].role, "assistant");
}

#[test]
fn normalize_tool_results_is_noop_when_already_ordered() {
    let messages = vec![
        assistant_with_calls(&["a", "b"]),
        ChatMessage::tool("a".to_string(), "ra".to_string()),
        ChatMessage::tool("b".to_string(), "rb".to_string()),
    ];
    let reordered = normalize_tool_call_groups(messages);
    assert_eq!(reordered[1].tool_call_id.as_deref(), Some("a"));
    assert_eq!(reordered[2].tool_call_id.as_deref(), Some("b"));
}

#[test]
fn user_message_serializes_native_openai_image_parts_without_base64_text() {
    let stable_uri =
        "cairn://p/CAIRN/images/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let message = ChatMessage::user_content(&MessageContent {
        text: format!("inspect {stable_uri}"),
        images: vec![MessageImage {
            mime_type: "image/png".to_string(),
            bytes: b"\x89PNG\r\n\x1a\nfixture".to_vec(),
        }],
    });

    let request_messages = serde_json::to_value([message]).unwrap();
    let content = request_messages[0]["content"]
        .as_array()
        .expect("multimodal content parts");
    assert_eq!(
        content[0],
        json!({ "type": "text", "text": format!("inspect {stable_uri}") })
    );
    assert_eq!(content[1]["type"], "image_url");
    assert!(content[1]["image_url"]["url"]
        .as_str()
        .unwrap()
        .starts_with("data:image/png;base64,"));
    assert!(!content[0]["text"].as_str().unwrap().contains("base64,"));
}

#[test]
fn chat_message_serializes_tool_call_id_for_tool_role() {
    let value = serde_json::to_value(ChatMessage::tool("call-1".into(), "result".into())).unwrap();
    assert_eq!(value["role"], "tool");
    assert_eq!(value["tool_call_id"], "call-1");
}

#[test]
fn delta_parses_reasoning_and_reasoning_details() {
    let delta: ChatStreamDelta = serde_json::from_value(json!({
        "reasoning": "let me think",
        "reasoning_details": [{
            "type": "reasoning.text",
            "id": "rd-1",
            "format": "anthropic-claude-v1",
            "index": 0,
            "signature": "sig-abc",
            "text": "structured thought"
        }]
    }))
    .unwrap();
    assert_eq!(delta.reasoning.as_deref(), Some("let me think"));
    let details = delta.reasoning_details.expect("reasoning_details present");
    assert_eq!(details.len(), 1);
    assert_eq!(details[0]["type"], "reasoning.text");
    assert_eq!(details[0]["signature"], "sig-abc");
    assert_eq!(details[0]["index"], 0);
    assert_eq!(details[0]["format"], "anthropic-claude-v1");
}

#[test]
fn a_delta_carrying_both_spellings_does_not_double_the_reasoning() {
    // vLLM's transition releases fill `reasoning` and `reasoning_content`
    // with the same text, and send `"reasoning":""` on frames with no thinking.
    let mut aggregate = StreamingAggregate::default();
    for delta in [
        json!({"reasoning": "", "content": ""}),
        json!({"reasoning": "one ", "reasoning_content": "one "}),
        json!({"reasoning": "two", "reasoning_content": "two"}),
    ] {
        let chunk: ChatStreamChunk =
            serde_json::from_value(json!({"choices": [{"delta": delta}]})).unwrap();
        aggregate.apply_chunk(&chunk);
    }
    assert_eq!(aggregate.reasoning(), "one two");
    let message = aggregate.into_response(true).choices.remove(0).message;
    assert_eq!(
        message.reasoning,
        Some(PlainReasoning::Reasoning("one two".to_string()))
    );
}

#[test]
fn a_stored_message_reads_back_with_its_reasoning_key() {
    // A host that persists the conversation as sent must get the same key
    // back when it reloads it.
    for (key, expected) in [
        ("reasoning", PlainReasoning::Reasoning("why".to_string())),
        (
            "reasoning_content",
            PlainReasoning::ReasoningContent("why".to_string()),
        ),
    ] {
        let message: ChatMessage =
            serde_json::from_value(json!({"role": "assistant", "content": "a", key: "why"}))
                .unwrap();
        assert_eq!(message.reasoning, Some(expected));
        assert_eq!(serde_json::to_value(&message).unwrap()[key], "why");
    }
    let plain: ChatMessage =
        serde_json::from_value(json!({"role": "assistant", "content": "a", "reasoning": null}))
            .unwrap();
    assert_eq!(plain.reasoning, None);
    assert!(serde_json::to_value(&plain)
        .unwrap()
        .get("reasoning")
        .is_none());
}

#[test]
fn reasoning_details_round_trip_through_conversation() {
    let details = json!([
        {"type": "reasoning.text", "id": "rd-0", "index": 0, "text": "first"},
        {"type": "reasoning.encrypted", "id": "rd-1", "index": 1, "data": "xxx"}
    ]);
    let event = ReplayEvent {
        content: None,

        tool_uses: Some(vec![ToolUseInfo {
            id: "call-1".to_string(),
            name: "read".to_string(),
            input: json!({"paths": ["file:lib.rs"]}),
        }]),
        tool_use_id: None,
        tool_result: None,

        raw: Some(json!({"reasoning_details": details.clone()})),
    };
    let message = transcript_event_to_chat_message("assistant", event)
        .expect("assistant message reconstructed");
    assert_eq!(message.reasoning_details, Some(details.clone()));
    let serialized = serde_json::to_value(&message).unwrap();
    assert_eq!(serialized["reasoning_details"], details);
    // Original order is preserved through store and reconstruct.
    assert_eq!(serialized["reasoning_details"][0]["id"], "rd-0");
    assert_eq!(serialized["reasoning_details"][1]["id"], "rd-1");

    // A message built without any reasoning omits the key entirely.
    let plain = serde_json::to_value(ChatMessage::user("hi".to_string())).unwrap();
    assert!(plain.get("reasoning_details").is_none());
}

#[test]
fn tool_call_without_reasoning_details_omits_key_on_resume() {
    // Ordinary (non-reasoning) tool-call turn: store_assistant_tool_call writes
    // `"reasoningDetails": null`, and a missing key behaves the same. Neither
    // should reconstruct a `reasoning_details: null` chat message.
    for raw in [
        json!({"backend": "openrouter", "reasoningDetails": serde_json::Value::Null}),
        json!({"backend": "openrouter"}),
        json!({"backend": "openrouter", "reasoningDetails": []}),
    ] {
        let event = ReplayEvent {
            content: None,

            tool_uses: Some(vec![ToolUseInfo {
                id: "call-1".to_string(),
                name: "read".to_string(),
                input: json!({"paths": ["file:lib.rs"]}),
            }]),
            tool_use_id: None,
            tool_result: None,

            raw: Some(raw),
        };
        let message = transcript_event_to_chat_message("assistant", event)
            .expect("assistant message reconstructed");
        assert_eq!(message.reasoning_details, None);
        let serialized = serde_json::to_value(&message).unwrap();
        assert!(serialized.get("reasoning_details").is_none());
    }
}

fn blank_event() -> ReplayEvent {
    ReplayEvent {
        content: None,

        tool_uses: None,
        tool_use_id: None,
        tool_result: None,

        raw: None,
    }
}

#[test]
fn blank_stored_content_is_never_replayed_as_an_empty_block() {
    // The CAIRN-3263 shape, on the assembler Cairn owns: a turn whose text came
    // back empty must not replay as `{"type":"text","text":""}`, which the
    // provider rejects on every later turn.
    for blank in ["", "   ", "\n"] {
        let mut user = blank_event();
        user.content = Some(blank.to_string());
        assert!(transcript_event_to_chat_message("user", user).is_none());

        let mut assistant = blank_event();
        assistant.content = Some(blank.to_string());
        assert!(transcript_event_to_chat_message("assistant", assistant).is_none());

        // With tool calls, the turn still replays — as its calls alone.
        let mut with_calls = blank_event();
        with_calls.content = Some(blank.to_string());
        with_calls.tool_uses = Some(vec![ToolUseInfo {
            id: "call-1".to_string(),
            name: "read".to_string(),
            input: json!({}),
        }]);
        let message = transcript_event_to_chat_message("assistant", with_calls)
            .expect("a tool-call turn still replays");
        assert!(message.content.is_none());
        assert_eq!(message.tool_calls.as_ref().unwrap().len(), 1);
    }
}

#[test]
fn an_empty_stored_tool_result_is_stated_rather_than_sent_blank() {
    // Dropping it would orphan its call, so the emptiness is spelled out.
    let mut event = blank_event();
    event.tool_use_id = Some("call-1".to_string());
    event.tool_result = Some(String::new());
    let message =
        transcript_event_to_chat_message("tool_result", event).expect("tool results always replay");
    assert_eq!(message.tool_call_id.as_deref(), Some("call-1"));
    match message.content.expect("tool message carries content") {
        ChatContent::Text(text) => assert!(!text.trim().is_empty()),
        other => panic!("unexpected tool content: {other:?}"),
    }
}

#[test]
fn reasoning_details_merge_by_index_across_chunks() {
    // A thinking block streams across chunks: text fragments first, signature
    // last. They must coalesce into ONE block (merged by index), not three
    // fragmented entries, or Anthropic rejects the replayed signature.
    let mut aggregate = StreamingAggregate::default();
    let chunk1: ChatStreamChunk = serde_json::from_value(json!({
        "choices": [{"delta": {"reasoning_details": [{
            "type": "reasoning.text",
            "id": "rd-0",
            "format": "anthropic-claude-v1",
            "index": 0,
            "text": "Let me "
        }]}}]
    }))
    .unwrap();
    let chunk2: ChatStreamChunk = serde_json::from_value(json!({
        "choices": [{"delta": {"reasoning_details": [{
            "index": 0,
            "text": "think.",
            "signature": "sig-final"
        }]}}]
    }))
    .unwrap();
    aggregate.apply_chunk(&chunk1);
    aggregate.apply_chunk(&chunk2);
    let details = aggregate.reasoning_detail_values();
    assert_eq!(
        details.len(),
        1,
        "deltas for one index merge into one block"
    );
    assert_eq!(details[0]["text"], "Let me think.");
    assert_eq!(details[0]["signature"], "sig-final");
    assert_eq!(details[0]["type"], "reasoning.text");
    assert_eq!(details[0]["id"], "rd-0");
    assert_eq!(details[0]["format"], "anthropic-claude-v1");
    assert_eq!(details[0]["index"], 0);
}

#[test]
fn streaming_aggregate_accumulates_split_tool_call_deltas() {
    // The tool name/id arrive in the first delta and the JSON arguments stream
    // in fragments across later deltas. They must coalesce (merged by index)
    // into one complete `tool_calls` entry in the response, or `run_http_turn`
    // sees `tool_calls.is_empty()`, skips execution, and the read never fires.
    let mut aggregate = StreamingAggregate::default();
    let chunk1: ChatStreamChunk = serde_json::from_value(json!({
        "choices": [{"delta": {
            "reasoning": "thinking",
            "tool_calls": [{
                "index": 0,
                "id": "call-1",
                "type": "function",
                "function": {"name": "read", "arguments": "{\"paths\":[\"fi"}
            }]
        }}]
    }))
    .unwrap();
    let chunk2: ChatStreamChunk = serde_json::from_value(json!({
        "choices": [{"delta": {
            "tool_calls": [{
                "index": 0,
                "function": {"arguments": "le:src/lib.rs\"]}"}
            }]
        }}]
    }))
    .unwrap();
    aggregate.apply_chunk(&chunk1);
    aggregate.apply_chunk(&chunk2);

    let response = aggregate.into_response(true);
    let calls = response.choices[0]
        .message
        .tool_calls
        .as_ref()
        .expect("tool_calls accumulated across chunks");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, "call-1");
    assert_eq!(calls[0].function.name, "read");
    // Argument fragments concatenate into valid JSON the dispatch path parses.
    let input: serde_json::Value = serde_json::from_str(&calls[0].function.arguments).unwrap();
    assert_eq!(input["paths"][0], "file:src/lib.rs");
}

#[test]
fn mid_stream_error_chunk_is_detected() {
    let chunk: ChatStreamChunk = serde_json::from_value(json!({
        "error": {"code": 502, "message": "upstream failed"},
        "choices": [{"delta": {}, "finish_reason": "error"}]
    }))
    .unwrap();
    assert_eq!(chunk.error_message().as_deref(), Some("upstream failed"));
}

fn assistant_call(id: &str, name: &str) -> ChatMessage {
    ChatMessage {
        role: "assistant".to_string(),
        content: None,
        tool_call_id: None,
        tool_calls: Some(vec![ToolCall {
            id: id.to_string(),
            r#type: default_function_type(),
            function: ToolFunction {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }]),
        reasoning_details: None,
        reasoning: None,
    }
}

#[test]
fn fit_conversation_borrows_when_window_unknown_or_under_budget() {
    let messages = vec![
        ChatMessage::system("system".to_string()),
        ChatMessage::user("hello".to_string()),
    ];
    // Unknown window: pass the conversation through untouched.
    assert!(matches!(
        fit_conversation(&messages, None),
        std::borrow::Cow::Borrowed(_)
    ));
    // Known window with a tiny conversation: already under budget, untouched.
    assert!(matches!(
        fit_conversation(&messages, Some(200_000)),
        std::borrow::Cow::Borrowed(_)
    ));
}

#[test]
fn trims_oldest_tool_outputs_first_and_protects_system_and_recent() {
    // 800 lines / 4000 chars (~1000 tokens) per tool output.
    let big = "line\n".repeat(800);
    let mut messages = vec![
        ChatMessage::system("system prompt".to_string()),
        ChatMessage::user("do the work".to_string()),
    ];
    // Six (assistant tool_call, tool result) pairs, read/run alternating, so
    // the conversation is 14 messages. PROTECT_RECENT_MESSAGES = 8 protects
    // indices 6..14, leaving only the tool results at index 3 and 5 eligible.
    for (id, name) in [
        ("a", "read"),
        ("b", "run"),
        ("c", "read"),
        ("d", "run"),
        ("e", "read"),
        ("f", "run"),
    ] {
        messages.push(assistant_call(id, name));
        messages.push(ChatMessage::tool(id.to_string(), big.clone()));
    }

    let budget = 4500;
    assert!(estimate_conversation_tokens(&messages) > budget);
    let trimmed = trim_conversation_to_budget(&messages, budget);

    // System and user turns are never collapsed.
    assert_eq!(
        trimmed[0].content.as_ref().and_then(ChatContent::as_text),
        Some("system prompt")
    );
    assert_eq!(
        trimmed[1].content.as_ref().and_then(ChatContent::as_text),
        Some("do the work")
    );
    // The oldest eligible tool results collapse to named, line-counted markers.
    assert_eq!(
        trimmed[3].content.as_ref().and_then(ChatContent::as_text),
        Some("[read output elided — 800 lines]")
    );
    assert_eq!(
        trimmed[5].content.as_ref().and_then(ChatContent::as_text),
        Some("[run output elided — 800 lines]")
    );
    // Assistant tool-call decisions stay intact (only `tool` messages collapse).
    assert!(trimmed[2].tool_calls.is_some());
    assert!(trimmed[2].content.is_none());
    // The most recent exchanges' tool outputs are protected in full.
    assert_eq!(
        trimmed[7].content.as_ref().unwrap().estimated_chars(),
        big.len()
    );
    assert_eq!(
        trimmed[13].content.as_ref().unwrap().estimated_chars(),
        big.len()
    );
    // The trimmed outgoing request now fits under budget.
    assert!(estimate_conversation_tokens(&trimmed) <= budget);
}

#[test]
fn echoed_plain_reasoning_counts_toward_the_budget_and_survives_trimming() {
    // Plain reasoning goes back to the endpoint, so it costs context like any
    // other request content. Here it alone tips the request over budget.
    let mut messages = vec![
        ChatMessage::system("system".to_string()),
        ChatMessage::user("do the work".to_string()),
        assistant_call("a", "read"),
        ChatMessage::tool("a".to_string(), "line\n".repeat(800)),
    ];
    // Four small pairs fill the PROTECT_RECENT_MESSAGES = 8 tail, leaving
    // the big tool result at index 3 as the only eligible one.
    for id in ["b", "c", "d", "e"] {
        messages.push(assistant_call(id, "run"));
        messages.push(ChatMessage::tool(id.to_string(), "ok".to_string()));
    }
    let budget = 1500;
    assert!(estimate_conversation_tokens(&messages) <= budget);

    let thinking = PlainReasoning::ReasoningContent("t".repeat(4000));
    messages[10].reasoning = Some(thinking.clone());
    assert!(estimate_conversation_tokens(&messages) > budget);

    let trimmed = trim_conversation_to_budget(&messages, budget);
    assert_eq!(
        trimmed[3].content.as_ref().and_then(ChatContent::as_text),
        Some("[read output elided — 800 lines]")
    );
    assert_eq!(trimmed[10].reasoning, Some(thinking));
    assert!(estimate_conversation_tokens(&trimmed) <= budget);
}

#[test]
fn fit_conversation_returns_owned_trim_when_over_budget() {
    let big = "x".repeat(8000);
    let mut messages = vec![
        ChatMessage::system("s".to_string()),
        ChatMessage::user("u".to_string()),
    ];
    // Ten pairs (22 messages) so enough tool outputs are eligible to trim the
    // request back under budget rather than bottoming out on protected ones.
    for (id, name) in [
        ("a", "read"),
        ("b", "run"),
        ("c", "read"),
        ("d", "run"),
        ("e", "read"),
        ("f", "run"),
        ("g", "read"),
        ("h", "run"),
        ("i", "read"),
        ("j", "run"),
    ] {
        messages.push(assistant_call(id, name));
        messages.push(ChatMessage::tool(id.to_string(), big.clone()));
    }
    let window = 20_000;
    let fitted = fit_conversation(&messages, Some(window));
    assert!(matches!(fitted, std::borrow::Cow::Owned(_)));
    assert!(estimate_conversation_tokens(&fitted) <= context_fit_budget(window));
}

#[test]
fn streaming_aggregate_captures_length_finish_reason() {
    // The output-token cutoff arrives as finish_reason "length" on the final
    // chunk; into_response must carry it so the turn loop can refuse a
    // partial side-effecting call.
    let mut aggregate = StreamingAggregate::default();
    let chunk: ChatStreamChunk = serde_json::from_value(json!({
        "choices": [{"delta": {"content": "partial"}, "finish_reason": "length"}]
    }))
    .unwrap();
    aggregate.apply_chunk(&chunk);
    let response = aggregate.into_response(false);
    assert_eq!(response.finish_reason.as_deref(), Some("length"));
}

#[test]
fn tool_schemas_include_core_verbs() {
    let names: Vec<_> = tool_schemas(&crate::cairn_tool_definitions())
        .into_iter()
        .filter_map(|schema| {
            schema
                .pointer("/function/name")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .collect();
    assert_eq!(names, vec!["read", "write", "run"]);
}
