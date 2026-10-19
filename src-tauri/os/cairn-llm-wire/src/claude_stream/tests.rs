//! Claude stream-json contract tests. The tool-input payloads are real
//! rejections recovered from `__unparsedToolInput` markers; the event lines are
//! the shapes the CLI emits.

use std::io::Cursor;

use serde_json::Value;

use super::tool_input::classify_input_defect;
use super::*;
use crate::codex_app_server::build_codex_input;
use crate::replay::{MessageContent, MessageImage};

// Every payload below is a real rejected tool input, recovered from the
// `__unparsedToolInput` markers the runtime handed back on this machine.

fn defect_of(raw: &str) -> InputDefect {
    classify_input_defect(raw, false)
}

#[test]
fn regex_backslash_in_a_query_reads_as_an_invalid_escape() {
    // `\.` is a regex escape and not a JSON one, so the payload dies on a
    // grep pattern that reached the wire singly escaped.
    let raw = r#"{"paths": ["cairn://p/cairn/2802/1/coordinator/chat?grep=\.cairn/logs|jsonl"]}"#;
    assert_eq!(defect_of(raw), InputDefect::InvalidEscape);
}

#[test]
fn a_rust_style_unicode_escape_is_not_a_json_one() {
    // JSON wants four bare hex digits; Rust's braced form is a defect.
    let raw = r#"{"content": "assert!(s.starts_with(\u{2717}))"}"#;
    assert_eq!(defect_of(raw), InputDefect::UnicodeEscape);
}

#[test]
fn a_heredoc_newline_survives_into_the_string_unescaped() {
    let raw = "{\"commands\": [{\"command\": \"cat > m.rs <<EOF\nuse std::io;\"}]}";
    assert_eq!(defect_of(raw), InputDefect::ControlCharacter);
}

#[test]
fn a_duplicated_object_tail_reads_as_trailing_garbage() {
    // The generation stutters and re-emits a trailing top-level key after
    // the object has already closed.
    let raw = r#"{"commands": [{"command": "true"}], "branch": "main"}, "branch": "main"}"#;
    assert_eq!(defect_of(raw), InputDefect::TrailingGarbage);
}

#[test]
fn a_whole_payload_whose_string_never_closes_is_named_as_such() {
    let raw = r#"{"paths": ["file:src/lib.rs?grep=Command.*git|\"git\"]}"#;
    assert_eq!(defect_of(raw), InputDefect::UnterminatedString);
}

#[test]
fn a_clipped_prefix_is_not_blamed_for_ending_early() {
    // The runtime retains only a prefix. Ending mid-string says nothing
    // about the payload, so the verdict points past the capture instead of
    // inventing an unterminated string.
    let raw = r#"{"paths": ["file:src-tauri/os/cairn-core/src/backends/cla"#;
    assert_eq!(classify_input_defect(raw, true), InputDefect::BeyondCapture);
}

#[test]
fn a_defect_before_the_clip_still_counts() {
    // A concrete escaping fault inside the retained prefix is decidable
    // even though the rest was dropped.
    let raw = r#"{"paths": ["file:src/lib.rs?grep=a\|b"#;
    assert_eq!(classify_input_defect(raw, true), InputDefect::InvalidEscape);
}

#[test]
fn syntactically_sound_json_is_reported_as_a_non_syntax_rejection() {
    let raw = r#"{"paths": ["file:src/lib.rs"]}"#;
    assert_eq!(defect_of(raw), InputDefect::NotSyntax);
}

#[test]
fn an_unescaped_quote_desyncs_the_object_around_it() {
    // The string closes early, so what follows is read as structure.
    let raw = r#"{"content": "const N: &str = "node_modules";", "msg": "x"}"#;
    assert_eq!(defect_of(raw), InputDefect::Structure);
}

#[test]
fn detect_reports_the_full_length_and_what_was_inspected() {
    let input = serde_json::json!({
        "__unparsedToolInput": {
            "len": 26559,
            "raw": r#"{"changes": [{"target": "file:a.rs?grep=\.x"}]}"#,
        }
    });
    let rejected = RejectedToolInput::detect(&input).expect("marker recognized");
    assert_eq!(rejected.len, 26559);
    assert_eq!(rejected.captured, 47);
    assert_eq!(rejected.defect, InputDefect::InvalidEscape);
}

#[test]
fn a_rejected_call_survives_parsing_carrying_its_marker() {
    // The runtime still emits the assistant message; only the input is
    // replaced. The block has to survive parsing with the marker intact,
    // because that is the whole record of a call that never ran.
    let line = r#"{"type":"assistant","uuid":"u1","session_id":"s1","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_01","name":"mcp__cairn__read","input":{"__unparsedToolInput":{"len":87,"raw":"{}"}}}]}}"#;
    let (event, _raw) = parse_event(line).expect("assistant event parses");
    let ClaudeEvent::Assistant { message, .. } = event else {
        panic!("Expected Assistant event");
    };
    let uses = extract_assistant_content(&message.content)
        .tool_uses
        .expect("tool use surfaced");
    assert_eq!(uses[0].name, "mcp__cairn__read");
    let rejected = RejectedToolInput::detect(&uses[0].input).expect("marker survives parsing");
    assert_eq!(rejected.len, 87);
    // Only two bytes were retained of eighty-seven, so no verdict is
    // available from the prefix alone.
    assert_eq!(rejected.defect, InputDefect::BeyondCapture);
}

#[test]
fn an_ordinary_tool_input_carries_no_marker() {
    let input = serde_json::json!({"paths": ["file:src/lib.rs"]});
    assert!(RejectedToolInput::detect(&input).is_none());
}

#[test]
fn test_parse_system_init_event() {
    let json = r#"{"type":"system","subtype":"init","session_id":"abc123","tools":[]}"#;
    let (event, _raw) = parse_event(json).unwrap();

    match event {
        ClaudeEvent::System {
            subtype,
            session_id,
            ..
        } => {
            assert_eq!(subtype, "init");
            assert_eq!(session_id, "abc123");
        }
        _ => panic!("Expected System event"),
    }
}

#[test]
fn test_parse_user_text_event() {
    let json = r#"{"type":"user","uuid":"u123","session_id":"s456","message":{"role":"user","content":"Hello Claude"}}"#;
    let (event, _raw) = parse_event(json).unwrap();

    match event {
        ClaudeEvent::User {
            uuid,
            session_id,
            message,
            ..
        } => {
            assert_eq!(uuid, "u123");
            assert_eq!(session_id, "s456");
            assert_eq!(message.role, "user");
            match message.content {
                MessageContentInner::Text(text) => assert_eq!(text, "Hello Claude"),
                _ => panic!("Expected text content"),
            }
        }
        _ => panic!("Expected User event"),
    }
}

#[test]
fn test_parse_assistant_with_tool_use() {
    let json = r#"{"type":"assistant","uuid":"a1","session_id":"s1","message":{"role":"assistant","content":[{"type":"text","text":"Let me help"},{"type":"tool_use","id":"tool1","name":"write_plan","input":{"title":"My Plan"}}]}}"#;
    let (event, _raw) = parse_event(json).unwrap();

    match event {
        ClaudeEvent::Assistant { message, .. } => match message.content {
            MessageContentInner::Blocks(blocks) => {
                assert_eq!(blocks.len(), 2);
                match &blocks[0] {
                    ContentBlock::Text { text } => assert_eq!(text, "Let me help"),
                    _ => panic!("Expected Text block"),
                }
                match &blocks[1] {
                    ContentBlock::ToolUse { id, name, input } => {
                        assert_eq!(id, "tool1");
                        assert_eq!(name, "write_plan");
                        assert_eq!(input["title"], "My Plan");
                    }
                    _ => panic!("Expected ToolUse block"),
                }
            }
            _ => panic!("Expected Blocks content"),
        },
        _ => panic!("Expected Assistant event"),
    }
}

#[test]
fn test_parse_result_event() {
    let json = r#"{"type":"result","subtype":"success","session_id":"s1","is_error":false,"duration_ms":1234,"num_turns":5,"total_cost_usd":0.05,"usage":{"input_tokens":100,"output_tokens":200}}"#;
    let (event, _raw) = parse_event(json).unwrap();

    match event {
        ClaudeEvent::Result {
            subtype,
            is_error,
            duration_ms,
            usage,
            ..
        } => {
            assert_eq!(subtype, "success");
            assert!(!is_error);
            assert_eq!(duration_ms, Some(1234));
            let usage = usage.unwrap();
            assert_eq!(usage.input_tokens, 100);
            assert_eq!(usage.output_tokens, 200);
        }
        _ => panic!("Expected Result event"),
    }
}

#[test]
fn test_parse_control_response_with_nested_request_id() {
    let json =
        r#"{"type":"control_response","response":{"subtype":"success","request_id":"req-1234"}}"#;
    let (event, _raw) = parse_event(json).unwrap();

    match event {
        ClaudeEvent::ControlResponse {
            request_id,
            response: ControlResponseInner::Success { .. },
        } => {
            assert_eq!(request_id, "req-1234");
        }
        _ => panic!("Expected ControlResponse success event"),
    }
}

#[test]
fn test_parse_invalid_json() {
    let result = parse_event("not valid json");
    assert!(result.is_err());
}

#[test]
fn test_parse_claude_tool_use_stream_start_and_input_delta() {
    let start_json = r#"{"type":"stream_event","session_id":"s1","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"Bash","input":{},"caller":{"type":"direct"}}}}"#;
    let (event, _) = parse_event(start_json).unwrap();
    match event {
        ClaudeEvent::StreamEvent {
            inner:
                StreamEventInner::ContentBlockStart {
                    index,
                    content_block: Some(content_block),
                },
            ..
        } => {
            assert_eq!(index, 1);
            assert_eq!(
                extract_tool_start(&content_block),
                Some((Some("toolu_1".to_string()), "Bash".to_string()))
            );
        }
        _ => panic!("Expected tool-use ContentBlockStart"),
    }

    let delta_json = r#"{"type":"stream_event","session_id":"s1","event":{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"command\": \"echo tool\"}"}}}"#;
    let (event, _) = parse_event(delta_json).unwrap();
    match event {
        ClaudeEvent::StreamEvent {
            inner:
                StreamEventInner::ContentBlockDelta {
                    index,
                    delta: DeltaContent::InputJsonDelta { partial_json },
                },
            ..
        } => {
            assert_eq!(index, 1);
            assert_eq!(partial_json, r#"{"command": "echo tool"}"#);
        }
        _ => panic!("Expected input_json_delta"),
    }
}

#[test]
fn a_text_block_start_is_not_a_tool_start() {
    let block = serde_json::json!({"type": "text", "text": ""});
    assert_eq!(extract_tool_start(&block), None);
}

#[test]
fn test_parse_rate_limit_event() {
    let json = r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","resetsAt":1717000000,"overageResetsAt":1717100000}}"#;
    let (event, _raw) = parse_event(json).unwrap();

    match event {
        ClaudeEvent::RateLimitEvent { rate_limit_info } => {
            assert_eq!(rate_limit_info.status, "allowed");
            assert_eq!(
                rate_limit_info.rate_limit_type.as_deref(),
                Some("five_hour")
            );
            assert_eq!(rate_limit_info.resets_at, Some(1717000000));
            assert_eq!(rate_limit_info.overage_resets_at, Some(1717100000));
            assert!(!rate_limit_info.is_blocking());
        }
        _ => panic!("Expected RateLimitEvent"),
    }
}

#[test]
fn test_parse_rate_limit_event_blocking_status() {
    let json = r#"{"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}"#;
    let (event, _raw) = parse_event(json).unwrap();
    match event {
        ClaudeEvent::RateLimitEvent { rate_limit_info } => {
            assert!(rate_limit_info.is_blocking());
            // Absent camelCase fields tolerate gracefully.
            assert_eq!(rate_limit_info.resets_at, None);
        }
        _ => panic!("Expected RateLimitEvent"),
    }
}

#[test]
fn test_parse_unknown_event_type_is_ok() {
    // A future/unmodeled event type must parse cleanly to Unknown, not error.
    let json = r#"{"type":"some_future_event","foo":42}"#;
    let (event, _raw) = parse_event(json).expect("unknown type should parse, not error");
    assert!(matches!(event, ClaudeEvent::Unknown));
    assert_eq!(event.kind(), "unknown");
}

#[test]
fn user_text_reads_as_a_user_message() {
    let content = MessageContentInner::Text("Explore the codebase".to_string());
    let user = extract_user_content(&content);
    assert_eq!(user.kind, UserContentKind::User);
    assert_eq!(user.kind.as_str(), "user");
    assert_eq!(user.text.as_deref(), Some("Explore the codebase"));
    assert_eq!(user.tool_use_id, None);
    assert!(!user.is_error);
}

#[test]
fn a_tool_result_block_makes_the_user_message_a_tool_result() {
    let json = r#"{"type":"user","uuid":"u1","session_id":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"file contents here","is_error":false}]}}"#;
    let (ClaudeEvent::User { message, .. }, _) = parse_event(json).unwrap() else {
        panic!("Expected User event");
    };
    let user = extract_user_content(&message.content);

    assert_eq!(user.kind, UserContentKind::ToolResult);
    assert_eq!(user.kind.as_str(), "tool_result");
    assert_eq!(user.tool_use_id.as_deref(), Some("t1"));
    assert_eq!(user.tool_result.as_deref(), Some("file contents here"));
    assert_eq!(user.text, None);
    assert!(!user.is_error);
}

#[test]
fn an_array_tool_result_joins_its_text_parts_and_carries_the_error_flag() {
    let content = MessageContentInner::Blocks(vec![ContentBlock::ToolResult {
        tool_use_id: "t2".to_string(),
        content: serde_json::json!([{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]),
        is_error: Some(true),
    }]);
    let user = extract_user_content(&content);
    assert_eq!(user.tool_result.as_deref(), Some("a\nb"));
    assert!(user.is_error);
}

#[test]
fn assistant_content_separates_text_tools_and_thinking() {
    let json = r#"{"type":"assistant","uuid":"a1","session_id":"s1","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Let me analyze this problem..."},{"type":"text","text":"Analyzing..."},{"type":"tool_use","id":"t1","name":"read_file","input":{"path":"a.rs"}},{"type":"tool_use","id":"t2","name":"read_file","input":{"path":"b.rs"}}]}}"#;
    let (ClaudeEvent::Assistant { message, .. }, _) = parse_event(json).unwrap() else {
        panic!("Expected Assistant event");
    };
    let assistant = extract_assistant_content(&message.content);

    assert_eq!(assistant.text.as_deref(), Some("Analyzing..."));
    assert_eq!(
        assistant.thinking.as_deref(),
        Some("Let me analyze this problem...")
    );
    let uses = assistant.tool_uses.expect("tool uses");
    assert_eq!(uses.len(), 2);
    assert_eq!(uses[1].id, "t2");
    assert_eq!(uses[1].input["path"], "b.rs");
}

#[test]
fn an_assistant_message_without_blocks_has_no_tools() {
    let assistant = extract_assistant_content(&MessageContentInner::Blocks(Vec::new()));
    assert_eq!(assistant.text, None);
    assert!(assistant.tool_uses.is_none());
    assert_eq!(assistant.thinking, None);
}

// Reasoning arrives in one of two dispositions, distinguished ONLY by whether
// the block's text is empty: visible intent prose, or private reasoning whose
// text is withheld and whose volume is reported by `thinking_tokens`. Both are
// ordinary `thinking` blocks on the wire.

fn thinking_of(blocks: Vec<ContentBlock>) -> Option<String> {
    extract_assistant_content(&MessageContentInner::Blocks(blocks)).thinking
}

fn reasoning(text: &str) -> ContentBlock {
    ContentBlock::Thinking {
        thinking: text.to_string(),
    }
}

#[test]
fn a_private_reasoning_block_yields_no_prose() {
    // Not `Some("")`: that is not `None`, so it would defeat the downstream
    // `is_none()` backfill guards and persist an empty-prose orphan row.
    assert_eq!(thinking_of(vec![reasoning("")]), None);
}

#[test]
fn a_private_block_before_a_visible_one_leaves_no_separator() {
    assert_eq!(
        thinking_of(vec![reasoning(""), reasoning("Reading both files first.")]).as_deref(),
        Some("Reading both files first."),
        "the private block must not contribute a leading newline"
    );
}

#[test]
fn a_visible_reasoning_block_keeps_its_prose_beside_tool_calls() {
    let assistant = extract_assistant_content(&MessageContentInner::Blocks(vec![
        reasoning(""),
        reasoning("Checking the config."),
        ContentBlock::ToolUse {
            id: "t1".to_string(),
            name: "read_file".to_string(),
            input: serde_json::json!({"path": "a.rs"}),
        },
    ]));
    assert_eq!(assistant.thinking.as_deref(), Some("Checking the config."));
    assert_eq!(assistant.tool_uses.map(|uses| uses.len()), Some(1));
}

#[test]
fn a_signature_delta_is_discarded_rather_than_modeled() {
    // The standing guarantee that the opaque private-reasoning payload never
    // enters Cairn: there is no variant to carry it, so it parses as Unknown.
    let delta: DeltaContent = serde_json::from_value(serde_json::json!({
        "type": "signature_delta",
        "signature": "<redacted>",
    }))
    .expect("a signature delta must parse, not error");
    assert!(matches!(delta, DeltaContent::Unknown));

    // Nor does a consolidated thinking block retain its signature.
    let block: ContentBlock = serde_json::from_value(serde_json::json!({
        "type": "thinking",
        "thinking": "",
        "signature": "<redacted>",
    }))
    .expect("a private thinking block must parse");
    assert!(!serde_json::to_string(&block).unwrap().contains("redacted"));
}

#[test]
fn parse_thinking_tokens_estimate_reads_absolute_estimate() {
    let data = serde_json::json!({
        "estimated_tokens": 150,
        "estimated_tokens_delta": 100,
    });
    assert_eq!(parse_thinking_tokens_estimate(&data), Some(150));
}

#[test]
fn message_start_usage_reads_prefill_and_cache_tokens() {
    let message = serde_json::json!({
        "id": "msg-1",
        "usage": {
            "input_tokens": 17,
            "output_tokens": 0,
            "cache_read_input_tokens": 23,
            "cache_creation_input_tokens": 5
        }
    });

    let usage = message_start_usage(Some(&message)).expect("message_start usage");
    assert_eq!(usage.input_tokens, 17);
    assert_eq!(usage.cache_read_input_tokens, Some(23));
    assert_eq!(usage.cache_creation_input_tokens, Some(5));
}

#[test]
fn a_failed_result_reporting_success_is_still_terminal_and_names_its_429() {
    let (event, _) = parse_event(
        r#"{"type":"result","subtype":"success","session_id":"session-1","is_error":true,"api_error_status":429,"terminal_reason":"api_error","result":"You've hit your session limit"}"#,
    )
    .unwrap();
    assert!(event.is_terminal());
    let ClaudeEvent::Result { is_error, data, .. } = event else {
        panic!("fixture must parse as a terminal result");
    };
    assert!(is_error);
    assert!(result_is_rate_limited(&data));
}

#[test]
fn a_terminal_api_error_without_429_is_not_a_rate_limit() {
    let data = serde_json::json!({
        "api_error_status": 500,
        "terminal_reason": "api_error"
    });
    assert!(!result_is_rate_limited(&data));
    assert!(!result_is_rate_limited(&serde_json::json!({})));
}

#[test]
fn only_a_result_is_terminal() {
    for line in [
        r#"{"type":"system","subtype":"init","session_id":"s1"}"#,
        r#"{"type":"stream_event","session_id":"s1","event":{"type":"message_stop"}}"#,
        r#"{"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}"#,
    ] {
        assert!(!parse_event(line).unwrap().0.is_terminal(), "{line}");
    }
}

#[test]
fn the_cli_resume_miss_is_recognized_on_stderr() {
    // Verbatim from the runner log that opened CAIRN-3104.
    assert!(stderr_reports_unknown_session(
        "No conversation found with session ID: 5c7fe52a-b0bd-4f3b-8bde-d7af9a2b2b93"
    ));
    // Merely naming a session is not this failure.
    assert!(!stderr_reports_unknown_session(
        "Resuming session ID: 5c7fe52a-b0bd-4f3b-8bde-d7af9a2b2b93"
    ));
}

#[test]
fn the_cli_session_id_collision_is_recognized_on_stderr() {
    // Verbatim from the runner log that opened cairn/4978.
    assert!(stderr_reports_session_id_in_use(
        "Error: Session ID 30fb0390-c508-4345-b87a-fd0f2f10f1b5 is already in use."
    ));
    // A resume miss and an ordinary mention are different facts.
    assert!(!stderr_reports_session_id_in_use(
        "No conversation found with session ID: 30fb0390-c508-4345-b87a-fd0f2f10f1b5"
    ));
    assert!(!stderr_reports_session_id_in_use(
        "Resuming session ID: 30fb0390-c508-4345-b87a-fd0f2f10f1b5"
    ));
}

#[test]
fn stream_evidence_starts_empty() {
    let evidence = StreamEvidence::default();
    assert!(!evidence.saw_terminal_result);
    assert!(!evidence.saw_rate_limit_event);
    assert!(!evidence.saw_blocking_rate_limit);
    assert_eq!(evidence.last_event_kind, "none");
}

#[test]
fn stream_evidence_records_rate_limits_results_and_the_last_kind() {
    let mut evidence = StreamEvidence::default();
    for line in [
        r#"{"type":"system","subtype":"init","session_id":"s1"}"#,
        r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed"}}"#,
    ] {
        evidence.observe(&parse_event(line).unwrap().0);
    }
    assert!(evidence.saw_rate_limit_event);
    assert!(!evidence.saw_blocking_rate_limit);
    assert!(!evidence.saw_terminal_result);
    assert_eq!(evidence.last_event_kind, "rate_limit_event");

    evidence.observe(
        &parse_event(r#"{"type":"rate_limit_event","rate_limit_info":{"status":"rejected"}}"#)
            .unwrap()
            .0,
    );
    assert!(evidence.saw_blocking_rate_limit);

    evidence.observe(
        &parse_event(r#"{"type":"result","subtype":"success","session_id":"s1"}"#)
            .unwrap()
            .0,
    );
    assert!(evidence.saw_terminal_result);
    assert_eq!(evidence.last_event_kind, "result");
}

#[test]
fn a_429_result_is_left_for_the_reader_to_record() {
    let mut evidence = StreamEvidence::default();
    evidence.observe(
        &parse_event(r#"{"type":"result","subtype":"success","session_id":"s1","is_error":true,"api_error_status":429}"#)
            .unwrap()
            .0,
    );
    assert!(evidence.saw_terminal_result);
    assert!(!evidence.saw_blocking_rate_limit);
    evidence.record_terminal_rate_limit();
    assert!(evidence.saw_blocking_rate_limit);
}

fn parse_buffer(buffer: Cursor<Vec<u8>>) -> Value {
    let output = String::from_utf8(buffer.into_inner()).unwrap();
    serde_json::from_str(output.trim()).unwrap()
}

fn png(bytes: &[u8]) -> MessageImage {
    MessageImage {
        mime_type: "image/png".to_string(),
        bytes: bytes.to_vec(),
    }
}

fn assert_no_text_contains(value: &Value, needle: &str) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(text)) = map.get("text") {
                assert!(!text.contains(needle), "encoded image leaked into text");
            }
            map.values()
                .for_each(|v| assert_no_text_contains(v, needle));
        }
        Value::Array(values) => values
            .iter()
            .for_each(|v| assert_no_text_contains(v, needle)),
        _ => {}
    }
}

#[test]
fn an_image_serializes_as_a_native_claude_image_block() {
    let content = MessageContent {
        text: "inspect the attached image".to_string(),
        images: vec![png(b"native-image-payload")],
    };
    let serialized = build_message_content(&content).unwrap();
    assert_eq!(serialized[0]["type"], "text");
    assert_eq!(serialized[1]["type"], "image");
    assert_eq!(serialized[1]["source"]["media_type"], "image/png");
    let encoded = serialized[1]["source"]["data"].as_str().unwrap();
    assert_no_text_contains(&serialized, encoded);
}

#[test]
fn empty_message_is_refused_by_both_serializers_and_never_reaches_stdin() {
    for blank in ["", "   ", "\n\t "] {
        let content = MessageContent::text(blank);
        assert!(build_message_content(&content).is_err());
        assert!(build_codex_input(&content).is_err());

        let mut buffer = Cursor::new(Vec::new());
        assert!(send_user_message(&mut buffer, "session-123", &content, None).is_err());
        assert!(
            buffer.into_inner().is_empty(),
            "a refused message must not be written"
        );
    }
}

#[test]
fn blank_text_beside_an_image_serializes_as_the_image_alone() {
    let content = MessageContent {
        text: "  ".to_string(),
        images: vec![png(b"native-image-payload")],
    };

    let claude = build_message_content(&content).unwrap();
    assert_eq!(claude.as_array().unwrap().len(), 1);
    assert_eq!(claude[0]["type"], "image");

    let codex = build_codex_input(&content).unwrap();
    assert_eq!(codex.as_array().unwrap().len(), 1);
    assert_eq!(codex[0]["type"], "image");
}

#[test]
fn test_send_user_message() {
    let mut buffer = Cursor::new(Vec::new());

    send_user_message(
        &mut buffer,
        "session-123",
        &MessageContent::text("Hello, Claude!"),
        None,
    )
    .unwrap();

    let parsed = parse_buffer(buffer);

    assert_eq!(parsed["type"], "user");
    assert_eq!(parsed["session_id"], "session-123");
    assert_eq!(parsed["message"]["role"], "user");
    assert_eq!(parsed["message"]["content"], "Hello, Claude!");
    assert!(parsed["parent_tool_use_id"].is_null());
}

#[test]
fn test_send_user_message_with_parent_tool_use_id() {
    let mut buffer = Cursor::new(Vec::new());

    send_user_message(
        &mut buffer,
        "session-456",
        &MessageContent::text("Subagent message"),
        Some("toolu_abc123"),
    )
    .unwrap();

    let parsed = parse_buffer(buffer);

    assert_eq!(parsed["type"], "user");
    assert_eq!(parsed["parent_tool_use_id"], "toolu_abc123");
}

#[test]
fn test_send_control_response_allow() {
    let mut buffer = Cursor::new(Vec::new());

    send_control_response(&mut buffer, "req-789", true, Some("Approved by user")).unwrap();

    let parsed = parse_buffer(buffer);

    assert_eq!(parsed["type"], "control_response");
    assert_eq!(parsed["request_id"], "req-789");
    assert_eq!(parsed["response"]["subtype"], "success");
    assert_eq!(parsed["response"]["response"]["behavior"], "allow");
    assert_eq!(
        parsed["response"]["response"]["message"],
        "Approved by user"
    );
}

#[test]
fn test_send_control_response_deny() {
    let mut buffer = Cursor::new(Vec::new());

    send_control_response(&mut buffer, "req-abc", false, None).unwrap();

    let parsed = parse_buffer(buffer);

    assert_eq!(parsed["type"], "control_response");
    assert_eq!(parsed["response"]["response"]["behavior"], "deny");
    assert!(parsed["response"]["response"]["message"].is_null());
}

#[test]
fn test_send_interrupt_request() {
    let mut buffer = Cursor::new(Vec::new());

    send_interrupt_request(&mut buffer, "req-int-1").unwrap();

    let parsed = parse_buffer(buffer);

    assert_eq!(parsed["type"], "control_request");
    assert_eq!(parsed["request_id"], "req-int-1");
    assert_eq!(parsed["request"]["subtype"], "interrupt");
}

#[test]
fn test_send_set_model_request() {
    let mut buffer = Cursor::new(Vec::new());

    send_set_model_request(&mut buffer, "req-model-1", "claude-sonnet-4-20250514").unwrap();

    let parsed = parse_buffer(buffer);

    assert_eq!(parsed["type"], "control_request");
    assert_eq!(parsed["request_id"], "req-model-1");
    assert_eq!(parsed["request"]["subtype"], "set_model");
    assert_eq!(parsed["request"]["model"], "claude-sonnet-4-20250514");
}

#[test]
fn test_send_set_permission_mode_request() {
    let mut buffer = Cursor::new(Vec::new());

    send_set_permission_mode_request(&mut buffer, "req-perm-1", "bypassPermissions").unwrap();

    let parsed = parse_buffer(buffer);

    assert_eq!(parsed["type"], "control_request");
    assert_eq!(parsed["request_id"], "req-perm-1");
    assert_eq!(parsed["request"]["subtype"], "set_permission_mode");
    assert_eq!(parsed["request"]["mode"], "bypassPermissions");
}

#[test]
fn an_environment_push_is_one_top_level_line() {
    let mut out = Vec::new();
    send_update_environment_variables(
        &mut out,
        "cairn-credential-3-x",
        &[("CLAUDE_CODE_OAUTH_TOKEN", "tok")],
    )
    .unwrap();
    let line: serde_json::Value =
        serde_json::from_str(String::from_utf8(out).unwrap().trim()).unwrap();
    assert_eq!(
        line,
        serde_json::json!({
            "type": "update_environment_variables",
            "variables": {"CLAUDE_CODE_OAUTH_TOKEN": "tok"},
            "request_id": "cairn-credential-3-x",
        })
    );
}

#[test]
fn the_cli_s_environment_ack_carries_its_request_id_inside_the_response() {
    // As measured from CLI 2.1.296.
    let (ack, _) = parse_event(
        r#"{"type":"control_response","response":{"subtype":"success","request_id":"env-push-1"}}"#,
    )
    .unwrap();
    assert_eq!(ack.control_request_id(), Some("env-push-1"));
    let (top, _) = parse_event(
        r#"{"type":"control_response","request_id":"req-1","response":{"subtype":"success"}}"#,
    )
    .unwrap();
    assert_eq!(top.control_request_id(), Some("req-1"));
    let (refused, _) = parse_event(
        r#"{"type":"control_response","response":{"subtype":"error","request_id":"env-2","error":"refused"}}"#,
    )
    .unwrap();
    assert!(matches!(
        &refused,
        ClaudeEvent::ControlResponse { response: ControlResponseInner::Error { message: Some(m), .. }, .. } if m == "refused"
    ));
    assert_eq!(refused.control_request_id(), Some("env-2"));
}

#[test]
fn an_api_retry_frame_exposes_its_status() {
    let (retry, _) = parse_event(
        r#"{"type":"system","subtype":"api_retry","attempt":1,"error_status":401,"session_id":"s"}"#,
    )
    .unwrap();
    assert_eq!(retry.api_retry_status(), Some(401));
    let (init, _) = parse_event(r#"{"type":"system","subtype":"init","session_id":"s"}"#).unwrap();
    assert_eq!(init.api_retry_status(), None);
}

const CREDIT_DEPLETION_ASSISTANT: &str = r#"{"type":"assistant","uuid":"a1","session_id":"s1","is_api_error_message":true,"api_error_status":429,"error_details":"credits_required","message":{"role":"assistant","content":[{"type":"text","text":"Manage credits with /usage-credits or https://claude.ai/settings/usage?from=cc_cli_limit_message"}]}}"#;

#[test]
fn structured_assistant_errors_classify_credit_depletion() {
    let (event, _) = parse_event(CREDIT_DEPLETION_ASSISTANT).unwrap();
    assert_eq!(event.provider_depletion(), Some(ProviderDepletion::Credits));
}

#[test]
fn structured_429_without_credit_marker_is_a_rolling_window() {
    let (event, _) = parse_event(r#"{"type":"assistant","uuid":"a1","session_id":"s1","is_api_error_message":true,"api_error_status":429,"message":{"role":"assistant","content":"limit reached"}}"#).unwrap();
    assert_eq!(
        event.provider_depletion(),
        Some(ProviderDepletion::RollingWindow { resets_at: None })
    );
}

#[test]
fn ordinary_assistant_quoting_credit_marker_is_inert() {
    let (event, _) = parse_event(r#"{"type":"assistant","uuid":"a1","session_id":"s1","message":{"role":"assistant","content":"quoted cc_cli_limit_message and /usage-credits"}}"#).unwrap();
    assert_eq!(event.provider_depletion(), None);
}

#[test]
fn assistant_credit_depletion_followed_by_generic_error_result_is_terminal() {
    let (assistant, _) = parse_event(CREDIT_DEPLETION_ASSISTANT).unwrap();
    let pending = assistant.provider_depletion();
    let (result, _) = parse_event(r#"{"type":"result","subtype":"error_during_execution","is_error":true,"duration_ms":1,"duration_api_ms":1,"num_turns":1,"result":"An error occurred","session_id":"s1","total_cost_usd":0}"#).unwrap();
    let ClaudeEvent::Result { is_error, data, .. } = result else {
        panic!("fixture must end with a Result");
    };

    assert_eq!(
        terminal_provider_depletion(is_error, &data, pending),
        Some(ProviderDepletion::Credits)
    );
    assert_eq!(terminal_provider_depletion(false, &data, pending), None);
}
