use super::context::{
    context_fit_budget, estimate_conversation_tokens, fit_conversation, trim_conversation_to_budget,
};
use super::conversation::{
    render_tool_result, reorder_tool_results_by_call_order, transcript_event_to_chat_message,
};
use super::persist::{
    store_assistant_tool_call, store_tool_result, tool_call_usage, AssistantStreamState,
};
use super::tools::{
    build_provider_object, collapse_envelope_text, normalize_tool_payload, prepare_tool_call,
    resolve_home_target, tool_schemas, PreparedCall,
};
use super::wire::{
    default_function_type, ChatMessage, ChatStreamChunk, ChatStreamDelta, OpenRouterUsage,
    StreamingAggregate, ToolCall, ToolFunction,
};
use crate::agent_process::stream::{TokenCounts, ToolUseInfo, TranscriptEvent};
use crate::dispatch::DispatchOutput;
use crate::models::{OpenRouterRouting, OpenRouterSort};
use serde_json::json;

#[test]
fn provider_object_default_only_requires_parameters() {
    let provider = build_provider_object(&OpenRouterRouting::default());
    assert_eq!(provider, json!({ "require_parameters": true }));
}

#[test]
fn provider_object_includes_zdr_when_opted_in() {
    let provider = build_provider_object(&OpenRouterRouting {
        zero_data_retention: true,
        sort: None,
    });
    assert_eq!(provider["zdr"], json!(true));
    assert_eq!(provider["require_parameters"], json!(true));
    assert!(provider.get("sort").is_none());
}

#[test]
fn provider_object_maps_each_sort_variant() {
    for (variant, expected) in [
        (OpenRouterSort::Price, "price"),
        (OpenRouterSort::Throughput, "throughput"),
        (OpenRouterSort::Latency, "latency"),
    ] {
        let provider = build_provider_object(&OpenRouterRouting {
            zero_data_retention: false,
            sort: Some(variant),
        });
        assert_eq!(provider["sort"], json!(expected));
        assert!(provider.get("zdr").is_none());
    }
}

#[test]
fn provider_object_combines_zdr_and_sort() {
    let provider = build_provider_object(&OpenRouterRouting {
        zero_data_retention: true,
        sort: Some(OpenRouterSort::Latency),
    });
    assert_eq!(provider["require_parameters"], json!(true));
    assert_eq!(provider["zdr"], json!(true));
    assert_eq!(provider["sort"], json!("latency"));
}

#[test]
fn tool_schemas_include_core_verbs() {
    let names: Vec<_> = tool_schemas()
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

#[test]
fn collapse_envelope_text_extracts_run_text_and_drops_images() {
    // The OpenAI tool-message format is text-only, so the run/read envelope
    // collapses to its text on the OpenRouter edge; image blocks are dropped
    // (they cannot ride a chat-completions tool result).
    let envelope = cairn_common::read::RunBatchEnvelope {
        text: "=== look ===\nAX tree".to_string(),
        images: vec![cairn_common::read::ImageBlock {
            mime_type: "image/png".to_string(),
            data: "b64".to_string(),
        }],
    };
    let json = serde_json::to_string(&envelope).unwrap();
    assert_eq!(collapse_envelope_text(json), "=== look ===\nAX tree");
}

#[test]
fn collapse_envelope_text_passes_through_non_envelope() {
    let plain = "Exit code: 0".to_string();
    assert_eq!(collapse_envelope_text(plain.clone()), plain);
}

#[test]
fn tool_result_renders_reminders_after_content() {
    let rendered = render_tool_result(DispatchOutput {
        content: "ok".into(),
        reminders: vec!["note".into()],
    });
    assert!(rendered.starts_with("ok"));
    assert!(rendered.contains("<system-reminder>"));
}

#[test]
fn usage_maps_token_counts() {
    let usage = OpenRouterUsage {
        prompt_tokens: Some(10),
        completion_tokens: Some(5),
        total_tokens: Some(15),
        reasoning_tokens: None,
        prompt_tokens_details: Some(json!({"cached_tokens": 3})),
        completion_tokens_details: Some(json!({"reasoning_tokens": 2})),
        cost: None,
        cost_details: None,
    };
    assert_eq!(
        usage.token_counts(),
        TokenCounts {
            input: Some(10),
            output: Some(5),
            cache_read: Some(3),
            cache_create: None,
            thinking: Some(2)
        }
    );
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
    }
}

#[test]
fn reorder_tool_results_follows_assistant_tool_call_order() {
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
    let reordered = reorder_tool_results_by_call_order(messages);
    // Tool results now follow the assistant's tool_calls order: w then r.
    assert_eq!(reordered[2].tool_call_id.as_deref(), Some("w"));
    assert_eq!(reordered[2].content.as_deref(), Some("real answer"));
    assert_eq!(reordered[3].tool_call_id.as_deref(), Some("r"));
    // Non-tool messages keep their position.
    assert_eq!(reordered[0].role, "user");
    assert_eq!(reordered[1].role, "assistant");
}

#[test]
fn reorder_tool_results_is_noop_when_already_ordered() {
    let messages = vec![
        assistant_with_calls(&["a", "b"]),
        ChatMessage::tool("a".to_string(), "ra".to_string()),
        ChatMessage::tool("b".to_string(), "rb".to_string()),
    ];
    let reordered = reorder_tool_results_by_call_order(messages);
    assert_eq!(reordered[1].tool_call_id.as_deref(), Some("a"));
    assert_eq!(reordered[2].tool_call_id.as_deref(), Some("b"));
}

#[test]
fn chat_message_serializes_tool_call_id_for_tool_role() {
    let value = serde_json::to_value(ChatMessage::tool("call-1".into(), "result".into())).unwrap();
    assert_eq!(value["role"], "tool");
    assert_eq!(value["tool_call_id"], "call-1");
}

#[test]
fn normalizes_home_relative_tool_targets() {
    let home = "cairn://p/CAIRN/1883/1/builder";
    assert_eq!(
        resolve_home_target("cairn:~/todos?limit=1", home),
        "cairn://p/CAIRN/1883/1/builder/todos?limit=1"
    );

    let mut read = json!({"paths": ["cairn:~/todos", "file:src/lib.rs"]});
    normalize_tool_payload("read", &mut read, home);
    assert_eq!(read["paths"][0], "cairn://p/CAIRN/1883/1/builder/todos");
    assert_eq!(read["paths"][1], "file:src/lib.rs");

    let mut write = json!({"changes": [{"target": "cairn:~/messages", "mode": "append"}]});
    normalize_tool_payload("write", &mut write, home);
    assert_eq!(
        write["changes"][0]["target"],
        "cairn://p/CAIRN/1883/1/builder/messages"
    );
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
fn reasoning_details_round_trip_through_conversation() {
    let details = json!([
        {"type": "reasoning.text", "id": "rd-0", "index": 0, "text": "first"},
        {"type": "reasoning.encrypted", "id": "rd-1", "index": 1, "data": "xxx"}
    ]);
    let event = TranscriptEvent {
        event_type: "assistant".to_string(),
        session_id: Some("s1".to_string()),
        parent_tool_use_id: None,
        content: None,
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: Some(vec![ToolUseInfo {
            id: "call-1".to_string(),
            name: "read".to_string(),
            input: json!({"paths": ["file:lib.rs"]}),
        }]),
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
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
        let event = TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some("s1".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: Some(vec![ToolUseInfo {
                id: "call-1".to_string(),
                name: "read".to_string(),
                input: json!({"paths": ["file:lib.rs"]}),
            }]),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(raw),
        };
        let message = transcript_event_to_chat_message("assistant", event)
            .expect("assistant message reconstructed");
        assert_eq!(message.reasoning_details, None);
        let serialized = serde_json::to_value(&message).unwrap();
        assert!(serialized.get("reasoning_details").is_none());
    }
}

#[test]
fn system_prompt_carries_uri_shapes_workspace_and_orientation() {
    // The wire system message must be the full assembled prompt, not the agent
    // role content alone — otherwise the model never gets Cairn's path/URI
    // conventions or its own working directory and mis-paths file reads.
    use crate::orchestrator::session::{assemble_prompt_segments, flatten_prompt_segments};
    let cairn = crate::system_prompt::cairn_system_prompt();
    let agent = "<agent_role>\nbuilder body\n\n## Orientation\n\n- Working directory (cwd): `/work/dir/x`\n</agent_role>";
    let dynamic = "\n\n## Orientation\n\n- Working directory (cwd): `/work/dir/x`\n</agent_role>";
    let segments = assemble_prompt_segments(
        &cairn,
        Some("workspace doctrine"),
        Some("project doctrine"),
        Some(agent),
        Some(dynamic),
    );
    let flattened = flatten_prompt_segments(&segments);
    // Cairn system-prompt layer: read/write/run verbs and path schemas.
    assert!(
        flattened.contains("## URI Shapes"),
        "missing Cairn URI guidance"
    );
    // Workspace + project instruction layers.
    assert!(
        flattened.contains("workspace doctrine"),
        "missing workspace instructions"
    );
    assert!(
        flattened.contains("project doctrine"),
        "missing project instructions"
    );
    // Orientation cwd from the dynamic tail.
    assert!(flattened.contains("/work/dir/x"), "missing orientation cwd");
    // Wire prompt equals the persisted concatenation (same segment source).
    let persisted: String = segments.iter().map(|s| s.text.as_str()).collect();
    assert_eq!(flattened, persisted);
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

#[test]
fn tool_call_usage_recorded_only_when_not_streamed() {
    let usage: OpenRouterUsage = serde_json::from_value(json!({
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_tokens": 150,
        "cost": 0.42
    }))
    .unwrap();

    // Non-streamed pure tool-call response: no streamed assistant event
    // carried this generation's usage, so its tokens must be recorded on the
    // tool-call event or they vanish from the breakdown.
    let recorded = tool_call_usage(false, Some(&usage)).expect("usage recorded");
    let counts = recorded.token_counts();
    assert_eq!(counts.input, Some(100));
    assert_eq!(counts.output, Some(50));

    // Streamed: the usage already landed on the finalized streamed assistant
    // event, so recording it again here would double-count.
    assert!(tool_call_usage(true, Some(&usage)).is_none());
}

/// End-to-end storage contract for a streamed reasoning-plus-tool-call turn,
/// reproducing exactly what `run_http_turn` does on the streaming branch:
/// the streamed reasoning lands as one finalized assistant event (sequence
/// S), and the tool call lands as a SEPARATE assistant event carrying
/// `tool_uses` at the next sequence (S+1). Both must survive and be returned
/// by the same `list_events_for_session_delta` query the transcript reads, so
/// the read renders alongside the reasoning — the OpenRouter transcript bug
/// (CAIRN-1909) is reasoning surfacing while the tool call vanishes.
#[tokio::test]
async fn streamed_reasoning_then_tool_call_persists_both_events() {
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;
    use turso::params;

    let db_dir = tempfile::tempdir().unwrap();
    let db = LocalDb::open(db_dir.path().join("or-transcript-test.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();

    let root = tempfile::tempdir().unwrap().keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    let orch = OrchestratorBuilder::new(db_state, services, config_dir).build();

    let now = chrono::Utc::now().timestamp() as i32;
    orch.db
        .local
        .write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs(id, status, session_id, created_at, updated_at)
                         VALUES ('run-1', 'live', 'session-1', ?1, ?2)",
                    params![now, now],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

    // Sequence S: the streamed reasoning event, finalized exactly as the live
    // SSE reader does (open stream, append thinking deltas, finalize).
    let mut state =
        AssistantStreamState::open(&orch, orch.db.local.clone(), "run-1", "session-1", None, 2)
            .unwrap();
    state
        .append_thinking(&orch, "run-1", "Let me read the file.")
        .unwrap();
    state
        .finalize(
            &orch,
            "run-1",
            "session-1",
            String::new(),
            "Let me read the file.".to_string(),
            vec![],
            None,
            None,
            None,
        )
        .unwrap();

    // Sequence S+1: the tool-call assistant event (empty text, tool_uses set).
    let read_call = ToolCall {
        id: "call-read-1".to_string(),
        r#type: default_function_type(),
        function: ToolFunction {
            name: "read".to_string(),
            arguments: r#"{"paths":["file:src/lib.rs"]}"#.to_string(),
        },
    };
    store_assistant_tool_call(
        &orch,
        &orch.db.local,
        "run-1",
        "session-1",
        None,
        3,
        "",
        std::slice::from_ref(&read_call),
        None,
        None,
        None,
        None,
    )
    .unwrap();

    // Sequence S+2: the tool result.
    store_tool_result(
        &orch,
        &orch.db.local,
        "run-1",
        "session-1",
        None,
        4,
        "call-read-1",
        &DispatchOutput {
            content: "file body".to_string(),
            reminders: vec![],
        },
    )
    .unwrap();

    // Load through the exact query the transcript UI consumes.
    let delta = crate::runs::queries::list_events_for_session_delta(
        orch.db.local.clone(),
        "session-1",
        None,
    )
    .unwrap();
    let parsed: Vec<TranscriptEvent> = delta
        .events
        .iter()
        .map(|event| serde_json::from_str(&event.data).unwrap())
        .collect();

    // Reasoning surfaces (the part the user already sees).
    assert!(
        parsed.iter().any(|event| event.thinking.is_some()),
        "streamed reasoning event must persist"
    );
    // The tool call must ALSO surface, carrying tool_uses for the read.
    let tool_call = parsed.iter().find(|event| {
        event
            .tool_uses
            .as_ref()
            .map(|uses| uses.iter().any(|use_| use_.name == "read"))
            .unwrap_or(false)
    });
    let tool_call = tool_call.expect(
            "tool-call event with `tool_uses` for the read must be persisted and returned by the session delta",
        );
    let uses = tool_call.tool_uses.as_ref().unwrap();
    assert_eq!(uses.len(), 1);
    assert_eq!(uses[0].id, "call-read-1");
    assert_eq!(uses[0].input["paths"][0], "file:src/lib.rs");
    // And its result is matchable by tool_use_id for the renderer's pairing.
    assert!(
        parsed
            .iter()
            .any(|event| event.tool_use_id.as_deref() == Some("call-read-1")),
        "tool_result keyed to the read must persist"
    );
}

#[test]
fn usage_parses_inline_cost() {
    let usage: OpenRouterUsage = serde_json::from_value(json!({
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_tokens": 150,
        "reasoning_tokens": 20,
        "cost": 0.95,
        "cost_details": {"upstream_inference_cost": 0.40}
    }))
    .unwrap();
    assert_eq!(usage.cost, Some(0.95));
    assert_eq!(usage.token_counts().thinking, Some(20));
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
    assert_eq!(trimmed[0].content.as_deref(), Some("system prompt"));
    assert_eq!(trimmed[1].content.as_deref(), Some("do the work"));
    // The oldest eligible tool results collapse to named, line-counted markers.
    assert_eq!(
        trimmed[3].content.as_deref(),
        Some("[read output elided — 800 lines]")
    );
    assert_eq!(
        trimmed[5].content.as_deref(),
        Some("[run output elided — 800 lines]")
    );
    // Assistant tool-call decisions stay intact (only `tool` messages collapse).
    assert!(trimmed[2].tool_calls.is_some());
    assert!(trimmed[2].content.is_none());
    // The most recent exchanges' tool outputs are protected in full.
    assert_eq!(trimmed[7].content.as_ref().unwrap().len(), big.len());
    assert_eq!(trimmed[13].content.as_ref().unwrap().len(), big.len());
    // The trimmed outgoing request now fits under budget.
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

fn tool_call(name: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: "call-1".to_string(),
        r#type: default_function_type(),
        function: ToolFunction {
            name: name.to_string(),
            arguments: arguments.to_string(),
        },
    }
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
fn prepare_tool_call_dispatches_valid_call() {
    let call = tool_call("run", r#"{"command": "ls"}"#);
    match prepare_tool_call(&call, false) {
        PreparedCall::Dispatch {
            verb,
            payload,
            repaired_args,
        } => {
            assert_eq!(verb, "run");
            assert_eq!(payload, json!({"command": "ls"}));
            assert!(!repaired_args);
        }
        PreparedCall::Reject(_) => panic!("valid call should dispatch"),
    }
}

#[test]
fn prepare_tool_call_normalizes_name() {
    let call = tool_call("Write_File", r#"{"changes": []}"#);
    match prepare_tool_call(&call, false) {
        PreparedCall::Dispatch { verb, .. } => assert_eq!(verb, "write"),
        PreparedCall::Reject(_) => panic!("alias should normalize"),
    }
}

#[test]
fn prepare_tool_call_rejects_unknown_name_without_crashing() {
    let call = tool_call("frobnicate", "{}");
    match prepare_tool_call(&call, false) {
        PreparedCall::Reject(output) => {
            assert!(output.content.contains("Unsupported OpenRouter tool"));
            assert!(output.content.contains("read, write, run"));
        }
        PreparedCall::Dispatch { .. } => panic!("unknown tool should reject"),
    }
}

#[test]
fn prepare_tool_call_unrecoverable_args_reject_not_crash() {
    // The CAIRN-1784 failure shape: a large write whose JSON was truncated
    // mid-stream after a key/colon (unrecoverable). It must become a
    // tool-result error, never a propagated Err that crashes the run.
    let call = tool_call("write", r#"{"changes": [{"target": "file:x", "mode":"#);
    match prepare_tool_call(&call, false) {
        PreparedCall::Reject(output) => {
            assert!(output.content.contains("were not valid JSON"));
            assert!(output.content.contains("split it into smaller calls"));
        }
        PreparedCall::Dispatch { .. } => panic!("unrecoverable args should reject"),
    }
}

#[test]
fn prepare_tool_call_rejects_truncation_balanced_write() {
    // Truncation mid-string in a write balances into valid JSON with
    // shortened content. Dispatching it would write a partial file, so it
    // must be refused and surfaced as a retry instead.
    let call = tool_call(
        "write",
        r#"{"changes": [{"target": "file:x", "content": "partial body"#,
    );
    match prepare_tool_call(&call, false) {
        PreparedCall::Reject(output) => {
            assert!(output.content.contains("appear truncated"));
            assert!(output
                .content
                .contains("split it into several smaller calls"));
        }
        PreparedCall::Dispatch { .. } => panic!("truncated write must not dispatch"),
    }
}

#[test]
fn prepare_tool_call_rejects_truncation_balanced_run() {
    // A truncated shell command must not run partially (a half-formed `rm`).
    let call = tool_call("run", r#"{"command": "echo hello"#);
    assert!(matches!(
        prepare_tool_call(&call, false),
        PreparedCall::Reject(_)
    ));
}

#[test]
fn prepare_tool_call_allows_truncation_balanced_read() {
    // read is non-destructive: a recovered partial is acceptable rather than
    // forcing a retry round-trip.
    let call = tool_call("read", r#"{"paths": ["file:a", "file:b"#);
    match prepare_tool_call(&call, false) {
        PreparedCall::Dispatch {
            verb,
            repaired_args,
            ..
        } => {
            assert_eq!(verb, "read");
            assert!(repaired_args);
        }
        PreparedCall::Reject(_) => panic!("truncated read may dispatch"),
    }
}

#[test]
fn prepare_tool_call_rejects_length_capped_write_even_when_valid() {
    // finish_reason "length" => the generation was cut off. Even though this
    // write's JSON parses cleanly, the cutoff may have dropped later changes,
    // so a side-effecting call is refused.
    let call = tool_call(
        "write",
        r#"{"changes": [{"target": "file:x", "mode": "delete"}]}"#,
    );
    assert!(matches!(
        prepare_tool_call(&call, true),
        PreparedCall::Reject(_)
    ));
    // The same call dispatches when the generation finished normally.
    assert!(matches!(
        prepare_tool_call(&call, false),
        PreparedCall::Dispatch { .. }
    ));
}

#[test]
fn prepare_tool_call_allows_length_capped_read() {
    // A length cutoff on a read is harmless: dispatch the (possibly partial)
    // read rather than forcing a retry.
    let call = tool_call("read", r#"{"paths": ["file:a"]}"#);
    assert!(matches!(
        prepare_tool_call(&call, true),
        PreparedCall::Dispatch { .. }
    ));
}

#[test]
fn prepare_tool_call_unrecoverable_truncated_gives_split_guidance() {
    // Unrepairable JSON that was ALSO cut off at the output-token cap
    // (suspected_truncated) gets the truncation-specific message that tells
    // the model to split the call, not the generic invalid-JSON message.
    let call = tool_call("write", r#"{"changes": [{"target": "file:x", "mode":"#);
    match prepare_tool_call(&call, true) {
        PreparedCall::Reject(output) => {
            assert!(output.content.contains("appear truncated"));
            assert!(output
                .content
                .contains("split it into several smaller calls"));
        }
        PreparedCall::Dispatch { .. } => {
            panic!("unrecoverable + truncated args should reject")
        }
    }
}
