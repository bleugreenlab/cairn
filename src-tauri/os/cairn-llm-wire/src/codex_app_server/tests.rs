//! Codex app-server contract tests: notification classification, the terminal
//! rule, usage normalization, and the goal and input payloads.

use serde_json::{json, Value};

use super::goal::{parse_response, Goal, GoalResponse};
use super::*;
use crate::replay::{MessageContent, MessageImage};

fn notification(total_tokens: i64, last_tokens: i64) -> Value {
    usage_notification(
        Some((39_995, 9_999, 5, 0)),
        (18_671, 3_456, 5, 0),
        last_tokens,
        total_tokens,
    )
}

fn usage_notification(
    total: Option<(u32, u32, u32, u32)>,
    last: (u32, u32, u32, u32),
    last_total_tokens: i64,
    total_tokens: i64,
) -> Value {
    let mut token_usage = json!({
        "last": {
            "totalTokens": last_total_tokens,
            "inputTokens": last.0,
            "cachedInputTokens": last.1,
            "outputTokens": last.2,
            "reasoningOutputTokens": last.3
        },
        "modelContextWindow": 258_400
    });
    if let Some(total) = total {
        token_usage["total"] = json!({
            "totalTokens": total_tokens,
            "inputTokens": total.0,
            "cachedInputTokens": total.1,
            "outputTokens": total.2,
            "reasoningOutputTokens": total.3
        });
    }
    json!({ "tokenUsage": token_usage })
}

#[test]
fn codex_token_usage_maps_last_for_single_turn() {
    let context = context_usage(&notification(18_676, 18_676)).expect("token usage");

    assert_eq!(context.usage.input_tokens, 15_215);
    assert_eq!(context.usage.cache_read_input_tokens, Some(3_456));
    assert_eq!(context.usage.cache_creation_input_tokens, None);
    assert_eq!(context.usage.output_tokens, 5);
    assert_eq!(context.used_tokens, 18_676);
    assert_eq!(context.context_window, Some(258_400));
    assert_eq!(context.reasoning_tokens, 0);
    assert_eq!(context.last_output_tokens, 5);
}

#[test]
fn codex_token_usage_total_does_not_drive_context_gauge() {
    let context = context_usage(&notification(40_000, 18_676)).expect("token usage");

    assert_eq!(context.used_tokens, 18_676);
    assert_eq!(context.usage.input_tokens, 15_215);
    assert_eq!(context.usage.cache_read_input_tokens, Some(3_456));
    assert_eq!(context.usage.output_tokens, 5);
}

#[test]
fn codex_token_usage_without_last_reports_nothing() {
    assert!(context_usage(&json!({ "tokenUsage": { "total": {} } })).is_none());
}

#[test]
fn codex_usage_accumulates_multi_response_deltas_and_deduplicates_snapshots() {
    let first = usage_notification(Some((100, 80, 10, 4)), (100, 80, 10, 4), 110, 110);
    let second = usage_notification(Some((160, 120, 20, 7)), (60, 40, 10, 3), 70, 180);
    let mut accumulator = CodexTurnUsageAccumulator::default();

    accumulator.observe(&first);
    accumulator.observe(&first);
    let usage = accumulator.observe(&second).expect("accumulated usage");

    assert_eq!(usage.input_tokens, 40);
    assert_eq!(usage.cache_read_input_tokens, Some(120));
    assert_eq!(usage.output_tokens, 20);
    assert_eq!(
        usage.output_tokens_details.unwrap().thinking_tokens,
        Some(7)
    );
}

#[test]
fn codex_usage_preserves_total_baseline_across_turns() {
    let first = usage_notification(Some((100, 80, 10, 4)), (100, 80, 10, 4), 110, 110);
    let next_turn = usage_notification(Some((160, 120, 20, 7)), (60, 40, 10, 3), 70, 180);
    let mut accumulator = CodexTurnUsageAccumulator::default();
    accumulator.observe(&first);
    accumulator.start_turn();
    let usage = accumulator.observe(&next_turn).expect("next turn usage");

    assert_eq!(usage.input_tokens, 20);
    assert_eq!(usage.cache_read_input_tokens, Some(40));
    assert_eq!(usage.output_tokens, 10);
}

#[test]
fn codex_usage_first_snapshot_after_resume_uses_last_response() {
    let resumed = usage_notification(
        Some((10_100, 8_080, 510, 204)),
        (100, 80, 10, 4),
        110,
        10_610,
    );
    let mut accumulator = CodexTurnUsageAccumulator::default();
    let usage = accumulator.observe(&resumed).expect("usage");

    assert_eq!(usage.input_tokens, 20);
    assert_eq!(usage.cache_read_input_tokens, Some(80));
    assert_eq!(usage.output_tokens, 10);
}

#[test]
fn codex_usage_total_reset_falls_back_to_last_response() {
    let before_reset = usage_notification(Some((500, 400, 50, 20)), (100, 80, 10, 4), 110, 550);
    let after_reset = usage_notification(Some((60, 40, 10, 3)), (60, 40, 10, 3), 70, 70);
    let mut accumulator = CodexTurnUsageAccumulator::default();
    accumulator.observe(&before_reset);
    let usage = accumulator.observe(&after_reset).expect("usage");

    assert_eq!(usage.input_tokens, 40);
    assert_eq!(usage.cache_read_input_tokens, Some(120));
    assert_eq!(usage.output_tokens, 20);
}

#[test]
fn codex_usage_without_total_adds_last_response() {
    let notification = usage_notification(None, (100, 80, 10, 4), 110, 0);
    let mut accumulator = CodexTurnUsageAccumulator::default();
    let usage = accumulator.observe(&notification).expect("usage");

    assert_eq!(usage.input_tokens, 20);
    assert_eq!(usage.cache_read_input_tokens, Some(80));
    assert_eq!(usage.output_tokens, 10);
}

#[test]
fn codex_usage_retains_the_latest_response_separately_from_turn_totals() {
    let first = usage_notification(Some((100, 80, 10, 4)), (100, 80, 10, 4), 110, 110);
    let second = usage_notification(Some((160, 120, 30, 11)), (60, 40, 20, 7), 80, 190);
    let mut accumulator = CodexTurnUsageAccumulator::default();
    accumulator.observe(&first);
    let turn_usage = accumulator.observe(&second).expect("turn usage");
    let response_usage = accumulator
        .last_response_usage()
        .expect("latest response usage");

    assert_eq!(turn_usage.output_tokens, 30);
    assert_eq!(
        turn_usage.output_tokens_details.unwrap().thinking_tokens,
        Some(11)
    );
    assert_eq!(response_usage.output_tokens, 20);
    assert_eq!(
        response_usage
            .output_tokens_details
            .unwrap()
            .thinking_tokens,
        Some(7)
    );
}

#[test]
fn selected_model_capacity_classifier_is_narrow_and_punctuation_tolerant() {
    for message in [
        "Selected model is at capacity. Please try a different model.",
        "selected MODEL is at capacity; please try a different model!",
        "Selected model is at capacity -- please try a different model...",
    ] {
        assert!(is_selected_model_capacity_error(message), "{message}");
    }
    for message in [
        "You have exceeded your quota.",
        "Authentication failed.",
        "Context window exceeded.",
        "The service is at capacity.",
        "Selected model failed. Please try a different model.",
    ] {
        assert!(!is_selected_model_capacity_error(message), "{message}");
    }
}

#[test]
fn a_message_with_an_id_and_no_result_is_a_server_request() {
    let msg = json!({"id": 7, "method": "item/commandExecution/requestApproval", "params": {}});
    assert_eq!(
        Notification::parse(&msg),
        Notification::ServerRequest {
            id: &json!(7),
            method: "item/commandExecution/requestApproval"
        }
    );
    // A response to our own request carries the id beside a result, and a
    // method-less message is never a request.
    let response = json!({"id": 7, "method": "turn/start", "result": {}});
    assert!(!matches!(
        Notification::parse(&response),
        Notification::ServerRequest { .. }
    ));
    assert_eq!(
        Notification::parse(&json!({"id": 7})),
        Notification::Unknown { method: None }
    );
}

#[test]
fn the_token_refresh_request_is_named_apart_from_other_requests() {
    let request =
        json!({"id": "req-9", "method": "account/chatgptAuthTokens/refresh", "params": {}});
    let parsed = Notification::parse(&request);
    assert_eq!(
        parsed,
        Notification::AuthTokensRefreshRequest {
            id: &json!("req-9")
        }
    );
    assert!(!parsed.ends_turn());

    // The same method answering a request of ours is not a request.
    let answered =
        json!({"id": "req-9", "method": "account/chatgptAuthTokens/refresh", "result": {}});
    let parsed = Notification::parse(&answered);
    assert!(!matches!(
        parsed,
        Notification::AuthTokensRefreshRequest { .. } | Notification::ServerRequest { .. }
    ));
    assert!(!parsed.ends_turn());
}

#[test]
fn turn_started_reads_only_the_nested_turn_id() {
    let msg = json!({"method": "turn/started", "params": {"turn": {"id": "turn-1"}}});
    assert_eq!(
        Notification::parse(&msg),
        Notification::TurnStarted {
            turn_id: Some("turn-1")
        }
    );
    let flat = json!({"method": "turn/started", "params": {"turnId": "turn-1"}});
    assert_eq!(
        Notification::parse(&flat),
        Notification::TurnStarted { turn_id: None }
    );
}

#[test]
fn deltas_read_whichever_key_the_server_used() {
    let agent = json!({"method": "item/agentMessage/delta", "params": {"delta": "hi"}});
    assert_eq!(
        Notification::parse(&agent),
        Notification::AgentMessageDelta { delta: Some("hi") }
    );
    for method in [
        "item/reasoning/textDelta",
        "item/reasoning/summaryTextDelta",
    ] {
        let msg = json!({"method": method, "params": {"textDelta": "think"}});
        assert_eq!(
            Notification::parse(&msg),
            Notification::ReasoningDelta {
                delta: Some("think")
            }
        );
    }
    let empty = json!({"method": "item/agentMessage/delta", "params": {}});
    assert_eq!(
        Notification::parse(&empty),
        Notification::AgentMessageDelta { delta: None }
    );
}

#[test]
fn items_are_borrowed_and_an_absent_item_reads_as_null() {
    let item = json!({"id": "cmd-1", "type": "commandExecution"});
    let started = json!({"method": "item/started", "params": {"item": item}});
    assert_eq!(
        Notification::parse(&started),
        Notification::ItemStarted { item: &item }
    );
    let completed = json!({"method": "item/completed", "params": {}});
    assert_eq!(
        Notification::parse(&completed),
        Notification::ItemCompleted {
            turn_id: None,
            item: &Value::Null
        }
    );
    let message = json!({"id": "msg-1", "type": "agentMessage", "text": "Done."});
    let completed = json!({"method": "item/completed", "params": {"threadId": "thread-1", "turnId": "turn-9", "item": message}});
    assert_eq!(
        Notification::parse(&completed),
        Notification::ItemCompleted {
            turn_id: Some("turn-9"),
            item: &message
        }
    );
}

#[test]
fn the_nested_turn_id_wins_over_the_flat_one() {
    let both = json!({"params": {"turn": {"id": "nested"}, "turnId": "flat"}});
    assert_eq!(notification_turn_id(&both), Some("nested"));
    let flat = json!({"params": {"turnId": "flat"}});
    assert_eq!(notification_turn_id(&flat), Some("flat"));
    assert_eq!(notification_turn_id(&json!({"params": {}})), None);
}

#[test]
fn turn_completed_status_defaults_to_completed_and_keeps_unknown_names() {
    let bare = json!({"method": "turn/completed", "params": {"turn": {"id": "t"}}});
    assert_eq!(
        Notification::parse(&bare),
        Notification::TurnCompleted {
            turn_id: Some("t"),
            status: ProviderTurnStatus::Completed
        }
    );
    for (wire, status) in [
        ("completed", ProviderTurnStatus::Completed),
        ("interrupted", ProviderTurnStatus::Interrupted),
        ("failed", ProviderTurnStatus::Other("failed")),
        (
            "new_provider_failure",
            ProviderTurnStatus::Other("new_provider_failure"),
        ),
    ] {
        let msg =
            json!({"method": "turn/completed", "params": {"turn": {"id": "t", "status": wire}}});
        let Notification::TurnCompleted { status: parsed, .. } = Notification::parse(&msg) else {
            panic!("turn/completed must classify as TurnCompleted");
        };
        assert_eq!(parsed, status);
        assert_eq!(parsed.as_str(), wire);
    }
}

#[test]
fn an_abort_on_request_is_an_interruption_and_anything_else_an_error() {
    for reason in ["interrupted", "replaced", "review_ended"] {
        assert_eq!(
            ProviderTurnStatus::from_abort_reason(reason),
            ProviderTurnStatus::Interrupted
        );
    }
    for reason in ["unknown", "crashed", ""] {
        let status = ProviderTurnStatus::from_abort_reason(reason);
        assert_eq!(status, ProviderTurnStatus::Other("error"));
        assert_eq!(status.as_str(), "error");
    }
}

#[test]
fn an_abort_reason_falls_back_to_the_message_then_unknown() {
    let with_message =
        json!({"method": "turn/aborted", "params": {"turnId": "t", "message": "replaced"}});
    assert_eq!(
        Notification::parse(&with_message),
        Notification::TurnAborted {
            turn_id: Some("t"),
            reason: "replaced"
        }
    );
    let bare = json!({"method": "turn/aborted", "params": {}});
    assert_eq!(
        Notification::parse(&bare),
        Notification::TurnAborted {
            turn_id: None,
            reason: "unknown"
        }
    );
}

#[test]
fn errors_carry_their_message_and_retry_flag() {
    let msg = json!({"method": "error", "params": {"turnId": "t", "error": {"message": "boom"}, "willRetry": true}});
    assert_eq!(
        Notification::parse(&msg),
        Notification::Error {
            turn_id: Some("t"),
            message: Some("boom"),
            will_retry: true
        }
    );
    let bare = json!({"method": "error", "params": {}});
    assert_eq!(
        Notification::parse(&bare),
        Notification::Error {
            turn_id: None,
            message: None,
            will_retry: false
        }
    );
}

#[test]
fn usage_rate_limit_and_output_deltas_expose_their_payloads() {
    let usage = json!({"method": "thread/tokenUsage/updated", "params": {"tokenUsage": {}}});
    assert_eq!(
        Notification::parse(&usage),
        Notification::TokenUsageUpdated {
            params: Some(&json!({"tokenUsage": {}}))
        }
    );
    let limits = json!({"method": "account/rateLimits/updated", "params": {}});
    assert_eq!(
        Notification::parse(&limits),
        Notification::RateLimitsUpdated {
            rate_limits: &Value::Null
        }
    );
    let output = json!({"method": "item/commandExecution/outputDelta", "params": {"itemId": "cmd-1", "delta": "ok\n"}});
    assert_eq!(
        Notification::parse(&output),
        Notification::CommandOutputDelta {
            item_id: "cmd-1",
            delta: "ok\n"
        }
    );
    let bare_output = json!({"method": "item/commandExecution/outputDelta"});
    assert_eq!(
        Notification::parse(&bare_output),
        Notification::CommandOutputDelta {
            item_id: "",
            delta: ""
        }
    );
}

#[test]
fn known_noise_is_ignored_and_new_methods_are_unknown() {
    for method in [
        "item/fileChange/outputDelta",
        "serverRequest/resolved",
        "turn/diff/updated",
        "item/plan/delta",
        "item/reasoning/summaryPartAdded",
        "thread/status/changed",
    ] {
        let msg = json!({"method": method, "params": {}});
        assert_eq!(Notification::parse(&msg), Notification::Ignored { method });
    }
    let future = json!({"method": "thread/goal/updated", "params": {}});
    assert_eq!(
        Notification::parse(&future),
        Notification::Unknown {
            method: Some("thread/goal/updated")
        }
    );
}

#[test]
fn only_completion_abort_and_a_final_error_end_the_turn() {
    let ends = [
        json!({"method": "turn/completed", "params": {}}),
        json!({"method": "turn/aborted", "params": {}}),
        json!({"method": "error", "params": {"willRetry": false}}),
        json!({"method": "error", "params": {}}),
    ];
    for msg in &ends {
        assert!(Notification::parse(msg).ends_turn(), "{msg}");
    }
    let continues = [
        json!({"method": "error", "params": {"willRetry": true}}),
        json!({"method": "turn/started", "params": {}}),
        json!({"method": "item/completed", "params": {}}),
        json!({"method": "thread/tokenUsage/updated", "params": {}}),
        json!({"id": 1, "method": "turn/completed"}),
    ];
    for msg in &continues {
        assert!(!Notification::parse(msg).ends_turn(), "{msg}");
    }
}

#[test]
fn command_items_summarize_output_and_failure() {
    assert_eq!(
        extract_command_execution(&json!(["git", "status"])),
        (
            "git status".to_string(),
            vec!["git".to_string(), "status".to_string()]
        )
    );
    assert_eq!(
        summarize_command_result(&json!({"aggregatedOutput": "clean", "exitCode": 0})),
        ("clean".to_string(), false)
    );
    assert_eq!(
        summarize_command_result(&json!({"exitCode": 2})),
        ("Exit code 2".to_string(), true)
    );
    assert_eq!(
        summarize_file_change_result(&json!({"status": "declined"})),
        ("File changes declined".to_string(), true)
    );
}

#[test]
fn mcp_results_keep_raw_only_for_non_text_content() {
    let text_only = Some(json!({"Ok": {"content": [{"type": "text", "text": "done"}]}}));
    assert_eq!(
        extract_mcp_result(&text_only),
        ("done".to_string(), false, None)
    );
    let failure = Some(json!({"Err": "nope"}));
    let (text, is_error, raw) = extract_mcp_result(&failure);
    assert_eq!(text, "Error: nope");
    assert!(is_error);
    assert!(raw.is_some());
}

#[test]
fn mcp_media_reads_as_a_marker_and_never_its_payload() {
    let data = "iVBORw0KGgo".repeat(200);
    let result = Some(json!({"content": [
        {"type": "text", "text": "rendered"},
        {"type": "image", "mimeType": "image/png", "data": data},
        {"type": "audio", "data": data},
        {"type": "resource_link", "uri": "file:///a.txt", "name": "a"},
    ], "structured_content": null}));
    let (text, is_error, raw) = extract_mcp_result(&result);
    assert_eq!(
        text,
        "rendered\n[image: image/png]\n[audio]\n[resource: file:///a.txt]"
    );
    assert!(!is_error);
    assert_eq!(raw, result, "the raw result is kept beside the text");

    let image_only = Some(json!({"content": [
        {"type": "image", "mimeType": "image/webp", "data": data},
    ]}));
    assert_eq!(extract_mcp_result(&image_only).0, "[image: image/webp]");
}

#[test]
fn mcp_tool_call_items_read_their_result_or_their_error() {
    let completed = json!({"type": "mcpToolCall", "status": "completed",
        "result": {"content": [{"type": "text", "text": "hi"}], "structured_content": null},
        "error": null});
    let (text, is_error, _) = summarize_mcp_tool_call(&completed);
    assert_eq!((text.as_str(), is_error), ("hi", false));

    let failed = json!({"type": "mcpToolCall", "status": "failed", "result": null,
        "error": {"message": "tool timed out"}});
    assert_eq!(
        summarize_mcp_tool_call(&failed),
        ("Error: tool timed out".to_string(), true, None)
    );

    let failed_bare = json!({"type": "mcpToolCall", "status": "failed"});
    assert_eq!(
        summarize_mcp_tool_call(&failed_bare),
        ("Error".to_string(), true, None)
    );

    let tool_error = json!({"type": "mcpToolCall", "status": "completed",
        "result": {"content": [{"type": "text", "text": "bad input"}], "is_error": true}});
    let (text, is_error, _) = summarize_mcp_tool_call(&tool_error);
    assert_eq!((text.as_str(), is_error), ("bad input", true));
}

#[test]
fn parses_all_goal_statuses_and_nullable_budget() {
    for status in [
        "active",
        "paused",
        "blocked",
        "usageLimited",
        "budgetLimited",
        "complete",
    ] {
        let goal: Goal = serde_json::from_value(json!({
            "threadId": "thread-1", "objective": "finish work", "status": status,
            "tokenBudget": null, "tokensUsed": 2, "timeUsedSeconds": 3,
            "createdAt": 4, "updatedAt": 5
        }))
        .unwrap();
        assert_eq!(goal.token_budget, None);
    }
}

#[test]
fn rejects_malformed_goal_response() {
    let error = parse_response::<GoalResponse>(
        "thread/goal/get",
        json!({
            "goal": { "threadId": "thread-1" }
        }),
    )
    .unwrap_err();
    assert!(error.contains("Invalid Codex thread/goal/get response"));
}

#[test]
fn an_image_serializes_as_a_native_codex_image_without_base64_text() {
    let content = MessageContent {
        text: "inspect the attached image".to_string(),
        images: vec![MessageImage {
            mime_type: "image/png".to_string(),
            bytes: b"native-image-payload".to_vec(),
        }],
    };
    let serialized = build_codex_input(&content).unwrap();
    assert_eq!(serialized[0]["type"], "text");
    assert_eq!(serialized[1]["type"], "image");
    let data_url = serialized[1]["url"].as_str().unwrap();
    assert!(data_url.starts_with("data:image/png;base64,"));
    let encoded = data_url.split_once(',').unwrap().1;
    assert!(!serialized[0]["text"].as_str().unwrap().contains(encoded));
}

/// Items recorded from codex-cli 0.153.3 (cairn/4905's probe), with the
/// megabytes of base64 cut to a stand-in.
mod image_generation {
    use super::*;

    #[test]
    fn a_completed_image_item_reads_its_image_prompt_and_path() {
        let item = json!({
            "type": "imageGeneration",
            "id": "exec-431922e8-9f5a-4a8d-917f-a218953354da",
            "status": "completed",
            "revisedPrompt": "A small watercolor of a red fox sitting in snow, tall portrait composition (2:3).",
            "result": "iVBORw0KGgo=",
            "transparentBackground": false,
            "failure": null,
            "savedPath": "/scratch/codex-imgworker-087ljppn/generated_images/01a137e1-04f3-71f1-9a9d-f69164bb8bdb/exec-431922e8-9f5a-4a8d-917f-a218953354da.png"
        });
        let image = ImageGenerationItem::parse(&item).unwrap();
        assert!(image.is_completed());
        assert_eq!(image.id, "exec-431922e8-9f5a-4a8d-917f-a218953354da");
        assert_eq!(image.result, "iVBORw0KGgo=");
        assert!(image
            .revised_prompt
            .unwrap()
            .starts_with("A small watercolor"));
        assert!(image
            .saved_path
            .unwrap()
            .ends_with("exec-431922e8-9f5a-4a8d-917f-a218953354da.png"));
        assert_eq!(image.failure, None);
    }

    #[test]
    fn a_moderation_block_is_a_failed_item_with_no_failure_and_no_image() {
        let item = json!({
            "type": "imageGeneration",
            "id": "exec-3a563379-5f77-4bfd-8042-05bab2231dab",
            "status": "failed",
            "revisedPrompt": "A photorealistic photo of Taylor Swift and Joe Biden shaking hands at a press event.",
            "result": "",
            "transparentBackground": null,
            "failure": null
        });
        let image = ImageGenerationItem::parse(&item).unwrap();
        assert!(!image.is_completed());
        assert!(image.result.is_empty());
        assert_eq!(image.saved_path, None);
        assert_eq!(image.failure, None);
    }

    /// The quota shape is Codex's own (`ext/items/src/image_generation.rs`):
    /// tagged by `type`.
    #[test]
    fn a_quota_hit_carries_its_limit_and_reset() {
        let item = json!({
            "type": "imageGeneration",
            "id": "ig_123",
            "status": "failed",
            "revisedPrompt": null,
            "result": "",
            "failure": {"type": "usageLimitExceeded", "limitId": "image_gen", "resetsAt": 1786150800}
        });
        assert_eq!(
            ImageGenerationItem::parse(&item).unwrap().failure,
            Some(ImageGenerationFailure::UsageLimitExceeded {
                limit_id: "image_gen".to_string(),
                resets_at: Some(1_786_150_800),
            })
        );
        let mut unknown = item.clone();
        unknown["failure"] = json!({"type": "somethingNew", "detail": 1});
        assert_eq!(
            ImageGenerationItem::parse(&unknown).unwrap().failure,
            Some(ImageGenerationFailure::Other)
        );
    }

    #[test]
    fn another_item_kind_is_not_an_image() {
        let message = json!({"type": "agentMessage", "id": "m", "text": "DONE"});
        assert_eq!(ImageGenerationItem::parse(&message), None);
        assert_eq!(IMAGE_GENERATION_ITEM, "imageGeneration");
    }
}
