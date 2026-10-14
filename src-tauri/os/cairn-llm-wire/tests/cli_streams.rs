//! Whole recorded streams through the CLI wires, line by line, the way a
//! reader consumes them. The fixtures are assembled from the event shapes the
//! unit tests already pin, so they carry no field the CLIs never emit.

use cairn_llm_wire::claude_stream::{self, ClaudeEvent, StreamEventInner, StreamEvidence, Usage};
use cairn_llm_wire::codex_app_server::{
    context_usage, CodexTurnUsageAccumulator, Notification, ProviderTurnStatus,
};
use serde_json::Value;

const CLAUDE_STREAM: &str = include_str!("fixtures/claude_stream.jsonl");
const CODEX_APP_SERVER: &str = include_str!("fixtures/codex_app_server.jsonl");

#[test]
fn a_recorded_claude_tool_turn_parses_in_order_and_ends_on_its_result() {
    let mut evidence = StreamEvidence::default();
    let mut kinds = Vec::new();
    let mut inference_usage = Vec::new();
    let mut result_usage = None;

    for line in CLAUDE_STREAM.lines() {
        let (event, _raw) = claude_stream::parse_event(line)
            .unwrap_or_else(|error| panic!("fixture line must parse: {error}\n{line}"));
        evidence.observe(&event);
        kinds.push(event.kind());
        match event {
            ClaudeEvent::StreamEvent {
                inner:
                    StreamEventInner::MessageDelta {
                        usage: Some(usage), ..
                    },
                ..
            } => inference_usage.push(serde_json::from_value::<Usage>(usage).unwrap()),
            ClaudeEvent::Result { usage, .. } => result_usage = usage,
            _ => {}
        }
    }

    assert_eq!(
        kinds,
        [
            "system",
            "rate_limit_event",
            "stream_event",
            "stream_event",
            "stream_event",
            "stream_event",
            "stream_event",
            "stream_event",
            "stream_event",
            "assistant",
            "user",
            "stream_event",
            "stream_event",
            "stream_event",
            "stream_event",
            "stream_event",
            "stream_event",
            "assistant",
            "stream_event",
            "stream_event",
            "result",
        ]
    );
    assert_eq!(
        evidence,
        StreamEvidence {
            saw_terminal_result: true,
            saw_rate_limit_event: true,
            saw_blocking_rate_limit: false,
            last_event_kind: "result",
        }
    );

    // Each inference reports its own usage on message_delta; the turn's result
    // reports their sum.
    assert_eq!(inference_usage.len(), 2);
    let summed = inference_usage.iter().fold((0, 0, 0), |sum, usage| {
        (
            sum.0 + usage.input_tokens,
            sum.1 + usage.output_tokens,
            sum.2 + usage.cache_read_input_tokens.unwrap_or(0),
        )
    });
    let result_usage = result_usage.expect("the result carries usage");
    assert_eq!(
        summed,
        (
            result_usage.input_tokens,
            result_usage.output_tokens,
            result_usage.cache_read_input_tokens.unwrap_or(0),
        )
    );
    assert_eq!(summed, (16, 29, 30));
}

#[test]
fn a_recorded_codex_turn_classifies_in_order_and_ends_exactly_once() {
    let messages: Vec<Value> = CODEX_APP_SERVER
        .lines()
        .map(|line| serde_json::from_str(line).expect("fixture line is JSON"))
        .collect();
    let notifications: Vec<Notification<'_>> = messages.iter().map(Notification::parse).collect();

    let variants: Vec<&str> = notifications
        .iter()
        .map(|notification| match notification {
            Notification::ServerRequest { .. } => "ServerRequest",
            Notification::AuthTokensRefreshRequest { .. } => "AuthTokensRefreshRequest",
            Notification::TurnStarted { .. } => "TurnStarted",
            Notification::AgentMessageDelta { .. } => "AgentMessageDelta",
            Notification::ReasoningDelta { .. } => "ReasoningDelta",
            Notification::ItemStarted { .. } => "ItemStarted",
            Notification::ItemCompleted { .. } => "ItemCompleted",
            Notification::RawResponseItemCompleted { .. } => "RawResponseItemCompleted",
            Notification::TurnCompleted { .. } => "TurnCompleted",
            Notification::TurnAborted { .. } => "TurnAborted",
            Notification::Error { .. } => "Error",
            Notification::TokenUsageUpdated { .. } => "TokenUsageUpdated",
            Notification::RateLimitsUpdated { .. } => "RateLimitsUpdated",
            Notification::CommandOutputDelta { .. } => "CommandOutputDelta",
            Notification::Ignored { .. } => "Ignored",
            Notification::Unknown { .. } => "Unknown",
        })
        .collect();
    assert_eq!(
        variants,
        [
            "TurnStarted",
            "ReasoningDelta",
            "ItemStarted",
            "CommandOutputDelta",
            "ItemCompleted",
            "TokenUsageUpdated",
            "AgentMessageDelta",
            "ItemCompleted",
            "Error",
            "TokenUsageUpdated",
            "Ignored",
            "TurnCompleted",
        ]
    );

    // The retryable error is not an ending; only the final notification is.
    let endings: Vec<usize> = notifications
        .iter()
        .enumerate()
        .filter(|(_, notification)| notification.ends_turn())
        .map(|(index, _)| index)
        .collect();
    assert_eq!(endings, [notifications.len() - 1]);
    assert_eq!(
        notifications.last(),
        Some(&Notification::TurnCompleted {
            turn_id: Some("turn-1"),
            status: ProviderTurnStatus::Completed,
        })
    );

    let mut accumulator = CodexTurnUsageAccumulator::default();
    let mut latest_context = None;
    for notification in &notifications {
        match notification {
            Notification::TurnStarted { .. } => accumulator.start_turn(),
            Notification::TokenUsageUpdated {
                params: Some(params),
            } => {
                accumulator.observe(params);
                latest_context = context_usage(params);
            }
            _ => {}
        }
    }
    let turn_usage = accumulator.take_turn_usage().expect("turn usage");
    assert_eq!(turn_usage.input_tokens, 40);
    assert_eq!(turn_usage.cache_read_input_tokens, Some(120));
    assert_eq!(turn_usage.output_tokens, 20);
    assert_eq!(
        turn_usage.output_tokens_details.unwrap().thinking_tokens,
        Some(7)
    );
    // The gauge reads the latest response, not the cumulative total.
    let context = latest_context.expect("context usage");
    assert_eq!(context.used_tokens, 70);
    assert_eq!(context.context_window, Some(258_400));
}
