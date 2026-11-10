use super::persist::{
    store_assistant_tool_call, store_tool_result, tool_call_usage, TranscriptSink,
};
use super::tools::{
    collapse_envelope_text, model_callback_request, prepare_tool_call, render_tool_result,
    PreparedCall,
};
use super::{HttpUsageExt, TurnToolCall, TurnUsage};
use crate::agent_process::stream::{TokenCounts, TranscriptEvent};
use crate::dispatch::DispatchOutput;
use crate::storage::LocalDb;
use serde_json::json;
use std::sync::Arc;

fn tool_call(name: &str, arguments: &str) -> TurnToolCall {
    TurnToolCall {
        id: "call-1".to_string(),
        name: name.to_string(),
        arguments: arguments.to_string(),
    }
}

#[test]
fn home_relative_tool_targets_pass_through_to_live_host_resolution() {
    for (name, arguments) in [
        ("read", r#"{"paths":["cairn:~/todos","file:src/lib.rs"]}"#),
        (
            "write",
            r#"{"changes":[{"target":"cairn:~/tasks","mode":"append","payload":{}}]}"#,
        ),
    ] {
        let PreparedCall::Dispatch { payload, .. } =
            prepare_tool_call(&tool_call(name, arguments), false)
        else {
            panic!("valid {name} call must dispatch");
        };
        assert_eq!(
            payload,
            serde_json::from_str::<serde_json::Value>(arguments).unwrap(),
            "the HTTP backend must not freeze a home-relative target"
        );
    }
}

#[test]
fn collapse_envelope_text_extracts_run_text_and_drops_images() {
    // The OpenAI tool-message format is text-only, so the run/read envelope
    // collapses to its text on the HTTP-loop edge; image blocks are dropped
    // (they cannot ride a chat-completions tool result).
    let envelope = cairn_common::read::RunBatchEnvelope {
        text: "=== look ===\nAX tree".to_string(),
        images: vec![cairn_common::read::ImageBlock::inline("image/png", "b64")],
        invalid_input: false,
        data: None,
        over_ceiling: None,
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
    let usage = TurnUsage {
        prompt_tokens: Some(10),
        completion_tokens: Some(5),
        total_tokens: Some(15),
        reasoning_tokens: None,
        prompt_tokens_details: Some(json!({"cached_tokens": 3, "cache_write_tokens": 4})),
        completion_tokens_details: Some(json!({"reasoning_tokens": 2})),
        cost: None,
        cost_details: None,
    };
    assert_eq!(
        usage.token_counts(),
        TokenCounts {
            input: Some(3),
            output: Some(5),
            cache_read: Some(3),
            cache_create: Some(4),
            thinking: Some(2)
        }
    );
}

#[test]
fn system_prompt_carries_uri_shapes_workspace_and_orientation() {
    // The wire system message must be the full assembled prompt, not the agent
    // role content alone — otherwise the model never gets Cairn's path/URI
    // conventions or its own working directory and mis-paths file reads.
    use crate::orchestrator::session::{assemble_prompt_segments, flatten_prompt_segments};
    let cairn = crate::system_prompt::cairn_system_prompt(false);
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
fn tool_call_usage_recorded_only_when_not_streamed() {
    let usage: TurnUsage = serde_json::from_value(json!({
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

#[test]
fn usage_parses_inline_cost() {
    let usage: TurnUsage = serde_json::from_value(json!({
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

/// The loop hands every result back to the model, so every verb it dispatches
/// is stamped model-facing; a missing stamp would leave the run's queued
/// steers undelivered until the turn ends.
#[test]
fn every_verb_the_loop_dispatches_is_stamped_model_facing() {
    let call = tool_call("read", "{}");
    for (verb, payload, tool) in [
        ("read", json!({"paths": ["file:a"]}), "read_batch"),
        ("read", json!({"uri": "file:a"}), "read"),
        ("write", json!({"changes": []}), "write"),
        ("run", json!({"commands": [{"command": "true"}]}), "run"),
    ] {
        let request = model_callback_request("/work", "run-1", &call, verb, payload);
        assert!(request.is_model_facing(), "{verb}: {:?}", request.payload);
        assert_eq!(request.tool, tool, "{verb}");
        assert_eq!(request.run_id.as_deref(), Some("run-1"));
        assert_eq!(request.cwd, "/work");
        assert_eq!(request.tool_use_id.as_deref(), Some("call-1"));
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

/// End-to-end storage contract for a streamed reasoning-plus-tool-call turn,
/// reproducing exactly what `run_http_turn` does on the streaming branch:
/// the streamed reasoning lands as one finalized assistant event (sequence
/// S), and the tool call lands as a SEPARATE assistant event carrying
/// `tool_uses` at the next sequence (S+1). Both must survive and be returned
/// by the session's transcript load (`list_events_for_session`), so
/// the read renders alongside the reasoning — the transcript bug (CAIRN-1909)
/// is reasoning surfacing while the tool call vanishes. The neutral persist
/// layer takes the adapter's backend key explicitly (here `"openrouter"`).
#[tokio::test]
async fn streamed_reasoning_then_tool_call_persists_both_events() {
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use cairn_db::turso::params;

    let db_dir = tempfile::tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(db_dir.path().join("or-transcript-test.db"))
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
    use cairn_models::wire::stream::{StreamSink, StreamSummary};
    let mut sink = TranscriptSink::new(
        &orch,
        orch.db.local.clone(),
        "run-1",
        "session-1",
        None,
        "openrouter",
    );
    sink.append_thinking("Let me read the file.").unwrap();
    sink.finalize(StreamSummary {
        text: String::new(),
        thinking: "Let me read the file.".to_string(),
        reasoning_details: vec![json!({"type":"reasoning.encrypted","data":"opaque-signature"})],
        usage: Some(
            serde_json::from_value(json!({"prompt_tokens": 20, "completion_tokens": 3})).unwrap(),
        ),
        generation_id: Some("generation-1".into()),
        model: Some("model-1".into()),
    })
    .unwrap();

    // Sequence S+1: the tool-call assistant event (empty text, tool_uses set).
    let read_call = TurnToolCall {
        id: "call-read-1".to_string(),
        name: "read".to_string(),
        arguments: r#"{"paths":["file:src/lib.rs"]}"#.to_string(),
    };
    store_assistant_tool_call(
        &orch,
        &orch.db.local,
        "run-1",
        "session-1",
        None,
        "",
        std::slice::from_ref(&read_call),
        None,
        None,
        None,
        None,
        "openrouter",
    )
    .unwrap();

    // Sequence S+2: the tool result.
    store_tool_result(
        &orch,
        &orch.db.local,
        "run-1",
        "session-1",
        None,
        "call-read-1",
        &DispatchOutput {
            content: "file body".to_string(),
            reminders: vec![],
        },
        "openrouter",
    )
    .unwrap();

    // Load the session's stored transcript in transcript order.
    let events =
        crate::runs::queries::list_events_for_session(orch.db.local.clone(), "session-1").unwrap();
    let parsed: Vec<TranscriptEvent> = events
        .iter()
        .map(|event| serde_json::from_str(&event.data).unwrap())
        .collect();

    // Reasoning surfaces (the part the user already sees).
    assert!(
        parsed.iter().any(|event| event.thinking.is_some()),
        "streamed reasoning event must persist"
    );
    let reasoning = parsed
        .iter()
        .find(|event| event.thinking.is_some())
        .unwrap();
    let raw = reasoning.raw.as_ref().unwrap();
    assert_eq!(raw["backend"], "openrouter");
    assert_eq!(raw["generationId"], "generation-1");
    assert_eq!(raw["model"], "model-1");
    assert_eq!(raw["usage"]["prompt_tokens"], 20);
    let replay: cairn_models::wire::ReplayEvent = reasoning.clone().into();
    assert_eq!(
        cairn_models::wire::replay::stored_reasoning_details(&replay).unwrap()[0]["data"],
        "opaque-signature"
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

// ===========================================================================
// Seam tests: drive `run_http_turn` against a scripted provider whose replies
// arm the loop's boundaries, proving the owned loop drives tool iterations,
// suspend, terminal-tool, cancellation, and budget finalization end to end.
// ===========================================================================

/// Golden: what one owned-loop turn writes for each protocol family's usage,
/// as the event token and cost columns and the stored `raw` payload. Every
/// writer that takes a usage is driven, so a change to how usage is read can
/// only land if what is stored stays byte-for-byte the same.
#[tokio::test]
async fn each_familys_usage_persists_as_its_golden() {
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use cairn_db::turso::params;
    use cairn_models::wire::stream::{StreamSink, StreamSummary};

    let families: Vec<(&str, TurnUsage)> = vec![
        (
            "chat",
            serde_json::from_value(json!({
                "prompt_tokens": 1200,
                "completion_tokens": 300,
                "total_tokens": 1500,
                "prompt_tokens_details": {"cached_tokens": 1000},
                "completion_tokens_details": {"reasoning_tokens": 120},
                "cost": 0.0042,
                "cost_details": {"upstream_inference_cost": 0.004}
            }))
            .unwrap(),
        ),
        (
            "anthropic",
            TurnUsage::from_anthropic(Some(40), Some(25), Some(128), Some(64), Some(0.0031)),
        ),
        (
            "responses",
            TurnUsage::from_responses(
                Some(900),
                Some(450),
                None,
                Some(json!({"cached_tokens": 300})),
                Some(json!({"reasoning_tokens": 200})),
                Some(0.012),
            ),
        ),
    ];

    let db_dir = tempfile::tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(db_dir.path().join("usage-golden.db"))
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

    let mut golden = Vec::new();
    for (family, usage) in families {
        let run_id = format!("run-{family}");
        let session_id = format!("session-{family}");
        let now = chrono::Utc::now().timestamp() as i32;
        let (run, session) = (run_id.clone(), session_id.clone());
        orch.db
            .local
            .write(move |conn| {
                let (run, session) = (run.clone(), session.clone());
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO runs(id, status, session_id, created_at, updated_at)
                             VALUES (?1, 'live', ?2, ?3, ?4)",
                        params![run.as_str(), session.as_str(), now, now],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let mut sink = TranscriptSink::new(
            &orch,
            orch.db.local.clone(),
            &run_id,
            &session_id,
            None,
            "golden",
        );
        sink.append("streamed answer").unwrap();
        sink.finalize(StreamSummary {
            text: "streamed answer".to_string(),
            thinking: String::new(),
            reasoning_details: vec![],
            usage: Some(usage.clone()),
            generation_id: Some(format!("gen-{family}")),
            model: Some(format!("model-{family}")),
        })
        .unwrap();
        super::persist::store_assistant_message(
            &orch,
            &orch.db.local,
            &run_id,
            &session_id,
            None,
            "whole answer",
            Some(&usage),
            Some(&format!("gen-{family}")),
            Some(&format!("model-{family}")),
            usage.cost,
            "golden",
        )
        .unwrap();
        store_assistant_tool_call(
            &orch,
            &orch.db.local,
            &run_id,
            &session_id,
            None,
            "",
            &[tool_call("read", r#"{"paths":["file:a"]}"#)],
            Some(&usage),
            Some(&format!("gen-{family}")),
            Some(&format!("model-{family}")),
            None,
            "golden",
        )
        .unwrap();
        super::persist::store_success_result(
            &orch,
            &orch.db.local,
            &run_id,
            &session_id,
            None,
            Some(&usage),
            Some(&format!("gen-{family}")),
            Some(&format!("model-{family}")),
            usage.cost,
            "golden",
        )
        .unwrap();

        let events: Vec<serde_json::Value> =
            crate::runs::queries::list_events_for_session(orch.db.local.clone(), &session_id)
                .unwrap()
                .iter()
                .map(|event| {
                    let data: serde_json::Value = serde_json::from_str(&event.data).unwrap();
                    json!({
                        "eventType": event.event_type,
                        "inputTokens": event.input_tokens,
                        "cacheReadTokens": event.cache_read_tokens,
                        "cacheCreateTokens": event.cache_create_tokens,
                        "outputTokens": event.output_tokens,
                        "thinkingTokens": event.thinking_tokens,
                        "costUsd": event.cost_usd,
                        "raw": data["raw"],
                    })
                })
                .collect();
        golden.push(json!({"family": family, "events": events}));
    }
    crate::backends::assert_golden_file(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/backends/testdata/usage_persisted.json"
        ),
        &(serde_json::to_string_pretty(&golden).unwrap() + "\n"),
    );
}

#[test]
fn live_context_occupancy_counts_every_prompt_part() {
    // 40 uncached, 128 read from cache, 64 written to it, 25 out. The writes are
    // part of the prompt the model saw, so the dial must count them.
    let usage = TurnUsage::from_anthropic(Some(40), Some(25), Some(128), Some(64), None);
    let state = super::context_state(
        "run",
        "session",
        "golden",
        "model",
        Some(200_000),
        &usage,
        7,
    );
    assert_eq!(state.used_tokens, 40 + 128 + 64 + 25);
    assert_eq!(state.last_output_tokens, Some(25));
    assert_eq!(state.context_window, Some(200_000));
    assert_eq!(state.session_id.as_deref(), Some("session"));
    assert_eq!(state.captured_at, 7);
}
