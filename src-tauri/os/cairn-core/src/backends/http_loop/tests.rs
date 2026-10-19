use super::persist::{
    store_assistant_tool_call, store_tool_result, tool_call_usage, TranscriptSink,
};
use super::scripted::{self, chat_text, chat_tool_calls_with_ids};
use super::tools::{collapse_envelope_text, prepare_tool_call, render_tool_result, PreparedCall};
use super::{HttpUsageExt, TurnToolCall, TurnUsage, WireAdapter};
use crate::agent_process::process::AgentProcessState;
use crate::agent_process::stream::{TokenCounts, TranscriptEvent};
use crate::backends::openai_compat::http::{ChatDialect, Endpoint};
use crate::backends::openai_compat::wire::ChatMessage;
use crate::backends::SessionConfig;
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_models::interface::complete::{
    Complete, CompletionCall, CompletionError, CompletionReply, Delivery,
};
use cairn_models::turn::ChatFamily;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

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
        prompt_tokens_details: Some(json!({"cached_tokens": 3})),
        completion_tokens_details: Some(json!({"reasoning_tokens": 2})),
        cost: None,
        cost_details: None,
    };
    assert_eq!(
        usage.token_counts(),
        TokenCounts {
            input: Some(7),
            output: Some(5),
            cache_read: Some(3),
            cache_create: None,
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
/// by the same `list_events_for_session_delta` query the transcript reads, so
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

const SEAM_RUN: &str = "seam-run";
const SEAM_SESSION: &str = "seam-session";

#[derive(Clone, Copy)]
enum SeamMode {
    /// One tool round, then a plain text answer → success finalize.
    SuccessThenText,
    /// Arm the terminal-tool boundary during the POST → finalize after one round.
    TerminalTool,
    /// Request a suspend during the POST → finalize suspended after one round.
    Suspend(cairn_models::turn::SuspendKind),
    CancelBeforePost,
    CancelDuringPost,
    Failure,
    Panic,
    /// Always return a tool call → hit the iteration budget and finalize.
    Budget,
}

/// Answers each generation as `mode` says, acting on the run's process state
/// the way a tool dispatched in that generation would. Every tool call names a
/// verb nothing answers to, so the loop exercises its full
/// store/execute/persist path with no real dispatch.
struct SeamComplete {
    mode: SeamMode,
    calls: Arc<AtomicUsize>,
    process_state: OnceLock<Arc<AgentProcessState>>,
}

impl Complete for SeamComplete {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let state = self.process_state.get().expect("wired before the turn");
        let tool_call = || chat_tool_calls_with_ids(std::iter::once(format!("seam-call-{n}")));
        match self.mode {
            SeamMode::SuccessThenText => Ok(if n == 0 {
                tool_call()
            } else {
                chat_text("all done")
            }),
            SeamMode::TerminalTool => {
                // The loop checks this AFTER executing the round's tool calls.
                state.arm_terminal_tool(SEAM_RUN);
                Ok(tool_call())
            }
            SeamMode::Suspend(kind) => {
                state.request_suspend(SEAM_RUN, kind);
                Ok(tool_call())
            }
            SeamMode::Budget => Ok(tool_call()),
            SeamMode::CancelBeforePost => panic!("cancelled loop must not POST"),
            SeamMode::CancelDuringPost => {
                if let Delivery::Stream { cancel, .. } = call.delivery {
                    cancel.store(true, Ordering::SeqCst);
                }
                Ok(chat_text("partial response must not become success"))
            }
            SeamMode::Failure => Err(CompletionError::Stream("fixture failure".into())),
            SeamMode::Panic => panic!("fixture panic"),
        }
    }
}

struct SeamAdapter;

impl WireAdapter for SeamAdapter {
    type Family = ChatFamily;

    fn backend_key(&self) -> &str {
        "seam"
    }
    fn backend_name(&self) -> &str {
        "Seam"
    }
    fn default_model(&self) -> &'static str {
        "seam/default"
    }
    fn connection(&self, _orch: &Orchestrator) -> Result<super::Connection, String> {
        Ok(super::Connection {
            api_key: Some("seam-key".to_string()),
        })
    }
    fn family(&self, _connection: &super::Connection) -> Result<ChatFamily, String> {
        Ok(ChatFamily {
            endpoint: Endpoint {
                provider_name: "Seam".to_string(),
                backend_key: "seam".to_string(),
                chat_url: "http://127.0.0.1:1/v1/chat/completions".to_string(),
                headers: Vec::new(),
            },
            dialect: ChatDialect::default(),
            extras: serde_json::Map::new(),
        })
    }
    fn build_conversation(
        &self,
        _orch: &Orchestrator,
        _config: &SessionConfig,
        _session_id: &str,
        _system_prompt: &str,
    ) -> Result<Vec<ChatMessage>, String> {
        Ok(vec![ChatMessage::system("system".to_string())])
    }
    fn context_window(&self, _orch: &Orchestrator, _model: &str) -> Option<i64> {
        None
    }
}

/// Drive `run_http_turn` on a dedicated OS thread (as production does), so its
/// blocking DB writes and finalize path run outside the tokio test runtime.
async fn run_seam(mode: SeamMode) -> (Orchestrator, usize, tempfile::TempDir) {
    let calls = Arc::new(AtomicUsize::new(0));
    let complete = Arc::new(SeamComplete {
        mode,
        calls: calls.clone(),
        process_state: OnceLock::new(),
    });
    let (orch, db_dir) = scripted::orchestrator(complete.clone()).await;
    let _ = complete.process_state.set(orch.process_state.clone());
    scripted::prepare_run(&orch, SEAM_RUN, SEAM_SESSION).await;
    let orch_thread = orch.clone();
    let run_db = orch.db.local.clone();
    let mut cfg = scripted::session_config("seam/default");
    cfg.run_id = SEAM_RUN.to_string();
    cfg.session_start = crate::backends::SessionStart::New {
        session_id: SEAM_SESSION.to_string(),
    };
    let sid = SEAM_SESSION.to_string();
    std::thread::spawn(move || {
        super::drive_http_turn(&orch_thread, SEAM_RUN, &sid, "Seam", || {
            let outcome = super::run_http_turn(
                &orch_thread,
                run_db,
                cfg,
                super::Connection {
                    api_key: Some("seam-key".to_string()),
                },
                sid.clone(),
                None,
                Arc::new(AtomicBool::new(matches!(mode, SeamMode::CancelBeforePost))),
                String::new(),
                SeamAdapter,
            )?;
            // Reporting a completion must not apply host policy inside the loop.
            assert_eq!(
                crate::backends::run_state::run_status("Seam", &orch_thread.db.local, SEAM_RUN)
                    .as_deref(),
                Some("live")
            );
            assert!(orch_thread
                .process_state
                .processes
                .lock()
                .unwrap()
                .get(SEAM_RUN)
                .is_some());
            Ok(outcome)
        });
    })
    .join()
    .unwrap();
    assert!(orch
        .process_state
        .processes
        .lock()
        .unwrap()
        .get(SEAM_RUN)
        .is_none());
    let status = crate::backends::run_state::run_status("Seam", &orch.db.local, SEAM_RUN);
    assert_eq!(
        status.as_deref(),
        Some(if matches!(mode, SeamMode::Failure | SeamMode::Panic) {
            "crashed"
        } else {
            "exited"
        })
    );
    (orch, calls.load(Ordering::SeqCst), db_dir)
}

fn seam_events(orch: &Orchestrator, session_id: &str) -> Vec<TranscriptEvent> {
    let delta = crate::runs::queries::list_events_for_session_delta(
        orch.db.local.clone(),
        session_id,
        None,
    )
    .unwrap();
    delta
        .events
        .iter()
        .map(|event| serde_json::from_str(&event.data).unwrap())
        .collect()
}

fn seam_has_tool_use(events: &[TranscriptEvent]) -> bool {
    events.iter().any(|event| {
        event
            .tool_uses
            .as_ref()
            .map(|uses| uses.iter().any(|use_| use_.name == "frobnicate"))
            .unwrap_or(false)
    })
}

#[tokio::test]
async fn seam_loop_drives_tool_iteration_then_success() {
    let (orch, calls, _dir) = run_seam(SeamMode::SuccessThenText).await;
    // Two generations: the tool round, then the final text answer.
    assert_eq!(calls, 2);
    let events = seam_events(&orch, "seam-session");
    assert!(seam_has_tool_use(&events), "tool-call event must persist");
    assert!(
        events.iter().any(|e| e.event_type == "tool_result"),
        "tool_result must persist"
    );
    assert!(
        events.iter().any(|e| e.event_type == "result:success"),
        "success result must persist"
    );
}

#[tokio::test]
async fn seam_loop_finalizes_on_terminal_tool() {
    let (orch, calls, _dir) = run_seam(SeamMode::TerminalTool).await;
    // The terminal-tool boundary ends the turn after one round — no second POST.
    assert_eq!(calls, 1);
    let events = seam_events(&orch, "seam-session");
    assert!(seam_has_tool_use(&events));
    assert!(
        events.iter().any(|e| e.event_type == "result:success"),
        "terminal-tool boundary must finalize with a success result"
    );
}

#[tokio::test]
async fn seam_loop_finalizes_on_suspend() {
    let (orch, calls, _dir) =
        run_seam(SeamMode::Suspend(cairn_models::turn::SuspendKind::Prompt)).await;
    // The suspend is read back after the round's tool executes and ends the turn
    // before any success result — no second POST.
    assert_eq!(calls, 1);
    let events = seam_events(&orch, "seam-session");
    assert!(seam_has_tool_use(&events));
    assert!(
        !events.iter().any(|e| e.event_type == "result:success"),
        "a suspended turn records no success result"
    );
}

#[tokio::test]
async fn seam_loop_finalizes_on_budget() {
    let (orch, calls, _dir) = run_seam(SeamMode::Budget).await;
    // Every generation returned a tool call, so the loop runs the full budget
    // then finalizes gracefully.
    assert_eq!(calls, super::MAX_TOOL_ITERATIONS);
    let events = seam_events(&orch, "seam-session");
    assert!(
        events.iter().any(|e| e
            .content
            .as_deref()
            .map(|c| c.contains("tool-iteration budget"))
            .unwrap_or(false)),
        "budget finalization must note the cap to the transcript"
    );
    assert!(
        events.iter().any(|e| e.event_type == "result:success"),
        "budget finalization records a success result"
    );
}

#[tokio::test]
async fn seam_loop_cancellation_discards_partial_generation() {
    for mode in [SeamMode::CancelBeforePost, SeamMode::CancelDuringPost] {
        let (orch, calls, _dir) = run_seam(mode).await;
        assert_eq!(
            calls,
            usize::from(matches!(mode, SeamMode::CancelDuringPost))
        );
        assert_eq!(seam_exit_reason(&orch).await, "user_stop");
        assert!(seam_events(&orch, "seam-session").is_empty());
    }
}

#[tokio::test]
async fn seam_loop_preserves_both_durable_wait_reasons() {
    use cairn_models::turn::SuspendKind;
    for (kind, reason) in [
        (SuspendKind::Prompt, "prompt_wait_suspended"),
        (SuspendKind::DelegatedTask, "delegated_wait_suspended"),
    ] {
        let (orch, calls, _dir) = run_seam(SeamMode::Suspend(kind)).await;
        assert_eq!(calls, 1);
        assert_eq!(seam_exit_reason(&orch).await, reason);
        assert!(!seam_events(&orch, "seam-session")
            .iter()
            .any(|e| e.event_type == "result:success"));
    }
}

#[tokio::test]
async fn seam_loop_errors_and_panics_settle_the_run_once() {
    for (mode, detail) in [
        (SeamMode::Failure, "Seam turn failed: fixture failure"),
        (SeamMode::Panic, "Seam turn panicked: fixture panic"),
    ] {
        let (orch, calls, _dir) = run_seam(mode).await;
        assert_eq!(calls, 1);
        // Top-level failure remains resumable; only terminal delegated failures
        // receive turn_failed/turn_panicked exit reasons.
        assert_eq!(
            orch.db
                .local
                .query_opt(
                    "SELECT exit_reason FROM runs WHERE id='seam-run'",
                    (),
                    |row| {
                        use crate::storage::RowExt;
                        row.opt_text(0)
                    }
                )
                .await
                .unwrap(),
            Some(None)
        );
        let events = seam_events(&orch, "seam-session");
        assert_eq!(events.len(), 1, "one diagnostic, no synthetic success");
        assert_eq!(events[0].content.as_deref(), Some(detail));
    }
}

async fn seam_exit_reason(orch: &Orchestrator) -> String {
    use crate::storage::RowExt;
    orch.db
        .local
        .query_opt(
            "SELECT exit_reason FROM runs WHERE id='seam-run'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .unwrap()
}

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

        let delta = crate::runs::queries::list_events_for_session_delta(
            orch.db.local.clone(),
            &session_id,
            None,
        )
        .unwrap();
        let events: Vec<serde_json::Value> = delta
            .events
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
