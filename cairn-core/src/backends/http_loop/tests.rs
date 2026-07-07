use super::persist::{
    store_assistant_tool_call, store_tool_result, tool_call_usage, AssistantStreamState,
};
use super::tools::{
    collapse_envelope_text, normalize_tool_payload, prepare_tool_call, render_tool_result,
    resolve_home_target, PreparedCall,
};
use super::{Generation, TurnToolCall, TurnUsage, WireAdapter};
use crate::agent_process::stream::{TokenCounts, TranscriptEvent};
use crate::backends::{AgentPermissions, SessionConfig, SessionStart};
use crate::dispatch::DispatchOutput;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use serde_json::json;
use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn tool_call(name: &str, arguments: &str) -> TurnToolCall {
    TurnToolCall {
        id: "call-1".to_string(),
        name: name.to_string(),
        arguments: arguments.to_string(),
    }
}

#[test]
fn collapse_envelope_text_extracts_run_text_and_drops_images() {
    // The OpenAI tool-message format is text-only, so the run/read envelope
    // collapses to its text on the HTTP-loop edge; image blocks are dropped
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
            input: Some(10),
            output: Some(5),
            cache_read: Some(3),
            cache_create: None,
            thinking: Some(2)
        }
    );
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
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use cairn_db::turso::params;

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
    let mut state = AssistantStreamState::open(
        &orch,
        orch.db.local.clone(),
        "run-1",
        "session-1",
        None,
        2,
        "openrouter",
    )
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
        3,
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
        4,
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
// Seam test: drive the generic loop with an in-test WireAdapter whose
// `Message` is NOT an OpenRouter wire type, proving `run_http_turn` is truly
// provider-agnostic — it drives tool iterations, suspend, terminal-tool, and
// budget finalization with no OpenRouter wire DTOs in sight.
// ===========================================================================

/// A conversation message that is deliberately not any wire DTO.
#[derive(Clone)]
struct SeamMessage(#[allow(dead_code)] String);

#[derive(Clone, Copy)]
enum SeamMode {
    /// One tool round, then a plain text answer → success finalize.
    SuccessThenText,
    /// Arm the terminal-tool boundary during the POST → finalize after one round.
    TerminalTool,
    /// Request a suspend during the POST → finalize suspended after one round.
    Suspend,
    /// Always return a tool call → hit the iteration budget and finalize.
    Budget,
}

struct SeamAdapter {
    mode: SeamMode,
    calls: Arc<AtomicUsize>,
}

/// A generation with a single tool call the loop will reject (unknown verb), so
/// it exercises the full store/execute/persist machinery with no real dispatch.
fn seam_tool_gen(n: usize) -> Generation<SeamMessage> {
    Generation {
        assistant_message: SeamMessage(format!("assistant-{n}")),
        assistant_text: String::new(),
        tool_calls: vec![TurnToolCall {
            id: format!("seam-call-{n}"),
            name: "frobnicate".to_string(),
            arguments: "{}".to_string(),
        }],
        reasoning_details: None,
        usage: None,
        finish_reason: Some("tool_calls".to_string()),
        generation_id: None,
        response_model: None,
        streamed_text: false,
    }
}

fn seam_text_gen(text: &str) -> Generation<SeamMessage> {
    Generation {
        assistant_message: SeamMessage("assistant-final".to_string()),
        assistant_text: text.to_string(),
        tool_calls: Vec::new(),
        reasoning_details: None,
        usage: None,
        finish_reason: Some("stop".to_string()),
        generation_id: None,
        response_model: None,
        streamed_text: false,
    }
}

impl WireAdapter for SeamAdapter {
    type Message = SeamMessage;

    fn backend_key(&self) -> &'static str {
        "seam"
    }
    fn backend_name(&self) -> &'static str {
        "Seam"
    }
    fn default_model(&self) -> &'static str {
        "seam/default"
    }
    fn api_key(&self, _orch: &Orchestrator) -> Option<String> {
        Some("seam-key".to_string())
    }
    fn build_conversation(
        &self,
        _orch: &Orchestrator,
        _config: &SessionConfig,
        _session_id: &str,
        _system_prompt: &str,
    ) -> Result<Vec<SeamMessage>, String> {
        Ok(vec![SeamMessage("system".to_string())])
    }
    fn context_window(&self, _orch: &Orchestrator, _model: &str) -> Option<i64> {
        None
    }
    fn fit_conversation<'a>(
        &self,
        messages: &'a [SeamMessage],
        _window: Option<i64>,
    ) -> Cow<'a, [SeamMessage]> {
        Cow::Borrowed(messages)
    }
    #[allow(clippy::too_many_arguments)]
    fn post_generation(
        &self,
        orch: &Orchestrator,
        _run_db: &Arc<LocalDb>,
        _api_key: &str,
        _model: &str,
        _session_id: &str,
        _outgoing: &[SeamMessage],
        _config: &SessionConfig,
        run_id: &str,
        _turn_id: Option<&str>,
        _sequence: i32,
        _cancel: &Arc<AtomicBool>,
    ) -> Result<Generation<SeamMessage>, String> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            SeamMode::SuccessThenText => {
                if n == 0 {
                    Ok(seam_tool_gen(n))
                } else {
                    Ok(seam_text_gen("all done"))
                }
            }
            SeamMode::TerminalTool => {
                // The loop checks this AFTER executing the round's tool calls.
                orch.process_state.arm_terminal_tool(run_id);
                Ok(seam_tool_gen(n))
            }
            SeamMode::Suspend => {
                orch.process_state
                    .request_suspend(run_id, crate::agent_process::process::SuspendKind::Prompt);
                Ok(seam_tool_gen(n))
            }
            SeamMode::Budget => Ok(seam_tool_gen(n)),
        }
    }
    fn render_tool_result_message(
        &self,
        _tool_call_id: &str,
        _output: DispatchOutput,
    ) -> SeamMessage {
        SeamMessage("tool-result".to_string())
    }
}

fn seam_config(run_id: &str, session_id: &str) -> SessionConfig {
    SessionConfig {
        run_id: run_id.to_string(),
        working_dir: "/tmp".to_string(),
        prompt: "hi".to_string(),
        system_prompt_content: None,
        system_prompt_dynamic_tail: None,
        model: None,
        session_start: SessionStart::New {
            session_id: session_id.to_string(),
        },
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        mcp_config_json: "{}".to_string(),
        home_uri: "cairn://p/SEAM/1/1/builder".to_string(),
        max_thinking_tokens: None,
        reasoning_effort: None,
        service_tier: None,
        permissions: AgentPermissions::new(Fence::Allow),
        bidirectional: false,
        identity: None,
        output_schema: None,
        ambient: false,
        is_ephemeral_call: false,
    }
}

async fn seam_orch() -> (Orchestrator, tempfile::TempDir) {
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    let db_dir = tempfile::tempdir().unwrap();
    let db = LocalDb::open(db_dir.path().join("seam.db")).await.unwrap();
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
    (orch, db_dir)
}

async fn seam_prepare_run(orch: &Orchestrator, run_id: &str, session_id: &str) {
    use cairn_db::turso::params;
    let now = chrono::Utc::now().timestamp() as i32;
    let rid = run_id.to_string();
    let sid = session_id.to_string();
    orch.db
        .local
        .write(|conn| {
            let rid = rid.clone();
            let sid = sid.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs(id, status, session_id, created_at, updated_at)
                         VALUES (?1, 'live', ?2, ?3, ?4)",
                    params![rid.as_str(), sid.as_str(), now, now],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    // Register a run handle so take_suspend / terminal_tool_armed / arm resolve.
    let cancel = Arc::new(AtomicBool::new(false));
    let mut handle = crate::agent_process::process::RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(Box::new(super::HttpTurnStdin { cancel })))),
        Some(session_id.to_string()),
        None,
    );
    handle.owns_turn_loop = true;
    if let Ok(mut processes) = orch.process_state.processes.lock() {
        processes.register(run_id.to_string(), handle);
    }
}

/// Drive `run_http_turn` on a dedicated OS thread (as production does), so its
/// blocking DB writes and finalize path run outside the tokio test runtime.
async fn run_seam(mode: SeamMode) -> (Orchestrator, usize, tempfile::TempDir) {
    let (orch, db_dir) = seam_orch().await;
    let run_id = "seam-run";
    let session_id = "seam-session";
    seam_prepare_run(&orch, run_id, session_id).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let orch_thread = orch.clone();
    let run_db = orch.db.local.clone();
    let cfg = seam_config(run_id, session_id);
    let adapter = SeamAdapter {
        mode,
        calls: calls.clone(),
    };
    let sid = session_id.to_string();
    std::thread::spawn(move || {
        super::run_http_turn(
            &orch_thread,
            run_db,
            cfg,
            "seam-key".to_string(),
            sid,
            0,
            None,
            Arc::new(AtomicBool::new(false)),
            String::new(),
            adapter,
        )
    })
    .join()
    .unwrap()
    .unwrap();
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
    let (orch, calls, _dir) = run_seam(SeamMode::Suspend).await;
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
