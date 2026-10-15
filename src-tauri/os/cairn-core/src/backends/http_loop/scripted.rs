//! A scripted provider for driving real owned-loop turns in tests.
//!
//! [`ScriptedComplete`] stands in for the facet's HTTP `Complete`: it answers
//! each generation with the next queued reply and records the request body it
//! was handed. [`record_turn`] runs one turn through `run_http_turn` against it
//! and returns every body serialized, which is what the per-provider golden
//! tests compare.
//!
//! Goldens compare text, and the text is exactly what reaches the wire.
//! `serde_json`'s `preserve_order` feature is enabled in this workspace only
//! by build scripts (tree-sitter's, Tauri's), and build-dependency features do
//! not reach runtime code under Cargo's v2 resolver. So every request `Value`
//! serializes with its keys sorted, here and in a shipped build alike, and the
//! order a builder inserts keys in is not observable. `serde_json_sorts_keys`
//! pins that: if the feature ever reaches runtime, it fails, and the goldens
//! need re-deriving in whatever order the builders then produce.
//!
//! Advertised tools are abbreviated to their names. The schemas come from one
//! shared definition and do not vary per provider; spelling them out in every
//! golden would bury the shape these tests exist to watch.

use super::{run_http_turn, Connection, WireAdapter};
use crate::agent_process::process::{AgentProcessState, RunHandle};
use crate::backends::{AgentPermissions, SessionConfig, SessionStart};
use crate::models::Fence;
use crate::orchestrator::models::ModelRuntime;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_models::interface::complete::{
    Complete, CompletionCall, CompletionError, CompletionReply,
};
use cairn_models::turn::TurnExit;
use cairn_models::wire::anthropic_messages::wire::MessagesResponse;
use cairn_models::wire::openai_compat::wire::ChatResponse;
use cairn_models::wire::openai_responses::wire::{ResponseEnvelope, ResponsesResponse};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

pub(in crate::backends) const RUN_ID: &str = "scripted-run";
pub(in crate::backends) const SESSION_ID: &str = "scripted-session";
/// The tool every scripted call names. No verb answers to it, so the loop
/// rejects it with a fixed message and nothing is dispatched.
pub(in crate::backends) const UNKNOWN_TOOL: &str = "frobnicate";

/// Answers each generation from a queue and records what was sent.
pub(in crate::backends) struct ScriptedComplete {
    replies: Mutex<VecDeque<CompletionReply>>,
    bodies: Arc<Mutex<Vec<Value>>>,
}

impl ScriptedComplete {
    pub(in crate::backends) fn new(
        replies: Vec<CompletionReply>,
    ) -> (Self, Arc<Mutex<Vec<Value>>>) {
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let complete = Self {
            replies: Mutex::new(replies.into()),
            bodies: bodies.clone(),
        };
        (complete, bodies)
    }
}

impl Complete for ScriptedComplete {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
        self.bodies.lock().unwrap().push(call.body);
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| CompletionError::Stream("the script has no more replies".to_string()))
    }
}

/// An orchestrator on a migrated temporary database whose every HTTP
/// generation is answered by `complete`.
pub(in crate::backends) async fn orchestrator(
    complete: Arc<dyn Complete>,
) -> (Orchestrator, tempfile::TempDir) {
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    let db_dir = tempfile::tempdir().unwrap();
    let db = LocalDb::open(db_dir.path().join("scripted.db"))
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
    let mut runtime = ModelRuntime::local(
        services.process.clone(),
        Arc::new(AgentProcessState::default()),
    );
    runtime.complete = complete;
    let orch = OrchestratorBuilder::new(db_state, services, config_dir)
        .models_runtime(runtime)
        .build();
    (orch, db_dir)
}

/// A live run row and a registered owned-loop handle, which is what the
/// loop's suspend and terminal-tool reads resolve against.
pub(in crate::backends) async fn prepare_run(orch: &Orchestrator, run_id: &str, session_id: &str) {
    use cairn_db::turso::params;
    let now = chrono::Utc::now().timestamp() as i32;
    let (rid, sid) = (run_id.to_string(), session_id.to_string());
    orch.db
        .local
        .write(|conn| {
            let (rid, sid) = (rid.clone(), sid.clone());
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
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(Box::new(super::HttpTurnControl {
            cancel: Arc::new(AtomicBool::new(false)),
            provider: "Test".to_string(),
        })))),
        Some(session_id.to_string()),
        None,
    );
    handle.owns_turn_loop = true;
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register(run_id.to_string(), handle);
}

/// A fresh session saying "hi" to `model`, with no generation knobs set.
pub(in crate::backends) fn session_config(model: &str) -> SessionConfig {
    SessionConfig {
        run_id: RUN_ID.to_string(),
        working_dir: "/tmp".to_string(),
        project_id: "project-scripted".to_string(),
        project_key: "SCRIPTED".to_string(),
        prompt: "hi".to_string(),
        goal_objective: None,
        clear_native_goal: false,
        message_content: crate::agent_process::stdin::MessageContent::text("hi"),
        system_prompt_content: None,
        system_prompt_dynamic_tail: None,
        model: Some(model.into()),
        session_start: SessionStart::New {
            session_id: SESSION_ID.to_string(),
        },
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        mcp: crate::config::mcp_setup::stdio_attachment(
            "cairn-cmd",
            3849,
            None,
            "dev",
            None,
            "standard",
        ),
        mcp_bearer: None,
        home_uri: "cairn://p/SCRIPTED/1/1/builder".to_string(),
        max_thinking_tokens: None,
        reasoning_effort: None,
        service_tier: None,
        permissions: AgentPermissions::new(Fence::Allow),
        bidirectional: false,
        identity: None,
        account_id: None,
        output_schema: None,
        ambient: false,
        is_ephemeral_call: false,
    }
}

/// The schema the structured-output goldens ask for.
pub(in crate::backends) fn answer_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"]
    })
}

/// Run one turn of the adapter `make_adapter` builds, answered by `replies`,
/// and return every request body it sent, serialized.
///
/// The adapter is built against the scripted orchestrator so a test can
/// configure it there first (settings, for one). The connection carries a
/// fixed key rather than resolving a credential, which no test store holds.
pub(in crate::backends) async fn record_turn<A>(
    config: SessionConfig,
    replies: Vec<CompletionReply>,
    make_adapter: impl FnOnce(&Orchestrator) -> A,
) -> Vec<String>
where
    A: WireAdapter + Send + 'static,
{
    let (complete, bodies) = ScriptedComplete::new(replies);
    let (orch, _db_dir) = orchestrator(Arc::new(complete)).await;
    prepare_run(&orch, RUN_ID, SESSION_ID).await;
    let adapter = make_adapter(&orch);
    let thread_orch = orch.clone();
    let exit = std::thread::spawn(move || {
        run_http_turn(
            &thread_orch,
            thread_orch.db.local.clone(),
            config,
            Connection {
                api_key: Some("scripted-key".to_string()),
            },
            SESSION_ID.to_string(),
            None,
            Arc::new(AtomicBool::new(false)),
            "SYSTEM".to_string(),
            adapter,
        )
    })
    .join()
    .unwrap();
    assert!(
        matches!(exit, Ok(TurnExit::Completed)),
        "the scripted turn must complete: {exit:?}"
    );
    let bodies = bodies.lock().unwrap().clone();
    bodies.into_iter().map(abbreviate_tools).collect()
}

/// Compare recorded bodies with their goldens. A mismatch prints the actual
/// bodies one per line, unescaped, so they read (and paste back) as `r#"…"#`
/// literals.
#[track_caller]
pub(in crate::backends) fn assert_golden(actual: &[String], expected: &[&str]) {
    if actual
        .iter()
        .map(String::as_str)
        .ne(expected.iter().copied())
    {
        panic!(
            "request bodies differ from their goldens; actual:\n{}",
            actual.join("\n")
        );
    }
}

fn abbreviate_tools(mut body: Value) -> String {
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools.iter_mut() {
            let name = tool
                .pointer("/function/name")
                .or_else(|| tool.get("name"))
                .cloned()
                .expect("every advertised tool has a name");
            *tool = name;
        }
    }
    serde_json::to_string(&body).unwrap()
}

fn call_ids(count: usize) -> impl Iterator<Item = String> {
    (1..=count).map(|n| format!("call-{n}"))
}

/// A chat-completions answer.
pub(in crate::backends) fn chat_text(text: &str) -> CompletionReply {
    let response: ChatResponse = serde_json::from_value(json!({
        "choices": [{"message": {"role": "assistant", "content": text}}],
        "finish_reason": "stop",
    }))
    .unwrap();
    CompletionReply::Chat(response)
}

/// A chat-completions generation calling [`UNKNOWN_TOOL`] once per id. A turn
/// that makes many generations should give each its own ids, as a provider
/// does: transcript persistence matches results to calls by id, and a reused
/// id makes every later round slower.
pub(in crate::backends) fn chat_tool_calls_with_ids(
    ids: impl Iterator<Item = String>,
) -> CompletionReply {
    let calls: Vec<Value> = ids
        .map(|id| json!({"id": id, "type": "function", "function": {"name": UNKNOWN_TOOL, "arguments": "{}"}}))
        .collect();
    let response: ChatResponse = serde_json::from_value(json!({
        "choices": [{"message": {"role": "assistant", "content": null, "tool_calls": calls}}],
        "finish_reason": "tool_calls",
    }))
    .unwrap();
    CompletionReply::Chat(response)
}

/// An Anthropic Messages answer.
pub(in crate::backends) fn messages_text(text: &str) -> CompletionReply {
    let response: MessagesResponse = serde_json::from_value(json!({
        "content": [{"type": "text", "text": text}],
        "stop_reason": "end_turn",
    }))
    .unwrap();
    CompletionReply::Messages(response)
}

/// An Anthropic Messages generation making `count` calls to [`UNKNOWN_TOOL`].
pub(in crate::backends) fn messages_tool_uses(count: usize) -> CompletionReply {
    let uses: Vec<Value> = call_ids(count)
        .map(|id| json!({"type": "tool_use", "id": id, "name": UNKNOWN_TOOL, "input": {}}))
        .collect();
    let response: MessagesResponse = serde_json::from_value(json!({
        "content": uses,
        "stop_reason": "tool_use",
    }))
    .unwrap();
    CompletionReply::Messages(response)
}

/// An OpenAI Responses answer.
pub(in crate::backends) fn responses_text(text: &str) -> CompletionReply {
    responses(json!([{
        "type": "message", "role": "assistant",
        "content": [{"type": "output_text", "text": text}],
    }]))
}

/// An OpenAI Responses generation making `count` calls to [`UNKNOWN_TOOL`].
pub(in crate::backends) fn responses_function_calls(count: usize) -> CompletionReply {
    let calls: Vec<Value> = call_ids(count)
        .map(|id| json!({"type": "function_call", "call_id": id, "name": UNKNOWN_TOOL, "arguments": "{}"}))
        .collect();
    responses(Value::Array(calls))
}

fn responses(output: Value) -> CompletionReply {
    let envelope: ResponseEnvelope =
        serde_json::from_value(json!({"status": "completed", "output": output})).unwrap();
    CompletionReply::Responses(ResponsesResponse::from_envelope(envelope))
}

#[test]
fn serde_json_sorts_keys() {
    let mut object = serde_json::Map::new();
    object.insert("zebra".to_string(), json!(1));
    object.insert("apple".to_string(), json!(2));
    assert_eq!(
        serde_json::to_string(&json!({"model": 1, "messages": 2, "extra": object})).unwrap(),
        r#"{"extra":{"apple":2,"zebra":1},"messages":2,"model":1}"#,
        "serde_json's preserve_order reached runtime; re-derive the request-body goldens"
    );
}
