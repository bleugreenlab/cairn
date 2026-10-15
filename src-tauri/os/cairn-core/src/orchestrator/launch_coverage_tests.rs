//! Every provider launches through the cairn-models facet.
//!
//! A recording [`ModelRuntime`] replaces the facet's implementations: a
//! [`Complete`] that records and refuses, Claude and Codex providers that
//! record and hand back inert sessions, and a host that spawns nothing. Each
//! provider in the shipped catalog, plus a custom one, is then launched
//! through the ordinary `backend_for_name(key).start_session` path, and the
//! recorder must have seen it. A provider added to the catalog without an
//! adapter here fails, because `plan` has no answer for it.

use std::io::Read;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use cairn_models::codex::app_server::AppServerClient;
use cairn_models::interface::complete::{
    Complete, CompletionCall, CompletionError, CompletionReply, Delivery,
};
use cairn_models::interface::session::{
    Cli, CodexModels, CodexServer, CodexSessionProvider, LaunchedSession, ProviderSpec,
    ServerLaunch, SessionControl, SessionError, SessionEvents, SessionFacts, SessionHost,
    SessionInput, SessionProvider, SessionSpec,
};
use cairn_models::interface::Models;
use cairn_models::types::catalog::{
    claude_models, DiscoveredWireProtocol, ProviderModelCatalog, CUSTOM_KEY_PREFIX,
    PROVIDER_CATALOG,
};
use cairn_models::wire::MessageContent;
use crossbeam_channel::{unbounded, Receiver, Sender};
use serde_json::{json, Value};

use super::models::tests::seeded_orchestrator;
use super::models::ModelRuntime;
use super::Orchestrator;
use crate::agent_process::process::PauseIntent;
use crate::backends::{
    backend_for_name, AgentPermissions, CompletionMessage, CompletionRequest, CompletionRole,
    SessionConfig, SessionStart,
};
use crate::credentials::{ActualTargetKind, ApiProvider, ClaudeAuth, CodexAuth, ProviderAuth};
use crate::identity::UserIdentity;
use crate::models::{Fence, Model};

const CLAUDE_KEY: &str = "sk-ant-coverage-claude-key";
const CODEX_KEY: &str = "sk-coverage-codex-key";
const OPENROUTER_KEY: &str = "sk-or-coverage-key";
const OPENCODE_KEY: &str = "sk-oc-coverage-key";
const WAIT: Duration = Duration::from_secs(20);

/// Everything the fakes saw, and the gate that holds an HTTP turn open.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<String>>,
    released: Mutex<bool>,
    gate: Condvar,
    /// Senders that keep each fake session's output open until the test ends.
    open_outputs: Mutex<Vec<Sender<Vec<u8>>>>,
    open_notifications: Mutex<Vec<Sender<Value>>>,
    /// The notification stream of the last app-server started, which a
    /// tool-free Codex turn answers on.
    server_notifications: Mutex<Option<Sender<Value>>>,
}

impl Recorder {
    fn record(&self, call: impl Into<String>) {
        self.calls.lock().unwrap().push(call.into());
    }

    fn saw(&self, call: &str) -> bool {
        self.calls.lock().unwrap().iter().any(|seen| seen == call)
    }

    fn wait_for(&self, call: &str) {
        let deadline = Instant::now() + WAIT;
        while !self.saw(call) {
            assert!(
                Instant::now() < deadline,
                "never saw {call}; saw {:?}",
                self.calls.lock().unwrap()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Hold the calling HTTP turn until the test has looked at it.
    fn hold(&self) {
        let mut released = self.released.lock().unwrap();
        let deadline = Instant::now() + WAIT;
        while !*released && Instant::now() < deadline {
            released = self
                .gate
                .wait_timeout(released, Duration::from_millis(50))
                .unwrap()
                .0;
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.gate.notify_all();
        self.open_outputs.lock().unwrap().clear();
        self.open_notifications.lock().unwrap().clear();
        self.server_notifications.lock().unwrap().take();
    }
}

/// What the scripted providers answer a tool-free one-shot with.
const CLAUDE_ANSWER: &str = "claude answer";
const CODEX_ANSWER: &str = "codex answer";
const SCHEMA_ANSWER: &str = r#"{"answer":"yes"}"#;

/// A tool-free Claude one-shot's stream-json: one terminal result, with a
/// structured output when the spec asked for a schema.
fn claude_one_shot_output(spec: &SessionSpec) -> Vec<u8> {
    let mut result = json!({
        "type": "result", "subtype": "success", "session_id": spec.start.session_id(),
        "is_error": false, "result": CLAUDE_ANSWER, "total_cost_usd": 0.02,
        "usage": {"input_tokens": 12, "output_tokens": 5,
                  "cache_creation_input_tokens": 100, "cache_read_input_tokens": 900}
    });
    if spec.output_schema.is_some() {
        result["structured_output"] = serde_json::from_str(SCHEMA_ANSWER).unwrap();
    }
    format!("{result}\n").into_bytes()
}

/// Answers a tool-free Codex turn on the shared server's notification stream,
/// the way the app-server would: the final message, usage, completion.
struct ScriptedCodexTurn {
    recorder: Arc<Recorder>,
    thread_id: String,
    answer: &'static str,
}

impl SessionControl for ScriptedCodexTurn {
    fn open_turn(&mut self, _content: &MessageContent) -> Result<(), String> {
        self.recorder.record("codex:open_turn");
        let notify = self
            .recorder
            .server_notifications
            .lock()
            .unwrap()
            .clone()
            .expect("a shared server was started");
        let thread = self.thread_id.as_str();
        let counts = json!({"totalTokens": 70, "inputTokens": 60, "cachedInputTokens": 40,
            "outputTokens": 10, "reasoningOutputTokens": 0});
        for message in [
            json!({"method": "item/completed", "params": {"threadId": thread,
                "item": {"id": "m", "type": "agentMessage", "text": self.answer}}}),
            json!({"method": "thread/tokenUsage/updated", "params": {"threadId": thread,
                "tokenUsage": {"total": counts, "last": counts}}}),
            json!({"method": "turn/completed", "params": {"threadId": thread,
                "turn": {"id": "turn-1", "status": "completed"}}}),
        ] {
            notify.send(message).map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    fn send(&mut self, _input: &SessionInput<'_>) -> Result<(), String> {
        Err("a one-shot takes one turn".to_string())
    }
    fn interrupt(&mut self) -> Result<(), String> {
        self.recorder.record("codex:interrupt");
        Ok(())
    }
    fn set_model(&mut self, _model: &str) -> Result<(), String> {
        Ok(())
    }
    fn set_permission_mode(&mut self, _mode: &str) -> Result<(), String> {
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct RecordingComplete(Arc<Recorder>);

impl Complete for RecordingComplete {
    fn complete(&self, call: CompletionCall<'_>) -> Result<CompletionReply, CompletionError> {
        let delivery = match call.delivery {
            Delivery::Stream { .. } => "stream",
            Delivery::Whole { .. } => "whole",
        };
        self.0.record(format!(
            "complete:{}:{}:{delivery}",
            call.wire.family(),
            call.wire.backend_key()
        ));
        if delivery == "stream" {
            self.0.hold();
        }
        Err(CompletionError::Upstream("recorded, not sent".to_string()))
    }
}

struct RecordingControl {
    recorder: Arc<Recorder>,
    provider: &'static str,
}

impl SessionControl for RecordingControl {
    fn open_turn(&mut self, _content: &MessageContent) -> Result<(), String> {
        self.recorder.record(format!("{}:open_turn", self.provider));
        Ok(())
    }
    fn send(&mut self, _input: &SessionInput<'_>) -> Result<(), String> {
        self.recorder.record(format!("{}:send", self.provider));
        Ok(())
    }
    fn interrupt(&mut self) -> Result<(), String> {
        self.recorder.record(format!("{}:interrupt", self.provider));
        Ok(())
    }
    fn set_model(&mut self, _model: &str) -> Result<(), String> {
        Ok(())
    }
    fn set_permission_mode(&mut self, _mode: &str) -> Result<(), String> {
        Ok(())
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Output that stays open until the recorder releases it.
struct HeldOutput {
    rx: Receiver<Vec<u8>>,
}

impl Read for HeldOutput {
    fn read(&mut self, _out: &mut [u8]) -> std::io::Result<usize> {
        let _ = self.rx.recv();
        Ok(0)
    }
}

fn facts(
    spec: &SessionSpec,
    provider: &str,
    backend_session_id: &str,
    host: &dyn SessionHost,
) -> SessionFacts {
    SessionFacts {
        provider: provider.to_string(),
        backend_session_id: Some(backend_session_id.to_string()),
        account_id: spec.account_id.clone(),
        host: host.name().to_string(),
        model: spec.model.clone(),
    }
}

struct RecordingClaude(Arc<Recorder>);

impl SessionProvider for RecordingClaude {
    fn open(
        &self,
        host: &dyn SessionHost,
        spec: SessionSpec,
    ) -> Result<LaunchedSession, SessionError> {
        self.0.record("claude:open");
        if spec.secrets.contains_value(CLAUDE_KEY) {
            self.0.record("claude:credential");
        }
        if spec.mcp.is_none() {
            let ProviderSpec::Claude(claude) = &spec.provider else {
                panic!("a Claude launch opened a {:?}", spec.provider);
            };
            assert!(matches!(
                claude.mode,
                cairn_models::interface::session::ClaudeMode::OneShot { .. }
            ));
            assert!(
                claude.allowed_tools.is_empty(),
                "a tool-free session allows nothing"
            );
            assert!(claude.disallowed_tools.contains(&"Bash".to_string()));
            assert!(claude.hook_settings.is_none());
            self.0.record("claude:tool_free");
            if spec.secrets.names().any(|name| name == "CLAUDE_CONFIG_DIR") {
                self.0.record("claude:managed_home");
            }
            return Ok(LaunchedSession {
                control: Box::new(RecordingControl {
                    recorder: self.0.clone(),
                    provider: "claude",
                }),
                events: SessionEvents::ClaudeStream {
                    stdout: Box::new(std::io::Cursor::new(claude_one_shot_output(&spec))),
                    stderr: None,
                },
                process: Arc::new(Mutex::new(None)),
                facts: facts(&spec, "claude", "claude-one-shot", host),
            });
        }
        let (tx, rx) = unbounded();
        self.0.open_outputs.lock().unwrap().push(tx);
        Ok(LaunchedSession {
            control: Box::new(RecordingControl {
                recorder: self.0.clone(),
                provider: "claude",
            }),
            events: SessionEvents::ClaudeStream {
                stdout: Box::new(std::io::BufReader::new(HeldOutput { rx })),
                stderr: None,
            },
            process: Arc::new(Mutex::new(None)),
            facts: facts(&spec, "claude", "claude-session", host),
        })
    }
}

struct RecordingCodex(Arc<Recorder>);

impl SessionProvider for RecordingCodex {
    fn open(
        &self,
        host: &dyn SessionHost,
        spec: SessionSpec,
    ) -> Result<LaunchedSession, SessionError> {
        self.0.record("codex:open");
        let ProviderSpec::Codex(codex) = &spec.provider else {
            panic!("a Codex launch opened a {:?}", spec.provider);
        };
        let (client, notifications) = match &codex.server {
            CodexServer::Dedicated(client) => {
                self.0.record("codex:dedicated");
                let (tx, rx) = unbounded();
                self.0.open_notifications.lock().unwrap().push(tx);
                (client.clone(), Some(rx))
            }
            CodexServer::Shared(client) => {
                self.0.record("codex:shared");
                (client.clone(), None)
            }
        };
        if spec.mcp.is_none() {
            assert!(
                matches!(codex.server, CodexServer::Shared(_)),
                "a Codex one-shot runs on the pool"
            );
            self.0.record("codex:tool_free");
            return Ok(LaunchedSession {
                control: Box::new(ScriptedCodexTurn {
                    recorder: self.0.clone(),
                    thread_id: "codex-one-shot".to_string(),
                    answer: if spec.output_schema.is_some() {
                        SCHEMA_ANSWER
                    } else {
                        CODEX_ANSWER
                    },
                }),
                events: SessionEvents::CodexAppServer {
                    client,
                    notifications,
                    current_turn: Arc::new(Mutex::new(None)),
                },
                process: Arc::new(Mutex::new(None)),
                facts: facts(&spec, "codex", "codex-one-shot", host),
            });
        }
        Ok(LaunchedSession {
            control: Box::new(RecordingControl {
                recorder: self.0.clone(),
                provider: "codex",
            }),
            events: SessionEvents::CodexAppServer {
                client,
                notifications,
                current_turn: Arc::new(Mutex::new(None)),
            },
            process: Arc::new(Mutex::new(None)),
            facts: facts(&spec, "codex", "codex-thread", host),
        })
    }
}

impl CodexSessionProvider for RecordingCodex {
    fn start_server(
        &self,
        _host: &dyn SessionHost,
        launch: ServerLaunch<'_>,
    ) -> Result<Arc<AppServerClient>, SessionError> {
        self.0.record("codex:start_server");
        if launch
            .login
            .is_some_and(|login| login.expose()["apiKey"] == CODEX_KEY)
        {
            self.0.record("codex:login");
        }
        let (client, notifications) = AppServerClient::for_test_with_notifications(
            |_| Ok(json!({"result": {}})),
            Arc::new(Mutex::new(Vec::new())),
        );
        *self.0.server_notifications.lock().unwrap() = Some(notifications);
        Ok(Arc::new(client))
    }
}

/// A host that runs nothing.
struct InertHost;

impl SessionHost for InertHost {
    fn name(&self) -> &str {
        "coverage-host"
    }
    fn resolve_cli(&self, cli: Cli) -> Result<String, SessionError> {
        Ok(format!("/coverage/{cli:?}"))
    }
    fn spawn(
        &self,
        _config: cairn_platform::services::process::SpawnConfig,
    ) -> Result<Box<dyn cairn_platform::services::process::ChildProcess>, String> {
        Err("a coverage launch must not spawn a process".to_string())
    }
    fn place_file(&self, dir: &str, name: &str, _contents: &[u8]) -> Result<String, String> {
        Ok(format!("{dir}/{name}"))
    }
    /// A clean host: no model catalog exists until a signed-in refresh writes
    /// one, which is what hardening the home needs.
    fn prepare_codex_home(&self, models: CodexModels<'_>) -> Result<String, SessionError> {
        match models {
            CodexModels::Refresh { login: Some(_) } => Ok("/coverage/codex-home".to_string()),
            _ => Err(SessionError::Setup(
                "Codex did not produce a model catalog for hardening".to_string(),
            )),
        }
    }
}

/// The model a provider is launched with, and what its launch must show.
struct Plan {
    model: &'static str,
    /// Recorder calls the launch must produce.
    launch: Vec<String>,
    /// The one-shot completion call, for providers that serve one.
    one_shot: Option<String>,
    /// Whether the session has a warm writer to send to and interrupt.
    warm: Option<&'static str>,
}

fn plan(key: &str) -> Plan {
    let http = |model: &'static str, key: &str| Plan {
        model,
        launch: vec![format!("complete:chat:{key}:stream")],
        one_shot: Some(format!("complete:chat:{key}:whole")),
        warm: None,
    };
    match key {
        "claude" => Plan {
            model: "sonnet",
            launch: vec![
                "claude:open".into(),
                "claude:credential".into(),
                "claude:open_turn".into(),
            ],
            one_shot: None,
            warm: Some("claude"),
        },
        "codex" => Plan {
            model: "gpt-coverage",
            launch: vec![
                "codex:start_server".into(),
                "codex:login".into(),
                "codex:open".into(),
                "codex:dedicated".into(),
                "codex:open_turn".into(),
            ],
            one_shot: None,
            warm: Some("codex"),
        },
        "openrouter" => http("openai/gpt-coverage", key),
        "opencode-go" => http("glm-coverage", key),
        "ollama" => http("qwen3:32b", key),
        "actual" => http("coverage-model", key),
        custom if custom.starts_with(CUSTOM_KEY_PREFIX) => http("qwen3.8-27b", custom),
        other => panic!(
            "provider {other} is in the shipped catalog but has no launch coverage: give it an \
             adapter over the cairn-models facet and a plan here"
        ),
    }
}

async fn orchestrator(recorder: &Arc<Recorder>) -> (Orchestrator, String) {
    let mut orch = seeded_orchestrator().await;
    orch.models_runtime = ModelRuntime {
        complete: Arc::new(RecordingComplete(recorder.clone())),
        claude: Arc::new(RecordingClaude(recorder.clone())),
        codex: Arc::new(RecordingCodex(recorder.clone())),
        host: Arc::new(InertHost),
    };
    for (provider, label, auth) in [
        (
            ApiProvider::OpenRouter,
            "OpenRouter",
            ProviderAuth::ApiKey {
                value: OPENROUTER_KEY.into(),
            },
        ),
        (
            ApiProvider::OpenCode,
            "OpenCode Go",
            ProviderAuth::ApiKey {
                value: OPENCODE_KEY.into(),
            },
        ),
        (
            ApiProvider::Ollama,
            "Ollama box",
            ProviderAuth::BaseUrl {
                url: "http://127.0.0.1:9".into(),
            },
        ),
        (
            ApiProvider::Actual,
            "Actual box",
            ProviderAuth::ActualTarget {
                kind: ActualTargetKind::Local,
                base_url: "http://127.0.0.1:9".into(),
                api_key: None,
                cluster_id: None,
            },
        ),
    ] {
        orch.add_account(provider, label.into(), auth, None)
            .unwrap();
    }
    let mut glm = claude_models().remove(0);
    glm.id = "glm-coverage".into();
    glm.model = "glm-coverage".into();
    glm.supported_parameters = vec!["tools".into()];
    glm.wire_protocol = Some(DiscoveredWireProtocol::OpenAiChatCompletions);
    orch.model_catalog.write().unwrap().insert(
        "opencode-go".to_string(),
        ProviderModelCatalog {
            backend: "opencode-go".to_string(),
            models: vec![glm],
            options: Vec::new(),
            refreshed_at: Some(1),
            error: None,
        },
    );
    let custom = orch
        .list_accounts(None)
        .into_iter()
        .find(|account| account.label == "DGX Spark")
        .expect("the seeded custom provider")
        .id;
    (orch, format!("{CUSTOM_KEY_PREFIX}{custom}"))
}

fn identity() -> UserIdentity {
    UserIdentity {
        user_id: "coverage-user".into(),
        email: "coverage@example.com".into(),
        name: "Coverage".into(),
        claude_auth: Some(ClaudeAuth::ApiKey(CLAUDE_KEY.into())),
        codex_auth: Some(CodexAuth::ApiKey(CODEX_KEY.into())),
        github_token: None,
    }
}

fn config(run_id: &str, session_id: &str, model: &str, ephemeral: bool) -> SessionConfig {
    SessionConfig {
        run_id: run_id.to_string(),
        working_dir: "/tmp".to_string(),
        project_id: "project-coverage".to_string(),
        project_key: "COVERAGE".to_string(),
        prompt: "hi".to_string(),
        goal_objective: None,
        clear_native_goal: false,
        message_content: MessageContent::text("hi"),
        system_prompt_content: None,
        system_prompt_dynamic_tail: None,
        model: Some(Model::new(model)),
        session_start: SessionStart::New {
            session_id: session_id.to_string(),
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
        home_uri: "cairn://p/COVERAGE/1/1/builder".to_string(),
        max_thinking_tokens: None,
        reasoning_effort: None,
        service_tier: None,
        permissions: AgentPermissions::new(Fence::Allow),
        bidirectional: true,
        identity: Some(identity()),
        account_id: None,
        output_schema: None,
        ambient: false,
        is_ephemeral_call: ephemeral,
    }
}

async fn seed_run(orch: &Orchestrator, run_id: &str, session_id: &str) {
    let (run_id, session_id) = (run_id.to_string(), session_id.to_string());
    orch.db
        .local
        .write(move |conn| {
            let (run_id, session_id) = (run_id.clone(), session_id.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs(id, status, session_id, created_at, updated_at)
                     VALUES (?1, 'starting', ?2, 1, 1)",
                    (run_id.as_str(), session_id.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

/// Every text value the launches could have written: events, runs, sessions.
async fn stored_text(orch: &Orchestrator) -> String {
    orch.db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut all = String::new();
                for sql in [
                    "SELECT COALESCE(group_concat(COALESCE(data, ''), ' '), '') FROM events",
                    "SELECT COALESCE(group_concat(COALESCE(error_message, '') || ' ' || COALESCE(exit_reason, ''), ' '), '') FROM runs",
                    "SELECT COALESCE(group_concat(COALESCE(backend_id, '') || ' ' || COALESCE(account_id, ''), ' '), '') FROM sessions",
                ] {
                    let mut rows = conn.query(sql, ()).await?;
                    if let Some(row) = rows.next().await? {
                        all.push_str(&row.get::<String>(0)?);
                        all.push(' ');
                    }
                }
                Ok(all)
            })
        })
        .await
        .unwrap()
}

fn view(orch: &Orchestrator, run_id: &str) -> cairn_models::interface::SessionView {
    orch.sessions()
        .into_iter()
        .find(|view| view.run_id == run_id)
        .unwrap_or_else(|| panic!("no live session for {run_id}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn every_catalog_provider_launches_through_the_facet() {
    let recorder = Arc::new(Recorder::default());
    let (orch, custom_key) = orchestrator(&recorder).await;
    let keys: Vec<String> = PROVIDER_CATALOG
        .iter()
        .map(|descriptor| descriptor.key.to_string())
        .chain(std::iter::once(custom_key.clone()))
        .collect();

    for key in &keys {
        let plan = plan(key);
        let run_id = format!("run-{key}");
        let session_id = format!("session-{key}");
        seed_run(&orch, &run_id, &session_id).await;
        let backend = backend_for_name(Some(key));
        let (launch_orch, launch_config) = (
            orch.clone(),
            config(&run_id, &session_id, plan.model, false),
        );
        tokio::task::spawn_blocking(move || backend.start_session(launch_config, &launch_orch))
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("{key} did not launch: {error}"));
        for call in &plan.launch {
            recorder.wait_for(call);
        }

        if let Some(provider) = plan.warm {
            let state = orch.process_state.clone();
            let (run, session) = (run_id.clone(), session_id.clone());
            tokio::task::spawn_blocking(move || {
                crate::backends::stdin::send_user_message(
                    &state,
                    &run,
                    &MessageContent::text("warm"),
                    &session,
                    None,
                )
                .unwrap();
                crate::backends::stdin::send_interrupt(&state, &run, PauseIntent::Reassert)
                    .unwrap();
            })
            .await
            .unwrap();
            assert!(recorder.saw(&format!("{provider}:send")), "{key} warm send");
            assert!(
                recorder.saw(&format!("{provider}:interrupt")),
                "{key} interrupt"
            );
        }

        if let Some(call) = plan.one_shot {
            let backend = backend_for_name(Some(key));
            let (one_shot_orch, model) = (orch.clone(), plan.model.to_string());
            let outcome = tokio::task::spawn_blocking(move || {
                backend.complete(
                    CompletionRequest {
                        system: None,
                        messages: vec![CompletionMessage {
                            role: CompletionRole::User,
                            content: "hi".to_string(),
                        }],
                        model,
                        project_id: None,
                        extras: Value::Null,
                        output_schema: None,
                        timeout: Duration::from_secs(5),
                    },
                    &one_shot_orch,
                )
            })
            .await
            .unwrap();
            assert!(outcome.is_err(), "{key}: the recorder refuses every call");
            assert!(recorder.saw(&call), "{key} one-shot did not reach Complete");
        }
    }

    // The pooled Codex call path starts its server and opens its thread
    // through the same provider, on a shared server.
    seed_run(&orch, "run-codex-call", "session-codex-call").await;
    let (call_orch, call_config) = (
        orch.clone(),
        config("run-codex-call", "session-codex-call", "gpt-coverage", true),
    );
    tokio::task::spawn_blocking(move || {
        backend_for_name(Some("codex")).start_session(call_config, &call_orch)
    })
    .await
    .unwrap()
    .unwrap();
    recorder.wait_for("codex:shared");

    // Live sessions report their launch facts, not a database guess.
    assert_eq!(
        view(&orch, "run-claude").backend_session_id.as_deref(),
        Some("claude-session")
    );
    orch.process_state
        .set_backend_session_id("run-claude", "claude-confirmed");
    assert_eq!(
        view(&orch, "run-claude").backend_session_id.as_deref(),
        Some("claude-confirmed"),
        "the Claude reader's confirmation reaches the view"
    );
    let codex = view(&orch, "run-codex");
    assert_eq!(codex.backend_session_id.as_deref(), Some("codex-thread"));
    assert_eq!(codex.host, "coverage-host");
    let custom = view(&orch, &format!("run-{custom_key}"));
    assert_eq!(
        custom.account_id.as_deref(),
        custom_key.strip_prefix(CUSTOM_KEY_PREFIX),
        "an HTTP session names the account its connection chose"
    );
    assert_eq!(
        custom.backend_session_id.as_deref(),
        Some(format!("session-{custom_key}").as_str())
    );

    recorder.release();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Secrets reached the providers (checked above) and nothing stored.
    let stored = stored_text(&orch).await;
    for secret in [CLAUDE_KEY, CODEX_KEY, OPENROUTER_KEY, OPENCODE_KEY] {
        assert!(!stored.contains(secret), "{secret} reached a stored row");
    }
}

fn one_shot_request(model: &str, output_schema: Option<Value>) -> CompletionRequest {
    CompletionRequest {
        system: None,
        messages: vec![CompletionMessage {
            role: CompletionRole::User,
            content: "hi".to_string(),
        }],
        model: model.to_string(),
        project_id: None,
        extras: Value::Null,
        output_schema,
        timeout: Duration::from_secs(5),
    }
}

async fn complete_on(
    orch: &Orchestrator,
    key: &'static str,
    request: CompletionRequest,
) -> Result<crate::backends::CompletionOutcome, CompletionError> {
    let orch = orch.clone();
    tokio::task::spawn_blocking(move || backend_for_name(Some(key)).complete(request, &orch))
        .await
        .unwrap()
}

/// Rows a one-shot must never write.
async fn session_rows(orch: &Orchestrator) -> i64 {
    let mut total = 0;
    for table in ["runs", "sessions", "events"] {
        total += orch
            .db
            .local
            .query_opt_i64(format!("SELECT COUNT(*) FROM {table}"), ())
            .await
            .unwrap()
            .unwrap_or(0);
    }
    total
}

/// Claude and Codex serve a response as one tool-free round trip through the
/// facet, on the account Cairn routes to, with the credential only
/// subscription support presents, and without a run, session, or event.
#[tokio::test(flavor = "multi_thread")]
async fn claude_and_codex_serve_responses_through_the_facet() {
    use cairn_models::subscription::testing::TempHomes;

    let recorder = Arc::new(Recorder::default());
    let mut orch = super::models::tests::bare_orchestrator().await;
    orch.models_runtime = ModelRuntime {
        complete: Arc::new(RecordingComplete(recorder.clone())),
        claude: Arc::new(RecordingClaude(recorder.clone())),
        codex: Arc::new(RecordingCodex(recorder.clone())),
        host: Arc::new(InertHost),
    };
    let homes = TempHomes::new(orch.config_dir.join("claude-profiles"));
    orch.subscriptions = Arc::new(homes.clone());
    orch.update_settings(crate::models::UpdateSettings {
        enabled_providers: Some(vec!["claude".to_string(), "codex".to_string()]),
        ..Default::default()
    })
    .unwrap();

    // With no account, both refuse, naming where to fix it, and nothing opens.
    for key in ["claude", "codex"] {
        let backend = backend_for_name(Some(key));
        let reason = backend
            .response_completion_availability(&orch, None)
            .unwrap_err();
        assert!(reason.contains("Settings → Providers"), "{key}: {reason}");
        assert!(complete_on(&orch, key, one_shot_request("m", None))
            .await
            .is_err());
    }
    // The default `sm` tier, the way thread era summaries ask for it.
    let era = crate::responses::parse_definition(
        "---\nname: Coverage\ndescription: a coverage response\ntimeout: 30s\n---\nSay hi",
    )
    .unwrap();
    let refused = crate::responses::invoke_definition(
        &orch,
        &era,
        &json!({}),
        "builtin:coverage",
        crate::responses::ResponseCaller::Internal {
            label: "coverage".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            refused,
            crate::responses::ResponseError::BackendUnavailable(_)
        ),
        "{refused:?}"
    );
    assert!(
        recorder.calls.lock().unwrap().is_empty(),
        "nothing launched"
    );

    // A signed-in Claude subscription, and an OpenAI API key for Codex.
    let profile = orch
        .add_account(
            ApiProvider::Anthropic,
            "Max".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
        )
        .unwrap();
    homes.sign_in(&profile.id);
    let openai = orch
        .add_account(
            ApiProvider::OpenAI,
            "OpenAI key".into(),
            ProviderAuth::ApiKey {
                value: CODEX_KEY.into(),
            },
            None,
        )
        .unwrap();
    for (key, model) in [("claude", "haiku"), ("codex", "gpt-coverage")] {
        let backend = backend_for_name(Some(key));
        backend
            .response_completion_availability(&orch, None)
            .unwrap_or_else(|error| panic!("{key}: {error}"));
        backend
            .response_model_availability(&orch, None, model)
            .unwrap_or_else(|error| panic!("{key}: {error}"));
    }
    let capabilities = crate::responses::model_capabilities(&orch, None, None, None);
    let claude = capabilities
        .backends
        .iter()
        .find(|backend| backend.backend == "claude")
        .unwrap();
    assert!(claude.runnable, "{:?}", claude.unavailable_reason);
    assert!(claude
        .selections
        .iter()
        .any(|selection| selection.kind == "tier" && selection.runnable));

    // Claude: text, and a schema's structured output as the answer.
    let plain = complete_on(&orch, "claude", one_shot_request("haiku", None))
        .await
        .unwrap();
    assert_eq!(plain.text, CLAUDE_ANSWER);
    assert_eq!(plain.account_id.as_deref(), Some(profile.id.as_str()));
    assert_eq!(plain.tokens.input, Some(1012), "the whole prompt");
    assert_eq!(plain.tokens.output, Some(5));
    assert_eq!(plain.cost, None, "a subscription's cost is notional");
    let schema = json!({"type": "object", "properties": {"answer": {"type": "string"}}});
    let structured = complete_on(
        &orch,
        "claude",
        one_shot_request("haiku", Some(schema.clone())),
    )
    .await
    .unwrap();
    assert_eq!(structured.parsed, Some(json!({"answer": "yes"})));
    assert_eq!(structured.text, SCHEMA_ANSWER);
    for call in ["claude:open", "claude:tool_free", "claude:managed_home"] {
        assert!(recorder.saw(call), "never saw {call}");
    }

    // Codex: a runless thread on the pool, signed in with the routed key.
    let codex = complete_on(&orch, "codex", one_shot_request("gpt-coverage", None))
        .await
        .unwrap();
    assert_eq!(codex.text, CODEX_ANSWER);
    assert_eq!(codex.account_id.as_deref(), Some(openai.id.as_str()));
    assert_eq!(codex.tokens.input, Some(60));
    assert_eq!(codex.tokens.output, Some(10));
    let codex_structured = complete_on(
        &orch,
        "codex",
        one_shot_request("gpt-coverage", Some(schema)),
    )
    .await
    .unwrap();
    assert_eq!(codex_structured.parsed, Some(json!({"answer": "yes"})));
    for call in [
        "codex:start_server",
        "codex:login",
        "codex:open",
        "codex:shared",
        "codex:tool_free",
        "codex:open_turn",
    ] {
        assert!(recorder.saw(call), "never saw {call}");
    }
    assert_eq!(
        orch.codex_pool
            .binding_for_thread("codex-one-shot")
            .map(|b| b.run_id),
        None,
        "a one-shot binds no run"
    );

    // A journaled response names its account, and nothing else was written.
    let journaled = crate::responses::invoke_definition(
        &orch,
        &era,
        &json!({}),
        "builtin:coverage",
        crate::responses::ResponseCaller::Internal {
            label: "coverage".into(),
        },
    )
    .await
    .unwrap();
    let rows = crate::storage::list_response_invocations(
        &orch.db.local,
        "workspace",
        "builtin:coverage",
        10,
    )
    .await
    .unwrap();
    let row = rows
        .iter()
        .find(|row| row.seq == journaled.seq)
        .expect("the journaled row");
    assert_eq!(row.status, "ok");
    assert!(
        [profile.id.as_str(), openai.id.as_str()]
            .contains(&row.account_id.as_deref().unwrap_or("")),
        "the serving account is journaled: {:?}",
        row.account_id
    );
    assert_eq!(session_rows(&orch).await, 0);

    recorder.release();
}

/// A decision is answered only by a decision-capable offer. A subscription-only
/// workspace refuses with the fixes and launches nothing (no CLI, no Response);
/// a Decisions provider is asked over the Decisions wire, whole and unstreamed;
/// and a custom chat server is offered as a logprobs route.
#[tokio::test(flavor = "multi_thread")]
async fn decisions_launch_only_on_a_decision_capable_offer() {
    use crate::credentials::{CustomModelPolicy, CustomProtocol, CustomSampling};
    use crate::models::decision::{DecisionNode, MeasuredBy};
    use cairn_models::subscription::testing::TempHomes;

    let recorder = Arc::new(Recorder::default());
    let mut orch = super::models::tests::bare_orchestrator().await;
    orch.models_runtime = ModelRuntime {
        complete: Arc::new(RecordingComplete(recorder.clone())),
        claude: Arc::new(RecordingClaude(recorder.clone())),
        codex: Arc::new(RecordingCodex(recorder.clone())),
        host: Arc::new(InertHost),
    };
    let homes = TempHomes::new(orch.config_dir.join("claude-profiles"));
    orch.subscriptions = Arc::new(homes.clone());
    orch.update_settings(crate::models::UpdateSettings {
        enabled_providers: Some(vec!["claude".to_string()]),
        ..Default::default()
    })
    .unwrap();
    let profile = orch
        .add_account(
            ApiProvider::Anthropic,
            "Max".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
        )
        .unwrap();
    homes.sign_in(&profile.id);
    let decision = DecisionNode::port_choice(
        "Does the issue mention a database?",
        &["yes".to_string(), "no".to_string()],
        None,
    );

    let refused = orch.decide(None, &decision, "{}".into()).await.unwrap_err();
    assert!(
        refused.starts_with("No decision-capable model is configured"),
        "{refused}"
    );
    assert!(
        refused.contains("Decisions API key") && refused.contains("logprobs"),
        "the refusal names both fixes: {refused}"
    );
    assert!(
        recorder.calls.lock().unwrap().is_empty(),
        "a signed-in subscription is not a decision offer; nothing launches"
    );

    let jev = orch
        .add_custom_provider(
            "TypeSafe".into(),
            ProviderAuth::custom_endpoint(
                "https://ai-gateway.vercel.sh/v1",
                CustomProtocol::Decisions,
                Some("gateway-key"),
                &[],
                CustomModelPolicy::Explicit {
                    ids: vec!["typesafe-ai/jev".into()],
                },
                CustomSampling::default(),
                1,
            )
            .unwrap(),
            None,
        )
        .unwrap();
    let jev_key = cairn_models::types::catalog::custom_key(&jev.id);
    orch.refresh_provider_model_catalog_blocking(&jev_key);

    let offer = orch.decision_offer(None, &decision).unwrap();
    assert_eq!(
        (offer.provider.as_str(), offer.model.as_str(), offer.method),
        (jev_key.as_str(), "typesafe-ai/jev", MeasuredBy::Native)
    );
    let failed = orch.decide(None, &decision, "{}".into()).await.unwrap_err();
    assert!(failed.contains("recorded, not sent"), "{failed}");
    assert_eq!(
        *recorder.calls.lock().unwrap(),
        [format!("complete:decisions:{jev_key}:whole")],
        "one whole POST on the Decisions wire"
    );

    let spark = orch
        .add_custom_provider(
            "Spark".into(),
            ProviderAuth::custom_endpoint(
                "http://10.10.10.83:8000",
                CustomProtocol::OpenAiChatCompletions,
                None,
                &[],
                CustomModelPolicy::Explicit {
                    ids: vec!["gemma4-26b".into()],
                },
                CustomSampling::default(),
                4_096,
            )
            .unwrap(),
            None,
        )
        .unwrap();
    let spark_key = cairn_models::types::catalog::custom_key(&spark.id);
    orch.refresh_provider_model_catalog_blocking(&spark_key);
    let pinned = DecisionNode {
        model: Some("gemma4-26b".into()),
        ..decision.clone()
    };
    let offer = orch.decision_offer(None, &pinned).unwrap();
    assert_eq!(
        (offer.provider.as_str(), offer.method),
        (spark_key.as_str(), MeasuredBy::Logprobs)
    );
    assert_eq!(
        orch.decision_offer(None, &decision).unwrap().method,
        MeasuredBy::Native,
        "unset prefers a native offer over a logprobs one"
    );
    assert_eq!(session_rows(&orch).await, 0);

    recorder.release();
}

/// The Claude CLI as it behaved in cairn/4978, scripted by session id. The
/// first launch under an id creates the conversation and then reports an
/// `init` without Cairn's tools; every later fresh start under that id is
/// refused on stderr; an id it has never seen launches cleanly.
#[derive(Default)]
struct ScriptedClaude {
    created: Mutex<std::collections::HashSet<String>>,
    /// The id each launch handed the CLI as `--session-id`, in order.
    launched_ids: Mutex<Vec<String>>,
}

impl SessionProvider for ScriptedClaude {
    fn open(
        &self,
        host: &dyn SessionHost,
        spec: SessionSpec,
    ) -> Result<LaunchedSession, SessionError> {
        let id = spec.start.session_id().to_string();
        self.launched_ids.lock().unwrap().push(id.clone());
        let first_launch_of_id = self.created.lock().unwrap().insert(id.clone());
        let refused = !first_launch_of_id;
        let rejected_at_init = first_launch_of_id && id.ends_with("-rejected");
        let (stdout, stderr): (Vec<u8>, Option<String>) = if refused {
            (
                Vec::new(),
                Some(format!("Error: Session ID {id} is already in use.\n")),
            )
        } else if rejected_at_init {
            (
                format!(
                    "{}\n",
                    json!({"type": "system", "subtype": "init", "session_id": id, "tools": []})
                )
                .into_bytes(),
                None,
            )
        } else {
            let tools = cairn_session::agent_process::toolkits::CORE_VERBS;
            let mut out = format!(
                "{}\n",
                json!({"type": "system", "subtype": "init", "session_id": id, "tools": tools})
            )
            .into_bytes();
            out.extend(claude_one_shot_output(&spec));
            (out, None)
        };
        Ok(LaunchedSession {
            control: Box::new(RecordingControl {
                recorder: Arc::new(Recorder::default()),
                provider: "claude",
            }),
            events: SessionEvents::ClaudeStream {
                stdout: Box::new(std::io::Cursor::new(stdout)),
                stderr: stderr.map(|text| {
                    Box::new(std::io::Cursor::new(text.into_bytes()))
                        as Box<dyn std::io::BufRead + Send>
                }),
            },
            process: Arc::new(Mutex::new(None)),
            facts: facts(&spec, "claude", &id, host),
        })
    }
}

/// `(status, exit_reason, error_message)` once the run leaves `starting`/`live`.
async fn settled_run(
    orch: &Orchestrator,
    run_id: &str,
) -> (String, Option<String>, Option<String>) {
    let deadline = Instant::now() + WAIT;
    loop {
        let run = run_id.to_string();
        let row = orch
            .db
            .local
            .read(move |conn| {
                let run = run.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT status, exit_reason, error_message FROM runs WHERE id = ?1",
                            (run.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.expect("the run row");
                    Ok((
                        row.get::<String>(0)?,
                        row.get::<Option<String>>(1)?,
                        row.get::<Option<String>>(2)?,
                    ))
                })
            })
            .await
            .unwrap();
        if row.0 != "starting" && row.0 != "live" {
            return row;
        }
        assert!(
            Instant::now() < deadline,
            "run {run_id} never settled after its process exited: {row:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn launch_claude(orch: &Orchestrator, run_id: &str, session_id: &str) {
    seed_run(orch, run_id, session_id).await;
    let (launch_orch, launch_config) = (orch.clone(), config(run_id, session_id, "sonnet", false));
    tokio::task::spawn_blocking(move || {
        backend_for_name(Some("claude")).start_session(launch_config, &launch_orch)
    })
    .await
    .unwrap()
    .unwrap_or_else(|error| panic!("{run_id} did not launch: {error}"));
}

/// cairn/4978, the provider half. A launch rejected at `init` records why on
/// its run; a relaunch the CLI refuses because the id is taken settles on its
/// own run, promptly and typed, rather than waiting for the reaper; and a
/// fresh id launches and confirms its handle.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_claude_start_settles_on_its_own_run() {
    let recorder = Arc::new(Recorder::default());
    let (mut orch, _) = orchestrator(&recorder).await;
    let claude = Arc::new(ScriptedClaude::default());
    orch.models_runtime.claude = claude.clone();
    orch.db
        .local
        .execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
               VALUES ('ws-4978', 'Workspace', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj-4978', 'ws-4978', 'Project', 'p4978', '/tmp/p4978', 1, 1);
             INSERT INTO jobs(id, project_id, status, created_at, updated_at)
               VALUES ('job-4978', 'proj-4978', 'running', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('sess-a-rejected', 'job-4978', 'claude', 'open', 24, 1, 1),
                      ('sess-b', 'job-4978', 'claude', 'open', 25, 1, 1);",
        )
        .await
        .unwrap();

    // The first launch: the CLI creates the conversation, then its `init`
    // lacks Cairn's tools and the host rejects it.
    launch_claude(&orch, "run-1", "sess-a-rejected").await;
    let (status, exit_reason, error) = settled_run(&orch, "run-1").await;
    assert_eq!(status, "crashed");
    assert_eq!(exit_reason.as_deref(), Some("session_start_rejected"));
    assert!(error.is_some_and(|error| !error.is_empty()));

    // A retry under the same id: the CLI refuses on stderr and exits with no
    // output. Before cairn/4978 this run stayed `live` until the reaper.
    launch_claude(&orch, "run-2", "sess-a-rejected").await;
    let (status, exit_reason, error) = settled_run(&orch, "run-2").await;
    assert_eq!(status, "crashed");
    assert_eq!(exit_reason.as_deref(), Some("session_id_in_use"));
    assert!(
        error
            .as_deref()
            .is_some_and(|error| error.contains("earlier, unconfirmed launch")),
        "{error:?}"
    );

    // A fresh id launches and confirms its handle.
    launch_claude(&orch, "run-3", "sess-b").await;
    let (status, exit_reason, _) = settled_run(&orch, "run-3").await;
    assert_eq!(status, "exited");
    assert_eq!(exit_reason, None);
    assert_eq!(
        orch.db
            .local
            .query_opt_text("SELECT backend_id FROM sessions WHERE id = 'sess-b'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("sess-b")
    );
    assert_eq!(
        *claude.launched_ids.lock().unwrap(),
        ["sess-a-rejected", "sess-a-rejected", "sess-b"]
    );
}
