//! Codex CLI backend implementation.
//!
//! Communicates with `codex app-server` over stdio using JSON-RPC.
//! Cairn starts or resumes a Codex thread, starts turns against that thread,
//! and translates app-server notifications into Cairn transcript events.

use crate::agent_process::process::{ActiveProcess, BackendStdin};
use crate::agent_process::stream::{ToolUseInfo, TranscriptEvent, Usage};

use crate::diesel_models::*;
use crate::identity::{ApiProvider, CodexAuth, ProviderAuth};
use crate::models::{
    Model, ProviderCreditsSnapshot, ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow,
    RunStatus,
};
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::schema::*;
use crate::transcripts::stream_store::{
    append_chunks, finalize_stream, open_stream, process_post_commit_outbox, read_active_stream,
    ActiveMessageStream, StreamChunkInput,
};
use diesel::prelude::*;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::{
    AgentBackend, DiscoveredModel, DiscoveredReasoningEffort, ResolvedTools, SessionConfig,
};
use crate::models::{ApprovalPolicy, FilesystemScope};
pub mod app_server;
use app_server::AppServerClient;
use std::io::Result as IoResult;

const CODEX_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const MIN_CODEX_VERSION: (u32, u32, u32) = (0, 37, 0);
const CODEX_PERMISSION_TIMEOUT: Duration = Duration::from_secs(300);
const RESUME_FALLBACK_TRANSCRIPT_CHARS: usize = 24_000;
const CAIRN_CODEX_MODELS_RESOURCE: &str = ".cairn/resources/codex/codex-rs/core/models.json";

struct CodexStdin {
    client: Arc<AppServerClient>,
    thread_id: String,
    cwd: String,
    model: Option<String>,
    reasoning_effort: Option<String>,
    current_turn_id: Arc<Mutex<Option<String>>>,
}

impl CodexStdin {
    fn new(
        client: Arc<AppServerClient>,
        thread_id: String,
        cwd: String,
        model: Option<String>,
        reasoning_effort: Option<String>,
        current_turn_id: Arc<Mutex<Option<String>>>,
    ) -> Self {
        Self {
            client,
            thread_id,
            cwd,
            model,
            reasoning_effort,
            current_turn_id,
        }
    }

    fn send_turn(&self, content: &str) -> Result<(), String> {
        let input = serde_json::json!([{ "type": "text", "text": content }]);
        let mut params = serde_json::json!({
            "threadId": self.thread_id,
            "cwd": self.cwd,
            "input": input,
        });
        if let Some(ref model) = self.model {
            params["model"] = serde_json::json!(model);
        }
        if let Some(ref effort) = self.reasoning_effort {
            params["reasoningEffort"] = serde_json::json!(effort);
        }
        let result = self.client.send_request("turn/start", params)?;
        if let Some(turn_id) = result
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(|v| v.as_str())
        {
            if let Ok(mut guard) = self.current_turn_id.lock() {
                *guard = Some(turn_id.to_string());
            }
        }
        Ok(())
    }

    fn interrupt(&self) -> Result<(), String> {
        let turn_id = {
            let guard = self
                .current_turn_id
                .lock()
                .map_err(|e| format!("Lock poisoned: {}", e))?;
            guard.clone()
        };
        if let Some(turn_id) = turn_id {
            let params = serde_json::json!({
                "threadId": self.thread_id,
                "turnId": turn_id,
            });
            self.client.send_request("turn/interrupt", params)?;
        }
        Ok(())
    }
}

impl Write for CodexStdin {
    fn write(&mut self, _buf: &[u8]) -> IoResult<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "CodexStdin does not support raw writes",
        ))
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl BackendStdin for CodexStdin {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[derive(Clone)]
struct CodexOAuthTokens {
    id_token: String,
    access_token: String,
    refresh_token: Option<String>,
    chatgpt_account_id: Option<String>,
}

struct CodexAuthState {
    raw_json: String,
    tokens: CodexOAuthTokens,
}

impl CodexAuthState {
    fn new(raw_json: &str) -> Result<Self, String> {
        let tokens = parse_codex_oauth_tokens(raw_json)?;
        Ok(Self {
            raw_json: raw_json.to_string(),
            tokens,
        })
    }

    fn id_access_pair(&self) -> (String, String) {
        (
            self.tokens.id_token.clone(),
            self.tokens.access_token.clone(),
        )
    }

    fn chatgpt_account_id(&self) -> Option<String> {
        self.tokens.chatgpt_account_id.clone()
    }

    fn refresh_token(&self) -> Option<String> {
        self.tokens.refresh_token.clone()
    }

    fn apply_refresh(&mut self, new_tokens: CodexOAuthTokens) -> Result<String, String> {
        let mut value: Value = serde_json::from_str(&self.raw_json)
            .map_err(|e| format!("Failed to parse Codex auth JSON: {}", e))?;
        let tokens_obj = value
            .get_mut("tokens")
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| "Codex auth JSON missing tokens object".to_string())?;

        tokens_obj.insert(
            "id_token".into(),
            Value::String(new_tokens.id_token.clone()),
        );
        tokens_obj.insert(
            "access_token".into(),
            Value::String(new_tokens.access_token.clone()),
        );

        let refresh_to_store = new_tokens
            .refresh_token
            .clone()
            .or_else(|| self.tokens.refresh_token.clone());
        if let Some(refresh) = refresh_to_store.clone() {
            tokens_obj.insert("refresh_token".into(), Value::String(refresh));
        }

        let account_id = new_tokens
            .chatgpt_account_id
            .clone()
            .or_else(|| self.tokens.chatgpt_account_id.clone());
        self.tokens = CodexOAuthTokens {
            id_token: new_tokens.id_token,
            access_token: new_tokens.access_token,
            refresh_token: refresh_to_store,
            chatgpt_account_id: account_id,
        };

        self.raw_json = serde_json::to_string(&value)
            .map_err(|e| format!("Failed to serialize Codex auth JSON: {}", e))?;
        Ok(self.raw_json.clone())
    }
}

// ============================================================================
// Protocol types
// ============================================================================

/// MCP tool invocation details (nested in McpToolCallBegin/End).
#[derive(Debug, Deserialize)]
struct McpInvocation {
    #[serde(default)]
    server: Option<String>,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    arguments: Option<Value>,
}

/// Legacy outbound submission format (stdin → codex proto).
///
/// Kept only for fallback stdin helpers and parser tests while Cairn finishes
/// removing the last proto-era compatibility code.
#[derive(Debug, Serialize)]
struct Submission {
    id: String,
    op: Value,
}

/// Legacy inbound event envelope (codex proto → stdout).
#[derive(Debug, Deserialize)]
struct Event {
    #[allow(dead_code)]
    id: String,
    msg: EventMsg,
}

/// The tagged event message inside an Event envelope.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EventMsg {
    // Session lifecycle
    SessionConfigured {
        session_id: String,
        model: String,
        #[allow(dead_code)]
        #[serde(default)]
        reasoning_effort: Option<String>,
    },
    TaskStarted {},
    TaskComplete {},
    TurnAborted {
        #[serde(default)]
        reason: Option<String>,
    },

    // Agent output
    AgentMessage {
        message: String,
    },
    AgentMessageDelta {
        delta: String,
    },

    // Tool execution
    ExecCommandBegin {
        #[serde(default)]
        call_id: Option<String>,
        #[serde(default)]
        command: Option<Vec<String>>,
        #[serde(default)]
        cwd: Option<String>,
    },
    ExecCommandEnd {
        #[serde(default)]
        call_id: Option<String>,
        #[serde(default)]
        exit_code: Option<i32>,
        #[serde(default)]
        stdout: Option<String>,
        #[serde(default)]
        stderr: Option<String>,
    },
    PatchApplyBegin {
        #[serde(default)]
        call_id: Option<String>,
        #[serde(default)]
        patch: Option<String>,
    },
    PatchApplyEnd {
        #[serde(default)]
        call_id: Option<String>,
        #[serde(default)]
        error: Option<String>,
    },
    McpToolCallBegin {
        call_id: String,
        #[serde(default)]
        invocation: Option<McpInvocation>,
    },
    McpToolCallEnd {
        call_id: String,
        #[serde(default)]
        #[allow(dead_code)]
        invocation: Option<McpInvocation>,
        #[serde(default)]
        result: Option<Value>,
    },

    // Approval requests
    ExecApprovalRequest {
        #[serde(default)]
        call_id: Option<String>,
        #[serde(default)]
        turn_id: Option<String>,
    },
    ApplyPatchApprovalRequest {
        #[serde(default)]
        call_id: Option<String>,
    },

    // Status
    Error {
        message: String,
    },
    TokenCount {
        #[serde(default)]
        input_tokens: Option<u32>,
        #[serde(default)]
        output_tokens: Option<u32>,
    },

    // Reasoning (maps to thinking in the UI)
    AgentReasoning {
        #[serde(default)]
        #[allow(dead_code)]
        text: Option<String>,
    },
    AgentReasoningDelta {
        #[serde(default)]
        delta: Option<String>,
    },
    #[serde(alias = "agent_reasoning_section_break")]
    AgentReasoningSectionBreak {},

    // Catch-all for events we don't handle yet
    #[serde(other)]
    Unknown,
}

// ============================================================================
// Streaming state
// ============================================================================

struct StreamingState {
    stream_id: String,
    version: i32,
}

impl StreamingState {
    fn new(stream: &ActiveMessageStream) -> Self {
        Self {
            stream_id: stream.stream.id.clone(),
            version: stream.stream.version,
        }
    }
}

// ============================================================================
// CodexBackend
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexModelListResult {
    data: Vec<CodexModelListEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexModelListEntry {
    id: String,
    model: String,
    display_name: String,
    description: Option<String>,
    #[serde(default)]
    hidden: bool,
    #[serde(default)]
    is_default: bool,
    default_reasoning_effort: Option<String>,
    #[serde(default)]
    supported_reasoning_efforts: Vec<CodexReasoningEffortEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexReasoningEffortEntry {
    reasoning_effort: String,
    description: Option<String>,
}

pub struct CodexBackend;

impl AgentBackend for CodexBackend {
    fn name(&self) -> &str {
        "Codex"
    }

    fn is_available(&self) -> Result<(), String> {
        crate::env::find_binary("codex").map(|_| ())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        discover_codex_models()
    }

    fn resolve_tools(&self, agent_tools: &[String], _agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;

        // Map agent tool names to preferred versions (same canonical resolution as Claude)
        let (mut allowed, _force_disallowed) = toolkits::resolve_tools(agent_tools);

        allowed.retain(|tool| tool != "apply_patch");

        // Auto-add submission tool
        if !allowed.contains(&"mcp__cairn__return".to_string()) {
            allowed.push("mcp__cairn__return".to_string());
        }

        // Auto-add skill tool
        if !allowed.contains(&"mcp__cairn__skill".to_string()) {
            allowed.push("mcp__cairn__skill".to_string());
        }

        // Always allow Glob and Grep
        for tool in &["mcp__cairn__glob", "mcp__cairn__grep"] {
            if !allowed.contains(&tool.to_string()) {
                allowed.push(tool.to_string());
            }
        }

        // Codex ignores disallowed lists — tool access is controlled by MCP config + approval
        ResolvedTools {
            allowed,
            disallowed: vec![],
        }
    }

    fn start_session(&self, config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
        let start_time = std::time::Instant::now();
        let session_id = Some(config.session_start.session_id().to_string());

        let codex_path = crate::env::find_binary("codex").map_err(|e| {
            insert_error_event(
                orch,
                &config.run_id,
                session_id.as_deref(),
                &format!("Codex CLI not found: {}", e),
            );
            e
        })?;

        if let Err(e) = check_codex_version(&codex_path) {
            insert_error_event(orch, &config.run_id, session_id.as_deref(), &e);
            return Err(e);
        }

        log::debug!(
            "CodexBackend(app-server): codex_path={}, working_dir={}",
            codex_path,
            config.working_dir
        );
        log::info!(
            "[PROFILE] CodexBackend command resolved: {:?}",
            start_time.elapsed()
        );

        let mcp_secret = orch
            .mcp_auth
            .get_secret_for_mcp()
            .map_err(|e| format!("Failed to get MCP auth secret: {}", e))?;

        let codex_home = write_codex_config(
            &orch.mcp_binary_path,
            orch.mcp_callback_port,
            &config.mcp_config_path,
        )?;

        let identity = config
            .identity
            .as_ref()
            .cloned()
            .or_else(|| orch.get_identity());
        let codex_auth = identity.as_ref().and_then(|i| i.codex_auth.clone());
        if codex_auth.is_none() {
            insert_error_event(
                orch,
                &config.run_id,
                session_id.as_deref(),
                "Codex credentials not configured. Run `connect_codex_auth`.",
            );
            return Err("Missing Codex credentials".to_string());
        }

        let oauth_state = match codex_auth.as_ref() {
            Some(CodexAuth::OAuthToken(json)) => Some(Arc::new(Mutex::new(
                CodexAuthState::new(json).map_err(|e| {
                    insert_error_event(
                        orch,
                        &config.run_id,
                        session_id.as_deref(),
                        &format!("Invalid Codex OAuth tokens: {}", e),
                    );
                    e
                })?,
            ))),
            _ => None,
        };

        let mut env = HashMap::new();
        env.insert("CODEX_HOME".to_string(), codex_home.clone());
        env.insert("CAIRN_RUN_ID".to_string(), config.run_id.clone());
        env.insert("CAIRN_MCP_SECRET".to_string(), mcp_secret.clone());

        orch.collect_warm_if_needed();

        let client = Arc::new(
            AppServerClient::spawn(
                orch.services.process.as_ref(),
                &codex_path,
                &env,
                &config.working_dir,
            )
            .map_err(|e| {
                insert_error_event(
                    orch,
                    &config.run_id,
                    session_id.as_deref(),
                    &format!("Failed to start Codex app-server: {}", e),
                );
                e
            })?,
        );

        client.send_request(
            "initialize",
            serde_json::json!({
                "clientInfo": {
                    "name": "cairn",
                    "title": "Cairn",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true,
                }
            }),
        )?;
        client.send_notification("initialized", serde_json::json!({}))?;

        let approval_policy = match config.permissions.approval {
            ApprovalPolicy::AcceptAll => "never",
            _ => "on-request",
        };
        let sandbox_mode = codex_sandbox_mode(config.permissions.filesystem);
        let model_str = config.model.as_ref().map(|m| m.to_string());

        match (codex_auth.as_ref(), oauth_state.as_ref()) {
            (Some(CodexAuth::OAuthToken(_)), Some(state)) => {
                let guard = state
                    .lock()
                    .map_err(|_| "Codex auth state lock poisoned".to_string())?;
                let (id_token, access_token) = guard.id_access_pair();
                let account_id = guard.chatgpt_account_id();
                drop(guard);
                let mut login_params = serde_json::json!({
                    "type": "chatgptAuthTokens",
                    "idToken": id_token,
                    "accessToken": access_token,
                });
                if let Some(ref acct_id) = account_id {
                    login_params["chatgptAccountId"] = serde_json::json!(acct_id);
                }
                let _ = client.send_request("account/login/start", login_params)?;
            }
            (Some(CodexAuth::ApiKey(key)), _) => {
                let _ = client.send_request(
                    "account/login/start",
                    serde_json::json!({
                        "type": "apiKey",
                        "apiKey": key,
                    }),
                )?;
            }
            _ => {}
        }

        let mut prompt_text = config.prompt.clone();
        let thread_resp = match &config.session_start {
            crate::backends::SessionStart::Resume { backend_id, session_id } => {
                log::info!(
                    "Codex session start: mode=resume cairn_session_id={} source_backend_id={}",
                    session_id,
                    backend_id
                );
                let resume_params = build_thread_resume_params(
                    backend_id,
                    &config.working_dir,
                    approval_policy,
                    sandbox_mode,
                    model_str.as_deref(),
                    config.reasoning_effort.as_deref(),
                );
                match client.send_request("thread/resume", resume_params) {
                    Ok(resp) => {
                        log::info!(
                            "Codex session start dispatched thread/resume for cairn_session_id={} source_backend_id={}",
                            session_id,
                            backend_id
                        );
                        resp
                    }
                    Err(err) if is_missing_rollout_error(&err, backend_id) => {
                        log::warn!(
                            "Codex thread/resume failed for stale thread {}, starting fresh thread with transcript preload",
                            backend_id
                        );
                        let cairn_sid = session_id.as_str();
                        if let Some(preloaded_prompt) =
                            build_resume_fallback_prompt(orch, cairn_sid, &config.prompt)
                        {
                            prompt_text = preloaded_prompt;
                        }
                        let resp = client.send_request(
                            "thread/start",
                            build_thread_start_params(
                                &config.working_dir,
                                approval_policy,
                                sandbox_mode,
                                model_str.as_deref(),
                                config.reasoning_effort.as_deref(),
                            ),
                        )?;
                        log::info!(
                            "Codex session start fell back to thread/start for cairn_session_id={} after stale resume source_backend_id={}",
                            session_id,
                            backend_id
                        );
                        resp
                    }
                    Err(err) => return Err(err),
                }
            }
            crate::backends::SessionStart::Fork {
                source_backend_id,
                session_id,
            } => {
                log::info!(
                    "Codex session start: mode=fork cairn_session_id={} source_backend_id={}",
                    session_id,
                    source_backend_id
                );
                let resp = client.send_request(
                    "thread/fork",
                    build_thread_fork_params(
                        source_backend_id,
                        &config.working_dir,
                        approval_policy,
                        sandbox_mode,
                        model_str.as_deref(),
                        config.reasoning_effort.as_deref(),
                    ),
                )?;
                log::info!(
                    "Codex session start dispatched thread/fork for cairn_session_id={} source_backend_id={}",
                    session_id,
                    source_backend_id
                );
                resp
            }
            crate::backends::SessionStart::New { session_id } => {
                log::info!(
                    "Codex session start: mode=new cairn_session_id={}",
                    session_id
                );
                let resp = client.send_request(
                    "thread/start",
                    build_thread_start_params(
                        &config.working_dir,
                        approval_policy,
                        sandbox_mode,
                        model_str.as_deref(),
                        config.reasoning_effort.as_deref(),
                    ),
                )?;
                log::info!(
                    "Codex session start dispatched thread/start for cairn_session_id={}",
                    session_id
                );
                resp
            }
        };

        let thread_id = thread_resp
            .get("thread")
            .and_then(|t| t.get("id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| "thread/start response missing thread id".to_string())?
            .to_string();
        log::info!(
            "Codex session start received thread_id={} for cairn_session_id={}",
            thread_id,
            session_id.as_deref().unwrap_or("<none>")
        );

        // Store the Codex thread_id as the session's backend resume handle
        if let Some(ref sid) = session_id {
            if let Ok(mut conn) = orch.db.conn.lock() {
                let _ = crate::sessions::queries::set_backend_id(&mut conn, sid, &thread_id);
            }
        }

        let full_prompt = build_prompt(&prompt_text, config.system_prompt_content.as_deref());
        let mut turn_params = serde_json::json!({
            "threadId": thread_id.clone(),
            "cwd": config.working_dir,
            "input": [{ "type": "text", "text": full_prompt }]
        });
        if let Some(ref model) = model_str {
            turn_params["model"] = serde_json::json!(model);
        }
        let turn_resp = client.send_request("turn/start", turn_params)?;

        let current_turn_id = Arc::new(Mutex::new(
            turn_resp
                .get("turn")
                .and_then(|t| t.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        ));

        // Transition run to Running AFTER successful spawn (sets started_at accurately)
        {
            let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
            if let Err(e) = crate::transitions::transition_run(
                &mut conn,
                &config.run_id,
                crate::models::RunStatus::Live,
                &*orch.services.emitter,
            ) {
                log::warn!("Failed to transition codex run to running: {}", e);
            }
            // Job is already Running from start_job's transition_job call — no write needed
        }

        let child_arc = client.child_handle();
        let stdin_arc: Arc<Mutex<Option<Box<dyn BackendStdin>>>> =
            Arc::new(Mutex::new(Some(Box::new(CodexStdin::new(
                client.clone(),
                thread_id.clone(),
                config.working_dir.clone(),
                model_str,
                config.reasoning_effort.clone(),
                current_turn_id.clone(),
            )) as Box<dyn BackendStdin>)));

        let process_job_id: Option<String> = {
            let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
            runs::table
                .find(&config.run_id)
                .select(runs::job_id)
                .first::<Option<String>>(&mut *conn)
                .ok()
                .flatten()
        };

        {
            let mut processes = orch
                .process_state
                .processes
                .lock()
                .map_err(|e| e.to_string())?;
            let mut active_process = ActiveProcess::new(
                child_arc.clone(),
                stdin_arc.clone(),
                session_id.clone(),
                process_job_id,
            );
            active_process.backend = Some("codex".to_string());
            processes.register(config.run_id.clone(), active_process);
        }

        let notification_rx = client.notifications();
        let run_id = config.run_id.clone();
        let orch_clone = orch.clone();
        let emitter = orch.services.emitter.clone();
        // Use the Cairn UUID for event storage, not the Codex thread_id
        let event_session_id = session_id;
        thread::spawn(move || {
            Self::reader_thread_app_server(
                &orch_clone,
                &emitter,
                &run_id,
                event_session_id,
                notification_rx,
                client,
                current_turn_id,
                oauth_state,
            );
        });

        log::info!(
            "[PROFILE] CodexBackend(app-server)::start_session returning: {:?}",
            start_time.elapsed()
        );
        Ok(())
    }

    fn supports_resume(&self) -> bool {
        true // app-server can resume persisted threads via thread/resume
    }

    fn supports_warm_processes(&self) -> bool {
        true // app-server stays alive after turn completion and accepts more user_input
    }

    fn send_user_message(
        &self,
        stdin: &mut dyn BackendStdin,
        content: &str,
        _session_id: &str,
        _parent_tool_use_id: Option<&str>,
        _working_dir: Option<&str>,
    ) -> Result<(), String> {
        if let Some(app_stdin) = stdin.as_any_mut().downcast_mut::<CodexStdin>() {
            app_stdin.send_turn(content)
        } else {
            Self::send_legacy_user_input(stdin, content)
        }
    }

    fn send_interrupt(&self, stdin: &mut dyn BackendStdin) -> Result<(), String> {
        if let Some(app_stdin) = stdin.as_any_mut().downcast_mut::<CodexStdin>() {
            app_stdin.interrupt()
        } else {
            let msg = serde_json::json!({
                "id": Uuid::new_v4().to_string(),
                "op": {"type": "interrupt"}
            });
            writeln!(stdin, "{}", msg).map_err(|e| format!("stdin write: {}", e))?;
            stdin.flush().map_err(|e| format!("stdin flush: {}", e))?;
            Ok(())
        }
    }

    fn send_set_model(&self, stdin: &mut dyn BackendStdin, model: &str) -> Result<(), String> {
        if stdin.as_any_mut().downcast_mut::<CodexStdin>().is_some() {
            Err("Changing Codex model mid-turn not yet supported".to_string())
        } else {
            let msg = serde_json::json!({
                "id": Uuid::new_v4().to_string(),
                "op": {
                    "type": "override_turn_context",
                    "model": model
                }
            });
            writeln!(stdin, "{}", msg).map_err(|e| format!("stdin write: {}", e))?;
            stdin.flush().map_err(|e| format!("stdin flush: {}", e))?;
            Ok(())
        }
    }

    fn send_set_permission_mode(
        &self,
        stdin: &mut dyn BackendStdin,
        mode: &str,
    ) -> Result<(), String> {
        if stdin.as_any_mut().downcast_mut::<CodexStdin>().is_some() {
            Err("Changing Codex permission mode mid-session is not yet supported".to_string())
        } else {
            let codex_policy = codex_approval_policy_for_mode(Some(mode));
            let msg = serde_json::json!({
                "id": Uuid::new_v4().to_string(),
                "op": {
                    "type": "override_turn_context",
                    "approval_policy": codex_policy
                }
            });
            writeln!(stdin, "{}", msg).map_err(|e| format!("stdin write: {}", e))?;
            stdin.flush().map_err(|e| format!("stdin flush: {}", e))?;
            Ok(())
        }
    }
}

fn discover_codex_models() -> Result<Vec<DiscoveredModel>, String> {
    let codex_path = crate::env::find_binary("codex")?;
    let mut child = Command::new(&codex_path)
        .arg("app-server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to start Codex app-server: {}", e))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "Failed to capture Codex app-server stdin".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture Codex app-server stdout".to_string())?;
    let mut reader = BufReader::new(stdout);

    for message in [
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "cairn",
                    "title": "Cairn",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true,
                }
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "method": "initialized",
            "params": {}
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "model/list",
            "params": {}
        }),
    ] {
        writeln!(stdin, "{}", message)
            .map_err(|e| format!("Failed to write to Codex app-server stdin: {}", e))?;
    }
    stdin
        .flush()
        .map_err(|e| format!("Failed to flush Codex app-server stdin: {}", e))?;

    let mut line = String::new();
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|e| format!("Failed reading Codex app-server output: {}", e))?;
        if read == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line.trim())
            .map_err(|e| format!("Failed to parse Codex model/list response: {}", e))?;
        if value.get("id").and_then(Value::as_i64) != Some(2) {
            continue;
        }
        let result: CodexModelListResult = serde_json::from_value(
            value
                .get("result")
                .cloned()
                .ok_or_else(|| "Codex model/list response missing result".to_string())?,
        )
        .map_err(|e| format!("Failed to decode Codex model/list result: {}", e))?;
        let _ = child.kill();
        let _ = child.wait();
        return Ok(result
            .data
            .into_iter()
            .map(|model| DiscoveredModel {
                id: model.id,
                display_name: model.model.clone(),
                model: model.model,
                description: model.description,
                hidden: model.hidden,
                is_default: model.is_default,
                default_reasoning_effort: model.default_reasoning_effort,
                supported_reasoning_efforts: model
                    .supported_reasoning_efforts
                    .into_iter()
                    .map(|effort| DiscoveredReasoningEffort {
                        reasoning_effort: effort.reasoning_effort,
                        description: effort.description,
                    })
                    .collect(),
            })
            .collect());
    }

    let stderr = child
        .stderr
        .take()
        .and_then(|mut stderr| {
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut stderr, &mut buf).ok()?;
            Some(buf)
        })
        .unwrap_or_default();
    let _ = child.kill();
    let _ = child.wait();
    Err(if stderr.trim().is_empty() {
        "Codex app-server exited before returning model/list".to_string()
    } else {
        format!(
            "Codex app-server exited before returning model/list: {}",
            stderr.trim()
        )
    })
}

impl CodexBackend {
    fn send_legacy_user_input(stdin: &mut dyn BackendStdin, content: &str) -> Result<(), String> {
        let msg = serde_json::json!({
            "id": Uuid::new_v4().to_string(),
            "op": {
                "type": "user_input",
                "items": [{"type": "text", "text": content}]
            }
        });
        writeln!(stdin, "{}", msg).map_err(|e| format!("stdin write: {}", e))?;
        stdin.flush().map_err(|e| format!("stdin flush: {}", e))?;
        Ok(())
    }

    /// Send a legacy Submission op via stdin.
    fn send_op(
        stdin: &Arc<Mutex<Option<Box<dyn BackendStdin>>>>,
        op: Value,
    ) -> Result<String, String> {
        let id = Uuid::new_v4().to_string();
        let sub = Submission { id: id.clone(), op };
        let line = serde_json::to_string(&sub).map_err(|e| e.to_string())?;
        let mut guard = stdin.lock().map_err(|e| e.to_string())?;
        if let Some(ref mut writer) = *guard {
            writeln!(writer, "{}", line).map_err(|e| format!("stdin write: {}", e))?;
            writer.flush().map_err(|e| format!("stdin flush: {}", e))?;
        } else {
            return Err("stdin not available".to_string());
        }
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    fn reader_thread(
        orch: &Orchestrator,
        emitter: &Arc<dyn crate::services::EventEmitter>,
        run_id: &str,
        session_id: Option<String>,
        stdout: Box<dyn BufRead + Send>,
        stdin: Arc<Mutex<Option<Box<dyn BackendStdin>>>>,
        prompt: &str,
        system_prompt: Option<&str>,
    ) {
        log::debug!("codex_reader: started");
        let mut sequence: i32 = 0;
        let mut streaming_state: Option<StreamingState> = None;
        let mut prompt_sent = false;
        let mut pending_usage: Option<Usage> = None;

        for line_result in stdout.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    log::error!("codex_reader: read error: {}", e);
                    break;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            log::info!("codex_reader: raw: {}", &line[..line.len().min(300)]);

            let event: Event = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(e) => {
                    log::warn!(
                        "codex_reader: parse error: {} - {}",
                        e,
                        &line[..line.len().min(200)]
                    );
                    continue;
                }
            };

            match event.msg {
                EventMsg::SessionConfigured {
                    session_id: ref codex_session_id,
                    ref model,
                    ..
                } => {
                    log::debug!(
                        "codex_reader: session configured, model={}, sid={}",
                        model,
                        codex_session_id
                    );

                    // Update session_id in process state
                    if let Ok(mut processes) = orch.process_state.processes.lock() {
                        if let Some(proc) = processes.get_mut(run_id) {
                            proc.session_id = Some(codex_session_id.clone());
                        }
                    }

                    // Emit system:init event
                    let init_event = TranscriptEvent {
                        event_type: "system:init".to_string(),
                        session_id: session_id.clone(),
                        parent_tool_use_id: None,
                        content: Some(format!("Codex session started (model: {})", model)),
                        thinking: None,
                        tool_name: None,
                        tool_input: None,
                        tool_uses: None,
                        tool_use_id: None,
                        tool_result: None,
                        is_error: false,
                        usage: None,
                        raw: None,
                    };
                    store_event(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &init_event,
                    );
                    sequence += 1;

                    // Send the user prompt now that session is configured
                    if !prompt_sent {
                        let full_prompt = build_prompt(prompt, system_prompt);
                        let op = serde_json::json!({
                            "type": "user_input",
                            "items": [{"type": "text", "text": full_prompt}]
                        });
                        if let Err(e) = Self::send_op(&stdin, op) {
                            log::error!("codex_reader: failed to send prompt: {}", e);
                            insert_error_event(
                                orch,
                                run_id,
                                session_id.as_deref(),
                                &format!("Failed to send prompt: {}", e),
                            );
                            crate::orchestrator::lifecycle::finalize_run(
                                orch,
                                run_id,
                                RunStatus::Crashed,
                            );
                            return;
                        }
                        prompt_sent = true;
                    }
                }

                EventMsg::TaskStarted {} => {
                    log::debug!("codex_reader: task started");
                }

                EventMsg::TaskComplete { .. } => {
                    finalize_streaming(
                        orch,
                        emitter,
                        &mut streaming_state,
                        session_id.as_deref(),
                        &mut sequence,
                    );

                    // Emit result:success
                    let result_event = TranscriptEvent {
                        event_type: "result:success".to_string(),
                        session_id: session_id.clone(),
                        parent_tool_use_id: None,
                        content: None,
                        thinking: None,
                        tool_name: None,
                        tool_input: None,
                        tool_uses: None,
                        tool_use_id: None,
                        tool_result: None,
                        is_error: false,
                        usage: pending_usage.take(),
                        raw: None,
                    };
                    store_event(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &result_event,
                    );
                    sequence += 1;

                    let is_task = is_task_spawned_run(orch, run_id);
                    if is_task {
                        crate::orchestrator::lifecycle::finalize_run(
                            orch,
                            run_id,
                            RunStatus::Exited,
                        );
                        orch.process_state.transition_to_warm(run_id);
                    } else {
                        crate::orchestrator::lifecycle::transition_to_warm_state(orch, run_id);
                        let _ = emitter.emit(
                            "run-turn-completed",
                            serde_json::json!({
                                "runId": run_id,
                                "run_id": run_id,
                                "isWarm": true,
                                "is_warm": true
                            }),
                        );
                    }
                    log::info!(
                        "codex turn completed for run {}",
                        &run_id[..run_id.len().min(8)]
                    );
                }

                EventMsg::TurnAborted { reason } => {
                    finalize_streaming(
                        orch,
                        emitter,
                        &mut streaming_state,
                        session_id.as_deref(),
                        &mut sequence,
                    );
                    let reason_str = reason.as_deref().unwrap_or("unknown");
                    pending_usage = None;
                    match reason_str {
                        "interrupted" | "replaced" | "review_ended" => {
                            log::info!("codex turn aborted ({}), handling interrupt", reason_str);
                            handle_codex_interrupted_turn(orch, emitter, run_id);
                        }
                        _ => {
                            // Unexpected abort — treat as error
                            insert_error_event(
                                orch,
                                run_id,
                                session_id.as_deref(),
                                &format!("Turn aborted: {}", reason_str),
                            );
                            crate::orchestrator::lifecycle::finalize_run(
                                orch,
                                run_id,
                                RunStatus::Crashed,
                            );
                        }
                    }
                }

                EventMsg::AgentMessage { message } => {
                    if streaming_state.is_some() {
                        finalize_streaming_with_event(
                            orch,
                            emitter,
                            &mut streaming_state,
                            Some(TranscriptEvent {
                                event_type: "assistant".to_string(),
                                session_id: session_id.clone(),
                                parent_tool_use_id: None,
                                content: Some(message.clone()),
                                thinking: None,
                                tool_name: None,
                                tool_input: None,
                                tool_uses: None,
                                tool_use_id: None,
                                tool_result: None,
                                is_error: false,
                                usage: None,
                                raw: None,
                            }),
                            &mut sequence,
                        );
                    } else {
                        // No streaming - create event directly
                        let event = TranscriptEvent {
                            event_type: "assistant".to_string(),
                            session_id: session_id.clone(),
                            parent_tool_use_id: None,
                            content: Some(message),
                            thinking: None,
                            tool_name: None,
                            tool_input: None,
                            tool_uses: None,
                            tool_use_id: None,
                            tool_result: None,
                            is_error: false,
                            usage: None,
                            raw: None,
                        };
                        store_event(
                            orch,
                            emitter,
                            run_id,
                            session_id.as_deref(),
                            sequence,
                            &event,
                        );
                        sequence += 1;
                    }
                }

                EventMsg::AgentMessageDelta { ref delta } => {
                    handle_agent_message_delta(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        &mut streaming_state,
                        &mut sequence,
                        delta,
                    );
                }

                EventMsg::ExecCommandBegin {
                    call_id: item_id,
                    command,
                    cwd,
                } => {
                    finalize_streaming(
                        orch,
                        emitter,
                        &mut streaming_state,
                        session_id.as_deref(),
                        &mut sequence,
                    );
                    let cmd_str = command.as_ref().map(|c| c.join(" ")).unwrap_or_default();
                    let tool_input = serde_json::json!({
                        "command": cmd_str,
                        "description": format!("Run in {}", cwd.as_deref().unwrap_or(".")),
                    });
                    let event = TranscriptEvent {
                        event_type: "assistant".to_string(),
                        session_id: session_id.clone(),
                        parent_tool_use_id: None,
                        content: None,
                        thinking: None,
                        tool_name: None,
                        tool_input: None,
                        tool_uses: Some(vec![ToolUseInfo {
                            id: item_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                            name: "bash".to_string(),
                            input: tool_input,
                        }]),
                        tool_use_id: None,
                        tool_result: None,
                        is_error: false,
                        usage: None,
                        raw: None,
                    };
                    store_event(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &event,
                    );
                    sequence += 1;
                }

                EventMsg::ExecCommandEnd {
                    call_id: item_id,
                    exit_code,
                    stdout: cmd_stdout,
                    stderr: cmd_stderr,
                } => {
                    let result_text = if let Some(out) = cmd_stdout.filter(|s| !s.is_empty()) {
                        out
                    } else if let Some(err) = cmd_stderr.filter(|s| !s.is_empty()) {
                        format!("stderr: {}", err)
                    } else if let Some(code) = exit_code {
                        if code == 0 {
                            "Command completed successfully".to_string()
                        } else {
                            format!("Exit code {}", code)
                        }
                    } else {
                        "Completed".to_string()
                    };
                    let event = TranscriptEvent {
                        event_type: "tool_result".to_string(),
                        session_id: session_id.clone(),
                        parent_tool_use_id: None,
                        content: None,
                        thinking: None,
                        tool_name: None,
                        tool_input: None,
                        tool_uses: None,
                        tool_use_id: item_id,
                        tool_result: Some(result_text),
                        is_error: exit_code.is_some_and(|c| c != 0),
                        usage: None,
                        raw: None,
                    };
                    store_event(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &event,
                    );
                    sequence += 1;
                }

                EventMsg::PatchApplyBegin {
                    call_id: item_id,
                    patch,
                } => {
                    finalize_streaming(
                        orch,
                        emitter,
                        &mut streaming_state,
                        session_id.as_deref(),
                        &mut sequence,
                    );
                    let event = TranscriptEvent {
                        event_type: "assistant".to_string(),
                        session_id: session_id.clone(),
                        parent_tool_use_id: None,
                        content: None,
                        thinking: None,
                        tool_name: None,
                        tool_input: None,
                        tool_uses: Some(vec![ToolUseInfo {
                            id: item_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                            name: "edit".to_string(),
                            input: serde_json::json!({"patch": patch}),
                        }]),
                        tool_use_id: None,
                        tool_result: None,
                        is_error: false,
                        usage: None,
                        raw: None,
                    };
                    store_event(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &event,
                    );
                    sequence += 1;
                }

                EventMsg::PatchApplyEnd {
                    call_id: item_id,
                    error,
                } => {
                    let (result_text, is_err) = if let Some(ref err) = error {
                        (format!("Error: {}", err), true)
                    } else {
                        ("Patch applied".to_string(), false)
                    };
                    let event = TranscriptEvent {
                        event_type: "tool_result".to_string(),
                        session_id: session_id.clone(),
                        parent_tool_use_id: None,
                        content: None,
                        thinking: None,
                        tool_name: None,
                        tool_input: None,
                        tool_uses: None,
                        tool_use_id: item_id,
                        tool_result: Some(result_text),
                        is_error: is_err,
                        usage: None,
                        raw: None,
                    };
                    store_event(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &event,
                    );
                    sequence += 1;
                }

                EventMsg::McpToolCallBegin {
                    ref call_id,
                    ref invocation,
                } => {
                    finalize_streaming(
                        orch,
                        emitter,
                        &mut streaming_state,
                        session_id.as_deref(),
                        &mut sequence,
                    );
                    let (server, tool_name, arguments) = match invocation {
                        Some(inv) => (
                            inv.server.clone().unwrap_or_default(),
                            inv.tool.clone().unwrap_or_default(),
                            inv.arguments.clone().unwrap_or(Value::Null),
                        ),
                        None => (String::new(), String::new(), Value::Null),
                    };
                    let full_name = if server.is_empty() {
                        tool_name
                    } else {
                        format!("mcp__{}__{}", server, tool_name)
                    };
                    let event = TranscriptEvent {
                        event_type: "assistant".to_string(),
                        session_id: session_id.clone(),
                        parent_tool_use_id: None,
                        content: None,
                        thinking: None,
                        tool_name: None,
                        tool_input: None,
                        tool_uses: Some(vec![ToolUseInfo {
                            id: call_id.clone(),
                            name: full_name,
                            input: arguments,
                        }]),
                        tool_use_id: None,
                        tool_result: None,
                        is_error: false,
                        usage: None,
                        raw: None,
                    };
                    store_event(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &event,
                    );
                    sequence += 1;
                }

                EventMsg::McpToolCallEnd {
                    ref call_id,
                    result: ref mcp_result,
                    ..
                } => {
                    // Extract text content from CallToolResult for display,
                    // keep full structure in raw.
                    let (result_text, is_err, raw_result) = extract_mcp_result(mcp_result);
                    let event = TranscriptEvent {
                        event_type: "tool_result".to_string(),
                        session_id: session_id.clone(),
                        parent_tool_use_id: None,
                        content: None,
                        thinking: None,
                        tool_name: None,
                        tool_input: None,
                        tool_uses: None,
                        tool_use_id: Some(call_id.clone()),
                        tool_result: Some(result_text),
                        is_error: is_err,
                        usage: None,
                        raw: raw_result,
                    };
                    store_event(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &event,
                    );
                    sequence += 1;
                }

                EventMsg::AgentReasoningDelta { delta } => {
                    if let Some(ref text) = delta {
                        handle_reasoning_delta(
                            orch,
                            emitter,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            sequence,
                            text,
                        );
                    }
                }

                EventMsg::AgentReasoning { .. } | EventMsg::AgentReasoningSectionBreak {} => {
                    // Full reasoning text — already accumulated via deltas, ignore
                }

                EventMsg::ExecApprovalRequest {
                    call_id: item_id,
                    turn_id,
                    ..
                } => {
                    log::debug!("codex_reader: auto-accepting exec approval");
                    let _ = Self::send_op(
                        &stdin,
                        serde_json::json!({
                            "type": "exec_approval",
                            "id": item_id,
                            "turn_id": turn_id,
                            "decision": "accept"
                        }),
                    );
                }

                EventMsg::ApplyPatchApprovalRequest {
                    call_id: item_id, ..
                } => {
                    log::debug!("codex_reader: declining native patch approval");
                    let _ = Self::send_op(
                        &stdin,
                        serde_json::json!({
                            "type": "patch_approval",
                            "id": item_id,
                            "decision": "decline",
                            "reason": native_edit_decline_message()
                        }),
                    );
                }

                EventMsg::Error { message } => {
                    log::error!("codex_reader: error event: {}", message);
                    insert_error_event(orch, run_id, session_id.as_deref(), &message);
                }

                EventMsg::TokenCount {
                    input_tokens,
                    output_tokens,
                } => {
                    log::debug!(
                        "codex_reader: tokens in={:?} out={:?}",
                        input_tokens,
                        output_tokens
                    );
                    pending_usage = Some(Usage {
                        input_tokens: input_tokens.unwrap_or(0),
                        output_tokens: output_tokens.unwrap_or(0),
                        cache_creation_input_tokens: None,
                        cache_read_input_tokens: None,
                    });
                }

                EventMsg::Unknown => {
                    // Forward compat — ignore unknown event types
                }
            }
        }

        // EOF: process terminated
        finalize_streaming(
            orch,
            emitter,
            &mut streaming_state,
            session_id.as_deref(),
            &mut sequence,
        );

        // Check if already finalized (e.g., by TurnComplete)
        if let Ok(mut conn) = orch.db.conn.lock() {
            let status: Option<Option<String>> = runs::table
                .find(run_id)
                .select(runs::status)
                .first(&mut *conn)
                .ok();
            if status.flatten() == Some("running".to_string()) {
                log::warn!(
                    "Codex process {} terminated without completing",
                    &run_id[..run_id.len().min(8)]
                );
                drop(conn);
                insert_error_event(
                    orch,
                    run_id,
                    session_id.as_deref(),
                    "Codex process terminated unexpectedly",
                );
                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Crashed);
            }
        }

        if let Ok(mut processes) = orch.process_state.processes.lock() {
            processes.remove(run_id);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn reader_thread_app_server(
        orch: &Orchestrator,
        emitter: &Arc<dyn crate::services::EventEmitter>,
        run_id: &str,
        session_id: Option<String>,
        notifications: crossbeam_channel::Receiver<Value>,
        client: Arc<AppServerClient>,
        current_turn_id: Arc<Mutex<Option<String>>>,
        oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
    ) {
        log::debug!("codex_app_server: reader started");
        let mut sequence: i32 = 0;
        let mut streaming_state: Option<StreamingState> = None;
        let mut pending_usage: Option<Usage> = None;
        let mut last_direct_assistant_text: Option<String> = None;

        let init_event = TranscriptEvent {
            event_type: "system:init".to_string(),
            session_id: session_id.clone(),
            parent_tool_use_id: None,
            content: Some("Codex session started".to_string()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            usage: None,
            raw: None,
        };
        store_event(
            orch,
            emitter,
            run_id,
            session_id.as_deref(),
            sequence,
            &init_event,
        );
        sequence += 1;

        for msg in notifications.iter() {
            let method = msg.get("method").and_then(|v| v.as_str());
            let expects_response = msg.get("id").is_some()
                && msg.get("result").is_none()
                && msg.get("error").is_none()
                && method.is_some();
            if expects_response {
                if let Some(id_value) = msg.get("id") {
                    let response = match method {
                        Some("item/commandExecution/requestApproval") => {
                            handle_codex_approval_request(
                                orch,
                                run_id,
                                "codex/command_execution",
                                msg.get("params").cloned().unwrap_or(Value::Null),
                                client.as_ref(),
                                id_value,
                                true,
                            )
                        }
                        Some("item/fileChange/requestApproval") => {
                            decline_codex_native_file_change(client.as_ref(), id_value)
                        }
                        Some("item/fileRead/requestApproval") => handle_codex_approval_request(
                            orch,
                            run_id,
                            "codex/file_read",
                            msg.get("params").cloned().unwrap_or(Value::Null),
                            client.as_ref(),
                            id_value,
                            false,
                        ),
                        Some("item/mcpToolCall/requestApproval") => handle_codex_approval_request(
                            orch,
                            run_id,
                            "codex/mcp_tool_call",
                            msg.get("params").cloned().unwrap_or(Value::Null),
                            client.as_ref(),
                            id_value,
                            false,
                        ),
                        Some("item/permissions/requestApproval") => handle_codex_approval_request(
                            orch,
                            run_id,
                            "codex/permissions",
                            msg.get("params").cloned().unwrap_or(Value::Null),
                            client.as_ref(),
                            id_value,
                            false,
                        ),
                        Some("account/chatgptAuthTokens/refresh") => {
                            if let Some(state_arc) = oauth_state.as_ref() {
                                let refresh_token = state_arc
                                    .lock()
                                    .ok()
                                    .and_then(|state| state.refresh_token());
                                match refresh_token {
                                    Some(rt) => match refresh_codex_tokens_via_http(&rt) {
                                        Ok(new_tokens) => {
                                            let updated_json = {
                                                match state_arc.lock() {
                                                    Ok(mut guard) => guard.apply_refresh(new_tokens.clone()),
                                                    Err(_) => Err(
                                                        "Codex auth state lock poisoned".to_string(),
                                                    ),
                                                }
                                            };
                                            match updated_json {
                                                Ok(json_str) => {
                                                    persist_codex_oauth_tokens(orch, &json_str);
                                                    client.respond(
                                                        id_value,
                                                        serde_json::json!({
                                                            "idToken": new_tokens.id_token,
                                                            "accessToken": new_tokens.access_token,
                                                        }),
                                                    )
                                                }
                                                Err(err) => client.respond_error(
                                                    id_value,
                                                    -32000,
                                                    &format!("Failed to update Codex tokens: {}", err),
                                                ),
                                            }
                                        }
                                        Err(err) => client.respond_error(
                                            id_value,
                                            -32000,
                                            &format!(
                                                "Codex token refresh failed: {}. Please rerun connect_codex_auth.",
                                                err
                                            ),
                                        ),
                                    },
                                    None => client.respond_error(
                                        id_value,
                                        -32000,
                                        "Codex refresh token unavailable; run connect_codex_auth",
                                    ),
                                }
                            } else {
                                client.respond_error(
                                    id_value,
                                    -32000,
                                    "Codex OAuth tokens unavailable; run connect_codex_auth",
                                )
                            }
                        }
                        Some("item/tool/call") => {
                            log::debug!("Codex requested dynamic tool call — declining");
                            client.respond_error(
                                id_value,
                                -32601,
                                "Dynamic tool calls are not supported",
                            )
                        }
                        Some("item/tool/requestUserInput") | Some("tool/requestUserInput") => {
                            log::debug!("Codex requested interactive input — declining");
                            client.respond_error(
                                id_value,
                                -32601,
                                "Interactive prompts are not supported",
                            )
                        }
                        Some(other) => {
                            log::warn!("Unhandled Codex server request: {}", other);
                            client.respond_error(id_value, -32601, "Unsupported request")
                        }
                        None => Ok(()),
                    };
                    if let Err(e) = response {
                        log::warn!("Failed to answer Codex request {:?}: {}", method, e);
                    }
                }
                continue;
            }

            match method {
                Some("turn/started") => {
                    if let Some(turn_id) = msg.pointer("/params/turn/id").and_then(|v| v.as_str()) {
                        if let Ok(mut guard) = current_turn_id.lock() {
                            *guard = Some(turn_id.to_string());
                        }
                    }
                    pending_usage = None;
                }
                Some("item/agentMessage/delta") => {
                    if let Some(delta) = extract_app_server_delta(&msg) {
                        handle_agent_message_delta(
                            orch,
                            emitter,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            &mut sequence,
                            delta,
                        );
                    }
                }
                Some("item/reasoning/textDelta") | Some("item/reasoning/summaryTextDelta") => {
                    if let Some(text) = extract_app_server_delta(&msg) {
                        handle_reasoning_delta(
                            orch,
                            emitter,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            sequence,
                            text,
                        );
                    }
                }
                Some("item/started") => {
                    if let Some(item_type) =
                        msg.pointer("/params/item/type").and_then(|v| v.as_str())
                    {
                        match item_type {
                            "commandExecution" => {
                                finalize_streaming(
                                    orch,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                    &mut sequence,
                                );
                                let tool_use_id = msg
                                    .pointer("/params/item/id")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                let (command_display, command_vec) = extract_command_execution(
                                    msg.pointer("/params/item/command").unwrap_or(&Value::Null),
                                );
                                let cwd = msg
                                    .pointer("/params/item/cwd")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or(".");
                                let tool_input = serde_json::json!({
                                    "command": command_display,
                                    "commandArgs": command_vec,
                                    "cwd": cwd,
                                });
                                let event = TranscriptEvent {
                                    event_type: "assistant".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: Some(vec![ToolUseInfo {
                                        id: tool_use_id,
                                        name: "bash".to_string(),
                                        input: tool_input,
                                    }]),
                                    tool_use_id: None,
                                    tool_result: None,
                                    is_error: false,
                                    usage: None,
                                    raw: None,
                                };
                                store_event(
                                    orch,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                sequence += 1;
                            }
                            "fileChange" => {
                                finalize_streaming(
                                    orch,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                    &mut sequence,
                                );
                                let tool_use_id = msg
                                    .pointer("/params/item/id")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                let mut tool_input = serde_json::json!({});
                                if let Value::Object(ref mut map) = tool_input {
                                    if let Some(changes) =
                                        msg.pointer("/params/item/changes").cloned()
                                    {
                                        map.insert("changes".into(), changes);
                                    }
                                    if let Some(summary) =
                                        msg.pointer("/params/item/summary").cloned()
                                    {
                                        map.insert("summary".into(), summary);
                                    }
                                }
                                let event = TranscriptEvent {
                                    event_type: "assistant".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: Some(vec![ToolUseInfo {
                                        id: tool_use_id,
                                        name: "edit".to_string(),
                                        input: tool_input,
                                    }]),
                                    tool_use_id: None,
                                    tool_result: None,
                                    is_error: false,
                                    usage: None,
                                    raw: None,
                                };
                                store_event(
                                    orch,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                sequence += 1;
                            }
                            "mcpToolCall" => {
                                finalize_streaming(
                                    orch,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                    &mut sequence,
                                );
                                let tool_use_id = msg
                                    .pointer("/params/item/id")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                let server = msg
                                    .pointer("/params/item/server")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let tool_name = msg
                                    .pointer("/params/item/tool")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let arguments = msg
                                    .pointer("/params/item/arguments")
                                    .cloned()
                                    .unwrap_or(Value::Null);
                                let full_name = if server.is_empty() {
                                    tool_name.to_string()
                                } else {
                                    format!("mcp__{}__{}", server, tool_name)
                                };
                                let event = TranscriptEvent {
                                    event_type: "assistant".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: Some(vec![ToolUseInfo {
                                        id: tool_use_id,
                                        name: full_name,
                                        input: arguments,
                                    }]),
                                    tool_use_id: None,
                                    tool_result: None,
                                    is_error: false,
                                    usage: None,
                                    raw: None,
                                };
                                store_event(
                                    orch,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                sequence += 1;
                            }
                            _ => {}
                        }
                    }
                }
                Some("item/completed") => {
                    if let Some(item_type) =
                        msg.pointer("/params/item/type").and_then(|v| v.as_str())
                    {
                        match item_type {
                            "agentMessage" => {
                                if let Some(text) =
                                    msg.pointer("/params/item/text").and_then(|v| v.as_str())
                                {
                                    if streaming_state.is_none()
                                        && last_direct_assistant_text.as_deref() == Some(text)
                                    {
                                        continue;
                                    }
                                    finalize_agent_message(
                                        orch,
                                        emitter,
                                        run_id,
                                        session_id.as_deref(),
                                        &mut streaming_state,
                                        &mut sequence,
                                        text,
                                    );
                                    if streaming_state.is_none() {
                                        last_direct_assistant_text = Some(text.to_string());
                                    }
                                }
                            }
                            "commandExecution" => {
                                let tool_use_id = msg
                                    .pointer("/params/item/id")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                let (result_text, is_error) = summarize_command_result(
                                    msg.pointer("/params/item").unwrap_or(&Value::Null),
                                );
                                let event = TranscriptEvent {
                                    event_type: "tool_result".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: None,
                                    tool_use_id,
                                    tool_result: Some(result_text),
                                    is_error,
                                    usage: None,
                                    raw: None,
                                };
                                store_event(
                                    orch,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                sequence += 1;
                            }
                            "fileChange" => {
                                let tool_use_id = msg
                                    .pointer("/params/item/id")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                let (result_text, is_error) = summarize_file_change_result(
                                    msg.pointer("/params/item").unwrap_or(&Value::Null),
                                );
                                let event = TranscriptEvent {
                                    event_type: "tool_result".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: None,
                                    tool_use_id,
                                    tool_result: Some(result_text),
                                    is_error,
                                    usage: None,
                                    raw: None,
                                };
                                store_event(
                                    orch,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                sequence += 1;
                            }
                            "mcpToolCall" => {
                                let tool_use_id = msg
                                    .pointer("/params/item/id")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                let result_value = msg.pointer("/params/item/result").cloned();
                                let (result_text, is_err, raw_value) =
                                    extract_mcp_result(&result_value);
                                let event = TranscriptEvent {
                                    event_type: "tool_result".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: None,
                                    tool_use_id,
                                    tool_result: Some(result_text),
                                    is_error: is_err,
                                    usage: None,
                                    raw: raw_value,
                                };
                                store_event(
                                    orch,
                                    emitter,
                                    run_id,
                                    session_id.as_deref(),
                                    sequence,
                                    &event,
                                );
                                sequence += 1;
                            }
                            _ => {}
                        }
                    }
                }
                Some("rawResponseItem/completed") => {
                    if let Some(text) = extract_raw_response_message_text(
                        msg.pointer("/params/item").unwrap_or(&Value::Null),
                    ) {
                        if let Some(ref mut state) = streaming_state {
                            if let Ok(mut conn) = orch.db.conn.lock() {
                                if let Ok(Some(active)) =
                                    read_active_stream(&mut conn, &state.stream_id)
                                {
                                    if active.content.is_empty() {
                                        if let Ok(active) = append_chunks(
                                            &mut conn,
                                            &state.stream_id,
                                            state.version,
                                            &[StreamChunkInput::content(text.clone())],
                                        ) {
                                            state.version = active.version();
                                            emit_streaming_update(emitter, run_id, &active);
                                        }
                                    }
                                }
                            }
                        } else if last_direct_assistant_text.as_deref() != Some(text.as_str()) {
                            let event = TranscriptEvent {
                                event_type: "assistant".to_string(),
                                session_id: session_id.clone(),
                                parent_tool_use_id: None,
                                content: Some(text.clone()),
                                thinking: None,
                                tool_name: None,
                                tool_input: None,
                                tool_uses: None,
                                tool_use_id: None,
                                tool_result: None,
                                is_error: false,
                                usage: None,
                                raw: None,
                            };
                            store_event(
                                orch,
                                emitter,
                                run_id,
                                session_id.as_deref(),
                                sequence,
                                &event,
                            );
                            sequence += 1;
                            last_direct_assistant_text = Some(text);
                        }
                    }
                }
                Some("turn/completed") => {
                    let status = msg
                        .pointer("/params/turn/status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("completed");
                    handle_turn_completed(
                        orch,
                        emitter,
                        run_id,
                        session_id.as_deref(),
                        &mut streaming_state,
                        &mut sequence,
                        status,
                        pending_usage.take(),
                    );
                    if let Ok(mut guard) = current_turn_id.lock() {
                        *guard = None;
                    }
                }
                Some("turn/aborted") => {
                    finalize_streaming(
                        orch,
                        emitter,
                        &mut streaming_state,
                        session_id.as_deref(),
                        &mut sequence,
                    );
                    let reason = msg
                        .pointer("/params/reason")
                        .and_then(|v| v.as_str())
                        .or_else(|| msg.pointer("/params/message").and_then(|v| v.as_str()))
                        .unwrap_or("unknown");
                    pending_usage = None;
                    match reason {
                        "interrupted" | "replaced" | "review_ended" => {
                            log::info!("codex turn aborted ({}), handling interrupt", reason);
                            handle_codex_interrupted_turn(orch, emitter, run_id);
                        }
                        _ => {
                            insert_error_event(
                                orch,
                                run_id,
                                session_id.as_deref(),
                                &format!("Turn aborted: {}", reason),
                            );
                            crate::orchestrator::lifecycle::finalize_run(
                                orch,
                                run_id,
                                RunStatus::Crashed,
                            );
                        }
                    }
                    if let Ok(mut guard) = current_turn_id.lock() {
                        *guard = None;
                    }
                }
                Some("error") => {
                    let message = msg
                        .pointer("/params/error/message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown Codex error");
                    let will_retry = msg
                        .pointer("/params/willRetry")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if will_retry {
                        log::warn!("Codex retryable error: {}", message);
                    } else {
                        log::error!("Codex fatal error: {}", message);
                        finalize_streaming(
                            orch,
                            emitter,
                            &mut streaming_state,
                            session_id.as_deref(),
                            &mut sequence,
                        );
                        insert_error_event(
                            orch,
                            run_id,
                            session_id.as_deref(),
                            &format!("Codex error: {}", message),
                        );
                        crate::orchestrator::lifecycle::finalize_run(
                            orch,
                            run_id,
                            RunStatus::Crashed,
                        );
                    }
                }
                Some("thread/tokenUsage/updated") => {
                    let input = msg
                        .pointer("/params/usage/inputTokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    let output = msg
                        .pointer("/params/usage/outputTokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    pending_usage = Some(Usage {
                        input_tokens: input,
                        output_tokens: output,
                        cache_creation_input_tokens: None,
                        cache_read_input_tokens: None,
                    });
                }
                Some("thread/compacted") => {
                    let event = build_codex_compaction_event(
                        msg.pointer("/params").cloned().unwrap_or(Value::Null),
                    );
                    store_event_with_id(
                        orch,
                        run_id,
                        session_id.as_deref(),
                        sequence,
                        &Uuid::new_v4().to_string(),
                        &event,
                    );
                    sequence += 1;
                }
                Some("account/rateLimits/updated") => {
                    if let Some(snapshot) = codex_rate_limit_snapshot_from_value(
                        msg.pointer("/params/rateLimits")
                            .cloned()
                            .unwrap_or(Value::Null),
                    ) {
                        store_provider_usage_snapshot(orch, snapshot);
                    }
                }
                Some("item/commandExecution/outputDelta") | Some("item/fileChange/outputDelta") => {
                    let delta = msg
                        .pointer("/params/delta")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let item_id = msg
                        .pointer("/params/itemId")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if !delta.is_empty() && !item_id.is_empty() {
                        let _ = emitter.emit(
                            "tool-output-delta",
                            serde_json::json!({
                                "run_id": run_id,
                                "tool_use_id": item_id,
                                "delta": delta,
                            }),
                        );
                    }
                }
                Some("serverRequest/resolved")
                | Some("turn/diff/updated")
                | Some("item/plan/delta")
                | Some("item/reasoning/summaryPartAdded")
                | Some("thread/status/changed") => {}
                _ => {
                    log::debug!("Unhandled Codex notification: {:?}", method);
                }
            }
        }

        finalize_streaming(
            orch,
            emitter,
            &mut streaming_state,
            session_id.as_deref(),
            &mut sequence,
        );
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn build_prompt(user_prompt: &str, system_prompt: Option<&str>) -> String {
    let mut combined = String::from(crate::system_prompt::CAIRN_SYSTEM_PROMPT);
    combined.push_str("\n\n");
    combined.push_str(
        "Native Codex file edits and apply_patch are disabled in Cairn. Do not use native edit tools. Use mcp__cairn__edit for file changes.",
    );
    if let Some(sp) = system_prompt {
        if !sp.trim().is_empty() {
            combined.push_str("\n\n");
            combined.push_str(sp);
        }
    }
    combined.push_str("\n\n");
    combined.push_str(user_prompt);
    combined
}

fn codex_sandbox_mode(filesystem: FilesystemScope) -> &'static str {
    match filesystem {
        FilesystemScope::ReadOnly => "read-only",
        FilesystemScope::CwdOnly => "workspace-write",
        FilesystemScope::FullAccess => "danger-full-access",
    }
}

fn build_thread_start_params(
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
) -> Value {
    let mut params = serde_json::json!({
        "model": model.unwrap_or(Model::GPT_5_4_MINI),
        "cwd": cwd,
        "approvalPolicy": approval_policy,
        "sandbox": sandbox_mode,
        "serviceName": "cairn",
    });
    if let Some(model) = model {
        params["model"] = serde_json::json!(model);
    }
    if let Some(effort) = reasoning_effort {
        params["reasoningEffort"] = serde_json::json!(effort);
    }
    params
}

fn build_thread_resume_params(
    thread_id: &str,
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
) -> Value {
    let mut params = serde_json::json!({
        "threadId": thread_id,
        "cwd": cwd,
        "approvalPolicy": approval_policy,
        "sandbox": sandbox_mode,
        "serviceName": "cairn",
    });
    if let Some(model) = model {
        params["model"] = serde_json::json!(model);
    }
    if let Some(effort) = reasoning_effort {
        params["reasoningEffort"] = serde_json::json!(effort);
    }
    params
}

fn build_thread_fork_params(
    thread_id: &str,
    cwd: &str,
    approval_policy: &str,
    sandbox_mode: &str,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
) -> Value {
    build_thread_resume_params(
        thread_id,
        cwd,
        approval_policy,
        sandbox_mode,
        model,
        reasoning_effort,
    )
}

fn is_missing_rollout_error(err: &str, thread_id: &str) -> bool {
    err.contains("no rollout found for thread id")
        && (thread_id.is_empty() || err.contains(thread_id))
}

fn build_resume_fallback_prompt(
    orch: &Orchestrator,
    stale_session_id: &str,
    latest_user_message: &str,
) -> Option<String> {
    let mut conn = orch.db.conn.lock().ok()?;
    let event_rows: Vec<(String, i32, String, String)> = events::table
        .filter(events::session_id.eq(stale_session_id))
        .order((events::created_at.asc(), events::sequence.asc()))
        .select((
            events::run_id,
            events::sequence,
            events::event_type,
            events::data,
        ))
        .load(&mut *conn)
        .ok()?;

    if event_rows.is_empty() {
        return None;
    }

    let transcript = crate::transcripts::format_transcript_full(&event_rows);
    let transcript = trim_resume_transcript(&transcript, RESUME_FALLBACK_TRANSCRIPT_CHARS);

    Some(format!(
        "The previous Codex thread could not be resumed, so you are continuing from a transcript snapshot instead.\n\n\
## Prior Transcript\n\n{}\n\n\
## Latest User Message\n\n{}",
        transcript, latest_user_message
    ))
}

fn trim_resume_transcript(transcript: &str, max_chars: usize) -> String {
    if transcript.chars().count() <= max_chars {
        return transcript.to_string();
    }

    let tail: String = transcript
        .chars()
        .rev()
        .take(max_chars)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("[Earlier transcript omitted]\n\n{}", tail)
}

fn extract_app_server_delta(msg: &Value) -> Option<&str> {
    msg.pointer("/params/delta")
        .and_then(|v| v.as_str())
        .or_else(|| msg.pointer("/params/delta/text").and_then(|v| v.as_str()))
        .or_else(|| msg.pointer("/params/textDelta").and_then(|v| v.as_str()))
}

fn extract_command_execution(command: &Value) -> (String, Vec<String>) {
    if let Some(command_str) = command.as_str() {
        return (command_str.to_string(), vec![command_str.to_string()]);
    }

    let command_vec: Vec<String> = command
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|val| val.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let command_display = if command_vec.is_empty() {
        String::new()
    } else {
        command_vec.join(" ")
    };
    (command_display, command_vec)
}

fn extract_raw_response_message_text(item: &Value) -> Option<String> {
    if item.get("type").and_then(|v| v.as_str()) != Some("message") {
        return None;
    }

    let content = item.get("content")?.as_array()?;
    let parts: Vec<&str> = content
        .iter()
        .filter_map(|entry| {
            let item_type = entry.get("type").and_then(|v| v.as_str())?;
            match item_type {
                "output_text" | "input_text" => entry.get("text").and_then(|v| v.as_str()),
                _ => None,
            }
        })
        .filter(|text| !text.is_empty())
        .collect();

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn codex_approval_policy_for_mode(mode: Option<&str>) -> &'static str {
    match mode {
        Some("bypassPermissions") => "never",
        _ => "on-request",
    }
}

fn handle_codex_approval_request(
    orch: &Orchestrator,
    run_id: &str,
    tool_name: &str,
    params: Value,
    client: &AppServerClient,
    id_value: &Value,
    allow_accept_for_session: bool,
) -> Result<(), String> {
    let item_id = params
        .get("itemId")
        .and_then(|v| v.as_str())
        .unwrap_or("codex-approval");
    let response = request_codex_permission(
        orch,
        run_id,
        item_id,
        tool_name,
        &params,
        allow_accept_for_session,
    )?;
    client.respond(id_value, response)
}

fn decline_codex_native_file_change(
    client: &AppServerClient,
    id_value: &Value,
) -> Result<(), String> {
    client.respond(id_value, native_file_change_decline_payload())
}

fn native_edit_decline_message() -> &'static str {
    "Native Codex file edits and apply_patch are disabled in Cairn. Use mcp__cairn__edit instead."
}

fn native_file_change_decline_payload() -> Value {
    serde_json::json!({
        "decision": "decline",
        "message": native_edit_decline_message()
    })
}

fn request_codex_permission(
    orch: &Orchestrator,
    run_id: &str,
    tool_use_id: &str,
    tool_name: &str,
    tool_input: &Value,
    allow_accept_for_session: bool,
) -> Result<Value, String> {
    let request_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;
    let tool_input_json = serde_json::to_string(tool_input).unwrap_or_default();

    {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        let new_request = NewPermissionRequest {
            id: &request_id,
            run_id,
            tool_use_id,
            tool_name,
            tool_input: &tool_input_json,
            status: "pending",
            created_at: now,
            turn_id: None,
        };

        diesel::insert_into(permission_requests::table)
            .values(&new_request)
            .execute(&mut *conn)
            .map_err(|e| format!("Failed to store Codex approval request: {}", e))?;
    }

    let _ = orch.services.emitter.emit(
        "permission-request",
        serde_json::json!({
            "requestId": request_id,
            "runId": run_id,
            "toolUseId": tool_use_id,
            "toolName": tool_name,
            "input": tool_input,
        }),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "insert"}),
    );

    let mut rx = orch.permission_responses.subscribe();
    let deadline = Instant::now() + CODEX_PERMISSION_TIMEOUT;

    loop {
        match rx.try_recv() {
            Ok((resp_request_id, response_json)) => {
                if resp_request_id != request_id {
                    continue;
                }

                let mut parsed: Value = serde_json::from_str(&response_json)
                    .map_err(|e| format!("Invalid permission response payload: {}", e))?;
                if !allow_accept_for_session
                    && parsed.get("decision").and_then(|v| v.as_str()) == Some("acceptForSession")
                {
                    parsed["decision"] = serde_json::json!("accept");
                }
                return Ok(parsed);
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                if Instant::now() >= deadline {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                return Err("Permission response channel closed".to_string());
            }
        }
    }

    if let Ok(mut conn) = orch.db.conn.lock() {
        let now = chrono::Utc::now().timestamp() as i32;
        let response_json = serde_json::json!({ "decision": "decline" }).to_string();
        let _ = diesel::update(permission_requests::table.find(&request_id))
            .set((
                permission_requests::status.eq("denied"),
                permission_requests::response.eq(Some(response_json.as_str())),
                permission_requests::responded_at.eq(Some(now)),
            ))
            .execute(&mut *conn);
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "update"}),
    );

    Err("Permission request timed out after 5 minutes".to_string())
}

/// Write a stable Codex config with Cairn MCP server.
/// Uses a single shared CODEX_HOME (~/.cairn/codex/) so Codex's thread/state DB
/// persists across runs, enabling cross-run resume.
///
/// Run-specific values (CAIRN_RUN_ID, CAIRN_MCP_SECRET) are inherited from the
/// process env via `env_vars` rather than baked into the config, so the config
/// is safe to share across parallel runs.
fn write_codex_config(
    mcp_binary_path: &str,
    callback_port: u16,
    mcp_config_path: &std::path::Path,
) -> Result<String, String> {
    let home = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
    let codex_home = home.join(".cairn").join("codex");

    std::fs::create_dir_all(&codex_home)
        .map_err(|e| format!("Failed to create codex home: {}", e))?;

    let callback_url = format!("http://127.0.0.1:{}/api/mcp", callback_port);
    let model_catalog_path = write_codex_model_catalog_override(&home, &codex_home)?;

    // Read args from the Claude MCP config JSON (same args for cairn-mcp)
    let mcp_args: Vec<String> = if mcp_config_path.exists() {
        let config_str = std::fs::read_to_string(mcp_config_path).unwrap_or_default();
        if let Ok(config) = serde_json::from_str::<Value>(&config_str) {
            config
                .pointer("/mcpServers/cairn/args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    // Format args as TOML array
    let args_toml = if mcp_args.is_empty() {
        "[]".to_string()
    } else {
        let quoted: Vec<String> = mcp_args
            .iter()
            .map(|a| format!("\"{}\"", a.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect();
        format!("[{}]", quoted.join(", "))
    };
    let model_catalog_toml = model_catalog_path.as_ref().map_or(String::new(), |path| {
        format!(
            "model_catalog_json = \"{}\"\n",
            path.to_string_lossy()
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
        )
    });

    let config_toml = format!(
        r#"# Auto-generated by Cairn
include_apply_patch_tool = false
{model_catalog_toml}[features]
apply_patch_freeform = false

[mcp_servers.cairn]
command = "{mcp_binary}"
args = {args_toml}
env = {{ CAIRN_CALLBACK_URL = "{callback_url}" }}
env_vars = ["CAIRN_RUN_ID", "CAIRN_MCP_SECRET"]
"#,
        mcp_binary = mcp_binary_path.replace('\\', "\\\\").replace('"', "\\\""),
        callback_url = callback_url,
        args_toml = args_toml,
        model_catalog_toml = model_catalog_toml,
    );

    let config_path = codex_home.join("config.toml");
    std::fs::write(&config_path, config_toml)
        .map_err(|e| format!("Failed to write codex config: {}", e))?;

    // Auth is written by the caller (start_session) from Cairn-managed identity

    log::info!("Wrote Codex config to {:?}", config_path);
    Ok(codex_home.to_string_lossy().to_string())
}

fn write_codex_model_catalog_override(
    home: &std::path::Path,
    codex_home: &std::path::Path,
) -> Result<Option<std::path::PathBuf>, String> {
    let source_path = home.join(CAIRN_CODEX_MODELS_RESOURCE);
    if !source_path.exists() {
        log::warn!(
            "Codex models catalog not found at {:?}; native apply_patch may remain available",
            source_path
        );
        return Ok(None);
    }

    let source = std::fs::read_to_string(&source_path).map_err(|e| {
        format!(
            "Failed to read Codex models catalog {:?}: {}",
            source_path, e
        )
    })?;
    let rewritten = disable_apply_patch_in_model_catalog(&source)?;
    let catalog_path = codex_home.join("model_catalog.json");
    std::fs::write(&catalog_path, rewritten)
        .map_err(|e| format!("Failed to write Codex model catalog override: {}", e))?;
    Ok(Some(catalog_path))
}

fn disable_apply_patch_in_model_catalog(catalog_json: &str) -> Result<String, String> {
    let mut catalog: Value = serde_json::from_str(catalog_json)
        .map_err(|e| format!("Failed to parse Codex models catalog: {}", e))?;
    let models = catalog
        .get_mut("models")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| "Codex models catalog missing models array".to_string())?;

    for model in models.iter_mut() {
        let Some(model_obj) = model.as_object_mut() else {
            return Err("Codex models catalog contains non-object model entry".to_string());
        };
        model_obj.insert("apply_patch_tool_type".to_string(), Value::Null);
    }

    serde_json::to_string(&catalog)
        .map_err(|e| format!("Failed to serialize Codex model catalog override: {}", e))
}

fn store_event(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    sequence: i32,
    event: &TranscriptEvent,
) {
    let event_id = Uuid::new_v4().to_string();
    store_event_with_id(orch, run_id, session_id, sequence, &event_id, event);
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "events", "action": "insert"}),
    );
}

fn emit_streaming_update(
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    active: &ActiveMessageStream,
) {
    let _ = emitter.emit(
        "streaming-update",
        serde_json::json!({
            "run_id": run_id,
            "event_id": active.stream.id,
            "content": active.content,
            "thinking": active.thinking,
        }),
    );
}

fn ensure_stream_open(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: i32,
) -> Result<(), String> {
    if streaming_state.is_some() {
        return Ok(());
    }
    let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
    let current_turn = orch.process_state.get_current_turn_id(run_id);
    let stream = open_stream(
        &mut conn,
        run_id,
        session_id,
        current_turn.as_deref(),
        "codex",
        Some(sequence),
    )?;
    *streaming_state = Some(StreamingState::new(&stream));
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "events", "action": "insert"}),
    );
    Ok(())
}

fn handle_agent_message_delta(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: &mut i32,
    delta: &str,
) {
    if ensure_stream_open(
        orch,
        emitter,
        run_id,
        session_id,
        streaming_state,
        *sequence,
    )
    .is_err()
    {
        return;
    }

    if let Some(ref mut state) = streaming_state {
        if let Ok(mut conn) = orch.db.conn.lock() {
            match append_chunks(
                &mut conn,
                &state.stream_id,
                state.version,
                &[StreamChunkInput::content(delta.to_string())],
            ) {
                Ok(active) => {
                    state.version = active.version();
                    emit_streaming_update(emitter, run_id, &active);
                }
                Err(error) => {
                    log::warn!(
                        "Failed to append Codex content delta for {}: {}",
                        run_id,
                        error
                    );
                }
            }
        }
    }
}

fn handle_reasoning_delta(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: i32,
    delta: &str,
) {
    if ensure_stream_open(orch, emitter, run_id, session_id, streaming_state, sequence).is_err() {
        return;
    }

    if let Some(ref mut state) = streaming_state {
        if let Ok(mut conn) = orch.db.conn.lock() {
            match append_chunks(
                &mut conn,
                &state.stream_id,
                state.version,
                &[StreamChunkInput::thinking(delta.to_string())],
            ) {
                Ok(active) => {
                    state.version = active.version();
                    emit_streaming_update(emitter, run_id, &active);
                }
                Err(error) => {
                    log::warn!(
                        "Failed to append Codex reasoning delta for {}: {}",
                        run_id,
                        error
                    );
                }
            }
        }
    }
}

fn finalize_agent_message(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: &mut i32,
    text: &str,
) {
    if streaming_state.is_some() {
        finalize_streaming_with_event(
            orch,
            emitter,
            streaming_state,
            Some(TranscriptEvent {
                event_type: "assistant".to_string(),
                session_id: session_id.map(|s| s.to_string()),
                parent_tool_use_id: None,
                content: Some(text.to_string()),
                thinking: None,
                tool_name: None,
                tool_input: None,
                tool_uses: None,
                tool_use_id: None,
                tool_result: None,
                is_error: false,
                usage: None,
                raw: None,
            }),
            sequence,
        );
    } else {
        let event = TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: session_id.map(|s| s.to_string()),
            parent_tool_use_id: None,
            content: Some(text.to_string()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            usage: None,
            raw: None,
        };
        store_event(orch, emitter, run_id, session_id, *sequence, &event);
        *sequence += 1;
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_turn_completed(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: &mut i32,
    status: &str,
    usage: Option<Usage>,
) {
    finalize_streaming(orch, emitter, streaming_state, session_id, sequence);

    match status {
        "completed" => {
            let result_event = TranscriptEvent {
                event_type: "result:success".to_string(),
                session_id: session_id.map(|s| s.to_string()),
                parent_tool_use_id: None,
                content: None,
                thinking: None,
                tool_name: None,
                tool_input: None,
                tool_uses: None,
                tool_use_id: None,
                tool_result: None,
                is_error: false,
                usage,
                raw: None,
            };
            store_event(orch, emitter, run_id, session_id, *sequence, &result_event);
            *sequence += 1;

            let is_task = is_task_spawned_run(orch, run_id);
            if is_task {
                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
                orch.process_state.transition_to_warm(run_id);
            } else {
                crate::orchestrator::lifecycle::transition_to_warm_state(orch, run_id);
                let _ = emitter.emit(
                    "run-turn-completed",
                    serde_json::json!({
                        "runId": run_id,
                        "run_id": run_id,
                        "isWarm": true,
                        "is_warm": true
                    }),
                );
            }
        }
        "interrupted" => {
            handle_codex_interrupted_turn(orch, emitter, run_id);
        }
        _ => {
            insert_error_event(
                orch,
                run_id,
                session_id,
                &format!("Codex turn failed: {}", status),
            );
            crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Crashed);
        }
    }
}

fn emit_codex_run_turn_completed(emitter: &Arc<dyn crate::services::EventEmitter>, run_id: &str) {
    let _ = emitter.emit(
        "run-turn-completed",
        serde_json::json!({
            "runId": run_id,
            "run_id": run_id,
            "isWarm": true,
            "is_warm": true
        }),
    );
}

fn terminal_tool_called_for_run(orch: &Orchestrator, run_id: &str) -> bool {
    orch.process_state
        .processes
        .lock()
        .ok()
        .and_then(|processes| {
            processes
                .get(run_id)
                .map(|process| process.terminal_tool_called.load(Ordering::Acquire))
        })
        .unwrap_or(false)
}

fn should_finalize_task_run_on_interrupted_turn(
    terminal_tool_called: bool,
    is_task_spawned: bool,
) -> bool {
    terminal_tool_called && is_task_spawned
}

fn handle_codex_interrupted_turn(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
) {
    if should_finalize_task_run_on_interrupted_turn(
        terminal_tool_called_for_run(orch, run_id),
        is_task_spawned_run(orch, run_id),
    ) {
        log::info!(
            "codex interrupted after terminal tool for task-spawned run {}; finalizing as completed",
            &run_id[..run_id.len().min(8)]
        );
        crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
        orch.process_state.transition_to_warm(run_id);
    } else {
        crate::orchestrator::lifecycle::transition_to_warm_state(orch, run_id);
        emit_codex_run_turn_completed(emitter, run_id);
    }
}

fn build_codex_compaction_event(raw: Value) -> TranscriptEvent {
    TranscriptEvent {
        event_type: "system:compact_boundary".to_string(),
        session_id: None,
        parent_tool_use_id: None,
        content: Some("Context compacted".to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        usage: None,
        raw: Some(json!({
            "provider": "codex",
            "compaction": raw,
        })),
    }
}

fn codex_rate_limit_snapshot_from_value(raw: Value) -> Option<ProviderUsageSnapshot> {
    let mut windows = Vec::new();
    for id in ["primary", "secondary"] {
        let Some(window) = raw.get(id) else {
            continue;
        };
        if window.is_null() {
            continue;
        }
        let used_percent = window.get("usedPercent").and_then(Value::as_f64)?;
        let window_duration_mins = window
            .get("windowDurationMins")
            .and_then(Value::as_i64)
            .map(|mins| mins as i32);
        windows.push(ProviderUsageWindow {
            id: id.to_string(),
            label: codex_rate_limit_window_label(id, window_duration_mins),
            scope: codex_rate_limit_scope(window_duration_mins),
            scope_target: None,
            used_percent,
            remaining_percent: (100.0 - used_percent).clamp(0.0, 100.0),
            resets_at: window.get("resetsAt").and_then(Value::as_i64),
            reset_at_text: None,
            window_duration_mins,
        });
    }

    let credits = raw.get("credits").and_then(|value| {
        if value.is_null() {
            return None;
        }
        Some(ProviderCreditsSnapshot {
            balance: value.get("balance").and_then(Value::as_f64),
            total_granted: value.get("totalGranted").and_then(Value::as_f64),
            total_used: value.get("totalUsed").and_then(Value::as_f64),
            currency: value
                .get("currency")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    });

    Some(ProviderUsageSnapshot {
        backend: "codex".to_string(),
        source: "codex_rate_limits".to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows,
        credits,
        error: None,
        unsupported_reason: None,
        raw: Some(raw),
    })
}

fn codex_rate_limit_scope(window_duration_mins: Option<i32>) -> ProviderUsageScope {
    if window_duration_mins == Some(10_080) {
        ProviderUsageScope::Weekly
    } else {
        ProviderUsageScope::RollingWindow
    }
}

fn codex_rate_limit_window_label(id: &str, window_duration_mins: Option<i32>) -> String {
    match window_duration_mins {
        Some(10_080) => "Weekly window".to_string(),
        Some(mins) if mins % 60 == 0 => format!("{}-hour window", mins / 60),
        Some(mins) => format!("{}-minute window", mins),
        None => match id {
            "primary" => "Primary window".to_string(),
            "secondary" => "Secondary window".to_string(),
            _ => "Usage window".to_string(),
        },
    }
}

fn store_provider_usage_snapshot(orch: &Orchestrator, snapshot: ProviderUsageSnapshot) {
    if let Ok(mut guard) = orch.provider_usage_snapshots.write() {
        guard.insert(snapshot.backend.clone(), snapshot);
    }
}

fn store_event_with_id(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    sequence: i32,
    event_id: &str,
    event: &TranscriptEvent,
) {
    let Ok(mut conn) = orch.db.conn.lock() else {
        return;
    };
    let now = chrono::Utc::now().timestamp() as i32;
    let data = serde_json::to_string(event).unwrap_or_default();
    let (input_tokens, cache_read_tokens, cache_create_tokens, output_tokens) =
        if let Some(ref usage) = event.usage {
            (
                Some(usage.input_tokens as i32),
                usage.cache_read_input_tokens.map(|t| t as i32),
                usage.cache_creation_input_tokens.map(|t| t as i32),
                Some(usage.output_tokens as i32),
            )
        } else {
            (None, None, None, None)
        };

    let current_turn = orch.process_state.get_current_turn_id(run_id);
    let new_event = NewEvent {
        id: event_id,
        run_id,
        session_id,
        sequence,
        timestamp: now,
        event_type: &event.event_type,
        data: &data,
        parent_tool_use_id: event.parent_tool_use_id.as_deref(),
        created_at: now,
        input_tokens,
        cache_read_tokens,
        cache_create_tokens,
        output_tokens,
        turn_id: current_turn.as_deref(),
    };

    if diesel::insert_into(events::table)
        .values(&new_event)
        .execute(&mut *conn)
        .is_ok()
    {
        // Sync event to cloud
        orch.sync(crate::sync::SyncMessage::Event(crate::sync::SyncEvent {
            id: event_id.to_string(),
            run_id: run_id.to_string(),
            session_id: session_id.map(|s| s.to_string()),
            sequence: Some(sequence),
            event_type: event.event_type.clone(),
            data: Some(data.clone()),
            input_tokens,
            output_tokens,
            cache_read_tokens,
            created_at: Some(now as i64),
            turn_id: current_turn.clone(),
        }));

        if event.event_type == "assistant" {
            if let Some(ref engine) = orch.embedding_engine {
                crate::embeddings::embed_event_inline(engine, &mut conn, event_id, &data);
            }
        }
    }
}

fn finalize_streaming_with_event(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    streaming_state: &mut Option<StreamingState>,
    final_event: Option<TranscriptEvent>,
    sequence: &mut i32,
) {
    let Some(state) = streaming_state.take() else {
        return;
    };
    let Ok(mut conn) = orch.db.conn.lock() else {
        return;
    };
    match finalize_stream(&mut conn, &state.stream_id, state.version, final_event) {
        Ok(finalized) => {
            drop(conn);
            let _ = emitter.emit(
                "db-change",
                serde_json::json!({"table": "events", "action": "insert"}),
            );
            process_post_commit_outbox(orch, &finalized.outbox_entries);
            *sequence += 1;
        }
        Err(error) => {
            log::warn!(
                "Failed to finalize Codex stream {}: {}",
                state.stream_id,
                error
            );
        }
    }
}

fn finalize_streaming(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    streaming_state: &mut Option<StreamingState>,
    _session_id: Option<&str>,
    sequence: &mut i32,
) {
    finalize_streaming_with_event(orch, emitter, streaming_state, None, sequence);
}

/// Extract display text from a Codex MCP CallToolResult.
///
/// The wire format is `Result<CallToolResult, String>` serialized as:
///   `{"Ok": {"content": [{"type":"text","text":"..."}], "is_error": false}}`
///   or `{"Err": "message"}`
///
/// Returns (display_text, is_error, optional raw value for storage).
fn extract_mcp_result(result: &Option<Value>) -> (String, bool, Option<Value>) {
    let Some(val) = result else {
        return ("Completed".to_string(), false, None);
    };

    // Handle Result::Err
    if let Some(err_str) = val.get("Err").and_then(|v| v.as_str()) {
        return (format!("Error: {}", err_str), true, Some(val.clone()));
    }

    // Handle Result::Ok -> CallToolResult
    let call_result = val.get("Ok").unwrap_or(val);

    let is_err = call_result
        .get("is_error")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Extract text from content array: [{"type":"text","text":"..."}, ...]
    let text = call_result
        .get("content")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    if item.get("type").and_then(|t| t.as_str()) == Some("text") {
                        item.get("text").and_then(|t| t.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();

    let display = if text.is_empty() {
        if is_err {
            "Error".to_string()
        } else {
            "Completed".to_string()
        }
    } else {
        text
    };

    // Only store raw if there's non-text content (images, structured_content, etc.)
    let has_non_text = call_result
        .get("content")
        .and_then(|v| v.as_array())
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("type").and_then(|t| t.as_str()) != Some("text"))
        });
    let raw = if has_non_text || call_result.get("structured_content").is_some() {
        Some(val.clone())
    } else {
        None
    };

    (display, is_err, raw)
}

fn refresh_codex_tokens_via_http(refresh_token: &str) -> Result<CodexOAuthTokens, String> {
    let client = Client::new();
    let response = client
        .post(CODEX_OAUTH_TOKEN_URL)
        .json(&json!({
            "grant_type": "refresh_token",
            "client_id": CODEX_CLIENT_ID,
            "refresh_token": refresh_token,
        }))
        .send()
        .map_err(|e| format!("Refresh request failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_else(|_| "<no body>".to_string());
        return Err(format!(
            "Refresh token endpoint returned {}: {}",
            status, body
        ));
    }

    let value: Value = response
        .json()
        .map_err(|e| format!("Invalid refresh response: {}", e))?;

    let id_token = value
        .get("id_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Refresh response missing id_token".to_string())?
        .to_string();
    let access_token = value
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Refresh response missing access_token".to_string())?
        .to_string();
    let refresh_token = value
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let chatgpt_account_id = extract_chatgpt_account_id_from_jwt(&access_token);
    Ok(CodexOAuthTokens {
        id_token,
        access_token,
        refresh_token,
        chatgpt_account_id,
    })
}

fn persist_codex_oauth_tokens(orch: &Orchestrator, new_json: &str) {
    let Some(mut store) = orch.get_identity_store() else {
        return;
    };

    let mut updated = false;
    for account in store.accounts.iter_mut() {
        if account.api_provider == ApiProvider::OpenAI {
            if let ProviderAuth::OAuthToken { .. } = account.auth {
                account.auth = ProviderAuth::OAuthToken {
                    value: new_json.to_string(),
                };
                updated = true;
                break;
            }
        }
    }

    if updated {
        if let Err(err) = orch.save_identity_store(store) {
            log::warn!("Failed to persist refreshed Codex tokens: {}", err);
        } else {
            let _ = orch.services.emitter.emit(
                "config-changed",
                serde_json::json!({"entity_type": "identity"}),
            );
        }
    }
}

fn summarize_command_result(item: &Value) -> (String, bool) {
    let aggregated = item
        .get("aggregatedOutput")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let stdout = item
        .get("stdout")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let stderr = item
        .get("stderr")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| format!("stderr: {}", s));
    let exit_code = item.get("exitCode").and_then(|v| v.as_i64());
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed");

    let result_text = aggregated
        .or(stdout)
        .or(stderr)
        .unwrap_or_else(|| match exit_code {
            Some(0) => "Command completed successfully".to_string(),
            Some(code) => format!("Exit code {}", code),
            None => status.to_string(),
        });

    let is_error = exit_code.is_some_and(|c| c != 0) || matches!(status, "failed" | "declined");
    (result_text, is_error)
}

fn summarize_file_change_result(item: &Value) -> (String, bool) {
    if let Some(err) = item.get("error").and_then(|v| v.as_str()) {
        return (format!("Error: {}", err), true);
    }
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed");
    let text = match status {
        "completed" => "File changes applied".to_string(),
        "declined" => "File changes declined".to_string(),
        "failed" => "File changes failed".to_string(),
        other => format!("File changes {}", other),
    };
    let is_error = matches!(status, "failed" | "declined");
    (text, is_error)
}

/// Parse a version string like "codex 0.37.1" or "0.37.1" into (major, minor, patch).
fn parse_codex_version(version_str: &str) -> Option<(u32, u32, u32)> {
    // Find the version number portion — skip any leading text like "codex "
    let version_part = version_str
        .split_whitespace()
        .find(|s| s.chars().next().is_some_and(|c| c.is_ascii_digit()))?;
    let mut parts = version_part.split('.');
    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts.next()?.parse::<u32>().ok()?;
    // Patch may contain pre-release suffix like "1-beta", take only digits
    let patch_str = parts.next().unwrap_or("0");
    let patch = patch_str
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse::<u32>()
        .unwrap_or(0);
    Some((major, minor, patch))
}

fn check_codex_version(codex_path: &str) -> Result<(), String> {
    let output = std::process::Command::new(codex_path)
        .arg("--version")
        .output()
        .map_err(|e| format!("Failed to check codex version: {}", e))?;

    let version_str = String::from_utf8_lossy(&output.stdout);
    let version_str = version_str.trim();

    // If we can't parse the version, log a warning but don't block
    let Some(version) = parse_codex_version(version_str) else {
        log::warn!("Could not parse Codex version from: {:?}", version_str);
        return Ok(());
    };

    let (min_major, min_minor, min_patch) = MIN_CODEX_VERSION;
    if version < (min_major, min_minor, min_patch) {
        return Err(format!(
            "Codex CLI version {}.{}.{} is too old. Minimum required: {}.{}.{}. Run: npm install -g @openai/codex",
            version.0, version.1, version.2, min_major, min_minor, min_patch
        ));
    }

    log::debug!(
        "Codex CLI version: {}.{}.{}",
        version.0,
        version.1,
        version.2
    );
    Ok(())
}

fn parse_codex_oauth_tokens(auth_json: &str) -> Result<CodexOAuthTokens, String> {
    let value: Value =
        serde_json::from_str(auth_json).map_err(|e| format!("Invalid Codex OAuth JSON: {}", e))?;
    let tokens = value
        .get("tokens")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "Missing tokens field".to_string())?;
    let id_token = tokens
        .get("id_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing id_token".to_string())?
        .to_string();
    let access_token = tokens
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing access_token".to_string())?
        .to_string();
    let refresh_token = tokens
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let chatgpt_account_id = extract_chatgpt_account_id_from_jwt(&access_token);
    Ok(CodexOAuthTokens {
        id_token,
        access_token,
        refresh_token,
        chatgpt_account_id,
    })
}

/// Extract `chatgpt_account_id` from a Codex access token JWT without signature verification.
fn extract_chatgpt_account_id_from_jwt(jwt: &str) -> Option<String> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let payload_b64 = jwt.split('.').nth(1)?;
    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let payload: Value = serde_json::from_slice(&payload_bytes).ok()?;
    payload
        .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Generate the auth.json contents for a Codex session from a [`CodexAuth`] credential.
///
/// - **OAuth**: writes the stored JSON string verbatim (it came from `codex login`).
/// - **API key**: constructs the `auth.json` structure Codex expects.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::process::BackendStdin;
    use std::io::Write;

    struct TestStdin(Vec<u8>);

    impl Write for TestStdin {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl BackendStdin for TestStdin {
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    // ---- build_prompt ---------------------------------------------------

    #[test]
    fn build_prompt_with_system() {
        let result = build_prompt("do stuff", Some("You are an agent."));
        let base = crate::system_prompt::CAIRN_SYSTEM_PROMPT;
        let warning = "Native Codex file edits and apply_patch are disabled in Cairn. Do not use native edit tools. Use mcp__cairn__edit for file changes.";
        assert_eq!(
            result,
            format!("{base}\n\n{warning}\n\nYou are an agent.\n\ndo stuff")
        );
    }

    #[test]
    fn build_prompt_without_system() {
        let result = build_prompt("do stuff", None);
        let base = crate::system_prompt::CAIRN_SYSTEM_PROMPT;
        let warning = "Native Codex file edits and apply_patch are disabled in Cairn. Do not use native edit tools. Use mcp__cairn__edit for file changes.";
        assert_eq!(result, format!("{base}\n\n{warning}\n\ndo stuff"));
    }

    #[test]
    fn build_prompt_empty_system() {
        let result = build_prompt("do stuff", Some(""));
        let base = crate::system_prompt::CAIRN_SYSTEM_PROMPT;
        let warning = "Native Codex file edits and apply_patch are disabled in Cairn. Do not use native edit tools. Use mcp__cairn__edit for file changes.";
        assert_eq!(result, format!("{base}\n\n{warning}\n\ndo stuff"));
    }

    #[test]
    fn approval_policy_from_permission_mode() {
        assert_eq!(
            codex_approval_policy_for_mode(Some("bypassPermissions")),
            "never"
        );
        assert_eq!(
            codex_approval_policy_for_mode(Some("acceptEdits")),
            "on-request"
        );
        assert_eq!(codex_approval_policy_for_mode(None), "on-request");
    }

    #[test]
    fn codex_sandbox_mode_maps_filesystem_scope() {
        use crate::models::FilesystemScope;

        assert_eq!(codex_sandbox_mode(FilesystemScope::ReadOnly), "read-only");
        assert_eq!(
            codex_sandbox_mode(FilesystemScope::CwdOnly),
            "workspace-write"
        );
        assert_eq!(
            codex_sandbox_mode(FilesystemScope::FullAccess),
            "danger-full-access"
        );
    }

    #[test]
    fn native_file_change_decline_payload_has_recovery_message() {
        let payload = native_file_change_decline_payload();
        assert_eq!(payload["decision"], "decline");
        assert_eq!(payload["message"], native_edit_decline_message());
    }

    // ---- extract_mcp_result ---------------------------------------------

    #[test]
    fn extract_mcp_result_none() {
        let (text, is_err, raw) = extract_mcp_result(&None);
        assert_eq!(text, "Completed");
        assert!(!is_err);
        assert!(raw.is_none());
    }

    #[test]
    fn extract_mcp_result_err() {
        let val = serde_json::json!({"Err": "something went wrong"});
        let (text, is_err, raw) = extract_mcp_result(&Some(val));
        assert_eq!(text, "Error: something went wrong");
        assert!(is_err);
        assert!(raw.is_some());
    }

    #[test]
    fn extract_mcp_result_ok_text() {
        let val = serde_json::json!({
            "Ok": {
                "content": [{"type": "text", "text": "hello world"}],
                "is_error": false
            }
        });
        let (text, is_err, raw) = extract_mcp_result(&Some(val));
        assert_eq!(text, "hello world");
        assert!(!is_err);
        assert!(raw.is_none()); // pure text → no raw stored
    }

    #[test]
    fn extract_mcp_result_ok_is_error() {
        let val = serde_json::json!({
            "Ok": {
                "content": [{"type": "text", "text": "fail"}],
                "is_error": true
            }
        });
        let (text, is_err, _) = extract_mcp_result(&Some(val));
        assert_eq!(text, "fail");
        assert!(is_err);
    }

    #[test]
    fn extract_mcp_result_ok_empty_content() {
        let val = serde_json::json!({"Ok": {"content": [], "is_error": false}});
        let (text, is_err, _) = extract_mcp_result(&Some(val));
        assert_eq!(text, "Completed");
        assert!(!is_err);
    }

    #[test]
    fn extract_mcp_result_ok_mixed_content_stores_raw() {
        let val = serde_json::json!({
            "Ok": {
                "content": [
                    {"type": "text", "text": "description"},
                    {"type": "image", "data": "base64..."}
                ],
                "is_error": false
            }
        });
        let (text, is_err, raw) = extract_mcp_result(&Some(val));
        assert_eq!(text, "description");
        assert!(!is_err);
        assert!(raw.is_some()); // non-text content → raw stored
    }

    #[test]
    fn extract_mcp_result_multiple_text_items() {
        let val = serde_json::json!({
            "Ok": {
                "content": [
                    {"type": "text", "text": "line1"},
                    {"type": "text", "text": "line2"}
                ],
                "is_error": false
            }
        });
        let (text, _, _) = extract_mcp_result(&Some(val));
        assert_eq!(text, "line1\nline2");
    }

    // ---- EventMsg deserialization ---------------------------------------

    #[test]
    fn parse_session_configured() {
        let json =
            r#"{"id":"1","msg":{"type":"session_configured","session_id":"s1","model":"gpt-5"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::SessionConfigured {
                session_id, model, ..
            } => {
                assert_eq!(session_id, "s1");
                assert_eq!(model, "gpt-5");
            }
            other => panic!("Expected SessionConfigured, got {:?}", other),
        }
    }

    #[test]
    fn parse_agent_message() {
        let json = r#"{"id":"2","msg":{"type":"agent_message","message":"hello"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::AgentMessage { message } => assert_eq!(message, "hello"),
            other => panic!("Expected AgentMessage, got {:?}", other),
        }
    }

    #[test]
    fn parse_agent_message_delta() {
        let json = r#"{"id":"3","msg":{"type":"agent_message_delta","delta":"chunk"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::AgentMessageDelta { delta } => assert_eq!(delta, "chunk"),
            other => panic!("Expected AgentMessageDelta, got {:?}", other),
        }
    }

    #[test]
    fn parse_exec_command_begin() {
        let json = r#"{"id":"4","msg":{"type":"exec_command_begin","call_id":"c1","command":["ls","-la"],"cwd":"/tmp"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::ExecCommandBegin {
                call_id,
                command,
                cwd,
            } => {
                assert_eq!(call_id.unwrap(), "c1");
                assert_eq!(command.unwrap(), vec!["ls", "-la"]);
                assert_eq!(cwd.unwrap(), "/tmp");
            }
            other => panic!("Expected ExecCommandBegin, got {:?}", other),
        }
    }

    #[test]
    fn parse_exec_command_end() {
        let json = r#"{"id":"5","msg":{"type":"exec_command_end","call_id":"c1","exit_code":0,"stdout":"ok","stderr":""}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::ExecCommandEnd {
                exit_code,
                stdout,
                stderr,
                ..
            } => {
                assert_eq!(exit_code.unwrap(), 0);
                assert_eq!(stdout.unwrap(), "ok");
                assert_eq!(stderr.unwrap(), "");
            }
            other => panic!("Expected ExecCommandEnd, got {:?}", other),
        }
    }

    #[test]
    fn parse_task_complete() {
        let json = r#"{"id":"6","msg":{"type":"task_complete"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert!(matches!(event.msg, EventMsg::TaskComplete {}));
    }

    #[test]
    fn parse_turn_aborted_with_reason() {
        let json = r#"{"id":"7","msg":{"type":"turn_aborted","reason":"interrupted"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::TurnAborted { reason } => assert_eq!(reason.unwrap(), "interrupted"),
            other => panic!("Expected TurnAborted, got {:?}", other),
        }
    }

    #[test]
    fn parse_turn_aborted_without_reason() {
        let json = r#"{"id":"8","msg":{"type":"turn_aborted"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::TurnAborted { reason } => assert!(reason.is_none()),
            other => panic!("Expected TurnAborted, got {:?}", other),
        }
    }

    #[test]
    fn interrupted_terminal_tool_task_is_treated_as_completed() {
        assert!(should_finalize_task_run_on_interrupted_turn(true, true));
        assert!(!should_finalize_task_run_on_interrupted_turn(false, true));
        assert!(!should_finalize_task_run_on_interrupted_turn(true, false));
    }

    #[test]
    fn parse_error_event() {
        let json = r#"{"id":"9","msg":{"type":"error","message":"boom"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::Error { message } => assert_eq!(message, "boom"),
            other => panic!("Expected Error, got {:?}", other),
        }
    }

    #[test]
    fn parse_patch_apply_begin() {
        let json = r#"{"id":"10","msg":{"type":"patch_apply_begin","call_id":"p1","patch":"--- a/f\n+++ b/f"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::PatchApplyBegin { call_id, patch } => {
                assert_eq!(call_id.unwrap(), "p1");
                assert!(patch.unwrap().contains("--- a/f"));
            }
            other => panic!("Expected PatchApplyBegin, got {:?}", other),
        }
    }

    #[test]
    fn parse_patch_apply_end_success() {
        let json = r#"{"id":"11","msg":{"type":"patch_apply_end","call_id":"p1"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::PatchApplyEnd { error, .. } => assert!(error.is_none()),
            other => panic!("Expected PatchApplyEnd, got {:?}", other),
        }
    }

    #[test]
    fn parse_patch_apply_end_error() {
        let json =
            r#"{"id":"12","msg":{"type":"patch_apply_end","call_id":"p1","error":"bad patch"}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::PatchApplyEnd { error, .. } => assert_eq!(error.unwrap(), "bad patch"),
            other => panic!("Expected PatchApplyEnd, got {:?}", other),
        }
    }

    #[test]
    fn parse_mcp_tool_call_begin() {
        let json = r#"{"id":"13","msg":{"type":"mcp_tool_call_begin","call_id":"m1","invocation":{"server":"cairn","tool":"read","arguments":{"path":"f.rs"}}}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::McpToolCallBegin {
                call_id,
                invocation,
            } => {
                assert_eq!(call_id, "m1");
                let inv = invocation.unwrap();
                assert_eq!(inv.server.unwrap(), "cairn");
                assert_eq!(inv.tool.unwrap(), "read");
                assert_eq!(inv.arguments.unwrap(), serde_json::json!({"path": "f.rs"}));
            }
            other => panic!("Expected McpToolCallBegin, got {:?}", other),
        }
    }

    #[test]
    fn parse_unknown_event_type() {
        let json = r#"{"id":"99","msg":{"type":"some_future_event","data":123}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert!(matches!(event.msg, EventMsg::Unknown));
    }

    #[test]
    fn parse_agent_reasoning_delta() {
        let json = r#"{"id":"14","msg":{"type":"agent_reasoning_delta","delta":"thinking..."}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::AgentReasoningDelta { delta } => {
                assert_eq!(delta.unwrap(), "thinking...");
            }
            other => panic!("Expected AgentReasoningDelta, got {:?}", other),
        }
    }

    #[test]
    fn parse_token_count() {
        let json =
            r#"{"id":"15","msg":{"type":"token_count","input_tokens":100,"output_tokens":50}}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        match event.msg {
            EventMsg::TokenCount {
                input_tokens,
                output_tokens,
            } => {
                assert_eq!(input_tokens.unwrap(), 100);
                assert_eq!(output_tokens.unwrap(), 50);
            }
            other => panic!("Expected TokenCount, got {:?}", other),
        }
    }

    // ---- send_* protocol format -----------------------------------------

    #[test]
    fn send_user_message_format() {
        let backend = CodexBackend;
        let mut buf = TestStdin(Vec::new());
        backend
            .send_user_message(&mut buf, "hello", "s1", None, None)
            .unwrap();
        let line = String::from_utf8(buf.0).unwrap();
        let parsed: Value = serde_json::from_str(line.trim()).unwrap();
        assert!(parsed.get("id").is_some());
        let op = parsed.get("op").unwrap();
        assert_eq!(op["type"], "user_input");
        assert_eq!(op["items"][0]["type"], "text");
        assert_eq!(op["items"][0]["text"], "hello");
    }

    #[test]
    fn send_interrupt_format() {
        let backend = CodexBackend;
        let mut buf = TestStdin(Vec::new());
        backend.send_interrupt(&mut buf).unwrap();
        let line = String::from_utf8(buf.0).unwrap();
        let parsed: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["op"]["type"], "interrupt");
    }

    #[test]
    fn send_set_model_format() {
        let backend = CodexBackend;
        let mut buf = TestStdin(Vec::new());
        backend.send_set_model(&mut buf, "gpt-5").unwrap();
        let line = String::from_utf8(buf.0).unwrap();
        let parsed: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["op"]["type"], "override_turn_context");
        assert_eq!(parsed["op"]["model"], "gpt-5");
    }

    #[test]
    fn send_set_permission_mode_bypass() {
        let backend = CodexBackend;
        let mut buf = TestStdin(Vec::new());
        backend
            .send_set_permission_mode(&mut buf, "bypassPermissions")
            .unwrap();
        let line = String::from_utf8(buf.0).unwrap();
        let parsed: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["op"]["approval_policy"], "never");
    }

    #[test]
    fn send_set_permission_mode_accept_edits() {
        let backend = CodexBackend;
        let mut buf = TestStdin(Vec::new());
        backend
            .send_set_permission_mode(&mut buf, "acceptEdits")
            .unwrap();
        let line = String::from_utf8(buf.0).unwrap();
        let parsed: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["op"]["approval_policy"], "on-request");
    }

    #[test]
    fn send_set_permission_mode_default() {
        let backend = CodexBackend;
        let mut buf = TestStdin(Vec::new());
        backend
            .send_set_permission_mode(&mut buf, "default")
            .unwrap();
        let line = String::from_utf8(buf.0).unwrap();
        let parsed: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["op"]["approval_policy"], "on-request");
    }

    #[test]
    fn summarize_command_result_prefers_output() {
        let value = serde_json::json!({
            "aggregatedOutput": "All good",
            "stdout": "ignored",
            "exitCode": 0,
            "status": "completed"
        });
        let (text, is_err) = summarize_command_result(&value);
        assert_eq!(text, "All good");
        assert!(!is_err);
    }

    #[test]
    fn summarize_command_result_handles_exit_code_failure() {
        let value = serde_json::json!({
            "exitCode": 3,
            "status": "failed"
        });
        let (text, is_err) = summarize_command_result(&value);
        assert_eq!(text, "Exit code 3");
        assert!(is_err);
    }

    #[test]
    fn summarize_file_change_result_handles_error() {
        let value = serde_json::json!({
            "status": "failed",
            "error": "patch rejected"
        });
        let (text, is_err) = summarize_file_change_result(&value);
        assert_eq!(text, "Error: patch rejected");
        assert!(is_err);
    }

    #[test]
    fn summarize_file_change_result_completed() {
        let value = serde_json::json!({
            "status": "completed"
        });
        let (text, is_err) = summarize_file_change_result(&value);
        assert_eq!(text, "File changes applied");
        assert!(!is_err);
    }

    #[test]
    fn parse_codex_oauth_tokens_success() {
        let json = r#"{"auth_mode":"chatgpt","tokens":{"id_token":"id","access_token":"acc","refresh_token":"ref"}}"#;
        let tokens = parse_codex_oauth_tokens(json).unwrap();
        assert_eq!(tokens.id_token, "id");
        assert_eq!(tokens.access_token, "acc");
        assert_eq!(tokens.refresh_token.as_deref(), Some("ref"));
        // "acc" is not a valid JWT, so account_id should be None
        assert!(tokens.chatgpt_account_id.is_none());
    }

    #[test]
    fn extract_account_id_from_jwt() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;

        // Build a minimal JWT with chatgpt_account_id in claims
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD
            .encode(r#"{"https://api.openai.com/auth":{"chatgpt_account_id":"test-acct-123"}}"#);
        let jwt = format!("{}.{}.fake-sig", header, payload);

        let result = extract_chatgpt_account_id_from_jwt(&jwt);
        assert_eq!(result.as_deref(), Some("test-acct-123"));
    }

    #[test]
    fn extract_account_id_from_invalid_jwt() {
        assert!(extract_chatgpt_account_id_from_jwt("not-a-jwt").is_none());
        assert!(extract_chatgpt_account_id_from_jwt("a.b.c").is_none()); // invalid base64
    }

    #[test]
    fn parse_codex_oauth_tokens_missing_fields() {
        let json = r#"{"auth_mode":"chatgpt"}"#;
        assert!(parse_codex_oauth_tokens(json).is_err());
    }

    // ---- parse_codex_version ------------------------------------------------

    #[test]
    fn parse_version_with_prefix() {
        assert_eq!(parse_codex_version("codex 0.37.1"), Some((0, 37, 1)));
    }

    #[test]
    fn parse_version_bare() {
        assert_eq!(parse_codex_version("0.37.0"), Some((0, 37, 0)));
    }

    #[test]
    fn parse_version_with_prerelease() {
        assert_eq!(parse_codex_version("1.2.3-beta.1"), Some((1, 2, 3)));
    }

    #[test]
    fn parse_version_major_only() {
        // Only two parts — missing patch
        assert_eq!(parse_codex_version("1.2"), Some((1, 2, 0)));
    }

    #[test]
    fn parse_version_nonsense() {
        assert_eq!(parse_codex_version("not a version"), None);
    }

    #[test]
    fn parse_version_empty() {
        assert_eq!(parse_codex_version(""), None);
    }

    #[test]
    fn parse_version_multiword_prefix() {
        assert_eq!(
            parse_codex_version("OpenAI Codex CLI 0.42.5"),
            Some((0, 42, 5))
        );
    }

    #[test]
    fn version_comparison_ok() {
        // Current minimum is (0, 37, 0)
        let version = (0, 38, 0);
        assert!(version >= MIN_CODEX_VERSION);
    }

    #[test]
    fn version_comparison_exact() {
        let version = (0, 37, 0);
        assert!(version >= MIN_CODEX_VERSION);
    }

    #[test]
    fn version_comparison_too_old() {
        let version = (0, 36, 9);
        assert!(version < MIN_CODEX_VERSION);
    }

    #[test]
    fn extract_app_server_delta_supports_string_shape() {
        let msg = serde_json::json!({
            "params": {
                "delta": "hello"
            }
        });
        assert_eq!(extract_app_server_delta(&msg), Some("hello"));
    }

    #[test]
    fn extract_app_server_delta_supports_legacy_object_shape() {
        let msg = serde_json::json!({
            "params": {
                "delta": {
                    "text": "hello"
                }
            }
        });
        assert_eq!(extract_app_server_delta(&msg), Some("hello"));
    }

    #[test]
    fn extract_command_execution_supports_string_shape() {
        let (display, args) = extract_command_execution(&serde_json::json!("cargo test"));
        assert_eq!(display, "cargo test");
        assert_eq!(args, vec!["cargo test"]);
    }

    #[test]
    fn extract_raw_response_message_text_reads_output_text() {
        let item = serde_json::json!({
            "type": "message",
            "role": "assistant",
            "content": [
                { "type": "output_text", "text": "hello" }
            ]
        });
        assert_eq!(
            extract_raw_response_message_text(&item),
            Some("hello".to_string())
        );
    }

    #[test]
    fn extract_raw_response_message_text_joins_multiple_parts() {
        let item = serde_json::json!({
            "type": "message",
            "role": "assistant",
            "content": [
                { "type": "output_text", "text": "hello" },
                { "type": "output_text", "text": "world" }
            ]
        });
        assert_eq!(
            extract_raw_response_message_text(&item),
            Some("hello\nworld".to_string())
        );
    }

    #[test]
    fn missing_rollout_error_detection_matches_current_app_server_error() {
        let err = r#"{"code":-32600,"message":"no rollout found for thread id db751329-1648-45df-afb6-d09a33efb499"}"#;
        assert!(is_missing_rollout_error(
            err,
            "db751329-1648-45df-afb6-d09a33efb499"
        ));
    }

    #[test]
    fn trim_resume_transcript_keeps_tail_when_over_limit() {
        let trimmed = trim_resume_transcript("abcdefghij", 4);
        assert_eq!(trimmed, "[Earlier transcript omitted]\n\nghij");
    }

    // ---- CodexAuthState (account_id preservation) ---------------------------

    fn make_auth_json(id: &str, access: &str, refresh: Option<&str>) -> String {
        let mut tokens = serde_json::json!({
            "id_token": id,
            "access_token": access,
        });
        if let Some(r) = refresh {
            tokens["refresh_token"] = serde_json::json!(r);
        }
        serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": tokens,
        })
        .to_string()
    }

    #[test]
    fn auth_state_apply_refresh_preserves_account_id_from_original() {
        // Original tokens have an account_id (via JWT), new tokens don't
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;

        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD
            .encode(r#"{"https://api.openai.com/auth":{"chatgpt_account_id":"original-acct"}}"#);
        let jwt_with_acct = format!("{}.{}.sig", header, payload);

        let json = make_auth_json("id1", &jwt_with_acct, Some("ref1"));
        let mut state = CodexAuthState::new(&json).unwrap();
        assert_eq!(state.chatgpt_account_id().as_deref(), Some("original-acct"));

        // Refresh with tokens that have no account_id
        let new_tokens = CodexOAuthTokens {
            id_token: "id2".into(),
            access_token: "plain-access".into(),
            refresh_token: Some("ref2".into()),
            chatgpt_account_id: None,
        };
        state.apply_refresh(new_tokens).unwrap();

        // account_id should be preserved from original
        assert_eq!(state.chatgpt_account_id().as_deref(), Some("original-acct"));
        assert_eq!(state.tokens.access_token, "plain-access");
    }

    #[test]
    fn auth_state_apply_refresh_overrides_account_id_from_new_tokens() {
        let json = make_auth_json("id1", "acc1", Some("ref1"));
        let mut state = CodexAuthState::new(&json).unwrap();
        assert!(state.chatgpt_account_id().is_none());

        // Refresh with tokens that have an account_id
        let new_tokens = CodexOAuthTokens {
            id_token: "id2".into(),
            access_token: "acc2".into(),
            refresh_token: None,
            chatgpt_account_id: Some("new-acct".into()),
        };
        state.apply_refresh(new_tokens).unwrap();

        assert_eq!(state.chatgpt_account_id().as_deref(), Some("new-acct"));
        // refresh_token should fall back to original
        assert_eq!(state.refresh_token().as_deref(), Some("ref1"));
    }

    #[test]
    fn auth_state_apply_refresh_new_account_id_overrides_old() {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;

        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD
            .encode(r#"{"https://api.openai.com/auth":{"chatgpt_account_id":"old-acct"}}"#);
        let jwt = format!("{}.{}.sig", header, payload);

        let json = make_auth_json("id1", &jwt, None);
        let mut state = CodexAuthState::new(&json).unwrap();
        assert_eq!(state.chatgpt_account_id().as_deref(), Some("old-acct"));

        // New tokens provide a different account_id — should override
        let new_tokens = CodexOAuthTokens {
            id_token: "id2".into(),
            access_token: "acc2".into(),
            refresh_token: None,
            chatgpt_account_id: Some("new-acct".into()),
        };
        state.apply_refresh(new_tokens).unwrap();

        assert_eq!(state.chatgpt_account_id().as_deref(), Some("new-acct"));
    }

    // ---- build_thread_start/resume_params with reasoning_effort ---------

    #[test]
    fn thread_start_params_include_reasoning_effort() {
        let params = build_thread_start_params(
            "/tmp",
            "on-request",
            "workspace-write",
            Some(Model::GPT_5_4_MINI),
            Some("high"),
        );
        assert_eq!(params["reasoningEffort"], "high");
        assert_eq!(params["model"], Model::GPT_5_4_MINI);
        assert_eq!(params["cwd"], "/tmp");
        assert_eq!(params["sandbox"], "workspace-write");
    }

    #[test]
    fn thread_start_params_omit_reasoning_effort_when_none() {
        let params = build_thread_start_params(
            "/tmp",
            "on-request",
            "workspace-write",
            Some(Model::GPT_5_4_MINI),
            None,
        );
        assert!(params.get("reasoningEffort").is_none());
    }

    #[test]
    fn thread_resume_params_include_reasoning_effort() {
        let params = build_thread_resume_params(
            "thread-123",
            "/tmp",
            "on-request",
            "danger-full-access",
            Some(Model::GPT_5_4_MINI),
            Some("medium"),
        );
        assert_eq!(params["reasoningEffort"], "medium");
        assert_eq!(params["threadId"], "thread-123");
        assert_eq!(params["sandbox"], "danger-full-access");
    }

    #[test]
    fn thread_resume_params_omit_reasoning_effort_when_none() {
        let params = build_thread_resume_params(
            "thread-123",
            "/tmp",
            "on-request",
            "workspace-write",
            None,
            None,
        );
        assert!(params.get("reasoningEffort").is_none());
    }

    #[test]
    fn thread_fork_params_use_thread_id() {
        let params = build_thread_fork_params(
            "thread-source",
            "/tmp",
            "on-request",
            "danger-full-access",
            Some(Model::GPT_5_4_MINI),
            Some("medium"),
        );
        assert_eq!(params["threadId"], "thread-source");
        assert!(params.get("sourceThreadId").is_none());
        assert_eq!(params["model"], Model::GPT_5_4_MINI);
        assert_eq!(params["reasoningEffort"], "medium");
    }

    // ---- resolve_tools: Write/Edit both resolve to mcp__cairn__edit ------

    #[test]
    fn build_codex_compaction_event_marks_compaction_boundary() {
        let event = build_codex_compaction_event(json!({
            "summary": "Compacted after turn 3",
            "droppedTokens": 2048
        }));

        assert_eq!(event.event_type, "system:compact_boundary");
        assert_eq!(event.content.as_deref(), Some("Context compacted"));
        assert_eq!(
            event
                .raw
                .as_ref()
                .and_then(|raw| raw.get("provider"))
                .and_then(Value::as_str),
            Some("codex")
        );
        assert_eq!(
            event
                .raw
                .as_ref()
                .and_then(|raw| raw.get("compaction"))
                .and_then(|value| value.get("droppedTokens"))
                .and_then(Value::as_i64),
            Some(2048)
        );
    }

    #[test]
    fn codex_rate_limit_snapshot_from_value_tracks_remaining_usage() {
        let snapshot = codex_rate_limit_snapshot_from_value(json!({
            "primary": {
                "usedPercent": 12.0,
                "resetsAt": 1_700_000_000,
                "windowDurationMins": 300
            },
            "secondary": {
                "usedPercent": 40.0,
                "resetsAt": 1_700_000_500,
                "windowDurationMins": 10080
            },
            "credits": {
                "balance": 9.5,
                "currency": "USD"
            }
        }))
        .expect("snapshot should parse");

        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.windows[0].label, "5-hour window");
        assert_eq!(snapshot.windows[0].remaining_percent, 88.0);
        assert_eq!(snapshot.windows[1].scope, ProviderUsageScope::Weekly);
        assert_eq!(
            snapshot
                .credits
                .as_ref()
                .and_then(|credits| credits.balance),
            Some(9.5)
        );
    }

    #[test]
    fn resolve_tools_maps_write_edit_to_cairn_edit() {
        let backend = CodexBackend;
        let tools = vec!["Write".to_string(), "Edit".to_string()];
        let resolved = backend.resolve_tools(&tools, &[]);

        assert!(
            resolved.allowed.contains(&"mcp__cairn__edit".to_string()),
            "Write/Edit should resolve to mcp__cairn__edit"
        );
        // Should appear exactly once (merged pack)
        let edit_count = resolved
            .allowed
            .iter()
            .filter(|t| *t == "mcp__cairn__edit")
            .count();
        assert_eq!(edit_count, 1, "mcp__cairn__edit should appear exactly once");
    }

    #[test]
    fn resolve_tools_strips_native_apply_patch() {
        let backend = CodexBackend;
        let tools = vec!["apply_patch".to_string(), "Write".to_string()];
        let resolved = backend.resolve_tools(&tools, &[]);

        assert!(
            !resolved.allowed.contains(&"apply_patch".to_string()),
            "native apply_patch should not be exposed"
        );
        assert!(resolved.allowed.contains(&"mcp__cairn__edit".to_string()));
    }

    #[test]
    fn disable_apply_patch_in_model_catalog_nulls_tool_type() {
        let rewritten = disable_apply_patch_in_model_catalog(
            r#"{"models":[{"slug":"gpt-5-codex","apply_patch_tool_type":"freeform"},{"slug":"gpt-5.4","apply_patch_tool_type":"function"}]}"#,
        )
        .unwrap();
        let catalog: Value = serde_json::from_str(&rewritten).unwrap();
        let models = catalog["models"].as_array().unwrap();
        assert!(models
            .iter()
            .all(|model| model["apply_patch_tool_type"].is_null()));
    }

    #[test]
    fn disable_apply_patch_in_model_catalog_rejects_missing_models() {
        let err = disable_apply_patch_in_model_catalog(r#"{"not_models":[]}"#).unwrap_err();
        assert!(err.contains("missing models array"));
    }
}

fn is_task_spawned_run(orch: &Orchestrator, run_id: &str) -> bool {
    if let Ok(mut conn) = orch.db.conn.lock() {
        runs::table
            .find(run_id)
            .select(runs::job_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
            .and_then(|job_id| {
                jobs::table
                    .find(&job_id)
                    .select(jobs::parent_job_id)
                    .first::<Option<String>>(&mut *conn)
                    .ok()
                    .flatten()
            })
            .is_some()
    } else {
        false
    }
}
