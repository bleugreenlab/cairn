//! [`CodexAppServer`]: starting `codex app-server` and opening threads on it.
//!
//! One function starts an app-server, one opens a thread, and the thread's
//! [`CodexControl`] starts its turns. A session's own app-server and a pooled
//! one differ only in which [`CodexServer`] the thread opens on.

use std::sync::{Arc, Mutex};

use serde_json::Value;

use super::control::CodexControl;
use super::params::{
    build_thread_fork_params, build_thread_resume_params, build_thread_start_params,
    codex_approval_policy, codex_sandbox_mode,
};
use super::thread::{
    ensure_mcp_attached, reconcile_goal, resume_thread, verify_resumed_thread_binding,
};
use crate::codex::app_server::AppServerClient;
use crate::interface::session::{
    CodexNativeTools, CodexServer, CodexSessionProvider, LaunchedSession, NativeGoal, ProviderSpec,
    ServerLaunch, SessionError, SessionEvents, SessionFacts, SessionHost, SessionProvider,
    SessionSpec, SessionStart,
};
use crate::wire::codex_app_server::errors::is_missing_rollout_error;

/// Opens Codex threads.
#[derive(Debug, Clone, Copy, Default)]
pub struct CodexAppServer;

fn thread_id_of(response: &Value, what: &str) -> Result<String, SessionError> {
    response
        .get("thread")
        .and_then(|thread| thread.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| SessionError::Protocol(format!("Codex {what} response missing thread id")))
}

impl CodexSessionProvider for CodexAppServer {
    fn start_server(
        &self,
        host: &dyn SessionHost,
        launch: ServerLaunch<'_>,
    ) -> Result<Arc<AppServerClient>, SessionError> {
        let mut config = AppServerClient::spawn_config(
            launch.cli_path,
            &launch.mcp.codex_config_overrides(),
            launch.cwd,
        );
        for (name, value) in launch.env {
            config = config.env(name, value);
        }
        for (name, value) in launch.secrets.iter() {
            config = config.env(name, value);
        }
        let client = AppServerClient::launch(|config| host.spawn(config), config)
            .map_err(SessionError::Spawn)?
            .with_mcp_servers(launch.mcp.names());

        client
            .send_request(
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
            )
            .map_err(SessionError::Protocol)?;
        client
            .send_notification("initialized", serde_json::json!({}))
            .map_err(SessionError::Protocol)?;
        if let Some(login) = launch.login {
            client
                .send_request("account/login/start", login.expose().clone())
                .map_err(SessionError::Protocol)?;
        }
        Ok(Arc::new(client))
    }
}

impl SessionProvider for CodexAppServer {
    fn open(
        &self,
        host: &dyn SessionHost,
        spec: SessionSpec,
    ) -> Result<LaunchedSession, SessionError> {
        let SessionSpec {
            start,
            model,
            effort,
            approval,
            output_schema,
            cwd,
            mcp,
            account_id,
            provider,
            ..
        } = spec;
        let ProviderSpec::Codex(mut codex) = provider else {
            return Err(SessionError::Setup(
                "the Codex app-server opens only Codex sessions".to_string(),
            ));
        };
        let tool_free = mcp.is_none();
        if tool_free && !matches!(start, SessionStart::New { .. }) {
            return Err(SessionError::Setup(
                "a tool-free Codex thread can only be a new one".to_string(),
            ));
        }
        if codex.native_tools != CodexNativeTools::None && !tool_free {
            return Err(SessionError::Setup(
                "a Codex-native tool is given back only to a tool-free thread".to_string(),
            ));
        }
        let native_tools = codex.native_tools;
        let (client, dedicated) = match &codex.server {
            CodexServer::Dedicated { router, .. } => (router.client(), true),
            CodexServer::Shared(router) => (router.client(), false),
        };
        let approval_policy = codex_approval_policy(approval);
        let sandbox_mode = codex_sandbox_mode(approval);
        let model = model.as_deref();
        let effort = effort.as_deref();
        let service_tier = codex.service_tier.as_deref();
        let base = codex.base_instructions.as_str();
        let developer = codex.developer_instructions.as_deref();
        let clear_native_goal = codex.native_goal == NativeGoal::Clear;
        let process_mcp_servers = client.mcp_servers().to_vec();
        let start_params = || {
            build_thread_start_params(
                &cwd,
                approval_policy,
                sandbox_mode,
                model,
                effort,
                service_tier,
                base,
                developer,
                tool_free,
                &process_mcp_servers,
                native_tools,
            )
        };
        let mut fallback = codex.resume_fallback.take();
        let mut fallback_text = || fallback.as_mut().and_then(|fallback| fallback());

        // The first turn's text, replaced when the thread is rebuilt.
        let mut opening_text = None;
        let resume_mode = match &start {
            SessionStart::Resume { .. } => "resume",
            SessionStart::Fork { .. } => "fork",
            SessionStart::New { .. } => "new",
        };
        let mut rebuilt = false;
        let thread_response = match &start {
            SessionStart::Resume {
                backend_id,
                session_id,
            } => {
                log::info!(
                    "Codex session start: mode=resume cairn_session_id={} source_backend_id={}",
                    session_id,
                    backend_id
                );
                let resume_params = build_thread_resume_params(
                    backend_id,
                    &cwd,
                    approval_policy,
                    sandbox_mode,
                    model,
                    effort,
                    service_tier,
                    base,
                    developer,
                );
                match resume_thread(&client, backend_id, resume_params, clear_native_goal) {
                    Ok(response) => {
                        log::info!(
                            "Codex session start dispatched thread/resume for cairn_session_id={} source_backend_id={}",
                            session_id,
                            backend_id
                        );
                        response
                    }
                    Err(error) if is_missing_rollout_error(&error, backend_id) => {
                        log::warn!(
                            "Codex thread/resume failed for stale thread {}, starting fresh thread with transcript preload",
                            backend_id
                        );
                        if let Some(text) = fallback_text() {
                            opening_text = Some(text);
                        }
                        let response = client
                            .send_request("thread/start", start_params())
                            .map_err(SessionError::Protocol)?;
                        log::info!(
                            "Codex session start fell back to thread/start for cairn_session_id={} after stale resume source_backend_id={}",
                            session_id,
                            backend_id
                        );
                        rebuilt = true;
                        response
                    }
                    Err(error) => return Err(SessionError::Protocol(error)),
                }
            }
            SessionStart::Fork {
                source_backend_id,
                session_id,
            } => {
                log::info!(
                    "Codex session start: mode=fork cairn_session_id={} source_backend_id={}",
                    session_id,
                    source_backend_id
                );
                let response = client
                    .send_request(
                        "thread/fork",
                        build_thread_fork_params(
                            source_backend_id,
                            &cwd,
                            approval_policy,
                            sandbox_mode,
                            model,
                            effort,
                            service_tier,
                            base,
                            developer,
                        ),
                    )
                    .map_err(SessionError::Protocol)?;
                log::info!(
                    "Codex session start dispatched thread/fork for cairn_session_id={} source_backend_id={}",
                    session_id,
                    source_backend_id
                );
                response
            }
            SessionStart::New { session_id } => {
                log::info!(
                    "Codex session start: mode=new cairn_session_id={}",
                    session_id
                );
                let response = client
                    .send_request("thread/start", start_params())
                    .map_err(SessionError::Protocol)?;
                log::info!(
                    "Codex session start dispatched thread/start for cairn_session_id={}",
                    session_id
                );
                response
            }
        };

        let mut thread_id = thread_id_of(&thread_response, "thread")?;
        // A rebuilt thread is a deliberate replacement; only a resume that
        // answered must be the thread the session is bound to.
        if !rebuilt {
            verify_resumed_thread_binding(&start, &thread_id).map_err(SessionError::Protocol)?;
        }
        log::info!(
            "Codex session start received thread_id={} for cairn_session_id={}",
            thread_id,
            start.session_id()
        );

        // A tool-free thread has its process's servers switched off, so there
        // is no attachment to check. A shared server's set is the pool's, not
        // this spec's (see `CodexServer::Shared`).
        if let (true, Some(servers)) = (dedicated, mcp.as_ref()) {
            if let Err(attachment_error) =
                ensure_mcp_attached(&client, &thread_id, resume_mode, servers)
            {
                let SessionStart::Resume { backend_id, .. } = &start else {
                    return Err(SessionError::McpUnavailable(format!(
                        "Codex Cairn MCP infrastructure unavailable: thread {thread_id} mode={resume_mode}: {attachment_error}"
                    )));
                };
                log::warn!(
                    "Codex resumed thread MCP attachment failed; replacing thread with transcript preload: thread_id={} resume_mode=resume server=cairn startup_state=unavailable reason={}",
                    thread_id,
                    attachment_error
                );
                if let Some(text) = fallback_text() {
                    opening_text = Some(text);
                }
                let replacement = client
                    .send_request("thread/start", start_params())
                    .map_err(SessionError::Protocol)?;
                thread_id = thread_id_of(&replacement, "replacement thread")?;
                if let Err(reason) =
                    ensure_mcp_attached(&client, &thread_id, "resume-transcript-fallback", servers)
                {
                    return Err(SessionError::McpUnavailable(format!(
                        "Codex Cairn MCP infrastructure unavailable: replacement thread {thread_id} for resumed thread {backend_id} failed attachment: {reason}"
                    )));
                }
            }
        }

        let expected_objective = match &codex.native_goal {
            NativeGoal::Objective(objective) => Some(objective.as_str()),
            NativeGoal::Leave | NativeGoal::Clear => None,
        };
        reconcile_goal(&client, &thread_id, expected_objective, clear_native_goal)
            .map_err(SessionError::Protocol)?;

        let current_turn = Arc::new(Mutex::new(None));
        let control = CodexControl::new(
            client.clone(),
            thread_id.clone(),
            cwd.clone(),
            model.map(str::to_string),
            effort.map(str::to_string),
            codex.service_tier.clone(),
            current_turn.clone(),
        )
        .with_opening(output_schema, opening_text);
        let (process, notifications) = match &codex.server {
            CodexServer::Dedicated { events, .. } => (client.child_handle(), Some(events.clone())),
            CodexServer::Shared(_) => (Arc::new(Mutex::new(None)), None),
        };
        Ok(LaunchedSession {
            control: Box::new(control),
            events: SessionEvents::CodexAppServer {
                client,
                notifications,
                current_turn,
            },
            process,
            facts: SessionFacts {
                provider: "codex".to_string(),
                backend_session_id: Some(thread_id),
                account_id,
                host: host.name().to_string(),
                model: model.map(str::to_string),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex::thread_router::{Scoped, ThreadRouter};
    use crate::interface::session::{
        ApprovalMode, Cli, CodexSpec, Secret, SecretEnv, ServerReply, SessionControl, SessionError,
    };
    use crate::session::mcp::{McpServer, McpServers};
    use crate::wire::MessageContent;
    use cairn_platform::services::process::{ChildProcess, SpawnConfig};
    use crossbeam_channel::{unbounded, Receiver, Sender};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::time::{Duration, Instant};

    type Requests = Arc<Mutex<Vec<Value>>>;

    /// A host whose one app-server answers every request from `respond`.
    struct ScriptedHost {
        spawned: Mutex<Vec<SpawnConfig>>,
        requests: Requests,
        respond: fn(&Value) -> Value,
    }

    impl SessionHost for ScriptedHost {
        fn name(&self) -> &str {
            "test-host"
        }
        fn resolve_cli(&self, _cli: Cli) -> Result<String, SessionError> {
            Ok("/bin/codex".to_string())
        }
        fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
            self.spawned.lock().unwrap().push(config);
            let (tx, rx) = unbounded();
            Ok(Box::new(ScriptedChild {
                stdin: Some(Box::new(ScriptedStdin {
                    tx,
                    requests: self.requests.clone(),
                    respond: self.respond,
                    buffer: Vec::new(),
                })),
                stdout: Some(rx),
            }))
        }
        fn place_file(&self, _: &str, _: &str, _: &[u8]) -> Result<String, String> {
            unreachable!("codex places no files")
        }
        fn prepare_codex_home(
            &self,
            _: crate::interface::session::CodexModels<'_>,
        ) -> Result<String, SessionError> {
            Ok("/codex-home".to_string())
        }
    }

    struct ScriptedChild {
        stdin: Option<Box<dyn Write + Send>>,
        stdout: Option<Receiver<Vec<u8>>>,
    }

    /// A child's stdin that hands each line the client writes to a fake
    /// server's inbox, for a server that answers on a thread of its own.
    struct Inbox {
        tx: Sender<Value>,
        buffer: Vec<u8>,
    }

    impl Write for Inbox {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buffer.extend_from_slice(buf);
            while let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') {
                let line: Vec<u8> = self.buffer.drain(..=end).collect();
                let _ = self.tx.send(serde_json::from_slice(&line).unwrap());
            }
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct ScriptedStdin {
        tx: Sender<Vec<u8>>,
        requests: Requests,
        respond: fn(&Value) -> Value,
        buffer: Vec<u8>,
    }

    impl Write for ScriptedStdin {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.buffer.extend_from_slice(buf);
            while let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') {
                let line: Vec<u8> = self.buffer.drain(..=end).collect();
                let message: Value = serde_json::from_slice(&line).unwrap();
                self.requests.lock().unwrap().push(message.clone());
                if let Some(id) = message.get("id") {
                    let reply =
                        json!({"jsonrpc": "2.0", "id": id, "result": (self.respond)(&message)});
                    let _ = self.tx.send(format!("{reply}\n").into_bytes());
                }
            }
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct ChannelReader {
        rx: Receiver<Vec<u8>>,
        pending: Vec<u8>,
    }

    impl Read for ChannelReader {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.pending.is_empty() {
                match self.rx.recv() {
                    Ok(bytes) => self.pending = bytes,
                    Err(_) => return Ok(0),
                }
            }
            let n = out.len().min(self.pending.len());
            out[..n].copy_from_slice(&self.pending[..n]);
            self.pending.drain(..n);
            Ok(n)
        }
    }

    impl ChildProcess for ScriptedChild {
        fn id(&self) -> u32 {
            7
        }
        fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
            self.stdout.take().map(|rx| {
                Box::new(BufReader::new(ChannelReader {
                    rx,
                    pending: Vec::new(),
                })) as Box<dyn BufRead + Send>
            })
        }
        fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
            None
        }
        fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
            self.stdin.take()
        }
        fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            Ok(None)
        }
        fn kill(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn empty_result(_: &Value) -> Value {
        json!({})
    }

    fn host(respond: fn(&Value) -> Value) -> ScriptedHost {
        ScriptedHost {
            spawned: Mutex::new(Vec::new()),
            requests: Arc::new(Mutex::new(Vec::new())),
            respond,
        }
    }

    fn mcp() -> McpServers {
        McpServers::new(vec![McpServer::cairn_http("https://orch.example/mcp")]).unwrap()
    }

    /// The server start sends what every Codex launch has sent: initialize,
    /// the initialized notification, then the caller's login, unchanged.
    #[test]
    fn a_server_starts_initialized_and_signed_in_as_before() {
        let host = host(empty_result);
        let env = BTreeMap::from([("CODEX_HOME".to_string(), "/codex-home".to_string())]);
        let secrets = SecretEnv::new().with("CAIRN_MCP_TOKEN", "bearer-1");
        let login = Secret::new(json!({"type": "apiKey", "apiKey": "sk-1"}));
        CodexAppServer
            .start_server(
                &host,
                ServerLaunch {
                    cli_path: "/bin/codex",
                    cwd: "/work",
                    env: &env,
                    secrets: &secrets,
                    mcp: &mcp(),
                    login: Some(&login),
                },
            )
            .unwrap();

        let spawned = host.spawned.lock().unwrap();
        assert_eq!(spawned[0].program, "/bin/codex");
        assert_eq!(
            spawned[0].args,
            vec![
                "app-server".to_string(),
                "-c".to_string(),
                mcp().codex_config_overrides().remove(0)
            ]
        );
        assert_eq!(spawned[0].cwd.as_deref(), Some("/work"));
        assert_eq!(spawned[0].env.get("CODEX_HOME").unwrap(), "/codex-home");
        assert_eq!(spawned[0].env.get("CAIRN_MCP_TOKEN").unwrap(), "bearer-1");

        let requests = host.requests.lock().unwrap();
        assert_eq!(requests[0]["method"], "initialize");
        assert_eq!(
            requests[0]["params"],
            json!({
                "clientInfo": {"name": "cairn", "title": "Cairn", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"experimentalApi": true}
            })
        );
        assert_eq!(
            requests[1],
            json!({"jsonrpc": "2.0", "method": "initialized", "params": {}})
        );
        assert_eq!(requests[2]["method"], "account/login/start");
        assert_eq!(
            requests[2]["params"],
            json!({"type": "apiKey", "apiKey": "sk-1"})
        );
        assert_eq!(requests.len(), 3);
    }

    fn ready_status() -> Value {
        json!({"data": [{"name": "cairn", "tools": {"read": {}, "run": {}, "write": {}}}]})
    }

    fn scripted_client(
        respond: impl FnMut(&Value) -> Result<Value, String> + Send + 'static,
    ) -> (Arc<AppServerClient>, Requests) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(AppServerClient::for_test_responding(
            respond,
            requests.clone(),
        ));
        (client, requests)
    }

    /// `client` as a shared server, routed by a router its test feeds.
    fn shared(client: Arc<AppServerClient>) -> CodexServer {
        CodexServer::Shared(ThreadRouter::new(client, |_, _| Scoped::Passed))
    }

    /// `client` as a session's own server, whose router answers nothing.
    fn dedicated(client: Arc<AppServerClient>) -> CodexServer {
        let (router, events) = ThreadRouter::start_owned(client, |_, _| Scoped::Passed);
        CodexServer::Dedicated { router, events }
    }

    fn spec(start: SessionStart, server: CodexServer, goal: NativeGoal) -> SessionSpec {
        SessionSpec {
            start,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            approval: ApprovalMode::Ask,
            output_schema: Some(json!({"type": "object", "properties": {"a": {"type": "string"}}})),
            cwd: "/work".to_string(),
            env: BTreeMap::new(),
            mcp: Some(mcp()),
            secrets: SecretEnv::new(),
            account_id: Some("acct-1".to_string()),
            provider: ProviderSpec::Codex(CodexSpec {
                server,
                base_instructions: "BASE".to_string(),
                developer_instructions: Some("AGENT".to_string()),
                service_tier: Some("priority".to_string()),
                native_goal: goal,
                native_tools: CodexNativeTools::None,
                resume_fallback: Some(Box::new(|| Some("TRANSCRIPT + latest".to_string()))),
            }),
        }
    }

    fn methods(requests: &Requests) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request["method"].as_str().unwrap().to_string())
            .collect()
    }

    fn goal(objective: &str) -> Value {
        json!({"goal": {"threadId": "thread-new", "objective": objective, "status": "active",
            "tokenBudget": null, "tokensUsed": 0, "timeUsedSeconds": 0, "createdAt": 1, "updatedAt": 1}})
    }

    /// A new dedicated thread: today's thread params, the attachment check, the
    /// goal set, and a first turn carrying the normalized output schema but
    /// not effort or tier.
    #[test]
    fn a_dedicated_thread_opens_and_starts_its_first_turn_as_before() {
        let (client, requests) = scripted_client(|request| {
            Ok(match request["method"].as_str().unwrap() {
                "thread/start" => json!({"result": {"thread": {"id": "thread-new"}}}),
                "mcpServerStatus/list" => json!({"result": ready_status()}),
                "thread/goal/get" => json!({"result": {"goal": null}}),
                "thread/goal/set" => json!({"result": goal("do the work")}),
                "turn/start" => json!({"result": {"turn": {"id": "turn-1"}}}),
                other => panic!("unexpected {other}"),
            })
        });
        let host = host(empty_result);
        let mut launched = CodexAppServer
            .open(
                &host,
                spec(
                    SessionStart::New {
                        session_id: "session-1".to_string(),
                    },
                    dedicated(client),
                    NativeGoal::Objective("do the work".to_string()),
                ),
            )
            .unwrap();
        assert_eq!(
            launched.facts.backend_session_id.as_deref(),
            Some("thread-new")
        );
        assert_eq!(launched.facts.account_id.as_deref(), Some("acct-1"));
        launched
            .control
            .open_turn(&MessageContent::text("hello"))
            .unwrap();

        assert_eq!(
            methods(&requests),
            [
                "thread/start",
                "mcpServerStatus/list",
                "thread/goal/get",
                "thread/goal/set",
                "turn/start"
            ]
        );
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests[0]["params"],
            json!({
                "model": "gpt-test", "cwd": "/work", "approvalPolicy": "on-request",
                "sandbox": "workspace-write", "serviceName": "cairn", "baseInstructions": "BASE",
                "config": {"model_reasoning_effort": "high"},
                "serviceTier": "priority", "developerInstructions": "AGENT"
            })
        );
        assert_eq!(
            requests[4]["params"],
            json!({
                "threadId": "thread-new",
                "cwd": "/work",
                "input": crate::wire::codex_app_server::build_codex_input(&MessageContent::text("hello")).unwrap(),
                "model": "gpt-test",
                "outputSchema": super::super::params::codex_strict_output_schema(
                    &json!({"type": "object", "properties": {"a": {"type": "string"}}})
                ),
            })
        );
        // A later turn is a plain `turn/start` with effort and tier.
        drop(requests);
    }

    /// A pooled thread opens on the shared server with no attachment check and
    /// no process of its own.
    #[test]
    fn a_pooled_thread_opens_without_the_attachment_check() {
        let (client, requests) = scripted_client(|request| {
            Ok(match request["method"].as_str().unwrap() {
                "thread/start" => json!({"result": {"thread": {"id": "thread-call"}}}),
                "turn/start" => json!({"result": {"turn": {"id": "turn-1"}}}),
                other => panic!("unexpected {other}"),
            })
        });
        let host = host(empty_result);
        let mut launched = CodexAppServer
            .open(
                &host,
                spec(
                    SessionStart::New {
                        session_id: "session-1".to_string(),
                    },
                    shared(client),
                    NativeGoal::Leave,
                ),
            )
            .unwrap();
        assert!(launched.process.lock().unwrap().is_none());
        assert!(matches!(
            launched.events,
            SessionEvents::CodexAppServer {
                notifications: None,
                ..
            }
        ));
        launched
            .control
            .open_turn(&MessageContent::text("call"))
            .unwrap();
        assert_eq!(methods(&requests), ["thread/start", "turn/start"]);
    }

    /// A resume whose thread is gone rebuilds a fresh one, and the first turn
    /// carries the transcript instead of the bare message.
    #[test]
    fn a_stale_resume_rebuilds_and_the_first_turn_carries_the_transcript() {
        let (client, requests) =
            scripted_client(|request| match request["method"].as_str().unwrap() {
                "thread/resume" => Err("no rollout found for thread id thread-old".to_string()),
                "thread/start" => Ok(json!({"result": {"thread": {"id": "thread-rebuilt"}}})),
                "mcpServerStatus/list" => Ok(json!({"result": ready_status()})),
                "turn/start" => Ok(json!({"result": {"turn": {"id": "turn-1"}}})),
                other => panic!("unexpected {other}"),
            });
        let host = host(empty_result);
        let mut launched = CodexAppServer
            .open(
                &host,
                spec(
                    SessionStart::Resume {
                        session_id: "session-1".to_string(),
                        backend_id: "thread-old".to_string(),
                    },
                    dedicated(client),
                    NativeGoal::Leave,
                ),
            )
            .unwrap();
        assert_eq!(
            launched.facts.backend_session_id.as_deref(),
            Some("thread-rebuilt")
        );
        launched
            .control
            .open_turn(&MessageContent::text("latest"))
            .unwrap();
        assert_eq!(
            methods(&requests),
            [
                "thread/resume",
                "thread/start",
                "mcpServerStatus/list",
                "turn/start"
            ]
        );
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests[3]["params"]["input"],
            crate::wire::codex_app_server::build_codex_input(&MessageContent::text(
                "TRANSCRIPT + latest"
            ))
            .unwrap()
        );
    }

    /// A dedicated thread whose Cairn MCP server never attaches is refused with
    /// the text the run's transcript has always shown.
    #[test]
    fn a_missing_attachment_is_reported_as_before() {
        let (client, _requests) = scripted_client(|request| {
            Ok(match request["method"].as_str().unwrap() {
                "thread/start" => json!({"result": {"thread": {"id": "thread-new"}}}),
                "mcpServerStatus/list" => json!({"result": {"data": []}}),
                "config/mcpServer/reload" => json!({"result": {}}),
                other => panic!("unexpected {other}"),
            })
        });
        let host = host(empty_result);
        let error = CodexAppServer
            .open(
                &host,
                spec(
                    SessionStart::New {
                        session_id: "session-1".to_string(),
                    },
                    dedicated(client),
                    NativeGoal::Leave,
                ),
            )
            .err()
            .unwrap();
        let SessionError::McpUnavailable(text) = error else {
            panic!("{error:?}");
        };
        assert!(
            text.starts_with(
                "Codex Cairn MCP infrastructure unavailable: thread thread-new mode=new:"
            ),
            "{text}"
        );
    }

    /// A tool-free thread is started with the tool-free params and, having no
    /// `cairn` server, is not checked for one, even on its own server.
    #[test]
    fn a_tool_free_thread_opens_without_its_server_and_skips_the_attachment_check() {
        let (client, requests) = scripted_client(|request| {
            Ok(match request["method"].as_str().unwrap() {
                "thread/start" => json!({"result": {"thread": {"id": "thread-free"}}}),
                other => panic!("unexpected {other}"),
            })
        });
        let host = host(empty_result);
        let mut tool_free = spec(
            SessionStart::New {
                session_id: "session-1".to_string(),
            },
            dedicated(client),
            NativeGoal::Leave,
        );
        tool_free.mcp = None;
        CodexAppServer.open(&host, tool_free).unwrap();
        assert_eq!(methods(&requests), ["thread/start"]);
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0]["params"]["ephemeral"], true);
        assert_eq!(requests[0]["params"]["environments"], json!([]));
        assert_eq!(requests[0]["params"]["sandbox"], "read-only");
    }

    /// A token refresh Codex asks for while `thread/start` is pending is
    /// answered before the start returns. This server holds its `thread/start`
    /// answer until the refresh is answered or five seconds pass (Codex itself
    /// gives up on the refresh after ten), and what the router does not answer
    /// still reaches the session, in order.
    #[test]
    fn a_dedicated_server_answers_a_refresh_asked_for_while_its_thread_starts() {
        let (inbox_tx, inbox) = unbounded::<Value>();
        let (stdout_tx, stdout_rx) = unbounded::<Vec<u8>>();
        let order = Arc::new(Mutex::new(Vec::new()));
        let server_order = order.clone();
        std::thread::spawn(move || {
            let say = |message: Value| {
                let _ = stdout_tx.send(format!("{message}\n").into_bytes());
            };
            for request in inbox.iter() {
                if request["method"] != "thread/start" {
                    continue;
                }
                say(json!({"method": "thread/started", "params": {"threadId": "thread-new"}}));
                say(
                    json!({"id": 0, "method": "account/chatgptAuthTokens/refresh",
                    "params": {"reason": "unauthorized"}}),
                );
                let deadline = Instant::now() + Duration::from_secs(5);
                let reply = loop {
                    match inbox.recv_deadline(deadline) {
                        Ok(reply) if reply["id"] == 0 => break Some(reply),
                        Ok(_) => continue,
                        Err(_) => break None,
                    }
                };
                let mut order = server_order.lock().unwrap();
                order.push(match reply {
                    Some(reply) => format!("refresh answered: {}", reply["result"]["accessToken"]),
                    None => "refresh unanswered".to_string(),
                });
                order.push("thread/start answered".to_string());
                say(json!({"id": request["id"], "result": {"thread": {"id": "thread-new"}}}));
            }
        });
        let client = AppServerClient::launch(
            move |_| {
                Ok(Box::new(ScriptedChild {
                    stdin: Some(Box::new(Inbox {
                        tx: inbox_tx,
                        buffer: Vec::new(),
                    })),
                    stdout: Some(stdout_rx),
                }))
            },
            SpawnConfig::new("/bin/codex"),
        )
        .unwrap();
        let (router, events) = ThreadRouter::start_owned(Arc::new(client), |client, message| {
            if message["method"] != "account/chatgptAuthTokens/refresh" {
                return Scoped::Passed;
            }
            let tokens = json!({"accessToken": "fresh", "chatgptAccountId": "acct"});
            client
                .reply(&message["id"], ServerReply::Result(tokens))
                .unwrap();
            Scoped::Answered
        });
        let mut tool_free = spec(
            SessionStart::New {
                session_id: "session-1".to_string(),
            },
            CodexServer::Dedicated { router, events },
            NativeGoal::Leave,
        );
        tool_free.mcp = None;

        let launched = CodexAppServer.open(&host(empty_result), tool_free).unwrap();

        assert_eq!(
            *order.lock().unwrap(),
            ["refresh answered: \"fresh\"", "thread/start answered"]
        );
        let SessionEvents::CodexAppServer {
            notifications: Some(notifications),
            ..
        } = launched.events
        else {
            panic!("a dedicated thread has its notifications");
        };
        let first = notifications.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(first["method"], "thread/started");
        assert!(
            notifications.try_recv().is_err(),
            "the refresh was the router's to answer, not the session's"
        );
    }

    /// Only a new thread can be tool-free: a resume or fork would continue a
    /// conversation that had tools.
    #[test]
    fn a_tool_free_resume_or_fork_is_refused() {
        for start in [
            SessionStart::Resume {
                session_id: "session-1".to_string(),
                backend_id: "thread-old".to_string(),
            },
            SessionStart::Fork {
                session_id: "session-1".to_string(),
                source_backend_id: "thread-old".to_string(),
            },
        ] {
            let (client, requests) =
                scripted_client(|request| panic!("no request expected, got {}", request["method"]));
            let mut tool_free = spec(start, shared(client), NativeGoal::Leave);
            tool_free.mcp = None;
            let error = CodexAppServer
                .open(&host(empty_result), tool_free)
                .err()
                .unwrap();
            assert!(matches!(error, SessionError::Setup(_)), "{error:?}");
            assert!(requests.lock().unwrap().is_empty());
        }
    }

    /// A native tool is given back only on a thread without tools: on one
    /// with them it would ride beside the host's servers.
    #[test]
    fn a_native_tool_on_a_thread_with_tools_is_refused() {
        let (client, requests) =
            scripted_client(|request| panic!("no request expected, got {}", request["method"]));
        let mut with_tools = spec(
            SessionStart::New {
                session_id: "session-1".to_string(),
            },
            shared(client),
            NativeGoal::Leave,
        );
        let ProviderSpec::Codex(codex) = &mut with_tools.provider else {
            unreachable!()
        };
        codex.native_tools = CodexNativeTools::ImageGeneration;
        let error = CodexAppServer
            .open(&host(empty_result), with_tools)
            .err()
            .unwrap();
        assert!(matches!(error, SessionError::Setup(_)), "{error:?}");
        assert!(requests.lock().unwrap().is_empty());
    }

    #[test]
    fn a_later_turn_carries_effort_and_tier() {
        let (client, requests) = scripted_client(|_| Ok(json!({"result": {"turn": {"id": "t"}}})));
        let mut control = CodexControl::new(
            client,
            "thread-1".to_string(),
            "/work".to_string(),
            Some("gpt-test".to_string()),
            Some("high".to_string()),
            Some("priority".to_string()),
            Arc::new(Mutex::new(None)),
        );
        control
            .send(&crate::interface::session::SessionInput {
                content: &MessageContent::text("again"),
                session_id: "session-1",
                parent_tool_use_id: None,
            })
            .unwrap();
        let requests = requests.lock().unwrap();
        // Exactly the keys Codex 0.163.0's `TurnStartParams` declares: the
        // effort field is `effort`.
        let params = &requests[0]["params"];
        let mut keys: Vec<&str> = params
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["cwd", "effort", "input", "model", "serviceTier", "threadId"]
        );
        assert_eq!(params["effort"], "high");
        assert_eq!(params["serviceTier"], "priority");
    }
}
