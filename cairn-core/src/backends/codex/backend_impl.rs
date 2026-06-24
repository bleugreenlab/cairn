use super::app_server::AppServerClient;
use super::auth::{find_codex_oauth_account_id, CodexAuthState};
use super::config::{write_codex_config, write_codex_config_for_provider};
use super::models::discover_codex_models;
use super::protocol::CodexStdin;
use super::thread_params::{
    build_resume_fallback_prompt, build_thread_fork_params, build_thread_resume_params,
    build_thread_start_params, codex_sandbox_mode, is_missing_rollout_error,
};
use super::version::check_codex_version;
use super::{CodexAppServerProfile, CodexBackend, CODEX_BACKEND_NAME};
use crate::agent_process::process::{ActiveProcess, BackendStdin};
use crate::backends::run_state::{run_job_id, set_session_backend_id, transition_run_to_live};
use crate::identity::CodexAuth;
use crate::orchestrator::session::{
    assemble_prompt_segments, base_instructions_from_segments,
    developer_instructions_from_segments, insert_error_event, persist_system_prompt_event,
};
use crate::orchestrator::Orchestrator;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use super::super::{
    AgentBackend, DiscoveredModel, OptionChoice, OptionKind, ProviderOptionDescriptor,
    ProviderOptionKey, ResolvedTools, SessionConfig,
};

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

    fn option_descriptors(&self) -> Vec<ProviderOptionDescriptor> {
        vec![
            ProviderOptionDescriptor {
                key: ProviderOptionKey::ReasoningEffort,
                label: "Effort".to_string(),
                kind: OptionKind::Enum,
                choices: ["low", "medium", "high", "xhigh"]
                    .into_iter()
                    .map(|value| OptionChoice {
                        value: value.to_string(),
                        label: value.to_string(),
                    })
                    .collect(),
                default: None,
            },
            ProviderOptionDescriptor {
                key: ProviderOptionKey::FastMode,
                label: "Fast mode".to_string(),
                kind: OptionKind::Boolean,
                choices: Vec::new(),
                default: None,
            },
        ]
    }

    fn resolve_tools(&self, agent_tools: &[String], _agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;

        // Resolve agent tool names to the Cairn allow-list (same canonical
        // resolution as Claude).
        let mut allowed = toolkits::resolve_tools(agent_tools);

        // Temporary permissions floor: the three core verbs are always allowed
        // for every agent (CAIRN-1172).
        toolkits::ensure_core_verbs(&mut allowed);

        allowed.retain(|tool| tool != "apply_patch");

        // Auto-add submission tool
        if !allowed.contains(&"mcp__cairn__return".to_string()) {
            allowed.push("mcp__cairn__return".to_string());
        }

        // Codex ignores `--disallowedTools` — tool access is governed by its own
        // MCP/approval config. Native-off is enforced by simply not allowing
        // native tools (resolve_tools already drops them), so the disallow list
        // stays empty.
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
            &config.mcp_config_json,
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
            Some(CodexAuth::OAuthToken(json)) => {
                let account_id = find_codex_oauth_account_id(orch, json);
                Some(Arc::new(Mutex::new(
                    CodexAuthState::new_for_account(json, account_id).map_err(|e| {
                        insert_error_event(
                            orch,
                            &config.run_id,
                            session_id.as_deref(),
                            &format!("Invalid Codex OAuth tokens: {}", e),
                        );
                        e
                    })?,
                )))
            }
            _ => None,
        };

        let mut env = HashMap::new();
        env.insert("CODEX_HOME".to_string(), codex_home.clone());
        env.insert("CAIRN_RUN_ID".to_string(), config.run_id.clone());
        env.insert("CAIRN_MCP_SECRET".to_string(), mcp_secret.clone());
        env.insert("CAIRN_HOME_URI".to_string(), config.home_uri.clone());

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

        let approval_policy = match config.permissions.fence {
            crate::models::Fence::Allow => "never",
            _ => "on-request",
        };
        let sandbox_mode = codex_sandbox_mode(config.permissions.fence);
        let model_str = config.model.as_ref().map(|m| m.to_string());
        let workspace_instructions = crate::workspace::instructions::read_workspace_instructions();
        let project_instructions = crate::workspace::instructions::read_project_instructions(
            std::path::Path::new(&config.working_dir),
        );
        // Assemble the uniform segment stack once, then derive Codex's two
        // payloads by slicing it: baseInstructions = cairn + workspace + project,
        // developerInstructions = agent + orientation. Sent and persisted bytes
        // are then equal by construction.
        let prompt_segments = assemble_prompt_segments(
            &crate::system_prompt::cairn_system_prompt(),
            workspace_instructions.as_deref(),
            project_instructions.as_deref(),
            config.system_prompt_content.as_deref(),
            config.system_prompt_dynamic_tail.as_deref(),
        );
        let base_instructions = base_instructions_from_segments(&prompt_segments);
        let developer_instructions = developer_instructions_from_segments(&prompt_segments);

        match (codex_auth.as_ref(), oauth_state.as_ref()) {
            (Some(CodexAuth::OAuthToken(_)), Some(state)) => {
                let guard = state
                    .lock()
                    .map_err(|_| "Codex auth state lock poisoned".to_string())?;
                let (id_token, access_token) = guard.id_access_pair();
                let account_id = guard
                    .chatgpt_account_id()
                    .ok_or_else(|| "Missing ChatGPT account id in Codex auth tokens".to_string())?;
                drop(guard);
                let login_params = serde_json::json!({
                    "type": "chatgptAuthTokens",
                    "idToken": id_token,
                    "accessToken": access_token,
                    "chatgptAccountId": account_id,
                });
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
            crate::backends::SessionStart::Resume {
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
                    &config.working_dir,
                    approval_policy,
                    sandbox_mode,
                    model_str.as_deref(),
                    config.reasoning_effort.as_deref(),
                    config.service_tier.as_deref(),
                    &base_instructions,
                    developer_instructions.as_deref(),
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
                                config.service_tier.as_deref(),
                                &base_instructions,
                                developer_instructions.as_deref(),
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
                        config.service_tier.as_deref(),
                        &base_instructions,
                        developer_instructions.as_deref(),
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
                        config.service_tier.as_deref(),
                        &base_instructions,
                        developer_instructions.as_deref(),
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
            let _ = set_session_backend_id(CODEX_BACKEND_NAME, orch, sid, &thread_id);
        }

        let initial_sequence = persist_system_prompt_event(
            orch,
            &config.run_id,
            session_id.as_deref(),
            "codex",
            &prompt_segments,
        );

        let mut turn_params = serde_json::json!({
            "threadId": thread_id.clone(),
            "cwd": config.working_dir,
            "input": [{ "type": "text", "text": prompt_text }]
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
        if let Err(e) = transition_run_to_live(CODEX_BACKEND_NAME, orch, &config.run_id) {
            log::warn!("Failed to transition codex run to running: {}", e);
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
                config.service_tier.clone(),
                current_turn_id.clone(),
            )) as Box<dyn BackendStdin>)));

        let process_job_id: Option<String> = run_job_id(CODEX_BACKEND_NAME, orch, &config.run_id);

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
            active_process.model = config.model.as_ref().map(|m| m.as_str().to_string());
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
                initial_sequence,
                "codex".to_string(),
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
            Err("Codex app-server stdin unavailable".to_string())
        }
    }

    fn send_interrupt(&self, stdin: &mut dyn BackendStdin) -> Result<(), String> {
        if let Some(app_stdin) = stdin.as_any_mut().downcast_mut::<CodexStdin>() {
            app_stdin.interrupt()
        } else {
            Err("Codex app-server stdin unavailable".to_string())
        }
    }

    fn send_set_model(&self, _stdin: &mut dyn BackendStdin, _model: &str) -> Result<(), String> {
        Err("Changing Codex model mid-turn not yet supported".to_string())
    }

    fn send_set_permission_mode(
        &self,
        _stdin: &mut dyn BackendStdin,
        _mode: &str,
    ) -> Result<(), String> {
        Err("Changing Codex permission mode mid-session is not yet supported".to_string())
    }
}

pub(crate) fn start_app_server_session(
    config: SessionConfig,
    orch: &Orchestrator,
    profile: CodexAppServerProfile,
) -> Result<(), String> {
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

    let codex_home = write_codex_config_for_provider(
        &orch.mcp_binary_path,
        orch.mcp_callback_port,
        &config.mcp_config_json,
        profile.model_provider.as_ref(),
    )?;

    let identity = config
        .identity
        .as_ref()
        .cloned()
        .or_else(|| orch.get_identity());
    let codex_auth = identity.as_ref().and_then(|i| i.codex_auth.clone());
    if profile.require_codex_auth && codex_auth.is_none() {
        insert_error_event(
            orch,
            &config.run_id,
            session_id.as_deref(),
            "Codex credentials not configured. Run `connect_codex_auth`.",
        );
        return Err("Missing Codex credentials".to_string());
    }

    let oauth_state = match codex_auth.as_ref() {
        Some(CodexAuth::OAuthToken(json)) => {
            let account_id = find_codex_oauth_account_id(orch, json);
            Some(Arc::new(Mutex::new(
                CodexAuthState::new_for_account(json, account_id).map_err(|e| {
                    insert_error_event(
                        orch,
                        &config.run_id,
                        session_id.as_deref(),
                        &format!("Invalid Codex OAuth tokens: {}", e),
                    );
                    e
                })?,
            )))
        }
        _ => None,
    };

    let mut env = HashMap::new();
    env.insert("CODEX_HOME".to_string(), codex_home.clone());
    env.insert("CAIRN_RUN_ID".to_string(), config.run_id.clone());
    env.insert("CAIRN_MCP_SECRET".to_string(), mcp_secret.clone());
    env.insert("CAIRN_HOME_URI".to_string(), config.home_uri.clone());
    if let Some((key, value)) = profile.api_key_env.as_ref() {
        env.insert((*key).to_string(), value.clone());
    }

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

    let approval_policy = match config.permissions.fence {
        crate::models::Fence::Allow => "never",
        _ => "on-request",
    };
    let sandbox_mode = codex_sandbox_mode(config.permissions.fence);
    let model_str = config.model.as_ref().map(|m| m.to_string());
    let workspace_instructions = crate::workspace::instructions::read_workspace_instructions();
    let project_instructions = crate::workspace::instructions::read_project_instructions(
        std::path::Path::new(&config.working_dir),
    );
    // Assemble the uniform segment stack once, then derive Codex's two payloads
    // by slicing it: baseInstructions = cairn + workspace + project,
    // developerInstructions = agent + orientation. Sent and persisted bytes are
    // then equal by construction.
    let prompt_segments = assemble_prompt_segments(
        &crate::system_prompt::cairn_system_prompt(),
        workspace_instructions.as_deref(),
        project_instructions.as_deref(),
        config.system_prompt_content.as_deref(),
        config.system_prompt_dynamic_tail.as_deref(),
    );
    let base_instructions = base_instructions_from_segments(&prompt_segments);
    let developer_instructions = developer_instructions_from_segments(&prompt_segments);

    if profile.require_codex_auth {
        match (codex_auth.as_ref(), oauth_state.as_ref()) {
            (Some(CodexAuth::OAuthToken(_)), Some(state)) => {
                let guard = state
                    .lock()
                    .map_err(|_| "Codex auth state lock poisoned".to_string())?;
                let (id_token, access_token) = guard.id_access_pair();
                let account_id = guard
                    .chatgpt_account_id()
                    .ok_or_else(|| "Missing ChatGPT account id in Codex auth tokens".to_string())?;
                drop(guard);
                let login_params = serde_json::json!({
                    "type": "chatgptAuthTokens",
                    "idToken": id_token,
                    "accessToken": access_token,
                    "chatgptAccountId": account_id,
                });
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
    }

    let mut prompt_text = config.prompt.clone();
    let thread_resp = match &config.session_start {
        crate::backends::SessionStart::Resume {
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
                &config.working_dir,
                approval_policy,
                sandbox_mode,
                model_str.as_deref(),
                config.reasoning_effort.as_deref(),
                config.service_tier.as_deref(),
                &base_instructions,
                developer_instructions.as_deref(),
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
                            config.service_tier.as_deref(),
                            &base_instructions,
                            developer_instructions.as_deref(),
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
                    config.service_tier.as_deref(),
                    &base_instructions,
                    developer_instructions.as_deref(),
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
                    config.service_tier.as_deref(),
                    &base_instructions,
                    developer_instructions.as_deref(),
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
        let _ = set_session_backend_id(profile.backend_name, orch, sid, &thread_id);
    }

    let initial_sequence = persist_system_prompt_event(
        orch,
        &config.run_id,
        session_id.as_deref(),
        profile.backend_key,
        &prompt_segments,
    );

    let mut turn_params = serde_json::json!({
        "threadId": thread_id.clone(),
        "cwd": config.working_dir,
        "input": [{ "type": "text", "text": prompt_text }]
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
    if let Err(e) = transition_run_to_live(profile.backend_name, orch, &config.run_id) {
        log::warn!("Failed to transition codex run to running: {}", e);
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
            config.service_tier.clone(),
            current_turn_id.clone(),
        )) as Box<dyn BackendStdin>)));

    let process_job_id: Option<String> = run_job_id(profile.backend_name, orch, &config.run_id);

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
        active_process.backend = Some(profile.backend_key.to_string());
        active_process.model = config.model.as_ref().map(|m| m.as_str().to_string());
        processes.register(config.run_id.clone(), active_process);
    }

    let notification_rx = client.notifications();
    let run_id = config.run_id.clone();
    let orch_clone = orch.clone();
    let emitter = orch.services.emitter.clone();
    // Use the Cairn UUID for event storage, not the Codex thread_id
    let event_session_id = session_id;
    thread::spawn(move || {
        CodexBackend::reader_thread_app_server(
            &orch_clone,
            &emitter,
            &run_id,
            event_session_id,
            notification_rx,
            client,
            current_turn_id,
            oauth_state,
            initial_sequence,
            profile.backend_key.to_string(),
        );
    });

    log::info!(
        "[PROFILE] CodexBackend(app-server)::start_session returning: {:?}",
        start_time.elapsed()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::process::wrap_plain_stdin;

    /// Codex cannot switch models on a live process. The warm-reuse path relies
    /// on this error to fall back to a restart, so it must stay explicit.
    #[test]
    fn codex_send_set_model_is_unsupported() {
        let backend = CodexBackend;
        let mut stdin = wrap_plain_stdin(Box::new(Vec::<u8>::new()));
        let err = backend
            .send_set_model(stdin.as_mut(), "gpt-5.4")
            .unwrap_err();
        assert!(
            err.to_lowercase().contains("not yet supported"),
            "unexpected error: {err}"
        );
    }
}
