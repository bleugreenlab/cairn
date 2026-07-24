use super::app_server::AppServerClient;
use super::auth::{find_codex_oauth_account_id, CodexAuthState};
use super::config::{write_codex_config, write_codex_config_for_provider};
use super::models::{discover_codex_models, refresh_codex_model_cache};
use super::protocol::CodexStdin;
use super::thread_params::{
    build_resume_fallback_prompt, build_thread_fork_params, build_thread_resume_params,
    build_thread_start_params, codex_sandbox_mode, is_missing_rollout_error,
};

const REQUIRED_CAIRN_MCP_TOOLS: [&str; 3] = ["read", "run", "write"];

fn cairn_mcp_status(client: &AppServerClient, thread_id: &str) -> Result<(), String> {
    let response = client.send_request(
        "mcpServerStatus/list",
        serde_json::json!({
            "threadId": thread_id,
            "detail": "toolsAndAuthOnly",
        }),
    )?;
    let servers = response
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "malformed mcpServerStatus/list response: missing data array".to_string())?;
    let cairn = servers
        .iter()
        .find(|server| server.get("name").and_then(serde_json::Value::as_str) == Some("cairn"))
        .ok_or_else(|| "cairn server is absent from the thread MCP registry".to_string())?;
    let tools = cairn
        .get("tools")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "cairn server status is missing its tool inventory".to_string())?;
    let mut advertised = tools.keys().map(String::as_str).collect::<Vec<_>>();
    advertised.sort_unstable();
    if advertised != REQUIRED_CAIRN_MCP_TOOLS {
        return Err(format!(
            "cairn server advertised tools [{}], required exactly [{}]",
            advertised.join(", "),
            REQUIRED_CAIRN_MCP_TOOLS.join(", ")
        ));
    }

    Ok(())
}

fn verify_resumed_thread_with_fallback<StartReplacement, Persist, ReportError>(
    client: &AppServerClient,
    resumed_thread_id: &str,
    start_replacement: StartReplacement,
    mut persist: Persist,
    mut report_error: ReportError,
) -> Result<String, String>
where
    StartReplacement: FnOnce() -> Result<String, String>,
    Persist: FnMut(&str) -> Result<(), String>,
    ReportError: FnMut(&str),
{
    let result: Result<String, String> = (|| {
        if ensure_cairn_mcp_attached(client, resumed_thread_id, "resume").is_ok() {
            persist(resumed_thread_id)?;
            return Ok(resumed_thread_id.to_string());
        }

        let replacement_thread_id = start_replacement()?;
        ensure_cairn_mcp_attached(
            client,
            &replacement_thread_id,
            "resume-transcript-fallback",
        )
        .map_err(|reason| {
            format!(
                "replacement thread {replacement_thread_id} for resumed thread {resumed_thread_id} failed attachment: {reason}"
            )
        })?;
        persist(&replacement_thread_id)?;
        Ok(replacement_thread_id)
    })();
    if let Err(error) = &result {
        report_error(error);
    }
    result
}

fn ensure_cairn_mcp_attached(
    client: &AppServerClient,
    thread_id: &str,
    resume_mode: &str,
) -> Result<(), String> {
    match cairn_mcp_status(client, thread_id) {
        Ok(()) => {
            log::info!(
                "Codex MCP attachment verified: thread_id={} resume_mode={} server=cairn startup_state=ready",
                thread_id,
                resume_mode
            );
            return Ok(());
        }
        Err(first_reason) => log::warn!(
            "Codex MCP attachment unavailable; refreshing once: thread_id={} resume_mode={} server=cairn startup_state=unavailable reason={}",
            thread_id,
            resume_mode,
            first_reason
        ),
    }

    client
        .send_request("config/mcpServer/reload", serde_json::Value::Null)
        .map_err(|reason| format!("cairn MCP refresh failed for thread {thread_id}: {reason}"))?;
    cairn_mcp_status(client, thread_id).map_err(|reason| {
        format!("cairn MCP attachment failed after refresh for thread {thread_id}: {reason}")
    })?;
    log::info!(
        "Codex MCP attachment recovered: thread_id={} resume_mode={} server=cairn startup_state=ready",
        thread_id,
        resume_mode
    );
    Ok(())
}
use super::version::check_codex_version;
use super::{CodexAppServerProfile, CodexBackend, CODEX_BACKEND_NAME};
use crate::agent_process::process::{ActiveProcess, BackendStdin};
use crate::backends::run_state::{
    resolve_run_db, run_job_id, set_session_backend_id, transition_run_to_live,
};
use crate::identity::CodexAuth;
use crate::orchestrator::session::{
    assemble_prompt_segments, base_instructions_from_segments,
    developer_instructions_from_segments, insert_error_event, persist_system_prompt_event,
};
use crate::orchestrator::Orchestrator;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

static CODEX_START_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
use std::thread;

use super::super::{
    AgentBackend, DiscoveredModel, OptionChoice, OptionKind, ProviderOptionDescriptor,
    ProviderOptionKey, ResolvedTools, SessionConfig,
};

fn codex_strict_output_schema(schema: &serde_json::Value) -> serde_json::Value {
    let mut schema = schema.clone();
    normalize_codex_strict_schema_node(&mut schema);
    schema
}

fn normalize_codex_strict_schema_node(schema: &mut serde_json::Value) {
    match schema {
        serde_json::Value::Object(map) => {
            for key in [
                "$defs",
                "definitions",
                "properties",
                "patternProperties",
                "dependentSchemas",
            ] {
                if let Some(serde_json::Value::Object(children)) = map.get_mut(key) {
                    for child in children.values_mut() {
                        normalize_codex_strict_schema_node(child);
                    }
                }
            }

            for key in [
                "items",
                "additionalItems",
                "contains",
                "propertyNames",
                "not",
                "if",
                "then",
                "else",
            ] {
                if let Some(child) = map.get_mut(key) {
                    normalize_codex_strict_schema_node(child);
                }
            }

            for key in ["anyOf", "oneOf", "allOf", "prefixItems"] {
                if let Some(serde_json::Value::Array(children)) = map.get_mut(key) {
                    for child in children {
                        normalize_codex_strict_schema_node(child);
                    }
                }
            }

            if is_json_object_schema(map) {
                let property_names = map
                    .get("properties")
                    .and_then(|value| value.as_object())
                    .map(|properties| properties.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();

                let mut required = map
                    .get("required")
                    .and_then(|value| value.as_array())
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(|value| value.as_str().map(str::to_owned))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                let originally_required = required
                    .iter()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>();

                if let Some(serde_json::Value::Object(properties)) = map.get_mut("properties") {
                    for property_name in &property_names {
                        if !originally_required.contains(property_name) {
                            if let Some(property_schema) = properties.get_mut(property_name) {
                                make_schema_nullable(property_schema);
                            }
                        }
                    }
                }

                for property_name in property_names {
                    if !required
                        .iter()
                        .any(|required_name| required_name == &property_name)
                    {
                        required.push(property_name);
                    }
                }

                map.insert(
                    "required".to_string(),
                    serde_json::Value::Array(
                        required
                            .into_iter()
                            .map(serde_json::Value::String)
                            .collect(),
                    ),
                );
                map.insert(
                    "additionalProperties".to_string(),
                    serde_json::Value::Bool(false),
                );
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_codex_strict_schema_node(value);
            }
        }
        _ => {}
    }
}

fn is_json_object_schema(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    if map.contains_key("properties") {
        return true;
    }

    match map.get("type") {
        Some(serde_json::Value::String(kind)) => kind == "object",
        Some(serde_json::Value::Array(kinds)) => kinds.iter().any(|kind| kind == "object"),
        _ => false,
    }
}

fn make_schema_nullable(schema: &mut serde_json::Value) {
    normalize_codex_strict_schema_node(schema);

    let serde_json::Value::Object(map) = schema else {
        return;
    };

    if let Some(serde_json::Value::Array(any_of)) = map.get_mut("anyOf") {
        if !any_of.iter().any(is_null_schema) {
            any_of.push(serde_json::json!({ "type": "null" }));
        }
        return;
    }

    if let Some(serde_json::Value::Array(one_of)) = map.get_mut("oneOf") {
        if !one_of.iter().any(is_null_schema) {
            one_of.push(serde_json::json!({ "type": "null" }));
        }
        return;
    }

    if let Some(serde_json::Value::Array(enum_values)) = map.get_mut("enum") {
        if !enum_values.iter().any(serde_json::Value::is_null) {
            enum_values.push(serde_json::Value::Null);
        }
        return;
    }

    match map.get_mut("type") {
        Some(serde_json::Value::String(kind)) if kind != "null" => {
            let kind = kind.clone();
            map.insert(
                "type".to_string(),
                serde_json::Value::Array(vec![
                    serde_json::Value::String(kind),
                    serde_json::Value::String("null".to_string()),
                ]),
            );
        }
        Some(serde_json::Value::Array(kinds)) => {
            if !kinds.iter().any(|kind| kind == "null") {
                kinds.push(serde_json::Value::String("null".to_string()));
            }
        }
        Some(_) => {}
        None => {
            let original = serde_json::Value::Object(map.clone());
            map.clear();
            map.insert(
                "anyOf".to_string(),
                serde_json::Value::Array(vec![original, serde_json::json!({ "type": "null" })]),
            );
        }
    }
}

fn is_null_schema(schema: &serde_json::Value) -> bool {
    schema
        .as_object()
        .and_then(|map| map.get("type"))
        .is_some_and(|kind| kind == "null")
}

impl AgentBackend for CodexBackend {
    fn name(&self) -> &str {
        "Codex"
    }

    fn is_available(&self) -> Result<(), String> {
        crate::env::find_binary("codex").map(|_| ())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        let _guard = CODEX_START_LOCK
            .lock()
            .map_err(|_| "Codex start lock poisoned".to_string())?;
        discover_codex_models(None, &HashMap::new())
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

        // The retired `mcp__cairn__return` tool is no longer injected: returning
        // is `write cairn:~/return` (CAIRN-2505). No MCP handler dispatched it.

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
        // Pooled ephemeral call (CAIRN-2549): run this call as a lightweight
        // `thread/start` on a shared app-server rather than spawning a process
        // for it. Node/task sessions fall through to the process-per-session
        // path below, unchanged.
        if config.is_ephemeral_call {
            return start_pooled_call(config, orch);
        }

        let start_time = std::time::Instant::now();
        let session_id = Some(config.session_start.session_id().to_string());

        // Resolve the run's owning DB ONCE (CAIRN-2208) and thread it through the
        // run-state, resume-fallback, and streaming-transcript writes below. A team
        // run lives wholly in its synced replica; routing those writes to the
        // private DB would fail the message_streams→runs foreign key. Fail-closed.
        let run_db = resolve_run_db(CODEX_BACKEND_NAME, orch, &config.run_id)?;

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

        let catalog_login_params = match (codex_auth.as_ref(), oauth_state.as_ref()) {
            (Some(CodexAuth::OAuthToken(_)), Some(state)) => {
                let guard = state
                    .lock()
                    .map_err(|_| "Codex auth state lock poisoned".to_string())?;
                Some(serde_json::json!({
                    "type": "chatgptAuthTokens",
                    "idToken": guard.id_access_pair().0,
                    "accessToken": guard.id_access_pair().1,
                    "chatgptAccountId": guard
                        .chatgpt_account_id()
                        .ok_or_else(|| "Missing ChatGPT account id in Codex auth tokens".to_string())?,
                }))
            }
            (Some(CodexAuth::ApiKey(key)), _) => Some(serde_json::json!({
                "type": "apiKey",
                "apiKey": key,
            })),
            _ => None,
        };
        let start_guard = CODEX_START_LOCK
            .lock()
            .map_err(|_| "Codex start lock poisoned".to_string())?;
        refresh_codex_model_cache(catalog_login_params, &HashMap::new())?;
        let codex_home = write_codex_config(
            &orch.mcp_binary_path,
            orch.mcp_callback_port,
            &config.mcp_config_json,
        )?;

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
        drop(start_guard);

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
            &crate::system_prompt::cairn_system_prompt(config.ambient),
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
        let resume_mode = match &config.session_start {
            crate::backends::SessionStart::Resume { .. } => "resume",
            crate::backends::SessionStart::Fork { .. } => "fork",
            crate::backends::SessionStart::New { .. } => "new",
        };
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
                            build_resume_fallback_prompt(&run_db, cairn_sid, &config.prompt)
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

        let mut thread_id = thread_resp
            .get("thread")
            .and_then(|t| t.get("id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Codex thread response missing thread id".to_string())?
            .to_string();
        log::info!(
            "Codex session start received thread_id={} for cairn_session_id={}",
            thread_id,
            session_id.as_deref().unwrap_or("<none>")
        );

        if let Err(attachment_error) = ensure_cairn_mcp_attached(&client, &thread_id, resume_mode) {
            if let crate::backends::SessionStart::Resume {
                backend_id,
                session_id: cairn_session_id,
            } = &config.session_start
            {
                log::warn!(
                    "Codex resumed thread MCP attachment failed; replacing thread with transcript preload: thread_id={} resume_mode=resume server=cairn startup_state=unavailable reason={}",
                    thread_id,
                    attachment_error
                );
                if let Some(preloaded_prompt) =
                    build_resume_fallback_prompt(&run_db, cairn_session_id, &config.prompt)
                {
                    prompt_text = preloaded_prompt;
                }
                let replacement = client.send_request(
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
                thread_id = replacement
                    .get("thread")
                    .and_then(|thread| thread.get("id"))
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        "Codex replacement thread response missing thread id".to_string()
                    })?
                    .to_string();
                if let Err(reason) =
                    ensure_cairn_mcp_attached(&client, &thread_id, "resume-transcript-fallback")
                {
                    let error = format!(
                        "Codex Cairn MCP infrastructure unavailable: replacement thread {thread_id} for resumed thread {backend_id} failed attachment: {reason}"
                    );
                    insert_error_event(orch, &config.run_id, session_id.as_deref(), &error);
                    return Err(error);
                }
            } else {
                let error = format!(
                    "Codex Cairn MCP infrastructure unavailable: thread {thread_id} mode={resume_mode}: {attachment_error}"
                );
                insert_error_event(orch, &config.run_id, session_id.as_deref(), &error);
                return Err(error);
            }
        }

        // Store only a verified Codex thread_id as the session's backend resume handle.
        if let Some(ref sid) = session_id {
            let _ = set_session_backend_id(CODEX_BACKEND_NAME, &run_db, sid, &thread_id);
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
        // Schema-constrained call (CAIRN-2505): request native structured output.
        // The app-server maps `outputSchema` to the provider's
        // `final_output_json_schema`, so the constrained result arrives as the
        // turn's final agent message, captured server-side at turn/completed.
        if let Some(ref schema) = config.output_schema {
            turn_params["outputSchema"] = codex_strict_output_schema(schema);
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
        if let Err(e) = transition_run_to_live(CODEX_BACKEND_NAME, orch, &run_db, &config.run_id) {
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

        let process_job_id: Option<String> =
            run_job_id(CODEX_BACKEND_NAME, &run_db, &config.run_id);

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
                run_db,
                None,
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

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        // Codex app-server is one long-lived pooled process; each call is a
        // lightweight `thread/start` session on it. Unbounded today.
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::PooledSessions,
            max_concurrency: None,
        }
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

    // Resolve the run's owning DB ONCE (CAIRN-2208); see the sibling start path.
    let run_db = resolve_run_db(profile.backend_name, orch, &config.run_id)?;

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

    let catalog_login_params = match (codex_auth.as_ref(), oauth_state.as_ref()) {
        (Some(CodexAuth::OAuthToken(_)), Some(state)) => {
            let guard = state
                .lock()
                .map_err(|_| "Codex auth state lock poisoned".to_string())?;
            Some(serde_json::json!({
                "type": "chatgptAuthTokens",
                "idToken": guard.id_access_pair().0,
                "accessToken": guard.id_access_pair().1,
                "chatgptAccountId": guard
                    .chatgpt_account_id()
                    .ok_or_else(|| "Missing ChatGPT account id in Codex auth tokens".to_string())?,
            }))
        }
        (Some(CodexAuth::ApiKey(key)), _) => Some(serde_json::json!({
            "type": "apiKey",
            "apiKey": key,
        })),
        _ => None,
    };
    let mut discovery_env = HashMap::new();
    if let Some((key, value)) = profile.api_key_env.as_ref() {
        discovery_env.insert((*key).to_string(), value.clone());
    }
    let start_guard = CODEX_START_LOCK
        .lock()
        .map_err(|_| "Codex start lock poisoned".to_string())?;
    refresh_codex_model_cache(catalog_login_params, &discovery_env)?;
    let codex_home = write_codex_config_for_provider(
        &orch.mcp_binary_path,
        orch.mcp_callback_port,
        &config.mcp_config_json,
        profile.model_provider.as_ref(),
    )?;

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
    drop(start_guard);

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
        &crate::system_prompt::cairn_system_prompt(config.ambient),
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
                        build_resume_fallback_prompt(&run_db, cairn_sid, &config.prompt)
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

    let resume_mode = match &config.session_start {
        crate::backends::SessionStart::Resume { .. } => "resume",
        crate::backends::SessionStart::Fork { .. } => "fork",
        crate::backends::SessionStart::New { .. } => "new",
    };
    let mut thread_id = thread_resp
        .get("thread")
        .and_then(|t| t.get("id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Codex thread response missing thread id".to_string())?
        .to_string();

    if let crate::backends::SessionStart::Resume {
        session_id: cairn_session_id,
        ..
    } = &config.session_start
    {
        let run_id = config.run_id.clone();
        thread_id = verify_resumed_thread_with_fallback(
            &client,
            &thread_id,
            || {
                if let Some(preloaded_prompt) =
                    build_resume_fallback_prompt(&run_db, cairn_session_id, &config.prompt)
                {
                    prompt_text = preloaded_prompt;
                }
                let replacement = client.send_request(
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
                replacement
                    .get("thread")
                    .and_then(|thread| thread.get("id"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        "Codex replacement thread response missing thread id".to_string()
                    })
            },
            |verified_thread_id| {
                set_session_backend_id(
                    profile.backend_name,
                    &run_db,
                    cairn_session_id,
                    verified_thread_id,
                )
            },
            |reason| {
                insert_error_event(
                    orch,
                    &run_id,
                    session_id.as_deref(),
                    &format!("Codex Cairn MCP infrastructure unavailable: {reason}"),
                );
            },
        )?;
    } else {
        if let Err(attachment_error) = ensure_cairn_mcp_attached(&client, &thread_id, resume_mode) {
            let error = format!(
                "Codex Cairn MCP infrastructure unavailable: thread {thread_id} mode={resume_mode}: {attachment_error}"
            );
            insert_error_event(orch, &config.run_id, session_id.as_deref(), &error);
            return Err(error);
        }
        if let Some(ref sid) = session_id {
            set_session_backend_id(profile.backend_name, &run_db, sid, &thread_id)?;
        }
    }
    log::info!(
        "Codex session start received thread_id={} for cairn_session_id={}",
        thread_id,
        session_id.as_deref().unwrap_or("<none>")
    );

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
    // Schema-constrained call (CAIRN-2505): request native structured output,
    // mirroring the primary `start_session` turn/start path so the two never
    // drift.
    if let Some(ref schema) = config.output_schema {
        turn_params["outputSchema"] = codex_strict_output_schema(schema);
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
    if let Err(e) = transition_run_to_live(profile.backend_name, orch, &run_db, &config.run_id) {
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

    let process_job_id: Option<String> = run_job_id(profile.backend_name, &run_db, &config.run_id);

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
            run_db,
            None,
        );
    });

    log::info!(
        "[PROFILE] CodexBackend(app-server)::start_session returning: {:?}",
        start_time.elapsed()
    );
    Ok(())
}

/// Auth-identity fingerprint for the Codex app-server pool key (CAIRN-2549).
///
/// Distinct identities must never share an app-server (auth is process-global),
/// but worktree/model/fence are deliberately excluded so scratch-dir fan-out
/// under one identity collapses to a single pooled process.
fn pool_key_for_auth(auth: &CodexAuth, oauth_state: Option<&Arc<Mutex<CodexAuthState>>>) -> String {
    match auth {
        CodexAuth::OAuthToken(_) => {
            let account = oauth_state
                .and_then(|s| s.lock().ok())
                .and_then(|guard| guard.chatgpt_account_id())
                .unwrap_or_default();
            format!("codex-oauth:{account}")
        }
        CodexAuth::ApiKey(key) => {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            format!("codex-apikey:{:x}", hasher.finish())
        }
    }
}

/// Perform the one-time `account/login/start` for a pooled app-server, mirroring
/// the dedicated-session login so both auth paths stay identical.
fn codex_login(
    client: &AppServerClient,
    auth: &CodexAuth,
    oauth_state: Option<&Arc<Mutex<CodexAuthState>>>,
) -> Result<(), String> {
    match (auth, oauth_state) {
        (CodexAuth::OAuthToken(_), Some(state)) => {
            let guard = state
                .lock()
                .map_err(|_| "Codex auth state lock poisoned".to_string())?;
            let (id_token, access_token) = guard.id_access_pair();
            let account_id = guard
                .chatgpt_account_id()
                .ok_or_else(|| "Missing ChatGPT account id in Codex auth tokens".to_string())?;
            drop(guard);
            client.send_request(
                "account/login/start",
                serde_json::json!({
                    "type": "chatgptAuthTokens",
                    "idToken": id_token,
                    "accessToken": access_token,
                    "chatgptAccountId": account_id,
                }),
            )?;
        }
        (CodexAuth::ApiKey(key), _) => {
            client.send_request(
                "account/login/start",
                serde_json::json!({
                    "type": "apiKey",
                    "apiKey": key,
                }),
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// Start an ephemeral CALL as a thread on the shared pooled Codex app-server
/// (CAIRN-2549). One process hosts N call threads; each call finalizes its
/// one-shot turn and abandons its thread. Isolation: the per-call `RunHandle`
/// carries a NULL child so kill/stop/finalize never signal the shared process.
fn start_pooled_call(config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
    let start_time = std::time::Instant::now();
    let session_id = Some(config.session_start.session_id().to_string());
    let run_db = resolve_run_db(CODEX_BACKEND_NAME, orch, &config.run_id)?;

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
    let Some(codex_auth) = identity.as_ref().and_then(|i| i.codex_auth.clone()) else {
        insert_error_event(
            orch,
            &config.run_id,
            session_id.as_deref(),
            "Codex credentials not configured. Run `connect_codex_auth`.",
        );
        return Err("Missing Codex credentials".to_string());
    };

    let oauth_state = match &codex_auth {
        CodexAuth::OAuthToken(json) => {
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

    // Env for the SHARED app-server process. `CAIRN_RUN_ID`/`CAIRN_HOME_URI` are
    // NON-authoritative here: per-call attribution rides on the tool call's
    // `_meta.threadId`, which the host maps back to the owning run. The sentinel
    // run id makes a (never-expected) threadId-less tool call fail LOUD in run
    // lookup rather than silently attributing to another thread's run.
    let mut env = HashMap::new();
    env.insert("CODEX_HOME".to_string(), codex_home.clone());
    env.insert(
        "CAIRN_RUN_ID".to_string(),
        "codex-pooled-app-server".to_string(),
    );
    env.insert("CAIRN_MCP_SECRET".to_string(), mcp_secret.clone());
    env.insert("CAIRN_HOME_URI".to_string(), config.home_uri.clone());

    let pool_key = pool_key_for_auth(&codex_auth, oauth_state.as_ref());

    orch.collect_warm_if_needed();

    // Get-or-spawn the shared app-server (spawn + initialize + login run ONCE per
    // key, serialized by the pool's per-key init lock).
    let working_dir = config.working_dir.clone();
    let codex_path_for_build = codex_path.clone();
    let auth_for_build = codex_auth.clone();
    let oauth_for_build = oauth_state.clone();
    let server = orch
        .codex_pool
        .ensure(&pool_key, orch, move || {
            let client = Arc::new(AppServerClient::spawn(
                orch.services.process.as_ref(),
                &codex_path_for_build,
                &env,
                &working_dir,
            )?);
            client.send_request(
                "initialize",
                serde_json::json!({
                    "clientInfo": {
                        "name": "cairn",
                        "title": "Cairn",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": { "experimentalApi": true }
                }),
            )?;
            client.send_notification("initialized", serde_json::json!({}))?;
            codex_login(client.as_ref(), &auth_for_build, oauth_for_build.as_ref())?;
            Ok((client, oauth_for_build))
        })
        .map_err(|e| {
            insert_error_event(
                orch,
                &config.run_id,
                session_id.as_deref(),
                &format!("Failed to start Codex app-server pool: {}", e),
            );
            e
        })?;
    let client = server.client();

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
    let prompt_segments = assemble_prompt_segments(
        &crate::system_prompt::cairn_system_prompt(config.ambient),
        workspace_instructions.as_deref(),
        project_instructions.as_deref(),
        config.system_prompt_content.as_deref(),
        config.system_prompt_dynamic_tail.as_deref(),
    );
    let base_instructions = base_instructions_from_segments(&prompt_segments);
    let developer_instructions = developer_instructions_from_segments(&prompt_segments);

    // A call is always a fresh thread.
    let thread_resp = client.send_request(
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
    let thread_id = thread_resp
        .get("thread")
        .and_then(|t| t.get("id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Codex thread response missing thread id".to_string())?
        .to_string();

    if let Some(ref sid) = session_id {
        let _ = set_session_backend_id(CODEX_BACKEND_NAME, &run_db, sid, &thread_id);
    }

    // Register `threadId -> run` and the per-call notification channel BEFORE
    // `turn/start`, so the first tool call routes to this run (demux #2) and the
    // dispatcher can deliver this thread's notifications (demux #1).
    let notification_rx = server.register_call(&thread_id, &config.run_id, &config.working_dir);

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
        "input": [{ "type": "text", "text": config.prompt }]
    });
    if let Some(ref model) = model_str {
        turn_params["model"] = serde_json::json!(model);
    }
    if let Some(ref schema) = config.output_schema {
        turn_params["outputSchema"] = codex_strict_output_schema(schema);
    }
    let turn_resp = client.send_request("turn/start", turn_params)?;
    let current_turn_id = Arc::new(Mutex::new(
        turn_resp
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    ));

    if let Err(e) = transition_run_to_live(CODEX_BACKEND_NAME, orch, &run_db, &config.run_id) {
        log::warn!("Failed to transition codex pooled call to running: {}", e);
    }

    // A NULL child is the isolation lever: `kill_session_with_reason` /
    // `stop_and_remove` resolve `child.lock().take()` to None and skip
    // `graceful_stop`, so the shared app-server is never signalled. Interrupt
    // still reaches this call through `CodexStdin` -> `turn/interrupt`.
    let null_child: Arc<Mutex<Option<Box<dyn crate::services::ChildProcess>>>> =
        Arc::new(Mutex::new(None));
    let stdin_arc: Arc<Mutex<Option<Box<dyn BackendStdin>>>> =
        Arc::new(Mutex::new(Some(Box::new(CodexStdin::new(
            client.clone(),
            thread_id.clone(),
            config.working_dir.clone(),
            model_str.clone(),
            config.reasoning_effort.clone(),
            config.service_tier.clone(),
            current_turn_id.clone(),
        )) as Box<dyn BackendStdin>)));

    let process_job_id: Option<String> = run_job_id(CODEX_BACKEND_NAME, &run_db, &config.run_id);
    {
        let mut processes = orch
            .process_state
            .processes
            .lock()
            .map_err(|e| e.to_string())?;
        let mut active_process =
            ActiveProcess::new(null_child, stdin_arc, session_id.clone(), process_job_id);
        active_process.backend = Some("codex".to_string());
        active_process.model = config.model.as_ref().map(|m| m.as_str().to_string());
        processes.register(config.run_id.clone(), active_process);
    }
    // `transition_to_active` (CAIRN-2526) is applied by the calls path
    // (`start_call_run_now`) after this returns, exactly like the process path.

    let run_id = config.run_id.clone();
    let orch_clone = orch.clone();
    let emitter = orch.services.emitter.clone();
    let event_session_id = session_id;
    let cleanup = super::pool::PooledCall::new(server.clone(), thread_id.clone());
    thread::spawn(move || {
        CodexBackend::reader_thread_app_server(
            &orch_clone,
            &emitter,
            &run_id,
            event_session_id,
            notification_rx,
            client,
            current_turn_id,
            // Pool-scoped auth-token refresh is answered by the pool dispatcher
            // (it carries no threadId), so the per-call reader needs no oauth state.
            None,
            initial_sequence,
            "codex".to_string(),
            run_db,
            Some(cleanup),
        );
    });

    log::info!(
        "[PROFILE] CodexBackend pooled call started: {:?}",
        start_time.elapsed()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::process::wrap_plain_stdin;
    use serde_json::json;

    fn ready_mcp_status() -> serde_json::Value {
        json!({
            "result": {
                "data": [{
                    "name": "cairn",
                    "tools": {
                        "read": {"name": "read"},
                        "write": {"name": "write"},
                        "run": {"name": "run"}
                    }
                }]
            }
        })
    }

    fn request_methods(requests: &Arc<Mutex<Vec<serde_json::Value>>>) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|request| request["method"].as_str().map(str::to_string))
            .collect()
    }

    #[test]
    fn healthy_thread_verifies_without_refresh() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client =
            AppServerClient::for_test_scripted(vec![Ok(ready_mcp_status())], Arc::clone(&requests));

        ensure_cairn_mcp_attached(&client, "thread-new", "new").unwrap();

        assert_eq!(request_methods(&requests), vec!["mcpServerStatus/list"]);
        assert_eq!(
            requests.lock().unwrap()[0]["params"]["threadId"],
            "thread-new"
        );
    }

    #[test]
    fn empty_resume_status_refreshes_then_recovers() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(json!({"result": {"data": []}})),
                Ok(json!({"result": {}})),
                Ok(ready_mcp_status()),
            ],
            Arc::clone(&requests),
        );

        ensure_cairn_mcp_attached(&client, "thread-resume", "resume").unwrap();

        assert_eq!(
            request_methods(&requests),
            vec![
                "mcpServerStatus/list",
                "config/mcpServer/reload",
                "mcpServerStatus/list"
            ]
        );
    }

    #[test]
    fn healthy_resume_checks_the_existing_thread_without_replacement() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client =
            AppServerClient::for_test_scripted(vec![Ok(ready_mcp_status())], Arc::clone(&requests));

        ensure_cairn_mcp_attached(&client, "existing-thread", "resume").unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["params"]["threadId"], "existing-thread");
    }

    #[test]
    fn persistent_callback_failure_never_sends_turn_start() {
        let callback_error =
            json!({"error": {"code": -32000, "message": "callback connection refused"}});
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(callback_error.clone()),
                Ok(json!({"result": {}})),
                Ok(callback_error),
            ],
            Arc::clone(&requests),
        );

        let reported = Arc::new(Mutex::new(Vec::<String>::new()));
        let error = verify_resumed_thread_with_fallback(
            &client,
            "thread-offline",
            || Err("replacement app-server unavailable".to_string()),
            |_| Ok(()),
            {
                let reported = Arc::clone(&reported);
                move |error| reported.lock().unwrap().push(error.to_string())
            },
        )
        .unwrap_err();

        assert!(error.contains("replacement app-server unavailable"));
        assert_eq!(reported.lock().unwrap().as_slice(), [error.as_str()]);
        assert!(!request_methods(&requests).contains(&"turn/start".to_string()));
    }

    #[test]
    fn resumed_thread_fallback_persists_replacement_before_turn_start() {
        let empty_status = json!({"result": {"data": []}});
        let requests = Arc::new(Mutex::new(Vec::new()));
        let persisted = Arc::new(Mutex::new(Vec::<String>::new()));
        let errors = Arc::new(Mutex::new(Vec::<String>::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(empty_status.clone()),
                Ok(json!({"result": {}})),
                Ok(empty_status),
                Ok(json!({"result": {"thread": {"id": "replacement-thread"}}})),
                Ok(ready_mcp_status()),
                Ok(json!({"result": {"turn": {"id": "turn-1"}}})),
            ],
            Arc::clone(&requests),
        );

        let verified_thread = verify_resumed_thread_with_fallback(
            &client,
            "stale-thread",
            || {
                let replacement =
                    client.send_request("thread/start", json!({"cwd": "/tmp/worktree"}))?;
                replacement["thread"]["id"]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| "replacement missing id".to_string())
            },
            {
                let persisted = Arc::clone(&persisted);
                move |thread_id| {
                    persisted.lock().unwrap().push(thread_id.to_string());
                    Ok(())
                }
            },
            {
                let errors = Arc::clone(&errors);
                move |error| errors.lock().unwrap().push(error.to_string())
            },
        )
        .unwrap();
        assert_eq!(verified_thread, "replacement-thread");
        assert_eq!(persisted.lock().unwrap().as_slice(), ["replacement-thread"]);
        assert!(errors.lock().unwrap().is_empty());

        client
            .send_request(
                "turn/start",
                json!({"threadId": verified_thread, "input": []}),
            )
            .unwrap();

        assert_eq!(
            request_methods(&requests),
            vec![
                "mcpServerStatus/list",
                "config/mcpServer/reload",
                "mcpServerStatus/list",
                "thread/start",
                "mcpServerStatus/list",
                "turn/start",
            ]
        );
    }

    #[test]
    fn missing_core_mcp_verb_is_rejected_after_one_refresh() {
        let status = json!({
            "result": {
                "data": [{
                    "name": "cairn",
                    "tools": {
                        "read": {"name": "read"},
                        "write": {"name": "write"}
                    }
                }]
            }
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![Ok(status.clone()), Ok(json!({"result": {}})), Ok(status)],
            Arc::clone(&requests),
        );

        let error = ensure_cairn_mcp_attached(&client, "thread-broken", "resume").unwrap_err();

        assert!(error.contains("required exactly [read, run, write]"));
        assert_eq!(
            request_methods(&requests),
            vec![
                "mcpServerStatus/list",
                "config/mcpServer/reload",
                "mcpServerStatus/list"
            ]
        );
    }

    /// Codex cannot switch models on a live process. The warm-reuse path relies
    /// on this error to fall back to a restart, so it must stay explicit.

    #[test]
    fn codex_strict_output_schema_normalizes_deep_research_scope_schema() {
        let schema = serde_json::json!({
            "type": "object",
            "required": ["question", "summary", "angles"],
            "properties": {
                "question": { "type": "string" },
                "summary": { "type": "string" },
                "angles": {
                    "type": "array",
                    "minItems": 3,
                    "maxItems": 6,
                    "items": {
                        "type": "object",
                        "required": ["label", "query"],
                        "properties": {
                            "label": { "type": "string" },
                            "query": { "type": "string" },
                            "rationale": { "type": "string" }
                        }
                    }
                }
            }
        });

        let normalized = codex_strict_output_schema(&schema);

        assert_eq!(
            normalized["additionalProperties"],
            serde_json::Value::Bool(false)
        );
        assert_required_keys(&normalized, &["question", "summary", "angles"]);

        let angle_schema = &normalized["properties"]["angles"]["items"];
        assert_eq!(
            angle_schema["additionalProperties"],
            serde_json::Value::Bool(false)
        );
        assert_required_keys(angle_schema, &["label", "query", "rationale"]);
        assert_eq!(
            angle_schema["properties"]["rationale"]["type"],
            serde_json::json!(["string", "null"])
        );
        assert_eq!(
            angle_schema["properties"]["label"],
            serde_json::json!({ "type": "string" })
        );
    }

    fn assert_required_keys(schema: &serde_json::Value, expected: &[&str]) {
        let mut actual = schema["required"]
            .as_array()
            .expect("schema required must be an array")
            .iter()
            .map(|value| value.as_str().expect("required item must be a string"))
            .collect::<Vec<_>>();
        actual.sort_unstable();

        let mut expected = expected.to_vec();
        expected.sort_unstable();

        assert_eq!(actual, expected);
    }
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
