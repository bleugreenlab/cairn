use super::thread_params::build_resume_fallback_prompt;
use super::{CodexBackend, CODEX_BACKEND_NAME};
use crate::agent_process::process::ActiveProcess;
use crate::backends::run_state::{
    resolve_run_db, run_job_id, set_session_backend_id, transition_run_to_live,
};
use crate::credentials::CodexAuth;
use crate::credentials::RoutedProvider;
use crate::orchestrator::session::{
    assemble_prompt_segments, base_instructions_from_segments,
    developer_instructions_from_segments, insert_error_event, persist_system_prompt_event,
    select_routed_serving_identity, SelectionMode,
};
use crate::orchestrator::Orchestrator;
use cairn_models::codex::tokens::{find_codex_oauth_account_id, CodexAuthState};
use cairn_models::interface::session::{
    Cli, CodexModels, CodexServer, CodexSpec, NativeGoal, ProviderSpec, Secret, SecretEnv,
    ServerLaunch, SessionError, SessionEvents, SessionHost, SessionSpec, SessionStart,
};
use cairn_models::session::codex::models::discover_codex_models;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};

static CODEX_START_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
use std::thread;

use super::super::{
    AgentBackend, CompletionError, CompletionOutcome, CompletionRequest, CompletionShape,
    ResolvedTools, SessionConfig,
};
use cairn_models::types::catalog::{
    DiscoveredModel, OptionChoice, OptionKind, ProviderOptionDescriptor, ProviderOptionKey,
};

/// The Codex CLI on the session host, reporting a missing or unusable one on
/// the run's transcript.
fn resolve_codex_cli(
    orch: &Orchestrator,
    host: &dyn SessionHost,
    run_id: &str,
    session_id: Option<&str>,
) -> Result<String, String> {
    host.resolve_cli(Cli::Codex).map_err(|error| {
        match &error {
            SessionError::CliNotFound(reason) => insert_error_event(
                orch,
                run_id,
                session_id,
                &format!("Codex CLI not found: {}", reason),
            ),
            SessionError::CliIncompatible(reason) => {
                insert_error_event(orch, run_id, session_id, reason)
            }
            _ => {}
        }
        error.to_string()
    })
}

/// A Codex OAuth credential's live state, and the Cairn account it belongs
/// to. `None` for an API key.
type CodexOAuth = (Option<Arc<Mutex<CodexAuthState>>>, Option<String>);

fn codex_oauth_state(orch: &Orchestrator, auth: &CodexAuth) -> Result<CodexOAuth, String> {
    let CodexAuth::OAuthToken(json) = auth else {
        return Ok((None, None));
    };
    let account_id = orch
        .get_credential_store()
        .and_then(|store| find_codex_oauth_account_id(&store, json));
    let state = CodexAuthState::new_for_account(orch, json, account_id.clone())
        .map_err(|e| format!("Invalid Codex OAuth tokens: {}", e))?;
    Ok((Some(Arc::new(Mutex::new(state))), account_id))
}

/// The `account/login/start` params an app-server is signed in with. A
/// subscription's are the subscription support's to present; an API key is
/// presented as itself.
fn codex_login(
    orch: &Orchestrator,
    auth: &CodexAuth,
    oauth_state: Option<&Arc<Mutex<CodexAuthState>>>,
) -> Result<Option<Secret<serde_json::Value>>, String> {
    match (auth, oauth_state) {
        (CodexAuth::OAuthToken(_), Some(state)) => {
            let guard = state
                .lock()
                .map_err(|_| "Codex auth state lock poisoned".to_string())?;
            orch.subscriptions()
                .codex_session_login(orch, &guard)
                .map(Some)
        }
        (CodexAuth::ApiKey(key), _) => Ok(Some(Secret::new(serde_json::json!({
            "type": "apiKey",
            "apiKey": key,
        })))),
        _ => Ok(None),
    }
}

fn native_goal(config: &SessionConfig) -> NativeGoal {
    if config.clear_native_goal {
        NativeGoal::Clear
    } else {
        config
            .goal_objective
            .clone()
            .map_or(NativeGoal::Leave, NativeGoal::Objective)
    }
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
        let codex_path = crate::env::find_binary("codex")?;
        discover_codex_models(
            &codex_path,
            &super::cairn_codex_home(),
            None,
            &HashMap::new(),
        )
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

        let runtime = orch.models_runtime();
        let codex_path = resolve_codex_cli(
            orch,
            runtime.host.as_ref(),
            &config.run_id,
            session_id.as_deref(),
        )?;
        log::debug!(
            "CodexBackend(app-server): codex_path={}, working_dir={}",
            codex_path,
            config.working_dir
        );
        log::info!(
            "[PROFILE] CodexBackend command resolved: {:?}",
            start_time.elapsed()
        );

        let (mcp_credential_var, mcp_credential) = crate::mcp::run_bearer::mcp_credential_env(
            orch,
            &config.mcp,
            config.mcp_bearer.as_ref(),
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
        let (oauth_state, account_id) =
            codex_oauth_state(orch, &codex_auth).inspect_err(|error| {
                insert_error_event(orch, &config.run_id, session_id.as_deref(), error);
            })?;
        let login = codex_login(orch, &codex_auth, oauth_state.as_ref())?;

        let start_guard = CODEX_START_LOCK
            .lock()
            .map_err(|_| "Codex start lock poisoned".to_string())?;
        let codex_home = runtime
            .host
            .prepare_codex_home(CodexModels::Refresh {
                login: login.as_ref(),
            })
            .map_err(String::from)?;

        let env = BTreeMap::from([
            ("CODEX_HOME".to_string(), codex_home),
            ("CAIRN_RUN_ID".to_string(), config.run_id.clone()),
            ("CAIRN_HOME_URI".to_string(), config.home_uri.clone()),
        ]);
        let secrets = SecretEnv::new().with(mcp_credential_var, mcp_credential);

        orch.collect_warm_if_needed();

        let client = runtime
            .codex
            .start_server(
                runtime.host.as_ref(),
                ServerLaunch {
                    cli_path: &codex_path,
                    cwd: &config.working_dir,
                    env: &env,
                    secrets: &secrets,
                    mcp: &crate::backends::cairn_servers(&config.mcp),
                    login: login.as_ref(),
                },
            )
            .map_err(|error| {
                if let SessionError::Spawn(reason) = &error {
                    insert_error_event(
                        orch,
                        &config.run_id,
                        session_id.as_deref(),
                        &format!("Failed to start Codex app-server: {}", reason),
                    );
                }
                error.to_string()
            })?;
        drop(start_guard);

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

        // A resumed thread that is gone is rebuilt from Cairn's transcript; the
        // first turn then carries that transcript before the latest message.
        let resume_fallback = {
            let run_db = run_db.clone();
            let session_id = config.session_start.session_id().to_string();
            let prompt = config.prompt.clone();
            Box::new(move || build_resume_fallback_prompt(&run_db, &session_id, &prompt))
                as Box<dyn FnMut() -> Option<String> + Send>
        };
        let spec = SessionSpec {
            start: config.session_start.clone(),
            model: config.model.as_ref().map(|model| model.to_string()),
            effort: config.reasoning_effort.clone(),
            approval: crate::backends::approval_mode(config.permissions.fence),
            output_schema: config.output_schema.clone(),
            cwd: config.working_dir.clone(),
            env: BTreeMap::new(),
            mcp: Some(crate::backends::cairn_servers(&config.mcp)),
            secrets: SecretEnv::new(),
            account_id,
            provider: ProviderSpec::Codex(CodexSpec {
                server: CodexServer::Dedicated(client),
                base_instructions: base_instructions_from_segments(&prompt_segments),
                developer_instructions: developer_instructions_from_segments(&prompt_segments),
                service_tier: config.service_tier.clone(),
                native_goal: native_goal(&config),
                resume_fallback: Some(resume_fallback),
            }),
        };
        let launched = runtime
            .codex
            .open(runtime.host.as_ref(), spec)
            .map_err(|error| {
                if let SessionError::McpUnavailable(reason) = &error {
                    insert_error_event(orch, &config.run_id, session_id.as_deref(), reason);
                }
                error.to_string()
            })?;
        let thread_id = launched
            .facts
            .backend_session_id
            .clone()
            .ok_or_else(|| "Codex opened a thread without an id".to_string())?;

        // Store only a verified Codex thread_id as the session's backend resume handle.
        if let Some(ref sid) = session_id {
            let _ = set_session_backend_id(CODEX_BACKEND_NAME, &run_db, sid, &thread_id);
        }

        persist_system_prompt_event(
            orch,
            &config.run_id,
            session_id.as_deref(),
            "codex",
            &prompt_segments,
        );

        let mut control = launched.control;
        control.open_turn(&config.message_content.with_text(config.prompt.clone()))?;

        // Transition run to Running AFTER successful spawn (sets started_at accurately)
        if let Err(e) = transition_run_to_live(CODEX_BACKEND_NAME, orch, &run_db, &config.run_id) {
            log::warn!("Failed to transition codex run to running: {}", e);
            // Job is already Running from start_job's transition_job call — no write needed
        }

        let SessionEvents::CodexAppServer {
            client,
            notifications: Some(notification_rx),
            current_turn,
        } = launched.events
        else {
            return Err("a dedicated Codex thread opened without its notifications".to_string());
        };

        let process_job_id: Option<String> =
            run_job_id(CODEX_BACKEND_NAME, &run_db, &config.run_id);
        {
            let mut active_process = ActiveProcess::new(
                launched.process,
                Arc::new(Mutex::new(Some(control))),
                session_id.clone(),
                process_job_id,
            );
            active_process.backend = Some("codex".to_string());
            active_process.model = config.model.as_ref().map(|m| m.as_str().to_string());
            active_process.account_id = config.account_id.clone();
            active_process.facts = Some(launched.facts);
            orch.process_state
                .register_process(config.run_id.clone(), active_process)?;
        }

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
                current_turn,
                oauth_state,
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

    fn response_completion_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
    ) -> Result<(), String> {
        codex_response_availability(orch, project_id, None)
    }

    fn response_model_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
        model: &str,
    ) -> Result<(), String> {
        codex_response_availability(orch, project_id, Some(model))
    }

    /// One tool-free turn on a pooled app-server signed in as the account
    /// routing chose. The thread is ephemeral and bound to no run.
    fn complete(
        &self,
        request: CompletionRequest,
        orch: &Orchestrator,
    ) -> Result<CompletionOutcome, CompletionError> {
        use crate::backends::one_shot::{
            instructions, map_one_shot_error, map_open_error, one_shot_timeout,
            response_scratch_dir, runtime_extras, whole_prompt_tokens,
        };
        use cairn_models::interface::session::ApprovalMode;
        use cairn_models::session::one_shot::await_one_shot;
        use cairn_models::wire::MessageContent;

        if request.messages.is_empty() {
            return Err(CompletionError::InvalidRequest(
                "at least one message is required".to_string(),
            ));
        }
        let selection = select_routed_serving_identity(
            orch,
            RoutedProvider::Codex,
            request.project_id.as_deref(),
            Some(&request.model),
            SelectionMode::Serve,
        )
        .map_err(CompletionError::Upstream)?;
        let codex_auth = selection.identity.codex_auth.as_ref().ok_or_else(|| {
            CompletionError::Upstream(crate::backends::codex::NO_CODEX_CREDENTIAL.to_string())
        })?;
        let (server, _) =
            acquire_pooled_server(orch, codex_auth).map_err(CompletionError::Upstream)?;
        let scratch = response_scratch_dir()?;
        let extras = runtime_extras(&request);
        let spec = SessionSpec {
            start: SessionStart::New {
                session_id: uuid::Uuid::new_v4().to_string(),
            },
            model: Some(request.model.clone()),
            effort: extras.reasoning_effort,
            approval: ApprovalMode::Deny,
            output_schema: request.output_schema.clone(),
            cwd: scratch.path(),
            env: BTreeMap::new(),
            mcp: None,
            secrets: SecretEnv::new(),
            account_id: selection.account_id.clone(),
            provider: ProviderSpec::Codex(CodexSpec {
                server: CodexServer::Shared(server.client()),
                base_instructions: instructions(&request),
                developer_instructions: None,
                service_tier: extras.service_tier,
                native_goal: NativeGoal::Leave,
                resume_fallback: None,
            }),
        };
        let started = std::time::Instant::now();
        let runtime = orch.models_runtime();
        let mut launched = runtime
            .codex
            .open(runtime.host.as_ref(), spec)
            .map_err(map_open_error)?;
        let thread_id = launched.facts.backend_session_id.clone().ok_or_else(|| {
            CompletionError::InvalidResponse("Codex opened a thread without an id".to_string())
        })?;
        // Routed before the turn starts, so none of its notifications are
        // dropped; the guard removes the route on every exit.
        let notifications = server.register_completion(&thread_id);
        let _route = super::pool::PooledCall::new(server.clone(), thread_id);
        if let SessionEvents::CodexAppServer {
            notifications: slot,
            ..
        } = &mut launched.events
        {
            *slot = Some(notifications);
        }
        launched
            .control
            .open_turn(&MessageContent::text(request.single_prompt()))
            .map_err(CompletionError::Upstream)?;
        let reply =
            await_one_shot(launched, one_shot_timeout(&request)).map_err(map_one_shot_error)?;
        let latency_ms = started.elapsed().as_millis() as u64;

        let parsed = match request.output_schema {
            Some(_) => Some(serde_json::from_str(&reply.text).map_err(|error| {
                CompletionError::InvalidResponse(format!(
                    "Codex's answer is not the requested JSON: {error}"
                ))
            })?),
            None => None,
        };
        Ok(CompletionOutcome {
            text: reply.text,
            parsed,
            model: request.model,
            tokens: whole_prompt_tokens(reply.usage.as_ref()),
            // Codex reports no price; a subscription's would be notional.
            cost: None,
            latency_ms,
            account_id: selection.account_id,
        })
    }

    fn completion_shape(&self) -> CompletionShape {
        CompletionShape::PooledSessions
    }

    fn supports_resume(&self) -> bool {
        true // app-server can resume persisted threads via thread/resume
    }

    fn supports_warm_processes(&self) -> bool {
        true // app-server stays alive after turn completion and accepts more user_input
    }

    fn runtime_launch_capability(
        &self,
        launch: &crate::backends::RuntimeLaunch,
    ) -> Result<crate::backends::RuntimeLaunchCapability, String> {
        use crate::backends::{
            RuntimeClass, RuntimeHostKind, RuntimeHostRequirement, RuntimeLane,
            RuntimeLaunchCapability, RuntimeResourceClaim,
        };
        if launch.is_ephemeral_call {
            return Ok(RuntimeLaunchCapability {
                class: RuntimeClass::MultiplexedServer,
                lane: RuntimeLane::new("codex-app-server"),
                host_requirement: RuntimeHostRequirement::ResidentOn(RuntimeHostKind::Runner),
                claim: RuntimeResourceClaim {
                    resident_process_units: 1,
                    // The call funnel classifies runtime cost before Codex auth is
                    // resolved. Durable admission must replace this provisional
                    // claim with the exact pool_key_for_auth identity before
                    // leasing; the synchronous local backstop may classify it
                    // without turning existing Codex calls into launch failures.
                    server_instance_key: launch.server_instance_key.clone(),
                    logical_stream_units: 1,
                    estimated_memory_bytes: None,
                },
                supports_warm_reuse: true,
            });
        }
        Ok(RuntimeLaunchCapability {
            class: RuntimeClass::DedicatedProcess,
            lane: RuntimeLane::new("codex-session-process"),
            host_requirement: RuntimeHostRequirement::ResidentOn(RuntimeHostKind::Runner),
            claim: RuntimeResourceClaim {
                resident_process_units: 1,
                server_instance_key: None,
                logical_stream_units: 1,
                estimated_memory_bytes: None,
            },
            supports_warm_reuse: true,
        })
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        // Codex app-server is one long-lived pooled process; each call is a
        // lightweight `thread/start` session on it. Unbounded today.
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::PooledSessions,
            max_concurrency: None,
        }
    }
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

/// Whether a response can run on Codex in this scope: routing must find a
/// credential. No CLI probe; resolving the Codex CLI runs it.
fn codex_response_availability(
    orch: &Orchestrator,
    project_id: Option<&str>,
    model: Option<&str>,
) -> Result<(), String> {
    select_routed_serving_identity(
        orch,
        RoutedProvider::Codex,
        project_id,
        model,
        SelectionMode::Probe,
    )
    .map(|_| ())
}

/// The `cairn` MCP entry a pooled app-server is started with, built from
/// the orchestrator alone so it never depends on which caller spawned the
/// server. It carries no home: every pooled tool call is attributed by its
/// `_meta.threadId`. A pooled server serves many runs, so no one run's HTTP
/// bearer could credential it; it always reaches Cairn over the stdio shim.
fn pooled_mcp_attachment(orch: &Orchestrator) -> cairn_models::session::mcp::McpServer {
    let cairn_home = cairn_common::paths::cairn_home();
    let log_level = crate::config::settings::load_log_level(&orch.config_dir);
    crate::config::mcp_setup::stdio_attachment(
        &orch.mcp_binary_path,
        orch.mcp_callback_port,
        None,
        cairn_common::paths::env_str(),
        Some(cairn_home.to_string_lossy().as_ref()),
        log_level.as_str(),
    )
}

/// The pooled app-server that serves `codex_auth`, started if none is running,
/// and the Cairn account its login belongs to. Records nothing: a caller with
/// a run reports a failure there itself.
fn acquire_pooled_server(
    orch: &Orchestrator,
    codex_auth: &CodexAuth,
) -> Result<(Arc<super::pool::PooledAppServer>, Option<String>), String> {
    let runtime = orch.models_runtime().clone();
    let (oauth_state, account_id) = codex_oauth_state(orch, codex_auth)?;
    let pool_key = pool_key_for_auth(codex_auth, oauth_state.as_ref());
    orch.collect_warm_if_needed();

    // Get-or-spawn the shared app-server (spawn + initialize + login run ONCE per
    // key, serialized by the pool's per-key init lock).
    let codex_auth = codex_auth.clone();
    let server = orch
        .codex_pool
        .ensure(&pool_key, move || {
            let codex_path = runtime.host.resolve_cli(Cli::Codex).map_err(String::from)?;
            let login = codex_login(orch, &codex_auth, oauth_state.as_ref())?;
            // The home is hardened from a model catalog, which only a refresh
            // signed in as this identity writes. The pool may be the first thing
            // on this host to start Codex (a response needs no session first),
            // so it refreshes exactly as a dedicated session does, under the
            // same lock that keeps two writers off the shared home.
            let codex_home = {
                let _home = CODEX_START_LOCK
                    .lock()
                    .map_err(|_| "Codex start lock poisoned".to_string())?;
                runtime
                    .host
                    .prepare_codex_home(CodexModels::Refresh {
                        login: login.as_ref(),
                    })
                    .map_err(String::from)?
            };
            let mcp = pooled_mcp_attachment(orch);
            let (mcp_credential_var, mcp_credential) =
                crate::mcp::run_bearer::mcp_credential_env(orch, &mcp, None)?;
            // `CAIRN_RUN_ID` is a sentinel, not a run: every pooled tool call is
            // attributed by its `_meta.threadId`, and one that somehow carries
            // none fails loudly in run lookup instead of landing on some run.
            let env = BTreeMap::from([
                ("CODEX_HOME".to_string(), codex_home),
                (
                    "CAIRN_RUN_ID".to_string(),
                    "codex-pooled-app-server".to_string(),
                ),
            ]);
            let secrets = SecretEnv::new().with(mcp_credential_var, mcp_credential);
            // The process's own residence only; each thread passes its cwd.
            let residence = cairn_common::scratch::scratch_root();
            std::fs::create_dir_all(&residence)
                .map_err(|error| format!("Failed to prepare {}: {error}", residence.display()))?;
            let client = runtime.codex.start_server(
                runtime.host.as_ref(),
                ServerLaunch {
                    cli_path: &codex_path,
                    cwd: &residence.to_string_lossy(),
                    env: &env,
                    secrets: &secrets,
                    mcp: &crate::backends::cairn_servers(&mcp),
                    login: login.as_ref(),
                },
            )?;
            Ok((client, super::pool::pool_host(orch, oauth_state)))
        })
        .map_err(|error| format!("Failed to start Codex app-server pool: {error}"))?;
    Ok((server, account_id))
}

/// Start an ephemeral CALL as a thread on the shared pooled Codex app-server
/// (CAIRN-2549). One process hosts N call threads; each call finalizes its
/// one-shot turn and abandons its thread. Isolation: the per-call `RunHandle`
/// carries a NULL child so kill/stop/finalize never signal the shared process.
fn start_pooled_call(config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
    let start_time = std::time::Instant::now();
    let session_id = Some(config.session_start.session_id().to_string());
    let run_db = resolve_run_db(CODEX_BACKEND_NAME, orch, &config.run_id)?;
    let runtime = orch.models_runtime().clone();
    // Checked here, against the run, so a missing CLI is reported on its
    // transcript the way it always has been.
    resolve_codex_cli(
        orch,
        runtime.host.as_ref(),
        &config.run_id,
        session_id.as_deref(),
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
    let (server, account_id) = acquire_pooled_server(orch, &codex_auth).inspect_err(|error| {
        insert_error_event(orch, &config.run_id, session_id.as_deref(), error);
    })?;

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

    // A call is always a fresh thread.
    let spec = SessionSpec {
        start: SessionStart::New {
            session_id: config.session_start.session_id().to_string(),
        },
        model: config.model.as_ref().map(|model| model.to_string()),
        effort: config.reasoning_effort.clone(),
        approval: crate::backends::approval_mode(config.permissions.fence),
        output_schema: config.output_schema.clone(),
        cwd: config.working_dir.clone(),
        env: BTreeMap::new(),
        mcp: Some(crate::backends::cairn_servers(&config.mcp)),
        secrets: SecretEnv::new(),
        account_id,
        provider: ProviderSpec::Codex(CodexSpec {
            server: CodexServer::Shared(server.client()),
            base_instructions: base_instructions_from_segments(&prompt_segments),
            developer_instructions: developer_instructions_from_segments(&prompt_segments),
            service_tier: config.service_tier.clone(),
            native_goal: NativeGoal::Leave,
            resume_fallback: None,
        }),
    };
    let launched = runtime.codex.open(runtime.host.as_ref(), spec)?;
    let thread_id = launched
        .facts
        .backend_session_id
        .clone()
        .ok_or_else(|| "Codex opened a thread without an id".to_string())?;

    if let Some(ref sid) = session_id {
        let _ = set_session_backend_id(CODEX_BACKEND_NAME, &run_db, sid, &thread_id);
    }

    // Register `threadId -> run` and the per-call notification channel BEFORE
    // `turn/start`, so the first tool call routes to this run (demux #2) and the
    // dispatcher can deliver this thread's notifications (demux #1).
    let notification_rx = server.register_call(&thread_id, &config.run_id);

    persist_system_prompt_event(
        orch,
        &config.run_id,
        session_id.as_deref(),
        "codex",
        &prompt_segments,
    );

    let mut control = launched.control;
    control.open_turn(&config.message_content)?;

    if let Err(e) = transition_run_to_live(CODEX_BACKEND_NAME, orch, &run_db, &config.run_id) {
        log::warn!("Failed to transition codex pooled call to running: {}", e);
    }

    let SessionEvents::CodexAppServer {
        client,
        current_turn,
        ..
    } = launched.events
    else {
        return Err("a pooled Codex thread opened with no app-server".to_string());
    };

    // A NULL child is the isolation lever: `kill_session_with_reason` /
    // `stop_and_remove` resolve `child.lock().take()` to None and skip
    // `graceful_stop`, so the shared app-server is never signalled. Interrupt
    // still reaches this call through `CodexControl` -> `turn/interrupt`.
    let process_job_id: Option<String> = run_job_id(CODEX_BACKEND_NAME, &run_db, &config.run_id);
    {
        let mut active_process = ActiveProcess::new(
            launched.process,
            Arc::new(Mutex::new(Some(control))),
            session_id.clone(),
            process_job_id,
        );
        active_process.backend = Some("codex".to_string());
        active_process.model = config.model.as_ref().map(|m| m.as_str().to_string());
        active_process.account_id = config.account_id.clone();
        active_process.facts = Some(launched.facts);
        orch.process_state
            .register_process(config.run_id.clone(), active_process)?;
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
            current_turn,
            // Pool-scoped auth-token refresh is answered by the pool dispatcher
            // (it carries no threadId), so the per-call reader needs no oauth state.
            None,
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
    use cairn_models::codex::app_server::AppServerClient;
    use cairn_models::interface::session::SessionControl;
    use cairn_models::session::codex::CodexControl;

    #[test]
    fn codex_send_set_model_is_unsupported() {
        let mut control = CodexControl::new(
            Arc::new(AppServerClient::for_test_scripted(
                Vec::new(),
                Arc::new(Mutex::new(Vec::new())),
            )),
            "thread-1".to_string(),
            "/tmp/project".to_string(),
            None,
            None,
            None,
            Arc::new(Mutex::new(None)),
        );
        let err = control.set_model("gpt-5.4").unwrap_err();
        assert!(
            err.to_lowercase().contains("not yet supported"),
            "unexpected error: {err}"
        );
    }
}
