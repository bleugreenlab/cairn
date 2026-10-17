//! Application host for in-process HTTP agent turns.
//!
//! Each adapter resolves its provider's credentials and endpoint into a
//! `cairn_models::turn` protocol family and replays Cairn's stored transcript into
//! that family's native conversation. The facet's `FamilyTurn` owns the rest of
//! the request side: the loop, fitting, request bodies, replies, and native
//! appends. Core contributes `CairnEffects`, which streams into the live
//! transcript, dispatches Cairn tools, and persists events, and it maps the
//! turn's exit onto lifecycle policy.

mod host;
mod persist;
mod tools;
pub(in crate::backends) mod transcript;

#[cfg(test)]
pub(in crate::backends) mod scripted;
#[cfg(test)]
mod tests;

pub(in crate::backends) use cairn_models::wire::{repair, TurnToolCall, TurnUsage};
pub(in crate::backends) use persist::TranscriptSink;
pub(in crate::backends) use tools::render_tool_result;

use persist::{
    store_assistant_message, store_assistant_tool_call, store_success_result, store_tool_result,
};
use tools::execute_tool_call;

use crate::agent_process::completion::OwnedTurnOutcome;
use crate::agent_process::process::{RunHandle, SessionControl};
use crate::agent_process::stream::TokenCounts;
use crate::backends::run_state::{
    resolve_run_db, run_backend_db, run_job_id, set_session_backend_id, transition_run_to_live,
};
use crate::backends::{SessionConfig, SessionStart};
use crate::dispatch::DispatchOutput;
use crate::models::ContextTokenState;
use crate::orchestrator::lifecycle::apply_owned_turn_outcome;
use crate::orchestrator::session::{
    assemble_prompt_segments, flatten_prompt_segments, insert_error_event,
    persist_system_prompt_event,
};
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_models::interface::complete::CompletionError;
use cairn_models::interface::session::SessionInput;
use cairn_models::turn::{FamilyTurn, ProtocolFamily, TurnExit};
use cairn_models::wire::MessageContent;
use std::any::Any;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use cairn_models::turn::MAX_TOOL_ITERATIONS;

pub(super) trait HttpUsageExt {
    fn token_counts(&self) -> TokenCounts;
}

impl HttpUsageExt for TurnUsage {
    fn token_counts(&self) -> TokenCounts {
        TokenCounts::from(&cairn_models::usage::metered(self))
    }
}

#[derive(Debug, Clone)]
pub(in crate::backends) struct Connection {
    pub(in crate::backends) api_key: Option<String>,
}

/// What one provider contributes to an owned-loop turn: its identity, its
/// credential, the protocol family it speaks (configured with its endpoint,
/// dialect, and extras), and how Cairn's stored transcript replays into that
/// family's native conversation.
pub(in crate::backends) trait WireAdapter {
    /// The protocol family this provider's requests speak.
    type Family: ProtocolFamily;

    /// Stable backend key stored in transcript `raw` payloads and used to route
    /// the run to its owning DB (OpenRouter: `"openrouter"`).
    ///
    /// Borrowed from the adapter rather than `'static`, because a custom
    /// provider's key names the account that defines it and is therefore minted
    /// at configuration time rather than at compile time.
    fn backend_key(&self) -> &str;

    /// Human-readable backend name for logs and transcript error text. For a
    /// custom provider this is the name its operator gave it, which is what
    /// makes diagnostics about it legible.
    fn backend_name(&self) -> &str;
    /// The account this session's requests are made as, when the adapter
    /// chose one.
    fn account_id(&self) -> Option<String> {
        None
    }

    /// Fallback model id when the session carries none.
    fn default_model(&self) -> &'static str;

    /// Resolve provider connection details and own any missing-configuration error.
    fn connection(&self, orch: &Orchestrator) -> Result<Connection, String>;

    /// The protocol family this turn's requests go through, on `connection`.
    fn family(&self, connection: &Connection) -> Result<Self::Family, String>;

    /// The text a failed generation request reports in the transcript. A
    /// provider overrides this to say what its bare failures mean.
    fn explain_error(&self, error: CompletionError) -> String {
        error.into_stream_text()
    }

    /// Build the outgoing message array (system + prior transcript + new user).
    fn build_conversation(
        &self,
        orch: &Orchestrator,
        config: &SessionConfig,
        session_id: &str,
        system_prompt: &str,
    ) -> Result<Vec<<Self::Family as ProtocolFamily>::Message>, String>;

    /// The model's real context window, or `None` to disable outgoing trimming.
    fn context_window(&self, orch: &Orchestrator, model: &str) -> Option<i64>;
}

/// The control registered for an owned-loop run. There is no process to write
/// to; its one job is the cancel flag the streaming turn polls at SSE line
/// boundaries. An interrupt flips it, which breaks the SSE reader and drops the
/// connection (stopping billing). Every other write is refused: a message
/// starts a new turn, and model and permission changes apply to the next one.
pub(in crate::backends) struct HttpTurnControl {
    pub(in crate::backends) cancel: Arc<AtomicBool>,
    /// The provider's display name, for the refusals.
    pub(in crate::backends) provider: String,
}

impl SessionControl for HttpTurnControl {
    fn open_turn(&mut self, _content: &MessageContent) -> Result<(), String> {
        Err(format!(
            "{} HTTP turns do not keep a warm stdin; start a new run/turn",
            self.provider
        ))
    }

    fn send(&mut self, _input: &SessionInput<'_>) -> Result<(), String> {
        Err(format!(
            "{} HTTP turns do not keep a warm stdin; start a new run/turn",
            self.provider
        ))
    }

    fn interrupt(&mut self) -> Result<(), String> {
        self.cancel.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn set_model(&mut self, _model: &str) -> Result<(), String> {
        Err(format!(
            "{} model changes apply to the next HTTP turn",
            self.provider
        ))
    }

    fn set_permission_mode(&mut self, _mode: &str) -> Result<(), String> {
        Err(format!(
            "{} permission changes apply to the next HTTP turn",
            self.provider
        ))
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Start an owned-loop session: resolve the api key and owning DB, persist the
/// system prompt, register the run handle (with its cancel stdin), then spawn the
/// turn loop on a dedicated thread with a panic guard. Generic over the wire
/// adapter; OpenRouter passes `OpenRouterAdapter`.
pub(in crate::backends) fn start_session<A>(
    config: SessionConfig,
    orch: &Orchestrator,
    adapter: A,
) -> Result<(), String>
where
    A: WireAdapter + Send + 'static,
{
    let session_id = config.session_start.session_id().to_string();
    // Owned rather than borrowed from the adapter, which moves into the turn
    // thread below. For a custom provider these are configuration — the key its
    // account minted, the name its operator gave it — not compile-time literals.
    let backend_key = adapter.backend_key().to_string();
    let backend_name = adapter.backend_name().to_string();
    let connection = adapter.connection(orch).inspect_err(|error| {
        insert_error_event(orch, &config.run_id, Some(&session_id), error);
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
    // Resolve the run's owning DB ONCE (CAIRN-2208) and thread it through the
    // run-state and streaming-transcript writes below. A team run lives wholly in
    // its synced replica; targeting the private DB would fail the
    // message_streams→runs foreign key. Fail-closed.
    let run_db = match resolve_run_db(&backend_name, orch, &config.run_id) {
        Ok(db) => db,
        Err(error) => {
            insert_error_event(orch, &config.run_id, Some(&session_id), &error);
            return Err(error);
        }
    };

    if matches!(
        config.session_start,
        SessionStart::New { .. } | SessionStart::Fork { .. }
    ) {
        persist_system_prompt_event(
            orch,
            &config.run_id,
            Some(&session_id),
            &backend_key,
            &prompt_segments,
        );
    }
    // Flatten the same segments that were (or would be) persisted. Each HTTP turn
    // re-sends the whole message array with no server-side system-prompt
    // retention, so the full harness contract (Cairn prompt + workspace + agent +
    // orientation tail) must lead the system message on New and Resume alike.
    let system_prompt = flatten_prompt_segments(&prompt_segments);

    if let Err(error) = set_session_backend_id(&backend_name, &run_db, &session_id, &session_id) {
        log::warn!("Failed to set {backend_name} backend id: {error}");
    }
    if let Err(error) = transition_run_to_live(&backend_name, orch, &run_db, &config.run_id) {
        log::warn!("Failed to transition {backend_name} run to live: {error}");
    }

    let process_job_id = run_job_id(&backend_name, &run_db, &config.run_id);
    let cancel = Arc::new(AtomicBool::new(false));
    {
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(Box::new(HttpTurnControl {
                cancel: cancel.clone(),
                provider: backend_name.clone(),
            })))),
            Some(session_id.clone()),
            process_job_id,
        );
        handle.backend = Some(backend_key.clone());
        handle.model = config.model.as_ref().map(|model| model.to_string());
        handle.facts = Some(cairn_models::interface::session::SessionFacts {
            provider: backend_key.clone(),
            // An HTTP session's conversation is Cairn's own transcript.
            backend_session_id: Some(session_id.clone()),
            account_id: adapter.account_id(),
            host: cairn_models::interface::LOCAL_HOST.to_string(),
            model: handle.model.clone(),
        });
        // An HTTP-loop backend owns its turn/tool loop in-process and has no warm
        // process, so foreground questions and inline delegated tasks suspend the
        // turn rather than inline-waiting. The blocking handlers key off this flag.
        handle.owns_turn_loop = true;
        if let Err(error) = orch
            .process_state
            .register_process(config.run_id.clone(), handle)
        {
            log::warn!("Failed to register {backend_name} run handle: {error}");
        }
    }

    let orch_clone = orch.clone();
    let run_id = config.run_id.clone();
    let error_session_id = session_id.clone();
    std::thread::spawn(move || {
        drive_http_turn(
            &orch_clone,
            &run_id,
            &error_session_id,
            &backend_name,
            || {
                run_http_turn(
                    &orch_clone,
                    run_db,
                    config,
                    connection,
                    session_id,
                    None,
                    cancel,
                    system_prompt,
                    adapter,
                )
            },
        );
    });

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_http_turn<A: WireAdapter>(
    orch: &Orchestrator,
    run_db: Arc<LocalDb>,
    config: SessionConfig,
    connection: Connection,
    session_id: String,
    turn_id: Option<String>,
    cancel: Arc<AtomicBool>,
    system_prompt: String,
    adapter: A,
) -> Result<TurnExit, String> {
    let backend_key = adapter.backend_key().to_string();
    let backend_name = adapter.backend_name().to_string();
    let model = config
        .model
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| adapter.default_model().to_string());
    let conversation = adapter.build_conversation(orch, &config, &session_id, &system_prompt)?;
    let context_window = adapter.context_window(orch, &model);
    let family = adapter.family(&connection)?;
    let generation_config = (&config).into();
    let complete = orch.models_runtime().complete.clone();
    let mut turn = FamilyTurn {
        family,
        model: model.clone(),
        config: generation_config,
        context_window,
        conversation,
        effects: host::CairnEffects {
            orch,
            run_db,
            config,
            session_id,
            turn_id,
            adapter,
            backend_key,
            backend_name,
            model,
            context_window,
        },
        complete: &*complete,
        cancel,
    };
    turn.run()
}

/// Catch both loop and success-finalization panics, preserving the former
/// worker's failure boundary while the loop itself only reports an exit.
fn drive_http_turn(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    backend_name: &str,
    run: impl FnOnce() -> Result<TurnExit, String>,
) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let exit = run()?;
        apply_owned_turn_outcome(orch, run_id, session_id, backend_name, exit.into());
        Ok::<(), String>(())
    }));
    let failure = match result {
        Ok(Ok(())) => return,
        Ok(Err(message)) => OwnedTurnOutcome::Failed { message },
        Err(panic) => OwnedTurnOutcome::Panicked {
            message: panic_message(panic.as_ref()),
        },
    };
    apply_owned_turn_outcome(orch, run_id, session_id, backend_name, failure);
}

/// Extract a human-readable message from a caught panic payload.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Push a context-token snapshot to the frontend so the live occupancy gauge
/// (the radial context dial) updates during the turn, mirroring the Claude and
/// Codex backends. Fires once per generation when usage is reported, keyed on the
/// durable session id and carrying the model's real catalog window so the dial
/// renders its ring rather than a bare token count.
fn emit_context_snapshot(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
    backend_key: &str,
    model: &str,
    context_window: Option<i64>,
    usage: &TurnUsage,
) {
    let counts = usage.token_counts();
    let input = counts.input.unwrap_or(0) as i64;
    let output = counts.output.unwrap_or(0) as i64;
    orch.store_context_token_snapshot(ContextTokenState {
        run_id: run_id.to_string(),
        session_id: Some(session_id.to_string()),
        backend: backend_key.to_string(),
        model: Some(model.to_string()),
        // Persisted usage components are disjoint, so occupancy is the complete
        // input prompt (uncached + cached) plus the response output.
        used_tokens: input + counts.cache_read.unwrap_or(0) as i64 + output,
        context_window,
        auto_compact_limit: None,
        reasoning_tokens: counts.thinking.map(|thinking| thinking as i64),
        last_output_tokens: counts.output.map(|output| output as i64),
        captured_at: chrono::Utc::now().timestamp(),
    });
}
